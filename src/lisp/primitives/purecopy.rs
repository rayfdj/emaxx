use super::*;
use crate::lisp::types::Kind;

fn purify_table(interp: &Interpreter, env: &Env) -> Option<crate::lisp::types::HashTableRef> {
    let Kind::HashTable(id) = (interp.lookup_var("purify-flag", env)?).kind() else {
        return None;
    };
    Some(id)
}

fn hash_cons_lookup(interp: &Interpreter, value: &Value, env: &Env) -> Option<Value> {
    purify_table(interp, env).and_then(|id| interp.equal_hash_lookup(id, value, env).flatten())
}

fn hash_cons_insert(interp: &mut Interpreter, value: Value, env: &Env) -> Value {
    if let Some(id) = purify_table(interp, env) {
        interp.equal_hash_put(id, value, value, env);
    }
    value
}

fn purecopy_cons_chain(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    if is_vector_value(value) && vector_items(value)?.is_empty() {
        return Ok(*value);
    }

    let mut copied_cars = Vec::new();
    let mut cursor = *value;
    let tail = loop {
        if let Some(cached) = hash_cons_lookup(interp, &cursor, env) {
            break cached;
        }
        let Some((car, cdr)) = cursor.cons_values() else {
            break interp.with_lisp_stack_roots(&copied_cars, |interp| {
                purecopy_inner(interp, &cursor, env)
            })?;
        };
        // pure_cons captures both fields before copying either. The saved
        // cdr and earlier copied cars must survive a message callback's GC.
        let copied_car = interp.with_lisp_stack_roots(&(&copied_cars, cdr), |interp| {
            purecopy_inner(interp, &car, env)
        })?;
        copied_cars.push(copied_car);
        cursor = cdr;
    };

    let mut copied_tail = tail;
    for copied_car in copied_cars.into_iter().rev() {
        copied_tail = hash_cons_insert(interp, Value::cons(copied_car, copied_tail), env);
    }
    Ok(copied_tail)
}

fn purecopy_vector(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let items = vector_items(value)?;
    if items.is_empty() {
        // GNU's zero-length vector is a static pure object, so PURE_P makes
        // alloc.c:purecopy return it unchanged.
        return Ok(*value);
    }
    // alloc.c:purecopy snapshots all fields with memcpy before recursively
    // copying any of them. Our destination is still GC storage, so root it
    // across callbacks; its fields keep both copied and pending values live.
    let copied = Value::vector(items);
    let Kind::Vector(vector) = copied.kind() else {
        unreachable!("Value::vector creates an ordinary vector")
    };
    interp.with_lisp_stack_roots(&copied, |interp| {
        for (index, item) in vector.slots().enumerate() {
            vector.set(index, purecopy_inner(interp, &item, env)?);
        }
        Ok(hash_cons_insert(interp, copied, env))
    })
}

fn purecopy_hash_table(
    interp: &mut Interpreter,
    table: crate::lisp::types::HashTableRef,
    env: &mut Env,
) -> Result<Value, LispError> {
    let source = Value::HashTable(table);
    if table.weakness() != 0 || !table.purecopy() {
        return Ok(source);
    }
    if let Some(cached) = hash_cons_lookup(interp, &source, env) {
        return Ok(cached);
    }

    // alloc.c:purecopy_hash_table copies hashes and both chains verbatim.
    // Purifying keys must not call user hash functions or compact holes.
    let copy = table.copy();
    let copied = Value::HashTable(copy);
    interp.with_lisp_stack_roots(&copied, |interp| {
        for slot in 0..copy.capacity() {
            if let Some((key, value)) = copy.entry(slot) {
                copy.set_key(slot, purecopy_inner(interp, &key, env)?);
                copy.set_value(slot, purecopy_inner(interp, &value, env)?);
            }
        }
        copy.set_mutable(false);
        Ok(hash_cons_insert(interp, copied, env))
    })
}

fn purecopy_record(interp: &mut Interpreter, id: u64, env: &mut Env) -> Result<Value, LispError> {
    let source = interp.record_value(id);
    if let Some(cached) = hash_cons_lookup(interp, &source, env) {
        return Ok(cached);
    }
    let record = interp
        .find_record(id)
        .cloned()
        .ok_or_else(|| LispError::TypeError("record".into(), format!("record<{id}>")))?;
    if record.kind != crate::lisp::eval::RecordKind::Closure {
        return Err(LispError::Signal(format!(
            "Don't know how to purify: {} ({:?}, {:?})",
            source.type_name(),
            record.kind,
            record.type_tag,
        )));
    }

    let mut slots = Vec::with_capacity(record.slots.len());
    for slot in &record.slots {
        let copied = interp.with_lisp_stack_roots(&(&record.slots, &slots), |interp| {
            purecopy_inner(interp, slot, env)
        })?;
        slots.push(copied);
    }
    let copied = interp.copy_record(id)?;
    let Kind::Record(copied_id) = copied.kind() else {
        unreachable!("copy_record preserves the record representation")
    };
    interp
        .find_record_mut(copied_id)
        .expect("copied record remains live")
        .slots = slots;
    Ok(hash_cons_insert(interp, Value::Record(copied_id), env))
}

fn purecopy_inner(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    if matches!(value.kind(), Kind::SymbolWithPos(_)) && symbols_with_pos_enabled(interp, env) {
        // SYMBOLP includes PVEC_SYMBOL_WITH_POS while this flag is active,
        // so alloc.c:Fpurecopy returns it unchanged with ordinary symbols.
        return Ok(*value);
    }
    if matches!(value.kind(), Kind::Record(id)
        if interp.find_record(id).is_some_and(|record|
            record.kind == crate::lisp::eval::RecordKind::NativeCompiledFunction))
    {
        // Native compiled functions use GNU's PVEC_SUBR representation and
        // therefore take alloc.c:Fpurecopy's SUBRP already-pure return.
        return Ok(*value);
    }
    match value.kind() {
        Kind::Nil
        | Kind::T
        | Kind::Integer(_)
        | Kind::Symbol(_)
        | Kind::BuiltinFunc(_)
        | Kind::Marker(_)
        | Kind::Overlay(_) => return Ok(*value),
        _ => {}
    }
    if matches!(value.kind(), Kind::StringObject(state) if !state.borrow().props.is_empty()) {
        // A callback may detach this string from the original graph before
        // collecting. The message's formatted string is a different object.
        interp.with_lisp_stack_roots(value, |interp| {
            super::dispatch::display::message_with_string(
                interp,
                "Dropping text-properties while making string `%s' pure",
                *value,
                env,
            )
        })?;
    }
    if let Some(cached) = hash_cons_lookup(interp, value, env) {
        return Ok(cached);
    }

    let copied = match value.kind() {
        Kind::BigInteger(integer) => Value::big_integer((*integer).clone()),
        Kind::Float(number) => Value::Float(number),
        Kind::String(text) => Value::String(text.to_string().into()),
        Kind::StringObject(_) => {
            let string = string_like(value).expect("StringObject is string-like");
            if string.extended_chars.is_empty() {
                Value::String(string.text.into())
            } else {
                make_shared_string_value_with_extended_chars(
                    string.text,
                    Vec::new(),
                    string.multibyte,
                    string.extended_chars,
                )
            }
        }
        Kind::Vector(_) => return purecopy_vector(interp, value, env),
        Kind::Cons(_) if is_vector_value(value) => {
            return purecopy_vector(interp, value, env);
        }
        Kind::Cons(_) => return purecopy_cons_chain(interp, value, env),
        Kind::Lambda(lambda) => {
            let slots = interp.interpreted_closure_slots(&lambda);
            let mut copied_slots = Vec::with_capacity(slots.len());
            for slot in &slots {
                let copied = interp.with_lisp_stack_roots(&(&slots, &copied_slots), |interp| {
                    purecopy_inner(interp, slot, env)
                })?;
                copied_slots.push(copied);
            }
            interp.make_interpreted_closure_value(&copied_slots)?
        }
        Kind::LispRecord(record) => {
            let copy = record.shallow_copy();
            // As for vectors, snapshot before callbacks and keep every
            // copied or pending field live in our collectable destination.
            interp.with_lisp_stack_roots(&Value::LispRecord(copy), |interp| {
                for (index, field) in copy.slots().enumerate() {
                    copy.set(index, purecopy_inner(interp, &field, env)?);
                }
                Ok::<Value, LispError>(Value::LispRecord(copy))
            })?
        }
        Kind::HashTable(table) => purecopy_hash_table(interp, table, env)?,
        Kind::Record(id) => {
            return interp
                .with_lisp_stack_roots(value, |interp| purecopy_record(interp, id.id, env));
        }
        Kind::Buffer(_)
        | Kind::CharTable(_)
        | Kind::SubCharTable(_)
        | Kind::Frame(_)
        | Kind::Terminal(_)
        | Kind::SymbolWithPos(_)
        | Kind::Finalizer(_)
        | Kind::ReaderForm(_)
        | Kind::Unbound => {
            return Err(LispError::Signal(format!(
                "Don't know how to purify: {}",
                value.type_name()
            )));
        }
        Kind::Nil
        | Kind::T
        | Kind::Integer(_)
        | Kind::Symbol(_)
        | Kind::BuiltinFunc(_)
        | Kind::Marker(_)
        | Kind::Overlay(_) => unreachable!("returned before hash-cons lookup"),
    };
    Ok(hash_cons_insert(interp, copied, env))
}

/// alloc.c:Fpurecopy.  GNU enables this while constructing the dumped Lisp
/// image and optionally supplies an `equal` hash table to deduplicate objects.
pub(crate) fn purecopy_value(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let purify = interp.lookup_var("purify-flag", env).unwrap_or(Value::Nil);
    if purify.is_nil() {
        return Ok(*value);
    }
    // GNU reads Vpurify_flag again at each hash lookup and insertion. A
    // redisplay callback can enable, disable or replace hash consing midway.
    interp.with_lisp_stack_roots(value, |interp| purecopy_inner(interp, value, env))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vector(values: impl IntoIterator<Item = Value>) -> Value {
        Value::list(std::iter::once(Value::symbol("vector-literal")).chain(values))
    }

    #[test]
    fn purecopy_keeps_vectors_vectorlike_and_hash_conses_equal_copies() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let purify = json::make_hash_table(&mut interp, "equal", Vec::new());
        interp.define_special_variable("purify-flag", purify);

        let source = vector([Value::Integer(7), vector([Value::string("nested")])]);
        let equal_source = vector([Value::Integer(7), vector([Value::string("nested")])]);
        let copied = purecopy_value(&mut interp, &source, &mut env)
            .expect("vector should be copied into pure storage");
        let equal_copy = purecopy_value(&mut interp, &equal_source, &mut env)
            .expect("equal vector should be hash-consed in pure storage");

        assert!(is_vector_value(&copied));
        let (
            Kind::Vector(source_vector),
            Kind::Vector(copied_vector),
            Kind::Vector(equal_copy_vector),
        ) = (source.kind(), copied.kind(), equal_copy.kind())
        else {
            panic!("purecopy must preserve GNU's vector object class")
        };
        assert!(!source_vector.ptr_eq(&copied_vector));
        assert!(copied_vector.ptr_eq(&equal_copy_vector));

        let source_items = vector_items(&source).expect("source should remain vectorlike");
        let copied_items = vector_items(&copied).expect("copy should remain vectorlike");
        let (Kind::Vector(source_nested), Kind::Vector(copied_nested)) =
            (source_items[1].kind(), copied_items[1].kind())
        else {
            panic!("nested values remain vectors")
        };
        assert!(!source_nested.ptr_eq(&copied_nested));
    }
}
