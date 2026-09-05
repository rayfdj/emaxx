use super::*;

fn purify_table(interp: &Interpreter, env: &Env) -> Option<u64> {
    let Value::Record(id) = interp.lookup_var("purify-flag", env)? else {
        return None;
    };
    json::is_hash_table(interp, &Value::Record(id)).then_some(id)
}

fn hash_cons_lookup(
    interp: &Interpreter,
    table: Option<u64>,
    value: &Value,
    env: &Env,
) -> Option<Value> {
    table.and_then(|id| interp.equal_hash_lookup(id, value, env).flatten())
}

fn hash_cons_insert(
    interp: &mut Interpreter,
    table: Option<u64>,
    value: Value,
    env: &Env,
) -> Value {
    if let Some(id) = table {
        interp.equal_hash_put(id, value.clone(), value.clone(), env);
    }
    value
}

fn purecopy_cons_chain(
    interp: &mut Interpreter,
    value: &Value,
    table: Option<u64>,
    env: &mut Env,
) -> Result<Value, LispError> {
    if is_vector_value(value) && vector_items(value)?.is_empty() {
        return Ok(value.clone());
    }

    let mut source_cells = Vec::new();
    let mut cursor = value.clone();
    let tail = loop {
        if let Some(cached) = hash_cons_lookup(interp, table, &cursor, env) {
            break cached;
        }
        let Some((car, cdr)) = cursor.cons_values() else {
            break purecopy_inner(interp, &cursor, table, env)?;
        };
        source_cells.push((cursor, purecopy_inner(interp, &car, table, env)?));
        cursor = cdr;
    };

    let mut copied_tail = tail;
    for (_, copied_car) in source_cells.into_iter().rev() {
        copied_tail = hash_cons_insert(interp, table, Value::cons(copied_car, copied_tail), env);
    }
    Ok(copied_tail)
}

fn purecopy_vector(
    interp: &mut Interpreter,
    value: &Value,
    table: Option<u64>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let items = vector_items(value)?;
    if items.is_empty() {
        // GNU's zero-length vector is a static pure object, so PURE_P makes
        // alloc.c:purecopy return it unchanged.
        return Ok(value.clone());
    }
    let mut copied = Vec::with_capacity(items.len());
    for item in items {
        copied.push(purecopy_inner(interp, &item, table, env)?);
    }
    Ok(hash_cons_insert(interp, table, Value::vector(copied), env))
}

fn purecopy_hash_table(
    interp: &mut Interpreter,
    id: u64,
    table: Option<u64>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let source = Value::Record(id);
    let record = interp
        .find_record(id)
        .cloned()
        .ok_or_else(|| LispError::TypeError("hash-table".into(), format!("record<{id}>")))?;
    let weakness = record.slots.get(5).cloned().unwrap_or(Value::Nil);
    let copy_to_pure = record.slots.get(6).is_some_and(Value::is_truthy);
    if weakness.is_truthy() || !copy_to_pure {
        return Ok(source);
    }
    if let Some(cached) = hash_cons_lookup(interp, table, &source, env) {
        return Ok(cached);
    }

    let (_, entries) = json::hash_table_entries(interp, &source)
        .ok_or_else(|| LispError::TypeError("hash-table".into(), format!("record<{id}>")))?;
    let mut copied_entries = Vec::with_capacity(entries.len());
    for (key, value) in entries {
        copied_entries.push((
            purecopy_inner(interp, &key, table, env)?,
            purecopy_inner(interp, &value, table, env)?,
        ));
    }

    let copied = interp.copy_record(id)?;
    set_hash_table_entries(interp, &copied, copied_entries)?;
    let Value::Record(copied_id) = copied else {
        unreachable!("copy_record preserves the hash-table representation")
    };
    interp.mark_hash_table_immutable(copied_id);
    Ok(hash_cons_insert(
        interp,
        table,
        Value::Record(copied_id),
        env,
    ))
}

fn purecopy_record(
    interp: &mut Interpreter,
    id: u64,
    table: Option<u64>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let source = Value::Record(id);
    if let Some(cached) = hash_cons_lookup(interp, table, &source, env) {
        return Ok(cached);
    }
    let record = interp
        .find_record(id)
        .cloned()
        .ok_or_else(|| LispError::TypeError("record".into(), format!("record<{id}>")))?;
    if record.kind == crate::lisp::eval::RecordKind::HashTable {
        return purecopy_hash_table(interp, id, table, env);
    }
    if !matches!(
        record.kind,
        crate::lisp::eval::RecordKind::Record | crate::lisp::eval::RecordKind::Closure
    ) {
        return Err(LispError::Signal(format!(
            "Don't know how to purify: {} ({:?}, {:?})",
            source.type_name(),
            record.kind,
            record.type_tag,
        )));
    }

    let type_tag = purecopy_inner(interp, &record.type_tag, table, env)?;
    let mut slots = Vec::with_capacity(record.slots.len());
    for slot in record.slots {
        slots.push(purecopy_inner(interp, &slot, table, env)?);
    }
    let copied = interp.copy_record(id)?;
    let Value::Record(copied_id) = copied else {
        unreachable!("copy_record preserves the record representation")
    };
    if record.kind == crate::lisp::eval::RecordKind::Record {
        interp.retag_record(copied_id, type_tag)?;
    }
    interp
        .find_record_mut(copied_id)
        .expect("copied record remains live")
        .slots = slots;
    Ok(hash_cons_insert(
        interp,
        table,
        Value::Record(copied_id),
        env,
    ))
}

fn purecopy_inner(
    interp: &mut Interpreter,
    value: &Value,
    table: Option<u64>,
    env: &mut Env,
) -> Result<Value, LispError> {
    if matches!(value, Value::Record(id)
        if interp.find_record(*id).is_some_and(|record|
            record.kind == crate::lisp::eval::RecordKind::SymbolWithPos))
        && symbols_with_pos_enabled(interp, env)
    {
        // SYMBOLP includes PVEC_SYMBOL_WITH_POS while this flag is active,
        // so alloc.c:Fpurecopy returns it unchanged with ordinary symbols.
        return Ok(value.clone());
    }
    if matches!(value, Value::Record(id)
        if interp.find_record(*id).is_some_and(|record|
            record.kind == crate::lisp::eval::RecordKind::NativeCompiledFunction))
    {
        // Native compiled functions use GNU's PVEC_SUBR representation and
        // therefore take alloc.c:Fpurecopy's SUBRP already-pure return.
        return Ok(value.clone());
    }
    match value {
        Value::Nil
        | Value::T
        | Value::Integer(_)
        | Value::Symbol(_)
        | Value::BuiltinFunc(_)
        | Value::Marker(_)
        | Value::Overlay(_) => return Ok(value.clone()),
        _ => {}
    }
    if let Some(cached) = hash_cons_lookup(interp, table, value, env) {
        return Ok(cached);
    }

    let copied = match value {
        Value::BigInteger(integer) => Value::big_integer((**integer).clone()),
        Value::Float(number) => Value::Float(number.clone()),
        Value::String(text) => Value::String(text.to_string().into()),
        Value::StringObject(_) => {
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
        Value::Vector(_) => return purecopy_vector(interp, value, table, env),
        Value::Cons(_) if is_vector_value(value) => {
            return purecopy_vector(interp, value, table, env);
        }
        Value::Cons(_) => return purecopy_cons_chain(interp, value, table, env),
        Value::Lambda(lambda) => {
            let slots = interp.interpreted_closure_slots(lambda);
            let mut copied_slots = Vec::with_capacity(slots.len());
            for slot in slots {
                copied_slots.push(purecopy_inner(interp, &slot, table, env)?);
            }
            interp.make_interpreted_closure_value(&copied_slots)?
        }
        Value::Record(id) => return purecopy_record(interp, *id, table, env),
        Value::Buffer(_)
        | Value::CharTable(_)
        | Value::Frame(_)
        | Value::Terminal(_)
        | Value::Finalizer(_)
        | Value::ReaderForm(_)
        | Value::Unbound => {
            return Err(LispError::Signal(format!(
                "Don't know how to purify: {}",
                value.type_name()
            )));
        }
        Value::Nil
        | Value::T
        | Value::Integer(_)
        | Value::Symbol(_)
        | Value::BuiltinFunc(_)
        | Value::Marker(_)
        | Value::Overlay(_) => unreachable!("returned before hash-cons lookup"),
    };
    Ok(hash_cons_insert(interp, table, copied, env))
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
        return Ok(value.clone());
    }
    purecopy_inner(interp, value, purify_table(interp, env), env)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::rc::Rc;

    fn vector(values: impl IntoIterator<Item = Value>) -> Value {
        Value::vector(values)
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
            Value::Vector(source_vector),
            Value::Vector(copied_vector),
            Value::Vector(equal_copy_vector),
        ) = (&source, &copied, &equal_copy)
        else {
            panic!("purecopy must preserve GNU's vector object class")
        };
        assert!(!Rc::ptr_eq(source_vector, copied_vector));
        assert!(Rc::ptr_eq(copied_vector, equal_copy_vector));

        let source_items = vector_items(&source).expect("source should remain vectorlike");
        let copied_items = vector_items(&copied).expect("copy should remain vectorlike");
        let (Value::Vector(source_nested), Value::Vector(copied_nested)) =
            (&source_items[1], &copied_items[1])
        else {
            panic!("nested values remain vectors")
        };
        assert!(!Rc::ptr_eq(source_nested, copied_nested));
    }
}
