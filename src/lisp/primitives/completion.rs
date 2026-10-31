use super::*;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;
use crate::lisp::types::obarray::{self, Buckets};

pub(crate) const OBARRAY_RECORD_TYPE: &str = "obarray";

#[derive(Clone)]
pub(crate) struct CompletionCandidate {
    name: String,
    predicate_args: Vec<Value>,
}

pub(crate) fn ensure_interaction_allowed(interp: &Interpreter, env: &Env) -> Result<(), LispError> {
    if !interaction_allowed(interp, env) {
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("inhibited-interaction".into()),
            Value::String("Interaction inhibited".into()),
        ])));
    }
    Ok(())
}

pub(crate) fn interaction_allowed(interp: &Interpreter, env: &Env) -> bool {
    !interp
        .lookup_var("inhibit-interaction", env)
        .is_some_and(|value| value.is_truthy())
}

pub(crate) fn is_window_value(interp: &Interpreter, value: &Value) -> bool {
    matches!(value.kind(), Kind::Symbol(symbol) if symbol == "window")
        || matches!(value.kind(), Kind::Record(id) if interp.find_record(id).is_some_and(|record|
            record.kind == crate::lisp::eval::RecordKind::Window))
}

#[cfg(test)]
pub(crate) fn make_obarray(interp: &mut Interpreter) -> Value {
    make_obarray_with_bits(interp, obarray::DEFAULT_BITS)
}

/// lread.c:make_obarray: an obarray of 2^BITS empty buckets.
pub(crate) fn make_obarray_with_bits(interp: &mut Interpreter, bits: u32) -> Value {
    interp.create_pseudovector(
        crate::lisp::eval::RecordKind::Obarray,
        OBARRAY_RECORD_TYPE,
        obarray::record_slots(bits),
    )
}

/// The bucket slots of the private obarray record ID.
fn private_obarray_slots(record: &crate::lisp::eval::RecordState) -> Option<&[Value]> {
    (record.has_symbol_type(OBARRAY_RECORD_TYPE) && obarray::is_record_slots(&record.slots))
        .then_some(record.slots.as_slice())
}

pub(crate) fn clear_obarray(interp: &mut Interpreter, obarray: &Value) -> Result<Value, LispError> {
    let Kind::Record(id) = obarray.kind() else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    let Some(record) = interp.find_record_mut(id) else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    if record.has_symbol_type(OBARRAY_RECORD_TYPE) {
        // Fobarray_clear.  Not GNU: the initial obarray keeps its symbols,
        // which this implementation's own code names by their text.
        if obarray::is_record_slots(&record.slots) {
            obarray::RecordBuckets(&mut record.slots).clear();
        }
    } else if record.has_symbol_type(ABBREV_TABLE_RECORD_TYPE) {
        if record.slots.len() <= ABBREV_TABLE_ENTRIES_SLOT {
            record
                .slots
                .resize(ABBREV_TABLE_ENTRIES_SLOT + 1, Value::Nil);
        }
        record.slots[ABBREV_TABLE_ENTRIES_SLOT] = Value::Nil;
    } else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    }
    Ok(Value::Nil)
}

pub(crate) fn is_obarray_like_value(interp: &Interpreter, value: &Value) -> bool {
    let Kind::Record(id) = value.kind() else {
        return false;
    };
    interp
        .find_record(id)
        .is_some_and(|record| record.kind == crate::lisp::eval::RecordKind::Obarray)
}

pub(crate) fn obarray_symbols(
    interp: &Interpreter,
    obarray: &Value,
) -> Result<Vec<Value>, LispError> {
    // Read-only face of check_obarray_slow's legacy-vector rule: a vector
    // already carrying an obarray in slot 0 reads through it, and an
    // untouched one (slot 0 still the fixnum 0) reads as empty.
    if is_vector_value(obarray) {
        let slots = vector_items(obarray)?;
        if let Some(current) = slots.first() {
            if is_obarray_like_value(interp, current) {
                return obarray_symbols(interp, current);
            }
            if matches!(current.kind(), Kind::Integer(0)) {
                return Ok(Vec::new());
            }
        }
    }
    let Kind::Record(id) = obarray.kind() else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    if interp.is_standard_obarray_id(id.id) {
        return Ok(interp
            .known_symbols_shared()
            .iter()
            .cloned()
            .map(|symbol| match symbol.as_str() {
                "nil" => Value::Nil,
                "t" => Value::T,
                _ => Value::Symbol(symbol),
            })
            .collect());
    }
    let Some(record) = interp.find_record(id) else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    if record.has_symbol_type(ABBREV_TABLE_RECORD_TYPE) {
        return abbrev_table_entries(interp, obarray).map(|entries| {
            std::iter::once(Value::Symbol(abbrev_symbol_name(id.id, "").into()))
                .chain(
                    entries
                        .into_iter()
                        .map(|(name, _, _)| Value::Symbol(abbrev_symbol_name(id.id, &name).into())),
                )
                .collect()
        });
    }
    if let Some(slots) = private_obarray_slots(record) {
        // DOOBARRAY's walk.
        return Ok(obarray::RecordBucketsRef(slots)
            .symbols()
            .into_iter()
            .map(Value::Symbol)
            .collect());
    }
    Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray))
}

pub(crate) fn obarray_symbol_matches(value: &Value, symbol_name: &str) -> bool {
    matches!(
        (value.kind(), symbol_name),
        (Kind::Nil, "nil") | (Kind::T, "t")
    ) || matches!(
        value.kind(),
        Kind::Symbol(name) if crate::lisp::types::visible_symbol_name(&name) == symbol_name
    )
}

/// Finternal__obarray_buckets: each bucket's symbols in chain order.
pub(crate) fn obarray_bucket_lists(
    interp: &mut Interpreter,
    obarray: &Value,
) -> Result<Value, LispError> {
    let obarray = &coerce_legacy_vector_obarray(interp, obarray)?;
    let Kind::Record(id) = obarray.kind() else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    let lists = if interp.is_standard_obarray_id(id.id) {
        interp.standard_obarray_bucket_lists()
    } else {
        let Some(record) = interp.find_record(id) else {
            return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
        };
        match private_obarray_slots(record) {
            Some(slots) => obarray::RecordBucketsRef(slots).bucket_lists(),
            // Not GNU: an abbrev table's symbols, each a bucket of its own.
            None => {
                return Ok(Value::list(
                    obarray_symbols(interp, obarray)?
                        .into_iter()
                        .map(|symbol| Value::list([symbol])),
                ));
            }
        }
    };
    Ok(Value::list(lists.into_iter().map(|bucket| {
        Value::list(bucket.into_iter().map(Value::Symbol))
    })))
}

/// lread.c check_obarray_slow: a legacy VECTOR obarray whose first slot
/// is the fixnum 0 receives a real obarray object stored there on first
/// use (the rest of the vector stays unused), and a vector already
/// carrying one answers with it.  Everything else of vector shape is
/// still not an obarray.
pub(crate) fn coerce_legacy_vector_obarray(
    interp: &mut Interpreter,
    obarray: &Value,
) -> Result<Value, LispError> {
    if !is_vector_value(obarray) {
        return Ok(*obarray);
    }
    let slots = vector_items(obarray)?;
    let Some(current) = slots.first().cloned() else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    if is_obarray_like_value(interp, &current) {
        return Ok(current);
    }
    if matches!(current.kind(), Kind::Integer(0)) {
        // make_obarray (0): one bucket.
        let fresh = make_obarray_with_bits(interp, 0);
        aset_vector_value(obarray, 0, fresh)?;
        return Ok(fresh);
    }
    Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray))
}

pub(crate) fn intern_in_obarray(
    interp: &mut Interpreter,
    obarray: &Value,
    symbol_name: &str,
) -> Result<Value, LispError> {
    // check_obarray's common case, the initial obarray itself: oblookup,
    // and on a miss intern_driver with the name made only then.
    if let Kind::Record(id) = obarray.kind()
        && interp.is_standard_obarray_id(id.id)
    {
        return Ok(Value::Symbol(
            crate::lisp::types::SymbolName::intern_initial_with(symbol_name, || {
                Value::string(symbol_name)
            }),
        ));
    }
    intern_in_obarray_with_name(interp, obarray, symbol_name, |_| {
        Ok(Value::string(symbol_name))
    })
}

/// lread.c:intern_driver is reached only after oblookup misses. In particular,
/// Fintern must not allocate/purecopy a name string when a symbol already exists.
pub(crate) fn intern_in_obarray_with_name(
    interp: &mut Interpreter,
    obarray: &Value,
    symbol_name: &str,
    make_name: impl FnOnce(&mut Interpreter) -> Result<Value, LispError>,
) -> Result<Value, LispError> {
    let obarray = &coerce_legacy_vector_obarray(interp, obarray)?;
    let Kind::Record(id) = obarray.kind() else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    if interp.is_standard_obarray_id(id.id) {
        // oblookup's hit is the bucket's symbol; intern_driver makes one
        // for a miss.
        if let Some(symbol) = interp.standard_obarray_symbol(symbol_name) {
            return Ok(Value::Symbol(symbol));
        }
        let name = make_name(interp)?;
        return Ok(Value::Symbol(
            crate::lisp::types::SymbolName::intern_with_lisp_name(
                symbol_name.to_owned(),
                Some(name),
            ),
        ));
    }
    let Some(record) = interp.find_record_mut(id) else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    if record.has_symbol_type(ABBREV_TABLE_RECORD_TYPE) {
        if symbol_name.is_empty() {
            let symbol = abbrev_symbol_name(id.id, "");
            interp.set_global_binding(&symbol, Value::Nil);
            return Ok(Value::Symbol(symbol.into()));
        }
        if abbrev_table_entries(interp, obarray)?
            .iter()
            .any(|(existing, _, _)| existing == symbol_name)
        {
            return Ok(Value::Symbol(abbrev_symbol_name(id.id, symbol_name).into()));
        }
        define_abbrev_entry(interp, obarray, symbol_name, Value::Nil, Value::Nil)?;
        return Ok(Value::Symbol(abbrev_symbol_name(id.id, symbol_name).into()));
    }
    let Some(slots) = private_obarray_slots(record) else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    // oblookup, then intern_driver for a miss.
    let (bytes, chars) = obarray::text_key(symbol_name);
    if let Ok(existing) = obarray::RecordBucketsRef(slots).lookup(bytes, chars) {
        return Ok(Value::Symbol(existing));
    }
    let name = make_name(interp)?;
    let symbol = crate::lisp::types::SymbolName::intern_with_lisp_name(
        crate::lisp::types::make_fresh_obarray_symbol_name(symbol_name, id.id),
        Some(name),
    );
    let record = interp.find_record_mut(id).expect("validated obarray");
    let mut buckets = obarray::RecordBuckets(&mut record.slots);
    // intern_sym files it in the bucket of the name oblookup compared.
    let index = obarray::name_bucket(symbol, buckets.size_bits());
    buckets.insert(symbol, index);
    Ok(Value::Symbol(symbol))
}

pub(crate) fn intern_soft_in_obarray(
    interp: &Interpreter,
    obarray: &Value,
    symbol_name: &str,
) -> Result<Value, LispError> {
    if matches!(obarray.kind(), Kind::Record(id) if interp.is_standard_obarray_id(id.id)) {
        return Ok(interp
            .standard_obarray_symbol(symbol_name)
            .map_or(Value::Nil, Value::Symbol));
    }
    if let Kind::Record(id) = obarray.kind()
        && let Some(slots) = interp.find_record(id).and_then(private_obarray_slots)
    {
        // oblookup.
        let (bytes, chars) = obarray::text_key(symbol_name);
        return Ok(obarray::RecordBucketsRef(slots)
            .lookup(bytes, chars)
            .map_or(Value::Nil, Value::Symbol));
    }
    // An abbrev table, or a legacy vector, by its symbols.
    Ok(obarray_symbols(interp, obarray)?
        .into_iter()
        .find(|value| obarray_symbol_matches(value, symbol_name))
        .unwrap_or(Value::Nil))
}

pub(crate) fn unintern_from_obarray(
    interp: &mut Interpreter,
    obarray: &Value,
    target: &Value,
    env: &Env,
) -> Result<bool, LispError> {
    let obarray = &coerce_legacy_vector_obarray(interp, obarray)?;
    let Kind::Record(id) = obarray.kind() else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    // Funintern: a symbol is looked up by its own name and only it is
    // removed; a string, after its shorthand, names any symbol.
    let (name, only) = match checked_symbol_identity(interp, target, env) {
        Ok(symbol) => (symbol.lisp_name(), Some(symbol)),
        Err(_) => {
            let text = string_text(target)?;
            let longhand = apply_symbol_shorthands_in_env(interp, &text, env)?;
            if longhand == text {
                (*target, None)
            } else {
                (Value::string(&longhand), None)
            }
        }
    };
    if interp.is_standard_obarray_id(id.id) {
        return Ok(interp.unintern_standard_symbol(name, only));
    }
    let Some(record) = interp.find_record_mut(id) else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    };
    if !(record.has_symbol_type(OBARRAY_RECORD_TYPE) && obarray::is_record_slots(&record.slots)) {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), *obarray));
    }
    let mut buckets = obarray::RecordBuckets(&mut record.slots);
    let Some(Ok(symbol)) = buckets.lookup_name(name) else {
        return Ok(false);
    };
    if only.is_some_and(|only| only != symbol) {
        return Ok(false);
    }
    let index = obarray::name_bucket(symbol, buckets.size_bits());
    buckets.remove(symbol, index);
    symbol.cell().mark_uninterned();
    Ok(true)
}

pub(crate) fn values_eq_for_substitution(left: &Value, right: &Value) -> bool {
    match (left.kind(), right.kind()) {
        (Kind::Nil, Kind::Nil) | (Kind::T, Kind::T) => true,
        (Kind::Integer(a), Kind::Integer(b)) => a == b,
        (Kind::BigInteger(a), Kind::BigInteger(b)) => a == b,
        (Kind::Integer(a), Kind::BigInteger(b)) | (Kind::BigInteger(b), Kind::Integer(a)) => {
            BigInt::from(a) == *b
        }
        // Representation equality, like eq/eql (see values_eq_in_env).
        (Kind::Float(a), Kind::Float(b)) => a.to_bits() == b.to_bits(),
        (Kind::Symbol(a), Kind::Symbol(b)) => a == b,
        (Kind::BuiltinFunc(a), Kind::BuiltinFunc(b)) => a == b,
        (Kind::NativeFunction(a), Kind::NativeFunction(b)) => a.ptr_eq(&b),
        (Kind::StringObject(left), Kind::StringObject(right)) => left.ptr_eq(&right),
        (Kind::Cons(left), Kind::Cons(right)) => {
            crate::lisp::types::SharedCons::ptr_eq(&left, &right)
        }
        (Kind::Vector(left), Kind::Vector(right)) => left.ptr_eq(&right),
        (Kind::Closure(left), Kind::Closure(right)) => left.ptr_eq(&right),
        (Kind::Buffer(left), Kind::Buffer(right)) => left.ptr_eq(&right),
        (Kind::Marker(left_id), Kind::Marker(right_id)) => left_id == right_id,
        (Kind::Overlay(left_id), Kind::Overlay(right_id)) => left_id.ptr_eq(&right_id),
        (Kind::CharTable(left_id), Kind::CharTable(right_id)) => left_id == right_id,
        (Kind::Finalizer(left_id), Kind::Finalizer(right_id)) => left_id == right_id,
        (Kind::Record(left), Kind::Record(right)) => left.ptr_eq(&right),
        (Kind::LispRecord(left), Kind::LispRecord(right)) => left.ptr_eq(&right),
        _ => false,
    }
}

pub(crate) fn substitution_visit_key(value: &Value) -> Option<(u8, usize)> {
    match value.kind() {
        Kind::Cons(cell) => Some((0, crate::lisp::types::ConsCell::identity(&cell))),
        Kind::Vector(vector) => Some((4, vector.identity())),
        Kind::StringObject(state) => Some((1, state.identity())),
        Kind::Record(id) => Some((2, id.identity())),
        Kind::LispRecord(record) => Some((4, record.identity())),
        Kind::CharTable(id) => Some((3, id.identity())),
        Kind::SubCharTable(id) => Some((5, id.identity())),
        _ => None,
    }
}

pub(crate) fn substitute_object_recurse(
    interp: &mut Interpreter,
    object: &Value,
    placeholder: &Value,
    subtree: &Value,
    seen: &mut HashSet<(u8, usize)>,
) -> Result<Value, LispError> {
    if values_eq_for_substitution(subtree, placeholder) {
        return Ok(*object);
    }

    // lread.c:substitute_object_recurse cannot traverse Lisp_Subr's C fields.
    // EQ replacement happens first; a different subr signals sequencep.
    if matches!(
        subtree.kind(),
        Kind::BuiltinFunc(_) | Kind::NativeFunction(_)
    ) {
        return Err(wrong_type_argument("sequencep", *subtree));
    }

    let Some(key) = substitution_visit_key(subtree) else {
        return Ok(*subtree);
    };
    if !seen.insert(key) {
        return Ok(*subtree);
    }

    match subtree.kind() {
        Kind::Vector(vector) => {
            let slot_count = vector.len();
            for index in 0..slot_count {
                let current = vector.get(index).expect("in-range vector index");
                let updated =
                    substitute_object_recurse(interp, object, placeholder, &current, seen)?;
                vector.set(index, updated);
            }
            Ok(*subtree)
        }
        Kind::Cons(_) => {
            let Some((car, cdr)) = subtree.cons_values() else {
                return Ok(*subtree);
            };
            let Some((car_slot, cdr_slot)) = subtree.cons_cells() else {
                return Ok(*subtree);
            };
            car_slot.set(substitute_object_recurse(
                interp,
                object,
                placeholder,
                &car,
                seen,
            )?);
            cdr_slot.set(substitute_object_recurse(
                interp,
                object,
                placeholder,
                &cdr,
                seen,
            )?);
            Ok(*subtree)
        }
        Kind::StringObject(state) => {
            // substitute_in_interval: each interval's plist.
            let intervals = state.borrow_mut().props.take();
            if let Some(mut intervals) = intervals {
                let mut failure = None;
                intervals.rewrite_plists(|plist| {
                    match substitute_object_recurse(interp, object, placeholder, &plist, seen) {
                        Ok(value) => value,
                        Err(error) => {
                            failure.get_or_insert(error);
                            plist
                        }
                    }
                });
                state.borrow_mut().props.set(Some(intervals));
                if let Some(error) = failure {
                    return Err(error);
                }
            }
            Ok(*subtree)
        }
        Kind::LispRecord(record) => {
            for index in 0..record.len() {
                let current = record.get(index).expect("in-range record field");
                let updated =
                    substitute_object_recurse(interp, object, placeholder, &current, seen)?;
                record.set(index, updated);
            }
            Ok(*subtree)
        }
        Kind::Record(id) => {
            let slot_count = interp
                .find_record(id)
                .map(|record| record.slots.len())
                .unwrap_or(0);
            for index in 0..slot_count {
                let current = interp
                    .find_record(id)
                    .and_then(|record| record.slots.get(index).cloned())
                    .unwrap_or(Value::Nil);
                let updated =
                    substitute_object_recurse(interp, object, placeholder, &current, seen)?;
                if let Some(record) = interp.find_record_mut(id)
                    && let Some(slot) = record.slots.get_mut(index)
                {
                    *slot = updated;
                }
            }
            Ok(*subtree)
        }
        Kind::CharTable(table) => {
            for (index, value) in table.slots().enumerate() {
                table.set_slot(
                    index,
                    substitute_object_recurse(interp, object, placeholder, &value, seen)?,
                );
            }
            Ok(*subtree)
        }
        Kind::SubCharTable(table) => {
            for (index, value) in table.slots().enumerate() {
                table.set_slot(
                    index,
                    substitute_object_recurse(interp, object, placeholder, &value, seen)?,
                );
            }
            Ok(*subtree)
        }
        _ => Ok(*subtree),
    }
}

pub(crate) fn substitute_object_in_subtree(
    interp: &mut Interpreter,
    object: &Value,
    placeholder: &Value,
    _completed: &Value,
) -> Result<(), LispError> {
    let _ = substitute_object_recurse(interp, object, placeholder, object, &mut HashSet::new())?;
    Ok(())
}

pub(crate) fn completion_display_name(value: &Value) -> Result<String, LispError> {
    match value.kind() {
        Kind::StringObject(_) => string_text(value),
        Kind::Nil => Ok("nil".into()),
        Kind::T => Ok("t".into()),
        Kind::Symbol(symbol) => Ok(crate::lisp::types::visible_symbol_name(&symbol).to_string()),
        _ => Err(LispError::TypeError(
            "string-or-symbol".into(),
            value.type_name(),
        )),
    }
}

pub(crate) fn completion_list_candidates(
    collection: &Value,
) -> Result<Vec<CompletionCandidate>, LispError> {
    let mut candidates = Vec::new();
    let mut current = *collection;
    let mut seen = HashSet::new();

    loop {
        match current.kind() {
            Kind::Nil => return Ok(candidates),
            Kind::Cons(cons_cell) => {
                let cdr = &cons_cell.cdr;
                let id = crate::lisp::types::ConsCell::identity(&cons_cell);
                if !seen.insert(id) {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("circular-list".into()),
                        Value::String("Circular list".into()),
                    ])));
                }
                let item = cons_cell.car.get();
                // minibuf.c: an element that is neither a string nor a
                // symbol "is not a possible completion" — it is skipped,
                // never an error (semantic's texi tables carry characters).
                let key = if matches!(item.kind(), Kind::Cons(_)) {
                    item.car()?
                } else {
                    item
                };
                if let Ok(name) = completion_display_name(&key) {
                    candidates.push(CompletionCandidate {
                        name,
                        predicate_args: vec![item],
                    });
                }
                current = cdr.get();
            }
            // minibuf.c iterates `for (tail = collection; CONSP (tail);
            // tail = XCDR (tail))': any non-cons tail simply ends the
            // walk, dotted lists included.
            _ => return Ok(candidates),
        }
    }
}

pub(crate) fn completion_candidates(
    interp: &Interpreter,
    collection: &Value,
) -> Result<Vec<CompletionCandidate>, LispError> {
    if let Some((_, entries)) = json::hash_table_entries(interp, collection) {
        return entries
            .into_iter()
            .filter_map(|(key, value)| {
                // Same skip rule as the list walk: only string/symbol
                // hash keys are possible completions.
                completion_display_name(&key)
                    .ok()
                    .map(|name| CompletionCandidate {
                        name,
                        predicate_args: vec![key, value],
                    })
            })
            .map(Ok)
            .collect();
    }
    match obarray_symbols(interp, collection).map_err(LispError::into_kind) {
        Ok(symbols) => {
            return symbols
                .into_iter()
                .map(|symbol| {
                    let name = completion_display_name(&symbol)?;
                    Ok(CompletionCandidate {
                        name,
                        predicate_args: vec![symbol],
                    })
                })
                .collect();
        }
        Err(LispErrorKind::TypeError(expected, _)) if expected == "obarray" => {}
        Err(LispErrorKind::WrongTypeArgument(predicate, _)) if predicate == "obarrayp" => {}
        Err(error) => return Err(LispError::from(error)),
    }
    completion_list_candidates(collection)
}

pub(crate) fn completion_ignores_case(interp: &Interpreter, env: &Env) -> bool {
    interp
        .lookup_var("completion-ignore-case", env)
        .is_some_and(|value| value.is_truthy())
}

pub(crate) fn completion_matches_prefix(input: &str, candidate: &str, ignore_case: bool) -> bool {
    let input_chars: Vec<char> = input.chars().collect();
    let candidate_chars: Vec<char> = candidate.chars().collect();
    input_chars.len() <= candidate_chars.len()
        && input_chars
            .iter()
            .zip(candidate_chars.iter())
            .all(|(left, right)| {
                if ignore_case {
                    left.eq_ignore_ascii_case(right)
                } else {
                    left == right
                }
            })
}

pub(crate) fn completion_strings_equal(left: &str, right: &str, ignore_case: bool) -> bool {
    if ignore_case {
        left.eq_ignore_ascii_case(right)
    } else {
        left == right
    }
}

/// minibuf.c's match_regexps for one regexp of `completion-regexp-list':
/// whether PATTERN matches CANDIDATE, folding case with CASE_TABLE's
/// canonicalization when given.
pub(crate) fn completion_regex_matches(
    interp: &mut Interpreter,
    env: &mut Env,
    candidate: &str,
    pattern: &Value,
    case_table: Option<crate::lisp::types::CharTableRef>,
) -> Result<bool, LispError> {
    if !pattern.is_string() {
        return Err(LispError::WrongTypeArgument("stringp".into(), *pattern));
    }
    Ok(
        search::fast_text_match(interp, env, pattern, candidate, &[], true, case_table, true)?
            .is_some(),
    )
}

pub(crate) fn filtered_completion_matches(
    interp: &mut Interpreter,
    input: &str,
    collection: &Value,
    predicate: Option<&Value>,
    env: &mut Env,
) -> Result<Vec<CompletionCandidate>, LispError> {
    let ignore_case = completion_ignores_case(interp, env);
    // minibuf.c match_regexps folds with the buffer's canonical table.
    let case_table = ignore_case.then(|| interp.current_case_tables().canon);
    let regexp_list = interp
        .lookup_var("completion-regexp-list", env)
        .and_then(|value| value.to_vec().ok())
        .unwrap_or_default();
    let predicate = predicate.filter(|value| !value.is_nil()).cloned();
    let mut matches = Vec::new();

    // A FUNCTION completion table answers (TABLE STRING PRED t) with the
    // list of matching completions itself.
    if completion_table_is_function(interp, collection, env) {
        let all = call_function_value(
            interp,
            collection,
            &[
                Value::String(input.to_string().into()),
                predicate.unwrap_or(Value::Nil),
                Value::T,
            ],
            env,
        )?;
        for name in all.to_vec().unwrap_or_default() {
            let value = name;
            let name = completion_display_name(&value)?;
            matches.push(CompletionCandidate {
                name,
                predicate_args: Vec::new(),
            });
        }
        return Ok(matches);
    }

    for candidate in completion_candidates(interp, collection)? {
        if !completion_matches_prefix(input, &candidate.name, ignore_case) {
            continue;
        }
        let mut regex_match = true;
        for pattern in &regexp_list {
            if !completion_regex_matches(interp, env, &candidate.name, pattern, case_table)? {
                regex_match = false;
                break;
            }
        }
        if !regex_match {
            continue;
        }
        if let Some(predicate) = &predicate {
            let predicate = resolve_callable(interp, predicate, env)?;
            if !invoke_function_value(interp, &predicate, &candidate.predicate_args, env)?
                .is_truthy()
            {
                continue;
            }
        }
        matches.push(candidate);
    }

    Ok(matches)
}

/// minibuf.c's table type for COLLECTION: a hash table, an obarray (a
/// vector goes through check_obarray), nil or a cons that is not a
/// function as a list, and anything else as a function to call.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CompletionTable {
    Function,
    List,
    Obarray,
    Hash,
}

fn completion_table_type(interp: &Interpreter, collection: &Value, env: &Env) -> CompletionTable {
    if matches!(collection.kind(), Kind::HashTable(_)) {
        CompletionTable::Hash
    } else if is_vector_value(collection) || is_obarray_like_value(interp, collection) {
        CompletionTable::Obarray
    } else if collection.is_nil()
        || (collection.is_cons() && !function_value_p(interp, collection, env))
    {
        CompletionTable::List
    } else {
        CompletionTable::Function
    }
}

/// call3 (collection, string, predicate, ACTION) for a function table.
fn call_completion_table(
    interp: &mut Interpreter,
    string: Value,
    collection: Value,
    predicate: Option<&Value>,
    action: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    interp.call_function_value(
        collection,
        None,
        &[string, predicate.copied().unwrap_or(Value::Nil), action],
        env,
    )
}

/// minibuf.c's walk over a COLLECTION that is not a function: the
/// elements of a list, the symbols of an obarray, or a hash table's keys
/// with their values.  What the walk holds is rooted across the
/// predicate's calls.
struct CompletionElements {
    kind: CompletionElementsKind,
    /// List: the tail and the tortoise of the cycle check; obarray: its
    /// symbols; hash table: its keys and values, in pairs.
    roots: crate::lisp::alloc::RootedVec<Value>,
    index: usize,
    steps: usize,
    limit: usize,
}

enum CompletionElementsKind {
    List,
    StandardObarray(std::rc::Rc<Vec<crate::lisp::types::SymbolName>>),
    Obarray,
    Hash,
}

/// One possible completion: the element the predicate is given, the name
/// it offers (a symbol's name object), and a hash table's value.
struct CompletionElement {
    elt: Value,
    eltstring: Value,
    hash_value: Option<Value>,
}

/// Fsymbol_name for an element: the symbol's own name object.
fn completion_element_name(value: Value) -> Value {
    match value.kind() {
        Kind::Nil => crate::lisp::types::symbols::builtin_boolean(false).lisp_name(),
        Kind::T => crate::lisp::types::symbols::builtin_boolean(true).lisp_name(),
        Kind::Symbol(symbol) => symbol.lisp_name(),
        _ => value,
    }
}

impl CompletionElements {
    fn new(interp: &Interpreter, collection: &Value) -> Result<Self, LispError> {
        let (kind, roots) = if let Some((_, entries)) = json::hash_table_entries(interp, collection)
        {
            let mut roots = Vec::with_capacity(entries.len() * 2);
            for (key, value) in entries {
                roots.push(key);
                roots.push(value);
            }
            (CompletionElementsKind::Hash, roots)
        } else if matches!(collection.kind(), Kind::Record(id) if interp.is_standard_obarray_id(id.id))
        {
            (
                CompletionElementsKind::StandardObarray(interp.known_symbols_shared()),
                Vec::new(),
            )
        } else if is_vector_value(collection) || is_obarray_like_value(interp, collection) {
            (
                CompletionElementsKind::Obarray,
                obarray_symbols(interp, collection)?,
            )
        } else {
            (CompletionElementsKind::List, vec![*collection, *collection])
        };
        Ok(CompletionElements {
            kind,
            roots: crate::lisp::alloc::RootedVec::from_vec(roots),
            index: 0,
            steps: 0,
            limit: 2,
        })
    }

    fn next(&mut self) -> Result<Option<CompletionElement>, LispError> {
        match &self.kind {
            CompletionElementsKind::List => {
                let Some((elt, tail)) = self.roots[0].cons_values() else {
                    return Ok(None);
                };
                self.roots[0] = tail;
                // minibuf.c walks the list with no cycle check; a circular
                // one is signaled here instead of looping forever.
                if tail.eq_value(self.roots[1]) && tail.is_cons() {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("circular-list".into()),
                        Value::String("Circular list".into()),
                    ])));
                }
                self.steps += 1;
                if self.steps == self.limit {
                    self.roots[1] = tail;
                    self.steps = 0;
                    self.limit *= 2;
                }
                let eltstring = match elt.cons_values() {
                    Some((key, _)) => key,
                    None => elt,
                };
                Ok(Some(CompletionElement {
                    elt,
                    eltstring: completion_element_name(eltstring),
                    hash_value: None,
                }))
            }
            CompletionElementsKind::StandardObarray(symbols) => {
                let Some(symbol) = symbols.get(self.index) else {
                    return Ok(None);
                };
                self.index += 1;
                let elt = match symbol.as_str() {
                    "nil" => Value::Nil,
                    "t" => Value::T,
                    _ => Value::Symbol(*symbol),
                };
                Ok(Some(CompletionElement {
                    elt,
                    eltstring: completion_element_name(elt),
                    hash_value: None,
                }))
            }
            CompletionElementsKind::Obarray => {
                let Some(&elt) = self.roots.get(self.index) else {
                    return Ok(None);
                };
                self.index += 1;
                Ok(Some(CompletionElement {
                    elt,
                    eltstring: completion_element_name(elt),
                    hash_value: None,
                }))
            }
            CompletionElementsKind::Hash => {
                let Some(&elt) = self.roots.get(self.index) else {
                    return Ok(None);
                };
                let value = self.roots[self.index + 1];
                self.index += 2;
                Ok(Some(CompletionElement {
                    elt,
                    eltstring: completion_element_name(elt),
                    hash_value: Some(value),
                }))
            }
        }
    }
}

/// SCHARS of a string value.
fn string_chars(value: &Value) -> usize {
    match value.kind() {
        Kind::StringObject(string) => string.borrow().len(),
        _ => 0,
    }
}

/// Fcompare_strings (A, 0, A_END, B, 0, B_END, IGNORE_CASE) with both
/// ends within their strings.
fn compare_prefixes(
    interp: &mut Interpreter,
    a: &Value,
    a_end: usize,
    b: &Value,
    b_end: usize,
    ignore_case: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    compare_string_ranges(interp, a, (0, a_end), b, (0, b_end), ignore_case, env)
}

/// minibuf.c match_regexps: whether STRING matches every regexp of
/// REGEXPS, folding case with the buffer's canonical table when asked.
fn match_regexps(
    interp: &mut Interpreter,
    env: &mut Env,
    string: &Value,
    regexps: Value,
    ignore_case: bool,
) -> Result<bool, LispError> {
    let mut tail = regexps;
    while let Some((regexp, next)) = tail.cons_values() {
        if !regexp.is_string() {
            return Err(LispError::WrongTypeArgument("stringp".into(), regexp));
        }
        let case_table = ignore_case.then(|| interp.current_case_tables().canon);
        if search::fast_string_match(interp, env, &regexp, string, case_table, true)?.is_none() {
            return Ok(false);
        }
        tail = next;
    }
    Ok(true)
}

/// The predicate test of Ftry_completion and Fall_completions: a hash
/// table's predicate also receives the value.
fn completion_predicate_accepts(
    interp: &mut Interpreter,
    predicate: &Value,
    element: &CompletionElement,
    env: &mut Env,
) -> Result<bool, LispError> {
    let predicate = resolve_callable(interp, predicate, env)?;
    let accepted = match element.hash_value {
        Some(value) => invoke_function_value(interp, &predicate, &[element.elt, value], env)?,
        None => invoke_function_value(interp, &predicate, &[element.elt], env)?,
    };
    Ok(accepted.is_truthy())
}

pub(crate) fn try_completion(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.len() < 2 || args.len() > 4 {
        return Err(LispError::WrongNumberOfArgs(
            "try-completion".into(),
            args.len(),
        ));
    }
    // minibuf.c:Ftry_completion: check_obarray on a vector, then CHECK_STRING.
    let string = args[0];
    let elements = match completion_table_type(interp, &args[1], env) {
        CompletionTable::Function => None,
        _ => Some(CompletionElements::new(interp, &args[1])?),
    };
    if !string.is_string() {
        return Err(LispError::WrongTypeArgument("stringp".into(), string));
    }
    let Some(mut elements) = elements else {
        return call_completion_table(interp, string, args[1], args.get(2), Value::Nil, env);
    };
    let ignore_case = completion_ignores_case(interp, env);
    let regexps = interp
        .lookup_var("completion-regexp-list", env)
        .unwrap_or(Value::Nil);
    let predicate = args.get(2).copied().filter(|value| !value.is_nil());
    let size = string_chars(&string);
    let mut best = crate::lisp::alloc::RootedVec::from_vec(vec![Value::Nil]);
    let mut bestmatchsize = 0usize;
    let mut matchcount = 0;
    while let Some(element) = elements.next()? {
        let eltstring = element.eltstring;
        if !eltstring.is_string()
            || size > string_chars(&eltstring)
            || !compare_prefixes(interp, &eltstring, size, &string, size, ignore_case, env)?
                .eq_value(Value::T)
        {
            continue;
        }
        // Ignore this element if it fails to match all the regexps.
        if !match_regexps(interp, env, &eltstring, regexps, ignore_case)? {
            continue;
        }
        // Ignore this element if there is a predicate and the predicate
        // doesn't like it.
        if let Some(predicate) = &predicate
            && !completion_predicate_accepts(interp, predicate, &element, env)?
        {
            continue;
        }
        // Update computation of how much all possible completions match.
        let eltsize = string_chars(&eltstring);
        if best[0].is_nil() {
            matchcount = 1;
            best[0] = eltstring;
            bestmatchsize = eltsize;
            continue;
        }
        let compare = bestmatchsize.min(eltsize);
        let bestmatch = best[0];
        let tem = compare_prefixes(
            interp,
            &bestmatch,
            compare,
            &eltstring,
            compare,
            ignore_case,
            env,
        )?;
        let matchsize = match tem.kind() {
            Kind::Integer(position) => position.unsigned_abs() as usize - 1,
            _ => compare,
        };
        if ignore_case {
            // If this is an exact match except for case, use it as the
            // best match rather than one that is not an exact match, to
            // get the case pattern of the actual match.  If there is more
            // than one exact match ignoring case and one of them is exact
            // including case, prefer that one; with no exact match
            // ignoring case, prefer a match that does not change the case
            // of the input.
            let best_size = string_chars(&bestmatch);
            if (matchsize == eltsize && matchsize < best_size)
                || ((matchsize == eltsize) == (matchsize == best_size)
                    && compare_prefixes(interp, &eltstring, size, &string, size, false, env)?
                        .eq_value(Value::T)
                    && !compare_prefixes(interp, &bestmatch, size, &string, size, false, env)?
                        .eq_value(Value::T))
            {
                best[0] = eltstring;
            }
        }
        if bestmatchsize != eltsize
            || bestmatchsize != matchsize
            || (ignore_case
                && !compare_prefixes(interp, &bestmatch, compare, &eltstring, compare, false, env)?
                    .eq_value(Value::T))
        {
            // Don't count the same string multiple times.
            if matchcount <= 1 {
                matchcount += 1;
            }
        }
        bestmatchsize = matchsize;
        if matchsize <= size && !ignore_case && matchcount > 1 {
            // No need to look any further.
            break;
        }
    }
    let bestmatch = best[0];
    if bestmatch.is_nil() {
        return Ok(Value::Nil);
    }
    // If we are ignoring case, and there is no exact match, and no
    // additional text was supplied, don't change the case of what the user
    // typed.
    if ignore_case && bestmatchsize == size && string_chars(&bestmatch) > bestmatchsize {
        return Ok(string);
    }
    // Return t if the supplied string is an exact match (counting case).
    if matchcount == 1 && values_equal(interp, &bestmatch, &string)? {
        return Ok(Value::T);
    }
    substring_value(
        bestmatch,
        Value::Integer(0),
        Value::Integer(bestmatchsize as i64),
        true,
    )
}

pub(crate) fn all_completions(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.len() < 2 || args.len() > 4 {
        return Err(LispError::WrongNumberOfArgs(
            "all-completions".into(),
            args.len(),
        ));
    }
    // minibuf.c:Fall_completions: check_obarray on a vector, then CHECK_STRING.
    let string = args[0];
    let elements = match completion_table_type(interp, &args[1], env) {
        CompletionTable::Function => None,
        _ => Some(CompletionElements::new(interp, &args[1])?),
    };
    if !string.is_string() {
        return Err(LispError::WrongTypeArgument("stringp".into(), string));
    }
    let Some(mut elements) = elements else {
        return call_completion_table(interp, string, args[1], args.get(2), Value::T, env);
    };
    let ignore_case = completion_ignores_case(interp, env);
    let regexps = interp
        .lookup_var("completion-regexp-list", env)
        .unwrap_or(Value::Nil);
    let predicate = args.get(2).copied().filter(|value| !value.is_nil());
    let hide_spaces = args.get(3).is_some_and(Value::is_truthy);
    let string_starts_with_space = string_first_byte(&string) == Some(b' ');
    let size = string_chars(&string);
    let mut allmatches = crate::lisp::alloc::RootedVec::new();
    while let Some(element) = elements.next()? {
        let eltstring = element.eltstring;
        if !eltstring.is_string()
            || size > string_chars(&eltstring)
            // If HIDE_SPACES, reject alternatives that start with space
            // unless the input starts with space.
            || (hide_spaces
                && !string_starts_with_space
                && string_first_byte(&eltstring) == Some(b' '))
            || !compare_prefixes(interp, &eltstring, size, &string, size, ignore_case, env)?
                .eq_value(Value::T)
        {
            continue;
        }
        if !match_regexps(interp, env, &eltstring, regexps, ignore_case)? {
            continue;
        }
        if let Some(predicate) = &predicate
            && !completion_predicate_accepts(interp, predicate, &element, env)?
        {
            continue;
        }
        allmatches.push(eltstring);
    }
    Ok(Value::list(allmatches.iter().copied()))
}

/// SREF (STRING, 0): the first byte of a nonempty string.
fn string_first_byte(value: &Value) -> Option<u8> {
    match value.kind() {
        Kind::StringObject(string) => string.borrow().bytes().first().copied(),
        _ => None,
    }
}

pub(crate) fn test_completion(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.len() < 2 || args.len() > 4 {
        return Err(LispError::WrongNumberOfArgs(
            "test-completion".into(),
            args.len(),
        ));
    }
    let string = args[0];
    if !string.is_string() {
        return Err(LispError::WrongTypeArgument("stringp".into(), string));
    }
    // minibuf.c:Ftest_completion.
    let collection = args[1];
    let table = completion_table_type(interp, &collection, env);
    if table == CompletionTable::Function {
        let action = Value::Symbol("lambda".into());
        return call_completion_table(interp, string, collection, args.get(2), action, env);
    }
    let ignore_case = completion_ignores_case(interp, env);
    let mut roots = crate::lisp::alloc::RootedVec::from_vec(vec![Value::Nil, Value::Nil]);
    let hash_table = table == CompletionTable::Hash;
    if let Kind::HashTable(table) = collection.kind() {
        let (_, index) = hash_insert::hash_table_lookup(interp, table, &string, env)?;
        if let Some((key, value)) = index.and_then(|index| table.entry(index)) {
            roots[0] = key;
            roots[1] = value;
        } else {
            let mut found = false;
            for (key, value) in table
                .entries()
                .map(|(_, key, value)| (key, value))
                .collect::<Vec<_>>()
            {
                let strkey = completion_element_name(key);
                if !strkey.is_string() {
                    continue;
                }
                let (string_size, key_size) = (string_chars(&string), string_chars(&strkey));
                if compare_string_ranges(
                    interp,
                    &string,
                    (0, string_size),
                    &strkey,
                    (0, key_size),
                    ignore_case,
                    env,
                )?
                .eq_value(Value::T)
                {
                    roots[0] = key;
                    roots[1] = value;
                    found = true;
                    break;
                }
            }
            if !found {
                return Ok(Value::Nil);
            }
        }
    } else if table == CompletionTable::Obarray {
        // Bypass intern-soft as that loses for nil.
        let name = string_text(&string)?;
        let symbols = obarray_symbols(interp, &collection)?;
        let mut tem = symbols
            .iter()
            .copied()
            .find(|symbol| obarray_symbol_matches(symbol, &name));
        if ignore_case && tem.is_none() {
            let string_size = string_chars(&string);
            for symbol in symbols {
                let symbol_name = completion_element_name(symbol);
                let name_size = string_chars(&symbol_name);
                if compare_string_ranges(
                    interp,
                    &string,
                    (0, string_size),
                    &symbol_name,
                    (0, name_size),
                    true,
                    env,
                )?
                .eq_value(Value::T)
                {
                    tem = Some(symbol);
                    break;
                }
            }
        }
        let Some(tem) = tem else {
            return Ok(Value::Nil);
        };
        roots[0] = tem;
    } else {
        let tem = super::call(
            interp,
            "assoc-string",
            &[
                string,
                collection,
                if ignore_case { Value::T } else { Value::Nil },
            ],
            env,
        )?;
        if tem.is_nil() {
            return Ok(Value::Nil);
        }
        roots[0] = tem;
    }
    // Reject this element if it fails to match all the regexps.
    let regexps = interp
        .lookup_var("completion-regexp-list", env)
        .unwrap_or(Value::Nil);
    if !match_regexps(interp, env, &string, regexps, ignore_case)? {
        return Ok(Value::Nil);
    }
    // Finally, check the predicate.
    match args.get(2).filter(|value| !value.is_nil()) {
        Some(predicate) => {
            let predicate = resolve_callable(interp, predicate, env)?;
            if hash_table {
                invoke_function_value(interp, &predicate, &[roots[0], roots[1]], env)
            } else {
                invoke_function_value(interp, &predicate, &[roots[0]], env)
            }
        }
        None => Ok(Value::T),
    }
}

pub(crate) fn internal_complete_buffer(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.len() != 3 {
        return Err(LispError::WrongNumberOfArgs(
            "internal-complete-buffer".into(),
            args.len(),
        ));
    }
    let input = string_text(&args[0])?;
    let buffer_alist = Value::list(
        interp
            .buffer_list
            .iter()
            .map(|(id, name)| {
                Value::cons(
                    string_like_value(name.clone(), None),
                    interp.buffer_value(*id).expect("live buffer object"),
                )
            })
            .collect::<Vec<_>>(),
    );
    match args[2].kind() {
        Kind::Nil => try_completion(interp, &[args[0], buffer_alist, args[1]], env),
        Kind::T => {
            let completions = all_completions(interp, &[args[0], buffer_alist, args[1]], env)?;
            if !input.is_empty() {
                return Ok(completions);
            }
            let all = completions.to_vec()?;
            let visible = all
                .iter()
                .filter(|value| {
                    string_like(value).is_some_and(|string| !string.text.starts_with(' '))
                })
                .cloned()
                .collect::<Vec<_>>();
            if visible.is_empty() && all.len() == interp.buffer_list.len() {
                Ok(completions)
            } else {
                Ok(Value::list(visible))
            }
        }
        Kind::Symbol(flag) if flag == "lambda" => {
            test_completion(interp, &[args[0], buffer_alist, args[1]], env)
        }
        Kind::Symbol(flag) if flag == "metadata" => Ok(Value::list([
            Value::Symbol("metadata".into()),
            Value::cons(
                Value::Symbol("category".into()),
                Value::Symbol("buffer".into()),
            ),
            Value::cons(
                Value::Symbol("cycle-sort-function".into()),
                Value::Symbol("identity".into()),
            ),
        ])),
        _ => Ok(Value::Nil),
    }
}

pub(crate) fn completing_read(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.len() < 2 || args.len() > 8 {
        return Err(LispError::WrongNumberOfArgs(
            "completing-read".into(),
            args.len(),
        ));
    }
    ensure_interaction_allowed(interp, env)?;

    // GNU's Fcompleting_read delegates the entire policy to this variable;
    // minibuffer.el normally installs `completing-read-default'.  Honor that
    // loaded Elisp owner (including dynamic replacement and its mockable call
    // to `read-from-minibuffer') before using the file-less native fallback.
    if let Some(function) = interp
        .lookup_var("completing-read-function", env)
        .filter(|function| !function.is_nil())
    {
        return call_function_value(interp, &function, args, env);
    }

    check_recursive_minibuffer(interp, env)?;
    if executing_kbd_macro_p(interp, env) {
        crate::lisp::primitives::dispatch::prepare_kbd_macro_minibuffer_entry(interp, env)?;
    }
    let minibuffer = activate_completing_read_minibuffer(interp, args, env)?;
    run_active_minibuffer(interp, env, minibuffer, |interp, env| {
        completing_read_contents(interp, args, env)
    })
}

pub(crate) fn run_active_minibuffer<T>(
    interp: &mut Interpreter,
    env: &mut Env,
    minibuffer: ActiveMinibuffer,
    body: impl FnOnce(&mut Interpreter, &mut Env) -> Result<T, LispError>,
) -> Result<T, LispError> {
    // read_minibuf registers its window-configuration restore BEFORE
    // selecting the minibuffer window or running the setup hook: a
    // pop-up the hook itself makes (tmm-add-prompt's completion window)
    // vanishes when the read finishes, on every exit path, quits
    // included, and the pre-read window selection returns.
    let saved_windows = minibuffer.saved_windows.clone();
    // The recursive command loop leaves `this-command' and its companions
    // as its last command set them; call-interactively, not read_minibuf,
    // restores the outer command's once its arguments are read.
    let result = (|| {
        // GNU minibuf.c:read_minibuf runs `minibuffer-setup-hook' with the
        // minibuffer already current (the next statement there is
        // `bset_undo_list (current_buffer, Qnil)').  Hooks therefore see the
        // minibuffer's buffer-local state, including the completion table.
        if interp.has_buffer_id(minibuffer.buffer_id) {
            interp.set_current_buffer_id(minibuffer.buffer_id)?;
        }
        run_named_hooks(
            interp,
            "minibuffer-setup-hook",
            env,
            Some(minibuffer.buffer_id),
        )?;

        // A setup hook may temporarily select another buffer (most notably
        // *Completions*).  The command loop itself starts in the minibuffer.
        if interp.has_buffer_id(minibuffer.buffer_id) {
            interp.set_current_buffer_id(minibuffer.buffer_id)?;
            // "Don't allow the user to undo past this point."
            interp.buffer.borrow_mut().set_undo_list(Value::Nil);
        }

        // read_minibuf's recursive_edit_1.
        with_recursive_edit_bindings(interp, env, body)
    })();
    // The unwinding of every exit: `minibuffer-exit-hook' first, then
    // read_minibuf_unwind.
    let exit = run_exit_minibuf_hook(interp, minibuffer.buffer_id, env);
    restore_active_minibuffer(interp, minibuffer, env);
    if interp
        .lookup_var("read-minibuffer-restore-windows", env)
        .is_none_or(|restore| restore.is_truthy())
    {
        let _ = interp.restore_window_configuration(saved_windows);
    }
    let value = result?;
    exit.map(|()| value)
}

pub(crate) struct ActiveMinibuffer {
    pub(crate) buffer_id: u64,
    /// The window tree as it stood BEFORE the minibuffer window was
    /// selected — read_minibuf saves its configuration first, so the
    /// restore never re-selects the minibuffer.
    saved_windows: crate::lisp::eval::WindowConfigurationSnapshot,
    saved_buffer_id: u64,
    saved_selected_window_id: u64,
    saved_selected_window_buffer_id: u64,
    previous_minibuffer_selected_window_id: Option<u64>,
    previous_runtime: crate::lisp::eval::MinibufferRuntimeState,
}

fn activate_completing_read_minibuffer(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<ActiveMinibuffer, LispError> {
    let prompt = args
        .first()
        .ok_or_else(|| LispError::TypeError("string".into(), "nil".into()))?;
    if string_like(prompt).is_none() {
        return Err(LispError::TypeError("string".into(), prompt.type_name()));
    }
    // completing-read-default: a cons INITIAL-INPUT's zero-indexed
    // position becomes read-from-minibuffer's one-indexed one.
    let initial_input = match args.get(4).copied().unwrap_or(Value::Nil) {
        initial if initial.is_cons() => Value::cons(
            initial.car()?,
            super::call(interp, "1+", &[initial.cdr()?], env)?,
        ),
        initial => initial,
    };
    let require_match = args.get(3).is_some_and(Value::is_truthy);
    let map_name = if require_match {
        "minibuffer-local-must-match-map"
    } else {
        "minibuffer-local-completion-map"
    };
    let local_map = interp.lookup_var(map_name, env).unwrap_or(Value::Nil);
    let active = activate_minibuffer(interp, prompt, &initial_input, local_map, env)?;
    let buffer_id = active.buffer_id;

    interp.set_buffer_local_value(
        buffer_id,
        "minibuffer-completion-table",
        args.get(1).cloned().unwrap_or(Value::Nil),
    );
    interp.set_buffer_local_value(
        buffer_id,
        "minibuffer-completion-predicate",
        args.get(2).cloned().unwrap_or(Value::Nil),
    );
    interp.set_buffer_local_value(
        buffer_id,
        "minibuffer--require-match",
        args.get(3).cloned().unwrap_or(Value::Nil),
    );
    interp.set_buffer_local_value(
        buffer_id,
        "minibuffer-default",
        args.get(6).cloned().unwrap_or(Value::Nil),
    );
    Ok(active)
}

/// minibuf.c:read_minibuf's INITIAL: a string, or (STRING . POSITION)
/// with point at one-indexed POSITION in STRING (at its start for any
/// POSITION below 1).  Returns the string and the `forward-char' from
/// its end that places point; past the end is `end-of-buffer'.
pub(crate) fn minibuffer_initial_input(initial: &Value) -> Result<(Value, i64), LispError> {
    let (string, backup) = match initial.kind() {
        Kind::Cons(_) => (initial.car()?, initial.cdr()?),
        _ if initial.is_nil() => return Ok((Value::Nil, 0)),
        _ => (*initial, Value::Nil),
    };
    let Some(text) = string_like(&string) else {
        return Err(wrong_type_argument("stringp", string));
    };
    if backup.is_nil() {
        return Ok((string, 0));
    }
    let Kind::Integer(position) = backup.kind() else {
        return Err(wrong_type_argument("fixnump", backup));
    };
    let length = text.text.chars().count() as i64;
    let pos = if position < 1 {
        -length
    } else {
        position.saturating_sub(1).saturating_sub(length)
    };
    if pos > 0 {
        return Err(LispError::SignalValue(Value::list([Value::symbol(
            "end-of-buffer",
        )])));
    }
    Ok((string, pos))
}

pub(crate) fn activate_minibuffer(
    interp: &mut Interpreter,
    prompt: &Value,
    initial_input: &Value,
    local_map: Value,
    env: &mut Env,
) -> Result<ActiveMinibuffer, LispError> {
    let prompt_string =
        string_like(prompt).ok_or_else(|| wrong_type_argument("stringp", *prompt))?;
    let prompt_length = prompt_string.text.chars().count();
    let (initial_input, initial_backup) = minibuffer_initial_input(initial_input)?;
    let initial_input = &initial_input;
    let initial_string = string_like(initial_input).unwrap_or_else(|| StringLike {
        text: String::new(),
        props: None,
        multibyte: false,
        extended_chars: Vec::new(),
    });
    let saved_windows = interp.snapshot_window_configuration();
    let depth = interp.minibuffer_depth().saturating_add(1);
    let (buffer_id, reused) = get_minibuffer(interp, depth);
    let saved_buffer_id = interp.current_buffer_id();
    let saved_selected_window_id = interp.selected_window_id();
    let saved_selected_window_buffer_id = interp.selected_window_buffer_id();
    let previous_minibuffer_selected_window_id =
        interp.replace_minibuffer_selected_window_id(Some(saved_selected_window_id));
    let default_directory = interp.lookup_var("default-directory", env);
    if reused {
        // GNU get_minibuffer deletes both overlay trees before reset_buffer.
        // Reusing a minibuffer without this step leaves completion UI from
        // the preceding read (for example M-x's Vertico count) attached to
        // the next Consult prompt.
        interp
            .get_buffer_by_id_mut(buffer_id)
            .expect("reused minibuffer remains live")
            .overlays
            .clear();
    }
    // read_minibuf's set_minibuffer_mode: `minibuffer-mode', whose
    // kill-all-local-variables drops what the previous read left local.
    set_minibuffer_mode(interp, buffer_id, depth, env)?;
    if let Some(default_directory) = default_directory {
        interp.set_buffer_local_value(buffer_id, "default-directory", default_directory);
    }
    interp.set_buffer_local_value(buffer_id, "buffer-read-only", Value::Nil);
    // "Defeat (setq-default truncate-lines t), since truncated lines do
    // not work correctly in minibuffers."
    interp.set_buffer_local_value(buffer_id, "truncate-lines", Value::Nil);
    // GNU reads in the minibuffer window: the minibuffer buffer becomes
    // current without displaying itself in the selected text window,
    // whose glass keeps showing its own buffer during the read.  The
    // window's point slot is saved by hand — the raw selection swap
    // below skips select-window's bookkeeping.
    let entry_point = interp.buffer.borrow().point();
    if let Some(window) = interp.find_record_mut(saved_selected_window_id) {
        if window.slots.len() <= super::WINDOW_POINT_SLOT {
            window
                .slots
                .resize(super::WINDOW_POINT_SLOT + 1, Value::Nil);
        }
        window.slots[super::WINDOW_POINT_SLOT] = Value::Integer(entry_point as i64);
    }
    interp.set_current_buffer_id(buffer_id)?;
    // "Empty out the minibuffers of all frames, except those frames where
    // there is an active minibuffer": the level-0 one goes inactive.
    if let Some((empty, _)) = interp.find_buffer(" *Minibuf-0*") {
        set_minibuffer_mode(interp, empty, 0, env)?;
    }
    // read_minibuf erases the buffer and inserts the prompt with
    // `inhibit-read-only' and `inhibit-modification-hooks' bound: the
    // ordinary editing primitives, recording undo, then the field and
    // stickiness controls that keep typed text from inheriting the prompt's
    // properties (front-sticky, rear-nonsticky), then
    // `minibuffer-prompt-properties' (the read-only guard; its face is
    // added after the prompt's own faces).
    with_minibuffer_edits_inhibited(interp, env, |interp, env| {
        super::call(interp, "erase-buffer", &[], env)?;
        super::call(interp, "insert", &[*prompt], env)?;
        let prompt_end = Value::Integer(interp.buffer.borrow().point() as i64);
        if prompt_length == 0 {
            return Ok(());
        }
        let stamp = |interp: &mut Interpreter, env: &mut Env, name: Value, value: Value| {
            super::call(
                interp,
                "put-text-property",
                &[Value::Integer(1), prompt_end, name, value],
                env,
            )
            .map(drop)
        };
        for name in ["front-sticky", "rear-nonsticky", "field"] {
            stamp(interp, env, Value::symbol(name), Value::T)?;
        }
        let properties = interp
            .lookup_var("minibuffer-prompt-properties", env)
            .unwrap_or(Value::Nil);
        let mut list = properties;
        while let Some((name, rest)) = list.cons_values() {
            let Some((value, rest)) = rest.cons_values() else {
                break;
            };
            if name.eq_value(Value::symbol("face")) {
                super::call(
                    interp,
                    "add-face-text-property",
                    &[Value::Integer(1), prompt_end, value, Value::T],
                    env,
                )?;
            } else {
                stamp(interp, env, name, value)?;
            }
            list = rest;
        }
        Ok(())
    })?;
    // The initial input goes in after the bindings are gone, an ordinary
    // insertion.
    if !initial_string.text.is_empty() {
        if matches!(initial_input.kind(), Kind::StringObject(_)) {
            super::call(interp, "insert", &[*initial_input], env)?;
        } else {
            let initial_start = interp.buffer.borrow().point();
            let props = match &initial_string.props {
                Some(intervals) => crate::buffer::InsertedProperties::Intervals {
                    source: intervals,
                    inherit: false,
                },
                None => crate::buffer::InsertedProperties::PLAIN,
            };
            let vars = interp.insertion_property_variables();
            interp
                .buffer
                .borrow_mut()
                .insert_with(&initial_string.text, props, false, &vars);
            interp
                .buffer
                .borrow_mut()
                .set_inserted_extended_chars(initial_start, &initial_string.extended_chars);
        }
    }
    {
        let mut buffer = interp.buffer.borrow_mut();
        let position = buffer.point_max();
        buffer.goto_char(position.saturating_sub(initial_backup.unsigned_abs() as usize));
    }

    // Select the minibuffer window for the read, GNU's read_minibuf: the
    // minibuffer buffer shows there, never in the entry window.
    if let Kind::Record(minibuffer_window_id) = interp.minibuffer_window_value().kind() {
        interp.set_selected_window_id(minibuffer_window_id.id);
        interp.set_selected_window_buffer_id(buffer_id);
    }

    let previous_runtime =
        interp.begin_minibuffer_runtime(buffer_id, interp.selected_window_id(), prompt_string.text);

    let active = ActiveMinibuffer {
        buffer_id,
        saved_windows,
        saved_buffer_id,
        saved_selected_window_id,
        saved_selected_window_buffer_id,
        previous_minibuffer_selected_window_id,
        previous_runtime,
    };
    // GNU minibuf.c:read_minibuf has already selected/activated the
    // minibuffer when clear_message runs, but has not installed its map.
    if let Err(error) = super::dispatch::clear_message(interp, env, true, true) {
        // read_minibuf's unwind records also cover a nonlocal exit from
        // clear-message-function. Run the exit hook before restoring state,
        // and restore even if that hook itself exits nonlocally.
        let saved_windows = active.saved_windows.clone();
        let exit_result = run_exit_minibuf_hook(interp, buffer_id, env);
        restore_active_minibuffer(interp, active, env);
        if interp
            .lookup_var("read-minibuffer-restore-windows", env)
            .is_none_or(|restore| restore.is_truthy())
        {
            interp.restore_window_configuration(saved_windows)?;
        }
        exit_result?;
        return Err(error);
    }
    if let Some(mut buffer) = interp.get_buffer_by_id_mut(buffer_id) {
        buffer.set_keymap(local_map);
    }
    Ok(active)
}

/// minibuf.c:read_minibuf's guard: without `enable-recursive-minibuffers'
/// a read while a minibuffer is active is a user error when the minibuffer
/// window is selected, and from any other window cancels the active read
/// (its recursive edit signals the thrown string as an error).
pub(crate) fn check_recursive_minibuffer(interp: &Interpreter, env: &Env) -> Result<(), LispError> {
    if interp.minibuffer_depth() == 0
        || interp
            .lookup_var("enable-recursive-minibuffers", env)
            .is_some_and(|value| value.is_truthy())
    {
        return Ok(());
    }
    let message = Value::string("Command attempted to use minibuffer while in minibuffer");
    let in_minibuffer_window = matches!(
        interp.minibuffer_window_value().kind(),
        Kind::Record(window) if window.id == interp.selected_window_id()
    );
    if in_minibuffer_window {
        Err(LispError::SignalValue(Value::list([
            Value::symbol("user-error"),
            message,
        ])))
    } else {
        Err(LispError::Throw(Value::symbol("exit"), message))
    }
}

/// minibuf.c:this_minibuffer_depth: BUFFER's depth among the active
/// minibuffers, or 0 when it is none of them.
pub(crate) fn this_minibuffer_depth(interp: &Interpreter, buffer: &Value) -> usize {
    let Kind::Buffer(buffer) = buffer.kind() else {
        return 0;
    };
    if !interp
        .buffer_object(buffer.id)
        .is_some_and(|live| live.ptr_eq(&buffer))
    {
        return 0;
    }
    (1..=interp.minibuffer_depth())
        .find(|depth| interp.find_buffer_id(&format!(" *Minibuf-{depth}*")) == Some(buffer.id))
        .unwrap_or(0)
}

/// minibuf.c:Fminibuffer_innermost_command_loop_p: BUFFER is an active
/// minibuffer read at the current command loop level.
pub(crate) fn minibuffer_innermost_command_loop_p(interp: &Interpreter, buffer: &Value) -> bool {
    let depth = this_minibuffer_depth(interp, buffer);
    depth != 0
        && interp.minibuffer_command_loop_level(depth) == Some(interp.command_loop_recursion_depth)
}

/// minibuf.c:get_minibuffer: the DEPTHth minibuffer and whether it was
/// reused; a new one has undo enabled ("Although the buffer's name starts
/// with a space, undo should be enabled in it").
pub(crate) fn get_minibuffer(interp: &mut Interpreter, depth: usize) -> (u64, bool) {
    let name = format!(" *Minibuf-{depth}*");
    if let Some((id, _)) = interp.find_buffer(&name) {
        return (id, true);
    }
    let id = interp.create_buffer(&name).0;
    if let Some(mut buffer) = interp.get_buffer_by_id_mut(id) {
        buffer.enable_undo();
    }
    (id, false)
}

/// minibuf.c:set_minibuffer_mode: in BUFFER, `minibuffer-mode' for an
/// active DEPTH, `minibuffer-inactive-mode' for level 0 (where
/// minibuffer.el is not loaded, kill-all-local-variables), the current
/// buffer kept.  A runtime without minibuffer.el, which GNU never is at an
/// active level, drops the previous read's locals instead.
fn set_minibuffer_mode(
    interp: &mut Interpreter,
    buffer_id: u64,
    depth: usize,
    env: &mut Env,
) -> Result<(), LispError> {
    let mode = if depth > 0 {
        "minibuffer-mode"
    } else {
        "minibuffer-inactive-mode"
    };
    let defined = interp.lookup_function(mode, env).is_ok();
    if !defined && depth > 0 {
        interp.clear_buffer_local_state(buffer_id);
        return Ok(());
    }
    let saved = interp.current_buffer_id();
    interp.set_current_buffer_id(buffer_id)?;
    let result = if defined {
        interp
            .call_function_value(Value::symbol(mode), Some(mode), &[], env)
            .map(drop)
    } else {
        super::call(interp, "kill-all-local-variables", &[], env).map(drop)
    };
    if interp.has_buffer_id(saved) {
        interp.set_current_buffer_id(saved)?;
    }
    result
}

/// emacs.c's set_initial_minibuffer_mode, before the top level: the
/// level-0 minibuffer in its inactive mode.
pub(crate) fn set_initial_minibuffer_mode(interp: &mut Interpreter) -> Result<(), LispError> {
    let (buffer_id, _) = get_minibuffer(interp, 0);
    set_minibuffer_mode(interp, buffer_id, 0, &mut Env::new())
}

/// BODY with `inhibit-read-only' and `inhibit-modification-hooks' bound to
/// t, as read_minibuf and read_minibuf_unwind edit the minibuffer.
fn with_minibuffer_edits_inhibited<T>(
    interp: &mut Interpreter,
    env: &mut Env,
    body: impl FnOnce(&mut Interpreter, &mut Env) -> Result<T, LispError>,
) -> Result<T, LispError> {
    let read_only = interp.bind_special_dynamic("inhibit-read-only", Value::T, env)?;
    let hooks = match interp.bind_special_dynamic("inhibit-modification-hooks", Value::T, env) {
        Ok(restore) => restore,
        Err(error) => {
            interp.restore_special_dynamic(read_only, env)?;
            return Err(error);
        }
    };
    let result = body(interp, env);
    let restored = interp
        .restore_special_dynamic(hooks, env)
        .and(interp.restore_special_dynamic(read_only, env));
    let value = result?;
    restored.map(|()| value)
}

/// keyboard.c:recursive_edit_1's bindings around BODY's command loop:
/// `inhibit-redisplay' nil, and `undo-auto--undoably-changed-buffers' nil,
/// "so that changes in the recursive edit will not result in undo
/// boundaries in buffers changed before we entered there recursive edit"
/// (Bug#23632).
pub(crate) fn with_recursive_edit_bindings<T>(
    interp: &mut Interpreter,
    env: &mut Env,
    body: impl FnOnce(&mut Interpreter, &mut Env) -> Result<T, LispError>,
) -> Result<T, LispError> {
    let redisplay = interp.bind_special_dynamic("inhibit-redisplay", Value::Nil, env)?;
    let changed =
        match interp.bind_special_dynamic("undo-auto--undoably-changed-buffers", Value::Nil, env) {
            Ok(restore) => restore,
            Err(error) => {
                interp.restore_special_dynamic(redisplay, env)?;
                return Err(error);
            }
        };
    let result = body(interp, env);
    let restored = interp
        .restore_special_dynamic(changed, env)
        .and(interp.restore_special_dynamic(redisplay, env));
    let value = result?;
    restored.map(|()| value)
}

/// minibuf.c:run_exit_minibuf_hook: `minibuffer-exit-hook' in the
/// minibuffer while it is live, the current buffer kept.
fn run_exit_minibuf_hook(
    interp: &mut Interpreter,
    buffer_id: u64,
    env: &mut Env,
) -> Result<(), LispError> {
    let saved = interp.current_buffer_id();
    if interp.has_buffer_id(buffer_id) {
        interp.set_current_buffer_id(buffer_id)?;
    }
    let result = super::safe_run_named_hooks(
        interp,
        "minibuffer-exit-hook",
        env,
        Some(interp.current_buffer_id()),
    );
    if interp.has_buffer_id(saved) {
        interp.set_current_buffer_id(saved)?;
    }
    result
}

/// read_minibuf_unwind's erasure of the expired minibuffer, which keeps
/// `deactivate-mark' as it was ("Prevent error in erase-buffer").
fn erase_expired_minibuffer(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    with_minibuffer_edits_inhibited(interp, env, |interp, env| {
        let old_deactivate_mark = interp
            .lookup_var("deactivate-mark", env)
            .unwrap_or(Value::Nil);
        let erased = super::call(interp, "erase-buffer", &[], env).map(drop);
        interp.set_c_variable("deactivate-mark", old_deactivate_mark);
        erased
    })
}

pub(crate) fn restore_active_minibuffer(
    interp: &mut Interpreter,
    state: ActiveMinibuffer,
    env: &mut Env,
) {
    // read_minibuf_unwind, in the expired minibuffer: the outer level's
    // depth and prompt return, the buffer is erased, and it goes back to
    // `minibuffer-inactive-mode' so no binding made for the read outlives it.
    let expired = interp.has_buffer_id(state.buffer_id)
        && interp.set_current_buffer_id(state.buffer_id).is_ok();
    interp.restore_minibuffer_runtime(state.previous_runtime);
    if expired {
        let _ = erase_expired_minibuffer(interp, env);
        if interp
            .lookup_function("minibuffer-inactive-mode", env)
            .is_ok()
        {
            let _ = interp.call_function_value(
                Value::symbol("minibuffer-inactive-mode"),
                Some("minibuffer-inactive-mode"),
                &[],
                env,
            );
        }
    }
    interp.replace_minibuffer_selected_window_id(state.previous_minibuffer_selected_window_id);

    interp.set_selected_window_id(state.saved_selected_window_id);
    if interp.has_buffer_id(state.saved_selected_window_buffer_id) {
        interp.set_selected_window_buffer_id(state.saved_selected_window_buffer_id);
    }
    if interp.has_buffer_id(state.saved_buffer_id) {
        let _ = interp.set_current_buffer_id(state.saved_buffer_id);
    }
}

fn completing_read_initial_input(args: &[Value]) -> Option<String> {
    args.get(4).and_then(|value| {
        let value = if matches!(value.kind(), Kind::Cons(_)) {
            value.car_if_list()?
        } else {
            *value
        };
        string_like(&value)
            .map(|string| string.text)
            .filter(|text| !text.is_empty())
    })
}

fn completing_read_contents(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    // With simulated input queued (ert-simulate-keys), run a minibuffer
    // key loop: self-inserting chars, TAB completion, RET submits.
    if !crate::lisp::primitives::unread_command_events(interp, env)?.is_empty() {
        return simulated_completing_read(interp, args, env);
    }

    let initial_input = completing_read_initial_input(args);
    if executing_kbd_macro_p(interp, env)
        && let Some(contents) =
            crate::lisp::primitives::dispatch::read_minibuffer_text_from_kbd_macro_inner(
                interp,
                env,
                initial_input.as_deref().unwrap_or_default(),
            )?
    {
        return Ok(Value::String(contents.into()));
    }
    // A live terminal reads through GNU's route: Fcompleting_read defers
    // to `completing-read-function' — minibuffer.el's
    // `completing-read-default' let-binds the completion context, picks
    // the completion keymap, and calls `read-from-minibuffer', whose
    // recursive command loop runs here.  Without that Lisp machinery the
    // native minibuffer subset reads instead.
    if crate::lisp::primitives::has_tty_event_reader()
        && interp
            .lookup_var("noninteractive", env)
            .is_some_and(|value| value.is_nil())
    {
        if real_minibuffer_machinery_available(interp, env)
            && interp
                .lookup_function("completing-read-default", env)
                .is_ok()
        {
            return call_function_value(
                interp,
                &Value::Symbol("completing-read-default".into()),
                args,
                env,
            );
        }
        return interactive_completing_read(interp, args, env);
    }
    // GNU never invents an answer.  With no queued input the minibuffer
    // read happens for real: `read_minibuf' consumes stdin in batch (EOF
    // signals end-of-file), and DEF applies only when a real read submits
    // empty input -- a rule read-from-minibuffer already owns.  The old
    // chain here (initial input, then DEF, then the first candidate, then
    // "") answered on GNU's behalf without reading anything (finding 12).
    let default = args.get(6).cloned().unwrap_or(Value::Nil);
    crate::lisp::primitives::call(
        interp,
        "read-from-minibuffer",
        &[
            args[0],
            initial_input
                .map(|text| Value::String(text.into()))
                .unwrap_or(Value::Nil),
            Value::Nil,
            Value::Nil,
            Value::Nil,
            default,
        ],
        env,
    )
}

pub(crate) fn interactive_form_items(func: &Value) -> Option<Vec<Value>> {
    if let Kind::BuiltinFunc(name) = func.kind() {
        if let Some(form) = generated_builtin_arities::generated_builtin_interactive_form(&name) {
            let parsed = crate::lisp::reader::Reader::new(form)
                .read_all()
                .ok()?
                .into_iter()
                .next()?;
            return parsed.to_vec().ok();
        }
        // The GNU-generated interactive-form table is the only native
        // source; a miss means the builtin is not a command.
        return None;
    }
    // A raw `(lambda ARGS . BODY)' LIST also has an interactive form (GNU
    // interactive_form handles unevaluated lambda expressions; advice.el's
    // ad-interactive-form probes stored advice bodies this way).
    if let Ok(items) = func.to_vec()
        && matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(head)) if head == "lambda")
    {
        return items.get(2..).and_then(interactive_form_in_body);
    }
    let Kind::Closure(lambda) = func.kind() else {
        return None;
    };
    lambda
        .interactive_spec()
        .map(|spec| vec![Value::Symbol("interactive".into()), spec])
}

/// Return GNU's `(interactive SPEC)' metadata for every callable
/// representation.  Genuine byte-code closures store SPEC in closure slot
/// five rather than as a body form; keep that representation detail behind
/// the same query used by `commandp', `interactive-form', and dispatch.
pub(crate) fn callable_interactive_form_items(
    interp: &Interpreter,
    func: &Value,
) -> Option<Vec<Value>> {
    if let Kind::Record(id) = func.kind()
        && let Some(record) = interp.find_record(id)
        && record.kind == crate::lisp::eval::RecordKind::ModuleFunction
    {
        return record
            .slots
            .get(1)?
            .to_vec()
            .ok()
            .filter(|items| !items.is_empty());
    }
    if let Kind::NativeFunction(function) = func.kind() {
        let spec = function.interactive();
        return (!spec.is_nil()).then(|| spec.to_vec().ok()).flatten();
    }
    // callint.c's cons-lambda branch: a spec without MODES entries is
    // answered verbatim (`(interactive)' stays bare, `(interactive "p")'
    // unchanged); only a spec carrying modes is trimmed to
    // `(interactive DESCRIPTOR)'.
    interactive_form_items(func).map(|items| {
        if items.len() <= 2 {
            items
        } else {
            vec![
                Value::symbol("interactive"),
                items.get(1).cloned().unwrap_or(Value::Nil),
            ]
        }
    })
}

fn interactive_form_in_body(body: &[Value]) -> Option<Vec<Value>> {
    for form in body.iter() {
        if matches!(form.kind(), Kind::StringObject(_)) {
            continue;
        }
        // Internal evaluator closure markers precede the interactive form
        // in lowered bodies.
        if matches!(form.kind(), Kind::Symbol(marker) if marker.starts_with(":closure-")) {
            continue;
        }
        if is_declare_form(form) {
            continue;
        }
        let Ok(items) = form.to_vec() else {
            break;
        };
        if matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(name)) if name == "interactive")
        {
            return Some(items);
        }
        break;
    }
    None
}

pub(crate) fn interactive_spec_form(interp: &Interpreter, func: &Value) -> Option<Value> {
    callable_interactive_form_items(interp, func)
        .map(|items| items.get(1).cloned().unwrap_or(Value::Nil))
}

pub(crate) fn interactive_list_form_items(form: &Value) -> Option<Vec<Value>> {
    let items = form.to_vec().ok()?;
    matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(name)) if name == "list")
        .then(|| items[1..].to_vec())
}

// Whether COLLECTION is a programmed completion table (a function).
pub(crate) fn completion_table_is_function(
    interp: &Interpreter,
    collection: &Value,
    env: &Env,
) -> bool {
    match collection.kind() {
        Kind::Symbol(_) | Kind::Closure(_) | Kind::BuiltinFunc(_) | Kind::NativeFunction(_) => true,
        Kind::Record(_) => callable_value_p(interp, collection, env),
        Kind::Cons(_) => matches!(
            collection.car().map(|v| v.kind()),
            Ok(Kind::Symbol(head)) if head == "lambda" || head == "closure"
        ),
        _ => false,
    }
}

// Longest common prefix of candidate names.
fn common_prefix(names: &[String]) -> String {
    let Some(first) = names.first() else {
        return String::new();
    };
    let mut prefix: Vec<char> = first.chars().collect();
    for name in &names[1..] {
        let chars: Vec<char> = name.chars().collect();
        let mut len = 0usize;
        while len < prefix.len() && len < chars.len() && prefix[len] == chars[len] {
            len += 1;
        }
        prefix.truncate(len);
    }
    prefix.into_iter().collect()
}

// GNU partial-completion over '/'-separated components: expand each
// component as a prefix against the table, one directory level at a time.
fn partial_completion_expand(
    interp: &mut Interpreter,
    contents: &str,
    collection: &Value,
    predicate: Option<&Value>,
    env: &mut Env,
) -> Result<Option<String>, LispError> {
    let components: Vec<&str> = contents.split('/').collect();
    if components.len() < 2 {
        return Ok(None);
    }
    let mut prefixes = vec![String::new()];
    for (index, component) in components.iter().enumerate() {
        let last = index + 1 == components.len();
        let mut expanded = Vec::new();
        for prefix in &prefixes {
            let query = format!("{prefix}{component}");
            for candidate in
                filtered_completion_matches(interp, &query, collection, predicate, env)?
            {
                let component_dir = !last
                    && candidate.name.ends_with('/')
                    && candidate.name[prefix.len()..candidate.name.len() - 1]
                        .starts_with(component)
                    && !candidate.name[prefix.len()..candidate.name.len() - 1].contains('/');
                if last || component_dir {
                    expanded.push(candidate.name);
                }
            }
        }
        expanded.sort();
        expanded.dedup();
        if expanded.is_empty() {
            return Ok(None);
        }
        prefixes = expanded;
    }
    Ok(Some(common_prefix(&prefixes)))
}

/// One submission attempt for minibuffer contents: RET's require-match
/// validation and blank-input default handling (GNU's
/// `minibuffer-complete-and-exit' contract).  `None' means the input was
/// rejected and the minibuffer keeps reading.
pub(crate) fn minibuffer_submission(
    interp: &mut Interpreter,
    env: &mut Env,
    contents: &[char],
    collection: &Value,
    predicate: Option<&Value>,
    require_match: bool,
    default: Option<&String>,
) -> Result<Option<String>, LispError> {
    let used_default = contents.is_empty() && default.is_some();
    let entered = if used_default {
        default.cloned().unwrap_or_default()
    } else {
        contents.iter().collect()
    };
    // GNU accepts DEFAULT on blank input even when REQUIRE-MATCH
    // and PREDICATE would otherwise reject that default.
    if require_match && !used_default {
        let ignore_case = completion_ignores_case(interp, env);
        let matches = filtered_completion_matches(interp, &entered, collection, predicate, env)?;
        if !matches
            .iter()
            .any(|candidate| completion_strings_equal(&candidate.name, &entered, ignore_case))
        {
            return Ok(None);
        }
    }
    Ok(Some(entered))
}

/// TAB in a completing minibuffer: delegate to the loaded Lisp completion
/// engine when present, else extend by the native common prefix (with the
/// trailing-slash and partial-completion fallbacks the simulated reader
/// always had).
fn apply_minibuffer_completion(
    interp: &mut Interpreter,
    env: &mut Env,
    contents: &mut Vec<char>,
    cursor: &mut usize,
    collection: &Value,
    predicate: Option<&Value>,
) -> Result<(), LispError> {
    let current: String = contents.iter().collect();
    // GNU's minibuffer TAB command delegates to the Lisp
    // completion-style engine.  Use that same boundary when it
    // is loaded: programmed tables may return a completion base
    // separate from their candidate strings, which cannot be
    // reconstructed from `all-completions' alone.
    if interp
        .lookup_function("completion-try-completion", env)
        .is_ok()
    {
        let result = call_function_value(
            interp,
            &Value::Symbol("completion-try-completion".into()),
            &[
                Value::String(current.into()),
                *collection,
                predicate.cloned().unwrap_or(Value::Nil),
                Value::Integer(*cursor as i64),
            ],
            env,
        )?;
        if let Some((completed, completed_point)) = result.cons_values()
            && let Some(completed) = string_like(&completed)
            && let Kind::Integer(completed_point) = completed_point.kind()
        {
            *contents = completed.text.chars().collect();
            *cursor = usize::try_from(completed_point)
                .unwrap_or(contents.len())
                .min(contents.len());
        }
        return Ok(());
    }
    let matches = filtered_completion_matches(interp, &current, collection, predicate, env)?;
    if !matches.is_empty() {
        let names: Vec<String> = matches.into_iter().map(|m| m.name).collect();
        let lcp = common_prefix(&names);
        if lcp.chars().count() > contents.len() {
            *contents = lcp.chars().collect();
            *cursor = contents.len();
        }
    } else if let Some(trimmed) = current.strip_suffix('/') {
        // "dir-prefix/" — complete the component before the
        // trailing slash (partial-completion's trailing case).
        let matches = filtered_completion_matches(interp, trimmed, collection, predicate, env)?;
        if !matches.is_empty() {
            let names: Vec<String> = matches.into_iter().map(|m| m.name).collect();
            let lcp = common_prefix(&names);
            if lcp.chars().count() > trimmed.chars().count() {
                *contents = lcp.chars().collect();
                *cursor = contents.len();
            }
        }
    } else if let Some(expanded) =
        partial_completion_expand(interp, &current, collection, predicate, env)?
    {
        *contents = expanded.chars().collect();
        *cursor = contents.len();
    }
    Ok(())
}

/// A plain editing key applied to minibuffer contents: the cursor-motion,
/// deletion, and self-insertion subset every minibuffer keymap shares.
fn apply_minibuffer_edit_key(contents: &mut Vec<char>, cursor: &mut usize, ch: char) {
    match ch {
        '\u{1}' => *cursor = 0,                        // C-a
        '\u{2}' => *cursor = cursor.saturating_sub(1), // C-b
        '\u{4}' if *cursor < contents.len() => {
            contents.remove(*cursor); // C-d
        }
        '\u{5}' => *cursor = contents.len(), // C-e
        '\u{6}' => *cursor = (*cursor + 1).min(contents.len()), // C-f
        '\u{8}' | '\u{7f}' if *cursor > 0 => {
            *cursor -= 1;
            contents.remove(*cursor);
        }
        '\u{b}' => contents.truncate(*cursor), // C-k
        '\u{15}' => {
            contents.clear(); // C-u
            *cursor = 0;
        }
        _ => {
            contents.insert(*cursor, ch);
            *cursor += 1;
        }
    }
}

/// The history variable a minibuffer read records into: HIST arg shapes
/// are SYMBOL, (SYMBOL . STARTPOS), nil (the default
/// `minibuffer-history'), and t (no recording).
pub(crate) fn history_variable_name(spec: &Value) -> Option<String> {
    match spec.kind() {
        Kind::Nil => Some("minibuffer-history".to_string()),
        Kind::Symbol(name) if name == "t" => None,
        Kind::Symbol(name) => Some(name.to_string()),
        Kind::Cons(_) => match spec.car().map(|v| v.kind()) {
            Ok(Kind::Symbol(name)) if name == "t" => None,
            Ok(Kind::Symbol(name)) => Some(name.to_string()),
            _ => Some("minibuffer-history".to_string()),
        },
        _ => Some("minibuffer-history".to_string()),
    }
}

/// minibuf.c:763: read_minibuf sets an unbound history variable to nil
/// before the read, so simple.el's history motion and `add-to-history'
/// (both of which take `symbol-value' of it) see a real list.
fn ensure_history_variable_bound(interp: &mut Interpreter, env: &mut Env, variable: &str) {
    if interp.lookup_var(variable, env).is_none() {
        interp.set_variable(variable, Value::Nil, env);
    }
}

/// Record submitted minibuffer input, GNU's add_to_history: skip empty
/// input and an immediate duplicate, honor `history-length'.
pub(crate) fn push_minibuffer_history(
    interp: &mut Interpreter,
    env: &mut Env,
    variable: &str,
    text: &str,
) {
    if text.is_empty() {
        return;
    }
    // minibuf.c:968: unless `history-add-new-input' is nil, the string
    // goes through subr.el's `add-to-history', which owns deduplication
    // (`history-delete-duplicates' anywhere in the list), per-variable
    // `history-length' properties, and trimming.  Only a runtime without
    // subr.el keeps the native head-dedup subset.
    if interp
        .lookup_var("history-add-new-input", env)
        .is_some_and(|value| value.is_nil())
    {
        return;
    }
    if interp.lookup_function("add-to-history", env).is_ok() {
        let _ = interp.call_function_value(
            Value::Symbol("add-to-history".into()),
            Some("add-to-history"),
            &[Value::Symbol(variable.into()), Value::String(text.into())],
            env,
        );
        return;
    }
    let current = interp.lookup_var(variable, env).unwrap_or(Value::Nil);
    let mut items = current.to_vec().unwrap_or_default();
    if items
        .first()
        .and_then(string_like)
        .is_some_and(|entry| entry.text == text)
    {
        return;
    }
    items.insert(0, Value::String(text.into()));
    let limit = interp
        .lookup_var("history-length", env)
        .and_then(|value| value.integer_value())
        .unwrap_or(100);
    if limit >= 0 {
        items.truncate(limit as usize);
    }
    interp.set_variable(variable, Value::list(items), env);
}

/// The strings currently in a history variable, newest first.
fn history_entries(interp: &Interpreter, env: &Env, variable: &str) -> Vec<String> {
    interp
        .lookup_var(variable, env)
        .and_then(|value| value.to_vec().ok())
        .map(|items| {
            items
                .iter()
                .filter_map(string_like)
                .map(|entry| entry.text)
                .collect()
        })
        .unwrap_or_default()
}

/// Whether the runtime carries the real Lisp minibuffer machinery: the
/// recursive command loop needs `exit-minibuffer' (minibuffer.el) to
/// throw its exit and a populated `minibuffer-local-map' to dispatch
/// keys.  A session without the Lisp tree falls back to the native
/// editing subset below, like a batch runtime without isearch.el leaves
/// C-s unbound.
fn real_minibuffer_machinery_available(interp: &mut Interpreter, env: &mut Env) -> bool {
    interp.lookup_function("exit-minibuffer", env).is_ok()
        && interp
            .lookup_var("minibuffer-local-map", env)
            .is_some_and(|map| crate::lisp::primitives::is_keymap_value(interp, &map))
}

/// GNU read_minibuf's recursive command loop, driven by live terminal
/// events.  Every key sequence resolves through the minibuffer's real
/// keymaps (`key-binding' sees the buffer-local map installed by
/// `activate_minibuffer') and executes the bound Lisp commands —
/// minibuffer.el's TAB/RET machinery, simple.el's history motion — with
/// the full per-command ceremony, until `exit-minibuffer' throws to the
/// `exit' tag.  Runs inside an activated minibuffer; the caller's
/// `run_active_minibuffer' restores the session on every path.
pub(crate) fn interactive_minibuffer_command_loop(
    interp: &mut Interpreter,
    env: &mut Env,
    history_spec: &Value,
) -> Result<String, LispError> {
    // C's read_minibuf seeds the buffer-local history bookkeeping that
    // simple.el's history commands read.
    let buffer_id = interp.current_buffer_id();
    let history_variable = match history_spec.kind() {
        Kind::Nil => Value::Symbol("minibuffer-history".into()),
        Kind::Cons(_) => history_spec.car().unwrap_or(Value::Nil),
        other => other.value(),
    };
    let history_position = history_spec
        .cdr()
        .ok()
        .and_then(|position| position.integer_value())
        .unwrap_or(0);
    interp.set_buffer_local_value(buffer_id, "minibuffer-history-variable", history_variable);
    interp.set_buffer_local_value(
        buffer_id,
        "minibuffer-history-position",
        Value::Integer(history_position),
    );
    if let Some(variable) = history_variable_name(history_spec) {
        ensure_history_variable_bound(interp, env, &variable);
    }

    // The read is one big `catch' for the `exit' tag, GNU read_minibuf's
    // recursive-edit shape: registering the tag makes `throw' from
    // `exit-minibuffer' arrive here instead of signaling `no-catch'.
    interp.push_active_catch_tag(Value::Symbol("exit".into()));
    let loop_outcome = (|interp: &mut Interpreter, env: &mut Env| -> Result<Value, LispError> {
        let mut reader: Option<super::KeySequenceReader> = None;
        // A command error or an undefined key echoes its message until
        // the next keystroke, GNU's transient echo.
        let mut hold_echo = false;
        // read_minibuf's recursive_edit_1 enters command_loop_1, whose entry
        // runs the post-command hooks once before the first input wait
        // (Vertico computes and displays its first candidate set there)
        // and sets `last-command'.
        crate::lisp::primitives::command_loop_entry(interp, env)?;
        loop {
            if reader.is_none() {
                if !hold_echo {
                    let minibuffer_text = interp
                        .active_minibuffer_buffer_id()
                        .and_then(|id| {
                            if id == interp.current_buffer_id() {
                                Some(interp.buffer.borrow().buffer_string())
                            } else {
                                interp
                                    .get_buffer_by_id(id)
                                    .map(|buffer| buffer.buffer_string())
                            }
                        })
                        .unwrap_or_else(|| interp.buffer.borrow().buffer_string());
                    super::set_echo_area_message(Some(minibuffer_text));
                }
                // Window-configuration changes made mid-read (the
                // completion help pop-up) reach the glass before the next
                // key blocks.
                crate::lisp::primitives::run_tty_frame_redraw(interp, env);
            }
            // read_key_sequence reads C-g as a key: the minibuffer's maps
            // bind it to `abort-minibuffers'.
            let event = crate::lisp::primitives::read_key_sequence_event(interp, env)?;
            hold_echo = false;
            if reader.is_none() {
                reader = Some(super::KeySequenceReader::new(interp, Value::Nil, env)?);
            }
            match reader
                .as_mut()
                .expect("active key reader")
                .read_event(interp, event, env)?
            {
                resolution @ (crate::lisp::primitives::KeyResolution::Command(_)
                | crate::lisp::primitives::KeyResolution::Undefined) => {
                    // An undefined key sequence is a nil command, run as
                    // `undefined'.
                    let binding = match resolution {
                        crate::lisp::primitives::KeyResolution::Command(binding) => binding,
                        _ => Value::Nil,
                    };
                    let original = reader
                        .as_ref()
                        .expect("completed key reader")
                        .command_binding();
                    let keys = reader
                        .take()
                        .expect("completed key reader")
                        .finish(interp, false, env);
                    let last_event = keys.last().cloned().unwrap_or(Value::Nil);
                    match crate::lisp::primitives::execute_recorded_input_command_binding(
                        interp, env, original, binding, &keys, last_event,
                    )
                    .map_err(LispError::into_kind)
                    {
                        Ok(()) => {}
                        Err(LispErrorKind::Throw(tag, value)) if matches!(tag.kind(), Kind::Symbol(name) if name == "exit") =>
                        {
                            return Ok(value);
                        }
                        Err(LispErrorKind::Terminate(termination)) => {
                            return Err(LispError::Terminate(termination));
                        }
                        Err(error @ LispErrorKind::Throw(..)) => {
                            // A throw bound for an outer catch unwinds the
                            // whole read, GNU's non-local exit.
                            return Err(LispError::from(error));
                        }
                        Err(error) => {
                            // The recursive edit's command_loop_2 handler,
                            // cmd_error, reports the error in the echo area,
                            // and command_loop_1 is entered again; reading
                            // goes on, and any input restores the
                            // minibuffer display.
                            let error = LispError::from(error);
                            crate::lisp::primitives::cmd_error(interp, env, &error)?;
                            crate::lisp::primitives::command_loop_entry(interp, env)?;
                            hold_echo = true;
                        }
                    }
                }
                crate::lisp::primitives::KeyResolution::Prefix => {}
            }
        }
    })(interp, env);
    interp.pop_active_catch_tag();
    // recursive_edit_1 then acts on the value thrown to `exit', outside
    // the catch: `abort-minibuffers' throws a function that signals
    // `minibuffer-quit'.
    let loop_outcome = loop_outcome.and_then(|value| {
        crate::lisp::primitives::dispatch::recursive_edit_exit_value(interp, env, value)
    });
    // The contents are read before read_minibuf unwinds: the caller's
    // `run_active_minibuffer' then runs the exit hook, erases the
    // minibuffer and restores the window configuration, on every exit
    // path, quits included.
    if let Err(error) = loop_outcome {
        super::set_echo_area_message(None);
        return Err(error);
    }
    super::set_echo_area_message(None);
    // "Make minibuffer contents into a string": read_minibuf sets the
    // minibuffer current first (the exiting command may have run in
    // *Completions*).
    if interp.has_buffer_id(buffer_id) {
        interp.set_current_buffer_id(buffer_id)?;
    }
    let submitted = super::call(interp, "minibuffer-contents-no-properties", &[], env).and_then(
        |contents| {
            string_like(&contents).map(|text| text.text).ok_or_else(|| {
                LispError::Signal("minibuffer-contents-no-properties must answer a string".into())
            })
        },
    )?;
    // add_to_history, C's side of the exit (the Lisp history commands
    // only navigate; recording is read_minibuf's job).
    if let Some(variable) = history_variable_name(history_spec) {
        push_minibuffer_history(interp, env, &variable, &submitted);
    }
    Ok(submitted)
}

/// Drive the active minibuffer with terminal events: a native rendition
/// of GNU's minibuffer command loop over the dumped keymaps' core —
/// self-insertion and editing motion, TAB completion against the
/// installed table, M-p/M-n history recall, RET submission with
/// require-match, C-g quit.  This is the fallback for sessions without
/// the Lisp minibuffer machinery; a full session dispatches through
/// `interactive_minibuffer_command_loop' instead.  The echo area shows
/// the live prompt and contents while the loop runs; the frontend's
/// event reader paints it before blocking for each key.
#[allow(clippy::too_many_arguments)]
fn drive_interactive_minibuffer(
    interp: &mut Interpreter,
    env: &mut Env,
    collection: Option<&Value>,
    predicate: Option<&Value>,
    require_match: bool,
    default: Option<&String>,
    history_spec: &Value,
    initial: &str,
) -> Result<String, LispError> {
    let prompt = interp
        .minibuffer_prompt_text()
        .map(str::to_string)
        .unwrap_or_default();
    let history_variable = history_variable_name(history_spec);
    if let Some(variable) = history_variable.as_deref() {
        ensure_history_variable_bound(interp, env, variable);
    }
    let mut contents: Vec<char> = initial.chars().collect();
    let mut cursor = contents.len();
    // 0 = editing fresh input; N = showing the Nth newest history entry.
    let mut history_position = 0usize;
    let mut stashed_input: Vec<char> = Vec::new();
    let submitted = loop {
        let live: String = contents.iter().collect();
        super::set_echo_area_message(Some(format!("{prompt}{live}")));
        // Window-configuration changes made mid-read (the *Completions*
        // pop-up below) reach the glass before the next key blocks.
        crate::lisp::primitives::run_tty_frame_redraw(interp, env);
        let event = match crate::lisp::primitives::pop_unread_command_event_value(interp, env) {
            Ok(event) => event,
            Err(error) => {
                super::set_echo_area_message(None);
                return Err(error);
            }
        };
        let Some(ch) = crate::lisp::primitives::translated_unread_event_char(&event) else {
            continue;
        };
        match ch {
            '\r' | '\n' => {
                let accepted = match collection {
                    Some(collection) => minibuffer_submission(
                        interp,
                        env,
                        &contents,
                        collection,
                        predicate,
                        require_match,
                        default,
                    )?,
                    None => Some(contents.iter().collect()),
                };
                if let Some(text) = accepted {
                    break text;
                }
            }
            '\t' if collection.is_some() => {
                if let Some(collection) = collection {
                    apply_minibuffer_completion(
                        interp,
                        env,
                        &mut contents,
                        &mut cursor,
                        collection,
                        predicate,
                    )?;
                }
            }
            '\u{1b}' => {
                // A meta key arrives as the ESC prefix plus its base
                // character (the frontend's key encoding).
                let follow =
                    match crate::lisp::primitives::pop_unread_command_event_value(interp, env) {
                        Ok(event) => event,
                        Err(error) => {
                            super::set_echo_area_message(None);
                            return Err(error);
                        }
                    };
                let follow = crate::lisp::primitives::translated_unread_event_char(&follow);
                if let Some(variable) = history_variable.as_deref() {
                    let entries = history_entries(interp, env, variable);
                    match follow {
                        // M-p: previous (older) history element.
                        Some('p') if history_position < entries.len() => {
                            if history_position == 0 {
                                stashed_input = contents.clone();
                            }
                            history_position += 1;
                            contents = entries[history_position - 1].chars().collect();
                            cursor = contents.len();
                        }
                        // M-n: next (newer), back down to the fresh input.
                        Some('n') if history_position > 0 => {
                            history_position -= 1;
                            contents = if history_position == 0 {
                                stashed_input.clone()
                            } else {
                                entries[history_position - 1].chars().collect()
                            };
                            cursor = contents.len();
                        }
                        _ => {}
                    }
                }
            }
            other => apply_minibuffer_edit_key(&mut contents, &mut cursor, other),
        }
    };
    super::set_echo_area_message(None);
    if let Some(variable) = history_variable.as_deref() {
        push_minibuffer_history(interp, env, variable, &submitted);
    }
    Ok(submitted)
}

/// `completing-read' driven by live terminal events.
pub(crate) fn interactive_completing_read(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let collection = args.get(1).cloned().unwrap_or(Value::Nil);
    let predicate = args.get(2).filter(|value| !value.is_nil()).cloned();
    let require_match = args.get(3).is_some_and(Value::is_truthy);
    let default = args.get(6).and_then(string_like).map(|string| string.text);
    let history_spec = args.get(5).cloned().unwrap_or(Value::Nil);
    let initial = completing_read_initial_input(args).unwrap_or_default();
    let text = drive_interactive_minibuffer(
        interp,
        env,
        Some(&collection),
        predicate.as_ref(),
        require_match,
        default.as_ref(),
        &history_spec,
        &initial,
    )?;
    Ok(Value::String(text.into()))
}

/// A minibuffer read driven by live terminal events: read-string and
/// read-from-minibuffer's interactive path.  With the Lisp minibuffer
/// machinery loaded this is GNU's recursive command loop over the real
/// keymaps; without it, the native editing subset reads against any
/// live completion context (`completing-read-default' lets
/// `minibuffer-completion-table' before calling `read-from-minibuffer').
pub(crate) fn interactive_minibuffer_read(
    interp: &mut Interpreter,
    env: &mut Env,
    initial: &str,
    history_spec: &Value,
) -> Result<String, LispError> {
    if real_minibuffer_machinery_available(interp, env) {
        return interactive_minibuffer_command_loop(interp, env, history_spec);
    }
    let collection = interp
        .lookup_var("minibuffer-completion-table", env)
        .filter(|table| !table.is_nil());
    let predicate = interp
        .lookup_var("minibuffer-completion-predicate", env)
        .filter(|predicate| !predicate.is_nil());
    drive_interactive_minibuffer(
        interp,
        env,
        collection.as_ref(),
        predicate.as_ref(),
        false,
        None,
        history_spec,
        initial,
    )
}

fn simulated_completing_read(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let collection = args.get(1).cloned().unwrap_or(Value::Nil);
    let predicate = args.get(2).filter(|value| !value.is_nil()).cloned();
    let require_match = args.get(3).is_some_and(Value::is_truthy);
    let default = args.get(6).and_then(string_like).map(|string| string.text);
    let mut contents = Vec::<char>::new();
    let mut cursor = 0usize;
    let mut accepted = None;
    while let Ok(event) = crate::lisp::primitives::pop_unread_command_event_value(interp, env) {
        let Some(ch) = crate::lisp::primitives::translated_unread_event_char(&event) else {
            continue;
        };
        match ch {
            '\r' | '\n' => {
                if let Some(entered) = minibuffer_submission(
                    interp,
                    env,
                    &contents,
                    &collection,
                    predicate.as_ref(),
                    require_match,
                    default.as_ref(),
                )? {
                    accepted = Some(entered);
                    break;
                }
            }
            '\t' => {
                apply_minibuffer_completion(
                    interp,
                    env,
                    &mut contents,
                    &mut cursor,
                    &collection,
                    predicate.as_ref(),
                )?;
            }
            other => apply_minibuffer_edit_key(&mut contents, &mut cursor, other),
        }
    }
    Ok(Value::String(
        accepted.unwrap_or_else(|| contents.iter().collect()).into(),
    ))
}
