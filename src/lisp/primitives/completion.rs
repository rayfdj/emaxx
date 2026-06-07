use super::*;

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

pub(crate) fn refresh_buffer_menu(
    interp: &mut Interpreter,
    files_only: bool,
    buffer_list: Option<&Value>,
    filter_predicate: Option<&Value>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let entries =
        collect_buffer_menu_entries(interp, files_only, buffer_list, filter_predicate, env)?;
    let rendered = entries
        .iter()
        .filter_map(|entry| match entry {
            Value::Buffer(id, _) => interp
                .get_buffer_by_id(*id)
                .map(|buffer| buffer.name.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");

    let menu_buffer = match interp.find_buffer(BUFFER_MENU_BUFFER_NAME) {
        Some((id, name)) => Value::Buffer(id, name),
        None => {
            let (id, _) = interp.create_buffer(BUFFER_MENU_BUFFER_NAME);
            Value::Buffer(id, BUFFER_MENU_BUFFER_NAME.into())
        }
    };
    let menu_buffer_id = interp.resolve_buffer_id(&menu_buffer)?;
    {
        let buffer = interp
            .get_buffer_by_id_mut(menu_buffer_id)
            .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", menu_buffer_id)))?;
        let end = buffer.point_max();
        if end > 1 {
            buffer
                .delete_region(1, end)
                .map_err(|error| LispError::Signal(error.to_string()))?;
        }
        buffer.goto_char(1);
        buffer.insert(&rendered);
        buffer.goto_char(1);
        buffer.set_unmodified();
    }
    interp.set_buffer_local_value(
        menu_buffer_id,
        BUFFER_MENU_ENTRIES_VAR,
        Value::list(entries),
    );
    Ok(menu_buffer)
}

pub(crate) fn collect_buffer_menu_entries(
    interp: &mut Interpreter,
    files_only: bool,
    buffer_list: Option<&Value>,
    filter_predicate: Option<&Value>,
    env: &mut Env,
) -> Result<Vec<Value>, LispError> {
    let current = Value::Buffer(interp.current_buffer_id(), interp.buffer.name.clone());
    let candidates = match buffer_list {
        Some(value) if !value.is_nil() => resolve_buffer_menu_source(interp, value, env)?,
        _ => {
            let mut ordered = vec![current];
            for (id, name) in interp.buffer_list.clone() {
                if id != interp.current_buffer_id() {
                    ordered.push(Value::Buffer(id, name));
                }
            }
            ordered
        }
    };

    let mut entries = Vec::new();
    for candidate in candidates {
        let buffer_id = interp.resolve_buffer_id(&candidate)?;
        let Some(buffer) = interp.get_buffer_by_id(buffer_id) else {
            continue;
        };
        let name = buffer.name.clone();
        let file = buffer.file.clone();
        if name == BUFFER_MENU_BUFFER_NAME {
            continue;
        }
        if name.starts_with(' ') && file.is_none() {
            continue;
        }
        if files_only && file.is_none() {
            continue;
        }
        let buffer_value = Value::Buffer(buffer_id, name);
        if let Some(predicate) = filter_predicate.filter(|value| !value.is_nil()) {
            let keep = interp.call_function_value(
                predicate.clone(),
                None,
                std::slice::from_ref(&buffer_value),
                env,
            )?;
            if !keep.is_truthy() {
                continue;
            }
        }
        if entries
            .iter()
            .any(|entry| matches!(entry, Value::Buffer(id, _) if *id == buffer_id))
        {
            continue;
        }
        entries.push(buffer_value);
    }

    Ok(entries)
}

pub(crate) fn resolve_buffer_menu_source(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
) -> Result<Vec<Value>, LispError> {
    let source = match value {
        Value::BuiltinFunc(_) | Value::Lambda(_, _, _) => {
            interp.call_function_value(value.clone(), None, &[], env)?
        }
        Value::Symbol(symbol) if interp.lookup_function(symbol, env).is_ok() => {
            interp.call_function_value(value.clone(), None, &[], env)?
        }
        other => other.clone(),
    };
    source.to_vec()
}

pub(crate) fn is_window_value(interp: &Interpreter, value: &Value) -> bool {
    matches!(value, Value::Symbol(symbol) if symbol == "window")
        || matches!(value, Value::Record(id) if interp.find_record(*id).is_some_and(|record| record.type_name == "window"))
}

pub(crate) fn make_obarray(interp: &mut Interpreter) -> Value {
    interp.create_record(OBARRAY_RECORD_TYPE, vec![Value::Nil])
}

pub(crate) fn clear_obarray(interp: &mut Interpreter, obarray: &Value) -> Result<Value, LispError> {
    let Value::Record(id) = obarray else {
        return Err(LispError::TypeError("obarray".into(), obarray.type_name()));
    };
    let Some(record) = interp.find_record_mut(*id) else {
        return Err(LispError::TypeError("obarray".into(), obarray.type_name()));
    };
    match record.type_name.as_str() {
        OBARRAY_RECORD_TYPE => {
            if record.slots.is_empty() {
                record.slots.push(Value::Nil);
            } else {
                record.slots[0] = Value::Nil;
            }
        }
        ABBREV_TABLE_RECORD_TYPE => {
            if record.slots.len() <= ABBREV_TABLE_ENTRIES_SLOT {
                record
                    .slots
                    .resize(ABBREV_TABLE_ENTRIES_SLOT + 1, Value::Nil);
            }
            record.slots[ABBREV_TABLE_ENTRIES_SLOT] = Value::Nil;
        }
        _ => return Err(LispError::TypeError("obarray".into(), obarray.type_name())),
    }
    Ok(Value::Nil)
}

pub(crate) fn is_obarray_like_value(interp: &Interpreter, value: &Value) -> bool {
    let Value::Record(id) = value else {
        return false;
    };
    interp.find_record(*id).is_some_and(|record| {
        matches!(
            record.type_name.as_str(),
            OBARRAY_RECORD_TYPE | ABBREV_TABLE_RECORD_TYPE
        )
    })
}

pub(crate) fn obarray_symbols(
    interp: &Interpreter,
    obarray: &Value,
) -> Result<Vec<Value>, LispError> {
    let Value::Record(id) = obarray else {
        return Err(LispError::TypeError("obarray".into(), obarray.type_name()));
    };
    let Some(record) = interp.find_record(*id) else {
        return Err(LispError::TypeError("obarray".into(), obarray.type_name()));
    };
    if record.type_name == ABBREV_TABLE_RECORD_TYPE {
        return abbrev_table_entries(interp, obarray).map(|entries| {
            std::iter::once(Value::Symbol(abbrev_symbol_name(*id, "")))
                .chain(
                    entries
                        .into_iter()
                        .map(|(name, _, _)| Value::Symbol(abbrev_symbol_name(*id, &name))),
                )
                .collect()
        });
    }
    if record.type_name == OBARRAY_RECORD_TYPE {
        return record.slots.first().cloned().unwrap_or(Value::Nil).to_vec();
    }
    Err(LispError::TypeError("obarray".into(), obarray.type_name()))
}

pub(crate) fn obarray_symbol_matches(value: &Value, symbol_name: &str) -> bool {
    matches!(
        value,
        Value::Symbol(name) if crate::lisp::types::visible_symbol_name(name) == symbol_name
    )
}

pub(crate) fn intern_in_obarray(
    interp: &mut Interpreter,
    obarray: &Value,
    symbol_name: &str,
) -> Result<Value, LispError> {
    let Value::Record(id) = obarray else {
        return Err(LispError::TypeError("obarray".into(), obarray.type_name()));
    };
    let Some(record) = interp.find_record_mut(*id) else {
        return Err(LispError::TypeError("obarray".into(), obarray.type_name()));
    };
    if record.type_name == ABBREV_TABLE_RECORD_TYPE {
        if symbol_name.is_empty() {
            let symbol = abbrev_symbol_name(*id, "");
            interp.set_global_binding(&symbol, Value::Nil);
            return Ok(Value::Symbol(symbol));
        }
        if abbrev_table_entries(interp, obarray)?
            .iter()
            .any(|(existing, _, _)| existing == symbol_name)
        {
            return Ok(Value::Symbol(abbrev_symbol_name(*id, symbol_name)));
        }
        define_abbrev_entry(interp, obarray, symbol_name, Value::Nil, Value::Nil)?;
        return Ok(Value::Symbol(abbrev_symbol_name(*id, symbol_name)));
    }
    if record.type_name != OBARRAY_RECORD_TYPE {
        return Err(LispError::TypeError("obarray".into(), obarray.type_name()));
    }
    let mut symbols = record
        .slots
        .first()
        .cloned()
        .unwrap_or(Value::Nil)
        .to_vec()?;
    if let Some(existing) = symbols
        .iter()
        .find(|value| obarray_symbol_matches(value, symbol_name))
        .cloned()
    {
        return Ok(existing);
    }
    let symbol = Value::Symbol(crate::lisp::types::make_obarray_symbol_name(
        symbol_name,
        *id,
    ));
    symbols.push(symbol.clone());
    if record.slots.is_empty() {
        record.slots.push(Value::list(symbols));
    } else {
        record.slots[0] = Value::list(symbols);
    }
    Ok(symbol)
}

pub(crate) fn intern_soft_in_obarray(
    interp: &Interpreter,
    obarray: &Value,
    symbol_name: &str,
) -> Result<Value, LispError> {
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
    let Value::Record(id) = obarray else {
        return Err(LispError::TypeError("obarray".into(), obarray.type_name()));
    };
    let Some(record) = interp.find_record(*id) else {
        return Err(LispError::TypeError("obarray".into(), obarray.type_name()));
    };
    if record.type_name != OBARRAY_RECORD_TYPE {
        return Err(LispError::TypeError("obarray".into(), obarray.type_name()));
    }
    let mut symbols = record
        .slots
        .first()
        .cloned()
        .unwrap_or(Value::Nil)
        .to_vec()?;
    let original_len = symbols.len();
    match target {
        Value::Symbol(symbol_name) => {
            symbols.retain(|value| !matches!(value, Value::Symbol(name) if name == symbol_name));
        }
        _ => {
            let symbol_name = apply_symbol_shorthands_in_env(interp, &string_text(target)?, env)?;
            symbols.retain(|value| !obarray_symbol_matches(value, &symbol_name));
        }
    }
    let removed = symbols.len() != original_len;
    let Some(record) = interp.find_record_mut(*id) else {
        return Err(LispError::TypeError("obarray".into(), obarray.type_name()));
    };
    if record.slots.is_empty() {
        record.slots.push(Value::list(symbols));
    } else {
        record.slots[0] = Value::list(symbols);
    }
    Ok(removed)
}

pub(crate) fn values_eq_for_substitution(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Nil, Value::Nil) | (Value::T, Value::T) => true,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::BigInteger(a), Value::BigInteger(b)) => a == b,
        (Value::Integer(a), Value::BigInteger(b)) | (Value::BigInteger(b), Value::Integer(a)) => {
            &BigInt::from(*a) == b
        }
        (Value::Float(a), Value::Float(b)) => a == b,
        (Value::Symbol(a), Value::Symbol(b)) => a == b,
        (Value::BuiltinFunc(a), Value::BuiltinFunc(b)) => a == b,
        (Value::StringObject(left), Value::StringObject(right)) => Rc::ptr_eq(left, right),
        (Value::String(_), Value::String(_))
        | (Value::String(_), Value::StringObject(_))
        | (Value::StringObject(_), Value::String(_)) => false,
        (Value::Cons(left_car, _), Value::Cons(right_car, _)) => Rc::ptr_eq(left_car, right_car),
        (
            Value::Lambda(left_params, left_body, left_env),
            Value::Lambda(right_params, right_body, right_env),
        ) => {
            left_params == right_params
                && left_body == right_body
                && Rc::ptr_eq(left_env, right_env)
        }
        (Value::Buffer(left_id, _), Value::Buffer(right_id, _))
        | (Value::Marker(left_id), Value::Marker(right_id))
        | (Value::Overlay(left_id), Value::Overlay(right_id))
        | (Value::CharTable(left_id), Value::CharTable(right_id))
        | (Value::Record(left_id), Value::Record(right_id))
        | (Value::Finalizer(left_id), Value::Finalizer(right_id)) => left_id == right_id,
        _ => false,
    }
}

pub(crate) fn substitution_visit_key(value: &Value) -> Option<(u8, usize)> {
    match value {
        Value::Cons(car, _) => Some((0, Rc::as_ptr(car) as usize)),
        Value::StringObject(state) => Some((1, Rc::as_ptr(state) as usize)),
        Value::Record(id) => Some((2, *id as usize)),
        Value::CharTable(id) => Some((3, *id as usize)),
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
        return Ok(object.clone());
    }

    let Some(key) = substitution_visit_key(subtree) else {
        return Ok(subtree.clone());
    };
    if !seen.insert(key) {
        return Ok(subtree.clone());
    }

    match subtree {
        Value::Cons(_, _) => {
            let Some((car, cdr)) = subtree.cons_values() else {
                return Ok(subtree.clone());
            };
            let Some((car_slot, cdr_slot)) = subtree.cons_cells() else {
                return Ok(subtree.clone());
            };
            *car_slot.borrow_mut() =
                substitute_object_recurse(interp, object, placeholder, &car, seen)?;
            *cdr_slot.borrow_mut() =
                substitute_object_recurse(interp, object, placeholder, &cdr, seen)?;
            Ok(subtree.clone())
        }
        Value::StringObject(state) => {
            let mut state = state.borrow_mut();
            for span in &mut state.props {
                for (_, prop_value) in &mut span.props {
                    *prop_value =
                        substitute_object_recurse(interp, object, placeholder, prop_value, seen)?;
                }
            }
            Ok(subtree.clone())
        }
        Value::Record(id) => {
            let slot_count = interp
                .find_record(*id)
                .map(|record| record.slots.len())
                .unwrap_or(0);
            for index in 0..slot_count {
                let current = interp
                    .find_record(*id)
                    .and_then(|record| record.slots.get(index).cloned())
                    .unwrap_or(Value::Nil);
                let updated =
                    substitute_object_recurse(interp, object, placeholder, &current, seen)?;
                if let Some(record) = interp.find_record_mut(*id)
                    && let Some(slot) = record.slots.get_mut(index)
                {
                    *slot = updated;
                }
            }
            Ok(subtree.clone())
        }
        Value::CharTable(id) => {
            let (default, extra_slots, entries) = match interp.find_char_table(*id) {
                Some(table) => (
                    table.default.clone(),
                    table.extra_slots.clone(),
                    table.entries.clone(),
                ),
                None => return Ok(subtree.clone()),
            };

            let default = substitute_object_recurse(interp, object, placeholder, &default, seen)?;
            let mut updated_slots = Vec::with_capacity(extra_slots.len());
            for slot in extra_slots {
                updated_slots.push(substitute_object_recurse(
                    interp,
                    object,
                    placeholder,
                    &slot,
                    seen,
                )?);
            }
            let mut updated_entries = Vec::with_capacity(entries.len());
            for mut entry in entries {
                entry.value =
                    substitute_object_recurse(interp, object, placeholder, &entry.value, seen)?;
                updated_entries.push(entry);
            }

            if let Some(table) = interp.find_char_table_mut(*id) {
                table.default = default;
                table.extra_slots = updated_slots;
                table.entries = updated_entries;
            }
            Ok(subtree.clone())
        }
        _ => Ok(subtree.clone()),
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

pub(crate) fn default_intern_soft_result(
    interp: &Interpreter,
    symbol_name: &str,
    env: &Env,
) -> Value {
    if matches!(symbol_name, "nil" | "t") {
        crate::lisp::types::interned_symbol_value(symbol_name.into())
    } else if symbol_name.starts_with(':')
        || interp.lookup_var(symbol_name, env).is_some()
        || interp.lookup_function(symbol_name, env).is_ok()
        || is_builtin(symbol_name)
    {
        Value::Symbol(symbol_name.into())
    } else {
        Value::Nil
    }
}

pub(crate) fn completion_display_name(value: &Value) -> Result<String, LispError> {
    match value {
        Value::String(_) | Value::StringObject(_) => string_text(value),
        Value::Symbol(symbol) => Ok(crate::lisp::types::visible_symbol_name(symbol).to_string()),
        _ => Err(LispError::TypeError(
            "string-or-symbol".into(),
            value.type_name(),
        )),
    }
}

pub(crate) fn ensure_completion_list_item_identity(
    item: &Rc<RefCell<Value>>,
) -> Result<Value, LispError> {
    let current = item.borrow().clone();
    match current {
        Value::String(text) => {
            let shared = make_shared_string_value_with_multibyte(text, Vec::new(), false);
            *item.borrow_mut() = shared.clone();
            Ok(shared)
        }
        value => Ok(value),
    }
}

pub(crate) fn completion_list_candidates(
    collection: &Value,
) -> Result<Vec<CompletionCandidate>, LispError> {
    let mut candidates = Vec::new();
    let mut current = collection.clone();
    let mut seen = HashSet::new();

    loop {
        match current {
            Value::Nil => return Ok(candidates),
            Value::Cons(car, cdr) => {
                let id = Rc::as_ptr(&car) as usize;
                if !seen.insert(id) {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("circular-list".into()),
                        Value::String("Circular list".into()),
                    ])));
                }
                let item = ensure_completion_list_item_identity(&car)?;
                if matches!(item, Value::Cons(_, _)) {
                    let key = item.car()?;
                    candidates.push(CompletionCandidate {
                        name: completion_display_name(&key)?,
                        predicate_args: vec![item],
                    });
                } else {
                    candidates.push(CompletionCandidate {
                        name: completion_display_name(&item)?,
                        predicate_args: vec![item],
                    });
                }
                current = cdr.borrow().clone();
            }
            Value::Integer(_) => return Ok(candidates),
            _ => return Err(LispError::TypeError("list".into(), current.type_name())),
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
            .map(|(key, value)| {
                Ok(CompletionCandidate {
                    name: completion_display_name(&key)?,
                    predicate_args: vec![key, value],
                })
            })
            .collect();
    }
    match obarray_symbols(interp, collection) {
        Ok(symbols) => {
            return symbols
                .into_iter()
                .map(|symbol| {
                    Ok(CompletionCandidate {
                        name: completion_display_name(&symbol)?,
                        predicate_args: vec![symbol],
                    })
                })
                .collect();
        }
        Err(LispError::TypeError(expected, _)) if expected == "obarray" => {}
        Err(error) => return Err(error),
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

pub(crate) fn completion_regex_matches(
    interp: &Interpreter,
    env: &Env,
    candidate: &str,
    pattern: &Value,
) -> Result<bool, LispError> {
    let pattern = string_like(pattern)
        .ok_or_else(|| LispError::TypeError("string".into(), pattern.type_name()))?;
    let regex = regexp::compile_elisp_regex(interp, &pattern, env, "", true)?;
    regex
        .is_match(candidate)
        .map_err(|error| LispError::Signal(error.to_string()))
}

pub(crate) fn completion_common_prefix(
    matches: &[CompletionCandidate],
    input: &str,
    ignore_case: bool,
) -> String {
    let match_chars = matches
        .iter()
        .map(|candidate| candidate.name.chars().collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let input_chars = input.chars().collect::<Vec<_>>();
    let mut prefix = String::new();
    let max_len = match_chars.iter().map(Vec::len).min().unwrap_or(0);

    for index in 0..max_len {
        let first = match_chars[0][index];
        let same_actual = match_chars.iter().all(|chars| chars[index] == first);
        let same_folded = match_chars.iter().all(|chars| {
            if ignore_case {
                chars[index].eq_ignore_ascii_case(&first)
            } else {
                chars[index] == first
            }
        });
        if !same_folded {
            break;
        }
        if !ignore_case || same_actual {
            prefix.push(first);
            continue;
        }
        if let Some(input_char) = input_chars
            .get(index)
            .copied()
            .filter(|input_char| input_char.eq_ignore_ascii_case(&first))
        {
            prefix.push(input_char);
        } else {
            prefix.push(first.to_ascii_lowercase());
        }
    }

    prefix
}

pub(crate) fn filtered_completion_matches(
    interp: &mut Interpreter,
    input: &str,
    collection: &Value,
    predicate: Option<&Value>,
    env: &mut Env,
) -> Result<Vec<CompletionCandidate>, LispError> {
    let ignore_case = completion_ignores_case(interp, env);
    let regexp_list = interp
        .lookup_var("completion-regexp-list", env)
        .and_then(|value| value.to_vec().ok())
        .unwrap_or_default();
    let predicate = predicate.filter(|value| !value.is_nil()).cloned();
    let mut matches = Vec::new();

    for candidate in completion_candidates(interp, collection)? {
        if !completion_matches_prefix(input, &candidate.name, ignore_case) {
            continue;
        }
        let mut regex_match = true;
        for pattern in &regexp_list {
            if !completion_regex_matches(interp, env, &candidate.name, pattern)? {
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
    let input = string_text(&args[0])?;
    let matches = filtered_completion_matches(interp, &input, &args[1], args.get(2), env)?;
    if matches.is_empty() {
        return Ok(Value::Nil);
    }

    let ignore_case = completion_ignores_case(interp, env);
    if ignore_case {
        if let Some(candidate) = matches.iter().find(|candidate| candidate.name == input) {
            if matches.len() == 1 {
                return Ok(Value::T);
            }
            return Ok(Value::String(candidate.name.clone()));
        }
        if let Some(candidate) = matches
            .iter()
            .find(|candidate| candidate.name.eq_ignore_ascii_case(&input))
        {
            return Ok(Value::String(candidate.name.clone()));
        }
    } else if matches.len() == 1 && matches[0].name == input {
        return Ok(Value::T);
    }

    Ok(Value::String(completion_common_prefix(
        &matches,
        &input,
        ignore_case,
    )))
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
    let input = string_text(&args[0])?;
    Ok(Value::list(
        filtered_completion_matches(interp, &input, &args[1], args.get(2), env)?
            .into_iter()
            .map(|candidate| {
                make_shared_string_value_with_multibyte(candidate.name, Vec::new(), false)
            }),
    ))
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
    let input = string_text(&args[0])?;
    let ignore_case = completion_ignores_case(interp, env);
    let matches = filtered_completion_matches(interp, &input, &args[1], args.get(2), env)?;
    Ok(
        if matches
            .iter()
            .any(|candidate| completion_strings_equal(&candidate.name, &input, ignore_case))
        {
            Value::T
        } else {
            Value::Nil
        },
    )
}

pub(crate) fn completing_read(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.is_empty() || args.len() > 8 {
        return Err(LispError::WrongNumberOfArgs(
            "completing-read".into(),
            args.len(),
        ));
    }
    ensure_interaction_allowed(interp, env)?;

    let initial_input = args.get(4).and_then(|value| {
        let value = if matches!(value, Value::Cons(_, _)) {
            value.car().ok()?
        } else {
            value.clone()
        };
        string_like(&value)
            .map(|string| string.text)
            .filter(|text| !text.is_empty())
    });
    if let Some(initial_input) = initial_input {
        return Ok(Value::String(initial_input));
    }

    let default = args.get(6).and_then(|value| match value {
        Value::Nil => None,
        Value::Cons(_, _) => value.car().ok(),
        other => Some(other.clone()),
    });
    if let Some(default) = default
        && let Some(string) = string_like(&default)
        && !string.text.is_empty()
    {
        return Ok(Value::String(string.text));
    }

    if let Some(collection) = args.get(1) {
        let predicate = args.get(2);
        if let Some(candidate) =
            filtered_completion_matches(interp, "", collection, predicate, env)?
                .into_iter()
                .next()
        {
            return Ok(Value::String(candidate.name));
        }
    }

    Ok(Value::String(String::new()))
}

pub(crate) fn list_contains_with(
    interp: &mut Interpreter,
    items: &[Value],
    needle: &Value,
    test: &Value,
    env: &mut Env,
) -> Result<bool, LispError> {
    for item in items {
        if call_function_value(interp, test, &[needle.clone(), item.clone()], env)?.is_truthy() {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn interactive_form_items(func: &Value) -> Option<Vec<Value>> {
    let Value::Lambda(_, body, _) = func else {
        return None;
    };
    for form in body {
        if matches!(form, Value::String(_) | Value::StringObject(_)) {
            continue;
        }
        if is_declare_form(form) {
            continue;
        }
        let Ok(items) = form.to_vec() else {
            break;
        };
        if matches!(items.first(), Some(Value::Symbol(name)) if name == "interactive") {
            return Some(items);
        }
        break;
    }
    None
}

pub(crate) fn interactive_spec_form(func: &Value) -> Option<Value> {
    interactive_form_items(func).map(|items| items.get(1).cloned().unwrap_or(Value::Nil))
}

pub(crate) fn interactive_list_form_items(form: &Value) -> Option<Vec<Value>> {
    let items = form.to_vec().ok()?;
    matches!(items.first(), Some(Value::Symbol(name)) if name == "list")
        .then(|| items[1..].to_vec())
}

pub(crate) fn interactive_args_overrides(func: &Value) -> Vec<(String, Value)> {
    let Value::Lambda(_, body, _) = func else {
        return Vec::new();
    };
    let mut overrides = Vec::new();
    for form in body {
        if matches!(form, Value::String(_) | Value::StringObject(_)) {
            continue;
        }
        if !is_declare_form(form) {
            break;
        }
        let Ok(items) = form.to_vec() else {
            continue;
        };
        for decl in &items[1..] {
            let Ok(parts) = decl.to_vec() else {
                continue;
            };
            if !matches!(parts.first(), Some(Value::Symbol(name)) if name == "interactive-args") {
                continue;
            }
            for arg in &parts[1..] {
                let Ok(entry) = arg.to_vec() else {
                    continue;
                };
                if entry.len() >= 2
                    && let Value::Symbol(name) = &entry[0]
                {
                    overrides.push((name.clone(), entry[1].clone()));
                }
            }
        }
    }
    overrides
}
