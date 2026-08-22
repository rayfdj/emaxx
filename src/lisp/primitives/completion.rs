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

pub(crate) fn is_window_value(interp: &Interpreter, value: &Value) -> bool {
    matches!(value, Value::Symbol(symbol) if symbol == "window")
        || matches!(value, Value::Record(id) if interp.find_record(*id).is_some_and(|record|
            record.kind == crate::lisp::eval::RecordKind::Window))
}

pub(crate) fn make_obarray(interp: &mut Interpreter) -> Value {
    interp.create_pseudovector(
        crate::lisp::eval::RecordKind::Obarray,
        OBARRAY_RECORD_TYPE,
        vec![Value::Nil],
    )
}

pub(crate) fn clear_obarray(interp: &mut Interpreter, obarray: &Value) -> Result<Value, LispError> {
    let Value::Record(id) = obarray else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), obarray.clone()));
    };
    let Some(record) = interp.find_record_mut(*id) else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), obarray.clone()));
    };
    if record.has_symbol_type(OBARRAY_RECORD_TYPE) {
        if record.slots.is_empty() {
            record.slots.push(Value::Nil);
        } else {
            record.slots[0] = Value::Nil;
        }
    } else if record.has_symbol_type(ABBREV_TABLE_RECORD_TYPE) {
        if record.slots.len() <= ABBREV_TABLE_ENTRIES_SLOT {
            record
                .slots
                .resize(ABBREV_TABLE_ENTRIES_SLOT + 1, Value::Nil);
        }
        record.slots[ABBREV_TABLE_ENTRIES_SLOT] = Value::Nil;
    } else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), obarray.clone()));
    }
    Ok(Value::Nil)
}

pub(crate) fn is_obarray_like_value(interp: &Interpreter, value: &Value) -> bool {
    let Value::Record(id) = value else {
        return false;
    };
    interp
        .find_record(*id)
        .is_some_and(|record| record.kind == crate::lisp::eval::RecordKind::Obarray)
}

pub(crate) fn obarray_symbols(
    interp: &Interpreter,
    obarray: &Value,
) -> Result<Vec<Value>, LispError> {
    let Value::Record(id) = obarray else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), obarray.clone()));
    };
    if interp.is_standard_obarray_id(*id) {
        return Ok(interp
            .known_symbol_names()
            .into_iter()
            .map(crate::lisp::types::interned_symbol_value)
            .collect());
    }
    let Some(record) = interp.find_record(*id) else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), obarray.clone()));
    };
    if record.has_symbol_type(ABBREV_TABLE_RECORD_TYPE) {
        return abbrev_table_entries(interp, obarray).map(|entries| {
            std::iter::once(Value::Symbol(abbrev_symbol_name(*id, "").into()))
                .chain(
                    entries
                        .into_iter()
                        .map(|(name, _, _)| Value::Symbol(abbrev_symbol_name(*id, &name).into())),
                )
                .collect()
        });
    }
    if record.has_symbol_type(OBARRAY_RECORD_TYPE) {
        return record.slots.first().cloned().unwrap_or(Value::Nil).to_vec();
    }
    Err(LispError::WrongTypeArgument("obarrayp".into(), obarray.clone()))
}

pub(crate) fn obarray_symbol_matches(value: &Value, symbol_name: &str) -> bool {
    matches!((value, symbol_name), (Value::Nil, "nil") | (Value::T, "t"))
        || matches!(
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
        return Err(LispError::WrongTypeArgument("obarrayp".into(), obarray.clone()));
    };
    if interp.is_standard_obarray_id(*id) {
        interp.intern_symbol_name(symbol_name);
        return Ok(crate::lisp::types::interned_symbol_value(
            symbol_name.to_string(),
        ));
    }
    let Some(record) = interp.find_record_mut(*id) else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), obarray.clone()));
    };
    if record.has_symbol_type(ABBREV_TABLE_RECORD_TYPE) {
        if symbol_name.is_empty() {
            let symbol = abbrev_symbol_name(*id, "");
            interp.set_global_binding(&symbol, Value::Nil);
            return Ok(Value::Symbol(symbol.into()));
        }
        if abbrev_table_entries(interp, obarray)?
            .iter()
            .any(|(existing, _, _)| existing == symbol_name)
        {
            return Ok(Value::Symbol(abbrev_symbol_name(*id, symbol_name).into()));
        }
        define_abbrev_entry(interp, obarray, symbol_name, Value::Nil, Value::Nil)?;
        return Ok(Value::Symbol(abbrev_symbol_name(*id, symbol_name).into()));
    }
    if !record.has_symbol_type(OBARRAY_RECORD_TYPE) {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), obarray.clone()));
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
    let symbol =
        Value::Symbol(crate::lisp::types::make_obarray_symbol_name(symbol_name, *id).into());
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
    if matches!(obarray, Value::Record(id) if interp.is_standard_obarray_id(*id)) {
        return Ok(if interp.standard_obarray_contains_symbol(symbol_name) {
            crate::lisp::types::interned_symbol_value(symbol_name.to_string())
        } else {
            Value::Nil
        });
    }
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
        return Err(LispError::WrongTypeArgument("obarrayp".into(), obarray.clone()));
    };
    if interp.is_standard_obarray_id(*id) {
        let symbol_name = match target {
            Value::Nil => "nil".to_string(),
            Value::T => "t".to_string(),
            Value::Symbol(name) => {
                let visible = crate::lisp::types::visible_symbol_name(name);
                if visible != name {
                    return Ok(false);
                }
                visible.to_string()
            }
            _ => apply_symbol_shorthands_in_env(interp, &string_text(target)?, env)?,
        };
        return Ok(interp.unintern_standard_symbol_name(&symbol_name));
    }
    let Some(record) = interp.find_record(*id) else {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), obarray.clone()));
    };
    if !record.has_symbol_type(OBARRAY_RECORD_TYPE) {
        return Err(LispError::WrongTypeArgument("obarrayp".into(), obarray.clone()));
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
        return Err(LispError::WrongTypeArgument("obarrayp".into(), obarray.clone()));
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
        (Value::Cons(left), Value::Cons(right)) => Rc::ptr_eq(left, right),
        (Value::Lambda(left), Value::Lambda(right)) => Rc::ptr_eq(left, right),
        (Value::Buffer(left), Value::Buffer(right)) => left.id == right.id,
        (Value::Marker(left_id), Value::Marker(right_id))
        | (Value::Overlay(left_id), Value::Overlay(right_id))
        | (Value::CharTable(left_id), Value::CharTable(right_id))
        | (Value::Record(left_id), Value::Record(right_id))
        | (Value::Finalizer(left_id), Value::Finalizer(right_id)) => left_id == right_id,
        _ => false,
    }
}

pub(crate) fn substitution_visit_key(value: &Value) -> Option<(u8, usize)> {
    match value {
        Value::Cons(cell) => Some((0, crate::lisp::types::ConsCell::identity(cell))),
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
        Value::Cons(_) => {
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
                table.replace_entries(updated_entries);
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
    if interp.standard_obarray_symbol_is_uninterned(symbol_name) {
        Value::Nil
    } else if matches!(symbol_name, "nil" | "t") {
        crate::lisp::types::interned_symbol_value(symbol_name.into())
    } else if symbol_name.starts_with(':')
        || interp.lookup_var(symbol_name, env).is_some()
        || interp.lookup_function(symbol_name, env).is_ok()
        || is_builtin(symbol_name)
        // Symbols carrying properties are interned too (defface names have
        // no value or function cell, but erc's face plumbing intern-softs
        // "erc-error-face" and expects the symbol back).
        || !interp.symbol_plist(symbol_name).is_nil()
    {
        Value::Symbol(symbol_name.into())
    } else {
        Value::Nil
    }
}

pub(crate) fn completion_display_name(value: &Value) -> Result<String, LispError> {
    match value {
        Value::String(_) | Value::StringObject(_) => string_text(value),
        Value::Nil => Ok("nil".into()),
        Value::T => Ok("t".into()),
        Value::Symbol(symbol) => Ok(crate::lisp::types::visible_symbol_name(symbol).to_string()),
        _ => Err(LispError::TypeError(
            "string-or-symbol".into(),
            value.type_name(),
        )),
    }
}

pub(crate) fn ensure_completion_list_item_identity(item: &ConsSlot) -> Result<Value, LispError> {
    let current = item.borrow().clone();
    match current {
        Value::String(text) => {
            let shared =
                make_shared_string_value_with_multibyte(text.to_string(), Vec::new(), false);
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
            Value::Cons(cons_cell) => {
                let cdr = &cons_cell.cdr;
                let id = crate::lisp::types::ConsCell::identity(&cons_cell);
                if !seen.insert(id) {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("circular-list".into()),
                        Value::String("Circular list".into()),
                    ])));
                }
                let item = ensure_completion_list_item_identity(&ConsSlot::car(&cons_cell))?;
                if matches!(item, Value::Cons(_)) {
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
            _ => return Err(LispError::WrongTypeArgument("listp".into(), current.clone())),
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
        Err(LispError::WrongTypeArgument(predicate, _)) if predicate == "obarrayp" => {}
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
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), pattern.clone()))?;
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

    // A FUNCTION completion table answers (TABLE STRING PRED t) with the
    // list of matching completions itself.
    if completion_table_is_function(interp, collection) {
        let all = call_function_value(
            interp,
            collection,
            &[
                Value::String(input.to_string().into()),
                predicate.clone().unwrap_or(Value::Nil),
                Value::T,
            ],
            env,
        )?;
        for name in all.to_vec().unwrap_or_default() {
            let name = completion_display_name(&name)?;
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

fn completion_collection_function(
    collection: &Value,
    interp: &Interpreter,
    env: &Env,
) -> Result<Option<Value>, LispError> {
    let function = match collection {
        Value::Symbol(symbol) => interp.lookup_function(symbol, env)?,
        _ if callable_value_p(interp, collection) => collection.clone(),
        _ => return Ok(None),
    };
    Ok(Some(function))
}

fn call_programmed_completion(
    interp: &mut Interpreter,
    input: &str,
    collection: &Value,
    predicate: Option<&Value>,
    action: Value,
    env: &mut Env,
) -> Result<Option<Value>, LispError> {
    let Some(function) = completion_collection_function(collection, interp, env)? else {
        return Ok(None);
    };
    Ok(Some(interp.call_function_value(
        function,
        None,
        &[
            Value::String(input.into()),
            predicate.cloned().unwrap_or(Value::Nil),
            action,
        ],
        env,
    )?))
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
    if let Some(result) =
        call_programmed_completion(interp, &input, &args[1], args.get(2), Value::Nil, env)?
    {
        return Ok(result);
    }
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
            return Ok(make_shared_string_value_with_multibyte(
                candidate.name.clone(),
                Vec::new(),
                false,
            ));
        }
        if let Some(candidate) = matches
            .iter()
            .find(|candidate| candidate.name.eq_ignore_ascii_case(&input))
        {
            return Ok(make_shared_string_value_with_multibyte(
                candidate.name.clone(),
                Vec::new(),
                false,
            ));
        }
    } else if matches.iter().all(|candidate| candidate.name == input) {
        return Ok(Value::T);
    }

    // GNU's Ftry_completion returns a fresh string (not `eq' to any
    // candidate; probed 2026-08-21), and callers mutate it in place --
    // completion-preview.el sets a `face' on it and reads the result back
    // through compiled locals.  A plain immutable string here made
    // `set-text-properties' silently rewrite only the caller's environment
    // binding, which bytecode stack slots never see; a shared string gives
    // every holder the same mutable object, like `all-completions' above.
    Ok(make_shared_string_value_with_multibyte(
        completion_common_prefix(&matches, &input, ignore_case),
        Vec::new(),
        false,
    ))
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
    if let Some(result) =
        call_programmed_completion(interp, &input, &args[1], args.get(2), Value::T, env)?
    {
        return Ok(result);
    }
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
    if let Some(result) = call_programmed_completion(
        interp,
        &input,
        &args[1],
        args.get(2),
        Value::Symbol("lambda".into()),
        env,
    )? {
        return Ok(result);
    }
    let ignore_case = completion_ignores_case(interp, env);
    let regexp_list = interp
        .lookup_var("completion-regexp-list", env)
        .and_then(|value| value.to_vec().ok())
        .unwrap_or_default();
    let predicate = args.get(2).filter(|value| !value.is_nil()).cloned();
    for candidate in completion_candidates(interp, &args[1])? {
        if !completion_strings_equal(&candidate.name, &input, ignore_case) {
            continue;
        }
        let mut matches_regexps = true;
        for pattern in &regexp_list {
            if !completion_regex_matches(interp, env, &candidate.name, pattern)? {
                matches_regexps = false;
                break;
            }
        }
        if !matches_regexps {
            continue;
        }
        if let Some(predicate) = &predicate {
            let predicate = resolve_callable(interp, predicate, env)?;
            let result = invoke_function_value(interp, &predicate, &candidate.predicate_args, env)?;
            if result.is_truthy() {
                return Ok(result);
            }
        } else {
            return Ok(Value::T);
        }
    }
    Ok(Value::Nil)
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
                    make_shared_string_value_with_multibyte(name.clone(), Vec::new(), false),
                    Value::buffer(*id, name.clone()),
                )
            })
            .collect::<Vec<_>>(),
    );
    match &args[2] {
        Value::Nil => try_completion(
            interp,
            &[args[0].clone(), buffer_alist, args[1].clone()],
            env,
        ),
        Value::T => {
            let completions = all_completions(
                interp,
                &[args[0].clone(), buffer_alist, args[1].clone()],
                env,
            )?;
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
        Value::Symbol(flag) if flag == "lambda" => test_completion(
            interp,
            &[args[0].clone(), buffer_alist, args[1].clone()],
            env,
        ),
        Value::Symbol(flag) if flag == "metadata" => Ok(Value::list([
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

    if !interp.kbd_macro_executions.is_empty() {
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
        }

        body(interp, env)
    })();
    restore_active_minibuffer(interp, minibuffer);
    result
}

pub(crate) struct ActiveMinibuffer {
    pub(crate) buffer_id: u64,
    saved_buffer_id: u64,
    saved_selected_window_id: u64,
    saved_selected_window_buffer_id: u64,
    previous_runtime: crate::lisp::eval::MinibufferRuntimeState,
}

fn activate_completing_read_minibuffer(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<ActiveMinibuffer, LispError> {
    let prompt = args
        .first()
        .and_then(string_like)
        .ok_or_else(|| {
            LispError::TypeError(
                "string".into(),
                args.first().map_or_else(|| "nil".into(), Value::type_name),
            )
        })?
        .text;
    let initial_input = completing_read_initial_input(args).unwrap_or_default();
    let require_match = args.get(3).is_some_and(Value::is_truthy);
    let map_name = if require_match {
        "minibuffer-local-must-match-map"
    } else {
        "minibuffer-local-completion-map"
    };
    let local_map = interp.lookup_var(map_name, env).unwrap_or(Value::Nil);
    let active = activate_minibuffer(interp, &prompt, &initial_input, local_map, env)?;
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

pub(crate) fn activate_minibuffer(
    interp: &mut Interpreter,
    prompt: &str,
    initial_input: &str,
    local_map: Value,
    env: &Env,
) -> Result<ActiveMinibuffer, LispError> {
    let depth = interp.minibuffer_depth().saturating_add(1);
    let buffer_id = interp
        .find_buffer(&format!(" *Minibuf-{depth}*"))
        .map(|(id, _)| id)
        .unwrap_or_else(|| interp.create_buffer(&format!(" *Minibuf-{depth}*")).0);
    let saved_buffer_id = interp.current_buffer_id();
    let saved_selected_window_id = interp.selected_window_id();
    let saved_selected_window_buffer_id = interp.selected_window_buffer_id();
    let default_directory = interp.lookup_var("default-directory", env);
    interp.clear_buffer_local_state(buffer_id);
    if let Some(default_directory) = default_directory {
        interp.set_buffer_local_value(buffer_id, "default-directory", default_directory);
    }
    interp.set_buffer_local_value(buffer_id, "buffer-read-only", Value::Nil);
    interp.switch_to_buffer_id(buffer_id)?;
    let end = interp.buffer.point_max();
    if end > interp.buffer.point_min() {
        interp
            .buffer
            .delete_region(interp.buffer.point_min(), end)
            .map_err(|error| LispError::Signal(error.to_string()))?;
    }
    interp.buffer.goto_char(interp.buffer.point_min());
    interp.buffer.insert(prompt);
    if !initial_input.is_empty() {
        interp.buffer.insert(initial_input);
    }
    interp.buffer.goto_char(interp.buffer.point_max());

    interp.set_buffer_local_value(buffer_id, "current-local-map", local_map);

    let previous_runtime =
        interp.begin_minibuffer_runtime(buffer_id, interp.selected_window_id(), prompt.to_string());

    Ok(ActiveMinibuffer {
        buffer_id,
        saved_buffer_id,
        saved_selected_window_id,
        saved_selected_window_buffer_id,
        previous_runtime,
    })
}

pub(crate) fn restore_active_minibuffer(interp: &mut Interpreter, state: ActiveMinibuffer) {
    interp.restore_minibuffer_runtime(state.previous_runtime);

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
        let value = if matches!(value, Value::Cons(_)) {
            value.car().ok()?
        } else {
            value.clone()
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
    if !interp.kbd_macro_executions.is_empty()
        && let Some(contents) =
            crate::lisp::primitives::dispatch::read_minibuffer_text_from_kbd_macro_inner(
                interp,
                env,
                initial_input.as_deref().unwrap_or_default(),
            )?
    {
        return Ok(Value::String(contents.into()));
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
            args[0].clone(),
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
    if let Value::BuiltinFunc(name) = func {
        if let Some(form) = generated_builtin_arities::generated_builtin_interactive_form(name) {
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
        && matches!(items.first(), Some(Value::Symbol(head)) if head == "lambda")
    {
        return items.get(2..).and_then(interactive_form_in_body);
    }
    let Value::Lambda(lambda) = func else {
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
    if let Value::Record(id) = func
        && let Some(record) = interp.find_record(*id)
        && record.kind == crate::lisp::eval::RecordKind::Closure
        && let Ok(Some(object)) = crate::lisp::bytecode::ByteCodeObject::from_slots(&record.slots)
        && let Some(spec) = object.interactive
        && !spec.is_nil()
    {
        return Some(vec![Value::symbol("interactive"), spec]);
    }
    interactive_form_items(func)
}

fn interactive_form_in_body(body: &[Value]) -> Option<Vec<Value>> {
    for form in body.iter() {
        if matches!(form, Value::String(_) | Value::StringObject(_)) {
            continue;
        }
        // Internal evaluator closure markers precede the interactive form
        // in lowered bodies.
        if matches!(form, Value::Symbol(marker) if marker.starts_with(":closure-")) {
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


pub(crate) fn interactive_spec_form(interp: &Interpreter, func: &Value) -> Option<Value> {
    callable_interactive_form_items(interp, func)
        .map(|items| items.get(1).cloned().unwrap_or(Value::Nil))
}

pub(crate) fn interactive_list_form_items(form: &Value) -> Option<Vec<Value>> {
    let items = form.to_vec().ok()?;
    matches!(items.first(), Some(Value::Symbol(name)) if name == "list")
        .then(|| items[1..].to_vec())
}

// Whether COLLECTION is a programmed completion table (a function).
pub(crate) fn completion_table_is_function(interp: &Interpreter, collection: &Value) -> bool {
    match collection {
        Value::Symbol(_) | Value::Lambda(_) | Value::BuiltinFunc(_) => true,
        Value::Record(_) => callable_value_p(interp, collection),
        Value::Cons(_) => matches!(
            collection.car(),
            Ok(Value::Symbol(head)) if head == "lambda" || head == "closure"
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
                let used_default = contents.is_empty() && default.is_some();
                let entered = if used_default {
                    default.clone().unwrap_or_default()
                } else {
                    contents.iter().collect()
                };
                // GNU accepts DEFAULT on blank input even when REQUIRE-MATCH
                // and PREDICATE would otherwise reject that default.
                if require_match && !used_default {
                    let ignore_case = completion_ignores_case(interp, env);
                    let matches = filtered_completion_matches(
                        interp,
                        &entered,
                        &collection,
                        predicate.as_ref(),
                        env,
                    )?;
                    if !matches.iter().any(|candidate| {
                        completion_strings_equal(&candidate.name, &entered, ignore_case)
                    }) {
                        continue;
                    }
                }
                accepted = Some(entered);
                break;
            }
            '\t' => {
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
                            collection.clone(),
                            predicate.clone().unwrap_or(Value::Nil),
                            Value::Integer(cursor as i64),
                        ],
                        env,
                    )?;
                    if let Some((completed, completed_point)) = result.cons_values()
                        && let Some(completed) = string_like(&completed)
                        && let Value::Integer(completed_point) = completed_point
                    {
                        contents = completed.text.chars().collect();
                        cursor = usize::try_from(completed_point)
                            .unwrap_or(contents.len())
                            .min(contents.len());
                    }
                } else {
                    let matches = filtered_completion_matches(
                        interp,
                        &current,
                        &collection,
                        predicate.as_ref(),
                        env,
                    )?;
                    if !matches.is_empty() {
                        let names: Vec<String> = matches.into_iter().map(|m| m.name).collect();
                        let lcp = common_prefix(&names);
                        if lcp.chars().count() > contents.len() {
                            contents = lcp.chars().collect();
                            cursor = contents.len();
                        }
                    } else if let Some(trimmed) = current.strip_suffix('/') {
                        // "dir-prefix/" — complete the component before the
                        // trailing slash (partial-completion's trailing case).
                        let matches = filtered_completion_matches(
                            interp,
                            trimmed,
                            &collection,
                            predicate.as_ref(),
                            env,
                        )?;
                        if !matches.is_empty() {
                            let names: Vec<String> = matches.into_iter().map(|m| m.name).collect();
                            let lcp = common_prefix(&names);
                            if lcp.chars().count() > trimmed.chars().count() {
                                contents = lcp.chars().collect();
                                cursor = contents.len();
                            }
                        }
                    } else if let Some(expanded) = partial_completion_expand(
                        interp,
                        &current,
                        &collection,
                        predicate.as_ref(),
                        env,
                    )? {
                        contents = expanded.chars().collect();
                        cursor = contents.len();
                    }
                }
            }
            '\u{1}' => cursor = 0,                        // C-a
            '\u{2}' => cursor = cursor.saturating_sub(1), // C-b
            '\u{4}' if cursor < contents.len() => {
                contents.remove(cursor); // C-d
            }
            '\u{5}' => cursor = contents.len(), // C-e
            '\u{6}' => cursor = (cursor + 1).min(contents.len()), // C-f
            '\u{8}' | '\u{7f}' if cursor > 0 => {
                cursor -= 1;
                contents.remove(cursor);
            }
            '\u{b}' => contents.truncate(cursor), // C-k
            '\u{15}' => {
                contents.clear(); // C-u
                cursor = 0;
            }
            _ => {
                contents.insert(cursor, ch);
                cursor += 1;
            }
        }
    }
    Ok(Value::String(
        accepted.unwrap_or_else(|| contents.iter().collect()).into(),
    ))
}
