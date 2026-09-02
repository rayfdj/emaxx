use super::*;

pub(crate) const OBARRAY_RECORD_TYPE: &str = "obarray";

#[derive(Clone)]
pub(crate) struct CompletionCandidate {
    name: String,
    result: Value,
    predicate_args: Vec<Value>,
}

fn completion_result_value(value: &Value, name: &str) -> Value {
    match value {
        Value::String(_) | Value::StringObject(_) => value.clone(),
        _ => make_shared_string_value_with_multibyte(name.to_string(), Vec::new(), false),
    }
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
        return Err(LispError::WrongTypeArgument(
            "obarrayp".into(),
            obarray.clone(),
        ));
    };
    let Some(record) = interp.find_record_mut(*id) else {
        return Err(LispError::WrongTypeArgument(
            "obarrayp".into(),
            obarray.clone(),
        ));
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
        return Err(LispError::WrongTypeArgument(
            "obarrayp".into(),
            obarray.clone(),
        ));
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
    // Read-only face of check_obarray_slow's legacy-vector rule: a vector
    // already carrying an obarray in slot 0 reads through it, and an
    // untouched one (slot 0 still the fixnum 0) reads as empty.
    if is_vector_value(obarray) {
        let slots = vector_slot_refs(obarray)?;
        if let Some(first) = slots.first() {
            let current = first.borrow().clone();
            if is_obarray_like_value(interp, &current) {
                return obarray_symbols(interp, &current);
            }
            if matches!(current, Value::Integer(0)) {
                return Ok(Vec::new());
            }
        }
    }
    let Value::Record(id) = obarray else {
        return Err(LispError::WrongTypeArgument(
            "obarrayp".into(),
            obarray.clone(),
        ));
    };
    if interp.is_standard_obarray_id(*id) {
        return Ok(interp
            .known_symbol_names()
            .into_iter()
            .map(crate::lisp::types::interned_symbol_value)
            .collect());
    }
    let Some(record) = interp.find_record(*id) else {
        return Err(LispError::WrongTypeArgument(
            "obarrayp".into(),
            obarray.clone(),
        ));
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
    Err(LispError::WrongTypeArgument(
        "obarrayp".into(),
        obarray.clone(),
    ))
}

pub(crate) fn obarray_symbol_matches(value: &Value, symbol_name: &str) -> bool {
    matches!((value, symbol_name), (Value::Nil, "nil") | (Value::T, "t"))
        || matches!(
            value,
            Value::Symbol(name) if crate::lisp::types::visible_symbol_name(name) == symbol_name
        )
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
        return Ok(obarray.clone());
    }
    let slots = vector_slot_refs(obarray)?;
    let Some(first) = slots.first() else {
        return Err(LispError::WrongTypeArgument(
            "obarrayp".into(),
            obarray.clone(),
        ));
    };
    let current = first.borrow().clone();
    if is_obarray_like_value(interp, &current) {
        return Ok(current);
    }
    if matches!(current, Value::Integer(0)) {
        let fresh = make_obarray(interp);
        *first.borrow_mut() = fresh.clone();
        return Ok(fresh);
    }
    Err(LispError::WrongTypeArgument(
        "obarrayp".into(),
        obarray.clone(),
    ))
}

pub(crate) fn intern_in_obarray(
    interp: &mut Interpreter,
    obarray: &Value,
    symbol_name: &str,
) -> Result<Value, LispError> {
    let obarray = &coerce_legacy_vector_obarray(interp, obarray)?;
    let Value::Record(id) = obarray else {
        return Err(LispError::WrongTypeArgument(
            "obarrayp".into(),
            obarray.clone(),
        ));
    };
    if interp.is_standard_obarray_id(*id) {
        interp.intern_symbol_name(symbol_name);
        return Ok(crate::lisp::types::interned_symbol_value(
            symbol_name.to_string(),
        ));
    }
    let Some(record) = interp.find_record_mut(*id) else {
        return Err(LispError::WrongTypeArgument(
            "obarrayp".into(),
            obarray.clone(),
        ));
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
        return Err(LispError::WrongTypeArgument(
            "obarrayp".into(),
            obarray.clone(),
        ));
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
    let obarray = &coerce_legacy_vector_obarray(interp, obarray)?;
    let Value::Record(id) = obarray else {
        return Err(LispError::WrongTypeArgument(
            "obarrayp".into(),
            obarray.clone(),
        ));
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
        return Err(LispError::WrongTypeArgument(
            "obarrayp".into(),
            obarray.clone(),
        ));
    };
    if !record.has_symbol_type(OBARRAY_RECORD_TYPE) {
        return Err(LispError::WrongTypeArgument(
            "obarrayp".into(),
            obarray.clone(),
        ));
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
        return Err(LispError::WrongTypeArgument(
            "obarrayp".into(),
            obarray.clone(),
        ));
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
        // Representation equality, like eq/eql (see values_eq_in_env).
        (Value::Float(a), Value::Float(b)) => a.to_bits() == b.to_bits(),
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
    // Treating every `:name' as interned is WRONG in one direction and right
    // in another, and the right direction is the common one.  GNU's
    // `intern-soft' is a pure `oblookup', so a keyword nobody has interned
    // answers nil -- Emaxx answers the keyword, which is finding 112.
    //
    // But tightening this to a real membership test made things worse, not
    // better, and the measurement is worth keeping: GNU's preloaded obarray
    // holds 429 keywords, Emaxx's holds 141.  Requiring real membership
    // answered nil for 288 of GNU's 429 -- `:key', `:buffer', `:error',
    // `:host' and so on -- trading one rare false positive for 288 common
    // false negatives.  The permissive clause is accidentally right for every
    // keyword GNU actually preloads.
    //
    // The honest fix is to seed the missing keywords into the obarray first,
    // then tighten this; finding 112 stays OPEN until then rather than being
    // closed by a change that regresses the realistic population.
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
                // minibuf.c: an element that is neither a string nor a
                // symbol "is not a possible completion" — it is skipped,
                // never an error (semantic's texi tables carry characters).
                let key = if matches!(item, Value::Cons(_)) {
                    item.car()?
                } else {
                    item.clone()
                };
                if let Ok(name) = completion_display_name(&key) {
                    candidates.push(CompletionCandidate {
                        result: completion_result_value(&key, &name),
                        name,
                        predicate_args: vec![item],
                    });
                }
                current = cdr.borrow().clone();
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
                        result: completion_result_value(&key, &name),
                        name,
                        predicate_args: vec![key, value],
                    })
            })
            .map(Ok)
            .collect();
    }
    match obarray_symbols(interp, collection) {
        Ok(symbols) => {
            return symbols
                .into_iter()
                .map(|symbol| {
                    let name = completion_display_name(&symbol)?;
                    Ok(CompletionCandidate {
                        result: completion_result_value(&symbol, &name),
                        name,
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
    if completion_table_is_function(interp, collection, env) {
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
            let value = name;
            let name = completion_display_name(&value)?;
            matches.push(CompletionCandidate {
                result: completion_result_value(&value, &name),
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
        _ if callable_value_p(interp, collection, env) => collection.clone(),
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
            .map(|candidate| candidate.result),
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
    // read_minibuf registers its window-configuration restore BEFORE
    // selecting the minibuffer window or running the setup hook: a
    // pop-up the hook itself makes (tmm-add-prompt's completion window)
    // vanishes when the read finishes, on every exit path, quits
    // included, and the pre-read window selection returns.
    let saved_windows = minibuffer.saved_windows.clone();
    // read_minibuf enters a recursive command loop, whose per-command
    // bookkeeping must not erase the command that opened the minibuffer.
    // In particular, execute-extended-command deliberately publishes the
    // invoked command through `real-this-command'; its delayed key-binding
    // suggestion checks that value after nested regexp/replacement prompts
    // return.  Restore all three outer command identities on every path.
    let saved_command_state = [
        (
            "this-command",
            interp.lookup_var("this-command", env).unwrap_or(Value::Nil),
        ),
        (
            "real-this-command",
            interp
                .lookup_var("real-this-command", env)
                .unwrap_or(Value::Nil),
        ),
        (
            "this-original-command",
            interp
                .lookup_var("this-original-command", env)
                .unwrap_or(Value::Nil),
        ),
    ];
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
    for (name, value) in saved_command_state {
        interp.set_variable(name, value, env);
    }
    if interp
        .lookup_var("read-minibuffer-restore-windows", env)
        .is_none_or(|restore| restore.is_truthy())
    {
        let _ = interp.restore_window_configuration(saved_windows);
    }
    result
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
    let initial_input = Value::String(
        completing_read_initial_input(args)
            .unwrap_or_default()
            .into(),
    );
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

pub(crate) fn activate_minibuffer(
    interp: &mut Interpreter,
    prompt: &Value,
    initial_input: &Value,
    local_map: Value,
    env: &mut Env,
) -> Result<ActiveMinibuffer, LispError> {
    let prompt_string =
        string_like(prompt).ok_or_else(|| wrong_type_argument("stringp", prompt.clone()))?;
    let prompt_length = prompt_string.text.chars().count();
    let initial_string = string_like(initial_input).unwrap_or_else(|| StringLike {
        text: String::new(),
        props: Vec::new(),
        multibyte: false,
        extended_chars: Vec::new(),
    });
    let initial_length = initial_string.text.chars().count();
    let saved_windows = interp.snapshot_window_configuration();
    let depth = interp.minibuffer_depth().saturating_add(1);
    let buffer_name = format!(" *Minibuf-{depth}*");
    let (buffer_id, reused) = interp
        .find_buffer(&buffer_name)
        .map(|(id, _)| (id, true))
        .unwrap_or_else(|| (interp.create_buffer(&buffer_name).0, false));
    let saved_buffer_id = interp.current_buffer_id();
    let saved_selected_window_id = interp.selected_window_id();
    let saved_selected_window_buffer_id = interp.selected_window_buffer_id();
    let previous_minibuffer_selected_window_id =
        interp.replace_minibuffer_selected_window_id(Some(saved_selected_window_id));
    let default_directory = interp.lookup_var("default-directory", env);
    interp.clear_buffer_local_state(buffer_id);
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
    if let Some(default_directory) = default_directory {
        interp.set_buffer_local_value(buffer_id, "default-directory", default_directory);
    }
    interp.set_buffer_local_value(buffer_id, "buffer-read-only", Value::Nil);
    // GNU reads in the minibuffer window: the minibuffer buffer becomes
    // current without displaying itself in the selected text window,
    // whose glass keeps showing its own buffer during the read.  The
    // window's point slot is saved by hand — the raw selection swap
    // below skips select-window's bookkeeping.
    let entry_point = interp.buffer.point();
    if let Some(window) = interp.find_record_mut(saved_selected_window_id) {
        if window.slots.len() <= super::WINDOW_POINT_SLOT {
            window
                .slots
                .resize(super::WINDOW_POINT_SLOT + 1, Value::Nil);
        }
        window.slots[super::WINDOW_POINT_SLOT] = Value::Integer(entry_point as i64);
    }
    interp.set_current_buffer_id(buffer_id)?;
    let end = interp.buffer.point_max();
    if end > interp.buffer.point_min() {
        interp
            .buffer
            .delete_region(interp.buffer.point_min(), end)
            .map_err(|error| LispError::Signal(error.to_string()))?;
    }
    interp.buffer.goto_char(interp.buffer.point_min());
    // minibuf.c inserts with `inhibit-modification-hooks' bound: copy the
    // prompt's string intervals directly rather than entering ordinary
    // buffer-change hooks.
    interp.buffer.insert(&prompt_string.text);
    for span in &prompt_string.props {
        interp.buffer.add_text_properties(
            1 + span.start,
            1 + span.end.min(prompt_length),
            &span.props,
        );
    }
    interp
        .buffer
        .set_inserted_extended_chars(1, &prompt_string.extended_chars);
    // read_minibuf stamps the prompt: `minibuffer-prompt-properties'
    // (the read-only guard and the prompt face), plus the field and
    // stickiness controls that keep typed text from inheriting them —
    // the prompt is front-sticky and rear-nonsticky.
    if !prompt_string.text.is_empty() {
        let prompt_end = Value::Integer(1 + prompt_length as i64);
        if let Some(properties) = interp
            .lookup_var("minibuffer-prompt-properties", env)
            .filter(|properties| !properties.is_nil())
        {
            let properties = properties.to_vec().unwrap_or_default();
            for pair in properties.as_chunks::<2>().0 {
                let name = pair[0].as_symbol()?;
                if name == "face" {
                    add_face_text_property(
                        interp,
                        "add-face-text-property",
                        &[
                            Value::Integer(1),
                            prompt_end.clone(),
                            pair[1].clone(),
                            Value::T,
                        ],
                    )?;
                } else {
                    interp
                        .buffer
                        .put_text_property(1, 1 + prompt_length, name, pair[1].clone());
                }
            }
        }
        for (name, value) in [
            ("field", Value::T),
            ("front-sticky", Value::T),
            ("rear-nonsticky", Value::T),
        ] {
            interp
                .buffer
                .put_text_property(1, 1 + prompt_length, name, value);
        }
    }
    if !initial_string.text.is_empty() {
        let initial_start = 1 + prompt_length;
        interp.buffer.insert(&initial_string.text);
        for span in &initial_string.props {
            interp.buffer.set_text_properties(
                initial_start + span.start,
                initial_start + span.end.min(initial_length),
                &span.props,
            );
        }
        interp
            .buffer
            .set_inserted_extended_chars(initial_start, &initial_string.extended_chars);
    }
    interp.buffer.goto_char(interp.buffer.point_max());

    interp.set_buffer_local_value(buffer_id, "current-local-map", local_map);

    // Select the minibuffer window for the read, GNU's read_minibuf: the
    // minibuffer buffer shows there, never in the entry window.
    if let Value::Record(minibuffer_window_id) = interp.minibuffer_window_value() {
        interp.set_selected_window_id(minibuffer_window_id);
        interp.set_selected_window_buffer_id(buffer_id);
    }

    let previous_runtime =
        interp.begin_minibuffer_runtime(buffer_id, interp.selected_window_id(), prompt_string.text);

    Ok(ActiveMinibuffer {
        buffer_id,
        saved_windows,
        saved_buffer_id,
        saved_selected_window_id,
        saved_selected_window_buffer_id,
        previous_minibuffer_selected_window_id,
        previous_runtime,
    })
}

pub(crate) fn restore_active_minibuffer(interp: &mut Interpreter, state: ActiveMinibuffer) {
    interp.restore_minibuffer_runtime(state.previous_runtime);
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
    {
        // GNU keys interactivity on the slot's presence (PVSIZE >
        // COMPILED_INTERACTIVE): a bare `(interactive)' stores nil there
        // and the function is still a command.  callint.c
        // Finteractive_form: a vector in the slot is the byte-compiler's
        // (SPEC MODES) encoding -- the form is element 0 alone; the mode
        // list is `command-modes' data and never reaches the caller of
        // the interactive form.
        let spec = match spec.to_vec() {
            Ok(items)
                if matches!(items.first(),
                    Some(Value::Symbol(tag)) if tag == "vector-literal") =>
            {
                items.get(1).cloned().unwrap_or(Value::Nil)
            }
            _ => spec,
        };
        return Some(vec![Value::symbol("interactive"), spec]);
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
pub(crate) fn completion_table_is_function(
    interp: &Interpreter,
    collection: &Value,
    env: &Env,
) -> bool {
    match collection {
        Value::Symbol(_) | Value::Lambda(_) | Value::BuiltinFunc(_) => true,
        Value::Record(_) => callable_value_p(interp, collection, env),
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
                collection.clone(),
                predicate.cloned().unwrap_or(Value::Nil),
                Value::Integer(*cursor as i64),
            ],
            env,
        )?;
        if let Some((completed, completed_point)) = result.cons_values()
            && let Some(completed) = string_like(&completed)
            && let Value::Integer(completed_point) = completed_point
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
fn history_variable_name(spec: &Value) -> Option<String> {
    match spec {
        Value::Nil => Some("minibuffer-history".to_string()),
        Value::Symbol(name) if name == "t" => None,
        Value::Symbol(name) => Some(name.to_string()),
        Value::Cons(_) => match spec.car() {
            Ok(Value::Symbol(name)) if name == "t" => None,
            Ok(Value::Symbol(name)) => Some(name.to_string()),
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
fn push_minibuffer_history(interp: &mut Interpreter, env: &mut Env, variable: &str, text: &str) {
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
        .and_then(|value| value.as_integer().ok())
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
    let history_variable = match history_spec {
        Value::Nil => Value::Symbol("minibuffer-history".into()),
        Value::Cons(_) => history_spec.car().unwrap_or(Value::Nil),
        other => other.clone(),
    };
    let history_position = history_spec
        .cdr()
        .ok()
        .and_then(|position| position.as_integer().ok())
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

    // read_minibuf saves the window configuration before the read; the
    // teardown below puts it back (default `read-minibuffer-restore-windows'
    // t), which is how a *Completions* pop-up vanishes on exit and the
    // shrunk window above it gets its lines back.
    let saved_windows = interp.snapshot_window_configuration();
    // The read is one big `catch' for the `exit' tag, GNU read_minibuf's
    // recursive-edit shape: registering the tag makes `throw' from
    // `exit-minibuffer' arrive here instead of signaling `no-catch'.
    interp.push_active_catch_tag(Value::Symbol("exit".into()));
    let loop_outcome = (|interp: &mut Interpreter, env: &mut Env| -> Result<(), LispError> {
        let mut pending: Vec<Value> = Vec::new();
        // A command error or an undefined key echoes its message until
        // the next keystroke, GNU's transient echo.
        let mut hold_echo = false;
        // read_minibuf enters its recursive command loop through the same
        // initial command-loop boundary as GNU keyboard.c: local
        // post-command hooks run once before the first input wait.  Vertico
        // deliberately uses that boundary to compute and display its first
        // candidate set, before the user presses a key.
        crate::lisp::primitives::safe_run_named_hooks(
            interp,
            "post-command-hook",
            env,
            Some(buffer_id),
        )
        .unwrap_or(());
        loop {
            if pending.is_empty() {
                if !hold_echo {
                    super::set_echo_area_message(Some(interp.buffer.buffer_string()));
                }
                // Window-configuration changes made mid-read (the
                // completion help pop-up) reach the glass before the next
                // key blocks.
                crate::lisp::primitives::run_tty_frame_redraw(interp, env);
            }
            // C-g propagates as GNU's quit out of the recursive edit.
            let event = crate::lisp::primitives::pop_unread_command_event_value(interp, env)?;
            hold_echo = false;
            pending.push(event);
            match crate::lisp::primitives::resolve_key_sequence(interp, env, &pending) {
                crate::lisp::primitives::KeyResolution::Command(binding) => {
                    let keys = std::mem::take(&mut pending);
                    let last_event = keys.last().cloned().unwrap_or(Value::Nil);
                    match crate::lisp::primitives::execute_command_binding(
                        interp, env, binding, &keys, last_event,
                    ) {
                        Ok(()) => {}
                        Err(LispError::Throw(tag, _value)) if matches!(&tag, Value::Symbol(name) if name == "exit") =>
                        {
                            return Ok(());
                        }
                        Err(LispError::Terminate(termination)) => {
                            return Err(LispError::Terminate(termination));
                        }
                        Err(error @ LispError::Throw(..)) => {
                            // A throw bound for an outer catch unwinds the
                            // whole read, GNU's non-local exit.
                            return Err(error);
                        }
                        Err(error) => {
                            // GNU prints the error in the echo area and
                            // keeps reading; any input restores the
                            // minibuffer display.
                            let text = crate::lisp::primitives::command_error_echo_text(
                                interp, env, &error,
                            );
                            super::set_echo_area_message(Some(text));
                            hold_echo = true;
                        }
                    }
                }
                crate::lisp::primitives::KeyResolution::Prefix => {}
                crate::lisp::primitives::KeyResolution::Undefined => {
                    pending.clear();
                }
            }
        }
    })(interp, env);
    interp.pop_active_catch_tag();
    // read_minibuf_unwind: the exit hook runs in the minibuffer, then the
    // saved window configuration returns — on every exit path, quits
    // included.
    crate::lisp::primitives::safe_run_named_hooks(
        interp,
        "minibuffer-exit-hook",
        env,
        Some(buffer_id),
    )
    .unwrap_or(());
    if interp
        .lookup_var("read-minibuffer-restore-windows", env)
        .is_none_or(|restore| restore.is_truthy())
    {
        let _ = interp.restore_window_configuration(saved_windows);
    }
    if let Err(error) = loop_outcome {
        super::set_echo_area_message(None);
        return Err(error);
    }
    super::set_echo_area_message(None);
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
