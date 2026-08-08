use super::*;

fn fixnum_index_arg(value: &Value) -> Result<i64, LispError> {
    match value {
        Value::Integer(index) => Ok(*index),
        other => Err(wrong_type_argument("fixnump", other.clone())),
    }
}

fn args_out_of_range(sequence: &Value, index: &Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("args-out-of-range".into()),
        sequence.clone(),
        index.clone(),
    ]))
}

fn current_category_table_id(interp: &mut Interpreter) -> u64 {
    interp
        .buffer_local_value(interp.current_buffer_id(), "category-table")
        .and_then(|value| match value {
            Value::CharTable(id) => Some(id),
            _ => None,
        })
        .unwrap_or_else(|| interp.ensure_standard_category_table())
}

fn category_table_arg(
    interp: &mut Interpreter,
    value: Option<&Value>,
    default_to_standard: bool,
) -> Result<u64, LispError> {
    let id = match value {
        Some(Value::CharTable(id)) => *id,
        Some(Value::Nil) | None if default_to_standard => interp.ensure_standard_category_table(),
        Some(Value::Nil) | None => current_category_table_id(interp),
        Some(other) => {
            return Err(LispError::TypeError(
                "category-table".into(),
                other.type_name(),
            ));
        }
    };
    if interp.char_table_subtype(id).flatten().as_deref() != Some("category-table") {
        return Err(LispError::TypeError(
            "category-table".into(),
            "char-table".into(),
        ));
    }
    Ok(id)
}

fn category_character_range(value: &Value) -> Result<(u32, u32), LispError> {
    let checked = |code: i64| {
        if (0..=char::MAX as i64).contains(&code) {
            Ok(code as u32)
        } else {
            Err(LispError::Signal("Args out of range".into()))
        }
    };
    match value {
        Value::Integer(code) => checked(*code).map(|code| (code, code)),
        Value::Cons(cell) => Ok((
            checked(cell.car.borrow().as_integer()?)?,
            checked(cell.cdr.borrow().as_integer()?)?,
        )),
        other => Err(LispError::TypeError(
            "character-or-cons".into(),
            other.type_name(),
        )),
    }
}

/// Boundaries at which the effective value of a char table can change.
///
/// Char-table writes are stored as ordered, possibly overlapping intervals.
/// Splitting a category update at every boundary preserves earlier per-range
/// values without walking every Unicode scalar in a large GNU category range.
fn char_table_change_boundaries(
    interp: &Interpreter,
    table_id: u64,
    start: u32,
    end: u32,
) -> Vec<u32> {
    let mut boundaries = vec![start];
    let mut next_table = Some(table_id);
    while let Some(id) = next_table {
        let Some(table) = interp.find_char_table(id) else {
            break;
        };
        for entry in &table.entries {
            if entry.end < start || entry.start > end {
                continue;
            }
            boundaries.push(entry.start.max(start));
            if entry.end < end {
                boundaries.push(entry.end + 1);
            }
        }
        next_table = table.parent;
    }
    boundaries.sort_unstable();
    boundaries.dedup();
    boundaries
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut crate::lisp::types::Env,
    ) -> Result<Value, LispError> {
        match name {
            // ── Plist operations ──
            "plist-get" => {
                need_arg_range(name, args, 2, 3)?;
                let plist = args[0].clone();
                let key = &args[1];
                let testfn = args.get(2);
                let mut current = plist.clone();
                let mut seen = crate::lisp::types::CycleGuard::new();
                loop {
                    match current {
                        Value::Nil => return Ok(Value::Nil),
                        Value::Cons(cons_cell) => {
                            let car = &cons_cell.car;
                            let cdr = &cons_cell.cdr;
                            let cell_id = crate::lisp::types::ConsCell::identity(&cons_cell);
                            if seen.step(cell_id) {
                                return Ok(Value::Nil);
                            }
                            let property = car.borrow().clone();
                            if value_matches_with_test(interp, &property, key, testfn, env)? {
                                return match cdr.borrow().clone() {
                                    Value::Cons(cell) => Ok(cell.car.borrow().clone()),
                                    _ => Ok(Value::Nil),
                                };
                            }
                            match cdr.borrow().clone() {
                                Value::Cons(cell) => current = cell.cdr.borrow().clone(),
                                Value::Nil => return Ok(Value::Nil),
                                _ => return Ok(Value::Nil),
                            }
                        }
                        _ => return Ok(Value::Nil),
                    }
                }
            }

            "plist-put" => {
                need_arg_range(name, args, 3, 4)?;
                let plist = args[0].clone();
                let key = &args[1];
                let val = &args[2];
                let testfn = args.get(3);
                let mut current = plist.clone();
                let mut seen = crate::lisp::types::CycleGuard::new();
                loop {
                    match current {
                        Value::Nil => {
                            let mut items = plist.to_vec()?;
                            items.push(key.clone());
                            items.push(val.clone());
                            return Ok(Value::list(items));
                        }
                        Value::Cons(cons_cell) => {
                            let car = &cons_cell.car;
                            let cdr = &cons_cell.cdr;
                            let cell_id = crate::lisp::types::ConsCell::identity(&cons_cell);
                            if seen.step(cell_id) {
                                return Err(LispError::SignalValue(Value::list([
                                    Value::Symbol("circular-list".into()),
                                    Value::String("Circular list".into()),
                                ])));
                            }
                            let property = car.borrow().clone();
                            if value_matches_with_test(interp, &property, key, testfn, env)? {
                                return match cdr.borrow().clone() {
                                    Value::Cons(cons_cell) => {
                                        let value = &cons_cell.car;
                                        let _ = &cons_cell.cdr;
                                        *value.borrow_mut() = val.clone();
                                        Ok(plist)
                                    }
                                    _ => Err(plist_type_error(&plist)),
                                };
                            }
                            match cdr.borrow().clone() {
                                Value::Cons(cons_cell) => {
                                    let _ = &cons_cell.car;
                                    let next_cdr = &cons_cell.cdr;
                                    let next = next_cdr.borrow().clone();
                                    if next.is_nil() {
                                        *next_cdr.borrow_mut() =
                                            Value::list([key.clone(), val.clone()]);
                                        return Ok(plist);
                                    }
                                    current = next;
                                }
                                _ => return Err(plist_type_error(&plist)),
                            }
                        }
                        _ => return Err(plist_type_error(&plist)),
                    }
                }
            }

            "plist-member" => {
                need_arg_range(name, args, 2, 3)?;
                let plist = args[0].clone();
                let key = &args[1];
                let testfn = args.get(2);
                let mut current = plist.clone();
                let mut seen = crate::lisp::types::CycleGuard::new();
                loop {
                    match current {
                        Value::Nil => return Ok(Value::Nil),
                        Value::Cons(cons_cell) => {
                            let car = &cons_cell.car;
                            let cdr = &cons_cell.cdr;
                            let cell_id = crate::lisp::types::ConsCell::identity(&cons_cell);
                            if seen.step(cell_id) {
                                return Err(LispError::SignalValue(Value::list([
                                    Value::Symbol("circular-list".into()),
                                    Value::String("Circular list".into()),
                                ])));
                            }
                            let property = car.borrow().clone();
                            if value_matches_with_test(interp, &property, key, testfn, env)? {
                                return Ok(Value::Cons(cons_cell));
                            }
                            // Skip the value
                            match cdr.borrow().clone() {
                                Value::Cons(cell) => current = cell.cdr.borrow().clone(),
                                Value::Nil => return Ok(Value::Nil),
                                _ => return Err(plist_type_error(&plist)),
                            }
                        }
                        _ => return Err(plist_type_error(&plist)),
                    }
                }
            }

            "seq-uniq" => {
                need_arg_range(name, args, 1, 2)?;
                seq_uniq(interp, &args[0], args.get(1), env)
            }

            // ── Sort ──
            "sort" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let (kind, items) = sort_sequence_kind_and_items(&args[0])?;
                let mut lessp = None;
                let mut key = None;
                let mut in_place = true;
                let mut reverse = false;
                let mut index = 1usize;
                if let Some(arg) = args.get(index)
                    && !matches!(arg, Value::Symbol(symbol) if symbol.starts_with(':'))
                {
                    lessp = Some(arg.clone());
                    index += 1;
                }
                while index + 1 < args.len() {
                    match &args[index] {
                        Value::Symbol(keyword) if keyword == ":key" => {
                            key = if args[index + 1].is_nil() {
                                None
                            } else {
                                Some(args[index + 1].clone())
                            };
                        }
                        Value::Symbol(keyword) if keyword == ":lessp" => {
                            lessp = if args[index + 1].is_nil() {
                                None
                            } else {
                                Some(args[index + 1].clone())
                            };
                        }
                        Value::Symbol(keyword) if keyword == ":in-place" => {
                            in_place = args[index + 1].is_truthy();
                        }
                        Value::Symbol(keyword) if keyword == ":reverse" => {
                            reverse = args[index + 1].is_truthy();
                        }
                        _ => return Err(LispError::WrongNumberOfArgs(name.into(), args.len())),
                    }
                    index += 2;
                }
                if index != args.len() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let sorted =
                    sort_sequence_items(interp, items, key.as_ref(), lessp.as_ref(), reverse, env)?;
                if in_place {
                    write_sorted_sequence(&args[0], &kind, &sorted)?;
                    Ok(args[0].clone())
                } else {
                    Ok(build_sorted_sequence(&kind, sorted))
                }
            }
            "cl-sort" => {
                if args.len() < 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let mut key_fn: Option<Value> = None;
                let mut index = 2usize;
                while index + 1 < args.len() {
                    if args[index].as_symbol()? == ":key" {
                        key_fn = if args[index + 1].is_nil() {
                            None
                        } else {
                            Some(args[index + 1].clone())
                        };
                    }
                    index += 2;
                }
                let mut items = args[0].to_vec()?;
                let len = items.len();
                for i in 1..len {
                    let mut j = i;
                    while j > 0 {
                        let left = if let Some(function) = &key_fn {
                            call_function_value(interp, function, &[items[j - 1].clone()], env)?
                        } else {
                            items[j - 1].clone()
                        };
                        let right = if let Some(function) = &key_fn {
                            call_function_value(interp, function, &[items[j].clone()], env)?
                        } else {
                            items[j].clone()
                        };
                        let result = call_function_value(interp, &args[1], &[left, right], env)?;
                        if result.is_nil() {
                            items.swap(j - 1, j);
                            j -= 1;
                        } else {
                            break;
                        }
                    }
                }
                Ok(Value::list(items))
            }

            "random" => {
                if args.is_empty() || args[0].is_nil() {
                    Ok(Value::Integer(rand_simple()))
                } else {
                    match &args[0] {
                        Value::T => {
                            set_random_seed(nondeterministic_random_seed());
                            Ok(Value::Integer(rand_simple()))
                        }
                        Value::String(_) | Value::StringObject(_) => {
                            let seed = string_like(&args[0])
                                .expect("string variants should be string-like")
                                .text;
                            set_random_seed(random_seed_from_bytes(seed.as_bytes()));
                            Ok(Value::Integer(rand_simple()))
                        }
                        _ => {
                            let limit = integer_like_bigint(interp, &args[0])?;
                            if limit <= BigInt::zero() {
                                Err(LispError::SignalValue(Value::list([
                                    Value::Symbol("args-out-of-range".into()),
                                    args[0].clone(),
                                ])))
                            } else {
                                Ok(normalize_bigint_value(random_bigint_below(&limit)))
                            }
                        }
                    }
                }
            }

            "vector" => {
                let mut items = vec![Value::symbol("vector-literal")];
                items.extend(args.iter().cloned());
                Ok(Value::list(items))
            }
            "bool-vector-count-population" => {
                need_args(name, args, 1)?;
                Ok(Value::Integer(
                    bool_vector_bits(interp, &args[0])?
                        .into_iter()
                        .filter(|bit| *bit)
                        .count() as i64,
                ))
            }
            "bool-vector-count-consecutive" => {
                need_args(name, args, 3)?;
                let bits = bool_vector_bits(interp, &args[0])?;
                let target = args[1].is_truthy();
                let start = args[2].as_integer()?.max(0) as usize;
                let mut count = 0usize;
                for bit in bits.into_iter().skip(start) {
                    if bit != target {
                        break;
                    }
                    count += 1;
                }
                Ok(Value::Integer(count as i64))
            }
            "bool-vector-intersection"
            | "bool-vector-union"
            | "bool-vector-exclusive-or"
            | "bool-vector-set-difference" => {
                need_arg_range(name, args, 2, 3)?;
                let left = bool_vector_bits(interp, &args[0])?;
                let right = bool_vector_bits(interp, &args[1])?;
                if left.len() != right.len() {
                    return Err(LispError::Signal("Args out of range".into()));
                }
                let result = left
                    .iter()
                    .zip(&right)
                    .map(|(left_bit, right_bit)| match name {
                        "bool-vector-intersection" => *left_bit && *right_bit,
                        "bool-vector-union" => *left_bit || *right_bit,
                        "bool-vector-exclusive-or" => *left_bit ^ *right_bit,
                        "bool-vector-set-difference" => *left_bit && !*right_bit,
                        _ => false,
                    })
                    .collect::<Vec<_>>();
                if let Some(target) = args.get(2).filter(|target| !target.is_nil()) {
                    let current = bool_vector_bits(interp, target)?;
                    if current.len() != result.len() {
                        return Err(LispError::Signal("Args out of range".into()));
                    }
                    let mut changed = false;
                    for (index, bit) in result.into_iter().enumerate() {
                        if current[index] != bit {
                            changed = true;
                        }
                        set_bool_vector_bit(interp, target, index, bit)?;
                    }
                    Ok(if changed { target.clone() } else { Value::Nil })
                } else {
                    Ok(make_bool_vector_value(interp, result))
                }
            }
            "bool-vector-not" => {
                need_args(name, args, 1)?;
                Ok(make_bool_vector_value(
                    interp,
                    bool_vector_bits(interp, &args[0])?
                        .into_iter()
                        .map(|bit| !bit),
                ))
            }

            "aref" => {
                need_args(name, args, 2)?;
                let raw_idx = fixnum_index_arg(&args[1])?;
                if raw_idx < 0 {
                    return Err(args_out_of_range(&args[0], &args[1]));
                }
                let idx = raw_idx as usize;
                // Support both list-vectors and strings
                if let Some(items) = record_literal_items(&args[0]) {
                    return record_literal_aref(&args[0], &items, idx, &args[1]);
                }
                match &args[0] {
                    Value::String(_) | Value::StringObject(_) => {
                        match string_like(&args[0])
                            .and_then(|string| string.text.chars().nth(idx).map(|ch| (string, ch)))
                        {
                            Some((string, ch)) => Ok(string_sequence_value(&string, ch)),
                            None => Err(args_out_of_range(&args[0], &args[1])),
                        }
                    }
                    // Interpreted functions expose arglist, body, and captured
                    // environment through aref like GNU's closure objects.
                    Value::Lambda(params, body, closure_env) => Ok(match idx {
                        0 => Value::list(
                            params
                                .iter()
                                .map(|param| Value::Symbol(param.clone()))
                                .collect::<Vec<_>>(),
                        ),
                        1 => Value::list(body.as_ref().clone()),
                        2 => {
                            let mut entries = Vec::new();
                            for frame in closure_env.borrow().iter().rev() {
                                for (name, value) in frame.iter().rev() {
                                    entries.push(Value::cons(
                                        Value::Symbol(name.clone()),
                                        value.clone(),
                                    ));
                                }
                            }
                            entries.push(Value::T);
                            Value::list(entries)
                        }
                        _ => Value::Nil,
                    }),
                    Value::CharTable(id) => {
                        let key = raw_idx as u32;
                        Ok(syntax::char_table_public_value(
                            interp,
                            *id,
                            interp.char_table_get(*id, key).unwrap_or(Value::Nil),
                        ))
                    }
                    Value::Record(id) => {
                        let record = interp.find_record(*id).ok_or_else(|| {
                            LispError::TypeError("record".into(), format!("record<{id}>"))
                        })?;
                        if record.type_name == "bool-vector" {
                            return record
                                .slots
                                .get(idx)
                                .cloned()
                                .ok_or_else(|| args_out_of_range(&args[0], &args[1]));
                        }
                        if record.type_name == "byte-code-function" {
                            if idx == 0 {
                                let arity = function_arity_value(interp, &args[0], env)?;
                                let minimum = arity.car()?.as_integer()?;
                                let maximum = arity.cdr()?;
                                let descriptor = match maximum {
                                    Value::Integer(maximum) => minimum + (maximum << 8),
                                    Value::Symbol(kind) if kind == "many" => {
                                        minimum + (minimum << 8) + 128
                                    }
                                    _ => {
                                        return Err(LispError::TypeError(
                                            "function".into(),
                                            args[0].type_name(),
                                        ));
                                    }
                                };
                                return Ok(Value::Integer(descriptor));
                            }
                            return record
                                .slots
                                .get(idx)
                                .cloned()
                                .ok_or_else(|| args_out_of_range(&args[0], &args[1]));
                        }
                        if idx == 0 {
                            Ok(Value::Symbol(record.type_name.clone()))
                        } else {
                            record
                                .slots
                                .get(idx - 1)
                                .cloned()
                                .ok_or_else(|| args_out_of_range(&args[0], &args[1]))
                        }
                    }
                    _ => {
                        if is_vector_value(&args[0]) {
                            vector_slot_value(&args[0], idx)
                        } else {
                            let items = vector_items(&args[0])?;
                            items
                                .get(idx)
                                .cloned()
                                .ok_or_else(|| args_out_of_range(&args[0], &args[1]))
                        }
                    }
                }
            }

            "aset" => {
                need_args(name, args, 3)?;
                let raw_idx = fixnum_index_arg(&args[1])?;
                if raw_idx < 0 {
                    return Err(args_out_of_range(&args[0], &args[1]));
                }
                let idx = raw_idx as usize;
                match &args[0] {
                    value if is_vector_value(value) => {
                        aset_vector_value(value, idx, args[2].clone())
                            .map_err(|_| args_out_of_range(&args[0], &args[1]))?;
                        Ok(args[2].clone())
                    }
                    Value::CharTable(id) => {
                        let key = raw_idx as u32;
                        interp.char_table_set(*id, key, args[2].clone())?;
                        Ok(args[2].clone())
                    }
                    value if is_bool_vector_value(interp, value) => {
                        set_bool_vector_bit(interp, value, idx, args[2].is_truthy())?;
                        Ok(args[2].clone())
                    }
                    Value::String(_) | Value::StringObject(_) => {
                        aset_string_value(&args[0], idx, &args[2])?;
                        Ok(args[2].clone())
                    }
                    Value::Record(id) => {
                        // GNU records are asettable; index 0 is the type tag
                        // (eieio's `make-instance' downgrades the class-object
                        // tag to the class symbol this way).
                        if idx == 0 {
                            let (type_name, tagged) = match &args[2] {
                                Value::Symbol(symbol) => (symbol.clone(), false),
                                Value::Record(class_id) => {
                                    let Some(class_name) =
                                        interp.find_class_state_name_by_record_id(*class_id)
                                    else {
                                        return Err(LispError::TypeError(
                                            "symbol".into(),
                                            args[2].type_name(),
                                        ));
                                    };
                                    (class_name, true)
                                }
                                _ => {
                                    return Err(LispError::TypeError(
                                        "symbol".into(),
                                        args[2].type_name(),
                                    ));
                                }
                            };
                            interp.retag_record(*id, &type_name, tagged)?;
                            return Ok(args[2].clone());
                        }
                        let record = interp
                            .find_record_mut(*id)
                            .ok_or_else(|| args_out_of_range(&args[0], &args[1]))?;
                        let Some(slot) = record.slots.get_mut(idx - 1) else {
                            return Err(args_out_of_range(&args[0], &args[1]));
                        };
                        *slot = args[2].clone();
                        Ok(args[2].clone())
                    }
                    _ => Err(LispError::TypeError("array".into(), args[0].type_name())),
                }
            }

            "seq-every-p" => {
                need_args(name, args, 2)?;
                let pred = args[0].clone();
                let seq = args[1].to_vec()?;
                for item in &seq {
                    let result = match &pred {
                        Value::BuiltinFunc(fname) => {
                            super::call(interp, fname, std::slice::from_ref(item), env)?
                        }
                        Value::Lambda(_, _, _) => {
                            call_function_value(interp, &pred, std::slice::from_ref(item), env)?
                        }
                        _ => return Err(LispError::TypeError("function".into(), pred.type_name())),
                    };
                    if result.is_nil() {
                        return Ok(Value::Nil);
                    }
                }
                Ok(Value::T)
            }

            "seq-into" => {
                need_args(name, args, 2)?;
                let items = sequence_values(interp, &args[0])?;
                match args[1].as_symbol()? {
                    "list" => Ok(Value::list(items)),
                    "vector" => {
                        let mut vector = vec![Value::symbol("vector-literal")];
                        vector.extend(items);
                        Ok(Value::list(vector))
                    }
                    "string" => {
                        let mut text = String::new();
                        for item in items {
                            let code = item.as_integer()?;
                            let ch = char::from_u32(code as u32).ok_or_else(|| {
                                LispError::Signal(format!("Invalid character: {code}"))
                            })?;
                            text.push(ch);
                        }
                        Ok(Value::String(text))
                    }
                    kind => Err(LispError::Signal(format!(
                        "seq-into unsupported target type: {kind}"
                    ))),
                }
            }

            "nreverse" => {
                need_args(name, args, 1)?;
                nreverse_sequence_value(interp, &args[0])
            }

            "copy-sequence" | "cl-copy-seq" => {
                need_args(name, args, 1)?;
                copy_sequence_value(interp, &args[0])
            }
            "fillarray" => {
                need_args(name, args, 2)?;
                match &args[0] {
                    value if is_vector_value(value) => {
                        let len = vector_items(value)?.len();
                        for index in 0..len {
                            aset_vector_value(value, index, args[1].clone())?;
                        }
                        Ok(args[0].clone())
                    }
                    Value::StringObject(state) => {
                        let mut state = state.borrow_mut();
                        let len = state.text.chars().count();
                        let fill_code = args[1].as_integer()?;
                        let fill_char = if state.multibyte {
                            char::from_u32(fill_code as u32)
                                .ok_or_else(|| LispError::Signal("Invalid character".into()))?
                        } else if !(0..=255).contains(&fill_code) {
                            return Err(LispError::Signal("Invalid character".into()));
                        } else if fill_code <= 0x7F {
                            char::from(fill_code as u8)
                        } else {
                            raw_byte_regex_char(fill_code as u8)
                        };
                        state.text = std::iter::repeat_n(fill_char, len).collect();
                        state.props.clear();
                        Ok(args[0].clone())
                    }
                    Value::String(text) => {
                        let len = text.chars().count();
                        let fill_code = args[1].as_integer()?;
                        if !(0..=0x7F).contains(&fill_code) {
                            return Err(LispError::Signal("Invalid character".into()));
                        }
                        let fill_char = char::from(fill_code as u8);
                        Ok(Value::String(std::iter::repeat_n(fill_char, len).collect()))
                    }
                    value if is_bool_vector_value(interp, value) => {
                        let len = bool_vector_bits(interp, value)?.len();
                        for index in 0..len {
                            set_bool_vector_bit(interp, value, index, args[1].is_truthy())?;
                        }
                        Ok(args[0].clone())
                    }
                    Value::CharTable(id) => {
                        let table = interp.find_char_table_mut(*id).ok_or_else(|| {
                            LispError::TypeError("char-table".into(), format!("char-table<{id}>"))
                        })?;
                        table.default = args[1].clone();
                        table.entries.clear();
                        Ok(args[0].clone())
                    }
                    other => Err(LispError::TypeError("array".into(), other.type_name())),
                }
            }
            "load-average" => {
                if args.len() > 1 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                Err(LispError::Signal("load-average not implemented".into()))
            }
            "locale-info" => {
                need_args(name, args, 1)?;
                let _item = args[0].as_symbol()?;
                Ok(Value::Nil)
            }
            "clear-string" => {
                need_args(name, args, 1)?;
                match &args[0] {
                    Value::StringObject(state) => {
                        let mut state = state.borrow_mut();
                        let len = state.text.len();
                        state.text = "\0".repeat(len);
                        state.props.clear();
                        state.multibyte = false;
                        Ok(Value::Nil)
                    }
                    Value::String(_) => Ok(Value::Nil),
                    other => Err(LispError::TypeError("string".into(), other.type_name())),
                }
            }

            "propertize" => {
                if args.is_empty() || args.len().is_multiple_of(2) {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let string = string_like(&args[0])
                    .ok_or_else(|| LispError::TypeError("string".into(), args[0].type_name()))?;
                let props = args[1..]
                    .chunks(2)
                    .map(|pair| Ok((pair[0].as_symbol()?.to_string(), pair[1].clone())))
                    .collect::<Result<Vec<_>, LispError>>()?;
                let len = string.text.chars().count();
                let value = make_shared_string_value_with_multibyte(
                    string.text,
                    string.props,
                    string.multibyte,
                );
                modify_shared_string_properties(&value, 0, len, |mut current| {
                    for (name, value) in &props {
                        if let Some((_, existing)) = current.iter_mut().find(|(key, _)| key == name)
                        {
                            *existing = value.clone();
                        } else {
                            current.push((name.clone(), value.clone()));
                        }
                    }
                    current
                })?;
                Ok(value)
            }

            "make-display-table" => {
                need_arg_range(name, args, 0, 0)?;
                Ok(interp.make_char_table(Some("display-table".into()), Value::Nil))
            }

            "make-char-table" => {
                need_args(name, args, 1)?;
                let subtype = match &args[0] {
                    Value::Nil => None,
                    Value::Symbol(symbol) => Some(symbol.clone()),
                    other => return Err(LispError::TypeError("symbol".into(), other.type_name())),
                };
                let default = args.get(1).cloned().unwrap_or(Value::Nil);
                Ok(interp.make_char_table(subtype, default))
            }

            "char-table-p" => {
                need_args(name, args, 1)?;
                Ok(if matches!(args[0], Value::CharTable(_)) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "case-table-p" => {
                need_args(name, args, 1)?;
                let Value::CharTable(id) = args[0] else {
                    return Ok(Value::Nil);
                };
                if interp.char_table_purpose(id) != Some("case-table") {
                    return Ok(Value::Nil);
                }
                let up = interp.char_table_extra_slot(id, 0).unwrap_or(Value::Nil);
                let canon = interp.char_table_extra_slot(id, 1).unwrap_or(Value::Nil);
                let equivalences = interp.char_table_extra_slot(id, 2).unwrap_or(Value::Nil);
                let valid = matches!(up, Value::Nil | Value::CharTable(_))
                    && ((canon.is_nil() && equivalences.is_nil())
                        || (matches!(canon, Value::CharTable(_))
                            && matches!(equivalences, Value::Nil | Value::CharTable(_))));
                Ok(if valid { Value::T } else { Value::Nil })
            }
            "syntax-table-p" => {
                need_args(name, args, 1)?;
                let valid = matches!(
                    args[0],
                    Value::CharTable(id) if interp.char_table_purpose(id) == Some("syntax-table")
                );
                Ok(if valid { Value::T } else { Value::Nil })
            }

            "char-table-subtype" => {
                need_args(name, args, 1)?;
                let Value::CharTable(id) = args[0] else {
                    return Err(LispError::TypeError(
                        "char-table".into(),
                        args[0].type_name(),
                    ));
                };
                Ok(interp
                    .char_table_subtype(id)
                    .flatten()
                    .map(Value::Symbol)
                    .unwrap_or(Value::Nil))
            }

            "char-table-parent" => {
                need_args(name, args, 1)?;
                let Value::CharTable(id) = args[0] else {
                    return Err(LispError::TypeError(
                        "char-table".into(),
                        args[0].type_name(),
                    ));
                };
                Ok(interp
                    .char_table_parent(id)
                    .flatten()
                    .and_then(|parent_id| {
                        interp
                            .find_char_table(parent_id)
                            .map(|_| Value::CharTable(parent_id))
                    })
                    .unwrap_or(Value::Nil))
            }

            "set-char-table-parent" => {
                need_args(name, args, 2)?;
                let Value::CharTable(id) = args[0] else {
                    return Err(LispError::TypeError(
                        "char-table".into(),
                        args[0].type_name(),
                    ));
                };
                let parent = match &args[1] {
                    Value::Nil => None,
                    Value::CharTable(parent_id) => Some(*parent_id),
                    other => {
                        return Err(LispError::TypeError("char-table".into(), other.type_name()));
                    }
                };
                interp.set_char_table_parent(id, parent)?;
                Ok(args[1].clone())
            }

            "char-table-extra-slot" => {
                need_args(name, args, 2)?;
                let Value::CharTable(id) = args[0] else {
                    return Err(LispError::TypeError(
                        "char-table".into(),
                        args[0].type_name(),
                    ));
                };
                let slot = args[1].as_integer()?.max(0) as usize;
                Ok(interp.char_table_extra_slot(id, slot).unwrap_or(Value::Nil))
            }

            "set-char-table-extra-slot" => {
                need_args(name, args, 3)?;
                let Value::CharTable(id) = args[0] else {
                    return Err(LispError::TypeError(
                        "char-table".into(),
                        args[0].type_name(),
                    ));
                };
                let slot = args[1].as_integer()?.max(0) as usize;
                interp.set_char_table_extra_slot(id, slot, args[2].clone())?;
                Ok(args[2].clone())
            }

            "char-table-range" => {
                need_args(name, args, 2)?;
                let Value::CharTable(id) = args[0] else {
                    return Err(LispError::TypeError(
                        "char-table".into(),
                        args[0].type_name(),
                    ));
                };
                match char_table_range_spec(&args[1])? {
                    None => Ok(interp
                        .find_char_table(id)
                        .map(|table| table.default.clone())
                        .unwrap_or(Value::Nil)),
                    Some((start, end)) if start == end => Ok(syntax::char_table_public_value(
                        interp,
                        id,
                        interp.char_table_get(id, start).unwrap_or(Value::Nil),
                    )),
                    Some((start, end)) => Ok(syntax::char_table_public_value(
                        interp,
                        id,
                        interp
                            .char_table_range(id, start, end)
                            .unwrap_or(Value::Nil),
                    )),
                }
            }

            "set-char-table-range" => {
                need_args(name, args, 3)?;
                let Value::CharTable(id) = args[0] else {
                    return Err(LispError::TypeError(
                        "char-table".into(),
                        args[0].type_name(),
                    ));
                };
                match char_table_range_spec(&args[1])? {
                    None => interp.char_table_set_default(id, args[2].clone())?,
                    Some((start, end)) => {
                        interp.char_table_set_range(id, start, end, args[2].clone())?
                    }
                }
                Ok(args[2].clone())
            }
            "optimize-char-table" => {
                need_arg_range(name, args, 1, 2)?;
                if !matches!(args[0], Value::CharTable(_)) {
                    return Err(LispError::TypeError(
                        "char-table".into(),
                        args[0].type_name(),
                    ));
                }
                // Emaxx stores ranges directly instead of allocating GNU's
                // nested sub-char-tables, so there is no structural compaction
                // to perform at this abstraction boundary.
                Ok(Value::Nil)
            }

            "map-char-table" => {
                need_args(name, args, 2)?;
                let Value::CharTable(id) = args[1] else {
                    return Err(LispError::TypeError(
                        "char-table".into(),
                        args[1].type_name(),
                    ));
                };
                let effective = interp.char_table_effective_ranges(id).ok_or_else(|| {
                    LispError::TypeError("char-table".into(), args[1].type_name())
                })?;
                for entry in effective {
                    let key = if entry.start == entry.end {
                        Value::Integer(entry.start as i64)
                    } else {
                        Value::cons(
                            Value::Integer(entry.start as i64),
                            Value::Integer(entry.end as i64),
                        )
                    };
                    let value = syntax::char_table_public_value(interp, id, entry.value);
                    call_function_value(interp, &args[0], &[key, value], env)?;
                }
                Ok(Value::Nil)
            }

            "current-case-table" => Ok(Value::CharTable(interp.current_case_table_id())),

            "standard-case-table" => Ok(Value::CharTable(interp.standard_case_table_id())),

            "set-case-table" => {
                need_args(name, args, 1)?;
                let Value::CharTable(id) = args[0] else {
                    return Err(LispError::TypeError(
                        "char-table".into(),
                        args[0].type_name(),
                    ));
                };
                interp.set_current_case_table(id);
                Ok(args[0].clone())
            }

            "set-standard-case-table" => {
                need_args(name, args, 1)?;
                let Value::CharTable(id) = args[0] else {
                    return Err(LispError::TypeError(
                        "char-table".into(),
                        args[0].type_name(),
                    ));
                };
                interp.set_standard_case_table(id);
                Ok(args[0].clone())
            }

            "make-syntax-table" => {
                if args.len() > 1 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let parent = match args.first() {
                    Some(Value::CharTable(id)) => Some(*id),
                    Some(Value::Nil) | None => Some(interp.standard_syntax_table_id()),
                    Some(other) => {
                        return Err(LispError::TypeError("char-table".into(), other.type_name()));
                    }
                };
                let table = interp.make_char_table(Some("syntax-table".into()), Value::Nil);
                let Value::CharTable(id) = table else {
                    unreachable!("make_char_table returns a char-table");
                };
                interp.set_char_table_parent(id, parent)?;
                Ok(Value::CharTable(id))
            }

            "copy-syntax-table" => {
                if args.len() > 1 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let source = match args.first() {
                    Some(Value::CharTable(id)) => *id,
                    Some(Value::Nil) | None => interp.current_syntax_table_id(),
                    Some(other) => {
                        return Err(LispError::TypeError("char-table".into(), other.type_name()));
                    }
                };
                interp.clone_char_table(source)
            }

            "syntax-table" => {
                need_args(name, args, 0)?;
                Ok(Value::CharTable(interp.current_syntax_table_id()))
            }

            "standard-syntax-table" => Ok(Value::CharTable(interp.standard_syntax_table_id())),

            "set-syntax-table" => {
                need_args(name, args, 1)?;
                let Value::CharTable(id) = args[0] else {
                    return Err(LispError::TypeError(
                        "char-table".into(),
                        args[0].type_name(),
                    ));
                };
                interp.set_current_syntax_table(id);
                Ok(args[0].clone())
            }

            "modify-syntax-entry" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let (start, end) = match &args[0] {
                    Value::Cons(_) => {
                        let start = u32::try_from(args[0].car()?.as_integer()?)
                            .map_err(|_| LispError::Signal("Invalid character".into()))?;
                        let end = u32::try_from(args[0].cdr()?.as_integer()?)
                            .map_err(|_| LispError::Signal("Invalid character".into()))?;
                        (start, end)
                    }
                    _ => {
                        let code = u32::try_from(args[0].as_integer()?)
                            .map_err(|_| LispError::Signal("Invalid character".into()))?;
                        (code, code)
                    }
                };
                let syntax = string_text(&args[1])?;
                if syntax::parse_syntax_spec(&syntax).is_none() {
                    let letter = syntax.chars().next().unwrap_or('\0');
                    return Err(LispError::Signal(format!(
                        "Invalid syntax description letter: {letter}"
                    )));
                }
                let table_id = match args.get(2) {
                    Some(Value::CharTable(id)) => *id,
                    Some(other) => {
                        return Err(LispError::TypeError("char-table".into(), other.type_name()));
                    }
                    None => interp.current_syntax_table_id(),
                };
                interp.char_table_set_range(table_id, start, end, Value::String(syntax.clone()))?;
                if table_id == interp.standard_syntax_table_id()
                    || table_id == interp.current_syntax_table_id()
                {
                    for code in start.min(end)..=start.max(end) {
                        interp.set_syntax_word_char(
                            normalize_case_key(code),
                            syntax.starts_with('w'),
                        );
                    }
                }
                Ok(Value::Nil)
            }

            "setcar" => {
                need_args(name, args, 2)?;
                let Value::Cons(_) = &args[0] else {
                    return Err(wrong_type_argument("consp", args[0].clone()));
                };
                args[0].set_car(args[1].clone())?;
                // A cons may be the live plist cell of a symbol.  Conservatively
                // invalidate macro metadata caches for arbitrary cons mutation;
                // GNU exposes no detached copy at `symbol-plist'.
                interp.note_definition_changed();
                Ok(args[1].clone())
            }

            "setcdr" => {
                need_args(name, args, 2)?;
                let Value::Cons(_) = &args[0] else {
                    return Err(wrong_type_argument("consp", args[0].clone()));
                };
                args[0].set_cdr(args[1].clone())?;
                interp.note_definition_changed();
                Ok(args[1].clone())
            }

            "emaxx-default-region-extract-function" => {
                need_args(name, args, 1)?;
                let (start, end) = interp.buffer.region().ok_or_else(|| {
                    LispError::Signal("The mark is not set now, so there is no region".into())
                })?;
                let start = Value::Integer(start as i64);
                let end = Value::Integer(end as i64);
                match &args[0] {
                    Value::Symbol(method) if method == "bounds" => {
                        Ok(Value::list([Value::cons(start, end)]))
                    }
                    Value::Symbol(method) if method == "delete-only" => {
                        super::call(interp, "delete-region", &[start, end], env)
                    }
                    method => super::call(
                        interp,
                        "filter-buffer-substring",
                        &[start, end, method.clone()],
                        env,
                    ),
                }
            }

            "make-category-table" => {
                Ok(interp
                    .make_char_table(Some("category-table".into()), Value::String(String::new())))
            }

            "category-table-p" => {
                need_args(name, args, 1)?;
                Ok(match &args[0] {
                    Value::CharTable(id)
                        if interp.char_table_subtype(*id).flatten().as_deref()
                            == Some("category-table") =>
                    {
                        Value::T
                    }
                    _ => Value::Nil,
                })
            }

            "standard-category-table" => {
                Ok(Value::CharTable(interp.ensure_standard_category_table()))
            }

            "category-table" => Ok(Value::CharTable(current_category_table_id(interp))),

            "set-category-table" => {
                need_args(name, args, 1)?;
                let table = Value::CharTable(category_table_arg(interp, args.first(), false)?);
                interp.set_buffer_local_value(
                    interp.current_buffer_id(),
                    "category-table",
                    table.clone(),
                );
                Ok(table)
            }

            "define-category" => {
                need_arg_range(name, args, 2, 3)?;
                let category = args[0].as_integer()?;
                let doc = string_text(&args[1])?;
                let table = category_table_arg(interp, args.get(2), false)?;
                interp.define_category(table, category as u32, doc)?;
                Ok(Value::Nil)
            }

            "category-docstring" => {
                need_arg_range(name, args, 1, 2)?;
                let category = args[0].as_integer()? as u32;
                let table = category_table_arg(interp, args.get(1), false)?;
                Ok(interp
                    .category_docstring(table, category)
                    .map(Value::String)
                    .unwrap_or(Value::Nil))
            }

            "get-unused-category" => {
                need_arg_range(name, args, 0, 1)?;
                let table = category_table_arg(interp, args.first(), false)?;
                Ok((b' '..=b'~')
                    .find(|category| {
                        interp
                            .category_docstring(table, u32::from(*category))
                            .is_none()
                    })
                    .map(|category| Value::Integer(i64::from(category)))
                    .unwrap_or(Value::Nil))
            }

            "make-category-set" => {
                need_args(name, args, 1)?;
                let text = string_text(&args[0])?;
                Ok(Value::String(normalize_category_set(&text)))
            }

            "category-set-mnemonics" => {
                need_args(name, args, 1)?;
                let text = string_text(&args[0])?;
                Ok(Value::String(normalize_category_set(&text)))
            }

            "modify-category-entry" => {
                need_arg_range(name, args, 2, 4)?;
                let (start, end) = category_character_range(&args[0])?;
                let category = args[1].as_integer()? as u32;
                if !(32..=126).contains(&category) {
                    return Err(LispError::Signal("Invalid category".into()));
                }
                let table = category_table_arg(interp, args.get(2), false)?;
                let reset = args.get(3).is_some_and(Value::is_truthy);
                if start > end {
                    return Ok(Value::Nil);
                }
                let category_char = char::from_u32(category).expect("ASCII category is a scalar");
                let boundaries = char_table_change_boundaries(interp, table, start, end);
                for (index, segment_start) in boundaries.iter().copied().enumerate() {
                    let segment_end = boundaries
                        .get(index + 1)
                        .map_or(end, |next| next.saturating_sub(1));
                    let current = interp
                        .char_table_get(table, segment_start)
                        .and_then(|value| string_like(&value).map(|s| s.text))
                        .unwrap_or_default();
                    let mut chars: Vec<char> = current.chars().collect();
                    if reset {
                        chars.retain(|existing| *existing != category_char);
                    } else if !chars.contains(&category_char) {
                        chars.push(category_char);
                    }
                    chars.sort_unstable();
                    chars.dedup();
                    interp.char_table_set_range(
                        table,
                        segment_start,
                        segment_end,
                        Value::String(chars.into_iter().collect()),
                    )?;
                }
                Ok(Value::Nil)
            }

            "char-category-set" => {
                need_args(name, args, 1)?;
                let character = args[0].as_integer()? as u32;
                let table_id = current_category_table_id(interp);
                // GNU returns a 128-slot bool-vector category set; entries
                // may be stored as category-character strings.
                let entry = interp.char_table_get(table_id, character);
                let mut slots = vec![Value::Nil; 128];
                if let Some(value) = entry {
                    match &value {
                        Value::String(text) => {
                            for ch in text.chars() {
                                if (ch as usize) < 128 {
                                    slots[ch as usize] = Value::T;
                                }
                            }
                        }
                        Value::Record(_) => return Ok(value),
                        _ => {}
                    }
                }
                Ok(interp.create_record("bool-vector", slots))
            }

            "copy-category-table" => {
                need_arg_range(name, args, 0, 1)?;
                let id = category_table_arg(interp, args.first(), true)?;
                interp.clone_char_table(id)
            }

            "translate-region" => {
                need_args(name, args, 3)?;
                let from = position_from_value(interp, &args[0])?;
                let to = position_from_value(interp, &args[1])?;
                let table = translation_table_from_value(interp, &args[2])?;
                translate_region_with_table(interp, from, to, &table)
            }

            "translate-region-internal" => {
                need_args(name, args, 3)?;
                let from = position_from_value(interp, &args[0])?;
                let to = position_from_value(interp, &args[1])?;
                let table_id = match &args[2] {
                    Value::CharTable(id) => *id,
                    _ => {
                        return Err(LispError::TypeError(
                            "char-table".into(),
                            args[2].type_name(),
                        ));
                    }
                };
                if interp.char_table_purpose(table_id) != Some("translation-table") {
                    return Err(LispError::Signal("Not a translation table".into()));
                }
                let table = TranslationTable::CharTable(table_id);
                translate_region_with_table(interp, from, to, &table)
            }

            #[dispatch(resets_undo)]
            "undo-boundary" => {
                interp.buffer.push_undo_boundary();
                Ok(Value::Nil)
            }

            "undo" => {
                interp.undo_current_buffer()?;
                Ok(Value::Nil)
            }

            "undo-more" => {
                let count = if args.is_empty() {
                    1
                } else {
                    match &args[0] {
                        Value::Nil => {
                            return Err(LispError::TypeError(
                                "number-or-marker-p".into(),
                                "nil".into(),
                            ));
                        }
                        value => value.as_integer()?,
                    }
                };
                for _ in 0..count.max(0) {
                    interp.undo_more_current_buffer()?;
                }
                Ok(Value::Nil)
            }

            "take" | "ntake" => {
                need_args(name, args, 2)?;
                let n = args[0].as_integer()?;
                if n <= 0 {
                    return Ok(Value::Nil);
                }
                let n = n as usize;
                if name == "take" {
                    let mut current = args[1].clone();
                    let mut items = Vec::new();
                    let mut remaining = n;
                    while remaining > 0 {
                        match current {
                            Value::Nil => break,
                            Value::Cons(cons_cell) => {
                                let car = &cons_cell.car;
                                let cdr = &cons_cell.cdr;
                                items.push(car.borrow().clone());
                                current = cdr.borrow().clone();
                                remaining -= 1;
                            }
                            value => {
                                return Err(LispError::TypeError("list".into(), value.type_name()));
                            }
                        }
                    }
                    Ok(Value::list(items))
                } else {
                    let head = args[1].clone();
                    let mut current = head.clone();
                    let mut remaining = n;
                    while remaining > 1 {
                        match current {
                            Value::Nil => return Ok(Value::Nil),
                            Value::Cons(cons_cell) => {
                                let _ = &cons_cell.car;
                                let cdr = &cons_cell.cdr;
                                let next = cdr.borrow().clone();
                                match next {
                                    Value::Cons(_) => {
                                        current = next;
                                        remaining -= 1;
                                    }
                                    Value::Nil => return Ok(head),
                                    value => {
                                        return Err(LispError::TypeError(
                                            "list".into(),
                                            value.type_name(),
                                        ));
                                    }
                                }
                            }
                            value => {
                                return Err(LispError::TypeError("list".into(), value.type_name()));
                            }
                        }
                    }
                    match current {
                        Value::Nil => Ok(Value::Nil),
                        Value::Cons(cons_cell) => {
                            let _ = &cons_cell.car;
                            let cdr = &cons_cell.cdr;
                            *cdr.borrow_mut() = Value::Nil;
                            Ok(head)
                        }
                        value => Err(LispError::TypeError("list".into(), value.type_name())),
                    }
                }
            }

            "delq" => {
                need_args(name, args, 2)?;
                let elt = &args[0];
                let mut head = args[1].clone();
                while let Value::Cons(cell) = head.clone() {
                    if values_eq_in_env(interp, &cell.car.borrow(), elt, env) {
                        head = cell.cdr.borrow().clone();
                    } else {
                        break;
                    }
                }
                let mut current = head.clone();
                while let Value::Cons(cell) = current.clone() {
                    let next = cell.cdr.borrow().clone();
                    match next {
                        Value::Cons(cons_cell) => {
                            let next_car = &cons_cell.car;
                            let next_cdr = &cons_cell.cdr;
                            if values_eq_in_env(interp, &next_car.borrow(), elt, env) {
                                *cell.cdr.borrow_mut() = next_cdr.borrow().clone();
                            } else {
                                current = Value::Cons(cons_cell);
                            }
                        }
                        _ => break,
                    }
                }
                Ok(head)
            }

            "delete" | "remq" => {
                need_args(name, args, 2)?;
                let elt = &args[0];
                let items = args[1].to_vec()?;
                let filtered: Vec<Value> = items
                    .into_iter()
                    .filter(|item| {
                        if name == "delete" {
                            !values_equal(interp, item, elt)
                        } else {
                            !values_eq_in_env(interp, item, elt, env)
                        }
                    })
                    .collect();
                Ok(Value::list(filtered))
            }

            "make-list" => {
                need_args(name, args, 2)?;
                let n = args[0].as_integer()?;
                let val = args[1].clone();
                let items: Vec<Value> = (0..n).map(|_| val.clone()).collect();
                Ok(Value::list(items))
            }
        }
    }
);
