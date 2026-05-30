use super::*;

pub(super) fn handles(name: &str) -> bool {
    matches!(
        name,
        "plist-get"
            | "plist-put"
            | "plist-member"
            | "seq-uniq"
            | "sort"
            | "cl-sort"
            | "random"
            | "vector"
            | "bool-vector-count-population"
            | "bool-vector-count-consecutive"
            | "bool-vector-intersection"
            | "bool-vector-union"
            | "bool-vector-exclusive-or"
            | "bool-vector-set-difference"
            | "bool-vector-not"
            | "aref"
            | "aset"
            | "seq-every-p"
            | "seq-into"
            | "nreverse"
            | "copy-sequence"
            | "fillarray"
            | "load-average"
            | "locale-info"
            | "clear-string"
            | "propertize"
            | "make-display-table"
            | "make-char-table"
            | "char-table-p"
            | "char-table-subtype"
            | "char-table-parent"
            | "set-char-table-parent"
            | "char-table-extra-slot"
            | "set-char-table-extra-slot"
            | "char-table-range"
            | "set-char-table-range"
            | "map-char-table"
            | "current-case-table"
            | "standard-case-table"
            | "set-case-table"
            | "set-standard-case-table"
            | "make-syntax-table"
            | "copy-syntax-table"
            | "syntax-table"
            | "standard-syntax-table"
            | "set-syntax-table"
            | "modify-syntax-entry"
            | "setcdr"
            | "emaxx-default-region-extract-function"
            | "make-category-table"
            | "category-table-p"
            | "standard-category-table"
            | "category-table"
            | "set-category-table"
            | "define-category"
            | "category-docstring"
            | "make-category-set"
            | "category-set-mnemonics"
            | "modify-category-entry"
            | "char-category-set"
            | "copy-category-table"
            | "translate-region"
            | "translate-region-internal"
            | "undo-boundary"
            | "undo"
            | "undo-more"
            | "take"
            | "ntake"
            | "delete"
            | "delq"
            | "remq"
            | "make-list"
    )
}

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
            let mut seen = HashSet::new();
            loop {
                match current {
                    Value::Nil => return Ok(Value::Nil),
                    Value::Cons(car, cdr) => {
                        let cell_id = Rc::as_ptr(&car) as usize;
                        if !seen.insert(cell_id) {
                            return Ok(Value::Nil);
                        }
                        let property = car.borrow().clone();
                        if value_matches_with_test(interp, &property, key, testfn, env)? {
                            return match cdr.borrow().clone() {
                                Value::Cons(value, _) => Ok(value.borrow().clone()),
                                _ => Ok(Value::Nil),
                            };
                        }
                        match cdr.borrow().clone() {
                            Value::Cons(_, next_cdr) => current = next_cdr.borrow().clone(),
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
            let mut seen = HashSet::new();
            loop {
                match current {
                    Value::Nil => {
                        let mut items = plist.to_vec()?;
                        items.push(key.clone());
                        items.push(val.clone());
                        return Ok(Value::list(items));
                    }
                    Value::Cons(car, cdr) => {
                        let cell_id = Rc::as_ptr(&car) as usize;
                        if !seen.insert(cell_id) {
                            return Err(LispError::SignalValue(Value::list([
                                Value::Symbol("circular-list".into()),
                                Value::String("Circular list".into()),
                            ])));
                        }
                        let property = car.borrow().clone();
                        if value_matches_with_test(interp, &property, key, testfn, env)? {
                            return match cdr.borrow().clone() {
                                Value::Cons(value, _) => {
                                    *value.borrow_mut() = val.clone();
                                    Ok(plist)
                                }
                                _ => Err(plist_type_error(&plist)),
                            };
                        }
                        match cdr.borrow().clone() {
                            Value::Cons(_, next_cdr) => {
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
            let mut seen = HashSet::new();
            loop {
                match current {
                    Value::Nil => return Ok(Value::Nil),
                    Value::Cons(car, cdr) => {
                        let cell_id = Rc::as_ptr(&car) as usize;
                        if !seen.insert(cell_id) {
                            return Err(LispError::SignalValue(Value::list([
                                Value::Symbol("circular-list".into()),
                                Value::String("Circular list".into()),
                            ])));
                        }
                        let property = car.borrow().clone();
                        if value_matches_with_test(interp, &property, key, testfn, env)? {
                            return Ok(Value::Cons(car, cdr));
                        }
                        // Skip the value
                        match cdr.borrow().clone() {
                            Value::Cons(_, next_cdr) => current = next_cdr.borrow().clone(),
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
            let idx = args[1].as_integer()? as usize;
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
                        None => Err(LispError::SignalValue(Value::list([
                            Value::Symbol("args-out-of-range".into()),
                            args[0].clone(),
                            args[1].clone(),
                        ]))),
                    }
                }
                Value::CharTable(id) => {
                    let key = args[1].as_integer()? as u32;
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
                        return record.slots.get(idx).cloned().ok_or_else(|| {
                            LispError::SignalValue(Value::list([
                                Value::Symbol("args-out-of-range".into()),
                                args[0].clone(),
                                args[1].clone(),
                            ]))
                        });
                    }
                    if record.type_name == "byte-code-function" {
                        return record.slots.get(idx).cloned().ok_or_else(|| {
                            LispError::SignalValue(Value::list([
                                Value::Symbol("args-out-of-range".into()),
                                args[0].clone(),
                                args[1].clone(),
                            ]))
                        });
                    }
                    if idx == 0 {
                        Ok(Value::Symbol(record.type_name.clone()))
                    } else {
                        record.slots.get(idx - 1).cloned().ok_or_else(|| {
                            LispError::SignalValue(Value::list([
                                Value::Symbol("args-out-of-range".into()),
                                args[0].clone(),
                                args[1].clone(),
                            ]))
                        })
                    }
                }
                _ => {
                    if is_vector_value(&args[0]) {
                        vector_slot_value(&args[0], idx)
                    } else {
                        let items = vector_items(&args[0])?;
                        items.get(idx).cloned().ok_or_else(|| {
                            LispError::SignalValue(Value::list([
                                Value::Symbol("args-out-of-range".into()),
                                args[0].clone(),
                                args[1].clone(),
                            ]))
                        })
                    }
                }
            }
        }

        "aset" => {
            need_args(name, args, 3)?;
            match &args[0] {
                value if is_vector_value(value) => {
                    aset_vector_value(value, args[1].as_integer()? as usize, args[2].clone())?;
                    Ok(args[2].clone())
                }
                Value::CharTable(id) => {
                    let key = args[1].as_integer()? as u32;
                    interp.char_table_set(*id, key, args[2].clone())?;
                    Ok(args[2].clone())
                }
                value if is_bool_vector_value(interp, value) => {
                    set_bool_vector_bit(
                        interp,
                        value,
                        args[1].as_integer()? as usize,
                        args[2].is_truthy(),
                    )?;
                    Ok(args[2].clone())
                }
                Value::String(_) | Value::StringObject(_) => {
                    aset_string_value(&args[0], args[1].as_integer()? as usize, &args[2])?;
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

        "copy-sequence" => {
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
                    if let Some((_, existing)) = current.iter_mut().find(|(key, _)| key == name) {
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

        "map-char-table" => {
            need_args(name, args, 2)?;
            let Value::CharTable(id) = args[1] else {
                return Err(LispError::TypeError(
                    "char-table".into(),
                    args[1].type_name(),
                ));
            };
            let entries = interp
                .find_char_table(id)
                .ok_or_else(|| LispError::TypeError("char-table".into(), args[1].type_name()))?
                .entries
                .clone();
            for entry in entries {
                if entry.value.is_nil() {
                    continue;
                }
                let key = if entry.start == entry.end {
                    Value::Integer(entry.start as i64)
                } else {
                    Value::cons(
                        Value::Integer(entry.start as i64),
                        Value::Integer(entry.end as i64),
                    )
                };
                call_function_value(interp, &args[0], &[key, entry.value], env)?;
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
                Value::Cons(_, _) => {
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
                    interp.set_syntax_word_char(normalize_case_key(code), syntax.starts_with('w'));
                }
            }
            Ok(Value::Nil)
        }

        "setcdr" => {
            need_args(name, args, 2)?;
            args[0].set_cdr(args[1].clone())?;
            Ok(args[1].clone())
        }

        "emaxx-default-region-extract-function" => {
            need_args(name, args, 1)?;
            match &args[0] {
                Value::Symbol(method) if method == "bounds" => {
                    let (start, end) = interp
                        .buffer
                        .region()
                        .unwrap_or((interp.buffer.point(), interp.buffer.point()));
                    Ok(Value::list([Value::cons(
                        Value::Integer(start as i64),
                        Value::Integer(end as i64),
                    )]))
                }
                _ => Ok(Value::String(String::new())),
            }
        }

        "make-category-table" => {
            Ok(interp.make_char_table(Some("category-table".into()), Value::String(String::new())))
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

        "standard-category-table" => Ok(Value::CharTable(interp.ensure_standard_category_table())),

        "category-table" => {
            let table = interp
                .buffer_local_value(interp.current_buffer_id(), "category-table")
                .and_then(|value| match value {
                    Value::CharTable(id) => Some(Value::CharTable(id)),
                    _ => None,
                })
                .unwrap_or_else(|| Value::CharTable(interp.ensure_standard_category_table()));
            Ok(table)
        }

        "set-category-table" => {
            need_args(name, args, 1)?;
            let table = match &args[0] {
                Value::CharTable(id) => Value::CharTable(*id),
                other => return Err(LispError::TypeError("char-table".into(), other.type_name())),
            };
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                "category-table",
                table.clone(),
            );
            Ok(table)
        }

        "define-category" => {
            need_args(name, args, 3)?;
            let category = args[0].as_integer()?;
            let doc = string_text(&args[1])?;
            let table = match args.get(2) {
                Some(Value::CharTable(id)) => *id,
                Some(Value::Nil) | None => interp.ensure_standard_category_table(),
                Some(other) => {
                    return Err(LispError::TypeError("char-table".into(), other.type_name()));
                }
            };
            interp.define_category(table, category as u32, doc)?;
            Ok(Value::Nil)
        }

        "category-docstring" => {
            need_args(name, args, 2)?;
            let category = args[0].as_integer()? as u32;
            let table = match &args[1] {
                Value::CharTable(id) => *id,
                other => return Err(LispError::TypeError("char-table".into(), other.type_name())),
            };
            Ok(interp
                .category_docstring(table, category)
                .map(Value::String)
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
            need_args(name, args, 3)?;
            let character = args[0].as_integer()? as u32;
            let category = args[1].as_integer()? as u32;
            let table = match &args[2] {
                Value::CharTable(id) => *id,
                other => return Err(LispError::TypeError("char-table".into(), other.type_name())),
            };
            let reset = args.get(3).is_some_and(Value::is_truthy);
            let category_char = char::from_u32(category)
                .ok_or_else(|| LispError::Signal("Invalid character".into()))?;
            let current = interp
                .char_table_get(table, character)
                .and_then(|value| string_like(&value).map(|s| s.text))
                .unwrap_or_default();
            let mut chars: Vec<char> = current.chars().collect();
            if reset {
                chars.retain(|existing| *existing != category_char);
            } else if !chars.contains(&category_char) {
                chars.push(category_char);
            }
            chars.sort_unstable();
            let updated = chars.into_iter().collect::<String>();
            interp.char_table_set(table, character, Value::String(updated))?;
            Ok(Value::Nil)
        }

        "char-category-set" => {
            need_args(name, args, 1)?;
            let character = args[0].as_integer()? as u32;
            let table_id = interp
                .buffer_local_value(interp.current_buffer_id(), "category-table")
                .and_then(|value| match value {
                    Value::CharTable(id) => Some(id),
                    _ => None,
                })
                .unwrap_or_else(|| interp.ensure_standard_category_table());
            Ok(interp
                .char_table_get(table_id, character)
                .unwrap_or_else(|| Value::String(String::new())))
        }

        "copy-category-table" => {
            need_args(name, args, 1)?;
            let Value::CharTable(id) = args[0] else {
                return Err(LispError::TypeError(
                    "char-table".into(),
                    args[0].type_name(),
                ));
            };
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
                        Value::Cons(car, cdr) => {
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
                        Value::Cons(_, cdr) => {
                            let next = cdr.borrow().clone();
                            match next {
                                Value::Cons(_, _) => {
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
                    Value::Cons(_, cdr) => {
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
            while let Value::Cons(car, cdr) = head.clone() {
                if values_eq_in_env(interp, &car.borrow(), elt, env) {
                    head = cdr.borrow().clone();
                } else {
                    break;
                }
            }
            let mut current = head.clone();
            while let Value::Cons(_, cdr) = current.clone() {
                let next = cdr.borrow().clone();
                match next {
                    Value::Cons(next_car, next_cdr) => {
                        if values_eq_in_env(interp, &next_car.borrow(), elt, env) {
                            *cdr.borrow_mut() = next_cdr.borrow().clone();
                        } else {
                            current = Value::Cons(next_car, next_cdr);
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

        _ => unreachable!("dispatch chunk called for unsupported primitive"),
    }
}
