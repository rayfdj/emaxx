use super::*;

pub(super) fn handles(name: &str) -> bool {
    matches!(
        name,
        "cons"
            | "car"
            | "cl-first"
            | "cl-second"
            | "cl-third"
            | "cdr"
            | "car-safe"
            | "cdr-safe"
            | "identity"
            | "list"
            | "nconc"
            | "append"
            | "nth"
            | "elt"
            | "nthcdr"
            | "last"
            | "butlast"
            | "length"
            | "safe-length"
            | "length<"
            | "length>"
            | "length="
            | "reverse"
            | "copy-tree"
            | "flatten-tree"
            | "flatten-list"
            | "copy-alist"
            | "delete-dups"
            | "remove"
            | "memq"
            | "memql"
            | "member"
            | "member-ignore-case"
            | "assq"
            | "rassq"
            | "rassoc"
            | "rassq-delete-all"
            | "assq-delete-all"
            | "assoc-delete-all"
            | "assoc"
            | "assoc-string"
            | "alist-get"
            | "cl-set-exclusive-or"
            | "cl-remove-if-not"
            | "cl-delete-if"
            | "mapcar"
            | "mapcan"
            | "cl-mapcar"
            | "cl-mapcan"
            | "cl-some"
            | "seq-mapcat"
            | "mapc"
            | "cl-reduce"
            | "eval"
            | "eval-buffer"
            | "mapconcat"
            | "string-join"
            | "ensure-list"
            | "position-symbol"
            | "symbol-with-pos-pos"
            | "remove-pos-from-symbol"
            | "bare-symbol"
            | "seq-find"
            | "seq-contains-p"
            | "seq-take"
            | "seq-position"
            | "cl-coerce"
            | "treesit-language-available-p"
            | "treesit--linecol-cache"
            | "treesit--linecol-cache-set"
            | "treesit--linecol-at"
            | "apply"
            | "apply-partially"
            | "funcall"
            | "fset"
            | "fmakunbound"
            | "funcall-interactively"
            | "call-interactively"
            | "keyboard-quit"
            | "start-kbd-macro"
            | "end-kbd-macro"
            | "define-keymap"
            | "define-abbrev-table"
            | "read-key"
            | "read-key-sequence"
            | "read-event"
            | "read-char"
            | "read-char-exclusive"
            | "mouse-double-click-time"
            | "context-menu-map"
            | "read-string"
            | "read-from-minibuffer"
            | "read-no-blanks-input"
            | "completing-read"
            | "format-prompt"
    )
}

pub(super) fn call(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    match name {
        // ── List operations ──
        "cons" => {
            need_args(name, args, 2)?;
            Ok(Value::cons(args[0].clone(), args[1].clone()))
        }
        "car" => {
            need_args(name, args, 1)?;
            if let Some(items) = keymap_list_items(interp, &args[0])? {
                Ok(items.into_iter().next().unwrap_or(Value::Nil))
            } else {
                args[0].car()
            }
        }
        "cl-first" | "cl-second" | "cl-third" => {
            need_args(name, args, 1)?;
            let index = match name {
                "cl-first" => 0,
                "cl-second" => 1,
                _ => 2,
            };
            let mut tail = args[0].clone();
            for _ in 0..index {
                tail = tail.cdr()?;
            }
            tail.car()
        }
        "cdr" => {
            need_args(name, args, 1)?;
            if let Some(items) = keymap_list_items(interp, &args[0])? {
                Ok(Value::list(items.into_iter().skip(1)))
            } else {
                args[0].cdr()
            }
        }
        "car-safe" => {
            need_args(name, args, 1)?;
            Ok(match &args[0] {
                Value::Cons(car, _) => car.borrow().clone(),
                value => keymap_list_items(interp, value)?
                    .and_then(|items| items.into_iter().next())
                    .unwrap_or(Value::Nil),
            })
        }
        "cdr-safe" => {
            need_args(name, args, 1)?;
            Ok(match &args[0] {
                Value::Cons(_, cdr) => cdr.borrow().clone(),
                value => keymap_list_items(interp, value)?
                    .map(|items| Value::list(items.into_iter().skip(1)))
                    .unwrap_or(Value::Nil),
            })
        }
        "identity" => {
            need_args(name, args, 1)?;
            Ok(args[0].clone())
        }
        "list" => Ok(Value::list(args.iter().cloned())),
        "nconc" => nconc_values(args),
        "append" => {
            let mut items: Vec<Value> = Vec::new();
            for (i, a) in args.iter().enumerate() {
                let is_last = i == args.len() - 1;
                if let Some(string) = sequence_string_like(a) {
                    items.extend(string_sequence_values(&string));
                    continue;
                }
                if is_vector_like_value(interp, a) {
                    items.extend(sequence_values(interp, a)?);
                    continue;
                }
                if is_last {
                    // `append` copies all preceding args and reuses the last tail as-is.
                    let mut result = a.clone();
                    for item in items.into_iter().rev() {
                        result = Value::cons(item, result);
                    }
                    return Ok(result);
                } else {
                    items.extend(a.to_vec()?);
                }
            }
            Ok(Value::list(items))
        }
        "nth" => {
            need_args(name, args, 2)?;
            let n = args[0].as_integer()? as usize;
            let list = list_sequence_items(interp, &args[1])?;
            Ok(list.get(n).cloned().unwrap_or(Value::Nil))
        }
        "elt" => {
            need_args(name, args, 2)?;
            if matches!(args[0], Value::Cons(_, _))
                && matches!(
                    args[0].to_vec().ok().and_then(|items| items.first().cloned()),
                    Some(Value::Symbol(symbol)) if symbol == "vector" || symbol == "vector-literal"
                )
            {
                super::call(interp, "aref", args, env)
            } else if matches!(args[0], Value::Nil | Value::Cons(_, _)) {
                let n = args[1].as_integer()? as usize;
                let list = args[0].to_vec()?;
                Ok(list.get(n).cloned().unwrap_or(Value::Nil))
            } else {
                super::call(interp, "aref", args, env)
            }
        }
        "nthcdr" => {
            need_args(name, args, 2)?;
            if let Some(items) = keymap_list_items(interp, &args[1])? {
                return nthcdr_value(&args[0], &Value::list(items));
            }
            nthcdr_value(&args[0], &args[1])
        }
        "last" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            if let Some(items) = keymap_list_items(interp, &args[0])? {
                let projected = Value::list(items);
                return super::call(
                    interp,
                    "last",
                    &[projected, args.get(1).cloned().unwrap_or(Value::Integer(1))],
                    env,
                );
            }
            let n = args
                .get(1)
                .map(Value::as_integer)
                .transpose()?
                .unwrap_or(1)
                .max(0) as usize;
            let mut tails = Vec::new();
            let mut current = args[0].clone();
            loop {
                match current.clone() {
                    Value::Cons(_, cdr) => {
                        tails.push(current.clone());
                        current = cdr.borrow().clone();
                    }
                    Value::Nil => {
                        return if n == 0 {
                            Ok(Value::Nil)
                        } else if let Some(index) = tails.len().checked_sub(n.max(1)) {
                            Ok(tails[index].clone())
                        } else {
                            Ok(args[0].clone())
                        };
                    }
                    other => {
                        return if n == 0 {
                            Ok(other)
                        } else if let Some(index) = tails.len().checked_sub(n.max(1)) {
                            Ok(tails[index].clone())
                        } else {
                            Ok(args[0].clone())
                        };
                    }
                }
            }
        }
        "butlast" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let n = args.get(1).map(Value::as_integer).transpose()?.unwrap_or(1);
            if n <= 0 {
                return Ok(args[0].clone());
            }
            let items = list_sequence_items(interp, &args[0])?;
            let keep = items.len().saturating_sub(n as usize);
            Ok(Value::list(items.into_iter().take(keep)))
        }
        "length" => {
            need_args(name, args, 1)?;
            if let Some(items) = keymap_list_items(interp, &args[0])? {
                return Ok(Value::Integer(items.len() as i64));
            }
            if let Some(items) = record_literal_items(&args[0]) {
                return Ok(Value::Integer((items.len().saturating_sub(1)) as i64));
            }
            match &args[0] {
                value if string_like(value).is_some() => {
                    Ok(Value::Integer(string_text(value)?.chars().count() as i64))
                }
                Value::Nil => Ok(Value::Integer(0)),
                Value::Cons(_, _) if is_vector_value(&args[0]) => {
                    Ok(Value::Integer(vector_items(&args[0])?.len() as i64))
                }
                Value::CharTable(_) => Ok(Value::Integer(0x40_0000)),
                value if is_bool_vector_value(interp, value) => Ok(Value::Integer(
                    bool_vector_values(interp, value)?.len() as i64,
                )),
                Value::Cons(_, _) => Ok(Value::Integer(args[0].to_vec()?.len() as i64)),
                Value::Record(id) => {
                    let record = interp.find_record(*id).ok_or_else(|| {
                        LispError::TypeError("record".into(), format!("record<{id}>"))
                    })?;
                    Ok(Value::Integer((record.slots.len() + 1) as i64))
                }
                _ => Err(LispError::TypeError("sequence".into(), args[0].type_name())),
            }
        }
        "safe-length" => {
            need_args(name, args, 1)?;
            Ok(Value::Integer(
                keymap_list_items(interp, &args[0])?
                    .map(|items| items.len() as i64)
                    .unwrap_or_else(|| safe_list_length(&args[0])),
            ))
        }
        "length<" | "length>" | "length=" => {
            need_args(name, args, 2)?;
            let length = sequence_length_value(interp, &args[0])?;
            let target = args[1].as_integer()?;
            let matches = match name {
                "length<" => length < target,
                "length>" => length > target,
                _ => length == target,
            };
            Ok(if matches { Value::T } else { Value::Nil })
        }
        "reverse" => {
            need_args(name, args, 1)?;
            reverse_sequence_value(interp, &args[0])
        }
        "copy-tree" => {
            need_arg_range(name, args, 1, 2)?;
            let vectors_and_records = args.get(1).is_some_and(Value::is_truthy);
            copy_tree_value(interp, &args[0], vectors_and_records)
        }
        "flatten-tree" | "flatten-list" => {
            need_args(name, args, 1)?;
            let mut leaves = Vec::new();
            flatten_tree_value(&args[0], &mut leaves);
            Ok(Value::list(leaves))
        }
        "copy-alist" => {
            need_args(name, args, 1)?;
            copy_alist_value(&args[0])
        }
        "delete-dups" => {
            need_args(name, args, 1)?;
            let mut deduped = Vec::new();
            for item in args[0].to_vec()? {
                if !deduped
                    .iter()
                    .any(|existing| values_equal(interp, existing, &item))
                {
                    deduped.push(item);
                }
            }
            Ok(Value::list(deduped))
        }
        "remove" => {
            need_args(name, args, 2)?;
            remove_equal(interp, &args[0], &args[1])
        }
        "memq" | "memql" | "member" => {
            need_args(name, args, 2)?;
            let mut current = args[1].clone();
            let mut seen = HashSet::new();
            loop {
                match current.clone() {
                    Value::Cons(car, cdr) => {
                        let cell_id = Rc::as_ptr(&car) as usize;
                        if !seen.insert(cell_id) {
                            return Err(LispError::SignalValue(Value::list([
                                Value::Symbol("circular-list".into()),
                                Value::String("Circular list".into()),
                            ])));
                        }
                        let item = car.borrow().clone();
                        let matches = match name {
                            "member" => values_equal(interp, &item, &args[0]),
                            "memql" => values_eql(&item, &args[0]),
                            _ => values_eq_in_env(interp, &item, &args[0], env),
                        };
                        if matches {
                            return Ok(current);
                        }
                        current = cdr.borrow().clone();
                    }
                    Value::Nil => return Ok(Value::Nil),
                    other => {
                        let matches = match name {
                            "member" => values_equal(interp, &other, &args[0]),
                            "memql" => values_eql(&other, &args[0]),
                            _ => values_eq_in_env(interp, &other, &args[0], env),
                        };
                        if matches {
                            return Ok(other);
                        }
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("wrong-type-argument".into()),
                            Value::Symbol("listp".into()),
                            other,
                        ])));
                    }
                }
            }
        }
        "member-ignore-case" => {
            need_args(name, args, 2)?;
            let needle = string_text(&args[0])?.to_ascii_lowercase();
            let items = args[1].to_vec()?;
            for (index, item) in items.iter().enumerate() {
                if string_like(item)
                    .is_some_and(|candidate| candidate.text.to_ascii_lowercase() == needle)
                {
                    return Ok(Value::list(items[index..].iter().cloned()));
                }
            }
            Ok(Value::Nil)
        }
        "assq" => {
            need_args(name, args, 2)?;
            let mut current = args[1].clone();
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
                        let item = car.borrow().clone();
                        if matches!(item, Value::Cons(_, _)) && item.car()? == args[0] {
                            return Ok(item);
                        }
                        current = cdr.borrow().clone();
                    }
                    other => {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("wrong-type-argument".into()),
                            Value::Symbol("listp".into()),
                            other,
                        ])));
                    }
                }
            }
        }
        "rassq" => {
            need_args(name, args, 2)?;
            let mut current = args[1].clone();
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
                        let item = car.borrow().clone();
                        if matches!(item, Value::Cons(_, _)) && item.cdr()? == args[0] {
                            return Ok(item);
                        }
                        current = cdr.borrow().clone();
                    }
                    other => {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("wrong-type-argument".into()),
                            Value::Symbol("listp".into()),
                            other,
                        ])));
                    }
                }
            }
        }
        "rassoc" => {
            need_args(name, args, 2)?;
            let mut current = args[1].clone();
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
                        let item = car.borrow().clone();
                        if matches!(item, Value::Cons(_, _))
                            && values_equal(interp, &item.cdr()?, &args[0])
                        {
                            return Ok(item);
                        }
                        current = cdr.borrow().clone();
                    }
                    other => {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("wrong-type-argument".into()),
                            Value::Symbol("listp".into()),
                            other,
                        ])));
                    }
                }
            }
        }
        "rassq-delete-all" => {
            need_args(name, args, 2)?;
            rassq_delete_all(&args[0], &args[1])
        }
        "assq-delete-all" => {
            need_args(name, args, 2)?;
            assq_delete_all(&args[0], &args[1])
        }
        "assoc-delete-all" => {
            need_args(name, args, 2)?;
            assoc_delete_all(interp, &args[0], &args[1])
        }
        "assoc" => {
            need_arg_range(name, args, 2, 3)?;
            let mut current = args[1].clone();
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
                        let item = car.borrow().clone();
                        if matches!(item, Value::Cons(_, _))
                            && if let Some(testfn) = args.get(2).filter(|value| !value.is_nil()) {
                                call_function_value(
                                    interp,
                                    testfn,
                                    &[args[0].clone(), item.car()?],
                                    env,
                                )?
                                .is_truthy()
                            } else {
                                values_equal(interp, &item.car()?, &args[0])
                            }
                        {
                            return Ok(item);
                        }
                        current = cdr.borrow().clone();
                    }
                    other => {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("wrong-type-argument".into()),
                            Value::Symbol("listp".into()),
                            other,
                        ])));
                    }
                }
            }
        }
        "assoc-string" => {
            need_arg_range(name, args, 2, 3)?;
            let items = args[1].to_vec()?;
            if items.is_empty() {
                return Ok(Value::Nil);
            }
            let key = assoc_string_text(&args[0])?;
            let key = if args.get(2).is_some_and(|value| !value.is_nil()) {
                assoc_string_folded_text(interp, &key)?
            } else {
                key
            };
            for item in &items {
                let thiscar = match item {
                    Value::Cons(_, _) => item.car()?,
                    _ => item.clone(),
                };
                let Some(candidate) = assoc_string_candidate_text(&thiscar) else {
                    continue;
                };
                let candidate = if args.get(2).is_some_and(|value| !value.is_nil()) {
                    assoc_string_folded_text(interp, &candidate)?
                } else {
                    candidate
                };
                if candidate == key {
                    return Ok(item.clone());
                }
            }
            Ok(Value::Nil)
        }
        "alist-get" => {
            if args.len() < 2 || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let default = args.get(2).cloned().unwrap_or(Value::Nil);
            let testfn = args.get(4);
            let items = args[1].to_vec()?;
            for item in items {
                let Some((car, cdr)) = item.cons_values() else {
                    continue;
                };
                if value_matches_with_test(interp, &args[0], &car, testfn, env)? {
                    return Ok(cdr);
                }
            }
            Ok(default)
        }
        "cl-set-exclusive-or" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let left = args[0].to_vec()?;
            let right = args[1].to_vec()?;
            let mut test = Value::BuiltinFunc("equal".into());
            let mut index = 2usize;
            while index + 1 < args.len() {
                if matches!(&args[index], Value::Symbol(keyword) if keyword == ":test") {
                    test = resolve_callable(interp, &args[index + 1], env)?;
                }
                index += 2;
            }
            let mut result = Vec::new();
            for item in &left {
                if !list_contains_with(interp, &right, item, &test, env)? {
                    result.push(item.clone());
                }
            }
            for item in &right {
                if !list_contains_with(interp, &left, item, &test, env)? {
                    result.push(item.clone());
                }
            }
            Ok(Value::list(result))
        }
        "cl-remove-if-not" => {
            need_args(name, args, 2)?;
            let mut kept = Vec::new();
            for item in args[1].to_vec()? {
                if call_function_value(interp, &args[0], std::slice::from_ref(&item), env)?
                    .is_truthy()
                {
                    kept.push(item);
                }
            }
            Ok(Value::list(kept))
        }
        "cl-delete-if" => cl_delete_if_values(interp, args, env),
        "mapcar" => {
            need_args(name, args, 2)?;
            let list = sequence_values(interp, &args[1])?;
            let mut results = Vec::new();
            for item in list {
                results.push(call_function_value(interp, &args[0], &[item], env)?);
            }
            Ok(Value::list(results))
        }
        "mapcan" => {
            need_args(name, args, 2)?;
            let list = sequence_values(interp, &args[1])?;
            let mut mapped = Vec::with_capacity(list.len());
            for item in list {
                mapped.push(call_function_value(interp, &args[0], &[item], env)?);
            }
            nconc_values(&mapped)
        }
        "cl-mapcar" => {
            need_args(name, args, 2)?;
            let lists = args[1..]
                .iter()
                .map(|value| sequence_values(interp, value))
                .collect::<Result<Vec<_>, _>>()?;
            let len = lists.iter().map(Vec::len).min().unwrap_or(0);
            let mut results = Vec::with_capacity(len);
            for index in 0..len {
                let call_args = lists
                    .iter()
                    .map(|list| list[index].clone())
                    .collect::<Vec<_>>();
                results.push(call_function_value(interp, &args[0], &call_args, env)?);
            }
            Ok(Value::list(results))
        }
        "cl-mapcan" => {
            need_args(name, args, 2)?;
            let mapped = super::call(interp, "cl-mapcar", args, env)?.to_vec()?;
            let mut flattened = Vec::new();
            for item in mapped {
                flattened.extend(item.to_vec()?);
            }
            Ok(Value::list(flattened))
        }
        "cl-some" => {
            need_args(name, args, 2)?;
            let sequences = args[1..]
                .iter()
                .map(|value| sequence_values(interp, value))
                .collect::<Result<Vec<_>, _>>()?;
            let len = sequences.iter().map(Vec::len).min().unwrap_or(0);
            for index in 0..len {
                let call_args = sequences
                    .iter()
                    .map(|sequence| sequence[index].clone())
                    .collect::<Vec<_>>();
                let result = call_function_value(interp, &args[0], &call_args, env)?;
                if result.is_truthy() {
                    return Ok(result);
                }
            }
            Ok(Value::Nil)
        }
        "seq-mapcat" => {
            need_arg_range(name, args, 2, 3)?;
            let sequence = sequence_values(interp, &args[1])?;
            let mut flattened = Vec::new();
            for item in sequence {
                let mapped = call_function_value(interp, &args[0], &[item], env)?;
                flattened.extend(sequence_values(interp, &mapped)?);
            }

            match args
                .get(2)
                .and_then(|value| value.as_symbol().ok())
                .unwrap_or("list")
            {
                "list" => Ok(Value::list(flattened)),
                "vector" => Ok(Value::list(
                    std::iter::once(Value::Symbol("vector".into())).chain(flattened),
                )),
                "string" => super::call(interp, "concat", &flattened, env),
                other => Err(LispError::Signal(format!(
                    "Unsupported seq-mapcat result type: {other}"
                ))),
            }
        }
        "mapc" => {
            need_args(name, args, 2)?;
            let list = sequence_values(interp, &args[1])?;
            for item in &list {
                let _ = call_function_value(interp, &args[0], std::slice::from_ref(item), env)?;
            }
            Ok(args[1].clone())
        }
        "cl-reduce" => {
            need_args(name, args, 2)?;
            let items = args[1].to_vec()?;
            let Some((first, rest)) = items.split_first() else {
                return Ok(Value::Nil);
            };
            let mut acc = first.clone();
            for item in rest {
                acc = call_function_value(interp, &args[0], &[acc.clone(), item.clone()], env)?;
            }
            Ok(acc)
        }
        "eval" => eval_impl(interp, args, env),
        "eval-buffer" => eval_buffer_impl(interp, args, env),
        "mapconcat" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let list = super::call(interp, "mapcar", &args[..2], env)?.to_vec()?;
            let sep = if args.len() == 3 {
                let text = string_text(&args[2])?;
                let multibyte = text.chars().any(|ch| (ch as u32) > 0x7F);
                string_like(&args[2]).unwrap_or(StringLike {
                    text,
                    props: Vec::new(),
                    multibyte,
                })
            } else {
                StringLike {
                    text: String::new(),
                    props: Vec::new(),
                    multibyte: false,
                }
            };
            let mut result = String::new();
            let mut props = Vec::new();
            for (index, item) in list.iter().enumerate() {
                if index > 0 {
                    let offset = result.chars().count();
                    result.push_str(&sep.text);
                    props.extend(shift_string_props(&sep.props, offset));
                }
                if let Some(string) = string_like(item) {
                    let offset = result.chars().count();
                    result.push_str(&string.text);
                    props.extend(shift_string_props(&string.props, offset));
                } else if item.is_nil() {
                } else {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("wrong-type-argument".into()),
                        Value::Symbol("sequencep".into()),
                        item.clone(),
                    ])));
                }
            }
            Ok(string_like_value(result, merge_string_props(props)))
        }
        "string-join" => {
            need_arg_range(name, args, 1, 2)?;
            let separator = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| Value::String(String::new()));
            super::call(
                interp,
                "mapconcat",
                &[
                    Value::BuiltinFunc("identity".into()),
                    args[0].clone(),
                    separator,
                ],
                env,
            )
        }
        "ensure-list" => {
            need_args(name, args, 1)?;
            Ok(
                if args[0].is_nil() || matches!(args[0], Value::Cons(_, _)) {
                    args[0].clone()
                } else {
                    Value::list([args[0].clone()])
                },
            )
        }
        "position-symbol" => {
            need_args(name, args, 2)?;
            let position = args[1].as_integer()?;
            Ok(interp.create_record(
                "symbol-with-pos",
                vec![args[0].clone(), Value::Integer(position)],
            ))
        }
        "symbol-with-pos-pos" => {
            need_args(name, args, 1)?;
            let (_, position) = symbol_with_pos_parts(interp, &args[0]).ok_or_else(|| {
                LispError::TypeError("symbol-with-pos".into(), args[0].type_name())
            })?;
            Ok(Value::Integer(position))
        }
        "remove-pos-from-symbol" | "bare-symbol" => {
            need_args(name, args, 1)?;
            Ok(symbol_with_pos_parts(interp, &args[0])
                .map(|(symbol, _)| symbol)
                .unwrap_or_else(|| args[0].clone()))
        }
        "seq-find" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let predicate = resolve_callable(interp, &args[0], env)?;
            if let Ok(items) = vector_items(&args[1]) {
                for item in items {
                    if interp
                        .call_function_value(
                            predicate.clone(),
                            args[0].as_symbol().ok(),
                            std::slice::from_ref(&item),
                            env,
                        )?
                        .is_truthy()
                    {
                        return Ok(item);
                    }
                }
                Ok(Value::Nil)
            } else if let Some(string) = sequence_string_like(&args[1]) {
                for ch in string.text.chars() {
                    let item = string_sequence_value(&string, ch);
                    if interp
                        .call_function_value(
                            predicate.clone(),
                            args[0].as_symbol().ok(),
                            std::slice::from_ref(&item),
                            env,
                        )?
                        .is_truthy()
                    {
                        return Ok(item);
                    }
                }
                Ok(Value::Nil)
            } else {
                Err(LispError::TypeError("sequence".into(), args[1].type_name()))
            }
        }
        "seq-contains-p" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            if let Ok(items) = vector_items(&args[0]) {
                for item in items {
                    let matches = if let Some(testfn) = args.get(2).filter(|value| !value.is_nil())
                    {
                        value_matches_with_test(interp, &item, &args[1], Some(testfn), env)?
                    } else {
                        values_equal(interp, &item, &args[1])
                    };
                    if matches {
                        return Ok(Value::T);
                    }
                }
                Ok(Value::Nil)
            } else if let Some(string) = sequence_string_like(&args[0]) {
                for ch in string.text.chars() {
                    let candidate = string_sequence_value(&string, ch);
                    let matches = if let Some(testfn) = args.get(2).filter(|value| !value.is_nil())
                    {
                        value_matches_with_test(interp, &candidate, &args[1], Some(testfn), env)?
                    } else {
                        values_equal(interp, &candidate, &args[1])
                    };
                    if matches {
                        return Ok(Value::T);
                    }
                }
                Ok(Value::Nil)
            } else {
                Err(LispError::TypeError("sequence".into(), args[0].type_name()))
            }
        }
        "seq-take" => {
            need_args(name, args, 2)?;
            let count = args[1].as_integer()?.max(0) as usize;
            if let Ok(items) = args[0].to_vec() {
                Ok(Value::list(items.into_iter().take(count)))
            } else if let Some(string) = string_like(&args[0]) {
                let text: String = string.text.chars().take(count).collect();
                let props = slice_string_props(&string.props, 0, text.chars().count());
                Ok(string_like_value(text, props))
            } else {
                Err(LispError::TypeError("sequence".into(), args[0].type_name()))
            }
        }
        "seq-position" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            if let Ok(items) = args[0].to_vec() {
                for (index, item) in items.into_iter().enumerate() {
                    let matches = if let Some(testfn) = args.get(2).filter(|value| !value.is_nil())
                    {
                        value_matches_with_test(interp, &item, &args[1], Some(testfn), env)?
                    } else {
                        values_equal(interp, &item, &args[1])
                    };
                    if matches {
                        return Ok(Value::Integer(index as i64));
                    }
                }
                Ok(Value::Nil)
            } else if let Some(string) = string_like(&args[0]) {
                for (index, ch) in string.text.chars().enumerate() {
                    let candidate = string_sequence_value(&string, ch);
                    let matches = if let Some(testfn) = args.get(2).filter(|value| !value.is_nil())
                    {
                        value_matches_with_test(interp, &candidate, &args[1], Some(testfn), env)?
                    } else {
                        values_equal(interp, &candidate, &args[1])
                    };
                    if matches {
                        return Ok(Value::Integer(index as i64));
                    }
                }
                Ok(Value::Nil)
            } else {
                Err(LispError::TypeError("sequence".into(), args[0].type_name()))
            }
        }
        "cl-coerce" => {
            need_args(name, args, 2)?;
            let items = if is_bool_vector_value(interp, &args[0]) {
                bool_vector_values(interp, &args[0])?
            } else {
                sequence_values(interp, &args[0])?
            };
            match args[1].as_symbol()? {
                "list" => Ok(Value::list(items)),
                "vector" => {
                    let mut vector = vec![Value::symbol("vector")];
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
                    "cl-coerce unsupported type: {kind}"
                ))),
            }
        }
        "treesit-language-available-p" => {
            need_args(name, args, 1)?;
            Ok(Value::Nil)
        }
        "treesit--linecol-cache" => {
            need_args(name, args, 0)?;
            Ok(interp
                .buffer_local_value(interp.current_buffer_id(), TREESIT_LINECOL_CACHE_VAR)
                .unwrap_or_else(treesit_default_linecol_cache))
        }
        "treesit--linecol-cache-set" => {
            need_args(name, args, 3)?;
            let cache = treesit_linecol_cache_value(
                args[0].as_integer()?,
                args[1].as_integer()?,
                args[2].as_integer()?,
            );
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                TREESIT_LINECOL_CACHE_VAR,
                cache,
            );
            Ok(Value::Nil)
        }
        "treesit--linecol-at" => {
            if args.is_empty() || args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let pos = args
                .first()
                .map(Value::as_integer)
                .transpose()?
                .map(|value| value.max(1) as usize)
                .unwrap_or_else(|| interp.current_buffer().point());
            treesit_linecol_at(interp, pos)
        }
        "apply" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs("apply".into(), args.len()));
            }
            let func = &args[0];
            let last = &args[args.len() - 1];
            let mut all_args: Vec<Value> = args[1..args.len() - 1].to_vec();
            all_args.extend(sequence_values(interp, last)?);
            let resolved = resolve_callable(interp, func, env)?;
            let original_name = func.as_symbol().ok();
            interp.call_function_value(resolved, original_name, &all_args, env)
        }
        "apply-partially" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            let rest_name = "__emaxx-apply-partially-rest".to_string();
            let mut body = vec![Value::Symbol("apply".into()), literal_form(&args[0])];
            body.extend(args[1..].iter().map(literal_form));
            body.push(Value::Symbol(rest_name.clone()));
            Ok(Value::Lambda(
                vec!["&rest".into(), rest_name],
                vec![Value::list(body)],
                shared_env(env.clone()),
            ))
        }
        "funcall" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs("funcall".into(), 0));
            }
            let resolved = resolve_callable(interp, &args[0], env)?;
            let original_name = args[0].as_symbol().ok();
            interp.call_function_value(resolved, original_name, &args[1..], env)
        }
        "fset" => {
            need_args(name, args, 2)?;
            let symbol = args[0].as_symbol()?;
            if args[1].is_nil() {
                interp.set_function_binding(symbol, None);
                Ok(Value::Nil)
            } else {
                interp.validate_function_binding(symbol, &args[1])?;
                interp.set_function_binding(symbol, Some(args[1].clone()));
                Ok(args[1].clone())
            }
        }
        "fmakunbound" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            interp.set_function_binding(symbol, None);
            Ok(Value::Symbol(symbol.to_string()))
        }
        "funcall-interactively" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            let func = resolve_callable(interp, &args[0], env)?;
            invoke_function_value(interp, &func, &args[1..], env)
        }
        "call-interactively" => call_interactively_impl(interp, args, env),
        "keyboard-quit" => Err(LispError::SignalValue(Value::list([
            Value::Symbol("quit".into()),
            Value::Nil,
        ]))),
        "start-kbd-macro" => {
            need_arg_range(name, args, 0, 2)?;
            // Batch-mode compatibility shim: enough for kmacro.el setup/advice
            // paths, but it does not record or replay keyboard events.
            interp.set_variable("defining-kbd-macro", Value::T, env);
            Ok(Value::Nil)
        }
        "end-kbd-macro" => {
            need_arg_range(name, args, 0, 2)?;
            // See start-kbd-macro above.
            interp.set_variable("defining-kbd-macro", Value::Nil, env);
            Ok(Value::Nil)
        }
        "define-keymap" => Ok(keymap_placeholder(None)),
        "define-abbrev-table" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let symbol = args[0].as_symbol()?.to_string();
            let table = match interp.lookup_var(&symbol, &Vec::new()) {
                Some(existing) if is_abbrev_table_value(interp, &existing) => existing,
                _ => {
                    let created = make_runtime_abbrev_table(interp, Some(&symbol), Value::Nil);
                    interp.set_global_binding(&symbol, created.clone());
                    register_abbrev_table_symbol(interp, &symbol);
                    created
                }
            };
            if let Some(docstring) = args.get(2)
                && matches!(docstring, Value::String(_) | Value::StringObject(_))
            {
                interp.put_symbol_property(&symbol, "variable-documentation", docstring.clone());
            }
            let mut prop_index = 2usize;
            if matches!(args.get(2), Some(Value::String(_) | Value::StringObject(_))) {
                prop_index = 3;
            }
            if !(args.len() - prop_index).is_multiple_of(2) {
                return Err(LispError::Signal(
                    "Invalid abbrev table property list".into(),
                ));
            }
            while prop_index + 1 < args.len() {
                set_abbrev_table_property(
                    interp,
                    &table,
                    &args[prop_index],
                    args[prop_index + 1].clone(),
                )?;
                prop_index += 2;
            }
            set_abbrev_table_entries_from_definitions(interp, &table, &args[1])?;
            Ok(table)
        }
        "read-key" => {
            need_arg_range(name, args, 0, 2)?;
            ensure_interaction_allowed(interp, env)?;
            let disable_fallbacks = args.get(1).is_some_and(Value::is_truthy);
            loop {
                let event = if let Some(decoded) = read_decoded_input_event(interp, env)? {
                    decoded
                } else {
                    normalize_input_event_value(pop_unread_command_event_value(interp, env)?)?
                };
                if !disable_fallbacks && is_mouse_down_event(&event) {
                    continue;
                }
                return Ok(event);
            }
        }
        "read-key-sequence" => {
            need_arg_range(name, args, 0, 4)?;
            ensure_interaction_allowed(interp, env)?;
            Ok(Value::list([
                Value::Symbol("vector".into()),
                read_key_sequence_event(interp, env)?,
            ]))
        }
        "read-event" | "read-char" | "read-char-exclusive" => {
            let read_event = name == "read-event";
            let timed_poll = args.len() >= 3 && args[2].is_truthy();
            if timed_poll {
                interp.drive_threads(env, true)?;
                if !interaction_allowed(interp, env) {
                    return Ok(Value::Nil);
                }
                return match pop_unread_command_event_value(interp, env) {
                    Ok(event) => {
                        if read_event {
                            normalize_input_event_value(event)
                        } else {
                            Ok(Value::Integer(unread_command_event_char(&event)? as i64))
                        }
                    }
                    Err(_) => Ok(Value::Nil),
                };
            }
            ensure_interaction_allowed(interp, env)?;
            let event = pop_unread_command_event_value(interp, env)?;
            if read_event {
                normalize_input_event_value(event)
            } else {
                Ok(Value::Integer(unread_command_event_char(&event)? as i64))
            }
        }
        "mouse-double-click-time" => {
            need_arg_range(name, args, 0, 0)?;
            let value = interp
                .lookup_var("double-click-time", env)
                .unwrap_or(Value::Nil);
            match value {
                Value::T => Ok(Value::Integer(10_000)),
                Value::Integer(value) if value > 0 => Ok(Value::Integer(value)),
                Value::Float(value) if value > 0.0 => Ok(Value::Float(value)),
                _ => Ok(Value::Integer(0)),
            }
        }
        "context-menu-map" => {
            need_arg_range(name, args, 0, 1)?;
            let click = args
                .first()
                .cloned()
                .or_else(|| interp.lookup_var("last-input-event", env))
                .unwrap_or(Value::Nil);
            let mut menu = make_runtime_keymap(interp, Some("Context Menu"));

            for function in interp
                .lookup_var("context-menu-functions", env)
                .unwrap_or(Value::Nil)
                .to_vec()?
            {
                let result =
                    call_function_value(interp, &function, &[menu.clone(), click.clone()], env)?;
                if is_keymap_value(interp, &result) {
                    menu = result;
                }
            }

            if let Some(filter) = interp.lookup_var("context-menu-filter-function", env)
                && !filter.is_nil()
            {
                let result =
                    call_function_value(interp, &filter, &[menu.clone(), click.clone()], env)?;
                if is_keymap_value(interp, &result) {
                    menu = result;
                }
            }

            context_menu_keymap_items(interp, &menu)
        }
        "read-string" | "read-from-minibuffer" | "read-no-blanks-input" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            ensure_interaction_allowed(interp, env)?;
            Ok(Value::String(String::new()))
        }
        "completing-read" => completing_read(interp, args, env),
        "format-prompt" => format_prompt(interp, args, env),

        _ => unreachable!("dispatch chunk called for unsupported primitive"),
    }
}
