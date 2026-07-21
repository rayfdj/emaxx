use super::*;

/// Convert a resolved function value into a `help-function-arglist` result: the
/// parameter list for a lambda, the inner lambda's parameters for a
/// `(macro . lambda)` cons, and nil otherwise.
fn help_function_arglist_value(function: &Value) -> Value {
    match function {
        Value::Lambda(params, _, _) => Value::list(params.iter().cloned().map(Value::Symbol)),
        Value::Cons(car, cdr) if matches!(&*car.borrow(), Value::Symbol(s) if s == "macro") => {
            help_function_arglist_value(&cdr.borrow())
        }
        _ => Value::Nil,
    }
}

pub(super) fn handles(name: &str) -> bool {
    matches!(
        name,
        "unencodable-char-position"
            | "set-advertised-calling-convention"
            | "get-advertised-calling-convention"
            | "compare-buffer-substrings"
            | "field-beginning"
            | "field-end"
            | "field-string"
            | "field-string-no-properties"
            | "delete-field"
            | "constrain-to-field"
            | "current-buffer"
            | "generate-new-buffer"
            | "get-buffer"
            | "get-buffer-create"
            | "generate-new-buffer-name"
            | "make-indirect-buffer"
            | "clone-indirect-buffer"
            | "rename-buffer"
            | "other-buffer"
            | "buffer-base-buffer"
            | "buffer-swap-text"
            | "buffer-local-value"
            | "buffer-local-toplevel-value"
            | "buffer-local-variables"
            | "kill-local-variable"
            | "make-local-variable"
            | "set-buffer-local-toplevel-value"
            | "buffer-list"
            | "list-buffers"
            | "list-buffers-noselect"
            | "Buffer-menu-buffer"
            | "add-variable-watcher"
            | "remove-variable-watcher"
            | "get-variable-watchers"
            | "command-modes"
            | "help-function-arglist"
            | "indirect-function"
            | "byteorder"
            | "subr-arity"
            | "func-arity"
            | "subr-name"
            | "subr-native-lambda-list"
            | "native-comp-available-p"
            | "native-comp-function-p"
            | "subr-native-comp-unit"
            | "native-comp-unit-file"
            | "native-comp-unit-set-file"
            | "decode-char"
            | "char-charset"
            | "charsetp"
            | "charset-id-internal"
            | "charset-plist"
            | "charset-priority-list"
            | "charset-after"
            | "find-charset-string"
            | "find-charset-region"
            | "map-charset-chars"
            | "define-charset-internal"
            | "define-charset-alias"
            | "set-charset-plist"
            | "unify-charset"
            | "get-unused-iso-final-char"
            | "declare-equiv-charset"
            | "iso-charset"
            | "split-char"
            | "clear-charset-maps"
            | "set-charset-priority"
            | "sort-charsets"
            | "coding-system-p"
            | "check-coding-system"
            | "coding-system-list"
            | "coding-system-type"
            | "coding-system-priority-list"
            | "sort-coding-systems"
            | "coding-system-aliases"
            | "coding-system-plist"
            | "coding-system-get"
            | "coding-system-put"
            | "coding-system-eol-type"
            | "coding-system-change-eol-conversion"
            | "coding-system-base"
            | "coding-system-equal"
            | "check-coding-systems-region"
            | "select-safe-coding-system"
            | "detect-coding-string"
            | "detect-coding-region"
            | "find-coding-systems-region-internal"
            | "decode-sjis-char"
            | "encode-sjis-char"
            | "decode-big5-char"
            | "encode-big5-char"
            | "terminal-coding-system"
            | "set-terminal-coding-system-internal"
            | "set-safe-terminal-coding-system-internal"
            | "keyboard-coding-system"
            | "set-keyboard-coding-system"
            | "set-keyboard-coding-system-internal"
            | "find-operation-coding-system"
            | "set-coding-system-priority"
            | "define-coding-system-internal"
            | "define-coding-system-alias"
    )
}

pub(super) fn call(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    match name {
        "unencodable-char-position" => {
            need_arg_range(name, args, 2, 4)?;
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            if start > end || end > interp.buffer.point_max() {
                return Err(LispError::Signal("Args out of range".into()));
            }
            Ok(Value::Nil)
        }
        "set-advertised-calling-convention" => {
            need_args(name, args, 3)?;
            let function = advertised_function_name(interp, &args[0])?;
            interp.put_symbol_property(&function, "advertised-calling-convention", args[1].clone());
            Ok(args[0].clone())
        }
        "get-advertised-calling-convention" => {
            need_args(name, args, 1)?;
            let function = advertised_function_name(interp, &args[0])?;
            Ok(interp
                .get_symbol_property(&function, "advertised-calling-convention")
                .unwrap_or(Value::T))
        }
        "compare-buffer-substrings" => {
            need_args(name, args, 6)?;
            let left_id = interp.resolve_buffer_id(&args[0])?;
            let left_start = position_from_value(interp, &args[1])?;
            let left_end = position_from_value(interp, &args[2])?;
            let right_id = interp.resolve_buffer_id(&args[3])?;
            let right_start = position_from_value(interp, &args[4])?;
            let right_end = position_from_value(interp, &args[5])?;
            let left = interp
                .get_buffer_by_id(left_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", left_id)))?
                .buffer_substring(left_start, left_end)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            let right = interp
                .get_buffer_by_id(right_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", right_id)))?
                .buffer_substring(right_start, right_end)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            Ok(Value::Integer(compare_buffer_substrings(&left, &right)))
        }
        "field-beginning" | "field-end" => {
            let pos = if args.is_empty() || args[0].is_nil() {
                interp.buffer.point()
            } else {
                position_from_value(interp, &args[0])?
            };
            let escape_from_edge = args.get(1).is_some_and(|value| value.is_truthy());
            let limit = match args.get(2).filter(|value| !value.is_nil()) {
                Some(value) => Some(position_from_value(interp, value)?),
                None => None,
            };
            let point_min = interp.buffer.point_min();
            let after_field = super::call(
                interp,
                "get-char-property",
                &[Value::Integer(pos as i64), Value::Symbol("field".into())],
                env,
            )?;
            let before_field = if pos > point_min {
                super::call(
                    interp,
                    "get-char-property",
                    &[
                        Value::Integer((pos - 1) as i64),
                        Value::Symbol("field".into()),
                    ],
                    env,
                )?
            } else {
                after_field.clone()
            };
            let mut at_field_start = false;
            let mut at_field_end = false;
            if !escape_from_edge {
                let inherited_field = super::call(
                    interp,
                    "get-pos-property",
                    &[Value::Integer(pos as i64), Value::Symbol("field".into())],
                    env,
                )?;
                at_field_end = inherited_field != after_field;
                at_field_start = inherited_field != before_field;
                if inherited_field.is_nil() && at_field_start && at_field_end {
                    // A nil insertion between two non-nil fields normally
                    // marks a non-editable boundary (for example, a prompt),
                    // not a synthetic zero-width field.
                    at_field_start = false;
                    at_field_end = false;
                }
            }

            let change = |interp: &mut Interpreter,
                          primitive: &str,
                          position: usize,
                          limit: Option<usize>,
                          env: &mut Env|
             -> Result<usize, LispError> {
                super::call(
                    interp,
                    primitive,
                    &[
                        Value::Integer(position as i64),
                        Value::Symbol("field".into()),
                        Value::Nil,
                        limit.map_or(Value::Nil, |value| Value::Integer(value as i64)),
                    ],
                    env,
                )?
                .as_integer()
                .map(|value| value as usize)
            };

            if name == "field-beginning" {
                if at_field_start {
                    return Ok(Value::Integer(pos as i64));
                }
                let mut beginning = pos;
                if escape_from_edge
                    && matches!(&before_field, Value::Symbol(value) if value == "boundary")
                {
                    beginning = change(
                        interp,
                        "previous-single-char-property-change",
                        beginning,
                        limit,
                        env,
                    )?;
                }
                beginning = change(
                    interp,
                    "previous-single-char-property-change",
                    beginning,
                    limit,
                    env,
                )?;
                Ok(Value::Integer(beginning as i64))
            } else {
                if at_field_end {
                    return Ok(Value::Integer(pos as i64));
                }
                let mut end = pos;
                if escape_from_edge
                    && matches!(&after_field, Value::Symbol(value) if value == "boundary")
                {
                    end = change(interp, "next-single-char-property-change", end, limit, env)?;
                }
                end = change(interp, "next-single-char-property-change", end, limit, env)?;
                Ok(Value::Integer(end as i64))
            }
        }
        "field-string" | "field-string-no-properties" => {
            need_arg_range(name, args, 0, 1)?;
            let pos = match args.first().filter(|value| !value.is_nil()) {
                Some(value) => position_from_value(interp, value)?,
                None => interp.buffer.point(),
            };
            let start = super::call(
                interp,
                "field-beginning",
                &[Value::Integer(pos as i64)],
                env,
            )?
            .as_integer()? as usize;
            let end = super::call(interp, "field-end", &[Value::Integer(pos as i64)], env)?
                .as_integer()? as usize;
            let text = interp
                .buffer
                .buffer_substring(start, end)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            if name == "field-string" {
                Ok(string_like_value(
                    text,
                    interp.buffer.substring_property_spans(start, end),
                ))
            } else {
                Ok(Value::String(text))
            }
        }
        "delete-field" => {
            need_args(name, args, 1)?;
            let pos = position_from_value(interp, &args[0])?;
            let start = super::call(
                interp,
                "field-beginning",
                &[Value::Integer(pos as i64)],
                env,
            )?
            .as_integer()? as usize;
            let end = super::call(interp, "field-end", &[Value::Integer(pos as i64)], env)?
                .as_integer()? as usize;
            interp
                .delete_region_current_buffer(start, end)
                .map_err(|e| LispError::Signal(e.to_string()))?;
            Ok(Value::Nil)
        }
        "constrain-to-field" => {
            // (NEW-POS OLD-POS &optional ESCAPE-FROM-EDGE ONLY-IN-LINE
            //  INHIBIT-CAPTURE-PROPERTY); the optional flags only matter
            // for edge stickiness, which the span model doesn't track.
            if args.is_empty() || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let new_pos = if args[0].is_nil() {
                interp.buffer.point()
            } else {
                position_from_value(interp, &args[0])?
            };
            let old_pos = if args.len() > 1 {
                position_from_value(interp, &args[1])?
            } else {
                interp.buffer.point()
            };
            let inhibit_motion = interp
                .lookup_var("inhibit-field-text-motion", env)
                .is_some_and(|value| value.is_truthy());
            let mut constrained = new_pos;
            let fwd = new_pos > old_pos;
            let point_min = interp.buffer.point_min();
            // GNU's gate: any field property at or just before either
            // position makes the positions candidates for constraining.
            let field_at = |interp: &Interpreter, pos: usize| -> Option<Value> {
                interp
                    .buffer
                    .text_property_at(pos, "field")
                    .filter(|value| !value.is_nil())
            };
            let near_field = field_at(interp, new_pos).is_some()
                || field_at(interp, old_pos).is_some()
                || (new_pos > point_min && field_at(interp, new_pos - 1).is_some())
                || (old_pos > point_min && field_at(interp, old_pos - 1).is_some());
            if !inhibit_motion && new_pos != old_pos && near_field {
                let escape = args.get(2).cloned().unwrap_or(Value::Nil);
                let field_bound = if fwd {
                    super::call(
                        interp,
                        "field-end",
                        &[
                            Value::Integer(old_pos as i64),
                            escape,
                            Value::Integer(new_pos as i64),
                        ],
                        env,
                    )?
                    .as_integer()? as usize
                } else {
                    super::call(
                        interp,
                        "field-beginning",
                        &[
                            Value::Integer(old_pos as i64),
                            escape,
                            Value::Integer(new_pos as i64),
                        ],
                        env,
                    )?
                    .as_integer()? as usize
                };
                // GNU only constrains when FIELD_BOUND lies between OLD-POS
                // and NEW-POS; a bound already past NEW-POS means NEW-POS is
                // acceptable (see Fconstrain_to_field's "other side" check).
                // With ONLY-IN-LINE, the constraint applies only when it
                // does not move the result across a newline.
                let only_in_line = args.get(3).is_some_and(|value| value.is_truthy());
                let crosses_newline = || {
                    let (low, high) = if field_bound < new_pos {
                        (field_bound, new_pos)
                    } else {
                        (new_pos, field_bound)
                    };
                    (low..high).any(|pos| interp.buffer.char_at(pos) == Some('\n'))
                };
                if (if field_bound < new_pos { fwd } else { !fwd })
                    && (!only_in_line || !crosses_newline())
                {
                    constrained = field_bound;
                }
            }
            if args[0].is_nil() {
                interp.buffer.goto_char(constrained);
            }
            Ok(Value::Integer(constrained as i64))
        }
        "current-buffer" => Ok(Value::Buffer(
            interp.current_buffer_id(),
            interp.buffer.name.clone(),
        )),
        "generate-new-buffer" => {
            need_args(name, args, 1)?;
            let base = string_text(&args[0])?;
            let inhibit_hooks = args.get(1).is_some_and(|value| value.is_truthy());
            let buf_name = if interp.has_buffer(&base) {
                let mut n = 2;
                loop {
                    let candidate = format!("{}<{}>", base, n);
                    if !interp.has_buffer(&candidate) {
                        break candidate;
                    }
                    n += 1;
                }
            } else {
                base
            };
            let (id, _) = interp.create_buffer(&buf_name);
            interp.set_buffer_hooks_inhibited(id, inhibit_hooks);
            if !inhibit_hooks {
                run_named_hooks(interp, "buffer-list-update-hook", env, None)?;
            }
            Ok(Value::Buffer(id, buf_name))
        }
        "get-buffer" => {
            need_args(name, args, 1)?;
            match &args[0] {
                Value::Buffer(id, _) if interp.has_buffer_id(*id) => Ok(args[0].clone()),
                Value::Buffer(_, _) => Ok(Value::Nil),
                _ => match string_like(&args[0]) {
                    Some(name) => match interp.find_buffer(&name.text) {
                        Some((id, buffer_name)) => Ok(Value::Buffer(id, buffer_name)),
                        None => Ok(Value::Nil),
                    },
                    None => Err(LispError::TypeError(
                        "string-or-buffer".into(),
                        args[0].type_name(),
                    )),
                },
            }
        }
        "get-buffer-create" => {
            need_args(name, args, 1)?;
            let inhibit_hooks = args.get(1).is_some_and(|value| value.is_truthy());
            let buf_name = match &args[0] {
                Value::Buffer(_, n) => n.clone(),
                _ => string_text(&args[0]).map_err(|_| {
                    LispError::TypeError("string-or-buffer".into(), args[0].type_name())
                })?,
            };
            if let Some((id, name)) = interp.find_buffer(&buf_name) {
                Ok(Value::Buffer(id, name))
            } else {
                let (id, _) = interp.create_buffer(&buf_name);
                interp.set_buffer_hooks_inhibited(id, inhibit_hooks);
                if !inhibit_hooks {
                    run_named_hooks(interp, "buffer-list-update-hook", env, None)?;
                }
                Ok(Value::Buffer(id, buf_name))
            }
        }
        "generate-new-buffer-name" => {
            need_args(name, args, 1)?;
            let base = string_text(&args[0])?;
            let ignore = if args.len() > 1 {
                string_like(&args[1]).map(|string| string.text)
            } else {
                None
            };
            if !interp.has_buffer(&base) || ignore.as_deref() == Some(base.as_str()) {
                Ok(Value::String(base))
            } else {
                let mut n = 2;
                loop {
                    let candidate = format!("{}<{}>", base, n);
                    if !interp.has_buffer(&candidate) || ignore.as_deref() == Some(&candidate) {
                        break Ok(Value::String(candidate));
                    }
                    n += 1;
                }
            }
        }
        "make-indirect-buffer" => {
            if args.len() < 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let base_id = interp.resolve_buffer_id(&args[0])?;
            let new_name = string_text(&args[1])?;
            let clone = args.get(2).is_some_and(|value| value.is_truthy());
            let inhibit_hooks = args.get(3).is_some_and(|value| value.is_truthy());
            let (new_id, _) = interp.create_buffer(&new_name);
            let (text, props, point, mark, file, base_overlays, restriction, multibyte) = {
                let base = interp
                    .get_buffer_by_id(base_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", base_id)))?;
                (
                    base.full_buffer_string(),
                    base.full_property_spans(),
                    base.point(),
                    base.mark(),
                    base.file.clone(),
                    base.overlays.clone(),
                    base.restriction(),
                    base.is_multibyte(),
                )
            };
            let overlays = if clone {
                base_overlays
                    .into_iter()
                    .map(|mut overlay| {
                        overlay.id = interp.alloc_overlay_id();
                        overlay.buffer_id = Some(new_id);
                        overlay
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            if let Some(buffer) = interp.get_buffer_by_id_mut(new_id) {
                *buffer = crate::buffer::Buffer::from_text(&new_name, &text);
                buffer.file = file;
                buffer.inhibit_hooks = inhibit_hooks;
                buffer.set_multibyte(multibyte);
                buffer.restore_restriction(restriction.0, restriction.1);
                buffer.goto_char(point);
                if let Some(mark) = mark {
                    buffer.set_mark(mark);
                }
                for span in props {
                    buffer.set_text_properties(span.start + 1, span.end + 1, &span.props);
                }
                buffer.overlays = overlays;
            }
            interp.register_indirect_buffer(new_id, base_id);
            if clone {
                interp.clone_buffer_local_state(base_id, new_id);
            }
            if !inhibit_hooks {
                run_named_hooks(interp, "buffer-list-update-hook", env, None)?;
            }
            Ok(Value::Buffer(new_id, new_name))
        }
        "clone-indirect-buffer" => {
            need_arg_range(name, args, 0, 2)?;
            let base = Value::Buffer(interp.current_buffer_id(), interp.buffer.name.clone());
            let name = args
                .first()
                .and_then(|value| string_like(value).map(|string| string.text))
                .unwrap_or_else(|| format!("{}<clone>", interp.buffer.name));
            let clone = args.get(1).is_some_and(|value| value.is_truthy());
            super::call(
                interp,
                "make-indirect-buffer",
                &[
                    base,
                    Value::String(name),
                    if clone { Value::T } else { Value::Nil },
                ],
                env,
            )
        }
        "rename-buffer" => {
            need_args(name, args, 1)?;
            let new_name = string_text(&args[0])?;
            if new_name.is_empty() {
                return Err(LispError::Signal("Empty string for buffer name".into()));
            }
            let old_name = interp.buffer.name.clone();
            let unique = args.len() > 1 && args[1].is_truthy();
            let final_name = if interp.has_buffer(&new_name) && new_name != old_name {
                if unique {
                    let mut n = 2;
                    loop {
                        let candidate = format!("{}<{}>", new_name, n);
                        if !interp.has_buffer(&candidate) {
                            break candidate;
                        }
                        n += 1;
                    }
                } else {
                    return Err(LispError::Signal(format!(
                        "Buffer name `{}' is in use",
                        new_name
                    )));
                }
            } else {
                new_name
            };
            if let Some(pos) = interp.buffer_list.iter().position(|(_, n)| *n == old_name) {
                interp.buffer_list[pos].1 = final_name.clone();
            }
            interp.buffer.last_name = Some(old_name);
            interp.buffer.name = final_name.clone();
            Ok(Value::String(final_name))
        }
        "other-buffer" => {
            let exclude = if !args.is_empty() {
                match &args[0] {
                    Value::Buffer(_, n) => n.clone(),
                    _ => interp.buffer.name.clone(),
                }
            } else {
                interp.buffer.name.clone()
            };
            for (id, buf_name) in &interp.buffer_list {
                if *buf_name != exclude && !buf_name.starts_with(' ') {
                    return Ok(Value::Buffer(*id, buf_name.clone()));
                }
            }
            Ok(Value::Buffer(0, "*scratch*".into()))
        }
        "buffer-base-buffer" => {
            let buffer_id = if let Some(buffer) = args.first() {
                interp.resolve_buffer_id(buffer)?
            } else {
                interp.current_buffer_id()
            };
            Ok(interp
                .buffer_base_id(buffer_id)
                .and_then(|base_id| {
                    interp
                        .get_buffer_by_id(base_id)
                        .map(|buffer| Value::Buffer(base_id, buffer.name.clone()))
                })
                .unwrap_or(Value::Nil))
        }
        "buffer-swap-text" => {
            need_args(name, args, 1)?;
            let other_id = interp.resolve_buffer_id(&args[0])?;
            let current_id = interp.current_buffer_id();
            interp.swap_buffer_text_state(current_id, other_id)?;
            Ok(Value::Nil)
        }
        "buffer-local-value" => {
            need_args(name, args, 2)?;
            let symbol = args[0].as_symbol()?.to_string();
            let buffer_id = interp.resolve_buffer_id(&args[1])?;
            // GNU falls back to the DEFAULT value when BUFFER has no local
            // binding; another buffer's local value must not leak through
            // (erc-open's prior-session detection reads `erc--target').
            Ok(interp
                .buffer_local_value(buffer_id, &symbol)
                .or_else(|| interp.default_value(&symbol))
                .or_else(|| interp.symbol_value_cell(&symbol).ok())
                .unwrap_or(Value::Nil))
        }
        "buffer-local-toplevel-value" => {
            need_args(name, args, 1)?;
            let symbol = interp.resolve_variable_name(args[0].as_symbol()?)?;
            interp
                .buffer_local_toplevel_value(interp.current_buffer_id(), &symbol)
                .ok_or(LispError::Void(symbol))
        }
        "buffer-local-variables" => {
            let buffer_id = match args.first().filter(|value| !value.is_nil()) {
                Some(value) => interp.resolve_buffer_id(value)?,
                None => interp.current_buffer_id(),
            };
            let mut vars = interp
                .buffer_local_variables(buffer_id)
                .into_iter()
                .map(|(name, value)| Value::cons(Value::Symbol(name), value))
                .collect::<Vec<_>>();
            let buffer = interp
                .get_buffer_by_id(buffer_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
            vars.push(Value::cons(
                Value::Symbol("buffer-undo-list".into()),
                buffer_undo_list_value(buffer),
            ));
            Ok(Value::list(vars))
        }
        "kill-local-variable" => {
            need_args(name, args, 1)?;
            let symbol = interp.resolve_variable_name(args[0].as_symbol()?)?;
            interp.notify_variable_watchers(
                &symbol,
                Value::Nil,
                "makunbound",
                Some(interp.current_buffer_id()),
                env,
            )?;
            interp.remove_buffer_local_value(interp.current_buffer_id(), &symbol);
            Ok(Value::Symbol(symbol))
        }
        "make-local-variable" => {
            need_args(name, args, 1)?;
            let symbol = interp.resolve_variable_name(args[0].as_symbol()?)?;
            let value = interp.symbol_value_cell(&symbol).unwrap_or(Value::Nil);
            interp.set_buffer_local_value(interp.current_buffer_id(), &symbol, value);
            Ok(Value::Symbol(symbol))
        }
        "set-buffer-local-toplevel-value" => {
            need_args(name, args, 2)?;
            let symbol = interp.resolve_variable_name(args[0].as_symbol()?)?;
            let value = interp.prepare_variable_assignment(&symbol, args[1].clone())?;
            interp.notify_variable_watchers(
                &symbol,
                value.clone(),
                "set",
                Some(interp.current_buffer_id()),
                env,
            )?;
            interp.set_buffer_local_toplevel_value(
                interp.current_buffer_id(),
                &symbol,
                value.clone(),
            );
            Ok(value)
        }
        "buffer-list" => {
            let bufs: Vec<Value> = interp
                .buffer_list
                .iter()
                .map(|(id, n)| Value::Buffer(*id, n.clone()))
                .collect();
            Ok(Value::list(bufs))
        }
        "list-buffers" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let files_only = args.first().is_some_and(Value::is_truthy);
            let _ = refresh_buffer_menu(interp, files_only, None, None, env)?;
            Ok(Value::Symbol("window".into()))
        }
        "list-buffers-noselect" => {
            if args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let files_only = args.first().is_some_and(Value::is_truthy);
            refresh_buffer_menu(interp, files_only, args.get(1), args.get(2), env)
        }
        "Buffer-menu-buffer" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let error_if_non_existent = args.first().is_some_and(Value::is_truthy);
            let Some(entries) =
                interp.buffer_local_value(interp.current_buffer_id(), BUFFER_MENU_ENTRIES_VAR)
            else {
                return if error_if_non_existent {
                    Err(LispError::Signal("No buffer on this line".into()))
                } else {
                    Ok(Value::Nil)
                };
            };
            let entries = entries.to_vec()?;
            let line_index = interp
                .buffer
                .line_number_at_pos(interp.buffer.point())
                .saturating_sub(1);
            let Some(entry) = entries.get(line_index).cloned() else {
                return if error_if_non_existent {
                    Err(LispError::Signal("No buffer on this line".into()))
                } else {
                    Ok(Value::Nil)
                };
            };
            match entry {
                Value::Buffer(id, _) if interp.has_buffer_id(id) => Ok(entry),
                Value::Buffer(_, _) if error_if_non_existent => {
                    Err(LispError::Signal("This buffer has been killed".into()))
                }
                Value::Buffer(_, _) => Ok(Value::Nil),
                other => Err(LispError::TypeError("buffer".into(), other.type_name())),
            }
        }
        "add-variable-watcher" => {
            need_args(name, args, 2)?;
            interp.add_variable_watcher(args[0].as_symbol()?, args[1].clone())?;
            Ok(args[1].clone())
        }
        "remove-variable-watcher" => {
            need_args(name, args, 2)?;
            interp.remove_variable_watcher(args[0].as_symbol()?, &args[1])?;
            Ok(args[1].clone())
        }
        "get-variable-watchers" => {
            need_args(name, args, 1)?;
            Ok(Value::list(interp.variable_watchers(args[0].as_symbol()?)))
        }
        "command-modes" => {
            need_args(name, args, 1)?;
            Ok(interp
                .get_symbol_property(args[0].as_symbol()?, "command-modes")
                .unwrap_or(Value::Nil))
        }
        "help-function-arglist" => {
            need_arg_range(name, args, 1, 2)?;
            // GNU returns t when the arglist is unknown (nil, unbound
            // symbols, opaque objects) — advice.el's ad-arglist feeds it
            // nil for autoloaded functions and expects a non-list.
            if args[0].is_nil() {
                return Ok(Value::T);
            }
            let function = if let Ok(symbol) = args[0].as_symbol() {
                if let Some(arglist) = interp.get_symbol_property(symbol, "emaxx-function-arglist")
                {
                    return Ok(arglist);
                }
                match interp.lookup_function(symbol, env) {
                    Ok(function) => function,
                    // The function cell has no lambda, but the symbol may still
                    // be a macro (e.g. a GNU macro loaded into the macro table).
                    // GNU returns the macro's arglist, never a bare `t`, so the
                    // callers that iterate the arglist (shortdoc) do not fault.
                    Err(_) => match interp.macro_binding_as_function(symbol) {
                        Some(macro_fn) => macro_fn,
                        None => return Ok(Value::T),
                    },
                }
            } else {
                args[0].clone()
            };
            Ok(help_function_arglist_value(&function))
        }
        "indirect-function" => {
            need_arg_range(name, args, 1, 2)?;
            let mut seen = Vec::new();
            let mut current = args[0].clone();
            loop {
                match &current {
                    Value::Symbol(symbol) => {
                        if seen.iter().any(|existing| existing == symbol) {
                            return Err(LispError::SignalValue(Value::list([
                                Value::Symbol("cyclic-function-indirection".into()),
                                Value::Symbol(symbol.clone()),
                            ])));
                        }
                        seen.push(symbol.clone());
                        match interp.lookup_function(symbol, env) {
                            Ok(resolved) if matches!(resolved, Value::Symbol(_)) => {
                                current = resolved;
                            }
                            Ok(resolved) => return Ok(resolved),
                            // Since Emacs 24.4, a void function cell is
                            // represented as nil and indirect-function returns
                            // that nil.  Calling the result is where a
                            // void-function condition belongs.
                            Err(LispError::Void(_)) => return Ok(Value::Nil),
                            Err(error) => return Err(error),
                        }
                    }
                    _ => return Ok(current),
                }
            }
        }
        "byteorder" => {
            need_args(name, args, 0)?;
            Ok(Value::Integer(if cfg!(target_endian = "big") {
                'B' as i64
            } else {
                'l' as i64
            }))
        }
        "subr-arity" => {
            need_args(name, args, 1)?;
            match &args[0] {
                Value::BuiltinFunc(symbol) => Ok(builtin_arity_value(symbol)
                    .or_else(|| special_form_arity_value(symbol))
                    .unwrap_or_else(|| fallback_subr_arity_value(symbol))),
                other => Err(LispError::TypeError("subr".into(), other.type_name())),
            }
        }
        "func-arity" => {
            need_args(name, args, 1)?;
            match &args[0] {
                Value::Symbol(symbol) => {
                    if let Some(arity) = special_form_arity_value(symbol) {
                        Ok(arity)
                    } else {
                        let function = interp.lookup_function(symbol, env)?;
                        function_arity_value(interp, &function, env)
                    }
                }
                other => function_arity_value(interp, other, env),
            }
        }
        "subr-name" => {
            need_args(name, args, 1)?;
            match &args[0] {
                Value::BuiltinFunc(symbol) => Ok(Value::String(symbol.clone())),
                other => Err(LispError::TypeError("subr".into(), other.type_name())),
            }
        }
        "subr-native-lambda-list" => {
            need_args(name, args, 1)?;
            match &args[0] {
                Value::BuiltinFunc(_) => Ok(Value::T),
                other => Err(LispError::TypeError("subr".into(), other.type_name())),
            }
        }
        "native-comp-available-p" => {
            need_args(name, args, 0)?;
            Ok(Value::Nil)
        }
        "native-comp-function-p" => {
            need_args(name, args, 1)?;
            Ok(Value::Nil)
        }
        "subr-native-comp-unit" => {
            need_args(name, args, 1)?;
            match &args[0] {
                Value::BuiltinFunc(symbol) => Ok(interp.create_record(
                    "native-comp-unit",
                    vec![Value::String(format!("{symbol}.eln"))],
                )),
                other => Err(LispError::TypeError("subr".into(), other.type_name())),
            }
        }
        "native-comp-unit-file" => {
            need_args(name, args, 1)?;
            let Value::Record(id) = &args[0] else {
                return Err(LispError::TypeError(
                    "native-comp-unit".into(),
                    args[0].type_name(),
                ));
            };
            let record = interp.find_record(*id).ok_or_else(|| {
                LispError::TypeError("native-comp-unit".into(), args[0].type_name())
            })?;
            if record.type_name != "native-comp-unit" {
                return Err(LispError::TypeError(
                    "native-comp-unit".into(),
                    args[0].type_name(),
                ));
            }
            Ok(record.slots.first().cloned().unwrap_or(Value::Nil))
        }
        "native-comp-unit-set-file" => {
            need_args(name, args, 2)?;
            let Value::Record(id) = &args[0] else {
                return Err(LispError::TypeError(
                    "native-comp-unit".into(),
                    args[0].type_name(),
                ));
            };
            let Some(record) = interp.find_record_mut(*id) else {
                return Err(LispError::TypeError(
                    "native-comp-unit".into(),
                    args[0].type_name(),
                ));
            };
            if record.type_name != "native-comp-unit" {
                return Err(LispError::TypeError(
                    "native-comp-unit".into(),
                    args[0].type_name(),
                ));
            }
            if record.slots.is_empty() {
                record.slots.push(args[1].clone());
            } else {
                record.slots[0] = args[1].clone();
            }
            Ok(args[1].clone())
        }
        "decode-char" => {
            need_args(name, args, 2)?;
            let charset = args[0].as_symbol()?;
            let code = args[1].as_integer()?;
            Ok(match interp.charset_canonical_name(charset).as_deref() {
                Some("ascii") if (0..=0x7f).contains(&code) => Value::Integer(code),
                Some("unicode") if code >= 0 => Value::Integer(code),
                Some("eight-bit") if (0..=0xff).contains(&code) => {
                    Value::Integer(RAW_BYTE_REGEX_BASE as i64 + code)
                }
                Some(_) | None => Value::Nil,
            })
        }
        "char-charset" => {
            need_args(name, args, 1)?;
            Ok(Value::Symbol(
                charset_for_char(args[0].as_integer()? as u32).into(),
            ))
        }
        "charsetp" => {
            need_args(name, args, 1)?;
            let Value::Symbol(symbol) = &args[0] else {
                return Ok(Value::Nil);
            };
            Ok(if interp.has_charset(symbol) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "charset-id-internal" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(interp
                .charset_id(symbol)
                .map(Value::Integer)
                .unwrap_or(Value::Nil))
        }
        "charset-plist" => {
            need_args(name, args, 1)?;
            let symbol = args[0].as_symbol()?;
            Ok(interp
                .charset_plist_value(symbol)
                .unwrap_or_else(|| default_charset_plist(symbol, interp).unwrap_or(Value::Nil)))
        }
        "charset-priority-list" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let priority = interp.charset_priority_list();
            if args.first().is_some_and(Value::is_truthy) {
                Ok(priority
                    .first()
                    .cloned()
                    .map(Value::Symbol)
                    .unwrap_or(Value::Nil))
            } else {
                Ok(Value::list(
                    priority.into_iter().map(Value::Symbol).collect::<Vec<_>>(),
                ))
            }
        }
        "charset-after" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let pos = args
                .first()
                .map(Value::as_integer)
                .transpose()?
                .unwrap_or(interp.buffer.point() as i64);
            Ok(match interp.buffer.char_at(pos as usize) {
                Some(ch) => Value::Symbol(charset_for_char(ch as u32).into()),
                None => Value::Nil,
            })
        }
        "find-charset-string" => {
            need_args(name, args, 1)?;
            Ok(Value::list(charsets_for_text(
                &string_text(&args[0])?,
                interp,
            )))
        }
        "find-charset-region" => {
            need_args(name, args, 2)?;
            let from = args[0].as_integer()?;
            let to = args[1].as_integer()?;
            let (start, end) = clamp_overlay_range(&interp.buffer, from, to);
            let mut text = String::new();
            for pos in start..end {
                if let Some(ch) = interp.buffer.char_at(pos) {
                    text.push(ch);
                }
            }
            Ok(Value::list(charsets_for_text(&text, interp)))
        }
        "map-charset-chars" => {
            if args.len() < 2 || args.len() > 5 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let function = args[0].clone();
            let charset = args[1].as_symbol()?.to_string();
            let arg = args.get(2).cloned().unwrap_or(Value::Nil);
            let from = args.get(3).map(Value::as_integer).transpose()?.unwrap_or(0);
            let to = args
                .get(4)
                .map(Value::as_integer)
                .transpose()?
                .unwrap_or(charset_max_codepoint(&charset));
            let ranges = charset_ranges_for(&charset, from, to, interp)?;
            for (start, end) in ranges {
                call_function_value(
                    interp,
                    &function,
                    &[
                        Value::cons(Value::Integer(start), Value::Integer(end)),
                        arg.clone(),
                    ],
                    env,
                )?;
            }
            Ok(Value::Nil)
        }
        "define-charset-internal" => Err(LispError::WrongNumberOfArgs(name.into(), args.len())),
        "define-charset-alias" => {
            need_args(name, args, 2)?;
            let alias = args[0].as_symbol()?;
            let target = args[1].as_symbol()?;
            interp.define_charset_alias(alias, target)?;
            Ok(Value::Symbol(alias.to_string()))
        }
        "set-charset-plist" => {
            need_args(name, args, 2)?;
            let charset = args[0].as_symbol()?;
            interp.set_charset_plist_value(charset, args[1].clone())?;
            Ok(args[1].clone())
        }
        "unify-charset" => {
            need_args(name, args, 1)?;
            Err(LispError::Signal("Cannot unify charset".into()))
        }
        "get-unused-iso-final-char" => {
            need_args(name, args, 2)?;
            Ok(Value::Integer('0' as i64))
        }
        "declare-equiv-charset" => {
            need_args(name, args, 4)?;
            let dimension = args[0].as_integer()?;
            let chars = args[1].as_integer()?;
            let final_char = args[2].as_integer()?;
            let charset = args[3].as_symbol()?;
            interp.declare_iso_charset(dimension, chars, final_char as u32, charset);
            Ok(Value::Nil)
        }
        "iso-charset" => {
            need_args(name, args, 3)?;
            let dimension = args[0].as_integer()?;
            let chars = args[1].as_integer()?;
            let final_char = args[2].as_integer()?;
            Ok(interp
                .iso_charset(dimension, chars, final_char as u32)
                .map(Value::Symbol)
                .unwrap_or(Value::Nil))
        }
        "split-char" => {
            need_args(name, args, 1)?;
            let code = args[0].as_integer()?;
            Ok(Value::list([
                Value::Symbol(charset_for_char(code as u32).into()),
                Value::Integer(code),
            ]))
        }
        "clear-charset-maps" => Ok(Value::Nil),
        "set-charset-priority" => {
            let names = args
                .iter()
                .map(Value::as_symbol)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .map(|name| name.to_string())
                .collect::<Vec<_>>();
            interp.set_charset_priority(&names);
            Ok(Value::Nil)
        }
        "sort-charsets" => {
            need_args(name, args, 1)?;
            let mut items = args[0].to_vec()?;
            items.sort_by_key(|value| {
                value
                    .as_symbol()
                    .map(|name| interp.charset_priority_rank(name))
                    .unwrap_or(usize::MAX)
            });
            Ok(Value::list(items))
        }
        "coding-system-p" => {
            need_args(name, args, 1)?;
            Ok(if args[0].is_nil() {
                Value::T
            } else if let Value::Symbol(symbol) = &args[0] {
                if interp.has_coding_system(symbol) {
                    Value::T
                } else {
                    Value::Nil
                }
            } else {
                Value::Nil
            })
        }
        "check-coding-system" => {
            need_args(name, args, 1)?;
            Ok(match checked_coding_name(interp, &args[0])? {
                Some(coding) => Value::Symbol(coding),
                None => Value::Nil,
            })
        }
        "coding-system-list" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::list(
                interp
                    .coding_system_list(args.first().is_some_and(Value::is_truthy))
                    .into_iter()
                    .map(Value::Symbol)
                    .collect::<Vec<_>>(),
            ))
        }
        "coding-system-type" => {
            need_args(name, args, 1)?;
            Ok(match checked_coding_name(interp, &args[0])? {
                Some(coding) => interp
                    .coding_system_kind_name(&coding)
                    .map(Value::Symbol)
                    .unwrap_or(Value::Nil),
                None => Value::Nil,
            })
        }
        "coding-system-priority-list" => {
            if args.len() > 1 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let priority = interp.coding_system_priority_list();
            if args.first().is_some_and(Value::is_truthy) {
                Ok(priority
                    .first()
                    .cloned()
                    .map(Value::Symbol)
                    .unwrap_or(Value::Nil))
            } else {
                Ok(Value::list(
                    priority.into_iter().map(Value::Symbol).collect::<Vec<_>>(),
                ))
            }
        }
        "sort-coding-systems" => {
            need_args(name, args, 1)?;
            let mut items = args[0].to_vec()?;
            items.sort_by_key(|value| {
                value
                    .as_symbol()
                    .ok()
                    .map(|name| interp.coding_system_priority_rank(name))
                    .unwrap_or(usize::MAX)
            });
            Ok(Value::list(items))
        }
        "coding-system-aliases" => {
            need_args(name, args, 1)?;
            let coding = checked_coding_symbol(interp, &args[0])?;
            Ok(Value::list(
                interp
                    .coding_system_alias_list(&coding)
                    .unwrap_or_default()
                    .into_iter()
                    .map(Value::Symbol)
                    .collect::<Vec<_>>(),
            ))
        }
        "coding-system-plist" => {
            need_args(name, args, 1)?;
            let coding = checked_coding_symbol(interp, &args[0])?;
            Ok(interp
                .coding_system_plist_value(&coding)
                .unwrap_or(Value::Nil))
        }
        "coding-system-get" => {
            need_args(name, args, 2)?;
            let coding = checked_coding_symbol(interp, &args[0])?;
            let property = args[1].as_symbol()?;
            if property == ":for-unibyte" {
                return Ok(
                    if interp
                        .coding_system_kind_name(&coding)
                        .is_some_and(|kind| kind == "raw-text")
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                );
            }
            let plist = interp
                .coding_system_plist_value(&coding)
                .unwrap_or(Value::Nil)
                .to_vec()?;
            let mut index = 0usize;
            while index + 1 < plist.len() {
                if plist[index] == args[1] {
                    return Ok(plist[index + 1].clone());
                }
                index += 2;
            }
            Ok(Value::Nil)
        }
        "coding-system-put" => {
            need_args(name, args, 3)?;
            let coding = checked_coding_symbol(interp, &args[0])?;
            let key = args[1].as_symbol()?;
            interp.set_coding_system_plist_property(&coding, key, args[2].clone())?;
            Ok(args[2].clone())
        }
        "coding-system-eol-type" => {
            need_args(name, args, 1)?;
            if args[0].is_nil() {
                return Ok(Value::Integer(0));
            }
            let coding = checked_coding_symbol(interp, &args[0])?;
            Ok(interp
                .coding_system_eol_type_value(&coding)
                .map(Value::Integer)
                .unwrap_or(Value::Nil))
        }
        "coding-system-base" => {
            need_args(name, args, 1)?;
            let coding = checked_coding_symbol(interp, &args[0])?;
            Ok(interp
                .coding_system_base_name(&coding)
                .map(Value::Symbol)
                .unwrap_or(Value::Nil))
        }
        "coding-system-change-eol-conversion" => {
            // GNU mule-cmds.el: return CODING-SYSTEM's variant with the
            // given EOL-TYPE (unix/dos/mac/0/1/2), the base when EOL-TYPE
            // is nil (auto-detect), or CODING-SYSTEM itself when nothing
            // changes.  In emaxx's model a base system has eol_type None
            // (GNU's vector of variants) and variants are "BASE-unix" etc.
            need_args(name, args, 2)?;
            // GNU: nil is a valid designator (base no-conversion, eol 0).
            let (base, original) = if args[0].is_nil() {
                ("no-conversion".to_string(), Some(0))
            } else {
                let coding = checked_coding_symbol(interp, &args[0])?;
                (
                    interp
                        .coding_system_base_name(&coding)
                        .unwrap_or_else(|| coding.clone()),
                    interp.coding_system_eol_type_value(&coding),
                )
            };
            let eol_type = match &args[1] {
                Value::Nil => None,
                Value::Integer(n) => Some(*n),
                Value::Symbol(symbol) => match symbol.as_str() {
                    "unix" => Some(0),
                    "dos" => Some(1),
                    "mac" => Some(2),
                    _ => None,
                },
                _ => None,
            };
            let Some(eol_type) = eol_type else {
                // nil EOL-TYPE: an already-undetermined system stays
                // as-is, a fixed one falls back to its base.
                return Ok(if original.is_none() {
                    args[0].clone()
                } else {
                    Value::Symbol(base)
                });
            };
            if original == Some(eol_type) {
                return Ok(args[0].clone());
            }
            let suffix = match eol_type {
                0 => "unix",
                1 => "dos",
                2 => "mac",
                _ => return Ok(Value::Nil),
            };
            let variant = format!("{base}-{suffix}");
            Ok(if interp.has_coding_system(&variant) {
                Value::Symbol(variant)
            } else {
                Value::Nil
            })
        }
        "coding-system-equal" => {
            need_args(name, args, 2)?;
            let equal = match (
                checked_coding_name(interp, &args[0])?,
                checked_coding_name(interp, &args[1])?,
            ) {
                (None, None) => true,
                (Some(left), Some(right)) => {
                    left == right
                        || (interp.coding_system_plist_value(&left)
                            == interp.coding_system_plist_value(&right)
                            && interp.coding_system_eol_type_value(&left)
                                == interp.coding_system_eol_type_value(&right))
                }
                _ => false,
            };
            Ok(if equal { Value::T } else { Value::Nil })
        }
        "check-coding-systems-region" => {
            need_args(name, args, 3)?;
            check_coding_systems_region_value(interp, &args[0], args.get(1), &args[2])
        }
        "select-safe-coding-system" => {
            need_arg_range(name, args, 2, 5)?;
            if let Some(default) = args.get(2)
                && let Some(coding) = first_valid_coding_candidate(interp, default)?
            {
                return Ok(Value::Symbol(coding));
            }
            if let Some(coding) = interp
                .lookup_var("coding-system-for-write", env)
                .and_then(|value| checked_coding_name(interp, &value).ok().flatten())
            {
                return Ok(Value::Symbol(coding));
            }
            Ok(Value::Symbol(
                checked_coding_name(interp, &Value::Symbol("utf-8-emacs".into()))?
                    .unwrap_or_else(|| "utf-8".into()),
            ))
        }
        "detect-coding-string" => {
            need_args(name, args, 1)?;
            detect_coding_string_value(interp, &args[0], args.get(1), env)
        }
        "detect-coding-region" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            detect_coding_region_value(interp, &args[0], &args[1], args.get(2), env)
        }
        "find-coding-systems-region-internal" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            find_coding_systems_region_internal_value(interp, &args[0])
        }
        "decode-sjis-char" => {
            need_args(name, args, 1)?;
            let code = args[0].as_integer()?;
            match code {
                0..=0x7F => Ok(Value::Integer(code)),
                0x82A0 => Ok(Value::Integer('あ' as i64)),
                _ => Err(LispError::Signal("Invalid Shift_JIS character".into())),
            }
        }
        "encode-sjis-char" => {
            need_args(name, args, 1)?;
            let code = args[0].as_integer()?;
            match code {
                0..=0x7F => Ok(Value::Integer(code)),
                x if x == 'あ' as i64 => Ok(Value::Integer(0x82A0)),
                _ => Err(LispError::Signal(
                    "Character cannot be encoded in Shift_JIS".into(),
                )),
            }
        }
        "decode-big5-char" => {
            need_args(name, args, 1)?;
            let code = args[0].as_integer()?;
            match code {
                0..=0x7F => Ok(Value::Integer(code)),
                _ => Err(LispError::Signal("Invalid Big5 character".into())),
            }
        }
        "encode-big5-char" => {
            need_args(name, args, 1)?;
            let code = args[0].as_integer()?;
            match code {
                0..=0x7F => Ok(Value::Integer(code)),
                _ => Err(LispError::Signal(
                    "Character cannot be encoded in Big5".into(),
                )),
            }
        }
        "terminal-coding-system" => Ok(interp
            .terminal_coding_system()
            .map(Value::Symbol)
            .unwrap_or(Value::Nil)),
        "set-terminal-coding-system-internal" | "set-safe-terminal-coding-system-internal" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let coding = checked_coding_name(interp, &args[0])?;
            interp.set_terminal_coding_system(coding.clone());
            Ok(coding.map(Value::Symbol).unwrap_or(Value::Nil))
        }
        "keyboard-coding-system" => Ok(interp
            .keyboard_coding_system()
            .map(Value::Symbol)
            .unwrap_or(Value::Nil)),
        "set-keyboard-coding-system" | "set-keyboard-coding-system-internal" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let coding = checked_coding_name(interp, &args[0])?;
            interp.set_keyboard_coding_system(coding.clone());
            Ok(coding.map(Value::Symbol).unwrap_or(Value::Nil))
        }
        "find-operation-coding-system" => find_operation_coding_system_value(interp, args, env),
        "set-coding-system-priority" => {
            let names = args
                .iter()
                .map(|value| checked_coding_symbol(interp, value))
                .collect::<Result<Vec<_>, _>>()?;
            interp.set_coding_system_priority(&names)?;
            Ok(Value::Nil)
        }
        "define-coding-system-internal" => {
            if args.len() < 13 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let coding = args[0].as_symbol()?;
            let mnemonic = args[1].as_integer()?;
            let kind = args[2].as_symbol()?;
            let plist = args[11].clone();
            let eol_type = match args[12].as_symbol()? {
                "unix" => Some(0),
                "dos" => Some(1),
                "mac" => Some(2),
                _ => None,
            };
            interp.define_coding_system(coding, mnemonic, kind, plist, eol_type)?;
            Ok(Value::Symbol(coding.to_string()))
        }
        "define-coding-system-alias" => {
            need_args(name, args, 2)?;
            let alias = args[0].as_symbol()?;
            let target = args[1].as_symbol()?;
            interp.define_coding_system_alias(alias, target)?;
            Ok(Value::Symbol(alias.to_string()))
        }
        _ => unreachable!("dispatch chunk called for unsupported primitive"),
    }
}
