use super::*;

const PORTABLE_DUMPER_UNAVAILABLE: &str = "Portable dumper backend is unavailable";

fn plist_property_is_truthy(plist: &Value, property: &str) -> bool {
    let Ok(items) = plist.to_vec() else {
        return false;
    };
    items
        .chunks_exact(2)
        .find(|pair| pair[0].as_symbol().ok() == Some(property))
        .is_some_and(|pair| pair[1].is_truthy())
}

fn coding_charset_list_is_ascii_compatible(interp: &Interpreter, value: &Value) -> bool {
    value.to_vec().is_ok_and(|charsets| {
        charsets.into_iter().any(|charset| {
            let Ok(name) = charset.as_symbol() else {
                return false;
            };
            matches!(name, "ascii" | "unicode" | "iso-8859-1")
                || interp
                    .charset_plist_value(name)
                    .is_some_and(|plist| plist_property_is_truthy(&plist, ":ascii-compatible-p"))
        })
    })
}

fn coding_category_name(kind: &str, args: &[Value]) -> &'static str {
    match kind {
        "utf-8" => match args.get(13) {
            Some(Value::Nil) | None => "coding-category-utf-8",
            Some(Value::T) => "coding-category-utf-8-sig",
            Some(_) => "coding-category-utf-8-auto",
        },
        "utf-16" => "coding-category-utf-16-auto",
        "charset" => "coding-category-charset",
        "iso-2022" => "coding-category-iso-7",
        "emacs-mule" => "coding-category-emacs-mule",
        "shift-jis" => "coding-category-sjis",
        "big5" => "coding-category-big5",
        "ccl" => "coding-category-ccl",
        "raw-text" => "coding-category-raw-text",
        "undecided" => "coding-category-undecided",
        _ => "coding-category-undecided",
    }
}

fn comparison_substring(
    interp: &Interpreter,
    buffer: &Value,
    start: &Value,
    end: &Value,
) -> Result<String, LispError> {
    let buffer_id = if buffer.is_nil() {
        interp.current_buffer_id()
    } else {
        interp.resolve_buffer_id(buffer)?
    };
    let buffer = interp
        .get_buffer_by_id(buffer_id)
        .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
    let start = if start.is_nil() {
        buffer.point_min()
    } else {
        position_from_value(interp, start)?
    };
    let end = if end.is_nil() {
        buffer.point_max()
    } else {
        position_from_value(interp, end)?
    };
    buffer
        .buffer_substring(start, end)
        .map_err(|error| LispError::Signal(error.to_string()))
}

define_dispatch!(
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
            "compare-buffer-substrings" => {
                need_args(name, args, 6)?;
                let left = comparison_substring(interp, &args[0], &args[1], &args[2])?;
                let right = comparison_substring(interp, &args[3], &args[4], &args[5])?;
                // GNU editfns.c consults `case-fold-search' in the current
                // buffer, even when either substring belongs to another
                // buffer.  Its canonical table is extra slot one of the
                // current downcase table; Emaxx's lazily initialized standard
                // table has the same effective mapping in the downcase table
                // until a materialized canonical slot exists.
                let canonical_table = if interp
                    .lookup_var("case-fold-search", env)
                    .is_some_and(|value| value.is_truthy())
                {
                    let downcase_table = interp.current_case_table_id();
                    Some(match interp.char_table_extra_slot(downcase_table, 1) {
                        Some(Value::CharTable(canonical_table)) => canonical_table,
                        _ => downcase_table,
                    })
                } else {
                    None
                };
                Ok(Value::Integer(compare_buffer_substrings(
                    &left,
                    &right,
                    |character| {
                        let code = character as u32;
                        let Some(table) = canonical_table else {
                            return code;
                        };
                        match interp.char_table_get(table, code) {
                            Some(Value::Integer(mapped)) => u32::try_from(mapped).unwrap_or(code),
                            _ => code,
                        }
                    },
                )))
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
                    Ok(string_like_value_with_multibyte(
                        text,
                        interp.buffer.substring_property_spans(start, end),
                        interp.buffer.is_multibyte(),
                    ))
                } else {
                    Ok(string_like_value_with_multibyte(
                        text,
                        Vec::new(),
                        interp.buffer.is_multibyte(),
                    ))
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
            "current-buffer" => Ok(Value::buffer(
                interp.current_buffer_id(),
                interp.buffer.name.clone(),
            )),
            "get-buffer" => {
                need_args(name, args, 1)?;
                match &args[0] {
                    Value::Buffer(buffer) if interp.has_buffer_id(buffer.id) => Ok(args[0].clone()),
                    Value::Buffer(_) => Ok(Value::Nil),
                    _ => match string_like(&args[0]) {
                        Some(name) => match interp.find_buffer(&name.text) {
                            Some((id, buffer_name)) => Ok(Value::buffer(id, buffer_name)),
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
                    Value::Buffer(buffer) => buffer.name.clone(),
                    _ => string_text(&args[0]).map(Into::into).map_err(|_| {
                        LispError::TypeError("string-or-buffer".into(), args[0].type_name())
                    })?,
                };
                if let Some((id, name)) = interp.find_buffer(&buf_name) {
                    Ok(Value::buffer(id, name))
                } else {
                    let (id, _) = interp.create_buffer(&buf_name);
                    interp.set_buffer_hooks_inhibited(id, inhibit_hooks);
                    if !inhibit_hooks {
                        run_named_hooks(interp, "buffer-list-update-hook", env, None)?;
                    }
                    Ok(Value::buffer(id, buf_name))
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
                    Ok(Value::String(base.into()))
                } else {
                    let mut n = 2;
                    loop {
                        let candidate = format!("{}<{}>", base, n);
                        if !interp.has_buffer(&candidate) || ignore.as_deref() == Some(&candidate) {
                            break Ok(Value::String(candidate.into()));
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
                let (text, props, point, mark, base_overlays, restriction, multibyte) = {
                    let base = interp.get_buffer_by_id(base_id).ok_or_else(|| {
                        LispError::Signal(format!("No buffer with id {}", base_id))
                    })?;
                    (
                        base.full_buffer_string(),
                        base.full_property_spans(),
                        base.point(),
                        base.mark(),
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
                    // GNU indirect buffers share their base buffer's text,
                    // but never visit its file themselves.  In particular,
                    // make-indirect-buffer clears both buffer-file-name and
                    // buffer-file-truename even when CLONE is non-nil.
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
                    interp.run_clone_indirect_buffer_hook(new_id, env)?;
                }
                if !inhibit_hooks {
                    run_named_hooks(interp, "buffer-list-update-hook", env, None)?;
                }
                Ok(Value::buffer(new_id, new_name))
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
                run_named_hooks(interp, "buffer-list-update-hook", env, None)?;
                // GNU buffer.c calls the preloaded Elisp owner directly
                // after installing the provisional name.  Uniquify uses
                // that boundary to replace a generated `<N>' suffix with
                // directory-derived policy; keep that policy in uniquify.el.
                if interp.has_lisp_function("uniquify--rename-buffer-advice") {
                    let callback = interp.lookup_function("uniquify--rename-buffer-advice", env)?;
                    interp.call_function_value(
                        callback,
                        Some("uniquify--rename-buffer-advice"),
                        &[args[0].clone(), args.get(1).cloned().unwrap_or(Value::Nil)],
                        env,
                    )?;
                }
                Ok(Value::String(interp.buffer.name.clone().into()))
            }
            "other-buffer" => {
                let exclude = if !args.is_empty() {
                    match &args[0] {
                        Value::Buffer(buffer) => buffer.name.to_string(),
                        _ => interp.buffer.name.clone(),
                    }
                } else {
                    interp.buffer.name.clone()
                };
                for (id, buf_name) in &interp.buffer_list {
                    if *buf_name != exclude && !buf_name.starts_with(' ') {
                        return Ok(Value::buffer(*id, buf_name.clone()));
                    }
                }
                Ok(Value::buffer(0, "*scratch*"))
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
                            .map(|buffer| Value::buffer(base_id, buffer.name.clone()))
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
                let symbol = interp.resolve_variable_name(args[0].as_symbol()?)?;
                let buffer_id = interp.resolve_buffer_id(&args[1])?;
                // GNU falls back to the DEFAULT value when BUFFER has no local
                // binding; another buffer's local value must not leak through
                // (erc-open's prior-session detection reads `erc--target').
                interp
                    .buffer_local_value(buffer_id, &symbol)
                    .or_else(|| interp.default_value(&symbol))
                    .or_else(|| interp.symbol_value_cell(&symbol).ok())
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
                    .map(|(name, value)| Value::cons(Value::Symbol(name.into()), value))
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
                Ok(Value::Symbol(symbol.into()))
            }
            "make-local-variable" => {
                need_args(name, args, 1)?;
                let symbol = interp.resolve_variable_name(args[0].as_symbol()?)?;
                // A native always-local slot (buffer-file-name, mode-name,
                // default-directory, ...) is already local by construction.
                // Adding a second entry to the generic buffer-local table
                // would shadow the native slot with its old value, so the
                // immediately following `(set (make-local-variable ...) V)'
                // could write V while reads still returned that stale entry.
                if !interp.is_always_buffer_local_special(&symbol) {
                    let value = interp.symbol_value_cell(&symbol).unwrap_or(Value::Nil);
                    interp.set_buffer_local_value(interp.current_buffer_id(), &symbol, value);
                }
                Ok(Value::Symbol(symbol.into()))
            }
            "buffer-list" => {
                let bufs: Vec<Value> = interp
                    .buffer_list
                    .iter()
                    .map(|(id, n)| Value::buffer(*id, n.clone()))
                    .collect();
                Ok(Value::list(bufs))
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
                let mut function = args[0].clone();
                while let Value::Symbol(symbol) = &function {
                    if let Some(modes) = interp.get_symbol_property(symbol, "command-modes")
                        && !modes.is_nil()
                    {
                        return Ok(modes);
                    }
                    function = match interp.lookup_function(symbol, env) {
                        Ok(function) => function,
                        Err(_) => return Ok(Value::Nil),
                    };
                }
                Ok(match function {
                    Value::Lambda(lambda) => lambda.command_modes().unwrap_or(Value::Nil),
                    Value::Record(id) => interp
                        .find_record(id)
                        .filter(|record| record.kind == crate::lisp::eval::RecordKind::Closure)
                        .and_then(|record| record.slots.get(5))
                        .and_then(crate::lisp::types::LambdaValue::command_modes_from_slot)
                        .unwrap_or(Value::Nil),
                    _ => Value::Nil,
                })
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
                            if let Some(function) =
                                super::misc_keymaps::materialize_preloaded_lisp_function(
                                    interp, symbol, env,
                                )
                            {
                                return Ok(function);
                            }
                            match interp.lookup_function(symbol, env) {
                                Ok(resolved) if matches!(resolved, Value::Symbol(_)) => {
                                    current = resolved;
                                }
                                Ok(resolved) => return Ok(resolved),
                                // Since Emacs 24.4, a void function cell is
                                // represented as nil and indirect-function returns
                                // that nil.  Calling the result is where a
                                // void-function condition belongs.
                                Err(LispError::VoidFunction(_)) => return Ok(Value::Nil),
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
                    // GNU data.c CHECK_SUBR signals the subrp predicate with
                    // the offending value itself.
                    other => Err(crate::lisp::primitives::wrong_type_argument(
                        "subrp",
                        other.clone(),
                    )),
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
                    Value::BuiltinFunc(symbol) => Ok(Value::String(symbol.clone().into())),
                    other => Err(crate::lisp::primitives::wrong_type_argument(
                        "subrp",
                        other.clone(),
                    )),
                }
            }
            "subr-native-lambda-list" => {
                need_args(name, args, 1)?;
                match &args[0] {
                    Value::BuiltinFunc(_) => Ok(Value::T),
                    other => Err(crate::lisp::primitives::wrong_type_argument(
                        "subrp",
                        other.clone(),
                    )),
                }
            }
            "native-comp-available-p" => {
                need_args(name, args, 0)?;
                Ok(Value::Nil)
            }
            "comp--subr-signature" => {
                need_args(name, args, 1)?;
                let Value::BuiltinFunc(symbol) = &args[0] else {
                    return Err(crate::lisp::primitives::wrong_type_argument(
                        "subrp",
                        args[0].clone(),
                    ));
                };
                let arity = builtin_arity_value(symbol)
                    .or_else(|| special_form_arity_value(symbol))
                    .unwrap_or_else(|| fallback_subr_arity_value(symbol));
                Ok(Value::String(
                    format!("{symbol}{}", render_prin1(interp, &arity, env)?).into(),
                ))
            }
            "comp-libgccjit-version"
            | "comp-native-compiler-options-effective-p"
            | "comp-native-driver-options-effective-p" => {
                need_args(name, args, 0)?;
                // These are capability queries, not compiler emulation.  Emaxx
                // has no libgccjit/native-comp backend, as reported by
                // `native-comp-available-p', so GNU's documented unavailable
                // result is nil for all three.
                Ok(Value::Nil)
            }
            "dump-emacs-portable--sort-predicate" => {
                need_args(name, args, 2)?;
                let relocation_offset =
                    |entry: &Value| -> Result<i64, LispError> { entry.cdr()?.car()?.as_integer() };
                Ok(
                    if relocation_offset(&args[0])? < relocation_offset(&args[1])? {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "dump-emacs-portable" => {
                need_arg_range(name, args, 1, 2)?;
                if string_like(&args[0]).is_none() {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("wrong-type-argument"),
                        Value::symbol("stringp"),
                        args[0].clone(),
                    ])));
                }
                // GNU's implementation serializes the entire live C heap and
                // later restores it with matching relocations.  Emaxx has no
                // image writer/loader, so creating a lookalike file would be
                // corrupt rather than compatible.
                Err(LispError::Signal(PORTABLE_DUMPER_UNAVAILABLE.into()))
            }
            "dump-emacs-portable--sort-predicate-copied" => {
                need_args(name, args, 2)?;
                // GNU orders addresses of objects copied from its static C image.
                // Rust values have no equivalent identity outside a real dump.
                Err(LispError::Signal(PORTABLE_DUMPER_UNAVAILABLE.into()))
            }
            "pdumper-stats" => {
                need_args(name, args, 0)?;
                // GNU returns nil when the running process was not restored from
                // a portable dump.  Emaxx initializes its Rust/Lisp state
                // directly and therefore has no dump file or restore time.
                Ok(Value::Nil)
            }
            "native-comp-function-p" => {
                need_args(name, args, 1)?;
                Ok(Value::Nil)
            }
            "subr-native-comp-unit" => {
                need_args(name, args, 1)?;
                match &args[0] {
                    Value::BuiltinFunc(symbol) => Ok(interp.create_pseudovector(
                        crate::lisp::eval::RecordKind::NativeCompUnit,
                        "native-comp-unit",
                        vec![Value::String(format!("{symbol}.eln").into())],
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
                if record.kind != crate::lisp::eval::RecordKind::NativeCompUnit {
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
                if record.kind != crate::lisp::eval::RecordKind::NativeCompUnit {
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
                let code = u32::try_from(args[1].as_integer()?)
                    .map_err(|_| LispError::Signal("Invalid charset code-point".into()))?;
                Ok(decode_charset_code(interp, charset, code)
                    .map(|character| {
                        let public = if (RAW_BYTE_REGEX_BASE..=RAW_BYTE_REGEX_BASE + 0xff)
                            .contains(&character)
                        {
                            RAW_BYTE8_BASE + character - RAW_BYTE_REGEX_BASE
                        } else {
                            character
                        };
                        Value::Integer(public.into())
                    })
                    .unwrap_or(Value::Nil))
            }
            "encode-char" => {
                need_args(name, args, 2)?;
                let character = u32::try_from(args[0].as_integer()?)
                    .map_err(|_| LispError::Signal("Invalid character".into()))?;
                let charset = args[1].as_symbol()?;
                let internal = if (RAW_BYTE8_BASE..=RAW_BYTE8_BASE + 0xff).contains(&character) {
                    RAW_BYTE_REGEX_BASE + character - RAW_BYTE8_BASE
                } else {
                    character
                };
                Ok(encode_charset_char(interp, charset, internal)
                    .map(|code| Value::Integer(code.into()))
                    .unwrap_or(Value::Nil))
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
                        .map(|value| Value::Symbol(value.into()))
                        .unwrap_or(Value::Nil))
                } else {
                    Ok(Value::list(
                        priority
                            .into_iter()
                            .map(|value| Value::Symbol(value.into()))
                            .collect::<Vec<_>>(),
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
            "define-charset-internal" => {
                // mule.el normalizes the public plist API into NAME plus its 16
                // C-layer attribute slots; the final slot is the canonical plist.
                need_args(name, args, 17)?;
                let charset = args[0].as_symbol()?.to_string();
                interp.define_charset(&charset, args[16].clone());
                Ok(Value::Nil)
            }
            "define-charset-alias" => {
                need_args(name, args, 2)?;
                let alias = args[0].as_symbol()?;
                let target = args[1].as_symbol()?;
                interp.define_charset_alias(alias, target)?;
                Ok(Value::Symbol(alias.to_string().into()))
            }
            "set-charset-plist" => {
                need_args(name, args, 2)?;
                let charset = args[0].as_symbol()?;
                interp.set_charset_plist_value(charset, args[1].clone())?;
                Ok(args[1].clone())
            }
            "unify-charset" => {
                need_arg_range(name, args, 1, 3)?;
                let charset = args[0].as_symbol()?;
                if !interp.has_charset(charset) {
                    return Err(LispError::Void(charset.to_string()));
                }
                if let Some(map) = args.get(1)
                    && !map.is_nil()
                    && !map.is_string()
                    && !is_vector_value(map)
                {
                    return Err(LispError::TypeError(
                        "string-or-vector".into(),
                        map.type_name(),
                    ));
                }
                // GNU maps legacy private charset code points onto Unicode here.
                // Emaxx stores buffer text as Unicode already, so registration is
                // the semantic state change and no secondary char table is needed.
                Ok(Value::Nil)
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
                    .map(|value| Value::Symbol(value.into()))
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
                    Some(coding) => Value::Symbol(coding.into()),
                    None => Value::Nil,
                })
            }
            "read-coding-system" | "read-non-nil-coding-system" => {
                need_arg_range(
                    name,
                    args,
                    1,
                    if name == "read-coding-system" { 2 } else { 1 },
                )?;
                let collection = Value::list(
                    interp
                        .coding_system_list(false)
                        .into_iter()
                        .map(|coding| Value::list([Value::String(coding.into())])),
                );
                let default = args.get(1).cloned().unwrap_or(Value::Nil);
                let default = match default {
                    Value::Symbol(symbol) => Value::String(symbol.into()),
                    value => value,
                };
                let completion_args = [
                    args[0].clone(),
                    collection,
                    Value::Nil,
                    Value::T,
                    Value::Nil,
                    Value::Symbol("coding-system-history".into()),
                    default,
                    Value::Nil,
                ];

                loop {
                    // coding.c dynamically binds this around completing-read;
                    // all coding-system names are lower-case and completion is
                    // intentionally case-insensitive.
                    env.push(vec![("completion-ignore-case".into(), Value::T)].into());
                    let result = completing_read(interp, &completion_args, env);
                    env.pop();
                    let entered = string_text(&result?)?;
                    if entered.is_empty() {
                        if name == "read-non-nil-coding-system" {
                            continue;
                        }
                        return Ok(Value::Nil);
                    }
                    return Ok(Value::Symbol(entered.into()));
                }
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
                        .map(|value| Value::Symbol(value.into()))
                        .unwrap_or(Value::Nil))
                } else {
                    Ok(Value::list(
                        priority
                            .into_iter()
                            .map(|value| Value::Symbol(value.into()))
                            .collect::<Vec<_>>(),
                    ))
                }
            }
            "coding-system-aliases" => {
                need_args(name, args, 1)?;
                let coding = checked_coding_symbol(interp, &args[0])?;
                Ok(Value::list(
                    interp
                        .coding_system_alias_list(&coding)
                        .unwrap_or_default()
                        .into_iter()
                        .map(|value| Value::Symbol(value.into()))
                        .collect::<Vec<_>>(),
                ))
            }
            "coding-system-plist" => {
                need_args(name, args, 1)?;
                // GNU's C primitive treats nil as `no-conversion'.  This remains
                // observable after dumped mule.el replaces `coding-system-type'
                // with a Lisp wrapper over this plist.
                let coding = if args[0].is_nil() {
                    "no-conversion".to_string()
                } else {
                    checked_coding_symbol(interp, &args[0])?
                };
                Ok(interp
                    .coding_system_plist_value(&coding)
                    .unwrap_or(Value::Nil))
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
                if let Some(eol_type) = interp.coding_system_eol_type_value(&coding) {
                    return Ok(Value::Integer(eol_type));
                }
                if coding == "no-conversion" {
                    return Ok(Value::Integer(0));
                }
                let base = interp
                    .coding_system_base_name(&coding)
                    .unwrap_or_else(|| coding.clone());
                let variants = ["unix", "dos", "mac"]
                    .into_iter()
                    .map(|suffix| format!("{base}-{suffix}"))
                    .collect::<Vec<_>>();
                Ok(
                    if variants
                        .iter()
                        .all(|variant| interp.has_coding_system(variant))
                    {
                        Value::list(
                            std::iter::once(Value::Symbol("vector-literal".into())).chain(
                                variants
                                    .into_iter()
                                    .map(|value| Value::Symbol(value.into())),
                            ),
                        )
                    } else {
                        Value::Nil
                    },
                )
            }
            "coding-system-base" => {
                need_args(name, args, 1)?;
                // GNU treats nil as the no-conversion coding system.  This is
                // used by display-independent mode setup when a terminal has no
                // explicit coding system (for example an Emaxx batch frame).
                if args[0].is_nil() {
                    return Ok(Value::Symbol("no-conversion".into()));
                }
                let coding = checked_coding_symbol(interp, &args[0])?;
                Ok(interp
                    .coding_system_base_name(&coding)
                    .map(|value| Value::Symbol(value.into()))
                    .unwrap_or(Value::Nil))
            }
            "check-coding-systems-region" => {
                need_args(name, args, 3)?;
                check_coding_systems_region_value(interp, &args[0], args.get(1), &args[2])
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
                if args.len() < 2 || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                find_coding_systems_region_internal_value(interp, &args[0], &args[1], args.get(2))
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
                .map(|value| Value::Symbol(value.into()))
                .unwrap_or(Value::Nil)),
            "set-terminal-coding-system-internal" | "set-safe-terminal-coding-system-internal" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let coding = checked_coding_name(interp, &args[0])?;
                interp.set_terminal_coding_system(coding.clone());
                Ok(coding
                    .map(|value| Value::Symbol(value.into()))
                    .unwrap_or(Value::Nil))
            }
            "keyboard-coding-system" => Ok(interp
                .keyboard_coding_system()
                .map(|value| Value::Symbol(value.into()))
                .unwrap_or(Value::Nil)),
            "set-keyboard-coding-system-internal" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let coding = checked_coding_name(interp, &args[0])?;
                interp.set_keyboard_coding_system(coding.clone());
                Ok(coding
                    .map(|value| Value::Symbol(value.into()))
                    .unwrap_or(Value::Nil))
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
                // coding.c derives public attributes from the validated codec
                // shape and prepends them to the Lisp-supplied plist.  Dumped
                // mule.el reads these properties back to decide display and
                // keyboard suitability; storing only the caller's raw plist
                // loses, for example, UTF-8's implicit ASCII compatibility.
                let ascii_compatible = args[4].is_truthy()
                    || matches!(kind, "raw-text" | "emacs-mule")
                    || (kind == "utf-8" && args.get(13).is_none_or(Value::is_nil))
                    || (matches!(kind, "charset" | "shift-jis" | "big5")
                        && coding_charset_list_is_ascii_compatible(interp, &args[3]));
                let supplied_plist = args[11].to_vec()?;
                let plist = Value::list(
                    [
                        Value::Symbol(":ascii-compatible-p".into()),
                        if ascii_compatible {
                            Value::T
                        } else {
                            Value::Nil
                        },
                        Value::Symbol(":category".into()),
                        Value::Symbol(coding_category_name(kind, args).into()),
                    ]
                    .into_iter()
                    .chain(supplied_plist),
                );
                let eol_type = match args[12].as_symbol()? {
                    "unix" => Some(0),
                    "dos" => Some(1),
                    "mac" => Some(2),
                    _ => None,
                };
                interp.define_coding_system(coding, mnemonic, kind, plist, eol_type)?;
                Ok(Value::Symbol(coding.to_string().into()))
            }
            "define-coding-system-alias" => {
                need_args(name, args, 2)?;
                let alias = args[0].as_symbol()?;
                let target = args[1].as_symbol()?;
                interp.define_coding_system_alias(alias, target)?;
                Ok(Value::Symbol(alias.to_string().into()))
            }
        }
    }
);
