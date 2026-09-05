use super::*;
use crate::lisp::primitives::string_like;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;

thread_local! {
    static SEMANTIC_CPP_INCLUDE_TAG_CACHE: RefCell<HashMap<PathBuf, Vec<Value>>> =
        RefCell::new(HashMap::new());
}

fn map_keymap_direct_value(
    interp: &mut Interpreter,
    function: &Value,
    keymap: &Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let full_table = keymap_char_table_value(interp, keymap).and_then(|table| match table {
        Value::CharTable(id) => Some(id),
        _ => None,
    });
    // GNU stores a full keymap's character bindings in ONE place (the
    // char-table), and keymap.c map_keymap_internal walks that store once,
    // with map_char_table reporting maximal ranges of equal values.  Emaxx
    // keeps a char-table facade AND direct bindings for the same keys, so
    // the walk must merge the two stores into one segment list -- reporting
    // each binding once -- instead of walking both (describe-map printed
    // every char range twice through keymap-canonicalize's map-keymap).
    let mut segments: Vec<(i64, i64, Value)> = Vec::new();
    if let Some(table_id) = full_table {
        for entry in interp
            .char_table_effective_ranges(table_id)
            .unwrap_or_default()
        {
            segments.push((i64::from(entry.start), i64::from(entry.end), entry.value));
        }
    }
    segments.sort_by_key(|(start, _, _)| *start);

    let bindings = keymap_direct_bindings(interp, keymap)?;
    let mut character_bindings = Vec::new();
    let mut sparse_bindings = Vec::new();
    for binding in bindings.iter() {
        let event = keymap_entry_key_value(&binding_key_parts(binding), &binding.key);
        if full_table.is_some()
            && let Value::Integer(code) = event
        {
            character_bindings.push((code, binding.value.clone()));
        } else {
            sparse_bindings.push((event, binding.value.clone()));
        }
    }
    // A direct character binding duplicating its char-table facade entry is
    // the same stored binding seen through the second store; only bindings
    // the char-table does not carry are additional.
    character_bindings.retain(|(code, value)| {
        !segments.iter().any(|(start, end, stored)| {
            start <= code && code <= end && values_equal(interp, stored, value)
        })
    });
    for (code, value) in character_bindings {
        segments.push((code, code, value));
    }
    segments.sort_by_key(|(start, _, _)| *start);
    let mut index = 0;
    while index < segments.len() {
        let (start, mut end, value) = segments[index].clone();
        while let Some((next_start, next_end, next_value)) = segments.get(index + 1) {
            if *next_start != end.saturating_add(1) || !values_equal(interp, &value, next_value) {
                break;
            }
            end = *next_end;
            index += 1;
        }
        let event = if start == end {
            Value::Integer(start)
        } else {
            Value::cons(Value::Integer(start), Value::Integer(end))
        };
        interp.call_function_value(function.clone(), None, &[event, value], env)?;
        index += 1;
    }
    for (event, value) in sparse_bindings {
        interp.call_function_value(function.clone(), None, &[event, value], env)?;
    }
    Ok(())
}

fn map_keymap_value(
    interp: &mut Interpreter,
    function: &Value,
    keymap: &Value,
    env: &mut Env,
    visited: &mut std::collections::HashSet<(bool, usize)>,
) -> Result<(), LispError> {
    let Some(identity) = keymap_value_identity(interp, keymap) else {
        return Ok(());
    };
    if !visited.insert(identity) {
        return Ok(());
    }

    map_keymap_direct_value(interp, function, keymap, env)?;
    for parent in keymap_parent_values(interp, keymap) {
        map_keymap_value(interp, function, &parent, env, visited)?;
    }
    Ok(())
}

fn vector_index_description(index: u32) -> Result<String, LispError> {
    key_sequence_binding_text(&Value::list([
        Value::Symbol("vector-literal".into()),
        Value::Integer(i64::from(index)),
    ]))
}

fn insert_description_indent(interp: &mut Interpreter, column: usize) {
    if column >= 16 {
        interp.insert_current_buffer(" ");
        return;
    }
    let mut current = column;
    while current < 16 {
        interp.insert_current_buffer("\t");
        current = (current / 8 + 1) * 8;
    }
}

fn describe_vector_value(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_arg_range("describe-vector", args, 1, 2)?;
    let mut ranges = Vec::<(u32, u32, Value)>::new();
    match &args[0] {
        Value::CharTable(table_id) => {
            for entry in interp
                .char_table_effective_ranges(*table_id)
                .unwrap_or_default()
            {
                if entry.value.is_nil() {
                    continue;
                }
                if let Some((_, end, previous)) = ranges.last_mut()
                    && end.saturating_add(1) == entry.start
                    && values_equal(interp, previous, &entry.value)
                {
                    *end = entry.end;
                } else {
                    ranges.push((entry.start, entry.end, entry.value));
                }
            }
        }
        vector if is_vector_value(vector) => {
            for (index, value) in vector_items(vector)?.into_iter().enumerate() {
                if value.is_nil() {
                    continue;
                }
                let index = u32::try_from(index)
                    .map_err(|_| LispError::Signal("Vector is too large".into()))?;
                if let Some((_, end, previous)) = ranges.last_mut()
                    && end.saturating_add(1) == index
                    && values_equal(interp, previous, &value)
                {
                    *end = index;
                } else {
                    ranges.push((index, index, value));
                }
            }
        }
        other => {
            return Err(LispError::TypeError(
                "vector-or-char-table-p".into(),
                other.type_name(),
            ));
        }
    }

    let describer = args
        .get(1)
        .filter(|value| !value.is_nil())
        .cloned()
        .unwrap_or_else(|| Value::Symbol("princ".into()));
    let output_buffer = Value::buffer(interp.current_buffer_id(), interp.buffer.name.clone());
    let restore = interp.bind_special_variable("standard-output", output_buffer, env)?;
    let mut result = (|| -> Result<Value, LispError> {
        let mut first = true;
        for (start, end, value) in ranges {
            let mut key = vector_index_description(start)?;
            if start != end {
                key.push_str(" .. ");
                key.push_str(&vector_index_description(end)?);
            }
            if first {
                interp.insert_current_buffer("\n");
                first = false;
            }
            interp.insert_current_buffer(&key);
            insert_description_indent(interp, key.chars().count());
            interp.call_function_value(describer.clone(), None, &[value], env)?;
            interp.insert_current_buffer("\n");
        }
        Ok(Value::Nil)
    })();
    if let Err(error) = interp.restore_special_binding(restore, env)
        && result.is_ok()
    {
        result = Err(error);
    }
    result
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut crate::lisp::types::Env,
    ) -> Result<Value, LispError> {
        match name {
            "define-key" => {
                need_arg_range(name, args, 3, 4)?;
                if let Ok(events) = vector_items(&args[1])
                    && let [event] = events.as_slice()
                    && let Some((Value::Integer(start), Value::Integer(end))) = event.cons_values()
                {
                    keymap_define_character_range(interp, &args[0], start, end, args[2].clone())?;
                    return Ok(args[2].clone());
                }
                // keymap.c:Fdefine_key converts Lucid-style event lists in
                // KEY through Fevent_convert_list, and a DEF vector opening
                // with a cons ("an XEmacs-style keyboard macro") gets the
                // same conversion, so `[(control meta shift kp-9)]' and
                // `[C-M-S-kp-9]' name one binding.
                let normalized_key = normalize_lucid_key_events(interp, &args[1], env)?;
                let def = normalize_xemacs_macro_definition(interp, &args[2])?;
                // keymap.c:define-key passes every symbolic vector event
                // through silly_event_symbol_error, whose first operation
                // is parse_modifiers.  Besides validation, that populates
                // the event-symbol-elements cache consumed by subr.el's
                // event-basic-type (notably for mouse-N events).
                if let Ok(events) = vector_items(&normalized_key) {
                    for event in events {
                        let bare_event = if event.is_symbol() {
                            Some(event)
                        } else if symbols_with_pos_enabled(interp, env) {
                            symbol_with_pos_parts(interp, &event).map(|(symbol, _)| symbol)
                        } else {
                            None
                        };
                        if let Some(bare_event) = bare_event {
                            parse_event_symbol_modifiers(interp, &bare_event)?;
                        }
                    }
                }
                let key = key_sequence_binding_text(&normalized_key)?;
                let key_parts = key_sequence_keymap_parts(&normalized_key)?;
                if def.is_nil() && args.get(3).is_some_and(Value::is_truthy) {
                    keymap_remove_binding(interp, &args[0], &key)?;
                } else {
                    keymap_define_binding_with_placement(
                        interp,
                        &args[0],
                        &key,
                        Some(key_parts),
                        def.clone(),
                        false,
                    )?;
                }
                Ok(def)
            }
            "lookup-key" => {
                need_arg_range(name, args, 2, 3)?;
                // keymap.c:lookup_key_1 applies the same Lucid event-list
                // conversion as Fdefine_key.
                let normalized_key = normalize_lucid_key_events(interp, &args[1], env)?;
                let key_parts = key_sequence_keymap_parts(&normalized_key)?;
                let result = keymap_lookup_sequence_value_with_default(
                    interp,
                    &args[0],
                    &key_parts,
                    args.get(2).is_some_and(Value::is_truthy),
                    env,
                )?;
                if let Value::Integer(prefix_len) = result {
                    let prefix_len = usize::try_from(prefix_len).unwrap_or(0);
                    Ok(Value::Integer(
                        key_sequence_prefix_event_count(&normalized_key, prefix_len)? as i64,
                    ))
                } else {
                    Ok(result)
                }
            }
            "accessible-keymaps" => accessible_keymaps(interp, args, env),
            "current-minor-mode-maps" => {
                need_args(name, args, 0)?;
                Ok(Value::list(
                    active_minor_mode_bindings(interp, env)?
                        .into_iter()
                        .map(|(_, map)| map),
                ))
            }
            "minor-mode-key-binding" => {
                need_arg_range(name, args, 1, 2)?;
                let key_parts = key_sequence_keymap_parts(&args[0])?;
                let accept_default = args.get(1).is_some_and(Value::is_truthy);
                let mut prefix_bindings = Vec::new();
                for (mode, map) in active_minor_mode_bindings(interp, env)? {
                    let binding = keymap_lookup_sequence_value_with_default(
                        interp,
                        &map,
                        &key_parts,
                        accept_default,
                        env,
                    )?;
                    if binding.is_nil() || matches!(binding, Value::Integer(_)) {
                        continue;
                    }
                    let entry = Value::cons(Value::Symbol(mode.into()), binding.clone());
                    if keymap_reference_map(interp, &binding, env).is_some() {
                        prefix_bindings.push(entry);
                    } else if prefix_bindings.is_empty() {
                        return Ok(Value::list([entry]));
                    }
                }
                Ok(Value::list(prefix_bindings))
            }
            "keymap--get-keyelt" => {
                need_args(name, args, 2)?;
                keymap_get_keyelt(interp, &args[0], args[1].is_truthy(), env)
            }
            "describe-buffer-bindings" => describe_buffer_bindings(interp, args, env),
            "help--describe-vector" => help_describe_vector(interp, args, env),
            "key-binding" => {
                need_arg_range(name, args, 1, 4)?;
                let key_parts = key_sequence_keymap_parts(&args[0])?;
                key_binding_with_parts(
                    interp,
                    &key_parts,
                    args.get(1).is_some_and(Value::is_truthy),
                    args.get(2).is_some_and(Value::is_truthy),
                    env,
                )
            }
            "keymap-prompt" => {
                need_args(name, args, 1)?;
                if let Some(id) = keymap_record_id(interp, &args[0]) {
                    return Ok(interp
                        .find_record(id)
                        .and_then(|record| record.slots.first().cloned())
                        .unwrap_or(Value::Nil));
                }
                if let Ok(items) = args[0].to_vec()
                    && matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "keymap")
                {
                    return Ok(items
                        .iter()
                        .skip(1)
                        .find(|item| string_like(item).is_some())
                        .cloned()
                        .unwrap_or(Value::Nil));
                }
                Err(LispError::WrongTypeArgument(
                    "keymapp".into(),
                    args[0].clone(),
                ))
            }
            "command-remapping" => {
                need_arg_range(name, args, 1, 3)?;
                command_remapping(interp, &args[0], args.get(2), env)
            }
            "keymap-parent" => {
                need_args(name, args, 1)?;
                match &args[0] {
                    Value::Record(id)
                        if interp.find_record(*id).is_some_and(|record| {
                            record.kind == crate::lisp::eval::RecordKind::Keymap
                        }) =>
                    {
                        Ok(interp
                            .find_record(*id)
                            .and_then(|record| record.slots.get(KEYMAP_PARENT_SLOT).cloned())
                            .unwrap_or(Value::Nil))
                    }
                    _ => Ok(Value::Nil),
                }
            }
            "set-keymap-parent" => {
                need_args(name, args, 2)?;
                if let Value::Record(id) = &args[0]
                    && let Some(record) = interp.find_record_mut(*id)
                    && record.kind == crate::lisp::eval::RecordKind::Keymap
                {
                    if record.slots.len() <= KEYMAP_PARENT_SLOT {
                        record.slots.resize(KEYMAP_PARENT_SLOT + 1, Value::Nil);
                    }
                    record.slots[KEYMAP_PARENT_SLOT] = args[1].clone();
                    refresh_runtime_keymap_public_view(interp, *id)?;
                }
                Ok(Value::Nil)
            }
            "map-keymap" => {
                need_arg_range(name, args, 2, 3)?;
                if args.get(2).is_some_and(Value::is_truthy) {
                    return interp.call_function_value(
                        Value::Symbol("map-keymap-sorted".into()),
                        Some("map-keymap-sorted"),
                        &args[..2],
                        env,
                    );
                }
                // GNU map-keymap also reports bindings inherited from parent
                // keymaps (the parent is spliced into the keymap's tail).
                let mut visited = std::collections::HashSet::new();
                map_keymap_value(interp, &args[0], &args[1], env, &mut visited)?;
                Ok(Value::Nil)
            }
            "map-keymap-internal" => {
                need_args(name, args, 2)?;
                if !is_keymap_value(interp, &args[1]) {
                    return Err(LispError::WrongTypeArgument(
                        "keymapp".into(),
                        args[1].clone(),
                    ));
                }
                map_keymap_direct_value(interp, &args[0], &args[1], env)?;
                Ok(keymap_parent_values(interp, &args[1])
                    .into_iter()
                    .next()
                    .unwrap_or(Value::Nil))
            }
            "describe-vector" => describe_vector_value(interp, args, env),
            "use-local-map" => {
                need_args(name, args, 1)?;
                if !args[0].is_nil() && !is_keymap_value(interp, &args[0]) {
                    return Err(LispError::WrongTypeArgument(
                        "keymapp".into(),
                        args[0].clone(),
                    ));
                }
                interp.set_buffer_local_value(
                    interp.current_buffer_id(),
                    "current-local-map",
                    args[0].clone(),
                );
                Ok(Value::Nil)
            }
            "use-global-map" => {
                need_args(name, args, 1)?;
                if !is_keymap_value(interp, &args[0]) {
                    return Err(LispError::WrongTypeArgument(
                        "keymapp".into(),
                        args[0].clone(),
                    ));
                }
                interp.set_current_global_map_value(args[0].clone());
                Ok(Value::Nil)
            }
            "current-local-map" => {
                need_args(name, args, 0)?;
                Ok(interp
                    .lookup_var("current-local-map", env)
                    .unwrap_or(Value::Nil))
            }
            "current-global-map" => {
                need_args(name, args, 0)?;
                Ok(interp.current_global_map_value())
            }
            "widget-get" => {
                need_args(name, args, 2)?;
                widget_get(interp, &args[0], &args[1])
            }
            "widget-put" => {
                need_args(name, args, 3)?;
                widget_put(interp, &args[0], &args[1], args[2].clone())
            }
            "widget-apply" => {
                need_arg_range(name, args, 2, usize::MAX)?;
                let function = widget_get(interp, &args[0], &args[1])?;
                if function.is_nil() {
                    return Ok(Value::Nil);
                }
                let mut call_args = Vec::with_capacity(args.len());
                call_args.push(args[0].clone());
                call_args.extend_from_slice(&args[2..]);
                interp.call_function_value(function, args[1].as_symbol().ok(), &call_args, env)
            }
            "symbol-function" => {
                need_args(name, args, 1)?;
                // GNU 30.2 data.c:Fsymbol_function uses
                // CHECK_SYMBOL/XSYMBOL, sharing the positioned-symbol
                // contract with `fboundp'.
                let symbol = checked_symbol_name(interp, &args[0], env)?;
                Ok(match interp.logical_function_binding(&symbol, env) {
                    Some(value) => value,
                    None if is_special_form_name(&symbol) => {
                        Value::BuiltinFunc(symbol.clone().into())
                    }
                    // GNU returns nil for an unbound function cell (nadvice's
                    // pending-advice path reads it).
                    None => Value::Nil,
                })
            }
            "symbol-name" => {
                need_args(name, args, 1)?;
                // GNU 30.2 data.c:Fsymbol_name uses CHECK_SYMBOL/XSYMBOL.
                let symbol_name = match &args[0] {
                    Value::Nil => {
                        return Ok(crate::lisp::types::SymbolName::from("nil").lisp_name());
                    }
                    Value::T => return Ok(crate::lisp::types::SymbolName::from("t").lisp_name()),
                    Value::Symbol(symbol) => symbol.clone(),
                    _ if symbols_with_pos_enabled(interp, env) => {
                        match symbol_with_pos_parts(interp, &args[0]) {
                            Some((Value::Nil, _)) => {
                                return Ok(crate::lisp::types::SymbolName::from("nil").lisp_name());
                            }
                            Some((Value::T, _)) => {
                                return Ok(crate::lisp::types::SymbolName::from("t").lisp_name());
                            }
                            Some((Value::Symbol(symbol), _)) => symbol,
                            _ => return Err(wrong_type_argument("symbolp", args[0].clone())),
                        }
                    }
                    _ => return Err(wrong_type_argument("symbolp", args[0].clone())),
                };
                Ok(symbol_name.lisp_name())
            }
            "user-login-name" => {
                if args.len() > 1 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                if args.first().is_none_or(Value::is_nil) {
                    return Ok(interp
                        .lookup_var("user-login-name", env)
                        .unwrap_or_else(|| {
                            Value::String(
                                current_user_login_name()
                                    .unwrap_or_else(|| "user".into())
                                    .into(),
                            )
                        }));
                }
                let uid = legacy_unsigned_id(&args[0])?;
                Ok(user_name_from_uid(uid)
                    .map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Nil))
            }
            "user-real-login-name" => {
                need_args(name, args, 0)?;
                Ok(interp
                    .lookup_var("user-real-login-name", env)
                    .unwrap_or_else(|| {
                        Value::String(
                            current_real_user_login_name()
                                .unwrap_or_else(|| "user".into())
                                .into(),
                        )
                    }))
            }
            "system-name" => {
                if !args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                Ok(Value::String(system_name_value().into()))
            }
            "user-full-name" => {
                if args.len() > 1 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let Some(requested) = args.first().filter(|value| !value.is_nil()) else {
                    return Ok(interp.lookup_var("user-full-name", env).unwrap_or_else(|| {
                        Value::String(
                            current_user_full_name()
                                .unwrap_or_else(|| "unknown".into())
                                .into(),
                        )
                    }));
                };
                let full_name = match requested {
                    Value::Integer(_) | Value::BigInteger(_) | Value::Float(_) => {
                        user_full_name_from_uid(legacy_unsigned_id(requested)?)
                    }
                    Value::String(_) | Value::StringObject(_) => {
                        let login = string_text(requested)?;
                        user_full_name(Some(&login))
                    }
                    _ => return Err(LispError::Signal("Invalid UID specification".into())),
                };
                Ok(full_name
                    .map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Nil))
            }

            "text-quoting-style" => {
                need_args(name, args, 0)?;
                Ok(Value::Symbol(
                    effective_text_quoting_style(interp, env).into(),
                ))
            }
            "user-uid" => Ok(Value::Integer(current_user_id()? as i64)),
            "user-real-uid" => Ok(Value::Integer(current_real_user_id()? as i64)),
            "group-gid" => Ok(Value::Integer(current_group_id()? as i64)),
            "group-real-gid" => Ok(Value::Integer(current_real_group_id()? as i64)),
            "group-name" => {
                need_args(name, args, 1)?;
                if !matches!(
                    args[0],
                    Value::Integer(_) | Value::BigInteger(_) | Value::Float(_) | Value::Cons(_)
                ) {
                    return Err(LispError::Signal("Invalid GID specification".into()));
                }
                let gid = legacy_unsigned_id(&args[0])?;
                Ok(group_name_from_gid(i64::from(gid))?
                    .map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Nil))
            }
            "lock-buffer" => {
                maybe_lock_current_buffer(interp, env)?;
                Ok(Value::Nil)
            }
            "unlock-buffer" => unlock_current_buffer(interp, env),

            "recent-auto-save-p" => {
                need_args(name, args, 0)?;
                Ok(if interp.buffer.is_autosaved() {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "set-buffer-auto-saved" => {
                need_args(name, args, 0)?;
                interp.buffer.set_autosaved();
                Ok(Value::Nil)
            }
            "clear-buffer-auto-save-failure" => {
                need_args(name, args, 0)?;
                Ok(Value::Nil)
            }
            "next-read-file-uses-dialog-p" => {
                need_args(name, args, 0)?;
                let use_dialog = interp
                    .lookup_var("use-dialog-box", env)
                    .is_some_and(|value| value.is_truthy());
                let use_file_dialog = interp
                    .lookup_var("use-file-dialog", env)
                    .is_some_and(|value| value.is_truthy());
                Ok(if use_dialog && use_file_dialog {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "do-auto-save" => {
                let path = interp
                    .buffer_local_value(interp.current_buffer_id(), "buffer-auto-save-file-name")
                    .and_then(|value| string_text(&value).ok())
                    .unwrap_or_else(|| auto_save_path_for_buffer(&interp.buffer));
                std::fs::write(&path, interp.buffer.buffer_string())
                    .map_err(|e| LispError::Signal(e.to_string()))?;
                interp.set_buffer_local_value(
                    interp.current_buffer_id(),
                    "buffer-auto-save-file-name",
                    Value::String(path.into()),
                );
                interp.buffer.set_autosaved();
                Ok(Value::Nil)
            }
            "unix-sync" => {
                need_args(name, args, 0)?;
                Ok(Value::Nil)
            }
            "set-binary-mode" => {
                need_args(name, args, 2)?;
                match &args[0] {
                    Value::Symbol(stream)
                        if matches!(stream.as_str(), "stdin" | "stdout" | "stderr") =>
                    {
                        Ok(Value::Nil)
                    }
                    _ => Err(LispError::Signal("Invalid stream".into())),
                }
            }
            "obarray-make" => {
                if args.len() > 1 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                if let Some(size) = args.first().filter(|size| !size.is_nil()) {
                    let size_value = size.as_fixnum()?;
                    if size_value < 0 {
                        return Err(LispError::WrongTypeArgument(
                            "wholenump".into(),
                            size.clone(),
                        ));
                    }
                }
                Ok(make_obarray(interp))
            }
            "obarrayp" => {
                need_args(name, args, 1)?;
                Ok(if is_obarray_like_value(interp, &args[0]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "obarray-clear" => {
                need_args(name, args, 1)?;
                clear_obarray(interp, &args[0])
            }
            "internal--obarray-buckets" => {
                need_args(name, args, 1)?;
                Ok(Value::list(
                    obarray_symbols(interp, &args[0])?
                        .into_iter()
                        .map(|symbol| Value::list([symbol])),
                ))
            }
            "define-hash-table-test" => {
                need_args(name, args, 3)?;
                let symbol = args[0].as_symbol()?;
                let spec = Value::list([args[1].clone(), args[2].clone()]);
                interp.put_symbol_property(symbol, "hash-table-test", spec.clone());
                Ok(spec)
            }
            "make-hash-table" => {
                let mut test = "eql".to_string();
                // fns.c:DEFAULT_HASH_SIZE is zero.  Storage is allocated on
                // the first insertion via maybe_resize_hash_table.
                let mut size = Value::Integer(0);
                let mut weakness = Value::Nil;
                // fns.c still accepts `:purecopy'; print.c:2609 reports it
                // back, so the flag has to be recorded rather than dropped.
                let mut purecopy = Value::Nil;
                let mut index = 0usize;
                while index + 1 < args.len() {
                    let key = args[index].as_symbol()?;
                    match key {
                        ":test" => {
                            test = match &args[index + 1] {
                                Value::Symbol(name) => name.to_string(),
                                Value::BuiltinFunc(name) => name.to_string(),
                                other => {
                                    return Err(LispError::WrongTypeArgument(
                                        "symbolp".into(),
                                        other.clone(),
                                    ));
                                }
                            };
                        }
                        ":size" => size = args[index + 1].clone(),
                        // fns.c accepts these obsolete keyword/value pairs
                        // but deliberately ignores their values.
                        ":rehash-size" | ":rehash-threshold" => {}
                        ":weakness" => {
                            weakness = match &args[index + 1] {
                                Value::T => Value::Symbol("key-and-value".into()),
                                other => other.clone(),
                            };
                        }
                        ":purecopy" => purecopy = args[index + 1].clone(),
                        _ => {
                            return Err(LispError::Signal(format!(
                                "Invalid hash table parameter: {key}"
                            )));
                        }
                    }
                    index += 2;
                }
                if !matches!(test.as_str(), "eq" | "eql" | "equal")
                    && hash_table_user_test_functions(interp, &test).is_none()
                {
                    return Err(LispError::Signal("Invalid hash table test".into()));
                }
                let capacity = size
                    .as_integer()
                    .ok()
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or_else(|| {
                        LispError::WrongTypeArgument("wholenump".into(), size.clone())
                    })?;
                let table =
                    json::make_hash_table_with_capacity(interp, &test, Vec::new(), capacity);
                let Value::Record(id) = table.clone() else {
                    unreachable!("hash tables are represented as records")
                };
                let record = interp
                    .find_record_mut(id)
                    .expect("make_hash_table should create a record");
                if record.slots.len() < 7 {
                    record.slots.resize(7, Value::Nil);
                }
                record.slots[2] = size;
                record.slots[5] = weakness;
                record.slots[6] = purecopy;
                Ok(table)
            }
            "hash-table-p" => {
                need_args(name, args, 1)?;
                Ok(if json::is_hash_table(interp, &args[0]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "copy-hash-table" => {
                need_args(name, args, 1)?;
                let Value::Record(id) = args[0] else {
                    return Err(LispError::WrongTypeArgument(
                        "hash-table-p".into(),
                        args[0].clone(),
                    ));
                };
                let Some(record) = interp.find_record(id) else {
                    return Err(LispError::WrongTypeArgument(
                        "hash-table-p".into(),
                        args[0].clone(),
                    ));
                };
                if record.kind != crate::lisp::eval::RecordKind::HashTable {
                    return Err(LispError::WrongTypeArgument(
                        "hash-table-p".into(),
                        args[0].clone(),
                    ));
                }
                let copy = interp.copy_record(id)?;
                if let Value::Record(copy_id) = copy {
                    interp.reindex_hash_table_runtime_entries_in_env(copy_id, env);
                    Ok(Value::Record(copy_id))
                } else {
                    Ok(copy)
                }
            }
            "gethash" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let default = args.get(2).cloned().unwrap_or(Value::Nil);
                if let Value::Record(id) = &args[1]
                    && let Some(value) = interp.equal_hash_lookup(*id, &args[0], env)
                {
                    return Ok(value.unwrap_or(default));
                }
                if let Value::Record(id) = &args[1]
                    && interp.has_custom_hash_table_index(*id)
                {
                    let test = interp
                        .find_record(*id)
                        .and_then(|record| record.slots.first())
                        .and_then(|value| value.as_symbol().ok())
                        .ok_or_else(|| LispError::Signal("Invalid hash table test".into()))?
                        .to_string();
                    return Ok(custom_hash_lookup_indexed(
                        interp, &args[1], *id, &test, &args[0], env,
                    )?
                    .unwrap_or(default));
                }
                let Some((test, entries)) = json::hash_table_entries(interp, &args[1]) else {
                    return Err(LispError::WrongTypeArgument(
                        "hash-table-p".into(),
                        args[1].clone(),
                    ));
                };
                for (existing_key, value) in entries {
                    if hash_table_key_matches(
                        interp,
                        &args[1],
                        &test,
                        &existing_key,
                        &args[0],
                        env,
                    )? {
                        return Ok(value);
                    }
                }
                Ok(default)
            }
            "puthash" => {
                need_args(name, args, 3)?;
                if let Value::Record(id) = &args[2]
                    && !interp.hash_table_is_mutable(*id)
                {
                    return Err(LispError::Signal("hash table test modifies table".into()));
                }
                if let Value::Record(id) = &args[2]
                    && interp.equal_hash_put(*id, args[0].clone(), args[1].clone(), env)
                {
                    return Ok(args[1].clone());
                }
                if let Value::Record(id) = &args[2]
                    && interp.has_custom_hash_table_index(*id)
                {
                    let test = interp
                        .find_record(*id)
                        .and_then(|record| record.slots.first())
                        .and_then(|value| value.as_symbol().ok())
                        .ok_or_else(|| LispError::Signal("Invalid hash table test".into()))?
                        .to_string();
                    if custom_hash_put_indexed(
                        interp,
                        &args[2],
                        *id,
                        &test,
                        args[0].clone(),
                        args[1].clone(),
                        env,
                    )? {
                        return Ok(args[1].clone());
                    }
                }
                let Some((test, mut entries)) = json::hash_table_entries(interp, &args[2]) else {
                    return Err(LispError::WrongTypeArgument(
                        "hash-table-p".into(),
                        args[2].clone(),
                    ));
                };
                touch_hash_table_key(interp, &args[2], &test, &args[0], env)?;
                let mut replaced = false;
                for (existing_key, existing_value) in &mut entries {
                    if hash_table_key_matches(interp, &args[2], &test, existing_key, &args[0], env)?
                    {
                        *existing_value = args[1].clone();
                        replaced = true;
                        break;
                    }
                }
                if !replaced {
                    entries.push((args[0].clone(), args[1].clone()));
                }
                set_hash_table_entries(interp, &args[2], entries)?;
                if let Value::Record(id) = args[2] {
                    interp.reindex_hash_table_runtime_entries_in_env(id, env);
                }
                Ok(args[1].clone())
            }
            "maphash" => {
                need_args(name, args, 2)?;
                if let Value::Record(id) = &args[1]
                    && interp.hash_table_entry_at_or_after(*id, 0).is_some()
                {
                    let mut slot = 0;
                    loop {
                        let Some(capacity) = interp.gnu_hash_table_capacity(*id) else {
                            return Err(LispError::WrongTypeArgument(
                                "hash-table-p".into(),
                                args[1].clone(),
                            ));
                        };
                        if slot >= capacity {
                            break;
                        }
                        let Some((entry_slot, key, value)) =
                            interp.hash_table_entry_at_or_after(*id, slot).flatten()
                        else {
                            break;
                        };
                        if entry_slot >= capacity {
                            break;
                        }
                        slot = entry_slot + 1;
                        call_function_value(interp, &args[0], &[key, value], env)?;
                    }
                    return Ok(Value::Nil);
                }
                let Some((_, entries)) = json::hash_table_entries(interp, &args[1]) else {
                    return Err(LispError::WrongTypeArgument(
                        "hash-table-p".into(),
                        args[1].clone(),
                    ));
                };
                for (key, value) in entries {
                    call_function_value(interp, &args[0], &[key, value], env)?;
                }
                Ok(Value::Nil)
            }
            "remhash" => {
                need_args(name, args, 2)?;
                if let Value::Record(id) = &args[1]
                    && !interp.hash_table_is_mutable(*id)
                {
                    return Err(LispError::Signal("hash table test modifies table".into()));
                }
                if let Value::Record(id) = &args[1]
                    && interp.equal_hash_remove(*id, &args[0], env).is_some()
                {
                    return Ok(Value::Nil);
                }
                if let Value::Record(id) = &args[1]
                    && interp.has_custom_hash_table_index(*id)
                {
                    let test = interp
                        .find_record(*id)
                        .and_then(|record| record.slots.first())
                        .and_then(|value| value.as_symbol().ok())
                        .ok_or_else(|| LispError::Signal("Invalid hash table test".into()))?
                        .to_string();
                    if custom_hash_remove_indexed(interp, &args[1], *id, &test, &args[0], env)? {
                        return Ok(Value::Nil);
                    }
                }
                let Some((test, entries)) = json::hash_table_entries(interp, &args[1]) else {
                    return Err(LispError::WrongTypeArgument(
                        "hash-table-p".into(),
                        args[1].clone(),
                    ));
                };
                let mut retained = Vec::new();
                for (existing_key, value) in entries {
                    if !hash_table_key_matches(
                        interp,
                        &args[1],
                        &test,
                        &existing_key,
                        &args[0],
                        env,
                    )? {
                        retained.push((existing_key, value));
                    }
                }
                set_hash_table_entries(interp, &args[1], retained)?;
                if let Value::Record(id) = args[1] {
                    interp.reindex_hash_table_runtime_entries_in_env(id, env);
                }
                Ok(Value::Nil)
            }
            "clrhash" => {
                need_args(name, args, 1)?;
                if let Value::Record(id) = &args[0]
                    && !interp.hash_table_is_mutable(*id)
                {
                    return Err(LispError::Signal("hash table test modifies table".into()));
                }
                if json::hash_table_entries(interp, &args[0]).is_none() {
                    return Err(LispError::WrongTypeArgument(
                        "hash-table-p".into(),
                        args[0].clone(),
                    ));
                }
                if let Value::Record(id) = &args[0]
                    && interp.clear_custom_hash_table(*id)
                {
                    return Ok(args[0].clone());
                }
                set_hash_table_entries(interp, &args[0], Vec::new())?;
                Ok(args[0].clone())
            }
            "hash-table-count" => {
                need_args(name, args, 1)?;
                let Some((_, entries)) = json::hash_table_entries(interp, &args[0]) else {
                    return Err(LispError::WrongTypeArgument(
                        "hash-table-p".into(),
                        args[0].clone(),
                    ));
                };
                Ok(Value::Integer(entries.len() as i64))
            }
            "hash-table-rehash-size" => {
                need_args(name, args, 1)?;
                if !json::is_hash_table(interp, &args[0]) {
                    return Err(LispError::WrongTypeArgument(
                        "hash-table-p".into(),
                        args[0].clone(),
                    ));
                }
                Ok(Value::float(1.5))
            }
            "hash-table-rehash-threshold" => {
                need_args(name, args, 1)?;
                if !json::is_hash_table(interp, &args[0]) {
                    return Err(LispError::WrongTypeArgument(
                        "hash-table-p".into(),
                        args[0].clone(),
                    ));
                }
                Ok(Value::float(0.8125))
            }
            "hash-table-size" => {
                need_args(name, args, 1)?;
                let Value::Record(id) = args[0] else {
                    return Err(LispError::WrongTypeArgument(
                        "hash-table-p".into(),
                        args[0].clone(),
                    ));
                };
                let capacity = interp.gnu_hash_table_capacity(id).ok_or_else(|| {
                    LispError::WrongTypeArgument("hash-table-p".into(), args[0].clone())
                })?;
                Ok(Value::Integer(capacity as i64))
            }
            "hash-table-test" => {
                need_args(name, args, 1)?;
                Ok(hash_table_metadata_slot(
                    interp,
                    &args[0],
                    0,
                    Value::Symbol("eql".into()),
                )?)
            }
            "hash-table-weakness" => {
                need_args(name, args, 1)?;
                Ok(hash_table_metadata_slot(interp, &args[0], 5, Value::Nil)?)
            }
            "try-completion" => try_completion(interp, args, env),
            "all-completions" => all_completions(interp, args, env),
            "test-completion" => test_completion(interp, args, env),
            "internal-complete-buffer" => internal_complete_buffer(interp, args, env),
            "internal--hash-table-index-size" => {
                need_args(name, args, 1)?;
                let Value::Record(id) = args[0] else {
                    return Err(LispError::WrongTypeArgument(
                        "hash-table-p".into(),
                        args[0].clone(),
                    ));
                };
                let capacity = interp.gnu_hash_table_capacity(id).ok_or_else(|| {
                    LispError::WrongTypeArgument("hash-table-p".into(), args[0].clone())
                })?;
                Ok(Value::Integer(
                    crate::lisp::eval::gnu_hash_table_index_slots(capacity) as i64,
                ))
            }
            "internal--hash-table-histogram" => {
                need_args(name, args, 1)?;
                if json::hash_table_entries(interp, &args[0]).is_none() {
                    return Err(LispError::WrongTypeArgument(
                        "hash-table-p".into(),
                        args[0].clone(),
                    ));
                }
                Ok(Value::Nil)
            }
            "internal--hash-table-buckets" => {
                need_args(name, args, 1)?;
                let Some((_, entries)) = json::hash_table_entries(interp, &args[0]) else {
                    return Err(LispError::WrongTypeArgument(
                        "hash-table-p".into(),
                        args[0].clone(),
                    ));
                };
                Ok(Value::list(entries.into_iter().map(|(key, value)| {
                    Value::list([Value::cons(key, value)])
                })))
            }
            "profiler-memory-running-p" => Ok(if interp.profiler_memory_running {
                Value::T
            } else {
                Value::Nil
            }),
            "profiler-memory-start" => {
                if interp.profiler_memory_running {
                    return Err(LispError::Signal("Memory profiler already running".into()));
                }
                interp.profiler_memory_running = true;
                interp.profiler_memory_log_pending = true;
                // profiler.c Fprofiler_memory_start returns t.
                Ok(Value::T)
            }
            "profiler-memory-stop" => {
                let was_running = interp.profiler_memory_running;
                interp.profiler_memory_running = false;
                Ok(if was_running { Value::T } else { Value::Nil })
            }
            "profiler-memory-log" => {
                if interp.profiler_memory_running || interp.profiler_memory_log_pending {
                    if !interp.profiler_memory_running {
                        interp.profiler_memory_log_pending = false;
                    }
                    // GNU's log is a real hash table.  Emaxx collects no
                    // samples, so the honest log is a real EMPTY hash table
                    // of the same test -- never a string spelled to print
                    // like one (2026-08-23 audit finding 78).
                    call(
                        interp,
                        "make-hash-table",
                        &[Value::symbol(":test"), Value::symbol("equal")],
                        env,
                    )
                } else {
                    Ok(Value::Nil)
                }
            }
            "profiler-cpu-running-p" => Ok(if interp.profiler_cpu_running {
                Value::T
            } else {
                Value::Nil
            }),
            "profiler-cpu-start" => {
                if interp.profiler_cpu_running {
                    return Err(LispError::Signal("CPU profiler already running".into()));
                }
                interp.profiler_cpu_running = true;
                interp.profiler_cpu_log_pending = true;
                if let Some(interval) = args.first() {
                    interp.set_variable(
                        "profiler-sampling-interval",
                        interval.clone(),
                        &mut Vec::new(),
                    );
                }
                // profiler.c Fprofiler_cpu_start returns t.
                Ok(Value::T)
            }
            "profiler-cpu-stop" => {
                let was_running = interp.profiler_cpu_running;
                interp.profiler_cpu_running = false;
                Ok(if was_running { Value::T } else { Value::Nil })
            }
            "profiler-cpu-log" => {
                if interp.profiler_cpu_running || interp.profiler_cpu_log_pending {
                    if !interp.profiler_cpu_running {
                        interp.profiler_cpu_log_pending = false;
                    }
                    call(
                        interp,
                        "make-hash-table",
                        &[Value::symbol(":test"), Value::symbol("equal")],
                        env,
                    )
                } else {
                    Ok(Value::Nil)
                }
            }

            "funcall-with-delayed-message" => {
                need_args(name, args, 3)?;
                let timeout = numeric_to_f64(interp, &args[0])?;
                let delayed = string_text(&args[1])?;
                let callback = resolve_callable(interp, &args[2], env)?;
                let buffer_id = interp
                    .find_buffer("*Messages*")
                    .map(|(id, _)| id)
                    .unwrap_or_else(|| interp.create_buffer("*Messages*").0);
                let before = interp
                    .get_buffer_by_id(buffer_id)
                    .map(|buffer| buffer.buffer_string())
                    .unwrap_or_default();
                let start = Instant::now();
                let result = interp.call_function_value(callback, None, &[], env)?;
                let elapsed = start.elapsed().as_secs_f64();
                if elapsed >= timeout
                    && let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id)
                {
                    let current = buffer.buffer_string();
                    let suffix = current
                        .strip_prefix(&before)
                        .map(str::to_string)
                        .unwrap_or(current);
                    let rewritten = if suffix.is_empty() {
                        format!("{delayed}\n")
                    } else {
                        format!("{delayed}\n{suffix}")
                    };
                    let end = buffer.point_max();
                    let _ = buffer.delete_region(1, end);
                    buffer.goto_char(1);
                    buffer.insert(&(before + &rewritten));
                }
                Ok(result)
            }
            "handler-bind-1" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                if args.len().is_multiple_of(2) {
                    return Err(LispError::Signal(
                        "Trailing CONDITIONS without HANDLER in `handler-bind`".into(),
                    ));
                }
                let mut active = Vec::with_capacity(args.len() / 2);
                for pair in args[1..].as_chunks::<2>().0 {
                    let conditions = match &pair[0] {
                        Value::Nil => Vec::new(),
                        Value::Cons(_) => pair[0]
                            .to_vec()?
                            .iter()
                            .map(|condition| condition.as_symbol().map(str::to_string))
                            .collect::<Result<Vec<_>, _>>()?,
                        condition => vec![condition.as_symbol()?.to_string()],
                    };
                    if !conditions.is_empty() {
                        active.push((conditions, pair[1].clone()));
                    }
                }
                active.reverse();
                let handler_start = interp.push_handler_bindings(&active);
                let result = interp.call_function_value(args[0].clone(), None, &[], env);
                interp.pop_handler_bindings(handler_start);
                result
            }

            "mapbacktrace" => {
                need_arg_range(name, args, 1, 2)?;
                let callback = resolve_callable(interp, &args[0], env)?;
                let base = args.get(1).filter(|value| !value.is_nil());
                let frames = interp.backtrace_frames_snapshot();
                let start = match base {
                    Some(base) => {
                        let Some(start) = frames
                            .iter()
                            .position(|(_, function, _, _)| function == base)
                        else {
                            // GNU get_backtrace_starting_at treats a base with
                            // no live activation frame as an empty traversal.
                            return Ok(Value::Nil);
                        };
                        start
                    }
                    None => 0,
                };
                for (evald, function, frame_args, debug_on_exit) in frames.into_iter().skip(start) {
                    let flags = if debug_on_exit {
                        Value::list([Value::Symbol(":debug-on-exit".into()), Value::T])
                    } else {
                        Value::Nil
                    };
                    let evald = if evald { Value::T } else { Value::Nil };
                    interp.call_function_value(
                        callback.clone(),
                        None,
                        &[evald, function, Value::list(frame_args), flags],
                        env,
                    )?;
                }
                Ok(Value::Nil)
            }
            "backtrace-frame--internal" => {
                need_args(name, args, 3)?;
                let callback = resolve_callable(interp, &args[0], env)?;
                let mut frame_offset = args[1].as_integer()?;
                if frame_offset < 0 {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("wrong-type-argument".into()),
                        Value::Symbol("natnump".into()),
                        args[1].clone(),
                    ])));
                }
                let mut base = args[2].clone();
                if let Value::Cons(cell) = &base {
                    frame_offset += cell.car.borrow().as_integer()?;
                    let function = cell.cdr.borrow().clone();
                    base = function;
                }
                if frame_offset < 0 {
                    return Ok(Value::Nil);
                }
                let frames = interp.backtrace_frames_snapshot();
                let start = if base.is_nil() {
                    0
                } else {
                    let Some(index) = frames
                        .iter()
                        .position(|(_, function, _, _)| function == &base)
                    else {
                        return Ok(Value::Nil);
                    };
                    index
                };
                let Some((evald, function, frame_args, debug_on_exit)) =
                    frames.into_iter().nth(start + frame_offset as usize)
                else {
                    return Ok(Value::Nil);
                };
                let flags = if debug_on_exit {
                    Value::list([Value::Symbol(":debug-on-exit".into()), Value::T])
                } else {
                    Value::Nil
                };
                let evald = if evald { Value::T } else { Value::Nil };
                interp.call_function_value(
                    callback,
                    None,
                    &[evald, function, Value::list(frame_args), flags],
                    env,
                )
            }
            "backtrace-debug" => {
                need_arg_range(name, args, 2, 3)?;
                interp.set_current_backtrace_debug(args[1].is_truthy());
                Ok(Value::Nil)
            }
            "backtrace-eval" => {
                need_arg_range(name, args, 2, 3)?;
                let index = usize::try_from(args[1].as_integer()?).unwrap_or(0);
                let base = args.get(2).filter(|value| !value.is_nil());
                let context = interp.backtrace_frame_context_env(index, base);
                let shared_context = shared_env(context);
                // Treat the suspended frames as captured lexical cells while the
                // expression runs.  `setq' then records changes by frame
                // identity, and the resumed activation observes them.
                for frame in shared_context.borrow().iter() {
                    frame.mark_captured();
                }
                interp.eval(&args[0], &mut shared_context.borrow_mut())
            }
            "backtrace--locals" => {
                need_args(name, args, 2)?;
                let raw_index = args[0].as_integer()?;
                if raw_index < 0 {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("wrong-type-argument".into()),
                        Value::Symbol("natnump".into()),
                        args[0].clone(),
                    ])));
                }
                let index = raw_index as usize;
                let base = args.get(1).filter(|value| !value.is_nil());
                let locals = interp
                    .backtrace_frame_locals_snapshot_with_base(index, base)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(name, value)| Value::cons(Value::Symbol(name), value))
                    .collect::<Vec<_>>();
                Ok(Value::list(locals))
            }
            "current-thread" => Ok(interp.current_thread_value()),
            "all-threads" => {
                need_args(name, args, 0)?;
                Ok(Value::list(interp.live_threads()))
            }
            "make-thread" => {
                need_arg_range(name, args, 1, 3)?;
                let thread_name = args.get(1).and_then(|value| {
                    if value.is_nil() {
                        None
                    } else {
                        string_like(value).map(|string| string.text)
                    }
                });
                let disposition = match args.get(2) {
                    None | Some(Value::Nil) => BufferDisposition::Default,
                    Some(Value::T) => BufferDisposition::Preserve,
                    Some(Value::Symbol(symbol)) if symbol == "silently" => {
                        BufferDisposition::Silently
                    }
                    Some(other) => {
                        return Err(LispError::TypeError(
                            "thread-buffer-disposition".into(),
                            other.type_name(),
                        ));
                    }
                };
                interp.make_thread(args[0].clone(), thread_name, disposition)
            }
            "thread-live-p" => {
                need_args(name, args, 1)?;
                Ok(if interp.thread_live(interp.resolve_thread_id(&args[0])?) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "thread-join" => {
                need_args(name, args, 1)?;
                interp.thread_join(interp.resolve_thread_id(&args[0])?, env)
            }
            "thread-name" => {
                need_args(name, args, 1)?;
                Ok(interp
                    .thread_name(interp.resolve_thread_id(&args[0])?)
                    .map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Nil))
            }
            "thread-signal" => {
                need_args(name, args, 3)?;
                interp.signal_thread(
                    interp.resolve_thread_id(&args[0])?,
                    args[1].clone(),
                    args[2].clone(),
                    env,
                )
            }
            "thread-last-error" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(interp.thread_last_error(args.first().is_some_and(Value::is_truthy)))
            }
            "thread-yield" => {
                need_args(name, args, 0)?;
                let stepped_thread = !interp.current_thread_is_main();
                // GNU's thread-yield lets any other thread run, including
                // one whose sleep has expired.  Main's yield therefore runs
                // a waking pass; a stepped thread's yield must not re-enter
                // the timer/notification machinery mid-step, so it drives
                // without waking (its parent's next pass wakes sleepers).
                interp.drive_threads(env, !stepped_thread)?;
                if stepped_thread {
                    // A spawned thread's body runs to completion inside one
                    // scheduler step; its parent -- the only thread that can
                    // flip this loop's condition -- stays suspended until we
                    // return.  When repeated yields drive nothing else, the
                    // loop can never progress: signal the cooperative-model
                    // deadlock (honesty audit finding 84) instead of
                    // spinning forever, as GNU's preemptive threads would
                    // simply interleave here.
                    interp.note_stepped_yield();
                    if interp.stepped_yield_exhausted() {
                        return Err(LispError::Signal(
                            "Cooperative thread model deadlock: yield cannot reach the suspended parent thread".into(),
                        ));
                    }
                } else if interp.has_advanceable_spawned_thread() {
                    interp.reset_stepped_yields();
                } else {
                    // Main spinning on yield with no spawned thread the
                    // scheduler can advance: nothing but this loop can
                    // change Lisp state, so the loop can never exit.  GNU's
                    // preemptive children would have progressed; this
                    // model's children have finished or blocked for good.
                    // Signal the cooperative-model deadlock (finding 84).
                    interp.note_stepped_yield();
                    if interp.stepped_yield_exhausted() {
                        interp.reset_stepped_yields();
                        return Err(LispError::Signal(
                            "Cooperative thread model deadlock: no other thread can advance this yield loop".into(),
                        ));
                    }
                }
                Ok(Value::Nil)
            }
            "make-mutex" => {
                need_arg_range(name, args, 0, 1)?;
                let mutex_name = args.first().and_then(|value| {
                    if value.is_nil() {
                        None
                    } else {
                        string_like(value).map(|string| string.text)
                    }
                });
                Ok(interp.make_mutex(mutex_name))
            }
            "mutex-name" => {
                need_args(name, args, 1)?;
                Ok(interp
                    .mutex_name(interp.resolve_mutex_id(&args[0])?)
                    .map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Nil))
            }
            "mutex-lock" => {
                need_args(name, args, 1)?;
                interp.lock_mutex_for_current_thread(interp.resolve_mutex_id(&args[0])?, env)
            }
            "mutex-unlock" => {
                need_args(name, args, 1)?;
                interp.unlock_mutex_for_current_thread(interp.resolve_mutex_id(&args[0])?)
            }
            "make-condition-variable" => {
                need_arg_range(name, args, 1, 2)?;
                let mutex_id = interp.resolve_mutex_id(&args[0])?;
                let condvar_name = args.get(1).and_then(|value| {
                    if value.is_nil() {
                        None
                    } else {
                        string_like(value).map(|string| string.text)
                    }
                });
                Ok(interp.make_condition_variable(mutex_id, condvar_name))
            }
            "condition-mutex" => {
                need_args(name, args, 1)?;
                let condvar_id = interp.resolve_condition_variable_id(&args[0])?;
                let mutex_id = interp
                    .condition_variable_mutex_id(condvar_id)
                    .ok_or_else(|| {
                        LispError::WrongTypeArgument("condition-variable-p".into(), args[0].clone())
                    })?;
                Ok(Value::Record(mutex_id))
            }
            "condition-name" => {
                need_args(name, args, 1)?;
                Ok(interp
                    .condition_variable_name(interp.resolve_condition_variable_id(&args[0])?)
                    .map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Nil))
            }
            "condition-wait" => {
                need_args(name, args, 1)?;
                let condvar_id = interp.resolve_condition_variable_id(&args[0])?;
                interp.wait_condition_variable(condvar_id, env)
            }
            "condition-notify" => {
                need_arg_range(name, args, 1, 2)?;
                interp.notify_condition_variable(
                    interp.resolve_condition_variable_id(&args[0])?,
                    args.get(1).is_some_and(Value::is_truthy),
                    env,
                )?;
                Ok(Value::Nil)
            }
            "thread--blocker" => {
                need_args(name, args, 1)?;
                Ok(interp.thread_blocker_value(interp.resolve_thread_id(&args[0])?))
            }
            "backtrace--frames-from-thread" => {
                need_args(name, args, 1)?;
                let frames = interp
                    .thread_backtrace_frames_snapshot(interp.resolve_thread_id(&args[0])?)
                    .into_iter()
                    .map(|(evald, function, frame_args, _debug_on_exit)| {
                        let evald = if evald { Value::T } else { Value::Nil };
                        let mut items = vec![evald, function];
                        items.extend(frame_args);
                        Value::list(items)
                    })
                    .collect::<Vec<_>>();
                Ok(Value::list(frames))
            }
            "regexp-quote" => {
                need_args(name, args, 1)?;
                Ok(Value::String(
                    regexp::regexp_quote_elisp(&string_text(&args[0])?).into(),
                ))
            }
            "kill-all-local-variables" => {
                need_arg_range(name, args, 0, 1)?;
                let buffer_id = interp.current_buffer_id();
                let kill_permanent = args.first().is_some_and(Value::is_truthy);
                run_named_hooks(interp, "change-major-mode-hook", env, Some(buffer_id))?;
                let locals = interp.buffer_local_variables(buffer_id);
                let mut permanent = Vec::new();
                for (name, value) in &locals {
                    let preserve = !kill_permanent
                        && interp
                            .get_symbol_property(name, "permanent-local")
                            .is_some_and(|value| value.is_truthy());
                    interp.notify_variable_watchers(
                        name,
                        Value::Nil,
                        "makunbound",
                        Some(buffer_id),
                        env,
                    )?;
                    if preserve {
                        permanent.push((name.clone(), value.clone()));
                        continue;
                    }
                    interp.mark_buffer_local_special_binding_killed(buffer_id, name);
                }
                if kill_permanent {
                    interp.clear_buffer_local_state(buffer_id);
                } else {
                    interp.clear_buffer_local_state_for_mode_change(buffer_id);
                }
                for (name, value) in permanent {
                    interp.set_buffer_local_value(buffer_id, &name, value);
                }
                Ok(Value::Nil)
            }
            "force-mode-line-update" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(Value::Nil)
            }
            "garbage-collect" => {
                need_args(name, args, 0)?;
                if interp.garbage_collection_is_inhibited() {
                    return Ok(Value::Nil);
                }
                // The reclamation emaxx really performs: weak hash entries
                // whose keys/values are no longer reachable are dropped, as
                // GNU's sweep does.  Everything else is freed by ownership
                // the moment it becomes unreachable.
                let native_roots = crate::lisp::native_comp::begin_garbage_collection(interp);
                collect_weak_hash_tables(interp, env, &native_roots)?;
                let census = interp.live_object_census();
                let threshold = interp
                    .symbol_value_cell("gc-cons-threshold")
                    .ok()
                    .and_then(|value| value.as_integer().ok())
                    .unwrap_or(800_000);
                let percentage = match interp.symbol_value_cell("gc-cons-percentage") {
                    Ok(Value::Float(value)) => Some(value.get()),
                    _ => None,
                };
                crate::lisp::native_comp::garbage_collection_finished(
                    interp,
                    census.total_bytes_of_live_objects(),
                    threshold,
                    percentage,
                );
                // GNU returns ((TYPE SIZE USED FREE) ...) in exactly this
                // row order (alloc.c, oracle-confirmed).  USED counts come
                // from the live reachability census (finding 110 -- these
                // were fabricated zeros); SIZE columns are THIS binary's
                // real per-object layout constants, so memory-report.el
                // computes emaxx-true byte totals, not GNU's.  FREE columns
                // are 0 truthfully: Rust ownership retains no free lists.
                let cons_size = std::mem::size_of::<crate::lisp::types::ConsCell>() as i64;
                let entry = |name: &str, rest: &[i64]| {
                    Value::list(
                        std::iter::once(Value::Symbol(name.into()))
                            .chain(rest.iter().map(|n| Value::Integer(*n))),
                    )
                };
                Ok(Value::list([
                    entry("conses", &[cons_size, census.conses as i64, 0]),
                    entry(
                        "symbols",
                        &[
                            std::mem::size_of::<String>() as i64,
                            census.symbols as i64,
                            0,
                        ],
                    ),
                    entry(
                        "strings",
                        &[
                            std::mem::size_of::<crate::lisp::types::SharedStringState>() as i64,
                            census.strings as i64,
                            0,
                        ],
                    ),
                    entry("string-bytes", &[1, census.string_bytes as i64]),
                    // Vectors ride on tagged cons chains internally, so a
                    // vector header and each slot really cost one cons cell.
                    entry("vectors", &[cons_size, census.vectors as i64]),
                    entry("vector-slots", &[cons_size, census.vector_slots as i64, 0]),
                    entry("floats", &[8, census.floats as i64, 0]),
                    entry(
                        "intervals",
                        &[
                            std::mem::size_of::<crate::buffer::TextPropertySpan>() as i64,
                            census.intervals as i64,
                            0,
                        ],
                    ),
                    entry(
                        "buffers",
                        &[
                            std::mem::size_of::<crate::buffer::Buffer>() as i64,
                            census.buffers as i64,
                        ],
                    ),
                ]))
            }
            "garbage-collect-maybe" => {
                need_args(name, args, 1)?;
                if !matches!(args[0], Value::Integer(value) if value >= 0) {
                    return Err(LispError::WrongTypeArgument(
                        "wholenump".into(),
                        args[0].clone(),
                    ));
                }
                // Emaxx uses Rust ownership rather than GNU's byte-allocation GC
                // threshold, so no pending automatic collection can be due.
                Ok(Value::Nil)
            }
            "memory-use-counts" => {
                need_args(name, args, 0)?;
                // GNU's seven counters are incremented at type-specific C arena
                // allocation sites and survive GC.  Rust ownership has neither
                // those arenas nor equivalent category accounting; allocator
                // byte totals or live-object scans would not implement this API.
                Err(LispError::Signal(
                    "GNU allocation counters are unavailable in the Rust ownership backend".into(),
                ))
            }
            "num-processors" => {
                need_args(name, args, 0)?;
                let count = std::thread::available_parallelism()
                    .map(|count| count.get() as i64)
                    .unwrap_or(1);
                Ok(Value::Integer(count.max(1)))
            }
            "current-cpu-time" => {
                need_args(name, args, 0)?;
                let nanos = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos();
                Ok(Value::list([normalize_bigint_value(BigInt::from(nanos))]))
            }
            "emacs-pid" => {
                need_args(name, args, 0)?;
                Ok(Value::Integer(emacs_pid_value()))
            }
            "type-of" => {
                need_args(name, args, 1)?;
                if let Some(type_name) = legacy_struct_vector_type(interp, &args[0], env) {
                    return Ok(Value::Symbol(type_name.into()));
                }
                let name = match &args[0] {
                    Value::Nil => "symbol",
                    Value::T => "symbol",
                    Value::Integer(_) => "integer",
                    Value::BigInteger(_) => "integer",
                    Value::Float(_) => "float",
                    Value::String(_) => "string",
                    Value::StringObject(_) => "string",
                    Value::Symbol(_) => "symbol",
                    Value::Vector(_) => "vector",
                    Value::Cons(_) if is_vector_value(&args[0]) => "vector",
                    Value::Cons(_) => "cons",
                    Value::BuiltinFunc(_) => "subr",
                    Value::Lambda(_) => "cons", // Emacs closures are cons cells
                    Value::Buffer(_) => "buffer",
                    Value::Marker(_) => "marker",
                    Value::Overlay(_) => "overlay",
                    Value::CharTable(_) => "char-table",
                    Value::Frame(_) => "frame",
                    Value::Terminal(_) => "terminal",
                    Value::Record(id) => {
                        let record = interp.find_record(*id).ok_or_else(|| {
                            LispError::TypeError("record".into(), format!("record<{id}>"))
                        })?;
                        // data.c:Ftype_of answers `subr' for every
                        // PVEC_SUBR; only `cl-type-of' distinguishes native
                        // functions, special forms, and primitives.
                        if record.kind == crate::lisp::eval::RecordKind::NativeCompiledFunction {
                            return Ok(Value::symbol("subr"));
                        }
                        return cl_type_value(interp, &args[0]);
                    }
                    Value::Finalizer(_) => "finalizer",
                    Value::ReaderForm(_) => {
                        return Err(LispError::Signal(
                            "reader form escaped object materialization".into(),
                        ));
                    }
                    Value::Unbound => "unbound",
                };
                Ok(Value::Symbol(name.into()))
            }
            #[dispatch(builtin_override)]
            "cl-type-of" => {
                need_args(name, args, 1)?;
                cl_type_value(interp, &args[0])
            } // GNU cl-macs.el defines `cl--find-class' as `(get TYPE 'cl--class)',
              // which projects to the same class storage as `cl-find-class' here.
        }
    }
);

/// Whether VALUE is an OClosure, by GNU's rule: data.c's `interactive_form'
/// treats a closure whose docstring slot is not a valid docstring as one, and
/// then dispatches to the Lisp `oclosure-type'/`oclosure-interactive-form'
/// owners.  The native shape probe below only recognises interpreted
/// lambdas; a COMPILED OClosure -- which is what nadvice produces for an
/// advised function -- is a closure record, so fall back to asking the real
/// `oclosure-type' owner, exactly as the autoload path already does.
pub(crate) fn value_is_oclosure(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut crate::lisp::types::Env,
) -> bool {
    if oclosure_type_of(value).is_some() {
        return true;
    }
    matches!(value, Value::Record(_) | Value::Lambda(_))
        && interp.has_lisp_function("oclosure-type")
        && interp
            .call_function_value(
                Value::Symbol("oclosure-type".into()),
                Some("oclosure-type"),
                std::slice::from_ref(value),
                env,
            )
            .map(|resolved| resolved.is_truthy())
            .unwrap_or(false)
}

pub(crate) fn oclosure_type_of(value: &Value) -> Option<String> {
    let Value::Lambda(lambda) = value else {
        return None;
    };
    // GNU oclosure-type recognizes a closure whose public slot four is a
    // symbol.  That slot is LambdaValue::documentation in the typed host
    // representation; no private body marker or Lisp-visible API is needed.
    (lambda.public_len() > 4).then(|| {
        lambda
            .documentation
            .as_ref()?
            .as_symbol()
            .ok()
            .map(String::from)
    })?
}

fn widget_get(interp: &Interpreter, widget: &Value, property: &Value) -> Result<Value, LispError> {
    widget_get_inner(interp, widget, property, &mut HashSet::new())
}

fn widget_get_inner(
    interp: &Interpreter,
    widget: &Value,
    property: &Value,
    seen: &mut HashSet<String>,
) -> Result<Value, LispError> {
    match widget {
        Value::Cons(cons_cell) => {
            let car = &cons_cell.car;
            let cdr = &cons_cell.cdr;
            let widget_type = car.borrow().clone();
            if let Some(value) = plist_get_exact(&cdr.borrow().clone(), property)? {
                return Ok(value);
            }
            widget_get_inner(interp, &widget_type, property, seen)
        }
        Value::Symbol(symbol) => {
            if !seen.insert(symbol.to_string()) {
                return Ok(Value::Nil);
            }
            match interp.get_symbol_property(symbol, "widget-type") {
                Some(parent) => widget_get_inner(interp, &parent, property, seen),
                None => Ok(Value::Nil),
            }
        }
        _ => Ok(Value::Nil),
    }
}

fn widget_put(
    _interp: &mut Interpreter,
    widget: &Value,
    property: &Value,
    value: Value,
) -> Result<Value, LispError> {
    let Some((_, cdr)) = (widget).cons_cells() else {
        return Err(LispError::TypeError("widget".into(), widget.type_name()));
    };
    let plist = cdr.borrow().clone();
    let updated = plist_put_exact(plist, property.clone(), value.clone())?;
    *cdr.borrow_mut() = updated;
    Ok(value)
}

fn plist_get_exact(plist: &Value, property: &Value) -> Result<Option<Value>, LispError> {
    let mut current = plist.clone();
    let mut seen = crate::lisp::types::CycleGuard::new();
    loop {
        match current {
            Value::Nil => return Ok(None),
            Value::Cons(cons_cell) => {
                let car = &cons_cell.car;
                let cdr = &cons_cell.cdr;
                let cell_id = crate::lisp::types::ConsCell::identity(&cons_cell);
                if seen.step(cell_id) {
                    return Ok(None);
                }
                if car.borrow().clone() == *property {
                    return match cdr.borrow().clone() {
                        Value::Cons(cell) => Ok(Some(cell.car.borrow().clone())),
                        _ => Ok(Some(Value::Nil)),
                    };
                }
                match cdr.borrow().clone() {
                    Value::Cons(cell) => current = cell.cdr.borrow().clone(),
                    _ => return Ok(None),
                }
            }
            _ => return Err(plist_type_error(plist)),
        }
    }
}

fn plist_put_exact(plist: Value, property: Value, value: Value) -> Result<Value, LispError> {
    let mut current = plist.clone();
    let mut seen = crate::lisp::types::CycleGuard::new();
    loop {
        match current {
            Value::Nil => return Ok(Value::list([property, value])),
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
                if car.borrow().clone() == property {
                    return match cdr.borrow().clone() {
                        Value::Cons(cons_cell) => {
                            let existing = &cons_cell.car;
                            let _ = &cons_cell.cdr;
                            *existing.borrow_mut() = value;
                            Ok(plist)
                        }
                        _ => Err(plist_type_error(&plist)),
                    };
                }
                match cdr.borrow().clone() {
                    Value::Cons(cons_cell) => {
                        let _ = &cons_cell.car;
                        let next = &cons_cell.cdr;
                        let next_value = next.borrow().clone();
                        if next_value.is_nil() {
                            *next.borrow_mut() = Value::list([property, value]);
                            return Ok(plist);
                        }
                        current = next_value;
                    }
                    _ => return Err(plist_type_error(&plist)),
                }
            }
            _ => return Err(plist_type_error(&plist)),
        }
    }
}

fn legacy_struct_vector_type(interp: &Interpreter, value: &Value, env: &Env) -> Option<String> {
    if !interp
        .lookup_var("cl-old-struct-compat-mode", env)
        .is_some_and(|value| value.is_truthy())
    {
        return None;
    }
    let items = vector_items(value).ok()?;
    let Some(Value::Symbol(tag)) = items.first() else {
        return None;
    };
    let type_name = tag.strip_prefix("cl-struct-")?;
    interp.class_value(type_name)?;
    let witness = interp.raw_function_binding(tag, env)?;
    if witness == Value::Symbol(":quick-object-witness-check".into()) {
        Some(type_name.to_string())
    } else {
        None
    }
}
