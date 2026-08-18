use super::*;
use crate::lisp::primitives::string_like;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

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
    if let Some(table_id) = full_table {
        for entry in interp
            .char_table_effective_ranges(table_id)
            .unwrap_or_default()
        {
            let event = if entry.start == entry.end {
                Value::Integer(i64::from(entry.start))
            } else {
                Value::cons(
                    Value::Integer(i64::from(entry.start)),
                    Value::Integer(i64::from(entry.end)),
                )
            };
            interp.call_function_value(function.clone(), None, &[event, entry.value], env)?;
        }
    }

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
    character_bindings.sort_by_key(|(code, _)| *code);
    let mut index = 0;
    while index < character_bindings.len() {
        let (start, value) = character_bindings[index].clone();
        let mut end = start;
        while let Some((next, next_value)) = character_bindings.get(index + 1) {
            if *next != end.saturating_add(1) || !values_equal(interp, &value, next_value) {
                break;
            }
            index += 1;
            end = *next;
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
                let key = key_sequence_binding_text(&args[1])?;
                let key_parts = key_sequence_keymap_parts(&args[1])?;
                if args[2].is_nil() && args.get(3).is_some_and(Value::is_truthy) {
                    keymap_remove_binding(interp, &args[0], &key)?;
                } else {
                    keymap_define_binding_with_placement(
                        interp,
                        &args[0],
                        &key,
                        Some(key_parts),
                        args[2].clone(),
                        false,
                    )?;
                }
                Ok(args[2].clone())
            }
            "lookup-key" => {
                need_arg_range(name, args, 2, 3)?;
                let key_parts = key_sequence_keymap_parts(&args[1])?;
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
                        key_sequence_prefix_event_count(&args[1], prefix_len)? as i64,
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
                let key = key_sequence_binding_text(&args[0])?;
                let parts = key_sequence_keymap_parts(&args[0])?;
                let binding = key_binding_with_parts(
                    interp,
                    &key,
                    &parts,
                    args.get(1).is_some_and(Value::is_truthy),
                    args.get(2).is_some_and(Value::is_truthy),
                    env,
                )?;
                // access_keymap resolves every answer through
                // get_keyelt: a menu binding ("Edit" . KEYMAP) or
                // (menu-item ...) answers its real definition.
                keymap_get_keyelt(interp, &binding, true, env)
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
                Err(LispError::TypeError("keymapp".into(), args[0].type_name()))
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
                    return Err(LispError::TypeError("keymapp".into(), args[1].type_name()));
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
                    return Err(LispError::TypeError("keymapp".into(), args[0].type_name()));
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
                    return Err(LispError::TypeError("keymapp".into(), args[0].type_name()));
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
                if let Some(wrapper) = materialize_preloaded_lisp_function(interp, &symbol, env) {
                    // The implementation stays native, but GNU's dumped
                    // function cell is compiled Lisp.  Materialize that stable
                    // observable wrapper on first inspection so help, advice,
                    // and byte-code-function-p see the dumped contract without
                    // moving the implementation across the Lisp/host boundary.
                    return Ok(wrapper);
                }
                Ok(match interp.logical_function_binding(&symbol, env) {
                    Some(value) => value,
                    None if is_special_form_name(&symbol) => {
                        Value::BuiltinFunc(symbol.clone().into())
                    }
                    None if symbol == "benchmark-run" => Value::list([
                        Value::Symbol("autoload".into()),
                        Value::String("benchmark.el".into()),
                        Value::String("Autoloaded benchmark-run.".into()),
                        Value::Nil,
                        Value::Nil,
                    ]),
                    None if symbol == "tetris" => Value::list([
                        Value::Symbol("autoload".into()),
                        Value::String("tetris.el".into()),
                        Value::String("Autoloaded tetris.".into()),
                        Value::T,
                        Value::Nil,
                    ]),
                    // GNU returns nil for an unbound function cell (nadvice's
                    // pending-advice path reads it).
                    None => Value::Nil,
                })
            }
            "symbol-name" => {
                need_args(name, args, 1)?;
                // GNU 30.2 data.c:Fsymbol_name uses CHECK_SYMBOL/XSYMBOL.
                let s = checked_symbol_name(interp, &args[0], env)?;
                Ok(Value::String(
                    crate::lisp::types::visible_symbol_name(&s)
                        .to_string()
                        .into(),
                ))
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
                if let Some(size) = args.first()
                    && size.as_integer()? < 0
                {
                    return Err(LispError::TypeError("natnump".into(), size.type_name()));
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
                let mut size = Value::Integer(65);
                let mut rehash_size = Value::Float(1.5);
                let mut rehash_threshold = Value::Float(0.8125);
                let mut weakness = Value::Nil;
                let mut index = 0usize;
                while index + 1 < args.len() {
                    let key = args[index].as_symbol()?;
                    match key {
                        ":test" => {
                            test = match &args[index + 1] {
                                Value::Symbol(name) => name.to_string(),
                                Value::BuiltinFunc(name) => name.to_string(),
                                other => {
                                    return Err(LispError::TypeError(
                                        "symbol".into(),
                                        other.type_name(),
                                    ));
                                }
                            };
                        }
                        ":size" => size = args[index + 1].clone(),
                        ":rehash-size" => rehash_size = args[index + 1].clone(),
                        ":rehash-threshold" => rehash_threshold = args[index + 1].clone(),
                        ":weakness" => {
                            weakness = match &args[index + 1] {
                                Value::T => Value::Symbol("key-and-value".into()),
                                other => other.clone(),
                            };
                        }
                        ":purecopy" => {}
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
                let table = json::make_hash_table(interp, &test, Vec::new());
                let Value::Record(id) = table.clone() else {
                    unreachable!("hash tables are represented as records")
                };
                let record = interp
                    .find_record_mut(id)
                    .expect("make_hash_table should create a record");
                if record.slots.len() < 6 {
                    record.slots.resize(6, Value::Nil);
                }
                record.slots[2] = size;
                record.slots[3] = rehash_size;
                record.slots[4] = rehash_threshold;
                record.slots[5] = weakness;
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
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[0].type_name(),
                    ));
                };
                let Some(record) = interp.find_record(id) else {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[0].type_name(),
                    ));
                };
                if record.kind != crate::lisp::eval::RecordKind::HashTable {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[0].type_name(),
                    ));
                }
                interp.copy_record(id)
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
                let Some((test, entries)) = json::hash_table_entries(interp, &args[1]) else {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[1].type_name(),
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
                    && interp.equal_hash_put(*id, args[0].clone(), args[1].clone(), env)
                {
                    return Ok(args[1].clone());
                }
                let Some((test, mut entries)) = json::hash_table_entries(interp, &args[2]) else {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[2].type_name(),
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
                Ok(args[1].clone())
            }
            "maphash" => {
                need_args(name, args, 2)?;
                let Some((_, entries)) = json::hash_table_entries(interp, &args[1]) else {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[1].type_name(),
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
                    && interp.equal_hash_remove(*id, &args[0], env).is_some()
                {
                    return Ok(Value::Nil);
                }
                let Some((test, entries)) = json::hash_table_entries(interp, &args[1]) else {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[1].type_name(),
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
                Ok(Value::Nil)
            }
            "clrhash" => {
                need_args(name, args, 1)?;
                if json::hash_table_entries(interp, &args[0]).is_none() {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[0].type_name(),
                    ));
                }
                set_hash_table_entries(interp, &args[0], Vec::new())?;
                Ok(args[0].clone())
            }
            "hash-table-count" => {
                need_args(name, args, 1)?;
                let Some((_, entries)) = json::hash_table_entries(interp, &args[0]) else {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[0].type_name(),
                    ));
                };
                Ok(Value::Integer(entries.len() as i64))
            }
            "hash-table-rehash-size" => {
                need_args(name, args, 1)?;
                Ok(hash_table_metadata_slot(
                    interp,
                    &args[0],
                    3,
                    Value::Float(1.5),
                )?)
            }
            "hash-table-rehash-threshold" => {
                need_args(name, args, 1)?;
                Ok(hash_table_metadata_slot(
                    interp,
                    &args[0],
                    4,
                    Value::Float(0.8125),
                )?)
            }
            "hash-table-size" => {
                need_args(name, args, 1)?;
                let default_size = json::hash_table_entries(interp, &args[0])
                    .map(|(_, entries)| Value::Integer(entries.len().max(65) as i64))
                    .unwrap_or(Value::Integer(65));
                Ok(hash_table_metadata_slot(interp, &args[0], 2, default_size)?)
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
                let default_size = json::hash_table_entries(interp, &args[0])
                    .map(|(_, entries)| Value::Integer(entries.len().max(65) as i64))
                    .unwrap_or(Value::Integer(65));
                Ok(hash_table_metadata_slot(interp, &args[0], 2, default_size)?)
            }
            "internal--hash-table-histogram" => {
                need_args(name, args, 1)?;
                if json::hash_table_entries(interp, &args[0]).is_none() {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[0].type_name(),
                    ));
                }
                Ok(Value::Nil)
            }
            "internal--hash-table-buckets" => {
                need_args(name, args, 1)?;
                let Some((_, entries)) = json::hash_table_entries(interp, &args[0]) else {
                    return Err(LispError::TypeError(
                        "hash-table".into(),
                        args[0].type_name(),
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
                Ok(Value::Nil)
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
                    Ok(Value::String("#<hash-table>".into()))
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
                Ok(Value::Nil)
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
                    Ok(Value::String("#<hash-table>".into()))
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
                for pair in args[1..].chunks_exact(2) {
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
                interp.register_captured_lexical_frames(&shared_context);
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
                    .map(|(name, value)| Value::cons(Value::Symbol(name.into()), value))
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
                interp.drive_threads(env, false)?;
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
                        LispError::TypeError("condition-variable-p".into(), args[0].type_name())
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
                collect_weak_hash_tables(interp)?;
                // GNU returns ((TYPE SIZE USED FREE) ...); the SIZE column is
                // the 64-bit object layout constant (memory-report.el computes
                // object sizes from it).  Counts are approximations.
                let entry = |name: &str, rest: &[i64]| {
                    Value::list(
                        std::iter::once(Value::Symbol(name.into()))
                            .chain(rest.iter().map(|n| Value::Integer(*n))),
                    )
                };
                Ok(Value::list([
                    entry("conses", &[16, 0, 0]),
                    entry("symbols", &[48, 0, 0]),
                    entry("strings", &[32, 0, 0]),
                    entry("string-bytes", &[1, 0]),
                    entry("vectors", &[16, 0]),
                    entry("vector-slots", &[8, 0, 0]),
                    entry("floats", &[8, 0, 0]),
                    entry("intervals", &[56, 0, 0]),
                    entry("buffers", &[984, 0]),
                ]))
            }
            "garbage-collect-maybe" => {
                need_args(name, args, 1)?;
                if !matches!(args[0], Value::Integer(value) if value >= 0) {
                    return Err(LispError::TypeError(
                        "wholenump".into(),
                        args[0].type_name(),
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
                        interp.find_record(*id).ok_or_else(|| {
                            LispError::TypeError("record".into(), format!("record<{id}>"))
                        })?;
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

fn byte_code_function_slots(
    interp: &Interpreter,
    symbol: Option<&str>,
    callable: Value,
    lap: Option<Value>,
    dynamic_binding: bool,
) -> Vec<Value> {
    let doc = symbol
        .and_then(|name| {
            interp
                .get_symbol_property(name, "function-documentation")
                .or_else(|| {
                    super::misc::fallback_function_documentation(interp, name)
                        .map(|value| Value::String(value.into()))
                })
        })
        .and_then(|value| byte_code_docstring(value, &callable));
    let interactive = symbol
        .and_then(|name| interp.get_symbol_property(name, "interactive-form"))
        .and_then(|form| form.to_vec().ok())
        .and_then(|items| items.get(1).cloned())
        .unwrap_or(Value::Nil);
    vec![
        callable,
        lap.unwrap_or(Value::Nil),
        if dynamic_binding {
            Value::Symbol("dynamic-binding".into())
        } else {
            Value::Nil
        },
        Value::Nil,
        doc.unwrap_or(Value::Nil),
        interactive,
    ]
}

pub(super) fn materialize_preloaded_lisp_function(
    interp: &mut Interpreter,
    symbol: &str,
    env: &Env,
) -> Option<Value> {
    let Value::BuiltinFunc(builtin) = interp.logical_function_binding(symbol, env)? else {
        return None;
    };
    if !builtin_is_gnu_preloaded_lisp(interp, &builtin) {
        return None;
    }
    let slots = byte_code_function_slots(
        interp,
        Some(symbol),
        Value::BuiltinFunc(builtin),
        None,
        false,
    );
    let wrapper = interp.create_pseudovector(
        crate::lisp::eval::RecordKind::Closure,
        "byte-code-function",
        slots,
    );
    interp.set_function_binding(symbol, Some(wrapper.clone()));
    Some(wrapper)
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

fn byte_code_docstring(doc: Value, callable: &Value) -> Option<Value> {
    let text = match doc {
        Value::String(text) => text,
        Value::StringObject(state) => state.borrow().text.clone().into(),
        _ => return None,
    };
    let Some(usage) = byte_code_usage(callable) else {
        return Some(Value::String(text));
    };
    Some(Value::String(format!("{text}\n\n{usage}").into()))
}

fn byte_code_usage(callable: &Value) -> Option<String> {
    match callable {
        Value::Lambda(lambda) => Some(format!("(fn{})", byte_code_usage_params(&lambda.params))),
        value if is_lambda_value(value) => {
            let items = value.to_vec().ok()?;
            let params = items.get(1)?.to_vec().ok()?;
            let params = params
                .iter()
                .filter_map(|value| value.as_symbol().ok().map(str::to_string))
                .collect::<Vec<_>>();
            Some(format!("(fn{})", byte_code_usage_params(&params)))
        }
        _ => None,
    }
}

fn byte_code_usage_params(params: &[String]) -> String {
    let rendered = params
        .iter()
        .filter(|param| !param.starts_with('&'))
        .map(|param| param.to_ascii_uppercase())
        .collect::<Vec<_>>();
    if rendered.is_empty() {
        String::new()
    } else {
        format!(" {}", rendered.join(" "))
    }
}

// GNU's 30.2 `preloaded-file-list' (loadup.el order, lisp/-relative).
// `symbol-file' resolves natives that are preloaded elisp in GNU by
// scanning these sources for the defining form, standing in for the
// dumped load-history.
const GNU_PRELOADED_LISP_FILES: &[&str] = &[
    "emacs-lisp/rmc",
    "international/iso-transl",
    "tooltip",
    "cus-start",
    "emacs-lisp/cconv",
    "emacs-lisp/eldoc",
    "emacs-lisp/shorthands",
    "paren",
    "electric",
    "uniquify",
    "vc/ediff-hook",
    "vc/vc-hooks",
    "emacs-lisp/float-sup",
    "progmodes/elisp-mode",
    "buff-menu",
    "emacs-lisp/tabulated-list",
    "replace",
    "newcomment",
    "textmodes/fill",
    "textmodes/text-mode",
    "emacs-lisp/lisp-mode",
    "progmodes/prog-mode",
    "textmodes/paragraphs",
    "register",
    "textmodes/page",
    "emacs-lisp/lisp",
    "tab-bar",
    "menu-bar",
    "rfn-eshadow",
    "isearch",
    "emacs-lisp/easymenu",
    "emacs-lisp/timer",
    "select",
    "mouse",
    "jit-lock",
    "font-lock",
    "emacs-lisp/syntax",
    "font-core",
    "term/tty-colors",
    "startup",
    "frame",
    "minibuffer",
    "emacs-lisp/nadvice",
    "emacs-lisp/seq",
    "simple",
    "emacs-lisp/cl-generic",
    "indent",
    "language/indonesian",
    "language/philippine",
    "language/cham",
    "language/burmese",
    "language/khmer",
    "language/georgian",
    "language/utf-8-lang",
    "language/misc-lang",
    "language/vietnamese",
    "language/tibetan",
    "language/thai",
    "language/tai-viet",
    "language/lao",
    "language/korean",
    "language/japanese",
    "international/eucjp-ms",
    "international/cp51932",
    "language/hebrew",
    "language/greek",
    "language/romanian",
    "language/slovak",
    "language/czech",
    "language/european",
    "language/ethiopic",
    "language/english",
    "language/sinhala",
    "language/indian",
    "language/cyrillic",
    "international/uni-special-lowercase.el",
    "language/chinese",
    "composite",
    "international/emoji-zwj",
    "international/charscript",
    "international/uni-lowercase.el",
    "international/uni-uppercase.el",
    "international/uni-category.el",
    "international/uni-brackets.el",
    "international/uni-mirrored.el",
    "international/uni-bidi.el",
    "international/characters",
    "international/charprop.el",
    "case-table",
    "international/mule-cmds",
    "epa-hook",
    "jka-cmpr-hook",
    "help",
    "abbrev",
    "obarray",
    "emacs-lisp/oclosure",
    "emacs-lisp/cl-preloaded",
    "button",
    "theme-loaddefs.el",
    "loaddefs",
    "faces",
    "cus-face",
    "emacs-lisp/macroexp",
    "files",
    "window",
    "bindings",
    "format",
    "env",
    "international/mule-conf",
    "international/mule",
    "emacs-lisp/map-ynp",
    "custom",
    "widget",
    "version",
    "keymap",
    "subr",
    "emacs-lisp/backquote",
    "emacs-lisp/byte-run",
    "emacs-lisp/debug-early",
    "loadup.el",
];

fn preloaded_lisp_directory(interp: &Interpreter) -> Option<PathBuf> {
    if let Some(repo_etc) = crate::lisp::primitives::compat_data_directory() {
        let directory = std::path::Path::new(&repo_etc).parent()?.join("lisp");
        if directory.join("subr.el").is_file() {
            return Some(directory);
        }
    }
    interp
        .lookup_var("load-path", &Vec::new())?
        .to_vec()
        .ok()?
        .into_iter()
        .filter_map(|value| string_like(&value).map(|string| PathBuf::from(string.text)))
        .find(|directory| directory.join("subr.el").is_file())
}

type PreloadedSourceIndex = HashMap<String, String>;
type PreloadedSourceIndexCache = Mutex<HashMap<PathBuf, Arc<PreloadedSourceIndex>>>;

fn build_preloaded_source_index(lisp_dir: &Path) -> PreloadedSourceIndex {
    static DEFINITION: OnceLock<regex::Regex> = OnceLock::new();
    let pattern = DEFINITION.get_or_init(|| {
        regex::Regex::new(r"(?m)^\(def\S*\s+'?([^\s()]+)[\s)\n]")
            .expect("static preloaded definition regex")
    });
    let mut index = HashMap::new();
    for relative in GNU_PRELOADED_LISP_FILES {
        let path = lisp_dir.join(format!("{relative}.el"));
        let Ok(contents) = std::fs::read_to_string(&path) else {
            continue;
        };
        let rendered_path = path.display().to_string();
        for captures in pattern.captures_iter(&contents) {
            if let Some(symbol) = captures.get(1) {
                // GNU's dumped load-history resolves the first preloaded
                // definition in loadup order.
                index
                    .entry(symbol.as_str().to_string())
                    .or_insert_with(|| rendered_path.clone());
            }
        }
    }
    index
}

fn preloaded_source_index(lisp_dir: &Path) -> Arc<PreloadedSourceIndex> {
    static CACHE: OnceLock<PreloadedSourceIndexCache> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut cache = cache.lock().expect("preloaded source index lock");
    Arc::clone(
        cache
            .entry(lisp_dir.to_path_buf())
            .or_insert_with(|| Arc::new(build_preloaded_source_index(lisp_dir))),
    )
}

// True when NAME is a native emaxx builtin that GNU defines in PRELOADED
// LISP (simple.el, lisp.el, subr.el...): such a function is NOT a subr in
// GNU (`subrp' nil; find-func resolves it through `symbol-file').
// The complete immutable source index is memoized per oracle tree.
pub(crate) fn builtin_is_gnu_preloaded_lisp(interp: &Interpreter, name: &str) -> bool {
    let Some(lisp_dir) = preloaded_lisp_directory(interp) else {
        return false;
    };
    preloaded_source_index(&lisp_dir).contains_key(name)
}

#[cfg(test)]
mod preloaded_source_index_tests {
    use super::*;

    #[test]
    fn index_preserves_load_order_and_is_reused_per_source_tree() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("emaxx-preloaded-index-{unique}"));
        let emacs_lisp = root.join("emacs-lisp");
        std::fs::create_dir_all(&emacs_lisp).expect("create preloaded source directory");
        let first = emacs_lisp.join("rmc.el");
        std::fs::write(
            &first,
            "(defun first-probe ())\n(defvar 'quoted-probe nil)\n  (defun indented-probe ())\n",
        )
        .expect("write first preloaded source");
        let second = root.join("international/iso-transl.el");
        std::fs::create_dir_all(second.parent().expect("second source parent"))
            .expect("create second preloaded source directory");
        std::fs::write(
            &second,
            "(defun first-probe ())\n(define-derived-mode derived-probe fundamental-mode \"Probe\")\n",
        )
        .expect("write second preloaded source");

        let first_index = preloaded_source_index(&root);
        let second_index = preloaded_source_index(&root);
        assert!(Arc::ptr_eq(&first_index, &second_index));
        assert_eq!(
            first_index.get("first-probe").map(String::as_str),
            Some(first.to_string_lossy().as_ref())
        );
        assert_eq!(
            first_index.get("quoted-probe").map(String::as_str),
            Some(first.to_string_lossy().as_ref())
        );
        assert_eq!(
            first_index.get("derived-probe").map(String::as_str),
            Some(second.to_string_lossy().as_ref())
        );
        assert!(!first_index.contains_key("indented-probe"));

        std::fs::remove_dir_all(root).expect("remove preloaded source fixture");
    }

    #[test]
    fn unique_preloaded_ownership_misses_share_one_fast_index() {
        let interp = Interpreter::new();
        let started = std::time::Instant::now();
        for index in 0..512 {
            assert!(!builtin_is_gnu_preloaded_lisp(
                &interp,
                &format!("emaxx-absent-preloaded-symbol-{index}")
            ));
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "preloaded ownership lookups rebuilt or rescanned the source index: {:?}",
            started.elapsed()
        );
    }
}
