use super::*;
use crate::lisp::primitives::string_like;
use crate::lisp::types::Kind;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;

thread_local! {
    static SEMANTIC_CPP_INCLUDE_TAG_CACHE: RefCell<HashMap<PathBuf, Vec<Value>>> =
        RefCell::new(HashMap::new());
}

pub(crate) fn restore_unicode_menu_case_table(
    interp: &mut Interpreter,
    value: Value,
) -> Result<(), String> {
    match value.kind() {
        Kind::Nil | Kind::CharTable(_) => {}
        _ => return Err("Unicode menu case table: expected nil or char-table".into()),
    }
    interp.unicode_menu_case_table = value;
    Ok(())
}

/// keymap.c:lookup_key_1 for Flookup_key: KEYMAP resolved (and loaded)
/// unless it is a list or nil, KEY checked, an empty KEY answering KEYMAP
/// before possibly_translate_key_sequence.
fn lookup_key_once(
    interp: &mut Interpreter,
    keymap: Value,
    key: Value,
    accept_default: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let keymap = if !keymap.is_cons() && !keymap.is_nil() {
        get_keymap(interp, keymap, true, true, env)?
    } else {
        keymap
    };
    if !is_vector_value(&key) && !key.is_string() {
        return Err(LispError::WrongTypeArgument("arrayp".into(), key));
    }
    interp.with_lisp_stack_roots(&(keymap, key), |interp| {
        if sequence_length_value(&key)? == 0 {
            return Ok(keymap);
        }
        let translated = possibly_translate_key_sequence(interp, key, env)?;
        keymap_lookup_live_sequence_value(interp, &keymap, translated, accept_default, env)
    })
}

fn lookup_key_value(
    interp: &mut Interpreter,
    keymap: Value,
    key: Value,
    accept_default: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut found = lookup_key_once(interp, keymap, key, accept_default, env)?;
    if !found.is_nil() && !is_number_value(&found) {
        return Ok(found);
    }
    // Flookup_key's compatibility fallback applies only to a vector whose
    // original first event is menu-bar, after exact lookup has failed.
    let Kind::Vector(original) = key.kind() else {
        return Ok(found);
    };
    if !original
        .get(0)
        .is_some_and(|event| values_eq_in_env(interp, &event, &Value::symbol("menu-bar"), env))
    {
        return Ok(found);
    }
    let unicode = match interp.unicode_menu_case_table.kind() {
        Kind::CharTable(table) => table,
        _ => {
            let table = super::strings::call(
                interp,
                "unicode-property-table-internal",
                &[Value::symbol("lowercase")],
                env,
            )?;
            let Kind::CharTable(table) = table.kind() else {
                return Ok(found);
            };
            if !table.is_uniprop() {
                return Ok(found);
            }
            interp.unicode_menu_case_table = Value::CharTable(table);
            table
        }
    };
    let local = interp.current_case_table_id();
    let new_key = Value::vector(std::iter::repeat_n(Value::Nil, original.len()));
    let Kind::Vector(converted) = new_key.kind() else {
        unreachable!()
    };
    interp.with_lisp_stack_roots(
        &vec![keymap, key, new_key, Value::CharTable(local)],
        |interp| {
            for table in [unicode, local] {
                for index in 0..original.len() {
                    let item = original.get(index).expect("vector length is fixed");
                    if !item.is_symbol()
                        && !(symbols_with_pos_enabled(interp, env)
                            && symbol_with_pos_parts(interp, &item).is_some())
                    {
                        converted.set(index, item);
                        continue;
                    }
                    let name = direct_symbol_name(interp, &[item], env)?;
                    let string = string_like(&name).expect("symbol-name returns a string");
                    let lowered = if !string.multibyte {
                        super::strings::call(interp, "downcase", &[name], env)?
                    } else {
                        let mut text = String::new();
                        let mut extended = Vec::new();
                        for (position, code) in string.character_codes().into_iter().enumerate() {
                            let mapped = interp
                                .char_table_get(table, code as u32)
                                .filter(|value| !value.is_nil())
                                .map_or(Ok(code), |value| value.as_integer())?;
                            if !(0..=0x3f_ffff).contains(&mapped) {
                                return Err(LispError::Signal("Invalid character".into()));
                            }
                            push_character_code(&mut text, &mut extended, position, mapped as u32);
                        }
                        make_shared_string_value_with_extended_chars(text, None, true, extended)
                    };
                    converted.set(index, super::call(interp, "intern", &[lowered], env)?);
                }
                found = lookup_key_once(interp, keymap, new_key, accept_default, env)?;
                if !found.is_nil() && !is_number_value(&found) {
                    break;
                }
                for index in 0..converted.len() {
                    let item = converted.get(index).expect("vector length is fixed");
                    if !item.is_symbol() {
                        continue;
                    }
                    let name = direct_symbol_name(interp, &[item], env)?;
                    let string = string_like(&name).expect("symbol-name returns a string");
                    if !string.text.contains(' ') {
                        continue;
                    }
                    let dashed = make_shared_string_value_with_extended_chars(
                        string.text.replace(' ', "-"),
                        None,
                        true,
                        string.extended_chars,
                    );
                    converted.set(index, super::call(interp, "intern", &[dashed], env)?);
                }
                found = lookup_key_once(interp, keymap, new_key, accept_default, env)?;
                if !found.is_nil() && !is_number_value(&found) {
                    break;
                }
            }
            Ok(found)
        },
    )
}

fn key_binding_value(
    interp: &mut Interpreter,
    key: Value,
    accept_default: bool,
    no_remap: bool,
    mut position: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    if position.is_nil()
        && let Kind::Vector(vector) = key.kind()
    {
        let Some(first) = vector.get(0) else {
            return Ok(Value::Nil);
        };
        let event = if first.is_symbol() && vector.len() > 1 {
            vector.get(1).expect("vector has a second event")
        } else {
            first
        };
        if let Some((head, parameters)) = event.cons_values()
            && let Some((start, _)) = parameters.cons_values()
            && let Kind::Symbol(name) = head.kind()
            && interp
                .get_symbol_property(&name, "event-kind")
                .is_some_and(|kind| kind.eq_value(Value::symbol("mouse-click")))
        {
            position = start;
        }
    }
    interp.with_lisp_stack_roots(&(key, position), |interp| {
        // keymap.c:Fkey_binding performs one Flookup_key on the active
        // map list. Searching each map separately skips prefix filters
        // and loses composition and mutation of the original sequence.
        let maps = Value::list(current_active_maps(interp, env, true, Some(&position))?);
        let binding = interp.with_lisp_stack_roots(&maps, |interp| {
            lookup_key_value(interp, maps, key, accept_default, env)
        })?;
        if binding.is_nil() || matches!(binding.kind(), Kind::Integer(_)) {
            return Ok(Value::Nil);
        }
        if no_remap || !binding.is_symbol() {
            return Ok(binding);
        }
        interp.with_lisp_stack_roots(&binding, |interp| {
            // A filter may have replaced an active map. GNU obtains the
            // active maps again for Fcommand_remapping after lookup.
            let remapped = command_remapping_value(interp, binding, position, Value::Nil, env)?;
            Ok(if remapped.is_nil() { binding } else { remapped })
        })
    })
}

fn command_remapping_value(
    interp: &mut Interpreter,
    command: Value,
    position: Value,
    maps: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    if !command.is_symbol() {
        return Ok(Value::Nil);
    }
    let key = Value::vector([Value::symbol("remap"), command]);
    interp.with_lisp_stack_roots(&(key, (maps, position)), |interp| {
        let binding = if maps.is_nil() {
            key_binding_value(interp, key, false, true, position, env)?
        } else {
            lookup_key_value(interp, maps, key, false, env)?
        };
        Ok(if matches!(binding.kind(), Kind::Integer(_)) {
            Value::Nil
        } else {
            binding
        })
    })
}

fn map_keymap_direct_value(
    interp: &mut Interpreter,
    function: &Value,
    keymap: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    interp.with_lisp_stack_roots(&vec![*function, *keymap], |interp| {
        map_keymap_own_entries(
            interp,
            *keymap,
            &mut |interp, key, value, env| {
                interp.call_function_value(*function, None, &[key, value], env)?;
                Ok(())
            },
            env,
        )
    })
}

/// get_backtrace_frame's CHECK_FIXNAT (NFRAMES).
/// eval.c:get_backtrace_starting_at's reading of BASE: (OFFSET . BASE)
/// when its car is a fixnum, else BASE itself with no offset.
fn backtrace_base(base: Value) -> (i64, Value) {
    match base.cons_ref() {
        Some(cell) => match cell.car.get().fixnum() {
            Some(offset) => (offset, cell.cdr.get()),
            None => (0, base),
        },
        None => (0, base),
    }
}

fn backtrace_nframes(nframes: Value) -> Result<usize, LispError> {
    match nframes.kind() {
        Kind::Integer(count) if count >= 0 => Ok(count as usize),
        _ => Err(LispError::WrongTypeArgument("wholenump".into(), nframes)),
    }
}

/// thread.c's NAME argument: nil, or a string kept as given
/// (CHECK_STRING).
fn thread_object_name(name: Option<&Value>) -> Result<Value, LispError> {
    match name {
        None => Ok(Value::Nil),
        Some(name) if name.is_nil() || name.is_string() => Ok(*name),
        Some(name) => Err(LispError::WrongTypeArgument("stringp".into(), *name)),
    }
}

fn vector_index_description(index: u32) -> Result<String, LispError> {
    key_sequence_binding_text(&Value::vector([Value::Integer(i64::from(index))]))
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
    match args[0].kind() {
        Kind::CharTable(table_id) => {
            for entry in interp
                .char_table_effective_ranges(table_id)
                .unwrap_or_default()
            {
                if entry.value.is_nil() {
                    continue;
                }
                if let Some((_, end, previous)) = ranges.last_mut()
                    && end.saturating_add(1) == entry.start
                    && values_equal(interp, previous, &entry.value)?
                {
                    *end = entry.end;
                } else {
                    ranges.push((entry.start, entry.end, entry.value));
                }
            }
        }
        vector if is_vector_value(&vector.value()) => {
            for (index, value) in vector_items(&vector.value())?.into_iter().enumerate() {
                if value.is_nil() {
                    continue;
                }
                let index = u32::try_from(index)
                    .map_err(|_| LispError::Signal("Vector is too large".into()))?;
                if let Some((_, end, previous)) = ranges.last_mut()
                    && end.saturating_add(1) == index
                    && values_equal(interp, previous, &value)?
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
                other.value().type_name(),
            ));
        }
    }

    let describer = args
        .get(1)
        .filter(|value| !value.is_nil())
        .cloned()
        .unwrap_or_else(|| Value::Symbol("princ".into()));
    let output_buffer = Value::Buffer(interp.buffer);
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
            interp.call_function_value(describer, None, &[value], env)?;
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
                define_key(
                    interp,
                    args[0],
                    args[1],
                    args[2],
                    args.get(3).is_some_and(Value::is_truthy),
                    env,
                )
            }
            "lookup-key" => {
                need_arg_range(name, args, 2, 3)?;
                lookup_key_value(
                    interp,
                    args[0],
                    args[1],
                    args.get(2).is_some_and(Value::is_truthy),
                    env,
                )
            }
            "accessible-keymaps" => {
                need_arg_range(name, args, 1, 2)?;
                accessible_keymaps(
                    interp,
                    args[0],
                    args.get(1).copied().unwrap_or(Value::Nil),
                    env,
                )
            }
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
                let normalized_key = normalize_lucid_key_events(interp, &args[0], env)?;
                let key_parts = key_sequence_keymap_parts(&normalized_key)?;
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
                    if binding.is_nil() || matches!(binding.kind(), Kind::Integer(_)) {
                        continue;
                    }
                    let entry = Value::cons(Value::Symbol(mode.into()), binding);
                    if keymapp(interp, binding, env) {
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
                key_binding_value(
                    interp,
                    args[0],
                    args.get(1).is_some_and(Value::is_truthy),
                    args.get(2).is_some_and(Value::is_truthy),
                    args.get(3).copied().unwrap_or(Value::Nil),
                    env,
                )
            }
            "keymap-prompt" => {
                need_args(name, args, 1)?;
                keymap_prompt(interp, args[0], env)
            }
            "command-remapping" => {
                need_arg_range(name, args, 1, 3)?;
                command_remapping_value(
                    interp,
                    args[0],
                    args.get(1).copied().unwrap_or(Value::Nil),
                    args.get(2).copied().unwrap_or(Value::Nil),
                    env,
                )
            }
            "keymap-parent" => {
                need_args(name, args, 1)?;
                keymap_parent(interp, args[0], true, env)
            }
            "set-keymap-parent" => {
                need_args(name, args, 2)?;
                set_keymap_parent(interp, args[0], args[1], env)
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
                let function = args[0];
                map_keymap(
                    interp,
                    args[1],
                    &mut |interp, key, value, env| {
                        interp.call_function_value(function, None, &[key, value], env)?;
                        Ok(())
                    },
                    true,
                    env,
                )?;
                Ok(Value::Nil)
            }
            "map-keymap-internal" => {
                need_args(name, args, 2)?;
                let keymap = get_keymap(interp, args[1], true, true, env)?;
                map_keymap_direct_value(interp, &args[0], &keymap, env)
            }
            "describe-vector" => describe_vector_value(interp, args, env),
            "use-local-map" => {
                need_args(name, args, 1)?;
                let keymap = if args[0].is_nil() {
                    Value::Nil
                } else {
                    get_keymap(interp, args[0], true, true, env)?
                };
                // bset_keymap: a slot of the buffer, no Lisp variable.
                interp.buffer.borrow_mut().set_keymap(keymap);
                Ok(Value::Nil)
            }
            "use-global-map" => {
                need_args(name, args, 1)?;
                let keymap = get_keymap(interp, args[0], true, true, env)?;
                interp.set_current_global_map_value(keymap);
                Ok(Value::Nil)
            }
            "current-local-map" => {
                need_args(name, args, 0)?;
                Ok(interp.buffer.borrow().keymap())
            }
            "current-global-map" => {
                need_args(name, args, 0)?;
                Ok(interp.current_global_map_value())
            }
            "widget-get" => {
                need_args(name, args, 2)?;
                widget_get(interp, env, args[0], args[1])
            }
            "widget-put" => {
                need_args(name, args, 3)?;
                // fns.c:Fwidget_put: CHECK_CONS, then plist_put on the cdr.
                let Some(cell) = args[0].cons_ref() else {
                    return Err(wrong_type_argument("consp", args[0]));
                };
                let plist = super::collections::plist_put_eq(
                    interp,
                    cell.cdr.get(),
                    args[1],
                    args[2],
                    env,
                )?;
                cell.cdr.set(plist);
                Ok(args[2])
            }
            "widget-apply" => {
                need_arg_range(name, args, 2, usize::MAX)?;
                // fns.c:Fwidget_apply: Fapply of the property's value, nil
                // or not, to WIDGET and ARGS.
                let function = widget_get(interp, env, args[0], args[1])?;
                let mut call_args = Vec::with_capacity(args.len() - 1);
                call_args.push(args[0]);
                call_args.extend_from_slice(&args[2..]);
                interp.call_function_value(function, None, &call_args, env)
            }
            "symbol-function" => direct_symbol_function(interp, args, env),
            "symbol-name" => direct_symbol_name(interp, args, env),
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
                                    .unwrap_or_else(|| "unknown".into())
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
                                .unwrap_or_else(|| "unknown".into())
                                .into(),
                        )
                    }))
            }
            "system-name" => {
                if !args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                Ok(system_name_lisp_value())
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
                let full_name = match requested.kind() {
                    Kind::Integer(_) | Kind::BigInteger(_) | Kind::Float(_) => {
                        user_full_name_from_uid(legacy_unsigned_id(requested)?)
                    }
                    Kind::StringObject(_) => {
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
                Ok(Value::Symbol(effective_text_quoting_style(interp).into()))
            }
            "user-uid" => Ok(Value::Integer(current_user_id()? as i64)),
            "user-real-uid" => Ok(Value::Integer(current_real_user_id()? as i64)),
            "group-gid" => Ok(Value::Integer(current_group_id()? as i64)),
            "group-real-gid" => Ok(Value::Integer(current_real_group_id()? as i64)),
            "group-name" => {
                need_args(name, args, 1)?;
                if !matches!(
                    args[0].kind(),
                    Kind::Integer(_) | Kind::BigInteger(_) | Kind::Float(_) | Kind::Cons(_)
                ) {
                    return Err(LispError::Signal("Invalid GID specification".into()));
                }
                let gid = legacy_unsigned_id(&args[0])?;
                Ok(group_name_from_gid(i64::from(gid))?
                    .map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Nil))
            }
            "lock-buffer" => {
                need_arg_range(name, args, 0, 1)?;
                lock_buffer(interp, env, args.first().copied())
            }
            "unlock-buffer" => {
                need_args(name, args, 0)?;
                unlock_buffer(interp, env, interp.current_buffer_id())?;
                Ok(Value::Nil)
            }

            "recent-auto-save-p" => {
                need_args(name, args, 0)?;
                Ok(if interp.buffer.borrow().recent_auto_save_p() {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "set-buffer-auto-saved" => {
                need_args(name, args, 0)?;
                // The auto-save mark at the text as it is, the size it
                // was saved at, and no failure.
                interp.buffer.borrow_mut().set_autosaved();
                let size = interp.buffer.borrow().size_total();
                let id = interp.current_buffer_id();
                interp.set_buffer_local_value_symbol(
                    id,
                    &cached_symbol!("buffer-saved-size").with(Clone::clone),
                    Value::Integer(size as i64),
                );
                interp.buffer.borrow_mut().auto_save_failure_time = 0;
                Ok(Value::Nil)
            }
            "clear-buffer-auto-save-failure" => {
                need_args(name, args, 0)?;
                interp.buffer.borrow_mut().auto_save_failure_time = 0;
                Ok(Value::Nil)
            }
            "next-read-file-uses-dialog-p" => {
                need_args(name, args, 0)?;
                // fileio.c:Fnext_read_file_uses_dialog_p requires a toolkit
                // AND a window-system frame. GNU's initial batch frame is
                // a terminal frame even in the NS build; noninteractive is
                // not evidence that a graphical display is available.
                if !cfg!(target_os = "macos") {
                    return Ok(Value::Nil);
                }
                let last_nonmenu_event = interp
                    .lookup_var("last-nonmenu-event", env)
                    .unwrap_or(Value::Nil);
                let event_allows = last_nonmenu_event.is_nil()
                    || matches!(last_nonmenu_event.kind(), Kind::Cons(_));
                let use_dialog = interp
                    .lookup_var("use-dialog-box", env)
                    .is_some_and(|value| value.is_truthy());
                let use_file_dialog = interp
                    .lookup_var("use-file-dialog", env)
                    .is_some_and(|value| value.is_truthy());
                let window_system =
                    crate::lisp::primitives::call(interp, "window-system", &[], env)?.is_truthy();
                Ok(
                    if event_allows && use_dialog && use_file_dialog && window_system {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "do-auto-save" => {
                need_arg_range(name, args, 0, 2)?;
                crate::lisp::primitives::file_io::do_auto_save(
                    interp,
                    env,
                    args.first().copied().unwrap_or(Value::Nil),
                    args.get(1).copied().unwrap_or(Value::Nil),
                )
            }
            "unix-sync" => {
                need_args(name, args, 0)?;
                Ok(Value::Nil)
            }
            "set-binary-mode" => {
                need_args(name, args, 2)?;
                match args[0].kind() {
                    Kind::Symbol(stream)
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
                // Fobarray_make: room for SIZE symbols, 2^(elogb (SIZE) + 1)
                // buckets.
                let mut bits = crate::lisp::types::obarray::DEFAULT_BITS;
                if let Some(size) = args.first().filter(|size| !size.is_nil()) {
                    let size_value = match size.kind() {
                        Kind::Integer(value) if value >= 0 => value as u64,
                        _ => return Err(LispError::WrongTypeArgument("wholenump".into(), *size)),
                    };
                    bits = u64::BITS - size_value.leading_zeros();
                    if bits > crate::lisp::types::obarray::MAX_BITS {
                        return Err(LispError::SignalValue(Value::list([
                            Value::symbol("args-out-of-range"),
                            *size,
                        ])));
                    }
                }
                Ok(make_obarray_with_bits(interp, bits))
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
                obarray_bucket_lists(interp, &args[0])
            }
            "define-hash-table-test" => {
                need_args(name, args, 3)?;
                let symbol = args[0].as_symbol()?;
                let spec = Value::list([args[1], args[2]]);
                interp.put_symbol_property(symbol, "hash-table-test", spec);
                Ok(spec)
            }
            "make-hash-table" => {
                let mut used = vec![false; args.len()];
                let mut argument = |keyword: &str, default: Value| {
                    let keyword = Value::symbol(keyword);
                    for index in 0..args.len().saturating_sub(1) {
                        if !used[index] && values_eq_in_env(interp, &args[index], &keyword, env) {
                            used[index] = true;
                            used[index + 1] = true;
                            return args[index + 1];
                        }
                    }
                    default
                };
                let test = argument(":test", Value::symbol("eql"));
                // fns.c resolves the descriptor before checking size or
                // weakness, and allocates the table only after all keywords.
                let descriptor = hash_table_test_descriptor(interp, test, env)?;
                let purecopy = argument(":purecopy", Value::Nil);
                let size = argument(":size", Value::Nil);
                let weakness = argument(":weakness", Value::Nil);
                let capacity = if size.is_nil() {
                    0
                } else {
                    size.as_fixnum()
                        .ok()
                        .and_then(|size| usize::try_from(size).ok())
                        .ok_or_else(|| {
                            LispError::SignalValue(Value::list([
                                Value::symbol("error"),
                                Value::string("Invalid hash table size"),
                                size,
                            ]))
                        })?
                };
                let weak = hash_table_weakness(interp, weakness, env)?;
                let mut index = 0;
                while index < args.len() {
                    if !used[index] {
                        if [":rehash-size", ":rehash-threshold"]
                            .into_iter()
                            .any(|name| {
                                values_eq_in_env(interp, &args[index], &Value::symbol(name), env)
                            })
                        {
                            index += 2;
                            continue;
                        }
                        return Err(LispError::SignalValue(Value::list([
                            Value::symbol("error"),
                            Value::string("Invalid argument list"),
                            args[index],
                        ])));
                    }
                    index += 1;
                }
                hash_table_from_test(descriptor, capacity, weak, purecopy)
            }
            "hash-table-p" => {
                need_args(name, args, 1)?;
                Ok(if matches!(args[0].kind(), Kind::HashTable(_)) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "copy-hash-table" => {
                need_args(name, args, 1)?;
                Ok(Value::HashTable(check_hash_table(&args[0])?.copy()))
            }
            "gethash" => {
                need_arg_range(name, args, 2, 3)?;
                let table = check_hash_table(&args[1])?;
                let (_, slot) = hash_table_lookup(interp, table, &args[0], env)?;
                Ok(slot
                    .and_then(|slot| table.entry(slot))
                    .map(|(_, value)| value)
                    .unwrap_or_else(|| args.get(2).copied().unwrap_or(Value::Nil)))
            }
            "puthash" => {
                need_args(name, args, 3)?;
                let table = check_hash_table(&args[2])?;
                hash_table_put(interp, table, args[0], args[1], env)?;
                Ok(args[1])
            }
            "maphash" => {
                need_args(name, args, 2)?;
                let table = check_hash_table(&args[1])?;
                let mut slot = 0;
                while let Some((index, key, value)) = table.entry_at_or_after(slot) {
                    slot = index + 1;
                    call_function_value(interp, &args[0], &[key, value], env)?;
                }
                Ok(Value::Nil)
            }
            "remhash" => {
                need_args(name, args, 2)?;
                let table = check_hash_table(&args[1])?;
                check_hash_table_mutable(table)?;
                if let (_, Some(slot)) = hash_table_lookup(interp, table, &args[0], env)? {
                    table.remove(slot);
                }
                Ok(Value::Nil)
            }
            "clrhash" => {
                need_args(name, args, 1)?;
                let table = check_hash_table(&args[0])?;
                check_hash_table_mutable(table)?;
                table.clear();
                Ok(args[0])
            }
            "hash-table-count" => {
                need_args(name, args, 1)?;
                Ok(Value::Integer(check_hash_table(&args[0])?.count() as i64))
            }
            "hash-table-rehash-size" | "hash-table-rehash-threshold" => {
                need_args(name, args, 1)?;
                check_hash_table(&args[0])?;
                Ok(Value::float(if name == "hash-table-rehash-size" {
                    1.5
                } else {
                    0.8125
                }))
            }
            "hash-table-size" => {
                need_args(name, args, 1)?;
                Ok(Value::Integer(check_hash_table(&args[0])?.capacity() as i64))
            }
            "hash-table-test" => {
                need_args(name, args, 1)?;
                Ok(check_hash_table(&args[0])?.test_name())
            }
            "hash-table-weakness" => {
                need_args(name, args, 1)?;
                Ok(hash_table_weakness_value(check_hash_table(&args[0])?))
            }
            "try-completion" => try_completion(interp, args, env),
            "all-completions" => all_completions(interp, args, env),
            "test-completion" => test_completion(interp, args, env),
            "internal-complete-buffer" => internal_complete_buffer(interp, args, env),
            "internal--hash-table-index-size" => {
                need_args(name, args, 1)?;
                Ok(Value::Integer(
                    check_hash_table(&args[0])?.index_size() as i64
                ))
            }
            "internal--hash-table-histogram" => {
                need_args(name, args, 1)?;
                let table = check_hash_table(&args[0])?;
                let mut frequencies = vec![0; table.capacity()];
                for bucket in 0..table.index_size() {
                    let mut slot = table.bucket_head(bucket);
                    let mut count = 0;
                    while slot >= 0 {
                        count += 1;
                        slot = table.next_in_bucket(slot as usize);
                    }
                    if count > 0 {
                        frequencies[count - 1] += 1;
                    }
                }
                Ok(Value::list(
                    frequencies
                        .into_iter()
                        .enumerate()
                        .filter(|&(_, frequency)| frequency > 0)
                        .map(|(index, frequency)| {
                            Value::cons(
                                Value::Integer((index + 1) as i64),
                                Value::Integer(frequency),
                            )
                        }),
                ))
            }
            "internal--hash-table-buckets" => {
                need_args(name, args, 1)?;
                let table = check_hash_table(&args[0])?;
                let mut buckets = Vec::new();
                for bucket in 0..table.index_size() {
                    let mut slot = table.bucket_head(bucket);
                    let mut entries = Vec::new();
                    while slot >= 0 {
                        let index = slot as usize;
                        let (key, _) = table.entry(index).expect("live collision chain");
                        entries.push(Value::cons(
                            key,
                            Value::Integer(i64::from(table.stored_hash(index))),
                        ));
                        slot = table.next_in_bucket(index);
                    }
                    if !entries.is_empty() {
                        buckets.push(Value::list(entries));
                    }
                }
                Ok(Value::list(buckets))
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
                        *interval,
                        &mut crate::lisp::types::Env::new(),
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
                    && let Some(mut buffer) = interp.get_buffer_by_id_mut(buffer_id)
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
                    let conditions = match pair[0].kind() {
                        Kind::Nil => Vec::new(),
                        Kind::Cons(_) => pair[0]
                            .to_vec()?
                            .iter()
                            .map(|condition| condition.as_symbol().map(str::to_string))
                            .collect::<Result<Vec<_>, _>>()?,
                        condition => vec![condition.value().as_symbol()?.to_string()],
                    };
                    if !conditions.is_empty() {
                        active.push((conditions, pair[1]));
                    }
                }
                active.reverse();
                let handler_start = interp.push_handler_bindings(&active);
                let result = interp.call_function_value(args[0], None, &[], env);
                interp.pop_handler_bindings(handler_start);
                result
            }

            "mapbacktrace" => {
                need_arg_range(name, args, 1, 2)?;
                let callback = resolve_callable(interp, &args[0], env)?;
                let (offset, base) = backtrace_base(args.get(1).copied().unwrap_or(Value::Nil));
                let frames = interp.backtrace_frames_snapshot();
                // get_backtrace_starting_at's frame, innermost first in the
                // snapshot; a base with no live activation frame is an
                // empty traversal.
                let Some(start) = interp
                    .backtrace_frame_index(0, base, offset, |object| {
                        crate::lisp::primitives::keymap::indirect_function(interp, object, env)
                    })
                    .map(|index| frames.len() - 1 - index)
                else {
                    return Ok(Value::Nil);
                };
                for (evald, function, frame_args, debug_on_exit) in frames.into_iter().skip(start) {
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
                    )?;
                }
                Ok(Value::Nil)
            }
            "backtrace-frame--internal" => {
                need_args(name, args, 3)?;
                let callback = resolve_callable(interp, &args[0], env)?;
                // eval.c:get_backtrace_frame's CHECK_FIXNAT, then its walk
                // from get_backtrace_starting_at's frame for BASE.
                let nframes = backtrace_nframes(args[1])?;
                let (offset, base) = backtrace_base(args[2]);
                let frame = interp.backtrace_frame_at(nframes, base, offset, |object| {
                    crate::lisp::primitives::keymap::indirect_function(interp, object, env)
                });
                let Some((evald, function, frame_args, debug_on_exit)) = frame else {
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
                    &[evald, function, frame_args, flags],
                    env,
                )
            }
            "backtrace-debug" => {
                need_arg_range(name, args, 2, 3)?;
                // eval.c:Fbacktrace_debug: CHECK_FIXNUM, then
                // get_backtrace_frame's CHECK_FIXNAT and walk; the flag is
                // set on the frame found, and FLAG is the value.
                if args[0].fixnum().is_none() {
                    return Err(LispError::WrongTypeArgument("fixnump".into(), args[0]));
                }
                let nframes = backtrace_nframes(args[0])?;
                let (offset, base) = backtrace_base(args.get(2).copied().unwrap_or(Value::Nil));
                let index = interp.backtrace_frame_index(nframes, base, offset, |object| {
                    crate::lisp::primitives::keymap::indirect_function(interp, object, env)
                });
                if let Some(index) = index {
                    interp.set_frame_debug_on_exit(index, args[1].is_truthy());
                }
                Ok(args[1])
            }
            "backtrace-eval" => {
                need_arg_range(name, args, 2, 3)?;
                let index = backtrace_nframes(args[1])?;
                let base = args.get(2).filter(|value| !value.is_nil());
                // The frame's environment is the alist itself: an
                // assignment made here is a setcdr on the suspended
                // activation's own binding cons.
                let mut context = interp.backtrace_frame_context_env(index, base);
                interp.eval(&args[0], &mut context)
            }
            "backtrace--locals" => {
                need_args(name, args, 2)?;
                let index = backtrace_nframes(args[0])?;
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
                need_arg_range(name, args, 1, 2)?;
                let thread_name = thread_object_name(args.get(1))?;
                interp.make_thread(args[0], thread_name)
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
                Ok(interp.thread_name(interp.resolve_thread_id(&args[0])?))
            }
            "thread-signal" => {
                need_args(name, args, 3)?;
                interp.signal_thread(interp.resolve_thread_id(&args[0])?, args[1], args[2], env)
            }
            "thread-last-error" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(interp.thread_last_error(args.first().is_some_and(Value::is_truthy)))
            }
            "thread-yield" => {
                need_args(name, args, 0)?;
                interp.yield_current_thread(env)
            }
            "make-mutex" => {
                need_arg_range(name, args, 0, 1)?;
                let mutex_name = thread_object_name(args.first())?;
                Ok(interp.make_mutex(mutex_name))
            }
            "mutex-name" => {
                need_args(name, args, 1)?;
                Ok(interp.mutex_name(interp.resolve_mutex_id(&args[0])?))
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
                let condvar_name = thread_object_name(args.get(1))?;
                Ok(interp.make_condition_variable(mutex_id, condvar_name))
            }
            "condition-mutex" => {
                need_args(name, args, 1)?;
                let condvar_id = interp.resolve_condition_variable_id(&args[0])?;
                let mutex_id = interp
                    .condition_variable_mutex_id(condvar_id)
                    .ok_or_else(|| {
                        LispError::WrongTypeArgument("condition-variable-p".into(), args[0])
                    })?;
                Ok(interp.record_value(mutex_id))
            }
            "condition-name" => {
                need_args(name, args, 1)?;
                Ok(interp.condition_variable_name(interp.resolve_condition_variable_id(&args[0])?))
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
                // search.c Fregexp_quote: a backslash before each special
                // byte of STRING's data, the result as multibyte as STRING
                // (without properties); STRING itself when none needs one.
                need_args(name, args, 1)?;
                let Kind::StringObject(state) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("stringp".into(), args[0]));
                };
                let special = |byte: &u8| {
                    matches!(byte, b'[' | b'*' | b'.' | b'\\' | b'?' | b'+' | b'^' | b'$')
                };
                let (quoted, characters, multibyte) = {
                    let state = state.borrow();
                    if !state.bytes().iter().any(special) {
                        return Ok(args[0]);
                    }
                    let mut quoted = Vec::with_capacity(state.bytes().len() * 2);
                    for byte in state.bytes() {
                        if special(byte) {
                            quoted.push(b'\\');
                        }
                        quoted.push(*byte);
                    }
                    let added = quoted.len() - state.bytes().len();
                    (quoted, state.len() + added, state.is_multibyte())
                };
                StringObjectRef::from_storage(quoted, characters, multibyte)
                    .map(Value::StringObject)
                    .map_err(|error| string_storage_error(interp, env, error))
            }
            "kill-all-local-variables" => {
                need_arg_range(name, args, 0, 1)?;
                let buffer_id = interp.current_buffer_id();
                let kill_permanent = args.first().is_some_and(Value::is_truthy);
                run_named_hooks(interp, "change-major-mode-hook", env, Some(buffer_id))?;
                // buffer.c:reset_buffer_local_variables.  Only the alist
                // cells (ordinary buffer-local variables) participate in
                // watchers and `permanent-local' properties; the
                // DEFVAR_PER_BUFFER slots are reset directly: an index -1
                // slot is kept (but for the three assigned below), an
                // indexed one returns to its default unless
                // buffer_permanent_local_flags keeps it.
                let locals = interp.buffer_local_cells(buffer_id);
                let mut permanent = Vec::new();
                for (name, value) in &locals {
                    if let Some(slot) = crate::lisp::eval::native_per_buffer_slot(name.as_str()) {
                        if slot.always_local || (slot.permanent && !kill_permanent) {
                            permanent.push((*name, *value));
                        } else {
                            interp.mark_buffer_local_special_binding_killed(buffer_id, name);
                        }
                        continue;
                    }
                    let prop = cached_symbol!("permanent-local")
                        .with(|property| interp.get_symbol_property_of(name, property))
                        .filter(|value| !kill_permanent && value.is_truthy());
                    interp.notify_variable_watchers(
                        name,
                        Value::Nil,
                        "makunbound",
                        Some(buffer_id),
                        env,
                    )?;
                    match prop {
                        // A partially permanent hook variable keeps only the
                        // elements that want to be kept.
                        Some(prop) if prop.eq_value(Value::symbol("permanent-local-hook")) => {
                            let newlist = permanent_hook_elements(interp, *value, env)?;
                            interp.notify_variable_watchers(
                                name,
                                newlist,
                                "makunbound",
                                Some(buffer_id),
                                env,
                            )?;
                            permanent.push((*name, newlist));
                            continue;
                        }
                        Some(_) => {
                            permanent.push((*name, *value));
                            continue;
                        }
                        None => {}
                    }
                    interp.mark_buffer_local_special_binding_killed(buffer_id, name);
                }
                if kill_permanent {
                    interp.clear_buffer_local_state(buffer_id);
                } else {
                    interp.clear_buffer_local_state_for_mode_change(buffer_id);
                }
                for (name, value) in permanent {
                    interp.set_buffer_local_value_symbol(buffer_id, &name, value);
                }
                interp.reset_major_mode_slots(buffer_id);
                Ok(Value::Nil)
            }
            "force-mode-line-update" => {
                need_arg_range(name, args, 0, 1)?;
                Ok(Value::Nil)
            }
            "garbage-collect" => direct_garbage_collect(interp, args, env),
            // alloc.c's watchers of the GC variables (abi's
            // C_VARIABLE_WATCHERS): the stored value retunes
            // consing_until_gc with the other variable's current value.
            "watch_gc_cons_threshold" => {
                need_args(name, args, 4)?;
                if let Some(threshold) = args[1].integer_value() {
                    let (_, percentage) = crate::lisp::native_comp::gc_tuning(interp, env);
                    crate::lisp::native_comp::bump_consing_until_gc(interp, threshold, percentage);
                }
                Ok(Value::Nil)
            }
            "watch_gc_cons_percentage" => {
                need_args(name, args, 4)?;
                let (threshold, _) = crate::lisp::native_comp::gc_tuning(interp, env);
                let percentage = match args[1].kind() {
                    Kind::Float(value) => Some(value.get()),
                    _ => None,
                };
                crate::lisp::native_comp::bump_consing_until_gc(interp, threshold, percentage);
                Ok(Value::Nil)
            }
            "garbage-collect-maybe" => {
                need_args(name, args, 1)?;
                let Kind::Integer(factor) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("wholenump".into(), args[0]));
                };
                if factor < 0 {
                    return Err(LispError::WrongTypeArgument("wholenump".into(), args[0]));
                }
                if !crate::lisp::native_comp::garbage_collection_maybe_due(interp, factor) {
                    return Ok(Value::Nil);
                }
                // alloc.c:Fgarbage_collect_maybe calls garbage_collect when
                // since_gc > gc_threshold / factor and returns Qt then, even
                // when the collection itself is inhibited.
                let _ = crate::lisp::native_comp::garbage_collect_now(interp, env)?;
                Ok(Value::T)
            }
            "memory-use-counts" => {
                need_args(name, args, 0)?;
                // alloc.c:Fmemory_use_counts: `make_int' of each counter, in
                // DEFVAR order. The words are the allocator's own, raised at
                // its allocation points and never lowered by collection.
                Ok(Value::list(
                    crate::lisp::alloc::counters::memory_use_counts()
                        .map(crate::lisp::primitives::normalize_integer_value),
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
                super::misc::current_cpu_time_value()
            }
            "emacs-pid" => {
                need_args(name, args, 0)?;
                Ok(Value::Integer(emacs_pid_value()))
            }
            "type-of" => {
                need_args(name, args, 1)?;
                // data.c:Ftype_of inspects object tags. Old vector-struct
                // policy belongs to cl-lib.el's advice around this subr,
                // not to the original primitive reached by that advice.
                let name = match args[0].kind() {
                    Kind::Nil => "symbol",
                    Kind::T => "symbol",
                    Kind::Integer(_) => "integer",
                    Kind::BigInteger(_) => "integer",
                    Kind::Float(_) => "float",
                    Kind::StringObject(_) => "string",
                    Kind::Symbol(_) => "symbol",
                    Kind::SymbolWithPos(_) if symbols_with_pos_enabled(interp, env) => "symbol",
                    Kind::SymbolWithPos(_) => "symbol-with-pos",
                    Kind::Vector(_) => "vector",
                    Kind::Cons(_) if is_vector_value(&args[0]) => "vector",
                    Kind::Cons(_) => "cons",
                    Kind::BuiltinFunc(_) => "subr",
                    // data.c:Ftype_of's PVEC_CLOSURE with a cons code slot.
                    Kind::Closure(closure) => {
                        if closure.is_bytecode() {
                            "byte-code-function"
                        } else {
                            "interpreted-function"
                        }
                    }
                    Kind::Buffer(_) => "buffer",
                    Kind::Marker(_) => "marker",
                    Kind::Overlay(_) => "overlay",
                    Kind::CharTable(_) => "char-table",
                    Kind::HashTable(_) => "hash-table",
                    Kind::SubCharTable(_) => "sub-char-table",
                    Kind::Frame(_) => "frame",
                    Kind::Terminal(_) => "terminal",
                    Kind::LispRecord(_) => return cl_type_value(interp, &args[0]),
                    Kind::BoolVector(_) => "bool-vector",
                    Kind::NativeFunction(_) => "subr",
                    Kind::NativeCompUnit(_) => "native-comp-unit",
                    Kind::Record(id) => {
                        let _record = interp.find_record(id).ok_or_else(|| {
                            LispError::TypeError("record".into(), format!("record<{}>", id.id))
                        })?;
                        return cl_type_value(interp, &args[0]);
                    }
                    Kind::Finalizer(_) => "finalizer",
                    Kind::ReaderForm(_) => {
                        return Err(LispError::Signal(
                            "reader form escaped object materialization".into(),
                        ));
                    }
                    Kind::Unbound => "unbound",
                };
                Ok(Value::Symbol(name.into()))
            }
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
    matches!(value.kind(), Kind::Record(_) | Kind::Closure(_))
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
    let Kind::Closure(lambda) = value.kind() else {
        return None;
    };
    // GNU oclosure-type recognizes a closure whose public slot four is a
    // symbol.  Read the actual slot without reconstructing closure metadata.
    (lambda.public_len() > 4).then(|| lambda.documentation()?.symbol_str().map(String::from))?
}

/// fns.c:Fwidget_get: PROPERTY's value in WIDGET's plist (plist_member),
/// else in the widget its type's `widget-type' property names, until a
/// nil widget or type; a widget neither nil nor a cons is CHECK_CONSed.
fn widget_get(
    interp: &mut Interpreter,
    env: &mut crate::lisp::types::Env,
    mut widget: Value,
    property: Value,
) -> Result<Value, LispError> {
    loop {
        if widget.is_nil() {
            return Ok(Value::Nil);
        }
        let Some(cell) = widget.cons_ref() else {
            return Err(wrong_type_argument("consp", widget));
        };
        let found =
            super::collections::plist_member_value(interp, env, cell.cdr.get(), property, None)?;
        if let Some(found) = found.cons_ref() {
            return found.cdr.get().car();
        }
        let widget_type = cell.car.get();
        if widget_type.is_nil() {
            return Ok(Value::Nil);
        }
        widget = crate::lisp::primitives::call(
            interp,
            "get",
            &[widget_type, Value::symbol("widget-type")],
            env,
        )?;
    }
}

/// The `symbol-function' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_symbol_function(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "symbol-function";
    need_args(name, args, 1)?;
    // GNU 30.2 data.c:Fsymbol_function uses
    // CHECK_SYMBOL/XSYMBOL, sharing the positioned-symbol
    // contract with `fboundp'.
    let symbol = checked_symbol_identity(interp, &args[0], env)?;
    Ok(match interp.raw_function_binding_symbol(&symbol, env) {
        Some(value) => value,
        // GNU returns nil for an unbound function cell (nadvice's
        // pending-advice path reads it).
        None => Value::Nil,
    })
}

/// The `symbol-name' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_symbol_name(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "symbol-name";
    need_args(name, args, 1)?;
    // GNU 30.2 data.c:Fsymbol_name uses CHECK_SYMBOL/XSYMBOL.
    let symbol_name = match args[0].kind() {
        Kind::Nil => {
            return Ok(crate::lisp::types::symbols::builtin_boolean(false).lisp_name());
        }
        Kind::T => return Ok(crate::lisp::types::symbols::builtin_boolean(true).lisp_name()),
        Kind::Symbol(symbol) => symbol,
        _ if symbols_with_pos_enabled(interp, env) => {
            match symbol_with_pos_parts(interp, &args[0]).map(|(a0, a1)| (a0.kind(), a1)) {
                Some((Kind::Nil, _)) => {
                    return Ok(crate::lisp::types::symbols::builtin_boolean(false).lisp_name());
                }
                Some((Kind::T, _)) => {
                    return Ok(crate::lisp::types::symbols::builtin_boolean(true).lisp_name());
                }
                Some((Kind::Symbol(symbol), _)) => symbol,
                _ => return Err(wrong_type_argument("symbolp", args[0])),
            }
        }
        _ => return Err(wrong_type_argument("symbolp", args[0])),
    };
    Ok(symbol_name.lisp_name())
}

/// Include tags are roots while cached. The captured Unicode menu table is
/// marked with the editor that owns it.
pub(crate) fn mark_semantic_cache_roots(mark: &mut dyn FnMut(&Value)) {
    SEMANTIC_CPP_INCLUDE_TAG_CACHE.with_borrow(|cache| {
        for tags in cache.values() {
            for tag in tags {
                mark(tag);
            }
        }
    });
}

/// The `garbage-collect' primitive, callable directly (a subr's function
/// pointer): alloc.c's Fgarbage_collect is a subr of its own with a small
/// frame above the collection's stack top, and so is this, rather than
/// an arm of the group dispatcher above, whose frame holds every arm's
/// locals and kept a stale word or two above the top.  The report is
/// built after the collection, in a frame of its own.
pub(super) fn direct_garbage_collect(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    need_args("garbage-collect", args, 0)?;
    // The stack top at the subr's entry (alloc.c's Fgarbage_collect calls
    // garbage_collect first thing): the census and the report below live
    // in the closure's frame, under the top, not in this frame above it.
    crate::lisp::alloc::flush_stack_call_func(|| {
        let Some(census) =
            crate::lisp::native_comp::garbage_collect_now_with_symbols_disabled(interp, env)?
        else {
            return Ok(Value::Nil);
        };
        Ok(garbage_collect_report(&census))
    })
}

#[inline(never)]
fn garbage_collect_report(census: &crate::lisp::eval::LiveObjectCensus) -> Value {
    let cons_size = crate::lisp::eval::GNU_CONS_SIZE as i64;
    let entry = |name: &str, rest: &[i64]| {
        Value::list(
            std::iter::once(Value::Symbol(name.into()))
                .chain(rest.iter().map(|n| Value::Integer(*n))),
        )
    };
    Value::list([
        entry("conses", &[cons_size, census.conses as i64, 0]),
        entry(
            "symbols",
            &[
                crate::lisp::eval::SYMBOL_SIZE as i64,
                census.symbols as i64,
                0,
            ],
        ),
        entry(
            "strings",
            &[
                crate::lisp::eval::STRING_HEADER_SIZE as i64,
                census.strings as i64,
                0,
            ],
        ),
        entry("string-bytes", &[1, census.string_bytes as i64]),
        entry(
            "vectors",
            &[
                crate::lisp::eval::GNU_VECTOR_SIZE as i64,
                census.vectors as i64,
            ],
        ),
        entry(
            "vector-slots",
            &[
                crate::lisp::eval::GNU_VECTOR_SLOT_SIZE as i64,
                census.vector_slots as i64,
                0,
            ],
        ),
        entry(
            "floats",
            &[
                crate::lisp::eval::GNU_FLOAT_SIZE as i64,
                census.floats as i64,
                0,
            ],
        ),
        entry(
            "intervals",
            &[
                crate::lisp::eval::GNU_INTERVAL_SIZE as i64,
                census.intervals as i64,
                0,
            ],
        ),
        entry(
            "buffers",
            &[
                crate::lisp::eval::GNU_BUFFER_SIZE as i64,
                census.buffers as i64,
            ],
        ),
    ])
}

/// reset_buffer_local_variables' partially permanent hook: the elements
/// of LIST that are t or a symbol with a `permanent-local-hook' property,
/// in order; a LIST that is no cons is what Fnreverse makes of it.
fn permanent_hook_elements(
    interp: &mut Interpreter,
    list: Value,
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    if !list.is_cons() {
        return super::call(interp, "nreverse", &[list], env);
    }
    let mut kept = Vec::new();
    let mut tail = list;
    while let Some(cell) = tail.cons_ref() {
        let element = cell.car.get();
        if matches!(element.kind(), Kind::T)
            || (element.is_symbol()
                && super::call(
                    interp,
                    "get",
                    &[element, Value::symbol("permanent-local-hook")],
                    env,
                )?
                .is_truthy())
        {
            kept.push(element);
        }
        tail = cell.cdr.get();
    }
    Ok(Value::list(kept))
}
