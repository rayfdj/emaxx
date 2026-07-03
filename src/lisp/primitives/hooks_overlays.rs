use super::*;

pub(crate) fn call_function_value(
    interp: &mut Interpreter,
    function: &Value,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    interp.call_function_value(function.clone(), None, args, env)
}

pub(crate) fn run_change_hooks(
    interp: &mut Interpreter,
    hook_name: &str,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<(), LispError> {
    if interp.change_hooks_are_running() {
        return Ok(());
    }
    let hook_values = interp
        .lookup_var(hook_name, env)
        .map(|value| value.to_vec().unwrap_or_default())
        .or_else(|| interp.buffer_local_hook(interp.current_buffer_id(), hook_name))
        .unwrap_or_default();
    if hook_values.is_empty() {
        return Ok(());
    }
    interp.enter_change_hooks();
    let mut result = Ok(());
    for hook in hook_values {
        if let Err(error) = call_function_value(interp, &hook, args, env) {
            result = Err(error);
            break;
        }
    }
    interp.leave_change_hooks();
    result
}

pub(crate) fn hook_values(
    interp: &Interpreter,
    hook_name: &str,
    env: &crate::lisp::types::Env,
    buffer_id: Option<u64>,
) -> Vec<Value> {
    let mut hooks = interp
        .lookup_var(hook_name, env)
        .map(|value| {
            if value.is_nil() {
                Vec::new()
            } else {
                value.to_vec().unwrap_or_else(|_| vec![value])
            }
        })
        .unwrap_or_default();
    if let Some(id) = buffer_id
        && let Some(local) = interp.buffer_local_hook(id, hook_name)
    {
        hooks.extend(local);
    }
    hooks
}

pub(crate) fn run_named_hooks(
    interp: &mut Interpreter,
    hook_name: &str,
    env: &mut crate::lisp::types::Env,
    buffer_id: Option<u64>,
) -> Result<(), LispError> {
    for hook in hook_values(interp, hook_name, env, buffer_id) {
        call_function_value(interp, &hook, &[], env)?;
    }
    Ok(())
}

// GNU runs the command-loop hooks through safe_run_hooks: an error in one
// hook function is demoted to a message and the remaining functions still
// run.  Nonlocal exits (throw) keep propagating.
pub(crate) fn safe_run_named_hooks(
    interp: &mut Interpreter,
    hook_name: &str,
    env: &mut crate::lisp::types::Env,
    buffer_id: Option<u64>,
) -> Result<(), LispError> {
    for hook in hook_values(interp, hook_name, env, buffer_id) {
        match call_function_value(interp, &hook, &[], env) {
            Ok(_) => {}
            Err(error @ LispError::Throw(_, _)) => return Err(error),
            Err(error) => {
                let function_name = match &hook {
                    Value::Symbol(name) | Value::BuiltinFunc(name) => name.clone(),
                    _ => "anonymous-function".to_string(),
                };
                let message = format!("Error in {hook_name} ({function_name}): {error}");
                let _ = crate::lisp::primitives::call(
                    interp,
                    "message",
                    &[Value::String(message)],
                    env,
                );
            }
        }
    }
    Ok(())
}

pub(crate) fn ert_simulate_command(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_args("ert-simulate-command", args, 1)?;
    let command = args[0].to_vec()?;
    let Some(original_command) = command.first().cloned() else {
        return Err(LispError::TypeError("command".into(), args[0].type_name()));
    };
    if interp
        .lookup_var("unread-command-events", env)
        .is_some_and(|value| value.is_truthy())
    {
        return Err(LispError::Signal(
            "Assertion failed: (not unread-command-events)".into(),
        ));
    }

    let remapped = command_remapping(interp, &original_command, None, env)?;
    let this_command = if remapped.is_nil() {
        original_command.clone()
    } else {
        remapped
    };
    interp.set_variable("deactivate-mark", Value::Nil, env);
    interp.set_variable("this-original-command", original_command.clone(), env);
    interp.set_variable("this-command", this_command.clone(), env);
    run_named_hooks(
        interp,
        "pre-command-hook",
        env,
        Some(interp.current_buffer_id()),
    )?;
    let return_value = call_function_value(interp, &original_command, &command[1..], env)?;
    run_named_hooks(
        interp,
        "post-command-hook",
        env,
        Some(interp.current_buffer_id()),
    )?;
    interp.set_variable("real-last-command", original_command, env);
    interp.set_variable("last-command", this_command, env);
    if interp.lookup_var("last-repeatable-command", env).is_some() {
        let real_last_command = interp
            .lookup_var("real-last-command", env)
            .unwrap_or(Value::Nil);
        interp.set_variable("last-repeatable-command", real_last_command, env);
    }
    Ok(return_value)
}

pub(crate) fn run_write_buffer_hooks_until_success(
    interp: &mut Interpreter,
    env: &mut crate::lisp::types::Env,
) -> Result<bool, LispError> {
    let buffer_id = Some(interp.current_buffer_id());
    for hook_name in [
        "write-contents-functions",
        "local-write-file-hooks",
        "write-file-functions",
    ] {
        for hook in hook_values(interp, hook_name, env, buffer_id) {
            if call_function_value(interp, &hook, &[], env)?.is_truthy() {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub(crate) fn call_named_function(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    match interp.lookup_function(name, env) {
        Ok(function) => call_function_value(interp, &function, args, env),
        Err(_) => Ok(Value::T),
    }
}

pub(crate) fn dispatch_file_notification(
    interp: &mut Interpreter,
    env: &mut Env,
    path: &str,
    action: &str,
) -> Result<(), LispError> {
    let Ok(descriptors) = interp.symbol_value_cell("auto-revert--buffer-by-watch-descriptor")
    else {
        return Ok(());
    };
    let Ok(entries) = descriptors.to_vec() else {
        return Ok(());
    };
    let saved_buffer_id = interp.current_buffer_id();
    for entry in entries {
        let Some((descriptor, Value::Buffer(buffer_id, _))) = entry.cons_values() else {
            continue;
        };
        if !interp.has_buffer_id(buffer_id) {
            continue;
        }
        let event = Value::list([
            descriptor,
            Value::Symbol(action.into()),
            Value::String(path.into()),
        ]);
        call_named_function(interp, "auto-revert-notify-handler", &[event], env)?;
    }
    interp.switch_to_buffer_id(saved_buffer_id)?;
    Ok(())
}

pub(crate) fn active_file_notify_descriptors() -> &'static Mutex<HashSet<i64>> {
    ACTIVE_FILE_NOTIFY_DESCRIPTORS.get_or_init(|| Mutex::new(HashSet::new()))
}

pub(crate) fn callable_display_action_function(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
) -> Option<Value> {
    match value {
        Value::BuiltinFunc(_) | Value::Lambda(..) => Some(value.clone()),
        Value::Symbol(name) if interp.lookup_function(name, env).is_ok() => Some(value.clone()),
        _ => None,
    }
}

pub(crate) fn split_display_buffer_action(
    interp: &Interpreter,
    action: Option<&Value>,
    env: &Env,
) -> (Option<Value>, Value) {
    let Some(action) = action.filter(|value| !value.is_nil()) else {
        return (None, Value::Nil);
    };
    if let Some(function) = callable_display_action_function(interp, action, env) {
        return (Some(function), Value::Nil);
    }
    if let Some((car, cdr)) = action.cons_values()
        && let Some(function) = callable_display_action_function(interp, &car, env)
    {
        return (Some(function), cdr);
    }
    (None, action.clone())
}

pub(crate) fn display_action_inhibits_same_window(action_alist: &Value) -> bool {
    let Ok(entries) = action_alist.to_vec() else {
        return false;
    };
    entries.into_iter().any(|entry| {
        let Ok(key) = entry
            .car()
            .and_then(|value| value.as_symbol().map(|symbol| symbol.to_string()))
        else {
            return false;
        };
        key == "inhibit-same-window" && entry.cdr().map(|value| value.is_truthy()).unwrap_or(false)
    })
}

#[derive(Clone)]
pub(crate) struct OverlayHookCall {
    overlay_id: u64,
    functions: Vec<Value>,
    before_tail: Vec<Value>,
    after_tail: Vec<Value>,
}

pub(crate) fn overlay_hook_functions(
    overlay: &crate::overlay::Overlay,
    property: &str,
) -> Vec<Value> {
    match overlay.get_prop(property) {
        Some(value) => value
            .to_vec()
            .unwrap_or_else(|_| vec![value.clone()])
            .into_iter()
            .filter(|value| value.is_truthy())
            .collect(),
        None => Vec::new(),
    }
}

pub(crate) fn overlay_insert_hook_calls(
    buffer: &crate::buffer::Buffer,
    pos: usize,
    inserted_len: usize,
) -> Vec<OverlayHookCall> {
    let mut calls = Vec::new();
    for overlay in &buffer.overlays {
        if overlay.is_dead() {
            continue;
        }
        if overlay.beg == overlay.end && overlay.beg == pos {
            for property in ["insert-in-front-hooks", "insert-behind-hooks"] {
                let functions = overlay_hook_functions(overlay, property);
                if !functions.is_empty() {
                    calls.push(OverlayHookCall {
                        overlay_id: overlay.id,
                        functions,
                        before_tail: vec![Value::Integer(pos as i64), Value::Integer(pos as i64)],
                        after_tail: vec![
                            Value::Integer(pos as i64),
                            Value::Integer((pos + inserted_len) as i64),
                            Value::Integer(0),
                        ],
                    });
                }
            }
            continue;
        }
        let property = if pos == overlay.beg {
            Some("insert-in-front-hooks")
        } else if pos == overlay.end {
            Some("insert-behind-hooks")
        } else if overlay.beg < pos && pos < overlay.end {
            Some("modification-hooks")
        } else {
            None
        };
        let Some(property) = property else {
            continue;
        };
        let functions = overlay_hook_functions(overlay, property);
        if functions.is_empty() {
            continue;
        }
        calls.push(OverlayHookCall {
            overlay_id: overlay.id,
            functions,
            before_tail: vec![Value::Integer(pos as i64), Value::Integer(pos as i64)],
            after_tail: vec![
                Value::Integer(pos as i64),
                Value::Integer((pos + inserted_len) as i64),
                Value::Integer(0),
            ],
        });
    }
    calls
}

pub(crate) fn overlay_change_hook_calls(
    buffer: &crate::buffer::Buffer,
    from: usize,
    to: usize,
    new_end: usize,
) -> Vec<OverlayHookCall> {
    let mut calls = Vec::new();
    let old_len = to.saturating_sub(from);
    for overlay in &buffer.overlays {
        if overlay.is_dead() || overlay.beg == overlay.end {
            continue;
        }
        if overlay.beg < to && from < overlay.end {
            let functions = overlay_hook_functions(overlay, "modification-hooks");
            if functions.is_empty() {
                continue;
            }
            calls.push(OverlayHookCall {
                overlay_id: overlay.id,
                functions,
                before_tail: vec![Value::Integer(from as i64), Value::Integer(to as i64)],
                after_tail: vec![
                    Value::Integer(from as i64),
                    Value::Integer(new_end as i64),
                    Value::Integer(old_len as i64),
                ],
            });
        }
    }
    calls
}

pub(crate) fn run_overlay_hook_calls(
    interp: &mut Interpreter,
    calls: &[OverlayHookCall],
    after: bool,
    env: &mut crate::lisp::types::Env,
) -> Result<(), LispError> {
    env.push(vec![("inhibit-modification-hooks".into(), Value::T)]);
    for call in calls {
        if after && interp.find_overlay(call.overlay_id).is_none() {
            continue;
        }
        for function in &call.functions {
            let mut args = vec![
                Value::Overlay(call.overlay_id),
                if after { Value::T } else { Value::Nil },
            ];
            args.extend(if after {
                call.after_tail.clone()
            } else {
                call.before_tail.clone()
            });
            call_function_value(interp, function, &args, env)?;
        }
    }
    env.pop();
    Ok(())
}

pub(crate) fn delete_region_with_hooks(
    interp: &mut Interpreter,
    from: usize,
    to: usize,
    env: &mut crate::lisp::types::Env,
) -> Result<String, LispError> {
    let (from, to) = if from <= to { (from, to) } else { (to, from) };
    if from >= to {
        return Ok(String::new());
    }
    let overlay_calls = overlay_change_hook_calls(&interp.buffer, from, to, from);
    run_overlay_hook_calls(interp, &overlay_calls, false, env)?;
    let has_before_hooks = interp
        .lookup_var("before-change-functions", env)
        .map(|value| !value.to_vec().unwrap_or_default().is_empty())
        .or_else(|| {
            interp
                .buffer_local_hook(interp.current_buffer_id(), "before-change-functions")
                .map(|hooks| !hooks.is_empty())
        })
        .unwrap_or(false);
    if has_before_hooks {
        let start_marker = interp.make_marker();
        let end_marker = interp.make_marker();
        let dead_marker = interp.make_marker();
        if let (Value::Marker(start_id), Value::Marker(end_id), Value::Marker(dead_id)) =
            (start_marker, end_marker, dead_marker)
        {
            let buffer_id = interp.current_buffer_id();
            let _ = interp.set_marker(start_id, Some(from), Some(buffer_id));
            let _ = interp.set_marker(end_id, Some(to), Some(buffer_id));
            let _ = interp.set_marker(dead_id, None, None);
            interp.buffer.push_undo_meta(Value::cons(
                Value::Marker(start_id),
                Value::Integer(-(from as i64)),
            ));
            interp.buffer.push_undo_meta(Value::cons(
                Value::Marker(end_id),
                Value::Integer(-(to as i64)),
            ));
            interp
                .buffer
                .push_undo_meta(Value::cons(Value::Marker(dead_id), Value::Integer(-1)));
        }
    }
    run_change_hooks(
        interp,
        "before-change-functions",
        &[Value::Integer(from as i64), Value::Integer(to as i64)],
        env,
    )?;
    let deleted = interp
        .delete_region_current_buffer(from, to)
        .map_err(|e| LispError::Signal(e.to_string()))?;
    run_change_hooks(
        interp,
        "after-change-functions",
        &[
            Value::Integer(from as i64),
            Value::Integer(from as i64),
            Value::Integer((to - from) as i64),
        ],
        env,
    )?;
    run_overlay_hook_calls(interp, &overlay_calls, true, env)?;
    Ok(deleted)
}

pub(crate) fn ensure_region_modifiable(
    interp: &Interpreter,
    from: usize,
    to: usize,
    env: &mut crate::lisp::types::Env,
) -> Result<(), LispError> {
    let (from, to) = if from <= to { (from, to) } else { (to, from) };
    let from = from.max(interp.buffer.point_min());
    let to = to.min(interp.buffer.point_max());
    if from >= to {
        return Ok(());
    }
    let inhibit_read_only = interp
        .lookup_var("inhibit-read-only", env)
        .unwrap_or(Value::Nil);
    let buffer_read_only = buffer_read_only_active(interp, env, &inhibit_read_only);

    for pos in from..to {
        let read_only = interp.buffer.text_property_at(pos, "read-only");
        let suppressor = interp.buffer.text_property_at(pos, "inhibit-read-only");
        if let Some(read_only_value) = read_only {
            if suppressor.is_some_and(|value| value.is_truthy())
                || inhibit_read_only_matches(&inhibit_read_only, &read_only_value)
            {
                continue;
            }
            return Err(LispError::Signal("Text is read-only".into()));
        }
        if buffer_read_only && !suppressor.is_some_and(|value| value.is_truthy()) {
            return Err(buffer_read_only_signal(interp));
        }
    }
    Ok(())
}

pub(crate) fn ensure_insert_modifiable(
    interp: &Interpreter,
    env: &mut crate::lisp::types::Env,
) -> Result<(), LispError> {
    let inhibit_read_only = interp
        .lookup_var("inhibit-read-only", env)
        .unwrap_or(Value::Nil);
    if !buffer_read_only_active(interp, env, &inhibit_read_only) {
        return Ok(());
    }
    let point = interp.buffer.point();
    let suppressor = interp.buffer.text_property_at(point, "inhibit-read-only");
    if suppressor.is_some_and(|value| value.is_truthy()) {
        return Ok(());
    }
    Err(buffer_read_only_signal(interp))
}

fn buffer_read_only_active(
    interp: &Interpreter,
    env: &mut crate::lisp::types::Env,
    inhibit_read_only: &Value,
) -> bool {
    interp
        .lookup_var("buffer-read-only", env)
        .is_some_and(|value| value.is_truthy())
        && !inhibit_read_only.is_truthy()
}

fn buffer_read_only_signal(interp: &Interpreter) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("buffer-read-only".into()),
        Value::Buffer(interp.current_buffer_id(), interp.buffer.name.clone()),
    ]))
}

pub(crate) fn inhibit_read_only_matches(inhibit: &Value, property: &Value) -> bool {
    if inhibit.is_nil() {
        return false;
    }
    if matches!(inhibit, Value::T) {
        return true;
    }
    if let Ok(items) = inhibit.to_vec() {
        return items.into_iter().any(|item| item == *property);
    }
    inhibit == property
}

pub(crate) fn font_lock_add_text_property(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    append: bool,
) -> Result<Value, LispError> {
    need_arg_range(name, args, 4, 5)?;
    let prop = args[2].as_symbol()?.to_string();
    let value = args[3].clone();
    if let Some(object) = args.get(4)
        && string_like(object).is_some()
    {
        let start = args[0].as_integer()?.max(0) as usize;
        let end = args[1].as_integer()?.max(0) as usize;
        let mut cursor = start;
        while cursor < end {
            let previous = string_property_at(object, cursor, &prop).unwrap_or(Value::Nil);
            let next = font_lock_next_string_property_change(object, cursor, end, &prop);
            let updated = combine_font_lock_property_value(&prop, previous, &value, append);
            modify_shared_string_properties(object, cursor, next, |mut current| {
                current.retain(|(key, _)| key != &prop);
                current.push((prop.clone(), updated.clone()));
                current
            })?;
            cursor = next;
        }
        return Ok(Value::Nil);
    }

    let start = position_from_value(interp, &args[0])?;
    let end = position_from_value(interp, &args[1])?;
    let buffer_id = font_lock_target_buffer_id(interp, args.get(4))?;
    let mut cursor = start;
    while cursor < end {
        let (previous, next) = font_lock_buffer_segment(interp, buffer_id, cursor, end, &prop)?;
        let updated = combine_font_lock_property_value(&prop, previous, &value, append);
        font_lock_put_buffer_property(interp, buffer_id, cursor, next, &prop, updated)?;
        cursor = next;
    }
    font_lock_push_buffer_undo_entry(interp, buffer_id)?;
    Ok(Value::Nil)
}

pub(crate) fn add_face_text_property(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
) -> Result<Value, LispError> {
    need_arg_range(name, args, 3, 5)?;
    let face = args[2].clone();
    let append = args.get(3).is_some_and(|value| value.is_truthy());
    if let Some(object) = args.get(4)
        && string_like(object).is_some()
    {
        let start = args[0].as_integer()?.max(0) as usize;
        let end = args[1].as_integer()?.max(0) as usize;
        let mut cursor = start;
        while cursor < end {
            let previous = string_property_at(object, cursor, "face").unwrap_or(Value::Nil);
            let next = font_lock_next_string_property_change(object, cursor, end, "face");
            let updated = if previous.is_nil() {
                face.clone()
            } else {
                combine_font_lock_property_value("face", previous, &face, append)
            };
            modify_shared_string_properties(object, cursor, next, |mut current| {
                current.retain(|(key, _)| key != "face");
                current.push(("face".into(), updated.clone()));
                current
            })?;
            cursor = next;
        }
        return Ok(Value::Nil);
    }

    let start = position_from_value(interp, &args[0])?;
    let end = position_from_value(interp, &args[1])?;
    let buffer_id = font_lock_target_buffer_id(interp, args.get(4))?;
    let mut cursor = start;
    while cursor < end {
        let (previous, next) = font_lock_buffer_segment(interp, buffer_id, cursor, end, "face")?;
        let updated = if previous.is_nil() {
            face.clone()
        } else {
            combine_font_lock_property_value("face", previous, &face, append)
        };
        font_lock_put_buffer_property(interp, buffer_id, cursor, next, "face", updated)?;
        cursor = next;
    }
    font_lock_push_buffer_undo_entry(interp, buffer_id)?;
    Ok(Value::Nil)
}

pub(crate) fn clear_font_lock_faces_in_current_buffer(
    interp: &mut Interpreter,
    start: usize,
    end: usize,
) -> Result<(), LispError> {
    let start = start.max(interp.buffer.point_min());
    let end = end.min(interp.buffer.point_max());
    if start >= end {
        return Ok(());
    }
    interp.buffer.remove_list_of_text_properties(
        start,
        end,
        &["face".to_string(), "font-lock-face".to_string()],
    );
    font_lock_push_buffer_undo_entry(interp, interp.current_buffer_id())?;
    Ok(())
}

pub(crate) fn font_lock_ensure_region(
    interp: &mut Interpreter,
    start: usize,
    end: usize,
    env: &mut Env,
) -> Result<(), LispError> {
    let start = start.max(interp.buffer.point_min());
    let end = end.min(interp.buffer.point_max());
    if start >= end {
        interp.set_buffer_local_value(interp.current_buffer_id(), "font-lock-fontified", Value::T);
        return Ok(());
    }

    clear_font_lock_faces_in_current_buffer(interp, start, end)?;
    let keywords = interp
        .lookup_var("hi-lock-interactive-patterns", env)
        .unwrap_or(Value::Nil)
        .to_vec()
        .unwrap_or_default();
    for keyword in keywords {
        font_lock_apply_hi_lock_keyword(interp, &keyword, start, end, env)?;
    }
    interp.set_buffer_local_value(interp.current_buffer_id(), "font-lock-fontified", Value::T);
    font_lock_push_buffer_undo_entry(interp, interp.current_buffer_id())?;
    Ok(())
}

pub(crate) fn font_lock_apply_hi_lock_keyword(
    interp: &mut Interpreter,
    keyword: &Value,
    start: usize,
    end: usize,
    env: &mut Env,
) -> Result<(), LispError> {
    let items = keyword.to_vec()?;
    if items.len() < 2 {
        return Ok(());
    }

    let matcher = items[0].clone();
    let action = items[1].to_vec()?;
    if action.len() < 2 {
        return Ok(());
    }

    let subexp = action[0].as_integer()?.max(0) as usize;
    let face = interp.eval(&action[1], env)?;
    let append = !matches!(action.get(2), Some(Value::Symbol(mode)) if mode == "prepend");
    let saved_point = interp.buffer.point();
    interp.buffer.goto_char(start);
    let mut matcher_env = Vec::new();

    while interp.buffer.point() <= end {
        let result = call_function_value(
            interp,
            &matcher,
            &[Value::Integer(end as i64)],
            &mut matcher_env,
        )?;
        if result.is_nil() {
            break;
        }

        let Some((match_start, match_end)) = interp
            .last_match_data
            .as_ref()
            .and_then(|data| data.get(subexp))
            .and_then(|entry| *entry)
        else {
            break;
        };

        if match_start >= match_end {
            let next = (interp.buffer.point().saturating_add(1)).min(end);
            if next <= interp.buffer.point() {
                break;
            }
            interp.buffer.goto_char(next);
            continue;
        }

        let previous = interp
            .buffer
            .text_property_at(match_start, "face")
            .unwrap_or(Value::Nil);
        let updated = combine_font_lock_property_value("face", previous, &face, append);
        font_lock_put_buffer_property(
            interp,
            interp.current_buffer_id(),
            match_start,
            match_end,
            "face",
            updated,
        )?;

        if interp.buffer.point() <= match_start {
            let next = (match_start.saturating_add(1)).min(end);
            if next <= interp.buffer.point() {
                break;
            }
            interp.buffer.goto_char(next);
        }
    }

    interp.buffer.goto_char(saved_point);
    Ok(())
}

pub(crate) fn font_lock_target_buffer_id(
    interp: &Interpreter,
    object: Option<&Value>,
) -> Result<u64, LispError> {
    match object {
        Some(value) if !value.is_nil() => interp.resolve_buffer_id(value),
        _ => Ok(interp.current_buffer_id()),
    }
}

pub(crate) fn font_lock_buffer_segment(
    interp: &Interpreter,
    buffer_id: u64,
    start: usize,
    end: usize,
    prop: &str,
) -> Result<(Value, usize), LispError> {
    let buffer = interp
        .get_buffer_by_id(buffer_id)
        .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
    let previous = buffer.text_property_at(start, prop).unwrap_or(Value::Nil);
    let next = font_lock_next_buffer_property_change(buffer, start, end, prop);
    Ok((previous, next))
}

pub(crate) fn font_lock_put_buffer_property(
    interp: &mut Interpreter,
    buffer_id: u64,
    start: usize,
    end: usize,
    prop: &str,
    value: Value,
) -> Result<(), LispError> {
    if buffer_id == interp.current_buffer_id() {
        if value.is_nil() {
            interp
                .buffer
                .remove_list_of_text_properties(start, end, &[prop.to_string()]);
        } else {
            interp.buffer.put_text_property(start, end, prop, value);
        }
        return Ok(());
    }

    let buffer = interp
        .get_buffer_by_id_mut(buffer_id)
        .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
    if value.is_nil() {
        buffer.remove_list_of_text_properties(start, end, &[prop.to_string()]);
    } else {
        buffer.put_text_property(start, end, prop, value);
    }
    Ok(())
}

pub(crate) fn font_lock_push_buffer_undo_entry(
    interp: &mut Interpreter,
    buffer_id: u64,
) -> Result<(), LispError> {
    let entry = crate::buffer::UndoEntry::Combined {
        display: Value::Nil,
        entries: Vec::new(),
    };
    if buffer_id == interp.current_buffer_id() {
        interp.buffer.push_undo_entry(entry);
        return Ok(());
    }
    let buffer = interp
        .get_buffer_by_id_mut(buffer_id)
        .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
    buffer.push_undo_entry(entry);
    Ok(())
}

pub(crate) fn font_lock_next_buffer_property_change(
    buffer: &crate::buffer::Buffer,
    start: usize,
    end: usize,
    prop: &str,
) -> usize {
    let initial = buffer.text_property_at(start, prop).unwrap_or(Value::Nil);
    for cursor in start.saturating_add(1)..end {
        if buffer.text_property_at(cursor, prop).unwrap_or(Value::Nil) != initial {
            return cursor;
        }
    }
    end
}

pub(crate) fn font_lock_next_string_property_change(
    value: &Value,
    start: usize,
    end: usize,
    prop: &str,
) -> usize {
    let initial = string_property_at(value, start, prop).unwrap_or(Value::Nil);
    for cursor in start.saturating_add(1)..end {
        if string_property_at(value, cursor, prop).unwrap_or(Value::Nil) != initial {
            return cursor;
        }
    }
    end
}

pub(crate) fn combine_font_lock_property_value(
    prop: &str,
    previous: Value,
    value: &Value,
    append: bool,
) -> Value {
    let mut previous_items = font_lock_previous_property_items(prop, previous);
    let mut value_items = font_lock_value_items(value);
    if append {
        previous_items.append(&mut value_items);
        Value::list(previous_items)
    } else {
        value_items.append(&mut previous_items);
        Value::list(value_items)
    }
}

pub(crate) fn font_lock_previous_property_items(prop: &str, previous: Value) -> Vec<Value> {
    if matches!(prop, "face" | "font-lock-face") && anonymous_font_lock_face(&previous) {
        return vec![previous];
    }
    previous.to_vec().unwrap_or_else(|_| vec![previous])
}

pub(crate) fn font_lock_value_items(value: &Value) -> Vec<Value> {
    match value.to_vec() {
        Ok(items) if !matches!(items.first(), Some(Value::Symbol(symbol)) if symbol.starts_with(':')) => {
            items
        }
        _ => vec![value.clone()],
    }
}

pub(crate) fn anonymous_font_lock_face(value: &Value) -> bool {
    let Ok(items) = value.to_vec() else {
        return false;
    };
    matches!(
        items.first(),
        Some(Value::Symbol(symbol))
            if symbol.starts_with(':')
                || matches!(symbol.as_str(), "foreground-color" | "background-color")
    )
}

pub(crate) fn remove_face_value(existing: Value, face: &Value) -> Value {
    match face_list_items(&existing) {
        Ok(items) => {
            let filtered = items
                .into_iter()
                .filter(|item| !values_equal_including_properties(item, face))
                .collect::<Vec<_>>();
            match filtered.as_slice() {
                [] => Value::Nil,
                [single] => single.clone(),
                _ => Value::list(filtered),
            }
        }
        Err(_) => {
            if values_equal_including_properties(&existing, face) {
                Value::Nil
            } else {
                existing
            }
        }
    }
}

pub(crate) fn face_attribute_property_name(attribute: &str) -> String {
    format!("emaxx-face-attribute::{attribute}")
}

pub(crate) fn face_attribute_value(
    interp: &Interpreter,
    face: &str,
    attribute: &str,
    inherit: Option<&Value>,
) -> Value {
    let mut visited = HashSet::new();
    face_attribute_value_inner(interp, face, attribute, inherit, &mut visited)
}

pub(crate) fn face_attribute_value_inner(
    interp: &Interpreter,
    face: &str,
    attribute: &str,
    inherit: Option<&Value>,
    visited: &mut HashSet<String>,
) -> Value {
    if !visited.insert(face.to_string()) {
        return Value::Symbol("unspecified".into());
    }
    if let Some(value) = interp.get_symbol_property(face, &face_attribute_property_name(attribute))
    {
        visited.remove(face);
        return value;
    }
    if inherit.is_some_and(Value::is_truthy) {
        if let Some(parent) = face_inherit_spec(interp, face) {
            let inherited = resolve_face_attribute_inherit(interp, &parent, attribute, visited);
            if !is_unspecified_face_attribute(&inherited) {
                visited.remove(face);
                return inherited;
            }
        }
        if let Some(extra) = inherit.filter(|value| !matches!(value, Value::T)) {
            let inherited = resolve_face_attribute_inherit(interp, extra, attribute, visited);
            if !is_unspecified_face_attribute(&inherited) {
                visited.remove(face);
                return inherited;
            }
        }
    }
    visited.remove(face);
    Value::Symbol("unspecified".into())
}

pub(crate) fn face_inherit_spec(interp: &Interpreter, face: &str) -> Option<Value> {
    interp
        .get_symbol_property(face, &face_attribute_property_name(":inherit"))
        .filter(|value| !value.is_nil())
        .or_else(|| interp.face_inherit_target(face).map(Value::Symbol))
}

pub(crate) fn resolve_face_attribute_inherit(
    interp: &Interpreter,
    inherit: &Value,
    attribute: &str,
    visited: &mut HashSet<String>,
) -> Value {
    match inherit {
        Value::Nil => Value::Symbol("unspecified".into()),
        Value::Symbol(symbol) => {
            face_attribute_value_inner(interp, symbol, attribute, Some(&Value::T), visited)
        }
        other => {
            let Ok(items) = other.to_vec() else {
                return Value::Symbol("unspecified".into());
            };
            for item in items {
                let value = resolve_face_attribute_inherit(interp, &item, attribute, visited);
                if !is_unspecified_face_attribute(&value) {
                    return value;
                }
            }
            Value::Symbol("unspecified".into())
        }
    }
}

pub(crate) fn is_unspecified_face_attribute(value: &Value) -> bool {
    matches!(value, Value::Symbol(symbol) if symbol == "unspecified")
}

pub(crate) fn face_exists(interp: &Interpreter, face: &str) -> bool {
    if face == "default" {
        return true;
    }
    if interp
        .get_symbol_property(face, "face-defface-spec")
        .is_some()
    {
        return true;
    }
    if interp.face_inherit_target(face).is_some() {
        return true;
    }
    interp
        .symbol_plist(face)
        .to_vec()
        .ok()
        .into_iter()
        .flatten()
        .step_by(2)
        .any(|property| {
            property
                .as_symbol()
                .ok()
                .is_some_and(|name| name.starts_with("emaxx-face-attribute::"))
        })
}

pub(crate) fn face_list_items(value: &Value) -> Result<Vec<Value>, LispError> {
    if plist_like_face(value) {
        Err(LispError::TypeError("face-list".into(), "plist".into()))
    } else {
        value.to_vec()
    }
}

pub(crate) fn plist_like_face(value: &Value) -> bool {
    let Ok(items) = value.to_vec() else {
        return false;
    };
    !items.is_empty()
        && items.len().is_multiple_of(2)
        && items
            .iter()
            .step_by(2)
            .all(|item| matches!(item, Value::Symbol(symbol) if symbol.starts_with(':')))
}
