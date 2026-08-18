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
    let hooks = hook_values(interp, hook_name, env, Some(interp.current_buffer_id()));
    let combining = interp
        .lookup_var("combine-after-change-calls", env)
        .is_some_and(|value| value.is_truthy());
    if hook_name == "before-change-functions" {
        if hooks.is_empty() && combining {
            return Ok(());
        }
        flush_combined_after_change(interp, env)?;
    } else if hook_name == "after-change-functions" {
        let can_defer = combining && interp.buffer.overlays.is_empty() && {
            hook_values(
                interp,
                "before-change-functions",
                env,
                Some(interp.current_buffer_id()),
            )
            .is_empty()
        };
        if can_defer {
            let buffer_id = interp.current_buffer_id();
            if interp
                .combined_after_change()
                .is_some_and(|pending| pending.buffer_id != buffer_id)
            {
                flush_combined_after_change(interp, env)?;
            }
            let [begin, end, old_length] = args else {
                return Err(LispError::Signal(
                    "Invalid after-change hook arguments".into(),
                ));
            };
            let begin = begin.as_integer()?;
            let end = end.as_integer()?;
            let old_length = old_length.as_integer()?;
            let new_length = end - begin;
            let unchanged_before = begin - interp.buffer.point_min() as i64;
            let unchanged_after =
                interp.buffer.point_max() as i64 - (begin - old_length + new_length);
            interp.record_combined_after_change(
                buffer_id,
                (unchanged_before, unchanged_after, new_length - old_length),
            );
            return Ok(());
        }
        flush_combined_after_change(interp, env)?;
    }
    if hooks.is_empty() {
        return Ok(());
    }
    // insdel.c dynamically inhibits recursive modification hooks while the
    // callbacks run.  Its unwind record also clears the active before/after
    // hook variable on every nonlocal exit, preventing a broken global or
    // local hook from poisoning subsequent edits.
    let inhibit_restore =
        interp.bind_special_dynamic("inhibit-modification-hooks", Value::T, env)?;
    interp.enter_change_hooks();
    let mut result = Ok(());
    for hook in hooks {
        if let Err(error) = call_function_value(interp, &hook, args, env) {
            result = Err(error);
            break;
        }
    }
    interp.leave_change_hooks();
    if result.is_err()
        && matches!(
            hook_name,
            "before-change-functions" | "after-change-functions"
        )
    {
        let local_buffer_id = interp.assignment_buffer_id(hook_name);
        interp.set_variable(hook_name, Value::Nil, env);
        if let Some(buffer_id) = local_buffer_id {
            interp.remove_buffer_local_hook(buffer_id, hook_name);
        }
    }
    let restore_result = interp.restore_special_dynamic(inhibit_restore, env);
    match (result, restore_result) {
        (Err(error), _) | (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

pub(crate) fn flush_combined_after_change(
    interp: &mut Interpreter,
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let Some(pending) = interp.take_combined_after_change() else {
        return Ok(Value::Nil);
    };
    if !interp.has_buffer_id(pending.buffer_id) {
        return Ok(Value::Nil);
    }
    let saved_buffer_id = interp.current_buffer_id();
    interp.switch_to_buffer_id(pending.buffer_id)?;
    let mut unchanged_before = interp.buffer.buffer_size() as i64;
    let mut unchanged_after = unchanged_before;
    let mut net_change = 0_i64;
    for (begin, end, change) in pending.changes {
        unchanged_before = unchanged_before.min(begin);
        unchanged_after = unchanged_after.min(end);
        net_change += change;
    }
    let begin = interp.buffer.point_min() as i64 + unchanged_before;
    let end = interp.buffer.point_max() as i64 - unchanged_after;
    let new_length = end - begin;
    let old_length = new_length - net_change;
    let restore = interp.bind_special_dynamic("combine-after-change-calls", Value::Nil, env)?;
    let result = run_change_hooks(
        interp,
        "after-change-functions",
        &[
            Value::Integer(begin),
            Value::Integer(end),
            Value::Integer(old_length),
        ],
        env,
    );
    interp.restore_special_dynamic(restore, env)?;
    if interp.has_buffer_id(saved_buffer_id) {
        interp.switch_to_buffer_id(saved_buffer_id)?;
    }
    result.map(|()| Value::Nil)
}

pub(crate) fn hook_values(
    interp: &Interpreter,
    hook_name: &str,
    env: &crate::lisp::types::Env,
    buffer_id: Option<u64>,
) -> Vec<Value> {
    let value_hooks = |value: Option<Value>| {
        value
            .map(|value| {
                if value.is_nil() {
                    Vec::new()
                } else {
                    value.to_vec().unwrap_or_else(|_| vec![value])
                }
            })
            .unwrap_or_default()
    };
    let current = interp.lookup_var(hook_name, env);
    let local = buffer_id.is_some_and(|id| {
        interp.buffer_local_value(id, hook_name).is_some()
            || interp.buffer_local_hook(id, hook_name).is_some()
    });
    let mut hooks = value_hooks(current);
    if !local {
        hooks.retain(|hook| !matches!(hook, Value::T));
        return hooks;
    }

    // The Lisp-visible local/default value cells are authoritative.  GNU's
    // Elisp `add-hook' owner can create or mutate a local hook without going
    // through the native bootstrap helper, so the Rust depth mirror may be
    // absent.  A local `t' sentinel splices the default at that exact point;
    // local nil deliberately suppresses the default.
    let mut default = value_hooks(interp.default_value(hook_name));
    default.retain(|hook| !matches!(hook, Value::T));
    let mut result = Vec::new();
    for hook in hooks {
        if matches!(hook, Value::T) {
            result.extend(default.iter().cloned());
        } else {
            result.push(hook);
        }
    }
    result
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
            Err(error @ (LispError::Throw(_, _) | LispError::Terminate(_))) => return Err(error),
            Err(error) => {
                // GNU keyboard.c:safe_run_hooks_error — message with prin1
                // renderings, then remove the failing function from the
                // hook's local value (or, failing that, its default) so a
                // broken hook cannot wedge every subsequent command.
                let rendered_fun = crate::lisp::primitives::print::render_prin1(interp, &hook, env)
                    .unwrap_or_else(|_| "?".to_string());
                let condition = crate::lisp::eval::error_condition_value(&error);
                let rendered_error =
                    crate::lisp::primitives::print::render_prin1(interp, &condition, env)
                        .unwrap_or_else(|_| condition.to_string());
                let message = format!("Error in {hook_name} ({rendered_fun}): {rendered_error}");
                let _ = crate::lisp::primitives::call(
                    interp,
                    "message",
                    &[Value::String(message.into())],
                    env,
                );
                remove_hook_function_after_error(interp, hook_name, &hook, env);
            }
        }
    }
    Ok(())
}

/// GNU keyboard.c:safe_run_hooks_error's recovery step: delete FUN from
/// HOOK's Lisp-visible value.  The local (current) value is preferred; if
/// FUN only appears in the default value, edit that instead.
fn remove_hook_function_after_error(
    interp: &mut Interpreter,
    hook_name: &str,
    fun: &Value,
    env: &mut crate::lisp::types::Env,
) {
    let filter = |interp: &Interpreter, value: Option<Value>| -> Option<Vec<Value>> {
        let items = value?.to_vec().ok()?;
        let mut found = false;
        let mut kept = Vec::with_capacity(items.len());
        for item in items {
            if crate::lisp::primitives::values::values_eq_in_env(interp, &item, fun, env) {
                found = true;
            } else {
                kept.push(item);
            }
        }
        found.then_some(kept)
    };
    if let Some(kept) = filter(interp, interp.lookup_var(hook_name, env)) {
        interp.set_variable(hook_name, Value::list(kept), env);
        return;
    }
    if let Some(kept) = filter(interp, interp.default_value(hook_name)) {
        interp.set_default_toplevel_value(hook_name, Value::list(kept));
    }
}

pub(crate) fn call_named_function(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    match interp.lookup_function(name, env) {
        Ok(function) => interp.call_function_value(function, Some(name), args, env),
        Err(_) => Ok(Value::T),
    }
}

pub(crate) fn dispatch_file_notification(
    interp: &mut Interpreter,
    _env: &mut Env,
    path: &str,
    action: &str,
) -> Result<(), LispError> {
    // File notifications are asynchronous in GNU Emacs: the file operation
    // returns first, and the notification is processed later when the
    // command loop goes idle.  Queue the event for the next idle pump.
    interp.queue_file_notification(path, action);
    Ok(())
}

pub(crate) fn deliver_file_notification(
    interp: &mut Interpreter,
    env: &mut Env,
    path: &str,
    action: &str,
    callbacks: Vec<(i64, Value)>,
) -> Result<(), LispError> {
    let saved_buffer_id = interp.current_buffer_id();
    let backend_action = match action {
        "created" => "create",
        "deleted" => "delete",
        "changed" => "write",
        "attribute-changed" => "attrib",
        "renamed" => "rename",
        other => other,
    };
    let mut result = Ok(());
    for (descriptor, callback) in callbacks {
        let event = Value::list([
            Value::Integer(descriptor),
            Value::list([Value::Symbol(backend_action.into())]),
            Value::String(path.into()),
        ]);
        if let Err(error) = call_function_value(interp, &callback, &[event], env) {
            result = Err(error);
            break;
        }
    }
    if interp.has_buffer_id(saved_buffer_id) {
        interp.switch_to_buffer_id(saved_buffer_id)?;
    }
    result
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
    match overlay.get_prop(&Value::Symbol(property.into())) {
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
    let restore = interp.bind_special_dynamic("inhibit-modification-hooks", Value::T, env)?;
    let mut outcome = Ok(());
    'outer: for call in calls {
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
            if let Err(error) = call_function_value(interp, function, &args, env) {
                outcome = Err(error);
                break 'outer;
            }
        }
    }
    interp.restore_special_dynamic(restore, env)?;
    outcome
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
    let range_length = to - from;
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
    let preserved_bounds = if has_before_hooks {
        let Value::Marker(preserve_id) = interp.make_marker() else {
            unreachable!("make_marker returns a marker")
        };
        let Value::Marker(start_id) = interp.make_marker() else {
            unreachable!("make_marker returns a marker")
        };
        let Value::Marker(end_id) = interp.make_marker() else {
            unreachable!("make_marker returns a marker")
        };
        let buffer_id = interp.current_buffer_id();
        let _ = interp.set_marker(preserve_id, Some(from), Some(buffer_id));
        let _ = interp.set_marker(start_id, Some(from), Some(buffer_id));
        let _ = interp.set_marker(end_id, Some(to), Some(buffer_id));
        Some((preserve_id, start_id, end_id))
    } else {
        None
    };
    let hook_result = run_change_hooks(
        interp,
        "before-change-functions",
        &[Value::Integer(from as i64), Value::Integer(to as i64)],
        env,
    );
    let (from, to) = if let Some((preserve_id, start_id, end_id)) = preserved_bounds {
        // GNU's del_range_1 preserves the start through Lisp callbacks and
        // then reapplies the original range length.  The start/end markers
        // belong to signal_before_change itself and must remain live while a
        // nested edit records its undo marker riders.
        let start = interp.marker_position(preserve_id).unwrap_or(from);
        let end = (start + range_length).min(interp.buffer.point_max());
        let _ = interp.set_marker(preserve_id, None, None);
        let _ = interp.set_marker(start_id, None, None);
        let _ = interp.set_marker(end_id, None, None);
        (start, end)
    } else {
        (from, to)
    };
    hook_result?;
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
        Value::buffer(interp.current_buffer_id(), interp.buffer.name.clone()),
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
    Ok(Value::Nil)
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

pub(crate) fn face_attribute_index(attribute: &str) -> Option<usize> {
    Some(match attribute {
        ":family" => 1,
        ":foundry" => 2,
        ":width" => 3,
        ":height" => 4,
        ":weight" | ":bold" => 5,
        ":slant" | ":italic" => 6,
        ":underline" => 7,
        ":inverse-video" | ":reverse-video" => 8,
        ":foreground" => 9,
        ":background" => 10,
        ":stipple" => 11,
        ":overline" => 12,
        ":strike-through" => 13,
        ":box" => 14,
        ":font" => 15,
        ":inherit" => 16,
        ":fontset" => 17,
        ":distant-foreground" => 18,
        ":extend" => 19,
        _ => return None,
    })
}
