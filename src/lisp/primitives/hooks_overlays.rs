use super::*;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;
use crate::lisp::types::PropertyName;

/// Ffuncall of FUNCTION, unnamed: a forwarder, inline at every caller.
#[inline(always)]
pub(crate) fn call_function_value(
    interp: &mut Interpreter,
    function: &Value,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    interp.call_function_value(*function, None, args, env)
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
    // The hook named once, for the tests below and the hook's symbol: a
    // caller's literal is the same constant as these, tested before the
    // text.
    const BEFORE: &str = "before-change-functions";
    const AFTER: &str = "after-change-functions";
    let before = std::ptr::eq(hook_name, BEFORE) || hook_name == BEFORE;
    let after = !before && (std::ptr::eq(hook_name, AFTER) || hook_name == AFTER);
    // insdel.c's run_undoable_change: every prepare-to-modify of a buffer
    // with undo enabled calls the Lisp `undo-auto--undoable-change' (which
    // registers the buffer in undo-auto--undoably-changed-buffers and arms
    // the auto-boundary timer), independent of any change hooks being
    // registered.  The function itself never modifies buffer text, so the
    // reentrancy guard above cannot be tripped by it.
    if before && !interp.buffer.borrow().undo_recording_disabled() {
        let symbol = cached_symbol!("undo-auto--undoable-change").with(Clone::clone);
        if interp.has_lisp_function_symbol(&symbol) {
            // The callee names its own frame (a name given here was
            // interned again per call).
            let _ = interp.call_function_value(Value::Symbol(symbol), None, &[], env);
        }
    }
    let symbol = if before {
        Some(cached_symbol!("before-change-functions").with(Clone::clone))
    } else if after {
        Some(cached_symbol!("after-change-functions").with(Clone::clone))
    } else {
        crate::lisp::types::SymbolName::interned_cached(hook_name)
    };
    let hooks = hook_values_of(
        interp,
        symbol.as_ref(),
        hook_name,
        env,
        Some(interp.current_buffer_id()),
    );
    let combining = interp
        .symbol_value_key(cached_symbol!("combine-after-change-calls"))
        .is_some_and(|value| value.is_truthy());
    if before {
        if hooks.is_empty() && combining {
            return Ok(());
        }
        flush_combined_after_change(interp, env)?;
    } else if after {
        let can_defer = combining && interp.buffer.borrow().overlays.is_empty() && {
            hook_values_of(
                interp,
                Some(&cached_symbol!("before-change-functions").with(Clone::clone)),
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
            let unchanged_before = begin - interp.buffer.borrow().point_min() as i64;
            let unchanged_after =
                interp.buffer.borrow().point_max() as i64 - (begin - old_length + new_length);
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
    if result.is_err() && (before || after) {
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
    let mut unchanged_before = interp.buffer.borrow().buffer_size() as i64;
    let mut unchanged_after = unchanged_before;
    let mut net_change = 0_i64;
    for (begin, end, change) in pending.changes {
        unchanged_before = unchanged_before.min(begin);
        unchanged_after = unchanged_after.min(end);
        net_change += change;
    }
    let begin = interp.buffer.borrow().point_min() as i64 + unchanged_before;
    let end = interp.buffer.borrow().point_max() as i64 - unchanged_after;
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
    // The hook's symbol once (no name hash per hook run): the value and
    // the buffer's binding are read by it when the name is interned.  The
    // two change hooks, run per edit, are held interned.
    let symbol = match hook_name {
        "before-change-functions" => {
            Some(cached_symbol!("before-change-functions").with(Clone::clone))
        }
        "after-change-functions" => {
            Some(cached_symbol!("after-change-functions").with(Clone::clone))
        }
        _ => crate::lisp::types::SymbolName::interned_cached(hook_name),
    };
    hook_values_of(interp, symbol.as_ref(), hook_name, env, buffer_id)
}

/// `hook_values' of the hook HOOK_NAME, whose symbol is SYMBOL when the
/// name is interned.
fn hook_values_of(
    interp: &Interpreter,
    symbol: Option<&crate::lisp::types::SymbolName>,
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
    // run_hook_with_args reads the hook with find_symbol_value.
    let current = match symbol {
        Some(symbol) => interp.symbol_value_symbol(symbol),
        None => interp.lookup_var(hook_name, env),
    };
    // A void or nil hook runs nothing, whatever the buffer's own binding
    // (the functions run are the current value's, a local `t' splicing
    // the default in): signal_before_change's NILP test.
    if current.is_none_or(|value| value.is_nil()) {
        return Vec::new();
    }
    let local = buffer_id.is_some_and(|id| {
        let has_local_value = match &symbol {
            Some(symbol) => interp
                .buffer_local_binding_symbol(id, symbol)
                .is_some_and(|binding| binding.is_some()),
            None => interp.buffer_local_value(id, hook_name).is_some(),
        };
        has_local_value || interp.has_buffer_local_hook(id, hook_name)
    });
    let mut hooks = value_hooks(current);
    if !local {
        hooks.retain(|hook| !matches!(hook.kind(), Kind::T));
        return hooks;
    }

    // The Lisp-visible local/default value cells are authoritative.  GNU's
    // Elisp `add-hook' owner can create or mutate a local hook without going
    // through the native bootstrap helper, so the Rust depth mirror may be
    // absent.  A local `t' sentinel splices the default at that exact point;
    // local nil deliberately suppresses the default.
    let mut default = value_hooks(match &symbol {
        Some(symbol) => interp.default_value_symbol(symbol).ok().flatten(),
        None => interp.default_value(hook_name),
    });
    default.retain(|hook| !matches!(hook.kind(), Kind::T));
    let mut result = Vec::new();
    for hook in hooks {
        if matches!(hook.kind(), Kind::T) {
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
        match call_function_value(interp, &hook, &[], env).map_err(LispError::into_kind) {
            Ok(_) => {}
            Err(error @ (LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_))) => {
                return Err(LispError::from(error));
            }
            Err(error) => {
                // GNU keyboard.c:safe_run_hooks_error — message with prin1
                // renderings, then remove the failing function from the
                // hook's local value (or, failing that, its default) so a
                // broken hook cannot wedge every subsequent command.
                // GNU keyboard.c:1896 does
                // CALLN (Fmessage, "Error in %s (%S): %S", hook, fun, error),
                // so the data is formatted BY `message' and a `%' inside a
                // printed object cannot be reinterpreted as a directive.
                let condition =
                    crate::lisp::eval::error_condition_value(&LispError::from(error.clone()));
                let _ = crate::lisp::primitives::call(
                    interp,
                    "message",
                    &[
                        Value::String("Error in %s (%S): %S".into()),
                        Value::symbol(hook_name),
                        hook,
                        condition,
                    ],
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
    // GNU's call1/call2 do not tolerate a missing function: an unbound name
    // signals void-function.  The old `Err(_) => Ok(t)' arm here fabricated
    // success -- most dangerously for `write-region's MUSTBENEW prompt,
    // where "the asker is missing" became "the user said yes" and Emaxx
    // overwrote files GNU refuses to touch (finding 64).
    let function = interp.lookup_function(name, env)?;
    interp.call_function_value(function, Some(name), args, env)
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
    secondary_path: Option<&str>,
    (descriptor, callback): (Value, Value),
) -> Result<(), LispError> {
    let saved_buffer_id = interp.current_buffer_id();
    let backend_action = match action {
        "created" => "create",
        "deleted" => "delete",
        "changed" => "write",
        "attribute-changed" => "attrib",
        "renamed" => "rename",
        "revoked" => "revoke",
        other => other,
    };
    let mut event = vec![
        descriptor,
        Value::list([Value::Symbol(backend_action.into())]),
        Value::String(path.into()),
    ];
    if let Some(secondary_path) = secondary_path {
        event.push(Value::String(secondary_path.into()));
    }
    let event = Value::list(event);
    let result = call_function_value(interp, &callback, &[event], env).map(|_| ());
    if interp.has_buffer_id(saved_buffer_id) {
        interp.switch_to_buffer_id(saved_buffer_id)?;
    }
    result
}

pub(crate) fn deliver_raw_file_notification(
    interp: &mut Interpreter,
    env: &mut Env,
    event: Value,
    callback: Value,
) -> Result<(), LispError> {
    let saved_buffer_id = interp.current_buffer_id();
    let result =
        call_function_value(interp, &callback, std::slice::from_ref(&event), env).map(|_| ());
    if interp.has_buffer_id(saved_buffer_id) {
        interp.switch_to_buffer_id(saved_buffer_id)?;
    }
    result
}

/// fileio.c report_file_notify_error: the condition data is MESSAGE, then
/// the rendered errno text, then NAME -- spliced in as the tail when it is
/// already a list (or nil) and wrapped in a one-element list otherwise, so a
/// dotted descriptor such as (a . 1) yields the same improper data GNU does.
#[cfg(target_os = "linux")]
pub(crate) fn file_notify_error_with_errno(
    message: &str,
    error: &std::io::Error,
    name: Value,
) -> LispError {
    let rendered = error.to_string();
    let detail = rendered
        .split_once(" (os error")
        .map_or(rendered.as_str(), |(detail, _)| detail);
    let tail = if name.is_nil() || name.cons_values().is_some() {
        name
    } else {
        Value::list([name])
    };
    LispError::SignalValue(Value::cons(
        Value::Symbol("file-notify-error".into()),
        Value::cons(
            Value::String(message.into()),
            Value::cons(Value::String(detail.into()), tail),
        ),
    ))
}

struct OverlayHookCall {
    overlay: crate::overlay::OverlayRef,
    functions: Value,
}

pub(crate) struct OverlayHookCalls {
    calls: Vec<OverlayHookCall>,
    before: [Value; 2],
    after: [Value; 3],
}

impl crate::lisp::eval::roots::TraceLispRoots for OverlayHookCalls {
    fn trace_lisp_roots(&self, marker: &mut crate::lisp::eval::roots::LispRootMarker<'_>) {
        for call in &self.calls {
            marker.value(&Value::Overlay(call.overlay));
            marker.value(&call.functions);
        }
    }
}

pub(crate) fn with_overlay_hook_roots<R>(
    interp: &mut Interpreter,
    calls: &OverlayHookCalls,
    body: impl FnOnce(&mut Interpreter) -> R,
) -> R {
    if calls.calls.is_empty() {
        body(interp)
    } else {
        interp.with_lisp_stack_roots(calls, body)
    }
}

pub(crate) fn overlay_insert_hook_calls(
    interp: &Interpreter,
    buffer: &crate::buffer::Buffer,
    pos: usize,
    inserted_len: usize,
) -> OverlayHookCalls {
    overlay_change_hook_calls(interp, buffer, pos, pos, pos + inserted_len)
}

pub(crate) fn overlay_change_hook_calls(
    interp: &Interpreter,
    buffer: &crate::buffer::Buffer,
    from: usize,
    to: usize,
    new_end: usize,
) -> OverlayHookCalls {
    let mut calls = Vec::new();
    let insertion = from == to;
    let mut add = |overlay, property| {
        if let Some(functions) =
            overlay_property_with_category(interp, &overlay, property).filter(Value::is_truthy)
        {
            calls.push(OverlayHookCall { overlay, functions });
        }
    };
    // buffer.c:report_overlay_modification searches only intersecting
    // intervals and stores the actual function lists, without copying or
    // filtering their elements. Lisp may mutate those lists while they run.
    let before = [Value::Integer(from as i64), Value::Integer(to as i64)];
    let after = [
        Value::Integer(from as i64),
        Value::Integer(new_end as i64),
        Value::Integer(to.saturating_sub(from) as i64),
    ];
    // A buffer with no overlays has none to search.
    if buffer.overlays.is_empty() {
        return OverlayHookCalls {
            calls,
            before,
            after,
        };
    }
    for overlay in buffer.overlays.intersecting(
        from as isize - isize::from(insertion),
        to as isize + isize::from(insertion),
        crate::overlay::Traversal::Ascending,
    ) {
        let (beg, end) = overlay.bounds();
        if insertion && from as isize == beg {
            add(overlay, "insert-in-front-hooks");
        }
        if insertion && from as isize == end {
            add(overlay, "insert-behind-hooks");
        }
        if (to as isize) > beg && (from as isize) < end {
            add(overlay, "modification-hooks");
        }
    }
    OverlayHookCalls {
        calls,
        before,
        after,
    }
}

pub(crate) fn run_overlay_hook_calls(
    interp: &mut Interpreter,
    calls: &OverlayHookCalls,
    after: bool,
    env: &mut crate::lisp::types::Env,
) -> Result<(), LispError> {
    if calls.calls.is_empty() {
        return Ok(());
    }
    let restore = interp.bind_special_dynamic("inhibit-modification-hooks", Value::T, env)?;
    let mut outcome = Ok(());
    'outer: for call in &calls.calls {
        // GNU skips a recorded hook when an earlier callback deleted or
        // moved its overlay, both before and after the edit.
        if !call
            .overlay
            .buffer()
            .is_some_and(|owner| owner.ptr_eq(&interp.buffer))
        {
            continue;
        }
        let mut args = [
            Value::Overlay(call.overlay),
            if after { Value::T } else { Value::Nil },
            Value::Nil,
            Value::Nil,
            Value::Nil,
        ];
        let len = if after {
            args[2..].copy_from_slice(&calls.after);
            5
        } else {
            args[2..4].copy_from_slice(&calls.before);
            4
        };
        let mut list = call.functions;
        while let Kind::Cons(cell) = list.kind() {
            let function = cell.car.get();
            let result = interp.with_lisp_stack_roots(&list, |interp| {
                call_function_value(interp, &function, &args[..len], env)
            });
            if let Err(error) = result {
                outcome = Err(error);
                break 'outer;
            }
            list = cell.cdr.get();
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
    let overlay_calls = overlay_change_hook_calls(interp, &interp.buffer.borrow(), from, to, from);
    with_overlay_hook_roots(interp, &overlay_calls, |interp| {
        run_overlay_hook_calls(interp, &overlay_calls, false, env)?;
        let has_before_hooks = interp
            .symbol_value_key(cached_symbol!("before-change-functions"))
            .map(|value| match value.kind() {
                // A proper list is nonempty; walking it copies nothing.
                Kind::Cons(_) => value.visit_list_elements(|_| Ok(())).is_ok(),
                _ => !value.to_vec().unwrap_or_default().is_empty(),
            })
            .or_else(|| {
                interp
                    .buffer_local_hook(interp.current_buffer_id(), "before-change-functions")
                    .map(|hooks| !hooks.is_empty())
            })
            .unwrap_or(false);
        let preserved_bounds = if has_before_hooks {
            let Kind::Marker(preserve_id) = interp.make_marker().kind() else {
                unreachable!("make_marker returns a marker")
            };
            let Kind::Marker(start_id) = interp.make_marker().kind() else {
                unreachable!("make_marker returns a marker")
            };
            let Kind::Marker(end_id) = interp.make_marker().kind() else {
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
        // The preserved bounds are markers held here, in no Lisp object:
        // register them as roots while the hooks run, or a collection inside
        // a hook unchains them.
        let held = preserved_bounds
            .map(|(preserve_id, start_id, end_id)| {
                vec![
                    Value::Marker(preserve_id),
                    Value::Marker(start_id),
                    Value::Marker(end_id),
                ]
            })
            .unwrap_or_default();
        let hook_result = interp.with_lisp_stack_roots(&held, |interp| {
            run_change_hooks(
                interp,
                "before-change-functions",
                &[Value::Integer(from as i64), Value::Integer(to as i64)],
                env,
            )
        });
        let (from, to) = if let Some((preserve_id, start_id, end_id)) = preserved_bounds {
            // GNU's del_range_1 preserves the start through Lisp callbacks and
            // then reapplies the original range length.  The start/end markers
            // belong to signal_before_change itself and must remain live while a
            // nested edit records its undo marker riders.
            let start = interp.marker_position(preserve_id).unwrap_or(from);
            let end = (start + range_length).min(interp.buffer.borrow().point_max());
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
    })
}

/// The buffer a text-property primitive's OBJECT names: the current
/// buffer for nil (textprop.c:validate_interval_range).
pub(crate) fn property_buffer_id(
    interp: &Interpreter,
    object: Option<&Value>,
) -> Result<u64, LispError> {
    match object {
        Some(object) if !object.is_nil() => interp.resolve_buffer_id(object),
        _ => Ok(interp.current_buffer_id()),
    }
}

/// textprop.c's buffer case of a property change, when CHANGES finds that
/// the properties of [START, END) in BUFFER_ID would change:
/// modify_text_properties (insdel.c:prepare_to_modify_buffer_1 over the
/// range with that buffer current: the read-only checks, the overlays'
/// modification hooks, the undoable-change notice and
/// `before-change-functions'), APPLY over the buffer and its indirect
/// relatives, then signal_after_change.  Whether anything changed.
pub(crate) fn modify_buffer_text_properties(
    interp: &mut Interpreter,
    buffer_id: u64,
    start: usize,
    end: usize,
    changes: &dyn Fn(&crate::buffer::Buffer) -> bool,
    apply: &dyn Fn(&mut crate::buffer::Buffer),
    env: &mut crate::lisp::types::Env,
) -> Result<bool, LispError> {
    let (start, end) = if start <= end {
        (start, end)
    } else {
        (end, start)
    };
    let original = interp.current_buffer_id();
    let changing = if buffer_id == original {
        changes(&interp.buffer.borrow())
    } else {
        interp
            .get_buffer_by_id(buffer_id)
            .is_some_and(|buffer| changes(&buffer))
    };
    if !changing {
        return Ok(false);
    }
    // modify_text_properties makes the buffer current for
    // prepare_to_modify_buffer_1 alone, and its signal leaves it so.
    if buffer_id != original {
        interp.set_current_buffer_id(buffer_id)?;
    }
    ensure_region_modifiable(interp, start, end)?;
    let overlay_calls = overlay_change_hook_calls(interp, &interp.buffer.borrow(), start, end, end);
    with_overlay_hook_roots(interp, &overlay_calls, |interp| {
        run_overlay_hook_calls(interp, &overlay_calls, false, env)?;
        run_change_hooks(
            interp,
            "before-change-functions",
            &[Value::Integer(start as i64), Value::Integer(end as i64)],
            env,
        )?;
        let _ = maybe_lock_current_buffer_on_change(interp, env);
        if buffer_id != original && interp.has_buffer_id(original) {
            interp.set_current_buffer_id(original)?;
        }
        interp.apply_text_property_change_shared_for(buffer_id, apply);
        run_change_hooks(
            interp,
            "after-change-functions",
            &[
                Value::Integer(start as i64),
                Value::Integer(end as i64),
                Value::Integer((end - start) as i64),
            ],
            env,
        )?;
        run_overlay_hook_calls(interp, &overlay_calls, true, env)
    })?;
    Ok(true)
}

/// Whether some interval of [FROM, TO) lacks one of PROPS with its value
/// (EQ): textprop.c:interval_has_all_properties false somewhere, so that
/// add_text_properties changes the text.
pub(crate) fn properties_would_add(
    buffer: &crate::buffer::Buffer,
    from: usize,
    to: usize,
    props: &[(PropertyName, Value)],
) -> bool {
    let (from, to) = clamp_property_range(buffer, from, to);
    buffer
        .try_for_each_property_interval(from, to, |_, _, current| {
            let has_all = props.iter().all(|(name, value)| {
                current
                    .iter()
                    .any(|(key, existing)| key == name && existing.word() == value.word())
            });
            if has_all { Ok(()) } else { Err(()) }
        })
        .is_err()
}

/// Whether some interval of [FROM, TO) has one of NAMES:
/// textprop.c:interval_has_some_properties(_list), so that removing them
/// changes the text.
pub(crate) fn properties_would_remove(
    buffer: &crate::buffer::Buffer,
    from: usize,
    to: usize,
    names: &[PropertyName],
) -> bool {
    let (from, to) = clamp_property_range(buffer, from, to);
    buffer
        .try_for_each_property_interval(from, to, |_, _, current| {
            if current.iter().any(|(key, _)| names.contains(key)) {
                Err(())
            } else {
                Ok(())
            }
        })
        .is_err()
}

/// Whether set_text_properties modifies the buffer: a nonempty range, and
/// either properties to set or intervals in the buffer, though every
/// plist be empty.
pub(crate) fn properties_would_set(
    buffer: &crate::buffer::Buffer,
    from: usize,
    to: usize,
    props: &[(PropertyName, Value)],
) -> bool {
    let (from, to) = clamp_property_range(buffer, from, to);
    from < to && (!props.is_empty() || buffer.has_interval_tree())
}

fn clamp_property_range(buffer: &crate::buffer::Buffer, from: usize, to: usize) -> (usize, usize) {
    let (from, to) = if from <= to { (from, to) } else { (to, from) };
    (from.max(buffer.point_min()), to.min(buffer.point_max()))
}

/// insdel.c:prepare_to_modify_buffer_1's read-only checks before a change
/// of [FROM, TO): buffer.c:Fbarf_if_buffer_read_only at FROM, then
/// textprop.c:verify_interval_modification over the intervals.
pub(crate) fn ensure_region_modifiable(
    interp: &Interpreter,
    from: usize,
    to: usize,
) -> Result<(), LispError> {
    let (from, to) = if from <= to { (from, to) } else { (to, from) };
    let from = from.max(interp.buffer.borrow().point_min());
    let to = to.min(interp.buffer.borrow().point_max());
    if from >= to {
        return Ok(());
    }
    let inhibit = inhibit_read_only_value(interp);
    let buffer_read_only = buffer_read_only_active(interp, &inhibit);
    let buffer = interp.buffer.borrow();
    if buffer_read_only
        && textget(
            interp,
            buffer.text_properties_at_ref(from),
            cached_symbol!("inhibit-read-only"),
        )
        .is_nil()
    {
        return Err(buffer_read_only_signal(interp));
    }
    // insdel.c: with no intervals there is nothing more to verify.
    if !buffer.has_text_properties() {
        return Ok(());
    }
    buffer.try_for_each_property_interval(from, to, |_, _, props| {
        let read_only = textget(interp, props, cached_symbol!("read-only"));
        // intervals.h INTERVAL_GENERALLY_WRITABLE_P and
        // INTERVAL_EXPRESSLY_WRITABLE_P.
        let implied = read_only.is_nil() || (!inhibit.is_nil() && !is_cons(&inhibit));
        let express = !textget(interp, props, cached_symbol!("inhibit-read-only")).is_nil()
            || (!read_only.is_nil()
                && is_cons(&inhibit)
                && memq(PropertyName::new(read_only), inhibit));
        if !implied && !express {
            return Err(text_read_only(read_only));
        }
        if !express && buffer_read_only {
            return Err(buffer_read_only_signal(interp));
        }
        Ok(())
    })
}

/// insdel.c:prepare_to_modify_buffer_1's read-only checks before an
/// insertion at point: buffer.c:Fbarf_if_buffer_read_only there, then
/// textprop.c:verify_interval_modification's insertion case, which
/// refuses read-only text on either side whose stickiness would extend
/// it over the inserted text.
pub(crate) fn ensure_insert_modifiable(interp: &Interpreter) -> Result<(), LispError> {
    let inhibit = inhibit_read_only_value(interp);
    let buffer = interp.buffer.borrow();
    let point = buffer.point();
    if buffer_read_only_active(interp, &inhibit)
        && textget(
            interp,
            buffer.text_properties_at_ref(point),
            cached_symbol!("inhibit-read-only"),
        )
        .is_nil()
    {
        return Err(buffer_read_only_signal(interp));
    }
    // A non-list `inhibit-read-only' lets any text be inserted into.
    if !buffer.has_text_properties() || (!inhibit.is_nil() && !is_cons(&inhibit)) {
        return Ok(());
    }
    // The intervals of the characters after and before point: none at
    // the accessible region's edges, and the same one inside a span.
    let after = (point < buffer.point_max()).then(|| buffer.text_properties_at_ref(point));
    let before = (point > buffer.point_min()).then(|| buffer.text_properties_at_ref(point - 1));
    let read_only_unless_inhibited = |props: &[(PropertyName, Value)]| {
        let value = textget(interp, props, cached_symbol!("read-only"));
        (!value.is_nil() && !memq(PropertyName::new(value), inhibit)).then_some(value)
    };
    // The read-only property sticks to text inserted after it unless it
    // is rear-nonsticky (directly, or through `category' when the
    // interval has no read-only property of its own), and before it when
    // it is front-sticky.
    let sticks_behind = |props: &[(PropertyName, Value)]| {
        let tem = textget(interp, props, cached_symbol!("rear-nonsticky"));
        !tmem(cached_symbol!("read-only"), tem)
            && (!plist_get(props, cached_symbol!("read-only")).is_nil()
                || !tmem(cached_symbol!("category"), tem))
    };
    let sticks_ahead = |props: &[(PropertyName, Value)]| {
        let tem = textget(interp, props, cached_symbol!("front-sticky"));
        tmem(cached_symbol!("read-only"), tem)
            || (plist_get(props, cached_symbol!("read-only")).is_nil()
                && tmem(cached_symbol!("category"), tem))
    };
    match (after, before) {
        (Some(after), Some(before)) if std::ptr::eq(after, before) => {
            if let Some(value) = read_only_unless_inhibited(after)
                && (sticks_ahead(after) || sticks_behind(after))
            {
                return Err(text_read_only(value));
            }
        }
        (after, before) => {
            if let Some(after) = after
                && let Some(value) = read_only_unless_inhibited(after)
                && sticks_ahead(after)
            {
                return Err(text_read_only(value));
            }
            if let Some(before) = before
                && let Some(value) = read_only_unless_inhibited(before)
                && sticks_behind(before)
            {
                return Err(text_read_only(value));
            }
        }
    }
    Ok(())
}

fn inhibit_read_only_value(interp: &Interpreter) -> Value {
    interp
        .symbol_value_key(cached_symbol!("inhibit-read-only"))
        .unwrap_or(Value::Nil)
}

fn buffer_read_only_active(interp: &Interpreter, inhibit_read_only: &Value) -> bool {
    interp
        .symbol_value_key(cached_symbol!("buffer-read-only"))
        .is_some_and(|value| value.is_truthy())
        && !inhibit_read_only.is_truthy()
}

fn buffer_read_only_signal(interp: &Interpreter) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("buffer-read-only".into()),
        Value::Buffer(interp.buffer),
    ]))
}

/// textprop.c:text_read_only: the property's string as the datum, none
/// for any other value.
fn text_read_only(property: Value) -> LispError {
    let mut data = vec![Value::Symbol("text-read-only".into())];
    if matches!(property.kind(), Kind::StringObject(_)) {
        data.push(property);
    }
    LispError::SignalValue(Value::list(data))
}

/// textprop.c:textget: PROP in PLIST as lookup_char_property reads it,
/// through the `category' symbol and `char-property-alias-alist'.
fn textget(
    interp: &Interpreter,
    plist: &[(PropertyName, Value)],
    prop: impl crate::lisp::types::PropertyKey,
) -> Value {
    property_from_props_with_category(interp, plist, prop).unwrap_or(Value::Nil)
}

/// fns.c:plist_get on an interval's own plist: no category.
fn plist_get(plist: &[(PropertyName, Value)], prop: impl crate::lisp::types::PropertyKey) -> Value {
    plist
        .iter()
        .find(|(name, _)| prop.names(name))
        .map_or(Value::Nil, |(_, value)| *value)
}

/// textprop.c TMEM: SYM in SET when SET is a list, else SET non-nil.
fn tmem(symbol: impl crate::lisp::types::PropertyKey, set: Value) -> bool {
    if is_cons(&set) {
        memq(symbol, set)
    } else {
        !set.is_nil()
    }
}

/// Fmemq: an element of LIST EQ to ELT (a circular list has none past
/// its cycle).
fn memq(elt: impl crate::lisp::types::PropertyKey, list: Value) -> bool {
    let mut tail = list;
    let mut seen = crate::lisp::types::CycleGuard::new();
    while let Kind::Cons(cell) = tail.kind() {
        if elt.is(cell.car.get()) {
            return true;
        }
        if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
            return false;
        }
        tail = cell.cdr.get();
    }
    false
}

fn is_cons(value: &Value) -> bool {
    matches!(value.kind(), Kind::Cons(_))
}

pub(crate) fn add_face_text_property(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    need_arg_range(name, args, 3, 5)?;
    let face = args[2];
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
                face
            } else {
                combine_font_lock_property_value("face", previous, &face, append)
            };
            modify_shared_string_properties(object, cursor, next, |mut current| {
                current.retain(|(key, _)| *key != "face");
                current.push((PropertyName::symbol("face"), updated));
                current
            })?;
            cursor = next;
        }
        return Ok(Value::Nil);
    }

    let start = position_from_value(interp, &args[0])?;
    let end = position_from_value(interp, &args[1])?;
    let buffer_id = font_lock_target_buffer_id(interp, args.get(4))?;
    // add_text_properties_1 with TEXT_PROPERTY_APPEND or _PREPEND skips no
    // interval: any nonempty range changes, and each of its intervals
    // takes FACE combined with its own.
    let (from, to) = if start <= end {
        (start, end)
    } else {
        (end, start)
    };
    modify_buffer_text_properties(
        interp,
        buffer_id,
        from,
        to,
        &|buffer| {
            let (from, to) = clamp_property_range(buffer, from, to);
            from < to
        },
        &|buffer| add_face_to_buffer(buffer, from, to, face, append),
        env,
    )?;
    Ok(Value::Nil)
}

/// add_properties' TEXT_PROPERTY_APPEND or _PREPEND of FACE over
/// [FROM, TO): each interval's face combined with FACE.
pub(crate) fn add_face_to_buffer(
    buffer: &mut crate::buffer::Buffer,
    from: usize,
    to: usize,
    face: Value,
    append: bool,
) {
    let face_name = PropertyName::symbol("face");
    let (from, to) = clamp_property_range(buffer, from, to);
    let mut cursor = from;
    while cursor < to {
        let previous = buffer
            .text_property_at(cursor, face_name)
            .unwrap_or(Value::Nil);
        let next = font_lock_next_buffer_property_change(buffer, cursor, to, "face");
        let updated = if previous.is_nil() {
            face
        } else {
            combine_font_lock_property_value("face", previous, &face, append)
        };
        if updated.is_nil() {
            buffer.remove_list_of_text_properties(cursor, next, &[face_name]);
        } else {
            buffer.put_text_property(cursor, next, face_name, updated);
        }
        cursor = next;
    }
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
        Ok(items) if !matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(symbol)) if symbol.starts_with(':')) => {
            items
        }
        _ => vec![*value],
    }
}

pub(crate) fn anonymous_font_lock_face(value: &Value) -> bool {
    let Ok(items) = value.to_vec() else {
        return false;
    };
    matches!(
        items.first().map(|v| v.kind()),
        Some(Kind::Symbol(symbol))
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
