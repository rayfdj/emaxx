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

/// The two hooks insdel.c runs around a change.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChangeHook {
    Before,
    After,
}

impl ChangeHook {
    fn name(self) -> &'static str {
        match self {
            ChangeHook::Before => "before-change-functions",
            ChangeHook::After => "after-change-functions",
        }
    }

    fn symbol(self) -> crate::lisp::types::SymbolName {
        match self {
            ChangeHook::Before => cached_symbol!("before-change-functions").with(Clone::clone),
            ChangeHook::After => cached_symbol!("after-change-functions").with(Clone::clone),
        }
    }
}

/// Whether HOOK's current value is non-nil (insdel.c's NILP test before
/// it runs the hook).
pub(crate) fn has_change_functions(interp: &Interpreter, hook: ChangeHook) -> bool {
    let value = match hook {
        ChangeHook::Before => interp.symbol_value_key(cached_symbol!("before-change-functions")),
        ChangeHook::After => interp.symbol_value_key(cached_symbol!("after-change-functions")),
    };
    value.is_some_and(|value| !value.is_nil())
}

/// run_hook_with_args of HOOK with ARGS in the current buffer, the hook
/// set to nil (its local binding, when that is the one run) if a function
/// exits nonlocally: insdel.c's reset_var_on_error, which keeps a broken
/// hook from breaking every later change.
pub(crate) fn run_change_functions(
    interp: &mut Interpreter,
    hook: ChangeHook,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<(), LispError> {
    let hook_name = hook.name();
    let hooks = hook_values_of(
        interp,
        Some(&hook.symbol()),
        hook_name,
        env,
        Some(interp.current_buffer_id()),
    );
    if hooks.is_empty() {
        return Ok(());
    }
    let result = interp.with_lisp_stack_roots(&hooks, |interp| {
        for function in &hooks {
            call_function_value(interp, function, args, env)?;
        }
        Ok(())
    });
    if result.is_err() {
        let local_buffer_id = interp.assignment_buffer_id(hook_name);
        interp.set_variable(hook_name, Value::Nil, env);
        if let Some(buffer_id) = local_buffer_id {
            interp.remove_buffer_local_hook(buffer_id, hook_name);
        }
    }
    result
}

/// insdel.c:Fcombine_after_change_execute: the changes deferred under
/// `combine-after-change-calls', signalled in their buffer as one change
/// over the text they span (whole-buffer offsets, as recorded).
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
    // Characters unchanged at the beginning and end of the buffer, and
    // the net insertion.
    let size = interp.buffer.borrow().size_total() as i64;
    let (mut unchanged_before, mut unchanged_after, mut net_change) = (size, size, 0_i64);
    for (before, after, change) in pending.changes {
        unchanged_before = unchanged_before.min(before);
        unchanged_after = unchanged_after.min(after);
        net_change += change;
    }
    let begin = 1 + unchanged_before;
    let end = (size + 1 - unchanged_after).max(begin);
    let change = AfterChange::new(
        begin as usize,
        (end - begin - net_change).max(0) as usize,
        (end - begin) as usize,
    );
    let restore = interp.bind_special_dynamic("combine-after-change-calls", Value::Nil, env)?;
    let result = signal_after_change(interp, change, &OverlayHookCalls::none(), env);
    let restore_result = interp.restore_special_dynamic(restore, env);
    if interp.has_buffer_id(saved_buffer_id) {
        interp.switch_to_buffer_id(saved_buffer_id)?;
    }
    result?;
    restore_result?;
    Ok(Value::Nil)
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

/// The overlays' hook functions buffer.c:report_overlay_modification
/// found before a change, run again after it.
pub(crate) struct OverlayHookCalls {
    calls: Vec<OverlayHookCall>,
}

impl OverlayHookCalls {
    pub(crate) fn none() -> Self {
        Self { calls: Vec::new() }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.calls.is_empty()
    }
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

/// buffer.c:report_overlay_modification's search before a change of
/// [FROM, TO) (FROM = TO for an insertion): the `modification-hooks' of
/// the overlays it touches and, for an insertion, the
/// `insert-in-front-hooks' and `insert-behind-hooks' of those starting or
/// ending there.
pub(crate) fn overlay_change_hook_calls(
    interp: &Interpreter,
    buffer: &crate::buffer::Buffer,
    from: usize,
    to: usize,
) -> OverlayHookCalls {
    let mut calls = Vec::new();
    // A buffer with no overlays has none to search.
    if buffer.overlays.is_empty() {
        return OverlayHookCalls { calls };
    }
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
    OverlayHookCalls { calls }
}

/// buffer.c:report_overlay_modification's calls: each recorded function
/// with its overlay, AFTER, and ARGS (the change's bounds before it, its
/// bounds and replaced length after it).
pub(crate) fn run_overlay_hook_calls(
    interp: &mut Interpreter,
    calls: &OverlayHookCalls,
    after: bool,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<(), LispError> {
    for call in &calls.calls {
        // GNU skips a recorded hook when an earlier callback deleted or
        // moved its overlay, both before and after the edit.
        if !call
            .overlay
            .buffer()
            .is_some_and(|owner| owner.ptr_eq(&interp.buffer))
        {
            continue;
        }
        let mut hook_args = [Value::Nil; 5];
        hook_args[0] = Value::Overlay(call.overlay);
        hook_args[1] = if after { Value::T } else { Value::Nil };
        hook_args[2..2 + args.len()].copy_from_slice(args);
        let hook_args = &hook_args[..2 + args.len()];
        let mut list = call.functions;
        while let Kind::Cons(cell) = list.kind() {
            let function = cell.car.get();
            interp.with_lisp_stack_roots(&list, |interp| {
                call_function_value(interp, &function, hook_args, env)
            })?;
            list = cell.cdr.get();
        }
    }
    Ok(())
}

/// insdel.c:del_range_1 with its hooks: [FROM, TO) of the accessible
/// text deleted, its start following the before-change hooks' changes
/// and its length kept.  The deleted text.
pub(crate) fn delete_region_with_hooks(
    interp: &mut Interpreter,
    from: usize,
    to: usize,
    env: &mut crate::lisp::types::Env,
) -> Result<String, LispError> {
    delete_region_with_hooks_then(interp, from, to, env, |_, _, _| ()).map(|(text, ())| text)
}

/// `delete-and-extract-region': the deleted text as a string, with its
/// properties and the buffer's multibyteness, made after the hooks ran.
pub(crate) fn extract_region_with_hooks(
    interp: &mut Interpreter,
    from: usize,
    to: usize,
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    // editfns.c: an empty region is the empty unibyte string, no change.
    if from == to {
        return Ok(bytes_to_unibyte_value(&[]));
    }
    let (text, (props, multibyte, extended)) =
        delete_region_with_hooks_then(interp, from, to, env, |interp, from, to| {
            let buffer = interp.buffer.borrow();
            (
                buffer.substring_property_spans(from, to),
                buffer.is_multibyte(),
                buffer.substring_extended_chars(from, to),
            )
        })?;
    Ok(string_like_value_with_extended_chars(
        text, props, multibyte, extended,
    ))
}

fn delete_region_with_hooks_then<R>(
    interp: &mut Interpreter,
    from: usize,
    to: usize,
    env: &mut crate::lisp::types::Env,
    before_delete: impl FnOnce(&Interpreter, usize, usize) -> R,
) -> Result<(String, R), LispError> {
    let (from, to) = if from <= to { (from, to) } else { (to, from) };
    let (from, to) = {
        let buffer = interp.buffer.borrow();
        (from.max(buffer.point_min()), to.min(buffer.point_max()))
    };
    if from >= to {
        return Ok((String::new(), before_delete(interp, from, from)));
    }
    let range_length = to - from;
    modify_current_buffer(interp, from, to, true, env, |interp, from, _| {
        let to = (from + range_length)
            .min(interp.buffer.borrow().point_max())
            .max(from);
        let extra = before_delete(interp, from, to);
        let deleted = interp
            .delete_region_current_buffer(from, to)
            .map_err(|e| LispError::Signal(e.to_string()))?;
        Ok((Some(AfterChange::new(from, to - from, 0)), (deleted, extra)))
    })
}

/// textprop.c's buffer case of a property change, when CHANGES finds that
/// the properties of [START, END) in BUFFER_ID would change, with that
/// buffer current throughout (the primitives' set_buffer_internal, bug
/// 36190) and the caller's restored after:
/// modify_text_properties (insdel.c:prepare_to_modify_buffer_1 over the
/// range: the read-only checks, the overlays' modification hooks, the
/// undoable-change notice and `before-change-functions'), APPLY over the
/// buffer and its indirect relatives, then signal_after_change.  Whether
/// anything changed.
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
    if buffer_id != original {
        interp.set_current_buffer_id(buffer_id)?;
    }
    let result = prepare_to_modify_buffer(interp, start, end, false, env).and_then(|prepared| {
        with_overlay_hook_roots(interp, &prepared.overlay_calls, |interp| {
            interp.apply_text_property_change_shared_for(buffer_id, apply);
            signal_after_change(
                interp,
                AfterChange::new(start, end - start, end - start),
                &prepared.overlay_calls,
                env,
            )
        })
    });
    if buffer_id != original && interp.has_buffer_id(original) {
        interp.set_current_buffer_id(original)?;
    }
    result?;
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

fn clamp_property_range(buffer: &crate::buffer::Buffer, from: usize, to: usize) -> (usize, usize) {
    let (from, to) = if from <= to { (from, to) } else { (to, from) };
    (from.max(buffer.point_min()), to.min(buffer.point_max()))
}

pub(crate) fn inhibit_read_only_value(interp: &Interpreter) -> Value {
    interp
        .symbol_value_key(cached_symbol!("inhibit-read-only"))
        .unwrap_or(Value::Nil)
}

pub(crate) fn buffer_read_only_active(interp: &Interpreter, inhibit_read_only: &Value) -> bool {
    interp
        .symbol_value_key(cached_symbol!("buffer-read-only"))
        .is_some_and(|value| value.is_truthy())
        && !inhibit_read_only.is_truthy()
}

pub(crate) fn buffer_read_only_signal(interp: &Interpreter) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("buffer-read-only".into()),
        Value::Buffer(interp.buffer),
    ]))
}

/// textprop.c:text_read_only: the property's string as the datum, none
/// for any other value.
pub(crate) fn text_read_only(property: Value) -> LispError {
    let mut data = vec![Value::Symbol("text-read-only".into())];
    if matches!(property.kind(), Kind::StringObject(_)) {
        data.push(property);
    }
    LispError::SignalValue(Value::list(data))
}

/// textprop.c:textget: PROP in PLIST as lookup_char_property reads it,
/// through the `category' symbol and `char-property-alias-alist'.
pub(crate) fn textget(
    interp: &Interpreter,
    plist: &[(PropertyName, Value)],
    prop: impl crate::lisp::types::PropertyKey,
) -> Value {
    property_from_props_with_category(interp, plist, prop).unwrap_or(Value::Nil)
}

/// fns.c:plist_get on an interval's own plist: no category.
pub(crate) fn plist_get(
    plist: &[(PropertyName, Value)],
    prop: impl crate::lisp::types::PropertyKey,
) -> Value {
    plist
        .iter()
        .find(|(name, _)| prop.names(name))
        .map_or(Value::Nil, |(_, value)| *value)
}

/// textprop.c TMEM: SYM in SET when SET is a list, else SET non-nil.
pub(crate) fn tmem(symbol: impl crate::lisp::types::PropertyKey, set: Value) -> bool {
    if is_cons(&set) {
        memq(symbol, set)
    } else {
        !set.is_nil()
    }
}

/// Fmemq: an element of LIST EQ to ELT (a circular list has none past
/// its cycle).
pub(crate) fn memq(elt: impl crate::lisp::types::PropertyKey, list: Value) -> bool {
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

pub(crate) fn is_cons(value: &Value) -> bool {
    matches!(value.kind(), Kind::Cons(_))
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
