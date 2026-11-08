use super::*;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;
use crate::lisp::types::SymbolName;

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
    let symbol = hook.symbol();
    let result = run_hook_with_args(interp, &symbol, env, |interp, function, env| {
        call_function_value(interp, &function, args, env)?;
        Ok(Value::Nil)
    });
    if result.is_err() {
        // `*rvoe_arg->location = Qnil': the C variable, which is the
        // current binding, stored without notifying watchers.
        interp.set_symbol_value_cell_resolved(&symbol, Value::Nil);
    }
    result.map(drop)
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
    // set_buffer_internal, the current buffer restored after.
    let saved_buffer_id = interp.current_buffer_id();
    interp.set_current_buffer_id(pending.buffer_id)?;
    // Characters unchanged at the beginning and end of the buffer, and
    // the net insertion.
    let size = interp.buffer.borrow().size_total() as i64;
    let (mut unchanged_before, mut unchanged_after, mut net_change) = (size, size, 0_i64);
    for (before, after, change) in pending.changes {
        unchanged_before = unchanged_before.min(before);
        unchanged_after = unchanged_after.min(after);
        net_change += change;
    }
    // Neither clipped: a deletion's end count includes the deleted text,
    // so END can fall before BEGIN and the counts go negative, as GNU's.
    let begin = 1 + unchanged_before;
    let end = size + 1 - unchanged_after;
    let change = AfterChange::signed(begin, end - begin - net_change, end - begin);
    // The flag is not turned off (despite insdel.c's comment): under a
    // non-nil `combine-after-change-calls' the combined change is deferred
    // again.  Fcombine_after_change_execute_1 sets back the value it had.
    let flag = interp
        .symbol_value_key(cached_symbol!("combine-after-change-calls"))
        .unwrap_or(Value::Nil);
    let result = interp.with_lisp_stack_roots(&[flag], |interp| {
        let result =
            signal_after_change(interp, change, &OverlayHookCalls::none(), env).and_then(|()| {
                if begin < 1 || end < begin {
                    return Ok(());
                }
                update_compositions(interp, begin as usize, end as usize, CHECK_ALL, env)
            });
        interp.set_symbol_value_cell("combine-after-change-calls", flag);
        result
    });
    if interp.has_buffer_id(saved_buffer_id) {
        interp.set_current_buffer_id(saved_buffer_id)?;
    }
    result?;
    Ok(Value::Nil)
}

/// FUNCTIONP of a cons, as run_hook_with_args asks it: a lambda list.
fn is_lambda_list(value: Value) -> bool {
    value.cons_ref().is_some_and(|cell| {
        cell.car
            .get()
            .symbol_cell()
            .is_some_and(|car| car.is_named("lambda"))
    })
}

/// eval.c:run_hook_with_args.  HOOK's value is read as find_symbol_value
/// reads it: a void or nil value runs nothing, a value that is not a cons,
/// or is a lambda, is the one function, and a list's functions run in
/// order while FUNCALL answers nil, a `t' running the default value's in
/// its place (Fdefault_value, so a void default signals; a `t' among the
/// default's functions is skipped).  The list is walked as it stands, the
/// next tail read after each call, so a function's change to the conses
/// is seen as GNU sees it; the tail in hand is a Rust local, which the
/// stack scan finds as it finds GNU's.  The answer is FUNCALL's last.
/// Nothing runs once shut_down_emacs has cleared Vrun_hooks.
pub(crate) fn run_hook_with_args(
    interp: &mut Interpreter,
    hook: &SymbolName,
    env: &mut Env,
    mut funcall: impl FnMut(&mut Interpreter, Value, &mut Env) -> Result<Value, LispError>,
) -> Result<Value, LispError> {
    if interp.hooks_cleared {
        return Ok(Value::Nil);
    }
    let Some(mut val) = interp.symbol_value_symbol(hook) else {
        return Ok(Value::Nil);
    };
    if val.is_nil() {
        return Ok(Value::Nil);
    }
    if !val.is_cons() || is_lambda_list(val) {
        return funcall(interp, val, env);
    }
    let mut ret = Value::Nil;
    while ret.is_nil()
        && let Some(cell) = val.cons_ref()
    {
        let function = cell.car.get();
        if matches!(function.kind(), Kind::T) {
            // t: this hook has a local binding, and the global one runs
            // here too.
            let mut global = interp
                .default_value_symbol(hook)?
                .ok_or_else(|| LispError::Void(Value::Symbol(*hook)))?;
            if !global.is_nil() {
                if !global.is_cons() || is_lambda_list(global) {
                    ret = funcall(interp, global, env)?;
                } else {
                    while ret.is_nil()
                        && let Some(global_cell) = global.cons_ref()
                    {
                        let function = global_cell.car.get();
                        // In a global value, t should not occur; one is
                        // ignored, which avoids an endless loop.
                        if !matches!(function.kind(), Kind::T) {
                            ret = funcall(interp, function, env)?;
                        }
                        global = global_cell.cdr.get();
                    }
                }
            }
        } else {
            ret = funcall(interp, function, env)?;
        }
        val = cell.cdr.get();
    }
    Ok(ret)
}

/// eval.c:run_hook: HOOK's functions called with no arguments
/// (Frun_hook_with_args with funcall_nil).
pub(crate) fn run_hook(
    interp: &mut Interpreter,
    hook: &SymbolName,
    env: &mut Env,
) -> Result<(), LispError> {
    run_hook_with_args(interp, hook, env, |interp, function, env| {
        call_function_value(interp, &function, &[], env)?;
        Ok(Value::Nil)
    })
    .map(drop)
}

/// keyboard.c:safe_run_hooks: HOOK run with `inhibit-quit' bound to t,
/// each function under internal_condition_case_n with Qt, so neither an
/// outer handler-bind handler nor the debugger sees its error, which is
/// reported and takes the function out of the hook (safe_run_hooks_error);
/// the remaining functions still run.  Nonlocal exits keep propagating.
pub(crate) fn safe_run_hooks(
    interp: &mut Interpreter,
    hook: &SymbolName,
    env: &mut Env,
) -> Result<(), LispError> {
    safe_run_hooks_with_args(interp, hook, &[], env)
}

/// keyboard.c:safe_run_hooks_2 and the like: `safe_run_hooks' calling
/// each function with ARGS.
pub(crate) fn safe_run_hooks_with_args(
    interp: &mut Interpreter,
    hook: &SymbolName,
    args: &[Value],
    env: &mut Env,
) -> Result<(), LispError> {
    let count = interp.specpdl_index();
    let result = interp
        .specbind_symbol(
            &cached_symbol!("inhibit-quit").with(|symbol| *symbol),
            Value::T,
            env,
        )
        .and_then(|()| {
            run_hook_with_args(interp, hook, env, |interp, function, env| {
                safe_run_hook_funcall(interp, hook, function, args, env).map(|()| Value::Nil)
            })
        });
    let unbound = interp.unbind_to(count, env);
    result.and(unbound)
}

/// keyboard.c:safe_run_hook_funcall: FUNCTION called with ARGS under
/// internal_condition_case_n, which establishes its catch before the
/// call, its error handled by safe_run_hooks_error.
fn safe_run_hook_funcall(
    interp: &mut Interpreter,
    hook: &SymbolName,
    function: Value,
    args: &[Value],
    env: &mut Env,
) -> Result<(), LispError> {
    let handlers = interp.push_internal_condition_case(crate::lisp::eval::InternalHandlers::All);
    let depth = env.len();
    let result = call_function_value(interp, &function, args, env);
    interp.pop_handler_bindings(handlers);
    env.truncate(depth);
    match result.map_err(LispError::into_kind) {
        Ok(_) => Ok(()),
        Err(error @ (LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_))) => {
            Err(LispError::from(error))
        }
        Err(error) => {
            let condition = crate::lisp::eval::error_condition_value(&LispError::from(error));
            safe_run_hooks_error(interp, hook, function, condition, env)
        }
    }
}

/// keyboard.c:safe_run_hooks_error: CALLN (Fmessage, "Error in %s (%S):
/// %S", hook, fun, error), the data formatted by `message' so a `%' in a
/// printed object is no directive; then FUN is deleted from HOOK's
/// current value with Fset, or failing that from its default value with
/// Fset_default, the other functions kept in order.
fn safe_run_hooks_error(
    interp: &mut Interpreter,
    hook: &SymbolName,
    fun: Value,
    error: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    crate::lisp::primitives::call(
        interp,
        "message",
        &[
            Value::String("Error in %s (%S): %S".into()),
            Value::Symbol(*hook),
            fun,
            error,
        ],
        env,
    )?;
    let without = |interp: &Interpreter, env: &Env, mut val: Value| {
        let mut found = false;
        let mut newval = Vec::new();
        while let Some(cell) = val.cons_ref() {
            let element = cell.car.get();
            if crate::lisp::primitives::values::values_eq_in_env(interp, &fun, &element, env) {
                found = true;
            } else {
                newval.push(element);
            }
            val = cell.cdr.get();
        }
        found.then(|| Value::list(newval))
    };
    let current = interp.symbol_value_symbol(hook).unwrap_or(Value::Nil);
    if let Some(newval) = without(interp, env, current) {
        return crate::lisp::primitives::set_internal_symbol(interp, hook, newval, env);
    }
    let default = interp.default_value_symbol(hook)?.unwrap_or(Value::Nil);
    if let Some(newval) = without(interp, env, default) {
        return crate::lisp::primitives::set_default_internal_symbol(interp, hook, newval, env);
    }
    Ok(())
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
        interp.set_current_buffer_id(saved_buffer_id)?;
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
        interp.set_current_buffer_id(saved_buffer_id)?;
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
/// ending there, each read by Foverlay_get, which can signal.
pub(crate) fn overlay_change_hook_calls(
    interp: &Interpreter,
    buffer: &crate::buffer::Buffer,
    from: usize,
    to: usize,
) -> Result<OverlayHookCalls, LispError> {
    let mut calls = Vec::new();
    // A buffer with no overlays has none to search.
    if buffer.overlays.is_empty() {
        return Ok(OverlayHookCalls { calls });
    }
    let insertion = from == to;
    let mut add = |overlay, property| -> Result<(), LispError> {
        if let Some(functions) =
            overlay_property_with_category(interp, &overlay, property)?.filter(Value::is_truthy)
        {
            calls.push(OverlayHookCall { overlay, functions });
        }
        Ok(())
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
            add(overlay, "insert-in-front-hooks")?;
        }
        if insertion && from as isize == end {
            add(overlay, "insert-behind-hooks")?;
        }
        if (to as isize) > beg && (from as isize) < end {
            add(overlay, "modification-hooks")?;
        }
    }
    Ok(OverlayHookCalls { calls })
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

/// insdel.c:del_range_1 with its hooks and no RET_STRING: [FROM, TO) of
/// the accessible text deleted, its start following the before-change
/// hooks' changes and its length kept.
pub(crate) fn delete_region_with_hooks(
    interp: &mut Interpreter,
    from: usize,
    to: usize,
    env: &mut crate::lisp::types::Env,
) -> Result<(), LispError> {
    delete_region_with_hooks_then(interp, from, to, false, env, |_, _, _| ()).map(drop)
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
        delete_region_with_hooks_then(interp, from, to, true, env, |interp, from, to| {
            let buffer = interp.buffer.borrow();
            (
                buffer.string_intervals(from, to),
                buffer.is_multibyte(),
                buffer.substring_extended_chars(from, to),
            )
        })?;
    Ok(string_like_value_with_extended_chars(
        text, props, multibyte, extended,
    ))
}

/// RET_STRING is del_range_1's: the caller wants the deleted text, so
/// its string is made (and `buffer-access-fontify-functions' run over it)
/// even in a buffer without undo.
fn delete_region_with_hooks_then<R>(
    interp: &mut Interpreter,
    from: usize,
    to: usize,
    ret_string: bool,
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
    modify_current_buffer(interp, from, to, true, env, |interp, from, env| {
        let to = (from + range_length)
            .min(interp.buffer.borrow().point_max())
            .max(from);
        // del_range_2's make_buffer_string_both, before the text goes.
        crate::lisp::primitives::textprop::update_deleted_text_properties(
            interp, from, to, ret_string, env,
        )?;
        let to = to.min(interp.buffer.borrow().point_max()).max(from);
        let extra = before_delete(interp, from, to);
        // del_range_1's RET_STRING: the text is read out only for a
        // caller that wants it (the undo list takes its own string).
        let deleted = if ret_string {
            interp.delete_region_current_buffer(from, to)
        } else {
            interp
                .delete_region_current_buffer_unreturned(from, to)
                .map(|()| String::new())
        }
        .map_err(|e| LispError::Signal(e.to_string()))?;
        Ok((
            Some(AfterChange::new(from, to - from, 0).composing(from, from, CHECK_HEAD)),
            (deleted, extra),
        ))
    })
}

/// textprop.c's buffer case of a property change, when PREPARE, the
/// primitive's interval analysis up to its modify_text_properties (with
/// the splits it makes there, which stay when a hook signals), finds that
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
    prepare: &dyn Fn(&mut crate::buffer::Buffer) -> Result<bool, LispError>,
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
        prepare(&mut interp.buffer.borrow_mut())?
    } else {
        match interp.get_buffer_by_id_mut(buffer_id) {
            Some(mut buffer) => prepare(&mut buffer)?,
            None => false,
        }
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
    plist: Value,
    prop: impl crate::lisp::types::PropertyKey,
) -> Result<Value, LispError> {
    Ok(property_from_props_with_category(interp, plist, prop)?.unwrap_or(Value::Nil))
}

/// fns.c:plist_get on an interval's own plist: no category.
pub(crate) fn plist_get(plist: Value, prop: impl crate::lisp::types::PropertyKey) -> Value {
    crate::lisp::primitives::strings::lisp_plist_get(plist, prop)
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
