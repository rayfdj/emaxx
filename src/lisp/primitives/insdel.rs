//! insdel.c's protocol around a change to the current buffer's text or
//! text properties: prepare_to_modify_buffer before it (the read-only
//! checks, the undoable-change notice, the text properties' and the
//! file lock's turn, then signal_before_change) and signal_after_change
//! after it.

use super::*;
use crate::lisp::types::{Kind, MarkerRef, PropertyName};

/// What a change reports to signal_after_change: where the changed text
/// starts, how many characters it replaced and how many are there now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AfterChange {
    pub(crate) charpos: usize,
    pub(crate) lendel: usize,
    pub(crate) lenins: usize,
}

impl AfterChange {
    pub(crate) fn new(charpos: usize, lendel: usize, lenins: usize) -> Self {
        Self {
            charpos,
            lendel,
            lenins,
        }
    }

    fn args(self) -> [Value; 3] {
        [
            Value::Integer(self.charpos as i64),
            Value::Integer((self.charpos + self.lenins) as i64),
            Value::Integer(self.lendel as i64),
        ]
    }
}

/// Whether `inhibit-modification-hooks' is non-nil: no change hooks run.
pub(crate) fn modification_hooks_inhibited(interp: &Interpreter) -> bool {
    interp
        .symbol_value_key(cached_symbol!("inhibit-modification-hooks"))
        .is_some_and(|value| !value.is_nil())
}

/// What prepare_to_modify_buffer leaves its change: the start the
/// caller preserved through the hooks, and the overlays' hooks to run
/// after the change (buffer.c's last_overlay_modification_hooks).
pub(crate) struct PreparedChange {
    pub(crate) start: usize,
    pub(crate) overlay_calls: OverlayHookCalls,
}

/// A change of [START, END) of the current buffer (START = END for an
/// insertion there) under insdel.c's protocol: prepare_to_modify_buffer,
/// then EDIT, then signal_after_change of the change EDIT reports (none
/// when it changed nothing).  With PRESERVE_START, EDIT is given START as
/// the hooks moved it (del_range_1's `&from'); otherwise START.
pub(crate) fn modify_current_buffer<R>(
    interp: &mut Interpreter,
    start: usize,
    end: usize,
    preserve_start: bool,
    env: &mut Env,
    edit: impl FnOnce(&mut Interpreter, usize, &mut Env) -> Result<(Option<AfterChange>, R), LispError>,
) -> Result<R, LispError> {
    let prepared = prepare_to_modify_buffer(interp, start, end, preserve_start, env)?;
    let start = prepared.start;
    with_overlay_hook_roots(interp, &prepared.overlay_calls, |interp| {
        let (change, result) = edit(interp, start, env)?;
        if let Some(change) = change {
            signal_after_change(interp, change, &prepared.overlay_calls, env)?;
        }
        Ok(result)
    })
}

/// insdel.c:prepare_to_modify_buffer_1 before a change of [START, END)
/// of the current buffer.
pub(crate) fn prepare_to_modify_buffer(
    interp: &mut Interpreter,
    start: usize,
    end: usize,
    preserve_start: bool,
    env: &mut Env,
) -> Result<PreparedChange, LispError> {
    let (start, end) = if start <= end {
        (start, end)
    } else {
        (end, start)
    };
    barf_if_buffer_read_only(interp, start)?;
    run_undoable_change(interp, env);
    let mut preserved = start;
    if interp.buffer.borrow().has_interval_tree() {
        verify_interval_modification(
            interp,
            start,
            end,
            preserve_start.then_some(&mut preserved),
            env,
        )?;
    }
    if modification_hooks_inhibited(interp) {
        return Ok(PreparedChange {
            start: preserved,
            overlay_calls: OverlayHookCalls::none(),
        });
    }
    lock_file_on_first_change(interp, env)?;
    let overlay_calls = signal_before_change(
        interp,
        start,
        end,
        preserve_start.then_some(&mut preserved),
        env,
    )?;
    set_internal_symbol(
        interp,
        &cached_symbol!("deactivate-mark").with(Clone::clone),
        Value::T,
        env,
    )?;
    Ok(PreparedChange {
        start: preserved,
        overlay_calls,
    })
}

/// buffer.c:Fbarf_if_buffer_read_only at POS.
pub(crate) fn barf_if_buffer_read_only(interp: &Interpreter, pos: usize) -> Result<(), LispError> {
    let inhibit = inhibit_read_only_value(interp);
    if buffer_read_only_active(interp, &inhibit)
        && textget(
            interp,
            interp.buffer.borrow().text_properties_at_ref(pos),
            cached_symbol!("inhibit-read-only"),
        )
        .is_nil()
    {
        return Err(buffer_read_only_signal(interp));
    }
    Ok(())
}

/// insdel.c:run_undoable_change: a buffer with undo calls the Lisp
/// `undo-auto--undoable-change' (which registers the buffer for the
/// auto-boundary timer) before every change, whatever the hooks.
fn run_undoable_change(interp: &mut Interpreter, env: &mut Env) {
    if interp.buffer.borrow().undo_recording_disabled() {
        return;
    }
    let symbol = cached_symbol!("undo-auto--undoable-change").with(Clone::clone);
    if interp.has_lisp_function_symbol(&symbol) {
        // The callee names its own frame (a name given here was interned
        // again per call).
        let _ = interp.call_function_value(Value::Symbol(symbol), None, &[], env);
    }
}

/// textprop.c:verify_interval_modification: the read-only checks over
/// the intervals of [START, END), then for a deletion or modification the
/// `modification-hooks' of its text, run here; for an insertion the
/// neighbours' `insert-behind-hooks' and `insert-in-front-hooks', run by
/// signal_after_change.  PRESERVED follows the hooks' changes.
fn verify_interval_modification(
    interp: &mut Interpreter,
    start: usize,
    end: usize,
    preserved: Option<&mut usize>,
    env: &mut Env,
) -> Result<(), LispError> {
    interp.interval_insert_hooks = [Value::Nil, Value::Nil];
    if start == end {
        interp.interval_insert_hooks = check_insertion_intervals(interp, start)?;
        return Ok(());
    }
    let hooks = check_modified_intervals(interp, start, end)?;
    if hooks.is_empty() || modification_hooks_inhibited(interp) {
        return Ok(());
    }
    let marker = preserved
        .as_ref()
        .map(|position| temporary_marker(interp, **position));
    let hook_count = hooks.len();
    let mut held = hooks;
    held.extend(marker.map(Value::Marker));
    let result = interp.with_lisp_stack_roots(&held, |interp| {
        let args = [Value::Integer(start as i64), Value::Integer(end as i64)];
        for hook in &held[..hook_count] {
            call_mod_hooks(interp, *hook, &args, env)?;
        }
        Ok(())
    });
    if let (Some(position), Some(marker)) = (preserved, marker) {
        *position = interp.marker_position(marker).unwrap_or(*position);
        detach_marker(interp, marker);
    }
    result
}

/// textprop.c:call_mod_hooks: each function of LIST called with ARGS.
fn call_mod_hooks(
    interp: &mut Interpreter,
    list: Value,
    args: &[Value],
    env: &mut Env,
) -> Result<(), LispError> {
    let mut tail = list;
    while !tail.is_nil() {
        let Kind::Cons(cell) = tail.kind() else {
            return Err(LispError::WrongTypeArgument("listp".into(), tail));
        };
        let function = cell.car.get();
        call_function_value(interp, &function, args, env)?;
        tail = cell.cdr.get();
    }
    Ok(())
}

/// textprop.c:report_interval_modification after an insertion of
/// [START, END): the hooks verify_interval_modification recorded, the
/// in-front ones unless they are the behind ones.
fn report_interval_modification(
    interp: &mut Interpreter,
    start: usize,
    end: usize,
    env: &mut Env,
) -> Result<(), LispError> {
    let [behind, in_front] = interp.interval_insert_hooks;
    if behind.is_nil() && in_front.is_nil() {
        return Ok(());
    }
    let held = vec![behind, in_front];
    interp.with_lisp_stack_roots(&held, |interp| {
        let args = [Value::Integer(start as i64), Value::Integer(end as i64)];
        if !behind.is_nil() {
            call_mod_hooks(interp, behind, &args, env)?;
        }
        if !in_front.is_nil() && in_front.word() != behind.word() {
            call_mod_hooks(interp, in_front, &args, env)?;
        }
        Ok(())
    })
}

/// insdel.c:prepare_to_modify_buffer_1: a file-visiting buffer's first
/// change locks its file truename (Flock_file, whose lock_file asks about
/// a supersession threat first).  Testing `buffer-file-name' too makes
/// binding it to nil effective.
fn lock_file_on_first_change(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    if interp.buffer.borrow().is_modified() {
        return Ok(());
    }
    let Some(truename) = interp
        .symbol_value_key(cached_symbol!("buffer-file-truename"))
        .filter(|value| value.is_truthy())
    else {
        return Ok(());
    };
    if !interp
        .symbol_value_key(cached_symbol!("buffer-file-name"))
        .is_some_and(|value| value.is_truthy())
    {
        return Ok(());
    }
    flock_file(interp, env, truename)
}

/// The markers signal_before_change keeps while the hooks run:
/// PRESERVE_START_END's bounds and PRESERVE_VALUE's preserved position.
struct BeforeChangeMarkers {
    bounds: Option<[MarkerRef; 2]>,
    preserved: Option<MarkerRef>,
}

impl crate::lisp::eval::roots::TraceLispRoots for BeforeChangeMarkers {
    fn trace_lisp_roots(&self, marker: &mut crate::lisp::eval::roots::LispRootMarker<'_>) {
        for bound in self.bounds.iter().flatten() {
            marker.value(&Value::Marker(*bound));
        }
        if let Some(preserved) = self.preserved {
            marker.value(&Value::Marker(preserved));
        }
    }
}

/// insdel.c:signal_before_change of [START, END), with
/// `inhibit-modification-hooks' bound: `first-change-hook' when the
/// buffer is unmodified, `before-change-functions', then the overlays'
/// hooks, the bounds following the earlier hooks' changes.  The overlays'
/// hooks to run after the change.
fn signal_before_change(
    interp: &mut Interpreter,
    start: usize,
    end: usize,
    preserved: Option<&mut usize>,
    env: &mut Env,
) -> Result<OverlayHookCalls, LispError> {
    let first_change = !interp.buffer.borrow().is_modified()
        && !hook_values(
            interp,
            "first-change-hook",
            env,
            Some(interp.current_buffer_id()),
        )
        .is_empty();
    let before_change = has_change_functions(interp, ChangeHook::Before);
    let overlays = !interp.buffer.borrow().overlays.is_empty();
    if !first_change && !before_change && !overlays {
        return Ok(OverlayHookCalls::none());
    }
    // PRESERVE_START_END before either hook, PRESERVE_VALUE before any.
    let markers = BeforeChangeMarkers {
        bounds: (first_change || before_change).then(|| {
            [
                temporary_marker(interp, start),
                temporary_marker(interp, end),
            ]
        }),
        preserved: preserved
            .as_ref()
            .map(|position| temporary_marker(interp, **position)),
    };
    let restore = interp.bind_special_dynamic("inhibit-modification-hooks", Value::T, env)?;
    let result: Result<OverlayHookCalls, LispError> =
        interp.with_lisp_stack_roots(&markers, |interp| {
            let fetch = |interp: &Interpreter| match markers.bounds {
                Some([start_marker, end_marker]) => (
                    interp.marker_position(start_marker).unwrap_or(start),
                    interp.marker_position(end_marker).unwrap_or(end),
                ),
                None => (start, end),
            };
            if first_change {
                let hooks = hook_values(
                    interp,
                    "first-change-hook",
                    env,
                    Some(interp.current_buffer_id()),
                );
                interp.with_lisp_stack_roots(&hooks, |interp| {
                    for hook in &hooks {
                        call_function_value(interp, hook, &[], env)?;
                    }
                    Ok::<(), LispError>(())
                })?;
            }
            if has_change_functions(interp, ChangeHook::Before) {
                let (start, end) = fetch(interp);
                run_change_functions(
                    interp,
                    ChangeHook::Before,
                    &[Value::Integer(start as i64), Value::Integer(end as i64)],
                    env,
                )?;
            }
            let (start, end) = fetch(interp);
            let calls = overlay_change_hook_calls(interp, &interp.buffer.borrow(), start, end);
            with_overlay_hook_roots(interp, &calls, |interp| {
                run_overlay_hook_calls(
                    interp,
                    &calls,
                    false,
                    &[Value::Integer(start as i64), Value::Integer(end as i64)],
                    env,
                )
            })?;
            Ok(calls)
        });
    for bound in markers.bounds.iter().flatten() {
        detach_marker(interp, *bound);
    }
    if let (Some(position), Some(marker)) = (preserved, markers.preserved) {
        *position = interp.marker_position(marker).unwrap_or(*position);
        detach_marker(interp, marker);
    }
    let restore_result = interp.restore_special_dynamic(restore, env);
    let calls = result?;
    restore_result?;
    Ok(calls)
}

/// insdel.c:signal_after_change of CHANGE, unless the hooks are
/// inhibited: deferred under `combine-after-change-calls' when nothing
/// else needs running, else with `inhibit-modification-hooks' bound
/// `after-change-functions', the overlays' hooks OVERLAY_CALLS prepared
/// before the change, and after an insertion the hooks the inserted
/// text's neighbours' properties named.
pub(crate) fn signal_after_change(
    interp: &mut Interpreter,
    change: AfterChange,
    overlay_calls: &OverlayHookCalls,
    env: &mut Env,
) -> Result<(), LispError> {
    if modification_hooks_inhibited(interp) {
        return Ok(());
    }
    if defer_after_change(interp, change, env) {
        return Ok(());
    }
    // The insert hooks this change recorded, kept from those a hook's own
    // changes record (insdel.c saves and restores them around the hook).
    let saved_insert_hooks = interp.interval_insert_hooks;
    interp.with_lisp_stack_roots(&saved_insert_hooks.to_vec(), |interp| {
        if interp.combined_after_change().is_some() {
            flush_combined_after_change(interp, env)?;
        }
        let after_change = has_change_functions(interp, ChangeHook::After);
        let overlays = !overlay_calls.is_empty();
        let insert_hooks = change.lendel == 0 && saved_insert_hooks.iter().any(|v| !v.is_nil());
        if !after_change && !overlays && !insert_hooks {
            return Ok(());
        }
        let restore = interp.bind_special_dynamic("inhibit-modification-hooks", Value::T, env)?;
        let result = (|| -> Result<(), LispError> {
            if after_change {
                run_change_functions(interp, ChangeHook::After, &change.args(), env)?;
            }
            interp.interval_insert_hooks = saved_insert_hooks;
            run_overlay_hook_calls(interp, overlay_calls, true, &change.args(), env)?;
            if change.lendel == 0 {
                report_interval_modification(
                    interp,
                    change.charpos,
                    change.charpos + change.lenins,
                    env,
                )?;
            }
            Ok(())
        })();
        let restore_result = interp.restore_special_dynamic(restore, env);
        result?;
        restore_result
    })
}

/// insdel.c's deferral of an after-change call under
/// `combine-after-change-calls': when no `before-change-functions' (but
/// `syntax-ppss-flush-cache' alone, buffer-locally) and no overlays need
/// running, the change's unchanged ends and size change are recorded for
/// `combine-after-change-execute'.  Whether the call was deferred.
fn defer_after_change(interp: &mut Interpreter, change: AfterChange, env: &mut Env) -> bool {
    if !interp
        .symbol_value_key(cached_symbol!("combine-after-change-calls"))
        .is_some_and(|value| !value.is_nil())
        || !interp.buffer.borrow().overlays.is_empty()
    {
        return false;
    }
    let before = interp
        .symbol_value_key(cached_symbol!("before-change-functions"))
        .unwrap_or(Value::Nil);
    if !before.is_nil() && !syntax_ppss_flush_cache_alone(interp, before) {
        return false;
    }
    let _ = env;
    let buffer_id = interp.current_buffer_id();
    if interp
        .combined_after_change()
        .is_some_and(|pending| pending.buffer_id != buffer_id)
    {
        // Fcombine_after_change_execute of another buffer's changes first.
        return false;
    }
    let z = interp.buffer.borrow().size_total() as i64 + 1;
    let (charpos, lendel, lenins) = (
        change.charpos as i64,
        change.lendel as i64,
        change.lenins as i64,
    );
    interp.record_combined_after_change(
        buffer_id,
        (
            charpos - 1,
            z - (charpos - lendel + lenins),
            lenins - lendel,
        ),
    );
    true
}

/// `before-change-functions' as `(t syntax-ppss-flush-cache)' over a nil
/// default: the one hook insdel.c lets after-change calls be combined
/// past.
fn syntax_ppss_flush_cache_alone(interp: &Interpreter, before: Value) -> bool {
    let Some((first, rest)) = before.cons_values() else {
        return false;
    };
    let Some((function, tail)) = rest.cons_values() else {
        return false;
    };
    matches!(first.kind(), Kind::T)
        && tail.is_nil()
        && function.symbol_str() == Some("syntax-ppss-flush-cache")
        && interp
            .default_value_symbol(&cached_symbol!("before-change-functions").with(Clone::clone))
            .ok()
            .flatten()
            .is_none_or(|value| value.is_nil())
}

/// A marker at POS in the current buffer, for the protocol's own use.
fn temporary_marker(interp: &mut Interpreter, pos: usize) -> MarkerRef {
    let Kind::Marker(marker) = interp.make_marker().kind() else {
        unreachable!("make_marker returns a marker")
    };
    let buffer_id = interp.current_buffer_id();
    let _ = interp.set_marker(marker, Some(pos), Some(buffer_id));
    marker
}

fn detach_marker(interp: &mut Interpreter, marker: MarkerRef) {
    let _ = interp.set_marker(marker, None, None);
}

/// textprop.c:verify_interval_modification's read-only checks of a
/// change of [FROM, TO), and the `modification-hooks' of its text, each
/// interval's unless the previous interval's are the same.
fn check_modified_intervals(
    interp: &Interpreter,
    from: usize,
    to: usize,
) -> Result<Vec<Value>, LispError> {
    let inhibit = inhibit_read_only_value(interp);
    let buffer_read_only = buffer_read_only_active(interp, &inhibit);
    let buffer = interp.buffer.borrow();
    let mut hooks = Vec::new();
    if !buffer.has_text_properties() {
        return Ok(hooks);
    }
    let mut previous = Value::Nil;
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
        let mod_hooks = textget(interp, props, cached_symbol!("modification-hooks"));
        if !mod_hooks.is_nil() && mod_hooks.word() != previous.word() {
            hooks.push(mod_hooks);
            previous = mod_hooks;
        }
        Ok(())
    })?;
    Ok(hooks)
}

/// textprop.c:verify_interval_modification's insertion case at POS: read-
/// only text on either side refuses the insertion when its stickiness
/// would extend it over the inserted text.  The neighbours'
/// `insert-behind-hooks' and `insert-in-front-hooks'.
fn check_insertion_intervals(interp: &Interpreter, pos: usize) -> Result<[Value; 2], LispError> {
    let inhibit = inhibit_read_only_value(interp);
    let buffer = interp.buffer.borrow();
    // The intervals of the characters after and before POS: none at the
    // accessible region's edges, and the same one inside a span.
    let after = (pos < buffer.point_max()).then(|| buffer.text_properties_at_ref(pos));
    let before = (pos > buffer.point_min()).then(|| buffer.text_properties_at_ref(pos - 1));
    // A non-list `inhibit-read-only' lets any text be inserted into.
    if inhibit.is_nil() || is_cons(&inhibit) {
        check_insertion_stickiness(interp, inhibit, after, before)?;
    }
    Ok([
        before.map_or(Value::Nil, |props| {
            textget(interp, props, cached_symbol!("insert-behind-hooks"))
        }),
        after.map_or(Value::Nil, |props| {
            textget(interp, props, cached_symbol!("insert-in-front-hooks"))
        }),
    ])
}

fn check_insertion_stickiness(
    interp: &Interpreter,
    inhibit: Value,
    after: Option<&[(PropertyName, Value)]>,
    before: Option<&[(PropertyName, Value)]>,
) -> Result<(), LispError> {
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
