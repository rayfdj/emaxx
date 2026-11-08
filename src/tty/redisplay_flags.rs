//! xdisp.c's bookkeeping of what redisplay must do: the
//! `windows_or_buffers_changed' and `update_mode_lines' requests, the
//! windows', buffers' and frames' redisplay bits, and what a window's
//! last complete redisplay left (mark_window_display_accurate_1), which
//! redisplay_internal and redisplay_window decide from.

use crate::lisp::cached_symbol;
use crate::lisp::eval::Interpreter;
use crate::lisp::eval::runtime::REDISPLAY_SOME;
use crate::lisp::primitives::{
    WINDOW_COLUMN_NUMBER_DISPLAYED_SLOT, WINDOW_LAST_HAD_STAR_SLOT, WINDOW_LAST_MODIFIED_SLOT,
    WINDOW_LAST_OVERLAY_MODIFIED_SLOT, WINDOW_LAST_POINT_SLOT, WINDOW_POINT_SLOT,
    WINDOW_REDISPLAY_SLOT, WINDOW_UPDATE_MODE_LINE_SLOT, set_window_count_slot, set_window_flag,
    window_count_slot, window_flag, window_marker_position,
};
use crate::lisp::types::{Env, Kind, Value};

/// REDISPLAY_SOME_P: redisplay need only consider the windows, buffers
/// and frames marked for it.
pub(super) fn redisplay_some_p(interp: &Interpreter) -> bool {
    matches!(interp.windows_or_buffers_changed, 0 | REDISPLAY_SOME)
        && matches!(interp.update_mode_lines, 0 | REDISPLAY_SOME)
}

/// The buffer WINDOW_ID shows.
pub(super) fn window_buffer(interp: &Interpreter, window_id: u64) -> Option<u64> {
    crate::lisp::primitives::window_buffer_id(interp, &interp.record_value(window_id))
}

/// window_point: the buffer's point for the selected window, else the
/// window's point marker.
pub(super) fn window_point(interp: &Interpreter, window_id: u64) -> Option<usize> {
    if window_id == interp.selected_window_id() {
        let buffer = window_buffer(interp, window_id)?;
        Some(interp.buffer_point_by_id(buffer))
    } else {
        window_marker_position(interp, window_id, WINDOW_POINT_SLOT)
    }
}

/// BUF_MODIFF, BUF_SAVE_MODIFF < BUF_MODIFF and BUF_OVERLAY_MODIFF of
/// BUFFER_ID.
fn buffer_counts(interp: &Interpreter, buffer_id: u64) -> Option<(i64, bool, i64)> {
    let buffer = interp.get_buffer_by_id(buffer_id)?;
    Some((
        buffer.modified_tick(),
        buffer.is_modified(),
        buffer.overlay_modified_tick(),
    ))
}

/// window_outdated: the window's buffer or its overlays changed since
/// the window's last complete redisplay.
pub(super) fn window_outdated(interp: &Interpreter, window_id: u64) -> bool {
    let Some((modiff, _, overlay_modiff)) =
        window_buffer(interp, window_id).and_then(|buffer| buffer_counts(interp, buffer))
    else {
        return true;
    };
    window_count_slot(interp, window_id, WINDOW_LAST_MODIFIED_SLOT).unwrap_or(0) < modiff
        || window_count_slot(interp, window_id, WINDOW_LAST_OVERLAY_MODIFIED_SLOT).unwrap_or(0)
            < overlay_modiff
}

/// window_buffer_changed: whether the buffer was modified, as it was
/// not when the mode line showed it last, or the reverse.
pub(super) fn window_buffer_changed(interp: &Interpreter, window_id: u64) -> bool {
    let modified = window_buffer(interp, window_id)
        .and_then(|buffer| buffer_counts(interp, buffer))
        .is_some_and(|(_, modified, _)| modified);
    modified != window_flag(interp, window_id, WINDOW_LAST_HAD_STAR_SLOT)
}

/// mode_line_update_needed: the window's mode line shows a column (`%c'
/// or `%C') that point's has left.  The current buffer is the window's.
pub(super) fn mode_line_update_needed(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
) -> bool {
    let Some(displayed) = window_count_slot(interp, window_id, WINDOW_COLUMN_NUMBER_DISPLAYED_SLOT)
    else {
        return false;
    };
    let point = interp.buffer.borrow().point();
    let last_point = window_count_slot(interp, window_id, WINDOW_LAST_POINT_SLOT);
    if last_point == Some(point as i64) && !window_outdated(interp, window_id) {
        return false;
    }
    crate::lisp::primitives::current_column(interp, env)
        .is_ok_and(|column| column as i64 != displayed)
}

/// needs_no_redisplay: under REDISPLAY_SOME, a window nothing asked to
/// redisplay (nor its frame, by a face change or its redisplay flag)
/// whose point did not move.
pub(super) fn needs_no_redisplay(interp: &Interpreter, window_id: u64) -> bool {
    if !redisplay_some_p(interp)
        || window_flag(interp, window_id, WINDOW_REDISPLAY_SLOT)
        || window_flag(interp, window_id, WINDOW_UPDATE_MODE_LINE_SLOT)
    {
        return false;
    }
    if interp.window_frame_id(window_id).is_some_and(|frame| {
        let frame = frame.borrow();
        frame.face_change || frame.redisplay
    }) {
        return false;
    }
    let Some(buffer_id) = window_buffer(interp, window_id) else {
        return false;
    };
    if interp
        .get_buffer_by_id(buffer_id)
        .is_none_or(|buffer| buffer.text_redisplay())
    {
        return false;
    }
    window_point(interp, window_id).map(|point| point as i64)
        == window_count_slot(interp, window_id, WINDOW_LAST_POINT_SLOT)
}

/// propagate_buffer_redisplay: each window whose buffer's text must be
/// redisplayed is marked itself, so that the buffers' flags can go.
pub(super) fn propagate_buffer_redisplay(interp: &mut Interpreter, windows: &[u64]) {
    for &window_id in windows {
        if window_buffer(interp, window_id)
            .and_then(|buffer| interp.get_buffer_by_id(buffer).map(|b| b.text_redisplay()))
            .unwrap_or(false)
        {
            set_window_flag(interp, window_id, WINDOW_REDISPLAY_SLOT, true);
        }
    }
}

/// mark_window_display_accurate_1 (W, true): what the window's complete
/// redisplay leaves for the next one to compare against.
pub(super) fn mark_window_display_accurate(interp: &mut Interpreter, window_id: u64) {
    let Some(buffer_id) = window_buffer(interp, window_id) else {
        return;
    };
    let Some((modiff, modified, overlay_modiff)) = buffer_counts(interp, buffer_id) else {
        return;
    };
    set_window_count_slot(interp, window_id, WINDOW_LAST_MODIFIED_SLOT, Some(modiff));
    set_window_count_slot(
        interp,
        window_id,
        WINDOW_LAST_OVERLAY_MODIFIED_SLOT,
        Some(overlay_modiff),
    );
    set_window_flag(interp, window_id, WINDOW_LAST_HAD_STAR_SLOT, modified);
    if let Some(mut buffer) = interp.get_buffer_by_id_mut(buffer_id) {
        buffer.set_clip_changed(false);
        buffer.set_prevent_redisplay_optimizations_p(false);
        buffer.set_text_redisplay(false);
        buffer.take_unchanged_counts();
    }
    let point = window_point(interp, window_id).map(|point| point as i64);
    set_window_count_slot(interp, window_id, WINDOW_LAST_POINT_SLOT, point);
    set_window_flag(interp, window_id, WINDOW_UPDATE_MODE_LINE_SLOT, false);
    set_window_flag(interp, window_id, WINDOW_REDISPLAY_SLOT, false);
}

/// overlay_arrow_string_or_property: VAR's `overlay-arrow-string'
/// property when a string, else `overlay-arrow-string'.
fn overlay_arrow_string(interp: &Interpreter, var: Value) -> Value {
    interp
        .get_property_of_symbol_value(var, "overlay-arrow-string")
        .filter(Value::is_string)
        .or_else(|| interp.lookup_var_key(cached_symbol!("overlay-arrow-string"), &Env::new()))
        .unwrap_or(Value::Nil)
}

/// The symbols of `overlay-arrow-variable-list' with their values.
fn overlay_arrow_variables(interp: &Interpreter) -> Vec<(Value, Option<Value>)> {
    let env = Env::new();
    let mut list = interp
        .lookup_var_key(cached_symbol!("overlay-arrow-variable-list"), &env)
        .unwrap_or(Value::Nil);
    let mut variables = Vec::new();
    while let Some((var, rest)) = list.cons_values() {
        list = rest;
        let Some(symbol) = var.symbol_cell() else {
            continue;
        };
        variables.push((var, interp.lookup_var_symbol(&symbol, &env)));
    }
    variables
}

/// overlay_arrows_changed_p (SET_REDISPLAY true): the buffers of the
/// overlay arrows whose position or string differs from what was last
/// displayed are marked for redisplay.
pub(super) fn overlay_arrows_changed(interp: &mut Interpreter) -> bool {
    let mut changed = false;
    for (var, value) in overlay_arrow_variables(interp) {
        let Some(Kind::Marker(marker)) = value.map(|value| value.kind()) else {
            continue;
        };
        let position = interp
            .marker_position(marker)
            .map_or(Value::Nil, |pos| Value::Integer(pos as i64));
        let last_position =
            crate::lisp::primitives::symbol_get(interp, var, cached_symbol!("last-arrow-position"));
        let string = overlay_arrow_string(interp, var);
        let last_string =
            crate::lisp::primitives::symbol_get(interp, var, cached_symbol!("last-arrow-string"));
        if !crate::lisp::types::eq(position, last_position)
            || !crate::lisp::types::eq(string, last_string)
        {
            if let Some(buffer_id) = interp.marker_buffer_id(marker) {
                interp.bset_redisplay(buffer_id);
            }
            changed = true;
        }
    }
    changed
}

/// update_overlay_arrows: with UP_TO_DATE above 0, each arrow's position
/// and string recorded as displayed; else t recorded, the arrows to be
/// drawn afresh (below 0 every variable's, at 0 those recorded).
pub(super) fn update_overlay_arrows(interp: &mut Interpreter, up_to_date: i32) {
    for (var, value) in overlay_arrow_variables(interp) {
        if up_to_date > 0 {
            let Some(Kind::Marker(marker)) = value.map(|value| value.kind()) else {
                continue;
            };
            let position = interp
                .marker_position(marker)
                .map_or(Value::Nil, |pos| Value::Integer(pos as i64));
            let string = overlay_arrow_string(interp, var);
            crate::lisp::primitives::symbol_put(
                interp,
                var,
                cached_symbol!("last-arrow-position"),
                position,
            );
            crate::lisp::primitives::symbol_put(
                interp,
                var,
                cached_symbol!("last-arrow-string"),
                string,
            );
        } else if up_to_date < 0
            || !crate::lisp::primitives::symbol_get(
                interp,
                var,
                cached_symbol!("last-arrow-position"),
            )
            .is_nil()
        {
            for property in [
                cached_symbol!("last-arrow-position"),
                cached_symbol!("last-arrow-string"),
            ] {
                crate::lisp::primitives::symbol_put(interp, var, property, Value::T);
            }
        }
    }
}

/// redisplay_internal's AINC: CAUSE counted in the hash table VARIABLE
/// holds (`redisplay--all-windows-cause', `redisplay--mode-lines-cause'):
/// Fgethash with 0 for a missing entry, then Fputhash of one more when
/// the entry is a fixnum.
pub(super) fn count_redisplay_cause(
    interp: &mut Interpreter,
    env: &mut Env,
    variable: crate::lisp::types::symbols::SymbolKey,
    cause: i32,
) {
    let Some(Kind::HashTable(table)) = interp.lookup_var_key(variable, env).map(|v| v.kind())
    else {
        return;
    };
    let key = Value::Integer(i64::from(cause));
    let Ok((_, slot)) = crate::lisp::primitives::hash_table_lookup(interp, table, &key, env) else {
        return;
    };
    let entry = slot
        .and_then(|slot| table.entry(slot))
        .map_or(Value::Integer(0), |(_, value)| value);
    if let Kind::Integer(count) = entry.kind() {
        let _ = crate::lisp::primitives::hash_table_put(
            interp,
            table,
            key,
            Value::Integer(count + 1),
            env,
        );
    }
}
