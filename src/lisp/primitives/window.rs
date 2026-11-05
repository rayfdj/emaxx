use super::{prefix_numeric_value, signal_condition};
use crate::lisp::eval::Interpreter;
use crate::lisp::types::{Env, Kind, LispError, Value};

/// Live geometry of the selected window in an interactive session,
/// published by the terminal frontend after each redisplay.  Batch
/// sessions have none and keep GNU's batch answers (window-end = ZV).
#[derive(Clone, Copy)]
pub(crate) struct InteractiveWindowMetrics {
    /// Text rows of the selected window (frame minus mode line and echo
    /// area).
    pub text_height: usize,
    /// Position just past the last displayed character, GNU's window-end.
    pub window_end: usize,
}

thread_local! {
    static INTERACTIVE_WINDOW_METRICS: std::cell::Cell<Option<InteractiveWindowMetrics>> =
        const { std::cell::Cell::new(None) };
}

pub(crate) fn set_interactive_window_metrics(metrics: Option<InteractiveWindowMetrics>) {
    INTERACTIVE_WINDOW_METRICS.with(|cell| cell.set(metrics));
}

pub(crate) fn interactive_window_metrics() -> Option<InteractiveWindowMetrics> {
    INTERACTIVE_WINDOW_METRICS.with(|cell| cell.get())
}

pub(crate) const WINDOW_BUFFER_SLOT: usize = 0;
pub(crate) const WINDOW_START_SLOT: usize = 1;
pub(crate) const WINDOW_OLD_POINT_SLOT: usize = 2;
pub(crate) const WINDOW_KIND_SLOT: usize = 3;
pub(crate) const WINDOW_PARAMETERS_SLOT: usize = 4;
pub(crate) const WINDOW_PREV_BUFFERS_SLOT: usize = 5;
pub(crate) const WINDOW_NEXT_BUFFERS_SLOT: usize = 6;
pub(crate) const WINDOW_DEDICATED_SLOT: usize = 7;
pub(crate) const WINDOW_NEW_PIXEL_SLOT: usize = 8;
pub(crate) const WINDOW_NEW_TOTAL_SLOT: usize = 9;
pub(crate) const WINDOW_NEW_NORMAL_SLOT: usize = 10;
pub(crate) const WINDOW_DISPLAY_TABLE_SLOT: usize = 11;
pub(crate) const WINDOW_CURSOR_TYPE_SLOT: usize = 12;
pub(crate) const WINDOW_PARENT_SLOT: usize = 13;
pub(crate) const WINDOW_PREV_SIBLING_SLOT: usize = 14;
pub(crate) const WINDOW_NEXT_SIBLING_SLOT: usize = 15;
pub(crate) const WINDOW_FIRST_CHILD_SLOT: usize = 16;
pub(crate) const WINDOW_PIXEL_WIDTH_SLOT: usize = 17;
pub(crate) const WINDOW_PIXEL_HEIGHT_SLOT: usize = 18;
pub(crate) const WINDOW_PIXEL_LEFT_SLOT: usize = 19;
pub(crate) const WINDOW_PIXEL_TOP_SLOT: usize = 20;
pub(crate) const WINDOW_NORMAL_WIDTH_SLOT: usize = 21;
pub(crate) const WINDOW_NORMAL_HEIGHT_SLOT: usize = 22;
pub(crate) const WINDOW_COMBINATION_LIMIT_SLOT: usize = 23;
pub(crate) const WINDOW_USE_TIME_SLOT: usize = 24;
pub(crate) const WINDOW_HSCROLL_SLOT: usize = 25;
pub(crate) const WINDOW_MIN_HSCROLL_SLOT: usize = 26;
pub(crate) const WINDOW_SUSPEND_AUTO_HSCROLL_SLOT: usize = 27;
pub(crate) const WINDOW_OLD_BUFFER_SLOT: usize = 28;
pub(crate) const WINDOW_POINT_SLOT: usize = 29;
pub(crate) const WINDOW_FRAME_SLOT: usize = 30;
/// window.c's change_stamp and old pixel and body sizes: the state
/// window_change_record_windows recorded when window change functions
/// last ran (absent, read as 0, for a window created since).
pub(crate) const WINDOW_CHANGE_STAMP_SLOT: usize = 31;
pub(crate) const WINDOW_OLD_PIXEL_WIDTH_SLOT: usize = 32;
pub(crate) const WINDOW_OLD_PIXEL_HEIGHT_SLOT: usize = 33;
pub(crate) const WINDOW_OLD_BODY_PIXEL_WIDTH_SLOT: usize = 34;
pub(crate) const WINDOW_OLD_BODY_PIXEL_HEIGHT_SLOT: usize = 35;
/// window.h's base_line_number and base_line_pos for the mode line's
/// %l, with the buffer and the state they were counted under (absent,
/// read as no cache, for a window that has not shown a line number).
pub(crate) const WINDOW_BASE_LINE_SLOT: usize = 36;
/// window.h's window_end_pos: Z minus the end of the text the window's
/// last redisplay showed (absent before one).
pub(crate) const WINDOW_END_POS_SLOT: usize = 37;
/// window.h's force_start, optional_new_start and start_at_line_beg:
/// how redisplay_window treats the window's start (absent, read as
/// false, until a command or redisplay sets them).
pub(crate) const WINDOW_FORCE_START_SLOT: usize = 38;
pub(crate) const WINDOW_OPTIONAL_NEW_START_SLOT: usize = 39;
pub(crate) const WINDOW_START_AT_LINE_BEG_SLOT: usize = 40;
/// window.h's left_col, top_line, total_cols and total_lines: the
/// window's edges and sizes in frame columns and lines.  window.c keeps
/// them apart from the pixel geometry and assigns them on its own
/// schedule (window_resize_apply, window-resize-apply-total,
/// resize_frame_windows), so the two can differ: a batch frame's root
/// keeps pixel_top 0 while window-resize-apply-total puts its top_line
/// below the menu bar.  A text frame's character cell is one pixel.
pub(crate) const WINDOW_LEFT_COL_SLOT: usize = 41;
pub(crate) const WINDOW_TOP_LINE_SLOT: usize = 42;
pub(crate) const WINDOW_TOTAL_COLS_SLOT: usize = 43;
pub(crate) const WINDOW_TOTAL_LINES_SLOT: usize = 44;
/// window.h's left_margin_cols and right_margin_cols (nil for none,
/// GNU's 0).
pub(crate) const WINDOW_LEFT_MARGIN_COLS_SLOT: usize = 45;
pub(crate) const WINDOW_RIGHT_MARGIN_COLS_SLOT: usize = 46;
const WINDOW_SLOT_COUNT: usize = 47;

/// WINDOW_ID's character geometry as (total_cols, total_lines, left_col,
/// top_line), in window_geometry's order.  A record from before these
/// slots existed reads its pixel geometry, which it then still equals.
pub(crate) fn window_char_geometry(interp: &Interpreter, window_id: u64) -> (i64, i64, i64, i64) {
    let record = interp.find_record(window_id);
    let slot = |slot: usize, fallback: usize| {
        record
            .and_then(|record| {
                record
                    .slots
                    .get(slot)
                    .filter(|value| !value.is_nil())
                    .or_else(|| record.slots.get(fallback))
            })
            .and_then(Value::integer_value)
            .unwrap_or(0)
    };
    (
        slot(WINDOW_TOTAL_COLS_SLOT, WINDOW_PIXEL_WIDTH_SLOT),
        slot(WINDOW_TOTAL_LINES_SLOT, WINDOW_PIXEL_HEIGHT_SLOT),
        slot(WINDOW_LEFT_COL_SLOT, WINDOW_PIXEL_LEFT_SLOT),
        slot(WINDOW_TOP_LINE_SLOT, WINDOW_PIXEL_TOP_SLOT),
    )
}

/// Store one of WINDOW_ID's character-geometry slots (above).
pub(crate) fn set_window_char_slot(
    interp: &mut Interpreter,
    window_id: u64,
    slot: usize,
    value: i64,
) {
    if let Some(record) = interp.find_record_mut(window_id) {
        if record.slots.len() <= slot {
            record.slots.resize(slot + 1, Value::Nil);
        }
        record.slots[slot] = Value::Integer(value);
    }
}

/// WINDOW_ID's left_margin_cols and right_margin_cols (None for 0).
pub(crate) fn window_margin_cols(
    interp: &Interpreter,
    window_id: u64,
) -> (Option<i64>, Option<i64>) {
    let record = interp.find_record(window_id);
    let slot = |slot: usize| {
        record
            .and_then(|record| record.slots.get(slot))
            .and_then(Value::integer_value)
            .filter(|cols| *cols != 0)
    };
    (
        slot(WINDOW_LEFT_MARGIN_COLS_SLOT),
        slot(WINDOW_RIGHT_MARGIN_COLS_SLOT),
    )
}

pub(crate) fn set_window_margin_cols(
    interp: &mut Interpreter,
    window_id: u64,
    left: Option<i64>,
    right: Option<i64>,
) {
    if let Some(record) = interp.find_record_mut(window_id) {
        if record.slots.len() <= WINDOW_RIGHT_MARGIN_COLS_SLOT {
            record
                .slots
                .resize(WINDOW_RIGHT_MARGIN_COLS_SLOT + 1, Value::Nil);
        }
        let value = |cols: Option<i64>| {
            cols.filter(|cols| *cols != 0)
                .map(Value::Integer)
                .unwrap_or(Value::Nil)
        };
        record.slots[WINDOW_LEFT_MARGIN_COLS_SLOT] = value(left);
        record.slots[WINDOW_RIGHT_MARGIN_COLS_SLOT] = value(right);
    }
}

/// One of WINDOW_ID's redisplay flags (the slots above).
pub(crate) fn window_flag(interp: &Interpreter, window_id: u64, slot: usize) -> bool {
    interp
        .find_record(window_id)
        .and_then(|record| record.slots.get(slot))
        .is_some_and(Value::is_truthy)
}

pub(crate) fn set_window_flag(interp: &mut Interpreter, window_id: u64, slot: usize, on: bool) {
    if let Some(record) = interp.find_record_mut(window_id) {
        if record.slots.len() <= slot {
            record.slots.resize(slot + 1, Value::Nil);
        }
        record.slots[slot] = if on { Value::T } else { Value::Nil };
    }
}

/// window.h's w->start, w->old_pointm and w->pointm: markers on the
/// chain of the buffer the window shows, so that insdel.c's marker
/// adjustment moves them with its text.  A leaf window holds one in each
/// slot from its creation; an internal window holds nil.
pub(crate) const WINDOW_MARKER_SLOTS: [usize; 3] =
    [WINDOW_START_SLOT, WINDOW_OLD_POINT_SLOT, WINDOW_POINT_SLOT];

/// The marker in SLOT of WINDOW_ID (nil in an internal window's).
pub(crate) fn window_marker(
    interp: &Interpreter,
    window_id: u64,
    slot: usize,
) -> Option<crate::lisp::types::MarkerRef> {
    match interp.find_record(window_id)?.slots.get(slot)?.kind() {
        Kind::Marker(marker) => Some(marker),
        _ => None,
    }
}

/// marker_position of SLOT's marker: None when it points nowhere.
pub(crate) fn window_marker_position(
    interp: &Interpreter,
    window_id: u64,
    slot: usize,
) -> Option<usize> {
    window_marker(interp, window_id, slot)?.position()
}

/// The marker in SLOT of WINDOW_ID, made when the slot holds none (as
/// make_window gives a window its three).
fn window_marker_for_set(
    interp: &mut Interpreter,
    window_id: u64,
    slot: usize,
) -> Option<crate::lisp::types::MarkerRef> {
    if let Some(marker) = window_marker(interp, window_id, slot) {
        return Some(marker);
    }
    let record = interp.find_record_mut(window_id)?;
    if record.slots.len() <= slot {
        record.slots.resize(slot + 1, Value::Nil);
    }
    let marker = crate::lisp::types::MarkerRef::new();
    record.slots[slot] = Value::Marker(marker);
    Some(marker)
}

/// marker.c:set_marker_both on SLOT's marker: CHARPOS of BUFFER_ID,
/// inside [BEG, Z].
pub(crate) fn set_window_marker(
    interp: &mut Interpreter,
    window_id: u64,
    slot: usize,
    buffer_id: u64,
    charpos: usize,
) {
    if let Some(marker) = window_marker_for_set(interp, window_id, slot) {
        let _ = interp.set_marker(marker, Some(charpos), Some(buffer_id));
    }
}

/// marker.c:set_marker_restricted on SLOT's marker: CHARPOS of
/// BUFFER_ID, inside its accessible portion.
pub(crate) fn set_window_marker_restricted(
    interp: &mut Interpreter,
    window_id: u64,
    slot: usize,
    buffer_id: u64,
    charpos: usize,
) {
    let (begv, zv) = buffer_point_bounds(interp, buffer_id);
    set_window_marker(interp, window_id, slot, buffer_id, charpos.clamp(begv, zv));
}

/// marker.c:unchain_marker on WINDOW_ID's three markers, as
/// delete_window and delete_all_child_windows do: they point nowhere.
pub(crate) fn unchain_window_markers(interp: &Interpreter, window_id: u64) {
    for slot in WINDOW_MARKER_SLOTS {
        if let Some(marker) = window_marker(interp, window_id, slot) {
            marker.detach();
        }
    }
}

/// set_window_buffer's insertion type for WINDOW_ID's point markers: the
/// value of `window-point-insertion-type' in BUFFER_ID.
pub(crate) fn set_window_point_insertion_type(
    interp: &Interpreter,
    window_id: u64,
    buffer_id: u64,
) {
    let insertion_type = window_point_insertion_type(interp, buffer_id);
    for slot in [WINDOW_POINT_SLOT, WINDOW_OLD_POINT_SLOT] {
        if let Some(marker) = window_marker(interp, window_id, slot) {
            marker.set_insertion_type(insertion_type);
        }
    }
}

/// `window-point-insertion-type' as BUFFER_ID sees it.
pub(crate) fn window_point_insertion_type(interp: &Interpreter, buffer_id: u64) -> bool {
    interp
        .buffer_local_value_key(buffer_id, cached_symbol!("window-point-insertion-type"))
        .or_else(|| interp.global_binding_value("window-point-insertion-type"))
        .is_some_and(|value| value.is_truthy())
}

/// A saved slot's position: its marker's (a window configuration's
/// copies), None when that points nowhere.
pub(crate) fn slot_marker_position(value: Option<&Value>) -> Option<usize> {
    match value?.kind() {
        Kind::Marker(marker) => marker.position(),
        _ => None,
    }
}
/// What a window record is, held in its kind slot: what GNU's struct
/// window tells apart by w->contents (a buffer for a leaf, the first
/// child for an internal window, nil once deleted), w->horizontal and
/// w->mini.  The slot holds nil for a leaf and a fixnum code otherwise, so
/// no symbol stands for a kind and a test of one is a word compare.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum WindowKind {
    Leaf,
    Minibuffer,
    InternalHorizontal,
    InternalVertical,
    Deleted,
}

impl WindowKind {
    /// The kind slot's value for this kind.
    pub(crate) fn value(self) -> Value {
        match self {
            WindowKind::Leaf => Value::Nil,
            WindowKind::Minibuffer => Value::Integer(1),
            WindowKind::InternalHorizontal => Value::Integer(2),
            WindowKind::InternalVertical => Value::Integer(3),
            WindowKind::Deleted => Value::Integer(4),
        }
    }

    /// The kind a kind slot's value holds; a missing slot is a leaf's.
    #[inline]
    pub(crate) fn of_slot(slot: Option<&Value>) -> WindowKind {
        match slot.and_then(Value::integer_value) {
            Some(1) => WindowKind::Minibuffer,
            Some(2) => WindowKind::InternalHorizontal,
            Some(3) => WindowKind::InternalVertical,
            Some(4) => WindowKind::Deleted,
            _ => WindowKind::Leaf,
        }
    }

    /// The kind of WINDOW's record.
    #[inline]
    pub(crate) fn of(window: crate::lisp::types::RecordRef) -> WindowKind {
        WindowKind::of_slot(window.slots.get(WINDOW_KIND_SLOT))
    }

    /// An internal window's (window_resize_check's !NILP (w->contents)
    /// && !BUFFERP).
    pub(crate) fn is_internal(self) -> bool {
        matches!(
            self,
            WindowKind::InternalHorizontal | WindowKind::InternalVertical
        )
    }
}

/// make_window's slots for a leaf window showing BUFFER_ID: its start,
/// point and old point markers at START of that buffer, the point
/// markers of the insertion type set_window_buffer gives them.
pub(crate) fn window_record_slots(
    interp: &Interpreter,
    buffer_id: Option<u64>,
    start: usize,
    kind: Value,
    geometry: (i64, i64, i64, i64),
) -> Vec<Value> {
    let (width, height, left, top) = geometry;
    let marker = |point: bool| {
        let marker = crate::lisp::types::MarkerRef::new();
        if let Some(buffer_id) = buffer_id
            && let Some(buffer) = interp.buffer_object(buffer_id)
        {
            let state = buffer.borrow();
            let position = start.clamp(1, state.size_total() + 1);
            marker.attach(buffer, position, state.marker_byte_position(position));
            marker.set_insertion_type(point && window_point_insertion_type(interp, buffer_id));
        }
        Value::Marker(marker)
    };
    let mut slots = vec![
        buffer_id
            .map(|id| Value::Integer(id as i64))
            .unwrap_or(Value::Nil),
        marker(false),
        marker(true),
        kind,
        Value::Nil,
        Value::Nil,
        Value::Nil,
        Value::Nil,
        Value::Integer(0),
        Value::Integer(0),
        Value::Integer(0),
        Value::Nil,
        Value::T,
        Value::Nil,
        Value::Nil,
        Value::Nil,
        Value::Nil,
        Value::Integer(width),
        Value::Integer(height),
        Value::Integer(left),
        Value::Integer(top),
        Value::float(1.0),
        Value::float(1.0),
        Value::Nil,
        Value::Integer(0),
        Value::Integer(0),
        Value::Integer(0),
        Value::Nil,
        Value::Nil,
        marker(true),
    ];
    slots.resize(WINDOW_SLOT_COUNT, Value::Nil);
    for (slot, value) in [
        (WINDOW_TOTAL_COLS_SLOT, width),
        (WINDOW_TOTAL_LINES_SLOT, height),
        (WINDOW_LEFT_COL_SLOT, left),
        (WINDOW_TOP_LINE_SLOT, top),
    ] {
        slots[slot] = Value::Integer(value);
    }
    slots
}

pub(crate) fn current_window_start(interp: &Interpreter) -> usize {
    interp.selected_window_start()
}

pub(crate) fn buffer_point_bounds(interp: &Interpreter, buffer_id: u64) -> (usize, usize) {
    interp.buffer_bounds_by_id(buffer_id).unwrap_or((
        interp.buffer.borrow().point_min(),
        interp.buffer.borrow().point_max(),
    ))
}

/// decode_any_window's WINDOWP: the window record VALUE is, live,
/// internal or deleted.
#[inline]
pub(crate) fn window_record(value: &Value) -> Option<crate::lisp::types::RecordRef> {
    match value.kind() {
        Kind::Record(record) if record.kind == crate::lisp::eval::RecordKind::Window => {
            Some(record)
        }
        _ => None,
    }
}

pub(crate) fn window_record_id_from_value(_interp: &Interpreter, value: &Value) -> Option<u64> {
    window_record(value).map(|record| record.id)
}

/// w->contents of a window record's buffer slot, BUFFERP: the buffer
/// object a leaf window shows (an id where a test built the record by
/// hand), whether or not it is live.
pub(crate) fn window_slot_buffer_id(slot: Option<&Value>) -> Option<u64> {
    match slot?.kind() {
        Kind::Buffer(buffer) => Some(buffer.id),
        Kind::Integer(id) => Some(id.max(0) as u64),
        _ => None,
    }
}

/// The live buffer the window VALUE shows (BUFFERP (w->contents)), read
/// from the record itself.
pub(crate) fn window_buffer_id(interp: &Interpreter, value: &Value) -> Option<u64> {
    let window = window_record(value)?;
    match window.slots.get(WINDOW_BUFFER_SLOT).map(|slot| slot.kind()) {
        // BUFFER_LIVE_P of the object itself.
        Some(Kind::Buffer(buffer)) => buffer.is_live().then_some(buffer.id),
        Some(Kind::Integer(id)) => {
            let id = id.max(0) as u64;
            interp.has_buffer_id(id).then_some(id)
        }
        // w->contents nil: a deleted window, even the selected one.
        _ => None,
    }
}

pub(crate) fn window_start(
    interp: &Interpreter,
    value: Option<&Value>,
) -> Result<usize, LispError> {
    match value.map(|v| v.kind()) {
        None | Some(Kind::Nil) => Ok(current_window_start(interp)),
        Some(value) => {
            let Some(id) = window_record_id_from_value(interp, &value.value()) else {
                return Err(LispError::WrongTypeArgument(
                    "windowp".into(),
                    value.value(),
                ));
            };
            if id == interp.selected_window_id() {
                return Ok(current_window_start(interp));
            }
            let buffer_id =
                window_buffer_id(interp, &value.value()).unwrap_or(interp.current_buffer_id());
            Ok(window_start_marker(interp, id, buffer_id))
        }
    }
}

/// WINDOW_ID's start marker position (Fmarker_position of w->start): a
/// marker stays inside [BEG, Z] of BUFFER_ID but not inside a narrowing
/// made after it was set.
pub(crate) fn window_start_marker(interp: &Interpreter, window_id: u64, buffer_id: u64) -> usize {
    if window_id == interp.selected_window_id() {
        return interp.selected_window_start();
    }
    window_marker_position(interp, window_id, WINDOW_START_SLOT)
        .unwrap_or_else(|| buffer_point_bounds(interp, buffer_id).0)
}

/// BUFFER_ID's Z: one past its last character, narrowing ignored.
pub(crate) fn buffer_z(interp: &Interpreter, buffer_id: u64) -> usize {
    if buffer_id == interp.current_buffer_id() {
        interp.buffer.borrow().size_total() + 1
    } else {
        interp
            .get_buffer_by_id(buffer_id)
            .map_or(1, |buffer| buffer.size_total() + 1)
    }
}

/// Store START (clamped to the window buffer's accessible region, as
/// set_marker_restricted clamps) as WINDOW_ID's start marker.
pub(crate) fn set_window_start_marker(interp: &mut Interpreter, window_id: u64, start: usize) {
    let buffer_id = window_buffer_id(interp, &interp.record_value(window_id))
        .unwrap_or(interp.current_buffer_id());
    let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
    let start = start.clamp(point_min, point_max);
    set_window_marker(interp, window_id, WINDOW_START_SLOT, buffer_id, start);
}

/// What a redisplay of WINDOW_ID showed: its point (the window's own,
/// for a window other than the selected one, as redisplay clamped or
/// moved it) and window_end_pos, Z less the end of the text it showed.
pub(crate) fn record_window_display(
    interp: &mut Interpreter,
    window_id: u64,
    buffer_id: u64,
    point: usize,
    window_end: usize,
) {
    let z = buffer_z(interp, buffer_id);
    set_window_marker(interp, window_id, WINDOW_POINT_SLOT, buffer_id, point);
    if let Some(record) = interp.find_record_mut(window_id) {
        if record.slots.len() <= WINDOW_END_POS_SLOT {
            record.slots.resize(WINDOW_END_POS_SLOT + 1, Value::Nil);
        }
        record.slots[WINDOW_END_POS_SLOT] = Value::Integer(z as i64 - window_end.min(z) as i64);
    }
}

/// window.c Fset_window_start: the exact position, clamped to the
/// accessible region; redisplay must show it (force_start) unless
/// NOFORCE, and treats it as not known to begin a line.
pub(crate) fn set_window_start_value(
    interp: &mut Interpreter,
    window: &Value,
    start: usize,
    noforce: bool,
) -> Result<(), LispError> {
    let window = if window.is_nil() {
        interp.selected_window_value()
    } else {
        *window
    };
    let Some(id) = window_record_id_from_value(interp, &window) else {
        return Err(LispError::WrongTypeArgument("windowp".into(), window));
    };
    set_window_start_marker(interp, id, start);
    set_window_flag(interp, id, WINDOW_START_AT_LINE_BEG_SLOT, false);
    if !noforce {
        set_window_flag(interp, id, WINDOW_FORCE_START_SLOT, true);
    }
    interp.wset_redisplay(id);
    Ok(())
}

/// window.c window_scroll_margin in lines for a window WINDOW_LINES
/// text lines tall: `scroll-margin', at most `maximum-scroll-margin'
/// (a float, 0.25 otherwise, within [0, 0.5]) of the window and less
/// than half of it.
pub(crate) fn scroll_margin_lines(
    scroll_margin: i64,
    maximum: Value,
    window_lines: usize,
) -> usize {
    if scroll_margin <= 0 {
        return 0;
    }
    let ratio = match maximum.kind() {
        Kind::Float(ratio) => ratio.get().clamp(0.0, 0.5),
        _ => 0.25,
    };
    let lines = window_lines as i64;
    let max_margin = ((lines - 1) / 2).min((lines as f64 * ratio) as i64);
    scroll_margin.min(max_margin).max(0) as usize
}

/// The selected window's scroll margin in lines, for commands.
pub(crate) fn selected_scroll_margin(
    interp: &Interpreter,
    env: &Env,
    window_lines: usize,
) -> usize {
    let scroll_margin = interp
        .lookup_var_key(cached_symbol!("scroll-margin"), env)
        .and_then(|value| value.integer_value())
        .unwrap_or(0);
    let maximum = interp
        .lookup_var_key(cached_symbol!("maximum-scroll-margin"), env)
        .unwrap_or(Value::Nil);
    scroll_margin_lines(scroll_margin, maximum, window_lines)
}

/// lisp.h clip_to_bounds: NUM, or LOWER below it, or UPPER above it.
pub(crate) fn clip_to_bounds(lower: i64, num: i64, upper: i64) -> i64 {
    if num < lower {
        lower
    } else if num <= upper {
        num
    } else {
        upper
    }
}

/// window.c Frecenter on the selected window, whose buffer is current.
pub(crate) fn recenter_selected_window(
    interp: &mut Interpreter,
    env: &mut Env,
    arg: Value,
    redisplay: bool,
    text_height: usize,
    interactive: bool,
) -> Result<(), LispError> {
    let window_id = interp.selected_window_id();
    if interp.selected_window_buffer_id() != interp.current_buffer_id() {
        return Err(crate::lisp::primitives::values::doprnt_error(
            interp,
            "`recenter'ing a window that does not display current-buffer.",
            &[],
        ));
    }
    let mut center = false;
    let mut iarg = 0i64;
    if arg.is_nil() {
        // REDISPLAY with `recenter-redisplay' redraws a garbaged frame,
        // which redisplay then considers whole.
        if redisplay
            && interp
                .lookup_var_key(cached_symbol!("recenter-redisplay"), env)
                .is_some_and(|value| value.is_truthy())
        {
            interp.redisplay_other_windows();
        }
        center = true;
    } else if matches!(arg.kind(), Kind::Cons(_)) {
        center = true;
    } else {
        iarg = prefix_numeric_value(&arg)?.as_integer()?;
    }
    let ht = text_height.max(1) as i64;
    let margin = selected_scroll_margin(interp, env, text_height) as i64;
    let point = interp.buffer.borrow().point();
    let start = if interactive && !center && iarg >= 0 {
        // The display code: the start of point's screen line, then
        // move_it_by_lines ARG lines back, which reseats at a line start.
        let nlines = clip_to_bounds(margin, iarg, ht - margin - 1).max(0);
        let row = super::move_it::move_it_by_lines(interp, env, window_id, point, 0).0;
        if nlines > 0 {
            super::move_it::move_it_by_lines(interp, env, window_id, row, -nlines).0
        } else {
            row
        }
    } else if interactive {
        // move_it_vertically_backward: whole screen lines above point's
        // row, no reseat.
        let lines_back = if center {
            ht / 2
        } else {
            let nlines = clip_to_bounds(margin + 1, -iarg, ht - margin);
            // Point's row ends NLINES from the bottom, empty lines past
            // the buffer's end counted in.
            let h = ht - nlines;
            if h <= 0 {
                return Ok(());
            }
            h
        };
        move_screen_lines(interp, env, point, -(lines_back as isize)).0
    } else {
        // An initial frame: vmotion with the margins applied to every arg.
        let mut line = if center {
            ht / 2
        } else if iarg < 0 {
            iarg + ht
        } else {
            iarg
        };
        line = clip_to_bounds(margin, line, ht - margin - 1);
        move_screen_lines(interp, env, point, -(line as isize)).0
    };
    interp.set_selected_window_start(start);
    set_window_flag(interp, window_id, WINDOW_OPTIONAL_NEW_START_SLOT, true);
    let at_line_beg = {
        let buffer = interp.buffer.borrow();
        start <= buffer.point_min() || buffer.char_at(start - 1) == Some('\n')
    };
    set_window_flag(
        interp,
        window_id,
        WINDOW_START_AT_LINE_BEG_SLOT,
        at_line_beg,
    );
    interp.wset_redisplay(window_id);
    Ok(())
}

/// Step COUNT screen lines from FROM (GNU's vmotion): continuation rows of
/// wrapped lines count individually, exactly as the display walks them.
/// Answers the landing screen-line start and the unmet distance.
pub(crate) fn move_screen_lines(
    interp: &mut Interpreter,
    env: &mut Env,
    from: usize,
    count: isize,
) -> (usize, isize) {
    let window_id = interp.selected_window_id();
    move_screen_lines_in(interp, env, window_id, from, count)
}

/// `move_screen_lines' under WINDOW_ID's geometry, in the current buffer
/// (which the caller makes WINDOW_ID's).
pub(crate) fn move_screen_lines_in(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    from: usize,
    count: isize,
) -> (usize, isize) {
    use crate::lisp::primitives::dispatch::{row_index_of, screen_row_starts, visual_line_bounds};
    let point_min = interp.buffer.borrow().point_min();
    let point_max = interp.buffer.borrow().point_max();
    let from = from.clamp(point_min, point_max);
    let (mut bol, mut eol) = visual_line_bounds(interp, from);
    let mut starts = screen_row_starts(interp, env, window_id, bol, eol);
    let mut index = row_index_of(&starts, from);
    let mut remaining = count;
    while remaining > 0 {
        if index + 1 < starts.len() {
            index += 1;
        } else if eol < point_max {
            let bounds = visual_line_bounds(interp, eol + 1);
            bol = bounds.0;
            eol = bounds.1;
            starts = screen_row_starts(interp, env, window_id, bol, eol);
            index = 0;
        } else {
            break;
        }
        remaining -= 1;
    }
    while remaining < 0 {
        if index > 0 {
            index -= 1;
        } else if bol > point_min {
            let bounds = visual_line_bounds(interp, bol - 1);
            bol = bounds.0;
            eol = bounds.1;
            starts = screen_row_starts(interp, env, window_id, bol, eol);
            index = starts.len().saturating_sub(1);
        } else {
            break;
        }
        remaining += 1;
    }
    (starts[index], remaining)
}

thread_local! {
    /// window.c's window_scroll_preserve_hpos and window_scroll_preserve_vpos:
    /// where point sat in the window when a run of scroll commands began.
    static SCROLL_PRESERVE: std::cell::Cell<(i64, i64)> = const { std::cell::Cell::new((0, 0)) };
}

/// Screen rows from START down to POS's row, and POS's column on that
/// row (compute_motion from the window start to POS).
fn motion_from_start(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    start: usize,
    pos: usize,
    limit: usize,
) -> (i64, i64) {
    let mut rows = 0i64;
    let mut cursor = start;
    while (rows as usize) < limit && pos >= cursor {
        let (next, remaining) = move_screen_lines_in(interp, env, window_id, cursor, 1);
        if remaining != 0 || next <= cursor || pos < next {
            break;
        }
        cursor = next;
        rows += 1;
    }
    let column_at = |interp: &mut Interpreter, env: &mut Env, at: usize| {
        let saved = interp.buffer.borrow().point();
        interp.buffer.borrow_mut().goto_char(at);
        let column = super::dispatch::call(interp, "current-column", &[], env)
            .ok()
            .and_then(|value| value.integer_value())
            .unwrap_or(0);
        interp.buffer.borrow_mut().goto_char(saved);
        column
    };
    let row_start = move_screen_lines_in(interp, env, window_id, pos, 0).0;
    let hpos = column_at(interp, env, pos) - column_at(interp, env, row_start);
    (rows, hpos.max(0))
}

/// window.c window_scroll_line_based on the selected window, whose
/// buffer is current: scroll N lines (N screenfuls less
/// `next-screen-context-lines' when WHOLE), then keep point out of the
/// scroll margins or, under `scroll-preserve-screen-position', on the
/// screen line it occupied.  TEXT_HEIGHT is window_internal_height and
/// TOTAL_LINES the window's total height.
#[allow(clippy::too_many_arguments)]
pub(crate) fn window_scroll(
    interp: &mut Interpreter,
    env: &mut Env,
    n: i64,
    whole: bool,
    noerror: bool,
    text_height: usize,
    total_lines: i64,
    hscroll: i64,
) -> Result<(), LispError> {
    use super::dispatch::visual_vertical_motion;
    let window_id = interp.selected_window_id();
    let window = interp.record_value(window_id);
    let opoint = interp.buffer.borrow().point();
    let ht = text_height.max(1) as i64;
    let mut startpos = interp.selected_window_start();
    let adjust_old_pointm =
        window_marker_position(interp, window_id, WINDOW_OLD_POINT_SLOT) == Some(opoint);
    let mut n = n;
    if whole {
        let nscls = interp
            .lookup_var_key(cached_symbol!("next-screen-context-lines"), env)
            .and_then(|value| value.integer_value())
            .unwrap_or(2)
            .clamp(0, 1_000_000);
        n = n.saturating_mul((ht - nscls).max(1));
    }
    let preserve = interp
        .lookup_var_key(cached_symbol!("scroll-preserve-screen-position"), env)
        .unwrap_or(Value::Nil);
    let mut original = (0i64, 0i64);
    if preserve.is_truthy() {
        let (hpos, vpos) = SCROLL_PRESERVE.with(|cell| cell.get());
        let last_command = interp
            .lookup_var_key(cached_symbol!("last-command"), env)
            .unwrap_or(Value::Nil);
        let scroll_command = matches!(last_command.kind(), Kind::Symbol(_) | Kind::Nil | Kind::T)
            && super::dispatch::call(
                interp,
                "get",
                &[last_command, Value::symbol("scroll-command")],
                env,
            )
            .is_ok_and(|value| value.is_truthy());
        original = if vpos <= 0 || !scroll_command {
            let (vpos, hpos) =
                motion_from_start(interp, env, window_id, startpos, opoint, text_height);
            let saved = (hpos + hscroll, vpos);
            SCROLL_PRESERVE.with(|cell| cell.set(saved));
            saved
        } else {
            (hpos, vpos)
        };
    }
    let visible = super::dispatch::call(
        interp,
        "pos-visible-in-window-p",
        &[Value::Integer(opoint as i64), window],
        env,
    )?;
    if visible.is_nil() {
        visual_vertical_motion(interp, env, window_id, -(ht / 2), None)?;
        startpos = interp.buffer.borrow().point();
    }
    interp.buffer.borrow_mut().goto_char(startpos);
    let lose = n < 0 && interp.buffer.borrow().point() <= interp.buffer.borrow().point_min();
    visual_vertical_motion(interp, env, window_id, n, None)?;
    let (pos, bolp, zv) = {
        let buffer = interp.buffer.borrow();
        let pos = buffer.point();
        let bolp = pos <= buffer.point_min() || buffer.char_at(pos - 1) == Some('\n');
        (pos, bolp, buffer.point_max())
    };
    interp.buffer.borrow_mut().goto_char(opoint);
    if lose {
        return if noerror {
            Ok(())
        } else {
            Err(signal_condition("beginning-of-buffer"))
        };
    }
    if pos >= zv {
        return if noerror {
            Ok(())
        } else {
            Err(signal_condition("end-of-buffer"))
        };
    }
    let margin = selected_scroll_margin(interp, env, text_height) as i64;
    set_window_start_marker(interp, window_id, pos);
    set_window_flag(interp, window_id, WINDOW_START_AT_LINE_BEG_SLOT, bolp);
    // force_start so redisplay runs the window-scroll-functions.
    set_window_flag(interp, window_id, WINDOW_FORCE_START_SLOT, true);
    let (preserve_hpos, preserve_vpos) = original;
    let goto = |interp: &mut Interpreter, at: usize| interp.buffer.borrow_mut().goto_char(at);
    if preserve.is_truthy() && margin == 0 && (whole || !matches!(preserve.kind(), Kind::T)) {
        goto(interp, pos);
        visual_vertical_motion(
            interp,
            env,
            window_id,
            preserve_vpos,
            Some(preserve_hpos.max(0) as usize),
        )?;
    } else if n > 0 {
        let top_margin = if margin > 0 {
            goto(interp, pos);
            visual_vertical_motion(interp, env, window_id, margin, None)?;
            interp.buffer.borrow().point()
        } else {
            pos
        };
        if top_margin <= opoint {
            goto(interp, opoint);
        } else if preserve.is_truthy() {
            let nlines = if preserve_vpos < margin {
                margin
            } else if preserve_vpos >= total_lines - margin {
                total_lines - margin - 1
            } else {
                preserve_vpos
            };
            goto(interp, pos);
            visual_vertical_motion(
                interp,
                env,
                window_id,
                nlines,
                Some(preserve_hpos.max(0) as usize),
            )?;
        } else {
            goto(interp, top_margin);
        }
    } else if n < 0 {
        // Scrolled backward: point near the window's end, outside the
        // scroll margin.
        goto(interp, pos);
        let moved = visual_vertical_motion(interp, env, window_id, ht - margin, None)?;
        let bottom_margin = if moved == ht - margin {
            interp.buffer.borrow().point()
        } else {
            interp.buffer.borrow().point() + 1
        };
        if bottom_margin > opoint {
            goto(interp, opoint);
        } else if preserve.is_truthy() {
            let nlines = if preserve_vpos < margin {
                margin
            } else if preserve_vpos >= ht - margin {
                ht - margin - 1
            } else {
                preserve_vpos
            };
            goto(interp, pos);
            visual_vertical_motion(
                interp,
                env,
                window_id,
                nlines,
                Some(preserve_hpos.max(0) as usize),
            )?;
        } else {
            visual_vertical_motion(interp, env, window_id, -1, None)?;
        }
    }
    if adjust_old_pointm {
        let point = interp.buffer.borrow().point();
        let buffer_id = interp.selected_window_buffer_id();
        set_window_marker(interp, window_id, WINDOW_OLD_POINT_SLOT, buffer_id, point);
    }
    Ok(())
}
