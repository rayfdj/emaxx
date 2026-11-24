use super::{prefix_numeric_value, signal_condition};
use crate::lisp::eval::Interpreter;
use crate::lisp::types::{Env, Kind, LispError, Value};

/// Live geometry of the selected window in an interactive session,
/// published by the terminal frontend after each redisplay.  Batch
/// sessions have none and keep GNU's batch answers (window-end = ZV).
#[derive(Clone, Copy)]
pub(crate) struct InteractiveWindowMetrics {
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

/// w->last_point of WINDOW: point as the window's last complete
/// redisplay left it, None where none has (the field is 0).
pub(crate) fn window_last_point(window: WindowRef) -> Option<i64> {
    Some(window.last_point()).filter(|point| *point != 0)
}

/// `w->last_point = POINT' for WINDOW, 0 for none.
pub(crate) fn set_window_last_point(window: WindowRef, point: Option<i64>) {
    window.set_last_point(point.unwrap_or(0));
}

/// w->column_number_displayed of WINDOW: the column a `%c' or `%C' of
/// its mode line last showed, None where none is displayed (the field is
/// -1).
pub(crate) fn window_column_number_displayed(window: WindowRef) -> Option<i64> {
    Some(window.column_number_displayed()).filter(|column| *column != -1)
}

/// `w->column_number_displayed = COLUMN' for WINDOW, -1 for none.
pub(crate) fn set_window_column_number_displayed(window: WindowRef, column: Option<i64>) {
    window.set_column_number_displayed(column.unwrap_or(-1));
}

/// WINDOW's character geometry as (total_cols, total_lines, left_col,
/// top_line), in window_geometry's order: window.h's fields of those
/// names, the window's edges and sizes in frame columns and lines.
/// window.c keeps them apart from the pixel geometry and assigns them on
/// its own schedule (window_resize_apply, window-resize-apply-total,
/// resize_frame_windows), so the two can differ: a batch frame's root
/// keeps pixel_top 0 while window-resize-apply-total puts its top_line
/// below the menu bar.  A text frame's character cell is one pixel, and a
/// window not yet given one of the four reads its pixel edge or size,
/// which it then still equals.
pub(crate) fn window_char_geometry(window: WindowRef) -> (i64, i64, i64, i64) {
    (
        window.total_cols().or(window.pixel_width()).unwrap_or(0),
        window.total_lines().or(window.pixel_height()).unwrap_or(0),
        window.left_col().or(window.pixel_left()).unwrap_or(0),
        window.top_line().or(window.pixel_top()).unwrap_or(0),
    )
}

/// WINDOW's left_margin_cols and right_margin_cols (None for 0, no
/// margin).
pub(crate) fn window_margin_cols(window: WindowRef) -> (Option<i64>, Option<i64>) {
    let margin = |cols: i64| Some(cols).filter(|cols| *cols != 0);
    (
        margin(window.left_margin_cols()),
        margin(window.right_margin_cols()),
    )
}

/// `w->left_margin_cols = LEFT' and `w->right_margin_cols = RIGHT', 0
/// for none.
pub(crate) fn set_window_margin_cols(window: WindowRef, left: Option<i64>, right: Option<i64>) {
    window.set_left_margin_cols(left.unwrap_or(0));
    window.set_right_margin_cols(right.unwrap_or(0));
}

/// window.h's three markers, w->start, w->pointm and w->old_pointm:
/// markers on the chain of the buffer the window shows, so that
/// insdel.c's marker adjustment moves them with its text.  A leaf window
/// holds one in each from make_window on; an internal window holds nil.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WindowMarker {
    Start,
    Pointm,
    OldPointm,
}

impl WindowMarker {
    /// The three in the order a new leaf's join its buffer's chain.
    pub(crate) const ALL: [WindowMarker; 3] = [Self::Start, Self::OldPointm, Self::Pointm];
}

/// WINDOW's marker WHICH (none in an internal window, whose is nil).
pub(crate) fn window_marker(
    window: WindowRef,
    which: WindowMarker,
) -> Option<crate::lisp::types::MarkerRef> {
    match window.marker_field(which).kind() {
        Kind::Marker(marker) => Some(marker),
        _ => None,
    }
}

/// marker_position of WINDOW's marker WHICH: None when it points nowhere.
pub(crate) fn window_marker_position(window: WindowRef, which: WindowMarker) -> Option<usize> {
    window_marker(window, which)?.position()
}

/// WINDOW's marker WHICH, made when the field holds none (as make_window
/// gives a window its three).
fn window_marker_for_set(window: WindowRef, which: WindowMarker) -> crate::lisp::types::MarkerRef {
    if let Some(marker) = window_marker(window, which) {
        return marker;
    }
    let marker = crate::lisp::types::MarkerRef::new();
    window.set_marker_field(which, Value::Marker(marker));
    marker
}

/// marker.c:set_marker_both on WINDOW's marker WHICH: CHARPOS of
/// BUFFER_ID, inside [BEG, Z].
pub(crate) fn set_window_marker(
    interp: &mut Interpreter,
    window: WindowRef,
    which: WindowMarker,
    buffer_id: u64,
    charpos: usize,
) {
    let marker = window_marker_for_set(window, which);
    let _ = interp.set_marker(marker, Some(charpos), Some(buffer_id));
}

/// marker.c:set_marker_restricted on WINDOW's marker WHICH: CHARPOS of
/// BUFFER_ID, inside its accessible portion.
pub(crate) fn set_window_marker_restricted(
    interp: &mut Interpreter,
    window: WindowRef,
    which: WindowMarker,
    buffer_id: u64,
    charpos: usize,
) {
    let (begv, zv) = buffer_point_bounds(interp, buffer_id);
    set_window_marker(interp, window, which, buffer_id, charpos.clamp(begv, zv));
}

/// marker.c:set_marker_restricted of WINDOW's marker WHICH to POSITION in
/// BUFFER_ID, the window's buffer (set_marker_internal with RESTRICTED):
/// nil, a marker pointing nowhere or a dead buffer unchains it; a marker
/// of that buffer, when WHICH's marker is in it already, lends its
/// positions as they are; a fixnum's or another marker's position is
/// clipped to the accessible portion; anything else is
/// `(wrong-type-argument integer-or-marker-p POSITION)'.
pub(crate) fn set_window_marker_restricted_value(
    interp: &mut Interpreter,
    window: WindowRef,
    which: WindowMarker,
    buffer_id: u64,
    position: &Value,
) -> Result<(), LispError> {
    let buffer = interp.buffer_object(buffer_id);
    let marker = window_marker_for_set(window, which);
    let buffer = match buffer {
        Some(buffer) if !position.is_nil() => buffer,
        _ => {
            marker.detach();
            return Ok(());
        }
    };
    if let Kind::Marker(source) = position.kind() {
        let Some(source_buffer) = source.buffer() else {
            marker.detach();
            return Ok(());
        };
        // "Optimize the special case where we are copying the position of
        // an existing marker, and MARKER is already in the same buffer."
        if source_buffer.ptr_eq(&buffer) && marker.buffer().is_some_and(|own| own.ptr_eq(&buffer)) {
            marker.set_positions(source.last_position(), source.bytepos());
            return Ok(());
        }
    }
    let charpos = match position.kind() {
        Kind::Integer(charpos) => charpos,
        Kind::Marker(source) => source.last_position() as i64,
        _ => {
            return Err(LispError::WrongTypeArgument(
                "integer-or-marker-p".into(),
                *position,
            ));
        }
    };
    let (begv, zv) = buffer_point_bounds(interp, buffer_id);
    let charpos = clip_to_bounds(begv as i64, charpos, zv as i64) as usize;
    set_window_marker(interp, window, which, buffer_id, charpos);
    Ok(())
}

/// marker.c:unchain_marker on WINDOW's three markers, as
/// delete_window and delete_all_child_windows do: they point nowhere.
pub(crate) fn unchain_window_markers(window: WindowRef) {
    for which in WindowMarker::ALL {
        if let Some(marker) = window_marker(window, which) {
            marker.detach();
        }
    }
}

/// set_window_buffer's insertion type for WINDOW's point markers: the
/// value of `window-point-insertion-type' in BUFFER_ID.
pub(crate) fn set_window_point_insertion_type(
    interp: &Interpreter,
    window: WindowRef,
    buffer_id: u64,
) {
    let insertion_type = window_point_insertion_type(interp, buffer_id);
    for which in [WindowMarker::Pointm, WindowMarker::OldPointm] {
        if let Some(marker) = window_marker(window, which) {
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

/// A saved field's position: its marker's (a window configuration's
/// copies), None when that points nowhere.
pub(crate) fn slot_marker_position(value: Option<&Value>) -> Option<usize> {
    match value?.kind() {
        Kind::Marker(marker) => marker.position(),
        _ => None,
    }
}

/// A window object, window.c's `struct window *': the one handle a
/// window's state is read and written through, and what the code that
/// works on a window holds and passes (a value, a frame's field, the
/// selected window).  The state behind it is the window object's own
/// (alloc/vectors/windows.rs): window.h's contents, its use_time and
/// sequence_number, the integers of its geometry and of its scrolling
/// and redisplay, its bits, and its Lisp fields, each by window.h's name.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WindowRef(crate::lisp::types::WindowObjectRef);

/// EQ of two windows: the same object.
impl PartialEq for WindowRef {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.0.ptr_eq(&other.0)
    }
}

impl Eq for WindowRef {}

impl std::hash::Hash for WindowRef {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.identity().hash(state);
    }
}

impl WindowRef {
    /// decode_any_window's WINDOWP: the window VALUE is, live, internal
    /// or deleted.
    #[inline]
    pub(crate) fn from_value(value: &Value) -> Option<WindowRef> {
        match value.kind() {
            Kind::Window(window) => Some(WindowRef(window)),
            _ => None,
        }
    }

    /// The window object WINDOW, as the handle.
    #[inline]
    pub(crate) fn from_object(window: crate::lisp::types::WindowObjectRef) -> WindowRef {
        WindowRef(window)
    }

    /// The window as a Lisp object.
    #[inline]
    pub(crate) fn value(self) -> Value {
        Value::Window(self.0)
    }

    /// WINDOW_FRAME (w) as an object: w->frame.  window.c stores the
    /// owning frame on every window, internal and deleted ones included,
    /// and it survives a frame switch and the frame's deletion; None for
    /// a window not made by a frame (a dump's dead window).
    #[inline]
    pub(crate) fn frame(self) -> Option<crate::lisp::types::FrameRef> {
        match self.0.frame().kind() {
            Kind::Frame(frame) => Some(frame),
            _ => None,
        }
    }

    /// `wset_frame (w, FRAME)'.
    #[inline]
    pub(crate) fn set_frame(self, frame: Value) {
        self.0.set_frame(frame);
    }

    /// w->parent as the window it is, none for a root window (or a
    /// deleted one whose link was cleared).
    #[inline]
    pub(crate) fn parent_window(self) -> Option<WindowRef> {
        WindowRef::from_value(&self.0.parent())
    }

    /// w->next as the window it is: the child after this one of its
    /// parent.
    #[inline]
    pub(crate) fn next_sibling(self) -> Option<WindowRef> {
        WindowRef::from_value(&self.0.next())
    }

    /// w->prev as the window it is: the child before this one.
    #[inline]
    pub(crate) fn prev_sibling(self) -> Option<WindowRef> {
        WindowRef::from_value(&self.0.prev())
    }

    /// `wset_parent (w, PARENT)', nil for none.
    #[inline]
    pub(crate) fn set_parent_window(self, parent: Option<WindowRef>) {
        self.0
            .set_parent(parent.map_or(Value::Nil, WindowRef::value));
    }

    /// `wset_next (w, NEXT)', nil for none.
    #[inline]
    pub(crate) fn set_next_sibling(self, next: Option<WindowRef>) {
        self.0.set_next(next.map_or(Value::Nil, WindowRef::value));
    }

    /// `wset_prev (w, PREV)', nil for none.
    #[inline]
    pub(crate) fn set_prev_sibling(self, prev: Option<WindowRef>) {
        self.0.set_prev(prev.map_or(Value::Nil, WindowRef::value));
    }

    /// w->normal_cols with HORFLAG, else w->normal_lines.
    #[inline]
    pub(crate) fn normal_size(self, horflag: bool) -> Value {
        if horflag {
            self.0.normal_cols()
        } else {
            self.0.normal_lines()
        }
    }

    /// `wset_normal_cols' with HORFLAG, else `wset_normal_lines' (w,
    /// VALUE).
    #[inline]
    pub(crate) fn set_normal_size(self, horflag: bool, value: Value) {
        if horflag {
            self.0.set_normal_cols(value);
        } else {
            self.0.set_normal_lines(value);
        }
    }

    /// The marker field WHICH names: w->start, w->pointm or w->old_pointm.
    #[inline]
    pub(crate) fn marker_field(self, which: WindowMarker) -> Value {
        match which {
            WindowMarker::Start => self.0.start(),
            WindowMarker::Pointm => self.0.pointm(),
            WindowMarker::OldPointm => self.0.old_pointm(),
        }
    }

    /// `wset_start', `wset_pointm' or `wset_old_pointm' (w, VALUE), as
    /// WHICH names.
    #[inline]
    pub(crate) fn set_marker_field(self, which: WindowMarker, value: Value) {
        match which {
            WindowMarker::Start => self.0.set_start(value),
            WindowMarker::Pointm => self.0.set_pointm(value),
            WindowMarker::OldPointm => self.0.set_old_pointm(value),
        }
    }

    /// w->base_line_number: the line number of the base line the mode
    /// line's `%l' counts from, 0 for none.
    #[inline]
    pub(crate) fn base_line_number(self) -> i64 {
        self.0.base_line_number()
    }

    /// `w->base_line_number = VALUE'.
    #[inline]
    pub(crate) fn set_base_line_number(self, value: i64) {
        self.0.set_base_line_number(value);
    }

    /// w->base_line_pos: the base line's position, 0 for none, -1 for
    /// "don't display line number as long as the window shows its
    /// buffer".
    #[inline]
    pub(crate) fn base_line_pos(self) -> i64 {
        self.0.base_line_pos()
    }

    /// `w->base_line_pos = VALUE'.
    #[inline]
    pub(crate) fn set_base_line_pos(self, value: i64) {
        self.0.set_base_line_pos(value);
    }

    /// w->contents: a leaf's buffer, an internal window's first child,
    /// nil for a deleted window.  (A window `make_leaf_window' made
    /// holds its buffer's id until wset_buffer first stores a buffer.)
    #[inline]
    pub(crate) fn contents(self) -> Value {
        self.0.contents()
    }

    /// `w->contents = VALUE' and no more: the store of wset_buffer,
    /// which counts the buffers' windows around it, and of
    /// wset_combination.
    #[inline]
    pub(crate) fn set_contents(self, value: Value) {
        self.0.set_contents(value);
    }

    /// window.h's WINDOW_VALID_P: `!NILP (w->contents)', a window of any
    /// sort, leaf or internal, that is not deleted.
    #[inline]
    pub(crate) fn is_valid(self) -> bool {
        !self.contents().is_nil()
    }

    /// window.h's WINDOW_LEAF_P, and WINDOW_LIVE_P: `BUFFERP
    /// (w->contents)', the buffer live or not.
    #[inline]
    pub(crate) fn is_leaf(self) -> bool {
        self.contents_buffer_id().is_some()
    }

    /// window.h's WINDOW_INTERNAL_P: `WINDOWP (w->contents)'.
    #[inline]
    pub(crate) fn is_internal(self) -> bool {
        self.first_child().is_some()
    }

    /// An internal window's first child: w->contents where it is a
    /// window.
    #[inline]
    pub(crate) fn first_child(self) -> Option<WindowRef> {
        WindowRef::from_value(&self.contents())
    }

    /// window.h's MINI_WINDOW_P: w->mini, which a window keeps when it is
    /// deleted.
    #[inline]
    pub(crate) fn mini(self) -> bool {
        self.0.mini()
    }

    /// `w->mini = MINI'.
    #[inline]
    pub(crate) fn set_mini(self, mini: bool) {
        self.0.set_mini(mini);
    }

    /// w->horizontal: an internal window's children are side by side
    /// (WINDOW_HORIZONTAL_COMBINATION_P) or one above the other.
    #[inline]
    pub(crate) fn horizontal(self) -> bool {
        self.0.horizontal()
    }

    /// `w->horizontal = HORIZONTAL'.
    #[inline]
    pub(crate) fn set_horizontal(self, horizontal: bool) {
        self.0.set_horizontal(horizontal);
    }

    /// w->use_time: how recently the window was selected, by
    /// window.c's window_select_count.
    #[inline]
    pub(crate) fn use_time(self) -> i64 {
        self.0.use_time()
    }

    /// `w->use_time = USE_TIME'.
    #[inline]
    pub(crate) fn set_use_time(self, use_time: i64) {
        self.0.set_use_time(use_time);
    }

    /// w->sequence_number, which print.c prints: the number make_window
    /// gave the window, 0 for one it did not make.
    #[inline]
    pub(crate) fn sequence_number(self) -> i64 {
        self.0.sequence_number()
    }

    /// `w->sequence_number = NUMBER', make_window's and
    /// make_parent_window's `++sequence_number'.
    #[inline]
    pub(crate) fn set_sequence_number(self, number: i64) {
        self.0.set_sequence_number(number);
    }

    /// w->hscroll_whole, w->vscroll, w->left_fringe_width,
    /// w->right_fringe_width, w->scroll_bar_width and
    /// w->scroll_bar_height: what save_window_save keeps of them.
    #[inline]
    pub(crate) fn hscroll_whole(self) -> i64 {
        self.0.hscroll_whole()
    }

    #[inline]
    pub(crate) fn vscroll(self) -> i64 {
        self.0.vscroll()
    }

    #[inline]
    pub(crate) fn left_fringe_width(self) -> i64 {
        self.0.left_fringe_width()
    }

    #[inline]
    pub(crate) fn right_fringe_width(self) -> i64 {
        self.0.right_fringe_width()
    }

    #[inline]
    pub(crate) fn scroll_bar_width(self) -> i64 {
        self.0.scroll_bar_width()
    }

    #[inline]
    pub(crate) fn scroll_bar_height(self) -> i64 {
        self.0.scroll_bar_height()
    }

    /// w->vertical_scroll_bar_type and w->horizontal_scroll_bar_type.
    #[inline]
    pub(crate) fn vertical_scroll_bar_type(self) -> Value {
        self.0.vertical_scroll_bar_type()
    }

    #[inline]
    pub(crate) fn horizontal_scroll_bar_type(self) -> Value {
        self.0.horizontal_scroll_bar_type()
    }

    /// w->cursor_off_p: the cursor is logically off.
    #[inline]
    pub(crate) fn cursor_off_p(self) -> bool {
        self.0.cursor_off_p()
    }

    /// `w->cursor_off_p = OFF'.
    #[inline]
    pub(crate) fn set_cursor_off_p(self, off: bool) {
        self.0.set_cursor_off_p(off);
    }

    /// w->pixel_left: the window's left edge in frame pixels; None for a window not given one
    /// (alloc/vectors/windows.rs).
    #[inline]
    pub(crate) fn pixel_left(self) -> Option<i64> {
        self.0.pixel_left()
    }

    /// `w->pixel_left = VALUE'; None takes it back.
    #[inline]
    pub(crate) fn set_pixel_left(self, value: Option<i64>) {
        self.0.set_pixel_left(value);
    }

    /// w->pixel_top: the window's top edge in frame pixels; None for a window not given one
    /// (alloc/vectors/windows.rs).
    #[inline]
    pub(crate) fn pixel_top(self) -> Option<i64> {
        self.0.pixel_top()
    }

    /// `w->pixel_top = VALUE'; None takes it back.
    #[inline]
    pub(crate) fn set_pixel_top(self, value: Option<i64>) {
        self.0.set_pixel_top(value);
    }

    /// w->left_col: the window's left edge in frame columns; None for a window not given one
    /// (alloc/vectors/windows.rs).
    #[inline]
    pub(crate) fn left_col(self) -> Option<i64> {
        self.0.left_col()
    }

    /// `w->left_col = VALUE'; None takes it back.
    #[inline]
    pub(crate) fn set_left_col(self, value: Option<i64>) {
        self.0.set_left_col(value);
    }

    /// w->top_line: the window's top edge in frame lines; None for a window not given one
    /// (alloc/vectors/windows.rs).
    #[inline]
    pub(crate) fn top_line(self) -> Option<i64> {
        self.0.top_line()
    }

    /// `w->top_line = VALUE'; None takes it back.
    #[inline]
    pub(crate) fn set_top_line(self, value: Option<i64>) {
        self.0.set_top_line(value);
    }

    /// w->pixel_width; None for a window not given one
    /// (alloc/vectors/windows.rs).
    #[inline]
    pub(crate) fn pixel_width(self) -> Option<i64> {
        self.0.pixel_width()
    }

    /// `w->pixel_width = VALUE'; None takes it back.
    #[inline]
    pub(crate) fn set_pixel_width(self, value: Option<i64>) {
        self.0.set_pixel_width(value);
    }

    /// w->pixel_height; None for a window not given one
    /// (alloc/vectors/windows.rs).
    #[inline]
    pub(crate) fn pixel_height(self) -> Option<i64> {
        self.0.pixel_height()
    }

    /// `w->pixel_height = VALUE'; None takes it back.
    #[inline]
    pub(crate) fn set_pixel_height(self, value: Option<i64>) {
        self.0.set_pixel_height(value);
    }

    /// w->total_cols: the window's width in columns; None for a window not given one
    /// (alloc/vectors/windows.rs).
    #[inline]
    pub(crate) fn total_cols(self) -> Option<i64> {
        self.0.total_cols()
    }

    /// `w->total_cols = VALUE'; None takes it back.
    #[inline]
    pub(crate) fn set_total_cols(self, value: Option<i64>) {
        self.0.set_total_cols(value);
    }

    /// w->total_lines: the window's height in lines; None for a window not given one
    /// (alloc/vectors/windows.rs).
    #[inline]
    pub(crate) fn total_lines(self) -> Option<i64> {
        self.0.total_lines()
    }

    /// `w->total_lines = VALUE'; None takes it back.
    #[inline]
    pub(crate) fn set_total_lines(self, value: Option<i64>) {
        self.0.set_total_lines(value);
    }

    /// w->old_pixel_width: the pixel width window_change_record recorded.
    #[inline]
    pub(crate) fn old_pixel_width(self) -> i64 {
        self.0.old_pixel_width()
    }

    /// `w->old_pixel_width = VALUE'.
    #[inline]
    pub(crate) fn set_old_pixel_width(self, value: i64) {
        self.0.set_old_pixel_width(value);
    }

    /// w->old_pixel_height.
    #[inline]
    pub(crate) fn old_pixel_height(self) -> i64 {
        self.0.old_pixel_height()
    }

    /// `w->old_pixel_height = VALUE'.
    #[inline]
    pub(crate) fn set_old_pixel_height(self, value: i64) {
        self.0.set_old_pixel_height(value);
    }

    /// w->old_body_pixel_width.
    #[inline]
    pub(crate) fn old_body_pixel_width(self) -> i64 {
        self.0.old_body_pixel_width()
    }

    /// `w->old_body_pixel_width = VALUE'.
    #[inline]
    pub(crate) fn set_old_body_pixel_width(self, value: i64) {
        self.0.set_old_body_pixel_width(value);
    }

    /// w->old_body_pixel_height.
    #[inline]
    pub(crate) fn old_body_pixel_height(self) -> i64 {
        self.0.old_body_pixel_height()
    }

    /// `w->old_body_pixel_height = VALUE'.
    #[inline]
    pub(crate) fn set_old_body_pixel_height(self, value: i64) {
        self.0.set_old_body_pixel_height(value);
    }

    /// w->left_margin_cols, 0 for no margin.
    #[inline]
    pub(crate) fn left_margin_cols(self) -> i64 {
        self.0.left_margin_cols()
    }

    /// `w->left_margin_cols = VALUE'.
    #[inline]
    pub(crate) fn set_left_margin_cols(self, value: i64) {
        self.0.set_left_margin_cols(value);
    }

    /// w->right_margin_cols, 0 for no margin.
    #[inline]
    pub(crate) fn right_margin_cols(self) -> i64 {
        self.0.right_margin_cols()
    }

    /// `w->right_margin_cols = VALUE'.
    #[inline]
    pub(crate) fn set_right_margin_cols(self, value: i64) {
        self.0.set_right_margin_cols(value);
    }

    /// w->change_stamp: the frame's stamp when window_change_record last
    /// recorded the window, 0 for a window made since.
    #[inline]
    pub(crate) fn change_stamp(self) -> i64 {
        self.0.change_stamp()
    }

    /// `w->change_stamp = VALUE'.
    #[inline]
    pub(crate) fn set_change_stamp(self, value: i64) {
        self.0.set_change_stamp(value);
    }

    /// w->hscroll.
    #[inline]
    pub(crate) fn hscroll(self) -> i64 {
        self.0.hscroll()
    }

    /// `w->hscroll = VALUE'.
    #[inline]
    pub(crate) fn set_hscroll(self, value: i64) {
        self.0.set_hscroll(value);
    }

    /// w->min_hscroll.
    #[inline]
    pub(crate) fn min_hscroll(self) -> i64 {
        self.0.min_hscroll()
    }

    /// `w->min_hscroll = VALUE'.
    #[inline]
    pub(crate) fn set_min_hscroll(self, value: i64) {
        self.0.set_min_hscroll(value);
    }

    /// w->last_modified: the buffer's modification count as the window's
    /// last complete redisplay left it.
    #[inline]
    pub(crate) fn last_modified(self) -> i64 {
        self.0.last_modified()
    }

    /// `w->last_modified = VALUE'.
    #[inline]
    pub(crate) fn set_last_modified(self, value: i64) {
        self.0.set_last_modified(value);
    }

    /// w->last_overlay_modified.
    #[inline]
    pub(crate) fn last_overlay_modified(self) -> i64 {
        self.0.last_overlay_modified()
    }

    /// `w->last_overlay_modified = VALUE'.
    #[inline]
    pub(crate) fn set_last_overlay_modified(self, value: i64) {
        self.0.set_last_overlay_modified(value);
    }

    /// w->last_point, 0 for none.
    #[inline]
    pub(crate) fn last_point(self) -> i64 {
        self.0.last_point()
    }

    /// `w->last_point = VALUE'.
    #[inline]
    pub(crate) fn set_last_point(self, value: i64) {
        self.0.set_last_point(value);
    }

    /// w->column_number_displayed, -1 for none.
    #[inline]
    pub(crate) fn column_number_displayed(self) -> i64 {
        self.0.column_number_displayed()
    }

    /// `w->column_number_displayed = VALUE'.
    #[inline]
    pub(crate) fn set_column_number_displayed(self, value: i64) {
        self.0.set_column_number_displayed(value);
    }

    /// w->window_end_pos: Z minus the end of the text the window's last
    /// redisplay showed; None for a window no redisplay and no
    /// set_window_buffer has given one.
    #[inline]
    pub(crate) fn window_end_pos(self) -> Option<i64> {
        self.0.window_end_pos()
    }

    /// `w->window_end_pos = VALUE'; None takes it back.
    #[inline]
    pub(crate) fn set_window_end_pos(self, value: Option<i64>) {
        self.0.set_window_end_pos(value);
    }

    /// w->update_mode_line: the window's mode line must be redisplayed
    /// (wset_update_mode_line), until mark_window_display_accurate_1
    /// clears it.
    #[inline]
    pub(crate) fn update_mode_line(self) -> bool {
        self.0.update_mode_line()
    }

    /// `w->update_mode_line = ON'.
    #[inline]
    pub(crate) fn set_update_mode_line(self, on: bool) {
        self.0.set_update_mode_line(on);
    }

    /// w->last_had_star: whether the buffer was modified when the
    /// window's mode line was last displayed.
    #[inline]
    pub(crate) fn last_had_star(self) -> bool {
        self.0.last_had_star()
    }

    /// `w->last_had_star = ON'.
    #[inline]
    pub(crate) fn set_last_had_star(self, on: bool) {
        self.0.set_last_had_star(on);
    }

    /// w->start_at_line_beg.
    #[inline]
    pub(crate) fn start_at_line_beg(self) -> bool {
        self.0.start_at_line_beg()
    }

    /// `w->start_at_line_beg = ON'.
    #[inline]
    pub(crate) fn set_start_at_line_beg(self, on: bool) {
        self.0.set_start_at_line_beg(on);
    }

    /// w->force_start.
    #[inline]
    pub(crate) fn force_start(self) -> bool {
        self.0.force_start()
    }

    /// `w->force_start = ON'.
    #[inline]
    pub(crate) fn set_force_start(self, on: bool) {
        self.0.set_force_start(on);
    }

    /// w->optional_new_start.
    #[inline]
    pub(crate) fn optional_new_start(self) -> bool {
        self.0.optional_new_start()
    }

    /// `w->optional_new_start = ON'.
    #[inline]
    pub(crate) fn set_optional_new_start(self, on: bool) {
        self.0.set_optional_new_start(on);
    }

    /// w->window_end_valid: the window's last redisplay completed
    /// (mark_window_display_accurate_1) and nothing since asked for its
    /// display to be made anew; try_cursor_movement needs it.
    #[inline]
    pub(crate) fn window_end_valid(self) -> bool {
        self.0.window_end_valid()
    }

    /// `w->window_end_valid = ON'.
    #[inline]
    pub(crate) fn set_window_end_valid(self, on: bool) {
        self.0.set_window_end_valid(on);
    }

    /// w->redisplay: the window must be redisplayed (wset_redisplay).
    #[inline]
    pub(crate) fn redisplay(self) -> bool {
        self.0.redisplay()
    }

    /// `w->redisplay = ON'.
    #[inline]
    pub(crate) fn set_redisplay(self, on: bool) {
        self.0.set_redisplay(on);
    }

    /// w->suspend_auto_hscroll.
    #[inline]
    pub(crate) fn suspend_auto_hscroll(self) -> bool {
        self.0.suspend_auto_hscroll()
    }

    /// `w->suspend_auto_hscroll = ON'.
    #[inline]
    pub(crate) fn set_suspend_auto_hscroll(self, on: bool) {
        self.0.set_suspend_auto_hscroll(on);
    }

    /// w->contents where it is a buffer, live or not
    /// (`window_slot_buffer_id' of the contents).
    #[inline]
    pub(crate) fn contents_buffer_id(self) -> Option<u64> {
        window_slot_buffer_id(Some(&self.contents()))
    }

    /// The live buffer the window shows (BUFFERP (w->contents) and
    /// BUFFER_LIVE_P of it), by its id.
    #[inline]
    pub(crate) fn live_buffer_id(self, interp: &Interpreter) -> Option<u64> {
        match self.contents().kind() {
            // BUFFER_LIVE_P of the object itself.
            Kind::Buffer(buffer) => buffer.is_live().then_some(buffer.id),
            Kind::Integer(id) => {
                let id = id.max(0) as u64;
                interp.has_buffer_id(id).then_some(id)
            }
            // w->contents nil, a deleted window, even the selected one;
            // or a window, the first child of an internal one.
            _ => None,
        }
    }

    /// The Lisp fields in alloc.c:mark_window's order (window.h's
    /// twenty-five, then prev_buffers and next_buffers), as
    /// make_parent_window's memcpy copies them.
    #[inline]
    pub(crate) fn lisp_values(
        self,
    ) -> [Value; crate::lisp::alloc::vectors::windows::WINDOW_LISP_WORDS + 2] {
        self.0.lisp_values()
    }

    /// Store VALUES in the Lisp fields, in `lisp_values'' order.
    #[inline]
    pub(crate) fn set_lisp_values(
        self,
        values: [Value; crate::lisp::alloc::vectors::windows::WINDOW_LISP_WORDS + 2],
    ) {
        self.0.set_lisp_values(values);
    }

    /// make_parent_window's memcpy of the fields only make_window gives
    /// values here: FROM's.
    #[inline]
    pub(crate) fn copy_display_fields(self, from: WindowRef) {
        self.0.copy_display_fields(&from.0);
    }

    /// window.c:make_window's initialization past allocate_window.
    #[inline]
    pub(crate) fn initialize_as_make_window(self) {
        self.0.initialize_as_make_window();
    }
}

/// `w->FIELD' and `wset_FIELD (w, VALUE)' on a window for the Lisp fields
/// named, each window.h's of that name.
macro_rules! window_lisp_fields {
    ($($(#[$doc:meta])* $field:ident $set:ident;)*) => {
        impl WindowRef {
            $(
                $(#[$doc])*
                #[inline]
                pub(crate) fn $field(self) -> Value {
                    self.0.$field()
                }

                #[inline]
                pub(crate) fn $set(self, value: Value) {
                    self.0.$set(value);
                }
            )*
        }
    };
}

window_lisp_fields! {
    /// w->normal_lines: the window's normal height, a fraction.
    normal_lines set_normal_lines;
    /// w->normal_cols: the window's normal width.
    normal_cols set_normal_cols;
    /// w->new_total: the total size staged for window-resize-apply-total.
    new_total set_new_total;
    /// w->new_normal: the normal size staged for a resize.
    new_normal set_new_normal;
    /// w->new_pixel: the pixel size staged for window-resize-apply.
    new_pixel set_new_pixel;
    /// w->old_buffer: the buffer run_window_change_functions last saw.
    old_buffer set_old_buffer;
    /// w->display_table: the window's own display table, nil for none.
    display_table set_display_table;
    /// w->dedicated.
    dedicated set_dedicated;
    /// w->combination_limit, and a deleted leaf's buffer.
    combination_limit set_combination_limit;
    /// w->window_parameters: an alist.
    window_parameters set_window_parameters;
    /// w->cursor_type.
    cursor_type set_cursor_type;
    /// w->prev_buffers: the buffers the window showed before.
    prev_buffers set_prev_buffers;
    /// w->next_buffers: the buffers switch-to-prev-buffer came from.
    next_buffers set_next_buffers;
}

/// window.c:make_window for a leaf that is to show BUFFER_ID from START,
/// a minibuffer window when MINI, with GEOMETRY its pixel width, height,
/// left and top edges (its columns and lines are those until window.c's
/// resizing assigns them: `window_char_geometry').  Its start, old point
/// and point markers point at START of that buffer, joining its chain in
/// that order, the point markers of the insertion type set_window_buffer
/// gives them.  Its contents name the buffer by its id, as a new
/// window's always have here: the buffer itself is stored by
/// wset_buffer, when set_window_buffer first shows a buffer in the
/// window.  The buffer's count of windows is the caller's to keep.
pub(crate) fn make_leaf_window(
    interp: &mut Interpreter,
    buffer_id: u64,
    start: usize,
    mini: bool,
    geometry: (i64, i64, i64, i64),
) -> WindowRef {
    let window = interp.make_window();
    if let Some(buffer) = interp.buffer_object(buffer_id) {
        let point_insertion = window_point_insertion_type(interp, buffer_id);
        let state = buffer.borrow();
        let position = start.clamp(1, state.size_total() + 1);
        for which in WindowMarker::ALL {
            if let Some(marker) = window_marker(window, which) {
                marker.attach(buffer, position, state.marker_byte_position(position));
                marker.set_insertion_type(point_insertion && which != WindowMarker::Start);
            }
        }
    }
    let (width, height, left, top) = geometry;
    window.set_pixel_width(Some(width));
    window.set_pixel_height(Some(height));
    window.set_pixel_left(Some(left));
    window.set_pixel_top(Some(top));
    window.set_contents(Value::Integer(buffer_id as i64));
    window.set_mini(mini);
    window
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

/// BUFFERP of a window's contents SLOT: the buffer a leaf window shows
/// (by its id where `make_leaf_window' named it so), whether or not it
/// is live.
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
    WindowRef::from_value(value)?.live_buffer_id(interp)
}

pub(crate) fn window_start(
    interp: &Interpreter,
    value: Option<&Value>,
) -> Result<usize, LispError> {
    match value.map(|v| v.kind()) {
        None | Some(Kind::Nil) => Ok(current_window_start(interp)),
        Some(value) => {
            let Some(window) = WindowRef::from_value(&value.value()) else {
                return Err(LispError::WrongTypeArgument(
                    "windowp".into(),
                    value.value(),
                ));
            };
            if Some(window) == interp.selected_window() {
                return Ok(current_window_start(interp));
            }
            let buffer_id = window
                .live_buffer_id(interp)
                .unwrap_or(interp.current_buffer_id());
            Ok(window_start_marker(interp, window, buffer_id))
        }
    }
}

/// WINDOW's start marker position (Fmarker_position of w->start): a
/// marker stays inside [BEG, Z] of BUFFER_ID but not inside a narrowing
/// made after it was set.
pub(crate) fn window_start_marker(
    interp: &Interpreter,
    window: WindowRef,
    buffer_id: u64,
) -> usize {
    if Some(window) == interp.selected_window() {
        return interp.selected_window_start();
    }
    window_marker_position(window, WindowMarker::Start)
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
/// set_marker_restricted clamps) as WINDOW's start marker.
pub(crate) fn set_window_start_marker(interp: &mut Interpreter, window: WindowRef, start: usize) {
    let buffer_id = window
        .live_buffer_id(interp)
        .unwrap_or(interp.current_buffer_id());
    let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
    let start = start.clamp(point_min, point_max);
    set_window_marker(interp, window, WindowMarker::Start, buffer_id, start);
}

/// What a redisplay of WINDOW showed: its point (the window's own,
/// for a window other than the selected one, as redisplay clamped or
/// moved it) and window_end_pos, Z less the end of the text it showed.
pub(crate) fn record_window_display(
    interp: &mut Interpreter,
    window: WindowRef,
    buffer_id: u64,
    point: usize,
    window_end: usize,
) {
    let z = buffer_z(interp, buffer_id);
    set_window_marker(interp, window, WindowMarker::Pointm, buffer_id, point);
    window.set_window_end_pos(Some(z as i64 - window_end.min(z) as i64));
}

/// window.c Fset_window_start past decode_live_window: the start marker
/// set_marker_restricted to POS in the window's buffer; redisplay must
/// show it (force_start) unless NOFORCE, and treats it as not known to
/// begin a line.
pub(crate) fn set_window_start_value(
    interp: &mut Interpreter,
    window: WindowRef,
    pos: &Value,
    noforce: bool,
) -> Result<(), LispError> {
    // set_marker_restricted (w->start, pos, w->contents): its live_buffer
    // is decode_buffer's, the current buffer for the nil contents of a
    // deleted selected window, which decode_live_window gives for nil.
    let buffer_id = window
        .live_buffer_id(interp)
        .unwrap_or(interp.current_buffer_id());
    set_window_marker_restricted_value(interp, window, WindowMarker::Start, buffer_id, pos)?;
    window.set_start_at_line_beg(false);
    // Bug#15957.
    window.set_window_end_valid(false);
    if !noforce {
        window.set_force_start(true);
    }
    interp.wset_update_mode_line(window);
    interp.wset_redisplay(window);
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
pub(crate) fn selected_scroll_margin(interp: &Interpreter, window_lines: usize) -> usize {
    let scroll_margin = interp
        .symbol_value_key(cached_symbol!("scroll-margin"))
        .and_then(|value| value.integer_value())
        .unwrap_or(0);
    let maximum = interp
        .symbol_value_key(cached_symbol!("maximum-scroll-margin"))
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
    let w = interp.selected_window_ref();
    // buf = XBUFFER (w->contents): a deleted selected window's nil is no
    // buffer, so not the current one either.
    if interp.selected_window_contents_buffer_id() != Some(interp.current_buffer_id()) {
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
                .symbol_value_key(cached_symbol!("recenter-redisplay"))
                .is_some_and(|value| value.is_truthy())
        {
            // redraw_frame: fset_redisplay (f).
            let selected = interp.selected_window_ref();
            interp.fset_redisplay_window_frame(selected);
        }
        center = true;
    } else if matches!(arg.kind(), Kind::Cons(_)) {
        center = true;
    } else {
        iarg = prefix_numeric_value(&arg)?.as_integer()?;
    }
    let ht = text_height.max(1) as i64;
    let margin = selected_scroll_margin(interp, text_height) as i64;
    let point = interp.buffer.borrow().point();
    // The display code is not used for initial frames, whose data
    // structures may not be set up yet, nor for a buffer with very long
    // lines, which it slows down tremendously.
    let display_code = interactive && !interp.buffer.borrow().long_line_optimizations_p();
    let start = if display_code {
        // The display code's iterator walks run `fontification-functions'
        // over the text they fetch, and what they measure follows; made
        // again while the functions run.
        let mut passes = 0;
        let mut fetches = crate::tty::IteratorFetches::off();
        loop {
            fetches.begin_pass(interp, env, w);
            let start = recenter_display_start(
                interp,
                env,
                w,
                RecenterArg { center, iarg },
                (ht, margin),
                point,
                &mut fetches,
            );
            passes += 1;
            if passes > crate::tty::ITERATOR_FONTIFICATION_PASSES
                || !fetches.fontify(interp, env, w)
            {
                match start {
                    Some(start) => break start,
                    None => return Ok(()),
                }
            }
        }
    } else {
        // vmotion, the margins applied to every arg.
        let mut line = if center {
            ht / 2
        } else if iarg < 0 {
            iarg + ht
        } else {
            iarg
        };
        line = clip_to_bounds(margin, line, ht - margin - 1);
        let win = super::indent_motion::MotionWindow {
            window: w,
            contents: interp.current_buffer_id(),
        };
        super::indent_motion::vmotion(interp, env, point, -line, win).bufpos
    };
    interp.set_selected_window_start(start);
    w.set_window_end_valid(false);
    w.set_optional_new_start(true);
    let at_line_beg = {
        let buffer = interp.buffer.borrow();
        start <= buffer.point_min() || buffer.char_at(start - 1) == Some('\n')
    };
    w.set_start_at_line_beg(at_line_beg);
    interp.wset_redisplay(w);
    Ok(())
}

/// Frecenter's ARG: no number (center) or IARG.
#[derive(Clone, Copy)]
struct RecenterArg {
    center: bool,
    iarg: i64,
}

/// Frecenter's display code in WINDOW, whose text area is HT rows with
/// MARGIN scroll margin lines: the new start for point at POINT, or none
/// when point's row and the ARG lines below it fill the window; FETCHES
/// recording what its iterators fetch.  Centered, start_display at point
/// and move_it_vertically_backward half the window; a negative ARG,
/// start_display at point, move_it_by_lines 0 and then -ARG lines down
/// to measure what point's row and the lines below it take, and again
/// start_display at point and move_it_vertically_backward what remains
/// above them; otherwise start_display at point, move_it_by_lines 0 and
/// then ARG lines back, which reseats at a line start.
fn recenter_display_start(
    interp: &mut Interpreter,
    env: &mut Env,
    window: WindowRef,
    RecenterArg { center, iarg }: RecenterArg,
    (ht, margin): (i64, i64),
    point: usize,
    fetches: &mut crate::tty::IteratorFetches,
) -> Option<usize> {
    if !center && iarg >= 0 {
        let nlines = clip_to_bounds(margin, iarg, ht - margin - 1).max(0);
        crate::tty::fetch_start_display(interp, window, fetches, point);
        let row = super::move_it::move_it_by_lines(interp, env, window, point, 0, fetches).0;
        return Some(if nlines > 0 {
            super::move_it::move_it_by_lines(interp, env, window, row, -nlines, fetches).0
        } else {
            row
        });
    }
    let lines_back = if center {
        ht / 2
    } else {
        let nlines = clip_to_bounds(margin + 1, -iarg, ht - margin);
        crate::tty::fetch_start_display(interp, window, fetches, point);
        let row = super::move_it::move_it_by_lines(interp, env, window, point, 0, fetches).0;
        super::move_it::move_it_by_lines(interp, env, window, row, nlines, fetches);
        // Point's row ends NLINES from the bottom, empty lines past the
        // buffer's end counted in.
        let h = ht - nlines;
        if h <= 0 {
            return None;
        }
        h
    };
    // move_it_vertically_backward: whole screen lines above point's row,
    // no reseat.
    crate::tty::fetch_start_display(interp, window, fetches, point);
    crate::tty::fetch_move_vertically_backward(
        interp,
        env,
        window,
        fetches,
        point,
        lines_back as usize,
    );
    Some(move_screen_lines(interp, env, point, -(lines_back as isize)).0)
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
    let window = interp.selected_window_ref();
    move_screen_lines_in(interp, env, window, from, count)
}

/// `move_screen_lines' under WINDOW's geometry, in the current buffer
/// (which the caller makes WINDOW's).
pub(crate) fn move_screen_lines_in(
    interp: &mut Interpreter,
    env: &mut Env,
    window: WindowRef,
    from: usize,
    count: isize,
) -> (usize, isize) {
    use crate::lisp::primitives::dispatch::{row_index_of, screen_row_starts, visual_line_bounds};
    let point_min = interp.buffer.borrow().point_min();
    let point_max = interp.buffer.borrow().point_max();
    let from = from.clamp(point_min, point_max);
    let (mut bol, mut eol) = visual_line_bounds(interp, from);
    let mut starts = screen_row_starts(interp, env, window, bol, eol);
    let mut index = row_index_of(&starts, from);
    let mut remaining = count;
    while remaining > 0 {
        if index + 1 < starts.len() {
            index += 1;
        } else if eol < point_max {
            let bounds = visual_line_bounds(interp, eol + 1);
            bol = bounds.0;
            eol = bounds.1;
            starts = screen_row_starts(interp, env, window, bol, eol);
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
            starts = screen_row_starts(interp, env, window, bol, eol);
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
    window: WindowRef,
    start: usize,
    pos: usize,
    limit: usize,
) -> Result<(i64, i64), LispError> {
    let mut rows = 0i64;
    let mut cursor = start;
    while (rows as usize) < limit && pos >= cursor {
        let (next, remaining) = move_screen_lines_in(interp, env, window, cursor, 1);
        if remaining != 0 || next <= cursor || pos < next {
            break;
        }
        cursor = next;
        rows += 1;
    }
    // indent.c's current_column_1 of a position from its line's start,
    // its error let through.
    let column_at = |interp: &mut Interpreter, env: &mut Env, at: usize| {
        let line_start = super::buffers::line_beginning_at(interp, at);
        super::buffers::column_at(interp, env, line_start, at).map(|column| column as i64)
    };
    let row_start = move_screen_lines_in(interp, env, window, pos, 0).0;
    let hpos = column_at(interp, env, pos)? - column_at(interp, env, row_start)?;
    Ok((rows, hpos.max(0)))
}

/// window.c window_scroll on the selected window, whose buffer is
/// current: a whole-window scroll of a buffer with very long lines that
/// this window truncates is window_scroll_for_long_lines', any other
/// window_scroll_line_based's, `fontification-functions' bound to nil
/// for a whole-window scroll that `fast-but-imprecise-scrolling' or
/// those truncated lines ask to be fast.  TEXT_HEIGHT is
/// window_internal_height and TOTAL_LINES the window's total height.
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
    let window = interp.selected_window_ref();
    let long_lines = interp.buffer.borrow().long_line_optimizations_p();
    let mut long_lines_truncated = long_lines
        && interp
            .symbol_value_key(cached_symbol!("truncate-lines"))
            .is_some_and(|value| !value.is_nil());
    let count = interp.specpdl_index();
    let n = n.clamp(i64::from(i32::MIN), i64::from(i32::MAX));
    interp.wset_redisplay(window);
    // Does this window's buffer have very long and truncated lines?
    let total_cols = window_char_geometry(window).0;
    if long_lines && !long_lines_truncated && total_cols < interp.frame_width() {
        let partial = interp
            .symbol_value_key(cached_symbol!("truncate-partial-width-windows"))
            .unwrap_or(Value::Nil);
        if !partial.is_nil() {
            // XFIXNAT of a fixnum, a negative one included.
            long_lines_truncated = match partial.kind() {
                Kind::Integer(columns) => total_cols < columns,
                _ => true,
            };
        }
    }
    let fast = interp
        .symbol_value_key(cached_symbol!("fast-but-imprecise-scrolling"))
        .is_some_and(|value| !value.is_nil());
    if whole && (fast || long_lines_truncated) {
        interp.specbind_symbol(
            &cached_symbol!("fontification-functions").with(|symbol| *symbol),
            Value::Nil,
            env,
        )?;
    }
    let result = if whole && long_lines_truncated {
        window_scroll_for_long_lines(interp, env, window, n, noerror, text_height)
    } else {
        window_scroll_line_based(
            interp,
            env,
            window,
            n,
            whole,
            noerror,
            ScrollGeometry {
                text_height,
                total_lines,
                hscroll,
            },
        )
    };
    let unbind = interp.unbind_to(count, env);
    result.and(unbind)?;
    // Bug#15957: a window-end asked for before the next redisplay must
    // be computed anew.
    window.set_window_end_valid(false);
    Ok(())
}

/// window.c window_scroll_for_long_lines: a whole-window scroll of
/// truncated very long lines, by vmotion alone, which honors neither
/// `scroll-preserve-screen-position' nor `scroll-margin'.
fn window_scroll_for_long_lines(
    interp: &mut Interpreter,
    env: &mut Env,
    window: WindowRef,
    n: i64,
    noerror: bool,
    text_height: usize,
) -> Result<(), LispError> {
    use super::indent_motion::{MotionWindow, vmotion};
    let win = MotionWindow {
        window,
        contents: interp.current_buffer_id(),
    };
    let mut startpos = interp.selected_window_start();
    let nscls = interp
        .symbol_value_key(cached_symbol!("next-screen-context-lines"))
        .and_then(|value| value.integer_value())
        .unwrap_or(2)
        .clamp(0, 1_000_000);
    let ht = text_height as i64;
    let n = n.saturating_mul((ht - nscls).max(1));
    // If point is not visible in the window, bring it inside the window.
    let (point, zv) = {
        let buffer = interp.buffer.borrow();
        (buffer.point(), buffer.point_max())
    };
    let visible = point >= startpos
        && point <= zv
        && startpos <= zv
        && super::dispatch::call(
            interp,
            "pos-visible-in-window-p",
            &[Value::Integer(point as i64), window.value()],
            env,
        )?
        .is_truthy();
    if !visible {
        startpos = vmotion(interp, env, point, -(ht / 2), win).bufpos;
    }
    interp.buffer.borrow_mut().goto_char(startpos);
    let lose = n < 0 && interp.buffer.borrow().point() == interp.buffer.borrow().point_min();
    let pos = vmotion(interp, env, startpos, n, win).bufpos;
    if lose {
        return if noerror {
            Ok(())
        } else {
            Err(signal_condition("beginning-of-buffer"))
        };
    }
    let (bolp, zv) = {
        let buffer = interp.buffer.borrow();
        (
            pos == buffer.point_min() || buffer.char_at(pos - 1) == Some('\n'),
            buffer.point_max(),
        )
    };
    if pos < zv {
        set_window_start_marker(interp, window, pos);
        window.set_start_at_line_beg(bolp);
        interp.wset_update_mode_line(window);
        // force_start so that redisplay_window runs the
        // window-scroll-functions.
        window.set_force_start(true);
        interp.buffer.borrow_mut().goto_char(pos);
        let pos = if n > 0 {
            vmotion(interp, env, pos, ht / 2, win).bufpos
        } else if n < 0 {
            vmotion(interp, env, pos, -(ht / 2), win).bufpos
        } else {
            pos
        };
        interp.buffer.borrow_mut().goto_char(pos);
        Ok(())
    } else if noerror {
        Ok(())
    } else {
        Err(signal_condition("end-of-buffer"))
    }
}

/// What window_scroll_line_based reads of the window's geometry: its
/// window_internal_height, its total height and its hscroll.
struct ScrollGeometry {
    text_height: usize,
    total_lines: i64,
    hscroll: i64,
}

/// window.c window_scroll_line_based on the selected window: scroll N
/// lines (N screenfuls less `next-screen-context-lines' when WHOLE),
/// then keep point out of the scroll margins or, under
/// `scroll-preserve-screen-position', on the screen line it occupied.
fn window_scroll_line_based(
    interp: &mut Interpreter,
    env: &mut Env,
    w: WindowRef,
    n: i64,
    whole: bool,
    noerror: bool,
    geometry: ScrollGeometry,
) -> Result<(), LispError> {
    use super::dispatch::visual_vertical_motion;
    let ScrollGeometry {
        text_height,
        total_lines,
        hscroll,
    } = geometry;
    let window = w.value();
    let opoint = interp.buffer.borrow().point();
    let ht = text_height.max(1) as i64;
    let mut startpos = interp.selected_window_start();
    let adjust_old_pointm = window_marker_position(w, WindowMarker::OldPointm) == Some(opoint);
    let mut n = n;
    if whole {
        let nscls = interp
            .symbol_value_key(cached_symbol!("next-screen-context-lines"))
            .and_then(|value| value.integer_value())
            .unwrap_or(2)
            .clamp(0, 1_000_000);
        n = n.saturating_mul((ht - nscls).max(1));
    }
    let preserve = interp
        .symbol_value_key(cached_symbol!("scroll-preserve-screen-position"))
        .unwrap_or(Value::Nil);
    let mut original = (0i64, 0i64);
    if preserve.is_truthy() {
        let (hpos, vpos) = SCROLL_PRESERVE.with(|cell| cell.get());
        let last_command = interp
            .symbol_value_key(cached_symbol!("last-command"))
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
            let (vpos, hpos) = motion_from_start(interp, env, w, startpos, opoint, text_height)?;
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
        visual_vertical_motion(interp, env, w, -(ht / 2), None)?;
        startpos = interp.buffer.borrow().point();
    }
    interp.buffer.borrow_mut().goto_char(startpos);
    let lose = n < 0 && interp.buffer.borrow().point() <= interp.buffer.borrow().point_min();
    visual_vertical_motion(interp, env, w, n, None)?;
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
    let margin = selected_scroll_margin(interp, text_height) as i64;
    set_window_start_marker(interp, w, pos);
    w.set_start_at_line_beg(bolp);
    interp.wset_update_mode_line(w);
    // force_start so redisplay runs the window-scroll-functions.
    w.set_force_start(true);
    let (preserve_hpos, preserve_vpos) = original;
    let goto = |interp: &mut Interpreter, at: usize| interp.buffer.borrow_mut().goto_char(at);
    if preserve.is_truthy() && margin == 0 && (whole || !matches!(preserve.kind(), Kind::T)) {
        goto(interp, pos);
        visual_vertical_motion(interp, env, w, preserve_vpos, Some(preserve_hpos))?;
    } else if n > 0 {
        let top_margin = if margin > 0 {
            goto(interp, pos);
            visual_vertical_motion(interp, env, w, margin, None)?;
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
            visual_vertical_motion(interp, env, w, nlines, Some(preserve_hpos))?;
        } else {
            goto(interp, top_margin);
        }
    } else if n < 0 {
        // Scrolled backward: point near the window's end, outside the
        // scroll margin.
        goto(interp, pos);
        let moved = visual_vertical_motion(interp, env, w, ht - margin, None)?;
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
            visual_vertical_motion(interp, env, w, nlines, Some(preserve_hpos))?;
        } else {
            visual_vertical_motion(interp, env, w, -1, None)?;
        }
    }
    if adjust_old_pointm {
        // Fset_marker (w->old_pointm, BUF_PT (XBUFFER (w->contents)),
        // w->contents): scroll_command has made the buffer current.
        let point = interp.buffer.borrow().point();
        let buffer_id = interp.current_buffer_id();
        set_window_marker(interp, w, WindowMarker::OldPointm, buffer_id, point);
    }
    Ok(())
}
