//! window.h:struct window, PVEC_WINDOW: the window object, laid out as
//! window.c's struct is and traced as alloc.c:mark_window traces it.
//!
//! The payload after the one-word vector header is window.h's fields in
//! window.h's order, each of its C type: the twenty-five Lisp fields from
//! `frame' to `mode_line_help_echo', the words allocate_pseudovector
//! counts as the pseudovector's Lisp size (PSEUDOVECSIZE (struct window,
//! mode_line_help_echo)); then the rest, from the glyph matrices (none
//! here: null) through `prev_buffers' and `next_buffers', the two Lisp
//! fields mark_window marks apart, the integers, the bits and
//! `window_end_bytepos'.  The whole is VECSIZE (struct window) words, so
//! a window is allocated, consed and counted by the census as window.c's
//! struct is.  The offsets are checked below against those of the
//! configured GNU build (gdb's `ptype /o struct window').  Like the C
//! struct, the object is its own identity: nothing names a window by a
//! number, and the windows are found as window.c finds them, from the
//! frames' window trees.
//!
//! Every field exists from make_window on, as in C: allocate_window's
//! window has its Lisp fields nil and the rest zero, and
//! Interpreter::make_window gives the fields window.c's make_window
//! gives a value other than those.  A field nothing here reads or writes
//! yet (the glyph matrices, the cursors, the fringe, scroll bar and line
//! heights, vscroll and the like) keeps the value make_window gives it,
//! which make_parent_window's copy takes and save_window_save keeps
//! where window.c keeps it; the Debug form shows them with the others.
//!
//! window.h's integers are C ints, which have no state for "never
//! assigned": window.c assigns a window's edges and sizes where it makes
//! the window.  The windows here are not yet all made so (a new window is
//! given its pixel edges and sizes and reads its columns and lines from
//! them until window.c's resizing assigns those, and the dead window of a
//! dump is given nothing), so each of the eight edges and sizes has a
//! `given' bit, in the four bytes window.h leaves unused after
//! `total_lines': without it the integer is 0 and the field reads as
//! none.  `window_end_pos' has one too: until a redisplay or
//! set_window_buffer stores it, the mode line reads the end of the text
//! being drawn instead.

use super::{HEADER_SIZE, VectorTag, Vectorlike, VectorlikeRef, WORD_SIZE};
use crate::lisp::types::Value;
use std::cell::Cell;
use std::mem::offset_of;

/// window.h's `struct cursor_pos': a cursor's pixel and glyph positions.
#[repr(C)]
#[derive(Default)]
struct CursorPos {
    x: Cell<i32>,
    y: Cell<i32>,
    hpos: Cell<i32>,
    vpos: Cell<i32>,
}

impl std::fmt::Debug for CursorPos {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "({}, {}, {}, {})",
            self.x.get(),
            self.y.get(),
            self.hpos.get(),
            self.vpos.get()
        )
    }
}

/// The payload after the one-word vector header: window.h's struct
/// window from its `frame' on.
#[repr(C)]
pub struct WindowState {
    /// window.h's frame: "The frame this window is on."
    frame: Cell<Value>,
    /// window.h's next and prev: "Following (to right or down) and
    /// preceding (to left or up) child at same level of tree.  Whether
    /// this is left/right or up/down is determined by the parent window."
    next: Cell<Value>,
    prev: Cell<Value>,
    /// window.h's parent: "The window this one is a child of.  For a
    /// root window this is nil."
    parent: Cell<Value>,
    /// window.h's normal_lines and normal_cols: "The normal size of the
    /// window.  These are fractions, but we do not use C doubles to avoid
    /// creating new Lisp_Float objects while interfacing Lisp in
    /// Fwindow_normal_size."
    normal_lines: Cell<Value>,
    normal_cols: Cell<Value>,
    /// window.h's new_total, new_normal and new_pixel: "New sizes of
    /// the window.  Note that Lisp code may set new_normal to something
    /// beyond an integer, so C int can't be used here."
    new_total: Cell<Value>,
    new_normal: Cell<Value>,
    new_pixel: Cell<Value>,
    /// window.h's contents: "For a leaf window or a tooltip window this
    /// is the buffer shown in the window; for a combination window this
    /// is the first of its child windows", a buffer for a minibuffer
    /// window as well; nil for a deleted window.
    contents: Cell<Value>,
    /// window.h's old_buffer: "The old buffer of this window, set to
    /// this window's buffer by run_window_change_functions every time it
    /// sees this window."
    old_buffer: Cell<Value>,
    /// window.h's start: "A marker pointing to where in the text to
    /// start displaying."
    start: Cell<Value>,
    /// window.h's pointm and old_pointm: "A marker pointing to where in
    /// the text point is in this window, used only when the window is
    /// not selected", and its old value as run_window_change_functions
    /// saw it.
    pointm: Cell<Value>,
    old_pointm: Cell<Value>,
    /// window.h's temslot: "No permanent meaning; used by
    /// save-window-excursion's bookkeeping."
    temslot: Cell<Value>,
    /// window.h's vertical_scroll_bar, vertical_scroll_bar_type,
    /// horizontal_scroll_bar and horizontal_scroll_bar_type.
    vertical_scroll_bar: Cell<Value>,
    vertical_scroll_bar_type: Cell<Value>,
    horizontal_scroll_bar: Cell<Value>,
    horizontal_scroll_bar_type: Cell<Value>,
    /// window.h's display_table: "Display-table to use for displaying
    /// chars in this window.  Nil means use the buffer's own display-table."
    display_table: Cell<Value>,
    /// window.h's dedicated: "Non-nil usually means window is marked as
    /// dedicated."
    dedicated: Cell<Value>,
    /// window.h's combination_limit: "If this window is an internal
    /// window, the value of window-combination-limit when the window was
    /// created", and a deleted leaf's buffer.
    combination_limit: Cell<Value>,
    /// window.h's window_parameters: "An alist with parameters."
    window_parameters: Cell<Value>,
    /// window.h's cursor_type: "The cursor type of the window."
    cursor_type: Cell<Value>,
    /// window.h's mode_line_help_echo, the last Lisp field.
    mode_line_help_echo: Cell<Value>,
    /// window.h's current_matrix and desired_matrix, `struct
    /// glyph_matrix *': no glyph matrix here, so null.
    current_matrix: Cell<usize>,
    desired_matrix: Cell<usize>,
    /// window.h's prev_buffers and next_buffers: "The two Lisp_Object
    /// fields below are marked in a special way, which is why they're
    /// placed after `current_matrix'."
    prev_buffers: Cell<Value>,
    next_buffers: Cell<Value>,
    /// window.h's use_time: "Number saying how recently window was
    /// selected."
    use_time: Cell<i64>,
    /// window.h's sequence_number: "Unique number of window assigned
    /// when it was created."  Zero for a window no make_window made, as
    /// the nilled window of a dump is.
    sequence_number: Cell<i64>,
    /// window.h's change_stamp: "The change stamp of this window.  Set to
    /// 0 when the window is created, it is set to its frame's change
    /// stamp every time run_window_change_functions is run on that frame
    /// with this window live."
    change_stamp: Cell<i32>,
    /// window.h's pixel_left and pixel_top: "The upper left corner pixel
    /// coordinates of this window, as integers relative to upper left
    /// corner of frame = 0, 0."
    pixel_left: Cell<i32>,
    pixel_top: Cell<i32>,
    /// window.h's left_col and top_line: "The upper left corner
    /// coordinates of this window, relative to upper left corner of frame
    /// = 0, 0."
    left_col: Cell<i32>,
    top_line: Cell<i32>,
    /// window.h's pixel_width and pixel_height: "The pixel size of the
    /// window."
    pixel_width: Cell<i32>,
    pixel_height: Cell<i32>,
    /// window.h's old_pixel_width, old_pixel_height, old_body_pixel_width
    /// and old_body_pixel_height: "The pixel and pixel body sizes of the
    /// window at the last time run_window_change_functions was run with
    /// this window live."
    old_pixel_width: Cell<i32>,
    old_pixel_height: Cell<i32>,
    old_body_pixel_width: Cell<i32>,
    old_body_pixel_height: Cell<i32>,
    /// window.h's total_cols and total_lines: "The size of the window."
    total_cols: Cell<i32>,
    total_lines: Cell<i32>,
    /// Which of the edges and sizes, and whether window_end_pos, the
    /// window has been given (the module's comment): the bits
    /// `GIVEN_PIXEL_LEFT' and the others, in the four bytes window.h's
    /// layout leaves unused here.
    given: Cell<u32>,
    /// window.h's hscroll: "Number of columns display within the window
    /// is scrolled to the left."
    hscroll: Cell<i64>,
    /// window.h's min_hscroll: "Minimum hscroll for automatic
    /// hscrolling.  This is the value the user has set, by
    /// set-window-hscroll for example."
    min_hscroll: Cell<i64>,
    /// window.h's hscroll_whole: "Maximum line length in pixels within
    /// window bound by size of window (set up by set_horizontal_scroll_bar)."
    hscroll_whole: Cell<i64>,
    /// window.h's last_modified: "Displayed buffer's text modification
    /// events counter as of last time display completed."
    last_modified: Cell<i64>,
    /// window.h's last_overlay_modified: "Displayed buffer's overlays
    /// modification events counter as of last complete update."
    last_overlay_modified: Cell<i64>,
    /// window.h's last_point: "Value of point at that time.  Since this
    /// is a position in a buffer, it should be positive."  Zero where no
    /// complete redisplay has left one.
    last_point: Cell<i64>,
    /// window.h's ephemeral_last_point and last_mark.
    ephemeral_last_point: Cell<i64>,
    last_mark: Cell<i64>,
    /// window.h's base_line_number: "Line number and position of a line
    /// somewhere above the top of the screen.  If this field is zero, it
    /// means we don't have a base line."
    base_line_number: Cell<i64>,
    /// window.h's base_line_pos: "If this field is zero, it means we
    /// don't have a base line.  If it is -1, it means don't display line
    /// number as long as the window shows its buffer."
    base_line_pos: Cell<i64>,
    /// window.h's column_number_displayed: "The column number currently
    /// displayed in this window's mode line, or -1 if column numbers are
    /// not being displayed."
    column_number_displayed: Cell<i64>,
    /// window.h's nrows_scale_factor and ncols_scale_factor: "Scaling
    /// factor for the glyph_matrix size calculation in this window."
    nrows_scale_factor: Cell<i32>,
    ncols_scale_factor: Cell<i32>,
    /// window.h's cursor, phys_cursor and output_cursor: "Intended
    /// cursor position", "Where the cursor actually is" and "Cursor
    /// position of output".
    cursor: CursorPos,
    phys_cursor: CursorPos,
    output_cursor: CursorPos,
    /// window.h's last_cursor_vpos: "Vertical cursor position as of last
    /// update that completed without pause."
    last_cursor_vpos: Cell<i32>,
    /// window.h's phys_cursor_type, phys_cursor_width,
    /// phys_cursor_ascent and phys_cursor_height: the cursor as it is on
    /// the display.
    phys_cursor_type: Cell<i32>,
    phys_cursor_width: Cell<i32>,
    phys_cursor_ascent: Cell<i32>,
    phys_cursor_height: Cell<i32>,
    /// window.h's left_fringe_width and right_fringe_width: "Width of
    /// left and right fringes, in pixels.  A value of -1 means use
    /// frame values."
    left_fringe_width: Cell<i32>,
    right_fringe_width: Cell<i32>,
    /// window.h's left_margin_cols and right_margin_cols: "Requested
    /// width of left and right marginal areas in columns.  A value of 0
    /// means no margin."
    left_margin_cols: Cell<i32>,
    right_margin_cols: Cell<i32>,
    /// window.h's scroll_bar_width and scroll_bar_height: "Pixel width
    /// of scroll bars.  A value of -1 means use frame values."
    scroll_bar_width: Cell<i32>,
    scroll_bar_height: Cell<i32>,
    /// window.h's mode_line_height, header_line_height and
    /// tab_line_height: "Effective height of the mode line, or -1 if not
    /// known", and of the header and tab lines.
    mode_line_height: Cell<i32>,
    header_line_height: Cell<i32>,
    tab_line_height: Cell<i32>,
    /// window.h's window_end_pos: "Z - the buffer position of the last
    /// glyph in the current matrix of W.  Only valid if window_end_valid
    /// is true."
    window_end_pos: Cell<i64>,
    /// window.h's window_end_vpos: "Glyph matrix row of the last glyph
    /// in the current matrix of W.  Only valid if window_end_valid is
    /// true."
    window_end_vpos: Cell<i32>,
    /// window.h's nineteen one-bit fields from `mini' to
    /// `preserve_vscroll_p', bit N of the word the Nth, as GCC lays the
    /// bit-fields out (the `MINI' bit and the others below).
    bits: Cell<u32>,
    /// window.h's vscroll: "Amount by which lines of this window are
    /// scrolled in y-direction (smooth scrolling)."
    vscroll: Cell<i32>,
    /// window.h's window_end_bytepos: "Z_BYTE - buffer position of the
    /// last glyph in the current matrix of W.  Only valid if
    /// window_end_valid is true."
    window_end_bytepos: Cell<i64>,
}

/// The bits of window.h's bit-fields used here: `mini' (bit 0) to
/// `optional_new_start' (6), then after phys_cursor_on_p `cursor_off_p'
/// (8), after last_cursor_off_p, must_be_updated_p, pseudo_window_p,
/// fringes_outside_margins, fringes_persistent and
/// scroll_bars_persistent `window_end_valid' (15), and `redisplay' (16)
/// and `suspend_auto_hscroll' (17) before preserve_vscroll_p (18).
const MINI: u32 = 1 << 0;
const HORIZONTAL: u32 = 1 << 1;
const UPDATE_MODE_LINE: u32 = 1 << 2;
const LAST_HAD_STAR: u32 = 1 << 3;
const START_AT_LINE_BEG: u32 = 1 << 4;
const FORCE_START: u32 = 1 << 5;
const OPTIONAL_NEW_START: u32 = 1 << 6;
const CURSOR_OFF_P: u32 = 1 << 8;
const WINDOW_END_VALID: u32 = 1 << 15;
const REDISPLAY: u32 = 1 << 16;
const SUSPEND_AUTO_HSCROLL: u32 = 1 << 17;

/// The `given' bits, one for each edge and size and one for
/// window_end_pos.
const GIVEN_PIXEL_LEFT: u32 = 1 << 0;
const GIVEN_PIXEL_TOP: u32 = 1 << 1;
const GIVEN_LEFT_COL: u32 = 1 << 2;
const GIVEN_TOP_LINE: u32 = 1 << 3;
const GIVEN_PIXEL_WIDTH: u32 = 1 << 4;
const GIVEN_PIXEL_HEIGHT: u32 = 1 << 5;
const GIVEN_TOTAL_COLS: u32 = 1 << 6;
const GIVEN_TOTAL_LINES: u32 = 1 << 7;
const GIVEN_WINDOW_END_POS: u32 = 1 << 8;

/// PSEUDOVECSIZE (struct window, mode_line_help_echo): the Lisp words
/// before the rest, the header's Lisp size.
pub(crate) const WINDOW_LISP_WORDS: usize =
    (offset_of!(WindowState, mode_line_help_echo) + WORD_SIZE) / WORD_SIZE;

/// VECSIZE (struct window): the words after the header, those
/// allocate_vectorlike conses (`vector_cells_consed += len').
pub(crate) const WINDOW_VECTOR_WORDS: usize = size_of::<WindowState>() / WORD_SIZE;

pub type WindowObjectRef = VectorlikeRef<WindowState>;

// window.h's layout: each field at the offset `ptype /o struct window'
// gives it in the configured GNU build, counted as GNU's are from the
// vector header before the payload, the Lisp fields first, 544 bytes in
// all.
const _: () = {
    assert!(offset_of!(WindowState, frame) + HEADER_SIZE == 8);
    assert!(offset_of!(WindowState, next) + HEADER_SIZE == 16);
    assert!(offset_of!(WindowState, prev) + HEADER_SIZE == 24);
    assert!(offset_of!(WindowState, parent) + HEADER_SIZE == 32);
    assert!(offset_of!(WindowState, normal_lines) + HEADER_SIZE == 40);
    assert!(offset_of!(WindowState, normal_cols) + HEADER_SIZE == 48);
    assert!(offset_of!(WindowState, new_total) + HEADER_SIZE == 56);
    assert!(offset_of!(WindowState, new_normal) + HEADER_SIZE == 64);
    assert!(offset_of!(WindowState, new_pixel) + HEADER_SIZE == 72);
    assert!(offset_of!(WindowState, contents) + HEADER_SIZE == 80);
    assert!(offset_of!(WindowState, old_buffer) + HEADER_SIZE == 88);
    assert!(offset_of!(WindowState, start) + HEADER_SIZE == 96);
    assert!(offset_of!(WindowState, pointm) + HEADER_SIZE == 104);
    assert!(offset_of!(WindowState, old_pointm) + HEADER_SIZE == 112);
    assert!(offset_of!(WindowState, temslot) + HEADER_SIZE == 120);
    assert!(offset_of!(WindowState, vertical_scroll_bar) + HEADER_SIZE == 128);
    assert!(offset_of!(WindowState, vertical_scroll_bar_type) + HEADER_SIZE == 136);
    assert!(offset_of!(WindowState, horizontal_scroll_bar) + HEADER_SIZE == 144);
    assert!(offset_of!(WindowState, horizontal_scroll_bar_type) + HEADER_SIZE == 152);
    assert!(offset_of!(WindowState, display_table) + HEADER_SIZE == 160);
    assert!(offset_of!(WindowState, dedicated) + HEADER_SIZE == 168);
    assert!(offset_of!(WindowState, combination_limit) + HEADER_SIZE == 176);
    assert!(offset_of!(WindowState, window_parameters) + HEADER_SIZE == 184);
    assert!(offset_of!(WindowState, cursor_type) + HEADER_SIZE == 192);
    assert!(offset_of!(WindowState, mode_line_help_echo) + HEADER_SIZE == 200);
    assert!(offset_of!(WindowState, current_matrix) + HEADER_SIZE == 208);
    assert!(offset_of!(WindowState, desired_matrix) + HEADER_SIZE == 216);
    assert!(offset_of!(WindowState, prev_buffers) + HEADER_SIZE == 224);
    assert!(offset_of!(WindowState, next_buffers) + HEADER_SIZE == 232);
    assert!(offset_of!(WindowState, use_time) + HEADER_SIZE == 240);
    assert!(offset_of!(WindowState, sequence_number) + HEADER_SIZE == 248);
    assert!(offset_of!(WindowState, change_stamp) + HEADER_SIZE == 256);
    assert!(offset_of!(WindowState, pixel_left) + HEADER_SIZE == 260);
    assert!(offset_of!(WindowState, pixel_top) + HEADER_SIZE == 264);
    assert!(offset_of!(WindowState, left_col) + HEADER_SIZE == 268);
    assert!(offset_of!(WindowState, top_line) + HEADER_SIZE == 272);
    assert!(offset_of!(WindowState, pixel_width) + HEADER_SIZE == 276);
    assert!(offset_of!(WindowState, pixel_height) + HEADER_SIZE == 280);
    assert!(offset_of!(WindowState, old_pixel_width) + HEADER_SIZE == 284);
    assert!(offset_of!(WindowState, old_pixel_height) + HEADER_SIZE == 288);
    assert!(offset_of!(WindowState, old_body_pixel_width) + HEADER_SIZE == 292);
    assert!(offset_of!(WindowState, old_body_pixel_height) + HEADER_SIZE == 296);
    assert!(offset_of!(WindowState, total_cols) + HEADER_SIZE == 300);
    assert!(offset_of!(WindowState, total_lines) + HEADER_SIZE == 304);
    assert!(offset_of!(WindowState, given) + HEADER_SIZE == 308);
    assert!(offset_of!(WindowState, hscroll) + HEADER_SIZE == 312);
    assert!(offset_of!(WindowState, min_hscroll) + HEADER_SIZE == 320);
    assert!(offset_of!(WindowState, hscroll_whole) + HEADER_SIZE == 328);
    assert!(offset_of!(WindowState, last_modified) + HEADER_SIZE == 336);
    assert!(offset_of!(WindowState, last_overlay_modified) + HEADER_SIZE == 344);
    assert!(offset_of!(WindowState, last_point) + HEADER_SIZE == 352);
    assert!(offset_of!(WindowState, ephemeral_last_point) + HEADER_SIZE == 360);
    assert!(offset_of!(WindowState, last_mark) + HEADER_SIZE == 368);
    assert!(offset_of!(WindowState, base_line_number) + HEADER_SIZE == 376);
    assert!(offset_of!(WindowState, base_line_pos) + HEADER_SIZE == 384);
    assert!(offset_of!(WindowState, column_number_displayed) + HEADER_SIZE == 392);
    assert!(offset_of!(WindowState, nrows_scale_factor) + HEADER_SIZE == 400);
    assert!(offset_of!(WindowState, ncols_scale_factor) + HEADER_SIZE == 404);
    assert!(offset_of!(WindowState, cursor) + HEADER_SIZE == 408);
    assert!(offset_of!(WindowState, phys_cursor) + HEADER_SIZE == 424);
    assert!(offset_of!(WindowState, output_cursor) + HEADER_SIZE == 440);
    assert!(offset_of!(WindowState, last_cursor_vpos) + HEADER_SIZE == 456);
    assert!(offset_of!(WindowState, phys_cursor_type) + HEADER_SIZE == 460);
    assert!(offset_of!(WindowState, phys_cursor_width) + HEADER_SIZE == 464);
    assert!(offset_of!(WindowState, phys_cursor_ascent) + HEADER_SIZE == 468);
    assert!(offset_of!(WindowState, phys_cursor_height) + HEADER_SIZE == 472);
    assert!(offset_of!(WindowState, left_fringe_width) + HEADER_SIZE == 476);
    assert!(offset_of!(WindowState, right_fringe_width) + HEADER_SIZE == 480);
    assert!(offset_of!(WindowState, left_margin_cols) + HEADER_SIZE == 484);
    assert!(offset_of!(WindowState, right_margin_cols) + HEADER_SIZE == 488);
    assert!(offset_of!(WindowState, scroll_bar_width) + HEADER_SIZE == 492);
    assert!(offset_of!(WindowState, scroll_bar_height) + HEADER_SIZE == 496);
    assert!(offset_of!(WindowState, mode_line_height) + HEADER_SIZE == 500);
    assert!(offset_of!(WindowState, header_line_height) + HEADER_SIZE == 504);
    assert!(offset_of!(WindowState, tab_line_height) + HEADER_SIZE == 508);
    assert!(offset_of!(WindowState, window_end_pos) + HEADER_SIZE == 512);
    assert!(offset_of!(WindowState, window_end_vpos) + HEADER_SIZE == 520);
    assert!(offset_of!(WindowState, bits) + HEADER_SIZE == 524);
    assert!(offset_of!(WindowState, vscroll) + HEADER_SIZE == 528);
    assert!(offset_of!(WindowState, window_end_bytepos) + HEADER_SIZE == 536);
    assert!(HEADER_SIZE + size_of::<WindowState>() == 544);
    // PSEUDOVECSIZE and VECSIZE as lisp.h computes them.
    assert!(WINDOW_LISP_WORDS == 25);
    assert!(WINDOW_VECTOR_WORDS == 67);
};

impl Vectorlike for WindowState {
    const TAG: VectorTag = VectorTag::Window;
    const LISP_SLOTS: usize = WINDOW_LISP_WORDS;
}

impl WindowObjectRef {
    /// window.c:allocate_window, ALLOCATE_ZEROED_PSEUDOVECTOR: every
    /// Lisp field nil and the rest zero (no contents, no edge and no
    /// size given, the bits clear), but for the column number displayed,
    /// none (-1) here even before make_window's `w->column_number_displayed
    /// = -1'.  Interpreter::allocate_window tallies its consing, and
    /// Interpreter::make_window gives a window for Lisp the fields
    /// make_window sets.
    pub(crate) fn new() -> Self {
        let nil = || Cell::new(Value::Nil);
        Self::allocate(WindowState {
            frame: nil(),
            next: nil(),
            prev: nil(),
            parent: nil(),
            normal_lines: nil(),
            normal_cols: nil(),
            new_total: nil(),
            new_normal: nil(),
            new_pixel: nil(),
            contents: nil(),
            old_buffer: nil(),
            start: nil(),
            pointm: nil(),
            old_pointm: nil(),
            temslot: nil(),
            vertical_scroll_bar: nil(),
            vertical_scroll_bar_type: nil(),
            horizontal_scroll_bar: nil(),
            horizontal_scroll_bar_type: nil(),
            display_table: nil(),
            dedicated: nil(),
            combination_limit: nil(),
            window_parameters: nil(),
            cursor_type: nil(),
            mode_line_help_echo: nil(),
            current_matrix: Cell::new(0),
            desired_matrix: Cell::new(0),
            prev_buffers: nil(),
            next_buffers: nil(),
            use_time: Cell::new(0),
            sequence_number: Cell::new(0),
            change_stamp: Cell::new(0),
            pixel_left: Cell::new(0),
            pixel_top: Cell::new(0),
            left_col: Cell::new(0),
            top_line: Cell::new(0),
            pixel_width: Cell::new(0),
            pixel_height: Cell::new(0),
            old_pixel_width: Cell::new(0),
            old_pixel_height: Cell::new(0),
            old_body_pixel_width: Cell::new(0),
            old_body_pixel_height: Cell::new(0),
            total_cols: Cell::new(0),
            total_lines: Cell::new(0),
            given: Cell::new(0),
            hscroll: Cell::new(0),
            min_hscroll: Cell::new(0),
            hscroll_whole: Cell::new(0),
            last_modified: Cell::new(0),
            last_overlay_modified: Cell::new(0),
            last_point: Cell::new(0),
            ephemeral_last_point: Cell::new(0),
            last_mark: Cell::new(0),
            base_line_number: Cell::new(0),
            base_line_pos: Cell::new(0),
            column_number_displayed: Cell::new(-1),
            nrows_scale_factor: Cell::new(0),
            ncols_scale_factor: Cell::new(0),
            cursor: CursorPos::default(),
            phys_cursor: CursorPos::default(),
            output_cursor: CursorPos::default(),
            last_cursor_vpos: Cell::new(0),
            phys_cursor_type: Cell::new(0),
            phys_cursor_width: Cell::new(0),
            phys_cursor_ascent: Cell::new(0),
            phys_cursor_height: Cell::new(0),
            left_fringe_width: Cell::new(0),
            right_fringe_width: Cell::new(0),
            left_margin_cols: Cell::new(0),
            right_margin_cols: Cell::new(0),
            scroll_bar_width: Cell::new(0),
            scroll_bar_height: Cell::new(0),
            mode_line_height: Cell::new(0),
            header_line_height: Cell::new(0),
            tab_line_height: Cell::new(0),
            window_end_pos: Cell::new(0),
            window_end_vpos: Cell::new(0),
            bits: Cell::new(0),
            vscroll: Cell::new(0),
            window_end_bytepos: Cell::new(0),
        })
    }
}

/// `w->FIELD' for the Lisp fields named.
macro_rules! lisp_getters {
    ($($field:ident)*) => {
        $(
            #[inline(always)]
            pub(crate) fn $field(&self) -> Value {
                self.$field.get()
            }
        )*
    };
}

/// `wset_FIELD (w, VALUE)' for the Lisp fields named.
macro_rules! lisp_setters {
    ($($set:ident $field:ident;)*) => {
        $(
            #[inline(always)]
            pub(crate) fn $set(&self, value: Value) {
                self.$field.set(value);
            }
        )*
    };
}

/// `w->FIELD' for the integers named, of the type C gives them, read as
/// i64.
macro_rules! integer_getters {
    ($($field:ident)*) => {
        $(
            #[inline(always)]
            pub(crate) fn $field(&self) -> i64 {
                i64::from(self.$field.get())
            }
        )*
    };
}

/// `w->FIELD = VALUE' for the integers named, stored as C's conversion
/// to the field's type stores it (an int keeps the low 32 bits).
macro_rules! integer_setters {
    ($($set:ident $field:ident;)*) => {
        $(
            #[inline(always)]
            pub(crate) fn $set(&self, value: i64) {
                self.$field.set(value as _);
            }
        )*
    };
}

/// `w->FIELD' and `w->FIELD = ON' for the bits.
macro_rules! bit_fields {
    ($($field:ident $set:ident $bit:ident;)*) => {
        $(
            #[inline(always)]
            pub(crate) fn $field(&self) -> bool {
                self.bits.get() & $bit != 0
            }

            #[inline(always)]
            pub(crate) fn $set(&self, on: bool) {
                let bits = self.bits.get();
                self.bits.set(if on { bits | $bit } else { bits & !$bit });
            }
        )*
    };
}

/// An edge or size the window may not have been given: `w->FIELD',
/// None for one it has not, and `w->FIELD = VALUE', None taking it back
/// (the integer is 0 again and its bit clear).
macro_rules! given_fields {
    ($($field:ident $set:ident $bit:ident;)*) => {
        $(
            #[inline(always)]
            pub(crate) fn $field(&self) -> Option<i64> {
                (self.given.get() & $bit != 0).then(|| i64::from(self.$field.get()))
            }

            #[inline(always)]
            pub(crate) fn $set(&self, value: Option<i64>) {
                self.$field.set(value.unwrap_or(0) as _);
                self.give($bit, value.is_some());
            }
        )*
    };
}

impl WindowState {
    lisp_getters! {
        frame next prev parent normal_lines normal_cols new_total new_normal new_pixel
        contents old_buffer start pointm old_pointm vertical_scroll_bar_type
        horizontal_scroll_bar_type display_table dedicated combination_limit
        window_parameters cursor_type prev_buffers next_buffers
    }

    lisp_setters! {
        set_frame frame;
        set_next next;
        set_prev prev;
        set_parent parent;
        set_normal_lines normal_lines;
        set_normal_cols normal_cols;
        set_new_total new_total;
        set_new_normal new_normal;
        set_new_pixel new_pixel;
        set_contents contents;
        set_old_buffer old_buffer;
        set_start start;
        set_pointm pointm;
        set_old_pointm old_pointm;
        set_display_table display_table;
        set_dedicated dedicated;
        set_combination_limit combination_limit;
        set_window_parameters window_parameters;
        set_cursor_type cursor_type;
        set_prev_buffers prev_buffers;
        set_next_buffers next_buffers;
    }

    integer_getters! {
        use_time sequence_number change_stamp old_pixel_width old_pixel_height
        old_body_pixel_width old_body_pixel_height hscroll min_hscroll hscroll_whole
        last_modified last_overlay_modified last_point base_line_number base_line_pos
        column_number_displayed left_fringe_width right_fringe_width left_margin_cols
        right_margin_cols scroll_bar_width scroll_bar_height vscroll
    }

    integer_setters! {
        set_use_time use_time;
        set_sequence_number sequence_number;
        set_change_stamp change_stamp;
        set_old_pixel_width old_pixel_width;
        set_old_pixel_height old_pixel_height;
        set_old_body_pixel_width old_body_pixel_width;
        set_old_body_pixel_height old_body_pixel_height;
        set_hscroll hscroll;
        set_min_hscroll min_hscroll;
        set_last_modified last_modified;
        set_last_overlay_modified last_overlay_modified;
        set_last_point last_point;
        set_base_line_number base_line_number;
        set_base_line_pos base_line_pos;
        set_column_number_displayed column_number_displayed;
        set_left_margin_cols left_margin_cols;
        set_right_margin_cols right_margin_cols;
    }

    bit_fields! {
        mini set_mini MINI;
        horizontal set_horizontal HORIZONTAL;
        update_mode_line set_update_mode_line UPDATE_MODE_LINE;
        last_had_star set_last_had_star LAST_HAD_STAR;
        start_at_line_beg set_start_at_line_beg START_AT_LINE_BEG;
        force_start set_force_start FORCE_START;
        optional_new_start set_optional_new_start OPTIONAL_NEW_START;
        cursor_off_p set_cursor_off_p CURSOR_OFF_P;
        window_end_valid set_window_end_valid WINDOW_END_VALID;
        redisplay set_redisplay REDISPLAY;
        suspend_auto_hscroll set_suspend_auto_hscroll SUSPEND_AUTO_HSCROLL;
    }

    given_fields! {
        pixel_left set_pixel_left GIVEN_PIXEL_LEFT;
        pixel_top set_pixel_top GIVEN_PIXEL_TOP;
        left_col set_left_col GIVEN_LEFT_COL;
        top_line set_top_line GIVEN_TOP_LINE;
        pixel_width set_pixel_width GIVEN_PIXEL_WIDTH;
        pixel_height set_pixel_height GIVEN_PIXEL_HEIGHT;
        total_cols set_total_cols GIVEN_TOTAL_COLS;
        total_lines set_total_lines GIVEN_TOTAL_LINES;
    }

    /// window.c:make_window past allocate_window: "Initialize Lisp data.
    /// Note that allocate_window initializes all Lisp data to nil, so do
    /// it only for slots which should not be nil", the three markers new
    /// ones that point nowhere; then the non-Lisp data make_window gives
    /// a value other than zero (HAVE_WINDOW_SYSTEM's phys_cursor_type
    /// and phys_cursor_width among them, as in the configured build),
    /// but the sequence number, which is the caller's.
    pub(crate) fn initialize_as_make_window(&self) {
        self.normal_lines.set(Value::float(1.0));
        self.normal_cols.set(Value::float(1.0));
        self.new_total.set(Value::Integer(0));
        self.new_normal.set(Value::Integer(0));
        self.new_pixel.set(Value::Integer(0));
        self.start
            .set(Value::Marker(crate::lisp::types::MarkerRef::new()));
        self.pointm
            .set(Value::Marker(crate::lisp::types::MarkerRef::new()));
        self.old_pointm
            .set(Value::Marker(crate::lisp::types::MarkerRef::new()));
        self.vertical_scroll_bar_type.set(Value::T);
        self.horizontal_scroll_bar_type.set(Value::T);
        self.cursor_type.set(Value::T);
        self.prev_buffers.set(Value::Nil);
        self.next_buffers.set(Value::Nil);
        self.nrows_scale_factor.set(1);
        self.ncols_scale_factor.set(1);
        self.left_fringe_width.set(-1);
        self.right_fringe_width.set(-1);
        self.mode_line_height.set(-1);
        self.tab_line_height.set(-1);
        self.header_line_height.set(-1);
        // dispextern.h's NO_CURSOR.
        self.phys_cursor_type.set(-1);
        self.phys_cursor_width.set(-1);
        self.scroll_bar_width.set(-1);
        self.scroll_bar_height.set(-1);
        self.column_number_displayed.set(-1);
    }

    /// make_parent_window's memcpy, for the fields of the rest nothing
    /// here reads or stores yet but make_window: the copy has FROM's.
    pub(crate) fn copy_display_fields(&self, from: &WindowState) {
        self.current_matrix.set(from.current_matrix.get());
        self.desired_matrix.set(from.desired_matrix.get());
        self.hscroll_whole.set(from.hscroll_whole.get());
        self.ephemeral_last_point
            .set(from.ephemeral_last_point.get());
        self.last_mark.set(from.last_mark.get());
        self.nrows_scale_factor.set(from.nrows_scale_factor.get());
        self.ncols_scale_factor.set(from.ncols_scale_factor.get());
        for (cursor, from) in [
            (&self.cursor, &from.cursor),
            (&self.phys_cursor, &from.phys_cursor),
            (&self.output_cursor, &from.output_cursor),
        ] {
            cursor.x.set(from.x.get());
            cursor.y.set(from.y.get());
            cursor.hpos.set(from.hpos.get());
            cursor.vpos.set(from.vpos.get());
        }
        for (field, from) in [
            (&self.last_cursor_vpos, &from.last_cursor_vpos),
            (&self.phys_cursor_type, &from.phys_cursor_type),
            (&self.phys_cursor_width, &from.phys_cursor_width),
            (&self.phys_cursor_ascent, &from.phys_cursor_ascent),
            (&self.phys_cursor_height, &from.phys_cursor_height),
            (&self.left_fringe_width, &from.left_fringe_width),
            (&self.right_fringe_width, &from.right_fringe_width),
            (&self.scroll_bar_width, &from.scroll_bar_width),
            (&self.scroll_bar_height, &from.scroll_bar_height),
            (&self.mode_line_height, &from.mode_line_height),
            (&self.header_line_height, &from.header_line_height),
            (&self.tab_line_height, &from.tab_line_height),
            (&self.window_end_vpos, &from.window_end_vpos),
            (&self.vscroll, &from.vscroll),
        ] {
            field.set(from.get());
        }
        self.window_end_bytepos.set(from.window_end_bytepos.get());
    }

    /// Set or clear BIT of `given'.
    #[inline(always)]
    fn give(&self, bit: u32, on: bool) {
        let given = self.given.get();
        self.given.set(if on { given | bit } else { given & !bit });
    }

    /// w->window_end_pos, None for a window no redisplay and no
    /// set_window_buffer has given one.
    #[inline(always)]
    pub(crate) fn window_end_pos(&self) -> Option<i64> {
        (self.given.get() & GIVEN_WINDOW_END_POS != 0).then(|| self.window_end_pos.get())
    }

    /// `w->window_end_pos = VALUE'; None takes it back.
    #[inline(always)]
    pub(crate) fn set_window_end_pos(&self, value: Option<i64>) {
        self.window_end_pos.set(value.unwrap_or(0));
        self.give(GIVEN_WINDOW_END_POS, value.is_some());
    }

    /// The Lisp fields, window.h's twenty-five and then prev_buffers and
    /// next_buffers, in that order.
    fn lisp_cells(&self) -> [&Cell<Value>; WINDOW_LISP_WORDS + 2] {
        [
            &self.frame,
            &self.next,
            &self.prev,
            &self.parent,
            &self.normal_lines,
            &self.normal_cols,
            &self.new_total,
            &self.new_normal,
            &self.new_pixel,
            &self.contents,
            &self.old_buffer,
            &self.start,
            &self.pointm,
            &self.old_pointm,
            &self.temslot,
            &self.vertical_scroll_bar,
            &self.vertical_scroll_bar_type,
            &self.horizontal_scroll_bar,
            &self.horizontal_scroll_bar_type,
            &self.display_table,
            &self.dedicated,
            &self.combination_limit,
            &self.window_parameters,
            &self.cursor_type,
            &self.mode_line_help_echo,
            &self.prev_buffers,
            &self.next_buffers,
        ]
    }

    /// The Lisp fields' values, in `lisp_cells'' order.
    pub(crate) fn lisp_values(&self) -> [Value; WINDOW_LISP_WORDS + 2] {
        self.lisp_cells().map(Cell::get)
    }

    /// Store VALUES in the Lisp fields, in `lisp_cells'' order.
    pub(crate) fn set_lisp_values(&self, values: [Value; WINDOW_LISP_WORDS + 2]) {
        for (cell, value) in self.lisp_cells().into_iter().zip(values) {
            cell.set(value);
        }
    }

    /// alloc.c:mark_window: mark_vectorlike's walk of the window's Lisp
    /// fields, then prev_buffers and next_buffers, which it marks apart.
    pub(crate) fn visit_lisp_values(&self, visit: &mut impl FnMut(&Value)) {
        for cell in self.lisp_cells() {
            visit(&cell.get());
        }
    }
}

/// Every field the rest of the struct has, by its window.h name.
impl std::fmt::Debug for WindowState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowState")
            .field("current_matrix", &self.current_matrix.get())
            .field("desired_matrix", &self.desired_matrix.get())
            .field("use_time", &self.use_time.get())
            .field("sequence_number", &self.sequence_number.get())
            .field("change_stamp", &self.change_stamp.get())
            .field("pixel_left", &self.pixel_left())
            .field("pixel_top", &self.pixel_top())
            .field("left_col", &self.left_col())
            .field("top_line", &self.top_line())
            .field("pixel_width", &self.pixel_width())
            .field("pixel_height", &self.pixel_height())
            .field("old_pixel_width", &self.old_pixel_width.get())
            .field("old_pixel_height", &self.old_pixel_height.get())
            .field("old_body_pixel_width", &self.old_body_pixel_width.get())
            .field("old_body_pixel_height", &self.old_body_pixel_height.get())
            .field("total_cols", &self.total_cols())
            .field("total_lines", &self.total_lines())
            .field("hscroll", &self.hscroll.get())
            .field("min_hscroll", &self.min_hscroll.get())
            .field("hscroll_whole", &self.hscroll_whole.get())
            .field("last_modified", &self.last_modified.get())
            .field("last_overlay_modified", &self.last_overlay_modified.get())
            .field("last_point", &self.last_point.get())
            .field("ephemeral_last_point", &self.ephemeral_last_point.get())
            .field("last_mark", &self.last_mark.get())
            .field("base_line_number", &self.base_line_number.get())
            .field("base_line_pos", &self.base_line_pos.get())
            .field(
                "column_number_displayed",
                &self.column_number_displayed.get(),
            )
            .field("nrows_scale_factor", &self.nrows_scale_factor.get())
            .field("ncols_scale_factor", &self.ncols_scale_factor.get())
            .field("cursor", &self.cursor)
            .field("phys_cursor", &self.phys_cursor)
            .field("output_cursor", &self.output_cursor)
            .field("last_cursor_vpos", &self.last_cursor_vpos.get())
            .field("phys_cursor_type", &self.phys_cursor_type.get())
            .field("phys_cursor_width", &self.phys_cursor_width.get())
            .field("phys_cursor_ascent", &self.phys_cursor_ascent.get())
            .field("phys_cursor_height", &self.phys_cursor_height.get())
            .field("left_fringe_width", &self.left_fringe_width.get())
            .field("right_fringe_width", &self.right_fringe_width.get())
            .field("left_margin_cols", &self.left_margin_cols.get())
            .field("right_margin_cols", &self.right_margin_cols.get())
            .field("scroll_bar_width", &self.scroll_bar_width.get())
            .field("scroll_bar_height", &self.scroll_bar_height.get())
            .field("mode_line_height", &self.mode_line_height.get())
            .field("header_line_height", &self.header_line_height.get())
            .field("tab_line_height", &self.tab_line_height.get())
            .field("window_end_pos", &self.window_end_pos())
            .field("window_end_vpos", &self.window_end_vpos.get())
            .field("bits", &format_args!("{:#x}", self.bits.get()))
            .field("vscroll", &self.vscroll.get())
            .field("window_end_bytepos", &self.window_end_bytepos.get())
            .finish_non_exhaustive()
    }
}
