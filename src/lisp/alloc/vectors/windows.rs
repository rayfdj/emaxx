//! window.h:struct window, PVEC_WINDOW: a window's Lisp words in the
//! object itself, traced as alloc.c:mark_window traces its Lisp fields.
//!
//! `contents' is window.h's field of that name, and the fields after the
//! Lisp words are window.h's integers and bits by their names: `use_time',
//! `sequence_number' and `change_stamp', the edges and sizes from
//! `pixel_left' to `total_lines' and the two margins, the scrolling and
//! what the last redisplay left from `hscroll' to `window_end_pos', and
//! the bits from `mini' to `suspend_auto_hscroll'.  The other words are
//! still the slots the `WINDOW_*_SLOT' constants of primitives/window.rs
//! number, not yet window.h's fields by name.  A window holds the first
//! `held' of them: a slot past that count is one the window has never
//! been given (it reads as absent, where a held slot reads as its value,
//! nil included), and a store that needs it raises the count.  The id is
//! the name the interpreter's side tables still know the window by, the
//! owner whose id space it is in.
//!
//! window.h's integers are C ints, which have no state for "never
//! assigned": window.c assigns a window's edges and sizes where it makes
//! the window.  The windows here are not yet all made so (a new window is
//! given its pixel edges and sizes and reads its columns and lines from
//! them until window.c's resizing assigns those, and the dead window of a
//! dump is given nothing), so each of the eight edges and sizes has a
//! `given' bit beside it: without it the integer is 0 and the field reads
//! as none.  `window_end_pos' has one too: until a redisplay or
//! set_window_buffer stores it, the mode line reads the end of the text
//! being drawn instead.  The other integers start as make_window leaves
//! them: zero, and `column_number_displayed' -1.

use super::{VectorTag, Vectorlike, VectorlikeRef};
use crate::lisp::types::Value;
use std::cell::Cell;

/// How many Lisp slots a window has, beside its contents.
pub(crate) const WINDOW_SLOT_COUNT: usize = 21;

/// The window's Lisp words: its contents and its slots.
pub(crate) const WINDOW_LISP_WORDS: usize = 1 + WINDOW_SLOT_COUNT;

/// window.c allocates struct window as a full pseudovector.  Its
/// configured VECSIZE is 66 words, including the header word: what
/// alloc.c:sweep_vectors counts for a window, and what make_window
/// tallies as consed for one.
pub(crate) const WINDOW_GNU_VECTOR_SLOTS: usize = 66;

/// The windows' name in the interpreter's type index, by which every
/// window is enumerated.
pub(crate) const WINDOW_TYPE_NAME: &str = "window";

/// The payload after the one-word vector header: the Lisp words first,
/// as a pseudovector's traced words are, then the fields that are not
/// Lisp objects.
#[repr(C)]
pub struct WindowState {
    /// window.h's contents: "For a leaf window or a tooltip window this
    /// is the buffer shown in the window; for a combination window this
    /// is the first of its child windows", a buffer for a minibuffer
    /// window as well; nil for a deleted window.
    contents: Cell<Value>,
    slots: [Cell<Value>; WINDOW_SLOT_COUNT],
    id: u64,
    owner: u32,
    /// How many of the slots the window holds, from the first.
    held: Cell<u32>,
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
    change_stamp: Cell<i64>,
    /// window.h's pixel_left and pixel_top: "The upper left corner pixel
    /// coordinates of this window, as integers relative to upper left
    /// corner of frame = 0, 0."
    pixel_left: Cell<i64>,
    pixel_top: Cell<i64>,
    /// window.h's left_col and top_line: "The upper left corner
    /// coordinates of this window, relative to upper left corner of frame
    /// = 0, 0."
    left_col: Cell<i64>,
    top_line: Cell<i64>,
    /// window.h's pixel_width and pixel_height: "The pixel size of the
    /// window."
    pixel_width: Cell<i64>,
    pixel_height: Cell<i64>,
    /// window.h's old_pixel_width, old_pixel_height, old_body_pixel_width
    /// and old_body_pixel_height: "The pixel and pixel body sizes of the
    /// window at the last time run_window_change_functions was run with
    /// this window live."
    old_pixel_width: Cell<i64>,
    old_pixel_height: Cell<i64>,
    old_body_pixel_width: Cell<i64>,
    old_body_pixel_height: Cell<i64>,
    /// window.h's total_cols and total_lines: "The size of the window."
    total_cols: Cell<i64>,
    total_lines: Cell<i64>,
    /// window.h's hscroll: "Number of columns display within the window
    /// is scrolled to the left."
    hscroll: Cell<i64>,
    /// window.h's min_hscroll: "Minimum hscroll for automatic
    /// hscrolling.  This is the value the user has set, by
    /// set-window-hscroll for example."
    min_hscroll: Cell<i64>,
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
    /// window.h's column_number_displayed: "The column number currently
    /// displayed in this window's mode line, or -1 if column numbers are
    /// not being displayed."
    column_number_displayed: Cell<i64>,
    /// window.h's left_margin_cols and right_margin_cols: "Requested
    /// width of left and right marginal areas in columns.  A value of 0
    /// means no margin."
    left_margin_cols: Cell<i64>,
    right_margin_cols: Cell<i64>,
    /// window.h's window_end_pos: "Z - the buffer position of the last
    /// glyph in the current matrix of W.  Only valid if window_end_valid
    /// is true."
    window_end_pos: Cell<i64>,
    /// window.h's mini: "True if this window is a minibuffer window."
    mini: Cell<bool>,
    /// window.h's horizontal: "Meaningful for internal windows only:
    /// true if this window is a horizontal combination, false if it is a
    /// vertical combination."
    horizontal: Cell<bool>,
    /// window.h's update_mode_line: "True means must regenerate mode
    /// line of this window."
    update_mode_line: Cell<bool>,
    /// window.h's last_had_star: "True if the buffer was "modified" when
    /// the window was last updated."
    last_had_star: Cell<bool>,
    /// window.h's start_at_line_beg: "True means current value of
    /// `start' was the beginning of a line when it was chosen."
    start_at_line_beg: Cell<bool>,
    /// window.h's force_start: "True means next redisplay must use the
    /// value of start set up for it in advance.  Set by scrolling
    /// commands."
    force_start: Cell<bool>,
    /// window.h's optional_new_start: "True means we have explicitly
    /// changed the value of start, but that the next redisplay is not
    /// obliged to use the new value."
    optional_new_start: Cell<bool>,
    /// window.h's cursor_off_p: "False means cursor is logically on,
    /// true means it's off."
    cursor_off_p: Cell<bool>,
    /// window.h's window_end_valid: "True if window_end_pos and
    /// window_end_vpos are truly valid."
    window_end_valid: Cell<bool>,
    /// window.h's redisplay: "True if it needs to be redisplayed."
    redisplay: Cell<bool>,
    /// window.h's suspend_auto_hscroll: "True if auto hscrolling is
    /// currently suspended in this window."
    suspend_auto_hscroll: Cell<bool>,
    /// Whether the window has been given each of its edges and sizes
    /// (the module's comment): the bit of the integer of that name.
    pixel_left_given: Cell<bool>,
    pixel_top_given: Cell<bool>,
    left_col_given: Cell<bool>,
    top_line_given: Cell<bool>,
    pixel_width_given: Cell<bool>,
    pixel_height_given: Cell<bool>,
    total_cols_given: Cell<bool>,
    total_lines_given: Cell<bool>,
    /// Whether the window has been given its window_end_pos.
    window_end_pos_given: Cell<bool>,
}

/// How many words of a window follow its Lisp words: the id, the owner
/// and held count, window.h's twenty-four integers from use_time to
/// window_end_pos, and three words of bits.
pub(crate) const WINDOW_REST_WORDS: usize = 29;

pub type WindowObjectRef = VectorlikeRef<WindowState>;

const _: () = {
    let rest = 8 * WINDOW_LISP_WORDS;
    assert!(size_of::<WindowState>() == rest + 8 * WINDOW_REST_WORDS);
    assert!(std::mem::offset_of!(WindowState, contents) == 0);
    assert!(std::mem::offset_of!(WindowState, slots) == 8);
    assert!(std::mem::offset_of!(WindowState, id) == rest);
    assert!(std::mem::offset_of!(WindowState, owner) == rest + 8);
    assert!(std::mem::offset_of!(WindowState, held) == rest + 12);
    assert!(std::mem::offset_of!(WindowState, use_time) == rest + 16);
    assert!(std::mem::offset_of!(WindowState, sequence_number) == rest + 24);
    // window.h's order, from change_stamp to window_end_pos.
    assert!(std::mem::offset_of!(WindowState, change_stamp) == rest + 32);
    assert!(std::mem::offset_of!(WindowState, pixel_left) == rest + 40);
    assert!(std::mem::offset_of!(WindowState, pixel_top) == rest + 48);
    assert!(std::mem::offset_of!(WindowState, left_col) == rest + 56);
    assert!(std::mem::offset_of!(WindowState, top_line) == rest + 64);
    assert!(std::mem::offset_of!(WindowState, pixel_width) == rest + 72);
    assert!(std::mem::offset_of!(WindowState, pixel_height) == rest + 80);
    assert!(std::mem::offset_of!(WindowState, old_pixel_width) == rest + 88);
    assert!(std::mem::offset_of!(WindowState, old_pixel_height) == rest + 96);
    assert!(std::mem::offset_of!(WindowState, old_body_pixel_width) == rest + 104);
    assert!(std::mem::offset_of!(WindowState, old_body_pixel_height) == rest + 112);
    assert!(std::mem::offset_of!(WindowState, total_cols) == rest + 120);
    assert!(std::mem::offset_of!(WindowState, total_lines) == rest + 128);
    assert!(std::mem::offset_of!(WindowState, hscroll) == rest + 136);
    assert!(std::mem::offset_of!(WindowState, min_hscroll) == rest + 144);
    assert!(std::mem::offset_of!(WindowState, last_modified) == rest + 152);
    assert!(std::mem::offset_of!(WindowState, last_overlay_modified) == rest + 160);
    assert!(std::mem::offset_of!(WindowState, last_point) == rest + 168);
    assert!(std::mem::offset_of!(WindowState, column_number_displayed) == rest + 176);
    assert!(std::mem::offset_of!(WindowState, left_margin_cols) == rest + 184);
    assert!(std::mem::offset_of!(WindowState, right_margin_cols) == rest + 192);
    assert!(std::mem::offset_of!(WindowState, window_end_pos) == rest + 200);
    // The bits in window.h's order, then those of the given integers.
    assert!(std::mem::offset_of!(WindowState, mini) == rest + 208);
    assert!(std::mem::offset_of!(WindowState, horizontal) == rest + 209);
    assert!(std::mem::offset_of!(WindowState, update_mode_line) == rest + 210);
    assert!(std::mem::offset_of!(WindowState, last_had_star) == rest + 211);
    assert!(std::mem::offset_of!(WindowState, start_at_line_beg) == rest + 212);
    assert!(std::mem::offset_of!(WindowState, force_start) == rest + 213);
    assert!(std::mem::offset_of!(WindowState, optional_new_start) == rest + 214);
    assert!(std::mem::offset_of!(WindowState, cursor_off_p) == rest + 215);
    assert!(std::mem::offset_of!(WindowState, window_end_valid) == rest + 216);
    assert!(std::mem::offset_of!(WindowState, redisplay) == rest + 217);
    assert!(std::mem::offset_of!(WindowState, suspend_auto_hscroll) == rest + 218);
    assert!(std::mem::offset_of!(WindowState, pixel_left_given) == rest + 219);
    assert!(std::mem::offset_of!(WindowState, total_lines_given) == rest + 226);
    assert!(std::mem::offset_of!(WindowState, window_end_pos_given) == rest + 227);
};

impl Vectorlike for WindowState {
    const TAG: VectorTag = VectorTag::Window;
    const LISP_SLOTS: usize = WINDOW_LISP_WORDS;
}

impl WindowObjectRef {
    /// window.c:make_window's allocate_window: a window of ID in OWNER's
    /// id space that holds HELD slots, each nil, with no contents, its
    /// integers zero and its bits clear: a window given no edge and no
    /// size, with no column number displayed (make_window's
    /// `w->column_number_displayed = -1').  The caller that makes one
    /// for Lisp tallies its consing (`WINDOW_GNU_VECTOR_SLOTS') and
    /// gives it its sequence number.
    pub(crate) fn new(id: u64, owner: u32, held: usize) -> Self {
        assert!(held <= WINDOW_SLOT_COUNT, "a window holds its slots");
        Self::allocate(WindowState {
            contents: Cell::new(Value::Nil),
            slots: [const { Cell::new(Value::Nil) }; WINDOW_SLOT_COUNT],
            id,
            owner,
            held: Cell::new(held as u32),
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
            hscroll: Cell::new(0),
            min_hscroll: Cell::new(0),
            last_modified: Cell::new(0),
            last_overlay_modified: Cell::new(0),
            last_point: Cell::new(0),
            column_number_displayed: Cell::new(-1),
            left_margin_cols: Cell::new(0),
            right_margin_cols: Cell::new(0),
            window_end_pos: Cell::new(0),
            mini: Cell::new(false),
            horizontal: Cell::new(false),
            update_mode_line: Cell::new(false),
            last_had_star: Cell::new(false),
            start_at_line_beg: Cell::new(false),
            force_start: Cell::new(false),
            optional_new_start: Cell::new(false),
            cursor_off_p: Cell::new(false),
            window_end_valid: Cell::new(false),
            redisplay: Cell::new(false),
            suspend_auto_hscroll: Cell::new(false),
            pixel_left_given: Cell::new(false),
            pixel_top_given: Cell::new(false),
            left_col_given: Cell::new(false),
            top_line_given: Cell::new(false),
            pixel_width_given: Cell::new(false),
            pixel_height_given: Cell::new(false),
            total_cols_given: Cell::new(false),
            total_lines_given: Cell::new(false),
            window_end_pos_given: Cell::new(false),
        })
    }
}

impl WindowState {
    /// The id the side tables know the window by.
    #[inline(always)]
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// The interpreter whose id space the id is in.
    #[inline(always)]
    pub(crate) fn owner(&self) -> u32 {
        self.owner
    }

    /// w->contents.
    #[inline(always)]
    pub(crate) fn contents(&self) -> Value {
        self.contents.get()
    }

    /// `w->contents = VALUE', the store of window.c's wset_buffer and
    /// wset_combination.
    #[inline(always)]
    pub(crate) fn set_contents(&self, value: Value) {
        self.contents.set(value);
    }

    /// w->mini.
    #[inline(always)]
    pub(crate) fn mini(&self) -> bool {
        self.mini.get()
    }

    #[inline(always)]
    pub(crate) fn set_mini(&self, mini: bool) {
        self.mini.set(mini);
    }

    /// w->horizontal.
    #[inline(always)]
    pub(crate) fn horizontal(&self) -> bool {
        self.horizontal.get()
    }

    #[inline(always)]
    pub(crate) fn set_horizontal(&self, horizontal: bool) {
        self.horizontal.set(horizontal);
    }

    /// w->use_time.
    #[inline(always)]
    pub(crate) fn use_time(&self) -> i64 {
        self.use_time.get()
    }

    #[inline(always)]
    pub(crate) fn set_use_time(&self, use_time: i64) {
        self.use_time.set(use_time);
    }

    /// w->sequence_number.
    #[inline(always)]
    pub(crate) fn sequence_number(&self) -> i64 {
        self.sequence_number.get()
    }

    /// `w->sequence_number = NUMBER', make_window's and
    /// make_parent_window's `++sequence_number'.
    #[inline(always)]
    pub(crate) fn set_sequence_number(&self, number: i64) {
        self.sequence_number.set(number);
    }

    /// w->cursor_off_p.
    #[inline(always)]
    pub(crate) fn cursor_off_p(&self) -> bool {
        self.cursor_off_p.get()
    }

    #[inline(always)]
    pub(crate) fn set_cursor_off_p(&self, off: bool) {
        self.cursor_off_p.set(off);
    }

    /// An edge or size the window has been given, None for one it has
    /// not.
    #[inline(always)]
    fn given(value: &Cell<i64>, given: &Cell<bool>) -> Option<i64> {
        given.get().then(|| value.get())
    }

    /// Give the window an edge or size, or with None take it back: the
    /// integer is 0 again and its bit clear.
    #[inline(always)]
    fn give(value: &Cell<i64>, given: &Cell<bool>, new: Option<i64>) {
        value.set(new.unwrap_or(0));
        given.set(new.is_some());
    }

    /// w->pixel_left, None for a window not given one.
    #[inline(always)]
    pub(crate) fn pixel_left(&self) -> Option<i64> {
        Self::given(&self.pixel_left, &self.pixel_left_given)
    }

    /// `w->pixel_left = VALUE'; None takes it back.
    #[inline(always)]
    pub(crate) fn set_pixel_left(&self, value: Option<i64>) {
        Self::give(&self.pixel_left, &self.pixel_left_given, value);
    }

    /// w->pixel_top, None for a window not given one.
    #[inline(always)]
    pub(crate) fn pixel_top(&self) -> Option<i64> {
        Self::given(&self.pixel_top, &self.pixel_top_given)
    }

    /// `w->pixel_top = VALUE'; None takes it back.
    #[inline(always)]
    pub(crate) fn set_pixel_top(&self, value: Option<i64>) {
        Self::give(&self.pixel_top, &self.pixel_top_given, value);
    }

    /// w->left_col, None for a window not given one.
    #[inline(always)]
    pub(crate) fn left_col(&self) -> Option<i64> {
        Self::given(&self.left_col, &self.left_col_given)
    }

    /// `w->left_col = VALUE'; None takes it back.
    #[inline(always)]
    pub(crate) fn set_left_col(&self, value: Option<i64>) {
        Self::give(&self.left_col, &self.left_col_given, value);
    }

    /// w->top_line, None for a window not given one.
    #[inline(always)]
    pub(crate) fn top_line(&self) -> Option<i64> {
        Self::given(&self.top_line, &self.top_line_given)
    }

    /// `w->top_line = VALUE'; None takes it back.
    #[inline(always)]
    pub(crate) fn set_top_line(&self, value: Option<i64>) {
        Self::give(&self.top_line, &self.top_line_given, value);
    }

    /// w->pixel_width, None for a window not given one.
    #[inline(always)]
    pub(crate) fn pixel_width(&self) -> Option<i64> {
        Self::given(&self.pixel_width, &self.pixel_width_given)
    }

    /// `w->pixel_width = VALUE'; None takes it back.
    #[inline(always)]
    pub(crate) fn set_pixel_width(&self, value: Option<i64>) {
        Self::give(&self.pixel_width, &self.pixel_width_given, value);
    }

    /// w->pixel_height, None for a window not given one.
    #[inline(always)]
    pub(crate) fn pixel_height(&self) -> Option<i64> {
        Self::given(&self.pixel_height, &self.pixel_height_given)
    }

    /// `w->pixel_height = VALUE'; None takes it back.
    #[inline(always)]
    pub(crate) fn set_pixel_height(&self, value: Option<i64>) {
        Self::give(&self.pixel_height, &self.pixel_height_given, value);
    }

    /// w->total_cols, None for a window not given one.
    #[inline(always)]
    pub(crate) fn total_cols(&self) -> Option<i64> {
        Self::given(&self.total_cols, &self.total_cols_given)
    }

    /// `w->total_cols = VALUE'; None takes it back.
    #[inline(always)]
    pub(crate) fn set_total_cols(&self, value: Option<i64>) {
        Self::give(&self.total_cols, &self.total_cols_given, value);
    }

    /// w->total_lines, None for a window not given one.
    #[inline(always)]
    pub(crate) fn total_lines(&self) -> Option<i64> {
        Self::given(&self.total_lines, &self.total_lines_given)
    }

    /// `w->total_lines = VALUE'; None takes it back.
    #[inline(always)]
    pub(crate) fn set_total_lines(&self, value: Option<i64>) {
        Self::give(&self.total_lines, &self.total_lines_given, value);
    }

    /// w->old_pixel_width.
    #[inline(always)]
    pub(crate) fn old_pixel_width(&self) -> i64 {
        self.old_pixel_width.get()
    }

    /// `w->old_pixel_width = VALUE'.
    #[inline(always)]
    pub(crate) fn set_old_pixel_width(&self, value: i64) {
        self.old_pixel_width.set(value);
    }

    /// w->old_pixel_height.
    #[inline(always)]
    pub(crate) fn old_pixel_height(&self) -> i64 {
        self.old_pixel_height.get()
    }

    /// `w->old_pixel_height = VALUE'.
    #[inline(always)]
    pub(crate) fn set_old_pixel_height(&self, value: i64) {
        self.old_pixel_height.set(value);
    }

    /// w->old_body_pixel_width.
    #[inline(always)]
    pub(crate) fn old_body_pixel_width(&self) -> i64 {
        self.old_body_pixel_width.get()
    }

    /// `w->old_body_pixel_width = VALUE'.
    #[inline(always)]
    pub(crate) fn set_old_body_pixel_width(&self, value: i64) {
        self.old_body_pixel_width.set(value);
    }

    /// w->old_body_pixel_height.
    #[inline(always)]
    pub(crate) fn old_body_pixel_height(&self) -> i64 {
        self.old_body_pixel_height.get()
    }

    /// `w->old_body_pixel_height = VALUE'.
    #[inline(always)]
    pub(crate) fn set_old_body_pixel_height(&self, value: i64) {
        self.old_body_pixel_height.set(value);
    }

    /// w->left_margin_cols, 0 for no margin.
    #[inline(always)]
    pub(crate) fn left_margin_cols(&self) -> i64 {
        self.left_margin_cols.get()
    }

    /// `w->left_margin_cols = VALUE'.
    #[inline(always)]
    pub(crate) fn set_left_margin_cols(&self, value: i64) {
        self.left_margin_cols.set(value);
    }

    /// w->right_margin_cols, 0 for no margin.
    #[inline(always)]
    pub(crate) fn right_margin_cols(&self) -> i64 {
        self.right_margin_cols.get()
    }

    /// `w->right_margin_cols = VALUE'.
    #[inline(always)]
    pub(crate) fn set_right_margin_cols(&self, value: i64) {
        self.right_margin_cols.set(value);
    }

    /// w->change_stamp.
    #[inline(always)]
    pub(crate) fn change_stamp(&self) -> i64 {
        self.change_stamp.get()
    }

    /// `w->change_stamp = VALUE'.
    #[inline(always)]
    pub(crate) fn set_change_stamp(&self, value: i64) {
        self.change_stamp.set(value);
    }

    /// w->hscroll.
    #[inline(always)]
    pub(crate) fn hscroll(&self) -> i64 {
        self.hscroll.get()
    }

    /// `w->hscroll = VALUE'.
    #[inline(always)]
    pub(crate) fn set_hscroll(&self, value: i64) {
        self.hscroll.set(value);
    }

    /// w->min_hscroll.
    #[inline(always)]
    pub(crate) fn min_hscroll(&self) -> i64 {
        self.min_hscroll.get()
    }

    /// `w->min_hscroll = VALUE'.
    #[inline(always)]
    pub(crate) fn set_min_hscroll(&self, value: i64) {
        self.min_hscroll.set(value);
    }

    /// w->last_modified.
    #[inline(always)]
    pub(crate) fn last_modified(&self) -> i64 {
        self.last_modified.get()
    }

    /// `w->last_modified = VALUE'.
    #[inline(always)]
    pub(crate) fn set_last_modified(&self, value: i64) {
        self.last_modified.set(value);
    }

    /// w->last_overlay_modified.
    #[inline(always)]
    pub(crate) fn last_overlay_modified(&self) -> i64 {
        self.last_overlay_modified.get()
    }

    /// `w->last_overlay_modified = VALUE'.
    #[inline(always)]
    pub(crate) fn set_last_overlay_modified(&self, value: i64) {
        self.last_overlay_modified.set(value);
    }

    /// w->last_point, 0 for none.
    #[inline(always)]
    pub(crate) fn last_point(&self) -> i64 {
        self.last_point.get()
    }

    /// `w->last_point = VALUE'.
    #[inline(always)]
    pub(crate) fn set_last_point(&self, value: i64) {
        self.last_point.set(value);
    }

    /// w->column_number_displayed, -1 for none.
    #[inline(always)]
    pub(crate) fn column_number_displayed(&self) -> i64 {
        self.column_number_displayed.get()
    }

    /// `w->column_number_displayed = VALUE'.
    #[inline(always)]
    pub(crate) fn set_column_number_displayed(&self, value: i64) {
        self.column_number_displayed.set(value);
    }

    /// w->window_end_pos, None for a window no redisplay and no
    /// set_window_buffer has given one.
    #[inline(always)]
    pub(crate) fn window_end_pos(&self) -> Option<i64> {
        Self::given(&self.window_end_pos, &self.window_end_pos_given)
    }

    /// `w->window_end_pos = VALUE'; None takes it back.
    #[inline(always)]
    pub(crate) fn set_window_end_pos(&self, value: Option<i64>) {
        Self::give(&self.window_end_pos, &self.window_end_pos_given, value);
    }

    /// w->update_mode_line.
    #[inline(always)]
    pub(crate) fn update_mode_line(&self) -> bool {
        self.update_mode_line.get()
    }

    /// `w->update_mode_line = ON'.
    #[inline(always)]
    pub(crate) fn set_update_mode_line(&self, on: bool) {
        self.update_mode_line.set(on);
    }

    /// w->last_had_star.
    #[inline(always)]
    pub(crate) fn last_had_star(&self) -> bool {
        self.last_had_star.get()
    }

    /// `w->last_had_star = ON'.
    #[inline(always)]
    pub(crate) fn set_last_had_star(&self, on: bool) {
        self.last_had_star.set(on);
    }

    /// w->start_at_line_beg.
    #[inline(always)]
    pub(crate) fn start_at_line_beg(&self) -> bool {
        self.start_at_line_beg.get()
    }

    /// `w->start_at_line_beg = ON'.
    #[inline(always)]
    pub(crate) fn set_start_at_line_beg(&self, on: bool) {
        self.start_at_line_beg.set(on);
    }

    /// w->force_start.
    #[inline(always)]
    pub(crate) fn force_start(&self) -> bool {
        self.force_start.get()
    }

    /// `w->force_start = ON'.
    #[inline(always)]
    pub(crate) fn set_force_start(&self, on: bool) {
        self.force_start.set(on);
    }

    /// w->optional_new_start.
    #[inline(always)]
    pub(crate) fn optional_new_start(&self) -> bool {
        self.optional_new_start.get()
    }

    /// `w->optional_new_start = ON'.
    #[inline(always)]
    pub(crate) fn set_optional_new_start(&self, on: bool) {
        self.optional_new_start.set(on);
    }

    /// w->window_end_valid.
    #[inline(always)]
    pub(crate) fn window_end_valid(&self) -> bool {
        self.window_end_valid.get()
    }

    /// `w->window_end_valid = ON'.
    #[inline(always)]
    pub(crate) fn set_window_end_valid(&self, on: bool) {
        self.window_end_valid.set(on);
    }

    /// w->redisplay.
    #[inline(always)]
    pub(crate) fn redisplay(&self) -> bool {
        self.redisplay.get()
    }

    /// `w->redisplay = ON'.
    #[inline(always)]
    pub(crate) fn set_redisplay(&self, on: bool) {
        self.redisplay.set(on);
    }

    /// w->suspend_auto_hscroll.
    #[inline(always)]
    pub(crate) fn suspend_auto_hscroll(&self) -> bool {
        self.suspend_auto_hscroll.get()
    }

    /// `w->suspend_auto_hscroll = ON'.
    #[inline(always)]
    pub(crate) fn set_suspend_auto_hscroll(&self, on: bool) {
        self.suspend_auto_hscroll.set(on);
    }

    /// How many slots the window holds.
    #[inline(always)]
    pub(crate) fn held(&self) -> usize {
        self.held.get() as usize
    }

    /// The slots the window holds.
    #[inline(always)]
    fn held_cells(&self) -> &[Cell<Value>] {
        &self.slots[..self.held()]
    }

    /// Slot INDEX, None where the window holds no such slot.
    #[inline(always)]
    pub(crate) fn slot(&self, index: usize) -> Option<Value> {
        self.held_cells().get(index).map(Cell::get)
    }

    /// The window holds slot INDEX from here on: the slots it gains are
    /// nil, as every slot it does not hold is.
    #[inline(always)]
    pub(crate) fn hold(&self, index: usize) {
        assert!(index < WINDOW_SLOT_COUNT, "a window has slot {index}");
        if self.held() <= index {
            self.held.set(index as u32 + 1);
        }
    }

    /// Store VALUE in slot INDEX of a window that holds it; one that
    /// does not is a bug (the index is outside the held slots).
    #[inline(always)]
    pub(crate) fn set_held(&self, index: usize, value: Value) {
        self.held_cells()[index].set(value);
    }

    /// Store VALUE in slot INDEX where the window holds that slot, and
    /// nothing where it does not; whether it was stored.
    #[inline(always)]
    pub(crate) fn set_if_held(&self, index: usize, value: Value) -> bool {
        match self.held_cells().get(index) {
            Some(slot) => {
                slot.set(value);
                true
            }
            None => false,
        }
    }

    /// The held slots' values, in order.
    pub(crate) fn held_values(&self) -> impl ExactSizeIterator<Item = Value> + '_ {
        self.held_cells().iter().map(Cell::get)
    }

    /// alloc.c:mark_window's mark_vectorlike: every Lisp word of the
    /// window, its contents and each slot, held or not (a slot the
    /// window does not hold is nil).
    pub(crate) fn visit_lisp_values(&self, visit: &mut impl FnMut(&Value)) {
        visit(&self.contents.get());
        for slot in &self.slots {
            visit(&slot.get());
        }
    }
}

impl std::fmt::Debug for WindowState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowState")
            .field("id", &self.id)
            .field("owner", &self.owner)
            .field("held", &self.held.get())
            .field("use_time", &self.use_time.get())
            .field("sequence_number", &self.sequence_number.get())
            .field("pixel_left", &self.pixel_left())
            .field("pixel_top", &self.pixel_top())
            .field("left_col", &self.left_col())
            .field("top_line", &self.top_line())
            .field("pixel_width", &self.pixel_width())
            .field("pixel_height", &self.pixel_height())
            .field("total_cols", &self.total_cols())
            .field("total_lines", &self.total_lines())
            .field("mini", &self.mini.get())
            .field("horizontal", &self.horizontal.get())
            .field("cursor_off_p", &self.cursor_off_p.get())
            .finish_non_exhaustive()
    }
}
