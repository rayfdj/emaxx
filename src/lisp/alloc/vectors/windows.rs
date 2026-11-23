//! window.h:struct window, PVEC_WINDOW: a window's Lisp words in the
//! object itself, traced as alloc.c:mark_window traces its Lisp fields.
//!
//! `contents' is window.h's field of that name, `use_time' and
//! `sequence_number' its integers, and `mini', `horizontal' and
//! `cursor_off_p' its bits.  The other words are still the slots the
//! `WINDOW_*_SLOT' constants of primitives/window.rs number, not yet
//! window.h's fields by name.  A window holds the first `held' of them: a
//! slot past that count is one the window has never been given (it reads
//! as absent, where a held slot reads as its value, nil included), and a
//! store that needs it raises the count.  The id is the name the
//! interpreter's side tables still know the window by, the owner whose id
//! space it is in.

use super::{VectorTag, Vectorlike, VectorlikeRef};
use crate::lisp::types::Value;
use std::cell::Cell;

/// How many Lisp slots a window has, beside its contents.
pub(crate) const WINDOW_SLOT_COUNT: usize = 51;

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
    /// window.h's mini: "True if this window is a minibuffer window."
    mini: Cell<bool>,
    /// window.h's horizontal: "Meaningful for internal windows only:
    /// true if this window is a horizontal combination, false if it is a
    /// vertical combination."
    horizontal: Cell<bool>,
    /// window.h's cursor_off_p: "False means cursor is logically on,
    /// true means it's off."
    cursor_off_p: Cell<bool>,
}

pub type WindowObjectRef = VectorlikeRef<WindowState>;

const _: () = {
    assert!(size_of::<WindowState>() == 8 * (WINDOW_LISP_WORDS + 5));
    assert!(std::mem::offset_of!(WindowState, contents) == 0);
    assert!(std::mem::offset_of!(WindowState, slots) == 8);
    assert!(std::mem::offset_of!(WindowState, id) == 8 * WINDOW_LISP_WORDS);
    assert!(std::mem::offset_of!(WindowState, owner) == 8 * WINDOW_LISP_WORDS + 8);
    assert!(std::mem::offset_of!(WindowState, held) == 8 * WINDOW_LISP_WORDS + 12);
    assert!(std::mem::offset_of!(WindowState, use_time) == 8 * WINDOW_LISP_WORDS + 16);
    assert!(std::mem::offset_of!(WindowState, sequence_number) == 8 * WINDOW_LISP_WORDS + 24);
    assert!(std::mem::offset_of!(WindowState, mini) == 8 * WINDOW_LISP_WORDS + 32);
    assert!(std::mem::offset_of!(WindowState, horizontal) == 8 * WINDOW_LISP_WORDS + 33);
    assert!(std::mem::offset_of!(WindowState, cursor_off_p) == 8 * WINDOW_LISP_WORDS + 34);
};

impl Vectorlike for WindowState {
    const TAG: VectorTag = VectorTag::Window;
    const LISP_SLOTS: usize = WINDOW_LISP_WORDS;
}

impl WindowObjectRef {
    /// window.c:make_window's allocate_window: a window of ID in OWNER's
    /// id space that holds HELD slots, each nil, with no contents, its
    /// integers zero and its bits clear.  The caller that makes one for
    /// Lisp tallies its consing (`WINDOW_GNU_VECTOR_SLOTS') and gives it
    /// its sequence number.
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
            mini: Cell::new(false),
            horizontal: Cell::new(false),
            cursor_off_p: Cell::new(false),
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
            .field("mini", &self.mini.get())
            .field("horizontal", &self.horizontal.get())
            .field("cursor_off_p", &self.cursor_off_p.get())
            .finish_non_exhaustive()
    }
}
