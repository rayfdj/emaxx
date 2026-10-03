//! alloc.c's string headers, free list and sweep. Lisp words point directly
//! at the four-word header, rather than a vector header and RefCell wrapper.
//!
//! Rust borrows still need dynamic exclusivity checks. The sixteen bytes after
//! each header hold that check, the GC epoch and the pure allocation flag; they
//! are allocator overhead, not part of Lisp_String. Data uses GNU sblocks;
//! borrowed data blocks remain stationary until their Rust guards end.

mod data;
pub(crate) use data::{PendingStringData, retire_string_data, string_data_size};
#[cfg(test)]
pub(crate) fn string_data_census() -> (usize, usize, usize) {
    data::census()
}

use super::super::types::{LispError, MarkBit, SharedStringState, Value};
use super::{BlockKind, FREE_MARK, blocks_of, new_block, release_block};
use std::cell::{Cell, UnsafeCell};
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

#[repr(C)]
pub(crate) struct StringCell {
    state: UnsafeCell<SharedStringState>,
    borrows: Cell<isize>,
    mark: MarkBit,
    pure: bool,
    permanent_empty: bool,
}

pub(super) const STRING_CELL_SIZE: usize = std::mem::size_of::<StringCell>();
pub(super) const STRINGS_PER_BLOCK: usize = 4096 / STRING_CELL_SIZE;

const _: () = {
    assert!(STRING_CELL_SIZE == 48);
    assert!(std::mem::offset_of!(StringCell, state) == 0);
    assert!(std::mem::offset_of!(StringCell, borrows) == 32);
};

impl StringCell {
    fn new(state: SharedStringState, pure: bool) -> Self {
        let mark = MarkBit::default();
        mark.set_raw(super::super::types::current_mark_epoch());
        Self {
            state: UnsafeCell::new(state),
            borrows: Cell::new(0),
            mark,
            pure,
            permanent_empty: false,
        }
    }
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct StringObjectRef(NonNull<StringCell>);

impl std::fmt::Debug for StringObjectRef {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.borrow().fmt(formatter)
    }
}

impl StringObjectRef {
    /// # Safety
    /// CELL is an allocated string protected from collection by a live root.
    pub(crate) unsafe fn from_raw(cell: *mut StringCell) -> Self {
        // SAFETY: the caller supplies a live allocated cell.
        Self(unsafe { NonNull::new_unchecked(cell) })
    }

    fn cell(&self) -> &StringCell {
        // SAFETY: handles must remain rooted across collection, as with the
        // other GC handles. Mutable state is accessed only through guards.
        unsafe { self.0.as_ref() }
    }

    pub(crate) fn identity(&self) -> usize {
        self.0.as_ptr() as usize
    }

    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    pub(crate) fn is_pure(&self) -> bool {
        self.cell().pure
    }

    pub(crate) fn is_empty_singleton(&self) -> bool {
        empty_string_at(self.identity()).is_some()
    }

    /// data.c/fns.c CHECK_IMPURE: preserve the original Lisp object in data.
    pub(crate) fn check_impure(&self) -> Result<(), LispError> {
        if self.is_pure() {
            Err(LispError::SignalValue(Value::list([
                Value::symbol("error"),
                Value::string("Attempt to modify read-only object"),
                Value::StringObject(*self),
            ])))
        } else {
            Ok(())
        }
    }

    pub fn borrow(&self) -> StringBorrow<'_> {
        let cell = self.cell();
        let count = cell.borrows.get();
        assert!(
            (0..isize::MAX).contains(&count),
            "string already mutably borrowed"
        );
        cell.borrows.set(count + 1);
        StringBorrow(cell)
    }

    /// Collection cannot run while this guard is alive: tracing would alias
    /// its exclusive access to the string state. The collector checks this
    /// before starting a mark epoch or reclaiming any object.
    pub(crate) fn borrow_mut(&self) -> StringBorrowMut<'_> {
        let cell = self.cell();
        assert!(!cell.pure, "pure string cannot be mutably borrowed");
        assert_eq!(cell.borrows.get(), 0, "string already borrowed");
        cell.borrows.set(-1);
        StringBorrowMut(cell)
    }

    pub(crate) fn mark_bit(&self) -> StringMark<'_> {
        StringMark(self.cell())
    }
}

pub struct StringBorrow<'a>(&'a StringCell);

impl Deref for StringBorrow<'_> {
    type Target = SharedStringState;
    fn deref(&self) -> &Self::Target {
        // SAFETY: the positive borrow count excludes all mutable guards.
        unsafe { &*self.0.state.get() }
    }
}

impl Drop for StringBorrow<'_> {
    fn drop(&mut self) {
        self.0.borrows.set(self.0.borrows.get() - 1);
    }
}

pub struct StringBorrowMut<'a>(&'a StringCell);

impl Deref for StringBorrowMut<'_> {
    type Target = SharedStringState;
    fn deref(&self) -> &Self::Target {
        // SAFETY: the negative borrow count belongs to this exclusive guard.
        unsafe { &*self.0.state.get() }
    }
}

impl DerefMut for StringBorrowMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: this sole guard owns the negative borrow count. Its mutable
        // receiver also excludes references made through its own Deref.
        unsafe { &mut *self.0.state.get() }
    }
}

impl Drop for StringBorrowMut<'_> {
    fn drop(&mut self) {
        self.0.borrows.set(0);
    }
}

pub(crate) struct StringMark<'a>(&'a StringCell);

impl StringMark<'_> {
    pub(crate) fn is_marked(&self, epoch: u32) -> bool {
        self.0.pure || self.0.permanent_empty || self.0.mark.is_marked(epoch)
    }

    pub(crate) fn mark(&self, epoch: u32) -> bool {
        // Pure strings have no properties and no collectable children.
        !self.0.pure && self.0.mark.mark(epoch)
    }
}

static EMPTY_UNIBYTE: OnceLock<usize> = OnceLock::new();
static EMPTY_MULTIBYTE: OnceLock<usize> = OnceLock::new();

pub(super) fn empty_string_at(address: usize) -> Option<*mut StringCell> {
    [EMPTY_UNIBYTE.get(), EMPTY_MULTIBYTE.get()]
        .into_iter()
        .flatten()
        .any(|&empty| empty == address)
        .then_some(address as *mut StringCell)
}

fn empty_string(multibyte: bool) -> StringObjectRef {
    let slot = if multibyte {
        &EMPTY_MULTIBYTE
    } else {
        &EMPTY_UNIBYTE
    };
    let address = *slot.get_or_init(|| {
        let state = SharedStringState::empty(multibyte);
        // Dumped empty strings retain their singleton identities, but are not
        // PURE_P at runtime: the first purecopy must allocate a new header.
        let mut cell = StringCell::new(state, false);
        cell.permanent_empty = true;
        Box::into_raw(Box::new(cell)) as usize
    });
    // SAFETY: each OnceLock owns a permanent allocated string cell.
    unsafe { StringObjectRef::from_raw(address as *mut StringCell) }
}

static FREE_LIST: AtomicPtr<StringCell> = AtomicPtr::new(std::ptr::null_mut());
static BUMP_NEXT: AtomicUsize = AtomicUsize::new(0);
static BUMP_END: AtomicUsize = AtomicUsize::new(0);
static LIVE_STRINGS: AtomicUsize = AtomicUsize::new(0);
static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static LIVE_SPANS: AtomicUsize = AtomicUsize::new(0);

/// Ordinary constructors canonicalize empties; restoration and purecopy
/// allocate distinct headers, as their GNU C paths require.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum StringAllocation {
    Ordinary,
    Restored,
    Pure,
}

/// Return an incompletely constructed header to the arena on an allocation
/// error or unwind. The content guard is dropped first, so no reference to
/// the cell remains when its first word becomes a free-list link.
struct StringConstruction(*mut StringCell);

impl Drop for StringConstruction {
    fn drop(&mut self) {
        // SAFETY: the initializer has released its exclusive guard. The
        // initialized state owns either empty data or a complete sdata entry.
        unsafe {
            let cell = self.0;
            std::ptr::drop_in_place((*cell).state.get());
            (*cell).mark.set_raw(FREE_MARK);
            cell.cast::<*mut StringCell>()
                .write(FREE_LIST.load(Ordering::Relaxed));
            FREE_LIST.store(cell, Ordering::Relaxed);
        }
    }
}

/// Allocate the stable header before constructing its data. The initializer
/// sees the final header address and cannot move its state into host storage.
pub(crate) fn allocate_string<E>(
    characters: usize,
    multibyte: bool,
    kind: StringAllocation,
    initialize: impl FnOnce(&mut SharedStringState) -> Result<(), E>,
) -> Result<StringObjectRef, E> {
    if kind == StringAllocation::Ordinary && characters == 0 {
        return Ok(empty_string(multibyte));
    }
    let head = FREE_LIST.load(Ordering::Relaxed);
    let cell = if head.is_null() {
        let next = BUMP_NEXT.load(Ordering::Relaxed);
        if next < BUMP_END.load(Ordering::Relaxed) {
            BUMP_NEXT.store(next + STRING_CELL_SIZE, Ordering::Relaxed);
            next as *mut StringCell
        } else {
            let block = new_block(BlockKind::String);
            BUMP_NEXT.store(block + STRING_CELL_SIZE, Ordering::Relaxed);
            BUMP_END.store(
                block + STRINGS_PER_BLOCK * STRING_CELL_SIZE,
                Ordering::Relaxed,
            );
            block as *mut StringCell
        }
    } else {
        // SAFETY: a free cell's first word is the free list link.
        FREE_LIST.store(
            unsafe { head.cast::<*mut StringCell>().read() },
            Ordering::Relaxed,
        );
        head
    };
    // SAFETY: initialize a valid empty header before exposing its final
    // address. The construction guard also rejects reentrant collection.
    unsafe { cell.write(StringCell::new(SharedStringState::empty(multibyte), false)) };
    let construction = StringConstruction(cell);
    let handle = unsafe {
        (*cell).borrows.set(-1);
        let mut guard = StringBorrowMut(&*cell);
        initialize(&mut guard)?;
        drop(guard);
        (*cell).pure = kind == StringAllocation::Pure;
        StringObjectRef::from_raw(cell)
    };
    let state = handle.borrow();
    assert_eq!(state.len(), characters);
    assert_eq!(state.is_multibyte(), multibyte);
    if kind == StringAllocation::Pure {
        assert!(state.props.is_empty(), "pure strings have no intervals");
    } else {
        super::super::types::note_string_allocation(state.storage_bytes());
        LIVE_STRINGS.store(LIVE_STRINGS.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
        LIVE_BYTES.store(
            LIVE_BYTES.load(Ordering::Relaxed) + state.storage_bytes(),
            Ordering::Relaxed,
        );
        LIVE_SPANS.store(
            LIVE_SPANS.load(Ordering::Relaxed) + state.props.len(),
            Ordering::Relaxed,
        );
    }
    drop(state);
    std::mem::forget(construction);
    Ok(handle)
}

/// # Safety
/// START is a newly allocated string block, with no initialized payloads.
pub(super) unsafe fn init_block(start: usize) {
    for index in 0..STRINGS_PER_BLOCK {
        let cell = (start + index * STRING_CELL_SIZE) as *mut StringCell;
        // SAFETY: only the mark word is accessed until a cell is allocated.
        unsafe {
            std::ptr::addr_of_mut!((*cell).mark).write(MarkBit::default());
            (*cell).mark.set_raw(FREE_MARK);
        }
    }
}

/// # Safety
/// START is a registered string block; every mark word is initialized.
pub(super) unsafe fn live_string_holding(start: usize, address: usize) -> Option<*mut StringCell> {
    let index = (address - start) / STRING_CELL_SIZE;
    if index >= STRINGS_PER_BLOCK {
        return None;
    }
    let cell = (start + index * STRING_CELL_SIZE) as *mut StringCell;
    // SAFETY: inside the block; the mark is initialized even in free cells.
    (unsafe { (*cell).mark.raw() } != FREE_MARK).then_some(cell)
}

/// A Rust borrow may live in host storage outside the conservative stack.
/// Return those roots before alloc.c's mark/weak-table/sweep sequence so the
/// ordinary string tracer also retains their property values. No registry or
/// lookup is added to string access; this scan belongs to collection.
///
/// A live exclusive guard may expose an `&mut SharedStringState`. Reject the
/// collection before its mark epoch begins instead of reading through that
/// reference or discovering the conflict after other objects were swept.
pub(crate) fn borrowed_string_roots() -> Vec<Value> {
    let mut roots = Vec::new();
    let mut inspect = |cell: *mut StringCell| {
        // SAFETY: block mark words are initialized even in free cells. The
        // remaining metadata is read only for allocated cells; no string
        // payload is accessed while checking for an exclusive borrow.
        unsafe {
            if (*cell).mark.raw() != FREE_MARK {
                let count = (*cell).borrows.get();
                assert!(
                    count >= 0,
                    "cannot collect while a string is mutably borrowed"
                );
                if count > 0 {
                    roots.push(Value::StringObject(StringObjectRef::from_raw(cell)));
                }
            }
        }
    };
    for start in blocks_of(BlockKind::String) {
        for index in 0..STRINGS_PER_BLOCK {
            inspect((start + index * STRING_CELL_SIZE) as *mut StringCell);
        }
    }
    for &address in [EMPTY_UNIBYTE.get(), EMPTY_MULTIBYTE.get()]
        .into_iter()
        .flatten()
    {
        inspect(address as *mut StringCell);
    }
    roots
}

/// alloc.c:sweep_strings runs after symbol names are no longer needed.
pub(crate) fn sweep_strings(epoch: u32) {
    let mut free_list = std::ptr::null_mut();
    let mut free_count = 0;
    let (mut strings, mut bytes, mut spans) = (0, 0, 0);
    let mut released = Vec::new();
    for start in blocks_of(BlockKind::String) {
        let limit =
            if BUMP_END.load(Ordering::Relaxed) == start + STRINGS_PER_BLOCK * STRING_CELL_SIZE {
                (BUMP_NEXT.load(Ordering::Relaxed) - start) / STRING_CELL_SIZE
            } else {
                STRINGS_PER_BLOCK
            };
        let previous_chain = free_list;
        let mut block_free = 0;
        for index in 0..limit {
            let cell = (start + index * STRING_CELL_SIZE) as *mut StringCell;
            // SAFETY: every mark is initialized; other fields are read only
            // after confirming the cell contains an allocated object.
            unsafe {
                let mark = (*cell).mark.raw();
                if mark != FREE_MARK {
                    if (*cell).pure {
                        continue;
                    }
                    if mark == epoch {
                        let handle = StringObjectRef::from_raw(cell);
                        let state = handle.borrow();
                        strings += 1;
                        bytes += state.storage_bytes();
                        spans += state.props.len();
                        continue;
                    }
                    debug_assert_eq!((*cell).borrows.get(), 0, "unmarked string borrow");
                    std::ptr::drop_in_place((*cell).state.get());
                    (*cell).mark.set_raw(FREE_MARK);
                }
                cell.cast::<*mut StringCell>().write(free_list);
            }
            free_list = cell;
            block_free += 1;
        }
        if block_free == STRINGS_PER_BLOCK && free_count > STRINGS_PER_BLOCK {
            free_list = previous_chain;
            released.push(start);
        } else {
            free_count += block_free;
        }
    }
    FREE_LIST.store(free_list, Ordering::Relaxed);
    for start in released {
        release_block(start, BlockKind::String);
    }
    data::sweep();
    LIVE_STRINGS.store(strings, Ordering::Relaxed);
    LIVE_BYTES.store(bytes, Ordering::Relaxed);
    LIVE_SPANS.store(spans, Ordering::Relaxed);
}

pub(crate) fn live_string_object_census() -> (usize, usize, usize) {
    (
        LIVE_STRINGS.load(Ordering::Relaxed),
        LIVE_BYTES.load(Ordering::Relaxed),
        LIVE_SPANS.load(Ordering::Relaxed),
    )
}
