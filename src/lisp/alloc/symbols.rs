//! alloc.c's storage for symbols: `symbol_block', `symbol_free_list',
//! `symbol_block_index' as the bump pointer into the newest block, and
//! `sweep_symbols', which returns unmarked cells to the free list and
//! gives back a block with nothing live in it.
//!
//! A `SymbolRef' is `Lisp_Object' for a symbol: the cell's address,
//! copied without a count, valid while the mark reaches the cell.  The
//! interned symbols are always reached (the obarray is a root); an
//! uninterned one lives while something names it, as in C.
//!
//! Not C: the cell keeps the host-side key text and its registry key
//! beside the name (the uninterned symbols' identity by text is the
//! pre-representation deviation the ledger records), the mark is the
//! epoch word, and the value, function and property cells still live in
//! the interpreter's tables by the symbol's id (phase C's next step).

use super::super::types::{MarkBit, Value};
use super::{BlockKind, FREE_MARK, SYMBOLS_PER_BLOCK, blocks_of, new_block, release_block};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

/// alloc.c's `struct Lisp_Symbol', as this implementation keeps it.
#[repr(C)]
pub struct SymbolCell {
    /// The host-side key text (the string cell the symbol keeps alive).
    pub(crate) internal: super::TextRef,
    /// `SYMBOL_NAME': the Lisp-visible name object.
    pub(crate) lisp_name: Value,
    pub(crate) mark: MarkBit,
    /// The symbol's index into an interpreter's `SymbolCells'.
    pub(crate) id: u32,
    /// An uninterned symbol's own copy of its key text, for the release
    /// of its registry entries when the sweep frees the cell.
    pub(crate) key: Option<Box<str>>,
}

pub(super) const SYMBOL_CELL_SIZE: usize = std::mem::size_of::<SymbolCell>();

/// alloc.c's `symbol_free_list', `symbol_block' with `symbol_block_index',
/// and gcstat's `total_symbols' and `total_free_symbols'.
static SYMBOL_FREE_LIST: AtomicPtr<SymbolCell> = AtomicPtr::new(std::ptr::null_mut());
static SYMBOL_BUMP_NEXT: AtomicUsize = AtomicUsize::new(0);
static SYMBOL_BUMP_END: AtomicUsize = AtomicUsize::new(0);
static LIVE_SYMBOLS: AtomicUsize = AtomicUsize::new(0);
static FREE_SYMBOLS: AtomicUsize = AtomicUsize::new(0);

struct FreeSymbol {
    next: *mut SymbolCell,
}

/// `Lisp_Object' for a symbol: the cell's address.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct SymbolRef(NonNull<SymbolCell>);

impl SymbolRef {
    /// # Safety
    /// CELL is an allocated symbol cell.
    pub(crate) unsafe fn from_raw(cell: *mut SymbolCell) -> Self {
        // SAFETY: the caller's contract.
        Self(unsafe { NonNull::new_unchecked(cell) })
    }

    #[inline]
    pub(crate) fn cell(&self) -> &SymbolCell {
        // SAFETY: the collector keeps the cell while a handle can be read.
        unsafe { self.0.as_ref() }
    }

    #[inline]
    pub(crate) fn identity(&self) -> usize {
        self.0.as_ptr() as usize
    }

    #[inline]
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    #[inline]
    pub(crate) fn mark_bit(&self) -> &MarkBit {
        &self.cell().mark
    }
}

impl std::ops::Deref for SymbolRef {
    type Target = SymbolCell;

    #[inline]
    fn deref(&self) -> &SymbolCell {
        self.cell()
    }
}

/// alloc.c's `Fmake_symbol' storage: the free list's head, else the next
/// cell of the newest block.
pub(crate) fn allocate_symbol(cell: SymbolCell) -> SymbolRef {
    let head = SYMBOL_FREE_LIST.load(Ordering::Relaxed);
    let slot = if head.is_null() {
        bump_symbol()
    } else {
        // SAFETY: a free cell's first word is the free-list link.
        let next = unsafe { (*head.cast::<FreeSymbol>()).next };
        SYMBOL_FREE_LIST.store(next, Ordering::Relaxed);
        FREE_SYMBOLS.store(
            FREE_SYMBOLS.load(Ordering::Relaxed).saturating_sub(1),
            Ordering::Relaxed,
        );
        head
    };
    LIVE_SYMBOLS.store(LIVE_SYMBOLS.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
    // SAFETY: SLOT is a free cell of a symbol block; every field is
    // written before a handle is made.
    unsafe {
        std::ptr::write(slot, cell);
        (*slot)
            .mark
            .set_raw(super::super::types::current_mark_epoch());
        SymbolRef(NonNull::new_unchecked(slot))
    }
}

fn bump_symbol() -> *mut SymbolCell {
    let next = SYMBOL_BUMP_NEXT.load(Ordering::Relaxed);
    let end = SYMBOL_BUMP_END.load(Ordering::Relaxed);
    if next < end {
        SYMBOL_BUMP_NEXT.store(next + SYMBOL_CELL_SIZE, Ordering::Relaxed);
        return next as *mut SymbolCell;
    }
    let block = new_block(BlockKind::Symbol);
    SYMBOL_BUMP_NEXT.store(block + SYMBOL_CELL_SIZE, Ordering::Relaxed);
    SYMBOL_BUMP_END.store(
        block + SYMBOLS_PER_BLOCK * SYMBOL_CELL_SIZE,
        Ordering::Relaxed,
    );
    block as *mut SymbolCell
}

/// Write the free mark into every cell of a fresh block.
///
/// # Safety
/// START is a block `new_block' just allocated.
pub(super) unsafe fn init_block(start: usize) {
    for index in 0..SYMBOLS_PER_BLOCK {
        let cell = (start + index * SYMBOL_CELL_SIZE) as *mut SymbolCell;
        // SAFETY: inside the block; the mark word is written as a word.
        unsafe {
            std::ptr::addr_of_mut!((*cell).mark)
                .cast::<u32>()
                .write(FREE_MARK)
        };
    }
}

/// alloc.c's `live_symbol_holding': the allocated cell containing
/// ADDRESS, or None.
///
/// # Safety
/// START is a registered symbol block.
pub(super) unsafe fn live_symbol_holding(start: usize, address: usize) -> Option<*mut SymbolCell> {
    let index = (address - start) / SYMBOL_CELL_SIZE;
    if index >= SYMBOLS_PER_BLOCK {
        return None;
    }
    let cell = (start + index * SYMBOL_CELL_SIZE) as *mut SymbolCell;
    // SAFETY: inside a registered symbol block; the mark word is readable
    // in every state.
    let mark = unsafe { (*cell).mark.raw() };
    (mark != FREE_MARK).then_some(cell)
}

/// alloc.c's `sweep_symbols': an unmarked cell is released (CLEANUP sees
/// it first: the registries an uninterned symbol's key names let it go)
/// and goes back on the free list, rebuilt from every free cell; a block
/// holding nothing but free cells is given back once more than a block's
/// worth of free cells is at hand.  Returns gcstat's `total_symbols'.
pub(crate) fn sweep_symbols(epoch: u32, mut cleanup: impl FnMut(&SymbolCell)) -> usize {
    let bump_next = SYMBOL_BUMP_NEXT.load(Ordering::Relaxed);
    let bump_end = SYMBOL_BUMP_END.load(Ordering::Relaxed);
    let mut free_list: *mut SymbolCell = std::ptr::null_mut();
    let mut num_free = 0usize;
    let mut num_used = 0usize;
    let mut released = Vec::new();
    for start in blocks_of(BlockKind::Symbol) {
        let lim = if bump_end == start + SYMBOLS_PER_BLOCK * SYMBOL_CELL_SIZE {
            (bump_next - start) / SYMBOL_CELL_SIZE
        } else {
            SYMBOLS_PER_BLOCK
        };
        let chain_before = free_list;
        let mut this_free = 0usize;
        for index in 0..lim {
            let cell = (start + index * SYMBOL_CELL_SIZE) as *mut SymbolCell;
            // SAFETY: inside a registered symbol block.
            let mark = unsafe { (*cell).mark.raw() };
            if mark == epoch {
                num_used += 1;
                continue;
            }
            if mark != FREE_MARK {
                // SAFETY: an allocated, unmarked cell: nothing reaches it,
                // so its registries let it go and the slot becomes free.
                unsafe {
                    cleanup(&*cell);
                    std::ptr::drop_in_place(cell);
                    if cfg!(debug_assertions) {
                        std::ptr::write_bytes(
                            cell.cast::<u8>().add(std::mem::size_of::<FreeSymbol>()),
                            0xA5,
                            SYMBOL_CELL_SIZE - std::mem::size_of::<FreeSymbol>(),
                        );
                    }
                    (*cell).mark.set_raw(FREE_MARK);
                }
            }
            this_free += 1;
            // SAFETY: a free cell's first word is the free-list link.
            unsafe { (*cell.cast::<FreeSymbol>()).next = free_list };
            free_list = cell;
        }
        if this_free == SYMBOLS_PER_BLOCK && num_free > SYMBOLS_PER_BLOCK {
            free_list = chain_before;
            released.push(start);
        } else {
            num_free += this_free;
        }
    }
    SYMBOL_FREE_LIST.store(free_list, Ordering::Relaxed);
    for start in released {
        release_block(start, BlockKind::Symbol);
    }
    LIVE_SYMBOLS.store(num_used, Ordering::Relaxed);
    FREE_SYMBOLS.store(num_free, Ordering::Relaxed);
    num_used
}

pub(crate) fn live_symbols() -> usize {
    LIVE_SYMBOLS.load(Ordering::Relaxed)
}
