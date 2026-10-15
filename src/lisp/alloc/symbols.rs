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
//! Not C: the cell keeps the host-side key text
//! beside the name (the uninterned symbols' identity by text is the
//! pre-representation deviation the ledger records), the mark is the
//! epoch word. Name, value/alias, function, plist and flags are authoritative
//! object fields. Remaining host name and localized/forwarded adapters
//! must be removed before this migration is complete.

use super::super::types::{MarkBit, SymbolName, Value};
use super::{BlockKind, FREE_MARK, SYMBOLS_PER_BLOCK, blocks_of, new_block, release_block};
use std::cell::Cell;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicI64, AtomicPtr, AtomicUsize, Ordering};

/// alloc.c's `struct Lisp_Symbol', as this implementation keeps it.
#[repr(C)]
pub struct SymbolCell {
    /// Immutable host lookup key, owned and released with the symbol.
    pub(crate) internal: String,
    /// `SYMBOL_NAME': the Lisp-visible name object.
    pub(crate) lisp_name: std::cell::Cell<Value>,
    /// `Lisp_Symbol.u.s.function' and `.plist': one authoritative word each.
    pub(crate) function: std::cell::Cell<Value>,
    pub(crate) plist: std::cell::Cell<Value>,
    /// Lisp_Symbol.val: a value word or an alias object, selected by state.
    pub(crate) payload: Cell<SymbolPayload>,
    pub(crate) flags: Cell<u8>,
    pub(crate) state: Cell<u16>,
    pub(crate) mark: MarkBit,
}

// VALUE_PRESENT retains the existing distinction between an absent binding
// and a localized, explicitly void default until localized storage is moved.
const VALUE_PRESENT: u16 = 1;
const ALIAS: u16 = 2;
const PLAIN_STORE: u16 = 4;
// The defining C initializer gives this object its dedicated slot. A later
// symbol with the same name must not inherit that slot. The remaining slot
// implementation is a migration adapter, not a negative name-lookup cache.
const C_SLOT: u16 = 8;
// Temporary startup adapter: consumers distinguish Lisp fset from defsubr.
// Remove after those consumers use the actual function capability. No separate
// payload, id index, enumeration position or per-symbol allocation is involved.
const FUNCTION_DEFINITION: u16 = 16;
// lisp.h: Lisp_Symbol.interned is object state, not an identity-number bit.
// Standard-obarray removal still has its separate membership adapter.
const UNINTERNED: u16 = 32;
// The fixed ABI words designate these actual objects, not any later symbol
// whose name happens to be nil or t after the original is uninterned.
const BUILTIN_NIL: u16 = 64;
const BUILTIN_T: u16 = 128;
// lisp.h: trapped_write == SYMBOL_NOWRITE. This remains on the object
// after uninterning and is copied/restored with its other header state.
const CONSTANT: u16 = 256;
// lisp.h: redirect == SYMBOL_FORWARDED to a Lisp_Intfwd. Set together with
// VALUE_PRESENT; the payload is the forwarded counter word.
const INT_FORWARD: u16 = 512;

#[derive(Clone, Copy)]
pub(crate) union SymbolPayload {
    value: Value,
    alias: SymbolName,
    /// `Lisp_Intfwd.intvar': one of alloc.c's process-wide counters.
    counter: &'static AtomicI64,
}

/// data.c:store_symval_forwarding's Lisp_Fwd_Int case: the store has passed
/// CHECK_INTEGER and the intmax_t range check before reaching the cell.
fn forwarded_integer(value: Value) -> Option<i64> {
    value.as_integer().ok()
}

impl SymbolPayload {
    pub(crate) fn unbound() -> Self {
        Self {
            value: Value::Unbound,
        }
    }
}

impl Default for SymbolPayload {
    fn default() -> Self {
        Self::unbound()
    }
}

impl SymbolCell {
    pub(crate) fn mark_builtin_boolean(&self, truth: bool) {
        self.state
            .set(self.state.get() | if truth { BUILTIN_T } else { BUILTIN_NIL });
    }

    pub(crate) fn builtin_boolean(&self) -> Option<Value> {
        match self.state.get() & (BUILTIN_NIL | BUILTIN_T) {
            BUILTIN_NIL => Some(Value::Nil),
            BUILTIN_T => Some(Value::T),
            _ => None,
        }
    }

    pub(crate) fn is_uninterned(&self) -> bool {
        self.state.get() & UNINTERNED != 0
    }

    pub(crate) fn mark_uninterned(&self) {
        self.state.set(self.state.get() | UNINTERNED);
    }

    #[inline]
    pub(crate) fn value(&self) -> Option<Value> {
        let state = self.state.get();
        if state & (VALUE_PRESENT | INT_FORWARD) == VALUE_PRESENT {
            // SAFETY: VALUE_PRESENT without INT_FORWARD selects the value member.
            return Some(unsafe { self.payload.get().value });
        }
        (state & INT_FORWARD != 0).then(|| self.forwarded_value())
    }

    /// data.c:do_symval_forwarding: `make_int (*intvar)'.
    #[cold]
    fn forwarded_value(&self) -> Value {
        // SAFETY: INT_FORWARD is set only with the counter union member.
        let counter = unsafe { self.payload.get().counter };
        crate::lisp::primitives::normalize_integer_value(counter.load(Ordering::Relaxed))
    }

    /// lread.c:defvar_int: the symbol's value is the C counter itself.
    pub(crate) fn forward_to_counter(&self, counter: &'static AtomicI64) {
        assert!(self.alias().is_none(), "forwarding a symbol alias");
        self.payload.set(SymbolPayload { counter });
        self.state
            .set((self.state.get() & !PLAIN_STORE) | VALUE_PRESENT | INT_FORWARD);
    }

    pub(crate) fn is_forwarded_counter(&self) -> bool {
        self.state.get() & INT_FORWARD != 0
    }

    /// The counter this symbol forwards to, for the test image copier, which
    /// copies the redirect itself.
    #[cfg(test)]
    pub(crate) fn forwarded_counter(&self) -> Option<&'static AtomicI64> {
        // SAFETY: INT_FORWARD is set only with the counter union member.
        self.is_forwarded_counter()
            .then(|| unsafe { self.payload.get().counter })
    }

    /// data.c:store_symval_forwarding into the counter. Callers check
    /// FWD_INT first; a value that cannot live there (only a store that
    /// data.c:set_internal would make plain) detaches and stores plainly.
    #[cold]
    fn store_forwarded(&self, value: Value) {
        match forwarded_integer(value) {
            Some(integer) => {
                // SAFETY: INT_FORWARD is set only with the counter union member.
                unsafe { self.payload.get().counter }.store(integer, Ordering::Relaxed);
            }
            None => {
                self.detach_counter();
                self.payload.set(SymbolPayload { value });
                self.state.set(self.state.get() | VALUE_PRESENT);
            }
        }
    }

    /// data.c:set_internal storing Qunbound into a forwarded symbol:
    /// `sym->u.s.redirect = SYMBOL_PLAINVAL'. The counter keeps counting;
    /// only this symbol stops forwarding to it, and it is left void.
    fn detach_counter(&self) {
        self.payload.set(SymbolPayload::unbound());
        self.state
            .set(self.state.get() & !(VALUE_PRESENT | INT_FORWARD));
    }

    pub(crate) fn set_value(&self, value: Value) {
        assert!(self.alias().is_none(), "plain store into a symbol alias");
        if self.state.get() & INT_FORWARD != 0 {
            return self.store_forwarded(value);
        }
        self.payload.set(SymbolPayload { value });
        self.state.set(self.state.get() | VALUE_PRESENT);
    }

    pub(crate) fn replace_bound_value(&self, value: Value) -> bool {
        let state = self.state.get();
        if state & VALUE_PRESENT == 0 {
            return false;
        }
        if state & INT_FORWARD != 0 {
            self.store_forwarded(value);
        } else {
            self.payload.set(SymbolPayload { value });
        }
        true
    }

    pub(crate) fn take_value(&self) -> Option<Value> {
        let value = self.value()?;
        self.detach_counter();
        Some(value)
    }

    pub(crate) fn alias(&self) -> Option<SymbolName> {
        (self.state.get() & ALIAS != 0).then(|| {
            // SAFETY: ALIAS is set only with the alias union member.
            unsafe { self.payload.get().alias }
        })
    }

    pub(crate) fn set_alias(&self, target: SymbolName) {
        self.payload.set(SymbolPayload { alias: target });
        self.state
            .set((self.state.get() & !(VALUE_PRESENT | PLAIN_STORE | INT_FORWARD)) | ALIAS);
    }

    pub(crate) fn clear_alias(&self) -> bool {
        if self.state.get() & ALIAS == 0 {
            return false;
        }
        self.payload.set(SymbolPayload::unbound());
        self.state
            .set(self.state.get() & !(VALUE_PRESENT | ALIAS | PLAIN_STORE));
        true
    }

    pub(crate) fn plain_store(&self) -> bool {
        self.state.get() & PLAIN_STORE != 0
    }

    pub(crate) fn set_plain_store(&self, plain: bool) {
        self.state.set(if plain {
            self.state.get() | PLAIN_STORE
        } else {
            self.state.get() & !PLAIN_STORE
        });
    }

    pub(crate) fn value_or_alias_root(&self) -> Option<Value> {
        // A forwarded counter is an integer word, never an object to trace;
        // reading it here could allocate a bignum during collection.
        if self.is_forwarded_counter() {
            return None;
        }
        self.value().or_else(|| self.alias().map(Value::Symbol))
    }

    pub(crate) fn has_function_definition(&self) -> bool {
        self.state.get() & FUNCTION_DEFINITION != 0
    }

    pub(crate) fn set_function_definition(&self, defined: bool) {
        self.state.set(if defined {
            self.state.get() | FUNCTION_DEFINITION
        } else {
            self.state.get() & !FUNCTION_DEFINITION
        });
    }

    pub(crate) fn has_c_slot(&self) -> bool {
        self.state.get() & C_SLOT != 0
    }

    pub(crate) fn set_c_slot(&self, present: bool) {
        self.state.set(if present {
            (self.state.get() | C_SLOT) & !PLAIN_STORE
        } else {
            self.state.get() & !C_SLOT
        });
    }

    pub(crate) fn is_constant(&self) -> bool {
        self.state.get() & CONSTANT != 0
    }

    pub(crate) fn set_constant(&self, constant: bool) {
        self.state.set(if constant {
            (self.state.get() | CONSTANT) & !PLAIN_STORE
        } else {
            self.state.get() & !CONSTANT
        });
    }
}

pub(super) const SYMBOL_CELL_SIZE: usize = std::mem::size_of::<SymbolCell>();
const _: () = assert!(SYMBOL_CELL_SIZE == 64);

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
    super::counters::tally(&super::counters::SYMBOLS_CONSED, 1);
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
