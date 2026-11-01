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
use super::{BlockKind, FREE_MARK, SYMBOLS_PER_BLOCK, new_block, release_block, sweep_blocks_of};
use std::cell::Cell;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicI64, AtomicPtr, AtomicUsize, Ordering};

/// The immutable host text a symbol keeps beside its Lisp name: a box the
/// cell owns and frees with it, or text of the permanent image, which
/// lives as long as the process.  Two words: the text's address, and its
/// length with the top bit set when the box is the cell's.
#[repr(C)]
pub(crate) struct SymbolText {
    text: NonNull<u8>,
    len: usize,
}

const OWNED_TEXT: usize = 1 << (usize::BITS - 1);

impl SymbolText {
    pub(crate) fn owned(text: String) -> Self {
        let len = text.len();
        let text = Box::into_raw(text.into_boxed_str());
        Self {
            // SAFETY: a box's pointer is never null.
            text: unsafe { NonNull::new_unchecked(text.cast::<u8>()) },
            len: len | OWNED_TEXT,
        }
    }

    /// TEXT of the permanent image.
    pub(crate) fn image(text: &'static str) -> Self {
        Self {
            text: NonNull::from(text.as_bytes()).cast(),
            len: text.len(),
        }
    }

    #[inline(always)]
    pub(crate) fn as_str(&self) -> &str {
        // SAFETY: the text was a `str' and is never written.
        unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                self.text.as_ptr(),
                self.len & !OWNED_TEXT,
            ))
        }
    }
}

impl Drop for SymbolText {
    fn drop(&mut self) {
        if self.len & OWNED_TEXT != 0 {
            let text =
                std::ptr::slice_from_raw_parts_mut(self.text.as_ptr(), self.len & !OWNED_TEXT);
            // SAFETY: the owned text is the box `owned' made.
            drop(unsafe { Box::from_raw(text as *mut str) });
        }
    }
}

/// alloc.c's `struct Lisp_Symbol', as this implementation keeps it.
#[repr(C)]
pub struct SymbolCell {
    /// Immutable host lookup key, released with the symbol.
    pub(crate) internal: SymbolText,
    /// `SYMBOL_NAME': the Lisp-visible name object.
    pub(crate) lisp_name: std::cell::Cell<Value>,
    /// `Lisp_Symbol.u.s.function' and `.plist': one authoritative word each.
    pub(crate) function: std::cell::Cell<Value>,
    pub(crate) plist: std::cell::Cell<Value>,
    /// Lisp_Symbol.val: a value word or an alias object, selected by state.
    pub(crate) payload: Cell<SymbolPayload>,
    /// `Lisp_Symbol.u.s.next': the next symbol of its obarray bucket.
    pub(crate) next: Cell<Option<SymbolName>>,
    pub(crate) flags: Cell<u8>,
    /// Allocated while an image loaded (see `ImageCells'); set by
    /// `allocate_symbol'.
    pub(crate) image: bool,
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
// lisp.h: redirect == SYMBOL_FORWARDED to a Lisp_Kboard_Objfwd. Set with
// C_SLOT on the variables whose values live in each terminal's KBOARD, so
// other forwarded variables are known not to be one without a name check.
const KBOARD_FORWARD: u16 = 1024;
// Set with C_SLOT on the C variables whose store also reaches interpreter
// state beyond the cell (`STORE_EFFECT_VARIABLES'), so a store to any other
// forwarded variable is known to be the cell's alone without a name check.
const STORE_EFFECT: u16 = 2048;
// lisp.h: trapped_write == SYMBOL_TRAPPED_WRITE: a variable watcher is
// registered under this object (its `watchers' property holds them), so a
// store notifies; without it none is.
const TRAPPED_WRITE: u16 = 4096;
// Set with C_SLOT on the C variables whose value the current buffer holds
// rather than the cell (`BUFFER_SLOT_VARIABLES'): the reads and stores that
// answer from the cell alone leave them to the full paths.
const BUFFER_SLOT: u16 = 8192;
// Set with C_SLOT on the C variables whose store converts the value beyond
// its slot kind's conversion (`STORE_CONVERTING_VARIABLES').
const STORE_CONVERTS: u16 = 16384;
// lisp.h's builtin_lisp_symbol_p: the symbol was interned when the
// interpreter's construction ended, as GNU's lispsym symbols exist before
// its dump loads. An image refers to such a symbol by name; it maps the
// others where it holds them.
const BUILTIN: u16 = 32768;

/// `buffer-undo-list' (the buffer's history) and `text-conversion-style'
/// (its field): C variables whose value the current buffer holds.
pub(crate) const BUFFER_SLOT_VARIABLES: [&str; 2] = ["buffer-undo-list", "text-conversion-style"];

/// The C variables whose store converts the value as their own C code
/// does (`prepare_variable_assignment_with').
pub(crate) const STORE_CONVERTING_VARIABLES: [&str; 3] = [
    "display-hourglass",
    "scroll-up-aggressively",
    "vertical-scroll-bar",
];

/// The forwarded variables `Interpreter::set_global_binding_resolved' does
/// more for than store the cell: the evaluator's own C cells, the buffers'
/// text conversion style and the feature list.
pub(crate) const STORE_EFFECT_VARIABLES: [&str; 10] = [
    "quit-flag",
    "inhibit-quit",
    "throw-on-input",
    "overriding-plist-environment",
    "load-path",
    "max-lisp-eval-depth",
    "debug-on-next-call",
    "symbols-with-pos-enabled",
    "text-conversion-style",
    "features",
];

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
    value.integer_value()
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

    /// data.c:find_symbol_value of a bound symbol that is no alias and not
    /// localized in any buffer, whose value is the one its cell holds: a
    /// plain symbol, or a C variable forwarded to an object slot (a KBOARD
    /// variable's, a counter's or a buffer-held variable's value is
    /// elsewhere).  Any other symbol, or a void one, answers None.
    #[inline(always)]
    pub(crate) fn unlocalized_value(&self) -> Option<Value> {
        use crate::lisp::eval::symbol_cell_flags::LOCALIZED;
        if self.state.get() & (VALUE_PRESENT | ALIAS | INT_FORWARD | KBOARD_FORWARD | BUFFER_SLOT)
            == VALUE_PRESENT
            && self.flags.get() & LOCALIZED == 0
        {
            // SAFETY: VALUE_PRESENT without INT_FORWARD selects the value member.
            return Some(unsafe { self.payload.get().value });
        }
        None
    }

    /// A bound C variable whose value is its cell's and whose store changes
    /// nothing else (`set_global_binding_resolved's cell-only case): no
    /// alias, counter, KBOARD, store-effect or buffer-held slot, no value
    /// conversion of its own, no watcher, not constant, and neither
    /// localized, local if set, per-buffer nor always local.
    /// eval.c:specbind's SYMBOL_FORWARDED let of it is SPECPDL_LET, a store
    /// into the slot (after the slot kind's conversion) and its restore.
    #[inline(always)]
    pub(crate) fn is_plain_c_slot(&self) -> bool {
        use crate::lisp::eval::symbol_cell_flags::{
            ALWAYS_LOCAL, FWD_INT, LOCAL_IF_SET, LOCALIZED, PER_BUFFER,
        };
        self.state.get()
            & (VALUE_PRESENT
                | ALIAS
                | C_SLOT
                | INT_FORWARD
                | KBOARD_FORWARD
                | STORE_EFFECT
                | TRAPPED_WRITE
                | CONSTANT
                | BUFFER_SLOT
                | STORE_CONVERTS)
            == VALUE_PRESENT | C_SLOT
            && self.flags.get() & (LOCALIZED | LOCAL_IF_SET | PER_BUFFER | ALWAYS_LOCAL | FWD_INT)
                == 0
    }

    /// A buffer-local variable whose bindings are values in the buffers'
    /// cells, read as they are: localized, and no alias, counter, KBOARD
    /// variable or buffer-held slot.  A read through the binding loaded for
    /// the current buffer is then data.c:find_symbol_value's
    /// SYMBOL_LOCALIZED read (per-buffer and always-local variables
    /// included, whose default reads stay their own).
    #[inline(always)]
    pub(crate) fn has_local_value_cells(&self) -> bool {
        use crate::lisp::eval::symbol_cell_flags::LOCALIZED;
        self.state.get() & (ALIAS | INT_FORWARD | KBOARD_FORWARD | BUFFER_SLOT) == 0
            && self.flags.get() & LOCALIZED != 0
    }

    /// The default of a variable with local value cells
    /// (`has_local_value_cells') that is not always local, when its cell
    /// holds it: what a buffer without a binding of it reads, blv's
    /// default cell.  None when the default is void or a C slot's.
    #[inline(always)]
    pub(crate) fn localized_default(&self) -> Option<Value> {
        use crate::lisp::eval::symbol_cell_flags::ALWAYS_LOCAL;
        if self.state.get() & VALUE_PRESENT != 0 && self.flags.get() & ALWAYS_LOCAL == 0 {
            // SAFETY: VALUE_PRESENT without INT_FORWARD (which local value
            // cells exclude) selects the value member.
            return Some(unsafe { self.payload.get().value });
        }
        None
    }

    /// A buffer-local variable whose store into a buffer's binding changes
    /// nothing else: no alias, counter, KBOARD, store-effect or
    /// buffer-held slot, no value conversion, no watcher, not constant, and
    /// localized without being per-buffer, always local or a typed C slot.
    /// data.c:set_internal's SYMBOL_LOCALIZED store into the loaded binding
    /// (set_blv_value) is then the whole assignment.
    #[inline(always)]
    pub(crate) fn is_plain_localized(&self) -> bool {
        use crate::lisp::eval::symbol_cell_flags::{
            ALWAYS_LOCAL, FWD_BOOL, FWD_INT, LOCALIZED, PER_BUFFER,
        };
        self.state.get()
            & (ALIAS
                | INT_FORWARD
                | KBOARD_FORWARD
                | STORE_EFFECT
                | TRAPPED_WRITE
                | CONSTANT
                | BUFFER_SLOT
                | STORE_CONVERTS)
            == 0
            && self.flags.get() & (LOCALIZED | PER_BUFFER | ALWAYS_LOCAL | FWD_BOOL | FWD_INT)
                == LOCALIZED
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

    /// data.c:set_internal's SYMBOL_PLAINVAL, untrapped case: store VALUE
    /// into a bound symbol whose assignments are learned to be plain stores.
    /// Any other symbol, or a void one, is left alone and answers false.
    #[inline(always)]
    pub(crate) fn store_plain_value(&self, value: Value) -> bool {
        if self.state.get() & (PLAIN_STORE | VALUE_PRESENT | INT_FORWARD)
            != PLAIN_STORE | VALUE_PRESENT
        {
            return false;
        }
        self.payload.set(SymbolPayload { value });
        true
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
        let name = self.internal.as_str();
        let kboard = if crate::lisp::types::symbols::KEYBOARD_VARIABLES.contains(&name) {
            KBOARD_FORWARD
        } else {
            0
        };
        let effect = if STORE_EFFECT_VARIABLES.contains(&name) {
            STORE_EFFECT
        } else {
            0
        };
        let buffer_slot = if BUFFER_SLOT_VARIABLES.contains(&name) {
            BUFFER_SLOT
        } else {
            0
        };
        let converts = if STORE_CONVERTING_VARIABLES.contains(&name) {
            STORE_CONVERTS
        } else {
            0
        };
        self.state.set(if present {
            (self.state.get() | C_SLOT | kboard | effect | buffer_slot | converts) & !PLAIN_STORE
        } else {
            self.state.get()
                & !(C_SLOT | KBOARD_FORWARD | STORE_EFFECT | BUFFER_SLOT | STORE_CONVERTS)
        });
    }

    /// Whether this C variable is one of a terminal's KBOARD variables.
    pub(crate) fn is_kboard_forwarded(&self) -> bool {
        self.state.get() & KBOARD_FORWARD != 0
    }

    /// Whether a store to this C variable does more than store its cell
    /// (`STORE_EFFECT_VARIABLES') or reaches a terminal's KBOARD.
    #[inline(always)]
    pub(crate) fn has_store_effect(&self) -> bool {
        self.state.get() & (STORE_EFFECT | KBOARD_FORWARD) != 0
    }

    pub(crate) fn is_constant(&self) -> bool {
        self.state.get() & CONSTANT != 0
    }

    #[inline(always)]
    pub(crate) fn is_trapped_write(&self) -> bool {
        self.state.get() & TRAPPED_WRITE != 0
    }

    pub(crate) fn set_trapped_write(&self, trapped: bool) {
        self.state.set(if trapped {
            self.state.get() | TRAPPED_WRITE
        } else {
            self.state.get() & !TRAPPED_WRITE
        });
    }

    pub(crate) fn set_constant(&self, constant: bool) {
        self.state.set(if constant {
            (self.state.get() | CONSTANT) & !PLAIN_STORE
        } else {
            self.state.get() & !CONSTANT
        });
    }

    /// Note that the symbol was interned when its interpreter's
    /// construction ended (`image_state' leaves it out).
    pub(crate) fn mark_builtin(&self) {
        self.state.set(self.state.get() | BUILTIN);
    }

    pub(crate) fn is_builtin(&self) -> bool {
        self.state.get() & BUILTIN != 0
    }

    /// The state an image keeps for a symbol it maps in place, or None for
    /// a symbol whose state is more than its cell's: a builtin one, one of
    /// `nil' and `t', an uninterned one, a C variable (its slot, counter,
    /// terminal or buffer field is this process's) or a watched one (its
    /// watchers are the interpreter's).  A learned plain store is learned
    /// again.
    pub(crate) fn image_state(&self) -> Option<u16> {
        let state = self.state.get();
        (state
            & (BUILTIN
                | BUILTIN_NIL
                | BUILTIN_T
                | UNINTERNED
                | C_SLOT
                | INT_FORWARD
                | KBOARD_FORWARD
                | STORE_EFFECT
                | BUFFER_SLOT
                | STORE_CONVERTS
                | TRAPPED_WRITE)
            == 0)
            .then_some(state & !PLAIN_STORE)
    }
}

// The cell's layout, which an image's symbol blocks hold (`image_cell_words').
const _: () = {
    use std::mem::offset_of;
    assert!(offset_of!(SymbolCell, internal) == 0);
    assert!(offset_of!(SymbolText, text) == 0 && offset_of!(SymbolText, len) == 8);
    assert!(offset_of!(SymbolCell, lisp_name) == 16);
    assert!(offset_of!(SymbolCell, function) == 24);
    assert!(offset_of!(SymbolCell, plist) == 32);
    assert!(offset_of!(SymbolCell, payload) == 40);
    assert!(offset_of!(SymbolCell, next) == 48);
    assert!(offset_of!(SymbolCell, flags) == 56);
    assert!(offset_of!(SymbolCell, image) == 57);
    assert!(offset_of!(SymbolCell, state) == 58);
    assert!(offset_of!(SymbolCell, mark) == 60);
};

/// The words of a symbol cell an image maps: the key text's offset in the
/// image and its length, the name, function, plist and value words, no
/// next symbol (the loader files the symbol), and the flags, the image
/// byte and STATE, the mark the loader's.  The offsets and the Lisp words
/// are the image's, made the process's by relocations.
pub(crate) fn image_cell_words(
    key: (u64, usize),
    lisp: [u64; 4],
    flags: u8,
    state: u16,
) -> [u64; 8] {
    [
        key.0,
        key.1 as u64,
        lisp[0],
        lisp[1],
        lisp[2],
        lisp[3],
        0,
        u64::from(flags) | 1 << 8 | u64::from(state) << 16,
    ]
}

/// The offset of the image's INDEXth symbol cell from its first block:
/// the blocks follow one another, each `SYMBOLS_PER_BLOCK' cells, whose
/// bytes keep a block's alignment.
pub(crate) const fn image_symbol_offset(index: usize) -> usize {
    index * SYMBOL_CELL_SIZE
}

/// The bytes of the blocks holding COUNT image symbols.
pub(crate) const fn image_symbol_bytes(count: usize) -> usize {
    count.div_ceil(SYMBOLS_PER_BLOCK) * SYMBOLS_PER_BLOCK * SYMBOL_CELL_SIZE
}

const _: () = assert!((SYMBOLS_PER_BLOCK * SYMBOL_CELL_SIZE).is_multiple_of(64));

/// pdumper's mapped symbols: the image's symbol blocks made the heap's
/// image blocks where they lie, their COUNT cells marked in the current
/// epoch, the last block's other cells free (on no free list: the image's
/// blocks are never allocated from or given back).
///
/// # Safety
/// START, aligned to a cache line, begins blocks of COUNT cells laid out
/// as `image_cell_words' lays them out and relocated, which stay for the
/// life of the process and nothing else uses.
pub(crate) unsafe fn adopt_image_symbol_blocks(start: *mut u8, count: usize) {
    let start = start as usize;
    assert!(start.is_multiple_of(64), "unaligned image symbols");
    let epoch = super::super::types::current_mark_epoch();
    let block_bytes = SYMBOLS_PER_BLOCK * SYMBOL_CELL_SIZE;
    let mut blocks = Vec::with_capacity(count.div_ceil(SYMBOLS_PER_BLOCK));
    for block in 0..count.div_ceil(SYMBOLS_PER_BLOCK) {
        let base = start + block * block_bytes;
        let cells = (count - block * SYMBOLS_PER_BLOCK).min(SYMBOLS_PER_BLOCK);
        for index in 0..SYMBOLS_PER_BLOCK {
            let cell = (base + index * SYMBOL_CELL_SIZE) as *mut SymbolCell;
            // SAFETY: inside the block; the mark word is written as a word.
            unsafe {
                std::ptr::addr_of_mut!((*cell).mark)
                    .cast::<u32>()
                    .write(if index < cells { epoch } else { FREE_MARK })
            };
        }
        blocks.push((base, block_bytes));
    }
    super::register_image_blocks(BlockKind::Symbol, &blocks);
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

    /// Mark the symbol for EPOCH; true when it was not yet marked in it.
    #[inline]
    pub(crate) fn mark(&self, epoch: u32) -> bool {
        let cell = self.cell();
        let marked = cell.mark.mark(epoch);
        if marked && cell.image {
            super::IMAGE_SYMBOLS.note_marked();
        }
        marked
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
        (*slot).image = super::image_region_open();
        (*slot)
            .mark
            .set_raw(super::super::types::current_mark_epoch());
        SymbolRef(NonNull::new_unchecked(slot))
    }
}

/// The next symbol takes a new block (see `ImageRegion').
pub(super) fn abandon_allocation_frontier() {
    SYMBOL_FREE_LIST.store(std::ptr::null_mut(), Ordering::Relaxed);
    SYMBOL_BUMP_NEXT.store(SYMBOL_BUMP_END.load(Ordering::Relaxed), Ordering::Relaxed);
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
    // When no image symbol died, the image's blocks are left unread
    // (checked builds read them and assert so).
    let image_unchanged = super::IMAGE_SYMBOLS.unchanged();
    let mut image_live = 0;
    for (start, image_block) in sweep_blocks_of(BlockKind::Symbol) {
        if image_block && image_unchanged {
            if cfg!(debug_assertions) {
                for index in 0..SYMBOLS_PER_BLOCK {
                    let cell = (start + index * SYMBOL_CELL_SIZE) as *mut SymbolCell;
                    // SAFETY: inside a registered symbol block.
                    let mark = unsafe { (*cell).mark.raw() };
                    assert!(
                        mark == epoch || mark == FREE_MARK,
                        "an unread image block holds a dead symbol at {:#x}",
                        cell as usize
                    );
                }
            }
            continue;
        }
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
                num_used += usize::from(!image_block);
                // SAFETY: an allocated cell.
                image_live += usize::from(unsafe { (*cell).image });
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
            if image_block {
                continue;
            }
            // SAFETY: a free cell's first word is the free-list link.
            unsafe { (*cell.cast::<FreeSymbol>()).next = free_list };
            free_list = cell;
        }
        if image_block {
            if this_free == SYMBOLS_PER_BLOCK {
                released.push(start);
            }
        } else if this_free == SYMBOLS_PER_BLOCK && num_free > SYMBOLS_PER_BLOCK {
            free_list = chain_before;
            released.push(start);
        } else {
            num_free += this_free;
        }
    }
    SYMBOL_FREE_LIST.store(free_list, Ordering::Relaxed);
    if !image_unchanged {
        super::IMAGE_SYMBOLS.set_live(image_live);
    }
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
