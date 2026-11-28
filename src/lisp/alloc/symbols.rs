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
//! pre-representation deviation the ledger records). Name, value/alias,
//! function, plist and flags are authoritative object fields, and the mark
//! is lisp.h's gcmarkbit, which the sweep clears. Remaining host name and
//! localized/forwarded adapters must be removed before this migration is
//! complete.

use super::super::types::{SymbolName, Value};
use super::{BlockKind, SYMBOLS_PER_BLOCK, new_block, release_block, sweep_blocks_of};
use std::cell::Cell;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};

/// The host text a symbol keeps beside its Lisp name, written once and
/// never changed after: a box the cell owns and frees with it, text of
/// the permanent image, which lives as long as the process, or none yet
/// (`pending': a symbol of Fmake_symbol, whose text is built when a
/// reader first asks for one).  Two words: the text's address, and its
/// length with the top bit set when the box is the cell's.
///
/// A pending text's length word has `PENDING_TEXT' set, which no text's
/// length has (no text that long fits in the address space), so a
/// comparison of lengths (`is') finds it equal to no text without a test
/// of its own; its address word is that of `PENDING_BYTE', the
/// uninterned marker, so that the read of a first byte (`first_byte')
/// reads a byte there, the marker, not one of the text it would be
/// built as.
///
/// The words are cells, so the one write of a pending text goes through
/// a shared reference: symbols are read and written by the OS thread that
/// holds the runtime, as the obarrays are, and no reference to
/// a pending text exists (`get' gives none), so no reader sees the words
/// change.
#[repr(C)]
pub(crate) struct SymbolText {
    text: Cell<*const u8>,
    len: Cell<usize>,
}

const OWNED_TEXT: usize = 1 << (usize::BITS - 1);
const PENDING_TEXT: usize = 1 << (usize::BITS - 2);
const PENDING_BYTE: &str = "\u{1F}";

impl SymbolText {
    pub(crate) fn owned(text: String) -> Self {
        let (text, len) = Self::boxed(text);
        Self {
            text: Cell::new(text),
            len: Cell::new(len),
        }
    }

    /// TEXT in a box of its own, and the length word that says so.
    fn boxed(text: String) -> (*const u8, usize) {
        let len = text.len();
        let text = Box::into_raw(text.into_boxed_str());
        (text.cast::<u8>().cast_const(), len | OWNED_TEXT)
    }

    /// TEXT of the permanent image.
    pub(crate) fn image(text: &'static str) -> Self {
        Self {
            text: Cell::new(text.as_ptr()),
            len: Cell::new(text.len()),
        }
    }

    /// No text yet: `install' writes it.
    pub(crate) const fn pending() -> Self {
        Self {
            text: Cell::new(PENDING_BYTE.as_ptr()),
            len: Cell::new(PENDING_TEXT),
        }
    }

    /// No text yet, for a symbol of the private obarray OBARRAY, whose
    /// text the builder makes with the obarray's id: beside
    /// `PENDING_TEXT', the length word holds the id plus one, zero for no
    /// obarray.
    pub(crate) fn pending_private(obarray: u64) -> Self {
        let len = usize::try_from(obarray)
            .ok()
            .and_then(|id| id.checked_add(1))
            .filter(|len| len & (OWNED_TEXT | PENDING_TEXT) == 0)
            .expect("an obarray's id");
        Self {
            text: Cell::new(PENDING_BYTE.as_ptr()),
            len: Cell::new(PENDING_TEXT | len),
        }
    }

    /// The private obarray a pending text was made for
    /// (`pending_private'), or None for a written text or an uninterned
    /// symbol's.
    pub(crate) fn pending_obarray(&self) -> Option<u64> {
        let len = self.len.get();
        (len & PENDING_TEXT != 0 && len != PENDING_TEXT).then(|| (len - PENDING_TEXT - 1) as u64)
    }

    /// The text, or None while it is pending.
    #[inline(always)]
    pub(crate) fn get(&self) -> Option<&str> {
        let len = self.len.get();
        if len & PENDING_TEXT != 0 {
            return None;
        }
        // SAFETY: a written text was a `str' and is never written again;
        // a box is freed only with the cell.
        Some(unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                self.text.get(),
                len & !OWNED_TEXT,
            ))
        })
    }

    /// Whether the text is TEXT: never while it is pending.
    #[inline(always)]
    pub(crate) fn is(&self, text: &str) -> bool {
        let len = self.len.get() & !OWNED_TEXT;
        // SAFETY: equal lengths are a written text's, LEN bytes at its
        // address (see the type).
        len == text.len()
            && unsafe { std::slice::from_raw_parts(self.text.get(), len) } == text.as_bytes()
    }

    /// The text's first byte, or None for an empty text; for a pending
    /// text, the uninterned marker (see the type).
    #[inline(always)]
    pub(crate) fn first_byte(&self) -> Option<u8> {
        // SAFETY: a nonempty text, or a pending one, has a byte at its
        // address (see the type).
        (self.len.get() & !OWNED_TEXT != 0).then(|| unsafe { *self.text.get() })
    }

    /// Write TEXT as the text of a pending symbol, once (see the type).
    pub(crate) fn install(&self, text: String) -> &str {
        assert!(
            self.len.get() & PENDING_TEXT != 0,
            "a symbol's host text written twice"
        );
        let (text, len) = Self::boxed(text);
        self.text.set(text);
        self.len.set(len);
        self.get().expect("the text just written")
    }
}

impl Drop for SymbolText {
    fn drop(&mut self) {
        let len = self.len.get();
        if len & (OWNED_TEXT | PENDING_TEXT) == OWNED_TEXT {
            let text =
                std::ptr::slice_from_raw_parts_mut(self.text.get().cast_mut(), len & !OWNED_TEXT);
            // SAFETY: the owned text is the box `boxed' made.
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
    /// Bit 0 (`GCMARKBIT'): lisp.h's `gcmarkbit', which the mark phase sets
    /// in a cell of the heap and `sweep_symbols' clears.
    /// Bit 7 (`DUMPED_CELL'): a mapped cell, as the image writes it.
    /// Bits 1 and 2: lisp.h's `interned' (`SymbolInterned'), in GNU's
    /// encoding.  Bits 3 to 5: a forward's kind (`FWD_KIND_BITS'), or 3 to
    /// 6 a kboard field's offset (`KBOARD_OFFSET_BITS').
    pub(crate) bits: Cell<u8>,
    pub(crate) state: Cell<u16>,
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
// buffer.c: a DEFVAR_PER_BUFFER slot (BUFFER_OBJFWD), set with C_SLOT on
// the variables buffer.c defines, as opposed to the ones Lisp or other C
// files make local per buffer.
const NATIVE_BUFFER_SLOT: u16 = 16;
// The fixed ABI words designate these actual objects, not any later symbol
// whose name happens to be nil or t after the original is uninterned.
const BUILTIN_NIL: u16 = 64;
const BUILTIN_T: u16 = 128;
// lisp.h: trapped_write == SYMBOL_NOWRITE. This remains on the object
// after uninterning and is copied/restored with its other header state.
const CONSTANT: u16 = 256;
// lisp.h: redirect == SYMBOL_FORWARDED to a Lisp_Intfwd, Lisp_Objfwd or
// Lisp_Boolfwd. Set together with VALUE_PRESENT; the payload is the
// forward (`LispFwd'), whose kind the bits hold (`FWD_KIND_BITS').
const FORWARD: u16 = 512;
// lisp.h: redirect == SYMBOL_FORWARDED to a Lisp_Kboard_Objfwd: the
// payload names the editor's current_kboard and the bits hold the field's
// offset (`KBOARD_OFFSET_BITS'), set by `forward_to_kboard' on the
// variables whose values live in each terminal's KBOARD.  VALUE_PRESENT
// beside it says the symbol is bound (`makunbound' clears it).
const KBOARD_FORWARD: u16 = 1024;
// Set with C_SLOT on the C variables whose store also reaches interpreter
// state beyond the variable (`STORE_EFFECT_VARIABLES'), and on a forwarded
// variable made local (`localize_forward'), so a store to any other
// forwarded variable is known to be the variable's alone without a name
// check.
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
pub(crate) const STORE_CONVERTING_VARIABLES: [&str; 1] = ["display-hourglass"];

/// The forwarded variables `Interpreter::set_global_binding_resolved' does
/// more for than store the variable: the three Ffuncall's depth gate
/// reads, undo-inhibit-record-point (which every buffer holds),
/// case-fold-search and parse-sexp-lookup-properties, local in the buffers
/// from the first (a store of the binding loaded for the current buffer
/// reaches their C variables), and the buffers' text conversion style.  A forwarded variable made local
/// later gets the bit then (`SymbolCell::localize_forward').
pub(crate) const STORE_EFFECT_VARIABLES: [&str; 7] = [
    "quit-flag",
    "max-lisp-eval-depth",
    "debug-on-next-call",
    "text-conversion-style",
    "undo-inhibit-record-point",
    "case-fold-search",
    "parse-sexp-lookup-properties",
];

/// Bit 0: lisp.h's `gcmarkbit', the first bit of Lisp_Symbol's flags, set
/// from the mark phase to the sweep and clear in every cell otherwise, a
/// free one's included.
const GCMARKBIT: u8 = 1;
/// Bit 7: a cell of the image's mapped blocks, marked in the dump's
/// mark_bits (`DUMP'), never by its gcmarkbit.
const DUMPED_CELL: u8 = 1 << 7;
const INTERNED_SHIFT: u32 = 1;
const INTERNED_BITS: u8 = 3 << INTERNED_SHIFT;
/// Bits 3 to 5: the kind of a FORWARD symbol's forward (lisp.h's
/// `enum Lisp_Fwd_Type'), meaningless without FORWARD.
const FWD_KIND_SHIFT: u32 = 3;
const FWD_KIND_BITS: u8 = 7 << FWD_KIND_SHIFT;
const KIND_COUNTER: u8 = 0;
const KIND_OBJ: u8 = 1 << FWD_KIND_SHIFT;
const KIND_BOOL: u8 = 2 << FWD_KIND_SHIFT;
const KIND_INT: u8 = 3 << FWD_KIND_SHIFT;
const KIND_STATIC_BOOL: u8 = 4 << FWD_KIND_SHIFT;
/// Bits 3 to 6: a KBOARD_FORWARD symbol's field offset in struct kboard
/// (Lisp_Kboard_Objfwd's `offset'), meaningless without KBOARD_FORWARD.
const KBOARD_OFFSET_BITS: u8 = 15 << FWD_KIND_SHIFT;

/// The state bits a read in line (`unlocalized_value') tests, and the
/// test of the flags, bits and state as one word (`SymbolCell::header'):
/// localized in no buffer, and a value in the cell, or a forward to a
/// DEFVAR_LISP.  A symbol that forwards nowhere has its kind and offset
/// bits clear (`clear_forward_bits'), so one masked word tells the two
/// cases apart.
const INLINE_READ_STATE: u16 = VALUE_PRESENT | ALIAS | FORWARD | KBOARD_FORWARD | BUFFER_SLOT;
const INLINE_READ_TEST: u32 = crate::lisp::eval::symbol_cell_flags::LOCALIZED as u32
    | (KBOARD_OFFSET_BITS as u32) << 8
    | (INLINE_READ_STATE as u32) << 16;
const UNLOCALIZED_PLAIN: u32 = (VALUE_PRESENT as u32) << 16;
const FORWARDED_OBJECT: u32 = (KIND_OBJ as u32) << 8 | ((VALUE_PRESENT | FORWARD) as u32) << 16;

/// lisp.h's `union Lisp_Fwd' for the C variables a symbol forwards to:
/// Lisp_Intfwd (one of alloc.c's process-wide counters, or an editor's
/// DEFVAR_INT), Lisp_Objfwd (a DEFVAR_LISP) and Lisp_Boolfwd (an editor's
/// DEFVAR_BOOL, or the process's one a fault handler reads).  Each names
/// the variable's storage, which outlives the forward: a static, or an
/// editor's variable, which lives as long as the editor, whose state
/// detaches its symbols from it when it is dropped
/// (`SymbolCell::unforward').
#[derive(Clone, Copy, Debug)]
pub(crate) enum LispFwd {
    Counter(&'static AtomicI64),
    Int(NonNull<Cell<i64>>),
    Obj(NonNull<Cell<Value>>),
    Bool(NonNull<Cell<bool>>),
    StaticBool(&'static AtomicBool),
}

/// lisp.h's `enum symbol_interned', the values of Lisp_Symbol.interned.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum SymbolInterned {
    /// SYMBOL_UNINTERNED: in no obarray (init_symbol, Funintern).
    Uninterned = 0,
    /// SYMBOL_INTERNED: in an obarray other than the initial one.
    Interned = 1,
    /// SYMBOL_INTERNED_IN_INITIAL_OBARRAY.
    InInitialObarray = 2,
}

#[derive(Clone, Copy)]
pub(crate) union SymbolPayload {
    value: Value,
    alias: SymbolName,
    /// `Lisp_Intfwd.intvar': one of alloc.c's process-wide counters.
    counter: &'static AtomicI64,
    /// `Lisp_Intfwd.intvar': an editor's DEFVAR_INT.
    intvar: NonNull<Cell<i64>>,
    /// `Lisp_Objfwd.objvar'.
    objvar: NonNull<Cell<Value>>,
    /// `Lisp_Boolfwd.boolvar': an editor's DEFVAR_BOOL, or the process's.
    boolvar: NonNull<Cell<bool>>,
    static_bool: &'static AtomicBool,
    /// keyboard.c's current_kboard, which a Lisp_Kboard_Objfwd's offset
    /// is read in.
    kboard: NonNull<Cell<NonNull<crate::lisp::eval::terminal::Kboard>>>,
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

    /// Whether this is the built-in nil or t, whose words are not their
    /// cell's (`builtin_boolean').
    #[inline(always)]
    pub(crate) fn is_builtin_boolean(&self) -> bool {
        // Both flags are in the state's low byte, which one test reads.
        const FLAGS: u8 = (BUILTIN_NIL | BUILTIN_T) as u8;
        const _: () = assert!(FLAGS as u16 == BUILTIN_NIL | BUILTIN_T);
        self.state.get() as u8 & FLAGS != 0
    }

    pub(crate) fn builtin_boolean(&self) -> Option<Value> {
        match self.state.get() & (BUILTIN_NIL | BUILTIN_T) {
            BUILTIN_NIL => Some(Value::Nil),
            BUILTIN_T => Some(Value::T),
            _ => None,
        }
    }

    /// Lisp_Symbol.interned.
    #[inline(always)]
    pub(crate) fn interned(&self) -> SymbolInterned {
        match (self.bits.get() & INTERNED_BITS) >> INTERNED_SHIFT {
            0 => SymbolInterned::Uninterned,
            1 => SymbolInterned::Interned,
            _ => SymbolInterned::InInitialObarray,
        }
    }

    /// The store of init_symbol, intern_sym and Funintern.
    pub(crate) fn set_interned(&self, interned: SymbolInterned) {
        self.bits
            .set((self.bits.get() & !INTERNED_BITS) | ((interned as u8) << INTERNED_SHIFT));
    }

    /// !SYMBOL_INTERNED_P.
    pub(crate) fn is_uninterned(&self) -> bool {
        self.interned() == SymbolInterned::Uninterned
    }

    /// Funintern's SYMBOL_UNINTERNED.
    pub(crate) fn mark_uninterned(&self) {
        self.set_interned(SymbolInterned::Uninterned);
    }

    #[inline]
    pub(crate) fn value(&self) -> Option<Value> {
        let state = self.state.get();
        if state & (VALUE_PRESENT | FORWARD | KBOARD_FORWARD) == VALUE_PRESENT {
            // SAFETY: VALUE_PRESENT without FORWARD or KBOARD_FORWARD
            // selects the value member.
            return Some(unsafe { self.payload.get().value });
        }
        self.redirected_value(state)
    }

    /// `value' of a forwarded symbol, or a void one.
    #[inline(never)]
    fn redirected_value(&self, state: u16) -> Option<Value> {
        if state & FORWARD != 0 {
            return Some(self.forwarded_value());
        }
        (state & (VALUE_PRESENT | KBOARD_FORWARD) == VALUE_PRESENT | KBOARD_FORWARD)
            .then(|| self.kboard_value())
    }

    /// data.c:do_symval_forwarding of a Lisp_Kboard_Objfwd: the field at
    /// the offset in current_kboard.
    fn kboard_value(&self) -> Value {
        // SAFETY: KBOARD_FORWARD is set only with the kboard member, which
        // names the editor's current_kboard, a live kboard while the
        // editor is (`forward_to_kboard').
        unsafe {
            self.payload
                .get()
                .kboard
                .as_ref()
                .get()
                .as_ref()
                .get(self.kboard_field())
        }
    }

    fn kboard_field(&self) -> usize {
        usize::from((self.bits.get() & KBOARD_OFFSET_BITS) >> FWD_KIND_SHIFT)
    }

    /// store_symval_forwarding of a Lisp_Kboard_Objfwd.
    fn store_kboard(&self, value: Value) {
        // SAFETY: as `kboard_value'.
        unsafe {
            self.payload
                .get()
                .kboard
                .as_ref()
                .get()
                .as_ref()
                .set(self.kboard_field(), value);
        }
    }

    /// The offset in struct kboard of the field this symbol forwards to,
    /// none for any other symbol.
    pub(crate) fn kboard_offset(&self) -> Option<usize> {
        self.is_kboard_forwarded().then(|| self.kboard_field())
    }

    /// keyboard.c's DEFVAR_KBOARD: the symbol's value is the field at
    /// OFFSET in the kboard CURRENT names, current_kboard, which keeps
    /// the value it holds.
    pub(crate) fn forward_to_kboard(
        &self,
        current: NonNull<Cell<NonNull<crate::lisp::eval::terminal::Kboard>>>,
        offset: usize,
    ) {
        assert!(
            self.alias().is_none() && self.forward().is_none(),
            "forwarding an alias or a forwarded symbol to a kboard"
        );
        let offset = u8::try_from(offset)
            .ok()
            .filter(|offset| *offset <= KBOARD_OFFSET_BITS >> FWD_KIND_SHIFT)
            .expect("a kboard field");
        self.payload.set(SymbolPayload { kboard: current });
        self.bits
            .set((self.bits.get() & !KBOARD_OFFSET_BITS) | (offset << FWD_KIND_SHIFT));
        self.state
            .set((self.state.get() & !PLAIN_STORE) | VALUE_PRESENT | KBOARD_FORWARD);
    }

    /// The symbol made plain with the value it reads, or void: data.c's
    /// set_internal storing Qunbound into a KBOARD variable's symbol, or
    /// one whose kboard goes away with its editor.
    pub(crate) fn unforward_kboard(&self) {
        if !self.is_kboard_forwarded() {
            return;
        }
        let value = self.value();
        self.state.set(self.state.get() & !KBOARD_FORWARD);
        self.clear_forward_bits();
        match value {
            Some(value) => self.payload.set(SymbolPayload { value }),
            None => self.payload.set(SymbolPayload::unbound()),
        }
    }

    /// data.c:find_symbol_value of a bound symbol that is no alias and not
    /// localized in any buffer, whose value is the one its cell holds: a
    /// plain symbol, or a C variable whose value is its cell's.  A
    /// forwarded variable's value is read through its forward
    /// (`forwarded_object_value', `value'), a buffer-held variable's
    /// elsewhere.  Any other symbol, or a void one, answers None.
    #[inline(always)]
    pub(crate) fn unlocalized_value(&self) -> Option<Value> {
        if self.header() & INLINE_READ_TEST == UNLOCALIZED_PLAIN {
            // SAFETY: VALUE_PRESENT without FORWARD selects the value member.
            return Some(unsafe { self.payload.get().value });
        }
        None
    }

    /// The flags, the bits and the state as one word, lowest first (the
    /// fields are at offsets 56, 57 and 58: see the layout's assertions),
    /// for the tests of the reads that answer in line.
    #[inline(always)]
    fn header(&self) -> u32 {
        // SAFETY: the reference covers the whole cell, and the word at
        // offset 56 is its flags, bits and state, four bytes of Cells
        // aligned to four, which nothing writes while this one thread
        // reads them.
        u32::from_le(unsafe {
            std::ptr::from_ref(self)
                .cast::<u8>()
                .add(std::mem::offset_of!(SymbolCell, flags))
                .cast::<u32>()
                .read()
        })
    }

    /// data.c:find_symbol_value of a symbol SYMBOL_FORWARDED to a
    /// DEFVAR_LISP and localized in no buffer: `*XOBJFWD (valcontents)->
    /// objvar', the C variable's value.  Any other symbol answers None.
    #[inline(always)]
    pub(crate) fn forwarded_object_value(&self) -> Option<Value> {
        if self.header() & INLINE_READ_TEST == FORWARDED_OBJECT {
            // SAFETY: KIND_OBJ is set only with the objvar member, whose
            // variable outlives the forward (`LispFwd').
            return Some(unsafe { self.payload.get().objvar.as_ref().get() });
        }
        None
    }

    /// Whether the forward of a FORWARD symbol is a Lisp_Objfwd or a
    /// Lisp_Boolfwd, whose stores need no integer check.
    #[inline(always)]
    fn forwards_object_or_bool(&self) -> bool {
        matches!(
            self.bits.get() & FWD_KIND_BITS,
            KIND_OBJ | KIND_BOOL | KIND_STATIC_BOOL
        )
    }

    /// A bound C variable whose store changes nothing but its value
    /// (`set_global_binding_resolved's cell-only case): its value in its
    /// cell, in the DEFVAR_LISP or DEFVAR_BOOL it forwards to, or in its
    /// field of current_kboard; no alias, counter, store-effect or
    /// buffer-held slot, no value
    /// conversion of its own, no watcher, not constant, and neither
    /// localized, local if set, per-buffer nor always local.
    /// eval.c:specbind's SYMBOL_FORWARDED let of it is SPECPDL_LET, a store
    /// into the slot (after the slot kind's conversion) and its restore; a
    /// KBOARD variable's records its kboard as well.
    #[inline(always)]
    pub(crate) fn is_plain_c_slot(&self) -> bool {
        use crate::lisp::eval::symbol_cell_flags::{
            ALWAYS_LOCAL, FWD_INT, LOCAL_IF_SET, LOCALIZED, PER_BUFFER,
        };
        let state = self.state.get()
            & (VALUE_PRESENT
                | ALIAS
                | C_SLOT
                | FORWARD
                | KBOARD_FORWARD
                | STORE_EFFECT
                | TRAPPED_WRITE
                | CONSTANT
                | BUFFER_SLOT
                | STORE_CONVERTS);
        (state == VALUE_PRESENT | C_SLOT
            || state == VALUE_PRESENT | C_SLOT | KBOARD_FORWARD
            || (state == VALUE_PRESENT | C_SLOT | FORWARD && self.forwards_object_or_bool()))
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
        self.state.get() & (ALIAS | FORWARD | KBOARD_FORWARD | BUFFER_SLOT) == 0
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
            // SAFETY: VALUE_PRESENT without FORWARD (which local value
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
                | FORWARD
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

    /// data.c:do_symval_forwarding: `make_int (*intvar)', `*objvar' or
    /// `*boolvar ? Qt : Qnil'.
    fn forwarded_value(&self) -> Value {
        let payload = self.payload.get();
        let truth = |truth: bool| if truth { Value::T } else { Value::Nil };
        // SAFETY: FORWARD is set only with the member its kind names, and
        // the variable outlives the forward (`LispFwd').
        unsafe {
            match self.bits.get() & FWD_KIND_BITS {
                KIND_OBJ => payload.objvar.as_ref().get(),
                KIND_BOOL => truth(payload.boolvar.as_ref().get()),
                KIND_STATIC_BOOL => truth(payload.static_bool.load(Ordering::Relaxed)),
                KIND_INT => {
                    crate::lisp::primitives::normalize_integer_value(payload.intvar.as_ref().get())
                }
                _ => crate::lisp::primitives::normalize_integer_value(
                    payload.counter.load(Ordering::Relaxed),
                ),
            }
        }
    }

    /// lread.c:defvar_int, defvar_lisp and defvar_bool: the symbol's value
    /// is the C variable FWD names itself, which keeps the value it holds.
    pub(crate) fn forward_to(&self, fwd: LispFwd) {
        assert!(self.alias().is_none(), "forwarding a symbol alias");
        let (payload, kind) = match fwd {
            LispFwd::Counter(counter) => (SymbolPayload { counter }, KIND_COUNTER),
            LispFwd::Int(intvar) => (SymbolPayload { intvar }, KIND_INT),
            LispFwd::Obj(objvar) => (SymbolPayload { objvar }, KIND_OBJ),
            LispFwd::Bool(boolvar) => (SymbolPayload { boolvar }, KIND_BOOL),
            LispFwd::StaticBool(static_bool) => (SymbolPayload { static_bool }, KIND_STATIC_BOOL),
        };
        self.payload.set(payload);
        self.bits.set((self.bits.get() & !FWD_KIND_BITS) | kind);
        self.state
            .set((self.state.get() & !PLAIN_STORE) | VALUE_PRESENT | FORWARD);
    }

    pub(crate) fn is_forwarded(&self) -> bool {
        self.state.get() & FORWARD != 0
    }

    /// The C variable this symbol forwards to, none for any other symbol.
    pub(crate) fn forward(&self) -> Option<LispFwd> {
        if self.state.get() & FORWARD == 0 {
            return None;
        }
        let payload = self.payload.get();
        // SAFETY: FORWARD is set only with the member its kind names, and
        // a counter's is a static's (`forward_to').
        Some(unsafe {
            match self.bits.get() & FWD_KIND_BITS {
                KIND_COUNTER => LispFwd::Counter(payload.counter),
                KIND_INT => LispFwd::Int(payload.intvar),
                KIND_OBJ => LispFwd::Obj(payload.objvar),
                KIND_BOOL => LispFwd::Bool(payload.boolvar),
                _ => LispFwd::StaticBool(payload.static_bool),
            }
        })
    }

    /// data.c:store_symval_forwarding into the C variable: an object as it
    /// is, a bool as `!NILP (newval)', an integer after the callers' check
    /// (FWD_INT).  A value that cannot live there (only a store that
    /// data.c:set_internal would make plain) detaches and stores plainly.
    fn store_forwarded(&self, value: Value) {
        let payload = self.payload.get();
        let unbound = matches!(value.kind(), crate::lisp::types::Kind::Unbound);
        // SAFETY: FORWARD is set only with the member its kind names, and
        // the variable outlives the forward (`LispFwd').
        match (self.bits.get() & FWD_KIND_BITS, forwarded_integer(value)) {
            (KIND_OBJ, _) if !unbound => unsafe { payload.objvar.as_ref() }.set(value),
            (KIND_BOOL, _) if !unbound => unsafe { payload.boolvar.as_ref() }.set(!value.is_nil()),
            (KIND_STATIC_BOOL, _) if !unbound => {
                unsafe { payload.static_bool }.store(!value.is_nil(), Ordering::Relaxed);
            }
            (KIND_INT, Some(integer)) => unsafe { payload.intvar.as_ref() }.set(integer),
            (KIND_COUNTER, Some(integer)) => {
                unsafe { payload.counter }.store(integer, Ordering::Relaxed);
            }
            _ => {
                self.detach_forward();
                self.payload.set(SymbolPayload { value });
                self.state.set(self.state.get() | VALUE_PRESENT);
            }
        }
    }

    /// data.c:set_internal storing Qunbound into a forwarded symbol:
    /// `sym->u.s.redirect = SYMBOL_PLAINVAL'. The C variable keeps its
    /// value (a counter keeps counting); only this symbol stops forwarding
    /// to it, and it is left void.
    fn detach_forward(&self) {
        self.payload.set(SymbolPayload::unbound());
        self.state
            .set(self.state.get() & !(VALUE_PRESENT | FORWARD));
        self.clear_forward_bits();
    }

    /// The kind and offset bits of a forward cleared, for a symbol that
    /// forwards nowhere now (`INLINE_READ_TEST').
    fn clear_forward_bits(&self) {
        self.bits.set(self.bits.get() & !KBOARD_OFFSET_BITS);
    }

    /// The symbol made void and plain, whatever it forwarded to, the C
    /// variable left as it is: pdumper.c dumps a symbol's redirect as it
    /// is, so an image's SYMBOL_PLAINVAL record of a C variable's symbol
    /// (`makunbound' detached it) installs a plain symbol, which the
    /// loading process forwarded when it defined its C variables.
    pub(crate) fn release_forward(&self) {
        if self.state.get() & (FORWARD | KBOARD_FORWARD) == 0 {
            return;
        }
        self.payload.set(SymbolPayload::unbound());
        self.state
            .set(self.state.get() & !(VALUE_PRESENT | FORWARD | KBOARD_FORWARD));
        self.clear_forward_bits();
    }

    /// The symbol's value made its cell's own, as the C variable holds it,
    /// and the forward gone: data.c:make_blv's default cell for a variable
    /// made local in a buffer (`localize_forward'), or a symbol whose C
    /// variable is going away with its editor.  A counter's symbol and any
    /// other are left alone.
    pub(crate) fn unforward(&self) {
        if !matches!(self.forward(), Some(fwd) if !matches!(fwd, LispFwd::Counter(_))) {
            return;
        }
        let value = self.forwarded_value();
        self.state.set(self.state.get() & !FORWARD);
        self.clear_forward_bits();
        self.payload.set(SymbolPayload { value });
    }

    /// data.c:Fmake_local_variable and Fmake_variable_buffer_local of a
    /// SYMBOL_FORWARDED symbol: make_blv keeps the forward beside the
    /// default cell.  The cell takes the C variable's value as the
    /// default, and a store then reaches the C variable through the
    /// evaluator's store of the binding loaded for the current buffer
    /// (STORE_EFFECT), as the C variable holds that binding.
    pub(crate) fn localize_forward(&self) {
        if matches!(self.forward(), Some(fwd) if !matches!(fwd, LispFwd::Counter(_))) {
            self.unforward();
            self.state
                .set((self.state.get() | STORE_EFFECT) & !PLAIN_STORE);
        }
    }

    pub(crate) fn set_value(&self, value: Value) {
        assert!(self.alias().is_none(), "plain store into a symbol alias");
        let state = self.state.get();
        if state & (FORWARD | KBOARD_FORWARD) != 0 {
            return self.store_redirected(value);
        }
        self.payload.set(SymbolPayload { value });
        self.state.set(state | VALUE_PRESENT);
    }

    /// The store of a forwarded symbol, which binds a void KBOARD one.
    #[inline(never)]
    fn store_redirected(&self, value: Value) {
        if self.state.get() & FORWARD != 0 {
            return self.store_forwarded(value);
        }
        self.store_kboard(value);
        self.state.set(self.state.get() | VALUE_PRESENT);
    }

    /// The value of a bound symbol whose assignments are learned to be
    /// plain stores (SYMBOL_PLAINVAL, untrapped), its cell's; none for any
    /// other symbol, or a void one.
    #[inline(always)]
    pub(crate) fn plain_value(&self) -> Option<Value> {
        // SAFETY: VALUE_PRESENT without FORWARD or KBOARD_FORWARD selects
        // the value member.
        (self.state.get() & (PLAIN_STORE | VALUE_PRESENT | FORWARD | KBOARD_FORWARD)
            == PLAIN_STORE | VALUE_PRESENT)
            .then(|| unsafe { self.payload.get().value })
    }

    /// data.c:set_internal's SYMBOL_PLAINVAL, untrapped case: store VALUE
    /// into a bound symbol whose assignments are learned to be plain stores.
    /// Any other symbol, or a void one, is left alone and answers false.
    #[inline(always)]
    pub(crate) fn store_plain_value(&self, value: Value) -> bool {
        if self.state.get() & (PLAIN_STORE | VALUE_PRESENT | FORWARD) != PLAIN_STORE | VALUE_PRESENT
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
        if state & (FORWARD | KBOARD_FORWARD) != 0 {
            self.store_redirected(value);
        } else {
            self.payload.set(SymbolPayload { value });
        }
        true
    }

    /// The value, and the symbol void.  A KBOARD variable's symbol stays
    /// forwarded to its field, which keeps its value, until it is detached
    /// from it (`unforward_kboard').
    pub(crate) fn take_value(&self) -> Option<Value> {
        let value = self.value()?;
        if self.is_kboard_forwarded() {
            self.state.set(self.state.get() & !VALUE_PRESENT);
        } else {
            self.detach_forward();
        }
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
        self.clear_forward_bits();
        self.state.set(
            (self.state.get() & !(VALUE_PRESENT | PLAIN_STORE | FORWARD | KBOARD_FORWARD)) | ALIAS,
        );
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
        // reading it here could allocate a bignum during collection.  A
        // DEFVAR_LISP's object is its own root (lread.c:defvar_lisp
        // staticpro's the C variable), traced by its owner, and a KBOARD
        // field is its kboard's, traced with its terminal.
        if self.is_forwarded() || self.is_kboard_forwarded() {
            return None;
        }
        self.value().or_else(|| self.alias().map(Value::Symbol))
    }

    pub(crate) fn has_c_slot(&self) -> bool {
        self.state.get() & C_SLOT != 0
    }

    pub(crate) fn set_c_slot(&self, present: bool) {
        // A pending text, an uninterned symbol's, names none of the C
        // variables the tables list.
        let name = self.internal.get().unwrap_or("");
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
        // A KBOARD variable's forward is `forward_to_kboard''s; losing the
        // slot detaches it.
        if !present {
            self.unforward_kboard();
        }
        self.state.set(if present {
            (self.state.get() | C_SLOT | effect | buffer_slot | converts) & !PLAIN_STORE
        } else {
            self.state.get() & !(C_SLOT | STORE_EFFECT | BUFFER_SLOT | STORE_CONVERTS)
        });
    }

    /// Note that this C variable is one of buffer.c's DEFVAR_PER_BUFFER
    /// slots.
    pub(crate) fn mark_native_buffer_slot(&self) {
        self.state.set(self.state.get() | NATIVE_BUFFER_SLOT);
    }

    /// Whether this symbol is one of buffer.c's DEFVAR_PER_BUFFER slots.
    pub(crate) fn is_native_buffer_slot(&self) -> bool {
        self.state.get() & NATIVE_BUFFER_SLOT != 0
    }

    /// Whether this C variable is one of a terminal's KBOARD variables.
    pub(crate) fn is_kboard_forwarded(&self) -> bool {
        self.state.get() & KBOARD_FORWARD != 0
    }

    /// Whether a store to this C variable does more than store its value
    /// (`STORE_EFFECT_VARIABLES').
    #[inline(always)]
    pub(crate) fn has_store_effect(&self) -> bool {
        self.state.get() & STORE_EFFECT != 0
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
                | C_SLOT
                | FORWARD
                | KBOARD_FORWARD
                | STORE_EFFECT
                | BUFFER_SLOT
                | STORE_CONVERTS
                | TRAPPED_WRITE)
            == 0
            && !self.is_uninterned())
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
    assert!(offset_of!(SymbolCell, bits) == 57);
    assert!(offset_of!(SymbolCell, state) == 58);
    // The flags, bits and state are the cell's last fields, one 32-bit
    // word (`SymbolCell::header'); the four bytes after them are padding.
    assert!(offset_of!(SymbolCell, state) + 2 == std::mem::size_of::<SymbolCell>() - 4);
};

/// The words of a symbol cell an image maps: the key text's offset in the
/// image and its length, the name, function, plist and value words, no
/// next symbol (the loader files the symbol, which sets its `interned'),
/// and the flags, the bits with the mapped flag alone (no sweep reads a
/// mapped cell, and its mark is the dump's, never its gcmarkbit) and
/// STATE.  The offsets and the Lisp words are the image's, made the
/// process's by relocations.
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
        u64::from(flags) | u64::from(DUMPED_CELL) << 8 | u64::from(state) << 16,
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
/// image blocks where they lie, their COUNT cells as the image holds them
/// (nothing is written for one), the last block's other cells free, their
/// function dead_object () (on no free list: the image's blocks are never
/// allocated from or given back).
///
/// # Safety
/// START, aligned to a cache line, begins blocks of COUNT cells laid out
/// as `image_cell_words' lays them out and relocated, which stay for the
/// life of the process and nothing else uses.
pub(crate) unsafe fn adopt_image_symbol_blocks(start: *mut u8, count: usize) {
    let start = start as usize;
    assert!(start.is_multiple_of(64), "unaligned image symbols");
    let block_bytes = SYMBOLS_PER_BLOCK * SYMBOL_CELL_SIZE;
    let mut blocks = Vec::with_capacity(count.div_ceil(SYMBOLS_PER_BLOCK));
    for block in 0..count.div_ceil(SYMBOLS_PER_BLOCK) {
        let base = start + block * block_bytes;
        let cells = (count - block * SYMBOLS_PER_BLOCK).min(SYMBOLS_PER_BLOCK);
        for index in cells..SYMBOLS_PER_BLOCK {
            let cell = (base + index * SYMBOL_CELL_SIZE) as *mut SymbolCell;
            // SAFETY: an unused cell of the last block.
            unsafe { set_free(cell) };
        }
        blocks.push((base, block_bytes));
    }
    super::register_image_blocks(BlockKind::Symbol, &blocks);
}

pub(super) const SYMBOL_CELL_SIZE: usize = std::mem::size_of::<SymbolCell>();
const _: () = assert!(SYMBOL_CELL_SIZE == 64);

thread_local! {
    /// alloc.c's `symbol_free_list', and `symbol_block' with
    /// `symbol_block_index': the cells of the OS thread that holds the
    /// runtime (see `alloc::Frontier').
    static SYMBOL_FREE_LIST: Cell<*mut SymbolCell> = const { Cell::new(std::ptr::null_mut()) };
    static SYMBOL_BUMP_NEXT: Cell<usize> = const { Cell::new(0) };
    static SYMBOL_BUMP_END: Cell<usize> = const { Cell::new(0) };
}

/// The symbols' share of `alloc::Frontier'.
pub(super) struct Frontier([usize; 3]);

impl Frontier {
    pub(super) const EMPTY: Self = Self([0; 3]);

    pub(super) fn take() -> Self {
        Self([
            SYMBOL_FREE_LIST.replace(std::ptr::null_mut()) as usize,
            SYMBOL_BUMP_NEXT.replace(0),
            SYMBOL_BUMP_END.replace(0),
        ])
    }

    /// The cells it gives next: the free list's head, the bump region's.
    #[cfg(test)]
    pub(super) fn cells(&self) -> [usize; 2] {
        let [free, next, end] = self.0;
        [free, if next < end { next } else { 0 }]
    }

    pub(super) fn install(self) {
        let [free, next, end] = self.0;
        SYMBOL_FREE_LIST.set(free as *mut SymbolCell);
        SYMBOL_BUMP_NEXT.set(next);
        SYMBOL_BUMP_END.set(end);
    }
}

/// gcstat's `total_symbols' and `total_free_symbols'.
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

    /// alloc.c's `symbol_marked_p': the dump's mark of a mapped symbol,
    /// else the cell's gcmarkbit.
    #[inline]
    pub(crate) fn is_marked(&self) -> bool {
        let bits = self.cell().bits.get();
        if bits & DUMPED_CELL != 0 {
            return super::DUMP.is_marked(self.0.as_ptr() as usize);
        }
        bits & GCMARKBIT != 0
    }

    /// alloc.c's `set_symbol_marked' after its test of `symbol_marked_p':
    /// true when the symbol was not yet marked.
    #[inline]
    pub(crate) fn mark(&self) -> bool {
        let cell = self.cell();
        let bits = cell.bits.get();
        if bits & DUMPED_CELL != 0 {
            return super::DUMP.mark(self.0.as_ptr() as usize);
        }
        if bits & GCMARKBIT != 0 {
            return false;
        }
        cell.bits.set(bits | GCMARKBIT);
        true
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
    let head = SYMBOL_FREE_LIST.get();
    let slot = if head.is_null() {
        bump_symbol()
    } else {
        // SAFETY: a free cell's first word is the free-list link.
        let next = unsafe { (*head.cast::<FreeSymbol>()).next };
        SYMBOL_FREE_LIST.set(next);
        head
    };
    LIVE_SYMBOLS.store(LIVE_SYMBOLS.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
    super::counters::tally(&super::counters::SYMBOLS_CONSED, 1);
    // init_symbol's `p->u.s.gcmarkbit = 0': a symbol is made unmarked, and
    // its function, nil, is what makes the cell an allocated one.
    debug_assert!(
        cell.bits.get() & (GCMARKBIT | DUMPED_CELL) == 0 && !cell.function.get().deadp(),
        "a symbol is made unmarked, in the heap"
    );
    // SAFETY: SLOT is a free cell of a symbol block; every field is
    // written before a handle is made.
    unsafe {
        std::ptr::write(slot, cell);
        SymbolRef(NonNull::new_unchecked(slot))
    }
}

/// The next symbol takes a new block (see `ImageRegion').
pub(super) fn abandon_allocation_frontier() {
    SYMBOL_FREE_LIST.set(std::ptr::null_mut());
    free_unused_cells();
}

/// The current block stops being the current one before it is full, which
/// alloc.c's symbol_block never does: the cells no symbol was made in
/// become free cells, as every cell of a block that is not the current
/// one is a symbol or a free cell, for `live_symbol_holding' and
/// `sweep_symbols', which read all of it.
fn free_unused_cells() {
    let end = SYMBOL_BUMP_END.get();
    for cell in (SYMBOL_BUMP_NEXT.get()..end).step_by(SYMBOL_CELL_SIZE) {
        // SAFETY: a cell of the current block at or past its index, which
        // holds no symbol.
        unsafe { set_free(cell as *mut SymbolCell) };
    }
    SYMBOL_BUMP_NEXT.set(end);
}

/// alloc.c's `symbol_block->symbols[symbol_block_index++]', after a new
/// symbol_block when the current one is full (`symbol_in_new_block').
fn bump_symbol() -> *mut SymbolCell {
    let next = SYMBOL_BUMP_NEXT.get();
    if next < SYMBOL_BUMP_END.get() {
        SYMBOL_BUMP_NEXT.set(next + SYMBOL_CELL_SIZE);
        return next as *mut SymbolCell;
    }
    symbol_in_new_block()
}

/// Fmake_symbol's `symbol_block_index == SYMBOL_BLOCK_SIZE' arm, out of
/// line as Fcons's is (`alloc::allocate_cons_in_new_block'), so that what
/// a symbol's allocation puts in line is the free list and the index.
/// Nothing is written in the new block: its cells are told from symbols
/// by the index (`live_symbol_holding').
#[cold]
#[inline(never)]
fn symbol_in_new_block() -> *mut SymbolCell {
    let block = new_block(BlockKind::Symbol);
    // A thread without the runtime took it in `new_block', and its
    // frontier with it (`alloc::Frontier'): the block that names is left
    // for the new one.  Any other thread's current block is full here.
    free_unused_cells();
    SYMBOL_BUMP_NEXT.set(block + SYMBOL_CELL_SIZE);
    SYMBOL_BUMP_END.set(block + SYMBOLS_PER_BLOCK * SYMBOL_CELL_SIZE);
    block as *mut SymbolCell
}

/// Make CELL a free cell: its function dead_object (), as alloc.c's
/// sweep_symbols leaves a symbol it frees (`symbol_free_list->u.s.function
/// = dead_object ()'), and its gcmarkbit clear, as a freed symbol's is.
///
/// # Safety
/// CELL is a cell of a symbol block that holds no symbol.
unsafe fn set_free(cell: *mut SymbolCell) {
    // SAFETY: the caller's contract; each field is written as itself.
    unsafe {
        std::ptr::addr_of_mut!((*cell).function).write(Cell::new(Value::dead_object()));
        std::ptr::addr_of_mut!((*cell).bits).write(Cell::new(0));
    }
}

/// alloc.c's `live_symbol_holding': the allocated cell containing
/// ADDRESS, or None.  "P must point into the Lisp_Symbol, not be one of
/// the unused cells in the current symbol block, and not be on the
/// free-list": `(b != symbol_block || offset / sizeof b->symbols[0] <
/// symbol_block_index) && !deadp (s->u.s.function)'.  The current block
/// and its index are the frontier's, which the thread that holds the
/// runtime has (`alloc::Frontier').
///
/// # Safety
/// START is a registered symbol block, and this thread holds the runtime.
pub(super) unsafe fn live_symbol_holding(start: usize, address: usize) -> Option<*mut SymbolCell> {
    debug_assert!(
        crate::lisp::runtime::held(),
        "the current symbol block is known to the thread that holds the runtime"
    );
    let index = (address - start) / SYMBOL_CELL_SIZE;
    if index >= SYMBOLS_PER_BLOCK {
        return None;
    }
    let cell = start + index * SYMBOL_CELL_SIZE;
    if SYMBOL_BUMP_END.get() == start + SYMBOLS_PER_BLOCK * SYMBOL_CELL_SIZE
        && cell >= SYMBOL_BUMP_NEXT.get()
    {
        // An unused cell of the current block, which nothing has written.
        return None;
    }
    let cell = cell as *mut SymbolCell;
    // SAFETY: a cell of a block that is not the current one, or one below
    // the current block's index: a symbol or a free cell, whose function
    // slot is a word in either state.
    let function = unsafe { (*cell).function.get() };
    (!function.deadp()).then_some(cell)
}

/// alloc.c's `sweep_symbols': a marked cell is kept and unmarked
/// (`sym->u.s.gcmarkbit = 0'); an unmarked one that holds a symbol is
/// released (CLEANUP sees it first: the registries an uninterned symbol's
/// key names let it go) and made a free cell; the free list is rebuilt
/// from every free cell; a block holding nothing but free cells is given
/// back once more than a block's worth of free cells is at hand.  Every
/// block outside the image's mapped ones is read at every collection, the
/// ones an image's load allocated among them, as alloc.c sweeps all that
/// is not its dump.  Returns gcstat's `total_symbols'.
pub(crate) fn sweep_symbols(mut cleanup: impl FnMut(&SymbolCell)) -> usize {
    let bump_next = SYMBOL_BUMP_NEXT.get();
    let bump_end = SYMBOL_BUMP_END.get();
    let mut free_list: *mut SymbolCell = std::ptr::null_mut();
    let mut num_free = 0usize;
    let mut num_used = 0usize;
    let mut released = Vec::new();
    for (start, image_block) in sweep_blocks_of(BlockKind::Symbol) {
        let lim = if bump_end == start + SYMBOLS_PER_BLOCK * SYMBOL_CELL_SIZE {
            (bump_next - start) / SYMBOL_CELL_SIZE
        } else {
            SYMBOLS_PER_BLOCK
        };
        let chain_before = free_list;
        let mut this_free = 0usize;
        for index in 0..lim {
            let cell = (start + index * SYMBOL_CELL_SIZE) as *mut SymbolCell;
            // SAFETY: inside a registered symbol block, below its bump
            // limit: a cell that is allocated or free, whose bits and
            // function slot are readable in either state.
            let bits = unsafe { (*cell).bits.get() };
            if bits & GCMARKBIT != 0 {
                // An image object stays out of gcstat's object totals, as
                // a dumped one does.
                num_used += usize::from(!image_block);
                // SAFETY: a marked cell holds a symbol.
                unsafe {
                    debug_assert!(
                        !(*cell).function.get().deadp(),
                        "a free symbol cell at {:#x} is marked",
                        cell as usize
                    );
                    (*cell).bits.set(bits & !GCMARKBIT);
                }
                continue;
            }
            // SAFETY: as above.
            if !unsafe { (*cell).function.get() }.deadp() {
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
                    set_free(cell);
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
    SYMBOL_FREE_LIST.set(free_list);
    for start in released {
        release_block(start, BlockKind::Symbol);
    }
    LIVE_SYMBOLS.store(num_used, Ordering::Relaxed);
    FREE_SYMBOLS.store(num_free, Ordering::Relaxed);
    num_used
}

/// The allocated symbol cells that are uninterned, live or not yet swept.
#[cfg(test)]
pub(crate) fn allocated_uninterned_symbols() -> usize {
    // The blocks and the frontier are the runtime's (`alloc::live_conses').
    crate::lisp::runtime::ensure_held();
    let bump_next = SYMBOL_BUMP_NEXT.get();
    let bump_end = SYMBOL_BUMP_END.get();
    let mut count = 0;
    for (start, _) in sweep_blocks_of(BlockKind::Symbol) {
        let lim = if bump_end == start + SYMBOLS_PER_BLOCK * SYMBOL_CELL_SIZE {
            (bump_next - start) / SYMBOL_CELL_SIZE
        } else {
            SYMBOLS_PER_BLOCK
        };
        for index in 0..lim {
            let cell = (start + index * SYMBOL_CELL_SIZE) as *const SymbolCell;
            // SAFETY: inside a registered symbol block, below its bump
            // limit: a cell that is free or allocated.
            unsafe {
                if !(*cell).function.get().deadp() && (*cell).is_uninterned() {
                    count += 1;
                }
            }
        }
    }
    count
}

/// `super::every_mark_bit_clear' for the symbol blocks: no cell's
/// gcmarkbit is set, of a symbol or of a free cell.
#[cfg(test)]
pub(super) fn every_mark_bit_clear() -> bool {
    let bump_next = SYMBOL_BUMP_NEXT.get();
    let bump_end = SYMBOL_BUMP_END.get();
    super::blocks_of(BlockKind::Symbol)
        .into_iter()
        .all(|start| {
            let lim = if bump_end == start + SYMBOLS_PER_BLOCK * SYMBOL_CELL_SIZE {
                (bump_next - start) / SYMBOL_CELL_SIZE
            } else {
                SYMBOLS_PER_BLOCK
            };
            (0..lim).all(|index| {
                let cell = (start + index * SYMBOL_CELL_SIZE) as *const SymbolCell;
                // SAFETY: a cell of a registered symbol block below its bump
                // limit, allocated or free.
                unsafe { (*cell).bits.get() & GCMARKBIT == 0 }
            })
        })
}

/// For tests: the cells of the image's mapped symbol blocks that hold a
/// symbol, and the ones that are free.
#[cfg(test)]
pub(crate) fn mapped_symbol_cells() -> [usize; 2] {
    // The blocks are the runtime's (`alloc::live_conses').
    crate::lisp::runtime::ensure_held();
    let mut cells = [0; 2];
    for start in super::mapped_blocks_of(BlockKind::Symbol) {
        for index in 0..SYMBOLS_PER_BLOCK {
            let address = start + index * SYMBOL_CELL_SIZE;
            // SAFETY: a registered symbol block.
            let found = unsafe { live_symbol_holding(start, address) };
            cells[usize::from(found.is_none())] += 1;
        }
    }
    cells
}

pub(crate) fn live_symbols() -> usize {
    // The books are the runtime's (`alloc::live_conses').
    crate::lisp::runtime::ensure_held();
    LIVE_SYMBOLS.load(Ordering::Relaxed)
}

/// gcstat's `total_free_symbols': the last sweep's count (Fmake_symbol does
/// not lower it).
pub(crate) fn free_symbols() -> usize {
    // The books are the runtime's (`alloc::live_conses').
    crate::lisp::runtime::ensure_held();
    FREE_SYMBOLS.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::{
        SYMBOL_BUMP_END, SYMBOL_BUMP_NEXT, SYMBOL_CELL_SIZE, abandon_allocation_frontier,
        bump_symbol, set_free,
    };
    use crate::lisp::alloc::SYMBOLS_PER_BLOCK;
    use crate::lisp::eval::Interpreter;
    use crate::lisp::types::{Env, Kind, Value};
    use crate::lisp::{json, primitives};

    // Every symbol is made in a frame of its own, cleared before the
    // collection that expects it dead, and the witnessed one is kept as
    // its address's complement: no word of the test's names it.
    #[inline(never)]
    fn make_symbols(interpreter: &mut Interpreter, count: usize) {
        for _ in 0..count {
            primitives::call(
                interpreter,
                "make-symbol",
                &[Value::String("dead".into())],
                &mut Env::new(),
            )
            .expect("make a symbol");
        }
    }

    #[inline(never)]
    fn witnessed_symbol(interpreter: &mut Interpreter, table: &Value) -> usize {
        let symbol = primitives::call(
            interpreter,
            "make-symbol",
            &[Value::String("first".into())],
            &mut Env::new(),
        )
        .expect("make a symbol");
        primitives::call(
            interpreter,
            "puthash",
            &[symbol, Value::T, *table],
            &mut Env::new(),
        )
        .expect("witness the symbol");
        !symbol.word()
    }

    #[inline(never)]
    fn frontier_ends_at(complement: usize) -> bool {
        SYMBOL_BUMP_END.get() == !complement
    }

    #[inline(never)]
    fn witnesses_after_collection(interpreter: &mut Interpreter, table: &Value) -> usize {
        crate::lisp::alloc::clobber_stack();
        let Kind::HashTable(id) = table.kind() else {
            panic!("a hash table");
        };
        primitives::call(interpreter, "garbage-collect", &[], &mut Env::new()).expect("collect");
        id.count()
    }

    #[test]
    fn a_dead_symbol_at_the_frontier_s_block_end_dies_in_its_first_collection() {
        // garbage_collect_reports_the_live_census's case.  Blocks are carved
        // in address order and reused last released first, and a sweep
        // releases them in address order, so the block the frontier takes
        // can end where a block already holding symbols begins: the
        // frontier's block end then names that block's first symbol.  GNU
        // frees a dead symbol at the first collection after its last
        // reference goes (symbol_block is no root), and so must a thread
        // whose stack is the OS's to scan, as this test's own (no Lisp
        // entry of it records a stack bottom).
        let mut interpreter = Interpreter::new();
        let table = json::make_hash_table(&mut interpreter, "eq", Vec::new());
        let Kind::HashTable(id) = table.kind() else {
            panic!("a hash table");
        };
        id.set_weakness(1);
        interpreter.set_global_binding("frontier-end-witnesses", table);
        // Blocks of dead symbols for the collection to release.
        abandon_allocation_frontier();
        make_symbols(&mut interpreter, 8 * SYMBOLS_PER_BLOCK);
        witnesses_after_collection(&mut interpreter, &table);
        let mut arranged = false;
        for _ in 0..4096 {
            primitives::call(&mut interpreter, "clrhash", &[table], &mut Env::new())
                .expect("forget the last try");
            abandon_allocation_frontier();
            let first = witnessed_symbol(&mut interpreter, &table);
            // The rest of its block, then the first symbol of the next.
            make_symbols(&mut interpreter, SYMBOLS_PER_BLOCK);
            if frontier_ends_at(first) {
                arranged = true;
                break;
            }
        }
        assert!(arranged, "no block was taken just below a symbol's");
        crate::lisp::alloc::clobber_stack();
        assert_eq!(
            witnesses_after_collection(&mut interpreter, &table),
            0,
            "the dead symbol at the frontier's block end is freed"
        );
    }

    #[inline(never)]
    fn kept_symbol(interpreter: &mut Interpreter, table: &Value, holder: &str) {
        let symbol = primitives::call(
            interpreter,
            "make-symbol",
            &[Value::String("kept".into())],
            &mut Env::new(),
        )
        .expect("make a symbol");
        let Kind::Symbol(name) = symbol.kind() else {
            panic!("a symbol");
        };
        assert!(!name.is_marked(), "Fmake_symbol makes a symbol unmarked");
        primitives::call(
            interpreter,
            "puthash",
            &[symbol, Value::T, *table],
            &mut Env::new(),
        )
        .expect("witness the symbol");
        interpreter.set_global_binding(holder, symbol);
    }

    #[test]
    fn the_sweep_that_keeps_a_symbol_clears_its_mark() {
        // alloc.c:sweep_symbols, of a symbol whose gcmarkbit is set:
        // `++num_used; sym->u.s.gcmarkbit = 0;' -- the symbol it keeps is
        // unmarked again, so the next collection frees it once nothing
        // holds it.
        let _runtime = crate::Runtime::lock();
        let mut interpreter = Interpreter::new();
        let table = json::make_hash_table(&mut interpreter, "eq", Vec::new());
        let Kind::HashTable(id) = table.kind() else {
            panic!("a hash table");
        };
        id.set_weakness(1);
        interpreter.set_global_binding("kept-symbol-witnesses", table);
        kept_symbol(&mut interpreter, &table, "kept-symbol-holder");
        for _ in 0..2 {
            assert_eq!(
                witnesses_after_collection(&mut interpreter, &table),
                1,
                "the symbol a variable holds is kept"
            );
            assert!(
                crate::lisp::alloc::every_mark_bit_clear(),
                "the sweep left no symbol marked"
            );
        }
        interpreter.set_global_binding("kept-symbol-holder", Value::Nil);
        assert_eq!(
            witnesses_after_collection(&mut interpreter, &table),
            0,
            "the symbol nothing holds is freed by the next collection"
        );
    }

    /// The word of the function slot of the symbol cell whose word's
    /// complement is COMPLEMENT, read as a word whatever the cell holds.
    #[inline(never)]
    fn function_word(complement: usize) -> usize {
        let cell = !complement as *const super::SymbolCell;
        // SAFETY: a cell of a registered symbol block, allocated or free;
        // the slot is a word in either state.
        unsafe { std::ptr::addr_of!((*cell).function).cast::<usize>().read() }
    }

    /// Whether the conservative scan takes that cell's address for a
    /// symbol (alloc.c's live_symbol_holding).
    #[inline(never)]
    fn scan_finds(complement: usize) -> bool {
        crate::lisp::alloc::conservative_value(!complement).is_some()
    }

    #[test]
    fn a_freed_symbol_s_function_is_the_dead_object() {
        // alloc.c:sweep_symbols puts an unmarked symbol on the free list
        // with `symbol_free_list->u.s.function = dead_object ()', and
        // live_symbol_holding takes a cell for a symbol unless its function
        // is that (deadp): lisp.h's dead_object is make_lisp_ptr (NULL,
        // Lisp_String), the string tag, 4, on a null pointer, which no
        // object's word is.
        const DEAD_OBJECT: usize = 4;
        assert_eq!(Value::dead_object().word(), DEAD_OBJECT);
        assert!(Value::dead_object().deadp() && !Value::Nil.deadp());
        let _runtime = crate::Runtime::lock();
        let mut interpreter = Interpreter::new();
        let table = json::make_hash_table(&mut interpreter, "eq", Vec::new());
        let Kind::HashTable(id) = table.kind() else {
            panic!("a hash table");
        };
        id.set_weakness(1);
        interpreter.set_global_binding("dead-object-witnesses", table);
        let symbol = witnessed_symbol(&mut interpreter, &table);
        assert_eq!(
            function_word(symbol),
            Value::Nil.word(),
            "Fmake_symbol's function is nil"
        );
        assert!(scan_finds(symbol), "the live cell is found");
        assert_eq!(
            witnesses_after_collection(&mut interpreter, &table),
            0,
            "the symbol nothing holds is freed"
        );
        assert_eq!(
            function_word(symbol),
            DEAD_OBJECT,
            "the freed cell's function is dead_object ()"
        );
        assert!(
            !scan_finds(symbol),
            "live_symbol_holding refuses the freed cell"
        );
    }

    /// lisp.h's dead_object: the string tag on a null pointer.
    const DEAD_OBJECT: usize = 4;

    /// The cells of this thread's frontier block that no symbol has been
    /// made in, as the complements of their addresses.
    #[inline(never)]
    fn unused_frontier_cells() -> Vec<usize> {
        (SYMBOL_BUMP_NEXT.get()..SYMBOL_BUMP_END.get())
            .step_by(SYMBOL_CELL_SIZE)
            .map(|cell| !cell)
            .collect()
    }

    /// Store WORD in the function slot of the cell whose address's
    /// complement is COMPLEMENT.
    #[inline(never)]
    fn write_function(complement: usize, word: usize) {
        let cell = !complement as *mut super::SymbolCell;
        // SAFETY: a cell of a registered symbol block that holds no symbol;
        // the slot is a word.
        unsafe {
            std::ptr::addr_of_mut!((*cell).function)
                .cast::<usize>()
                .write(word)
        };
    }

    /// A word no free cell's function holds and no symbol of these tests':
    /// what a block's memory may hold before a symbol is made in it.
    fn stale_word() -> usize {
        Value::Integer(0x5a5a).word()
    }

    #[test]
    fn a_cell_past_the_frontier_is_no_symbol_whatever_it_holds() {
        // alloc.c's Fmake_symbol takes the cells of a new symbol_block one
        // at a time, by symbol_block_index, and nothing writes a cell
        // before a symbol is made in it; live_symbol_holding refuses "one
        // of the unused cells in the current symbol block" by that index
        // (`b != symbol_block || offset / sizeof b->symbols[0] <
        // symbol_block_index'), whatever the cell holds.
        let (unused, found) = {
            let _runtime = crate::Runtime::lock();
            let mut interpreter = Interpreter::new();
            abandon_allocation_frontier();
            make_symbols(&mut interpreter, 1);
            let unused = unused_frontier_cells();
            for &cell in &unused {
                write_function(cell, stale_word());
            }
            // A collection reads the block to its index and no further.
            primitives::call(&mut interpreter, "garbage-collect", &[], &mut Env::new())
                .expect("collect");
            let found = unused.iter().filter(|&&cell| scan_finds(cell)).count();
            // Where a fresh block's cells are made free ones, these are
            // free ones again.
            for &cell in &unused {
                write_function(cell, DEAD_OBJECT);
            }
            (unused.len(), found)
        };
        assert_eq!(unused, SYMBOLS_PER_BLOCK - 1, "a new block's other cells");
        assert_eq!(found, 0, "an unused cell of the current block is a symbol");
    }

    #[test]
    fn the_cells_an_abandoned_block_leaves_are_free_cells() {
        // alloc.c leaves its symbol_block only when it is full.  An image's
        // load leaves the current one with cells no symbol was made in
        // (`ImageRegion'), and the block, no longer the current one, is
        // then read whole by live_symbol_holding and sweep_symbols: the
        // cells left become free cells there, their function
        // dead_object ().
        let (unused, found, stale) = {
            let _runtime = crate::Runtime::lock();
            let mut interpreter = Interpreter::new();
            abandon_allocation_frontier();
            make_symbols(&mut interpreter, 1);
            let unused = unused_frontier_cells();
            for &cell in &unused {
                write_function(cell, stale_word());
            }
            abandon_allocation_frontier();
            let found = unused.iter().filter(|&&cell| scan_finds(cell)).count();
            let stale = unused
                .iter()
                .filter(|&&cell| function_word(cell) != DEAD_OBJECT)
                .count();
            for &cell in &unused {
                write_function(cell, DEAD_OBJECT);
            }
            (unused.len(), found, stale)
        };
        assert_eq!(unused, SYMBOLS_PER_BLOCK - 1, "a new block's other cells");
        assert_eq!(found, 0, "a cell the abandoned block left is a symbol");
        assert_eq!(stale, 0, "a cell the abandoned block left is not free");
    }

    #[test]
    fn a_thread_that_takes_the_runtime_for_a_symbol_frees_the_block_it_leaves() {
        // A thread without the runtime has no frontier: its first symbol
        // takes the runtime, which gives it the frontier, and goes in a
        // new block all the same (`alloc::Frontier').  The block the
        // frontier named is left like an abandoned one, and its unused
        // cells are free cells from then on.
        let unused = std::thread::spawn(|| {
            crate::lisp::runtime::ensure_held();
            abandon_allocation_frontier();
            // SAFETY: the cell just taken holds no symbol.
            unsafe { set_free(bump_symbol()) };
            let unused = unused_frontier_cells();
            for &cell in &unused {
                write_function(cell, stale_word());
            }
            unused
        })
        .join()
        .expect("the thread that leaves the runtime a frontier");
        let (cells, stale) = std::thread::spawn(move || {
            // This thread's first allocation of any kind.
            // SAFETY: the cell just taken holds no symbol.
            unsafe { set_free(bump_symbol()) };
            // A cell another thread made a symbol in meanwhile holds
            // neither word; one left as it was is not a free cell.
            let stale = unused
                .iter()
                .filter(|&&cell| function_word(cell) == stale_word() && scan_finds(cell))
                .count();
            for &cell in &unused {
                if function_word(cell) == stale_word() {
                    write_function(cell, DEAD_OBJECT);
                }
            }
            (unused.len(), stale)
        })
        .join()
        .expect("the thread whose first allocation is a symbol");
        assert_eq!(cells, SYMBOLS_PER_BLOCK - 1, "a new block's other cells");
        assert_eq!(stale, 0, "a cell of the block the thread left is a symbol");
    }
}
