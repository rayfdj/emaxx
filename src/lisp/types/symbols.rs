//! Canonical symbols owned by an editor entry.
//!
//! GNU's Vobarray and Qsymbol references belong to one Emacs process. An
//! embedded process may run successive editors, so their canonical objects
//! must have separate owners even while the allocator uses one serialized
//! heap. The active owner is selected at the editor boundary, not per VM
//! instruction. Interpreter states retain their owner for GC and parked calls.

use super::obarray::{Buckets, StandardBuckets, text_key};
use super::{ProcessTable, SymbolName, Value};
use crate::lisp::alloc::vectors::hash_tables::DescriptorPool;
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::rc::Rc;

/// Not GNU: the symbols of private obarrays and abbrev tables by their
/// host keys, which carry the obarray's mark (see `make_obarray_symbol_name');
/// a name-keyed caller finds them here, and they are roots, as they were
/// when one table held every interned key.
type PrivateNames = HashSet<NameEntry, ObarrayBuildHasher>;
/// Host text's address with its length in the high bits a user-space
/// address leaves clear (`SymbolName::find_interned'), each in the slot
/// one multiply picks: the symbol last found for a text there.  A slot
/// answers only for its key and is checked against the symbol's own name,
/// so a runtime-built text whose storage is reused merely misses; a
/// table that filled with such texts and was emptied when full (every
/// name built with `format', every resolved alias) lost the literal names
/// with them.
pub(super) struct AddressNames(Box<[(usize, Option<SymbolName>)]>);

const ADDRESS_SLOT_BITS: u32 = 12;

impl Default for AddressNames {
    fn default() -> Self {
        Self(vec![(0, None); 1 << ADDRESS_SLOT_BITS].into_boxed_slice())
    }
}

impl AddressNames {
    #[inline]
    fn slot(key: usize) -> usize {
        key.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> (usize::BITS - ADDRESS_SLOT_BITS)
    }

    #[inline]
    pub(super) fn get(&self, key: usize) -> Option<SymbolName> {
        match self.0[Self::slot(key)] {
            (found, symbol) if found == key => symbol,
            _ => None,
        }
    }

    pub(super) fn insert(&mut self, key: usize, symbol: SymbolName) {
        self.0[Self::slot(key)] = (key, Some(symbol));
    }

    /// Funintern: no slot answers with SYMBOL any more.
    fn forget(&mut self, symbol: SymbolName) {
        for slot in self.0.iter_mut().filter(|slot| slot.1 == Some(symbol)) {
            *slot = (0, None);
        }
    }
}

/// The obarray's hash of a name: fns.c's `hash_string', which oblookup
/// takes of the name's bytes -- the length, then a word at a time (at most
/// eight steps across a long name, and its last word), or a short name's
/// bytes in one word -- each combined by lisp.h's `sxhash_combine'.
/// `finish' spreads the result over the word: GNU reduces it modulo the
/// obarray's size, while this table takes its probe filter from the top
/// bits, which the combination leaves empty for a short name.
#[derive(Default)]
pub(crate) struct ObarrayHasher(u64);

/// lisp.h's `sxhash_combine'.
#[inline(always)]
pub(crate) fn sxhash_combine(x: u64, y: u64) -> u64 {
    (x << 4).wrapping_add(x >> (u64::BITS - 4)).wrapping_add(y)
}

/// The little-endian word at AT of BYTES, which holds it.
#[inline(always)]
pub(crate) fn read_word(bytes: &[u8], at: usize) -> u64 {
    assert!(at + 8 <= bytes.len());
    // SAFETY: the eight bytes at AT are inside BYTES.
    u64::from_le(unsafe { bytes.as_ptr().add(at).cast::<u64>().read_unaligned() })
}

#[inline(always)]
pub(crate) fn read_u32(bytes: &[u8], at: usize) -> u32 {
    assert!(at + 4 <= bytes.len());
    // SAFETY: the four bytes at AT are inside BYTES.
    u32::from_le(unsafe { bytes.as_ptr().add(at).cast::<u32>().read_unaligned() })
}

#[inline(always)]
pub(crate) fn read_u16(bytes: &[u8], at: usize) -> u16 {
    assert!(at + 2 <= bytes.len());
    // SAFETY: the two bytes at AT are inside BYTES.
    u16::from_le(unsafe { bytes.as_ptr().add(at).cast::<u16>().read_unaligned() })
}

/// fns.c's `hash_string' of BYTES.
#[inline]
pub(crate) fn hash_string(bytes: &[u8]) -> u64 {
    const WORD: usize = std::mem::size_of::<u64>();
    let len = bytes.len();
    let mut hash = len as u64;
    if len >= WORD {
        let step = WORD.max(len >> 3);
        let mut at = 0;
        while at + WORD <= len {
            hash = sxhash_combine(hash, read_word(bytes, at));
            at += step;
        }
        sxhash_combine(hash, read_word(bytes, len - WORD))
    } else {
        let mut tail = 0_u64;
        let mut at = 0;
        if len - at >= 4 {
            tail = (tail << 32) + u64::from(read_u32(bytes, at));
            at += 4;
        }
        if len - at >= 2 {
            tail = (tail << 16) + u64::from(read_u16(bytes, at));
            at += 2;
        }
        if at < len {
            tail = (tail << 8) + u64::from(bytes[at]);
        }
        sxhash_combine(hash, tail)
    }
}

impl std::hash::Hasher for ObarrayHasher {
    #[inline]
    fn finish(&self) -> u64 {
        (self.0 ^ (self.0 >> 32)).wrapping_mul(0x9e37_79b9_7f4a_7c15)
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.0 = sxhash_combine(self.0, hash_string(bytes));
    }

    /// `str's terminator, which the name's hash leaves out.
    #[inline]
    fn write_u8(&mut self, _: u8) {}
}

pub(crate) type ObarrayBuildHasher = std::hash::BuildHasherDefault<ObarrayHasher>;

/// Lookup compares immutable host text; Lisp symbol equality compares the
/// allocated object. Keep the lookup key's equality separate so two editor
/// objects with identical names cannot compare equal as Lisp values.
#[derive(Clone, Copy)]
pub(super) struct NameEntry(pub(super) SymbolName);

impl std::borrow::Borrow<str> for NameEntry {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

impl std::ops::Deref for NameEntry {
    type Target = SymbolName;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq for NameEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_str() == other.0.as_str()
    }
}

impl Eq for NameEntry {}

impl std::hash::Hash for NameEntry {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(self.0.as_str(), state);
    }
}

const KEY_NAMES: &[&str] = &[
    "keymap",
    "parse-sexp-lookup-properties",
    "syntax-table",
    "syntax-propertize--done",
    "parse-sexp-ignore-comments",
    "comment-end-can-be-escaped",
    "multibyte-syntax-as-symbol",
    "open-paren-in-column-0-is-defun-start",
    "comment-use-syntax-ppss",
    "words-include-escapes",
    "describe-bindings-check-shadowing-in-ranges",
    "autoload",
    "lambda",
    "macro",
    "invisible",
    "menu-item",
    ":filter",
    "meta-prefix-char",
    "cache-long-scans",
    "char-property-alias-alist",
    "default-text-properties",
    "text-property-default-nonsticky",
    "inhibit-field-text-motion",
    "buffer-access-fontify-functions",
    "buffer-access-fontified-property",
    "buffer-saved-size",
    "search-spaces-regexp",
    "unicode-category-table",
    "char-script-table",
    "word-separating-categories",
    "word-combining-categories",
    "inhibit-changing-match-data",
    "symbol",
    "integer",
    "string",
    "cons",
    "float",
    "operations",
    "lexical-binding",
    "print-escape-nonascii",
    "print-escape-multibyte",
    "print-number-table",
    "print-charset-text-property",
    "print-escape-newlines",
    "print-escape-control-characters",
    "print-unreadable-function",
    "internal-make-interpreted-closure-function",
    "macroexp--dynvars",
    "inhibit-modification-hooks",
    "buffer-file-name",
    "buffer-file-truename",
    "combine-after-change-calls",
    "before-change-functions",
    "after-change-functions",
    "inhibit-read-only",
    "buffer-read-only",
    "read-only",
    "front-sticky",
    "rear-nonsticky",
    "category",
    "text-read-only",
    "deactivate-mark",
    "modification-hooks",
    "insert-behind-hooks",
    "insert-in-front-hooks",
    "char-code-property-alist",
    "char-code-property-table",
    "titlecase",
    "special-uppercase",
    "special-lowercase",
    "special-titlecase",
    "lowercase",
    "uppercase",
    "category-table",
    "case-fold-search",
    "undo-auto--undoable-change",
    "undo-auto-amalgamate",
    "field",
    "overriding-terminal-local-map",
    "last-command",
    "real-last-command",
    "keyboard-translate-table",
    "last-repeatable-command",
    "prefix-arg",
    "last-prefix-arg",
    "defining-kbd-macro",
    "last-kbd-macro",
    "system-key-alist",
    "window-system",
    "default-minibuffer-frame",
    "input-decode-map",
    "local-function-key-map",
    "quit-flag",
    "inhibit-quit",
    "throw-on-input",
    "overriding-plist-environment",
    "load-path",
    "max-lisp-eval-depth",
    "debug-on-next-call",
    "symbols-with-pos-enabled",
    "error-conditions",
    "error",
    "wrong-type-argument",
    "void-variable",
    "void-function",
    "wrong-number-of-arguments",
    "ert-test-failed",
    "no-catch",
    "ert-test-skipped",
    "end-of-file",
    "invalid-read-syntax",
    "messages-buffer-name",
    "message-log-max",
    "default-directory",
    "text-conversion-style",
    "print-circle",
    "print-continuous-numbering",
    "print-gensym",
    "print-integers-as-characters",
    "print-symbols-bare",
    "print-length",
    "print-level",
    "print-quoted",
    "float-output-format",
    "noninteractive",
    "clear-message-function",
    "set-message-function",
    "inhibit-message",
    "find-word-boundary-function-table",
    "apply",
    "set-buffer-multibyte",
    "explicit",
    "features",
    "current-load-list",
    "internal--text-quoting-flag",
    "standard-display-table",
    // buffer.c reset_buffer's always-local slots.
    "buffer-backed-up",
    "buffer-auto-save-file-name",
    "major-mode",
    "local-minor-modes",
    "mode-name",
    "buffer-file-format",
    "buffer-auto-save-file-format",
    "point-before-scroll",
    "buffer-invisibility-spec",
    "buffer-display-count",
    "buffer-display-time",
    "fundamental-mode",
    "quote",
    "function",
    "`",
    ",",
    ",@",
    "inhibit-file-name-operation",
    "inhibit-file-name-handlers",
    "file-name-handler-alist",
    "load",
    "file-exists-p",
    "expand-file-name",
    "require",
    "defun",
    "face-alias",
    "circular-list",
    "obarray",
    "read-symbol-shorthands",
    "identity",
    "case-symbols-as-words",
    "face--new-frame-defaults",
    "edebug-entered",
    "event-symbol-element-mask",
    "event-symbol-elements",
    "modifier-cache",
    "event-kind",
    "current-local-map",
    "tab-width",
    "ctl-arrow",
    "selective-display",
    "buffer-display-table",
    "standard-display-table",
    "permanent-local",
    "frame-windows-min-size",
    "mode-line-format",
    "header-line-format",
    "tab-line-format",
    "undo-auto--last-boundary-cause",
    "text-quoting-style",
    "coding-system-for-write",
    "locale-coding-system",
    "signal-hook-function",
    "debug-on-signal",
    ":success",
];

struct Symbols {
    /// Vobarray: the initial obarray's buckets.
    names: RefCell<StandardBuckets>,
    private: RefCell<PrivateNames>,
    by_address: RefCell<AddressNames>,
    builtins: [Cell<Option<SymbolName>>; 2],
    keys: [Cell<Option<SymbolName>>; KEY_NAMES.len()],
    hash_tests: RefCell<DescriptorPool>,
}

impl Default for Symbols {
    fn default() -> Self {
        Self {
            names: RefCell::new(StandardBuckets::default()),
            private: RefCell::new(PrivateNames::default()),
            by_address: RefCell::new(AddressNames::default()),
            builtins: std::array::from_fn(|_| Cell::new(None)),
            keys: std::array::from_fn(|_| Cell::new(None)),
            hash_tests: RefCell::new(DescriptorPool::default()),
        }
    }
}

// Internal heap operations that do not create an editor retain their existing
// process owner. Editor entry points always install a separate owner below.
static HOST_SYMBOLS: ProcessTable<Rc<Symbols>> = ProcessTable::new();

type KeySlots = [Cell<Option<SymbolName>>; KEY_NAMES.len()];

thread_local! {
    static ACTIVE: RefCell<Option<Rc<Symbols>>> = const { RefCell::new(None) };
    /// ACTIVE's key slots, or null when no editor is selected: a key is read
    /// through it without borrowing ACTIVE.  Every store into ACTIVE stores
    /// this too (`set_active_keys'), and a table being dropped clears it, so
    /// a non-null pointer always names a live table.
    static ACTIVE_KEYS: Cell<*const KeySlots> = const { Cell::new(std::ptr::null()) };
}

/// Record ACTIVE's key slots after a store into ACTIVE.
fn set_active_keys(active: &Option<Rc<Symbols>>) {
    ACTIVE_KEYS.set(
        active
            .as_ref()
            .map_or(std::ptr::null(), |symbols| &symbols.keys),
    );
}

impl Drop for Symbols {
    fn drop(&mut self) {
        let keys: *const KeySlots = &self.keys;
        if ACTIVE_KEYS.get() == keys {
            ACTIVE_KEYS.set(std::ptr::null());
        }
    }
}

fn with_current<R>(body: impl FnOnce(&Symbols) -> R) -> R {
    ACTIVE.with_borrow(|active| match active {
        Some(symbols) => body(symbols),
        None => {
            // Release ProcessTable's initialization borrow before BODY can
            // resolve another symbol through the same host owner.
            let symbols = HOST_SYMBOLS.with_borrow(Rc::clone);
            body(&symbols)
        }
    })
}

/// The interpreter retains its actual obarray owner, including when another
/// owned editor entry is nested or the interpreter is parked for collection.
#[derive(Clone)]
pub(crate) struct SymbolOwner(Rc<Symbols>);

impl Default for SymbolOwner {
    fn default() -> Self {
        ACTIVE.with_borrow(|active| {
            Self(match active {
                Some(symbols) => Rc::clone(symbols),
                None => HOST_SYMBOLS.with_borrow(Rc::clone),
            })
        })
    }
}

impl SymbolOwner {
    pub(crate) fn fresh() -> Self {
        Self(Rc::default())
    }

    #[cfg(test)]
    pub(crate) fn enter(&self) -> EditorSymbolScope {
        EditorSymbolScope(ACTIVE.with_borrow_mut(|active| {
            let previous = active.replace(Rc::clone(&self.0));
            set_active_keys(active);
            previous
        }))
    }

    /// Select at editor construction or a complete host fixture boundary.
    /// Public entries retain an enclosing scope that restores their caller.
    pub(crate) fn select(&self) {
        ACTIVE.with_borrow_mut(|active| {
            *active = Some(Rc::clone(&self.0));
            set_active_keys(active);
        });
    }

    /// The initial obarray's count.
    pub(crate) fn len(&self) -> usize {
        self.0.names.borrow().count() as usize
    }

    pub(crate) fn contains(&self, name: &str) -> bool {
        self.find(name).is_some()
    }

    /// The symbol host text NAME names: oblookup in the initial obarray,
    /// or a private obarray's key among the private names.
    pub(crate) fn find(&self, name: &str) -> Option<SymbolName> {
        if super::obarray::plain_ascii(name.as_bytes()) || super::is_visible_symbol_name(name) {
            let (bytes, chars) = text_key(name);
            self.0.names.borrow().lookup(bytes, chars).ok()
        } else {
            self.0.private.borrow().get(name).map(|entry| entry.0)
        }
    }

    /// oblookup in the initial obarray; None for a marked key.
    pub(crate) fn find_initial(&self, name: &str) -> Option<SymbolName> {
        if !super::obarray::plain_ascii(name.as_bytes()) && !super::is_visible_symbol_name(name) {
            return None;
        }
        let (bytes, chars) = text_key(name);
        self.0.names.borrow().lookup(bytes, chars).ok()
    }

    /// lread.c:Funintern of the symbol the Lisp string NAME names, when it
    /// is ONLY (or any symbol, for None): oblookup by the name's bytes and
    /// characters, then the bucket's unlinking.  The object keeps its
    /// fields while it is otherwise reachable; a later miss allocates anew.
    pub(crate) fn unintern_name(
        &self,
        name: Value,
        only: Option<SymbolName>,
    ) -> Option<SymbolName> {
        let symbol = {
            let mut names = self.0.names.borrow_mut();
            let Ok(symbol) = names.lookup_name(name)? else {
                return None;
            };
            if only.is_some_and(|only| only != symbol) {
                return None;
            }
            let index = super::obarray::name_bucket(symbol, names.size_bits());
            names.remove(symbol, index);
            symbol
        };
        self.forget(symbol);
        Some(symbol)
    }

    /// The uninterned state of a symbol Funintern unlinked.
    fn forget(&self, symbol: SymbolName) {
        symbol.cell().mark_uninterned();
        self.0.by_address.borrow_mut().forget(symbol);
    }

    /// The initial obarray's symbols in DOOBARRAY's order, then the
    /// private names.
    pub(crate) fn names(&self) -> Vec<SymbolName> {
        let mut names = self.0.names.borrow().symbols();
        names.extend(self.0.private.borrow().iter().map(|entry| entry.0));
        names
    }

    /// Finternal__obarray_buckets of the initial obarray.
    pub(crate) fn bucket_lists(&self) -> Vec<Vec<SymbolName>> {
        self.0.names.borrow().bucket_lists()
    }

    pub(crate) fn mark_roots(&self, mark: &mut dyn FnMut(&Value)) {
        mark_names(&self.0, mark);
    }
}

/// Select an editor's objects for a complete owned host entry. Holding an Rc
/// rather than a pointer into Interpreter avoids aliasing its mutable state.
/// Drop restores a nested caller's owner and releases this scope's ownership.
pub(crate) struct EditorSymbolScope(Option<Rc<Symbols>>);

impl EditorSymbolScope {
    pub(crate) fn enter() -> Self {
        Self(ACTIVE.with_borrow_mut(|active| {
            let previous = active.replace(Rc::default());
            set_active_keys(active);
            previous
        }))
    }
}

impl Drop for EditorSymbolScope {
    fn drop(&mut self) {
        ACTIVE.with_borrow_mut(|active| {
            *active = self.0.take();
            set_active_keys(active);
        });
    }
}

pub(super) struct InternedNames;

pub(super) fn note_builtin_boolean(symbol: SymbolName) {
    let truth = match symbol.as_str() {
        "nil" => false,
        "t" => true,
        _ => return,
    };
    with_current(|symbols| {
        let slot = &symbols.builtins[usize::from(truth)];
        if slot.get().is_none() {
            symbol.cell().mark_builtin_boolean(truth);
            // lread.c:syms_of_lread initializes the actual Qnil/Qt cells.
            // A fresh same-named symbol after unintern does not enter here.
            symbol.cell().set_constant(true);
            symbol
                .cell()
                .flags
                .set(crate::lisp::eval::symbol_cell_flags::SPECIAL);
            symbol
                .cell()
                .set_value(if truth { Value::T } else { Value::Nil });
            slot.set(Some(symbol));
        }
    });
}

pub(crate) fn builtin_boolean(truth: bool) -> SymbolName {
    with_current(|symbols| {
        symbols.builtins[usize::from(truth)]
            .get()
            .unwrap_or_else(|| SymbolName::intern_str(if truth { "t" } else { "nil" }))
    })
}

impl InternedNames {
    pub(super) fn with_borrow<R>(&self, body: impl FnOnce(&StandardBuckets) -> R) -> R {
        with_current(|symbols| body(&symbols.names.borrow()))
    }

    pub(super) fn with_borrow_mut<R>(&self, body: impl FnOnce(&mut StandardBuckets) -> R) -> R {
        with_current(|symbols| body(&mut symbols.names.borrow_mut()))
    }
}

pub(super) fn with_private_names<R>(body: impl FnOnce(&PrivateNames) -> R) -> R {
    with_current(|symbols| body(&symbols.private.borrow()))
}

pub(super) fn with_private_names_mut<R>(body: impl FnOnce(&mut PrivateNames) -> R) -> R {
    with_current(|symbols| body(&mut symbols.private.borrow_mut()))
}

pub(super) fn with_address_names<R>(body: impl FnOnce(&AddressNames) -> R) -> R {
    with_current(|symbols| body(&symbols.by_address.borrow()))
}

pub(super) fn with_address_names_mut<R>(body: impl FnOnce(&mut AddressNames) -> R) -> R {
    with_current(|symbols| body(&mut symbols.by_address.borrow_mut()))
}

pub(crate) fn with_hash_tests<R>(body: impl FnOnce(&mut DescriptorPool) -> R) -> R {
    with_current(|symbols| body(&mut symbols.hash_tests.borrow_mut()))
}

/// A Qsymbol-style reference in the current editor, with a fixed slot. The
/// slot contains the actual canonical symbol, never a second Lisp payload.
#[derive(Clone, Copy)]
pub(crate) struct SymbolKey(usize);

impl SymbolKey {
    pub(crate) const fn new(name: &str) -> Self {
        let wanted = name.as_bytes();
        let mut index = 0;
        while index < KEY_NAMES.len() {
            let candidate = KEY_NAMES[index].as_bytes();
            if candidate.len() == wanted.len() {
                let mut byte = 0;
                while byte < wanted.len() && candidate[byte] == wanted[byte] {
                    byte += 1;
                }
                if byte == wanted.len() {
                    return Self(index);
                }
            }
            index += 1;
        }
        panic!("undeclared editor symbol key");
    }

    fn get(self, symbols: &Symbols) -> SymbolName {
        symbols.keys[self.0].get().unwrap_or_else(|| {
            let symbol = SymbolName::intern_str(KEY_NAMES[self.0]);
            symbols.keys[self.0].set(Some(symbol));
            symbol
        })
    }

    #[inline]
    pub(crate) fn with<R>(self, body: impl FnOnce(&SymbolName) -> R) -> R {
        let keys = ACTIVE_KEYS.get();
        // SAFETY: a non-null pointer names the selected table's key slots,
        // which live as long as that table (its Drop clears the pointer).
        if let Some(symbol) = unsafe { keys.as_ref() }.and_then(|keys| keys[self.0].get()) {
            return body(&symbol);
        }
        body(&self.intern())
    }

    /// The key's symbol where its slot is empty or no table's slots are
    /// held: found or interned in the current table.
    #[cold]
    #[inline(never)]
    fn intern(self) -> SymbolName {
        with_current(|symbols| self.get(symbols))
    }

    /// The name the key's symbol has.
    pub(crate) const fn name(self) -> &'static str {
        KEY_NAMES[self.0]
    }
}

/// Intern every cached key's symbol now, where `SymbolKey::with' would
/// on its first use.
pub(crate) fn intern_all_keys() {
    for index in 0..KEY_NAMES.len() {
        SymbolKey(index).with(|_| ());
    }
}

/// Qsymbol references belong to each editor, like GNU's process references.
/// The existing terminal forwarding adapter consumes these until keyboard
/// bindings use an allocated Lisp_Fwd redirect. No symbol-id lookup is needed.
pub(crate) fn is_keyboard_variable(symbol: &SymbolName) -> bool {
    const KEYS: [SymbolKey; KEYBOARD_VARIABLES.len()] = {
        let mut keys = [SymbolKey(0); KEYBOARD_VARIABLES.len()];
        let mut index = 0;
        while index < keys.len() {
            keys[index] = SymbolKey::new(KEYBOARD_VARIABLES[index]);
            index += 1;
        }
        keys
    };
    with_current(|symbols| KEYS.iter().any(|key| key.get(symbols) == *symbol))
}

/// The variables keyboard.h keeps in each terminal's KBOARD.
pub(crate) const KEYBOARD_VARIABLES: [&str; 14] = [
    "overriding-terminal-local-map",
    "last-command",
    "real-last-command",
    "keyboard-translate-table",
    "last-repeatable-command",
    "prefix-arg",
    "last-prefix-arg",
    "defining-kbd-macro",
    "last-kbd-macro",
    "system-key-alist",
    "window-system",
    "default-minibuffer-frame",
    "input-decode-map",
    "local-function-key-map",
];

fn mark_names(symbols: &Symbols, mark: &mut dyn FnMut(&Value)) {
    fn mark_symbol(name: SymbolName, mark: &mut dyn FnMut(&Value)) {
        let value = Value::Symbol(name);
        if matches!(value.kind(), super::Kind::Nil | super::Kind::T) {
            // Fixed nil/t ABI words still use their allocated name cells.
            name.mark(super::current_mark_epoch());
            mark(&name.lisp_name());
            if let Some(root) = name.cell().value_or_alias_root() {
                mark(&root);
            }
            mark(&name.cell().function.get());
            mark(&name.cell().plist.get());
            if let Some(next) = name.cell().next.get() {
                mark(&Value::Symbol(next));
            }
        } else {
            mark(&value);
        }
    }
    symbols
        .names
        .borrow()
        .for_each(|name| mark_symbol(name, mark));
    for name in symbols.private.borrow().iter() {
        mark_symbol(name.0, mark);
    }
    // GNU's lispsym/static Qsymbol references remain roots after unintern.
    for name in symbols
        .builtins
        .iter()
        .chain(symbols.keys.iter())
        .filter_map(Cell::get)
    {
        mark_symbol(name, mark);
    }
    symbols.hash_tests.borrow().mark_roots(mark);
}

pub(super) fn mark_scope_roots(mark: &mut dyn FnMut(&Value)) {
    let host = HOST_SYMBOLS.with_borrow(Rc::clone);
    mark_names(&host, mark);
    ACTIVE.with_borrow(|active| {
        if let Some(symbols) = active {
            mark_names(symbols, mark);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::{EditorSymbolScope, SymbolName};
    use std::collections::{BTreeSet, HashMap};

    #[test]
    fn keyboard_variable_symbols_follow_each_editors_owner() {
        // GNU's keyboard variable references belong to its editor. Embedding
        // successive editors must not reuse an earlier editor's symbol ids.
        let _outer = EditorSymbolScope::enter();
        let outer = SymbolName::intern_str("last-command");
        assert!(crate::lisp::eval::Interpreter::is_keyboard_variable(&outer));
        let private = SymbolName::make_uninterned(
            super::Value::string("last-command"),
            "last-command",
            91873,
        );
        assert!(!crate::lisp::eval::Interpreter::is_keyboard_variable(
            &private
        ));
        {
            let _inner = EditorSymbolScope::enter();
            let inner = SymbolName::intern_str("last-command");
            assert_ne!(outer, inner);
            assert!(crate::lisp::eval::Interpreter::is_keyboard_variable(&inner));
            assert!(!crate::lisp::eval::Interpreter::is_keyboard_variable(
                &outer
            ));
        }
        assert!(crate::lisp::eval::Interpreter::is_keyboard_variable(&outer));
    }

    #[test]
    fn nested_editors_keep_same_named_symbols_distinct_and_restore_the_caller() {
        let _outer = EditorSymbolScope::enter();
        for text in ["owner-field", "owner-hook", "owner-λ"] {
            let outer = SymbolName::intern_str(text);
            let mut values = HashMap::from([(outer, 17)]);
            let mut ordered = BTreeSet::from([outer]);
            {
                let _inner = EditorSymbolScope::enter();
                let inner = SymbolName::intern_str(text);
                assert_ne!(outer, inner);
                assert_eq!(inner, SymbolName::intern_str(text));
                assert!(values.insert(inner, 43).is_none());
                assert_eq!(values.get(&outer), Some(&17));
                assert_eq!(values.get(&inner), Some(&43));
                assert!(ordered.insert(inner));
                assert_eq!(ordered.len(), 2);
            }
            assert_eq!(SymbolName::intern_str(text), outer);
            assert_eq!(values.get(&SymbolName::intern_str(text)), Some(&17));
        }
    }

    #[test]
    fn hash_string_is_gnu_hash_string() {
        // fns.c's hash_string of each name, as GNU 30.2 computes it.
        for (name, hash) in [
            ("", 0x0000_0000_0000_0000_u64),
            ("a", 0x0000_0000_0000_0071),
            ("ab", 0x0000_0000_0000_6281),
            ("abc", 0x0000_0000_0062_6193),
            ("abcd", 0x0000_0000_6463_62a1),
            ("abcde", 0x0000_0064_6362_61b5),
            ("abcdefg", 0x0064_6362_6166_65d7),
            ("abcdefgh", 0xeedd_ccbb_aa99_9077),
            ("abcdefghi", 0xefde_cdbc_ab9a_9278),
            ("car", 0x0000_0000_0061_63a2),
            ("emacs-lisp-mode-hook", 0xadf3_29c2_61ab_110b),
            ("\u{20ac}-euro", 0x6699_cbb8_0874_b908),
            (
                "this is a long symbol name with more than sixty-four bytes in it for the stepping",
                0xe803_9a2e_8d6b_20da,
            ),
        ] {
            assert_eq!(super::hash_string(name.as_bytes()), hash, "{name:?}");
        }
    }
}
