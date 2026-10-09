#![allow(dead_code)]

pub use crate::lisp::alloc::vectors::{NativeFunctionRef, NativeUnitRef};

pub use crate::lisp::alloc::vectors::{CharTableRef, HashTableRef, SubCharTableRef};
pub use crate::lisp::native_comp::abi::BuiltinRef;
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use std::fmt;
use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, HashSet},
    hash::{BuildHasherDefault, Hasher},
    iter::FromIterator,
    ops::Deref,
    ptr::NonNull,
};

const UNINTERNED_SYMBOL_MARKER: &str = "\u{1F}";
/// The marker as a character: `str::contains' with a one-character pattern
/// scans with memchr, where the string pattern walked the bytes one by one
/// on every name-to-id resolution (a tenth of a tight interpreted loop).
const UNINTERNED_SYMBOL_MARKER_CHAR: char = '\u{1F}';
const OBARRAY_SYMBOL_MARKER: &str = "\u{1E}";
const OBARRAY_SYMBOL_MARKER_CHAR: char = '\u{1E}';

/// Hashes an object address or identity word for the bridge's index maps.
///
/// The key is already unique, but hashbrown takes its bucket from the low
/// bits and its control tag from the top seven: aligned heap addresses share
/// their low bits and every user-space address shares its high bits, so the
/// raw word made every entry probe the same few buckets under one tag.  The
/// finalizer mixes the word (splitmix64's) before hashbrown sees it.  This
/// is bridge indexing only; Lisp hash semantics live in the fns.c-compatible
/// primitives.
#[derive(Default)]
pub(crate) struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        // One multiply spreads a dense id or an aligned address into the
        // high bits hashbrown's control bytes read; folding them back down
        // gives the bucket index bits of a pointer's zero low bits as well.
        let mixed = self.0.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        mixed ^ (mixed >> 32)
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut value = 0_u64;
        for (index, byte) in bytes.iter().take(8).enumerate() {
            value |= u64::from(*byte) << (index * 8);
        }
        self.0 = value;
    }

    fn write_usize(&mut self, value: usize) {
        self.0 = value as u64;
    }
}

pub(crate) type IdentityBuildHasher = BuildHasherDefault<IdentityHasher>;

/// Temporary derived-cache validation for keymap and syntax views. Snapshot
/// actual words: native code and the interpreter can mutate any cons directly,
/// without a mutation epoch, watcher registration or store barrier. These weak
/// snapshots never retain an otherwise unreachable Lisp graph. Their scan cost
/// belongs to the remaining derived views, not to ordinary cons operations.
#[derive(Debug, Clone, Default)]
pub(crate) struct ConsMutationSnapshot {
    cells: Vec<(WeakConsRef, [usize; 2])>,
}

impl ConsMutationSnapshot {
    pub(crate) fn cell(cell: &SharedCons) -> Self {
        Self::cells(std::iter::once(cell))
    }

    pub(crate) fn cells<'a>(cells: impl IntoIterator<Item = &'a SharedCons>) -> Self {
        let mut snapshot = Self {
            cells: cells
                .into_iter()
                .map(|cell| {
                    (
                        cell.downgrade(),
                        [cell.car.get().word(), cell.cdr.get().word()],
                    )
                })
                .collect(),
        };
        snapshot
            .cells
            .sort_unstable_by_key(|(cell, _)| cell.identity());
        snapshot.cells.dedup_by_key(|(cell, _)| cell.identity());
        snapshot
    }

    pub(crate) fn list_spine(value: &Value) -> Self {
        let mut cells = Vec::new();
        let mut seen = HashSet::new();
        let mut value = *value;
        while let Kind::Cons(cell) = value.kind() {
            if !seen.insert(ConsCell::identity(&cell)) {
                break;
            }
            cells.push(cell);
            value = cell.cdr.get();
        }
        Self::cells(&cells)
    }

    pub(crate) fn tree(value: &Value) -> Self {
        let mut snapshot = Self::default();
        snapshot.include_tree(value);
        snapshot
    }

    pub(crate) fn include_cell(&mut self, cell: &SharedCons) {
        if let Err(index) = self
            .cells
            .binary_search_by_key(&ConsCell::identity(cell), |(cell, _)| cell.identity())
        {
            self.cells.insert(
                index,
                (
                    cell.downgrade(),
                    [cell.car.get().word(), cell.cdr.get().word()],
                ),
            );
        }
    }

    pub(crate) fn include_tree(&mut self, value: &Value) {
        let existing_len = self.cells.len();
        let mut seen = HashSet::new();
        let mut pending = vec![*value];
        while let Some(value) = pending.pop() {
            let Kind::Cons(cell) = value.kind() else {
                continue;
            };
            let id = ConsCell::identity(&cell);
            if !seen.insert(id) {
                continue;
            }
            if self.cells[..existing_len]
                .binary_search_by_key(&id, |(cell, _)| cell.identity())
                .is_err()
            {
                self.cells.push((
                    cell.downgrade(),
                    [cell.car.get().word(), cell.cdr.get().word()],
                ));
            }
            pending.push(cell.car.get());
            pending.push(cell.cdr.get());
        }
        self.cells.sort_unstable_by_key(|(cell, _)| cell.identity());
        self.cells.dedup_by_key(|(cell, _)| cell.identity());
    }

    pub(crate) fn is_current(&self) -> bool {
        self.cells.iter().all(|(cell, words)| {
            cell.upgrade()
                .is_some_and(|cell| [cell.car.get().word(), cell.cdr.get().word()] == *words)
        })
    }
}

/// The current collection's number; see `MarkBit'.  The process's, as
/// the mark bits and `gc_in_progress' are alloc.c's globals: the objects
/// carry it, whichever thread marked them.
static GC_MARK_EPOCH: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// The current collection's epoch (the last one begun).
#[inline]
pub(crate) fn current_mark_epoch() -> u32 {
    GC_MARK_EPOCH.load(std::sync::atomic::Ordering::Relaxed)
}

/// Between a mark phase and its sweep (a checked build's guard: a second
/// mark phase in between would give the cells born after it a newer
/// epoch, and the sweep would free them).
static GC_SWEEP_PENDING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub(crate) fn set_sweep_pending(pending: bool) {
    GC_SWEEP_PENDING.store(pending, std::sync::atomic::Ordering::Relaxed);
}

/// Start a collection: the epoch every object marked in it will carry.
pub(crate) fn begin_mark_epoch() -> u32 {
    debug_assert!(
        !GC_SWEEP_PENDING.load(std::sync::atomic::Ordering::Relaxed),
        "a mark phase began while the previous one's sweep is pending"
    );
    let epoch = current_mark_epoch().wrapping_add(1).max(1);
    GC_MARK_EPOCH.store(epoch, std::sync::atomic::Ordering::Relaxed);
    epoch
}

/// alloc.c's mark bit, on the object: a cons, string, vector or symbol is
/// marked when it carries the current collection's epoch, so the mark
/// phase writes a word into the object instead of inserting its address
/// into a table, and no pass clears the bits afterwards.
#[derive(Debug, Default)]
pub(crate) struct MarkBit(Cell<u32>);

impl MarkBit {
    /// Mark for EPOCH; true when the object was not yet marked in it.
    pub(crate) fn mark(&self, epoch: u32) -> bool {
        if self.0.get() == epoch {
            return false;
        }
        self.0.set(epoch);
        true
    }

    pub(crate) fn is_marked(&self, epoch: u32) -> bool {
        self.0.get() == epoch
    }

    /// The word itself (the allocator's free mark rides in it).
    pub(crate) fn raw(&self) -> u32 {
        self.0.get()
    }

    pub(crate) fn set_raw(&self, word: u32) {
        self.0.set(word);
    }
}

/// A handle to the single canonical Lisp string payload. Rust text is
/// converted only at construction; variable bindings copy this handle.
pub type SharedText = StringObjectRef;

impl SharedText {
    pub fn new(text: String) -> Self {
        // Host callers still use the existing raw-byte text convention.
        // This conversion happens once, before publishing the Lisp object.
        let multibyte = !text.is_ascii()
            && text.chars().any(|ch| {
                !crate::lisp::primitives::is_raw_byte_regex_char(ch) && u32::from(ch) > 127
            });
        StringObjectRef::from_text(text, Vec::new(), multibyte, Vec::new())
    }

    pub fn into_string(self) -> String {
        self.borrow().text()
    }
}

impl PartialEq for SharedText {
    fn eq(&self, other: &Self) -> bool {
        if self.ptr_eq(other) {
            return true;
        }
        let left = self.borrow();
        let right = other.borrow();
        left.contents_equal(&right)
    }
}

impl Eq for SharedText {}

impl std::hash::Hash for SharedText {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let text = self.borrow();
        std::hash::Hash::hash(&text.len(), state);
        std::hash::Hash::hash(text.bytes(), state);
    }
}

impl From<String> for SharedText {
    fn from(text: String) -> Self {
        Self::new(text)
    }
}

impl From<&str> for SharedText {
    fn from(text: &str) -> Self {
        Self::new(text.to_owned())
    }
}

impl From<&String> for SharedText {
    fn from(text: &String) -> Self {
        Self::from(text.as_str())
    }
}

impl FromIterator<char> for SharedText {
    fn from_iter<T: IntoIterator<Item = char>>(iter: T) -> Self {
        String::from_iter(iter).into()
    }
}

impl<'a> FromIterator<&'a char> for SharedText {
    fn from_iter<T: IntoIterator<Item = &'a char>>(iter: T) -> Self {
        iter.into_iter().copied().collect::<String>().into()
    }
}

impl From<SharedText> for String {
    fn from(text: SharedText) -> Self {
        text.into_string()
    }
}

impl From<&SharedText> for String {
    fn from(text: &SharedText) -> Self {
        text.borrow().text()
    }
}

/// A symbol name shared by every live occurrence of the same interned name.
///
/// The ordinary-symbol table deliberately owns its entries for the owning
/// runtime thread's lifetime, matching the standard obarray's ownership in
/// Emaxx's single-threaded Lisp runtime.  Encoded
/// `make-symbol` names bypass the table so transient uninterned symbols are
/// still released when their last Lisp value dies.
/// A symbol: alloc.c's `struct Lisp_Symbol' in a symbol block, named by
/// its cell's address (`SymbolRef'), copied without a count.  The
/// interned ones are the obarray's (a root); an uninterned one lives
/// while something names it, and the sweep releases its registries.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct SymbolName(crate::lisp::alloc::SymbolRef);

impl SymbolName {
    /// The handle for a cell the allocator handed out (the conservative
    /// scan's).
    pub(crate) fn from_ref(cell: crate::lisp::alloc::SymbolRef) -> Self {
        Self(cell)
    }
}

/// A table of the process's, read and written without a lock by the one
/// Lisp thread that runs (alloc.c's `Vobarray' is a plain global under
/// the global lock; a template interpreter built on one thread is used
/// from another, one at a time).
pub(crate) struct ProcessTable<T>(std::cell::UnsafeCell<Option<T>>);

// SAFETY: one Lisp OS thread at a time, by construction (see above).
unsafe impl<T> Sync for ProcessTable<T> {}

impl<T: Default> ProcessTable<T> {
    pub(crate) const fn new() -> Self {
        Self(std::cell::UnsafeCell::new(None))
    }

    pub(crate) fn with_borrow<R>(&self, body: impl FnOnce(&T) -> R) -> R {
        // SAFETY: the one running Lisp thread's access.
        let table = unsafe { &mut *self.0.get() };
        body(table.get_or_insert_with(T::default))
    }

    pub(crate) fn with_borrow_mut<R>(&self, body: impl FnOnce(&mut T) -> R) -> R {
        // SAFETY: as above.
        let table = unsafe { &mut *self.0.get() };
        body(table.get_or_insert_with(T::default))
    }
}

/// The obarray: every interned symbol, keyed by its text; FNV, as the
/// interpreter's other name-keyed tables, since every name-to-id
/// resolution hashes here (SipHash was a tenth of a tight interpreted
/// loop).  The process's, as `Vobarray' is (it was the thread's, and a
/// collection on another thread swept the name strings of the symbols
/// only this table held).
static INTERNED_SYMBOL_NAMES: ProcessTable<
    HashSet<SymbolName, crate::lisp::primitives::FnvBuildHasher>,
> = ProcessTable::new();

/// Live uninterned states by their private internal text. Name-keyed
/// callers must resolve a live key to the existing symbol allocation.
/// This weak table belongs to the process-wide heap, just like the interned
/// table above. A later serialized host entry can run on another OS thread;
/// its sweep must retire entries made by every previous thread.
/// Access uses the existing runtime ownership boundary, without a second
/// lock on lookup/allocation. Entries are not GC roots and are removed by
/// sweep_symbol_cells before the corresponding allocation is released.
static UNINTERNED_SYMBOL_BOOK: ProcessTable<
    HashSet<SymbolName, crate::lisp::primitives::FnvBuildHasher>,
> = ProcessTable::new();

/// Each allocated symbol owns its id. Both lookup tables above are already
/// process-wide, so moving an interpreter between serialized OS-thread entries
/// needs no second name-to-id registry or lock. The counters remain temporary
/// until symbol fields move out of the per-interpreter indexed adapter.
static NEXT_SYMBOL_ID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
static NEXT_UNINTERNED_SYMBOL_ID: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(0);

/// Uninterned symbols draw ids from their own counter, marked by this bit,
/// so the dense per-interpreter cell table is sized by the number of
/// interned names rather than by every `make-symbol' ever evaluated.
pub(crate) const UNINTERNED_SYMBOL_ID_BIT: u32 = 1 << 31;

fn next_symbol_id(uninterned: bool) -> u32 {
    if uninterned {
        let serial = NEXT_UNINTERNED_SYMBOL_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        assert!(
            serial < UNINTERNED_SYMBOL_ID_BIT,
            "uninterned symbol id space exhausted"
        );
        serial | UNINTERNED_SYMBOL_ID_BIT
    } else {
        let id = NEXT_SYMBOL_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        assert!(id < UNINTERNED_SYMBOL_ID_BIT, "symbol id space exhausted");
        id
    }
}

/// alloc.c's `sweep_symbols': remove the weak lookup entry before releasing
/// the allocation whose own text is its key. The book holds no extra string
/// and never roots a symbol. It is a migration adapter for name-keyed callers.
pub(crate) fn sweep_symbol_cells(epoch: u32) {
    crate::lisp::alloc::sweep_symbols(epoch, |cell| {
        if cell.id & UNINTERNED_SYMBOL_ID_BIT == 0 {
            return;
        }
        UNINTERNED_SYMBOL_BOOK.with_borrow_mut(|book| {
            // Internal constructors can reuse an encoded key. Retire only
            // this allocation's entry, never a later live symbol's entry.
            if book
                .get(cell.internal.as_str())
                .is_some_and(|symbol| symbol.identity_ptr() == std::ptr::from_ref(cell) as usize)
            {
                book.remove(cell.internal.as_str());
            }
        });
    });
}

impl SymbolName {
    pub fn intern(text: String) -> Self {
        Self::intern_with_lisp_name(text, None)
    }

    /// The runtime lookup key is not SYMBOL_NAME. GNU init_symbol retains
    /// the supplied Lisp string; internal C-string callers create that name
    /// only when the symbol is first allocated.
    pub(crate) fn intern_with_lisp_name(text: String, lisp_name: Option<Value>) -> Self {
        if text.contains(UNINTERNED_SYMBOL_MARKER_CHAR) {
            if let Some(existing) = Self::live_uninterned(text.as_str()) {
                return existing;
            }
            let visible = visible_symbol_name(&text).to_owned();
            return Self::new_uninterned(
                lisp_name.unwrap_or_else(|| Value::String(SharedText::from(visible))),
                text,
            );
        }
        INTERNED_SYMBOL_NAMES.with_borrow_mut(|names| {
            if let Some(name) = names.get(text.as_str()) {
                return *name;
            }
            crate::lisp::native_comp::note_lisp_allocation(48);
            let private = text.contains(OBARRAY_SYMBOL_MARKER);
            let lisp_name = lisp_name.unwrap_or_else(|| {
                Value::String(SharedText::from(if private {
                    visible_symbol_name(&text)
                } else {
                    text.as_str()
                }))
            });
            let id = next_symbol_id(false);
            let name = Self(crate::lisp::alloc::allocate_symbol(
                crate::lisp::alloc::SymbolCell {
                    internal: text,
                    lisp_name,
                    mark: MarkBit::default(),
                    id,
                },
            ));
            names.insert(name);
            name
        })
    }

    /// alloc.c's mark bit on the symbol object.
    pub(crate) fn mark_bit(&self) -> &MarkBit {
        self.0.mark_bit()
    }

    /// Room for ADDITIONAL interned names (the image loader knows how many
    /// symbols it is about to intern; one growth instead of several).
    pub(crate) fn reserve_interned(additional: usize) {
        INTERNED_SYMBOL_NAMES.with_borrow_mut(|names| names.reserve(additional));
    }

    /// The interned state for TEXT, without allocating when it exists.
    pub(crate) fn intern_str(text: &str) -> Self {
        if !text.contains(UNINTERNED_SYMBOL_MARKER_CHAR)
            && let Some(name) = INTERNED_SYMBOL_NAMES.with_borrow(|names| names.get(text).cloned())
        {
            return name;
        }
        Self::intern(text.to_owned())
    }

    /// The id of the state TEXT currently names, if any state does: an
    /// interned name that was never mentioned, or an uninterned symbol that
    /// died, has no cell anywhere. Both tables are process-wide, including
    /// symbols allocated by another serialized OS-thread entry.
    pub(crate) fn id_of(text: &str) -> Option<u32> {
        if text.contains(UNINTERNED_SYMBOL_MARKER_CHAR) {
            return Self::live_uninterned(text).map(|name| name.id());
        }
        INTERNED_SYMBOL_NAMES.with_borrow(|names| names.get(text).map(|name| name.id()))
    }

    /// The interned symbol named TEXT, through a cache keyed by the text's
    /// address and length and verified by the text itself: a primitive
    /// reading a variable by its literal name presents the same address on
    /// every call, so the name is hashed once, not on each read.  A text
    /// that is not an interned symbol's is not cached (None: the caller
    /// keeps its by-name path, which may still know the name).
    pub(crate) fn interned_cached(text: &str) -> Option<Self> {
        thread_local! {
            static BY_ADDRESS: RefCell<
                HashMap<(usize, usize), SymbolName, crate::lisp::primitives::FnvBuildHasher>,
            > = RefCell::new(HashMap::default());
        }
        let key = (text.as_ptr() as usize, text.len());
        if let Some(symbol) = BY_ADDRESS.with_borrow(|cache| cache.get(&key).cloned())
            && symbol.as_str() == text
        {
            return Some(symbol);
        }
        if text.contains(UNINTERNED_SYMBOL_MARKER_CHAR) {
            return Self::live_uninterned(text);
        }
        let symbol = INTERNED_SYMBOL_NAMES.with_borrow(|names| names.get(text).cloned())?;
        BY_ADDRESS.with_borrow_mut(|cache| {
            // A bounded cache: a runtime-built name whose storage is freed
            // and reused would otherwise pin a stale entry (the text check
            // above keeps such an entry from ever answering wrongly).
            if cache.len() >= 8192 {
                cache.clear();
            }
            cache.insert(key, symbol);
        });
        Some(symbol)
    }

    fn live_uninterned(text: &str) -> Option<Self> {
        UNINTERNED_SYMBOL_BOOK.with_borrow(|book| book.get(text).copied())
    }

    pub(crate) fn make_uninterned(name: Value, visible: &str, id: u64) -> Self {
        Self::new_uninterned(name, make_uninterned_symbol_name(visible, id))
    }

    fn new_uninterned(lisp_name: Value, internal: String) -> Self {
        crate::lisp::native_comp::note_lisp_allocation(48);
        let id = next_symbol_id(true);
        let name = Self(crate::lisp::alloc::allocate_symbol(
            crate::lisp::alloc::SymbolCell {
                internal,
                lisp_name,
                mark: MarkBit::default(),
                id,
            },
        ));
        UNINTERNED_SYMBOL_BOOK.with_borrow_mut(|book| {
            book.replace(name);
        });
        name
    }

    /// The name's text; its lifetime is the symbol's, which the collector
    /// keeps while the symbol is reachable (an interned one, always).
    pub fn as_str(&self) -> &str {
        self.0.internal.as_str()
    }

    pub(crate) fn identity_ptr(&self) -> usize {
        self.0.identity()
    }

    /// The process-wide symbol id (see `SymbolCell::id'); an
    /// uninterned symbol's id carries `UNINTERNED_SYMBOL_ID_BIT'.
    pub(crate) fn id(&self) -> u32 {
        self.0.id
    }

    pub fn into_string(self) -> String {
        self.as_str().to_owned()
    }

    pub(crate) fn lisp_name(&self) -> Value {
        self.0.lisp_name
    }

    /// The symbol's cell (for the census and the dump).
    pub(crate) fn cell(&self) -> &crate::lisp::alloc::SymbolCell {
        &self.0
    }

    /// The name object in place, for a tracer.
    pub(crate) fn lisp_name_ref(&self) -> &Value {
        &self.0.lisp_name
    }
}

/// The obarray as a root (alloc.c staticpro's `Vobarray'): every
/// interned symbol, with its name strings, whether or not any object
/// names it.
pub(crate) fn mark_interned_symbol_roots(mark: &mut dyn FnMut(&Value)) {
    INTERNED_SYMBOL_NAMES.with_borrow(|names| {
        for name in names {
            let value = Value::Symbol(*name);
            if matches!(value.kind(), Kind::Nil | Kind::T) {
                // During symbol migration, the builtin word is constant
                // but its SymbolName still has an allocated name cell.
                // Keep that obarray-owned cell and both name strings.
                // This adapter goes when the builtin symbol allocation
                // also owns the value, function and property cells.
                name.mark_bit().mark(current_mark_epoch());
                mark(name.lisp_name_ref());
            } else {
                mark(&value);
            }
        }
    });
}

pub(crate) fn census_live_uninterned_symbols() -> usize {
    UNINTERNED_SYMBOL_BOOK.with_borrow(|book| book.len())
}

impl PartialEq for SymbolName {
    fn eq(&self, other: &Self) -> bool {
        self.0.ptr_eq(&other.0) || self.as_str() == other.as_str()
    }
}

impl Eq for SymbolName {}

impl PartialOrd for SymbolName {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SymbolName {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl std::hash::Hash for SymbolName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(self.as_str(), state);
    }
}

impl Deref for SymbolName {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0.internal
    }
}

impl AsRef<str> for SymbolName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::borrow::Borrow<str> for SymbolName {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for SymbolName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

impl fmt::Display for SymbolName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

impl From<String> for SymbolName {
    fn from(text: String) -> Self {
        Self::intern(text)
    }
}

impl From<&str> for SymbolName {
    fn from(text: &str) -> Self {
        Self::intern(text.to_owned())
    }
}

impl From<&String> for SymbolName {
    fn from(text: &String) -> Self {
        Self::from(text.as_str())
    }
}

impl From<SymbolName> for String {
    fn from(name: SymbolName) -> Self {
        name.into_string()
    }
}

impl From<&SymbolName> for String {
    fn from(name: &SymbolName) -> Self {
        name.as_str().to_owned()
    }
}

impl From<SharedText> for SymbolName {
    fn from(text: SharedText) -> Self {
        Self::intern(text.into_string())
    }
}

impl From<SymbolName> for SharedText {
    fn from(name: SymbolName) -> Self {
        name.as_str().into()
    }
}

impl PartialEq<str> for SymbolName {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for SymbolName {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for SymbolName {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<SymbolName> for String {
    fn eq(&self, other: &SymbolName) -> bool {
        self == other.as_str()
    }
}

impl PartialEq<SymbolName> for str {
    fn eq(&self, other: &SymbolName) -> bool {
        self == other.as_str()
    }
}

impl PartialEq<SymbolName> for &str {
    fn eq(&self, other: &SymbolName) -> bool {
        *self == other.as_str()
    }
}

/// `Lisp_Object' for a bignum: alloc.c's `PVEC_BIGNUM' pseudovector's
/// address, copied without a count.
#[repr(transparent)]
#[derive(Clone, Copy, Debug)]
pub struct SharedBigInt(crate::lisp::alloc::VectorlikeRef<LispBignum>);

impl PartialEq for SharedBigInt {
    fn eq(&self, other: &Self) -> bool {
        self.0.ptr_eq(&other.0) || (*self.0).0 == (*other.0).0
    }
}

impl Eq for SharedBigInt {}

impl PartialOrd for SharedBigInt {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SharedBigInt {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (*self.0).0.cmp(&(*other.0).0)
    }
}

impl std::hash::Hash for SharedBigInt {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (*self.0).0.hash(state);
    }
}

/// One allocated `struct Lisp_Bignum'; the live count is a counter.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LispBignum(BigInt);

impl SharedBigInt {
    pub(crate) fn identity_ptr(&self) -> usize {
        self.0.identity()
    }

    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        self.0.ptr_eq(&other.0)
    }

    pub(crate) fn mark_bit(&self) -> crate::lisp::alloc::vectors::VectorMark<'_> {
        self.0.mark_bit()
    }

    /// # Safety
    /// HEADER is an allocated bignum's header.
    pub(crate) unsafe fn from_raw(header: *mut crate::lisp::alloc::VectorHeader) -> Self {
        // SAFETY: the caller's contract.
        Self(unsafe { crate::lisp::alloc::VectorlikeRef::from_raw(header) })
    }
}

impl Deref for SharedBigInt {
    type Target = BigInt;

    fn deref(&self) -> &Self::Target {
        &(*self.0).0
    }
}

impl From<BigInt> for SharedBigInt {
    fn from(value: BigInt) -> Self {
        // lisp.h:Lisp_Bignum is 24 bytes on the supported GNU ABI.
        crate::lisp::native_comp::note_lisp_allocation(24);
        Self(crate::lisp::alloc::VectorlikeRef::allocate(LispBignum(
            value,
        )))
    }
}

impl From<SharedBigInt> for BigInt {
    fn from(value: SharedBigInt) -> Self {
        (*value.0).0.clone()
    }
}

impl PartialEq<BigInt> for SharedBigInt {
    fn eq(&self, other: &BigInt) -> bool {
        (*self.0).0 == *other
    }
}

impl PartialEq<SharedBigInt> for BigInt {
    fn eq(&self, other: &SharedBigInt) -> bool {
        *self == (*other.0).0
    }
}

impl fmt::Display for SharedBigInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (*self.0).0.fmt(f)
    }
}

/// One allocated Lisp floating-point object: alloc.c's `struct
/// Lisp_Float' in a float block, named by its address (`FloatRef').
pub type SharedFloat = crate::lisp::alloc::FloatRef;

pub type SharedCons = crate::lisp::alloc::ConsRef;
pub use crate::lisp::alloc::WeakConsRef;
pub type ConsCells = (ConsSlot, ConsSlot);
/// Interpreted and byte-code closures share their inline GNU fields.
pub use crate::lisp::alloc::ClosureRef;

impl ClosureRef {
    #[inline]
    pub(crate) fn parameters(&self) -> Value {
        self.get(0).expect("a closure has an argument slot")
    }

    #[inline]
    pub(crate) fn body(&self) -> Value {
        self.get(1).expect("a closure has a body slot")
    }

    #[inline]
    pub(crate) fn environment_value(&self) -> Value {
        self.get(2).expect("a closure has an environment slot")
    }

    pub(crate) fn documentation(&self) -> Option<Value> {
        self.get(4)
    }

    pub(crate) fn interactive(&self) -> Option<Value> {
        self.get(5)
    }

    /// eval.c:Fmake_interpreted_closure preserves IFORM's mode-list tail.
    pub(crate) fn interactive_slot_from_iform(iform: Value) -> Result<Value, LispError> {
        let tail = iform.cdr()?;
        let modes = tail.cdr()?;
        let spec = tail.car()?;
        Ok(if modes.is_nil() {
            spec
        } else {
            Value::vector([spec, modes])
        })
    }

    /// Return the public interactive specification from GNU closure slot
    /// five.  New-style slots are vectors `[SPEC MODES]' while old-style
    /// slots contain SPEC directly.
    pub fn interactive_spec(&self) -> Option<Value> {
        self.interactive().map(|slot| match slot.kind() {
            Kind::Vector(vector) => vector.get(0).unwrap_or(slot),
            _ => slot,
        })
    }

    pub fn command_modes(&self) -> Option<Value> {
        self.interactive()
            .as_ref()
            .and_then(Self::command_modes_from_slot)
    }

    pub fn command_modes_from_slot(slot: &Value) -> Option<Value> {
        match slot.kind() {
            Kind::Vector(vector) => Some(vector.get(1).unwrap_or(Value::Nil)),
            _ => None,
        }
    }
}

pub struct BufferValue {
    pub id: u64,
    /// Weak chain, as in buffer_text.markers. It does not keep markers alive.
    pub(crate) markers: Cell<Option<MarkerRef>>,
    /// The editable object, not a name/id proxy for an interpreter-owned copy.
    /// Rust callers must end these borrows before entering Lisp or collecting.
    pub(crate) state: RefCell<crate::buffer::Buffer>,
}

/// One allocated frame object. The native boundary reads its GNU
/// pseudovector header and leading name slot; the terminal backend owns
/// the rest of the state in this allocation rather than an ID proxy.
#[repr(C)]
pub struct FrameValue {
    pub(crate) name: Cell<Value>,
    pub(crate) state: RefCell<crate::lisp::eval::FrameState>,
}

impl FrameRef {
    pub(crate) fn new(name: Value, state: crate::lisp::eval::FrameState) -> Self {
        Self::allocate(FrameValue {
            name: Cell::new(name),
            state: RefCell::new(state),
        })
    }

    pub(crate) fn borrow(&self) -> std::cell::Ref<'_, crate::lisp::eval::FrameState> {
        self.state.borrow()
    }

    pub(crate) fn borrow_mut(&self) -> std::cell::RefMut<'_, crate::lisp::eval::FrameState> {
        self.state.borrow_mut()
    }

    pub(crate) fn is_live(&self) -> bool {
        // frame.h:FRAME_LIVE_P. Terminal teardown clears its device first,
        // then deletes every frame that still has this terminal pointer.
        self.borrow().terminal.is_some()
    }
}

impl std::fmt::Debug for FrameValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameValue")
            .field("id", &self.state.try_borrow().ok().map(|s| s.id))
            .finish()
    }
}
impl std::hash::Hash for FrameRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.identity().hash(state);
    }
}

impl PartialEq for FrameRef {
    fn eq(&self, other: &Self) -> bool {
        self.ptr_eq(other)
    }
}
impl Eq for FrameRef {}

/// terminal.c's object owns its Lisp slots and the terminal device state.
/// The four leading Lisp fields follow termhooks.h's struct terminal.
#[repr(C)]
#[derive(Debug)]
pub struct TerminalValue {
    pub(crate) param_alist: Cell<Value>,
    pub(crate) charset_list: Cell<Value>,
    pub(crate) selection_alist: Cell<Value>,
    pub(crate) glyph_code_table: Cell<Value>,
    pub id: u64,
    pub(crate) state: RefCell<crate::lisp::eval::terminal::TerminalState>,
}

impl TerminalRef {
    pub(crate) fn new(id: u64, state: crate::lisp::eval::terminal::TerminalState) -> Self {
        Self::allocate(TerminalValue {
            param_alist: Cell::new(Value::Nil),
            charset_list: Cell::new(Value::Nil),
            selection_alist: Cell::new(Value::Nil),
            glyph_code_table: Cell::new(Value::Nil),
            id,
            state: RefCell::new(state),
        })
    }

    pub(crate) fn borrow(&self) -> std::cell::Ref<'_, crate::lisp::eval::terminal::TerminalState> {
        self.state.borrow()
    }

    pub(crate) fn borrow_mut(
        &self,
    ) -> std::cell::RefMut<'_, crate::lisp::eval::terminal::TerminalState> {
        self.state.borrow_mut()
    }

    pub(crate) fn visit_lisp_values(&self, visit: &mut impl FnMut(&Value)) {
        for slot in [
            &self.param_alist,
            &self.charset_list,
            &self.selection_alist,
            &self.glyph_code_table,
        ] {
            visit(&slot.get());
        }
        let state = self.borrow();
        for value in state.keyboard.values() {
            visit(value);
        }
        if let Some(frame) = state.top_frame {
            visit(&Value::Frame(frame));
        }
    }
}

impl std::fmt::Debug for BufferValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferValue")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl BufferRef {
    pub fn new(id: u64, buffer: crate::buffer::Buffer) -> Self {
        let object = Self::for_restore(id, buffer);
        object.borrow_mut().install_mark_object(object, None);
        object
    }

    /// Allocate the buffer identity before relocating its existing mark.
    /// Restoring an image must not create a second, temporary mark object.
    pub(crate) fn for_restore(id: u64, buffer: crate::buffer::Buffer) -> Self {
        Self::allocate(BufferValue {
            id,
            markers: Cell::new(None),
            state: RefCell::new(buffer),
        })
    }

    pub fn borrow(&self) -> std::cell::Ref<'_, crate::buffer::Buffer> {
        self.state.borrow()
    }

    pub fn borrow_mut(&self) -> std::cell::RefMut<'_, crate::buffer::Buffer> {
        self.state.borrow_mut()
    }

    /// Replace a host-side text snapshot, establishing its Lisp mark owner.
    /// Ordinary Lisp edits operate on the existing buffer and marker fields.
    pub(crate) fn replace_state(&self, buffer: crate::buffer::Buffer) {
        self.detach_markers();
        let mut state = self.borrow_mut();
        *state = buffer;
        state.install_mark_object(*self, None);
    }
}

/// `Lisp_Object' for an ordinary vector: alloc.c's `struct Lisp_Vector'
/// in a vector block (or on its own when large), named by its address.
pub use crate::lisp::alloc::VectorRef;
/// The pseudovector kinds' handles (alloc.c's `allocate_pseudovector').
pub type BufferRef = crate::lisp::alloc::VectorlikeRef<BufferValue>;
mod marker;
pub use crate::overlay::{OverlayRef, OverlayValue};
pub use marker::{MarkerRef, MarkerValue};
pub type FrameRef = crate::lisp::alloc::VectorlikeRef<FrameValue>;
pub type TerminalRef = crate::lisp::alloc::VectorlikeRef<TerminalValue>;
pub use crate::lisp::alloc::StringObjectRef;
pub type ReaderFormRef = crate::lisp::alloc::VectorlikeRef<ReaderForm>;
/// PVEC_RECORD's handle: the record's state in a vector block.
pub type RecordRef = crate::lisp::alloc::VectorlikeRef<crate::lisp::eval::RecordState>;
pub use crate::lisp::alloc::vectors::{LispRecordRef, SymbolWithPosRef};

/// lisp.h:struct Lisp_Cons. Interpreter, bytecode and generated code read and
/// write these same two Lisp words. Allocator metadata is in the containing
/// block, with no read barrier, mirror, agreement check or per-store notice.
#[repr(C, align(8))]
#[derive(Debug)]
pub struct ConsCell {
    pub(crate) car: Cell<Value>,
    pub(crate) cdr: Cell<Value>,
}

const _: () = {
    assert!(std::mem::size_of::<ConsCell>() == 16);
    assert!(std::mem::offset_of!(ConsCell, car) == 0);
    assert!(std::mem::offset_of!(ConsCell, cdr) == 8);
};

// ===== Live-object accounting (finding 110) =====
//
// GNU's `garbage-collect' numbers come from allocator bookkeeping, not a
// heap walk; these are emaxx's equivalent books. The process-wide block
// allocator increments object counts on allocation and recomputes them
// during sweeping. A copied Value neither allocates nor retains an object
// independently of GC. Runtime serialization is required by that allocator;
// running tests serially does not itself establish public API soundness.
pub(crate) fn note_string_allocation(bytes: usize) {
    // alloc.c allocates a 32-byte Lisp_String plus `sdata_size': an 8-byte
    // back-pointer, the bytes, a terminating NUL, at least the 16-byte free
    // form, rounded to the 8-byte sdata alignment on the supported GNU ABI.
    let sdata = 8_usize
        .saturating_add(bytes)
        .saturating_add(1)
        .max(16)
        .div_ceil(8)
        .saturating_mul(8);
    crate::lisp::native_comp::note_lisp_allocation(32_usize.saturating_add(sdata));
}

#[derive(Default)]
pub(crate) struct StringCensus {
    pub(crate) count: usize,
    pub(crate) bytes: usize,
    pub(crate) property_spans: usize,
}

#[derive(Default)]
pub(crate) struct VectorCensus {
    pub(crate) count: usize,
    pub(crate) slots: usize,
    pub(crate) representation_conses: usize,
}

pub(crate) fn census_live_conses() -> usize {
    crate::lisp::alloc::live_conses()
}

pub(crate) fn census_live_strings() -> StringCensus {
    // One allocator census for every Lisp string.
    let (objects, bytes, property_spans) = crate::lisp::alloc::live_string_object_census();
    StringCensus {
        count: objects,
        bytes,
        property_spans,
    }
}

pub(crate) fn census_live_floats() -> usize {
    crate::lisp::alloc::live_floats()
}

pub(crate) fn census_live_vectors() -> VectorCensus {
    // gcstat's total_vectors and total_vector_slots: the ordinary vectors,
    // the closures (eval.c:Fmake_interpreted_closure allocates an
    // ordinary vector and retags it PVEC_CLOSURE; the header is one word
    // beyond the Lisp-visible slots) and the bignums (three words each).
    let (count, slots) = crate::lisp::alloc::live_vector_census();
    VectorCensus {
        count,
        slots,
        representation_conses: 0,
    }
}

impl ConsCell {
    fn new(car: Value, cdr: Value) -> Self {
        crate::lisp::native_comp::note_lisp_allocation(std::mem::size_of::<Self>());
        Self {
            car: Cell::new(car),
            cdr: Cell::new(cdr),
        }
    }

    pub(crate) fn identity(cell: &SharedCons) -> usize {
        cell.as_ptr() as usize
    }

    #[inline]
    pub(crate) fn car(&self) -> usize {
        self.car.get().word()
    }

    #[inline]
    pub(crate) fn cdr(&self) -> usize {
        self.cdr.get().word()
    }

    /// # Safety
    /// VALUE is a valid, live Lisp_Object word, as for generated XSETCAR.
    #[inline]
    pub(crate) unsafe fn set_car(&self, value: usize) {
        self.car.set(unsafe { Value::from_word(value) });
    }

    /// # Safety
    /// VALUE is a valid, live Lisp_Object word, as for generated XSETCDR.
    #[inline]
    pub(crate) unsafe fn set_cdr(&self, value: usize) {
        self.cdr.set(unsafe { Value::from_word(value) });
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConsField {
    Car,
    Cdr,
}

/// A retained reference to one mutable field of a cons.
///
/// Like GNU's address of XCAR/XCDR, this is the actual field address.
/// The collector recognizes either word of a cons as a root. There is no
/// separate selector or padded enum for an ordinary load/store to inspect.
#[repr(transparent)]
#[derive(Clone, Debug)]
pub struct ConsSlot {
    slot: NonNull<Cell<Value>>,
}

impl ConsSlot {
    #[inline]
    pub(crate) fn car(cell: &SharedCons) -> Self {
        Self {
            slot: NonNull::from(&cell.car),
        }
    }

    #[inline]
    pub(crate) fn cdr(cell: &SharedCons) -> Self {
        Self {
            slot: NonNull::from(&cell.cdr),
        }
    }

    #[inline]
    fn cell(&self) -> SharedCons {
        // ConsBlock's slots start at its aligned base and are exactly two
        // words each. Subtracting the field offset preserves provenance in
        // the same allocation; no registry lookup is needed.
        let offset = self.slot.as_ptr() as usize & (std::mem::size_of::<ConsCell>() - 1);
        debug_assert!(offset == 0 || offset == std::mem::offset_of!(ConsCell, cdr));
        // SAFETY: constructors use only the two fields of an allocated cons.
        // The retained interior pointer keeps that cons alive like ConsRef.
        unsafe { SharedCons::from_raw(self.slot.as_ptr().byte_sub(offset).cast::<ConsCell>()) }
    }

    #[inline]
    fn field(&self) -> &Cell<Value> {
        // Retain ConsRef's allocated-bit check in checked builds.
        #[cfg(debug_assertions)]
        let _ = &*self.cell();
        // SAFETY: the shared field address roots its containing cons; stores
        // use Cell's interior mutability under the runtime entry boundary.
        unsafe { self.slot.as_ref() }
    }

    #[inline]
    pub fn get(&self) -> Value {
        self.field().get()
    }

    #[inline]
    pub fn set(&self, value: Value) {
        self.field().set(value);
    }

    pub fn cell_id(&self) -> usize {
        ConsCell::identity(&self.cell())
    }

    pub fn ptr_eq(&self, other: &Self) -> bool {
        self.slot == other.slot
    }

    pub fn downgrade(&self) -> WeakConsSlot {
        let cell = self.cell();
        WeakConsSlot {
            cell: cell.downgrade(),
            field: if self.slot == NonNull::from(&cell.car) {
                ConsField::Car
            } else {
                ConsField::Cdr
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct WeakConsSlot {
    cell: WeakConsRef,
    field: ConsField,
}

impl WeakConsSlot {
    pub fn upgrade(&self) -> Option<ConsSlot> {
        let cell = self.cell.upgrade()?;
        Some(match self.field {
            ConsField::Car => ConsSlot::car(&cell),
            ConsField::Cdr => ConsSlot::cdr(&cell),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StringPropertySpan {
    pub start: usize,
    pub end: usize,
    pub props: Vec<(String, Value)>,
}

pub(crate) mod string_data;
pub use string_data::SharedStringState;

/// Detects circular lists during traversal with Brent's algorithm, the same
/// scheme GNU's FOR_EACH_TAIL uses: constant memory and no hashing.
pub struct CycleGuard {
    tortoise: usize,
    power: usize,
    lam: usize,
}

impl CycleGuard {
    pub fn new() -> Self {
        CycleGuard {
            tortoise: 0,
            power: 1,
            lam: 0,
        }
    }

    /// Advance past a cons cell; returns true when the cell closes a cycle.
    pub fn step(&mut self, cell_id: usize) -> bool {
        if cell_id == self.tortoise {
            return true;
        }
        if self.lam == self.power {
            self.tortoise = cell_id;
            self.power <<= 1;
            self.lam = 0;
        }
        self.lam += 1;
        false
    }
}

impl Default for CycleGuard {
    fn default() -> Self {
        CycleGuard::new()
    }
}

/// Parser output that still needs an Interpreter to allocate its final Lisp
/// object.  Keeping this typed prevents reader bookkeeping from entering the
/// Lisp namespace as a private symbol or callable bridge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[doc(hidden)]
pub enum ReaderClosureKind {
    Interpreted,
    ByteCode,
}

#[derive(Clone, Debug, PartialEq)]
#[doc(hidden)]
pub enum ReaderForm {
    CircularLabel {
        id: u32,
        payload: Value,
    },
    CircularReference(u32),
    HashTable {
        fields: Vec<Value>,
    },
    CharTable {
        fields: Vec<Value>,
    },
    SubCharTable {
        fields: Vec<Value>,
    },
    Record {
        slots: Vec<Value>,
    },
    Closure {
        kind: ReaderClosureKind,
        slots: Vec<Value>,
    },
    /// `#&N"..."'.  GNU's reader builds the bool vector directly; Emaxx's
    /// reader has no Interpreter to allocate one in, so the bits wait here
    /// for the same read/evaluation materialization boundary records use.
    BoolVector {
        bits: Vec<bool>,
    },
}

/// lisp.h's `Lisp_Object': one machine word, the object's address or an
/// immediate, with `enum Lisp_Type' in the low three bits (USE_LSB_TAG):
/// a symbol 0, a fixnum 2 or 6 (the value in the upper 62 bits), a cons
/// 3, a string 4, a vectorlike 5 (the kind in its header), a float 7.
/// Tag 1 (`Lisp_Type_Unused0') carries this implementation's remaining
/// immediates in bits 3 to 7: nil, t and the unbound marker, the kinds
/// still addressed by an id (bits 8 up).
/// Copied as a word, compared by `equal' (`PartialEq'), read through
/// `kind' as C reads `XTYPE' and the pseudovector header.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct Value(usize, std::marker::PhantomData<*mut ()>);

const TAG_MASK: usize = 7;
const TAG_SYMBOL: usize = 0;
const TAG_INT0: usize = 2;
const TAG_CONS: usize = 3;
const TAG_STRING: usize = 4;
const TAG_VECTORLIKE: usize = 5;
const TAG_FLOAT: usize = 7;

/// A tag no value carries: a panic in a debug build, and in a release
/// build the optimizer's licence to drop the arm (lisp.h reads a
/// `Lisp_Object''s tag and a pseudovector's header without a check,
/// and a checked read here would cost every inlined `kind' the header
/// load and the branch even where the site tests one immediate tag).
///
/// # Safety
/// The caller must have established that the tag cannot occur.
#[inline(always)]
unsafe fn impossible_tag(what: &'static str) -> ! {
    if cfg!(debug_assertions) {
        unreachable!("{what}");
    }
    // SAFETY: the caller's contract.
    unsafe { std::hint::unreachable_unchecked() }
}

/// What a `Value' names, as `XTYPE' and the pseudovector header tell
/// it: the object's handle or the immediate.  Read with `Value::kind';
/// the variants carry the same handles `Value''s constructors take.
#[derive(Debug, Clone, Copy)]
pub enum Kind {
    Nil,
    T,
    Integer(i64),
    BigInteger(SharedBigInt),
    Float(SharedFloat),
    StringObject(StringObjectRef),
    Symbol(SymbolName),
    /// GNU PVEC_SYMBOL_WITH_POS: the symbol and position in the allocation.
    SymbolWithPos(SymbolWithPosRef),
    Cons(SharedCons),
    /// An ordinary vector with GNU vector identity and contiguous slots.
    Vector(VectorRef),
    /// A static GNU-layout subr containing its arity and native entry point.
    BuiltinFunc(BuiltinRef),
    /// An allocated GNU-layout native subr, distinct from immutable DEFUN storage.
    NativeFunction(NativeFunctionRef),
    /// The authoritative GNU native compilation unit and owned library handle.
    NativeCompUnit(NativeUnitRef),
    /// GNU PVEC_CLOSURE: argument descriptor, code/body, constants/environment.
    Closure(ClosureRef),
    /// A buffer object: (id, name). The id is used for `eq` identity.
    Buffer(BufferRef),
    /// The canonical GNU-layout marker allocation.
    Marker(MarkerRef),
    /// The canonical GNU-layout overlay allocation.
    Overlay(OverlayRef),
    /// The canonical GNU char-table and internal radix nodes.
    CharTable(CharTableRef),
    SubCharTable(SubCharTableRef),
    /// GNU PVEC_HASH_TABLE and its canonical out-of-line entry array.
    HashTable(HashTableRef),
    /// An allocated frame with address identity and directly owned state.
    Frame(FrameRef),
    /// An allocated terminal with address identity and directly owned state.
    Terminal(TerminalRef),
    /// A record, or one of the pseudovector kinds this implementation
    /// keeps as records (alloc.c's PVEC_RECORD): the cell's address.
    Record(RecordRef),
    /// GNU PVEC_RECORD with its type and data slots inline.
    LispRecord(LispRecordRef),
    /// GNU PVEC_FINALIZER: the object containing its callback and list links.
    Finalizer(crate::lisp::alloc::FinalizerRef),
    /// Typed reader state awaiting Interpreter-owned object allocation.
    ReaderForm(ReaderFormRef),
    /// Internal marker for EIEIO slots that have not been bound.
    Unbound,
}

#[allow(non_snake_case, non_upper_case_globals)]
impl Value {
    /// A Lisp word may name mutable GC storage. The zero-sized marker keeps
    /// it local to its owning OS thread without changing the word-sized ABI.
    const fn from_bits(word: usize) -> Self {
        Self(word, std::marker::PhantomData)
    }

    /// GNU's builtin symbol words: globals.h indices 0, 1 and 2 with the
    /// supported 64-bit Lisp_Symbol size of 48 bytes. All execution modes
    /// use these words; no native boolean anchor or unbound handle exists.
    pub const Nil: Value = Value::from_bits(0);
    pub const T: Value = Value::from_bits(48);
    pub const Unbound: Value = Value::from_bits(96);

    /// lisp.h's `make_int': a fixnum for a value in `most-positive-fixnum''s
    /// range, a bignum past it.
    #[inline(always)]
    pub fn Integer(n: i64) -> Value {
        const MOST_POSITIVE: i64 = (1_i64 << 61) - 1;
        const MOST_NEGATIVE: i64 = -(1_i64 << 61);
        if (MOST_NEGATIVE..=MOST_POSITIVE).contains(&n) {
            Value::from_bits(((n << 2) as usize) | TAG_INT0)
        } else {
            Self::integer_bignum(n)
        }
    }

    // lisp.h:make_int keeps the ordinary immediate path separate from
    // make_bigint. Outlining allocation also lets each Rust caller inline
    // the range check and tagged word without the bignum construction body.
    #[cold]
    #[inline(never)]
    fn integer_bignum(n: i64) -> Value {
        Value::BigInteger(BigInt::from(n).into())
    }

    #[inline]
    pub fn BigInteger(value: SharedBigInt) -> Value {
        Value::from_bits(value.identity_ptr() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn Float(value: SharedFloat) -> Value {
        Value::from_bits(value.identity_ptr() | TAG_FLOAT)
    }
    #[inline]
    pub fn String(text: SharedText) -> Value {
        Value::StringObject(text)
    }
    #[inline]
    pub fn StringObject(state: StringObjectRef) -> Value {
        Value::from_bits(state.identity() | TAG_STRING)
    }
    #[inline]
    pub fn Symbol(name: SymbolName) -> Value {
        // Temporary until the builtin symbols and their mutable cells
        // share the same symbol allocation. Private-obarray and uninterned
        // names carry a distinct internal key and cannot take these arms.
        match name.as_str() {
            "nil" => Value::Nil,
            "t" => Value::T,
            _ => Value::from_bits(name.identity_ptr() | TAG_SYMBOL),
        }
    }
    #[inline]
    pub fn Cons(cell: SharedCons) -> Value {
        Value::from_bits(cell.as_ptr() as usize | TAG_CONS)
    }
    #[inline]
    pub fn SymbolWithPos(object: SymbolWithPosRef) -> Value {
        Value::from_bits(object.identity() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn Vector(vector: VectorRef) -> Value {
        Value::from_bits(vector.identity() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn BuiltinFunc(subr: BuiltinRef) -> Value {
        Value::from_bits(subr.identity_ptr() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn NativeCompUnit(unit: NativeUnitRef) -> Value {
        Value::from_bits(unit.identity() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn NativeFunction(function: NativeFunctionRef) -> Value {
        Value::from_bits(function.identity() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn Closure(lambda: ClosureRef) -> Value {
        Value::from_bits(lambda.identity() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn Buffer(buffer: BufferRef) -> Value {
        Value::from_bits(buffer.identity() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn Marker(marker: MarkerRef) -> Value {
        Value::from_bits(marker.identity() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn Overlay(overlay: OverlayRef) -> Value {
        Value::from_bits(overlay.identity() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn CharTable(table: CharTableRef) -> Value {
        Value::from_bits(table.identity() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn SubCharTable(table: SubCharTableRef) -> Value {
        Value::from_bits(table.identity() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn HashTable(table: HashTableRef) -> Value {
        Value::from_bits(table.identity() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn Frame(frame: FrameRef) -> Value {
        Value::from_bits(frame.identity() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn Terminal(terminal: TerminalRef) -> Value {
        Value::from_bits(terminal.identity() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn LispRecord(record: LispRecordRef) -> Value {
        Value::from_bits(record.identity() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn Record(record: RecordRef) -> Value {
        Value::from_bits(record.identity() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn Finalizer(object: crate::lisp::alloc::FinalizerRef) -> Value {
        Value::from_bits(object.identity() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn ReaderForm(form: ReaderFormRef) -> Value {
        Value::from_bits(form.identity() | TAG_VECTORLIKE)
    }

    /// The word itself (the conservative scan's and the native runtime's
    /// view of a `Lisp_Object').
    #[inline(always)]
    pub(crate) fn word(self) -> usize {
        self.0
    }

    /// A value from a word `word' produced (an object's tagged address or
    /// an immediate).
    ///
    /// # Safety
    /// WORD must have come from `word' of a value whose object is still
    /// allocated, or be an immediate.
    #[inline(always)]
    pub(crate) unsafe fn from_word(word: usize) -> Value {
        Value::from_bits(word)
    }

    /// `XTYPE' and the pseudovector header: the kind, with the handle.
    /// Always inlined, as lisp.h's type predicates are: at a site that
    /// tests one tag the optimizer keeps that mask alone and drops the
    /// rest of the switch (the header read of a vectorlike included).
    #[inline(always)]
    pub fn kind(self) -> Kind {
        let word = self.0;
        match word & TAG_MASK {
            TAG_INT0 | 6 => Kind::Integer((word as i64) >> 2),
            TAG_CONS => {
                // SAFETY: a value's cons is an allocated cell while the
                // value is reachable (the collector's contract).
                Kind::Cons(unsafe { SharedCons::from_raw((word & !TAG_MASK) as *const ConsCell) })
            }
            TAG_SYMBOL => {
                match word {
                    0 => return Kind::Nil,
                    48 => return Kind::T,
                    96 => return Kind::Unbound,
                    _ => {}
                }
                // SAFETY: as above, a symbol cell.
                Kind::Symbol(SymbolName::from_ref(unsafe {
                    crate::lisp::alloc::SymbolRef::from_raw(
                        word as *mut crate::lisp::alloc::SymbolCell,
                    )
                }))
            }
            TAG_STRING => {
                // SAFETY: every string word names its direct string header.
                Kind::StringObject(unsafe {
                    StringObjectRef::from_raw((word & !TAG_MASK) as *mut _)
                })
            }
            TAG_FLOAT => {
                // SAFETY: as above, a float cell.
                Kind::Float(unsafe { SharedFloat::from_raw((word & !TAG_MASK) as *mut _) })
            }
            TAG_VECTORLIKE => {
                let header = (word & !TAG_MASK) as *mut crate::lisp::alloc::VectorHeader;
                // SAFETY: as above, a vector header; each kind's handle is
                // made from the header the way `alloc::vectors::value_of'
                // makes it.
                unsafe {
                    match crate::lisp::alloc::vectors::header_tag(header) {
                        crate::lisp::alloc::VectorTag::Normal => {
                            Kind::Vector(VectorRef::from_raw(header))
                        }
                        crate::lisp::alloc::VectorTag::Bignum => {
                            Kind::BigInteger(SharedBigInt::from_raw(header))
                        }
                        crate::lisp::alloc::VectorTag::Finalizer => {
                            Kind::Finalizer(crate::lisp::alloc::FinalizerRef::from_raw(header))
                        }
                        crate::lisp::alloc::VectorTag::SymbolWithPos => {
                            Kind::SymbolWithPos(SymbolWithPosRef::from_raw(header))
                        }
                        crate::lisp::alloc::VectorTag::NativeCompUnit => {
                            Kind::NativeCompUnit(NativeUnitRef::from_raw(header))
                        }
                        crate::lisp::alloc::VectorTag::Subr => {
                            if crate::lisp::alloc::vectors::subr_is_allocated(header) {
                                Kind::NativeFunction(NativeFunctionRef::from_raw(header))
                            } else {
                                Kind::BuiltinFunc(BuiltinRef::from_raw(header as usize))
                            }
                        }
                        crate::lisp::alloc::VectorTag::Buffer => {
                            Kind::Buffer(crate::lisp::alloc::VectorlikeRef::from_raw(header))
                        }
                        crate::lisp::alloc::VectorTag::Marker => {
                            Kind::Marker(crate::lisp::alloc::VectorlikeRef::from_raw(header))
                        }
                        crate::lisp::alloc::VectorTag::Overlay => {
                            Kind::Overlay(OverlayRef::from_raw(header))
                        }
                        crate::lisp::alloc::VectorTag::Frame => {
                            Kind::Frame(crate::lisp::alloc::VectorlikeRef::from_raw(header))
                        }
                        crate::lisp::alloc::VectorTag::Terminal => {
                            Kind::Terminal(crate::lisp::alloc::VectorlikeRef::from_raw(header))
                        }
                        crate::lisp::alloc::VectorTag::CharTable => {
                            Kind::CharTable(CharTableRef::from_raw(header))
                        }
                        crate::lisp::alloc::VectorTag::SubCharTable => {
                            Kind::SubCharTable(SubCharTableRef::from_raw(header))
                        }
                        crate::lisp::alloc::VectorTag::HashTable => {
                            Kind::HashTable(HashTableRef::from_raw(header))
                        }
                        crate::lisp::alloc::VectorTag::Closure => {
                            Kind::Closure(crate::lisp::alloc::ClosureRef::from_raw(header))
                        }
                        crate::lisp::alloc::VectorTag::ReaderForm => {
                            Kind::ReaderForm(crate::lisp::alloc::VectorlikeRef::from_raw(header))
                        }
                        crate::lisp::alloc::VectorTag::Record => {
                            if crate::lisp::alloc::vectors::generic_records::record_has_inline_slots(
                                header,
                            ) {
                                Kind::LispRecord(LispRecordRef::from_raw(header))
                            } else {
                                Kind::Record(crate::lisp::alloc::VectorlikeRef::from_raw(header))
                            }
                        }
                        // SAFETY: a value names no free vector (the
                        // collector's contract); C reads the header's
                        // tag without a check, and so does the release
                        // build.
                        crate::lisp::alloc::VectorTag::Free => {
                            impossible_tag("a value names no free vector")
                        }
                    }
                }
            }
            // All supported values use their GNU tag and allocated address.
            _ => unsafe { impossible_tag("a value with an unknown tag") },
        }
    }

    /// The symbol a word names when its tag is the symbol tag, read from
    /// the word alone: no memory of the object is touched, so the sweep
    /// may ask it of a dead object's field (whose target, a vectorlike,
    /// might already be freed, and whose header `kind' would read).
    #[inline]
    pub(crate) fn symbol_name_by_tag(&self) -> Option<&str> {
        if self.0 & TAG_MASK == TAG_SYMBOL && !matches!(self.0, 0 | 48 | 96) {
            // SAFETY: a symbol-tagged word names a symbol cell; symbol
            // cells are swept after the vectors and the conses. Its host
            // key is immutable and the borrow cannot outlive this handle.
            let cell = unsafe { &*(self.0 as *const crate::lisp::alloc::SymbolCell) };
            Some(cell.internal.as_str())
        } else {
            None
        }
    }

    /// `EQ': the same word.
    #[inline(always)]
    pub fn eq_value(self, other: Value) -> bool {
        self.0 == other.0
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind().fmt(f)
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value().fmt(f)
    }
}

impl Kind {
    /// The value this kind was read from (the word again).
    #[inline(always)]
    pub fn value(self) -> Value {
        match self {
            Kind::Nil => Value::Nil,
            Kind::T => Value::T,
            Kind::Unbound => Value::Unbound,
            Kind::Integer(n) => Value::Integer(n),
            Kind::BigInteger(v) => Value::BigInteger(v),
            Kind::Float(v) => Value::Float(v),
            Kind::StringObject(v) => Value::StringObject(v),
            Kind::Symbol(v) => Value::Symbol(v),
            Kind::Cons(v) => Value::Cons(v),
            Kind::Vector(v) => Value::Vector(v),
            Kind::BuiltinFunc(v) => Value::BuiltinFunc(v),
            Kind::NativeFunction(v) => Value::NativeFunction(v),
            Kind::NativeCompUnit(v) => Value::NativeCompUnit(v),
            Kind::Closure(v) => Value::Closure(v),
            Kind::Buffer(v) => Value::Buffer(v),
            Kind::Marker(v) => Value::Marker(v),
            Kind::Overlay(v) => Value::Overlay(v),
            Kind::HashTable(v) => Value::HashTable(v),
            Kind::CharTable(v) => Value::CharTable(v),
            Kind::SubCharTable(v) => Value::SubCharTable(v),
            Kind::Frame(v) => Value::Frame(v),
            Kind::Terminal(v) => Value::Terminal(v),
            Kind::SymbolWithPos(v) => Value::SymbolWithPos(v),
            Kind::Record(v) => Value::Record(v),
            Kind::LispRecord(v) => Value::LispRecord(v),
            Kind::Finalizer(v) => Value::Finalizer(v),
            Kind::ReaderForm(v) => Value::ReaderForm(v),
        }
    }
}

impl Value {
    /// Drop a value the VM is done with.  Every kind is a cell address or
    /// an immediate now (nothing to drop); the method stays where the VM
    /// says it is done with a value.
    #[inline(always)]
    pub(crate) fn discard(self) {
        let _ = self;
    }
}

/// eval.c's `Vinternal_interpreter_environment' as one scope holds it:
/// the head of the alist of `(SYMBOL . VALUE)' binding conses, with `t'
/// for a lexical scope that binds nothing and a bare symbol for a
/// variable declared locally special (Fdefvar without a value); `nil' is
/// dynamic binding.
///
/// A frame on `Env' is one specbind of `internal-interpreter-environment'
/// (Flet, FletX, funcall_lambda, Feval, readevalloop) and `Env::truncate'
/// its unbind_to; the current environment is the last frame's.  A read is
/// Fassq over it, an assignment XSETCDR on the binding found, and a
/// closure holds the head it was made under (Ffunction), sharing the
/// binding conses of every scope still live: that sharing is the whole of
/// GNU's captured-variable semantics.
#[derive(Clone, Debug)]
pub struct EnvFrame(Value);

impl EnvFrame {
    /// ENVIRONMENT itself, as `Feval' installs its LEXICAL argument and
    /// funcall_lambda a closure's.
    #[inline]
    pub fn from_alist(environment: Value) -> Self {
        Self(environment)
    }

    /// `(t)': a lexical scope binding nothing (eval.c's `list_of_t').
    pub fn lexical() -> Self {
        Self(Value::list([Value::T]))
    }

    /// Dynamic binding: `nil'.
    #[inline]
    pub fn dynamic() -> Self {
        Self(Value::Nil)
    }

    /// BINDINGS consed onto OUTER in order, as Flet conses its varlist:
    /// the last binding is the first entry, the one Fassq finds when a
    /// name repeats.
    pub fn bindings(
        bindings: impl IntoIterator<Item = (SymbolName, Value)>,
        outer: &Value,
    ) -> Self {
        let mut environment = *outer;
        for (symbol, value) in bindings {
            environment = Value::cons(Value::cons(Value::Symbol(symbol), value), environment);
        }
        Self(environment)
    }

    /// The environment alist.
    #[inline]
    pub fn environment(&self) -> &Value {
        &self.0
    }

    /// A store into the variable without a new specpdl entry: FletX's
    /// lexical bindings after its first, Fdefvar's local declaration.
    #[inline]
    pub fn set_environment(&mut self, environment: Value) {
        self.0 = environment;
    }

    /// Whether the scope binds lexically (its environment is non-nil).
    #[inline]
    pub fn is_lexical(&self) -> bool {
        !self.0.is_nil()
    }
}

/// eval.c's `Vinternal_interpreter_environment' and the values `specbind'
/// saved of it: the frames a Rust scope holds live in a vector the
/// collector scans (a plain `Vec' on the Rust heap would not be).
pub type Env = crate::lisp::alloc::RootedVec<EnvFrame>;

/// The current interpreter environment: the last frame's, or `nil' with
/// no frame.
#[inline]
pub(crate) fn current_environment(env: &Env) -> Option<&Value> {
    env.last()
        .map(EnvFrame::environment)
        .filter(|e| !e.is_nil())
}

/// The current interpreter environment as a value (`nil' with no frame).
#[inline]
pub(crate) fn current_environment_value(env: &Env) -> Value {
    env.last().map_or(Value::Nil, |frame| *frame.environment())
}

/// Whether `Vinternal_interpreter_environment' is non-nil.
#[inline]
pub(crate) fn environment_is_lexical(env: &Env) -> bool {
    env.last().is_some_and(EnvFrame::is_lexical)
}

/// eval.c's `Fassq (form, Vinternal_interpreter_environment)': the
/// binding cons whose car is SYMBOL (by identity), entries that are not
/// conses passed over.  An improper alist signals `listp' and a circular
/// one `circular-list', as Fassq's FOR_EACH_TAIL does.
pub(crate) fn assq_binding(
    environment: &Value,
    symbol: &SymbolName,
) -> Result<Option<SharedCons>, LispError> {
    assq_environment(environment, |bound| bound.id() == symbol.id())
}

/// `assq_binding' for a name in hand: the binding whose symbol has that
/// text.
pub(crate) fn assq_binding_named(
    environment: &Value,
    name: &str,
) -> Result<Option<SharedCons>, LispError> {
    assq_environment(environment, |bound| bound.as_str() == name)
}

#[inline]
fn assq_environment(
    environment: &Value,
    matches: impl Fn(&SymbolName) -> bool,
) -> Result<Option<SharedCons>, LispError> {
    let mut tail = match environment.kind() {
        Kind::Cons(cell) => cell,
        Kind::Nil => return Ok(None),
        _ => return Err(improper_environment(environment, false)),
    };
    // FOR_EACH_TAIL's cycle check (Brent): the tortoise moves to the hare
    // at every power of two.
    let mut tortoise = tail.as_ptr();
    let mut steps = 0usize;
    let mut lap = 2usize;
    loop {
        {
            let entry = tail.car.get();
            if let Kind::Cons(binding) = (entry).kind()
                && matches!(binding.car.get().kind(), Kind::Symbol(bound) if matches(&bound))
            {
                return Ok(Some(binding));
            }
        }
        let next = match tail.cdr.get().kind() {
            Kind::Cons(cell) => cell,
            Kind::Nil => return Ok(None),
            _ => return Err(improper_environment(environment, false)),
        };
        if next.as_ptr() == tortoise {
            return Err(improper_environment(environment, true));
        }
        steps += 1;
        if steps == lap {
            tortoise = next.as_ptr();
            lap <<= 1;
        }
        tail = next;
    }
}

fn improper_environment(environment: &Value, circular: bool) -> LispError {
    if circular {
        LispError::SignalValue(Value::list([
            Value::Symbol("circular-list".into()),
            *environment,
        ]))
    } else {
        LispError::WrongTypeArgument("listp".into(), *environment)
    }
}

/// Flet's `Fmemq (var, Vinternal_interpreter_environment)': whether NAME
/// is an entry of the environment itself, a bare symbol declared locally
/// special.
pub(crate) fn environment_declares_special(environment: &Value, name: &str) -> bool {
    let mut tail = match environment.kind() {
        Kind::Cons(cell) => cell,
        _ => return false,
    };
    loop {
        if matches!(tail.car.get().kind(), Kind::Symbol(entry) if entry.as_str() == name) {
            return true;
        }
        let next = match tail.cdr.get().kind() {
            Kind::Cons(cell) => cell,
            _ => return false,
        };
        tail = next;
    }
}

pub(crate) fn make_uninterned_symbol_name(base: &str, id: u64) -> String {
    format!("{base}{UNINTERNED_SYMBOL_MARKER}{id}")
}

pub(crate) fn make_obarray_symbol_name(base: &str, obarray_id: u64) -> String {
    format!("{base}{OBARRAY_SYMBOL_MARKER}{obarray_id}")
}

static FRESH_OBARRAY_SYMBOL_SERIAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(1);

/// lread.c:intern_driver allocates a new symbol object on every obarray
/// miss.  A private obarray's symbol is keyed by its name here, so a name
/// uninterned and interned again must not resolve to the old object with
/// its old value, function and plist: each miss gets a fresh serial.
pub(crate) fn make_fresh_obarray_symbol_name(base: &str, obarray_id: u64) -> String {
    let serial = FRESH_OBARRAY_SYMBOL_SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("{base}{OBARRAY_SYMBOL_MARKER}{obarray_id}.{serial}")
}

pub(crate) fn is_uninterned_symbol(symbol: &str) -> bool {
    symbol.contains(UNINTERNED_SYMBOL_MARKER_CHAR)
}

/// A symbol interned in a private obarray (or an abbrev table): its own
/// object, whose Lisp name can equal an initial-obarray symbol's.
pub(crate) fn is_private_obarray_symbol(symbol: &str) -> bool {
    symbol.contains(OBARRAY_SYMBOL_MARKER)
}

/// Whether NAME carries neither marker: `visible_symbol_name' returns it
/// unchanged.  A byte scan, where the split cost every symbol of an
/// obarray walk.
pub(crate) fn is_visible_symbol_name(name: &str) -> bool {
    !name.contains(UNINTERNED_SYMBOL_MARKER_CHAR) && !name.contains(OBARRAY_SYMBOL_MARKER_CHAR)
}

pub(crate) fn visible_symbol_name(symbol: &str) -> &str {
    // Character patterns: a one-character `&str' pattern runs the general
    // substring searcher on every name `mapatoms' visits.
    symbol
        .split_once(UNINTERNED_SYMBOL_MARKER_CHAR)
        .or_else(|| symbol.split_once(OBARRAY_SYMBOL_MARKER_CHAR))
        .map(|(visible, _)| visible)
        .unwrap_or(symbol)
}

fn render_error_symbol_name(symbol: &str) -> String {
    let visible = visible_symbol_name(symbol);
    if visible.is_empty() {
        return "##".into();
    }

    let mut rendered = String::new();
    for ch in visible.chars() {
        if matches!(
            ch,
            '"' | '\\' | '\'' | ';' | '#' | '(' | ')' | ',' | '`' | '[' | ']'
        ) || ch <= ' '
            || ch == '\u{00A0}'
        {
            rendered.push('\\');
        }
        rendered.push(ch);
    }
    rendered
}

pub(crate) fn interned_symbol_value(symbol: String) -> Value {
    match symbol.as_str() {
        "nil" => Value::Nil,
        "t" => Value::T,
        _ => Value::Symbol(symbol.into()),
    }
}

pub(crate) fn format_float(value: f64) -> String {
    if value.is_infinite() {
        return if value.is_sign_positive() {
            "1.0e+INF".into()
        } else {
            "-1.0e+INF".into()
        };
    }
    if value.is_nan() {
        return if value.is_sign_negative() {
            "-0.0e+NaN".into()
        } else {
            "0.0e+NaN".into()
        };
    }

    // GNU's dtoastr starts at DBL_DIG significant digits (at one for a
    // subnormal, whose precision is below DBL_DIG: 5e-324, not
    // 4.94065645841247e-324) and grows only until parsing reproduces
    // the same f64.  Rust's Display instead prefers fixed notation for
    // many large integral values, which changes `read' from float to
    // bignum and breaks numeric round trips.
    let abs = value.abs();
    let mut rendered = if abs == 0.0 {
        value.to_string()
    } else {
        let exponent = abs.log10().floor() as i32;
        let first = if abs < f64::MIN_POSITIVE { 1 } else { 15 };
        (first..=17)
            .find_map(|significant| {
                let scientific = exponent < -4 || exponent >= significant;
                let candidate = if scientific {
                    let precision = (significant - 1) as usize;
                    let rendered = format!("{value:.precision$e}");
                    let (mantissa, exponent) = rendered.split_once('e')?;
                    let mantissa = mantissa.trim_end_matches('0').trim_end_matches('.');
                    let exponent = exponent.parse::<i32>().ok()?;
                    format!("{mantissa}e{exponent:+}")
                } else {
                    let precision = (significant - exponent - 1).max(0) as usize;
                    format!("{value:.precision$}")
                        .trim_end_matches('0')
                        .trim_end_matches('.')
                        .to_string()
                };
                candidate
                    .parse::<f64>()
                    .ok()
                    .filter(|parsed| parsed.to_bits() == value.to_bits())
                    .map(|_| candidate)
            })
            .unwrap_or_else(|| value.to_string())
    };
    if !rendered.contains(['.', 'e', 'E']) {
        rendered.push_str(".0");
    }
    rendered
}

impl Value {
    // Constructors

    pub fn int(n: i64) -> Self {
        Value::Integer(n)
    }

    pub fn big_integer(n: BigInt) -> Self {
        Value::BigInteger(n.into())
    }

    pub fn float(value: f64) -> Self {
        Value::Float(value.into())
    }

    pub fn string(s: &str) -> Self {
        Value::String(s.into())
    }

    pub fn symbol(s: &str) -> Self {
        Value::Symbol(s.into())
    }

    pub fn cons(car: Value, cdr: Value) -> Self {
        Value::Cons(crate::lisp::alloc::allocate_cons(ConsCell::new(car, cdr)))
    }

    pub(crate) fn positioned_symbol(symbol: Value, position: Value) -> Self {
        Self::SymbolWithPos(SymbolWithPosRef::allocate(symbol, position))
    }

    pub fn vector(items: impl IntoIterator<Item = Value>) -> Self {
        let slots = items.into_iter().collect::<Vec<_>>();
        if !slots.is_empty() {
            // The C footprint: the header word and a word a slot
            // (alloc.c:zero_vector is one static object, outside the
            // census).
            crate::lisp::native_comp::note_lisp_allocation(
                slots.len().saturating_add(1).saturating_mul(8),
            );
        }
        Value::Vector(VectorRef::allocate(slots))
    }

    /// Host-side construction uses the same Lisp lists as the reader and
    /// Fmake_interpreted_closure. The vectors are consumed at construction;
    /// they never become an additional representation of executable code.
    pub fn lambda(params: Vec<SymbolName>, body: Vec<Value>, env: Value) -> Self {
        let parameters = Value::list(params.into_iter().map(Value::Symbol));
        let body = if body.is_empty() {
            Value::list([Value::Nil])
        } else {
            Value::list(body)
        };
        Self::allocated_closure(&[parameters, body, env])
    }

    pub(crate) fn allocated_closure(slots: &[Value]) -> Self {
        Value::Closure(crate::lisp::alloc::ClosureRef::allocate(slots))
    }

    pub fn buffer(id: u64, name: impl Into<String>) -> Self {
        Value::Buffer(BufferRef::new(id, crate::buffer::Buffer::new(&name.into())))
    }

    /// Build a proper list from an iterator of values.
    pub fn list(items: impl IntoIterator<Item = Value>) -> Self {
        let items: Vec<Value> = items.into_iter().collect();
        if matches!(
            items.first().map(|v| v.kind()),
            Some(Kind::Symbol(tag)) if tag == "vector-literal"
        ) {
            return Value::vector(items.into_iter().skip(1));
        }
        let mut result = Value::Nil;
        for item in items.into_iter().rev() {
            result = Value::cons(item, result);
        }
        result
    }

    // Predicates

    pub fn is_nil(&self) -> bool {
        matches!(self.kind(), Kind::Nil)
    }

    pub fn is_truthy(&self) -> bool {
        !self.is_nil()
    }

    pub fn is_integer(&self) -> bool {
        matches!(self.kind(), Kind::Integer(_) | Kind::BigInteger(_))
    }

    pub fn is_string(&self) -> bool {
        self.0 & TAG_MASK == TAG_STRING
    }

    pub fn is_symbol(&self) -> bool {
        matches!(self.kind(), Kind::Nil | Kind::T | Kind::Symbol(_))
    }

    pub fn is_cons(&self) -> bool {
        matches!(self.kind(), Kind::Cons(_))
    }

    pub fn is_list(&self) -> bool {
        matches!(self.kind(), Kind::Nil | Kind::Cons(_))
    }

    // Accessors

    /// GNU's `CHECK_FIXNUM', which names `fixnump' -- NOT `integerp'.
    ///
    /// `as_integer' below is the `CHECK_INTEGER' analogue and names
    /// `integerp'; the two are genuinely different predicates and GNU picks
    /// deliberately.  `(nth 'a '(1))' signals `integerp' while
    /// `(get-unused-iso-final-char 'a 94)' signals `fixnump', so a primitive
    /// mirroring CHECK_FIXNUM must use this one.
    pub fn as_fixnum(&self) -> Result<i64, LispError> {
        match self.kind() {
            Kind::Integer(n) => Ok(n),
            // A bignum is an integer but not a fixnum, which is exactly what
            // CHECK_FIXNUM rejects.
            _ => Err(LispError::WrongTypeArgument("fixnump".into(), *self)),
        }
    }

    pub fn as_integer(&self) -> Result<i64, LispError> {
        // GNU's CHECK_INTEGER names `integerp'; CHECK_FIXNUM names `fixnump'
        // and is spelled `as_fixnum' above.
        match self.kind() {
            Kind::Integer(n) => Ok(n),
            Kind::BigInteger(n) => n
                .to_i64()
                .ok_or_else(|| LispError::WrongTypeArgument("fixnump".into(), *self)),
            _ => Err(LispError::WrongTypeArgument("integerp".into(), *self)),
        }
    }

    pub fn as_float(&self) -> Result<f64, LispError> {
        // Arithmetic contexts: GNU's coercion check names
        // `number-or-marker-p' ((+ 'a 1) => (number-or-marker-p a)).
        match self.kind() {
            Kind::Float(f) => Ok(f.get()),
            Kind::Integer(n) => Ok(n as f64),
            Kind::BigInteger(n) => n
                .to_f64()
                .ok_or_else(|| LispError::WrongTypeArgument("number-or-marker-p".into(), *self)),
            _ => Err(LispError::WrongTypeArgument(
                "number-or-marker-p".into(),
                *self,
            )),
        }
    }

    pub fn as_string(&self) -> Result<String, LispError> {
        match self.kind() {
            Kind::StringObject(s) => Ok(s.borrow().text()),
            _ => Err(LispError::WrongTypeArgument("stringp".into(), *self)),
        }
    }

    pub fn as_symbol(&self) -> Result<&str, LispError> {
        match self.kind() {
            Kind::Nil => Ok("nil"),
            Kind::T => Ok("t"),
            Kind::Symbol(_) => Ok(self.symbol_name_by_tag().expect("symbol tag")),
            _ => Err(LispError::WrongTypeArgument("symbolp".into(), *self)),
        }
    }

    pub fn car(&self) -> Result<Value, LispError> {
        match self.kind() {
            Kind::Cons(cell) => Ok(cell.car.get()),
            Kind::Nil => Ok(Value::Nil),
            _ => Err(LispError::WrongTypeArgument("listp".into(), *self)),
        }
    }

    pub fn cdr(&self) -> Result<Value, LispError> {
        match self.kind() {
            Kind::Cons(cell) => Ok(cell.cdr.get()),
            Kind::Nil => Ok(Value::Nil),
            _ => Err(LispError::WrongTypeArgument("listp".into(), *self)),
        }
    }

    pub fn set_car(&self, new_car: Value) -> Result<(), LispError> {
        match self.kind() {
            Kind::Cons(cell) => {
                cell.car.set(new_car);
                Ok(())
            }
            _ => Err(LispError::WrongTypeArgument("consp".into(), *self)),
        }
    }

    pub fn set_cdr(&self, new_cdr: Value) -> Result<(), LispError> {
        match self.kind() {
            Kind::Cons(cell) => {
                cell.cdr.set(new_cdr);
                Ok(())
            }
            _ => Err(LispError::WrongTypeArgument("consp".into(), *self)),
        }
    }

    pub fn cons_cells(&self) -> Option<ConsCells> {
        match self.kind() {
            Kind::Cons(cell) => Some((ConsSlot::car(&cell), ConsSlot::cdr(&cell))),
            _ => None,
        }
    }

    pub fn cons_id(&self) -> Option<usize> {
        match self.kind() {
            Kind::Cons(cell) => Some(ConsCell::identity(&cell)),
            _ => None,
        }
    }

    pub fn cons_values(&self) -> Option<(Value, Value)> {
        match self.kind() {
            Kind::Cons(cell) => Some((cell.car.get(), cell.cdr.get())),
            _ => None,
        }
    }

    /// Convert a proper list to a Vec.
    pub fn to_vec(self) -> Result<Vec<Value>, LispError> {
        if let Kind::Vector(vector) = self.kind() {
            let slots = vector.slots();
            let mut result = Vec::with_capacity(slots.len().saturating_add(1));
            result.push(Value::symbol("vector-literal"));
            result.extend(slots);
            return Ok(result);
        }
        let mut result = Vec::new();
        self.extend_list_elements(&mut result)?;
        Ok(result)
    }

    /// Append the elements of a proper list to an existing value buffer.
    ///
    /// Source evaluation and other callers share this path so cycle and
    /// improper-list handling cannot drift between independent list walkers.
    pub(crate) fn extend_list_elements(&self, result: &mut Vec<Value>) -> Result<(), LispError> {
        self.visit_list_elements(|value| {
            result.push(value);
            Ok(())
        })
    }

    /// Visit the actual list fields without allocating an argument copy.
    /// Retains the same cycle and improper-tail checks as `to_vec`.
    #[inline]
    pub(crate) fn visit_list_elements(
        &self,
        mut visit: impl FnMut(Value) -> Result<(), LispError>,
    ) -> Result<(), LispError> {
        let mut current = *self;
        let mut seen = CycleGuard::new();
        loop {
            match current.kind() {
                Kind::Nil => return Ok(()),
                Kind::Cons(cell) => {
                    if seen.step(ConsCell::identity(&cell)) {
                        return Err(circular_list_error());
                    }
                    visit(cell.car.get())?;
                    current = cell.cdr.get();
                }
                _ => {
                    return Err(LispError::WrongTypeArgument("listp".into(), current));
                }
            }
        }
    }

    pub fn type_name(&self) -> String {
        match self.kind() {
            Kind::Nil => "nil".into(),
            Kind::T => "t".into(),
            Kind::Integer(_) => "integer".into(),
            Kind::BigInteger(_) => "integer".into(),
            Kind::Float(_) => "float".into(),
            Kind::StringObject(_) => "string".into(),
            Kind::Symbol(_) => "symbol".into(),
            Kind::Cons(_) => "cons".into(),
            Kind::Vector(_) => "vector".into(),
            Kind::BuiltinFunc(name) => format!("builtin<{}>", name),
            Kind::NativeFunction(function) => format!("native-function<{}>", function.name()),
            Kind::NativeCompUnit(_) => "native-comp-unit".into(),
            Kind::Closure(closure) => if closure.is_bytecode() {
                "byte-code-function"
            } else {
                "lambda"
            }
            .into(),
            Kind::Buffer(buffer) => format!("buffer<{}>", buffer.borrow().name),
            Kind::Marker(id) => format!("marker<{}>", id),
            Kind::Overlay(id) => format!("overlay<{}>", id),
            Kind::HashTable(table) => format!("hash-table<{:x}>", table.identity()),
            Kind::CharTable(id) => format!("char-table<{}>", id),
            Kind::SubCharTable(id) => format!("sub-char-table<{:x}>", id.identity()),
            Kind::Frame(id) => format!("frame<{}>", id.borrow().id),
            Kind::Terminal(terminal) => format!("terminal<{}>", terminal.id),
            Kind::SymbolWithPos(_) => "symbol-with-pos".into(),
            Kind::Record(record) => format!("record<{}>", record.id),
            Kind::LispRecord(record) => format!("record<{:x}>", record.identity()),
            Kind::Finalizer(object) => format!("finalizer<{:x}>", object.identity()),
            Kind::ReaderForm(_) => "reader-form".into(),
            Kind::Unbound => "unbound".into(),
        }
    }
}

/// The `Kind' of each of ITEMS, for slice patterns over list elements.
pub fn kinds(items: &[Value]) -> Vec<Kind> {
    items.iter().map(|v| v.kind()).collect()
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        values_equal_recursive(self, other, &mut None)
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_value(self, f, &mut HashSet::new())
    }
}

fn circular_list_error() -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("circular-list".into()),
        Value::String("Circular list".into()),
    ]))
}

fn values_equal_recursive(
    left: &Value,
    right: &Value,
    seen: &mut Option<HashSet<(usize, usize)>>,
) -> bool {
    match (left.kind(), right.kind()) {
        (Kind::Nil, Kind::Nil) => true,
        (Kind::T, Kind::T) => true,
        (Kind::Integer(a), Kind::Integer(b)) => a == b,
        (Kind::BigInteger(a), Kind::BigInteger(b)) => a == b,
        (Kind::Integer(a), Kind::BigInteger(b)) | (Kind::BigInteger(b), Kind::Integer(a)) => {
            BigInt::from(a) == *b
        }
        // fns.c internal_equal via same_float: representation equality
        // (NaN equals NaN; 0.0 differs from -0.0).
        (Kind::Float(a), Kind::Float(b)) => a.to_bits() == b.to_bits(),
        (Kind::StringObject(a), Kind::StringObject(b)) => a == b,
        (Kind::Symbol(a), Kind::Symbol(b)) => a == b,
        (Kind::Cons(a), Kind::Cons(b)) => {
            if SharedCons::ptr_eq(&a, &b) {
                return true;
            }
            let ids = (ConsCell::identity(&a), ConsCell::identity(&b));
            if !seen.get_or_insert_with(HashSet::new).insert(ids) {
                return true;
            }
            values_equal_recursive(&a.car.get(), &b.car.get(), seen)
                && values_equal_recursive(&a.cdr.get(), &b.cdr.get(), seen)
        }
        (Kind::Vector(a), Kind::Vector(b)) => {
            if a.ptr_eq(&b) {
                return true;
            }
            let ids = (a.identity(), b.identity());
            if !seen.get_or_insert_with(HashSet::new).insert(ids) {
                return true;
            }
            let a = a.slots();
            let b = b.slots();
            a.len() == b.len() && a.zip(b).all(|(a, b)| values_equal_recursive(&a, &b, seen))
        }
        (Kind::BuiltinFunc(a), Kind::BuiltinFunc(b)) => a == b,
        (Kind::NativeFunction(a), Kind::NativeFunction(b)) => a.ptr_eq(&b),
        (Kind::NativeCompUnit(a), Kind::NativeCompUnit(b)) => a.ptr_eq(&b),
        (Kind::Closure(a), Kind::Closure(b)) => {
            if a.ptr_eq(&b)
                || !seen
                    .get_or_insert_with(HashSet::new)
                    .insert((a.identity(), b.identity()))
            {
                return true;
            }
            a.public_len() == b.public_len()
                && a.slots()
                    .zip(b.slots())
                    .all(|(a, b)| values_equal_recursive(&a, &b, seen))
        }
        (Kind::Buffer(a), Kind::Buffer(b)) => a.ptr_eq(&b),
        (Kind::Marker(a), Kind::Marker(b)) => a == b,
        (Kind::Overlay(a), Kind::Overlay(b)) => a == b,
        (Kind::CharTable(a), Kind::CharTable(b)) => {
            if a == b
                || !seen
                    .get_or_insert_with(HashSet::new)
                    .insert((a.identity(), b.identity()))
            {
                return true;
            }
            a.slot_count() == b.slot_count()
                && a.slots()
                    .zip(b.slots())
                    .all(|(a, b)| values_equal_recursive(&a, &b, seen))
        }
        (Kind::SubCharTable(a), Kind::SubCharTable(b)) => {
            if a == b
                || !seen
                    .get_or_insert_with(HashSet::new)
                    .insert((a.identity(), b.identity()))
            {
                return true;
            }
            a.depth() == b.depth()
                && a.min_char() == b.min_char()
                && a.slots()
                    .zip(b.slots())
                    .all(|(a, b)| values_equal_recursive(&a, &b, seen))
        }
        (Kind::Frame(a), Kind::Frame(b)) => a == b,
        (Kind::Terminal(a), Kind::Terminal(b)) => a.ptr_eq(&b),
        (Kind::SymbolWithPos(a), Kind::SymbolWithPos(b)) => a.ptr_eq(&b),
        (Kind::HashTable(a), Kind::HashTable(b)) => a == b,
        (Kind::Record(a), Kind::Record(b)) => a.ptr_eq(&b),
        (Kind::LispRecord(a), Kind::LispRecord(b)) => a.ptr_eq(&b),
        (Kind::Finalizer(a), Kind::Finalizer(b)) => a == b,
        (Kind::ReaderForm(a), Kind::ReaderForm(b)) => a.ptr_eq(&b),
        (Kind::Unbound, Kind::Unbound) => true,
        _ => false,
    }
}

fn format_value(
    value: &Value,
    f: &mut fmt::Formatter<'_>,
    seen: &mut HashSet<usize>,
) -> fmt::Result {
    match value.kind() {
        Kind::Nil => write!(f, "nil"),
        Kind::T => write!(f, "t"),
        Kind::Integer(n) => write!(f, "{}", n),
        Kind::BigInteger(n) => write!(f, "{}", n),
        Kind::Float(v) => write!(f, "{}", format_float(v.get())),
        Kind::StringObject(state) => {
            write!(f, "\"{}\"", state.borrow().text())
        }
        Kind::Symbol(s) => write!(f, "{}", visible_symbol_name(&s)),
        Kind::Vector(vector) => {
            let id = vector.identity();
            if !seen.insert(id) {
                return write!(f, "#<circular-vector>");
            }
            write!(f, "[")?;
            for (index, value) in vector.slots().enumerate() {
                if index != 0 {
                    write!(f, " ")?;
                }
                format_value(&value, f, seen)?;
            }
            seen.remove(&id);
            write!(f, "]")
        }
        Kind::Cons(cell) if matches!(cell.car.get().kind(), Kind::Symbol(head) if head == "vector-literal") =>
        {
            // Vector literals ride on conses internally but print as vectors.
            write!(f, "[")?;
            let mut current = cell.cdr.get();
            let mut first = true;
            while let Kind::Cons(cell) = current.kind() {
                if !first {
                    write!(f, " ")?;
                }
                format_value(&cell.car.get(), f, seen)?;
                first = false;
                current = cell.cdr.get();
            }
            write!(f, "]")
        }
        Kind::Cons(cell) => {
            // GNU prints reader shorthands: (quote X) as 'X and
            // (function X) as #'X.
            if let Kind::Symbol(head) = cell.car.get().kind()
                && (head == "quote" || head == "function")
                && let Kind::Cons(inner) = cell.cdr.get().kind()
                && matches!(inner.cdr.get().kind(), Kind::Nil)
            {
                write!(f, "{}", if head == "quote" { "'" } else { "#'" })?;
                return format_value(&inner.car.get(), f, seen);
            }
            write!(f, "(")?;
            let mut current = *value;
            let mut first = true;
            loop {
                match current.kind() {
                    Kind::Cons(cell) => {
                        let id = ConsCell::identity(&cell);
                        if !seen.insert(id) {
                            if !first {
                                write!(f, " ")?;
                            }
                            write!(f, "#<circular-list>")?;
                            break;
                        }
                        if !first {
                            write!(f, " ")?;
                        }
                        format_value(&cell.car.get(), f, seen)?;
                        first = false;
                        current = cell.cdr.get();
                    }
                    Kind::Nil => break,
                    other => {
                        write!(f, " . ")?;
                        format_value(&other.value(), f, seen)?;
                        break;
                    }
                }
            }
            write!(f, ")")
        }
        Kind::BuiltinFunc(name) => write!(f, "#<builtin {}>", name),
        Kind::NativeFunction(function) => write!(f, "#<subr {}>", function.name()),
        Kind::NativeCompUnit(unit) => write!(f, "#<native-comp-unit {}>", unit.field(0)),
        Kind::Closure(lambda) => write!(f, "#<lambda {}>", lambda.parameters()),
        Kind::Buffer(buffer) => write!(f, "#<buffer {}>", buffer.borrow().name),
        Kind::Marker(id) => write!(f, "#<marker id:{}>", id),
        Kind::Overlay(id) => write!(f, "#<overlay id:{}>", id),
        Kind::HashTable(table) => write!(f, "#<hash-table {:x}>", table.identity()),
        Kind::CharTable(id) => write!(f, "#<char-table {id}>"),
        Kind::SubCharTable(id) => write!(f, "#<sub-char-table {:x}>", id.identity()),
        Kind::Frame(id) => write!(f, "#<frame id:{}>", id.borrow().id),
        Kind::Terminal(terminal) => write!(f, "#<terminal id:{}>", terminal.id),
        Kind::SymbolWithPos(object) => {
            write!(f, "#<symbol {} at {}>", object.symbol(), object.position())
        }
        Kind::Record(record) => write!(f, "#<record id:{}>", record.id),
        Kind::LispRecord(record) => write!(f, "#<record {:x}>", record.identity()),
        // print.c prints a finalizer as `#<finalizer>' with no identity.
        Kind::Finalizer(_) => write!(f, "#<finalizer>"),
        Kind::ReaderForm(_) => write!(f, "#<reader-form>"),
        Kind::Unbound => write!(f, "#<unbound>"),
    }
}

/// An orderly process termination requested by `kill-emacs`.
///
/// This is evaluator control flow, not a Lisp condition: GNU's native
/// `kill-emacs` is `noreturn`, so `condition-case`, `handler-bind`, and
/// `unwind-protect` cannot intercept it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmacsTermination {
    pub exit_code: i32,
    pub restart: bool,
}

/// Lisp errors and non-local evaluator control flow: the kind, boxed in
/// `LispError' so that `Result<Value, LispError>' is two words and comes
/// back in registers (eval_sub returns a `Lisp_Object'; a signal is a
/// non-local exit whose data is off the fast path).
#[derive(Clone, Debug)]
pub enum LispErrorKind {
    /// Type mismatch: expected, got
    TypeError(String, String),
    /// GNU's `(wrong-type-argument PREDICATE VALUE)': the predicate symbol
    /// the failed check names, and the offending value itself.  The older
    /// `TypeError' carried a type *name* instead of the value, which is
    /// visible in every condition datum and error message (finding 57);
    /// construction sites migrate here as their predicates are verified
    /// against the oracle.
    WrongTypeArgument(String, Value),
    /// Unbound variable
    Void(String),
    /// Unbound function cell
    VoidFunction(String),
    /// Wrong number of arguments
    WrongNumberOfArgs(String, usize),
    /// Generic error with a message (like Emacs's `error` function)
    Signal(String),
    /// Generic error with explicit condition payload.
    SignalValue(Value),
    /// An ERT assertion failure.
    ErtTestFailed(String),
    /// Non-local exit via `throw`.
    Throw(Value, Value),
    /// Orderly, non-catchable process termination via `kill-emacs`.
    Terminate(EmacsTermination),
    /// An ERT skip condition.
    TestSkipped(String),
    /// End of input during read
    EndOfInput,
    /// Reader syntax error
    ReadError(String),
}

impl LispErrorKind {
    pub fn condition_type(&self) -> String {
        match self {
            LispErrorKind::TypeError(_, _) => "wrong-type-argument".into(),
            LispErrorKind::WrongTypeArgument(_, _) => "wrong-type-argument".into(),
            LispErrorKind::Void(_) => "void-variable".into(),
            LispErrorKind::VoidFunction(_) => "void-function".into(),
            LispErrorKind::WrongNumberOfArgs(_, _) => "wrong-number-of-arguments".into(),
            LispErrorKind::Signal(_) => "error".into(),
            LispErrorKind::SignalValue(value) => match value.car().map(|v| v.kind()) {
                Ok(Kind::Symbol(symbol)) => symbol.to_string(),
                _ => "error".into(),
            },
            LispErrorKind::ErtTestFailed(_) => "ert-test-failed".into(),
            LispErrorKind::Throw(_, _) => "no-catch".into(),
            LispErrorKind::Terminate(_) => {
                unreachable!("process termination is non-catchable evaluator control flow")
            }
            LispErrorKind::TestSkipped(_) => "ert-test-skipped".into(),
            LispErrorKind::EndOfInput => "end-of-file".into(),
            LispErrorKind::ReadError(_) => "invalid-read-syntax".into(),
        }
    }
}

impl fmt::Display for LispErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LispErrorKind::TypeError(expected, got) => {
                write!(f, "Wrong type argument: {}, {}", expected, got)
            }
            LispErrorKind::WrongTypeArgument(predicate, value) => {
                write!(f, "Wrong type argument: {}, {}", predicate, value)
            }
            LispErrorKind::Void(name) => write!(
                f,
                "Symbol's value as variable is void: {}",
                render_error_symbol_name(name)
            ),
            LispErrorKind::VoidFunction(name) => {
                write!(
                    f,
                    "Symbol's function definition is void: {}",
                    render_error_symbol_name(name)
                )
            }
            LispErrorKind::WrongNumberOfArgs(name, n) => {
                write!(f, "Wrong number of arguments: {}, {}", name, n)
            }
            LispErrorKind::Signal(msg) => write!(f, "{}", msg),
            LispErrorKind::SignalValue(value) => match value.to_vec() {
                Ok(items)
                    if items.len() == 2
                        && matches!(items[0].kind(), Kind::Symbol(kind) if kind == "void-variable") =>
                {
                    // Preserve the host diagnostic when Fsymbol_value
                    // carries its original Lisp object instead of a name.
                    write!(f, "Symbol's value as variable is void: {}", items[1])
                }
                Ok(items)
                    if items.len() >= 2
                        && matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(kind)) if kind == "search-failed") =>
                {
                    match items[1].kind() {
                        Kind::StringObject(object) => {
                            write!(f, "{:?}", object.borrow().text())
                        }
                        value => write!(f, "{value}"),
                    }
                }
                Ok(items)
                    if items.len() >= 4
                        && matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(kind)) if kind == "file-error" || kind == "file-missing") =>
                {
                    let message = match items[1].kind() {
                        Kind::StringObject(text) => text.borrow().text(),
                        _ => return write!(f, "{}", value),
                    };
                    let detail = match items[2].kind() {
                        Kind::StringObject(text) => text.borrow().text(),
                        _ => return write!(f, "{}", value),
                    };
                    let path = match items[3].kind() {
                        Kind::StringObject(text) => text.borrow().text(),
                        _ => return write!(f, "{}", value),
                    };
                    write!(f, "{}: {}, {}", message, detail, path)
                }
                Ok(items) if items.len() >= 2 => match items[1].kind() {
                    Kind::StringObject(object) => {
                        write!(f, "{}", object.borrow().text())
                    }
                    value => write!(f, "{value}"),
                },
                _ => write!(f, "{}", value),
            },
            LispErrorKind::ErtTestFailed(msg) => write!(f, "{}", msg),
            LispErrorKind::Throw(tag, value) => write!(f, "No catch for {}: {}", tag, value),
            LispErrorKind::Terminate(termination) => {
                if termination.restart {
                    write!(
                        f,
                        "Emacs requested restart with exit code {}",
                        termination.exit_code
                    )
                } else {
                    write!(
                        f,
                        "Emacs requested exit with code {}",
                        termination.exit_code
                    )
                }
            }
            LispErrorKind::TestSkipped(msg) => write!(f, "{}", msg),
            LispErrorKind::EndOfInput => write!(f, "End of file during parsing"),
            LispErrorKind::ReadError(msg) => write!(f, "Invalid read syntax: {}", msg),
        }
    }
}

/// A Lisp error: one word (the boxed kind), so that a `Result<Value,
/// LispError>' is two words and comes back in registers, as `eval_sub''s
/// `Lisp_Object' does.  The constructors keep the variants' names; a
/// site that reads the kind asks `kind' (a reference) or `into_kind'.
#[derive(Clone)]
pub struct LispError(Box<LispErrorKind>);

#[allow(non_snake_case)]
impl LispError {
    #[inline]
    pub fn kind(&self) -> &LispErrorKind {
        &self.0
    }

    #[inline]
    pub fn kind_mut(&mut self) -> &mut LispErrorKind {
        &mut self.0
    }

    #[inline]
    pub fn into_kind(self) -> LispErrorKind {
        *self.0
    }

    pub fn condition_type(&self) -> String {
        self.0.condition_type()
    }

    pub fn TypeError(a0: String, a1: String) -> Self {
        Self(Box::new(LispErrorKind::TypeError(a0, a1)))
    }

    pub fn WrongTypeArgument(a0: String, a1: Value) -> Self {
        Self(Box::new(LispErrorKind::WrongTypeArgument(a0, a1)))
    }

    pub fn Void(a0: String) -> Self {
        Self(Box::new(LispErrorKind::Void(a0)))
    }

    pub fn VoidFunction(a0: String) -> Self {
        Self(Box::new(LispErrorKind::VoidFunction(a0)))
    }

    pub fn WrongNumberOfArgs(a0: String, a1: usize) -> Self {
        Self(Box::new(LispErrorKind::WrongNumberOfArgs(a0, a1)))
    }

    pub fn Signal(a0: String) -> Self {
        Self(Box::new(LispErrorKind::Signal(a0)))
    }

    pub fn SignalValue(a0: Value) -> Self {
        Self(Box::new(LispErrorKind::SignalValue(a0)))
    }

    pub fn ErtTestFailed(a0: String) -> Self {
        Self(Box::new(LispErrorKind::ErtTestFailed(a0)))
    }

    pub fn Throw(a0: Value, a1: Value) -> Self {
        Self(Box::new(LispErrorKind::Throw(a0, a1)))
    }

    pub fn Terminate(a0: EmacsTermination) -> Self {
        Self(Box::new(LispErrorKind::Terminate(a0)))
    }

    pub fn TestSkipped(a0: String) -> Self {
        Self(Box::new(LispErrorKind::TestSkipped(a0)))
    }

    pub fn EndOfInput() -> Self {
        Self(Box::new(LispErrorKind::EndOfInput))
    }

    pub fn ReadError(a0: String) -> Self {
        Self(Box::new(LispErrorKind::ReadError(a0)))
    }
}

impl From<LispErrorKind> for LispError {
    fn from(kind: LispErrorKind) -> Self {
        Self(Box::new(kind))
    }
}

impl fmt::Debug for LispError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.0, f)
    }
}

impl fmt::Display for LispError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&*self.0, f)
    }
}

impl std::error::Error for LispError {}

impl From<crate::buffer::BufferError> for LispError {
    fn from(e: crate::buffer::BufferError) -> Self {
        // cmds.c signals the boundary conditions with `xsignal0': the
        // error object is `(beginning-of-buffer)' with nil data, so
        // condition-case handlers on those symbols can catch it (the
        // message comes from the condition's `error-message' property).
        match e {
            crate::buffer::BufferError::BeginningOfBuffer => {
                LispError::SignalValue(Value::list([Value::Symbol("beginning-of-buffer".into())]))
            }
            crate::buffer::BufferError::EndOfBuffer => {
                LispError::SignalValue(Value::list([Value::Symbol("end-of-buffer".into())]))
            }
            other => LispError::Signal(other.to_string()),
        }
    }
}

/// Depth- and length-bounded rendering of a LispError for host-side trace
/// lines.  The derived Debug impl recurses the full payload graph, which is
/// unbounded and cycle-blind; trace output must never be able to kill the
/// process that produces it.
pub(crate) fn bounded_error_debug(error: &LispError) -> String {
    fn render(value: &Value, depth: usize, out: &mut String) {
        if out.len() > 2048 {
            out.push('…');
            return;
        }
        if depth == 0 {
            out.push('…');
            return;
        }
        match value.kind() {
            Kind::Cons(cell) => {
                out.push('(');
                let mut cursor = Value::Cons(cell);
                let mut emitted = 0;
                while let Kind::Cons(cell) = cursor.kind() {
                    if emitted >= 8 || out.len() > 2048 {
                        out.push_str(" …");
                        break;
                    }
                    if emitted > 0 {
                        out.push(' ');
                    }
                    render(&cell.car.get().clone(), depth - 1, out);
                    emitted += 1;
                    let next = cell.cdr.get();
                    match next.kind() {
                        Kind::Nil => break,
                        Kind::Cons(_) => cursor = next,
                        other => {
                            out.push_str(" . ");
                            render(&other.value(), depth - 1, out);
                            break;
                        }
                    }
                }
                out.push(')');
            }
            Kind::StringObject(state) => {
                let text: String = state.borrow().text();
                let mut brief: String = text.chars().take(48).collect();
                if brief.len() < text.len() {
                    brief.push('…');
                }
                out.push('"');
                out.push_str(&brief);
                out.push('"');
            }
            other => {
                let _ = std::fmt::Write::write_fmt(out, format_args!("{other}"));
            }
        }
    }
    match error.kind() {
        LispErrorKind::Signal(message) => format!("Signal({message:?})"),
        LispErrorKind::SignalValue(value) => {
            let mut out = String::from("SignalValue(");
            render(value, 6, &mut out);
            out.push(')');
            out
        }
        LispErrorKind::Throw(tag, value) => {
            let mut out = String::from("Throw(");
            render(tag, 3, &mut out);
            out.push_str(", ");
            render(value, 4, &mut out);
            out.push(')');
            out
        }
        LispErrorKind::WrongTypeArgument(predicate, value) => {
            let mut out = format!("WrongTypeArgument({predicate}, ");
            render(value, 4, &mut out);
            out.push(')');
            out
        }
        other => {
            let text = format!("{other}");
            let mut brief: String = text.chars().take(256).collect();
            if brief.len() < text.len() {
                brief.push('…');
            }
            brief
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EnvFrame, Kind, LispError, SharedCons, StringObjectRef, SymbolName, Value, assq_binding,
        census_live_conses, census_live_floats, census_live_vectors, environment_declares_special,
        make_uninterned_symbol_name,
    };

    #[test]
    fn result_of_value_is_two_machine_words() {
        assert_eq!(
            std::mem::size_of::<Result<Value, LispError>>(),
            2 * std::mem::size_of::<usize>(),
            "eval_sub's result comes back in registers: the word, and the boxed signal",
        );
    }

    #[test]
    fn value_is_one_machine_word() {
        // This resolves only while Value is neither Send nor Sync. Giving
        // the word either trait introduces another matching implementation
        // and makes the inference ambiguous at compile time.
        trait LocalWord<A> {
            fn check() {}
        }
        struct IfSend;
        struct IfSync;
        impl<T> LocalWord<()> for T {}
        impl<T: Send> LocalWord<IfSend> for T {}
        impl<T: Sync> LocalWord<IfSync> for T {}
        <Value as LocalWord<_>>::check();
        assert_eq!(
            std::mem::size_of::<Value>(),
            std::mem::size_of::<usize>(),
            "lisp.h's Lisp_Object is one tagged word; every slot, stack cell and register copy depends on it",
        );
    }

    #[test]
    fn environment_frames_are_the_alist_head_flet_conses() {
        // Flet: `lexenv = Fcons (Fcons (var, tem), lexenv)' in varlist
        // order, so the last binding is the first entry and a repeated
        // name resolves to its later binding (the oracle's `(let ((x 1)
        // (x 2)) x)' is 2).
        let x: SymbolName = "cell".into();
        let outer = EnvFrame::lexical();
        let frame = EnvFrame::bindings(
            [(x, Value::Integer(1)), (x, Value::Integer(2))],
            outer.environment(),
        );
        assert_eq!(
            std::mem::size_of::<EnvFrame>(),
            std::mem::size_of::<Value>(),
            "a frame is the environment word"
        );
        let binding = assq_binding(frame.environment(), &x)
            .expect("proper alist")
            .expect("bound");
        assert_eq!(binding.cdr.get(), Value::Integer(2));
        // The head shares the outer scope's cells: the tail of the frame
        // is the outer environment itself.
        let mut tail = *frame.environment();
        for _ in 0..2 {
            tail = tail.cdr().expect("cons");
        }
        assert!(
            matches!(tail.kind(), Kind::Cons(cell) if matches!(outer.environment().kind(), Kind::Cons(o) if SharedCons::ptr_eq(&cell, &o)))
        );
        assert!(!environment_declares_special(frame.environment(), "cell"));
    }

    #[test]
    fn vector_slots_observe_lisp_mutation_and_gc_during_iteration() {
        let mut interpreter = crate::lisp::eval::Interpreter::new();
        let mut environment = super::Env::new();
        let value = Value::vector([Value::Integer(19), Value::Nil, Value::Nil]);
        let Kind::Vector(vector) = value.kind() else {
            unreachable!("constructed vector")
        };
        let mut slots = vector.slots();
        assert_eq!(slots.next(), Some(Value::Integer(19)));

        // Keep the slot iterator alive across Lisp stores and a collection.
        // It must read the authoritative words, and may not hold &Value or
        // &mut Value references that forbid the intervening mutations.
        crate::lisp::primitives::call(
            &mut interpreter,
            "aset",
            &[value, Value::Integer(1), Value::list([Value::Integer(73)])],
            &mut environment,
        )
        .expect("store a new Lisp object");
        crate::lisp::primitives::call(
            &mut interpreter,
            "aset",
            &[value, Value::Integer(2), value],
            &mut environment,
        )
        .expect("create a shared cycle");
        crate::lisp::primitives::call(&mut interpreter, "garbage-collect", &[], &mut environment)
            .expect("collect while the iterator remains live");
        assert_eq!(slots.next().expect("updated slot").to_string(), "(73)");
        assert_eq!(slots.next_back().expect("cycle").word(), value.word());
        assert!(slots.next().is_none());
    }

    #[test]
    fn vector_gnu_payload_offsets_survive_collection_and_direct_stores() {
        use std::cell::Cell;

        let mut interpreter = crate::lisp::eval::Interpreter::new();
        let mut environment = super::Env::new();
        let lengths = [0, 1, 2, 3, 63, 251, 252, 511, 4097];
        let mut roots = crate::lisp::alloc::RootedVec::new();
        // Include the small/large allocation boundary and enough objects
        // for several blocks. A root buffer keeps every cyclic vector live.
        for index in 0..270 {
            let len = lengths[index % lengths.len()];
            let value = Value::vector(std::iter::repeat_n(Value::Integer(index as i64), len));
            let Kind::Vector(vector) = value.kind() else {
                unreachable!("constructed vector")
            };
            // lisp.h:Lisp_Vector has one size word, immediately followed
            // by Lisp_Object slots. Exercise that ABI independently of get.
            let header = vector.identity() as *const usize;
            // SAFETY: a live vector's initialized, immutable size word.
            assert_eq!(unsafe { header.read() }, len);
            if len != 0 {
                // SAFETY: the first payload word is Cell<Value>, whose
                // representation is Cell<usize>. No exclusive borrow escapes.
                unsafe { &*header.add(1).cast::<Cell<usize>>() }.set(value.word());
                assert!(vector.get(0).expect("first slot").eq_value(value));
            }
            roots.push(value);
        }

        for _ in 0..3 {
            crate::lisp::alloc::clobber_stack();
            crate::lisp::primitives::call(
                &mut interpreter,
                "garbage-collect",
                &[],
                &mut environment,
            )
            .expect("collect vectors across blocks and sizes");
            for (index, value) in roots.iter().enumerate() {
                let Kind::Vector(vector) = value.kind() else {
                    unreachable!("retained vector")
                };
                let len = lengths[index % lengths.len()];
                assert_eq!(vector.len(), len);
                if len != 0 {
                    assert!(vector.get(0).expect("cycle").eq_value(*value));
                }
                if len > 1 {
                    assert_eq!(vector.get(1), Some(Value::Integer(index as i64)));
                    let replacement = Value::float(index as f64 + 0.5);
                    vector.set(len - 1, replacement);
                    // SAFETY: this is the last initialized Lisp payload
                    // word, after one header word at the GNU ABI offset.
                    let last = unsafe { &*(vector.identity() as *const Cell<usize>).add(len) };
                    assert_eq!(last.get(), replacement.word());
                    // Restore the test's second slot when it is also last.
                    vector.set(len - 1, Value::Integer(index as i64));
                }
            }
        }
    }

    #[test]
    fn cloning_string_reuses_the_text_allocation() {
        let value = Value::string("shared text");
        let clone = value;
        let (Kind::StringObject(text), Kind::StringObject(cloned_text)) =
            (value.kind(), clone.kind())
        else {
            unreachable!("constructed string values")
        };

        assert!(text.ptr_eq(&cloned_text));
    }

    #[test]
    fn string_value_equality_uses_bytes_and_character_counts() {
        let from_char = |code| {
            Value::StringObject(
                StringObjectRef::from_characters(&[Value::Integer(code)])
                    .expect("valid Lisp character"),
            )
        };
        // These are the storage distinctions in the ordinary GNU string
        // comparison fixture. The Rust Value API must preserve them too.
        let unibyte = Value::StringObject(StringObjectRef::from_unibyte(vec![0x80]));
        let byte8 = from_char(0x3fff80);
        let private_use = from_char(0xe080);
        assert_ne!(unibyte, byte8);
        assert_ne!(byte8, private_use);
        assert_ne!(unibyte, private_use);

        // The bytes alone are insufficient: these same two bytes represent
        // two unibyte characters or one multibyte character (GNU SCHARS).
        let two_characters = Value::StringObject(StringObjectRef::from_unibyte(vec![0xc2, 0x80]));
        assert_ne!(two_characters, from_char(0x80));

        let ascii = Value::string("x");
        let multibyte_ascii = Value::StringObject(
            StringObjectRef::repeated_character(u32::from(b'x'), 1, true)
                .expect("valid ASCII character"),
        );
        assert_eq!(ascii, multibyte_ascii);
        assert_eq!(byte8, from_char(0x3fff80));
        assert_eq!(byte8, byte8);
    }

    #[test]
    fn deep_cons_chains_allocate_and_release_on_a_small_stack() {
        // A chain deep along either word costs no stack to allocate or to
        // let go of: the cells are the collector's (alloc.c's blocks), so
        // dropping the handle recurses into nothing.  The sweep, which
        // walks the blocks, is exercised by the evaluator's collection
        // test.
        std::thread::Builder::new()
            .stack_size(256 * 1024)
            .spawn(|| {
                for along_car in [false, true] {
                    let before = census_live_conses();
                    let mut root = Value::Nil;
                    for _ in 0..100_000 {
                        root = if along_car {
                            Value::cons(root, Value::Nil)
                        } else {
                            Value::cons(Value::Nil, root)
                        };
                    }
                    assert!(census_live_conses() >= before + 100_000);
                    let _ = root;
                }
            })
            .expect("small-stack worker")
            .join()
            .expect("deep cons chains complete without stack overflow");
    }

    #[test]
    fn live_census_counts_one_gnu_vector_without_representation_conses() {
        let conses_before = census_live_conses();
        let vectors_before = census_live_vectors();
        let vector = Value::list([
            Value::symbol("vector-literal"),
            Value::Integer(1),
            Value::Integer(2),
        ]);
        let vectors_after = census_live_vectors();

        assert_eq!(census_live_conses(), conses_before);
        assert_eq!(vectors_after.count, vectors_before.count + 1);
        assert_eq!(vectors_after.slots, vectors_before.slots + 3);
        assert_eq!(
            vectors_after.representation_conses,
            vectors_before.representation_conses
        );

        let _ = vector;
        // A vector is freed by the sweep, not by the drop of a handle.
        let vectors_after_drop = census_live_vectors();
        assert_eq!(vectors_after_drop.count, vectors_before.count + 1);
        assert_eq!(vectors_after_drop.slots, vectors_before.slots + 3);
        assert_eq!(
            vectors_after_drop.representation_conses,
            vectors_before.representation_conses
        );
    }

    #[test]
    fn live_census_counts_float_allocations_once_across_clones() {
        let before = census_live_floats();
        let value = Value::float(1.5);
        let clone = value;
        assert_eq!(census_live_floats(), before + 1);
        let _ = value;
        assert_eq!(census_live_floats(), before + 1);
        let _ = clone;
        // A float is freed by the sweep, not by the drop of a handle
        // (`collection_frees_unreached_conses_and_expires_weak_slots').
        assert_eq!(census_live_floats(), before + 1);
    }

    #[test]
    fn live_census_counts_bignums_and_interpreted_closures_as_gnu_vectors() {
        let before = census_live_vectors();
        let integer = Value::big_integer(num_bigint::BigInt::from(1_u8) << 128);
        let closure = Value::lambda(Vec::new(), vec![Value::Nil], Value::Nil);
        let after = census_live_vectors();

        assert_eq!(after.count, before.count + 2);
        // Lisp_Bignum is three words.  A noninteractive interpreted closure
        // has three visible slots plus its one-word vector header.
        assert_eq!(after.slots, before.slots + 3 + 4);

        let _ = integer;
        let _ = closure;
        // Both are freed by the sweep, not by the drop of a handle.
        let after_drop = census_live_vectors();
        assert_eq!(after_drop.count, before.count + 2);
        assert_eq!(after_drop.slots, before.slots + 3 + 4);
    }

    #[test]
    fn shared_text_equality_covers_shared_and_distinct_equal_allocations() {
        use std::hash::{Hash, Hasher};

        let shared = super::SharedText::from("same text");
        let clone = shared;
        let distinct = super::SharedText::from("same text");
        let different = super::SharedText::from("different text");

        assert!(shared.ptr_eq(&clone));
        assert!(!shared.ptr_eq(&distinct));
        assert_eq!(shared, clone);
        assert_eq!(shared, distinct);
        assert_ne!(shared, different);

        let hash = |value: &super::SharedText| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            value.hash(&mut hasher);
            hasher.finish()
        };
        assert_eq!(hash(&shared), hash(&distinct));
    }

    #[test]
    fn cloning_big_integer_reuses_the_integer_allocation() {
        let value = Value::big_integer(num_bigint::BigInt::from(1_u8) << 256);
        let clone = value;
        let (Kind::BigInteger(integer), Kind::BigInteger(cloned_integer)) =
            (value.kind(), clone.kind())
        else {
            unreachable!("constructed big integer values")
        };

        assert!(integer.ptr_eq(&cloned_integer));
    }

    #[test]
    fn interned_symbol_names_reuse_one_text_allocation() {
        let first = SymbolName::from("emaxx-compact-symbol-test");
        let second = SymbolName::from("emaxx-compact-symbol-test");

        assert!(first.0.ptr_eq(&second.0));
    }

    #[test]
    fn uninterned_symbol_names_remain_reclaimable() {
        // An uninterned symbol nothing names is freed by the sweep (not by
        // the drop of a handle), and its entry in the book goes with it.
        // The stack is scanned conservatively: the symbol is made out of
        // this frame and the frames below are clobbered before the
        // collection.
        #[inline(never)]
        fn make(text: &str) {
            let name = SymbolName::from(text.to_owned());
            assert!(SymbolName::intern_str(text).0.ptr_eq(&name.0));
        }
        let mut interp = crate::lisp::eval::Interpreter::new();
        let mut env = super::Env::new();
        let text = make_uninterned_symbol_name("temporary", 1);
        make(&text);
        assert!(SymbolName::live_uninterned(&text).is_some());
        crate::lisp::alloc::clobber_stack();
        crate::lisp::primitives::call(&mut interp, "garbage-collect", &[], &mut env)
            .expect("collect");
        assert!(
            SymbolName::live_uninterned(&text).is_none(),
            "the unreached uninterned symbol is swept and leaves the book"
        );
    }

    #[test]
    fn uninterned_symbol_keeps_its_supplied_lisp_name() {
        let name = Value::string("temporary");
        let Kind::StringObject(expected) = name.kind() else {
            unreachable!("constructed string")
        };
        let symbol = SymbolName::make_uninterned(name, "temporary", 1);
        let Kind::StringObject(actual) = symbol.lisp_name().kind() else {
            unreachable!("immutable supplied name")
        };

        assert!(actual.ptr_eq(&expected));
    }

    #[test]
    fn symbol_ids_resolve_across_serialized_host_threads_and_expire_with_the_object() {
        const INTERNED: &str = "runtime-symbol-id-thread-probe";
        let text = make_uninterned_symbol_name("runtime-symbol-id-weak-probe", 317);
        let first_text = text.clone();
        let (interned, uninterned) = std::thread::spawn(move || {
            crate::lisp::runtime::with_runtime(|| {
                let interned = SymbolName::intern_str(INTERNED);
                let uninterned = SymbolName::intern_str(&first_text);
                (interned.id(), uninterned.id())
            })
        })
        .join()
        .expect("first serialized host entry");

        let second_text = text.clone();
        std::thread::spawn(move || {
            crate::lisp::runtime::with_runtime(|| {
                assert_eq!(SymbolName::id_of(INTERNED), Some(interned));
                assert_eq!(SymbolName::intern_str(INTERNED).id(), interned);
                assert_eq!(SymbolName::id_of(&second_text), Some(uninterned));
                assert_eq!(SymbolName::intern_str(&second_text).id(), uninterned);
            });
        })
        .join()
        .expect("lookup uses the allocated objects on another OS thread");

        // Both creator frames and their OS threads have ended. Only the weak
        // name lookup remains; it must not retain the uninterned allocation.
        let collected_text = text.clone();
        std::thread::spawn(move || {
            crate::lisp::runtime::with_runtime(|| {
                let mut interp = crate::lisp::eval::Interpreter::new();
                let mut env = super::Env::new();
                crate::lisp::primitives::call(&mut interp, "garbage-collect", &[], &mut env)
                    .expect("collect after both creator threads have exited");
                assert_eq!(SymbolName::id_of(INTERNED), Some(interned));
                assert_eq!(SymbolName::id_of(&collected_text), None);
                assert!(SymbolName::live_uninterned(&collected_text).is_none());
                let replacement = SymbolName::intern_str(&collected_text);
                assert_ne!(replacement.id(), uninterned);
                assert_eq!(SymbolName::id_of(&collected_text), Some(replacement.id()));
            });
        })
        .join()
        .expect("weak entry retires before the symbol allocation is reused");
    }

    #[test]
    fn nil_and_t_count_as_symbols() {
        assert!(Value::Nil.is_symbol());
        assert!(Value::T.is_symbol());
        assert_eq!(Value::Nil.as_symbol().expect("nil is a symbol"), "nil");
        assert_eq!(Value::T.as_symbol().expect("t is a symbol"), "t");
    }

    #[test]
    fn cloning_lambda_shares_argument_and_body_lists() {
        let lambda = Value::lambda(vec!["value".into()], Vec::new(), Value::Nil);
        let clone = lambda;

        let (Kind::Closure(lambda), Kind::Closure(cloned_lambda)) = (lambda.kind(), clone.kind())
        else {
            unreachable!("constructed lambda values")
        };
        assert!(lambda.ptr_eq(&cloned_lambda));
        assert_eq!(
            lambda.parameters().word(),
            cloned_lambda.parameters().word()
        );
        assert_eq!(lambda.body().word(), cloned_lambda.body().word());
    }

    #[test]
    fn cloning_buffer_reuses_the_buffer_descriptor() {
        let buffer = Value::buffer(7, "shared buffer");
        let clone = buffer;
        let (Kind::Buffer(buffer), Kind::Buffer(cloned_buffer)) = (buffer.kind(), clone.kind())
        else {
            unreachable!("constructed buffer values")
        };

        assert!(buffer.ptr_eq(&cloned_buffer));
    }

    #[test]
    fn retained_cons_fields_use_one_word_and_preserve_slot_identity() {
        assert_eq!(
            std::mem::size_of::<super::ConsSlot>(),
            std::mem::size_of::<usize>(),
            "a retained car/cdr field needs only its actual field address",
        );
        let pair = Value::cons(Value::Integer(13), Value::Integer(29));
        let (car, cdr) = pair.cons_cells().expect("cons");
        let weak_car = car.downgrade();
        let weak_cdr = cdr.downgrade();
        assert_eq!(car.cell_id(), pair.cons_id().expect("cell identity"));
        assert_eq!(car.cell_id(), cdr.cell_id());
        assert!(car.ptr_eq(&weak_car.upgrade().expect("live car")));
        assert!(cdr.ptr_eq(&weak_cdr.upgrade().expect("live cdr")));
        assert!(!car.ptr_eq(&cdr));
        car.set(pair);
        cdr.set(Value::Integer(53));
        let values = pair.cons_values().expect("shared fields");
        assert_eq!(values.0.word(), pair.word());
        assert_eq!(values.1, Value::Integer(53));
        assert_eq!(
            weak_car.upgrade().expect("same car").get().word(),
            pair.word()
        );
        assert_eq!(
            weak_cdr.upgrade().expect("same cdr").get(),
            Value::Integer(53)
        );
        car.set(Value::Nil);
    }

    #[test]
    fn cons_fields_share_one_cell_and_mutate_independently() {
        assert_eq!(
            std::mem::size_of::<SharedCons>(),
            std::mem::size_of::<usize>(),
            "a Value::Cons must retain exactly one shared pointer",
        );

        let pair = Value::cons(Value::Integer(1), Value::Integer(2));
        let clone = pair;
        let (car, cdr) = pair.cons_cells().expect("constructed cons");
        let (cloned_car, cloned_cdr) = clone.cons_cells().expect("cloned cons");

        assert_eq!(car.cell_id(), cdr.cell_id());
        assert!(!car.ptr_eq(&cdr), "car and cdr are distinct field handles");
        assert!(car.ptr_eq(&cloned_car));
        assert!(cdr.ptr_eq(&cloned_cdr));

        car.set(Value::Integer(3));
        cdr.set(Value::Integer(4));

        assert_eq!(clone.car().expect("car"), Value::Integer(3));
        assert_eq!(clone.cdr().expect("cdr"), Value::Integer(4));
    }

    #[test]
    fn every_cons_field_mutation_advances_the_shared_epoch() {
        // Historical selector retained: current words, not a write epoch,
        // now validate each field's derived dependencies.
        let pair = Value::cons(Value::Integer(1), Value::Integer(2));
        let before_car = super::ConsMutationSnapshot::list_spine(&pair);
        pair.set_car(Value::Integer(3)).expect("set car");
        assert!(!before_car.is_current());
        let before_cdr = super::ConsMutationSnapshot::list_spine(&pair);
        let (_, cdr) = pair.cons_cells().expect("cons");
        cdr.set(Value::Integer(4));
        assert!(!before_cdr.is_current());
    }

    #[test]
    fn cons_mutation_snapshot_ignores_unrelated_cells_and_tracks_dependencies() {
        let source = Value::list([Value::symbol("+"), Value::Integer(1)]);
        let unrelated = Value::list([Value::symbol("data"), Value::Integer(2)]);
        let snapshot = super::ConsMutationSnapshot::list_spine(&source);

        unrelated
            .set_cdr(Value::list([Value::Integer(3)]))
            .expect("unrelated value is a cons");
        assert!(snapshot.is_current());

        source
            .cdr()
            .expect("source has an argument spine")
            .set_car(Value::Integer(4))
            .expect("source argument spine is a cons");
        assert!(!snapshot.is_current());
    }

    #[test]
    fn watcher_compaction_preserves_live_mutation_subscriptions() {
        // Historical selector: dropping another snapshot must not discard
        // a live dependency. There is no longer a global watcher table.
        let source = Value::cons(Value::Integer(1), Value::Nil);
        let snapshot = super::ConsMutationSnapshot::list_spine(&source);
        let other = snapshot.clone();
        drop(snapshot);
        for _ in 0..1_024 {
            drop(super::ConsMutationSnapshot::list_spine(&source));
        }
        assert!(other.is_current());
        source
            .set_car(Value::Integer(2))
            .expect("mutate dependency");
        assert!(!other.is_current());
    }

    #[test]
    fn cons_mutation_bloom_collisions_only_probe_the_authoritative_watcher_map() {
        // Historical selector: exact word dependencies replace the Bloom
        // filter. Native stores need no prior crossing or notification.
        let source = Value::cons(Value::Integer(1), Value::Nil);
        let other = Value::cons(Value::Integer(3), Value::Nil);
        let snapshot = super::ConsMutationSnapshot::list_spine(&source);
        unsafe {
            *((other.word() & !7) as *mut usize) = Value::Integer(7).word();
        }
        assert!(snapshot.is_current());
        unsafe {
            *((source.word() & !7) as *mut usize) = Value::Integer(9).word();
        }
        assert!(!snapshot.is_current());
    }

    #[test]
    fn cons_mutation_bloom_resets_after_the_last_dead_watcher_is_drained() {
        // Historical selector: snapshot disposal leaves no registration on
        // a cell, and a fresh snapshot describes the current fields.
        let source = Value::cons(Value::Integer(1), Value::Integer(2));
        let snapshot = super::ConsMutationSnapshot::list_spine(&source);
        drop(snapshot);
        source.set_car(Value::Integer(3)).expect("cons car");
        source.set_cdr(Value::Integer(4)).expect("cons cdr");
        let fresh = super::ConsMutationSnapshot::list_spine(&source);
        assert!(fresh.is_current());
        source.set_cdr(Value::Nil).expect("later store");
        assert!(!fresh.is_current());
    }

    #[test]
    fn tree_mutation_snapshot_tracks_nested_cons_fields() {
        let nested = Value::list([Value::symbol("inner"), Value::Integer(1)]);
        let source = Value::list([Value::symbol("outer"), nested]);
        let spine_snapshot = super::ConsMutationSnapshot::list_spine(&source);
        let tree_snapshot = super::ConsMutationSnapshot::tree(&source);

        nested
            .set_car(Value::symbol("changed"))
            .expect("nested value is a cons");
        assert!(spine_snapshot.is_current());
        assert!(!tree_snapshot.is_current());
    }

    #[test]
    fn void_function_errors_print_function_symbols_readably() {
        assert_eq!(
            LispError::VoidFunction("not-defined".into()).to_string(),
            "Symbol's function definition is void: not-defined"
        );
        assert_eq!(
            LispError::VoidFunction("(setf gv-test-foo)".into()).to_string(),
            r"Symbol's function definition is void: \(setf\ gv-test-foo\)"
        );
    }

    #[test]
    fn signaled_string_messages_print_without_lisp_quotes() {
        assert_eq!(
            LispError::SignalValue(Value::list([
                Value::Symbol("error".into()),
                Value::String("Boo".into()),
            ]))
            .to_string(),
            "Boo"
        );
    }

    #[test]
    fn integral_floats_print_with_one_fractional_digit() {
        assert_eq!(Value::float(1.0).to_string(), "1.0");
        assert_eq!(Value::float(-10.0).to_string(), "-10.0");
        assert_eq!(Value::float(1.25).to_string(), "1.25");
    }

    #[test]
    fn float_values_preserve_lisp_object_identity() {
        let original = Value::float(f64::NAN);
        let shared = original;
        let distinct = Value::float(f64::NAN);
        let Kind::Float(original) = original.kind() else {
            unreachable!();
        };
        let Kind::Float(shared) = shared.kind() else {
            unreachable!();
        };
        let Kind::Float(distinct) = distinct.kind() else {
            unreachable!();
        };
        assert!(original.ptr_eq(&shared));
        assert!(!original.ptr_eq(&distinct));
        assert_eq!(original.to_bits(), distinct.to_bits());
    }
}

#[cfg(test)]
mod ownership_tests;
