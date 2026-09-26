#![allow(dead_code)]

pub use crate::lisp::native_comp::abi::BuiltinRef;
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use std::fmt;
use std::{
    borrow::Borrow,
    cell::{Cell, RefCell, UnsafeCell},
    collections::{HashMap, HashSet},
    hash::{BuildHasherDefault, Hasher},
    iter::FromIterator,
    ops::Deref,
    path::Path,
    rc::{Rc, Weak},
};

const UNINTERNED_SYMBOL_MARKER: &str = "\u{1F}";
/// The marker as a character: `str::contains' with a one-character pattern
/// scans with memchr, where the string pattern walked the bytes one by one
/// on every name-to-id resolution (a tenth of a tight interpreted loop).
const UNINTERNED_SYMBOL_MARKER_CHAR: char = '\u{1F}';
const OBARRAY_SYMBOL_MARKER: &str = "\u{1E}";
const OBARRAY_SYMBOL_MARKER_CHAR: char = '\u{1E}';

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ConsMutationEpoch(u64);

const CONS_MUTATION_WATCH_MINIMUM_KEY_LIMIT: usize = 1 << 20;

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

#[derive(Debug, Default)]
pub(crate) struct ConsMutationQueue {
    dirty: RefCell<HashSet<usize, IdentityBuildHasher>>,
    native_heap: Cell<*mut std::ffi::c_void>,
}

impl ConsMutationQueue {
    pub(crate) fn set_native_heap_owner(&self, owner: *mut std::ffi::c_void) {
        self.native_heap.set(owner);
    }

    pub(crate) fn dirty_keys(&self) -> Vec<usize> {
        self.dirty.borrow().iter().copied().collect()
    }

    fn insert(&self, key: usize) {
        self.dirty.borrow_mut().insert(key);
    }

    pub(crate) fn contains(&self, key: usize) -> bool {
        self.dirty.borrow().contains(&key)
    }

    pub(crate) fn remove(&self, key: usize) {
        self.dirty.borrow_mut().remove(&key);
    }
}

#[derive(Debug)]
struct ConsMutationWatch {
    valid: Cell<bool>,
}

thread_local! {
    static NATIVE_CONS_MUTATION_QUEUES: RefCell<IdentityMap<Weak<ConsMutationQueue>>> =
        RefCell::new(IdentityMap::default());
}

type IdentityMap<T> = HashMap<usize, T, IdentityBuildHasher>;

/// The existing registration ties a canonical cons to its live native heap.
/// Keep the owner once per queue, without enlarging every Lisp cons.
pub(crate) fn native_cons_heap_owner(address: usize) -> *mut std::ffi::c_void {
    NATIVE_CONS_MUTATION_QUEUES.with_borrow(|queues| {
        queues
            .get(&address)
            .and_then(Weak::upgrade)
            .map_or(std::ptr::null_mut(), |queue| queue.native_heap.get())
    })
}

#[derive(Debug)]
pub(crate) struct NativeConsMutationRegistration {
    key: usize,
    queue: Weak<ConsMutationQueue>,
}

impl NativeConsMutationRegistration {
    pub(crate) fn new(key: usize, queue: &Rc<ConsMutationQueue>) -> Self {
        let queue = Rc::downgrade(queue);
        NATIVE_CONS_MUTATION_QUEUES.with_borrow_mut(|queues| {
            queues.insert(key, queue.clone());
        });
        Self { key, queue }
    }

    pub(crate) fn is_current(&self) -> bool {
        self.queue
            .upgrade()
            .is_none_or(|queue| !queue.contains(self.key))
    }

    pub(crate) fn mark_current(&self) {
        if let Some(queue) = self.queue.upgrade() {
            queue.remove(self.key);
        }
    }
}

impl Drop for NativeConsMutationRegistration {
    fn drop(&mut self) {
        if let Some(queue) = self.queue.upgrade() {
            queue.remove(self.key);
        }
        NATIVE_CONS_MUTATION_QUEUES.with_borrow_mut(|queues| {
            if queues
                .get(&self.key)
                .is_some_and(|queue| Weak::ptr_eq(queue, &self.queue))
            {
                queues.remove(&self.key);
            }
        });
    }
}

fn note_native_cons_mutation(key: usize) {
    NATIVE_CONS_MUTATION_QUEUES.with_borrow_mut(|queues| {
        let Some(queue) = queues.get(&key) else {
            return;
        };
        let Some(queue) = queue.upgrade() else {
            queues.remove(&key);
            return;
        };
        queue.insert(key);
    });
}

type ConsMutationWatchers = HashMap<usize, Vec<Weak<ConsMutationWatch>>, IdentityBuildHasher>;

/// 256 Kibit Bloom filter over watched field addresses, allocated on first
/// registration.  Mutation of an unwatched field is by far the common case
/// (every `aset', `setcar', and buffer-local write lands here), so the
/// watcher-map probe must cost nothing for fields no cache depends on.
/// Stale bits from dead watchers only cause harmless extra probes; the
/// filter resets whenever the watcher map is observed empty and is rebuilt
/// when dead watcher keys are compacted.
const CONS_MUTATION_BLOOM_WORDS: usize = 4096;

type ConsMutationBloom = Option<Box<[u64; CONS_MUTATION_BLOOM_WORDS]>>;

fn cons_mutation_bloom_slot(field_id: usize) -> (usize, u64) {
    let mixed = (field_id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let bit = (mixed >> 46) as usize;
    (bit >> 6, 1u64 << (bit & 63))
}

thread_local! {
    static CONS_MUTATION_EPOCH: Cell<ConsMutationEpoch> =
        const { Cell::new(ConsMutationEpoch(0)) };
    static CONS_MUTATION_WATCHERS: RefCell<ConsMutationWatchers> =
        RefCell::new(ConsMutationWatchers::default());
    static CONS_MUTATION_WATCH_BLOOM: RefCell<ConsMutationBloom> = const { RefCell::new(None) };
    static CONS_MUTATION_WATCH_NEXT_KEY_LIMIT: Cell<usize> =
        const { Cell::new(CONS_MUTATION_WATCH_MINIMUM_KEY_LIMIT) };
}

pub(crate) fn cons_mutation_epoch() -> ConsMutationEpoch {
    CONS_MUTATION_EPOCH.get()
}

fn note_cons_mutation(field_id: usize) {
    let current = cons_mutation_epoch();
    CONS_MUTATION_EPOCH.set(ConsMutationEpoch(current.0.wrapping_add(1)));
    let watched = CONS_MUTATION_WATCH_BLOOM.with_borrow(|bloom| {
        bloom.as_ref().is_some_and(|bloom| {
            let (word, bit) = cons_mutation_bloom_slot(field_id);
            bloom[word] & bit != 0
        })
    });
    if !watched {
        return;
    }
    let emptied = CONS_MUTATION_WATCHERS.with_borrow_mut(|watchers| {
        let mut remove = false;
        if let Some(tokens) = watchers.get_mut(&field_id) {
            tokens.retain(|watch| {
                let Some(watch) = watch.upgrade() else {
                    return false;
                };
                watch.valid.set(false);
                true
            });
            remove = tokens.is_empty();
        }
        if remove {
            watchers.remove(&field_id);
        }
        watchers.is_empty()
    });
    if emptied {
        CONS_MUTATION_WATCH_BLOOM.with_borrow_mut(|bloom| {
            if let Some(bloom) = bloom.as_mut() {
                bloom.fill(0);
            }
        });
    }
}

fn retain_live_cons_mutation_watchers(watchers: &mut ConsMutationWatchers) {
    watchers.retain(|_, watches| {
        watches.retain(|watch| watch.strong_count() != 0);
        !watches.is_empty()
    });
}

/// The process is exiting: the watcher table's entries (a weak count per
/// watched cons, touched one by one on a drop) are left to the kernel,
/// as exit() leaves C's heap.
pub(crate) fn forget_cons_mutation_watchers_for_exit() {
    CONS_MUTATION_WATCHERS.with_borrow_mut(|watchers| {
        std::mem::forget(std::mem::take(watchers));
    });
    CONS_MUTATION_WATCH_BLOOM.with_borrow_mut(|bloom| {
        std::mem::forget(bloom.take());
    });
}

fn register_cons_mutation_watchers(field_ids: &[usize], watch: &Rc<ConsMutationWatch>) {
    if field_ids.is_empty() {
        return;
    }
    let rebuilt_field_ids = CONS_MUTATION_WATCHERS.with_borrow_mut(|watchers| {
        let compact =
            CONS_MUTATION_WATCH_NEXT_KEY_LIMIT.with(|limit| watchers.len() >= limit.get());
        if compact {
            retain_live_cons_mutation_watchers(watchers);
            CONS_MUTATION_WATCH_NEXT_KEY_LIMIT.with(|limit| {
                limit.set(
                    watchers
                        .len()
                        .saturating_mul(2)
                        .max(CONS_MUTATION_WATCH_MINIMUM_KEY_LIMIT),
                );
            });
        }
        let weak = Rc::downgrade(watch);
        for field_id in field_ids {
            watchers.entry(*field_id).or_default().push(weak.clone());
        }
        compact.then(|| watchers.keys().copied().collect::<Vec<_>>())
    });
    CONS_MUTATION_WATCH_BLOOM.with_borrow_mut(|bloom| {
        let bloom = bloom.get_or_insert_with(|| Box::new([0u64; CONS_MUTATION_BLOOM_WORDS]));
        let bloom_field_ids = if let Some(rebuilt_field_ids) = &rebuilt_field_ids {
            bloom.fill(0);
            rebuilt_field_ids.as_slice()
        } else {
            field_ids
        };
        for field_id in bloom_field_ids {
            let (word, bit) = cons_mutation_bloom_slot(*field_id);
            bloom[word] |= bit;
        }
    });
}

/// Mutation dependencies for one derived view of a cons graph.
///
/// Each dependency registers a weak invalidation token with the one mutation
/// hook used by both cons fields. Rust-only dependencies need one boolean
/// check. For cells exposed to generated code, also check the canonical words:
/// native stores bypass the Rust mutation hook. Only this cache's native
/// dependencies are inspected, never unrelated conses.
#[derive(Debug, Clone)]
pub(crate) struct ConsMutationSnapshot {
    watch: Rc<ConsMutationWatch>,
    field_ids: Vec<usize>,
    native_cells: Vec<WeakConsRef>,
}

impl ConsMutationSnapshot {
    pub(crate) fn cell(cell: &SharedCons) -> Self {
        let mut snapshot = Self::from_field_ids(ConsCell::mutation_field_ids(cell).to_vec());
        snapshot.track_native_cell(cell);
        snapshot
    }

    pub(crate) fn list_spine(value: &Value) -> Self {
        let mut field_ids = Vec::new();
        let mut native_cells = Vec::new();
        let mut seen = HashSet::new();
        let mut current = *value;
        while let Kind::Cons(cell) = current.kind() {
            let cell_id = ConsCell::identity(&cell);
            if !seen.insert(cell_id) {
                break;
            }
            field_ids.extend(ConsCell::mutation_field_ids(&cell));
            if cell.attached_native_address().is_some() {
                native_cells.push(cell.downgrade());
            }
            current = cell.cdr.get();
        }
        let mut snapshot = Self::from_field_ids(field_ids);
        snapshot.native_cells = native_cells;
        snapshot
    }

    pub(crate) fn tree(value: &Value) -> Self {
        let mut snapshot = Self::from_field_ids(Vec::new());
        snapshot.include_tree(value);
        snapshot
    }

    /// A snapshot over CELLS at once: one sort of the field ids and one
    /// registration, where adding the cells one by one sorted the ids
    /// after each (a keymap view of a thousand cells snapshotted on every
    /// `define-key' spent its time there: mwheel-tests 25x GNU).
    pub(crate) fn cells<'a>(cells: impl IntoIterator<Item = &'a SharedCons>) -> Self {
        let mut field_ids = Vec::new();
        let mut native_cells = Vec::new();
        for cell in cells {
            field_ids.extend(ConsCell::mutation_field_ids(cell));
            if cell.attached_native_address().is_some() {
                native_cells.push(cell.downgrade());
            }
        }
        let mut snapshot = Self::from_field_ids(field_ids);
        snapshot.native_cells = native_cells;
        snapshot
    }

    /// Add one cell's two fields (and its canonical words, when generated
    /// code can reach it) to the dependencies.
    pub(crate) fn include_cell(&mut self, cell: &SharedCons) {
        let fields = ConsCell::mutation_field_ids(cell);
        if self.field_ids.binary_search(&fields[0]).is_ok() {
            return;
        }
        self.track_native_cell(cell);
        register_cons_mutation_watchers(&fields, &self.watch);
        self.field_ids.extend(fields);
        self.field_ids.sort_unstable();
    }

    pub(crate) fn include_tree(&mut self, value: &Value) {
        let mut seen = HashSet::new();
        let mut pending = vec![*value];
        let mut added = Vec::new();
        while let Some(value) = pending.pop() {
            let Kind::Cons(cell) = value.kind() else {
                continue;
            };
            if !seen.insert(ConsCell::identity(&cell)) {
                continue;
            }
            let fields = ConsCell::mutation_field_ids(&cell);
            if self.field_ids.binary_search(&fields[0]).is_err() {
                self.track_native_cell(&cell);
            }
            added.extend(fields);
            pending.push(cell.car.get());
            pending.push(cell.cdr.get());
        }
        added.sort_unstable();
        added.dedup();
        added.retain(|field_id| self.field_ids.binary_search(field_id).is_err());
        register_cons_mutation_watchers(&added, &self.watch);
        self.field_ids.extend(added);
        self.field_ids.sort_unstable();
    }

    fn from_field_ids(mut field_ids: Vec<usize>) -> Self {
        field_ids.sort_unstable();
        field_ids.dedup();
        let watch = Rc::new(ConsMutationWatch {
            valid: Cell::new(true),
        });
        register_cons_mutation_watchers(&field_ids, &watch);
        Self {
            watch,
            field_ids,
            native_cells: Vec::new(),
        }
    }

    fn track_native_cell(&mut self, cell: &SharedCons) {
        if cell.attached_native_address().is_some() {
            self.native_cells.push(cell.downgrade());
        }
    }

    pub(crate) fn is_current(&self) -> bool {
        if !self.watch.valid.get() {
            return false;
        }
        for cell in &self.native_cells {
            let Some(cell) = cell.upgrade() else {
                self.watch.valid.set(false);
                return false;
            };
            cell.car.synchronize_native_write();
            cell.cdr.synchronize_native_write();
            if !self.watch.valid.get() {
                return false;
            }
        }
        self.watch.valid.get()
    }

    pub(crate) fn mark_current(&self) {
        self.watch.valid.set(true);
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

/// One Lisp string: alloc.c's `struct Lisp_String' in a string block,
/// named by its address (`TextRef'); the text's bytes on the Rust heap.
pub type SharedText = crate::lisp::alloc::TextRef;

impl Eq for SharedText {}

impl PartialOrd for SharedText {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SharedText {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl PartialEq for SharedText {
    fn eq(&self, other: &Self) -> bool {
        // Interned symbol names share one allocation, so the common case
        // (`eq'-style symbol comparison) never reaches the byte compare.
        self.ptr_eq(other) || self.text() == other.text()
    }
}

impl std::hash::Hash for SharedText {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(self.text(), state);
    }
}

impl SharedText {
    pub fn new(text: String) -> Self {
        if text.is_empty() {
            return crate::lisp::alloc::empty_text();
        }
        let storage_bytes = crate::lisp::primitives::immutable_lisp_string_storage_byte_len(&text);
        note_string_allocation(storage_bytes);
        crate::lisp::alloc::allocate_string(text, storage_bytes)
    }

    /// A string whose storage size the image records (`size_byte' of the
    /// dumped Lisp_String): pdumper.c relocates the string in place and
    /// scans nothing, so neither does the loader.
    pub(crate) fn with_storage_bytes(text: String, storage_bytes: usize) -> Self {
        if text.is_empty() {
            return crate::lisp::alloc::empty_text();
        }
        note_string_allocation(storage_bytes);
        crate::lisp::alloc::allocate_string(text, storage_bytes)
    }

    /// Host-only text which is not a Lisp string allocation.  Uninterned
    /// symbols need an identity-bearing lookup key in Emaxx, but GNU stores
    /// that identity in the symbol object rather than appending bytes to its
    /// Lisp-visible name string.  Keep the encoded key out of both allocation
    /// and live-string accounting (it lives in a string block all the same,
    /// marked with its symbol).
    fn new_untracked(text: String) -> Self {
        crate::lisp::alloc::allocate_string(text, crate::lisp::alloc::UNTRACKED_TEXT)
    }
}

impl Deref for SharedText {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        self.text()
    }
}

impl AsRef<str> for SharedText {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<Path> for SharedText {
    fn as_ref(&self) -> &Path {
        Path::new(self.as_str())
    }
}

impl Borrow<str> for SharedText {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for SharedText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.text().fmt(f)
    }
}

impl fmt::Display for SharedText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.text().fmt(f)
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
        text.as_str().to_owned()
    }
}

impl PartialEq<str> for SharedText {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for SharedText {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for SharedText {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<SharedText> for String {
    fn eq(&self, other: &SharedText) -> bool {
        self == other.as_str()
    }
}

impl PartialEq<SharedText> for str {
    fn eq(&self, other: &SharedText) -> bool {
        self == other.as_str()
    }
}

impl PartialEq<SymbolName> for SharedText {
    fn eq(&self, other: &SymbolName) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<SharedText> for SymbolName {
    fn eq(&self, other: &SharedText) -> bool {
        self.as_str() == other.as_str()
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
struct ProcessTable<T>(std::cell::UnsafeCell<Option<T>>);

// SAFETY: one Lisp OS thread at a time, by construction (see above).
unsafe impl<T> Sync for ProcessTable<T> {}

impl<T: Default> ProcessTable<T> {
    const fn new() -> Self {
        Self(std::cell::UnsafeCell::new(None))
    }

    fn with_borrow<R>(&self, body: impl FnOnce(&T) -> R) -> R {
        // SAFETY: the one running Lisp thread's access.
        let table = unsafe { &mut *self.0.get() };
        body(table.get_or_insert_with(T::default))
    }

    fn with_borrow_mut<R>(&self, body: impl FnOnce(&mut T) -> R) -> R {
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

thread_local! {
    /// Live uninterned states by their private internal text.  Two
    /// `SymbolName's with equal internal text compare equal, so a text
    /// that names a live uninterned symbol must resolve to that very
    /// state: otherwise a name-keyed caller (`set' through `&str') and the
    /// symbol object would disagree about which cell they address.
    /// The live uninterned symbols by their key text (identity by text
    /// is the pre-representation deviation the ledger records); the
    /// sweep removes a freed cell's entry, so an entry is always live.
    static UNINTERNED_SYMBOL_BOOK: RefCell<HashMap<String, SymbolName>> = RefCell::new(HashMap::new());
}

/// Symbol ids are process-wide: the same internal text carries the same id
/// on every thread, so an interpreter built on one thread (the test image
/// template) addresses the same cells when it is used on another.  An
/// interned text keeps its id forever; an uninterned text keeps it while
/// any state with that text is alive (the count), so a private name that
/// dies and is minted again gets a fresh id.
type SymbolIdTable = HashMap<String, (u32, usize), crate::lisp::primitives::FnvBuildHasher>;
static SYMBOL_IDS: std::sync::Mutex<Option<SymbolIdTable>> = std::sync::Mutex::new(None);
static NEXT_SYMBOL_ID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
static NEXT_UNINTERNED_SYMBOL_ID: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(0);

/// Uninterned symbols draw ids from their own counter, marked by this bit,
/// so the dense per-interpreter cell table is sized by the number of
/// interned names rather than by every `make-symbol' ever evaluated.
pub(crate) const UNINTERNED_SYMBOL_ID_BIT: u32 = 1 << 31;

fn symbol_id_for(text: &str, uninterned: bool) -> u32 {
    let mut registry = SYMBOL_IDS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let registry = registry.get_or_insert_with(HashMap::default);
    if let Some((id, states)) = registry.get_mut(text) {
        if uninterned {
            *states += 1;
        }
        return *id;
    }
    let id = if uninterned {
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
    };
    registry.insert(text.to_owned(), (id, usize::from(uninterned)));
    id
}

fn registered_symbol_id(text: &str) -> Option<u32> {
    let registry = SYMBOL_IDS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    registry.as_ref()?.get(text).map(|(id, _)| *id)
}

/// alloc.c's `sweep_symbols', with the registries an uninterned
/// symbol's key names released as the cell goes: the id registry's
/// count and the book of live uninterned symbols.
pub(crate) fn sweep_symbol_cells(epoch: u32) {
    crate::lisp::alloc::sweep_symbols(epoch, |cell| {
        let Some(key) = cell.key.as_deref() else {
            return;
        };
        UNINTERNED_SYMBOL_BOOK.with_borrow_mut(|book| {
            book.remove(key);
        });
        let mut registry = SYMBOL_IDS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(registry) = registry.as_mut() else {
            return;
        };
        if let Some((_, states)) = registry.get_mut(key) {
            *states = states.saturating_sub(1);
            if *states == 0 {
                registry.remove(key);
            }
        }
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
                SharedText::new_untracked(text),
            );
        }
        INTERNED_SYMBOL_NAMES.with_borrow_mut(|names| {
            if let Some(name) = names.get(text.as_str()) {
                return *name;
            }
            crate::lisp::native_comp::note_lisp_allocation(48);
            let private = text.contains(OBARRAY_SYMBOL_MARKER);
            let text = if private || lisp_name.is_some() {
                SharedText::new_untracked(text)
            } else {
                SharedText::from(text)
            };
            let lisp_name = lisp_name.unwrap_or_else(|| {
                Value::String(if private {
                    SharedText::from(visible_symbol_name(&text))
                } else {
                    text
                })
            });
            let id = symbol_id_for(text.as_str(), false);
            let name = Self(crate::lisp::alloc::allocate_symbol(
                crate::lisp::alloc::SymbolCell {
                    internal: text,
                    lisp_name,
                    mark: MarkBit::default(),
                    id,
                    key: None,
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
        let mut registry = SYMBOL_IDS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        registry
            .get_or_insert_with(HashMap::default)
            .reserve(additional);
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
    /// died, has no cell anywhere.  This thread's tables answer first; the
    /// process registry covers a name another thread interned.
    pub(crate) fn id_of(text: &str) -> Option<u32> {
        if text.contains(UNINTERNED_SYMBOL_MARKER_CHAR)
            && let Some(name) = Self::live_uninterned(text)
        {
            return Some(name.id());
        }
        if let Some(id) =
            INTERNED_SYMBOL_NAMES.with_borrow(|names| names.get(text).map(|name| name.0.id))
        {
            return Some(id);
        }
        registered_symbol_id(text)
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
        Self::new_uninterned(
            name,
            SharedText::new_untracked(make_uninterned_symbol_name(visible, id)),
        )
    }

    fn new_uninterned(lisp_name: Value, internal: SharedText) -> Self {
        crate::lisp::native_comp::note_lisp_allocation(48);
        let id = symbol_id_for(internal.as_str(), true);
        let key = internal
            .as_str()
            .contains(UNINTERNED_SYMBOL_MARKER_CHAR)
            .then(|| Box::<str>::from(internal.as_str()));
        let name = Self(crate::lisp::alloc::allocate_symbol(
            crate::lisp::alloc::SymbolCell {
                internal,
                lisp_name,
                mark: MarkBit::default(),
                id,
                key,
            },
        ));
        UNINTERNED_SYMBOL_BOOK.with_borrow_mut(|book| {
            book.insert(name.0.internal.as_str().to_owned(), name);
        });
        name
    }

    /// The name's text; its lifetime is the symbol's, which the collector
    /// keeps while the symbol is reachable (an interned one, always).
    pub fn as_str(&self) -> &'static str {
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

    /// The host-side key text (a string cell the symbol keeps alive).
    pub(crate) fn internal_text(&self) -> &SharedText {
        &self.0.internal
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
                mark(&Value::String(*name.internal_text()));
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
        self.0.internal.text()
    }
}

impl AsRef<str> for SymbolName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for SymbolName {
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
        name.0.internal
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
/// Interpreted closures keep their GNU slots in the vector allocation.
pub type LambdaValue = crate::lisp::alloc::ClosureRef;

impl LambdaValue {
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
    /// The editable object, not a name/id proxy for an interpreter-owned copy.
    /// Rust callers must end these borrows before entering Lisp or collecting.
    pub(crate) state: RefCell<crate::buffer::Buffer>,
}

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
        for value in self.borrow().keyboard.values() {
            visit(value);
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
        Self::allocate(BufferValue {
            id,
            state: RefCell::new(buffer),
        })
    }

    pub fn borrow(&self) -> std::cell::Ref<'_, crate::buffer::Buffer> {
        self.state.borrow()
    }

    pub fn borrow_mut(&self) -> std::cell::RefMut<'_, crate::buffer::Buffer> {
        self.state.borrow_mut()
    }
}

/// `Lisp_Object' for an ordinary vector: alloc.c's `struct Lisp_Vector'
/// in a vector block (or on its own when large), named by its address.
pub use crate::lisp::alloc::VectorRef;
/// The pseudovector kinds' handles (alloc.c's `allocate_pseudovector').
pub type LambdaRef = crate::lisp::alloc::ClosureRef;
pub type BufferRef = crate::lisp::alloc::VectorlikeRef<BufferValue>;
pub type TerminalRef = crate::lisp::alloc::VectorlikeRef<TerminalValue>;
pub type StringObjectRef = crate::lisp::alloc::VectorlikeRef<RefCell<SharedStringState>>;
pub type ReaderFormRef = crate::lisp::alloc::VectorlikeRef<ReaderForm>;
/// PVEC_RECORD's handle: the record's state in a vector block.
pub type RecordRef = crate::lisp::alloc::VectorlikeRef<crate::lisp::eval::RecordState>;

/// The two tagged Lisp words generated code reads and writes directly.
///
/// This is the Rust representation of GNU `struct Lisp_Cons`'s live fields.
/// It is the first field of `ConsCell`, so a cons allocated for the Rust
/// evaluator has the same address and field offsets at the native boundary.
#[repr(C, align(8))]
#[derive(Debug)]
pub(crate) struct ConsWords {
    car: UnsafeCell<usize>,
    cdr: UnsafeCell<usize>,
}

impl ConsWords {
    pub(crate) fn new(car: usize, cdr: usize) -> Self {
        Self {
            car: UnsafeCell::new(car),
            cdr: UnsafeCell::new(cdr),
        }
    }

    pub(crate) fn car(&self) -> usize {
        unsafe { *self.car.get() }
    }

    pub(crate) fn cdr(&self) -> usize {
        unsafe { *self.cdr.get() }
    }

    pub(crate) fn set_car(&self, value: usize) {
        unsafe { *self.car.get() = value };
    }

    pub(crate) fn set_cdr(&self, value: usize) {
        unsafe { *self.cdr.get() = value };
    }
}

/// The mutable payload of one Lisp cons.
///
/// GNU allocates the car and cdr together as one `Lisp_Cons`.  Keeping the
/// same ownership shape halves the allocation and reference-count traffic of
/// Emaxx's former two-`Rc` representation while retaining independent field
/// borrows for `setcar`, `setcdr`, reader fixups, and vector element slots.
#[repr(C, align(8))]
#[derive(Debug)]
pub struct ConsCell {
    words: ConsWords,
    pub(crate) car: ConsValueCell,
    pub(crate) cdr: ConsValueCell,
    pub(crate) mark: MarkBit,
    /// The allocation's serial (`WeakConsRef' tells a later cell in the
    /// same slot apart by it); written by the allocator.
    pub(crate) serial: u64,
}

/// One tracked field of a cons cell.
///
/// Every mutable borrow advances the single mutation epoch used to validate
/// all derived source-form caches.  A field that has crossed the native ABI
/// also keeps the address and last-agreed value of its GNU `Lisp_Object`
/// word.  Ordinary reads can therefore detect the overwhelmingly common
/// unchanged case without entering the native heap's lookup tables.
#[derive(Debug)]
pub(crate) struct ConsValueCell {
    /// The field's word (`XCAR'/`XCDR' read it, `XSETCAR'/`XSETCDR'
    /// write it: a plain load and a plain store, no borrow count).
    value: Cell<Value>,
    /// Low bit distinguishes cdr from car; native Lisp words are eight-byte
    /// aligned, so the tag does not consume pointer information.
    native_word: Cell<*const usize>,
    native_agreed: Cell<usize>,
}

impl ConsValueCell {
    fn new(value: Value) -> Self {
        Self {
            value: Cell::new(value),
            native_word: Cell::new(std::ptr::null()),
            native_agreed: Cell::new(0),
        }
    }

    fn attach_native_word(&self, native_word: *const usize, agreed: usize, cdr: bool) {
        self.native_agreed.set(agreed);
        self.native_word
            .set(((native_word as usize) | usize::from(cdr)) as *const usize);
    }

    fn native_word_pointer(&self) -> *const usize {
        ((self.native_word.get() as usize) & !1) as *const usize
    }

    fn native_cons_key(&self) -> Option<usize> {
        let tagged = self.native_word.get() as usize;
        if tagged == 0 {
            return None;
        }
        let word = tagged & !1;
        Some(if tagged & 1 == 0 {
            word
        } else {
            word - std::mem::size_of::<usize>()
        })
    }

    fn detach_native_word(&self, native_word: *const usize) {
        if self.native_word_pointer() == native_word {
            self.native_word.set(std::ptr::null());
        }
    }

    fn set_native_agreed(&self, agreed: usize) {
        self.native_agreed.set(agreed);
    }

    fn native_agreed(&self) -> usize {
        self.native_agreed.get()
    }

    #[inline(always)]
    fn synchronize_native_write(&self) {
        let native_word = self.native_word_pointer();
        if !native_word.is_null() && unsafe { *native_word } != self.native_agreed.get() {
            crate::lisp::native_comp::synchronize_cons_read(
                self.native_cons_key()
                    .expect("an attached native word has a cons address"),
            );
        }
    }

    /// `XCAR'/`XCDR': the word, after any write generated code left in
    /// the native view is brought over.
    #[inline]
    pub(crate) fn get(&self) -> Value {
        self.synchronize_native_write();
        self.value.get()
    }

    /// The Rust field as stored, for the heap check: no native
    /// synchronization, no mutation notice.
    pub(crate) fn value_in_place(&self) -> Value {
        self.value.get()
    }

    /// The image loader's relocation store into a cell it created an
    /// instant ago: no watcher, generated code or native word has seen
    /// the cell, so there is no mutation to note (pdumper.c writes the
    /// relocated word in place).
    pub(crate) fn initialize(&self, value: Value) {
        self.value.set(value);
    }

    /// `XSETCAR'/`XSETCDR': the store, noted for the caches keyed on
    /// the cell and for the native view.
    #[inline]
    pub(crate) fn set(&self, value: Value) {
        self.synchronize_native_write();
        note_cons_mutation(self as *const Self as usize);
        if let Some(key) = self.native_cons_key() {
            note_native_cons_mutation(key);
        }
        self.value.set(value);
    }
}

// ===== Live-object accounting (finding 110) =====
//
// GNU's `garbage-collect' numbers come from allocator bookkeeping, not a
// heap walk; these are emaxx's equivalent books.  Every Lisp value lives
// on one thread (Rc is !Send), so plain thread-locals are exact and each
// test interpreter thread keeps its own books.  Cons cells are counted at
// construction and un-counted in Drop -- Rust ownership is the sweep.
// Strings register a Weak handle at allocation; the census upgrades each
// handle and prunes the dead ones, which is the lazy equivalent of GNU's
// sweep visiting every string block.
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

/// A string OBJECT (text properties, raw bytes, an `aset' target): a
/// pseudovector of this implementation's, counted with the strings.
pub(crate) fn string_object_value(state: SharedStringState) -> Value {
    let bytes = state.storage_bytes();
    string_object_value_with_storage_bytes(state, bytes)
}

/// `string_object_value' for a string whose storage size is already
/// known (the image records it).
pub(crate) fn string_object_value_with_storage_bytes(
    state: SharedStringState,
    bytes: usize,
) -> Value {
    note_string_allocation(bytes);
    Value::StringObject(crate::lisp::alloc::VectorlikeRef::allocate(RefCell::new(
        state,
    )))
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
    // The texts and the string objects, both counted by the sweep and
    // raised by allocation (gcstat's total_strings, total_string_bytes).
    let (objects, bytes, property_spans) = crate::lisp::alloc::live_string_object_census();
    StringCensus {
        count: crate::lisp::alloc::live_strings() + objects,
        bytes: crate::lisp::alloc::live_string_bytes() + bytes,
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
        crate::lisp::native_comp::note_lisp_allocation(16);
        Self::new_representation(car, cdr)
    }

    fn new_representation(car: Value, cdr: Value) -> Self {
        Self {
            words: ConsWords::new(0, 0),
            car: ConsValueCell::new(car),
            cdr: ConsValueCell::new(cdr),
            mark: MarkBit::default(),
            serial: 0,
        }
    }

    pub(crate) fn from_native_words(car: usize, cdr: usize) -> SharedCons {
        crate::lisp::alloc::allocate_cons(Self {
            words: ConsWords::new(car, cdr),
            car: ConsValueCell::new(Value::Nil),
            cdr: ConsValueCell::new(Value::Nil),
            mark: MarkBit::default(),
            serial: 0,
        })
    }

    pub(crate) fn identity(cell: &SharedCons) -> usize {
        cell.as_ptr() as usize
    }

    pub(crate) fn native_words(cell: &SharedCons) -> *mut ConsWords {
        std::ptr::from_ref(&cell.words).cast_mut()
    }

    /// Attach the Rust value cache to the two words generated code accesses:
    /// this cell's own prefix, whether Rust or generated code allocated it.
    pub(crate) unsafe fn attach_native_words(&self, native: *mut ConsWords, agreed: [usize; 2]) {
        // Snapshots made before this crossing only watch Rust stores. Retire
        // them once so their replacements also watch the canonical words.
        note_cons_mutation(&self.car as *const ConsValueCell as usize);
        note_cons_mutation(&self.cdr as *const ConsValueCell as usize);
        self.car
            .attach_native_word(unsafe { (*native).car.get() }, agreed[0], false);
        self.cdr
            .attach_native_word(unsafe { (*native).cdr.get() }, agreed[1], true);
    }

    pub(crate) unsafe fn detach_native_words(&self, native: *mut ConsWords) {
        self.car.detach_native_word(unsafe { (*native).car.get() });
        self.cdr.detach_native_word(unsafe { (*native).cdr.get() });
    }

    pub(crate) fn set_native_words_agreed(&self, agreed: [usize; 2]) {
        self.car.set_native_agreed(agreed[0]);
        self.cdr.set_native_agreed(agreed[1]);
    }

    pub(crate) fn native_words_agreed(&self) -> [usize; 2] {
        [self.car.native_agreed(), self.cdr.native_agreed()]
    }

    pub(crate) fn attached_native_address(&self) -> Option<usize> {
        self.car.native_cons_key()
    }

    pub(crate) fn mutation_field_ids(cell: &SharedCons) -> [usize; 2] {
        [
            &cell.car as *const ConsValueCell as usize,
            &cell.cdr as *const ConsValueCell as usize,
        ]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConsField {
    Car,
    Cdr,
}

/// A retained reference to one mutable field of a cons.
///
/// This is deliberately the only field-address abstraction exported by the
/// Lisp value layer.  Callers cannot depend on the physical layout of
/// `ConsCell`, so future value-representation work stays localized here.
#[derive(Clone, Debug)]
pub struct ConsSlot {
    cell: SharedCons,
    field: ConsField,
}

impl ConsSlot {
    pub(crate) fn car(cell: &SharedCons) -> Self {
        Self {
            cell: *cell,
            field: ConsField::Car,
        }
    }

    pub(crate) fn cdr(cell: &SharedCons) -> Self {
        Self {
            cell: *cell,
            field: ConsField::Cdr,
        }
    }

    pub fn get(&self) -> Value {
        match self.field {
            ConsField::Car => self.cell.car.get(),
            ConsField::Cdr => self.cell.cdr.get(),
        }
    }

    pub fn set(&self, value: Value) {
        match self.field {
            ConsField::Car => self.cell.car.set(value),
            ConsField::Cdr => self.cell.cdr.set(value),
        }
    }

    pub fn cell_id(&self) -> usize {
        ConsCell::identity(&self.cell)
    }

    pub fn ptr_eq(&self, other: &Self) -> bool {
        self.field == other.field && SharedCons::ptr_eq(&self.cell, &other.cell)
    }

    pub fn downgrade(&self) -> WeakConsSlot {
        WeakConsSlot {
            cell: self.cell.downgrade(),
            field: self.field,
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
        Some(ConsSlot {
            cell: self.cell.upgrade()?,
            field: self.field,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StringPropertySpan {
    pub start: usize,
    pub end: usize,
    pub props: Vec<(String, Value)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SharedStringState {
    pub text: String,
    pub props: Vec<StringPropertySpan>,
    pub multibyte: bool,
    /// Sparse character-indexed values for Emacs characters outside
    /// Unicode's scalar range.  `text` contains one placeholder scalar at
    /// each recorded index, so ordinary Unicode strings retain Rust's fast
    /// native representation while the full GNU character range remains
    /// lossless and one entry still counts as one Lisp character.
    pub extended_chars: Vec<(usize, u32)>,
}

impl SharedStringState {
    /// `size_byte' as the census reads it.
    pub(crate) fn storage_bytes(&self) -> usize {
        crate::lisp::primitives::lisp_string_storage_byte_len(
            &self.text,
            self.multibyte,
            &self.extended_chars,
        )
    }
}

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
    /// A symbol occurrence read with `read-positioning-symbols': lread.c's
    /// read0 wraps every symbol it reads in a `symbol-with-pos' when
    /// LOCATE_SYMS is set.  The reader has no Interpreter to allocate the
    /// pseudovector in, so the name and character position wait here.
    PositionedSymbol {
        name: String,
        pos: i64,
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
const TAG_SPECIAL: usize = 1;
const TAG_INT0: usize = 2;
const TAG_CONS: usize = 3;
const TAG_STRING: usize = 4;
const TAG_VECTORLIKE: usize = 5;
const TAG_FLOAT: usize = 7;
/// The kinds under `TAG_SPECIAL', in bits 3 to 7.
const SUB_MARKER: usize = 3;
const SUB_OVERLAY: usize = 4;
const SUB_CHAR_TABLE: usize = 5;
const SUB_FRAME: usize = 6;
const SUB_SHIFT: u32 = 3;
const PAYLOAD_SHIFT: u32 = 8;

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

const fn special(sub: usize, payload: usize) -> usize {
    (payload << PAYLOAD_SHIFT) | (sub << SUB_SHIFT) | TAG_SPECIAL
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
    String(SharedText),
    StringObject(StringObjectRef),
    Symbol(SymbolName),
    Cons(SharedCons),
    /// An ordinary vector with GNU vector identity and contiguous slots.
    Vector(VectorRef),
    /// A static GNU-layout subr containing its arity and native entry point.
    BuiltinFunc(BuiltinRef),
    /// A lambda or closure: params, immutable shared body, captured env.
    Lambda(LambdaRef),
    /// A buffer object: (id, name). The id is used for `eq` identity.
    Buffer(BufferRef),
    /// A marker object, identified by unique id.
    Marker(u64),
    /// An overlay object, identified by unique id.
    Overlay(u64),
    /// A char-table object, identified by unique id.
    CharTable(u64),
    /// An opaque frame object, identified by unique id.
    Frame(u64),
    /// An opaque terminal object, identified by unique id.
    Terminal(TerminalRef),
    /// A record, or one of the pseudovector kinds this implementation
    /// keeps as records (alloc.c's PVEC_RECORD): the cell's address.
    Record(RecordRef),
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
    #[inline]
    pub fn Integer(n: i64) -> Value {
        const MOST_POSITIVE: i64 = (1_i64 << 61) - 1;
        const MOST_NEGATIVE: i64 = -(1_i64 << 61);
        if (MOST_NEGATIVE..=MOST_POSITIVE).contains(&n) {
            Value::from_bits(((n << 2) as usize) | TAG_INT0)
        } else {
            Value::BigInteger(BigInt::from(n).into())
        }
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
        Value::from_bits(text.identity_ptr() | TAG_STRING)
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
    pub fn Vector(vector: VectorRef) -> Value {
        Value::from_bits(vector.identity() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn BuiltinFunc(subr: BuiltinRef) -> Value {
        Value::from_bits(subr.identity_ptr() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn Lambda(lambda: LambdaRef) -> Value {
        Value::from_bits(lambda.identity() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn Buffer(buffer: BufferRef) -> Value {
        Value::from_bits(buffer.identity() | TAG_VECTORLIKE)
    }
    #[inline]
    pub fn Marker(id: u64) -> Value {
        Value::from_bits(special(SUB_MARKER, id as usize))
    }
    #[inline]
    pub fn Overlay(id: u64) -> Value {
        Value::from_bits(special(SUB_OVERLAY, id as usize))
    }
    #[inline]
    pub fn CharTable(id: u64) -> Value {
        Value::from_bits(special(SUB_CHAR_TABLE, id as usize))
    }
    #[inline]
    pub fn Frame(id: u64) -> Value {
        Value::from_bits(special(SUB_FRAME, id as usize))
    }
    #[inline]
    pub fn Terminal(terminal: TerminalRef) -> Value {
        Value::from_bits(terminal.identity() | TAG_VECTORLIKE)
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
                let address = word & !TAG_MASK;
                // SAFETY: both current string allocations start with an
                // initialized size word. Plain text sizes (including the
                // untracked-name sentinel) cannot have the StringObject
                // pseudovector tag. No native bridge pointer is involved.
                unsafe {
                    if crate::lisp::alloc::vectors::header_tag(address as *mut _)
                        == crate::lisp::alloc::VectorTag::StringObject
                    {
                        Kind::StringObject(crate::lisp::alloc::VectorlikeRef::from_raw(
                            address as *mut _,
                        ))
                    } else {
                        Kind::String(SharedText::from_raw(address as *mut _))
                    }
                }
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
                        crate::lisp::alloc::VectorTag::Subr => {
                            Kind::BuiltinFunc(BuiltinRef::from_raw(header as usize))
                        }
                        crate::lisp::alloc::VectorTag::Buffer => {
                            Kind::Buffer(crate::lisp::alloc::VectorlikeRef::from_raw(header))
                        }
                        crate::lisp::alloc::VectorTag::Terminal => {
                            Kind::Terminal(crate::lisp::alloc::VectorlikeRef::from_raw(header))
                        }
                        crate::lisp::alloc::VectorTag::Closure => {
                            Kind::Lambda(crate::lisp::alloc::ClosureRef::from_raw(header))
                        }
                        crate::lisp::alloc::VectorTag::StringObject => {
                            impossible_tag("a string object uses the string tag")
                        }
                        crate::lisp::alloc::VectorTag::ReaderForm => {
                            Kind::ReaderForm(crate::lisp::alloc::VectorlikeRef::from_raw(header))
                        }
                        crate::lisp::alloc::VectorTag::Record => {
                            Kind::Record(crate::lisp::alloc::VectorlikeRef::from_raw(header))
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
            _ => {
                let payload = (word >> PAYLOAD_SHIFT) as u64;
                match (word >> SUB_SHIFT) & 31 {
                    SUB_MARKER => Kind::Marker(payload),
                    SUB_OVERLAY => Kind::Overlay(payload),
                    SUB_CHAR_TABLE => Kind::CharTable(payload),
                    SUB_FRAME => Kind::Frame(payload),
                    // SAFETY: every word this implementation makes has
                    // one of the sub-tags above.
                    _ => unsafe { impossible_tag("a value with an unknown tag") },
                }
            }
        }
    }

    /// The symbol a word names when its tag is the symbol tag, read from
    /// the word alone: no memory of the object is touched, so the sweep
    /// may ask it of a dead object's field (whose target, a vectorlike,
    /// might already be freed, and whose header `kind' would read).
    #[inline]
    pub(crate) fn symbol_by_tag(self) -> Option<SymbolName> {
        if self.0 & TAG_MASK == TAG_SYMBOL && !matches!(self.0, 0 | 48 | 96) {
            // SAFETY: a symbol-tagged word names a symbol cell; symbol
            // cells are swept after the vectors and the conses.
            Some(SymbolName::from_ref(unsafe {
                crate::lisp::alloc::SymbolRef::from_raw(
                    self.0 as *mut crate::lisp::alloc::SymbolCell,
                )
            }))
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
            Kind::String(v) => Value::String(v),
            Kind::StringObject(v) => Value::StringObject(v),
            Kind::Symbol(v) => Value::Symbol(v),
            Kind::Cons(v) => Value::Cons(v),
            Kind::Vector(v) => Value::Vector(v),
            Kind::BuiltinFunc(v) => Value::BuiltinFunc(v),
            Kind::Lambda(v) => Value::Lambda(v),
            Kind::Buffer(v) => Value::Buffer(v),
            Kind::Marker(v) => Value::Marker(v),
            Kind::Overlay(v) => Value::Overlay(v),
            Kind::CharTable(v) => Value::CharTable(v),
            Kind::Frame(v) => Value::Frame(v),
            Kind::Terminal(v) => Value::Terminal(v),
            Kind::Record(v) => Value::Record(v),
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
        Self::allocated_lambda(&[parameters, body, env])
    }

    pub(crate) fn allocated_lambda(slots: &[Value]) -> Self {
        Value::Lambda(crate::lisp::alloc::ClosureRef::allocate(slots))
    }

    pub fn buffer(id: u64, name: impl Into<SharedText>) -> Self {
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

    pub fn as_string(&self) -> Result<&str, LispError> {
        match self.kind() {
            Kind::String(s) => Ok(s.as_str()),
            _ => Err(LispError::WrongTypeArgument("stringp".into(), *self)),
        }
    }

    pub fn as_symbol(&self) -> Result<&str, LispError> {
        match self.kind() {
            Kind::Nil => Ok("nil"),
            Kind::T => Ok("t"),
            Kind::Symbol(s) => Ok(s.as_str()),
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
        self.cons_cells().map(|(car, cdr)| (car.get(), cdr.get()))
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
        let mut current = *self;
        let mut seen = CycleGuard::new();
        loop {
            match current.kind() {
                Kind::Nil => return Ok(()),
                Kind::Cons(cell) => {
                    if seen.step(ConsCell::identity(&cell)) {
                        return Err(circular_list_error());
                    }
                    result.push(cell.car.get());
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
            Kind::String(_) => "string".into(),
            Kind::StringObject(_) => "string".into(),
            Kind::Symbol(_) => "symbol".into(),
            Kind::Cons(_) => "cons".into(),
            Kind::Vector(_) => "vector".into(),
            Kind::BuiltinFunc(name) => format!("builtin<{}>", name),
            Kind::Lambda(_) => "lambda".into(),
            Kind::Buffer(buffer) => format!("buffer<{}>", buffer.borrow().name),
            Kind::Marker(id) => format!("marker<{}>", id),
            Kind::Overlay(id) => format!("overlay<{}>", id),
            Kind::CharTable(id) => format!("char-table<{}>", id),
            Kind::Frame(id) => format!("frame<{}>", id),
            Kind::Terminal(terminal) => format!("terminal<{}>", terminal.id),
            Kind::Record(record) => format!("record<{}>", record.id),
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
        (Kind::String(a), Kind::String(b)) => a == b,
        (Kind::StringObject(a), Kind::StringObject(b)) => {
            let a = RefCell::borrow(a.as_ref());
            let b = RefCell::borrow(b.as_ref());
            a.text == b.text && a.extended_chars == b.extended_chars
        }
        (Kind::String(a), Kind::StringObject(b)) => {
            let b = RefCell::borrow(b.as_ref());
            b.extended_chars.is_empty() && a.as_str() == b.text
        }
        (Kind::StringObject(a), Kind::String(b)) => {
            let a = RefCell::borrow(a.as_ref());
            a.extended_chars.is_empty() && a.text == b.as_str()
        }
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
        (Kind::Lambda(a), Kind::Lambda(b)) => {
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
        (Kind::CharTable(a), Kind::CharTable(b)) => a == b,
        (Kind::Frame(a), Kind::Frame(b)) => a == b,
        (Kind::Terminal(a), Kind::Terminal(b)) => a.ptr_eq(&b),
        (Kind::Record(a), Kind::Record(b)) => a.ptr_eq(&b),
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
        Kind::String(s) => write!(f, "\"{}\"", s),
        Kind::StringObject(state) => {
            write!(f, "\"{}\"", state.as_ref().borrow().text)
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
        Kind::Lambda(lambda) => write!(f, "#<lambda {}>", lambda.parameters()),
        Kind::Buffer(buffer) => write!(f, "#<buffer {}>", buffer.borrow().name),
        Kind::Marker(id) => write!(f, "#<marker id:{}>", id),
        Kind::Overlay(id) => write!(f, "#<overlay id:{}>", id),
        Kind::CharTable(id) => write!(f, "#<char-table id:{}>", id),
        Kind::Frame(id) => write!(f, "#<frame id:{}>", id),
        Kind::Terminal(terminal) => write!(f, "#<terminal id:{}>", terminal.id),
        Kind::Record(record) => write!(f, "#<record id:{}>", record.id),
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
                        Kind::String(text) => write!(f, "{text:?}"),
                        Kind::StringObject(object) => {
                            write!(f, "{:?}", std::cell::RefCell::borrow(object.as_ref()).text)
                        }
                        value => write!(f, "{value}"),
                    }
                }
                Ok(items)
                    if items.len() >= 4
                        && matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(kind)) if kind == "file-error" || kind == "file-missing") =>
                {
                    let message = match items[1].kind() {
                        Kind::String(text) => text.as_str(),
                        _ => return write!(f, "{}", value),
                    };
                    let detail = match items[2].kind() {
                        Kind::String(text) => text.as_str(),
                        _ => return write!(f, "{}", value),
                    };
                    let path = match items[3].kind() {
                        Kind::String(text) => text.as_str(),
                        _ => return write!(f, "{}", value),
                    };
                    write!(f, "{}: {}, {}", message, detail, path)
                }
                Ok(items) if items.len() >= 2 => match items[1].kind() {
                    Kind::String(text) => write!(f, "{text}"),
                    Kind::StringObject(object) => {
                        write!(f, "{}", std::cell::RefCell::borrow(object.as_ref()).text)
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
                let text: String = std::cell::RefCell::borrow(&state).text.clone();
                let mut brief: String = text.chars().take(48).collect();
                if brief.len() < text.len() {
                    brief.push('…');
                }
                out.push('"');
                out.push_str(&brief);
                out.push('"');
            }
            Kind::String(text) => {
                let mut brief: String = text.chars().take(48).collect();
                if brief.chars().count() < text.chars().count() {
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
        EnvFrame, Kind, LispError, SharedCons, SymbolName, Value, assq_binding, census_live_conses,
        census_live_floats, census_live_vectors, environment_declares_special,
        make_uninterned_symbol_name,
    };
    use std::rc::Rc;

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
        let (Kind::String(text), Kind::String(cloned_text)) = (value.kind(), clone.kind()) else {
            unreachable!("constructed string values")
        };

        assert!(text.ptr_eq(&cloned_text));
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
        let Kind::String(expected) = name.kind() else {
            unreachable!("constructed string")
        };
        let symbol = SymbolName::make_uninterned(name, "temporary", 1);
        let Kind::String(actual) = symbol.lisp_name().kind() else {
            unreachable!("immutable supplied name")
        };

        assert!(actual.ptr_eq(&expected));
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

        let (Kind::Lambda(lambda), Kind::Lambda(cloned_lambda)) = (lambda.kind(), clone.kind())
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
        let pair = Value::cons(Value::Integer(1), Value::Integer(2));
        let before_car = super::cons_mutation_epoch();
        pair.set_car(Value::Integer(3)).expect("set car");
        assert_ne!(super::cons_mutation_epoch(), before_car);

        let (_, cdr) = pair.cons_cells().expect("constructed cons");
        let before_cdr = super::cons_mutation_epoch();
        cdr.set(Value::Integer(4));
        assert_ne!(super::cons_mutation_epoch(), before_cdr);
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
        super::CONS_MUTATION_WATCHERS.with_borrow_mut(|watchers| watchers.clear());
        super::CONS_MUTATION_WATCH_BLOOM.with_borrow_mut(|bloom| *bloom = None);
        super::CONS_MUTATION_WATCH_NEXT_KEY_LIMIT
            .with(|limit| limit.set(super::CONS_MUTATION_WATCH_MINIMUM_KEY_LIMIT));

        let source = Value::cons(Value::Integer(1), Value::Nil);
        let Kind::Cons(cell) = source.kind() else {
            unreachable!("constructed cons")
        };
        let snapshot = super::ConsMutationSnapshot::cell(&cell);
        let field_ids = super::ConsCell::mutation_field_ids(&cell);
        let dead_owner = Rc::new(super::ConsMutationWatch {
            valid: std::cell::Cell::new(true),
        });
        let dead = Rc::downgrade(&dead_owner);
        drop(dead_owner);
        super::CONS_MUTATION_WATCHERS.with_borrow_mut(|watchers| {
            watchers.insert(usize::MAX, vec![dead]);
        });
        super::CONS_MUTATION_WATCH_NEXT_KEY_LIMIT.with(|limit| limit.set(1));
        let other = Value::cons(Value::Integer(3), Value::Nil);
        let Kind::Cons(other_cell) = other.kind() else {
            unreachable!("constructed cons")
        };
        let _other_snapshot = super::ConsMutationSnapshot::cell(&other_cell);
        super::CONS_MUTATION_WATCHERS.with_borrow(|watchers| {
            assert!(!watchers.contains_key(&usize::MAX));
            assert!(field_ids.iter().all(|field| watchers.contains_key(field)));
        });

        source
            .set_car(Value::Integer(2))
            .expect("mutate watched cons");
        assert!(!snapshot.is_current());

        super::CONS_MUTATION_WATCHERS.with_borrow_mut(|watchers| watchers.clear());
        super::CONS_MUTATION_WATCH_BLOOM.with_borrow_mut(|bloom| *bloom = None);
        super::CONS_MUTATION_WATCH_NEXT_KEY_LIMIT
            .with(|limit| limit.set(super::CONS_MUTATION_WATCH_MINIMUM_KEY_LIMIT));
    }

    #[test]
    fn cons_mutation_bloom_collisions_only_probe_the_authoritative_watcher_map() {
        super::CONS_MUTATION_WATCHERS.with_borrow_mut(|watchers| watchers.clear());
        super::CONS_MUTATION_WATCH_BLOOM.with_borrow_mut(|bloom| *bloom = None);

        let watched = 1_usize;
        let slot = super::cons_mutation_bloom_slot(watched);
        let collision = (watched + 1..)
            .find(|candidate| super::cons_mutation_bloom_slot(*candidate) == slot)
            .expect("the finite Bloom filter must have an address collision");
        let snapshot = super::ConsMutationSnapshot::from_field_ids(vec![watched]);

        super::note_cons_mutation(collision);
        assert!(
            snapshot.is_current(),
            "a Bloom collision must not invalidate an unrelated dependency"
        );
        super::note_cons_mutation(watched);
        assert!(!snapshot.is_current());

        super::CONS_MUTATION_WATCHERS.with_borrow_mut(|watchers| watchers.clear());
        super::CONS_MUTATION_WATCH_BLOOM.with_borrow_mut(|bloom| *bloom = None);
    }

    #[test]
    fn cons_mutation_bloom_resets_after_the_last_dead_watcher_is_drained() {
        super::CONS_MUTATION_WATCHERS.with_borrow_mut(|watchers| watchers.clear());
        super::CONS_MUTATION_WATCH_BLOOM.with_borrow_mut(|bloom| *bloom = None);

        let source = Value::cons(Value::Integer(1), Value::Integer(2));
        let Kind::Cons(cell) = source.kind() else {
            unreachable!("constructed cons")
        };
        let field_ids = super::ConsCell::mutation_field_ids(&cell);
        let snapshot = super::ConsMutationSnapshot::from_field_ids(field_ids.to_vec());
        assert!(super::CONS_MUTATION_WATCH_BLOOM.with_borrow(|bloom| bloom.is_some()));

        drop(snapshot);
        source.set_car(Value::Integer(3)).expect("source is a cons");
        source.set_cdr(Value::Integer(4)).expect("source is a cons");

        assert!(super::CONS_MUTATION_WATCHERS.with_borrow(|watchers| watchers.is_empty()));
        assert!(super::CONS_MUTATION_WATCH_BLOOM.with_borrow(|bloom| {
            bloom
                .as_ref()
                .is_some_and(|words| words.iter().all(|word| *word == 0))
        }));
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
