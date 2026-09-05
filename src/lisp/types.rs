#![allow(dead_code)]

use num_bigint::BigInt;
use num_traits::ToPrimitive;
use std::fmt;
use std::{
    borrow::Borrow,
    cell::{Cell, Ref, RefCell, RefMut, UnsafeCell},
    collections::{HashMap, HashSet},
    hash::{BuildHasher, BuildHasherDefault, Hasher},
    iter::FromIterator,
    ops::{Deref, DerefMut},
    path::Path,
    rc::{Rc, Weak},
};

const UNINTERNED_SYMBOL_MARKER: &str = "\u{1F}";
const OBARRAY_SYMBOL_MARKER: &str = "\u{1E}";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ConsMutationEpoch(u64);

const CONS_MUTATION_WATCH_MINIMUM_KEY_LIMIT: usize = 1 << 20;

#[derive(Default)]
pub(crate) struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        self.0
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
}

impl ConsMutationQueue {
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
/// hook used by both cons fields.  Cache reads are therefore a single boolean
/// load regardless of unrelated data mutation; mutations pay only for caches
/// that actually depend on the field being borrowed mutably.
#[derive(Debug, Clone)]
pub(crate) struct ConsMutationSnapshot {
    watch: Rc<ConsMutationWatch>,
    field_ids: Vec<usize>,
}

impl ConsMutationSnapshot {
    pub(crate) fn cell(cell: &SharedCons) -> Self {
        Self::from_field_ids(ConsCell::mutation_field_ids(cell).to_vec())
    }

    pub(crate) fn list_spine(value: &Value) -> Self {
        let mut field_ids = Vec::new();
        let mut seen = HashSet::new();
        let mut current = value.clone();
        while let Value::Cons(cell) = current {
            let cell_id = ConsCell::identity(&cell);
            if !seen.insert(cell_id) {
                break;
            }
            field_ids.extend(ConsCell::mutation_field_ids(&cell));
            current = cell.cdr.borrow().clone();
        }
        Self::from_field_ids(field_ids)
    }

    pub(crate) fn tree(value: &Value) -> Self {
        let mut snapshot = Self::from_field_ids(Vec::new());
        snapshot.include_tree(value);
        snapshot
    }

    pub(crate) fn include_tree(&mut self, value: &Value) {
        let mut seen = HashSet::new();
        let mut pending = vec![value.clone()];
        let mut added = Vec::new();
        while let Some(value) = pending.pop() {
            let Value::Cons(cell) = value else {
                continue;
            };
            if !seen.insert(ConsCell::identity(&cell)) {
                continue;
            }
            added.extend(ConsCell::mutation_field_ids(&cell));
            pending.push(cell.car.borrow().clone());
            pending.push(cell.cdr.borrow().clone());
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
        Self { watch, field_ids }
    }

    pub(crate) fn is_current(&self) -> bool {
        self.watch.valid.get()
    }

    pub(crate) fn mark_current(&self) {
        self.watch.valid.set(true);
    }
}

/// Immutable shared text stored inside compact Lisp values.
#[repr(transparent)]
#[derive(Clone, Eq, PartialOrd, Ord)]
pub struct SharedText(Rc<String>);

thread_local! {
    // alloc.c returns its permanently rooted empty string from every zero-
    // length allocation.  Besides making `(eq "" "")' true, that identity
    // is observable through print-circle when compiler constants repeat it.
    static EMPTY_SHARED_TEXT: SharedText = SharedText(Rc::new(String::new()));
}

impl PartialEq for SharedText {
    fn eq(&self, other: &Self) -> bool {
        // Interned symbol names share one allocation, so the common case
        // (`eq'-style symbol comparison) never reaches the byte compare.
        Rc::ptr_eq(&self.0, &other.0) || self.0 == other.0
    }
}

impl std::hash::Hash for SharedText {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.0, state);
    }
}

impl SharedText {
    pub(crate) fn identity_ptr(&self) -> usize {
        Rc::as_ptr(&self.0) as usize
    }

    pub fn new(text: String) -> Self {
        if text.is_empty() {
            return EMPTY_SHARED_TEXT.with(Clone::clone);
        }
        note_string_allocation(
            crate::lisp::primitives::immutable_lisp_string_storage_byte_len(&text),
        );
        let text = Rc::new(text);
        INTERNED_TEXT_BOOK.with(|book| {
            book.borrow_mut().push(Rc::downgrade(&text));
            INTERNED_TEXT_BOOK_LIMIT.with(|limit| prune_book(book, limit));
        });
        Self(text)
    }

    /// Host-only text which is not a Lisp string allocation.  Uninterned
    /// symbols need an identity-bearing lookup key in Emaxx, but GNU stores
    /// that identity in the symbol object rather than appending bytes to its
    /// Lisp-visible name string.  Keep the encoded key out of both allocation
    /// and live-string accounting.
    fn new_untracked(text: String) -> Self {
        Self(Rc::new(text))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }

    pub fn into_string(self) -> String {
        Rc::try_unwrap(self.0).unwrap_or_else(|text| text.as_ref().clone())
    }
}

impl Deref for SharedText {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
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
        self.0.fmt(f)
    }
}

impl fmt::Display for SharedText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
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
#[derive(Debug)]
struct SymbolNameState {
    internal: SharedText,
    lisp_name: Value,
    ordered_binding_hash: u64,
}

#[repr(transparent)]
#[derive(Clone)]
pub struct SymbolName(Rc<SymbolNameState>);

thread_local! {
    static INTERNED_SYMBOL_NAMES: RefCell<HashSet<SymbolName>> = RefCell::new(HashSet::new());
    static UNINTERNED_SYMBOL_BOOK: RefCell<Vec<Weak<SymbolNameState>>> = const { RefCell::new(Vec::new()) };
    static UNINTERNED_SYMBOL_BOOK_LIMIT: Cell<usize> = const { Cell::new(1 << 16) };
}

impl SymbolName {
    pub fn intern(text: String) -> Self {
        Self::intern_with_lisp_name(text, None)
    }

    /// The runtime lookup key is not SYMBOL_NAME. GNU init_symbol retains
    /// the supplied Lisp string; internal C-string callers create that name
    /// only when the symbol is first allocated.
    pub(crate) fn intern_with_lisp_name(text: String, lisp_name: Option<Value>) -> Self {
        if text.contains(UNINTERNED_SYMBOL_MARKER) {
            let visible = visible_symbol_name(&text).to_owned();
            return Self::new_uninterned(
                lisp_name.unwrap_or_else(|| Value::String(SharedText::from(visible))),
                SharedText::new_untracked(text),
            );
        }
        INTERNED_SYMBOL_NAMES.with_borrow_mut(|names| {
            if let Some(name) = names.get(text.as_str()) {
                return name.clone();
            }
            crate::lisp::native_comp::note_lisp_allocation(48);
            let ordered_binding_hash =
                crate::lisp::primitives::FnvBuildHasher::default().hash_one(text.as_str());
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
                    text.clone()
                })
            });
            let name = Self(Rc::new(SymbolNameState {
                internal: text,
                lisp_name,
                ordered_binding_hash,
            }));
            names.insert(name.clone());
            name
        })
    }

    pub(crate) fn make_uninterned(name: Value, visible: &str, id: u64) -> Self {
        Self::new_uninterned(
            name,
            SharedText::new_untracked(make_uninterned_symbol_name(visible, id)),
        )
    }

    fn new_uninterned(lisp_name: Value, internal: SharedText) -> Self {
        crate::lisp::native_comp::note_lisp_allocation(48);
        let ordered_binding_hash =
            crate::lisp::primitives::FnvBuildHasher::default().hash_one(internal.as_str());
        let state = Rc::new(SymbolNameState {
            internal,
            lisp_name,
            ordered_binding_hash,
        });
        UNINTERNED_SYMBOL_BOOK.with(|book| {
            book.borrow_mut().push(Rc::downgrade(&state));
            UNINTERNED_SYMBOL_BOOK_LIMIT.with(|limit| prune_book(book, limit));
        });
        Self(state)
    }

    pub fn as_str(&self) -> &str {
        self.0.internal.as_str()
    }

    pub(crate) fn identity_ptr(&self) -> usize {
        Rc::as_ptr(&self.0) as usize
    }

    pub(crate) fn ordered_binding_hash(&self) -> u64 {
        self.0.ordered_binding_hash
    }

    pub fn into_string(self) -> String {
        self.as_str().to_owned()
    }

    pub(crate) fn lisp_name(&self) -> Value {
        self.0.lisp_name.clone()
    }
}

pub(crate) fn census_live_uninterned_symbols() -> usize {
    UNINTERNED_SYMBOL_BOOK.with(|book| {
        let mut book = book.borrow_mut();
        book.retain(|symbol| symbol.strong_count() != 0);
        UNINTERNED_SYMBOL_BOOK_LIMIT.with(|limit| limit.set((book.len() * 2).max(1 << 16)));
        book.len()
    })
}

impl PartialEq for SymbolName {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0) || self.as_str() == other.as_str()
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
        name.0.internal.clone()
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

#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SharedBigInt(Rc<BigInt>);

impl SharedBigInt {
    pub(crate) fn identity_ptr(&self) -> usize {
        Rc::as_ptr(&self.0) as usize
    }
}

impl Deref for SharedBigInt {
    type Target = BigInt;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<BigInt> for SharedBigInt {
    fn from(value: BigInt) -> Self {
        crate::lisp::native_comp::note_lisp_allocation(24);
        let value = Rc::new(value);
        BIGNUM_OBJECT_BOOK.with(|book| {
            book.borrow_mut().push(Rc::downgrade(&value));
            BIGNUM_OBJECT_BOOK_LIMIT.with(|limit| prune_book(book, limit));
        });
        Self(value)
    }
}

impl From<SharedBigInt> for BigInt {
    fn from(value: SharedBigInt) -> Self {
        Rc::try_unwrap(value.0).unwrap_or_else(|value| value.as_ref().clone())
    }
}

impl PartialEq<BigInt> for SharedBigInt {
    fn eq(&self, other: &BigInt) -> bool {
        self.0.as_ref() == other
    }
}

impl PartialEq<SharedBigInt> for BigInt {
    fn eq(&self, other: &SharedBigInt) -> bool {
        self == other.0.as_ref()
    }
}

impl fmt::Display for SharedBigInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// One allocated Lisp floating-point object.
///
/// GNU stores every float in a `struct Lisp_Float`; copying a `Lisp_Object`
/// copies its pointer, not the double.  Keeping the payload behind `Rc`
/// preserves that object identity while Rust ownership manages the storage.
#[repr(transparent)]
#[derive(Clone, Debug)]
pub struct SharedFloat(Rc<f64>);

impl SharedFloat {
    pub(crate) fn identity_ptr(&self) -> usize {
        Rc::as_ptr(&self.0) as usize
    }

    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }

    pub fn get(&self) -> f64 {
        *self.0
    }
}

impl PartialEq for SharedFloat {
    fn eq(&self, other: &Self) -> bool {
        self.get().to_bits() == other.get().to_bits()
    }
}

impl From<f64> for SharedFloat {
    fn from(value: f64) -> Self {
        crate::lisp::native_comp::note_lisp_allocation(8);
        let value = Rc::new(value);
        FLOAT_OBJECT_BOOK.with(|book| {
            book.borrow_mut().push(Rc::downgrade(&value));
            FLOAT_OBJECT_BOOK_LIMIT.with(|limit| prune_book(book, limit));
        });
        Self(value)
    }
}

impl Deref for SharedFloat {
    type Target = f64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for SharedFloat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

pub type SharedCons = Rc<ConsCell>;
pub type ConsCells = (ConsSlot, ConsSlot);
pub type SharedEnv = Rc<RefCell<Env>>;
pub type SharedLambdaParams = Rc<Vec<SymbolName>>;
pub type SharedLambdaBody = Rc<Vec<Value>>;

#[derive(Debug)]
pub struct LambdaValue {
    pub params: SharedLambdaParams,
    /// Exact GNU interpreted-closure slot zero.  Emaxx also keeps `params`
    /// as a compact binding vector, but Lisp-visible closure inspection and
    /// native compilation must see the original argument-list objects,
    /// including source-position symbols.
    pub public_parameters: Option<Value>,
    pub body: SharedLambdaBody,
    pub env: SharedEnv,
    /// GNU closure slot four.  Unlike ordinary source docstrings, a
    /// `(:documentation FORM)' may evaluate to any Lisp object (oclosure.el
    /// deliberately stores its type symbol here).
    pub documentation: Option<Value>,
    /// GNU closure slot five.  `Some(nil)' is distinct from an absent slot:
    /// `(interactive)' still makes the closure a command and gives it length
    /// six.  A vector-valued slot preserves GNU's command-modes metadata.
    pub interactive: Option<Value>,
    /// Exact Lisp object stored in GNU interpreted-closure slot two when the
    /// closure was constructed by `make-interpreted-closure'.  The object is
    /// observable and mutable: `aref' must return this same alist, and
    /// mutations of its binding cells must affect subsequent calls.
    pub public_environment: Option<Value>,
}

impl LambdaValue {
    /// Convert GNU's `(interactive SPEC . MODES)' form into closure slot
    /// five.  Multiple command modes use the modern `[SPEC MODES]' layout.
    pub fn interactive_slot_from_form(form: &Value) -> Option<Value> {
        let items = form.to_vec().ok()?;
        if !matches!(items.first(), Some(Value::Symbol(head)) if head == "interactive") {
            return None;
        }
        Some(Self::interactive_slot_from_iform_items(&items))
    }

    /// Convert the already-validated list passed as IFORM to GNU's
    /// `make-interpreted-closure' into slot five.
    pub fn interactive_slot_from_iform_items(items: &[Value]) -> Value {
        let spec = items.get(1).cloned().unwrap_or(Value::Nil);
        if items.len() <= 2 {
            spec
        } else {
            Value::list([
                Value::Symbol("vector-literal".into()),
                spec,
                Value::list(items[2..].to_vec()),
            ])
        }
    }

    /// Return the public interactive specification from GNU closure slot
    /// five.  New-style slots are vectors `[SPEC MODES]' while old-style
    /// slots contain SPEC directly.
    pub fn interactive_spec(&self) -> Option<Value> {
        self.interactive.as_ref().map(|slot| {
            slot.to_vec()
                .ok()
                .filter(|items| {
                    matches!(items.first(), Some(Value::Symbol(head)) if head == "vector-literal")
                })
                .and_then(|items| items.get(1).cloned())
                .unwrap_or_else(|| slot.clone())
        })
    }

    pub fn command_modes(&self) -> Option<Value> {
        self.interactive
            .as_ref()
            .and_then(Self::command_modes_from_slot)
    }

    pub fn command_modes_from_slot(slot: &Value) -> Option<Value> {
        let items = slot.to_vec().ok()?;
        matches!(items.first(), Some(Value::Symbol(head)) if head == "vector-literal")
            .then(|| items.get(2).cloned().unwrap_or(Value::Nil))
    }

    pub fn public_len(&self) -> usize {
        if self.interactive.is_some() {
            6
        } else if self.documentation.is_some() {
            5
        } else {
            3
        }
    }
}

#[derive(Debug)]
pub struct BufferValue {
    pub id: u64,
    pub name: SharedText,
}

/// One ordinary GNU vector: stable object identity plus contiguous mutable
/// Lisp slots.  The Rust owner is reference counted, while the payload shape
/// follows `struct Lisp_Vector` instead of the historical tagged-cons facade.
#[derive(Debug)]
pub struct VectorValue {
    slots: RefCell<Vec<Value>>,
    accounted_slots: usize,
}

impl VectorValue {
    fn allocated(slots: Vec<Value>) -> Rc<Self> {
        let accounted_slots = slots.len().saturating_add(1);
        crate::lisp::native_comp::note_lisp_allocation(accounted_slots.saturating_mul(8));
        LIVE_VECTORS.set(LIVE_VECTORS.get().saturating_add(1));
        LIVE_VECTOR_SLOTS.set(LIVE_VECTOR_SLOTS.get().saturating_add(accounted_slots));
        Rc::new(Self {
            slots: RefCell::new(slots),
            accounted_slots,
        })
    }

    fn static_zero() -> Rc<Self> {
        Rc::new(Self {
            slots: RefCell::new(Vec::new()),
            accounted_slots: 0,
        })
    }

    pub(crate) fn identity(value: &Rc<Self>) -> usize {
        Rc::as_ptr(value) as usize
    }

    pub(crate) fn slots(&self) -> Ref<'_, Vec<Value>> {
        self.slots.borrow()
    }

    pub(crate) fn slots_mut(&self) -> RefMut<'_, Vec<Value>> {
        self.slots.borrow_mut()
    }
}

impl Drop for VectorValue {
    fn drop(&mut self) {
        if self.accounted_slots == 0 {
            return;
        }
        LIVE_VECTORS.set(
            LIVE_VECTORS
                .get()
                .checked_sub(1)
                .expect("live GNU vector count is balanced"),
        );
        LIVE_VECTOR_SLOTS.set(
            LIVE_VECTOR_SLOTS
                .get()
                .checked_sub(self.accounted_slots)
                .expect("live GNU vector slot count is balanced"),
        );
    }
}

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
    value: RefCell<Value>,
    /// Low bit distinguishes cdr from car; native Lisp words are eight-byte
    /// aligned, so the tag does not consume pointer information.
    native_word: Cell<*const usize>,
    native_agreed: Cell<usize>,
}

impl ConsValueCell {
    fn new(value: Value) -> Self {
        Self {
            value: RefCell::new(value),
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

    pub(crate) fn borrow(&self) -> Ref<'_, Value> {
        self.synchronize_native_write();
        self.value.borrow()
    }

    pub(crate) fn borrow_mut(&self) -> RefMut<'_, Value> {
        self.synchronize_native_write();
        note_cons_mutation(self as *const Self as usize);
        if let Some(key) = self.native_cons_key() {
            note_native_cons_mutation(key);
        }
        self.value.borrow_mut()
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
thread_local! {
    static LIVE_CONSES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static STRING_OBJECT_BOOK: RefCell<Vec<std::rc::Weak<RefCell<SharedStringState>>>> =
        const { RefCell::new(Vec::new()) };
    static STRING_OBJECT_BOOK_LIMIT: std::cell::Cell<usize> = const { std::cell::Cell::new(1 << 16) };
    static INTERNED_TEXT_BOOK: RefCell<Vec<std::rc::Weak<String>>> =
        const { RefCell::new(Vec::new()) };
    static INTERNED_TEXT_BOOK_LIMIT: std::cell::Cell<usize> = const { std::cell::Cell::new(1 << 16) };
    static FLOAT_OBJECT_BOOK: RefCell<Vec<std::rc::Weak<f64>>> =
        const { RefCell::new(Vec::new()) };
    static FLOAT_OBJECT_BOOK_LIMIT: std::cell::Cell<usize> = const { std::cell::Cell::new(1 << 16) };
    static BIGNUM_OBJECT_BOOK: RefCell<Vec<std::rc::Weak<BigInt>>> =
        const { RefCell::new(Vec::new()) };
    static BIGNUM_OBJECT_BOOK_LIMIT: std::cell::Cell<usize> = const { std::cell::Cell::new(1 << 16) };
    static LAMBDA_OBJECT_BOOK: RefCell<Vec<std::rc::Weak<LambdaValue>>> =
        const { RefCell::new(Vec::new()) };
    static LAMBDA_OBJECT_BOOK_LIMIT: std::cell::Cell<usize> = const { std::cell::Cell::new(1 << 16) };
    static LIVE_VECTORS: Cell<usize> = const { Cell::new(0) };
    static LIVE_VECTOR_SLOTS: Cell<usize> = const { Cell::new(0) };
}

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

/// Drop the dead handles when a book outgrows its limit, so a session that
/// never calls `garbage-collect' holds at most ~2x the live handles.  The
/// amortized cost per registration stays O(1).
fn prune_book<T>(book: &RefCell<Vec<std::rc::Weak<T>>>, limit: &std::cell::Cell<usize>) {
    let mut book = book.borrow_mut();
    if book.len() < limit.get() {
        return;
    }
    book.retain(|weak| weak.strong_count() > 0);
    limit.set((book.len() * 2).max(1 << 16));
}

/// Every new string OBJECT must pass through here (all four construction
/// sites do); an unregistered object would be invisible to the census.
pub(crate) fn register_string_object(state: &Rc<RefCell<SharedStringState>>) {
    let bytes = {
        let state = RefCell::borrow(state);
        crate::lisp::primitives::lisp_string_storage_byte_len(
            &state.text,
            state.multibyte,
            &state.extended_chars,
        )
    };
    note_string_allocation(bytes);
    STRING_OBJECT_BOOK.with(|book| {
        book.borrow_mut().push(Rc::downgrade(state));
        STRING_OBJECT_BOOK_LIMIT.with(|limit| prune_book(book, limit));
    });
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
    LIVE_CONSES.with(|count| count.get())
}

pub(crate) fn census_live_strings() -> StringCensus {
    let mut census = StringCensus::default();
    STRING_OBJECT_BOOK.with(|book| {
        let mut book = book.borrow_mut();
        book.retain(|weak| match weak.upgrade() {
            Some(state) => {
                let state = RefCell::borrow(&state);
                census.count += 1;
                census.bytes += crate::lisp::primitives::lisp_string_storage_byte_len(
                    &state.text,
                    state.multibyte,
                    &state.extended_chars,
                );
                census.property_spans += state.props.len();
                true
            }
            None => false,
        });
        STRING_OBJECT_BOOK_LIMIT.with(|limit| limit.set((book.len() * 2).max(1 << 16)));
    });
    INTERNED_TEXT_BOOK.with(|book| {
        let mut book = book.borrow_mut();
        book.retain(|weak| match weak.upgrade() {
            Some(text) => {
                census.count += 1;
                census.bytes +=
                    crate::lisp::primitives::immutable_lisp_string_storage_byte_len(&text);
                true
            }
            None => false,
        });
        INTERNED_TEXT_BOOK_LIMIT.with(|limit| limit.set((book.len() * 2).max(1 << 16)));
    });
    census
}

pub(crate) fn census_live_floats() -> usize {
    FLOAT_OBJECT_BOOK.with(|book| {
        let mut book = book.borrow_mut();
        book.retain(|weak| weak.strong_count() > 0);
        FLOAT_OBJECT_BOOK_LIMIT.with(|limit| limit.set((book.len() * 2).max(1 << 16)));
        book.len()
    })
}

pub(crate) fn census_live_vectors() -> VectorCensus {
    let mut census = VectorCensus {
        count: LIVE_VECTORS.get(),
        slots: LIVE_VECTOR_SLOTS.get(),
        representation_conses: 0,
    };
    BIGNUM_OBJECT_BOOK.with(|book| {
        let mut book = book.borrow_mut();
        book.retain(|weak| weak.strong_count() > 0);
        census.count += book.len();
        // lisp.h:Lisp_Bignum is 24 bytes on the supported GNU ABI.
        census.slots = census.slots.saturating_add(book.len().saturating_mul(3));
        BIGNUM_OBJECT_BOOK_LIMIT.with(|limit| limit.set((book.len() * 2).max(1 << 16)));
    });
    LAMBDA_OBJECT_BOOK.with(|book| {
        let mut book = book.borrow_mut();
        book.retain(|weak| match weak.upgrade() {
            Some(lambda) => {
                census.count += 1;
                // eval.c:Fmake_interpreted_closure allocates an ordinary
                // vector and retags it PVEC_CLOSURE.  The header is one
                // word beyond the Lisp-visible slots.
                census.slots = census
                    .slots
                    .saturating_add(lambda.public_len().saturating_add(1));
                true
            }
            None => false,
        });
        LAMBDA_OBJECT_BOOK_LIMIT.with(|limit| limit.set((book.len() * 2).max(1 << 16)));
    });
    census
}

fn register_lambda_object(lambda: &Rc<LambdaValue>) {
    crate::lisp::native_comp::note_lisp_allocation(
        lambda.public_len().saturating_add(1).saturating_mul(8),
    );
    LAMBDA_OBJECT_BOOK.with(|book| {
        book.borrow_mut().push(Rc::downgrade(lambda));
        LAMBDA_OBJECT_BOOK_LIMIT.with(|limit| prune_book(book, limit));
    });
}

impl ConsCell {
    fn new(car: Value, cdr: Value) -> Self {
        crate::lisp::native_comp::note_lisp_allocation(16);
        Self::new_representation(car, cdr)
    }

    fn new_representation(car: Value, cdr: Value) -> Self {
        LIVE_CONSES.with(|count| count.set(count.get() + 1));
        Self {
            words: ConsWords::new(0, 0),
            car: ConsValueCell::new(car),
            cdr: ConsValueCell::new(cdr),
        }
    }

    pub(crate) fn from_native_words(car: usize, cdr: usize) -> SharedCons {
        LIVE_CONSES.with(|count| count.set(count.get() + 1));
        Rc::new(Self {
            words: ConsWords::new(car, cdr),
            car: ConsValueCell::new(Value::Nil),
            cdr: ConsValueCell::new(Value::Nil),
        })
    }

    pub(crate) fn identity(cell: &SharedCons) -> usize {
        Rc::as_ptr(cell) as usize
    }

    pub(crate) fn native_words(cell: &SharedCons) -> *mut ConsWords {
        std::ptr::from_ref(&cell.words).cast_mut()
    }

    /// Attach the Rust value cache to the two words generated code accesses.
    /// Rust-created conses point at their own prefix; conses allocated by
    /// generated code point at the owning native arena until that bridge is
    /// detached.
    pub(crate) unsafe fn attach_native_words(&self, native: *mut ConsWords, agreed: [usize; 2]) {
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

impl Drop for ConsCell {
    fn drop(&mut self) {
        LIVE_CONSES.with(|count| count.set(count.get().saturating_sub(1)));
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
            cell: cell.clone(),
            field: ConsField::Car,
        }
    }

    pub(crate) fn cdr(cell: &SharedCons) -> Self {
        Self {
            cell: cell.clone(),
            field: ConsField::Cdr,
        }
    }

    pub fn borrow(&self) -> Ref<'_, Value> {
        match self.field {
            ConsField::Car => self.cell.car.borrow(),
            ConsField::Cdr => self.cell.cdr.borrow(),
        }
    }

    pub fn borrow_mut(&self) -> RefMut<'_, Value> {
        match self.field {
            ConsField::Car => self.cell.car.borrow_mut(),
            ConsField::Cdr => self.cell.cdr.borrow_mut(),
        }
    }

    pub fn cell_id(&self) -> usize {
        ConsCell::identity(&self.cell)
    }

    pub fn ptr_eq(&self, other: &Self) -> bool {
        self.field == other.field && Rc::ptr_eq(&self.cell, &other.cell)
    }

    pub fn downgrade(&self) -> WeakConsSlot {
        WeakConsSlot {
            cell: Rc::downgrade(&self.cell),
            field: self.field,
        }
    }
}

#[derive(Clone, Debug)]
pub struct WeakConsSlot {
    cell: Weak<ConsCell>,
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

/// A Lisp value. This covers the subset we need for ERT tests.
#[derive(Clone, Debug)]
pub enum Value {
    Nil,
    T,
    Integer(i64),
    BigInteger(SharedBigInt),
    Float(SharedFloat),
    String(SharedText),
    StringObject(Rc<RefCell<SharedStringState>>),
    Symbol(SymbolName),
    Cons(SharedCons),
    /// An ordinary vector with GNU vector identity and contiguous slots.
    Vector(Rc<VectorValue>),
    /// Built-in function: name, arity (min, max), function pointer handled in eval
    BuiltinFunc(SymbolName),
    /// A lambda or closure: params, immutable shared body, captured env.
    ///
    /// Function-cell lookup clones Lisp values on every call.  Sharing the
    /// immutable code keeps that clone O(1) while the captured environment
    /// retains its independent Lisp identity and mutability.
    Lambda(Rc<LambdaValue>),
    /// A buffer object: (id, name). The id is used for `eq` identity.
    Buffer(Rc<BufferValue>),
    /// A marker object, identified by unique id.
    Marker(u64),
    /// An overlay object, identified by unique id.
    Overlay(u64),
    /// A char-table object, identified by unique id.
    CharTable(u64),
    /// An opaque frame object, identified by unique id.
    Frame(u64),
    /// An opaque terminal object, identified by unique id.
    Terminal(u64),
    /// A record object, identified by unique id.
    Record(u64),
    /// A finalizer object, identified by unique id.
    Finalizer(u64),
    /// Typed reader state awaiting Interpreter-owned object allocation.
    ReaderForm(Rc<ReaderForm>),
    /// Internal marker for EIEIO slots that have not been bound.
    Unbound,
}

thread_local! {
    // alloc.c:zero_vector is one static object returned by every zero-length
    // ordinary-vector allocation and is outside the heap vector census.
    static EMPTY_VECTOR_VALUE: Value = Value::Vector(VectorValue::static_zero());
}

/// One lexical environment frame.
///
/// Capturing or invoking a closure snapshots an environment far more often
/// than it mutates one.  Share the frame's ordered binding vector across
/// those snapshots and detach only the frame that is actually written.  The
/// evaluator's exact frame/name overlay remains the authority for GNU's
/// shared lexical-cell semantics; this type only removes redundant deep
/// copies of derived snapshots.
#[derive(Clone, Debug)]
pub struct EnvFrame(Rc<EnvFrameData>);

#[derive(Debug, Default)]
struct LexicalFrameState {
    captured: Cell<bool>,
    /// GNU closures share the `(SYMBOL . VALUE)` conses of their lexical
    /// environment.  Keep weak references by binding position so differently
    /// trimmed snapshots reuse the same cells without retaining dead
    /// closures or conflating unrelated frames that bind the same name.
    binding_cells: RefCell<Vec<Weak<ConsCell>>>,
}

#[derive(Clone, Debug)]
struct EnvFrameData {
    // eval.c:Flet, FletX and funcall_lambda retain the original symbol
    // object. Re-interning its printed/internal name would split the
    // identity of an uninterned lexical variable at closure capture.
    bindings: Vec<(SymbolName, Value)>,
    /// Stable identity used to align captured and live lexical frames.
    /// This is evaluator bookkeeping, never a Lisp binding.
    identity: Option<i64>,
    /// Whether this frame belongs to the function namespace (for example a
    /// cl-flet/cl-labels frame) rather than the value namespace.
    function_bindings: bool,
    /// GNU locally-special declarations, recorded at their position among
    /// real bindings so closure environment serialization remains exact.
    local_special_declarations: Vec<(usize, String)>,
    /// A GNU interpreter environment whose binding conses are the authority
    /// for this frame.  Keeping this typed metadata beside the bindings lets
    /// copied frames and nested closures share the original Lisp cells
    /// without a Lisp-visible marker or a process-global side table.
    lisp_environment: Option<Value>,
    state: Rc<LexicalFrameState>,
}

impl EnvFrame {
    /// Stable pointer identity of the shared frame data (image-clone memo
    /// key; see ImageGraphCopier in eval.rs).
    pub(crate) fn identity_ptr(&self) -> usize {
        Rc::as_ptr(&self.0) as usize
    }

    /// Rebuild this frame with every contained Lisp value mapped through
    /// COPY, preserving the frame's evaluator metadata (identity,
    /// namespace flag).  Frames shared between environments are
    /// deduplicated by the caller via `identity_ptr'.
    pub(crate) fn deep_copy_with(&self, copy: &mut impl FnMut(&Value) -> Value) -> Self {
        let data = &self.0;
        Self(Rc::new(EnvFrameData {
            bindings: data
                .bindings
                .iter()
                .map(|(name, value)| (name.clone(), copy(value)))
                .collect(),
            identity: data.identity,
            function_bindings: data.function_bindings,
            local_special_declarations: data.local_special_declarations.clone(),
            lisp_environment: data.lisp_environment.as_ref().map(copy),
            state: Rc::new(LexicalFrameState {
                captured: Cell::new(data.state.captured.get()),
                binding_cells: RefCell::new(Vec::new()),
            }),
        }))
    }

    pub fn new(bindings: Vec<(SymbolName, Value)>) -> Self {
        Self(Rc::new(EnvFrameData {
            bindings,
            identity: None,
            function_bindings: false,
            local_special_declarations: Vec::new(),
            lisp_environment: None,
            state: Rc::new(LexicalFrameState::default()),
        }))
    }

    pub fn with_identity(bindings: Vec<(SymbolName, Value)>, identity: i64) -> Self {
        Self(Rc::new(EnvFrameData {
            bindings,
            identity: Some(identity),
            function_bindings: false,
            local_special_declarations: Vec::new(),
            lisp_environment: None,
            state: Rc::new(LexicalFrameState::default()),
        }))
    }

    pub fn with_lisp_environment_and_identity(
        bindings: Vec<(SymbolName, Value)>,
        lisp_environment: Value,
        identity: i64,
    ) -> Self {
        Self(Rc::new(EnvFrameData {
            bindings,
            identity: Some(identity),
            function_bindings: false,
            local_special_declarations: Vec::new(),
            lisp_environment: Some(lisp_environment),
            state: Rc::new(LexicalFrameState {
                captured: Cell::new(true),
                binding_cells: RefCell::new(Vec::new()),
            }),
        }))
    }

    pub fn with_function_bindings(bindings: Vec<(SymbolName, Value)>, identity: i64) -> Self {
        Self(Rc::new(EnvFrameData {
            bindings,
            identity: Some(identity),
            function_bindings: true,
            local_special_declarations: Vec::new(),
            lisp_environment: None,
            state: Rc::new(LexicalFrameState::default()),
        }))
    }

    pub fn with_local_special(name: impl Into<String>, identity: i64) -> Self {
        Self::with_local_specials([name.into()], identity)
    }

    pub fn with_local_specials(names: impl IntoIterator<Item = String>, identity: i64) -> Self {
        Self(Rc::new(EnvFrameData {
            bindings: Vec::new(),
            identity: Some(identity),
            function_bindings: false,
            local_special_declarations: names.into_iter().map(|name| (0, name)).collect(),
            lisp_environment: None,
            state: Rc::new(LexicalFrameState::default()),
        }))
    }

    pub fn from_parts(
        bindings: Vec<(SymbolName, Value)>,
        identity: Option<i64>,
        function_bindings: bool,
        local_special_declarations: Vec<(usize, String)>,
    ) -> Self {
        Self(Rc::new(EnvFrameData {
            bindings,
            identity,
            function_bindings,
            local_special_declarations,
            lisp_environment: None,
            state: Rc::new(LexicalFrameState {
                captured: Cell::new(true),
                binding_cells: RefCell::new(Vec::new()),
            }),
        }))
    }

    pub(crate) fn mark_captured(&self) {
        self.0.state.captured.set(true);
    }

    pub(crate) fn is_captured(&self) -> bool {
        self.0.state.captured.get()
    }

    pub(crate) fn canonical_lisp_binding(
        &self,
        position: usize,
        name: &SymbolName,
        value: Value,
    ) -> Value {
        let mut cells = self.0.state.binding_cells.borrow_mut();
        if cells.len() <= position {
            cells.resize_with(position + 1, Weak::new);
        }
        if let Some(cell) = cells[position].upgrade()
            && cell
                .car
                .borrow()
                .as_symbol()
                .is_ok_and(|bound| bound == name)
        {
            return Value::Cons(cell);
        }
        let Value::Cons(cell) = Value::cons(Value::Symbol(name.clone()), value) else {
            unreachable!("Value::cons constructs a cons")
        };
        cells[position] = Rc::downgrade(&cell);
        Value::Cons(cell)
    }

    pub(crate) fn update_canonical_lisp_binding(&self, name: &str, value: Value) {
        for cell in self
            .0
            .state
            .binding_cells
            .borrow()
            .iter()
            .rev()
            .filter_map(Weak::upgrade)
        {
            if cell
                .car
                .borrow()
                .as_symbol()
                .is_ok_and(|bound| bound == name)
            {
                *cell.cdr.borrow_mut() = value;
                break;
            }
        }
    }

    /// Read GNU's canonical `(SYMBOL . VALUE)` cell for this binding when a
    /// closure environment has materialized it.  A filtered closure may wrap
    /// the same cell in a different typed frame, so the cell—not either
    /// frame's snapshot—remains the authoritative lexical value.
    pub(crate) fn canonical_lisp_binding_value(
        &self,
        position: usize,
        name: &str,
    ) -> Option<Value> {
        let cell = self
            .0
            .state
            .binding_cells
            .borrow()
            .get(position)?
            .upgrade()?;
        if !cell
            .car
            .borrow()
            .as_symbol()
            .is_ok_and(|bound| bound == name)
        {
            return None;
        }
        Some(cell.cdr.borrow().clone())
    }

    pub fn identity(&self) -> Option<i64> {
        self.0.identity
    }

    pub fn has_function_bindings(&self) -> bool {
        self.0.function_bindings
    }

    pub fn declares_local_special(&self, name: &str) -> bool {
        self.0
            .local_special_declarations
            .iter()
            .any(|(_, declared)| declared == name)
    }

    pub fn local_special_declarations(&self) -> &[(usize, String)] {
        &self.0.local_special_declarations
    }

    pub fn is_local_special_snapshot(&self) -> bool {
        self.0.bindings.is_empty()
            && !self.0.function_bindings
            && !self.0.local_special_declarations.is_empty()
            && self
                .0
                .local_special_declarations
                .iter()
                .all(|(position, _)| *position == 0)
    }

    pub fn set_lisp_environment(&mut self, environment: Value) {
        Rc::make_mut(&mut self.0).lisp_environment = Some(environment);
    }

    pub fn lisp_environment(&self) -> Option<&Value> {
        self.0.lisp_environment.as_ref()
    }
}

impl PartialEq for EnvFrame {
    fn eq(&self, other: &Self) -> bool {
        self.0.bindings == other.0.bindings
            && self.0.identity == other.0.identity
            && self.0.function_bindings == other.0.function_bindings
            && self.0.local_special_declarations == other.0.local_special_declarations
    }
}

impl Default for EnvFrame {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl From<Vec<(SymbolName, Value)>> for EnvFrame {
    fn from(bindings: Vec<(SymbolName, Value)>) -> Self {
        Self::new(bindings)
    }
}

impl FromIterator<(SymbolName, Value)> for EnvFrame {
    fn from_iter<T: IntoIterator<Item = (SymbolName, Value)>>(iter: T) -> Self {
        Self::new(iter.into_iter().collect())
    }
}

impl Deref for EnvFrame {
    type Target = Vec<(SymbolName, Value)>;

    fn deref(&self) -> &Self::Target {
        &self.0.bindings
    }
}

impl DerefMut for EnvFrame {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut Rc::make_mut(&mut self.0).bindings
    }
}

impl IntoIterator for EnvFrame {
    type Item = (SymbolName, Value);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        Rc::try_unwrap(self.0)
            .map(|frame| frame.bindings)
            .unwrap_or_else(|frame| frame.bindings.clone())
            .into_iter()
    }
}

impl<'a> IntoIterator for &'a EnvFrame {
    type Item = &'a (SymbolName, Value);
    type IntoIter = std::slice::Iter<'a, (SymbolName, Value)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.bindings.iter()
    }
}

impl<'a> IntoIterator for &'a mut EnvFrame {
    type Item = &'a mut (SymbolName, Value);
    type IntoIter = std::slice::IterMut<'a, (SymbolName, Value)>;

    fn into_iter(self) -> Self::IntoIter {
        Rc::make_mut(&mut self.0).bindings.iter_mut()
    }
}

/// An environment is an ordered outer-to-inner list of lexical frames.
pub type Env = Vec<EnvFrame>;

pub fn shared_env(env: Env) -> SharedEnv {
    Rc::new(RefCell::new(env))
}

pub(crate) fn make_uninterned_symbol_name(base: &str, id: u64) -> String {
    format!("{base}{UNINTERNED_SYMBOL_MARKER}{id}")
}

pub(crate) fn make_obarray_symbol_name(base: &str, obarray_id: u64) -> String {
    format!("{base}{OBARRAY_SYMBOL_MARKER}{obarray_id}")
}

pub(crate) fn is_uninterned_symbol(symbol: &str) -> bool {
    symbol.contains(UNINTERNED_SYMBOL_MARKER)
}

pub(crate) fn visible_symbol_name(symbol: &str) -> &str {
    symbol
        .split_once(UNINTERNED_SYMBOL_MARKER)
        .or_else(|| symbol.split_once(OBARRAY_SYMBOL_MARKER))
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

    // GNU's dtoastr starts at DBL_DIG significant digits and grows only
    // until parsing reproduces the same f64.  Rust's Display instead prefers
    // fixed notation for many large integral values, which changes `read'
    // from float to bignum and breaks numeric round trips.
    let abs = value.abs();
    let mut rendered = if abs == 0.0 {
        value.to_string()
    } else {
        let exponent = abs.log10().floor() as i32;
        (15..=17)
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
    /// Whether a reference-counted object wrapped for native code is still
    /// owned outside that native wrapper.  Id-backed values return true
    /// because their host representation has no reference count; their
    /// wrappers are the stable identities generated code observes.
    pub(crate) fn native_handle_has_external_owner(&self) -> bool {
        match self {
            Value::BigInteger(value) => Rc::strong_count(&value.0) > 1,
            Value::Float(value) => Rc::strong_count(&value.0) > 1,
            Value::String(value) => Rc::strong_count(&value.0) > 1,
            Value::StringObject(value) => Rc::strong_count(value) > 1,
            Value::Cons(value) => Rc::strong_count(value) > 1,
            Value::Vector(value) => Rc::strong_count(value) > 1,
            Value::Lambda(value) => Rc::strong_count(value) > 1,
            Value::Buffer(value) => Rc::strong_count(value) > 1,
            Value::ReaderForm(value) => Rc::strong_count(value) > 1,
            Value::Symbol(_) | Value::BuiltinFunc(_) | Value::Unbound => true,
            _ => false,
        }
    }

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
        Value::Cons(Rc::new(ConsCell::new(car, cdr)))
    }

    pub fn vector(items: impl IntoIterator<Item = Value>) -> Self {
        let slots = items.into_iter().collect::<Vec<_>>();
        if slots.is_empty() {
            EMPTY_VECTOR_VALUE.with(Clone::clone)
        } else {
            Value::Vector(VectorValue::allocated(slots))
        }
    }

    pub fn lambda(params: SharedLambdaParams, body: SharedLambdaBody, env: SharedEnv) -> Self {
        Self::lambda_with_documentation(params, body, env, None)
    }

    pub fn lambda_with_documentation(
        params: SharedLambdaParams,
        body: SharedLambdaBody,
        env: SharedEnv,
        documentation: Option<Value>,
    ) -> Self {
        Self::lambda_with_metadata(params, body, env, documentation, None)
    }

    pub fn lambda_with_metadata(
        params: SharedLambdaParams,
        body: SharedLambdaBody,
        env: SharedEnv,
        documentation: Option<Value>,
        interactive: Option<Value>,
    ) -> Self {
        Self::allocated_lambda(LambdaValue {
            params,
            public_parameters: None,
            body,
            env,
            documentation,
            interactive,
            public_environment: None,
        })
    }

    pub fn lambda_with_public_environment(
        params: SharedLambdaParams,
        public_parameters: Value,
        body: SharedLambdaBody,
        env: SharedEnv,
        documentation: Option<Value>,
        interactive: Option<Value>,
        public_environment: Value,
    ) -> Self {
        Self::allocated_lambda(LambdaValue {
            params,
            public_parameters: Some(public_parameters),
            body,
            env,
            documentation,
            interactive,
            public_environment: Some(public_environment),
        })
    }

    pub(crate) fn allocated_lambda(lambda: LambdaValue) -> Self {
        let lambda = Rc::new(lambda);
        register_lambda_object(&lambda);
        Value::Lambda(lambda)
    }

    pub fn buffer(id: u64, name: impl Into<SharedText>) -> Self {
        Value::Buffer(Rc::new(BufferValue {
            id,
            name: name.into(),
        }))
    }

    /// Build a proper list from an iterator of values.
    pub fn list(items: impl IntoIterator<Item = Value>) -> Self {
        let items: Vec<Value> = items.into_iter().collect();
        if matches!(
            items.first(),
            Some(Value::Symbol(tag)) if tag == "vector-literal"
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
        matches!(self, Value::Nil)
    }

    pub fn is_truthy(&self) -> bool {
        !self.is_nil()
    }

    pub fn is_integer(&self) -> bool {
        matches!(self, Value::Integer(_) | Value::BigInteger(_))
    }

    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_) | Value::StringObject(_))
    }

    pub fn is_symbol(&self) -> bool {
        matches!(self, Value::Nil | Value::T | Value::Symbol(_))
    }

    pub fn is_cons(&self) -> bool {
        matches!(self, Value::Cons(_))
    }

    pub fn is_list(&self) -> bool {
        matches!(self, Value::Nil | Value::Cons(_))
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
        match self {
            Value::Integer(n) => Ok(*n),
            // A bignum is an integer but not a fixnum, which is exactly what
            // CHECK_FIXNUM rejects.
            _ => Err(LispError::WrongTypeArgument("fixnump".into(), self.clone())),
        }
    }

    pub fn as_integer(&self) -> Result<i64, LispError> {
        // GNU's CHECK_INTEGER names `integerp'; CHECK_FIXNUM names `fixnump'
        // and is spelled `as_fixnum' above.
        match self {
            Value::Integer(n) => Ok(*n),
            Value::BigInteger(n) => n
                .to_i64()
                .ok_or_else(|| LispError::WrongTypeArgument("fixnump".into(), self.clone())),
            _ => Err(LispError::WrongTypeArgument(
                "integerp".into(),
                self.clone(),
            )),
        }
    }

    pub fn as_float(&self) -> Result<f64, LispError> {
        // Arithmetic contexts: GNU's coercion check names
        // `number-or-marker-p' ((+ 'a 1) => (number-or-marker-p a)).
        match self {
            Value::Float(f) => Ok(f.get()),
            Value::Integer(n) => Ok(*n as f64),
            Value::BigInteger(n) => n.to_f64().ok_or_else(|| {
                LispError::WrongTypeArgument("number-or-marker-p".into(), self.clone())
            }),
            _ => Err(LispError::WrongTypeArgument(
                "number-or-marker-p".into(),
                self.clone(),
            )),
        }
    }

    pub fn as_string(&self) -> Result<&str, LispError> {
        match self {
            Value::String(s) => Ok(s),
            _ => Err(LispError::WrongTypeArgument("stringp".into(), self.clone())),
        }
    }

    pub fn as_symbol(&self) -> Result<&str, LispError> {
        match self {
            Value::Nil => Ok("nil"),
            Value::T => Ok("t"),
            Value::Symbol(s) => Ok(s),
            _ => Err(LispError::WrongTypeArgument("symbolp".into(), self.clone())),
        }
    }

    pub fn car(&self) -> Result<Value, LispError> {
        match self {
            Value::Cons(cell) => Ok(cell.car.borrow().clone()),
            Value::Nil => Ok(Value::Nil),
            _ => Err(LispError::WrongTypeArgument("listp".into(), self.clone())),
        }
    }

    pub fn cdr(&self) -> Result<Value, LispError> {
        match self {
            Value::Cons(cell) => Ok(cell.cdr.borrow().clone()),
            Value::Nil => Ok(Value::Nil),
            _ => Err(LispError::WrongTypeArgument("listp".into(), self.clone())),
        }
    }

    pub fn set_car(&self, new_car: Value) -> Result<(), LispError> {
        match self {
            Value::Cons(cell) => {
                *cell.car.borrow_mut() = new_car;
                Ok(())
            }
            _ => Err(LispError::WrongTypeArgument("consp".into(), self.clone())),
        }
    }

    pub fn set_cdr(&self, new_cdr: Value) -> Result<(), LispError> {
        match self {
            Value::Cons(cell) => {
                *cell.cdr.borrow_mut() = new_cdr;
                Ok(())
            }
            _ => Err(LispError::WrongTypeArgument("consp".into(), self.clone())),
        }
    }

    pub fn cons_cells(&self) -> Option<ConsCells> {
        match self {
            Value::Cons(cell) => Some((ConsSlot::car(cell), ConsSlot::cdr(cell))),
            _ => None,
        }
    }

    pub fn cons_id(&self) -> Option<usize> {
        match self {
            Value::Cons(cell) => Some(ConsCell::identity(cell)),
            _ => None,
        }
    }

    pub fn cons_values(&self) -> Option<(Value, Value)> {
        self.cons_cells()
            .map(|(car, cdr)| (car.borrow().clone(), cdr.borrow().clone()))
    }

    /// Convert a proper list to a Vec.
    pub fn to_vec(&self) -> Result<Vec<Value>, LispError> {
        if let Value::Vector(vector) = self {
            let slots = vector.slots();
            let mut result = Vec::with_capacity(slots.len().saturating_add(1));
            result.push(Value::symbol("vector-literal"));
            result.extend(slots.iter().cloned());
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
        let mut current = self.clone();
        let mut seen = CycleGuard::new();
        loop {
            match current {
                Value::Nil => return Ok(()),
                Value::Cons(cell) => {
                    if seen.step(ConsCell::identity(&cell)) {
                        return Err(circular_list_error());
                    }
                    result.push(cell.car.borrow().clone());
                    current = cell.cdr.borrow().clone();
                }
                _ => {
                    return Err(LispError::WrongTypeArgument(
                        "listp".into(),
                        current.clone(),
                    ));
                }
            }
        }
    }

    pub fn type_name(&self) -> String {
        match self {
            Value::Nil => "nil".into(),
            Value::T => "t".into(),
            Value::Integer(_) => "integer".into(),
            Value::BigInteger(_) => "integer".into(),
            Value::Float(_) => "float".into(),
            Value::String(_) => "string".into(),
            Value::StringObject(_) => "string".into(),
            Value::Symbol(_) => "symbol".into(),
            Value::Cons(_) => "cons".into(),
            Value::Vector(_) => "vector".into(),
            Value::BuiltinFunc(name) => format!("builtin<{}>", name),
            Value::Lambda(_) => "lambda".into(),
            Value::Buffer(buffer) => format!("buffer<{}>", buffer.name),
            Value::Marker(id) => format!("marker<{}>", id),
            Value::Overlay(id) => format!("overlay<{}>", id),
            Value::CharTable(id) => format!("char-table<{}>", id),
            Value::Frame(id) => format!("frame<{}>", id),
            Value::Terminal(id) => format!("terminal<{}>", id),
            Value::Record(id) => format!("record<{}>", id),
            Value::Finalizer(id) => format!("finalizer<{}>", id),
            Value::ReaderForm(_) => "reader-form".into(),
            Value::Unbound => "unbound".into(),
        }
    }
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
    match (left, right) {
        (Value::Nil, Value::Nil) => true,
        (Value::T, Value::T) => true,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::BigInteger(a), Value::BigInteger(b)) => a == b,
        (Value::Integer(a), Value::BigInteger(b)) | (Value::BigInteger(b), Value::Integer(a)) => {
            BigInt::from(*a) == **b
        }
        // fns.c internal_equal via same_float: representation equality
        // (NaN equals NaN; 0.0 differs from -0.0).
        (Value::Float(a), Value::Float(b)) => a.to_bits() == b.to_bits(),
        (Value::String(a), Value::String(b)) => a == b,
        (Value::StringObject(a), Value::StringObject(b)) => {
            let a = RefCell::borrow(a.as_ref());
            let b = RefCell::borrow(b.as_ref());
            a.text == b.text && a.extended_chars == b.extended_chars
        }
        (Value::String(a), Value::StringObject(b)) => {
            let b = RefCell::borrow(b.as_ref());
            b.extended_chars.is_empty() && a.as_str() == b.text
        }
        (Value::StringObject(a), Value::String(b)) => {
            let a = RefCell::borrow(a.as_ref());
            a.extended_chars.is_empty() && a.text == b.as_str()
        }
        (Value::Symbol(a), Value::Symbol(b)) => a == b,
        (Value::Cons(a), Value::Cons(b)) => {
            if Rc::ptr_eq(a, b) {
                return true;
            }
            let ids = (ConsCell::identity(a), ConsCell::identity(b));
            if !seen.get_or_insert_with(HashSet::new).insert(ids) {
                return true;
            }
            values_equal_recursive(&a.car.borrow(), &b.car.borrow(), seen)
                && values_equal_recursive(&a.cdr.borrow(), &b.cdr.borrow(), seen)
        }
        (Value::Vector(a), Value::Vector(b)) => {
            if Rc::ptr_eq(a, b) {
                return true;
            }
            let ids = (VectorValue::identity(a), VectorValue::identity(b));
            if !seen.get_or_insert_with(HashSet::new).insert(ids) {
                return true;
            }
            let a = a.slots();
            let b = b.slots();
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|(a, b)| values_equal_recursive(a, b, seen))
        }
        (Value::BuiltinFunc(a), Value::BuiltinFunc(b)) => a == b,
        (Value::Lambda(a), Value::Lambda(b)) => {
            a.params == b.params
                && a.public_parameters == b.public_parameters
                && a.body == b.body
                && a.documentation == b.documentation
                && a.interactive == b.interactive
                && a.public_environment == b.public_environment
                && Rc::ptr_eq(&a.env, &b.env)
        }
        (Value::Buffer(a), Value::Buffer(b)) => a.id == b.id,
        (Value::Marker(a), Value::Marker(b)) => a == b,
        (Value::Overlay(a), Value::Overlay(b)) => a == b,
        (Value::CharTable(a), Value::CharTable(b)) => a == b,
        (Value::Frame(a), Value::Frame(b)) => a == b,
        (Value::Terminal(a), Value::Terminal(b)) => a == b,
        (Value::Record(a), Value::Record(b)) => a == b,
        (Value::Finalizer(a), Value::Finalizer(b)) => a == b,
        (Value::ReaderForm(a), Value::ReaderForm(b)) => Rc::ptr_eq(a, b),
        (Value::Unbound, Value::Unbound) => true,
        _ => false,
    }
}

fn format_value(
    value: &Value,
    f: &mut fmt::Formatter<'_>,
    seen: &mut HashSet<usize>,
) -> fmt::Result {
    match value {
        Value::Nil => write!(f, "nil"),
        Value::T => write!(f, "t"),
        Value::Integer(n) => write!(f, "{}", n),
        Value::BigInteger(n) => write!(f, "{}", n),
        Value::Float(v) => write!(f, "{}", format_float(v.get())),
        Value::String(s) => write!(f, "\"{}\"", s),
        Value::StringObject(state) => {
            write!(f, "\"{}\"", state.as_ref().borrow().text)
        }
        Value::Symbol(s) => write!(f, "{}", visible_symbol_name(s)),
        Value::Vector(vector) => {
            let id = VectorValue::identity(vector);
            if !seen.insert(id) {
                return write!(f, "#<circular-vector>");
            }
            write!(f, "[")?;
            for (index, value) in vector.slots().iter().enumerate() {
                if index != 0 {
                    write!(f, " ")?;
                }
                format_value(value, f, seen)?;
            }
            seen.remove(&id);
            write!(f, "]")
        }
        Value::Cons(cell) if matches!(&*cell.car.borrow(), Value::Symbol(head) if head == "vector-literal") =>
        {
            // Vector literals ride on conses internally but print as vectors.
            write!(f, "[")?;
            let mut current = cell.cdr.borrow().clone();
            let mut first = true;
            while let Value::Cons(cell) = current {
                if !first {
                    write!(f, " ")?;
                }
                format_value(&cell.car.borrow(), f, seen)?;
                first = false;
                current = cell.cdr.borrow().clone();
            }
            write!(f, "]")
        }
        Value::Cons(cell) => {
            // GNU prints reader shorthands: (quote X) as 'X and
            // (function X) as #'X.
            if let Value::Symbol(head) = &*cell.car.borrow()
                && (head == "quote" || head == "function")
                && let Value::Cons(inner) = &*cell.cdr.borrow()
                && matches!(&*inner.cdr.borrow(), Value::Nil)
            {
                write!(f, "{}", if head == "quote" { "'" } else { "#'" })?;
                return format_value(&inner.car.borrow(), f, seen);
            }
            write!(f, "(")?;
            let mut current = value.clone();
            let mut first = true;
            loop {
                match current {
                    Value::Cons(cell) => {
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
                        format_value(&cell.car.borrow(), f, seen)?;
                        first = false;
                        current = cell.cdr.borrow().clone();
                    }
                    Value::Nil => break,
                    other => {
                        write!(f, " . ")?;
                        format_value(&other, f, seen)?;
                        break;
                    }
                }
            }
            write!(f, ")")
        }
        Value::BuiltinFunc(name) => write!(f, "#<builtin {}>", name),
        Value::Lambda(lambda) => write!(f, "#<lambda ({})>", lambda.params.join(" ")),
        Value::Buffer(buffer) => write!(f, "#<buffer {}>", buffer.name),
        Value::Marker(id) => write!(f, "#<marker id:{}>", id),
        Value::Overlay(id) => write!(f, "#<overlay id:{}>", id),
        Value::CharTable(id) => write!(f, "#<char-table id:{}>", id),
        Value::Frame(id) => write!(f, "#<frame id:{}>", id),
        Value::Terminal(id) => write!(f, "#<terminal id:{}>", id),
        Value::Record(id) => write!(f, "#<record id:{}>", id),
        Value::Finalizer(id) => write!(f, "#<finalizer id:{}>", id),
        Value::ReaderForm(_) => write!(f, "#<reader-form>"),
        Value::Unbound => write!(f, "#<unbound>"),
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

/// Lisp errors and non-local evaluator control flow.
#[derive(Clone, Debug)]
pub enum LispError {
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
    /// Internal bytecode VM return; consumed before control leaves the VM.
    VmReturn(Value),
    /// Orderly, non-catchable process termination via `kill-emacs`.
    Terminate(EmacsTermination),
    /// An ERT skip condition.
    TestSkipped(String),
    /// End of input during read
    EndOfInput,
    /// Reader syntax error
    ReadError(String),
}

impl LispError {
    pub fn condition_type(&self) -> String {
        match self {
            LispError::TypeError(_, _) => "wrong-type-argument".into(),
            LispError::WrongTypeArgument(_, _) => "wrong-type-argument".into(),
            LispError::Void(_) => "void-variable".into(),
            LispError::VoidFunction(_) => "void-function".into(),
            LispError::WrongNumberOfArgs(_, _) => "wrong-number-of-arguments".into(),
            LispError::Signal(_) => "error".into(),
            LispError::SignalValue(value) => match value.car() {
                Ok(Value::Symbol(symbol)) => symbol.to_string(),
                _ => "error".into(),
            },
            LispError::ErtTestFailed(_) => "ert-test-failed".into(),
            LispError::Throw(_, _) => "no-catch".into(),
            LispError::VmReturn(_) => {
                unreachable!("bytecode return escaped the VM")
            }
            LispError::Terminate(_) => {
                unreachable!("process termination is non-catchable evaluator control flow")
            }
            LispError::TestSkipped(_) => "ert-test-skipped".into(),
            LispError::EndOfInput => "end-of-file".into(),
            LispError::ReadError(_) => "invalid-read-syntax".into(),
        }
    }
}

impl fmt::Display for LispError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LispError::TypeError(expected, got) => {
                write!(f, "Wrong type argument: {}, {}", expected, got)
            }
            LispError::WrongTypeArgument(predicate, value) => {
                write!(f, "Wrong type argument: {}, {}", predicate, value)
            }
            LispError::Void(name) => write!(
                f,
                "Symbol's value as variable is void: {}",
                render_error_symbol_name(name)
            ),
            LispError::VoidFunction(name) => {
                write!(
                    f,
                    "Symbol's function definition is void: {}",
                    render_error_symbol_name(name)
                )
            }
            LispError::WrongNumberOfArgs(name, n) => {
                write!(f, "Wrong number of arguments: {}, {}", name, n)
            }
            LispError::Signal(msg) => write!(f, "{}", msg),
            LispError::SignalValue(value) => match value.to_vec() {
                Ok(items)
                    if items.len() == 2
                        && matches!(&items[0], Value::Symbol(kind) if kind == "void-variable") =>
                {
                    // Preserve the host diagnostic when Fsymbol_value
                    // carries its original Lisp object instead of a name.
                    write!(f, "Symbol's value as variable is void: {}", items[1])
                }
                Ok(items)
                    if items.len() >= 2
                        && matches!(items.first(), Some(Value::Symbol(kind)) if kind == "search-failed") =>
                {
                    match &items[1] {
                        Value::String(text) => write!(f, "{text:?}"),
                        Value::StringObject(object) => {
                            write!(f, "{:?}", std::cell::RefCell::borrow(object.as_ref()).text)
                        }
                        value => write!(f, "{value}"),
                    }
                }
                Ok(items)
                    if items.len() >= 4
                        && matches!(items.first(), Some(Value::Symbol(kind)) if kind == "file-error" || kind == "file-missing") =>
                {
                    let message = match &items[1] {
                        Value::String(text) => text.as_str(),
                        _ => return write!(f, "{}", value),
                    };
                    let detail = match &items[2] {
                        Value::String(text) => text.as_str(),
                        _ => return write!(f, "{}", value),
                    };
                    let path = match &items[3] {
                        Value::String(text) => text.as_str(),
                        _ => return write!(f, "{}", value),
                    };
                    write!(f, "{}: {}, {}", message, detail, path)
                }
                Ok(items) if items.len() >= 2 => match &items[1] {
                    Value::String(text) => write!(f, "{text}"),
                    Value::StringObject(object) => {
                        write!(f, "{}", std::cell::RefCell::borrow(object.as_ref()).text)
                    }
                    value => write!(f, "{value}"),
                },
                _ => write!(f, "{}", value),
            },
            LispError::ErtTestFailed(msg) => write!(f, "{}", msg),
            LispError::Throw(tag, value) => write!(f, "No catch for {}: {}", tag, value),
            LispError::VmReturn(_) => unreachable!("bytecode return escaped the VM"),
            LispError::Terminate(termination) => {
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
            LispError::TestSkipped(msg) => write!(f, "{}", msg),
            LispError::EndOfInput => write!(f, "End of file during parsing"),
            LispError::ReadError(msg) => write!(f, "Invalid read syntax: {}", msg),
        }
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
        match value {
            Value::Cons(cell) => {
                out.push('(');
                let mut cursor = Value::Cons(cell.clone());
                let mut emitted = 0;
                while let Value::Cons(cell) = &cursor {
                    if emitted >= 8 || out.len() > 2048 {
                        out.push_str(" …");
                        break;
                    }
                    if emitted > 0 {
                        out.push(' ');
                    }
                    render(&cell.car.borrow().clone(), depth - 1, out);
                    emitted += 1;
                    let next = cell.cdr.borrow().clone();
                    match next {
                        Value::Nil => break,
                        Value::Cons(_) => cursor = next,
                        other => {
                            out.push_str(" . ");
                            render(&other, depth - 1, out);
                            break;
                        }
                    }
                }
                out.push(')');
            }
            Value::StringObject(state) => {
                let text: String = std::cell::RefCell::borrow(state).text.clone();
                let mut brief: String = text.chars().take(48).collect();
                if brief.len() < text.len() {
                    brief.push('…');
                }
                out.push('"');
                out.push_str(&brief);
                out.push('"');
            }
            Value::String(text) => {
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
    match error {
        LispError::Signal(message) => format!("Signal({message:?})"),
        LispError::SignalValue(value) => {
            let mut out = String::from("SignalValue(");
            render(value, 6, &mut out);
            out.push(')');
            out
        }
        LispError::Throw(tag, value) => {
            let mut out = String::from("Throw(");
            render(tag, 3, &mut out);
            out.push_str(", ");
            render(value, 4, &mut out);
            out.push(')');
            out
        }
        LispError::WrongTypeArgument(predicate, value) => {
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
        EnvFrame, LispError, SharedCons, SymbolName, Value, census_live_conses, census_live_floats,
        census_live_vectors, make_uninterned_symbol_name, shared_env,
    };
    use std::rc::Rc;

    #[test]
    fn value_fits_in_two_machine_words() {
        assert_eq!(
            std::mem::size_of::<Value>(),
            2 * std::mem::size_of::<usize>(),
            "Value clone and stack traffic depends on the compact two-word representation",
        );
    }

    #[test]
    fn environment_frames_are_one_pointer_shallow_snapshots() {
        assert_eq!(
            std::mem::size_of::<EnvFrame>(),
            std::mem::size_of::<usize>(),
            "environment snapshot traffic depends on a one-pointer frame",
        );

        let frame = EnvFrame::from(vec![("cell".into(), Value::Integer(1))]);
        let mut snapshot = frame.clone();
        assert!(Rc::ptr_eq(&frame.0, &snapshot.0));

        snapshot[0].1 = Value::Integer(2);
        assert!(!Rc::ptr_eq(&frame.0, &snapshot.0));
        assert_eq!(frame[0].1, Value::Integer(1));
        assert_eq!(snapshot[0].1, Value::Integer(2));
    }

    #[test]
    fn cloning_string_reuses_the_text_allocation() {
        let value = Value::string("shared text");
        let clone = value.clone();
        let (Value::String(text), Value::String(cloned_text)) = (&value, &clone) else {
            unreachable!("constructed string values")
        };

        assert!(Rc::ptr_eq(&text.0, &cloned_text.0));
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

        drop(vector);
        let vectors_after_drop = census_live_vectors();
        assert_eq!(vectors_after_drop.count, vectors_before.count);
        assert_eq!(vectors_after_drop.slots, vectors_before.slots);
        assert_eq!(
            vectors_after_drop.representation_conses,
            vectors_before.representation_conses
        );
    }

    #[test]
    fn live_census_counts_float_allocations_once_across_clones() {
        let before = census_live_floats();
        let value = Value::float(1.5);
        let clone = value.clone();
        assert_eq!(census_live_floats(), before + 1);
        drop(value);
        assert_eq!(census_live_floats(), before + 1);
        drop(clone);
        assert_eq!(census_live_floats(), before);
    }

    #[test]
    fn live_census_counts_bignums_and_interpreted_closures_as_gnu_vectors() {
        let before = census_live_vectors();
        let integer = Value::big_integer(num_bigint::BigInt::from(1_u8) << 128);
        let closure = Value::lambda(
            std::rc::Rc::new(Vec::new()),
            std::rc::Rc::new(vec![Value::Nil]),
            shared_env(Vec::new()),
        );
        let after = census_live_vectors();

        assert_eq!(after.count, before.count + 2);
        // Lisp_Bignum is three words.  A noninteractive interpreted closure
        // has three visible slots plus its one-word vector header.
        assert_eq!(after.slots, before.slots + 3 + 4);

        drop(integer);
        drop(closure);
        let after_drop = census_live_vectors();
        assert_eq!(after_drop.count, before.count);
        assert_eq!(after_drop.slots, before.slots);
    }

    #[test]
    fn shared_text_equality_covers_shared_and_distinct_equal_allocations() {
        use std::hash::{Hash, Hasher};

        let shared = super::SharedText::from("same text");
        let clone = shared.clone();
        let distinct = super::SharedText::from("same text");
        let different = super::SharedText::from("different text");

        assert!(Rc::ptr_eq(&shared.0, &clone.0));
        assert!(!Rc::ptr_eq(&shared.0, &distinct.0));
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
        let clone = value.clone();
        let (Value::BigInteger(integer), Value::BigInteger(cloned_integer)) = (&value, &clone)
        else {
            unreachable!("constructed big integer values")
        };

        assert!(Rc::ptr_eq(&integer.0, &cloned_integer.0));
    }

    #[test]
    fn interned_symbol_names_reuse_one_text_allocation() {
        let first = SymbolName::from("emaxx-compact-symbol-test");
        let second = SymbolName::from("emaxx-compact-symbol-test");

        assert!(Rc::ptr_eq(&first.0, &second.0));
    }

    #[test]
    fn uninterned_symbol_names_remain_reclaimable() {
        let weak = {
            let name = SymbolName::from(make_uninterned_symbol_name("temporary", 1));
            Rc::downgrade(&name.0)
        };

        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn uninterned_symbol_keeps_its_supplied_lisp_name() {
        let name = Value::string("temporary");
        let Value::String(expected) = &name else {
            unreachable!("constructed string")
        };
        let expected = expected.clone();
        let symbol = SymbolName::make_uninterned(name, "temporary", 1);
        let Value::String(actual) = symbol.lisp_name() else {
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
    fn cloning_lambda_shares_immutable_parameters() {
        let lambda = Value::lambda(
            vec!["value".into()].into(),
            Vec::new().into(),
            shared_env(Vec::new()),
        );
        let clone = lambda.clone();

        let (Value::Lambda(lambda), Value::Lambda(cloned_lambda)) = (&lambda, &clone) else {
            unreachable!("constructed lambda values")
        };
        assert!(Rc::ptr_eq(lambda, cloned_lambda));
        assert!(Rc::ptr_eq(&lambda.params, &cloned_lambda.params));
    }

    #[test]
    fn cloning_buffer_reuses_the_buffer_descriptor() {
        let buffer = Value::buffer(7, "shared buffer");
        let clone = buffer.clone();
        let (Value::Buffer(buffer), Value::Buffer(cloned_buffer)) = (&buffer, &clone) else {
            unreachable!("constructed buffer values")
        };

        assert!(Rc::ptr_eq(buffer, cloned_buffer));
    }

    #[test]
    fn cons_fields_share_one_cell_and_mutate_independently() {
        assert_eq!(
            std::mem::size_of::<SharedCons>(),
            std::mem::size_of::<usize>(),
            "a Value::Cons must retain exactly one shared pointer",
        );

        let pair = Value::cons(Value::Integer(1), Value::Integer(2));
        let clone = pair.clone();
        let (car, cdr) = pair.cons_cells().expect("constructed cons");
        let (cloned_car, cloned_cdr) = clone.cons_cells().expect("cloned cons");

        assert_eq!(car.cell_id(), cdr.cell_id());
        assert!(!car.ptr_eq(&cdr), "car and cdr are distinct field handles");
        assert!(car.ptr_eq(&cloned_car));
        assert!(cdr.ptr_eq(&cloned_cdr));

        let mut car_value = car.borrow_mut();
        let mut cdr_value = cdr.borrow_mut();
        *car_value = Value::Integer(3);
        *cdr_value = Value::Integer(4);
        drop((car_value, cdr_value));

        assert_eq!(clone.car().expect("car"), Value::Integer(3));
        assert_eq!(clone.cdr().expect("cdr"), Value::Integer(4));
    }

    #[test]
    fn weak_cons_slot_does_not_keep_cell_alive() {
        let weak = {
            let pair = Value::cons(Value::T, Value::Nil);
            let (car, _) = pair.cons_cells().expect("constructed cons");
            car.downgrade()
        };

        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn every_cons_field_mutation_advances_the_shared_epoch() {
        let pair = Value::cons(Value::Integer(1), Value::Integer(2));
        let before_car = super::cons_mutation_epoch();
        pair.set_car(Value::Integer(3)).expect("set car");
        assert_ne!(super::cons_mutation_epoch(), before_car);

        let (_, cdr) = pair.cons_cells().expect("constructed cons");
        let before_cdr = super::cons_mutation_epoch();
        *cdr.borrow_mut() = Value::Integer(4);
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
        let Value::Cons(cell) = &source else {
            unreachable!("constructed cons")
        };
        let snapshot = super::ConsMutationSnapshot::cell(cell);
        let field_ids = super::ConsCell::mutation_field_ids(cell);
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
        let Value::Cons(other_cell) = &other else {
            unreachable!("constructed cons")
        };
        let _other_snapshot = super::ConsMutationSnapshot::cell(other_cell);
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
        let Value::Cons(cell) = &source else {
            unreachable!("constructed cons")
        };
        let field_ids = super::ConsCell::mutation_field_ids(cell);
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
        let source = Value::list([Value::symbol("outer"), nested.clone()]);
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
        let shared = original.clone();
        let distinct = Value::float(f64::NAN);
        let Value::Float(original) = original else {
            unreachable!();
        };
        let Value::Float(shared) = shared else {
            unreachable!();
        };
        let Value::Float(distinct) = distinct else {
            unreachable!();
        };
        assert!(original.ptr_eq(&shared));
        assert!(!original.ptr_eq(&distinct));
        assert_eq!(original.to_bits(), distinct.to_bits());
    }
}
