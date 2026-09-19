//! alloc.c's storage for conses: cells in blocks, a free list threaded
//! through the free cells, a registry of the blocks for the conservative
//! marking of the stack, and the sweep that returns unmarked cells to the
//! free list.
//!
//! A `ConsRef' is `Lisp_Object' for a cons: a pointer the collector keeps
//! valid, copied without a reference count.  The mark bit is the cell's
//! own (`MarkBit', the collection's epoch); a free cell carries
//! `FREE_MARK' and its first word links the free list (alloc.c's
//! `cons_free_list' through `u.s.u.chain').
//!
//! The blocks, the free list, the bump pointer into the newest block and
//! the collection's epoch are the process's, as `cons_block',
//! `cons_free_list', `cons_block_index' and the mark state are plain
//! globals in alloc.c: one Lisp thread runs at a time there (the global
//! lock), and here too (a template interpreter built on one thread is
//! used from another, one at a time, under a lock).  A collection sweeps
//! every block, so it presumes -- as GNU's stop-the-world collector does
//! -- that no other OS thread holds Lisp objects in registers or on its
//! stack while it runs.

use super::types::{ConsCell, MarkBit};
use std::cell::Cell;
use std::ptr::NonNull;
use std::sync::Mutex;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

/// alloc.c's `BLOCK_BYTES': the size of one cons block.  Cells here are
/// wider than `struct Lisp_Cons' (the native words, the borrow flags and
/// the serial ride along until the representation shrinks), so the
/// block is wider too, for the same number of cells per block.
pub(crate) const CONS_BLOCK_BYTES: usize = 1 << 17;
const CELL_SIZE: usize = std::mem::size_of::<ConsCell>();
pub(crate) const CELLS_PER_BLOCK: usize = CONS_BLOCK_BYTES / CELL_SIZE;
const BLOCK_ALIGN: usize = 4096;

/// The mark word of a cell on the free list (alloc.c sets the mark bit
/// of free cells so the sweep skips them; a live cell carries its
/// collection's epoch, which `begin_mark_epoch' never makes this).
pub(crate) const FREE_MARK: u32 = u32::MAX;

/// The blocks, by start address, for `mem_find'.
static BLOCKS: Mutex<Vec<usize>> = Mutex::new(Vec::new());
/// gcstat's `total_conses' and `total_free_conses': raised by allocation
/// on any thread, set by the sweep (the heap is the process's).
static LIVE_CONSES: AtomicUsize = AtomicUsize::new(0);
static FREE_CONSES: AtomicUsize = AtomicUsize::new(0);

/// alloc.c's `cons_free_list', and `cons_block' with `cons_block_index'
/// as the bump region of the newest block: plain globals there, read and
/// written without a lock by the one Lisp thread that runs; relaxed
/// atomics here for the same access (never contended, see above).
static FREE_LIST: AtomicPtr<ConsCell> = AtomicPtr::new(std::ptr::null_mut());
static BUMP_NEXT: AtomicUsize = AtomicUsize::new(0);
static BUMP_END: AtomicUsize = AtomicUsize::new(0);
/// The allocation count, a serial for `WeakConsRef'.
static SERIAL: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// The OS stack region of this thread below the coroutine trampoline
    /// (the region the trampoline's caller uses), scanned with the rest.
    static OS_STACK: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
    /// The stacks of parked Lisp threads (coroutines suspended on this OS
    /// thread): base to saved stack pointer.
    static PARKED_STACKS: std::cell::RefCell<Vec<(usize, usize)>> = const { std::cell::RefCell::new(Vec::new()) };
    /// The regions of the stacks that resumed a coroutine and wait for it
    /// (alloc.c marks every thread's stack; here the driver's frames are
    /// another region of live words): stack pointer at the resume and the
    /// base of that stack.
    static DRIVER_REGIONS: std::cell::RefCell<Vec<(usize, usize)>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Heap buffers of Lisp values that a Rust frame holds across a call
/// into Lisp (lisp.h's SAFE_ALLOCA_LISP, recorded on the specpdl so
/// `mark_specpdl' reaches it): the buffer's address and size in bytes,
/// scanned conservatively with the stacks.  The process's, as the
/// specpdl entries are reachable from any thread's collection (a
/// template's buffers registered on one thread, dropped on another).
struct HeapRoots {
    regions: Vec<Option<(usize, usize)>>,
    free_slots: Vec<usize>,
}

static HEAP_ROOTS: Mutex<HeapRoots> = Mutex::new(HeapRoots {
    regions: Vec::new(),
    free_slots: Vec::new(),
});

fn heap_roots() -> std::sync::MutexGuard<'static, HeapRoots> {
    HEAP_ROOTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A vector not yet registered (no buffer to scan).
const UNREGISTERED: usize = usize::MAX;

/// A vector of values (or of anything holding values) whose buffer the
/// collector scans while the vector lives: lisp.h's SAFE_ALLOCA_LISP.  A
/// `Vec' on the Rust heap is invisible to the stack scan, so a list of
/// forms read ahead of their evaluation, or the results a primitive
/// gathers between calls into Lisp, live here.  The buffer past the
/// length is kept zero (SAFE_ALLOCA_LISP's `memclear'; the specpdl is
/// marked up to `specpdl_ptr'), so the scan of the whole buffer reads
/// no word an earlier owner of the memory, or a popped element, left.
pub struct RootedVec<T> {
    inner: Vec<T>,
    slot: usize,
    /// The registered region (address, bytes): the registry is written
    /// only when the buffer moves or grows.
    region: (usize, usize),
}

impl<T> RootedVec<T> {
    pub fn new() -> Self {
        Self::from_vec(Vec::new())
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self::from_vec(Vec::with_capacity(capacity))
    }

    pub fn from_vec(inner: Vec<T>) -> Self {
        let mut rooted = Self {
            inner,
            slot: UNREGISTERED,
            region: (0, 0),
        };
        rooted.refresh();
        rooted
    }

    /// The registration follows the buffer (its whole capacity, scanned
    /// conservatively, so a push within it changes nothing); an empty
    /// vector without a buffer registers nothing (most environments stay
    /// empty).
    #[inline]
    fn refresh(&mut self) {
        if self.inner.capacity() == 0 {
            return;
        }
        let region = (
            self.inner.as_ptr() as usize,
            self.inner.capacity() * std::mem::size_of::<T>(),
        );
        if region != self.region {
            self.register(region);
        }
    }

    /// Zero the buffer past the length.
    #[inline]
    fn clear_spare(&mut self) {
        let len = self.inner.len();
        let spare = self.inner.capacity() - len;
        if spare != 0 {
            // SAFETY: the allocated buffer past the initialized elements,
            // which no element of a `Vec' occupies.
            unsafe {
                std::ptr::write_bytes(
                    self.inner.as_mut_ptr().add(len).cast::<u8>(),
                    0,
                    spare * std::mem::size_of::<T>(),
                );
            }
        }
    }

    #[inline(never)]
    fn register(&mut self, region: (usize, usize)) {
        self.region = region;
        self.clear_spare();
        let mut roots = heap_roots();
        if self.slot == UNREGISTERED {
            self.slot = match roots.free_slots.pop() {
                Some(slot) => slot,
                None => {
                    roots.regions.push(None);
                    roots.regions.len() - 1
                }
            };
        }
        roots.regions[self.slot] = Some(region);
    }

    fn unregister(slot: usize) {
        if slot == UNREGISTERED {
            return;
        }
        let mut roots = heap_roots();
        roots.regions[slot] = None;
        roots.free_slots.push(slot);
    }

    pub fn push(&mut self, value: T) {
        self.inner.push(value);
        self.refresh();
    }

    pub fn extend(&mut self, values: impl IntoIterator<Item = T>) {
        self.inner.extend(values);
        self.refresh();
    }

    pub fn pop(&mut self) -> Option<T> {
        let value = self.inner.pop()?;
        self.clear_popped(1);
        Some(value)
    }

    /// Zero the COUNT slots past the length that were just vacated.
    #[inline]
    fn clear_popped(&mut self, count: usize) {
        if self.slot == UNREGISTERED {
            return;
        }
        // SAFETY: slots inside the buffer past the initialized elements.
        unsafe {
            std::ptr::write_bytes(
                self.inner.as_mut_ptr().add(self.inner.len()).cast::<u8>(),
                0,
                count * std::mem::size_of::<T>(),
            );
        }
    }

    pub fn clear(&mut self) {
        let len = self.inner.len();
        self.inner.clear();
        self.clear_popped(len);
    }

    pub fn truncate(&mut self, len: usize) {
        let before = self.inner.len();
        self.inner.truncate(len);
        self.clear_popped(before - self.inner.len());
    }

    pub fn insert(&mut self, index: usize, value: T) {
        self.inner.insert(index, value);
        self.refresh();
    }

    pub fn remove(&mut self, index: usize) -> T {
        let value = self.inner.remove(index);
        self.clear_popped(1);
        value
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        self.inner.as_mut_slice()
    }

    pub fn retain(&mut self, keep: impl FnMut(&T) -> bool) {
        let before = self.inner.len();
        self.inner.retain(keep);
        self.clear_popped(before - self.inner.len());
    }

    pub fn sort_by(&mut self, compare: impl FnMut(&T, &T) -> std::cmp::Ordering) {
        self.inner.sort_by(compare);
    }

    pub fn reverse(&mut self) {
        self.inner.reverse();
    }

    /// The drained elements, taken at once (the slots they leave are
    /// zeroed before this returns).
    pub fn drain(&mut self, range: impl std::ops::RangeBounds<usize>) -> std::vec::IntoIter<T> {
        let before = self.inner.len();
        let drained = self.inner.drain(range).collect::<Vec<_>>();
        self.clear_popped(before - self.inner.len());
        drained.into_iter()
    }

    /// The plain vector: no longer scanned.  For a value that is consumed
    /// at once, or whose elements are immediates.
    pub fn into_inner(mut self) -> Vec<T> {
        std::mem::take(&mut self.inner)
    }
}

impl<T> Default for RootedVec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone> Clone for RootedVec<T> {
    fn clone(&self) -> Self {
        Self::from_vec(self.inner.clone())
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for RootedVec<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: PartialEq> PartialEq for RootedVec<T> {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<T: PartialEq> PartialEq<Vec<T>> for RootedVec<T> {
    fn eq(&self, other: &Vec<T>) -> bool {
        self.inner == *other
    }
}

impl<T> Drop for RootedVec<T> {
    fn drop(&mut self) {
        Self::unregister(self.slot);
    }
}

impl<T> std::ops::Deref for RootedVec<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        &self.inner
    }
}

impl<T> std::ops::DerefMut for RootedVec<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        &mut self.inner
    }
}

impl<T> From<Vec<T>> for RootedVec<T> {
    fn from(inner: Vec<T>) -> Self {
        Self::from_vec(inner)
    }
}

impl<T> FromIterator<T> for RootedVec<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::from_vec(iter.into_iter().collect())
    }
}

/// Iteration by value keeps the buffer registered until the iterator is
/// dropped (the values moved out leave their words behind, which the
/// scan still reads: harmless over-marking).
pub struct RootedIntoIter<T> {
    iter: std::vec::IntoIter<T>,
    slot: usize,
}

impl<T> Iterator for RootedIntoIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        self.iter.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<T> DoubleEndedIterator for RootedIntoIter<T> {
    fn next_back(&mut self) -> Option<T> {
        self.iter.next_back()
    }
}

impl<T> ExactSizeIterator for RootedIntoIter<T> {}

impl<T> Drop for RootedIntoIter<T> {
    fn drop(&mut self) {
        RootedVec::<T>::unregister(self.slot);
    }
}

impl<T> IntoIterator for RootedVec<T> {
    type Item = T;
    type IntoIter = RootedIntoIter<T>;
    fn into_iter(self) -> RootedIntoIter<T> {
        let mut this = std::mem::ManuallyDrop::new(self);
        let inner = std::mem::take(&mut this.inner);
        RootedIntoIter {
            iter: inner.into_iter(),
            slot: this.slot,
        }
    }
}

impl<'a, T> IntoIterator for &'a RootedVec<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> std::slice::Iter<'a, T> {
        self.inner.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut RootedVec<T> {
    type Item = &'a mut T;
    type IntoIter = std::slice::IterMut<'a, T>;
    fn into_iter(self) -> std::slice::IterMut<'a, T> {
        self.inner.iter_mut()
    }
}

/// The interpreter states alive in the process, by the address of their
/// boxed state: a collection marks every one of them (they may share
/// objects, and a template interpreter built for the tests outlives the
/// clones that collect).
static LIVE_STATES: Mutex<Vec<usize>> = Mutex::new(Vec::new());

pub(crate) fn register_state(state: usize) {
    let mut states = LIVE_STATES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if !states.contains(&state) {
        states.push(state);
    }
}

pub(crate) fn unregister_state(state: usize) {
    let mut states = LIVE_STATES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    states.retain(|&existing| existing != state);
}

pub(crate) fn live_states() -> Vec<usize> {
    LIVE_STATES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

/// `EMAXX_GC_STRESS': collect at every `maybe_gc', to find a reference
/// the collector cannot see.
pub(crate) fn stress_collections() -> bool {
    static STRESS: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *STRESS.get_or_init(|| std::env::var_os("EMAXX_GC_STRESS").is_some())
}

/// A coroutine parks: its stack from BASE down to SP holds live words
/// until it resumes.
pub(crate) fn note_parked_stack(base: usize, sp: usize) {
    PARKED_STACKS.with_borrow_mut(|stacks| {
        stacks.retain(|&(existing, _)| existing != base);
        stacks.push((base, sp));
    });
}

pub(crate) fn forget_parked_stack(base: usize) {
    PARKED_STACKS.with_borrow_mut(|stacks| stacks.retain(|&(existing, _)| existing != base));
}

pub(crate) fn push_driver_region(sp: usize, base: usize) {
    DRIVER_REGIONS.with_borrow_mut(|regions| regions.push((sp, base)));
}

pub(crate) fn pop_driver_region() {
    DRIVER_REGIONS.with_borrow_mut(|regions| {
        regions.pop();
    });
}

/// The base (highest address) of this OS thread's own stack.
#[cfg(target_os = "macos")]
pub(crate) fn os_stack_base() -> Option<usize> {
    // SAFETY: a query about the calling thread.
    let base = unsafe { libc::pthread_get_stackaddr_np(libc::pthread_self()) } as usize;
    (base != 0).then_some(base)
}

#[cfg(target_os = "linux")]
pub(crate) fn os_stack_base() -> Option<usize> {
    // SAFETY: the pthread attribute calls on the calling thread, the
    // attribute object destroyed after the read.
    unsafe {
        let mut attributes = std::mem::MaybeUninit::<libc::pthread_attr_t>::uninit();
        if libc::pthread_getattr_np(libc::pthread_self(), attributes.as_mut_ptr()) != 0 {
            return None;
        }
        let mut attributes = attributes.assume_init();
        let mut start = std::ptr::null_mut();
        let mut size = 0;
        let status = libc::pthread_attr_getstack(&attributes, &mut start, &mut size);
        libc::pthread_attr_destroy(&mut attributes);
        (status == 0).then(|| start as usize + size)
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub(crate) fn os_stack_base() -> Option<usize> {
    None
}

/// alloc.c:mark_stack and mark_threads: the running stack, the OS stack
/// region below the trampoline, the parked coroutines' stacks and the
/// regions of the stacks waiting on a resumed coroutine.
#[inline(never)]
pub(crate) fn mark_all_stacks(current_base: Option<usize>, mut mark: impl FnMut(ConsRef)) {
    let base = current_base.or_else(os_stack_base);
    mark_stack(base, &mut mark);
    let parked = PARKED_STACKS.with_borrow(Clone::clone);
    for (base, sp) in parked {
        if base > sp {
            // SAFETY: a parked coroutine's stack stays mapped and unchanged
            // until it resumes, which cannot happen during a collection.
            unsafe { scan_words(sp, base, &mut mark) };
        }
    }
    let drivers = DRIVER_REGIONS.with_borrow(Clone::clone);
    for (sp, base) in drivers {
        if base > sp {
            // SAFETY: the driving stack's frames wait below the resume
            // call for the whole life of the coroutine's run.
            unsafe { scan_words(sp, base, &mut mark) };
        }
    }
    // SAFE_ALLOCA_LISP's buffers.
    let heap = heap_roots()
        .regions
        .iter()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    for (address, bytes) in heap {
        if bytes != 0 {
            // SAFETY: a registered buffer is a live allocation of that size
            // (its owner refreshes the entry when the buffer moves).
            unsafe { scan_words(address, address + bytes, &mut mark) };
        }
    }
}

/// `Lisp_Object' for a cons: the cell's address, copied freely, valid
/// while the collector can reach the cell.
#[repr(transparent)]
pub struct ConsRef(NonNull<ConsCell>);

impl Clone for ConsRef {
    #[inline(always)]
    fn clone(&self) -> Self {
        *self
    }
}

impl Copy for ConsRef {}

impl ConsRef {
    #[inline(always)]
    pub(crate) fn as_ptr(&self) -> *const ConsCell {
        self.0.as_ptr()
    }

    #[inline(always)]
    pub(crate) fn ptr_eq(a: &Self, b: &Self) -> bool {
        a.0 == b.0
    }

    /// The handle for a cell the allocator handed out (the loader's and
    /// the native runtime's cells included).
    ///
    /// # Safety
    /// CELL must be a cell of a cons block that is allocated.
    pub(crate) unsafe fn from_raw(cell: *const ConsCell) -> Self {
        // SAFETY: the caller's contract.
        Self(unsafe { NonNull::new_unchecked(cell.cast_mut()) })
    }

    /// A weak handle: valid until the cell is swept, told apart from a
    /// later cell in the same slot by the serial.
    pub(crate) fn downgrade(&self) -> WeakConsRef {
        WeakConsRef {
            cell: self.0,
            serial: self.serial,
        }
    }
}

impl std::ops::Deref for ConsRef {
    type Target = ConsCell;
    #[inline(always)]
    fn deref(&self) -> &ConsCell {
        // SAFETY: a `ConsRef' is only made for an allocated cell, and the
        // collector frees a cell only when nothing reaches it (the stack
        // and registers scanned conservatively).
        let cell = unsafe { self.0.as_ref() };
        // GC_CHECK_MARKED_OBJECTS' spirit: a reference the collector could
        // not see is caught at its first use, in a checked build.
        debug_assert!(
            cell.mark.raw() != FREE_MARK,
            "use of a cons the collector freed"
        );
        cell
    }
}

impl std::fmt::Debug for ConsRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, f)
    }
}

impl PartialEq for ConsRef {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for ConsRef {}

impl std::hash::Hash for ConsRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (self.0.as_ptr() as usize).hash(state);
    }
}

/// A reference that does not keep the cell: `upgrade' answers while the
/// cell is allocated and still the same allocation.
#[derive(Clone, Debug)]
pub struct WeakConsRef {
    cell: NonNull<ConsCell>,
    serial: u64,
}

impl WeakConsRef {
    /// A weak handle from a cell's address and serial (a test keeps them
    /// hidden from the conservative scan).
    #[cfg(test)]
    pub(crate) fn from_parts(address: usize, serial: u64) -> Self {
        Self {
            cell: NonNull::new(address as *mut ConsCell).expect("a cell address"),
            serial,
        }
    }

    pub(crate) fn upgrade(&self) -> Option<ConsRef> {
        let address = self.cell.as_ptr() as usize;
        // SAFETY: the address was a cell's; `mem_find' checks it still
        // names an allocated cell before anything reads it.
        let live = unsafe { mem_find(address) }?;
        if live as usize != address {
            return None;
        }
        // SAFETY: `mem_find' answered an allocated cell.
        let cell = unsafe { &*live };
        (cell.serial == self.serial).then_some(ConsRef(self.cell))
    }
}

struct FreeCell {
    next: *mut ConsCell,
}

/// alloc.c:Fcons's storage: the next free cell, or a cell of a new block.
#[inline]
pub(crate) fn allocate_cons(cell: ConsCell) -> ConsRef {
    // alloc.c:Fcons: the free list's head, else the next cell of the
    // newest block.
    let head = FREE_LIST.load(Ordering::Relaxed);
    let slot = if head.is_null() {
        bump_cell()
    } else {
        // SAFETY: a free cell's first word is the free-list link.
        let next = unsafe { (*head.cast::<FreeCell>()).next };
        FREE_LIST.store(next, Ordering::Relaxed);
        FREE_CONSES.store(
            FREE_CONSES.load(Ordering::Relaxed).saturating_sub(1),
            Ordering::Relaxed,
        );
        head
    };
    let serial = SERIAL.load(Ordering::Relaxed) + 1;
    SERIAL.store(serial, Ordering::Relaxed);
    LIVE_CONSES.store(LIVE_CONSES.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
    // SAFETY: SLOT is an uninitialized (free) cell of a block; every
    // field is written before a handle is made.
    unsafe {
        std::ptr::write(slot, cell);
        (*slot).serial = serial;
        // A cell born during a collection (the weak-table sweep conses
        // between the mark and the sweep) carries the collection's epoch
        // and survives its sweep; born outside one, it carries the last
        // epoch, which the next collection does not reuse.
        (*slot).mark.set_raw(super::types::current_mark_epoch());
        ConsRef(NonNull::new_unchecked(slot))
    }
}

fn bump_cell() -> *mut ConsCell {
    let next = BUMP_NEXT.load(Ordering::Relaxed);
    let end = BUMP_END.load(Ordering::Relaxed);
    if next < end {
        BUMP_NEXT.store(next + CELL_SIZE, Ordering::Relaxed);
        return next as *mut ConsCell;
    }
    let block = new_block();
    BUMP_NEXT.store(block + CELL_SIZE, Ordering::Relaxed);
    BUMP_END.store(block + CELLS_PER_BLOCK * CELL_SIZE, Ordering::Relaxed);
    block as *mut ConsCell
}

/// alloc.c's `lisp_align_free' of a cons block every cell of which is
/// free (the sweep found nothing live in it).
fn release_block(start: usize) {
    let mut blocks = BLOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let position = blocks.partition_point(|&existing| existing < start);
    debug_assert_eq!(blocks.get(position), Some(&start));
    blocks.remove(position);
    drop(blocks);
    let layout = std::alloc::Layout::from_size_align(CONS_BLOCK_BYTES, BLOCK_ALIGN)
        .expect("cons block layout");
    // SAFETY: a block `new_block' allocated with this layout, unregistered
    // above; every cell is free (nothing reaches it).
    unsafe { std::alloc::dealloc(start as *mut u8, layout) };
}

/// alloc.c's `lisp_align_malloc' of a cons block: every cell free.
fn new_block() -> usize {
    let layout = std::alloc::Layout::from_size_align(CONS_BLOCK_BYTES, BLOCK_ALIGN)
        .expect("cons block layout");
    // SAFETY: a non-zero layout.
    let block = unsafe { std::alloc::alloc(layout) };
    assert!(!block.is_null(), "out of memory for a cons block");
    let start = block as usize;
    for index in 0..CELLS_PER_BLOCK {
        let cell = (start + index * CELL_SIZE) as *mut ConsCell;
        // SAFETY: inside the block just allocated; only the mark word is
        // written, at its field offset.
        unsafe { mark_of(cell).set_raw(FREE_MARK) };
    }
    let mut blocks = BLOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let position = blocks.partition_point(|&existing| existing < start);
    blocks.insert(position, start);
    start
}

/// The cell's mark word, readable whether the cell is live or free.
///
/// # Safety
/// CELL must be a cell address inside a cons block.
unsafe fn mark_of<'a>(cell: *mut ConsCell) -> &'a MarkBit {
    // SAFETY: the mark word of a free cell is kept written; its offset is
    // fixed by the `repr(C)' layout.
    unsafe {
        &*(cell
            .cast::<u8>()
            .add(std::mem::offset_of!(ConsCell, mark))
            .cast::<MarkBit>())
    }
}

/// alloc.c:live_cons_holding: the allocated cell containing ADDRESS, or
/// None when ADDRESS is not inside a live cell of a cons block.
///
/// # Safety
/// The blocks registry is read under its lock; the cell's mark word is
/// readable in every state.
pub(crate) unsafe fn mem_find(address: usize) -> Option<*mut ConsCell> {
    let blocks = BLOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let position = blocks.partition_point(|&start| start <= address);
    let start = *blocks.get(position.checked_sub(1)?)?;
    drop(blocks);
    let offset = address.checked_sub(start)?;
    let index = offset / CELL_SIZE;
    if index >= CELLS_PER_BLOCK {
        return None;
    }
    let cell = (start + index * CELL_SIZE) as *mut ConsCell;
    // SAFETY: inside a registered block.
    let mark = unsafe { mark_of(cell) }.raw();
    if mark == FREE_MARK {
        return None;
    }
    // A cell the bump pointer has not reached is free too (its mark is
    // FREE_MARK from the block's birth), so nothing more to check.
    Some(cell)
}

/// The serial of the allocated cell at ADDRESS (a cell's address), or
/// None when the sweep has freed it (or it was never a cell): for a
/// holder that keeps a cell's address across a collection without
/// keeping the cell (the native heap's views).
pub(crate) fn allocated_serial(address: usize) -> Option<u64> {
    // SAFETY: `mem_find' answers an allocated cell, whose serial is
    // readable.
    unsafe { mem_find(address).map(|cell| (*cell).serial) }
}

/// The blocks, for the sweep.
fn all_blocks() -> Vec<usize> {
    BLOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

/// alloc.c:sweep_conses: every cell not marked in EPOCH is dropped and
/// put on the free list, which is rebuilt from every free cell as GNU's
/// is (`cons_free_list = 0' first), so that a block holding nothing but
/// free cells can be given back once more than a block's worth of free
/// cells is at hand.  The newest block is swept up to its bump pointer
/// (`cons_block_index'); the cells past it were never allocated.
/// Returns the number of live conses (gcstat's `total_conses').
pub(crate) fn sweep_conses(epoch: u32) -> usize {
    super::types::set_sweep_pending(false);
    if verify_heap_enabled() {
        verify_marking(epoch);
    }
    let bump_next = BUMP_NEXT.load(Ordering::Relaxed);
    let bump_end = BUMP_END.load(Ordering::Relaxed);
    let mut free_list: *mut ConsCell = std::ptr::null_mut();
    let mut num_free = 0usize;
    let mut num_used = 0usize;
    let mut released = Vec::new();
    for start in all_blocks() {
        let lim = if bump_end == start + CELLS_PER_BLOCK * CELL_SIZE {
            (bump_next - start) / CELL_SIZE
        } else {
            CELLS_PER_BLOCK
        };
        let chain_before = free_list;
        let mut this_free = 0usize;
        for index in 0..lim {
            let cell = (start + index * CELL_SIZE) as *mut ConsCell;
            // SAFETY: inside a registered block; the mark word is
            // readable in every state.
            let mark = unsafe { mark_of(cell) }.raw();
            if mark == epoch {
                num_used += 1;
                continue;
            }
            if mark != FREE_MARK {
                // SAFETY: an allocated, unmarked cell: nothing reaches
                // it, so its fields are dropped and the slot becomes free
                // (the free mark, the link in its first word).
                unsafe {
                    std::ptr::drop_in_place(cell);
                    if cfg!(debug_assertions) {
                        poison(cell);
                    }
                    mark_of(cell).set_raw(FREE_MARK);
                }
            }
            this_free += 1;
            // SAFETY: a free cell's first word is the free-list link.
            unsafe { (*cell.cast::<FreeCell>()).next = free_list };
            free_list = cell;
        }
        if this_free == CELLS_PER_BLOCK && num_free > CELLS_PER_BLOCK {
            // Unhook the block's cells (the ones just linked) and give
            // the block back.
            free_list = chain_before;
            released.push(start);
        } else {
            num_free += this_free;
        }
    }
    FREE_LIST.store(free_list, Ordering::Relaxed);
    for start in released {
        release_block(start);
    }
    LIVE_CONSES.store(num_used, Ordering::Relaxed);
    FREE_CONSES.store(num_free, Ordering::Relaxed);
    if verify_heap_enabled() {
        verify_heap();
    }
    num_used
}

/// `EMAXX_GC_VERIFY': the two whole-heap checks around every sweep
/// (GC_CHECK_MARKED_OBJECTS' spirit; a walk of every block twice per
/// collection, so opt-in).
fn verify_heap_enabled() -> bool {
    static VERIFY: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *VERIFY.get_or_init(|| std::env::var_os("EMAXX_GC_VERIFY").is_some())
}

/// Before the sweep, in a checked build: every cell a marked cell names is
/// marked too, or the mark phase reached the first without tracing it (or
/// its field was written after the marking, from a reference the phase
/// could not see).  The report names both cells and their words.
fn verify_marking(epoch: u32) {
    for start in all_blocks() {
        for index in 0..CELLS_PER_BLOCK {
            let cell = (start + index * CELL_SIZE) as *mut ConsCell;
            // SAFETY: inside a registered block.
            if unsafe { mark_of(cell) }.raw() != epoch {
                continue;
            }
            // SAFETY: an allocated, marked cell.
            let live = unsafe { &*cell };
            for (which, field) in [("car", &live.car), ("cdr", &live.cdr)] {
                let value = field.value_in_place();
                if let super::types::Value::Cons(target) = &*value {
                    // SAFETY: the pointer came from a marked cell; its
                    // words are readable in every state.
                    let target_mark = unsafe { mark_of(target.as_ptr().cast_mut()) }.raw();
                    if target_mark != epoch && target_mark != FREE_MARK {
                        let dead = &**target;
                        let mut with_target_mark = 0usize;
                        let mut with_epoch = 0usize;
                        for start in all_blocks() {
                            for index in 0..CELLS_PER_BLOCK {
                                // SAFETY: inside a registered block.
                                let mark = unsafe {
                                    mark_of((start + index * CELL_SIZE) as *mut ConsCell)
                                }
                                .raw();
                                if mark == target_mark {
                                    with_target_mark += 1;
                                } else if mark == epoch {
                                    with_epoch += 1;
                                }
                            }
                        }
                        let words = |address: usize| -> Vec<String> {
                            (0..CELL_SIZE / 8)
                                // SAFETY: inside a cell of a block.
                                .map(|i| {
                                    format!("{:#x}", unsafe {
                                        *((address + i * 8) as *const usize)
                                    })
                                })
                                .collect()
                        };
                        eprintln!(
                            "live cell words: {:?}\ndead cell words: {:?}\nmark offset {} serial offset {} cell size {}",
                            words(cell as usize),
                            words(target.as_ptr() as usize),
                            std::mem::offset_of!(ConsCell, mark),
                            std::mem::offset_of!(ConsCell, serial),
                            CELL_SIZE
                        );
                        panic!(
                            "before the sweep of epoch {epoch} (the counter reads {}; {with_epoch} cells carry {epoch}, {with_target_mark} carry {target_mark}), marked cons {:#x} (serial {}, car {}, cdr {}) holds unmarked cons {:#x} (serial {}, mark {}, car {}, cdr {}) in its {which}",
                            super::types::current_mark_epoch(),
                            cell as usize,
                            live.serial,
                            describe(&live.car.value_in_place()),
                            describe(&live.cdr.value_in_place()),
                            target.as_ptr() as usize,
                            dead.serial,
                            target_mark,
                            describe(&dead.car.value_in_place()),
                            describe(&dead.cdr.value_in_place()),
                        );
                    }
                }
            }
        }
    }
}

/// A word of a cell, for the reports: its kind, a symbol's name, an
/// integer, a cons's address.
fn describe(value: &super::types::Value) -> String {
    use super::types::Value;
    match value {
        Value::Symbol(symbol) => format!("symbol {}", symbol.as_str()),
        Value::Integer(n) => format!("integer {n}"),
        Value::Cons(cell) => format!("cons {:#x}", cell.as_ptr() as usize),
        Value::String(text) => format!(
            "string {:?}",
            text.as_str().chars().take(24).collect::<String>()
        ),
        other => other.type_name().to_string(),
    }
}

/// GC_CHECK_MARKED_OBJECTS' spirit after a sweep: no allocated cell names
/// a freed one.  A live cell that does means a reference the mark phase
/// could not see was written into it, or was reached but not traced.
fn verify_heap() {
    for start in all_blocks() {
        for index in 0..CELLS_PER_BLOCK {
            let cell = (start + index * CELL_SIZE) as *mut ConsCell;
            // SAFETY: inside a registered block.
            if unsafe { mark_of(cell) }.raw() == FREE_MARK {
                continue;
            }
            // SAFETY: an allocated cell.
            let live = unsafe { &*cell };
            for (which, field) in [("car", &live.car), ("cdr", &live.cdr)] {
                let value = field.value_in_place();
                if let super::types::Value::Cons(target) = *value {
                    // SAFETY: the pointer came from a live cell; only its
                    // mark word is read.
                    if unsafe { mark_of(target.as_ptr().cast_mut()) }.raw() == FREE_MARK {
                        panic!(
                            "after the sweep, live cons {:#x} (serial {}) holds freed cons {:#x} in its {which}",
                            cell as usize,
                            live.serial,
                            target.as_ptr() as usize
                        );
                    }
                }
            }
        }
    }
}

/// Fill a freed cell's words (past the free-list link) with a pattern a
/// use after free trips over.
///
/// # Safety
/// CELL is a cell of a block that has just been dropped.
unsafe fn poison(cell: *mut ConsCell) {
    // SAFETY: the caller's contract; the mark word is rewritten after.
    unsafe {
        let bytes = cell.cast::<u8>();
        std::ptr::write_bytes(
            bytes.add(std::mem::size_of::<FreeCell>()),
            0xA5,
            CELL_SIZE - std::mem::size_of::<FreeCell>(),
        );
    }
}

/// For tests: overwrite the stack below the caller's frame, so a word a
/// callee left behind (a cons handle in a dead frame or a register
/// spill) does not keep its cons through the conservative scan of the
/// collection that follows.  The caller's own frame is untouched: an
/// object it still holds in a local stays reachable, as a C local's
/// object does.  Eight megabytes: deeper than the collection that
/// follows, whose own frames' unwritten slots the scan reads too, so a
/// word an earlier collection spilled at that depth is gone.
#[cfg(test)]
#[inline(never)]
pub(crate) fn clobber_stack() {
    let mut scratch = std::mem::MaybeUninit::<[usize; 1 << 20]>::uninit();
    // SAFETY: a write of every word of the array on this frame.
    unsafe {
        std::ptr::write_bytes(scratch.as_mut_ptr().cast::<u8>(), 1, 8 << 20);
    }
    std::hint::black_box(&scratch);
}

pub(crate) fn live_conses() -> usize {
    LIVE_CONSES.load(Ordering::Relaxed)
}

/// gcstat's `total_free_conses'.
#[allow(dead_code)]
pub(crate) fn free_conses() -> usize {
    FREE_CONSES.load(Ordering::Relaxed)
}

/// Record the OS stack region this thread uses outside the coroutine
/// trampoline: from the trampoline's entry down to BASE (the highest
/// address of the thread's stack that may hold a value).
pub(crate) fn note_os_stack(entry_sp: usize, base: usize) {
    OS_STACK.with(|region| region.set((entry_sp, base)));
}

thread_local! {
    /// `current_thread->stack_top': the frame `flush_stack_call_func'
    /// recorded at the collection's entry, zero outside a collection.
    static STACK_TOP: Cell<usize> = const { Cell::new(0) };
}

/// alloc.c:flush_stack_call_func: the callee-saved registers spilled into
/// this frame and its address recorded as the thread's `stack_top';
/// BODY -- the collection -- runs below it, so the words its own frames
/// hold, and what an earlier collection left at the same depth, are not
/// read as roots.  Nested (the heap's collect under the runtime's
/// entry), the outer record stands.
#[inline(never)]
pub(crate) fn flush_stack_call_func<R>(body: impl FnOnce() -> R) -> R {
    struct Restore(usize);
    impl Drop for Restore {
        fn drop(&mut self) {
            STACK_TOP.with(|top| top.set(self.0));
        }
    }
    let mut spill = [0usize; 16];
    spill_registers(&mut spill);
    let previous = STACK_TOP.with(Cell::get);
    let _restore = Restore(previous);
    if previous == 0 {
        STACK_TOP.with(|top| top.set(spill.as_ptr() as usize));
    }
    let result = body();
    std::hint::black_box(&spill);
    result
}

/// The recorded `stack_top' of the running collection, or zero.
pub(crate) fn stack_top() -> usize {
    STACK_TOP.with(Cell::get)
}

/// alloc.c:mark_stack for the running thread: the callee-saved registers
/// spilled into a local array, then every word from the current stack
/// pointer to the stack's base (the coroutine's, and the OS stack region
/// below the trampoline) that names an allocated cell marks it.  MARK is
/// called with each such cell.
#[inline(never)]
pub(crate) fn mark_stack(base: Option<usize>, mut mark: impl FnMut(ConsRef)) {
    // alloc.c:mark_c_stack from the thread's `stack_top' (the collection's
    // entry, registers spilled there); a census outside a collection
    // starts at this frame, with the registers spilled here
    // (__builtin_unwind_init).
    let mut spill = [0usize; 16];
    let mut low = stack_top();
    if low == 0 {
        spill_registers(&mut spill);
        low = spill.as_ptr() as usize;
    }
    if let Some(base) = base
        && base > low
    {
        // SAFETY: the words between a live frame and the stack base are
        // this thread's stack, readable in full.
        unsafe { scan_words(low, base, &mut mark) };
    }
    let (entry_sp, os_base) = OS_STACK.with(Cell::get);
    if entry_sp != 0 && os_base > entry_sp {
        // SAFETY: the region the trampoline's caller uses, recorded at
        // entry; it stays mapped while the thread runs.
        unsafe { scan_words(entry_sp, os_base, &mut mark) };
    }
    std::hint::black_box(&spill);
}

/// Mark every cell a word of [LOW, HIGH) names (a parked coroutine's
/// stack, given its saved stack pointer and base).
///
/// # Safety
/// [LOW, HIGH) must be readable memory.
pub(crate) unsafe fn scan_words(low: usize, high: usize, mark: &mut impl FnMut(ConsRef)) {
    let low = low & !(std::mem::size_of::<usize>() - 1);
    let mut address = low;
    while address + std::mem::size_of::<usize>() <= high {
        // SAFETY: the caller's contract.
        let word = unsafe { std::ptr::read_volatile(address as *const usize) };
        address += std::mem::size_of::<usize>();
        // A reference into a cell is word-aligned (every field a reference
        // can name is); a word that names an odd byte inside one is a
        // stale pointer whose low bytes a narrower store overwrote, and
        // is not taken.  (alloc.c's live_cons_holding takes any byte of
        // a 16-byte cons; the cell here is seven times as wide.)
        if word & (std::mem::size_of::<usize>() - 1) != 0 {
            continue;
        }
        // SAFETY: `mem_find' validates the word before any cell is read.
        if let Some(cell) = unsafe { mem_find(word) } {
            // SAFETY: an allocated cell.
            mark(unsafe { ConsRef::from_raw(cell) });
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn spill_registers(spill: &mut [usize; 16]) {
    // SAFETY: reads of the callee-saved registers into the array.
    unsafe {
        std::arch::asm!(
            "mov [{s}], rbx",
            "mov [{s} + 8], rbp",
            "mov [{s} + 16], r12",
            "mov [{s} + 24], r13",
            "mov [{s} + 32], r14",
            "mov [{s} + 40], r15",
            s = in(reg) spill.as_mut_ptr(),
            options(nostack, preserves_flags)
        );
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn spill_registers(spill: &mut [usize; 16]) {
    // SAFETY: reads of the callee-saved registers into the array.
    unsafe {
        std::arch::asm!(
            "stp x19, x20, [{s}]",
            "stp x21, x22, [{s}, #16]",
            "stp x23, x24, [{s}, #32]",
            "stp x25, x26, [{s}, #48]",
            "stp x27, x28, [{s}, #64]",
            "str x29, [{s}, #80]",
            s = in(reg) spill.as_mut_ptr(),
            options(nostack, preserves_flags)
        );
    }
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline(always)]
fn spill_registers(_spill: &mut [usize; 16]) {}

/// The current stack pointer, approximately: the address of a local.
#[inline(never)]
pub(crate) fn approximate_stack_pointer() -> usize {
    let marker = 0u8;
    std::hint::black_box(std::ptr::addr_of!(marker) as usize)
}
