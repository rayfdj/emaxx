//! alloc.c's storage for conses: cells in blocks, a free list threaded
//! through the free cells, a registry of the blocks for the conservative
//! marking of the stack, and the sweep that returns unmarked cells to the
//! free list.
//!
//! A `ConsRef' is `Lisp_Object' for a cons: a pointer the collector keeps
//! valid, copied without a reference count. The aligned block owns compact
//! mark/allocation bitmaps and weak-reference serials; the cell contains only
//! two Lisp words. A free cell's first word links the free list.
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

use super::types::{ConsCell, Kind, Value};
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::ptr::NonNull;
use std::sync::Mutex;

mod arena;
pub(crate) mod counters;
mod finalizers;
mod strings;
mod symbols;
pub(crate) use finalizers::FinalizerList;
pub use finalizers::{FinalizerRef, FinalizerState};
pub use strings::StringObjectRef;
#[cfg(test)]
pub(crate) use strings::string_data_census;
pub(crate) use strings::{
    CodeState, IMAGE_STRING_BLOCK_BYTES, IMAGE_STRINGS_PER_BLOCK, PendingStringData,
    STRING_DATA_ALIGNMENT, STRING_DATA_PREFIX, StringAllocation, adopt_image_string_blocks,
    allocate_string, borrowed_string_roots, code_changes, image_string_bytes, image_string_offset,
    live_string_object_census, retire_string_data, string_data_size, sweep_strings,
};
pub(crate) mod vectors;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
pub use symbols::{SymbolCell, SymbolRef};
/// The actual header cells this allocator holds, as charged to collection.
pub(crate) const SYMBOL_CELL_BYTES: usize = symbols::SYMBOL_CELL_SIZE;
pub(crate) const STRING_CELL_BYTES: usize = strings::STRING_CELL_SIZE;
pub(crate) use symbols::{
    SymbolText, adopt_image_symbol_blocks, allocate_symbol, image_cell_words, image_symbol_bytes,
    image_symbol_offset, live_symbols, sweep_symbols,
};
pub use vectors::{ClosureRef, VectorHeader, VectorRef, VectorTag, VectorlikeRef};
pub(crate) use vectors::{
    FreedRecord, live_record_census, live_vector_census, sweep_vectors, take_freed_records,
};

/// alloc.c's block geometry: the cells per block that its formulas give
/// with the C sizes (`BLOCK_ALIGN' 1 << 15 without unexec, `BLOCK_BYTES'
/// = BLOCK_ALIGN - sizeof (struct ablocks *), `MALLOC_SIZE_NEAR (1024)'
/// = 1016 under glibc's 16-byte alignment). Conses and floats have GNU's
/// 16-/8-byte payloads in 32 KiB blocks. Weak-reference serials and two more
/// bitmaps reduce the cons slots per block; floats only need one extra bitmap.
const C_BLOCK_BYTES: usize = (1 << 15) - 8;
const CELL_SIZE: usize = std::mem::size_of::<ConsCell>();
/// Space for each 16-byte cons, an eight-byte weak-reference serial and three
/// bitmap bits, plus the block epoch and alignment padding. GNU needs only
/// its mark bit. Ordinary reads and stores never access this extra metadata.
pub(crate) const CELLS_PER_BLOCK: usize = (((1 << 15) - 8) * 8) / (16 * 8 + 64 + 3);
/// Eight-byte float payloads and two bits per slot: the collection mark,
/// and allocation state for checked native-word decoding. GNU has one mark
/// bit; the allocation bit replaces this runtime's in-object FREE_MARK.
pub(crate) const FLOATS_PER_BLOCK: usize = ((C_BLOCK_BYTES - 8) * 8) / (8 * 8 + 2);
/// `SYMBOL_BLOCK_SIZE': (1020 - sizeof (struct symbol_block *)) /
/// sizeof (struct Lisp_Symbol), a 48-byte symbol.
pub(crate) const SYMBOLS_PER_BLOCK: usize = (1020 - 8) / 48;
/// The alignment of a symbol or string block, which the registry finds by
/// its start (alloc.c mallocs them): a cache line.
const BLOCK_ALIGN: usize = 64;

/// The bytes of a block of KIND: its cells.
fn block_bytes(kind: BlockKind) -> usize {
    match kind {
        BlockKind::Cons => std::mem::size_of::<ConsBlock>(),
        BlockKind::Float => std::mem::size_of::<FloatBlock>(),
        BlockKind::Symbol => SYMBOLS_PER_BLOCK * symbols::SYMBOL_CELL_SIZE,
        BlockKind::String => strings::STRINGS_PER_BLOCK * strings::STRING_CELL_SIZE,
        BlockKind::VectorBlock | BlockKind::LargeVector => {
            unreachable!("vector storage is allocated by its own module")
        }
    }
}

fn block_alignment(kind: BlockKind) -> usize {
    match kind {
        BlockKind::Cons => CONS_BLOCK_ALIGN,
        BlockKind::Float => FLOAT_BLOCK_ALIGN,
        _ => BLOCK_ALIGN,
    }
}

/// The mark word of a cell on the free list (alloc.c sets the mark bit
/// of free cells so the sweep skips them; a live cell carries its
/// collection's epoch, which `begin_mark_epoch' never makes this).
pub(crate) const FREE_MARK: u32 = u32::MAX;

/// alloc.c's `mem_type' of a block: what its cells are.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub(crate) enum BlockKind {
    Cons,
    Float,
    /// alloc.c's `MEM_TYPE_VECTOR_BLOCK': small vectors carved by size.
    VectorBlock,
    /// alloc.c's `MEM_TYPE_VECTORLIKE': one large vector on its own.
    LargeVector,
    /// alloc.c's `MEM_TYPE_SYMBOL'.
    Symbol,
    /// alloc.c's MEM_TYPE_STRING.
    String,
}

const BLOCK_KINDS: usize = 6;
const _: () = assert!(BlockKind::String as usize == BLOCK_KINDS - 1);

/// The registered blocks.
struct BlockRegistry {
    /// Every block by start address, with its kind, for `mem_find'
    /// (alloc.c's mem tree, a red-black tree there; the map's lookup is
    /// the same order).
    all: BTreeMap<usize, BlockKind>,
    /// Each kind's blocks by start address, each with whether an image
    /// load registered it (see `IMAGE_REGION_OPEN'), for its sweep.
    by_kind: [BTreeMap<usize, bool>; BLOCK_KINDS],
    /// The image blocks that are the image's own bytes where the loader
    /// mapped them (`adopt_image_cons_blocks', `adopt_image_string_blocks',
    /// `vectors::adopt_image_vectors'): pdumper.c never frees its dump, so
    /// their storage is never given back.
    mapped: BTreeSet<usize>,
}

static BLOCKS: Mutex<BlockRegistry> = Mutex::new(BlockRegistry {
    all: BTreeMap::new(),
    by_kind: [const { BTreeMap::new() }; BLOCK_KINDS],
    mapped: BTreeSet::new(),
});

/// alloc.c's `min_heap_address' and `max_heap_address': the lowest block
/// start and the highest block end registered, which `mem_find' tests
/// before it takes the registry's lock (most words a stack scan reads are
/// return addresses, stack addresses and small numbers). A block's
/// release leaves them as they are.
static HEAP_LOW: AtomicUsize = AtomicUsize::new(usize::MAX);
static HEAP_HIGH: AtomicUsize = AtomicUsize::new(0);

/// alloc.c's `mem_insert' of a block of KIND and BYTES at START.
fn register_block(start: usize, bytes: usize, kind: BlockKind) {
    let image = IMAGE_REGION_OPEN.load(Ordering::Relaxed);
    register_block_of(start, bytes, kind, image, false);
}

/// `register_block' of an image block (IMAGE), one of the image's own
/// mapped bytes (MAPPED) or not.
fn register_block_of(start: usize, bytes: usize, kind: BlockKind, image: bool, mapped: bool) {
    let mut blocks = BLOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    blocks.all.insert(start, kind);
    blocks.by_kind[kind as usize].insert(start, image);
    if mapped {
        blocks.mapped.insert(start);
    }
    drop(blocks);
    HEAP_LOW.fetch_min(start, Ordering::Relaxed);
    HEAP_HIGH.fetch_max(start + bytes, Ordering::Relaxed);
}

/// `register_block_of' of each of the image's mapped blocks of KIND, a
/// (start, bytes) each, under one lock: the registry's maps take them
/// built in order, where an insertion each walked them for its place.
pub(crate) fn register_image_blocks(kind: BlockKind, blocks: &[(usize, usize)]) {
    let Some(low) = blocks.iter().map(|&(start, _)| start).min() else {
        return;
    };
    let high = blocks
        .iter()
        .map(|&(start, bytes)| start + bytes)
        .max()
        .expect("a block");
    let mut registry = BLOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut all = blocks.iter().map(|&(start, _)| (start, kind)).collect();
    registry.all.append(&mut all);
    let mut by_kind = blocks.iter().map(|&(start, _)| (start, true)).collect();
    registry.by_kind[kind as usize].append(&mut by_kind);
    let mut mapped = blocks.iter().map(|&(start, _)| start).collect();
    registry.mapped.append(&mut mapped);
    drop(registry);
    HEAP_LOW.fetch_min(low, Ordering::Relaxed);
    HEAP_HIGH.fetch_max(high, Ordering::Relaxed);
}

/// True when the block at START is one of the image's mapped blocks.
fn is_mapped_block(start: usize) -> bool {
    BLOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .mapped
        .contains(&start)
}

/// alloc.c's `mem_delete'.
fn unregister_block(start: usize) {
    let mut blocks = BLOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let removed = blocks.all.remove(&start);
    debug_assert!(removed.is_some());
    if let Some(kind) = removed {
        blocks.by_kind[kind as usize].remove(&start);
    }
    blocks.mapped.remove(&start);
}

/// pdumper's mapped region, as this loader has it. GNU's sweeps never visit
/// dumped objects, gcstat never counts them (so `total_bytes_of_live_objects'
/// and the `garbage-collect' census are the heap's own objects), and their
/// storage is never reused. This loader maps the image's conses, strings
/// and vectors where the image holds them and rebuilds its other objects in
/// ordinary blocks, and every block registered while an image loads is an
/// image block: the sweeps leave its objects out of the census and its freed
/// cells off the free lists. Dead objects there are still cleaned, because
/// they own host storage, and a block with nothing live is still released
/// unless it is the image's own bytes.
static IMAGE_REGION_OPEN: AtomicBool = AtomicBool::new(false);

/// The image's cells of one kind (strings and symbols allocated while an
/// image loads, vectors in its blocks), for a sweep that need not read
/// them: how many the current mark epoch has marked, raised as a mark
/// first reaches one, and how many the last sweep that read them left
/// (`UNKNOWN' until one has since an image load).  Only a sweep frees an
/// image cell and none is allocated outside a load, so when the two agree
/// every image cell that sweep left is marked again: nothing in the image
/// died, and the sweep passes its blocks by unread, as GNU's sweeps never
/// read its dump.
pub(crate) struct ImageCells {
    marked: AtomicUsize,
    live: AtomicUsize,
}

impl ImageCells {
    const UNKNOWN: usize = usize::MAX;

    const fn new() -> Self {
        Self {
            marked: AtomicUsize::new(0),
            live: AtomicUsize::new(Self::UNKNOWN),
        }
    }

    /// A mark reached an image cell not yet marked in this epoch.
    #[inline(always)]
    pub(crate) fn note_marked(&self) {
        self.marked
            .store(self.marked.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
    }

    /// True when every image cell the last reading sweep left is marked.
    fn unchanged(&self) -> bool {
        let live = self.live.load(Ordering::Relaxed);
        live != Self::UNKNOWN && self.marked.load(Ordering::Relaxed) == live
    }

    /// A sweep read every image cell and left LIVE of them (unknown while
    /// an image loads, as more are still to come).
    fn set_live(&self, live: usize) {
        let live = if IMAGE_REGION_OPEN.load(Ordering::Relaxed) {
            Self::UNKNOWN
        } else {
            live
        };
        self.live.store(live, Ordering::Relaxed);
    }
}

pub(crate) static IMAGE_STRINGS: ImageCells = ImageCells::new();
pub(crate) static IMAGE_SYMBOLS: ImageCells = ImageCells::new();
pub(crate) static IMAGE_VECTORS: ImageCells = ImageCells::new();

/// A mark epoch begins: no image cell is marked in it yet.
pub(crate) fn begin_image_mark_epoch() {
    for cells in [&IMAGE_STRINGS, &IMAGE_SYMBOLS, &IMAGE_VECTORS] {
        cells.marked.store(0, Ordering::Relaxed);
    }
}

/// True while an image loads: what is allocated now is the image's.
#[inline]
fn image_region_open() -> bool {
    IMAGE_REGION_OPEN.load(Ordering::Relaxed)
}

/// An image load in progress: blocks registered until this is dropped are
/// image blocks. Opening and closing both start every allocator on fresh
/// blocks, so no block holds both image objects and the heap's own.
pub(crate) struct ImageRegion(());

impl ImageRegion {
    pub(crate) fn open() -> Self {
        let was_open = IMAGE_REGION_OPEN.swap(true, Ordering::Relaxed);
        assert!(!was_open, "an image region is already open");
        abandon_allocation_frontiers();
        forget_image_live();
        Self(())
    }
}

impl Drop for ImageRegion {
    fn drop(&mut self) {
        abandon_allocation_frontiers();
        IMAGE_REGION_OPEN.store(false, Ordering::Relaxed);
        forget_image_live();
    }
}

/// The image's cells change: the next sweep reads them all.
fn forget_image_live() {
    for cells in [&IMAGE_STRINGS, &IMAGE_SYMBOLS, &IMAGE_VECTORS] {
        cells.live.store(ImageCells::UNKNOWN, Ordering::Relaxed);
    }
}

/// Make the next allocation of every kind take a new block. The cells left
/// behind stay free and return to the free lists at the next sweep (or, in
/// an image block, stay unused).
fn abandon_allocation_frontiers() {
    FREE_LIST.store(std::ptr::null_mut(), Ordering::Relaxed);
    BUMP_NEXT.store(BUMP_END.load(Ordering::Relaxed), Ordering::Relaxed);
    FLOAT_FREE_LIST.store(std::ptr::null_mut(), Ordering::Relaxed);
    FLOAT_BUMP_NEXT.store(FLOAT_BUMP_END.load(Ordering::Relaxed), Ordering::Relaxed);
    symbols::abandon_allocation_frontier();
    strings::abandon_allocation_frontier();
    vectors::abandon_allocation_frontier();
}
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
/// The last serial given a cons for `WeakConsRef' (see `ConsMark::serial').
static SERIAL: AtomicU64 = AtomicU64::new(0);

/// alloc.c's `float_block', `float_free_list' and `float_block_index'.
static FLOAT_FREE_LIST: AtomicPtr<FloatCell> = AtomicPtr::new(std::ptr::null_mut());
static FLOAT_BUMP_NEXT: AtomicUsize = AtomicUsize::new(0);
static FLOAT_BUMP_END: AtomicUsize = AtomicUsize::new(0);
/// gcstat's `total_floats' and `total_free_floats'.
static LIVE_FLOATS: AtomicUsize = AtomicUsize::new(0);
static FREE_FLOATS: AtomicUsize = AtomicUsize::new(0);
const FLOAT_CELL_SIZE: usize = std::mem::size_of::<FloatCell>();

/// lisp.h's `struct Lisp_Float': one double, also read directly by generated
/// code. Allocation and collection state belong to the containing block.
#[repr(C)]
pub struct FloatCell {
    value: f64,
}

const FLOAT_BLOCK_ALIGN: usize = 1 << 15;
const FLOAT_BITMAP_WORDS: usize = FLOATS_PER_BLOCK.div_ceil(usize::BITS as usize);

/// The marks start the block and the cells follow them.  The marks are
/// found from a cell's address by alignment, so the block's address is
/// computed on every allocation (and every checked read): ahead of the
/// cells, a copy of it that stays on the stack names no float, which the
/// conservative scan would otherwise keep (GNU's floats come first: outside
/// checking builds only its collector forms a block's address).
#[repr(C, align(32768))]
struct FloatBlock {
    marks: FloatMarks,
    cells: [std::mem::MaybeUninit<FloatCell>; FLOATS_PER_BLOCK],
}

/// The first float's offset in its block.
const FLOAT_CELLS: usize = std::mem::offset_of!(FloatBlock, cells);

/// The float cell INDEX of the block at START.
#[inline(always)]
fn float_cell(start: usize, index: usize) -> *mut FloatCell {
    (start + FLOAT_CELLS + index * FLOAT_CELL_SIZE) as *mut FloatCell
}

const _: () = {
    assert!(FLOAT_CELL_SIZE == 8);
    assert!(std::mem::size_of::<FloatBlock>() == FLOAT_BLOCK_ALIGN);
    assert!(std::mem::offset_of!(FloatBlock, marks) == 0);
    assert!(FLOAT_CELLS != 0);
};

/// alloc.c:float_block.gcmarkbits. One epoch for the block allows the
/// runtime's independent reachability passes to use these compact marks:
/// the first mark of a new pass clears the old bitmap. There is no epoch
/// or allocation-state load on an ordinary release-build float read.
#[repr(C)]
struct FloatMarks {
    epoch: Cell<u32>,
    allocated: [Cell<usize>; FLOAT_BITMAP_WORDS],
    marked: [Cell<usize>; FLOAT_BITMAP_WORDS],
}

impl FloatMarks {
    const fn new() -> Self {
        Self {
            epoch: Cell::new(0),
            allocated: [const { Cell::new(0) }; FLOAT_BITMAP_WORDS],
            marked: [const { Cell::new(0) }; FLOAT_BITMAP_WORDS],
        }
    }
}

pub(crate) struct FloatMark<'a> {
    block: &'a FloatMarks,
    index: usize,
}

impl FloatMark<'_> {
    #[inline]
    fn bit(&self) -> (usize, usize) {
        (
            self.index / usize::BITS as usize,
            1 << (self.index % usize::BITS as usize),
        )
    }

    #[inline]
    fn allocated(&self) -> bool {
        let (word, mask) = self.bit();
        self.block.allocated[word].get() & mask != 0
    }

    #[inline]
    pub(crate) fn is_marked(&self, epoch: u32) -> bool {
        let (word, mask) = self.bit();
        self.block.epoch.get() == epoch && self.block.marked[word].get() & mask != 0
    }

    pub(crate) fn mark(&self, epoch: u32) -> bool {
        debug_assert!(self.allocated(), "marking a free float");
        if self.block.epoch.get() != epoch {
            for word in &self.block.marked {
                word.set(0);
            }
            self.block.epoch.set(epoch);
        }
        let (word, mask) = self.bit();
        let old = self.block.marked[word].get();
        self.block.marked[word].set(old | mask);
        old & mask == 0
    }

    fn allocate(&self, epoch: u32) {
        let (word, mask) = self.bit();
        self.block.allocated[word].set(self.block.allocated[word].get() | mask);
        self.mark(epoch);
    }
}

/// FLOAT_BLOCK/FLOAT_INDEX: derive the metadata by alignment and index,
/// without a hash table or a registry lookup.
///
/// # Safety
/// CELL is a slot of an allocated FloatBlock. It may be on its free list.
/// The caller must keep the block allocated for the returned borrow;
/// its metadata remains initialized until the entire block is released.
#[inline]
unsafe fn float_mark<'a>(cell: *const FloatCell) -> FloatMark<'a> {
    let address = cell as usize;
    let base = address & !(FLOAT_BLOCK_ALIGN - 1);
    let index = (address - base - FLOAT_CELLS) / FLOAT_CELL_SIZE;
    debug_assert!(index < FLOATS_PER_BLOCK);
    // SAFETY: metadata is initialized before the block is registered, and
    // the caller keeps the block allocated for the returned borrow.
    let block = unsafe { &*std::ptr::addr_of!((*(base as *const FloatBlock)).marks) };
    FloatMark { block, index }
}

/// `Lisp_Object' for a float: the cell's address, copied freely, valid
/// while the collector can reach the cell.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct FloatRef(NonNull<FloatCell>);

impl FloatRef {
    fn cell(&self) -> &FloatCell {
        // SAFETY: a `FloatRef' names an allocated cell (see `ConsRef').
        let cell = unsafe { self.0.as_ref() };
        debug_assert!(
            self.mark_bit().allocated(),
            "use of a float the collector freed"
        );
        cell
    }

    pub fn get(&self) -> f64 {
        self.cell().value
    }

    /// The handle for a cell the allocator handed out (a value's word).
    ///
    /// # Safety
    /// CELL must be an allocated float cell.
    #[inline(always)]
    pub(crate) unsafe fn from_raw(cell: *mut FloatCell) -> Self {
        // SAFETY: the caller's contract.
        Self(unsafe { NonNull::new_unchecked(cell) })
    }

    pub(crate) fn identity_ptr(&self) -> usize {
        self.0.as_ptr() as usize
    }

    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    pub(crate) fn mark_bit(&self) -> FloatMark<'_> {
        // SAFETY: a reachable FloatRef keeps its containing block allocated.
        unsafe { float_mark(self.0.as_ptr()) }
    }
}

impl std::ops::Deref for FloatRef {
    type Target = f64;
    fn deref(&self) -> &f64 {
        &self.cell().value
    }
}

impl PartialEq for FloatRef {
    fn eq(&self, other: &Self) -> bool {
        self.get().to_bits() == other.get().to_bits()
    }
}

impl std::fmt::Debug for FloatRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.get(), f)
    }
}

impl std::fmt::Display for FloatRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.get(), f)
    }
}

impl From<f64> for FloatRef {
    fn from(value: f64) -> Self {
        crate::lisp::native_comp::note_lisp_allocation(8);
        allocate_float(value)
    }
}

struct FreeFloat {
    next: *mut FloatCell,
}

/// alloc.c:make_float: the free list's head, else the next cell of the
/// newest float block.
#[inline]
pub(crate) fn allocate_float(value: f64) -> FloatRef {
    let head = FLOAT_FREE_LIST.load(Ordering::Relaxed);
    let slot = if head.is_null() {
        bump_float()
    } else {
        // SAFETY: a free cell's first word is the free-list link.
        let next = unsafe { (*head.cast::<FreeFloat>()).next };
        FLOAT_FREE_LIST.store(next, Ordering::Relaxed);
        FREE_FLOATS.store(
            FREE_FLOATS.load(Ordering::Relaxed).saturating_sub(1),
            Ordering::Relaxed,
        );
        head
    };
    LIVE_FLOATS.store(LIVE_FLOATS.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
    counters::tally(&counters::FLOATS_CONSED, 1);
    // SAFETY: SLOT is a free cell of a float block. Initialize its one word
    // and the block's allocation/mark bits before publishing a handle.
    unsafe {
        std::ptr::write(slot, FloatCell { value });
        float_mark(slot).allocate(super::types::current_mark_epoch());
        FloatRef(NonNull::new_unchecked(slot))
    }
}

fn bump_float() -> *mut FloatCell {
    let next = FLOAT_BUMP_NEXT.load(Ordering::Relaxed);
    let end = FLOAT_BUMP_END.load(Ordering::Relaxed);
    if next < end {
        FLOAT_BUMP_NEXT.store(next + FLOAT_CELL_SIZE, Ordering::Relaxed);
        return next as *mut FloatCell;
    }
    let block = new_block(BlockKind::Float);
    let first = float_cell(block, 0);
    FLOAT_BUMP_NEXT.store(first as usize + FLOAT_CELL_SIZE, Ordering::Relaxed);
    FLOAT_BUMP_END.store(
        float_cell(block, FLOATS_PER_BLOCK) as usize,
        Ordering::Relaxed,
    );
    first
}

/// alloc.c:sweep_floats, as `sweep_conses' below.
pub(crate) fn sweep_floats(epoch: u32) -> usize {
    let bump_next = FLOAT_BUMP_NEXT.load(Ordering::Relaxed);
    let bump_end = FLOAT_BUMP_END.load(Ordering::Relaxed);
    let mut free_list: *mut FloatCell = std::ptr::null_mut();
    let mut num_free = 0usize;
    let mut num_used = 0usize;
    let mut released = Vec::new();
    for (start, image_block) in sweep_blocks_of(BlockKind::Float) {
        let lim = if bump_end == float_cell(start, FLOATS_PER_BLOCK) as usize {
            (bump_next - float_cell(start, 0) as usize) / FLOAT_CELL_SIZE
        } else {
            FLOATS_PER_BLOCK
        };
        let chain_before = free_list;
        let mut this_free = 0usize;
        // SAFETY: a registered float block's marks stay initialized until
        // the block is released.
        let marks = unsafe { &*std::ptr::addr_of!((*(start as *const FloatBlock)).marks) };
        let current = marks.epoch.get() == epoch;
        // A bitmap word at a time: an unmarked cell holds no owned storage,
        // so its bits are cleared (and, outside the image, it is linked
        // through its one word, in address order).
        for word in 0..lim.div_ceil(usize::BITS as usize) {
            let first = word * usize::BITS as usize;
            let valid = if lim - first >= usize::BITS as usize {
                usize::MAX
            } else {
                (1usize << (lim - first)) - 1
            };
            let marked = marks.marked[word].get();
            let live = if current { marked } else { 0 } & valid;
            let mut free = !live & valid;
            if free != 0 {
                marks.allocated[word].set(marks.allocated[word].get() & !free);
                marks.marked[word].set(marked & !free);
            }
            if !image_block {
                num_used += live.count_ones() as usize;
            }
            this_free += free.count_ones() as usize;
            if image_block {
                continue;
            }
            while free != 0 {
                let index = first + free.trailing_zeros() as usize;
                free &= free - 1;
                let cell = float_cell(start, index);
                // SAFETY: the cell is free; its one word is the free-list link.
                unsafe { (*cell.cast::<FreeFloat>()).next = free_list };
                free_list = cell;
            }
        }
        if image_block {
            if this_free == FLOATS_PER_BLOCK {
                released.push(start);
            }
        } else if this_free == FLOATS_PER_BLOCK && num_free > FLOATS_PER_BLOCK {
            free_list = chain_before;
            released.push(start);
        } else {
            num_free += this_free;
        }
    }
    FLOAT_FREE_LIST.store(free_list, Ordering::Relaxed);
    for start in released {
        release_block(start, BlockKind::Float);
    }
    LIVE_FLOATS.store(num_used, Ordering::Relaxed);
    FREE_FLOATS.store(num_free, Ordering::Relaxed);
    num_used
}

pub(crate) fn live_floats() -> usize {
    LIVE_FLOATS.load(Ordering::Relaxed)
}

/// A word the conservative scan resolved: the cell it names, by kind.
pub(crate) enum Found {
    Cons(*mut ConsCell),
    Float(*mut FloatCell),
    Vectorlike(*mut VectorHeader),
    Symbol(*mut SymbolCell),
    String(*mut strings::StringCell),
}

thread_local! {
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
    /// An empty vector: no buffer, nothing registered.
    pub const fn new() -> Self {
        Self {
            inner: Vec::new(),
            slot: UNREGISTERED,
            region: (0, 0),
        }
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
        self.clear_spare();
        self.record(region);
    }

    /// Enter REGION, the buffer whose spare room is zero, in the registry.
    fn record(&mut self, region: (usize, usize)) {
        self.region = region;
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
        // Within the capacity the buffer stays where it was registered.
        let len = self.inner.len();
        if len < self.inner.capacity() {
            // SAFETY: the slot at LEN is within the capacity just tested
            // (`Vec::push' would test it again).
            unsafe {
                self.inner.as_mut_ptr().add(len).write(value);
                self.inner.set_len(len + 1);
            }
            return;
        }
        self.inner.push(value);
        self.refresh();
    }

    /// Room for ADDITIONAL more elements, registered once. A first buffer
    /// is taken zeroed from the allocator, as SAFE_ALLOCA_LISP's xzalloc
    /// takes it: a large one comes as fresh zero pages instead of being
    /// cleared by a pass over it.
    pub fn reserve(&mut self, additional: usize) {
        if self.inner.capacity() == 0
            && additional != 0
            && std::mem::size_of::<T>() != 0
            && let Ok(layout) = std::alloc::Layout::array::<T>(additional)
        {
            // SAFETY: LAYOUT is nonzero. The buffer is the global
            // allocator's, with the layout a vector of ADDITIONAL elements
            // of T has, and holds no element yet (length 0).
            unsafe {
                let buffer = std::alloc::alloc_zeroed(layout);
                if buffer.is_null() {
                    std::alloc::handle_alloc_error(layout);
                }
                self.inner = Vec::from_raw_parts(buffer.cast::<T>(), 0, additional);
            }
            let region = (self.inner.as_ptr() as usize, layout.size());
            self.record(region);
            return;
        }
        self.inner.reserve(additional);
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
            let start = self.inner.as_mut_ptr().add(self.inner.len()).cast::<u8>();
            // One slot, the usual pop, is cleared by stores of its constant
            // size; a size known only at run time is a `memset' call.
            if count == 1 {
                std::ptr::write_bytes(start, 0, std::mem::size_of::<T>());
            } else {
                std::ptr::write_bytes(start, 0, count * std::mem::size_of::<T>());
            }
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

    /// The drained elements, taken at once and kept rooted until consumed.
    /// The source slots they leave are zeroed before this returns.
    pub fn drain(&mut self, range: impl std::ops::RangeBounds<usize>) -> RootedIntoIter<T> {
        let before = self.inner.len();
        let drained = self.inner.drain(range).collect::<Vec<_>>();
        let drained = Self::from_vec(drained);
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

/// The remaining elements stay in the registered buffer. Consuming an
/// element clears its old bytes, like popping a RootedVec: a collection
/// cannot mistake a consumed value for a still-live buffer element.
pub struct RootedIntoIter<T> {
    buffer: RootedVec<T>,
    front: usize,
    back: usize,
}

impl<T> RootedIntoIter<T> {
    /// # Safety
    /// INDEX is an initialized element removed from the remaining range.
    unsafe fn take(&mut self, index: usize) -> T {
        // SAFETY: the Vec still owns the complete allocation, with its
        // length zero so it cannot drop moved elements. The iterator's
        // front/back indices own the initialized elements instead. Derive
        // one mutable raw pointer from the Vec, move out exactly once, and
        // clear that vacated storage without creating an aliased reference.
        unsafe {
            let slot = self.buffer.inner.as_mut_ptr().add(index);
            let value = slot.read();
            std::ptr::write_bytes(slot.cast::<u8>(), 0, std::mem::size_of::<T>());
            value
        }
    }
}

impl<T> Iterator for RootedIntoIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        if self.front == self.back {
            return None;
        }
        let index = self.front;
        self.front += 1;
        // SAFETY: INDEX was the first remaining initialized element.
        Some(unsafe { self.take(index) })
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.back - self.front;
        (len, Some(len))
    }
}

impl<T> DoubleEndedIterator for RootedIntoIter<T> {
    fn next_back(&mut self) -> Option<T> {
        if self.front == self.back {
            return None;
        }
        self.back -= 1;
        // SAFETY: BACK was the last remaining initialized element.
        Some(unsafe { self.take(self.back) })
    }
}

impl<T> ExactSizeIterator for RootedIntoIter<T> {}
impl<T> std::iter::FusedIterator for RootedIntoIter<T> {}

impl<T> Drop for RootedIntoIter<T> {
    fn drop(&mut self) {
        if !std::mem::needs_drop::<T>() {
            return;
        }
        // As with Vec's owning iterator, one panicking destructor must not
        // skip the remaining destructors. The buffer stays registered until
        // this guard finishes, including any collection during a destructor.
        struct Remaining<'a, T>(&'a mut RootedIntoIter<T>);
        impl<T> Drop for Remaining<'_, T> {
            fn drop(&mut self) {
                for value in self.0.by_ref() {
                    drop(value);
                }
            }
        }
        let guard = Remaining(self);
        for value in guard.0.by_ref() {
            drop(value);
        }
    }
}

impl<T> IntoIterator for RootedVec<T> {
    type Item = T;
    type IntoIter = RootedIntoIter<T>;
    fn into_iter(mut self) -> RootedIntoIter<T> {
        let back = self.inner.len();
        // SAFETY: the owning iterator now manages every initialized
        // element. Its Drop releases them before the Vec frees the buffer.
        unsafe { self.inner.set_len(0) };
        RootedIntoIter {
            buffer: self,
            front: 0,
            back,
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

#[cfg(test)]
mod root_buffer_tests {
    use super::RootedVec;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn owning_iterator_drops_remaining_elements_and_unregisters_after_panic() {
        struct Probe {
            id: usize,
            drops: Rc<RefCell<Vec<usize>>>,
        }
        impl Drop for Probe {
            fn drop(&mut self) {
                self.drops.borrow_mut().push(self.id);
                assert_ne!(self.id, 1, "exercise a panicking element destructor");
            }
        }
        let drops = Rc::new(RefCell::new(Vec::new()));
        let roots: RootedVec<_> = (0..5)
            .map(|id| Probe {
                id,
                drops: drops.clone(),
            })
            .collect();
        let slot = roots.slot;
        let mut iter = roots.into_iter();
        drop(iter.next().expect("front"));
        drop(iter.next_back().expect("back"));
        assert_eq!(iter.len(), 3);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(iter)));
        assert!(panic.is_err());
        assert_eq!(&*drops.borrow(), &[0, 4, 1, 2, 3]);
        assert!(super::heap_roots().regions[slot].is_none());
    }

    #[test]
    fn owning_iterator_handles_empty_and_zero_sized_elements() {
        let mut empty = RootedVec::<()>::new().into_iter();
        assert_eq!(empty.next(), None);
        assert_eq!(empty.next_back(), None);
        let mut units = RootedVec::from_vec(vec![(); 3]).into_iter();
        assert_eq!(units.next(), Some(()));
        assert_eq!(units.next_back(), Some(()));
        assert_eq!(units.len(), 1);
        assert_eq!(units.next(), Some(()));
        assert_eq!(units.next_back(), None);
        assert_eq!(units.next(), None);
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

/// alloc.c:mark_stack and mark_threads: the running stack, the parked
/// coroutines' stacks and every live stack waiting on an alternate stack.
#[inline(never)]
pub(crate) fn mark_all_stacks(current_base: Option<usize>, mut mark: impl FnMut(Value)) {
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
/// GNU's CONS_BLOCK/CONS_INDEX arithmetic: a cell's block is found by
/// alignment, and its allocation, marking and weak-reference serials there.
const CONS_BLOCK_ALIGN: usize = 1 << 15;
const CONS_BITMAP_WORDS: usize = CELLS_PER_BLOCK.div_ceil(usize::BITS as usize);

/// The metadata starts the block and the cells follow it, as in
/// `FloatBlock': Fcons marks a new cell allocated through the block's
/// address, so a copy of that address left on the stack must name no cons.
#[repr(C, align(32768))]
struct ConsBlock {
    metadata: ConsMetadata,
    cells: ConsCells,
}

/// The cells start on a bitmap word's span, so that a cell's bit is its
/// slot's from the block's address, as with the cells first.
#[repr(C, align(1024))]
struct ConsCells([std::mem::MaybeUninit<ConsCell>; CELLS_PER_BLOCK]);

/// The first cell's offset in its block.
const CONS_CELLS: usize = std::mem::offset_of!(ConsBlock, cells);
/// The slots, of a cell's size from the block's address, and the bitmap
/// words that the metadata takes before the first cell.
const CONS_FIRST_SLOT: usize = CONS_CELLS / CELL_SIZE;
const CONS_FIRST_WORD: usize = CONS_FIRST_SLOT / usize::BITS as usize;

/// The cell INDEX of the cons block at START.
#[inline(always)]
fn cons_cell(start: usize, index: usize) -> *mut ConsCell {
    (start + CONS_CELLS + index * CELL_SIZE) as *mut ConsCell
}

#[repr(C)]
struct ConsMetadata {
    epoch: Cell<u32>,
    allocated: [Cell<usize>; CONS_BITMAP_WORDS],
    marked: [Cell<usize>; CONS_BITMAP_WORDS],
    /// The cells whose serial slot holds this allocation's serial: set when
    /// a weak reference first asks for it, cleared when the sweep frees the
    /// cell, so a cell is allocated with its bit clear.
    serial_given: [Cell<usize>; CONS_BITMAP_WORDS],
    // A reused address must not revive a WeakConsRef. GNU's raw Lisp values
    // need no serial; Rust's weak host cache references do. Ordinary car/cdr
    // and stores never read these serials or the bitmaps. A cell's serial
    // is written when a weak reference first asks for it, as Fcons writes
    // nothing beside the cell, and read only while its `serial_given' bit
    // is set, so any other slot may be uninitialized.
    serials: [Cell<std::mem::MaybeUninit<u64>>; CELLS_PER_BLOCK],
}

impl ConsMetadata {
    /// A new block's metadata, every cell free: the epoch and the bitmaps.
    /// GNU's Fcons clears only a new block's mark bits; the serials are
    /// left as they are (about 11 KB a block that zeroing wrote).
    ///
    /// # Safety
    /// METADATA is a new block's metadata, not yet in use.
    unsafe fn init(metadata: *mut Self) {
        // SAFETY: the caller's contract; the serials are MaybeUninit.
        unsafe {
            std::ptr::addr_of_mut!((*metadata).epoch).write(Cell::new(0));
            std::ptr::addr_of_mut!((*metadata).allocated)
                .write([const { Cell::new(0) }; CONS_BITMAP_WORDS]);
            std::ptr::addr_of_mut!((*metadata).marked)
                .write([const { Cell::new(0) }; CONS_BITMAP_WORDS]);
            std::ptr::addr_of_mut!((*metadata).serial_given)
                .write([const { Cell::new(0) }; CONS_BITMAP_WORDS]);
        }
    }
}

const _: () = {
    assert!(CELL_SIZE == 16);
    // The sweep frees a cell by its bits alone.
    assert!(!std::mem::needs_drop::<ConsCell>());
    assert!(std::mem::size_of::<ConsBlock>() == CONS_BLOCK_ALIGN);
    assert!(std::mem::offset_of!(ConsBlock, metadata) == 0);
    assert!(CONS_CELLS != 0);
    assert!(CONS_FIRST_SLOT.is_multiple_of(usize::BITS as usize));
    assert!(std::mem::offset_of!(ConsCell, car) == 0);
    assert!(std::mem::offset_of!(ConsCell, cdr) == std::mem::size_of::<Value>());
};

pub(crate) struct ConsMark<'a> {
    block: &'a ConsMetadata,
    /// The cell's slot from the block's address (`CONS_FIRST_SLOT' for
    /// the first cell).
    slot: usize,
}

impl ConsMark<'_> {
    #[inline]
    fn index(&self) -> usize {
        self.slot - CONS_FIRST_SLOT
    }

    #[inline]
    fn bit(&self) -> (usize, usize) {
        (
            self.slot / usize::BITS as usize - CONS_FIRST_WORD,
            1 << (self.slot % usize::BITS as usize),
        )
    }

    #[inline]
    fn allocated(&self) -> bool {
        let (word, mask) = self.bit();
        self.block.allocated[word].get() & mask != 0
    }

    #[inline]
    pub(crate) fn is_marked(&self, epoch: u32) -> bool {
        self.allocated()
            && self.block.epoch.get() == epoch
            && self.block.marked[self.bit().0].get() & self.bit().1 != 0
    }

    pub(crate) fn mark(&self, epoch: u32) -> bool {
        debug_assert!(self.allocated(), "marking a free cons");
        if self.block.epoch.get() != epoch {
            self.clear_marks(epoch);
        }
        let (word, mask) = self.bit();
        let old = self.block.marked[word].get();
        self.block.marked[word].set(old | mask);
        old & mask == 0
    }

    #[inline(always)]
    fn allocate(&self, epoch: u32) {
        let (word, mask) = self.bit();
        debug_assert!(self.index() < CELLS_PER_BLOCK);
        debug_assert!(
            self.block.serial_given[word].get() & mask == 0,
            "a free cons with a serial"
        );
        // SAFETY: `cons_mark' made this mark for a payload slot: its index
        // is below CELLS_PER_BLOCK, and its word below CONS_BITMAP_WORDS.
        let (allocated, marked) = unsafe {
            (
                self.block.allocated.get_unchecked(word),
                self.block.marked.get_unchecked(word),
            )
        };
        allocated.set(allocated.get() | mask);
        if self.block.epoch.get() != epoch {
            self.clear_marks(epoch);
        }
        marked.set(marked.get() | mask);
    }

    /// `allocate' for this cell and the COUNT - 1 after it in its block,
    /// a bitmap word at a time.
    fn allocate_run(&self, count: usize, epoch: u32) {
        debug_assert!(self.index() + count <= CELLS_PER_BLOCK);
        if self.block.epoch.get() != epoch {
            self.clear_marks(epoch);
        }
        let bits = usize::BITS as usize;
        let mut slot = self.slot;
        let end = self.slot + count;
        while slot < end {
            let word = slot / bits - CONS_FIRST_WORD;
            let first = slot % bits;
            let last = (end - (slot - first)).min(bits);
            let mask = (usize::MAX >> (bits - (last - first))) << first;
            debug_assert!(self.block.serial_given[word].get() & mask == 0);
            let allocated = &self.block.allocated[word];
            allocated.set(allocated.get() | mask);
            let marked = &self.block.marked[word];
            marked.set(marked.get() | mask);
            slot += last - first;
        }
    }

    /// A block's first mark of a collection: the marks of the last one
    /// are stale.
    #[cold]
    fn clear_marks(&self, epoch: u32) {
        for word in &self.block.marked {
            word.set(0);
        }
        self.block.epoch.set(epoch);
    }

    /// This allocation's serial, given now if no weak reference asked for
    /// it before: one more than the last given, so no earlier allocation
    /// of the cell had it.  Callers ask only for allocated cells (a live
    /// ConsRef, or one `mem_find' found allocated).
    fn serial(&self) -> u64 {
        debug_assert!(self.allocated(), "the serial of a free cons");
        if let Some(serial) = self.given_serial() {
            return serial;
        }
        let (word, mask) = self.bit();
        let serial = SERIAL.load(Ordering::Relaxed) + 1;
        SERIAL.store(serial, Ordering::Relaxed);
        self.block.serials[self.index()].set(std::mem::MaybeUninit::new(serial));
        let given = &self.block.serial_given[word];
        given.set(given.get() | mask);
        serial
    }

    /// The serial given this allocation, if a weak reference asked for one.
    fn given_serial(&self) -> Option<u64> {
        let (word, mask) = self.bit();
        // SAFETY: a set bit means the slot was written for this allocation.
        (self.block.serial_given[word].get() & mask != 0)
            .then(|| unsafe { self.block.serials[self.index()].get().assume_init() })
    }
}

/// # Safety
/// CELL is a slot in a still-allocated ConsBlock. Its metadata stays
/// initialized until the whole block is released. No registry lookup is
/// needed for the mark of a reached cons or for an allocation from a block.
#[inline]
unsafe fn cons_mark<'a>(cell: *const ConsCell) -> ConsMark<'a> {
    let address = cell as usize;
    let base = address & !(CONS_BLOCK_ALIGN - 1);
    let slot = (address - base) / CELL_SIZE;
    debug_assert!((CONS_FIRST_SLOT..CONS_FIRST_SLOT + CELLS_PER_BLOCK).contains(&slot));
    let block = unsafe { &*std::ptr::addr_of!((*(base as *const ConsBlock)).metadata) };
    ConsMark { block, slot }
}

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

    pub(crate) fn mark_bit(&self) -> ConsMark<'_> {
        // SAFETY: this reached handle keeps its containing block allocated.
        unsafe { cons_mark(self.0.as_ptr()) }
    }

    pub(crate) fn serial(&self) -> u64 {
        self.mark_bit().serial()
    }

    /// A weak handle: valid until the cell is swept, told apart from a
    /// later cell in the same slot by the serial.
    pub(crate) fn downgrade(&self) -> WeakConsRef {
        WeakConsRef {
            cell: self.0,
            serial: self.serial(),
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
            self.mark_bit().allocated(),
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
    pub(crate) fn identity(&self) -> usize {
        self.cell.as_ptr() as usize
    }

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
        let live = unsafe { mem_find_cons(address) }?;
        if live as usize != address {
            return None;
        }
        // SAFETY: mem_find answered an allocated cell and block.
        (unsafe { cons_mark(live) }.given_serial() == Some(self.serial))
            .then_some(ConsRef(self.cell))
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
        let next = BUMP_NEXT.load(Ordering::Relaxed);
        if next >= BUMP_END.load(Ordering::Relaxed) {
            return allocate_cons_in_new_block(cell);
        }
        BUMP_NEXT.store(next + CELL_SIZE, Ordering::Relaxed);
        next as *mut ConsCell
    } else {
        // SAFETY: a free cell's first word is the free-list link.
        let next = unsafe { (*head.cast::<FreeCell>()).next };
        FREE_LIST.store(next, Ordering::Relaxed);
        head
    };
    // SAFETY: SLOT is a free cell of a block.
    unsafe { fill_cons(slot, cell) }
}

/// The image's conses are this allocator's cons blocks, written whole by
/// the dumper and used where the loader maps them, as pdumper.c's dump
/// holds its objects in their runtime form: a block of `CONS_BLOCK_BYTES',
/// its metadata first and its `CELLS_PER_BLOCK' cells from
/// `CONS_BLOCK_CELLS', the image's conses numbered in that order.
pub(crate) const CONS_BLOCK_BYTES: usize = CONS_BLOCK_ALIGN;
pub(crate) const CONS_BLOCK_CELLS: usize = CONS_CELLS;

/// The offset of image cons INDEX from the start of the image's blocks.
pub(crate) const fn image_cons_offset(index: usize) -> usize {
    (index / CELLS_PER_BLOCK) * CONS_BLOCK_BYTES
        + CONS_CELLS
        + (index % CELLS_PER_BLOCK) * CELL_SIZE
}

/// The bytes the image's blocks of COUNT conses take.
pub(crate) const fn image_cons_bytes(count: usize) -> usize {
    count.div_ceil(CELLS_PER_BLOCK) * CONS_BLOCK_BYTES
}

/// pdumper's mapped conses: the COUNT cells of the image's blocks at
/// START become allocated cells of image blocks, as the dump's objects
/// are the heap's once it is mapped (nothing is allocated or counted for
/// them, and the collector never frees their storage).  The cells keep
/// the words the image gave them, for the loader to relocate in place.
///
/// # Safety
/// START is aligned to `CONS_BLOCK_BYTES' and the `image_cons_bytes'
/// bytes from it are writable for the life of the process, the blocks'
/// metadata zero, and no other block overlaps them.
pub(crate) unsafe fn adopt_image_cons_blocks(start: *mut u8, count: usize) {
    let start = start as usize;
    assert!(
        start.is_multiple_of(CONS_BLOCK_ALIGN),
        "unaligned image conses"
    );
    let epoch = super::types::current_mark_epoch();
    let mut blocks = Vec::with_capacity(count.div_ceil(CELLS_PER_BLOCK));
    for block in 0..count.div_ceil(CELLS_PER_BLOCK) {
        let base = start + block * CONS_BLOCK_BYTES;
        let cells = (count - block * CELLS_PER_BLOCK).min(CELLS_PER_BLOCK);
        // SAFETY: the caller's contract: a block's storage whose metadata
        // nothing reads yet; its first CELLS cells are written, the rest
        // stay free.
        unsafe {
            ConsMetadata::init(std::ptr::addr_of_mut!((*(base as *mut ConsBlock)).metadata));
            cons_mark(cons_cell(base, 0)).allocate_run(cells, epoch);
        }
        blocks.push((base, CONS_BLOCK_BYTES));
    }
    register_image_blocks(BlockKind::Cons, &blocks);
}

/// Fcons's `cons_block_index == CONS_BLOCK_SIZE' arm.  Out of line, and
/// reached by a tail call, so the allocation path needs no frame.
#[cold]
#[inline(never)]
fn allocate_cons_in_new_block(cell: ConsCell) -> ConsRef {
    let block = new_block(BlockKind::Cons);
    let first = cons_cell(block, 0);
    BUMP_NEXT.store(first as usize + CELL_SIZE, Ordering::Relaxed);
    BUMP_END.store(
        cons_cell(block, CELLS_PER_BLOCK) as usize,
        Ordering::Relaxed,
    );
    // SAFETY: the new block's first cell is free.
    unsafe { fill_cons(first, cell) }
}

/// # Safety
/// SLOT is an uninitialized (free) cell of a block, just taken from the
/// free list or the bump region.
#[inline(always)]
unsafe fn fill_cons(slot: *mut ConsCell, cell: ConsCell) -> ConsRef {
    LIVE_CONSES.store(LIVE_CONSES.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
    counters::tally(&counters::CONS_CELLS_CONSED, 1);
    // SAFETY: the caller's contract; every field is written before a
    // handle is made.
    unsafe {
        std::ptr::write(slot, cell);
        // A cell born during a collection (the weak-table sweep conses
        // between the mark and the sweep) carries the collection's epoch
        // and survives its sweep; born outside one, it carries the last
        // epoch, which the next collection does not reuse.
        cons_mark(slot).allocate(super::types::current_mark_epoch());
        ConsRef(NonNull::new_unchecked(slot))
    }
}

/// alloc.c's `lisp_align_free' of a block every cell of which is free
/// (the sweep found nothing live in it).
fn release_block(start: usize, kind: BlockKind) {
    // The image's mapped blocks stay as they are, their cells free.
    if is_mapped_block(start) {
        return;
    }
    unregister_block(start);
    // SAFETY: a block `new_block' allocated of this kind, unregistered
    // above; every cell is free (nothing reaches it).
    unsafe { arena::release_block(start as *mut u8, block_bytes(kind), block_alignment(kind)) };
}

/// alloc.c's `lisp_align_malloc' of a block of KIND: every cell free.
fn new_block(kind: BlockKind) -> usize {
    let block = arena::allocate_block(block_bytes(kind), block_alignment(kind));
    assert!(!block.is_null(), "out of memory for a block");
    let start = block as usize;
    match kind {
        BlockKind::Cons => {
            // SAFETY: fresh aligned storage; no payload slot is allocated.
            unsafe {
                ConsMetadata::init(std::ptr::addr_of_mut!(
                    (*(block.cast::<ConsBlock>())).metadata
                ))
            };
        }
        BlockKind::Float => {
            // SAFETY: a fresh aligned FloatBlock allocation. The cells stay
            // uninitialized; zero allocation bits reject every unused slot.
            unsafe {
                std::ptr::addr_of_mut!((*(block.cast::<FloatBlock>())).marks)
                    .write(FloatMarks::new())
            };
        }
        BlockKind::Symbol => {
            // SAFETY: the block just allocated.
            unsafe { symbols::init_block(start) };
        }
        BlockKind::String => {
            // SAFETY: the block just allocated.
            unsafe { strings::init_block(start) };
        }
        BlockKind::VectorBlock | BlockKind::LargeVector => {
            unreachable!("vectors have their own blocks (alloc/vectors.rs)")
        }
    }
    register_block(start, block_bytes(kind), kind);
    start
}

/// alloc.c:live_cons_holding: the allocated cell containing ADDRESS, or
/// None when ADDRESS is not inside a live cell of a cons block.
///
/// # Safety
/// The blocks registry is read under its lock; the cell's mark word is
/// readable in every state.
pub(crate) unsafe fn mem_find(address: usize) -> Option<Found> {
    // SAFETY: the caller's contract; the registry is locked for the
    // lookup alone.
    unsafe {
        mem_find_with(address, |address| {
            BLOCKS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .all
                .range(..=address)
                .next_back()
                .map(|(&start, &kind)| (start, kind))
        })
    }
}

/// `mem_find', with LOOKUP naming the registered block at or below an
/// address inside the heap's bounds.
///
/// # Safety
/// As `mem_find'.
#[inline(always)]
unsafe fn mem_find_with(
    address: usize,
    lookup: impl FnOnce(usize) -> Option<(usize, BlockKind)>,
) -> Option<Found> {
    if let Some(vector) = vectors::zero_vector_at(address) {
        return Some(Found::Vectorlike(vector));
    }
    if let Some(string) = strings::empty_string_at(address) {
        return Some(Found::String(string));
    }
    if address < HEAP_LOW.load(Ordering::Relaxed) || address >= HEAP_HIGH.load(Ordering::Relaxed) {
        return None;
    }
    let (start, kind) = lookup(address)?;
    let offset = address.checked_sub(start)?;
    match kind {
        BlockKind::Cons => {
            let index = offset.checked_sub(CONS_CELLS)? / CELL_SIZE;
            if index >= CELLS_PER_BLOCK {
                return None;
            }
            let cell = cons_cell(start, index);
            // SAFETY: inside a registered block.
            let mark = unsafe { cons_mark(cell) };
            mark.allocated().then_some(Found::Cons(cell))
        }
        BlockKind::Float => {
            let index = offset.checked_sub(FLOAT_CELLS)? / FLOAT_CELL_SIZE;
            if index >= FLOATS_PER_BLOCK {
                return None;
            }
            let cell = float_cell(start, index);
            // SAFETY: inside a registered float block.
            let mark = unsafe { float_mark(cell) };
            mark.allocated().then_some(Found::Float(cell))
        }
        BlockKind::VectorBlock => {
            vectors::live_small_vector_holding(start, address).map(Found::Vectorlike)
        }
        BlockKind::LargeVector => {
            vectors::live_large_vector_holding(start, address).map(Found::Vectorlike)
        }
        // SAFETY: a registered symbol block.
        BlockKind::Symbol => {
            unsafe { symbols::live_symbol_holding(start, address) }.map(Found::Symbol)
        }
        BlockKind::String => {
            // SAFETY: a registered string block.
            unsafe { strings::live_string_holding(start, address) }.map(Found::String)
        }
    }
}

/// `mem_find' for a cons cell's address (a weak reference, a native view).
///
/// # Safety
/// As `mem_find'.
pub(crate) unsafe fn mem_find_cons(address: usize) -> Option<*mut ConsCell> {
    match unsafe { mem_find(address) } {
        Some(Found::Cons(cell)) => Some(cell),
        _ => None,
    }
}

/// The serial of the allocated cell at ADDRESS (a cell's address), or
/// None when the sweep has freed it (or it was never a cell): for a
/// holder that keeps a cell's address across a collection without
/// keeping the cell (test witnesses hidden from the conservative scan).
#[cfg(test)]
pub(crate) fn allocated_serial(address: usize) -> Option<u64> {
    // SAFETY: `mem_find' answers an allocated cell, whose serial is
    // readable.
    unsafe {
        mem_find_cons(address)
            .filter(|cell| *cell as usize == address)
            .map(|cell| cons_mark(cell).serial())
    }
}

/// The blocks of KIND.
fn blocks_of(kind: BlockKind) -> Vec<usize> {
    BLOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .by_kind[kind as usize]
        .keys()
        .copied()
        .collect()
}

/// The blocks of KIND for a sweep, each with whether it is an image block.
fn sweep_blocks_of(kind: BlockKind) -> Vec<(usize, bool)> {
    BLOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .by_kind[kind as usize]
        .iter()
        .map(|(&start, &image)| (start, image))
        .collect()
}

/// The cons blocks, for the sweep and the checks.
fn all_blocks() -> Vec<usize> {
    blocks_of(BlockKind::Cons)
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
    for (start, image_block) in sweep_blocks_of(BlockKind::Cons) {
        let lim = if bump_end == cons_cell(start, CELLS_PER_BLOCK) as usize {
            (bump_next - cons_cell(start, 0) as usize) / CELL_SIZE
        } else {
            CELLS_PER_BLOCK
        };
        let chain_before = free_list;
        let mut this_free = 0usize;
        // SAFETY: a registered block's metadata stays initialized until the
        // block is released.
        let metadata = unsafe { &*std::ptr::addr_of!((*(start as *const ConsBlock)).metadata) };
        // A block no mark reached this collection holds stale marks only.
        let current = metadata.epoch.get() == epoch;
        // A bitmap word at a time, as alloc.c's sweep_conses reads its mark
        // words: a cell is live when allocated and marked; an allocated
        // cell that is not becomes free (its bits cleared; a cons has
        // nothing to drop); every cell that is not live goes on the free
        // list, in address order as before.
        for word in 0..lim.div_ceil(usize::BITS as usize) {
            let first = word * usize::BITS as usize;
            let valid = if lim - first >= usize::BITS as usize {
                usize::MAX
            } else {
                (1usize << (lim - first)) - 1
            };
            let allocated = metadata.allocated[word].get();
            let marked = metadata.marked[word].get();
            let live = allocated & if current { marked } else { 0 } & valid;
            let dead = allocated & !live & valid;
            if dead != 0 {
                metadata.allocated[word].set(allocated & !dead);
                metadata.marked[word].set(marked & !dead);
                let given = &metadata.serial_given[word];
                given.set(given.get() & !dead);
                if cfg!(debug_assertions) {
                    let mut bits = dead;
                    while bits != 0 {
                        let index = first + bits.trailing_zeros() as usize;
                        bits &= bits - 1;
                        // SAFETY: a cell of this block nothing reaches.
                        unsafe { poison(cons_cell(start, index)) };
                    }
                }
            }
            if !image_block {
                num_used += live.count_ones() as usize;
            }
            let mut free = !live & valid;
            this_free += free.count_ones() as usize;
            if image_block {
                continue;
            }
            while free != 0 {
                let index = first + free.trailing_zeros() as usize;
                free &= free - 1;
                let cell = cons_cell(start, index);
                // SAFETY: a free cell's first word is the free-list link.
                unsafe { (*cell.cast::<FreeCell>()).next = free_list };
                free_list = cell;
            }
        }
        if image_block {
            if this_free == CELLS_PER_BLOCK {
                released.push(start);
            }
        } else if this_free == CELLS_PER_BLOCK && num_free > CELLS_PER_BLOCK {
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
        release_block(start, BlockKind::Cons);
    }
    LIVE_CONSES.store(num_used, Ordering::Relaxed);
    FREE_CONSES.store(num_free, Ordering::Relaxed);
    if verify_heap_enabled() {
        verify_heap();
    }
    num_used
}

/// `EMAXX_GC_VERIFY': scan cons blocks around every sweep. Before sweeping,
/// check cons-to-cons and cons-to-vectorlike mark edges; afterwards, check
/// cons-to-cons allocation edges. Other heap fields are not verified here.
fn verify_heap_enabled() -> bool {
    static VERIFY: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *VERIFY.get_or_init(|| std::env::var_os("EMAXX_GC_VERIFY").is_some())
}

/// Before the sweep, with the optional verifier enabled: each cons or
/// supported vectorlike named by a marked cons must also be marked.
fn verify_marking(epoch: u32) {
    for start in all_blocks() {
        for index in 0..CELLS_PER_BLOCK {
            let cell = cons_cell(start, index);
            // SAFETY: a registered cons block, including its metadata.
            let mark = unsafe { cons_mark(cell) };
            if !mark.is_marked(epoch) {
                continue;
            }
            let live = unsafe { &*cell };
            for (which, field) in [("car", &live.car), ("cdr", &live.cdr)] {
                let value = field.get();
                assert_ne!(
                    vectorlike_is_marked(&value, epoch),
                    Some(false),
                    "marked cons {:#x} (serial {}) holds an unmarked vectorlike in {which}: {}",
                    cell as usize,
                    mark.serial(),
                    describe(&value)
                );
                if let Kind::Cons(target) = value.kind() {
                    // SAFETY: a cons field holds a Lisp object in a live block.
                    let target_mark = unsafe { cons_mark(target.as_ptr()) };
                    assert!(
                        target_mark.is_marked(epoch),
                        "marked cons {:#x} (serial {}) holds unmarked cons {:#x} (serial {}, allocated {}) in {which} at epoch {epoch}",
                        cell as usize,
                        mark.serial(),
                        target.as_ptr() as usize,
                        target_mark.serial(),
                        target_mark.allocated()
                    );
                }
            }
        }
    }
}

/// The mark state of a vectorlike the value names, for the checks.
fn vectorlike_is_marked(value: &super::types::Value, epoch: u32) -> Option<bool> {
    match value.kind() {
        Kind::Vector(vector) => Some(vector.mark_bit().is_marked(epoch)),
        Kind::Closure(lambda) => Some(lambda.mark_bit().is_marked(epoch)),
        Kind::NativeFunction(function) => Some(function.mark_bit().is_marked(epoch)),
        Kind::NativeCompUnit(unit) => Some(unit.mark_bit().is_marked(epoch)),
        Kind::Buffer(buffer) => Some(buffer.mark_bit().is_marked(epoch)),
        Kind::Marker(marker) => Some(marker.mark_bit().is_marked(epoch)),
        Kind::Overlay(overlay) => Some(overlay.mark_bit().is_marked(epoch)),
        Kind::StringObject(state) => Some(state.mark_bit().is_marked(epoch)),
        Kind::ReaderForm(form) => Some(form.mark_bit().is_marked(epoch)),
        Kind::BigInteger(integer) => Some(integer.mark_bit().is_marked(epoch)),
        Kind::CharTable(table) => Some(table.mark_bit().is_marked(epoch)),
        Kind::HashTable(table) => Some(table.mark_bit().is_marked(epoch)),
        Kind::SubCharTable(table) => Some(table.mark_bit().is_marked(epoch)),
        Kind::Frame(frame) => Some(frame.mark_bit().is_marked(epoch)),
        Kind::Terminal(terminal) => Some(terminal.mark_bit().is_marked(epoch)),
        Kind::Record(record) => Some(record.mark_bit().is_marked(epoch)),
        Kind::LispRecord(record) => Some(record.mark_bit().is_marked(epoch)),
        Kind::Finalizer(finalizer) => Some(finalizer.mark_bit().is_marked(epoch)),
        Kind::SymbolWithPos(positioned) => Some(positioned.mark_bit().is_marked(epoch)),
        _ => None,
    }
}

#[cfg(test)]
mod verifier_tests;

/// A word of a cell, for the reports: its kind, a symbol's name, an
/// integer, a cons's address.
fn describe(value: &super::types::Value) -> String {
    match value.kind() {
        Kind::Symbol(symbol) => format!("symbol {}", symbol.as_str()),
        Kind::Integer(n) => format!("integer {n}"),
        Kind::Cons(cell) => format!("cons {:#x}", cell.as_ptr() as usize),
        Kind::StringObject(text) => format!(
            "string {:?}",
            text.borrow().text().chars().take(24).collect::<String>()
        ),
        other => other.value().type_name().to_string(),
    }
}

/// GC_CHECK_MARKED_OBJECTS' spirit after a sweep: no allocated cell names
/// a freed one.  A live cell that does means a reference the mark phase
/// could not see was written into it, or was reached but not traced.
fn verify_heap() {
    for start in all_blocks() {
        for index in 0..CELLS_PER_BLOCK {
            let cell = cons_cell(start, index);
            // SAFETY: inside a registered block.
            if !unsafe { cons_mark(cell) }.allocated() {
                continue;
            }
            // SAFETY: an allocated cell.
            let live = unsafe { &*cell };
            for (which, field) in [("car", &live.car), ("cdr", &live.cdr)] {
                let value = field.get();
                if let super::types::Kind::Cons(target) = (value).kind() {
                    // SAFETY: the pointer came from a live cell; only its
                    // mark word is read.
                    if !unsafe { cons_mark(target.as_ptr()) }.allocated() {
                        panic!(
                            "after the sweep, live cons {:#x} (serial {}) holds freed cons {:#x} in its {which}",
                            cell as usize,
                            unsafe { cons_mark(cell) }.serial(),
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

/// gcstat's `total_free_conses': the last sweep's count (Fcons does not
/// decrement it).
#[allow(dead_code)]
pub(crate) fn free_conses() -> usize {
    FREE_CONSES.load(Ordering::Relaxed)
}

/// Physical block storage for allocator validation, including unused slots,
/// serials, bitmaps and alignment padding. Payload charging alone is not a
/// report of retained process memory.
#[cfg(test)]
pub(crate) fn cons_storage_for_test() -> [usize; 4] {
    [
        all_blocks().len(),
        std::mem::size_of::<ConsBlock>(),
        CELLS_PER_BLOCK * CELL_SIZE,
        std::mem::size_of::<ConsBlock>() - CELLS_PER_BLOCK * CELL_SIZE,
    ]
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
    if previous == 0 {
        clear_stack_below();
    }
    result
}

/// After a collection, zero the stack area its frames used (below this
/// frame): the marker and the sweep leave words naming cells (interior
/// pointers into the cells they read) where a later, deeper call chain
/// lays its frames, and the next collection's scan would take them as
/// roots.  GNU's alloc.c does not do this (its mark_object leaves fewer
/// such words, and its frames are smaller); the Boehm collector does
/// (`GC_clear_stack'), for the same reason.  Sixty-four kilobytes cover
/// the collection's own depth many times over.
#[inline(never)]
fn clear_stack_below() {
    let mut scratch = std::mem::MaybeUninit::<[usize; 1 << 13]>::uninit();
    // SAFETY: a write of every word of the array on this frame.
    unsafe {
        std::ptr::write_bytes(scratch.as_mut_ptr().cast::<u8>(), 0, 8 << 13);
    }
    std::hint::black_box(&scratch);
}

/// The recorded `stack_top' of the running collection, or zero.
pub(crate) fn stack_top() -> usize {
    STACK_TOP.with(Cell::get)
}

/// alloc.c:mark_stack for the running thread: the callee-saved registers
/// spilled into a local array, then every word from the current stack
/// pointer to this stack's base that names an allocated cell marks it.
/// Waiting callers' stacks are separate regions in `mark_all_stacks`.
#[inline(never)]
pub(crate) fn mark_stack(base: Option<usize>, mut mark: impl FnMut(Value)) {
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
    std::hint::black_box(&spill);
}

/// lisp.h's `VALMASK' complement: the three tag bits of a `Lisp_Object'.
const TAG_BITS: usize = 7;

/// alloc.c:mark_maybe_pointer. Validate a tagged word or interior pointer
/// against the shared allocator before forming a Lisp value. Both native
/// roots and conservative stack scans enter the same ordinary graph walk.
pub(crate) fn conservative_value(word: usize) -> Option<Value> {
    // SAFETY: mem_find validates the allocation before any payload is read.
    value_of_found(word, unsafe { mem_find(word & !TAG_BITS) }?)
}

/// `conservative_value' of each of WORDS, the registry locked once for
/// them all (a stack scan's thousands of words).
pub(crate) fn conservative_values(words: impl IntoIterator<Item = usize>) -> Vec<Value> {
    let blocks = BLOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let lookup = |address: usize| {
        blocks
            .all
            .range(..=address)
            .next_back()
            .map(|(&start, &kind)| (start, kind))
    };
    words
        .into_iter()
        .filter_map(|word| {
            // SAFETY: as in `conservative_value'; nothing below takes the
            // registry's lock.
            let found = unsafe { mem_find_with(word & !TAG_BITS, lookup) }?;
            value_of_found(word, found)
        })
        .collect()
}

/// The value WORD names, given the allocation `mem_find' found for it.
fn value_of_found(word: usize, found: Found) -> Option<Value> {
    Some(match found {
        Found::Cons(cell) => {
            // alloc.c:live_cons_holding accepts the cell, its Lisp tag,
            // or the actual cdr field address. Clearing arbitrary tag bits
            // must not turn padding or a byte-sized selector into a root.
            let offset = word.checked_sub(cell as usize)?;
            if offset != 0 && offset != 3 && offset != std::mem::offset_of!(ConsCell, cdr) {
                return None;
            }
            Value::Cons(unsafe { ConsRef::from_raw(cell) })
        }
        Found::Float(cell) => Value::Float(FloatRef(unsafe { NonNull::new_unchecked(cell) })),
        Found::Vectorlike(header) => unsafe { vectors::value_of(header) },
        Found::String(cell) => Value::StringObject(unsafe { StringObjectRef::from_raw(cell) }),
        Found::Symbol(cell) => Value::Symbol(super::types::SymbolName::from_ref(unsafe {
            SymbolRef::from_raw(cell)
        })),
    })
}

/// Mark every cell a word of [LOW, HIGH) names (a parked coroutine's
/// stack, given its saved stack pointer and base).
///
/// # Safety
/// [LOW, HIGH) must be readable memory.
pub(crate) unsafe fn scan_words(low: usize, high: usize, mark: &mut impl FnMut(Value)) {
    let low = low & !(std::mem::size_of::<usize>() - 1);
    let mut address = low;
    while address + std::mem::size_of::<usize>() <= high {
        // SAFETY: the caller's contract.
        let word = unsafe { std::ptr::read_volatile(address as *const usize) };
        address += std::mem::size_of::<usize>();
        if let Some(value) = conservative_value(word) {
            mark(value);
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub(crate) fn spill_registers(spill: &mut [usize; 16]) {
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
pub(crate) fn spill_registers(spill: &mut [usize; 16]) {
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
pub(crate) fn spill_registers(_spill: &mut [usize; 16]) {}

/// The current stack pointer, approximately: the address of a local.
#[inline(never)]
pub(crate) fn approximate_stack_pointer() -> usize {
    let marker = 0u8;
    std::hint::black_box(std::ptr::addr_of!(marker) as usize)
}
