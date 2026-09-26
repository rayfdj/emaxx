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

use super::types::{ConsCell, Kind, MarkBit, Value};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::ptr::NonNull;
use std::sync::Mutex;

mod finalizers;
mod symbols;
pub(crate) use finalizers::FinalizerList;
pub use finalizers::{FinalizerRef, FinalizerState};
pub(crate) mod vectors;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};
pub use symbols::{SymbolCell, SymbolRef};
pub(crate) use symbols::{allocate_symbol, live_symbols, sweep_symbols};
pub use vectors::{ClosureRef, VectorHeader, VectorRef, VectorTag, VectorlikeRef};
pub(crate) use vectors::{
    FreedRecord, live_record_census, live_string_object_census, live_vector_census, sweep_vectors,
    take_freed_records,
};

/// alloc.c's block geometry: the cells per block that its formulas give
/// with the C sizes (`BLOCK_ALIGN' 1 << 15 without unexec, `BLOCK_BYTES'
/// = BLOCK_ALIGN - sizeof (struct ablocks *), `MALLOC_SIZE_NEAR (1024)'
/// = 1016 under glibc's 16-byte alignment). Cons cells still carry a
/// second payload and metadata, so their blocks exceed C's footprint.
/// Float cells have C's eight-byte payload and a 32 KiB block; their
/// extra allocation bitmap slightly reduces the cells per block.
const C_BLOCK_BYTES: usize = (1 << 15) - 8;
const C_MALLOC_SIZE_NEAR_1024: usize = 1016;
const CELL_SIZE: usize = std::mem::size_of::<ConsCell>();
/// `CONS_BLOCK_SIZE': the block's bytes less the block pointer and the
/// padding, times CHAR_BIT, over the cons's bits plus its mark bit, with
/// a 16-byte cons.
pub(crate) const CELLS_PER_BLOCK: usize = ((C_BLOCK_BYTES - 8 - (16 - 8)) * 8) / (16 * 8 + 1);
/// Eight-byte float payloads and two bits per slot: the collection mark,
/// and allocation state for checked native-word decoding. GNU has one mark
/// bit; the allocation bit replaces this runtime's in-object FREE_MARK.
pub(crate) const FLOATS_PER_BLOCK: usize = ((C_BLOCK_BYTES - 8) * 8) / (8 * 8 + 2);
/// `STRING_BLOCK_SIZE': (MALLOC_SIZE_NEAR (1024) - sizeof (struct
/// string_block *)) / sizeof (struct Lisp_String), a 32-byte string.
pub(crate) const STRINGS_PER_BLOCK: usize = (C_MALLOC_SIZE_NEAR_1024 - 8) / 32;
/// `SYMBOL_BLOCK_SIZE': (1020 - sizeof (struct symbol_block *)) /
/// sizeof (struct Lisp_Symbol), a 48-byte symbol.
pub(crate) const SYMBOLS_PER_BLOCK: usize = (1020 - 8) / 48;
const BLOCK_ALIGN: usize = 4096;

/// The bytes of a block of KIND: its cells, rounded up to the alignment.
fn block_bytes(kind: BlockKind) -> usize {
    let cells = match kind {
        BlockKind::Cons => CELLS_PER_BLOCK * CELL_SIZE,
        BlockKind::Float => std::mem::size_of::<FloatBlock>(),
        BlockKind::String => STRINGS_PER_BLOCK * STRING_CELL_SIZE,
        BlockKind::Symbol => SYMBOLS_PER_BLOCK * symbols::SYMBOL_CELL_SIZE,
        BlockKind::VectorBlock | BlockKind::LargeVector => {
            unreachable!("vector storage is allocated by its own module")
        }
    };
    cells.div_ceil(BLOCK_ALIGN) * BLOCK_ALIGN
}

fn block_alignment(kind: BlockKind) -> usize {
    if kind == BlockKind::Float {
        FLOAT_BLOCK_ALIGN
    } else {
        BLOCK_ALIGN
    }
}

/// The mark word of a cell on the free list (alloc.c sets the mark bit
/// of free cells so the sweep skips them; a live cell carries its
/// collection's epoch, which `begin_mark_epoch' never makes this).
pub(crate) const FREE_MARK: u32 = u32::MAX;

/// alloc.c's `mem_type' of a block: what its cells are.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum BlockKind {
    Cons,
    Float,
    String,
    /// alloc.c's `MEM_TYPE_VECTOR_BLOCK': small vectors carved by size.
    VectorBlock,
    /// alloc.c's `MEM_TYPE_VECTORLIKE': one large vector on its own.
    LargeVector,
    /// alloc.c's `MEM_TYPE_SYMBOL'.
    Symbol,
}

/// The blocks, by start address, with their kinds, for `mem_find'
/// (alloc.c's mem tree, a red-black tree there; the map's lookup is the
/// same order).
static BLOCKS: Mutex<BTreeMap<usize, BlockKind>> = Mutex::new(BTreeMap::new());

/// alloc.c's `mem_insert' of a block of KIND at START.
fn register_block(start: usize, kind: BlockKind) {
    BLOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(start, kind);
}

/// alloc.c's `mem_delete'.
fn unregister_block(start: usize) {
    let removed = BLOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(&start);
    debug_assert!(removed.is_some());
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
/// The allocation count, a serial for `WeakConsRef'.
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

#[repr(C, align(32768))]
struct FloatBlock {
    cells: [std::mem::MaybeUninit<FloatCell>; FLOATS_PER_BLOCK],
    marks: FloatMarks,
}

const _: () = {
    assert!(FLOAT_CELL_SIZE == 8);
    assert!(std::mem::size_of::<FloatBlock>() == FLOAT_BLOCK_ALIGN);
    assert!(FLOATS_PER_BLOCK * FLOAT_CELL_SIZE < FLOAT_BLOCK_ALIGN);
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

    fn release(&self) {
        let (word, mask) = self.bit();
        self.block.allocated[word].set(self.block.allocated[word].get() & !mask);
        self.block.marked[word].set(self.block.marked[word].get() & !mask);
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
    let index = (address - base) / FLOAT_CELL_SIZE;
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
    FLOAT_BUMP_NEXT.store(block + FLOAT_CELL_SIZE, Ordering::Relaxed);
    FLOAT_BUMP_END.store(
        block + FLOATS_PER_BLOCK * FLOAT_CELL_SIZE,
        Ordering::Relaxed,
    );
    block as *mut FloatCell
}

/// alloc.c:sweep_floats, as `sweep_conses' below.
pub(crate) fn sweep_floats(epoch: u32) -> usize {
    let bump_next = FLOAT_BUMP_NEXT.load(Ordering::Relaxed);
    let bump_end = FLOAT_BUMP_END.load(Ordering::Relaxed);
    let mut free_list: *mut FloatCell = std::ptr::null_mut();
    let mut num_free = 0usize;
    let mut num_used = 0usize;
    let mut released = Vec::new();
    for start in blocks_of(BlockKind::Float) {
        let lim = if bump_end == start + FLOATS_PER_BLOCK * FLOAT_CELL_SIZE {
            (bump_next - start) / FLOAT_CELL_SIZE
        } else {
            FLOATS_PER_BLOCK
        };
        let chain_before = free_list;
        let mut this_free = 0usize;
        for index in 0..lim {
            let cell = (start + index * FLOAT_CELL_SIZE) as *mut FloatCell;
            // SAFETY: inside a registered float block.
            let mark = unsafe { float_mark(cell) };
            if mark.is_marked(epoch) {
                num_used += 1;
                continue;
            }
            // SAFETY: an unmarked cell holds no owned storage; clear its
            // allocation bit and store the free-list link in its one word.
            unsafe {
                mark.release();
                (*cell.cast::<FreeFloat>()).next = free_list;
            }
            this_free += 1;
            free_list = cell;
        }
        if this_free == FLOATS_PER_BLOCK && num_free > FLOATS_PER_BLOCK {
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
    String(*mut StringCell),
    Vectorlike(*mut VectorHeader),
    Symbol(*mut SymbolCell),
}

/// alloc.c's `string_block', `string_free_list' and the index into the
/// newest block; gcstat's `total_strings' and `total_string_bytes'.
static STRING_FREE_LIST: AtomicPtr<StringCell> = AtomicPtr::new(std::ptr::null_mut());
static STRING_BUMP_NEXT: AtomicUsize = AtomicUsize::new(0);
static STRING_BUMP_END: AtomicUsize = AtomicUsize::new(0);
static LIVE_STRINGS: AtomicUsize = AtomicUsize::new(0);
static LIVE_STRING_BYTES: AtomicUsize = AtomicUsize::new(0);
static FREE_STRINGS: AtomicUsize = AtomicUsize::new(0);
const STRING_CELL_SIZE: usize = std::mem::size_of::<StringCell>();

/// The storage size of a text that is not a Lisp string allocation at
/// all (a symbol's host-side key): counted nowhere.
pub(crate) const UNTRACKED_TEXT: usize = usize::MAX;

/// alloc.c's `struct Lisp_String': the text (its bytes on the Rust heap,
/// as a large string's are malloc'd in C; there is no sblock and no
/// compaction), `size_byte' as the storage size the census reads, the
/// mark word, and a serial for a holder that keeps the address without
/// keeping the string.
#[repr(C)]
pub struct StringCell {
    // This existing size word also distinguishes plain text storage from
    // the current property-bearing string allocation. It adds no word or
    // lookup. Both storage forms now use GNU's string tag at their address.
    storage_bytes: usize,
    text: String,
    mark: MarkBit,
    serial: u64,
}

/// `Lisp_Object' for a string: the cell's address, copied freely, valid
/// while the collector can reach the cell.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct TextRef(NonNull<StringCell>);

impl TextRef {
    #[inline]
    fn cell(&self) -> &StringCell {
        // SAFETY: a `TextRef' names an allocated cell (see `ConsRef').
        let cell = unsafe { self.0.as_ref() };
        debug_assert!(
            cell.mark.raw() != FREE_MARK,
            "use of a string the collector freed"
        );
        cell
    }

    /// The text, as the `String' the cell owns.
    #[inline]
    pub(crate) fn text(&self) -> &String {
        &self.cell().text
    }

    /// The text.  Its lifetime is the cell's, which the collector keeps
    /// while the string is reachable: a `Lisp_Object' read of `SDATA'.
    #[inline]
    pub fn as_str(&self) -> &'static str {
        // SAFETY: the cell is allocated while the caller holds a value
        // naming it (the collector's contract); the text is not moved or
        // freed before the cell is.
        unsafe { &*(self.cell().text.as_str() as *const str) }
    }

    /// The handle for a cell the allocator handed out (a value's word).
    ///
    /// # Safety
    /// CELL must be an allocated string cell.
    #[inline(always)]
    pub(crate) unsafe fn from_raw(cell: *mut StringCell) -> Self {
        // SAFETY: the caller's contract.
        Self(unsafe { NonNull::new_unchecked(cell) })
    }

    pub(crate) fn identity_ptr(&self) -> usize {
        self.0.as_ptr() as usize
    }

    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    pub(crate) fn mark_bit(&self) -> &MarkBit {
        &self.cell().mark
    }

    pub(crate) fn serial(&self) -> u64 {
        self.cell().serial
    }

    /// The text copied out (the cell keeps its own until the sweep).
    pub fn into_string(self) -> String {
        self.cell().text.clone()
    }
}

struct FreeString {
    next: *mut StringCell,
}

/// alloc.c's `empty_unibyte_string': one permanently rooted empty string,
/// outside every block (no sweep reaches it), returned by every
/// zero-length allocation so that `(eq "" "")' holds.
static EMPTY_TEXT: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

pub(crate) fn empty_text() -> TextRef {
    let address = *EMPTY_TEXT.get_or_init(|| {
        Box::leak(Box::new(StringCell {
            text: String::new(),
            storage_bytes: UNTRACKED_TEXT,
            mark: MarkBit::default(),
            serial: 0,
        })) as *mut StringCell as usize
    });
    // SAFETY: a leaked cell: always allocated.
    TextRef(unsafe { NonNull::new_unchecked(address as *mut StringCell) })
}

/// alloc.c:allocate_string: the free list's head, else the next cell of
/// the newest string block; the text's bytes stay where the `String'
/// keeps them (allocate_string_data's large-string case).
pub(crate) fn allocate_string(text: String, storage_bytes: usize) -> TextRef {
    // GNU's STRING_BYTES_BOUND is below the pseudovector flag. Enforce the
    // allocator invariant even for an invalid internal/image caller before
    // publishing a word whose storage class is read from this header.
    assert!(storage_bytes < (1 << (usize::BITS - 2)) || storage_bytes == UNTRACKED_TEXT);
    let head = STRING_FREE_LIST.load(Ordering::Relaxed);
    let slot = if head.is_null() {
        bump_string()
    } else {
        // SAFETY: a free cell's first word is the free-list link.
        let next = unsafe { (*head.cast::<FreeString>()).next };
        STRING_FREE_LIST.store(next, Ordering::Relaxed);
        FREE_STRINGS.store(
            FREE_STRINGS.load(Ordering::Relaxed).saturating_sub(1),
            Ordering::Relaxed,
        );
        head
    };
    let serial = SERIAL.load(Ordering::Relaxed) + 1;
    SERIAL.store(serial, Ordering::Relaxed);
    if storage_bytes != UNTRACKED_TEXT {
        LIVE_STRINGS.store(LIVE_STRINGS.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
        LIVE_STRING_BYTES.store(
            LIVE_STRING_BYTES.load(Ordering::Relaxed) + storage_bytes,
            Ordering::Relaxed,
        );
    }
    // SAFETY: SLOT is a free cell of a string block; every field is
    // written before a handle is made (the epoch as for a cons).
    unsafe {
        std::ptr::write(
            slot,
            StringCell {
                text,
                storage_bytes,
                mark: MarkBit::default(),
                serial,
            },
        );
        (*slot).mark.set_raw(super::types::current_mark_epoch());
        TextRef(NonNull::new_unchecked(slot))
    }
}

fn bump_string() -> *mut StringCell {
    let next = STRING_BUMP_NEXT.load(Ordering::Relaxed);
    let end = STRING_BUMP_END.load(Ordering::Relaxed);
    if next < end {
        STRING_BUMP_NEXT.store(next + STRING_CELL_SIZE, Ordering::Relaxed);
        return next as *mut StringCell;
    }
    let block = new_block(BlockKind::String);
    STRING_BUMP_NEXT.store(block + STRING_CELL_SIZE, Ordering::Relaxed);
    STRING_BUMP_END.store(
        block + STRINGS_PER_BLOCK * STRING_CELL_SIZE,
        Ordering::Relaxed,
    );
    block as *mut StringCell
}

/// alloc.c:sweep_strings, as `sweep_conses': an unmarked cell's text is
/// dropped (its bytes freed) and the cell goes back on the free list.
pub(crate) fn sweep_strings(epoch: u32) -> (usize, usize) {
    let bump_next = STRING_BUMP_NEXT.load(Ordering::Relaxed);
    let bump_end = STRING_BUMP_END.load(Ordering::Relaxed);
    let mut free_list: *mut StringCell = std::ptr::null_mut();
    let mut num_free = 0usize;
    let mut num_used = 0usize;
    let mut used_bytes = 0usize;
    let mut released = Vec::new();
    for start in blocks_of(BlockKind::String) {
        let lim = if bump_end == start + STRINGS_PER_BLOCK * STRING_CELL_SIZE {
            (bump_next - start) / STRING_CELL_SIZE
        } else {
            STRINGS_PER_BLOCK
        };
        let chain_before = free_list;
        let mut this_free = 0usize;
        for index in 0..lim {
            let cell = (start + index * STRING_CELL_SIZE) as *mut StringCell;
            // SAFETY: inside a registered string block.
            let mark = unsafe { (*cell).mark.raw() };
            if mark == epoch {
                num_used += 1;
                // SAFETY: a marked, allocated cell.
                let bytes = unsafe { (*cell).storage_bytes };
                if bytes != UNTRACKED_TEXT {
                    used_bytes += bytes;
                }
                continue;
            }
            if mark != FREE_MARK {
                // SAFETY: an allocated, unmarked cell: nothing reaches
                // it, so its text is dropped and the slot becomes free.
                unsafe {
                    std::ptr::drop_in_place(std::ptr::addr_of_mut!((*cell).text));
                    if cfg!(debug_assertions) {
                        std::ptr::write_bytes(
                            cell.cast::<u8>().add(std::mem::size_of::<FreeString>()),
                            0xA5,
                            STRING_CELL_SIZE - std::mem::size_of::<FreeString>(),
                        );
                    }
                    (*cell).mark.set_raw(FREE_MARK);
                }
            }
            this_free += 1;
            // SAFETY: a free cell's first word is the free-list link.
            unsafe { (*cell.cast::<FreeString>()).next = free_list };
            free_list = cell;
        }
        if this_free == STRINGS_PER_BLOCK && num_free > STRINGS_PER_BLOCK {
            free_list = chain_before;
            released.push(start);
        } else {
            num_free += this_free;
        }
    }
    STRING_FREE_LIST.store(free_list, Ordering::Relaxed);
    for start in released {
        release_block(start, BlockKind::String);
    }
    LIVE_STRINGS.store(num_used, Ordering::Relaxed);
    LIVE_STRING_BYTES.store(used_bytes, Ordering::Relaxed);
    FREE_STRINGS.store(num_free, Ordering::Relaxed);
    (num_used, used_bytes)
}

pub(crate) fn live_strings() -> usize {
    LIVE_STRINGS.load(Ordering::Relaxed)
}

pub(crate) fn live_string_bytes() -> usize {
    LIVE_STRING_BYTES.load(Ordering::Relaxed)
}

thread_local! {
    #[cfg(test)]
    static ROOT_SCAN_ORIGIN: Cell<(&'static str, usize)> = const { Cell::new(("unclassified", 0)) };
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
    #[cfg(test)]
    ROOT_SCAN_ORIGIN.with(|origin| origin.set(("current-stack", 0)));
    mark_stack(base, &mut mark);
    #[cfg(test)]
    ROOT_SCAN_ORIGIN.with(|origin| origin.set(("parked-stack", 0)));
    let parked = PARKED_STACKS.with_borrow(Clone::clone);
    for (base, sp) in parked {
        if base > sp {
            // SAFETY: a parked coroutine's stack stays mapped and unchanged
            // until it resumes, which cannot happen during a collection.
            unsafe { scan_words(sp, base, &mut mark) };
        }
    }
    let drivers = DRIVER_REGIONS.with_borrow(Clone::clone);
    #[cfg(test)]
    ROOT_SCAN_ORIGIN.with(|origin| origin.set(("driving-stack", 0)));
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
    #[cfg(test)]
    ROOT_SCAN_ORIGIN.with(|origin| origin.set(("registered-heap-buffer", 0)));
    for (address, bytes) in heap {
        if bytes != 0 {
            // SAFETY: a registered buffer is a live allocation of that size
            // (its owner refreshes the entry when the buffer moves).
            unsafe { scan_words(address, address + bytes, &mut mark) };
        }
    }
}

#[cfg(test)]
pub(crate) fn diagnostic_root_origin() -> (&'static str, usize) {
    ROOT_SCAN_ORIGIN.with(Cell::get)
}

/// Temporary diagnostic: identify the active frame containing a conservative
/// root without changing the collector's stack range or marking decisions.
#[cfg(all(test, target_os = "linux"))]
pub(crate) fn diagnose_conservative_frame(address: usize) {
    use std::ffi::c_void;

    struct Trace {
        frames: [(usize, usize); 128],
        len: usize,
    }

    unsafe extern "C" {
        fn _Unwind_Backtrace(
            callback: unsafe extern "C" fn(*mut c_void, *mut c_void) -> libc::c_int,
            argument: *mut c_void,
        ) -> libc::c_int;
        fn _Unwind_GetCFA(context: *mut c_void) -> usize;
        fn _Unwind_GetIP(context: *mut c_void) -> usize;
    }

    unsafe extern "C" fn frame(context: *mut c_void, argument: *mut c_void) -> libc::c_int {
        // SAFETY: _Unwind_Backtrace synchronously supplies its live context
        // and the unique Trace pointer passed below. No unwinding or Lisp
        // allocation occurs in this callback.
        let trace = unsafe { &mut *argument.cast::<Trace>() };
        if trace.len == trace.frames.len() {
            return 5; // _URC_END_OF_STACK
        }
        trace.frames[trace.len] = unsafe { (_Unwind_GetCFA(context), _Unwind_GetIP(context)) };
        trace.len += 1;
        0 // _URC_NO_REASON: keep walking
    }

    let mut trace = Trace {
        frames: [(0, 0); 128],
        len: 0,
    };
    // SAFETY: the callback's context has this function's lifetime; the GCC
    // unwind ABI walks frames without executing their cleanup handlers.
    unsafe { _Unwind_Backtrace(frame, std::ptr::from_mut(&mut trace).cast()) };
    for pair in trace.frames[..trace.len].windows(2) {
        let [(low, ip), (high, _)] = *pair else {
            unreachable!("a window has two frames");
        };
        if !(low..high).contains(&address) {
            continue;
        }
        let pc = ip.saturating_sub(1);
        // SAFETY: dladdr fills this C struct and only inspects the address.
        let mut info: libc::Dl_info = unsafe { std::mem::zeroed() };
        if unsafe { libc::dladdr(pc as *const c_void, &mut info) } == 0 {
            eprintln!("GC conservative owner low={low:x} high={high:x} pc={pc:x}");
            break;
        }
        let relative_pc = pc - info.dli_fbase as usize;
        eprintln!(
            "GC conservative owner low={low:x} high={high:x} pc={pc:x} module_offset={relative_pc:x}"
        );
        if let Ok(executable) = std::env::current_exe() {
            match std::process::Command::new("addr2line")
                .args(["-a", "-f", "-C", "-i", "-e"])
                .arg(executable)
                .arg(format!("{relative_pc:x}"))
                .output()
            {
                Ok(output) => eprintln!(
                    "GC conservative owner symbols status={}\n{}{}",
                    output.status,
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                ),
                Err(error) => eprintln!("GC conservative owner symbolization: {error}"),
            }
        }
        break;
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
        let live = unsafe { mem_find_cons(address) }?;
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
    let block = new_block(BlockKind::Cons);
    BUMP_NEXT.store(block + CELL_SIZE, Ordering::Relaxed);
    BUMP_END.store(block + CELLS_PER_BLOCK * CELL_SIZE, Ordering::Relaxed);
    block as *mut ConsCell
}

/// alloc.c's `lisp_align_free' of a block every cell of which is free
/// (the sweep found nothing live in it).
fn release_block(start: usize, kind: BlockKind) {
    unregister_block(start);
    let layout = std::alloc::Layout::from_size_align(block_bytes(kind), block_alignment(kind))
        .expect("block layout");
    // SAFETY: a block `new_block' allocated with this layout, unregistered
    // above; every cell is free (nothing reaches it).
    unsafe { std::alloc::dealloc(start as *mut u8, layout) };
}

/// alloc.c's `lisp_align_malloc' of a block of KIND: every cell free.
fn new_block(kind: BlockKind) -> usize {
    let layout = std::alloc::Layout::from_size_align(block_bytes(kind), block_alignment(kind))
        .expect("block layout");
    // SAFETY: a non-zero layout.
    let block = unsafe { std::alloc::alloc(layout) };
    assert!(!block.is_null(), "out of memory for a block");
    let start = block as usize;
    match kind {
        BlockKind::Cons => {
            for index in 0..CELLS_PER_BLOCK {
                let cell = (start + index * CELL_SIZE) as *mut ConsCell;
                // SAFETY: inside the block just allocated; only the mark
                // word is written, at its field offset.
                unsafe { mark_of(cell).set_raw(FREE_MARK) };
            }
        }
        BlockKind::Float => {
            // SAFETY: a fresh aligned FloatBlock allocation. The cells stay
            // uninitialized; zero allocation bits reject every unused slot.
            unsafe {
                std::ptr::addr_of_mut!((*(block.cast::<FloatBlock>())).marks)
                    .write(FloatMarks::new())
            };
        }
        BlockKind::String => {
            for index in 0..STRINGS_PER_BLOCK {
                let cell = (start + index * STRING_CELL_SIZE) as *mut StringCell;
                // SAFETY: as for a float block.
                unsafe {
                    std::ptr::addr_of_mut!((*cell).mark)
                        .cast::<u32>()
                        .write(FREE_MARK)
                };
            }
        }
        BlockKind::Symbol => {
            // SAFETY: the block just allocated.
            unsafe { symbols::init_block(start) };
        }
        BlockKind::VectorBlock | BlockKind::LargeVector => {
            unreachable!("vectors have their own blocks (alloc/vectors.rs)")
        }
    }
    register_block(start, kind);
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
pub(crate) unsafe fn mem_find(address: usize) -> Option<Found> {
    if let Some(vector) = vectors::zero_vector_at(address) {
        return Some(Found::Vectorlike(vector));
    }
    if EMPTY_TEXT.get().is_some_and(|&empty| empty == address) {
        return Some(Found::String(address as *mut StringCell));
    }
    let blocks = BLOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let (&start, &kind) = blocks.range(..=address).next_back()?;
    drop(blocks);
    let offset = address.checked_sub(start)?;
    match kind {
        BlockKind::Cons => {
            let index = offset / CELL_SIZE;
            if index >= CELLS_PER_BLOCK {
                return None;
            }
            let cell = (start + index * CELL_SIZE) as *mut ConsCell;
            // SAFETY: inside a registered block.
            let mark = unsafe { mark_of(cell) }.raw();
            // A cell the bump pointer has not reached is free too (its
            // mark is FREE_MARK from the block's birth).
            (mark != FREE_MARK).then_some(Found::Cons(cell))
        }
        BlockKind::Float => {
            let index = offset / FLOAT_CELL_SIZE;
            if index >= FLOATS_PER_BLOCK {
                return None;
            }
            let cell = (start + index * FLOAT_CELL_SIZE) as *mut FloatCell;
            // SAFETY: inside a registered float block.
            let mark = unsafe { float_mark(cell) };
            mark.allocated().then_some(Found::Float(cell))
        }
        BlockKind::String => {
            let index = offset / STRING_CELL_SIZE;
            if index >= STRINGS_PER_BLOCK {
                return None;
            }
            let cell = (start + index * STRING_CELL_SIZE) as *mut StringCell;
            // SAFETY: inside a registered string block.
            let mark = unsafe { (*cell).mark.raw() };
            (mark != FREE_MARK).then_some(Found::String(cell))
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
/// keeping the cell (the native heap's views).
pub(crate) fn allocated_serial(address: usize) -> Option<u64> {
    // SAFETY: `mem_find' answers an allocated cell, whose serial is
    // readable.
    unsafe { mem_find_cons(address).map(|cell| (*cell).serial) }
}

/// The blocks of KIND, for a sweep.
fn blocks_of(kind: BlockKind) -> Vec<usize> {
    BLOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .filter(|entry| *entry.1 == kind)
        .map(|entry| *entry.0)
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
        release_block(start, BlockKind::Cons);
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
                if vectorlike_is_marked(&value, epoch) == Some(false) {
                    panic!(
                        "before the sweep of epoch {epoch}, marked cons {:#x} (serial {}, car {}, cdr {}) holds an unmarked vectorlike in its {which}: {}",
                        cell as usize,
                        live.serial,
                        describe(&live.car.value_in_place()),
                        describe(&live.cdr.value_in_place()),
                        describe(&value),
                    );
                }
                if let super::types::Kind::Cons(target) = (value).kind() {
                    // SAFETY: the pointer came from a marked cell; its
                    // words are readable in every state.
                    let target_mark = unsafe { mark_of(target.as_ptr().cast_mut()) }.raw();
                    if target_mark != epoch && target_mark != FREE_MARK {
                        let dead = &*target;
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

/// The mark state of a vectorlike the value names, for the checks.
fn vectorlike_is_marked(value: &super::types::Value, epoch: u32) -> Option<bool> {
    match value.kind() {
        Kind::Vector(vector) => Some(vector.mark_bit().is_marked(epoch)),
        Kind::Lambda(lambda) => Some(lambda.mark_bit().is_marked(epoch)),
        Kind::Buffer(buffer) => Some(buffer.mark_bit().is_marked(epoch)),
        Kind::StringObject(state) => Some(state.mark_bit().is_marked(epoch)),
        Kind::ReaderForm(form) => Some(form.mark_bit().is_marked(epoch)),
        Kind::BigInteger(integer) => Some(integer.mark_bit().is_marked(epoch)),
        _ => None,
    }
}

/// A word of a cell, for the reports: its kind, a symbol's name, an
/// integer, a cons's address.
fn describe(value: &super::types::Value) -> String {
    match value.kind() {
        Kind::Symbol(symbol) => format!("symbol {}", symbol.as_str()),
        Kind::Integer(n) => format!("integer {n}"),
        Kind::Cons(cell) => format!("cons {:#x}", cell.as_ptr() as usize),
        Kind::String(text) => format!(
            "string {:?}",
            text.as_str().chars().take(24).collect::<String>()
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
            let cell = (start + index * CELL_SIZE) as *mut ConsCell;
            // SAFETY: inside a registered block.
            if unsafe { mark_of(cell) }.raw() == FREE_MARK {
                continue;
            }
            // SAFETY: an allocated cell.
            let live = unsafe { &*cell };
            for (which, field) in [("car", &live.car), ("cdr", &live.cdr)] {
                let value = field.value_in_place();
                if let super::types::Kind::Cons(target) = (value).kind() {
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
        #[cfg(test)]
        if verify_heap_enabled() {
            ROOT_SCAN_ORIGIN.with(|origin| origin.set((origin.get().0, address)));
        }
        address += std::mem::size_of::<usize>();
        // alloc.c:mark_maybe_pointer under USE_LSB_TAG: a `Lisp_Object'
        // word carries its type in the low three bits, so the tag is
        // taken off before the word is looked up; a handle (an untagged
        // address) has them clear already.
        let word = word & !TAG_BITS;
        // SAFETY: `mem_find' validates the word before any cell is read.
        match unsafe { mem_find(word) } {
            // SAFETY: allocated cells.
            Some(Found::Cons(cell)) => mark(Value::Cons(unsafe { ConsRef::from_raw(cell) })),
            Some(Found::Float(cell)) => mark(Value::Float(FloatRef(unsafe {
                NonNull::new_unchecked(cell)
            }))),
            Some(Found::String(cell)) => mark(Value::String(TextRef(unsafe {
                NonNull::new_unchecked(cell)
            }))),
            // SAFETY: an allocated vector.
            Some(Found::Vectorlike(header)) => mark(unsafe { vectors::value_of(header) }),
            // SAFETY: an allocated symbol cell.
            Some(Found::Symbol(cell)) => {
                mark(Value::Symbol(super::types::SymbolName::from_ref(unsafe {
                    SymbolRef::from_raw(cell)
                })))
            }
            None => {}
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
