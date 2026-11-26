//! alloc.c's storage for vectors and pseudovectors: small ones in
//! `vector_block's of `VECTOR_BLOCK_SIZE' bytes, carved by size with the
//! free lists `vector_free_lists' (one per footprint, the last for
//! anything larger), large ones malloc'd on their own (`large_vectors'),
//! and `sweep_vectors', which coalesces runs of unmarked vectors into
//! one free vector, gives back a block with nothing live in it, and
//! frees the unmarked large vectors.
//!
//! An ordinary vector is `struct Lisp_Vector': the header, then the slots
//! in place.  A pseudovector (a closure, a buffer object, a bignum, a
//! string object, a reader form) is `allocate_pseudovector''s: the
//! header, then the kind's own fields; `cleanup_vector' runs the kind's
//! destructor at the sweep.  The handle of either is the header's
//! address, copied without a count, valid while the mark reaches it.
//!
//! The header and Lisp slots are each one machine word, including closure
//! slots. A large vector's mark is C's ARRAY_MARK_FLAG, in its header's
//! size word from the mark phase to `sweep_vectors', which clears it; the
//! small vectors of a block share its bitmap. Ordinary object access reads
//! neither: between collections no header carries the flag.

pub(crate) mod bool_vectors;
pub use bool_vectors::BoolVectorRef;
pub(crate) mod char_tables;
pub use char_tables::{CharTableRef, SubCharTableRef};
pub(crate) mod generic_records;
pub use generic_records::LispRecordRef;
pub(crate) mod hash_tables;
pub use hash_tables::HashTableRef;
pub(crate) mod native_functions;
pub use native_functions::NativeFunctionRef;
pub(crate) mod native_units;
pub use native_units::NativeUnitRef;
mod symbols_with_pos;
pub use symbols_with_pos::SymbolWithPosRef;
pub(crate) mod windows;
pub use windows::WindowObjectRef;

#[cfg(test)]
mod tests;

use super::super::types::{BufferValue, LispBignum, Value};
use super::{BlockKind, blocks_of, register_block, sweep_blocks_of, unregister_block};
use std::cell::Cell;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

/// alloc.c's `VECTOR_BLOCK_SIZE'.
pub(crate) const VECTOR_BLOCK_SIZE: usize = 4096;
/// alloc.c's `roundup_size': the common alignment of object fields and Lisp
/// words. Three tag bits and all allocated payloads require one word here.
const ROUNDUP_SIZE: usize = WORD_SIZE;

const fn vroundup(bytes: usize) -> usize {
    (bytes + ROUNDUP_SIZE - 1) & !(ROUNDUP_SIZE - 1)
}

/// Space available to vectors. The block's bitmap holds the marks of the
/// vectors in it; no Lisp payload contains this metadata.
const VECTOR_BLOCK_BYTES: usize =
    (VECTOR_BLOCK_SIZE - std::mem::size_of::<VectorBlockMarks>()) & !(ROUNDUP_SIZE - 1);
// Distinct objects are at least VBLOCK_BYTES_MIN bytes apart even when their
// starts have only word alignment. One bit per minimum footprint is sufficient;
// reducing allocation padding need not enlarge the block bitmap.
const VECTOR_MARK_WORDS: usize =
    (VECTOR_BLOCK_SIZE / VBLOCK_BYTES_MIN).div_ceil(usize::BITS as usize);
/// alloc.c's `large_vector_offset': the bytes of the link GNU keeps before
/// each large vector (`struct large_vector'), which lisp_malloc is asked
/// for with the vector's own and memory_full is told of.  The block
/// registry lists the large vectors here, so no link is allocated.
const LARGE_VECTOR_OFFSET: usize = WORD_SIZE;
const HEADER_SIZE: usize = std::mem::size_of::<VectorHeader>();
const WORD_SIZE: usize = std::mem::size_of::<usize>();
/// alloc.c's `VBLOCK_BYTES_MIN': a one-slot vector.
const VBLOCK_BYTES_MIN: usize = vroundup(HEADER_SIZE + std::mem::size_of::<Value>());
/// alloc.c's `VBLOCK_BYTES_MAX': the largest vector carved from a block.
const VBLOCK_BYTES_MAX: usize = vroundup(VECTOR_BLOCK_BYTES / 2 - WORD_SIZE);
/// alloc.c's `VECTOR_FREE_LIST_ARRAY_SIZE'.
const FREE_LIST_ARRAY_SIZE: usize = (VBLOCK_BYTES_MAX - VBLOCK_BYTES_MIN) / ROUNDUP_SIZE + 1 + 2;

/// alloc.c's `VINDEX'.
const fn vindex(nbytes: usize) -> usize {
    (nbytes - VBLOCK_BYTES_MIN) / ROUNDUP_SIZE
}

/// lisp.h's `ARRAY_MARK_FLAG' (PTRDIFF_MIN): the mark of a vector outside
/// the blocks, in its header's size word from the mark phase to the sweep.
const ARRAY_MARK_FLAG: usize = 1 << (usize::BITS - 1);
/// lisp.h's `PSEUDOVECTOR_FLAG' and the tag's place in the size word.
const PSEUDOVECTOR_FLAG: usize = 1 << (usize::BITS - 2);
const PSEUDOVECTOR_SIZE_BITS: u32 = 12;
const PSEUDOVECTOR_AREA_BITS: u32 = 24;
const PSEUDOVECTOR_SIZE_MASK: usize = (1 << PSEUDOVECTOR_SIZE_BITS) - 1;

/// lisp.h's `pvec_type', for the kinds here (C's numbers where C has
/// the kind; the two past `PVEC_TAG_MAX' are this implementation's).
#[repr(usize)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum VectorTag {
    Normal = 0,
    Free = 1,
    Bignum = 2,
    Marker = 3,
    Overlay = 4,
    Finalizer = 5,
    SymbolWithPos = 6,
    Frame = 10,
    Window = 11,
    BoolVector = 12,
    Buffer = 13,
    HashTable = 14,
    Terminal = 16,
    Subr = 18,
    NativeCompUnit = 26,
    Closure = 31,
    CharTable = 32,
    SubCharTable = 33,
    /// lisp.h's PVEC_RECORD: a record, and the pseudovector kinds this
    /// implementation keeps as records (the kind is in the state).
    Record = 34,
}

impl VectorTag {
    #[inline(always)]
    fn from_word(word: usize) -> Self {
        match word {
            1 => Self::Free,
            2 => Self::Bignum,
            3 => Self::Marker,
            4 => Self::Overlay,
            5 => Self::Finalizer,
            6 => Self::SymbolWithPos,
            10 => Self::Frame,
            11 => Self::Window,
            12 => Self::BoolVector,
            13 => Self::Buffer,
            14 => Self::HashTable,
            16 => Self::Terminal,
            18 => Self::Subr,
            26 => Self::NativeCompUnit,
            31 => Self::Closure,
            32 => Self::CharTable,
            33 => Self::SubCharTable,
            34 => Self::Record,
            _ => Self::Normal,
        }
    }
}

/// lisp.h's `union vectorlike_header': exactly one word before the payload.
#[repr(C)]
pub struct VectorHeader {
    /// An ordinary vector's slot count; a pseudovector's or a free
    /// vector's `PSEUDOVECTOR_FLAG', tag and word count.  A large vector's
    /// carries `ARRAY_MARK_FLAG' while it is marked.  A cell, because the
    /// mark phase sets the flag through a handle others share.
    size: Cell<usize>,
}

const _: () = {
    assert!(HEADER_SIZE == WORD_SIZE);
    assert!(std::mem::align_of::<Value>() <= ROUNDUP_SIZE);
    assert!(std::mem::align_of::<VectorHeader>() <= ROUNDUP_SIZE);
    // The mark is none of a size word's own bits: the flag below it is
    // PSEUDOVECTOR_FLAG, and an ordinary vector's length is a fixnum.
    assert!(ARRAY_MARK_FLAG > PSEUDOVECTOR_FLAG && PSEUDOVECTOR_FLAG > VECTOR_ELTS_MAX);
    assert!(std::mem::align_of::<VectorBlockMarks>() <= ROUNDUP_SIZE);
};

/// A vector block's mark bits (alloc.c's ARRAY_MARK_FLAG on the header),
/// one for each VBLOCK_BYTES_MIN of the block by a vector's start: set by
/// the mark phase, cleared by `sweep_vectors' once it has read them.
struct VectorBlockMarks {
    /// Registered while an image loaded, one of its rebuilt blocks (see
    /// `ImageCells'); a mapped block is not, as no sweep reads it.
    image: bool,
    /// A mapped block, whose vectors are marked in the dump's mark_bits
    /// (`DUMP'), never in this bitmap.
    dumped: bool,
    words: [Cell<usize>; VECTOR_MARK_WORDS],
}

impl VectorBlockMarks {
    const fn new(image: bool, dumped: bool) -> Self {
        Self {
            image,
            dumped,
            words: [const { Cell::new(0) }; VECTOR_MARK_WORDS],
        }
    }
}

enum VectorMarkStorage<'a> {
    Block(&'a VectorBlockMarks, usize),
    /// A large vector of the heap: `ARRAY_MARK_FLAG' in its header
    /// (alloc.c's XVECTOR_MARKED_P and XMARK_VECTOR).
    Header(&'a VectorHeader),
    /// A mapped vector, marked in the dump's mark_bits by its address.
    Dump(usize),
    /// The empty vector, which alloc.c makes pure (`make_pure_vector (0)'):
    /// mark_object leaves a pure object as it is, and survives_gc_p
    /// answers for one without a mark.
    Pure,
}

pub(crate) struct VectorMark<'a>(VectorMarkStorage<'a>);

impl VectorMark<'_> {
    /// alloc.c's `vector_marked_p'.
    #[inline(always)]
    pub(crate) fn is_marked(&self) -> bool {
        match self.0 {
            VectorMarkStorage::Block(block, index) => {
                block.words[index / usize::BITS as usize].get()
                    & (1 << (index % usize::BITS as usize))
                    != 0
            }
            VectorMarkStorage::Header(header) => header.size.get() & ARRAY_MARK_FLAG != 0,
            VectorMarkStorage::Dump(address) => super::DUMP.is_marked(address),
            VectorMarkStorage::Pure => true,
        }
    }

    /// alloc.c's `set_vector_marked' after its test of `vector_marked_p':
    /// true when the vector was not yet marked.
    #[inline(always)]
    pub(crate) fn mark(&self) -> bool {
        match self.0 {
            VectorMarkStorage::Block(block, index) => {
                let word = &block.words[index / usize::BITS as usize];
                let mask = 1 << (index % usize::BITS as usize);
                let old = word.get();
                word.set(old | mask);
                let marked = old & mask == 0;
                if marked && block.image {
                    super::DUMP.vectors.note_marked();
                }
                marked
            }
            VectorMarkStorage::Header(header) => {
                let size = header.size.get();
                if size & ARRAY_MARK_FLAG != 0 {
                    return false;
                }
                header.size.set(size | ARRAY_MARK_FLAG);
                true
            }
            VectorMarkStorage::Dump(address) => super::DUMP.mark(address),
            VectorMarkStorage::Pure => false,
        }
    }
}

/// The marks of the vector block at BLOCK.
///
/// # Safety
/// BLOCK is a registered vector block: its initialized bitmap occupies the
/// reserved tail of the block until the block is released.
unsafe fn block_marks<'a>(block: usize) -> &'a VectorBlockMarks {
    // SAFETY: the caller's contract.
    unsafe { &*((block + VECTOR_BLOCK_BYTES) as *const VectorBlockMarks) }
}

/// `super::every_mark_bit_clear' for the vector blocks and the large
/// vectors.
#[cfg(test)]
pub(super) fn every_mark_bit_clear() -> bool {
    blocks_of(BlockKind::VectorBlock).into_iter().all(|block| {
        // SAFETY: a registered vector block.
        unsafe { block_marks(block) }
            .words
            .iter()
            .all(|word| word.get() == 0)
    }) && blocks_of(BlockKind::LargeVector).into_iter().all(|start| {
        // SAFETY: a registered large vector's header.
        unsafe { (*(start as *const VectorHeader)).size.get() & ARRAY_MARK_FLAG == 0 }
    })
}

impl VectorHeader {
    /// The collector derives metadata from the allocated object's address
    /// and size. This method is never needed to read or mutate a Lisp slot.
    #[inline(always)]
    fn mark_bit(&self) -> VectorMark<'_> {
        debug_assert_ne!(self.tag(), VectorTag::Free);
        let address = std::ptr::from_ref(self) as usize;
        if self.size.get() == 0 {
            // The only zero-length vector is the permanent `zero_vector'.
            return VectorMark(VectorMarkStorage::Pure);
        }
        // The footprint is the same with the flag or without it, so a
        // large vector already marked is found as it was when unmarked.
        let nbytes = self.nbytes();
        if nbytes > VBLOCK_BYTES_MAX {
            if super::DUMP.contains(address) {
                return VectorMark(VectorMarkStorage::Dump(address));
            }
            return VectorMark(VectorMarkStorage::Header(self));
        }
        let base = address & !(VECTOR_BLOCK_SIZE - 1);
        let index = (address - base) / VBLOCK_BYTES_MIN;
        // SAFETY: small vector blocks are aligned to VECTOR_BLOCK_SIZE.
        let block = unsafe { block_marks(base) };
        if block.dumped {
            return VectorMark(VectorMarkStorage::Dump(address));
        }
        VectorMark(VectorMarkStorage::Block(block, index))
    }

    fn is_marked(&self) -> bool {
        self.tag() != VectorTag::Free && self.mark_bit().is_marked()
    }

    #[inline(always)]
    fn tag(&self) -> VectorTag {
        let size = self.size.get();
        if size & PSEUDOVECTOR_FLAG == 0 {
            VectorTag::Normal
        } else {
            VectorTag::from_word((size >> PSEUDOVECTOR_AREA_BITS) & 0x3f)
        }
    }

    fn is_pseudovector(&self) -> bool {
        self.size.get() & PSEUDOVECTOR_FLAG != 0
    }

    /// alloc.c's `vectorlike_nbytes': the footprint, of a marked vector
    /// too (`hdr->size & ~ARRAY_MARK_FLAG').
    fn nbytes(&self) -> usize {
        let size = self.size.get() & !ARRAY_MARK_FLAG;
        if self.is_pseudovector() {
            let tag = self.tag();
            if tag == VectorTag::BoolVector {
                // SAFETY: a bool vector's length word follows its header.
                let nbits = unsafe {
                    payload(std::ptr::from_ref(self).cast_mut())
                        .cast::<usize>()
                        .read()
                };
                return bool_vectors::bool_vector_nbytes(nbits);
            }
            let traced = size & PSEUDOVECTOR_SIZE_MASK;
            let rest = (size >> PSEUDOVECTOR_SIZE_BITS) & PSEUDOVECTOR_SIZE_MASK;
            let bytes = HEADER_SIZE + (traced + rest) * WORD_SIZE;
            if matches!(
                tag,
                VectorTag::CharTable
                    | VectorTag::SubCharTable
                    | VectorTag::SymbolWithPos
                    | VectorTag::Record
                    | VectorTag::HashTable
            ) {
                vroundup(bytes)
            } else {
                bytes
            }
        } else {
            vroundup(HEADER_SIZE + size * std::mem::size_of::<Value>())
        }
    }

    fn pseudovector_size_word(tag: VectorTag, nbytes: usize) -> usize {
        Self::pseudovector_slots_word(tag, nbytes, 0)
    }

    fn pseudovector_slots_word(tag: VectorTag, nbytes: usize, traced: usize) -> usize {
        let rest = (nbytes - HEADER_SIZE) / WORD_SIZE - traced;
        assert!(traced <= PSEUDOVECTOR_SIZE_MASK && rest <= PSEUDOVECTOR_SIZE_MASK);
        PSEUDOVECTOR_FLAG
            | ((tag as usize) << PSEUDOVECTOR_AREA_BITS)
            | (rest << PSEUDOVECTOR_SIZE_BITS)
            | traced
    }
}

/// The payload past the header.
///
/// # Safety
/// HEADER is a vector's header.
unsafe fn payload(header: *mut VectorHeader) -> *mut u8 {
    // SAFETY: the caller's contract.
    unsafe { header.cast::<u8>().add(HEADER_SIZE) }
}

/// alloc.c's `ADVANCE'.
///
/// # Safety
/// The result is a header address only while inside the same block.
unsafe fn advance(header: *mut VectorHeader, nbytes: usize) -> *mut VectorHeader {
    // SAFETY: pointer arithmetic within a block or one past it.
    unsafe { header.cast::<u8>().add(nbytes).cast() }
}

/// alloc.c's `VECTOR_IN_BLOCK'.
fn vector_in_block(header: *mut VectorHeader, block: usize) -> bool {
    header as usize <= block + VECTOR_BLOCK_BYTES - VBLOCK_BYTES_MIN
}

thread_local! {
    /// alloc.c's `vector_free_lists' and `last_inserted_vector_free_idx':
    /// the cells of the OS thread that holds the runtime (see
    /// `alloc::Frontier').
    static FREE_LISTS: [Cell<*mut VectorHeader>; FREE_LIST_ARRAY_SIZE] =
        const { [const { Cell::new(std::ptr::null_mut()) }; FREE_LIST_ARRAY_SIZE] };
    static LAST_INSERTED_FREE_INDEX: Cell<usize> = const { Cell::new(FREE_LIST_ARRAY_SIZE) };
    /// alloc.c's staticpro'd zero_vector, a permanent cell (0 until made),
    /// the runtime's: given and returned with the frontier.
    static ZERO_VECTOR: Cell<usize> = const { Cell::new(0) };
}

/// The vectors' share of `alloc::Frontier'.
pub(super) struct Frontier([usize; FREE_LIST_ARRAY_SIZE], usize, usize);

impl Frontier {
    pub(super) const EMPTY: Self = Self([0; FREE_LIST_ARRAY_SIZE], FREE_LIST_ARRAY_SIZE, 0);

    pub(super) fn take() -> Self {
        Self(
            FREE_LISTS.with(|lists| {
                lists
                    .each_ref()
                    .map(|list| list.replace(std::ptr::null_mut()) as usize)
            }),
            LAST_INSERTED_FREE_INDEX.replace(FREE_LIST_ARRAY_SIZE),
            ZERO_VECTOR.replace(0),
        )
    }

    /// The free lists' heads.
    #[cfg(test)]
    pub(super) fn cells(&self) -> &[usize] {
        &self.0
    }

    pub(super) fn install(self) {
        FREE_LISTS.with(|lists| {
            for (list, head) in lists.iter().zip(self.0) {
                list.set(head as *mut VectorHeader);
            }
        });
        LAST_INSERTED_FREE_INDEX.set(self.1);
        ZERO_VECTOR.set(self.2);
    }
}

/// gcstat's `total_vectors' and `total_vector_slots' by kind, and the
/// string objects' share of `total_strings' and `total_string_bytes':
/// raised by allocation, set by the sweep.
static LIVE_VECTORS: AtomicUsize = AtomicUsize::new(0);
static LIVE_VECTOR_SLOTS: AtomicUsize = AtomicUsize::new(0);
static LIVE_CLOSURES: AtomicUsize = AtomicUsize::new(0);
/// The vector sweeps so far, each counted before it frees anything (see
/// `verify::entry_program').
static VECTOR_SWEEPS: AtomicUsize = AtomicUsize::new(0);

/// The count of `VECTOR_SWEEPS'.
#[inline(always)]
pub(crate) fn vector_sweeps() -> usize {
    VECTOR_SWEEPS.load(Ordering::Relaxed)
}
static LIVE_CLOSURE_SLOTS: AtomicUsize = AtomicUsize::new(0);
static LIVE_BIGNUMS: AtomicUsize = AtomicUsize::new(0);
static LIVE_OVERLAYS: AtomicUsize = AtomicUsize::new(0);
/// The records' share of `total_vectors' and `total_vector_slots'
/// (alloc.c counts a record as a vector of its slots).
static LIVE_RECORDS: AtomicUsize = AtomicUsize::new(0);
static LIVE_RECORD_SLOTS: AtomicUsize = AtomicUsize::new(0);

/// The records the sweep freed, as (owner, id): the interpreter whose
/// id space the record was in purges the side tables it keeps by that
/// id after its sweep (`Interpreter::drain_freed_records').  A record is
/// an object of the process, an id a name in one interpreter's tables.
static FREED_RECORDS: std::sync::Mutex<Vec<FreedRecord>> = std::sync::Mutex::new(Vec::new());

/// alloc.c's `all_buffers': the header of every buffer object, live or
/// dead, from its allocation until its storage is reclaimed, for the
/// sweep's marker chains (which would otherwise read every vector header
/// to find the buffers).  A set: the sweep reclaims each dead buffer by
/// its address, and a collection that frees thousands of temporary
/// buffers must not search a list for each.
type BufferHeaders = std::collections::HashSet<usize, crate::lisp::types::IdentityBuildHasher>;

static ALL_BUFFERS: std::sync::Mutex<BufferHeaders> = std::sync::Mutex::new(
    std::collections::HashSet::with_hasher(std::hash::BuildHasherDefault::new()),
);

fn all_buffers() -> std::sync::MutexGuard<'static, BufferHeaders> {
    ALL_BUFFERS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A record the sweep freed: whose id space, which id, and the symbol
/// its type tag named (the type index's key), read before the drop.
pub(crate) struct FreedRecord {
    pub(crate) owner: u32,
    pub(crate) id: u64,
    /// The freed cell's address (its handle's `identity'): a registry
    /// entry under the same id that names another cell (an image load
    /// replaced the record) is left alone.
    pub(crate) identity: usize,
    pub(crate) type_name: Option<Box<str>>,
}

fn note_freed_record(freed: FreedRecord) {
    FREED_RECORDS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(freed);
}

/// The records the sweeps freed since the last call.
pub(crate) fn take_freed_records() -> Vec<FreedRecord> {
    std::mem::take(
        &mut *FREED_RECORDS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
    )
}

fn raise(counter: &AtomicUsize, by: usize) {
    counter.store(counter.load(Ordering::Relaxed) + by, Ordering::Relaxed);
}

/// The free-list link of a free vector: its first payload word
/// (alloc.c's `next_vector').
///
/// # Safety
/// HEADER is a free vector.
unsafe fn next_free(header: *mut VectorHeader) -> *mut VectorHeader {
    // SAFETY: the caller's contract; the payload is at least a word.
    unsafe { payload(header).cast::<*mut VectorHeader>().read() }
}

/// alloc.c's `setup_on_free_list'.
///
/// # Safety
/// HEADER heads NBYTES of a vector block that nothing reaches.
unsafe fn setup_on_free_list(header: *mut VectorHeader, nbytes: usize) {
    debug_assert!(nbytes >= VBLOCK_BYTES_MIN && nbytes.is_multiple_of(ROUNDUP_SIZE));
    let index = vindex(nbytes).min(FREE_LIST_ARRAY_SIZE - 1);
    // SAFETY: the caller's contract; the header and the first payload
    // word are inside the block.
    unsafe {
        (*header).size = Cell::new(VectorHeader::pseudovector_size_word(
            VectorTag::Free,
            nbytes,
        ));
        payload(header)
            .cast::<*mut VectorHeader>()
            .write(FREE_LISTS.with(|lists| lists[index].get()));
    }
    FREE_LISTS.with(|lists| lists[index].set(header));
    LAST_INSERTED_FREE_INDEX.set(index);
}

/// A free run in an image block: tagged free, so later sweeps neither clean
/// nor count it, but on no free list, so it is never reused.
///
/// # Safety
/// As `setup_on_free_list'.
unsafe fn setup_free_unlinked(header: *mut VectorHeader, nbytes: usize) {
    debug_assert!(nbytes >= VBLOCK_BYTES_MIN && nbytes.is_multiple_of(ROUNDUP_SIZE));
    // SAFETY: the caller's contract.
    unsafe {
        (*header).size = Cell::new(VectorHeader::pseudovector_size_word(
            VectorTag::Free,
            nbytes,
        ))
    };
}

/// The next vector takes a new block (see `ImageRegion'). Free runs left
/// behind return to the free lists at the next sweep.
pub(super) fn abandon_allocation_frontier() {
    clear_free_lists();
}

/// Empty the vector free lists.
fn clear_free_lists() {
    FREE_LISTS.with(|lists| {
        for list in lists {
            list.set(std::ptr::null_mut());
        }
    });
    LAST_INSERTED_FREE_INDEX.set(FREE_LIST_ARRAY_SIZE);
}

/// alloc.c's `allocate_vector_block'.
fn allocate_vector_block() -> usize {
    // A thread without the runtime has empty free lists, so it reaches
    // here at its first allocation (see `alloc::Frontier').
    crate::lisp::runtime::ensure_held();
    let block = super::arena::allocate_block(VECTOR_BLOCK_SIZE, VECTOR_BLOCK_SIZE);
    assert!(!block.is_null(), "out of memory for a vector block");
    let start = block as usize;
    // SAFETY: metadata is outside the region carved into Lisp vectors and
    // is initialized before the block becomes visible to the collector.
    unsafe {
        block
            .add(VECTOR_BLOCK_BYTES)
            .cast::<VectorBlockMarks>()
            .write(VectorBlockMarks::new(super::image_region_open(), false))
    };
    register_block(start, VECTOR_BLOCK_SIZE, BlockKind::VectorBlock);
    start
}

fn release_vector_block(start: usize) {
    unregister_block(start);
    // SAFETY: a block `allocate_vector_block' made, with nothing live.
    unsafe { super::arena::release_block(start as *mut u8, VECTOR_BLOCK_SIZE, VECTOR_BLOCK_SIZE) };
}

/// alloc.c's `allocate_vector_from_block'.
fn allocate_vector_from_block(nbytes: usize) -> *mut VectorHeader {
    debug_assert!((VBLOCK_BYTES_MIN..=VBLOCK_BYTES_MAX).contains(&nbytes));
    debug_assert!(nbytes.is_multiple_of(ROUNDUP_SIZE));
    // First, the free list of exactly this size.
    let index = vindex(nbytes);
    let head = FREE_LISTS.with(|lists| lists[index].get());
    if !head.is_null() {
        // SAFETY: a free vector's link.
        unsafe {
            debug_assert!(
                (*head).tag() == VectorTag::Free && (*head).nbytes() == nbytes,
                "free list {index} held {:#x} with size word {:#x}",
                head as usize,
                (*head).size.get()
            );
            FREE_LISTS.with(|lists| lists[index].set(next_free(head)));
        }
        return head;
    }
    // Next, a larger free vector, split so that the rest is at least a
    // one-slot vector.
    let first = vindex(nbytes + VBLOCK_BYTES_MIN).max(LAST_INSERTED_FREE_INDEX.get());
    for index in first.min(FREE_LIST_ARRAY_SIZE)..FREE_LIST_ARRAY_SIZE {
        let head = FREE_LISTS.with(|lists| lists[index].get());
        if head.is_null() {
            continue;
        }
        // SAFETY: a free vector: its header and link are readable.
        let vector_nbytes = unsafe { (*head).nbytes() };
        debug_assert!(vector_nbytes > nbytes);
        FREE_LISTS.with(|lists| lists[index].set(unsafe { next_free(head) }));
        let rest = vector_nbytes - nbytes;
        debug_assert!(rest.is_multiple_of(ROUNDUP_SIZE));
        // SAFETY: the excess is inside the same free vector.
        unsafe { setup_on_free_list(advance(head, nbytes), rest) };
        return head;
    }
    // Finally, a new block: the vector at its start, the rest (a one-slot
    // vector at least) on a free list.
    let block = allocate_vector_block();
    let vector = block as *mut VectorHeader;
    let rest = VECTOR_BLOCK_BYTES - nbytes;
    if rest >= VBLOCK_BYTES_MIN {
        // SAFETY: inside the block just allocated.
        unsafe { setup_on_free_list(advance(vector, nbytes), rest) };
    }
    vector
}

/// alloc.c's lisp_malloc failing: the bytes it was asked for, which
/// memory_full receives (`SIZE_MAX' for a request no size can express).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AllocationFailed(pub(crate) usize);

/// alloc.c's `VECTOR_ELTS_MAX': the most slots a vector can have, its
/// bytes (the header and the large-vector link before them) within
/// PTRDIFF_MAX and its length a fixnum.
pub(crate) const VECTOR_ELTS_MAX: usize = {
    let words = (isize::MAX as usize - HEADER_SIZE - WORD_SIZE) / WORD_SIZE;
    let fixnum = ((1_i64 << 61) - 1) as usize;
    if words < fixnum { words } else { fixnum }
};

/// alloc.c's `allocate_vectorlike': NBYTES of storage (the header
/// included) from a block, or on its own past `VBLOCK_BYTES_MAX'.  The
/// header is the caller's to write.
fn allocate_vectorlike(nbytes: usize) -> *mut VectorHeader {
    try_allocate_vectorlike(nbytes).expect("out of memory for a vector")
}

/// alloc.c:allocate_vectorlike, which on a failed lisp_malloc reaches
/// memory_full before anything is counted.
fn try_allocate_vectorlike(nbytes: usize) -> Result<*mut VectorHeader, AllocationFailed> {
    let asked = nbytes;
    let nbytes = vroundup(nbytes).max(VBLOCK_BYTES_MIN);
    let vector = if nbytes <= VBLOCK_BYTES_MAX {
        allocate_vector_from_block(nbytes)
    } else {
        // What alloc.c's lisp_malloc is asked for, the link included.
        let failed = AllocationFailed(nbytes.saturating_add(LARGE_VECTOR_OFFSET));
        let layout =
            std::alloc::Layout::from_size_align(nbytes, ROUNDUP_SIZE).map_err(|_| failed)?;
        // A large vector is the heap's from here: only the OS thread that
        // holds the runtime makes one.
        crate::lisp::runtime::ensure_held();
        // SAFETY: a non-zero layout.
        let vector = unsafe { std::alloc::alloc(layout) };
        if vector.is_null() {
            return Err(failed);
        }
        // The header the caller writes is all there is to write: a vector
        // is made unmarked, its size word without ARRAY_MARK_FLAG.
        register_block(vector as usize, nbytes, BlockKind::LargeVector);
        vector.cast()
    };
    // alloc.c:allocate_vectorlike: `vector_cells_consed += len', the words
    // after the header that were asked for, before block rounding.
    super::counters::tally(
        &super::counters::VECTOR_CELLS_CONSED,
        (asked.saturating_sub(HEADER_SIZE) / WORD_SIZE) as i64,
    );
    Ok(vector)
}

fn free_large_vector(header: *mut VectorHeader, nbytes: usize) {
    unregister_block(header as usize);
    let layout =
        std::alloc::Layout::from_size_align(nbytes, ROUNDUP_SIZE).expect("large vector layout");
    // SAFETY: `allocate_vectorlike' made it with this layout; nothing
    // reaches it.
    unsafe { std::alloc::dealloc(header.cast(), layout) };
}

/// The image's vectors -- the ordinary vectors but the empty one, the
/// closures, the records and the char tables and their subtables -- are
/// this allocator's vector blocks and large vectors, written whole by the
/// dumper and used where the loader maps them, as pdumper.c's dump holds
/// its objects in their runtime form (see `adopt_image_cons_blocks'): a
/// block of `VECTOR_BLOCK_SIZE' holds vectors from its start, each in the
/// first block with room for it as they are dumped, its usable bytes past
/// the last one a free vector (when they are at least one), and its marks
/// after `VECTOR_BLOCK_BYTES'; each large
/// vector, past `VBLOCK_BYTES_MAX', follows the blocks on its own, the
/// next one after its last word.
pub(crate) const IMAGE_VECTOR_BLOCK_BYTES: usize = VECTOR_BLOCK_SIZE;

/// The dumper's image vectors: each vector's words, in its runtime form,
/// where the image will hold it.
#[derive(Default)]
pub(crate) struct ImageVectorWriter {
    /// The blocks' words, whole blocks.
    blocks: Vec<u64>,
    /// The bytes each block's vectors take from its start.
    used: Vec<usize>,
    /// The blocks with room for another vector, in order.
    open: Vec<usize>,
    /// The large vectors' words, one vector after another.
    large: Vec<u64>,
    large_count: usize,
}

/// Where an image vector goes: its offset from the first block, or from
/// the first large vector.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ImageVectorPlace {
    Block(usize),
    Large(usize),
}

impl ImageVectorWriter {
    /// Where the next vector, of NBYTES, goes: after the last vector of the
    /// first block it fits in below `VECTOR_BLOCK_BYTES', else at the start
    /// of a new block; a large one after the last large one.
    pub(crate) fn next_place(&self, nbytes: usize) -> ImageVectorPlace {
        if nbytes > VBLOCK_BYTES_MAX {
            return ImageVectorPlace::Large(self.large.len() * WORD_SIZE);
        }
        let block = self
            .open
            .iter()
            .copied()
            .find(|&block| self.used[block] + nbytes <= VECTOR_BLOCK_BYTES)
            .unwrap_or(self.used.len());
        ImageVectorPlace::Block(
            block * VECTOR_BLOCK_SIZE + self.used.get(block).copied().unwrap_or(0),
        )
    }

    /// Write WORDS, a vector's header and payload as the heap holds them,
    /// where `next_place' puts it.
    pub(crate) fn push(&mut self, words: &[u64]) -> ImageVectorPlace {
        let nbytes = words.len() * WORD_SIZE;
        debug_assert!(nbytes >= VBLOCK_BYTES_MIN);
        let place = self.next_place(nbytes);
        match place {
            ImageVectorPlace::Large(_) => {
                self.large.extend_from_slice(words);
                self.large_count += 1;
            }
            ImageVectorPlace::Block(at) => {
                let block = at / VECTOR_BLOCK_SIZE;
                if block == self.used.len() {
                    self.blocks
                        .resize(self.blocks.len() + VECTOR_BLOCK_SIZE / WORD_SIZE, 0);
                    self.used.push(0);
                    self.open.push(block);
                }
                self.blocks[at / WORD_SIZE..at / WORD_SIZE + words.len()].copy_from_slice(words);
                self.used[block] += nbytes;
                if self.used[block] + VBLOCK_BYTES_MIN > VECTOR_BLOCK_BYTES {
                    self.open.retain(|&open| open != block);
                }
            }
        }
        place
    }

    /// The blocks' words, each block's usable bytes past its last vector a
    /// free vector when they are at least one, its marks zero, and their
    /// count; the large vectors' words, and their count.
    pub(crate) fn finish(mut self) -> (Vec<u64>, usize, Vec<u64>, usize) {
        for (block, &used) in self.used.iter().enumerate() {
            let rest = VECTOR_BLOCK_BYTES - used;
            if rest >= VBLOCK_BYTES_MIN {
                self.blocks[(block * VECTOR_BLOCK_SIZE + used) / WORD_SIZE] =
                    VectorHeader::pseudovector_size_word(VectorTag::Free, rest) as u64;
            }
        }
        (self.blocks, self.used.len(), self.large, self.large_count)
    }
}

/// The size word of the vector whose header is at IDENTITY, and its
/// footprint (alloc.c's `vector_nbytes'), for the dumper, which writes the
/// image's vectors as the heap holds them.
///
/// # Safety
/// IDENTITY is an allocated vector's header.
pub(crate) unsafe fn header_and_footprint(identity: usize) -> (usize, usize) {
    let header = identity as *const VectorHeader;
    // SAFETY: the caller's contract.
    unsafe { ((*header).size.get(), (*header).nbytes()) }
}

/// One of the image's vectors as its adoption found it: the offsets, from
/// the first block, of its header, of its first Lisp word and past its
/// last, and its kind.
pub(crate) struct ImageVector {
    pub(crate) start: usize,
    pub(crate) lisp: usize,
    pub(crate) end: usize,
    pub(crate) tag: VectorTag,
}

/// The kind, footprint and offset of the first Lisp word of the image
/// vector at HEADER, which has ROOM bytes from its header on: one of the
/// kinds the image holds as vectors (`IMAGE_VECTOR_BLOCK_BYTES') with the
/// size word the allocator gives it, or a free vector.
///
/// # Safety
/// HEADER is aligned, and the ROOM bytes from it are readable.
unsafe fn image_vector_shape(
    header: *const VectorHeader,
    room: usize,
) -> Result<(VectorTag, usize, usize), String> {
    use char_tables::{CHAR_TABLE_MAX_CHAR, CHAR_TABLE_MAX_SLOTS, CHAR_TABLE_STANDARD_SLOTS};
    const CANONICAL: usize = PSEUDOVECTOR_FLAG | (0x3f << PSEUDOVECTOR_AREA_BITS) | 0xff_ffff;
    let invalid = || {
        Err(format!(
            "invalid image vector header at {:#x}",
            header as usize
        ))
    };
    if room < VBLOCK_BYTES_MIN {
        return invalid();
    }
    // SAFETY: the caller's contract.
    let size = unsafe { (*header).size.get() };
    if size & PSEUDOVECTOR_FLAG == 0 {
        if size == 0 || size > (room - HEADER_SIZE) / WORD_SIZE {
            return invalid();
        }
        return Ok((
            VectorTag::Normal,
            HEADER_SIZE + size * WORD_SIZE,
            HEADER_SIZE,
        ));
    }
    let tag = match (size >> PSEUDOVECTOR_AREA_BITS) & 0x3f {
        _ if size & !CANONICAL != 0 => return invalid(),
        1 => VectorTag::Free,
        12 => VectorTag::BoolVector,
        31 => VectorTag::Closure,
        32 => VectorTag::CharTable,
        33 => VectorTag::SubCharTable,
        34 => VectorTag::Record,
        _ => return invalid(),
    };
    let traced = size & PSEUDOVECTOR_SIZE_MASK;
    let rest = (size >> PSEUDOVECTOR_SIZE_BITS) & PSEUDOVECTOR_SIZE_MASK;
    // SAFETY: a pseudovector header, whose footprint it alone gives.
    let nbytes = unsafe { (*header).nbytes() };
    if nbytes > room || nbytes < VBLOCK_BYTES_MIN {
        return invalid();
    }
    let valid = match tag {
        // A bool vector's length gives its footprint; it has no Lisp words.
        VectorTag::BoolVector => traced == 0 && rest == 0,
        VectorTag::Free => traced == 0,
        VectorTag::Closure => rest == 0 && traced >= 3,
        VectorTag::Record => rest == 0 && traced >= 1,
        VectorTag::CharTable => {
            rest == 0 && (CHAR_TABLE_STANDARD_SLOTS..=CHAR_TABLE_MAX_SLOTS).contains(&traced)
        }
        _ => {
            // The packed word: the depth and the first character.
            // SAFETY: the footprint's first payload word, in ROOM.
            let (depth, min_char) = unsafe {
                let body = payload(header.cast_mut());
                (body.cast::<i32>().read(), body.add(4).cast::<i32>().read())
            };
            rest == 0
                && (1..=3).contains(&depth)
                && traced == 1 + char_tables::SUB_CHAR_TABLE_SIZES[depth as usize]
                && (0..=CHAR_TABLE_MAX_CHAR as i32).contains(&min_char)
        }
    };
    if !valid {
        return invalid();
    }
    let lisp = if tag == VectorTag::BoolVector {
        nbytes
    } else {
        HEADER_SIZE + usize::from(tag == VectorTag::SubCharTable) * WORD_SIZE
    };
    Ok((tag, nbytes, lisp))
}

/// pdumper's mapped vectors: the BLOCKS vector blocks at START and the
/// LARGE large vectors after them become the allocator's, image blocks
/// where the image holds them, as the dump's objects are the heap's once
/// it is mapped (nothing is allocated or counted for them, and the
/// collector never frees their storage).  Each vector is checked to be one
/// of the image's kinds with the size word the allocator gives it, the
/// blocks to be tiled by them, and every vector to lie in the LIMIT bytes
/// from START; VECTOR is told each one, in order.  The vectors keep the
/// words the image gave them, for the loader to relocate in place.  An
/// image that fails a check is refused, and no block is adopted.  The
/// result is the bytes from START past the last large vector.
///
/// # Safety
/// START is aligned to `VECTOR_BLOCK_SIZE' and the LIMIT bytes from it are
/// writable for the life of the process, and no other block overlaps them.
pub(crate) unsafe fn adopt_image_vectors(
    start: *mut u8,
    blocks: usize,
    large: usize,
    limit: usize,
    mut vector: impl FnMut(ImageVector),
) -> Result<usize, String> {
    let start = start as usize;
    assert!(
        start.is_multiple_of(VECTOR_BLOCK_SIZE),
        "unaligned image vectors"
    );
    let small = blocks
        .checked_mul(VECTOR_BLOCK_SIZE)
        .filter(|&bytes| bytes <= limit)
        .ok_or("the image's vector blocks are outside the image")?;
    for block in 0..blocks {
        let base = start + block * VECTOR_BLOCK_SIZE;
        let mut at = 0;
        while at <= VECTOR_BLOCK_BYTES - VBLOCK_BYTES_MIN {
            // SAFETY: an aligned word of the block, whose usable bytes
            // past it are the room.
            let (tag, nbytes, lisp) = unsafe {
                image_vector_shape((base + at) as *const VectorHeader, VECTOR_BLOCK_BYTES - at)?
            };
            if tag == VectorTag::Free {
                if at + nbytes != VECTOR_BLOCK_BYTES {
                    return Err(format!("a free image vector at {base:#x} ends early"));
                }
                break;
            }
            if nbytes > VBLOCK_BYTES_MAX {
                return Err(format!("a large image vector at {base:#x} is in a block"));
            }
            let first = block * VECTOR_BLOCK_SIZE + at;
            vector(ImageVector {
                start: first,
                lisp: first + lisp,
                end: first + nbytes,
                tag,
            });
            at += nbytes;
        }
        // SAFETY: the block's marks, past the bytes it holds vectors in.
        unsafe {
            (base as *mut u8)
                .add(VECTOR_BLOCK_BYTES)
                .cast::<VectorBlockMarks>()
                .write(VectorBlockMarks::new(false, true))
        };
    }
    let mut at = small;
    let mut large_vectors = Vec::with_capacity(large);
    for _ in 0..large {
        // SAFETY: an aligned word inside the LIMIT bytes, the rest of them
        // the room.
        let (tag, nbytes, lisp) =
            unsafe { image_vector_shape((start + at) as *const VectorHeader, limit - at)? };
        if tag == VectorTag::Free || nbytes <= VBLOCK_BYTES_MAX {
            return Err(format!("invalid large image vector at {:#x}", start + at));
        }
        // Nothing is written for a mapped vector: its mark is the dump's.
        vector(ImageVector {
            start: at,
            lisp: at + lisp,
            end: at + nbytes,
            tag,
        });
        large_vectors.push((start + at, nbytes));
        at += nbytes;
    }
    let block_list = (0..blocks)
        .map(|block| (start + block * VECTOR_BLOCK_SIZE, VECTOR_BLOCK_SIZE))
        .collect::<Vec<_>>();
    super::register_image_blocks(BlockKind::VectorBlock, &block_list);
    super::register_image_blocks(BlockKind::LargeVector, &large_vectors);
    // New tables, which no cached pattern has seen; and the image's
    // vectors changed, with or without an image region open: the next
    // sweep reads them all.
    char_tables::note_char_table_store();
    super::forget_image_live();
    Ok(at)
}

/// ADDRESS as the one zero-length vector (alloc.c's `zero_vector'), a
/// header outside every block: every empty allocation is it, so it is
/// never swept, and never marked (`VectorMarkStorage::Pure').
pub(super) fn zero_vector_at(address: usize) -> Option<*mut VectorHeader> {
    (address != 0 && address == ZERO_VECTOR.get()).then_some(address as *mut VectorHeader)
}

/// alloc.c's staticpro'd zero_vector.
#[inline]
fn zero_vector() -> *mut VectorHeader {
    match ZERO_VECTOR.get() {
        0 => make_zero_vector(),
        zero => zero as *mut VectorHeader,
    }
}

/// init_vectors' zero_vector, made once for the runtime.  A thread without
/// the runtime has none (see `alloc::Frontier'): it takes the runtime, and
/// the runtime's empty vector, first.
#[cold]
#[inline(never)]
fn make_zero_vector() -> *mut VectorHeader {
    if crate::lisp::runtime::took_runtime() && ZERO_VECTOR.get() != 0 {
        return ZERO_VECTOR.get() as *mut VectorHeader;
    }
    let zero =
        Box::leak(Box::new(VectorHeader { size: Cell::new(0) })) as *mut VectorHeader as usize;
    ZERO_VECTOR.set(zero);
    zero as *mut VectorHeader
}

/// `Lisp_Object' for an ordinary vector: the header's address.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct VectorRef(NonNull<VectorHeader>);

impl VectorRef {
    /// alloc.c's `make_vector' over SLOTS, moved in place.
    pub(crate) fn allocate(mut slots: Vec<Value>) -> Self {
        let len = slots.len();
        if len == 0 {
            // SAFETY: the leaked header.
            return Self(unsafe { NonNull::new_unchecked(zero_vector()) });
        }
        let header = allocate_vectorlike(HEADER_SIZE + len * std::mem::size_of::<Value>());
        // SAFETY: fresh storage of the size asked for; the slots are
        // moved out of the vector, which is emptied without dropping.
        unsafe {
            (*header).size = Cell::new(len);
            std::ptr::copy_nonoverlapping(slots.as_ptr(), payload(header).cast::<Value>(), len);
            slots.set_len(0);
        }
        raise(&LIVE_VECTORS, 1);
        raise(&LIVE_VECTOR_SLOTS, len + 1);
        // SAFETY: a fresh, non-null header.
        Self(unsafe { NonNull::new_unchecked(header) })
    }

    /// alloc.c's `make_vector' of LEN slots, each VALUE, written in
    /// place (no vector of the slots to move from).
    pub(crate) fn filled(len: usize, value: Value) -> Self {
        Self::try_filled(len, value).expect("out of memory for a vector")
    }

    /// alloc.c:allocate_clear_vector's `make_vector': LEN slots each
    /// VALUE, or what memory_full is to be given -- SIZE_MAX for more
    /// than VECTOR_ELTS_MAX slots, else the bytes lisp_malloc could not
    /// find.
    pub(crate) fn try_filled(len: usize, value: Value) -> Result<Self, AllocationFailed> {
        if len == 0 {
            // SAFETY: the leaked header.
            return Ok(Self(unsafe { NonNull::new_unchecked(zero_vector()) }));
        }
        if len > VECTOR_ELTS_MAX {
            return Err(AllocationFailed(usize::MAX));
        }
        let header = try_allocate_vectorlike(HEADER_SIZE + len * std::mem::size_of::<Value>())?;
        // SAFETY: fresh storage of the size asked for, each slot written
        // before the header is published.
        unsafe {
            (*header).size = Cell::new(len);
            let slots = payload(header).cast::<Value>();
            for index in 0..len {
                slots.add(index).write(value);
            }
        }
        raise(&LIVE_VECTORS, 1);
        raise(&LIVE_VECTOR_SLOTS, len + 1);
        // SAFETY: a fresh, non-null header.
        Ok(Self(unsafe { NonNull::new_unchecked(header) }))
    }

    pub(crate) fn shallow_copy(&self) -> Self {
        let len = self.len();
        if len == 0 {
            return *self;
        }
        let nbytes = HEADER_SIZE + len * WORD_SIZE;
        crate::lisp::native_comp::note_lisp_allocation(nbytes);
        let header = allocate_vectorlike(nbytes);
        // SAFETY: fresh word-aligned storage for this header and LEN cells.
        // Read each source Cell without exposing a shared Value reference.
        unsafe {
            (*header).size = Cell::new(len);
            let target = payload(header).cast::<Cell<Value>>();
            for (index, field) in self.cells().iter().enumerate() {
                target.add(index).write(Cell::new(field.get()));
            }
            raise(&LIVE_VECTORS, 1);
            raise(&LIVE_VECTOR_SLOTS, len + 1);
            Self(NonNull::new_unchecked(header))
        }
    }

    /// # Safety
    /// HEADER is an allocated ordinary vector's header.
    pub(crate) unsafe fn from_raw(header: *mut VectorHeader) -> Self {
        // SAFETY: the caller's contract.
        Self(unsafe { NonNull::new_unchecked(header) })
    }

    #[inline]
    fn header(&self) -> &VectorHeader {
        // SAFETY: the collector keeps the header while a handle can be
        // read.
        unsafe { self.0.as_ref() }
    }

    /// lisp.h's `ASIZE', with its `eassume (0 <= size)': no vector is
    /// marked while Lisp runs.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        let size = self.header().size.get();
        debug_assert!(
            size & ARRAY_MARK_FLAG == 0,
            "the length of a marked vector read outside the collector"
        );
        size
    }

    /// lisp.h's `gc_asize': "like ASIZE, but also can be used in the
    /// garbage collector", where a large vector's size word carries its
    /// mark until the sweep.
    #[inline]
    fn gc_len(&self) -> usize {
        self.header().size.get() & !ARRAY_MARK_FLAG
    }

    /// The inline Lisp words. `Cell` permits a callback or generated code
    /// to update a word while another caller retains the vector. Neither
    /// reads nor writes expose references into mutable Lisp storage.
    #[inline]
    fn cells(&self) -> &[Cell<Value>] {
        // SAFETY: LEN slots follow the header while the vector lives.
        // Cell<Value> has the same representation as its initialized Value.
        unsafe {
            std::slice::from_raw_parts(payload(self.0.as_ptr()).cast::<Cell<Value>>(), self.len())
        }
    }

    /// The slots for the collector's trace, which reads them once the
    /// vector is marked (alloc.c's mark_object takes the size first and
    /// pushes `ptr->contents' with it).
    #[inline]
    pub(crate) fn gc_slots(
        &self,
    ) -> impl DoubleEndedIterator<Item = Value> + ExactSizeIterator + '_ {
        // SAFETY: as `cells', the length without the mark.
        unsafe {
            std::slice::from_raw_parts(
                payload(self.0.as_ptr()).cast::<Cell<Value>>(),
                self.gc_len(),
            )
        }
        .iter()
        .map(Cell::get)
    }

    #[inline]
    pub(crate) fn get(&self, index: usize) -> Option<Value> {
        self.cells().get(index).map(Cell::get)
    }

    /// `AREF` without the bounds check.
    ///
    /// # Safety
    /// INDEX is below `len()`.
    #[inline(always)]
    pub(crate) unsafe fn get_unchecked(&self, index: usize) -> Value {
        // SAFETY: the caller keeps INDEX within the inline slots.
        unsafe { self.cells().get_unchecked(index).get() }
    }

    /// `ASET`: one store into the authoritative slot. The caller checks
    /// the Lisp index and reports the primitive's GNU error before this.
    #[inline]
    pub(crate) fn set(&self, index: usize, value: Value) {
        self.cells()[index].set(value);
    }

    pub(crate) fn slots(&self) -> impl DoubleEndedIterator<Item = Value> + ExactSizeIterator + '_ {
        self.cells().iter().map(Cell::get)
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
    pub(crate) fn mark_bit(&self) -> VectorMark<'_> {
        self.header().mark_bit()
    }
}

impl std::fmt::Debug for VectorRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.slots()).finish()
    }
}

/// GNU PVEC_CLOSURE: its header followed by the authoritative Lisp slots.
/// Interpreted and byte-code functions use this same allocation.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct ClosureRef(NonNull<VectorHeader>);

impl ClosureRef {
    pub(crate) fn allocate(slots: &[Value]) -> Self {
        Self::from_fields(slots.len(), |index| slots[index])
    }

    pub(crate) fn filled(len: usize, value: Value) -> Self {
        Self::from_fields(len, |_| value)
    }

    pub(crate) fn shallow_copy(&self) -> Self {
        Self::from_fields(self.public_len(), |index| self.cells()[index].get())
    }

    fn from_fields(len: usize, mut field: impl FnMut(usize) -> Value) -> Self {
        assert!(len <= PSEUDOVECTOR_SIZE_MASK);
        let nbytes = vroundup(HEADER_SIZE + len * WORD_SIZE);
        // GNU allocate_vectorlike charges the header and actual Lisp words.
        // Word alignment adds no payload padding; block metadata is separate.
        crate::lisp::native_comp::note_lisp_allocation(nbytes);
        let header = allocate_vectorlike(nbytes);
        // SAFETY: freshly allocated NBYTES, aligned for the header and slots.
        // Every traced slot is initialized before the object is published.
        unsafe {
            (*header).size = Cell::new(VectorHeader::pseudovector_slots_word(
                VectorTag::Closure,
                nbytes,
                len,
            ));
            let target = payload(header).cast::<Cell<Value>>();
            for index in 0..len {
                target.add(index).write(Cell::new(field(index)));
            }
            census_on_allocate(header);
            Self(NonNull::new_unchecked(header))
        }
    }

    /// # Safety
    /// HEADER names a live PVEC_CLOSURE allocated by this allocator.
    pub(crate) unsafe fn from_raw(header: *mut VectorHeader) -> Self {
        Self(unsafe { NonNull::new_unchecked(header) })
    }

    #[inline]
    fn header(&self) -> &VectorHeader {
        // SAFETY: a reachable closure keeps its allocation live.
        unsafe { self.0.as_ref() }
    }

    #[inline]
    pub(crate) fn public_len(&self) -> usize {
        self.header().size.get() & PSEUDOVECTOR_SIZE_MASK
    }

    #[inline]
    fn cells(&self) -> &[Cell<Value>] {
        // SAFETY: the header records the initialized inline slot count.
        // Cell permits loading/storing words without aliasing mutable refs.
        unsafe {
            std::slice::from_raw_parts(
                payload(self.0.as_ptr()).cast::<Cell<Value>>(),
                self.public_len(),
            )
        }
    }

    #[inline]
    pub(crate) fn get(&self, index: usize) -> Option<Value> {
        self.cells().get(index).map(Cell::get)
    }

    pub(crate) fn slots(&self) -> impl DoubleEndedIterator<Item = Value> + ExactSizeIterator + '_ {
        self.cells().iter().map(Cell::get)
    }

    /// `AREF` without the bounds check.
    ///
    /// # Safety
    /// INDEX is below `public_len()`.
    #[inline(always)]
    pub(crate) unsafe fn get_unchecked(&self, index: usize) -> Value {
        // SAFETY: the caller keeps INDEX within the inline slots.
        unsafe { self.cells().get_unchecked(index).get() }
    }

    #[inline]
    pub(crate) fn is_bytecode(&self) -> bool {
        // data.c:Fbyte_code_function_p checks the actual code field only.
        self.get(1).is_some_and(|code| code.is_string())
    }

    /// Fill a slot while reconstructing an object graph. Lisp cannot aset
    /// a closure, but a copier/loader must publish its identity before
    /// filling references that can lead back to the closure itself.
    pub(crate) fn initialize_slot(&self, index: usize, value: Value) {
        self.cells()[index].set(value);
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
    pub(crate) fn mark_bit(&self) -> VectorMark<'_> {
        self.header().mark_bit()
    }
}

impl std::fmt::Debug for ClosureRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.slots()).finish()
    }
}

/// A kind allocated as a pseudovector (alloc.c's
/// `allocate_pseudovector'): the kind's fields follow the header.
pub trait Vectorlike: Sized {
    const TAG: VectorTag;
    const LISP_SLOTS: usize = 0;
}

impl Vectorlike for LispBignum {
    const TAG: VectorTag = VectorTag::Bignum;
}

impl Vectorlike for crate::lisp::types::FrameValue {
    const TAG: VectorTag = VectorTag::Frame;
    const LISP_SLOTS: usize = 1;
}

impl Vectorlike for crate::lisp::types::TerminalValue {
    const TAG: VectorTag = VectorTag::Terminal;
    const LISP_SLOTS: usize = 4;
}

impl Vectorlike for crate::lisp::types::MarkerValue {
    const TAG: VectorTag = VectorTag::Marker;
}

impl Vectorlike for crate::lisp::types::OverlayValue {
    const TAG: VectorTag = VectorTag::Overlay;
    const LISP_SLOTS: usize = 1;
}

impl Vectorlike for BufferValue {
    const TAG: VectorTag = VectorTag::Buffer;
}

impl Vectorlike for crate::lisp::eval::RecordState {
    const TAG: VectorTag = VectorTag::Record;
}

/// `Lisp_Object' for a pseudovector of kind T: the header's address.
#[repr(transparent)]
pub struct VectorlikeRef<T>(NonNull<VectorHeader>, PhantomData<T>);

impl<T> Clone for VectorlikeRef<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for VectorlikeRef<T> {}

impl<T: Vectorlike> VectorlikeRef<T> {
    /// alloc.c's `allocate_pseudovector' of one T, moved in place.
    pub(crate) fn allocate(value: T) -> Self {
        const {
            assert!(std::mem::align_of::<T>() <= WORD_SIZE);
        }
        let nbytes = vroundup(HEADER_SIZE + std::mem::size_of::<T>()).max(VBLOCK_BYTES_MIN);
        let header = allocate_vectorlike(nbytes);
        // SAFETY: fresh storage of NBYTES; the payload is aligned for T.
        unsafe {
            (*header).size = Cell::new(VectorHeader::pseudovector_slots_word(
                T::TAG,
                nbytes,
                T::LISP_SLOTS,
            ));
            payload(header).cast::<T>().write(value);
            census_on_allocate(header);
            if T::TAG == VectorTag::Buffer {
                all_buffers().insert(header as usize);
            }
            Self(NonNull::new_unchecked(header), PhantomData)
        }
    }
}

impl<T> VectorlikeRef<T> {
    /// # Safety
    /// HEADER is an allocated pseudovector of kind T.
    pub(crate) unsafe fn from_raw(header: *mut VectorHeader) -> Self {
        // SAFETY: the caller's contract.
        Self(unsafe { NonNull::new_unchecked(header) }, PhantomData)
    }

    /// The payload's address, for a caller that holds the object and
    /// reads or writes its state in place.
    #[inline]
    pub(crate) fn as_ptr(&self) -> *mut T {
        // SAFETY: an allocated pseudovector of kind T (the handle's contract).
        unsafe { payload(self.0.as_ptr()).cast::<T>() }
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
    pub(crate) fn mark_bit(&self) -> VectorMark<'_> {
        // SAFETY: the header lives while a handle can be read.
        unsafe { (*self.0.as_ptr()).mark_bit() }
    }
}

impl<T> AsRef<T> for VectorlikeRef<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        self
    }
}

impl<T> std::ops::Deref for VectorlikeRef<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        // SAFETY: a T was written past the header at allocation and is
        // dropped only by the sweep, when nothing reaches it.
        unsafe { &*payload(self.0.as_ptr()).cast::<T>() }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for VectorlikeRef<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (**self).fmt(f)
    }
}

/// The census counters a fresh pseudovector raises.
///
/// # Safety
/// HEADER is an allocated pseudovector.
unsafe fn census_on_allocate(header: *mut VectorHeader) {
    // SAFETY: the caller's contract.
    unsafe {
        match (*header).tag() {
            VectorTag::Closure => {
                raise(&LIVE_CLOSURES, 1);
                raise(
                    &LIVE_CLOSURE_SLOTS,
                    ((*header).size.get() & PSEUDOVECTOR_SIZE_MASK) + 1,
                );
            }
            VectorTag::Bignum => raise(&LIVE_BIGNUMS, 1),
            VectorTag::Finalizer => {
                raise(&LIVE_VECTORS, 1);
                raise(&LIVE_VECTOR_SLOTS, 4);
            }
            VectorTag::Buffer
            | VectorTag::SymbolWithPos
            | VectorTag::Frame
            | VectorTag::Terminal
            | VectorTag::Window
            | VectorTag::Marker
            | VectorTag::Overlay
            | VectorTag::BoolVector
            | VectorTag::CharTable
            | VectorTag::SubCharTable
            | VectorTag::HashTable
            | VectorTag::Subr
            | VectorTag::NativeCompUnit => {
                if (*header).tag() == VectorTag::Overlay {
                    raise(&LIVE_OVERLAYS, 1);
                }
                raise(&LIVE_VECTORS, 1);
                // alloc.c:sweep_vectors counts the actual allocation, including
                // the header and rounding, even for a deleted but rooted object.
                raise(&LIVE_VECTOR_SLOTS, (*header).nbytes() / WORD_SIZE);
            }
            VectorTag::Record => {
                let slots = if generic_records::record_has_inline_slots(header) {
                    (*header).nbytes() / WORD_SIZE
                } else {
                    (&*payload(header).cast::<crate::lisp::eval::RecordState>()).gnu_vector_slots()
                };
                if slots != 0 {
                    raise(&LIVE_RECORDS, 1);
                    raise(&LIVE_RECORD_SLOTS, slots);
                }
            }
            VectorTag::Normal | VectorTag::Free => {}
        }
    }
}

/// alloc.c's `cleanup_vector': the kind's destructor, before its storage
/// goes to a free list.
///
/// # Safety
/// HEADER is an allocated vector nothing reaches.
unsafe fn cleanup_vector(header: *mut VectorHeader) {
    // SAFETY: the caller's contract; each kind's payload was written at
    // allocation.
    unsafe {
        let body = payload(header);
        match (*header).tag() {
            VectorTag::Normal => {
                std::ptr::drop_in_place(std::ptr::slice_from_raw_parts_mut(
                    body.cast::<Value>(),
                    (*header).size.get(),
                ));
            }
            VectorTag::Subr => {
                std::ptr::drop_in_place(body.cast::<native_functions::NativeFunctionState>())
            }
            VectorTag::NativeCompUnit => {
                std::ptr::drop_in_place(body.cast::<native_units::NativeUnitState>())
            }
            VectorTag::Free => {}
            VectorTag::Bignum => std::ptr::drop_in_place(body.cast::<LispBignum>()),
            VectorTag::Buffer => {
                std::ptr::drop_in_place(body.cast::<BufferValue>());
                let listed = all_buffers().remove(&(header as usize));
                assert!(listed, "a reclaimed buffer is in all_buffers");
            }
            VectorTag::Overlay => {
                std::ptr::drop_in_place(body.cast::<crate::lisp::types::OverlayValue>())
            }
            VectorTag::Marker => {
                let marker = body.cast::<crate::lisp::types::MarkerValue>();
                debug_assert!((*marker).is_detached());
                std::ptr::drop_in_place(marker);
            }
            VectorTag::Frame => {
                std::ptr::drop_in_place(body.cast::<crate::lisp::types::FrameValue>())
            }
            VectorTag::Terminal => {
                std::ptr::drop_in_place(body.cast::<crate::lisp::types::TerminalValue>())
            }
            VectorTag::Finalizer => std::ptr::drop_in_place(body.cast::<super::FinalizerState>()),
            // A closure owns only inline Lisp words, which have no Rust
            // destructor. Its children are reclaimed by tracing, as in C;
            // the byte-code entry cache's entry of it goes.
            VectorTag::Closure => crate::lisp::bytecode::verify::forget_closure(header as usize),
            VectorTag::BoolVector
            | VectorTag::CharTable
            | VectorTag::SubCharTable
            | VectorTag::SymbolWithPos => {}
            VectorTag::HashTable => hash_tables::cleanup(header),
            VectorTag::Record if generic_records::record_has_inline_slots(header) => {}
            VectorTag::Record => {
                let record = body.cast::<crate::lisp::eval::RecordState>();
                // The interpreter that owns the id purges its side tables
                // for the record after its sweep (`drain_freed_records').
                // The type tag by its word alone: a dead record's type may
                // be a class record this sweep has already freed, whose
                // header must not be read.
                note_freed_record(FreedRecord {
                    owner: (*record).owner,
                    id: (*record).id,
                    identity: header as usize,
                    type_name: (*record).symbol_type_name_by_tag().map(Box::from),
                });
                std::ptr::drop_in_place(record);
            }
            // A window owns only inline Lisp words and plain fields,
            // which have no Rust destructor; the three markers it alone
            // holds are reclaimed by this sweep as any object nothing
            // marked.
            VectorTag::Window => {}
        }
        if cfg!(debug_assertions) {
            let nbytes = (*header).nbytes();
            std::ptr::write_bytes(body, 0xA5, nbytes - HEADER_SIZE);
        }
    }
}

/// What the sweep counted of the marked vectors (gcstat's totals).
#[derive(Default)]
struct SweepStats {
    vectors: usize,
    vector_slots: usize,
    closures: usize,
    closure_slots: usize,
    bignums: usize,
    overlays: usize,
    records: usize,
    record_slots: usize,
}

impl SweepStats {
    /// # Safety
    /// HEADER is an allocated vector the sweep keeps, its size word
    /// without the mark (alloc.c's sweep_vectors counts a large vector
    /// after XUNMARK_VECTOR).
    unsafe fn count(&mut self, header: *mut VectorHeader) {
        // SAFETY: the caller's contract.
        unsafe {
            match (*header).tag() {
                VectorTag::Normal => {
                    self.vectors += 1;
                    self.vector_slots += (*header).size.get() + 1;
                }
                VectorTag::Closure => {
                    self.closures += 1;
                    self.closure_slots += ((*header).size.get() & PSEUDOVECTOR_SIZE_MASK) + 1;
                }
                VectorTag::Bignum => self.bignums += 1,
                VectorTag::Finalizer => {
                    self.vectors += 1;
                    self.vector_slots += 4;
                }
                VectorTag::Buffer
                | VectorTag::SymbolWithPos
                | VectorTag::Frame
                | VectorTag::Terminal
                | VectorTag::Window
                | VectorTag::Marker
                | VectorTag::Overlay
                | VectorTag::BoolVector
                | VectorTag::CharTable
                | VectorTag::SubCharTable
                | VectorTag::HashTable
                | VectorTag::Subr
                | VectorTag::NativeCompUnit => {
                    self.overlays += usize::from((*header).tag() == VectorTag::Overlay);
                    self.vectors += 1;
                    self.vector_slots += (*header).nbytes() / WORD_SIZE;
                }
                VectorTag::Record => {
                    let slots = if generic_records::record_has_inline_slots(header) {
                        (*header).nbytes() / WORD_SIZE
                    } else {
                        (&*payload(header).cast::<crate::lisp::eval::RecordState>())
                            .gnu_vector_slots()
                    };
                    if slots != 0 {
                        self.records += 1;
                        self.record_slots += slots;
                    }
                }
                VectorTag::Free => {}
            }
        }
    }
}

/// Close native libraries owned by a host editor that is ending. The
/// allocator already owns the complete object inventory; walking it here
/// avoids permanent roots or a duplicate unit registry on ordinary paths.
pub(crate) fn close_native_units_for_runtime(thread_cell: *mut std::ffi::c_void) {
    for kind in [BlockKind::VectorBlock, BlockKind::LargeVector] {
        for block in blocks_of(kind) {
            let mut header = block as *mut VectorHeader;
            loop {
                // SAFETY: registered blocks are tiled by initialized headers.
                // Closing an ELN changes no Lisp allocation and runs no Lisp.
                unsafe {
                    if (*header).tag() == VectorTag::NativeCompUnit {
                        NativeUnitRef::from_raw(header).close_for_runtime(thread_cell);
                    }
                    if matches!(kind, BlockKind::LargeVector) {
                        break;
                    }
                    header = advance(header, (*header).nbytes());
                    if !vector_in_block(header, block) {
                        break;
                    }
                }
            }
        }
    }
}

/// alloc.c's `sweep_vectors'.  Returns gcstat's `total_free_vector_slots':
/// the words of the free runs it put on the vector blocks' free lists.
pub(crate) fn sweep_vectors() -> usize {
    // The byte-code entry cache of this thread forgets each closure freed
    // below, and stays in step; other threads' empty theirs.
    let closures_in_sweep = crate::lisp::bytecode::verify::closures_in_sweep();
    VECTOR_SWEEPS.store(vector_sweeps().wrapping_add(1), Ordering::Relaxed);
    // alloc.c:sweep_buffers runs before vector storage can be reclaimed.
    // The chains are weak and can include markers whose buffer is also dead.
    // Checked builds find the buffers by every header as well.
    if cfg!(debug_assertions) {
        let mut found = Vec::new();
        for kind in [BlockKind::VectorBlock, BlockKind::LargeVector] {
            for block in blocks_of(kind) {
                let mut header = block as *mut VectorHeader;
                loop {
                    // SAFETY: registered blocks are tiled by initialized
                    // headers; this walk only reads them.
                    unsafe {
                        if (*header).tag() == VectorTag::Buffer {
                            found.push(header as usize);
                        }
                        if matches!(kind, BlockKind::LargeVector) {
                            break;
                        }
                        header = advance(header, (*header).nbytes());
                        if !vector_in_block(header, block) {
                            break;
                        }
                    }
                }
            }
        }
        let mut listed = all_buffers().iter().copied().collect::<Vec<_>>();
        found.sort_unstable();
        listed.sort_unstable();
        assert_eq!(found, listed, "all_buffers names every buffer header");
    }
    for &address in all_buffers().iter() {
        // SAFETY: a listed header is a buffer whose storage is not yet
        // reclaimed; this pass neither frees storage nor changes headers.
        unsafe {
            let buffer = crate::lisp::types::BufferRef::from_raw(address as *mut VectorHeader);
            buffer.sweep_markers();
            if !buffer.mark_bit().is_marked() {
                buffer.borrow_mut().overlays.clear();
            }
        }
    }
    clear_free_lists();
    let mut stats = SweepStats::default();
    let mut free_slots = 0usize;
    // When no vector in the image's blocks died, they are left unread
    // (checked builds read them and assert so).
    let image_unchanged = super::DUMP.vectors.unchanged();
    let mut image_live = 0;
    for (block, image_block) in sweep_blocks_of(BlockKind::VectorBlock) {
        // SAFETY: a registered vector block, released below at the
        // earliest.
        let marks = unsafe { block_marks(block) };
        if image_block && image_unchanged {
            if cfg!(debug_assertions) {
                let mut vector = block as *mut VectorHeader;
                while vector_in_block(vector, block) {
                    // SAFETY: as the walk below; this one only reads.
                    unsafe {
                        assert!(
                            (*vector).tag() == VectorTag::Free || (*vector).is_marked(),
                            "an unread image block holds a dead vector at {:#x}",
                            vector as usize
                        );
                        vector = advance(vector, (*vector).nbytes());
                    }
                }
            }
            // Its vectors unmarked (alloc.c's sweep_vectors clears
            // ARRAY_MARK_FLAG on each it keeps): the bitmap alone is read.
            super::clear_bits(&marks.words);
            continue;
        }
        let mut free_this_block = false;
        let mut vector = block as *mut VectorHeader;
        while vector_in_block(vector, block) {
            // SAFETY: the block is tiled with vectors, live or free, up
            // to where `vector_in_block' fails.
            unsafe {
                if (*vector).is_marked() {
                    if image_block {
                        // An image object stays out of gcstat's object
                        // totals, as a dumped one does.
                        image_live += 1;
                    } else {
                        stats.count(vector);
                    }
                    vector = advance(vector, (*vector).nbytes());
                    continue;
                }
                // Coalesce the run of unmarked vectors into one.
                let mut total = 0usize;
                let mut next = vector;
                loop {
                    if cfg!(debug_assertions) {
                        let first = payload(next).cast::<usize>().read();
                        assert!(
                            (*next).tag() == VectorTag::Free || first != 0xA5A5_A5A5_A5A5_A5A5,
                            "sweeping a cell already cleaned: header {:#x} at offset {} of block {:#x}, size word {:#x}, tag {:?}, marked {}, run start {:#x}",
                            next as usize,
                            next as usize - block,
                            block,
                            (*next).size.get(),
                            (*next).tag(),
                            (*next).is_marked(),
                            vector as usize,
                        );
                    }
                    // The footprint before the cleanup, which may write
                    // over a bool vector's length.
                    let nbytes = (*next).nbytes();
                    cleanup_vector(next);
                    total += nbytes;
                    next = advance(next, nbytes);
                    if !vector_in_block(next, block) || (*next).is_marked() {
                        break;
                    }
                }
                // A block with nothing live is given back.
                if vector as usize == block && !vector_in_block(next, block) {
                    free_this_block = true;
                } else if image_block {
                    setup_free_unlinked(vector, total);
                } else {
                    setup_on_free_list(vector, total);
                    free_slots += total / std::mem::size_of::<usize>();
                }
                vector = next;
            }
        }
        // The vectors kept are unmarked, as alloc.c's sweep_vectors leaves
        // them.
        super::clear_bits(&marks.words);
        if free_this_block {
            release_vector_block(block);
        }
    }
    if !image_unchanged {
        super::DUMP.vectors.set_live(image_live);
    }
    for (start, image_block) in sweep_blocks_of(BlockKind::LargeVector) {
        let vector = start as *mut VectorHeader;
        // SAFETY: a registered large vector.
        unsafe {
            let size = (*vector).size.get();
            if size & ARRAY_MARK_FLAG != 0 {
                // XUNMARK_VECTOR, then the census.
                (*vector).size.set(size & !ARRAY_MARK_FLAG);
                if !image_block {
                    stats.count(vector);
                }
            } else {
                let nbytes = (*vector).nbytes();
                cleanup_vector(vector);
                free_large_vector(vector, nbytes);
            }
        }
    }
    LIVE_VECTORS.store(stats.vectors, Ordering::Relaxed);
    LIVE_VECTOR_SLOTS.store(stats.vector_slots, Ordering::Relaxed);
    LIVE_CLOSURES.store(stats.closures, Ordering::Relaxed);
    LIVE_CLOSURE_SLOTS.store(stats.closure_slots, Ordering::Relaxed);
    LIVE_BIGNUMS.store(stats.bignums, Ordering::Relaxed);
    LIVE_OVERLAYS.store(stats.overlays, Ordering::Relaxed);
    LIVE_RECORDS.store(stats.records, Ordering::Relaxed);
    LIVE_RECORD_SLOTS.store(stats.record_slots, Ordering::Relaxed);
    crate::lisp::bytecode::verify::closures_swept(closures_in_sweep);
    free_slots
}

/// The vector census (gcstat's `total_vectors' and `total_vector_slots'
/// as this implementation has counted them: the ordinary vectors, the
/// closures, the bignums at three words each).
pub(crate) fn live_vector_census() -> (usize, usize) {
    // The books are the runtime's (`alloc::live_conses').
    crate::lisp::runtime::ensure_held();
    let bignums = LIVE_BIGNUMS.load(Ordering::Relaxed);
    (
        LIVE_VECTORS.load(Ordering::Relaxed) + LIVE_CLOSURES.load(Ordering::Relaxed) + bignums,
        LIVE_VECTOR_SLOTS.load(Ordering::Relaxed)
            + LIVE_CLOSURE_SLOTS.load(Ordering::Relaxed)
            + bignums * 3,
    )
}

/// The records' census: count and slots, as alloc.c counts a record
/// among the vectors.
pub(crate) fn live_record_census() -> (usize, usize) {
    // The books are the runtime's (`alloc::live_conses').
    crate::lisp::runtime::ensure_held();
    (
        LIVE_RECORDS.load(Ordering::Relaxed),
        LIVE_RECORD_SLOTS.load(Ordering::Relaxed),
    )
}

/// alloc.c's `live_vector_pointer': the vector, when ADDRESS names its
/// header or (an ordinary vector) one of its slots or (a pseudovector)
/// any of its fields.
///
/// # Safety
/// HEADER is an allocated vector.
unsafe fn live_vector_pointer(
    header: *mut VectorHeader,
    address: usize,
) -> Option<*mut VectorHeader> {
    let offset = address.wrapping_sub(header as usize);
    if offset == 0 {
        return Some(header);
    }
    // SAFETY: the caller's contract.
    let (nbytes, pseudo) = unsafe { ((*header).nbytes(), (*header).is_pseudovector()) };
    if offset < HEADER_SIZE || offset >= nbytes {
        return None;
    }
    if pseudo || (offset - HEADER_SIZE).is_multiple_of(std::mem::size_of::<Value>()) {
        Some(header)
    } else {
        None
    }
}

/// alloc.c's `live_small_vector_holding'.
pub(super) fn live_small_vector_holding(block: usize, address: usize) -> Option<*mut VectorHeader> {
    if address >= block + VECTOR_BLOCK_BYTES {
        return None;
    }
    let mut vector = block as *mut VectorHeader;
    while vector_in_block(vector, block) && vector as usize <= address {
        // SAFETY: the block is tiled with vectors.
        let (nbytes, free) = unsafe { ((*vector).nbytes(), (*vector).tag() == VectorTag::Free) };
        // SAFETY: as above.
        let next = unsafe { advance(vector, nbytes) };
        if address < next as usize {
            // SAFETY: an allocated vector, unless free.
            return if free {
                None
            } else {
                unsafe { live_vector_pointer(vector, address) }
            };
        }
        vector = next;
    }
    None
}

/// alloc.c's `live_large_vector_holding'.
pub(super) fn live_large_vector_holding(start: usize, address: usize) -> Option<*mut VectorHeader> {
    // SAFETY: a registered large vector.
    unsafe { live_vector_pointer(start as *mut VectorHeader, address) }
}

/// The kind of an allocated vector, from its header (`PSEUDOVECTOR_TYPE').
///
/// # Safety
/// HEADER is an allocated vector.
#[inline(always)]
pub(crate) unsafe fn header_tag(header: *mut VectorHeader) -> VectorTag {
    // SAFETY: the caller's contract.
    unsafe { (*header).tag() }
}

/// fns.c:sxhash_vector's view of a (pseudo)vector: `ASIZE', the whole
/// size word, and the `PVSIZE' (an ordinary vector's size) Lisp slots
/// `AREF' reads after the header.
///
/// # Safety
/// HEADER is a live ordinary vector or a pseudovector whose Lisp slots
/// follow its header (a closure, char-table or record), kept live while
/// the slots are read.
#[inline]
pub(crate) unsafe fn vector_size_and_slots<'a>(
    header: *mut VectorHeader,
) -> (usize, &'a [Cell<Value>]) {
    // SAFETY: the caller's contract.
    unsafe {
        let size = (*header).size.get();
        let count = if size & PSEUDOVECTOR_FLAG == 0 {
            size
        } else {
            size & PSEUDOVECTOR_SIZE_MASK
        };
        (
            size,
            std::slice::from_raw_parts(payload(header).cast::<Cell<Value>>(), count),
        )
    }
}

/// An allocated native subr has its actual ten payload words in the size
/// header. Static DEFUN subrs have zero Lisp/rest counts. This classification
/// uses only the shared header, before borrowing either different payload.
///
/// # Safety
/// HEADER names a live PVEC_SUBR.
pub(crate) unsafe fn subr_is_allocated(header: *mut VectorHeader) -> bool {
    unsafe { (*header).size.get() & ((1 << PSEUDOVECTOR_AREA_BITS) - 1) != 0 }
}

/// `header_tag(HEADER) == VectorTag::Subr' by one masked compare of the
/// size word, where the tag's decoding branches on the flag first.
///
/// # Safety
/// HEADER is an allocated vector.
#[inline(always)]
pub(crate) unsafe fn is_subr(header: *mut VectorHeader) -> bool {
    const KIND: usize = PSEUDOVECTOR_FLAG | (0x3f << PSEUDOVECTOR_AREA_BITS);
    const SUBR: usize = PSEUDOVECTOR_FLAG | ((VectorTag::Subr as usize) << PSEUDOVECTOR_AREA_BITS);
    // SAFETY: the caller's contract.
    unsafe { (*header).size.get() & KIND == SUBR }
}

/// The value naming an allocated vector (for the conservative scan).
///
/// # Safety
/// HEADER is an allocated (not free) vector.
pub(super) unsafe fn value_of(header: *mut VectorHeader) -> Value {
    // SAFETY: the caller's contract.
    unsafe {
        match (*header).tag() {
            VectorTag::Normal => Value::Vector(VectorRef::from_raw(header)),
            VectorTag::Bignum => {
                Value::BigInteger(super::super::types::SharedBigInt::from_raw(header))
            }
            VectorTag::Buffer => Value::Buffer(VectorlikeRef::from_raw(header)),
            VectorTag::Marker => Value::Marker(VectorlikeRef::from_raw(header)),
            VectorTag::Overlay => Value::Overlay(VectorlikeRef::from_raw(header)),
            VectorTag::Frame => Value::Frame(VectorlikeRef::from_raw(header)),
            VectorTag::Window => Value::Window(VectorlikeRef::from_raw(header)),
            VectorTag::Terminal => Value::Terminal(VectorlikeRef::from_raw(header)),
            VectorTag::Finalizer => Value::Finalizer(VectorlikeRef::from_raw(header)),
            VectorTag::SymbolWithPos => Value::SymbolWithPos(SymbolWithPosRef::from_raw(header)),
            VectorTag::BoolVector => Value::BoolVector(BoolVectorRef::from_raw(header)),
            VectorTag::Closure => Value::Closure(ClosureRef::from_raw(header)),
            VectorTag::CharTable => Value::CharTable(CharTableRef::from_raw(header)),
            VectorTag::SubCharTable => Value::SubCharTable(SubCharTableRef::from_raw(header)),
            VectorTag::HashTable => Value::HashTable(HashTableRef::from_raw(header)),
            VectorTag::Record if generic_records::record_has_inline_slots(header) => {
                Value::LispRecord(LispRecordRef::from_raw(header))
            }
            VectorTag::Record => Value::Record(VectorlikeRef::from_raw(header)),
            VectorTag::Subr => Value::NativeFunction(NativeFunctionRef::from_raw(header)),
            VectorTag::NativeCompUnit => Value::NativeCompUnit(NativeUnitRef::from_raw(header)),
            VectorTag::Free => unreachable!("a free vector is not a value"),
        }
    }
}

/// Each canonical overlay owns one configured 80-byte interval node. The
/// vector census already includes its 32-byte header and payload allocation.
pub(crate) fn live_overlay_node_bytes() -> usize {
    // The books are the runtime's (`alloc::live_conses').
    crate::lisp::runtime::ensure_held();
    LIVE_OVERLAYS.load(Ordering::Relaxed) * 80
}
