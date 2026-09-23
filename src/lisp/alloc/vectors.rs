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
//! slots. The current collector uses collection epochs rather than C's
//! ARRAY_MARK_FLAG: small vectors share a block bitmap, and large vectors
//! carry their mark past the payload. Ordinary object access reads neither.

use super::super::types::{BufferValue, LispBignum, MarkBit, ReaderForm, SharedStringState, Value};
use super::{BlockKind, blocks_of, register_block, unregister_block};
use std::cell::{Cell, RefCell};
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

/// alloc.c's `VECTOR_BLOCK_SIZE'.
pub(crate) const VECTOR_BLOCK_SIZE: usize = 4096;
/// alloc.c's `roundup_size': every vector's footprint is a multiple.
const ROUNDUP_SIZE: usize = 16;

const fn vroundup(bytes: usize) -> usize {
    (bytes + ROUNDUP_SIZE - 1) & !(ROUNDUP_SIZE - 1)
}

/// Space available to vectors. The allocator's block bitmap replaces the
/// per-vector epoch words; no Lisp payload contains this metadata.
const VECTOR_BLOCK_BYTES: usize =
    (VECTOR_BLOCK_SIZE - std::mem::size_of::<VectorBlockMarks>()) & !(ROUNDUP_SIZE - 1);
const VECTOR_MARK_WORDS: usize = (VECTOR_BLOCK_SIZE / ROUNDUP_SIZE).div_ceil(usize::BITS as usize);
const LARGE_MARK_BYTES: usize = vroundup(std::mem::size_of::<MarkBit>());
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
    Finalizer = 5,
    Buffer = 13,
    Subr = 18,
    Closure = 31,
    /// lisp.h's PVEC_RECORD: a record, and the pseudovector kinds this
    /// implementation keeps as records (the kind is in the state).
    Record = 34,
    StringObject = 40,
    ReaderForm = 41,
}

impl VectorTag {
    fn from_word(word: usize) -> Self {
        match word {
            1 => Self::Free,
            2 => Self::Bignum,
            5 => Self::Finalizer,
            13 => Self::Buffer,
            18 => Self::Subr,
            31 => Self::Closure,
            34 => Self::Record,
            40 => Self::StringObject,
            41 => Self::ReaderForm,
            _ => Self::Normal,
        }
    }
}

/// lisp.h's `union vectorlike_header': exactly one word before the payload.
#[repr(C)]
pub struct VectorHeader {
    /// An ordinary vector's slot count; a pseudovector's or a free
    /// vector's `PSEUDOVECTOR_FLAG', tag and word count.
    size: usize,
}

const _: () = assert!(HEADER_SIZE == WORD_SIZE);

struct VectorBlockMarks {
    epoch: Cell<u32>,
    words: [Cell<usize>; VECTOR_MARK_WORDS],
}

impl VectorBlockMarks {
    const fn new() -> Self {
        Self {
            epoch: Cell::new(0),
            words: [const { Cell::new(0) }; VECTOR_MARK_WORDS],
        }
    }
}

enum VectorMarkStorage<'a> {
    Block(&'a VectorBlockMarks, usize),
    Separate(&'a MarkBit),
}

pub(crate) struct VectorMark<'a>(VectorMarkStorage<'a>);

impl VectorMark<'_> {
    pub(crate) fn is_marked(&self, epoch: u32) -> bool {
        match self.0 {
            VectorMarkStorage::Block(block, index) => {
                block.epoch.get() == epoch
                    && block.words[index / usize::BITS as usize].get()
                        & (1 << (index % usize::BITS as usize))
                        != 0
            }
            VectorMarkStorage::Separate(mark) => mark.is_marked(epoch),
        }
    }

    pub(crate) fn mark(&self, epoch: u32) -> bool {
        match self.0 {
            VectorMarkStorage::Block(block, index) => {
                if block.epoch.get() != epoch {
                    for word in &block.words {
                        word.set(0);
                    }
                    block.epoch.set(epoch);
                }
                let word = &block.words[index / usize::BITS as usize];
                let mask = 1 << (index % usize::BITS as usize);
                let old = word.get();
                word.set(old | mask);
                old & mask == 0
            }
            VectorMarkStorage::Separate(mark) => mark.mark(epoch),
        }
    }
}

impl VectorHeader {
    /// The collector derives metadata from the allocated object's address
    /// and size. This method is never needed to read or mutate a Lisp slot.
    fn mark_bit(&self) -> VectorMark<'_> {
        debug_assert_ne!(self.tag(), VectorTag::Free);
        let address = std::ptr::from_ref(self) as usize;
        if self.size == 0 {
            // SAFETY: the only zero-length vector is the permanently
            // allocated ZeroVector, whose mark follows this header.
            return VectorMark(VectorMarkStorage::Separate(unsafe {
                &(*(address as *const ZeroVector)).mark
            }));
        }
        let nbytes = self.nbytes();
        if nbytes > VBLOCK_BYTES_MAX {
            // SAFETY: allocate_vectorlike reserves and initializes a mark
            // immediately after every large vector's rounded payload.
            return VectorMark(VectorMarkStorage::Separate(unsafe {
                &*((address + nbytes) as *const MarkBit)
            }));
        }
        let base = address & !(VECTOR_BLOCK_SIZE - 1);
        let index = (address - base) / ROUNDUP_SIZE;
        // SAFETY: small vector blocks are aligned to VECTOR_BLOCK_SIZE;
        // their initialized bitmap occupies the reserved tail of the block.
        let block = unsafe { &*((base + VECTOR_BLOCK_BYTES) as *const VectorBlockMarks) };
        VectorMark(VectorMarkStorage::Block(block, index))
    }

    fn is_marked(&self, epoch: u32) -> bool {
        self.tag() != VectorTag::Free && self.mark_bit().is_marked(epoch)
    }

    fn tag(&self) -> VectorTag {
        if self.size & PSEUDOVECTOR_FLAG == 0 {
            VectorTag::Normal
        } else {
            VectorTag::from_word((self.size >> PSEUDOVECTOR_AREA_BITS) & 0x3f)
        }
    }

    fn is_pseudovector(&self) -> bool {
        self.size & PSEUDOVECTOR_FLAG != 0
    }

    /// alloc.c's `vector_nbytes': the footprint.
    fn nbytes(&self) -> usize {
        if self.is_pseudovector() {
            let traced = self.size & PSEUDOVECTOR_SIZE_MASK;
            let rest = (self.size >> PSEUDOVECTOR_SIZE_BITS) & PSEUDOVECTOR_SIZE_MASK;
            HEADER_SIZE + (traced + rest) * WORD_SIZE
        } else {
            vroundup(HEADER_SIZE + self.size * std::mem::size_of::<Value>())
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

/// alloc.c's `vector_free_lists' and `last_inserted_vector_free_idx'.
static FREE_LISTS: [AtomicPtr<VectorHeader>; FREE_LIST_ARRAY_SIZE] =
    [const { AtomicPtr::new(std::ptr::null_mut()) }; FREE_LIST_ARRAY_SIZE];
static LAST_INSERTED_FREE_INDEX: AtomicUsize = AtomicUsize::new(FREE_LIST_ARRAY_SIZE);

/// gcstat's `total_vectors' and `total_vector_slots' by kind, and the
/// string objects' share of `total_strings' and `total_string_bytes':
/// raised by allocation, set by the sweep.
static LIVE_VECTORS: AtomicUsize = AtomicUsize::new(0);
static LIVE_VECTOR_SLOTS: AtomicUsize = AtomicUsize::new(0);
static LIVE_CLOSURES: AtomicUsize = AtomicUsize::new(0);
static LIVE_CLOSURE_SLOTS: AtomicUsize = AtomicUsize::new(0);
static LIVE_BIGNUMS: AtomicUsize = AtomicUsize::new(0);
/// The records' share of `total_vectors' and `total_vector_slots'
/// (alloc.c counts a record as a vector of its slots).
static LIVE_RECORDS: AtomicUsize = AtomicUsize::new(0);
static LIVE_RECORD_SLOTS: AtomicUsize = AtomicUsize::new(0);

/// The records the sweep freed, as (owner, id): the interpreter whose
/// id space the record was in purges the side tables it keeps by that
/// id after its sweep (`Interpreter::drain_freed_records').  A record is
/// an object of the process, an id a name in one interpreter's tables.
static FREED_RECORDS: std::sync::Mutex<Vec<FreedRecord>> = std::sync::Mutex::new(Vec::new());

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

fn note_freed_record(owner: u32, id: u64, identity: usize, type_name: Option<Box<str>>) {
    FREED_RECORDS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(FreedRecord {
            owner,
            id,
            identity,
            type_name,
        });
}

/// The records the sweeps freed since the last call.
pub(crate) fn take_freed_records() -> Vec<FreedRecord> {
    std::mem::take(
        &mut *FREED_RECORDS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
    )
}
static LIVE_STRING_OBJECTS: AtomicUsize = AtomicUsize::new(0);
static LIVE_STRING_OBJECT_BYTES: AtomicUsize = AtomicUsize::new(0);
static LIVE_STRING_OBJECT_SPANS: AtomicUsize = AtomicUsize::new(0);

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
        (*header).size = VectorHeader::pseudovector_size_word(VectorTag::Free, nbytes);
        payload(header)
            .cast::<*mut VectorHeader>()
            .write(FREE_LISTS[index].load(Ordering::Relaxed));
    }
    FREE_LISTS[index].store(header, Ordering::Relaxed);
    LAST_INSERTED_FREE_INDEX.store(index, Ordering::Relaxed);
}

/// alloc.c's `allocate_vector_block'.
fn allocate_vector_block() -> usize {
    let layout = std::alloc::Layout::from_size_align(VECTOR_BLOCK_SIZE, VECTOR_BLOCK_SIZE)
        .expect("vector block layout");
    // SAFETY: a non-zero layout.
    let block = unsafe { std::alloc::alloc(layout) };
    assert!(!block.is_null(), "out of memory for a vector block");
    let start = block as usize;
    // SAFETY: metadata is outside the region carved into Lisp vectors and
    // is initialized before the block becomes visible to the collector.
    unsafe {
        block
            .add(VECTOR_BLOCK_BYTES)
            .cast::<VectorBlockMarks>()
            .write(VectorBlockMarks::new())
    };
    register_block(start, BlockKind::VectorBlock);
    start
}

fn release_vector_block(start: usize) {
    unregister_block(start);
    let layout = std::alloc::Layout::from_size_align(VECTOR_BLOCK_SIZE, VECTOR_BLOCK_SIZE)
        .expect("vector block layout");
    // SAFETY: a block `allocate_vector_block' made, with nothing live.
    unsafe { std::alloc::dealloc(start as *mut u8, layout) };
}

/// alloc.c's `allocate_vector_from_block'.
fn allocate_vector_from_block(nbytes: usize) -> *mut VectorHeader {
    debug_assert!((VBLOCK_BYTES_MIN..=VBLOCK_BYTES_MAX).contains(&nbytes));
    debug_assert!(nbytes.is_multiple_of(ROUNDUP_SIZE));
    // First, the free list of exactly this size.
    let index = vindex(nbytes);
    let head = FREE_LISTS[index].load(Ordering::Relaxed);
    if !head.is_null() {
        // SAFETY: a free vector's link.
        unsafe {
            debug_assert!(
                (*head).tag() == VectorTag::Free && (*head).nbytes() == nbytes,
                "free list {index} held {:#x} with size word {:#x}",
                head as usize,
                (*head).size
            );
            FREE_LISTS[index].store(next_free(head), Ordering::Relaxed);
        }
        return head;
    }
    // Next, a larger free vector, split so that the rest is at least a
    // one-slot vector.
    let first =
        vindex(nbytes + VBLOCK_BYTES_MIN).max(LAST_INSERTED_FREE_INDEX.load(Ordering::Relaxed));
    for list in &FREE_LISTS[first.min(FREE_LIST_ARRAY_SIZE)..] {
        let head = list.load(Ordering::Relaxed);
        if head.is_null() {
            continue;
        }
        // SAFETY: a free vector: its header and link are readable.
        let vector_nbytes = unsafe { (*head).nbytes() };
        debug_assert!(vector_nbytes > nbytes);
        list.store(unsafe { next_free(head) }, Ordering::Relaxed);
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

/// alloc.c's `allocate_vectorlike': NBYTES of storage (the header
/// included) from a block, or on its own past `VBLOCK_BYTES_MAX'.  The
/// header is the caller's to write.
fn allocate_vectorlike(nbytes: usize) -> *mut VectorHeader {
    let nbytes = vroundup(nbytes).max(VBLOCK_BYTES_MIN);
    if nbytes <= VBLOCK_BYTES_MAX {
        allocate_vector_from_block(nbytes)
    } else {
        let layout = std::alloc::Layout::from_size_align(nbytes + LARGE_MARK_BYTES, ROUNDUP_SIZE)
            .expect("large vector layout");
        // SAFETY: a non-zero layout.
        let vector = unsafe { std::alloc::alloc(layout) };
        assert!(!vector.is_null(), "out of memory for a vector");
        // SAFETY: the reserved tail is aligned for MarkBit and is not part
        // of the Lisp-visible header or payload.
        unsafe {
            vector
                .add(nbytes)
                .cast::<MarkBit>()
                .write(MarkBit::default())
        };
        register_block(vector as usize, BlockKind::LargeVector);
        vector.cast()
    }
}

fn free_large_vector(header: *mut VectorHeader, nbytes: usize) {
    unregister_block(header as usize);
    let layout = std::alloc::Layout::from_size_align(nbytes + LARGE_MARK_BYTES, ROUNDUP_SIZE)
        .expect("large vector layout");
    // SAFETY: `allocate_vectorlike' made it with this layout; nothing
    // reaches it.
    unsafe { std::alloc::dealloc(header.cast(), layout) };
}

/// The one zero-length vector (alloc.c's `zero_vector'), outside every
/// block: every empty allocation is it, so it is never swept.
#[repr(C)]
struct ZeroVector {
    header: VectorHeader,
    mark: MarkBit,
}

static ZERO_VECTOR: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

pub(super) fn zero_vector_at(address: usize) -> Option<*mut VectorHeader> {
    ZERO_VECTOR
        .get()
        .filter(|&&zero| zero == address)
        .map(|&zero| zero as *mut VectorHeader)
}

fn zero_vector() -> *mut VectorHeader {
    *ZERO_VECTOR.get_or_init(|| {
        Box::leak(Box::new(ZeroVector {
            header: VectorHeader { size: 0 },
            mark: MarkBit::default(),
        })) as *mut ZeroVector as usize
    }) as *mut VectorHeader
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
            (*header).size = len;
            (*header)
                .mark_bit()
                .mark(super::super::types::current_mark_epoch());
            std::ptr::copy_nonoverlapping(slots.as_ptr(), payload(header).cast::<Value>(), len);
            slots.set_len(0);
        }
        raise(&LIVE_VECTORS, 1);
        raise(&LIVE_VECTOR_SLOTS, len + 1);
        // SAFETY: a fresh, non-null header.
        Self(unsafe { NonNull::new_unchecked(header) })
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

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.header().size
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

    #[inline]
    pub(crate) fn get(&self, index: usize) -> Option<Value> {
        self.cells().get(index).map(Cell::get)
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

/// An interpreted PVEC_CLOSURE: its header followed by the actual Lisp
/// slots. No copied parameter/body representation accompanies the object.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct ClosureRef(NonNull<VectorHeader>);

impl ClosureRef {
    pub(crate) fn allocate(slots: &[Value]) -> Self {
        assert!((3..=6).contains(&slots.len()));
        let nbytes = vroundup(HEADER_SIZE + slots.len() * WORD_SIZE);
        // Account for the Lisp allocation, including the one-word header
        // and allocator rounding. Block metadata is shared by small vectors.
        crate::lisp::native_comp::note_lisp_allocation(nbytes);
        let header = allocate_vectorlike(nbytes);
        // SAFETY: freshly allocated NBYTES, aligned for the header and slots.
        // Every traced slot is initialized before the object is published.
        unsafe {
            (*header).size =
                VectorHeader::pseudovector_slots_word(VectorTag::Closure, nbytes, slots.len());
            (*header)
                .mark_bit()
                .mark(super::super::types::current_mark_epoch());
            let target = payload(header).cast::<Cell<Value>>();
            for (index, value) in slots.iter().enumerate() {
                target.add(index).write(Cell::new(*value));
            }
            census_on_allocate(header);
            Self(NonNull::new_unchecked(header))
        }
    }

    /// # Safety
    /// HEADER names a live interpreted closure allocated by this allocator.
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
        self.header().size & PSEUDOVECTOR_SIZE_MASK
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

impl Vectorlike for BufferValue {
    const TAG: VectorTag = VectorTag::Buffer;
}

impl Vectorlike for RefCell<SharedStringState> {
    const TAG: VectorTag = VectorTag::StringObject;
}

impl Vectorlike for ReaderForm {
    const TAG: VectorTag = VectorTag::ReaderForm;
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
            (*header).size = VectorHeader::pseudovector_slots_word(T::TAG, nbytes, T::LISP_SLOTS);
            (*header)
                .mark_bit()
                .mark(super::super::types::current_mark_epoch());
            payload(header).cast::<T>().write(value);
            census_on_allocate(header);
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
                    ((*header).size & PSEUDOVECTOR_SIZE_MASK) + 1,
                );
            }
            VectorTag::Bignum => raise(&LIVE_BIGNUMS, 1),
            VectorTag::Finalizer => {
                raise(&LIVE_VECTORS, 1);
                raise(&LIVE_VECTOR_SLOTS, 4);
            }
            VectorTag::Record => {
                let record = &*payload(header).cast::<crate::lisp::eval::RecordState>();
                let slots = record.gnu_vector_slots();
                if slots != 0 {
                    raise(&LIVE_RECORDS, 1);
                    raise(&LIVE_RECORD_SLOTS, slots);
                }
            }
            VectorTag::StringObject => {
                let state = (*payload(header).cast::<RefCell<SharedStringState>>()).borrow();
                raise(&LIVE_STRING_OBJECTS, 1);
                raise(&LIVE_STRING_OBJECT_BYTES, state.storage_bytes());
                raise(&LIVE_STRING_OBJECT_SPANS, state.props.len());
            }
            VectorTag::Subr => unreachable!("static subrs do not enter vector allocation"),
            VectorTag::Normal | VectorTag::Free | VectorTag::Buffer | VectorTag::ReaderForm => {}
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
                    (*header).size,
                ));
            }
            VectorTag::Subr => unreachable!("static subrs are not swept"),
            VectorTag::Free => {}
            VectorTag::Bignum => std::ptr::drop_in_place(body.cast::<LispBignum>()),
            VectorTag::Buffer => std::ptr::drop_in_place(body.cast::<BufferValue>()),
            VectorTag::Finalizer => std::ptr::drop_in_place(body.cast::<super::FinalizerState>()),
            // A closure owns only inline Lisp words, which have no Rust
            // destructor. Its children are reclaimed by tracing, as in C.
            VectorTag::Closure => {}
            VectorTag::StringObject => {
                std::ptr::drop_in_place(body.cast::<RefCell<SharedStringState>>())
            }
            VectorTag::ReaderForm => std::ptr::drop_in_place(body.cast::<ReaderForm>()),
            VectorTag::Record => {
                let record = body.cast::<crate::lisp::eval::RecordState>();
                // The interpreter that owns the id purges its side tables
                // for the record after its sweep (`drain_freed_records').
                // The type tag by its word alone: a dead record's type may
                // be a class record this sweep has already freed, whose
                // header must not be read.
                note_freed_record(
                    (*record).owner,
                    (*record).id,
                    header as usize,
                    (*record).symbol_type_name_by_tag().map(Box::from),
                );
                std::ptr::drop_in_place(record);
            }
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
    records: usize,
    record_slots: usize,
    string_objects: usize,
    string_object_bytes: usize,
    string_object_spans: usize,
}

impl SweepStats {
    /// # Safety
    /// HEADER is an allocated, marked vector.
    unsafe fn count(&mut self, header: *mut VectorHeader) {
        // SAFETY: the caller's contract.
        unsafe {
            match (*header).tag() {
                VectorTag::Normal => {
                    self.vectors += 1;
                    self.vector_slots += (*header).size + 1;
                }
                VectorTag::Closure => {
                    self.closures += 1;
                    self.closure_slots += ((*header).size & PSEUDOVECTOR_SIZE_MASK) + 1;
                }
                VectorTag::Bignum => self.bignums += 1,
                VectorTag::Finalizer => {
                    self.vectors += 1;
                    self.vector_slots += 4;
                }
                VectorTag::Record => {
                    let record = &*payload(header).cast::<crate::lisp::eval::RecordState>();
                    let slots = record.gnu_vector_slots();
                    if slots != 0 {
                        self.records += 1;
                        self.record_slots += slots;
                    }
                }
                VectorTag::StringObject => {
                    self.string_objects += 1;
                    // A string object the collection found mutably borrowed
                    // (its mutator allocating) is counted without its bytes.
                    if let Ok(state) =
                        (*payload(header).cast::<RefCell<SharedStringState>>()).try_borrow()
                    {
                        self.string_object_bytes += state.storage_bytes();
                        self.string_object_spans += state.props.len();
                    }
                }
                VectorTag::Subr => unreachable!("static subrs are not swept"),
                VectorTag::Free | VectorTag::Buffer | VectorTag::ReaderForm => {}
            }
        }
    }
}

/// alloc.c's `sweep_vectors'.
pub(crate) fn sweep_vectors(epoch: u32) {
    for list in &FREE_LISTS {
        list.store(std::ptr::null_mut(), Ordering::Relaxed);
    }
    LAST_INSERTED_FREE_INDEX.store(FREE_LIST_ARRAY_SIZE, Ordering::Relaxed);
    let mut stats = SweepStats::default();
    for block in blocks_of(BlockKind::VectorBlock) {
        let mut free_this_block = false;
        let mut vector = block as *mut VectorHeader;
        while vector_in_block(vector, block) {
            // SAFETY: the block is tiled with vectors, live or free, up
            // to where `vector_in_block' fails.
            unsafe {
                if (*vector).is_marked(epoch) {
                    stats.count(vector);
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
                            (*next).size,
                            (*next).tag(),
                            (*next).is_marked(epoch),
                            vector as usize,
                        );
                    }
                    cleanup_vector(next);
                    let nbytes = (*next).nbytes();
                    total += nbytes;
                    next = advance(next, nbytes);
                    if !vector_in_block(next, block) || (*next).is_marked(epoch) {
                        break;
                    }
                }
                if vector as usize == block && !vector_in_block(next, block) {
                    free_this_block = true;
                } else {
                    setup_on_free_list(vector, total);
                }
                vector = next;
            }
        }
        if free_this_block {
            release_vector_block(block);
        }
    }
    for start in blocks_of(BlockKind::LargeVector) {
        let vector = start as *mut VectorHeader;
        // SAFETY: a registered large vector.
        unsafe {
            if (*vector).is_marked(epoch) {
                stats.count(vector);
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
    LIVE_RECORDS.store(stats.records, Ordering::Relaxed);
    LIVE_RECORD_SLOTS.store(stats.record_slots, Ordering::Relaxed);
    LIVE_STRING_OBJECTS.store(stats.string_objects, Ordering::Relaxed);
    LIVE_STRING_OBJECT_BYTES.store(stats.string_object_bytes, Ordering::Relaxed);
    LIVE_STRING_OBJECT_SPANS.store(stats.string_object_spans, Ordering::Relaxed);
}

/// The vector census (gcstat's `total_vectors' and `total_vector_slots'
/// as this implementation has counted them: the ordinary vectors, the
/// closures, the bignums at three words each).
pub(crate) fn live_vector_census() -> (usize, usize) {
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
    (
        LIVE_RECORDS.load(Ordering::Relaxed),
        LIVE_RECORD_SLOTS.load(Ordering::Relaxed),
    )
}

/// The string objects' census: count, storage bytes, property spans.
pub(crate) fn live_string_object_census() -> (usize, usize, usize) {
    (
        LIVE_STRING_OBJECTS.load(Ordering::Relaxed),
        LIVE_STRING_OBJECT_BYTES.load(Ordering::Relaxed),
        LIVE_STRING_OBJECT_SPANS.load(Ordering::Relaxed),
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
            VectorTag::Finalizer => Value::Finalizer(VectorlikeRef::from_raw(header)),
            VectorTag::Closure => Value::Lambda(ClosureRef::from_raw(header)),
            VectorTag::StringObject => Value::StringObject(VectorlikeRef::from_raw(header)),
            VectorTag::ReaderForm => Value::ReaderForm(VectorlikeRef::from_raw(header)),
            VectorTag::Record => Value::Record(VectorlikeRef::from_raw(header)),
            VectorTag::Subr => unreachable!("static subrs do not live in vector allocations"),
            VectorTag::Free => unreachable!("a free vector is not a value"),
        }
    }
}
