//! alloc.c: sdata, allocate_string_data, free_large_strings and
//! compact_small_strings. Only allocation and collection touch these lists.
//! As with the other arenas, callers must own the serialized Lisp runtime.

use super::{SharedStringState, StringCell};
use crate::lisp::types::string_data::StringError;
use std::alloc::{Layout, alloc, dealloc};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicPtr, Ordering};

const ALIGN: usize = std::mem::align_of::<usize>();
const PREFIX: usize = std::mem::size_of::<*mut SharedStringState>();
// alloc.c:MALLOC_SIZE_NEAR(8192), on the supported 64-bit targets.
const BLOCK_BYTES: usize = 8192 - std::mem::size_of::<usize>();
const LARGE_BYTES: usize = 1024;

#[repr(C)]
struct Block {
    next: *mut Block,
    next_free: *mut u8,
}

const HEADER: usize = std::mem::size_of::<Block>();

pub(crate) fn string_data_size(bytes: usize) -> usize {
    (PREFIX + bytes + 1).max(2 * ALIGN).next_multiple_of(ALIGN)
}

struct SmallBlocks {
    oldest: AtomicPtr<Block>,
    current: AtomicPtr<Block>,
}

impl SmallBlocks {
    const fn new() -> Self {
        Self {
            oldest: AtomicPtr::new(ptr::null_mut()),
            current: AtomicPtr::new(ptr::null_mut()),
        }
    }
}

static SMALL: SmallBlocks = SmallBlocks::new();
static LARGE: AtomicPtr<Block> = AtomicPtr::new(ptr::null_mut());
// Pure data is packed separately, remains immovable and is not collected,
// as GNU pure storage is. This does not implement a shared pure arena for
// other object kinds or claim complete physical allocation accounting.
static PURE_SMALL: SmallBlocks = SmallBlocks::new();
static PURE_LARGE: AtomicPtr<Block> = AtomicPtr::new(ptr::null_mut());

unsafe fn new_block(bytes: usize) -> Result<*mut Block, StringError> {
    let layout = Layout::from_size_align(bytes, ALIGN).expect("string block layout");
    // SAFETY: a nonzero aligned allocation; initialized fields precede data.
    let raw = unsafe { alloc(layout) };
    if raw.is_null() {
        return Err(StringError::AllocationFailed);
    }
    let block = raw.cast::<Block>();
    unsafe {
        block.write(Block {
            next: ptr::null_mut(),
            next_free: raw.add(HEADER),
        });
    }
    Ok(block)
}

/// An allocation not yet installed in its stable header. An unwinding
/// initializer leaves a correctly sized dead entry, never uninitialized
/// contents that the collector could mistake for a live string.
pub(crate) struct PendingStringData {
    data: NonNull<u8>,
    bytes: usize,
    pending: bool,
}

impl PendingStringData {
    /// # Safety
    /// OWNER is its final, exclusively borrowed string-header address.
    /// The caller initializes every content byte before installing DATA.
    pub(crate) unsafe fn new(
        owner: *mut SharedStringState,
        bytes: usize,
        pure: bool,
    ) -> Result<Self, StringError> {
        assert!(bytes > 0 && bytes < isize::MAX as usize - 2 * ALIGN);
        let needed = string_data_size(bytes);
        // SAFETY: the serialized runtime exclusively mutates the arena's
        // block links and unused storage. Existing contents do not move.
        let entry = unsafe {
            let block = if bytes > LARGE_BYTES {
                let chain = if pure { &PURE_LARGE } else { &LARGE };
                let block = new_block(HEADER + needed)?;
                (*block).next = chain.load(Ordering::Relaxed);
                chain.store(block, Ordering::Relaxed);
                block
            } else {
                let chain = if pure { &PURE_SMALL } else { &SMALL };
                let mut block = chain.current.load(Ordering::Relaxed);
                if block.is_null()
                    || (*block).next_free.offset_from(block.cast()) as usize + needed > BLOCK_BYTES
                {
                    let next = new_block(BLOCK_BYTES)?;
                    if block.is_null() {
                        chain.oldest.store(next, Ordering::Relaxed);
                    } else {
                        (*block).next = next;
                    }
                    chain.current.store(next, Ordering::Relaxed);
                    block = next;
                }
                block
            };
            let entry = (*block).next_free;
            (*block).next_free = entry.add(needed);
            entry.cast::<*mut SharedStringState>().write(owner);
            entry.add(PREFIX + bytes).write(0);
            entry
        };
        Ok(Self {
            // SAFETY: ENTRY belongs to a successful nonempty allocation.
            data: unsafe { NonNull::new_unchecked(entry.add(PREFIX)) },
            bytes,
            pending: true,
        })
    }

    pub(crate) fn as_ptr(&self) -> *mut u8 {
        self.data.as_ptr()
    }

    pub(crate) fn install(mut self) -> NonNull<u8> {
        self.pending = false;
        self.data
    }
}

impl Drop for PendingStringData {
    fn drop(&mut self) {
        if self.pending {
            // SAFETY: this guard still owns the uninstalled entry.
            unsafe { retire_string_data(self.data, self.bytes) };
        }
    }
}

/// # Safety
/// DATA names an allocated sdata entry with BYTES content bytes. No guard
/// may retain its contents after retirement. Empty static data is excluded.
pub(crate) unsafe fn retire_string_data(data: NonNull<u8>, bytes: usize) {
    unsafe {
        let entry = data.as_ptr().sub(PREFIX);
        entry
            .cast::<*mut SharedStringState>()
            .write(ptr::null_mut());
        data.as_ptr().cast::<usize>().write(bytes);
    }
}

unsafe fn entry_bytes(entry: *mut u8, owner: *mut SharedStringState) -> usize {
    unsafe {
        if owner.is_null() {
            entry.add(PREFIX).cast::<usize>().read()
        } else {
            (*owner).storage_bytes()
        }
    }
}

unsafe fn pinned(block: *mut Block) -> bool {
    unsafe {
        let mut entry = block.cast::<u8>().add(HEADER);
        while entry < (*block).next_free {
            let owner = entry.cast::<*mut SharedStringState>().read();
            if !owner.is_null() && (*owner.cast::<StringCell>()).borrows.get() > 0 {
                return true;
            }
            entry = entry.add(string_data_size(entry_bytes(entry, owner)));
        }
    }
    false
}

#[derive(Default)]
struct Chain {
    first: *mut Block,
    last: *mut Block,
}

impl Chain {
    unsafe fn append(&mut self, block: *mut Block) {
        unsafe {
            (*block).next = ptr::null_mut();
            if self.last.is_null() {
                self.first = block;
            } else {
                (*self.last).next = block;
            }
            self.last = block;
        }
    }
}

/// GNU compaction on blocks without Rust content borrows. A live Rust
/// reference cannot be relocated or have its header changed. Temporarily
/// keep its entire block outside the moving chain; the next collection
/// can compact it once the borrow ends. No registry or per-read lookup.
pub(super) fn sweep() {
    // SAFETY: string headers have been swept and all exclusive borrows
    // rejected before the GC epoch. Dead entries no longer name headers.
    unsafe {
        let mut block = LARGE.load(Ordering::Relaxed);
        let mut large = Chain::default();
        while !block.is_null() {
            let next = (*block).next;
            let entry = block.cast::<u8>().add(HEADER);
            if entry.cast::<*mut SharedStringState>().read().is_null() {
                let bytes = (*block).next_free.offset_from(block.cast()) as usize;
                dealloc(
                    block.cast(),
                    Layout::from_size_align(bytes, ALIGN).expect("allocated string block layout"),
                );
            } else {
                large.append(block);
            }
            block = next;
        }
        LARGE.store(large.first, Ordering::Relaxed);

        let mut held = Chain::default();
        let mut moving = Chain::default();
        block = SMALL.oldest.load(Ordering::Relaxed);
        while !block.is_null() {
            let next = (*block).next;
            if pinned(block) {
                held.append(block);
            } else {
                moving.append(block);
            }
            block = next;
        }

        let mut target = moving.first;
        if !target.is_null() {
            let mut to = target.cast::<u8>().add(HEADER);
            block = moving.first;
            while !block.is_null() {
                let end = (*block).next_free;
                let mut from = block.cast::<u8>().add(HEADER);
                while from < end {
                    let owner = from.cast::<*mut SharedStringState>().read();
                    let size = string_data_size(entry_bytes(from, owner));
                    let next = from.add(size);
                    if !owner.is_null() {
                        if to.offset_from(target.cast()) as usize + size > BLOCK_BYTES {
                            (*target).next_free = to;
                            target = (*target).next;
                            to = target.cast::<u8>().add(HEADER);
                        }
                        if from != to {
                            debug_assert!(target != block || to < from);
                            debug_assert_eq!((*owner.cast::<StringCell>()).borrows.get(), 0);
                            ptr::copy(from, to, size);
                            (*owner).relocate_data(NonNull::new_unchecked(to.add(PREFIX)));
                        }
                        to = to.add(size);
                    }
                    from = next;
                }
                block = (*block).next;
            }
            block = (*target).next;
            while !block.is_null() {
                let next = (*block).next;
                dealloc(
                    block.cast(),
                    Layout::from_size_align(BLOCK_BYTES, ALIGN).expect("fixed string block layout"),
                );
                block = next;
            }
            (*target).next = ptr::null_mut();
            (*target).next_free = to;
            moving.last = target;
        }
        if held.last.is_null() {
            SMALL.oldest.store(moving.first, Ordering::Relaxed);
        } else {
            (*held.last).next = moving.first;
            SMALL.oldest.store(held.first, Ordering::Relaxed);
        }
        SMALL.current.store(
            if moving.last.is_null() {
                held.last
            } else {
                moving.last
            },
            Ordering::Relaxed,
        );
    }
}

#[cfg(test)]
pub(super) fn census() -> (usize, usize, usize) {
    let mut blocks = 0;
    let mut bytes = 0;
    let mut live = 0;
    for (first, small) in [
        (SMALL.oldest.load(Ordering::Relaxed), true),
        (LARGE.load(Ordering::Relaxed), false),
        (PURE_SMALL.oldest.load(Ordering::Relaxed), true),
        (PURE_LARGE.load(Ordering::Relaxed), false),
    ] {
        let mut block = first;
        // SAFETY: tests hold the existing process runtime lock; this reads
        // real block extents, not allocation estimates or cached counts.
        unsafe {
            while !block.is_null() {
                blocks += 1;
                bytes += if small {
                    BLOCK_BYTES
                } else {
                    (*block).next_free.offset_from(block.cast()) as usize
                };
                let mut entry = block.cast::<u8>().add(HEADER);
                while entry < (*block).next_free {
                    let owner = entry.cast::<*mut SharedStringState>().read();
                    live += usize::from(!owner.is_null());
                    entry = entry.add(string_data_size(entry_bytes(entry, owner)));
                }
                block = (*block).next;
            }
        }
    }
    (blocks, bytes, live)
}
