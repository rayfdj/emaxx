//! The storage of the heap's aligned blocks, alloc.c's `lisp_align_malloc'
//! and the vector blocks' `xmalloc'.  Each block is carved, in address
//! order, from a large anonymous mapping kept for blocks of its size and
//! alignment, so an aligned block costs no allocator header and no
//! alignment slack: a page-sized block is one page, where an aligned
//! `malloc' of a page took two (the chunk header in the page before), and
//! the pages stay the kernel's zero pages until a block's first write.  A
//! released block goes on its class's free list for the next block of
//! the class, as `free' keeps a released chunk for the next `malloc'.
//!
//! Callers serialize on the Lisp runtime, as for every heap structure;
//! the arena's own lock makes it safe without that.

use std::sync::Mutex;

/// The address space mapped for a class at a time.
const CHUNK_BYTES: usize = 4 << 20;

struct Class {
    bytes: usize,
    align: usize,
    /// The next unused address of the class's current mapping, and its
    /// end.
    next: usize,
    end: usize,
    /// The released blocks: each one's first word is the next one's
    /// address, zero ending the list.
    free: usize,
}

static CLASSES: Mutex<Vec<Class>> = Mutex::new(Vec::new());

/// A block of BYTES aligned to ALIGN (a power of two), uninitialized;
/// null when the system has no memory for it.
pub(super) fn allocate_block(bytes: usize, align: usize) -> *mut u8 {
    debug_assert!(align.is_power_of_two() && bytes >= std::mem::size_of::<usize>());
    let mut classes = CLASSES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let index = match classes
        .iter()
        .position(|class| class.bytes == bytes && class.align == align)
    {
        Some(index) => index,
        None => {
            classes.push(Class {
                bytes,
                align,
                next: 0,
                end: 0,
                free: 0,
            });
            classes.len() - 1
        }
    };
    let class = &mut classes[index];
    if class.free != 0 {
        let block = class.free;
        // SAFETY: a released block of this class holds the free list's
        // next link in its first word.
        class.free = unsafe { (block as *const usize).read() };
        return block as *mut u8;
    }
    let mut start = class.next.next_multiple_of(align);
    if class.next == 0 || start + bytes > class.end {
        let size = CHUNK_BYTES.max(bytes + align);
        let Some(chunk) = map(size) else {
            return std::ptr::null_mut();
        };
        class.end = chunk + size;
        start = chunk.next_multiple_of(align);
    }
    class.next = start + bytes;
    start as *mut u8
}

/// Release a block `allocate_block' made of BYTES aligned to ALIGN.
///
/// # Safety
/// BLOCK is such a block, released once, and nothing uses it after.
pub(super) unsafe fn release_block(block: *mut u8, bytes: usize, align: usize) {
    let mut classes = CLASSES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let class = classes
        .iter_mut()
        .find(|class| class.bytes == bytes && class.align == align)
        .expect("a released block's class");
    // SAFETY: the caller's block, at least a word long and word aligned.
    unsafe { block.cast::<usize>().write(class.free) };
    class.free = block as usize;
}

#[cfg(unix)]
fn map(size: usize) -> Option<usize> {
    // SAFETY: a fresh private anonymous mapping at an address of the
    // kernel's choosing, never unmapped: its blocks are reused.
    let address = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    (address != libc::MAP_FAILED).then_some(address as usize)
}

#[cfg(not(unix))]
fn map(size: usize) -> Option<usize> {
    let layout = std::alloc::Layout::from_size_align(size, 4096).ok()?;
    // SAFETY: a non-zero layout, never freed: its blocks are reused.
    let address = unsafe { std::alloc::alloc(layout) };
    (!address.is_null()).then_some(address as usize)
}
