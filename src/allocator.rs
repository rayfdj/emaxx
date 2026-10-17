//! Host allocation with portable reallocation operations.
//!
//! GNU's sandbox permits malloc's ordinary mmap/brk/free operations but not
//! Linux mremap, which libc realloc can request for a growing mapped block.
//! Retain GlobalAlloc's default allocate/copy/deallocate reallocation instead
//! of System's libc-realloc override. Allocation contents, alignment, failure
//! ownership and zero initialization retain the standard Rust contract.
//!
//! A small block (at most `SMALL_LIMIT` bytes, aligned to at most 16) is
//! taken from libc in a size class of 16 bytes, and a freed one is kept on
//! its thread's list for the class, up to `DEPTH` blocks, for that
//! thread's next request in the class: a pop or a push of a few
//! instructions where libc's malloc and free take about 70 and 90 (its own
//! per-thread cache holds seven blocks a size and its free consults the
//! arena past that). The lists go back to libc when their thread exits.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::UnsafeCell;

pub(crate) struct HostAllocator;

/// The bytes of a size class.
const CLASS_BYTES: usize = 16;
/// The largest block kept on a thread's lists.
const SMALL_LIMIT: usize = 256;
const CLASSES: usize = SMALL_LIMIT / CLASS_BYTES;
/// The freed blocks a thread keeps for each class.
const DEPTH: u32 = 64;

/// A thread's free lists: each block's first word links the next.
struct SmallBlocks {
    heads: [*mut u8; CLASSES],
    /// The blocks each list can still take: zero for every class until the
    /// exit flush is arranged and again once it ran, so a free tests this
    /// count alone before keeping its block.
    room: [u32; CLASSES],
    state: ListState,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ListState {
    /// No block kept yet, and the exit flush not arranged.
    Unused,
    /// The exit flush is arranged; blocks may be kept.
    Keeping,
    /// The thread is exiting (or could not arrange the flush): every block
    /// goes to libc.
    Closed,
}

thread_local! {
    // A constant initializer and no destructor: the lists are a plain
    // thread-local static, valid for the thread's whole life, read with
    // no initialization test.
    static SMALL_BLOCKS: UnsafeCell<SmallBlocks> = const {
        UnsafeCell::new(SmallBlocks {
            heads: [std::ptr::null_mut(); CLASSES],
            room: [0; CLASSES],
            state: ListState::Unused,
        })
    };
    // Its destructor gives the lists back to libc when the thread exits.
    static FLUSH_AT_EXIT: FlushAtExit = const { FlushAtExit };
}

struct FlushAtExit;

impl Drop for FlushAtExit {
    fn drop(&mut self) {
        let blocks = HostAllocator::small_blocks();
        // SAFETY: the lists are this thread's own. Each block on a class's
        // list came from libc with that class's layout and is linked by its
        // first word; closed lists have no room and keep no block again.
        unsafe {
            (*blocks).state = ListState::Closed;
            for class in 0..CLASSES {
                let mut block = (*blocks).heads[class];
                (*blocks).heads[class] = std::ptr::null_mut();
                (*blocks).room[class] = 0;
                while !block.is_null() {
                    let next = block.cast::<*mut u8>().read();
                    System.dealloc(block, class_layout(class));
                    block = next;
                }
            }
        }
    }
}

/// The size class of LAYOUT, if its block is small enough to keep.
#[inline(always)]
fn small_class(layout: Layout) -> Option<usize> {
    // A zero size wraps around and is left to libc.
    let last = layout.size().wrapping_sub(1);
    (last < SMALL_LIMIT && layout.align() <= CLASS_BYTES).then_some(last / CLASS_BYTES)
}

/// The layout every block of CLASS is taken from libc with.
#[inline(always)]
fn class_layout(class: usize) -> Layout {
    // SAFETY: a nonzero multiple of 16 bytes, aligned to 16.
    unsafe { Layout::from_size_align_unchecked((class + 1) * CLASS_BYTES, CLASS_BYTES) }
}

impl HostAllocator {
    /// This thread's lists. They are a constant thread-local static without
    /// a destructor, so the pointer is valid for the thread's whole life;
    /// it is used only through place expressions, never a lasting
    /// reference, since registering the exit flush can allocate.
    #[inline(always)]
    fn small_blocks() -> *mut SmallBlocks {
        SMALL_BLOCKS.with(UnsafeCell::get)
    }

    /// A block of CLASS from this thread's list, or null when it is empty.
    #[inline(always)]
    fn take_small(class: usize) -> *mut u8 {
        let blocks = Self::small_blocks();
        // SAFETY: the lists are this thread's own; a listed block is
        // linked by its first word.
        unsafe {
            let block = (*blocks).heads[class];
            if !block.is_null() {
                (*blocks).heads[class] = block.cast::<*mut u8>().read();
                (*blocks).room[class] += 1;
            }
            block
        }
    }

    /// Free BLOCK of CLASS: kept on this thread's list while it has room,
    /// else given to libc.
    #[inline(always)]
    fn free_small(block: *mut u8, class: usize) {
        let blocks = Self::small_blocks();
        // SAFETY: as `take_small'; BLOCK is a freed block of CLASS, at
        // least 16 bytes, which its first word now links.
        unsafe {
            if (*blocks).room[class] == 0 {
                return Self::free_small_without_room(block, class);
            }
            block.cast::<*mut u8>().write((*blocks).heads[class]);
            (*blocks).heads[class] = block;
            (*blocks).room[class] -= 1;
        }
    }

    /// `free_small' when the list has no room: full, closed, or not open
    /// yet. The first free on a thread arranges the exit flush, then opens
    /// every list.
    #[cold]
    #[inline(never)]
    fn free_small_without_room(block: *mut u8, class: usize) {
        let blocks = Self::small_blocks();
        // SAFETY: as `free_small'. A block freed while the flush registers
        // (which can allocate) finds the lists closed and goes to libc.
        unsafe {
            if (*blocks).state == ListState::Unused {
                (*blocks).state = ListState::Closed;
                if FLUSH_AT_EXIT.try_with(|_| ()).is_ok() {
                    (*blocks).state = ListState::Keeping;
                    (*blocks).room = [DEPTH; CLASSES];
                    return Self::free_small(block, class);
                }
            }
            System.dealloc(block, class_layout(class));
        }
    }
}

// SAFETY: a large or overaligned block delegates to System with the
// caller's unchanged pointer and layout. A small one is always taken from
// System with its class's layout, which covers every layout of the class
// (at least as large, alignment 16), and is returned to System with that
// layout or kept on a list for a later request in the same class; a kept
// block is owned by no one else. Reallocation is GlobalAlloc's own,
// through these operations, except that a block whose new size stays in
// its class is returned as it is.
//
// Each operation is a function of its own, called as libc's malloc and
// free are: inlined, it went into every cold allocating path of a hot
// function (a fixnum `1+' kept two more registers for its bignum case).
unsafe impl GlobalAlloc for HostAllocator {
    #[inline(never)]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if let Some(class) = small_class(layout) {
            let block = Self::take_small(class);
            if !block.is_null() {
                return block;
            }
            return unsafe { System.alloc(class_layout(class)) };
        }
        unsafe { System.alloc(layout) }
    }

    #[inline(never)]
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        if let Some(class) = small_class(layout) {
            return Self::free_small(pointer, class);
        }
        unsafe { System.dealloc(pointer, layout) }
    }

    #[inline(never)]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if let Some(class) = small_class(layout) {
            let block = Self::take_small(class);
            if !block.is_null() {
                // SAFETY: a block of at least LAYOUT's size.
                unsafe { std::ptr::write_bytes(block, 0, layout.size()) };
                return block;
            }
            return unsafe { System.alloc_zeroed(class_layout(class)) };
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    #[inline(never)]
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller's contract makes NEW_SIZE, with LAYOUT's
        // alignment, a valid layout.
        let new_layout = unsafe { Layout::from_size_align_unchecked(new_size, layout.align()) };
        if let Some(class) = small_class(layout)
            && small_class(new_layout) == Some(class)
        {
            return pointer;
        }
        let new_pointer = unsafe { self.alloc(new_layout) };
        if !new_pointer.is_null() {
            // SAFETY: both blocks hold at least the smaller size, and they
            // are distinct allocations.
            unsafe {
                std::ptr::copy_nonoverlapping(pointer, new_pointer, layout.size().min(new_size));
                self.dealloc(pointer, layout);
            }
        }
        new_pointer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_allocator_preserves_bytes_and_alignment_when_growing_and_shrinking() {
        for alignment in [1, 8, 64, 4096] {
            let initial = Layout::from_size_align(256 * 1024, alignment).expect("initial layout");
            // SAFETY: each non-null allocation is accessed only within its
            // live layout, then ownership transfers through realloc or free.
            unsafe {
                let mut pointer = HostAllocator.alloc_zeroed(initial);
                assert!(!pointer.is_null());
                assert_eq!(pointer as usize % alignment, 0);
                let bytes = std::slice::from_raw_parts_mut(pointer, initial.size());
                assert!(bytes.iter().all(|byte| *byte == 0));
                for (index, byte) in bytes.iter_mut().enumerate() {
                    *byte = (index % 251) as u8;
                }
                let grown = Layout::from_size_align(1024 * 1024, alignment).expect("grown layout");
                pointer = HostAllocator.realloc(pointer, initial, grown.size());
                assert!(!pointer.is_null());
                assert_eq!(pointer as usize % alignment, 0);
                assert!(
                    std::slice::from_raw_parts(pointer, initial.size())
                        .iter()
                        .enumerate()
                        .all(|(index, byte)| *byte == (index % 251) as u8)
                );
                let shrunk = Layout::from_size_align(128 * 1024, alignment).expect("shrunk layout");
                pointer = HostAllocator.realloc(pointer, grown, shrunk.size());
                assert!(!pointer.is_null());
                assert_eq!(pointer as usize % alignment, 0);
                assert!(
                    std::slice::from_raw_parts(pointer, shrunk.size())
                        .iter()
                        .enumerate()
                        .all(|(index, byte)| *byte == (index % 251) as u8)
                );
                HostAllocator.dealloc(pointer, shrunk);
            }
        }
    }

    #[test]
    fn small_blocks_are_reused_aligned_and_zeroed_on_request() {
        // SAFETY: each block is accessed only within its layout while it is
        // allocated, and freed once with the layout it was taken with.
        unsafe {
            for size in [1, 8, 15, 16, 17, 31, 64, 100, 255, 256] {
                let layout = Layout::from_size_align(size, 8).expect("layout");
                let blocks: Vec<*mut u8> = (0..(DEPTH as usize + 8))
                    .map(|index| {
                        let block = HostAllocator.alloc(layout);
                        assert!(!block.is_null());
                        assert_eq!(block as usize % CLASS_BYTES, 0);
                        std::ptr::write_bytes(block, (index % 251) as u8 | 1, size);
                        block
                    })
                    .collect();
                for block in &blocks {
                    HostAllocator.dealloc(*block, layout);
                }
                // Blocks past the list's depth went back to libc; the kept
                // ones come back zeroed when zeroing is asked for.
                let zeroed: Vec<*mut u8> = (0..(DEPTH as usize + 8))
                    .map(|_| {
                        let block = HostAllocator.alloc_zeroed(layout);
                        assert!(!block.is_null());
                        assert!(
                            std::slice::from_raw_parts(block, size)
                                .iter()
                                .all(|byte| *byte == 0)
                        );
                        block
                    })
                    .collect();
                for block in zeroed {
                    HostAllocator.dealloc(block, layout);
                }
            }
        }
    }

    #[test]
    fn small_blocks_keep_their_bytes_through_reallocation() {
        // SAFETY: as above; each reallocation transfers the block.
        unsafe {
            let layout = Layout::from_size_align(20, 4).expect("layout");
            let block = HostAllocator.alloc(layout);
            for index in 0..20 {
                *block.add(index) = index as u8;
            }
            // 20 and 32 bytes are one class: the block stays where it is.
            let same = HostAllocator.realloc(block, layout, 32);
            assert_eq!(same, block);
            let layout = Layout::from_size_align(32, 4).expect("layout");
            for index in 20..32 {
                *same.add(index) = index as u8;
            }
            let grown = HostAllocator.realloc(same, layout, 4096);
            assert!(!grown.is_null());
            let layout = Layout::from_size_align(4096, 4).expect("layout");
            assert!((0..32).all(|index| *grown.add(index) == index as u8));
            let shrunk = HostAllocator.realloc(grown, layout, 3);
            assert!(!shrunk.is_null());
            assert!((0..3).all(|index| *shrunk.add(index) == index as u8));
            HostAllocator.dealloc(shrunk, Layout::from_size_align(3, 4).expect("layout"));
        }
    }

    #[test]
    fn small_blocks_freed_by_another_thread_return_to_libc_when_it_exits() {
        struct Blocks(Vec<usize>);
        let layout = Layout::from_size_align(48, 8).expect("layout");
        for _ in 0..4 {
            // SAFETY: the blocks are written within their layout, then
            // freed exactly once by the receiving thread.
            let blocks = Blocks(
                (0..200)
                    .map(|index| unsafe {
                        let block = HostAllocator.alloc(layout);
                        assert!(!block.is_null());
                        std::ptr::write_bytes(block, index as u8, layout.size());
                        block as usize
                    })
                    .collect(),
            );
            std::thread::spawn(move || {
                let blocks = blocks;
                for (index, block) in blocks.0.iter().enumerate() {
                    let block = *block as *mut u8;
                    // SAFETY: a live block of LAYOUT, freed once here.
                    unsafe {
                        assert!(
                            std::slice::from_raw_parts(block, layout.size())
                                .iter()
                                .all(|byte| *byte == index as u8)
                        );
                        HostAllocator.dealloc(block, layout);
                    }
                }
                // This thread's next small requests come from its list.
                let again: Vec<Box<[u8; 40]>> = (0..100).map(|_| Box::new([7; 40])).collect();
                assert!(
                    again
                        .iter()
                        .all(|block| block.iter().all(|byte| *byte == 7))
                );
            })
            .join()
            .expect("thread");
        }
    }
}
