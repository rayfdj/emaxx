//! Host allocation with portable reallocation operations.
//!
//! GNU's sandbox permits malloc's ordinary mmap/brk/free operations but not
//! Linux mremap, which libc realloc can request for a growing mapped block.
//! Retain GlobalAlloc's default allocate/copy/deallocate reallocation instead
//! of System's libc-realloc override. Allocation contents, alignment, failure
//! ownership and zero initialization retain the standard Rust contract.

use std::alloc::{GlobalAlloc, Layout, System};

pub(crate) struct HostAllocator;

// SAFETY: all primitive operations delegate to System with the caller's
// unchanged pointers and layouts. GlobalAlloc supplies reallocation using
// these same operations and retains the original block on allocation failure.
unsafe impl GlobalAlloc for HostAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        unsafe { System.alloc_zeroed(layout) }
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
}
