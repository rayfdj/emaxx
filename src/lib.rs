#![deny(clippy::unwrap_used)]
#![allow(clippy::result_large_err)]
#![recursion_limit = "512"]

#[cfg(any(target_os = "linux", test))]
mod allocator;

// GNU's inherited sandbox excludes mimalloc's startup sysinfo and libc's
// optional mremap realloc optimization. Use the host allocation backend with
// Rust's allocate/copy/free reallocation contract on Linux.
#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL_ALLOCATOR: allocator::HostAllocator = allocator::HostAllocator;

#[cfg(not(target_os = "linux"))]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// mimalloc gives freed pages back to the host `purge_delay' milliseconds
/// after they empty (10 ms in mimalloc 2, a second in mimalloc 3), so a
/// Lisp workload that frees and refills its working set took a page
/// fault and a zeroed page for memory it had just released: 303 purges
/// of 196 MB in a 3 s `macroexpand-all' loop, a quarter of its time in
/// the kernel, and 0.4 s of a `setq' loop's 8.  glibc's malloc, under
/// GNU, keeps its heap.  Purging is off here (-1): memory a Lisp program
/// frees stays with the process, as most of it does under GNU.  The
/// option is set only when it reads one of its documented defaults,
/// which also guards the numeric identifier (`mi_option_purge_delay' is
/// enumerator 15 of `mi_option_t' in the bundled mimalloc 2 and 3
/// headers); an environment setting (MIMALLOC_PURGE_DELAY) is left as
/// the user set it.
#[cfg(not(target_os = "linux"))]
pub fn tune_allocator() {
    use std::os::raw::{c_int, c_long};
    unsafe extern "C" {
        fn mi_option_get(option: c_int) -> c_long;
        fn mi_option_set(option: c_int, value: c_long);
    }
    const MI_OPTION_PURGE_DELAY: c_int = 15;
    const MIMALLOC_DEFAULT_PURGE_DELAYS: [c_long; 2] = [10, 1000];
    if std::env::var_os("MIMALLOC_PURGE_DELAY").is_some() {
        return;
    }
    // SAFETY: reads and writes of the allocator's option table, which
    // mimalloc exports for exactly this.
    unsafe {
        if MIMALLOC_DEFAULT_PURGE_DELAYS.contains(&mi_option_get(MI_OPTION_PURGE_DELAY)) {
            mi_option_set(MI_OPTION_PURGE_DELAY, -1);
        }
    }
}

/// The Linux runtime uses the host allocator without custom tuning.
#[cfg(target_os = "linux")]
pub fn tune_allocator() {}

pub mod batch;
pub(crate) mod buffer;
pub mod compat;
pub(crate) mod file_system;
pub mod lisp;
pub(crate) mod overlay;
pub mod perf;
mod startup;
pub mod tty;

pub mod anti_cheat;
#[cfg(test)]
pub(crate) mod test_support;
