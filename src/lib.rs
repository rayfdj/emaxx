#![deny(clippy::unwrap_used)]
#![allow(clippy::result_large_err)]
#![recursion_limit = "512"]

// Cons cells are two small heap allocations each, so the interpreter's
// throughput is allocator-bound on list-heavy code; mimalloc's small-object
// paths run several times faster than glibc malloc there.
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod batch;
pub mod buffer;
pub mod compat;
pub mod lisp;
pub mod overlay;
pub mod tty;
pub mod perf;

#[cfg(test)]
mod anti_cheat;
#[cfg(test)]
pub(crate) mod test_support;
