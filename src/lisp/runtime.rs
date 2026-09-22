//! The process-wide Lisp execution boundary.
//!
//! GNU's thread.c global lock protects the allocator and the Lisp heap while
//! one Lisp thread runs. Our Lisp threads switch stacks on the owning OS
//! thread, so the corresponding lock belongs around an editor entry point,
//! not around each object operation. The public API only accepts and returns
//! owned host data; GC handles and the interpreter are crate-private.

use std::cell::Cell;
use std::sync::Mutex;

static RUNTIME: Mutex<()> = Mutex::new(());

thread_local! {
    static ENTERED: Cell<bool> = const { Cell::new(false) };
}

struct Entered;

impl Drop for Entered {
    fn drop(&mut self) {
        ENTERED.set(false);
    }
}

/// Execute a complete host call while this thread owns the Lisp heap.
/// Nested entry on the same thread retains the outer lock. Both the result
/// and every value captured by a public caller must be owned host data:
/// exposing a GC handle here would let it outlive its collection boundary.
pub(crate) fn with_runtime<R>(body: impl FnOnce() -> R) -> R {
    if ENTERED.get() {
        return body();
    }
    // Do not recover a poisoned heap after a panic escapes the runtime.
    let _lock = RUNTIME.lock().expect("Lisp runtime panicked while active");
    ENTERED.set(true);
    let _entered = Entered;
    body()
}

#[cfg(test)]
mod tests {
    use super::with_runtime;
    use std::sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn concurrent_entries_serialize_and_nested_entry_does_not_release_the_owner() {
        let start = Arc::new(Barrier::new(5));
        let active = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let start = Arc::clone(&start);
                let active = Arc::clone(&active);
                let completed = Arc::clone(&completed);
                scope.spawn(move || {
                    start.wait();
                    for _ in 0..100 {
                        with_runtime(|| {
                            assert_eq!(active.fetch_add(1, Ordering::SeqCst), 0);
                            with_runtime(|| {
                                assert_eq!(active.load(Ordering::SeqCst), 1);
                                std::thread::yield_now();
                            });
                            assert_eq!(active.fetch_sub(1, Ordering::SeqCst), 1);
                            completed.fetch_add(1, Ordering::SeqCst);
                        });
                    }
                });
            }
            start.wait();
        });
        assert_eq!(completed.load(Ordering::SeqCst), 400);
    }
}
