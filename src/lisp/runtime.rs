//! The process-wide Lisp execution boundary.
//!
//! GNU's thread.c global lock protects the allocator and the Lisp heap while
//! one Lisp thread runs, and current_thread is the thread that holds it. Our
//! Lisp threads switch stacks on the owning OS thread, so the corresponding
//! lock belongs to that OS thread, not to each object operation: a scoped
//! entry holds it for one host call, and an OS thread that asks for it
//! outside one (`ensure_held') holds it from then until it exits, as
//! run_thread takes global_lock before its thread's first Lisp. The public
//! API only accepts and returns owned host data; GC handles and the
//! interpreter are crate-private.

use std::cell::Cell;
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::Thread;
use std::time::{Duration, Instant};

/// thread.c's global_lock with current_thread: the OS thread that holds
/// the runtime, kept where a thread waiting for it can name it.
struct RuntimeState {
    holder: Option<Thread>,
    waiters: usize,
    /// A panic left a scoped entry that held the runtime: the heap is not
    /// trusted after that.
    poisoned: bool,
}

/// The state is locked only while the runtime is taken or given back, not
/// while it is held, so an uncontended acquire is a compare-and-swap with
/// no futex wait (which GNU's Linux seccomp policy does not permit).
struct GlobalLock {
    state: Mutex<RuntimeState>,
    released: Condvar,
}

static RUNTIME: GlobalLock = GlobalLock {
    state: Mutex::new(RuntimeState {
        holder: None,
        waiters: 0,
        poisoned: false,
    }),
    released: Condvar::new(),
};

/// How long an OS thread waits for the runtime before it names the holder.
const PATIENCE: Duration = Duration::from_secs(60);

/// How this OS thread holds the runtime, if it does.
#[derive(Clone, Copy)]
struct Entered {
    /// It holds the runtime until it exits.
    tenure: bool,
    /// A scoped entry runs on it and has registered its stack base.
    boundary: bool,
}

thread_local! {
    /// Plain words with no destructor, so that `held' is a load and a test.
    static ENTERED: Cell<Entered> = const {
        Cell::new(Entered {
            tenure: false,
            boundary: false,
        })
    };
    /// Gives a tenure back when its OS thread exits, as run_thread's
    /// release_global_lock does once the thread's function has returned.
    static TENURE: Tenure = const { Tenure };
}

struct Tenure;

impl Drop for Tenure {
    fn drop(&mut self) {
        let entered = ENTERED.get();
        if entered.tenure {
            ENTERED.set(Entered {
                tenure: false,
                ..entered
            });
            // The thread's own panic was caught before its thread-locals
            // went: a failed test does not poison the runtime for the next.
            RUNTIME.release(false);
        }
    }
}

impl GlobalLock {
    fn state(&self) -> MutexGuard<'_, RuntimeState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// thread.c:acquire_global_lock: wait until no OS thread holds the
    /// runtime, then hold it.  A wait longer than PATIENCE is reported once
    /// through REPORT, naming the holder.  Answers whether the runtime is
    /// poisoned.
    fn acquire(&self, patience: Duration, report: &mut dyn FnMut(&str)) -> bool {
        let me = std::thread::current();
        let mut state = self.state();
        if state.holder.is_some() {
            state.waiters += 1;
            let deadline = Instant::now() + patience;
            let mut reported = false;
            while let Some(holder) = &state.holder {
                if reported {
                    state = self
                        .released
                        .wait(state)
                        .unwrap_or_else(PoisonError::into_inner);
                    continue;
                }
                let now = Instant::now();
                if now < deadline {
                    state = self
                        .released
                        .wait_timeout(state, deadline - now)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0;
                } else {
                    report(&format!(
                        "emaxx: {} has waited {} s for the Lisp runtime, which {} holds\n",
                        describe(&me),
                        patience.as_secs_f64(),
                        describe(holder)
                    ));
                    reported = true;
                }
            }
            state.waiters -= 1;
        }
        state.holder = Some(me);
        state.poisoned
    }

    /// thread.c:release_global_lock.
    fn release(&self, poison: bool) {
        let mut state = self.state();
        state.holder = None;
        state.poisoned |= poison;
        if state.waiters > 0 {
            self.released.notify_one();
        }
    }
}

fn describe(thread: &Thread) -> String {
    format!(
        "thread {} ({:?})",
        thread.name().unwrap_or("<unnamed>"),
        thread.id()
    )
}

fn report_wait(line: &str) {
    // C's stderr: libtest's capture would keep a stalled test's line until
    // the test ends.
    crate::lisp::primitives::batch_stdout::stderr_write(line.as_bytes());
}

/// thread.c:in_current_thread: whether this OS thread holds the runtime.
#[inline]
pub(crate) fn held() -> bool {
    let entered = ENTERED.get();
    entered.tenure || entered.boundary
}

/// Hold the runtime on this OS thread: at once if it already does, else
/// once no other OS thread holds it, and then until this one exits, as
/// run_thread acquires global_lock before its first Lisp.
#[inline]
pub(crate) fn ensure_held() {
    if !held() {
        take_tenure();
    }
}

#[cold]
#[inline(never)]
fn take_tenure() {
    // Its destructor gives the runtime back when the thread exits.
    TENURE.with(|_| ());
    // A tenure does not refuse a runtime a scoped entry's panic poisoned:
    // production never takes one, and one failed test must not fail every
    // test after it.
    RUNTIME.acquire(PATIENCE, &mut report_wait);
    ENTERED.set(Entered {
        tenure: true,
        ..ENTERED.get()
    });
}

/// Give the runtime up while WAIT waits for another OS thread, and hold it
/// again after, as thread.c's thread_join_callback waits on the joined
/// thread's condition variable (sys_cond_wait gives global_lock up) and
/// then post_acquire_global_lock.  The test harness waits so for the
/// thread it continues a test on.  GNU's collector scans every thread's
/// stack (mark_threads) and ours scans only its own OS thread's, so the
/// waiting thread must not count on its stack alone to keep an object.
#[cfg(test)]
pub(crate) fn release_while<R>(wait: impl FnOnce() -> R) -> R {
    struct Retake(Entered);
    impl Drop for Retake {
        fn drop(&mut self) {
            RUNTIME.acquire(PATIENCE, &mut report_wait);
            ENTERED.set(self.0);
        }
    }
    if !held() {
        return wait();
    }
    let entered = ENTERED.replace(Entered {
        tenure: false,
        boundary: false,
    });
    RUNTIME.release(false);
    let _retake = Retake(entered);
    wait()
}

/// The outermost scoped entry on an OS thread. It holds the runtime for
/// its call unless the thread's tenure already does.
struct Scope {
    acquired: bool,
}

impl Scope {
    fn enter() -> Self {
        let entered = ENTERED.get();
        let acquired = !entered.tenure;
        let poisoned = if acquired {
            RUNTIME.acquire(PATIENCE, &mut report_wait)
        } else {
            RUNTIME.state().poisoned
        };
        if poisoned {
            if acquired {
                RUNTIME.release(false);
            }
            // Do not recover a poisoned heap after a panic escapes the runtime.
            panic!("Lisp runtime panicked while active");
        }
        ENTERED.set(Entered {
            boundary: true,
            ..entered
        });
        Self { acquired }
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        ENTERED.set(Entered {
            boundary: false,
            ..ENTERED.get()
        });
        if self.acquired {
            RUNTIME.release(std::thread::panicking());
        }
    }
}

// Keep Lisp locals in a frame below the registered boundary even when the
// compiler inlines the closure body. The host-entry frame contains host data.
#[inline(never)]
fn invoke_owned_body<R>(body: impl FnOnce() -> R) -> R {
    body()
}

/// Execute a complete host call while this thread owns the Lisp heap.
/// Nested entry on the same thread retains the outer lock. Both the result
/// and every value captured by a public caller must be owned host data:
/// exposing a GC handle here would let it outlive its collection boundary,
/// and the result cannot be one, since no heap handle is Send.
pub(crate) fn with_runtime<R: Send>(body: impl FnOnce() -> R) -> R {
    if ENTERED.get().boundary {
        return body();
    }
    let _scope = Scope::enter();
    // emacs.c:main records a local as stack_bottom. This host entry is the
    // corresponding boundary: no public caller holds or returns Lisp GC
    // handles, and nested entries retain the outer boundary above. Register
    // it before an alternate stack records its waiting caller. In particular,
    // GNU's Linux seccomp filters do not permit pthread_getattr_np's affinity
    // query; no OS-stack query is needed for a range this frame already owns.
    // An entry inside a tenure registers a boundary of its own all the same.
    let stack_bottom = 0usize;
    let _stack = super::eval::continuations::StackBaseGuard::enter(
        std::ptr::from_ref(&stack_bottom) as usize,
    );
    let result = invoke_owned_body(body);
    std::hint::black_box(&stack_bottom);
    result
}

#[cfg(test)]
mod tests {
    use super::{RUNTIME, ensure_held, held, release_while, with_runtime};
    use crate::lisp::eval::continuations::current_stack_base;
    use std::sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };
    use std::time::Duration;

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

    #[test]
    fn tenure_holds_the_runtime_until_the_thread_exits() {
        // run_thread holds global_lock from its first Lisp until its
        // function returns: another OS thread's scoped entry waits for the
        // whole of the holder's life, not for one of its calls.
        let (held_tx, held_rx) = mpsc::channel();
        let (finish_tx, finish_rx) = mpsc::channel::<()>();
        let holder = std::thread::spawn(move || {
            assert!(!held());
            ensure_held();
            assert!(held());
            // A scoped entry inside the tenure gives nothing back.
            with_runtime(|| assert!(held()));
            assert!(held());
            held_tx.send(()).expect("announce the tenure");
            finish_rx.recv().expect("wait to finish");
        });
        held_rx.recv().expect("the holder takes its tenure");
        let (entered_tx, entered_rx) = mpsc::channel();
        let waiter = std::thread::spawn(move || {
            with_runtime(|| entered_tx.send(()).expect("announce the entry"));
            assert!(!held(), "the scoped entry gave the runtime back");
        });
        assert!(
            entered_rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "no scoped entry runs while another thread's tenure lives"
        );
        finish_tx.send(()).expect("let the holder finish");
        holder.join().expect("the holder exits");
        entered_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("the scoped entry runs once the holder has exited");
        waiter.join().expect("the waiter exits");
    }

    #[test]
    fn scoped_entry_under_tenure_registers_its_own_stack_base() {
        // The tenure registers no stack base of its own; a scoped entry in
        // it registers and pops one, and a nested entry keeps the outer.
        std::thread::spawn(|| {
            ensure_held();
            assert!(current_stack_base().is_none());
            with_runtime(|| {
                let base = current_stack_base();
                assert!(base.is_some());
                with_runtime(|| assert_eq!(current_stack_base(), base));
                assert_eq!(current_stack_base(), base);
            });
            assert!(current_stack_base().is_none());
            assert!(held(), "the tenure outlives the scoped entry");
        })
        .join()
        .expect("the tenure's thread completes");
    }

    #[test]
    fn waiting_names_the_holder() {
        // A thread that waits past its patience names the holder, once,
        // and still takes the runtime once the holder has exited.
        let (held_tx, held_rx) = mpsc::channel();
        let (finish_tx, finish_rx) = mpsc::channel::<()>();
        let holder = std::thread::Builder::new()
            .name("runtime-holder-probe".into())
            .spawn(move || {
                ensure_held();
                held_tx.send(()).expect("announce the tenure");
                finish_rx.recv().expect("wait to finish");
            })
            .expect("spawn the holder");
        held_rx.recv().expect("the holder takes its tenure");
        let (line_tx, line_rx) = mpsc::channel();
        let waiter = std::thread::Builder::new()
            .name("runtime-waiter-probe".into())
            .spawn(move || {
                let mut lines = Vec::new();
                let poisoned = RUNTIME.acquire(Duration::from_millis(50), &mut |line| {
                    lines.push(line.to_owned());
                    line_tx.send(line.to_owned()).expect("pass the report on");
                });
                RUNTIME.release(false);
                assert!(!poisoned);
                lines
            })
            .expect("spawn the waiter");
        let line = line_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("the waiter reports its wait");
        assert!(line.contains("thread runtime-holder-probe"), "{line}");
        assert!(line.contains("thread runtime-waiter-probe"), "{line}");
        assert!(line.ends_with('\n'), "{line}");
        finish_tx.send(()).expect("let the holder finish");
        holder.join().expect("the holder exits");
        let lines = waiter.join().expect("the waiter takes the runtime");
        assert_eq!(lines, [line], "the wait is reported once");
    }

    #[test]
    fn a_tenure_whose_thread_panicked_leaves_the_runtime_usable() {
        // The tenure goes at thread exit, after the thread's panic was
        // caught: it poisons nothing for the next holder.
        let outcome = std::thread::spawn(|| {
            ensure_held();
            panic!("a failing test that held the runtime");
        })
        .join();
        assert!(outcome.is_err());
        std::thread::spawn(|| with_runtime(|| assert!(held())))
            .join()
            .expect("a scoped entry still enters");
    }

    /// Evaluate SOURCE in INTERP and answer the fixnum it returns.
    fn fixnum(interp: &mut crate::lisp::eval::Interpreter, source: &str) -> i64 {
        let value =
            crate::test_support::eval_lisp(interp, &mut crate::lisp::types::Env::new(), source)
                .unwrap_or_else(|error| panic!("{source}: {error:?}"));
        match value.kind() {
            crate::lisp::types::Kind::Integer(number) => number,
            _ => panic!("{source}: not a fixnum"),
        }
    }

    #[test]
    fn editors_on_two_os_threads_take_the_runtime_in_turn() {
        // An OS thread that builds an editor holds the runtime until it
        // exits, so a second thread's editor is not built while the first
        // lives; each evaluates and collects with the heap to itself.
        let (built_tx, built_rx) = mpsc::channel();
        let (finish_tx, finish_rx) = mpsc::channel::<()>();
        let first = std::thread::spawn(move || {
            let mut interp = crate::lisp::eval::Interpreter::new();
            assert!(held());
            let before = fixnum(
                &mut interp,
                "(progn (setq kept (make-list 1000 'first)) (garbage-collect) (length kept))",
            );
            built_tx.send(()).expect("announce the editor");
            finish_rx.recv().expect("wait to finish");
            let after = fixnum(
                &mut interp,
                "(progn (garbage-collect) (+ (length kept) (length (make-list 10 nil))))",
            );
            (before, after)
        });
        built_rx.recv().expect("the first editor is built");
        let (second_tx, second_rx) = mpsc::channel();
        let second = std::thread::spawn(move || {
            let mut interp = crate::lisp::eval::Interpreter::new();
            second_tx.send(()).expect("announce the second editor");
            fixnum(
                &mut interp,
                "(progn (setq kept (make-list 2000 'second)) (garbage-collect) (length kept))",
            )
        });
        assert!(
            second_rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "the second editor waits while the first one's thread lives"
        );
        finish_tx.send(()).expect("let the first editor finish");
        assert_eq!(
            first.join().expect("the first editor's thread"),
            (1000, 1010)
        );
        second_rx
            .recv_timeout(Duration::from_secs(60))
            .expect("the second editor is built once the first thread has exited");
        assert_eq!(second.join().expect("the second editor's thread"), 2000);
    }

    #[test]
    fn a_waiting_holder_lends_the_runtime_to_the_thread_it_waits_for() {
        // thread.c's thread-join gives global_lock up while it waits: the
        // waiting thread's tenure, or its scoped entry, is lent to the
        // thread it joins and taken back after.
        std::thread::spawn(|| {
            ensure_held();
            let lent = std::thread::spawn(|| with_runtime(held));
            assert!(release_while(|| lent.join()).expect("the joined thread enters"));
            assert!(held(), "the tenure is taken back");
            with_runtime(|| {
                let base = current_stack_base();
                let lent = std::thread::spawn(|| {
                    ensure_held();
                    held()
                });
                assert!(release_while(|| lent.join()).expect("the joined thread holds it"));
                assert!(held());
                assert_eq!(current_stack_base(), base, "the boundary stays as it was");
            });
            assert!(held());
        })
        .join()
        .expect("the lending thread completes");
        // Without a hold there is nothing to lend.
        assert!(!held());
        assert_eq!(release_while(|| 7), 7);
        assert!(!held());
    }
}
