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
    /// The allocation frontier while no OS thread holds the runtime; the
    /// holder has it in its thread-local cells (see `alloc::Frontier').
    frontier: crate::lisp::alloc::Frontier,
    /// The obarray of heap operations that make no editor, made at its
    /// first use (see `with_host_symbols').
    host_symbols: Option<crate::lisp::types::symbols::HostSymbols>,
}

/// The state is locked only while the runtime is taken or given back, not
/// while it is held, so an uncontended acquire is a compare-and-swap with
/// no futex wait (which GNU's Linux seccomp policy does not permit).
struct GlobalLock {
    state: Mutex<RuntimeState>,
    released: Condvar,
}

static RUNTIME: GlobalLock = GlobalLock::new();

/// How long an OS thread waits for the runtime before it names the holder.
const PATIENCE: Duration = Duration::from_secs(60);

/// How this OS thread holds the runtime, if it does.  It holds it while
/// it has a tenure or any hold, and gives it back when the last of them
/// goes (`let_go'), as a recursive lock counts its holder's locks.
#[derive(Clone, Copy)]
struct Entered {
    /// It holds the runtime until it exits.
    tenure: bool,
    /// Its live `Runtime' handles and the scoped entries running on it.
    holds: u32,
    /// A scoped entry runs on it and has registered its stack base.
    boundary: bool,
}

impl Entered {
    const NONE: Self = Self {
        tenure: false,
        holds: 0,
        boundary: false,
    };

    fn held(self) -> bool {
        self.tenure || self.holds > 0
    }
}

thread_local! {
    /// Plain words with no destructor, so that `held' is a load and a test.
    static ENTERED: Cell<Entered> = const { Cell::new(Entered::NONE) };
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
            // A hold left (a handle forgotten) keeps the runtime.
            if entered.holds == 0 {
                release_thread_state();
                // The thread's own panic was caught before its
                // thread-locals went: a failed test does not poison the
                // runtime for the next.
                RUNTIME.release(false);
            }
        }
    }
}

/// One of this thread's holds goes; with the last, and no tenure, the
/// runtime goes back (thread.c:release_global_lock), the thread's editor
/// state released first.
fn let_go(poison: bool) {
    let entered = ENTERED.get();
    let holds = entered.holds - 1;
    ENTERED.set(Entered { holds, ..entered });
    if holds == 0 && !entered.tenure {
        release_thread_state();
        RUNTIME.release(poison);
    }
}

impl GlobalLock {
    const fn new() -> Self {
        Self {
            state: Mutex::new(RuntimeState {
                holder: None,
                waiters: 0,
                poisoned: false,
                frontier: crate::lisp::alloc::Frontier::EMPTY,
                host_symbols: None,
            }),
            released: Condvar::new(),
        }
    }

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
        // A holder that asks again would wait for itself: a release whose
        // reset reached a new block's path or the host obarray.
        debug_assert!(
            state
                .holder
                .as_ref()
                .is_none_or(|holder| holder.id() != me.id()),
            "the runtime's holder asks for the runtime again"
        );
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
        std::mem::replace(&mut state.frontier, crate::lisp::alloc::Frontier::EMPTY).install();
        state.poisoned
    }

    /// thread.c:release_global_lock.  The frontier goes back before the
    /// runtime does, so the next holder is given it.
    fn release(&self, poison: bool) {
        let mut state = self.state();
        let shared = !state
            .host_symbols
            .as_ref()
            .is_none_or(crate::lisp::types::symbols::HostSymbols::is_unshared);
        state.frontier = crate::lisp::alloc::Frontier::take();
        state.holder = None;
        state.poisoned |= poison;
        if state.waiters > 0 {
            self.released.notify_one();
        }
        drop(state);
        // After the release, so that the runtime is not left held.
        assert!(
            !shared,
            "a clone of the host obarray outlived its thread's hold of the Lisp runtime"
        );
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

/// The name of the OS thread that holds the runtime, if one does.
#[cfg(test)]
pub(crate) fn holder_name() -> Option<String> {
    RUNTIME
        .state()
        .holder
        .as_ref()
        .map(|holder| holder.name().unwrap_or("<unnamed>").to_owned())
}

/// thread.c:in_current_thread: whether this OS thread holds the runtime.
#[inline]
pub(crate) fn held() -> bool {
    ENTERED.get().held()
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

/// BODY on the host obarray, the runtime's, which this OS thread then
/// holds (taking it if it does not).  BODY runs under the runtime's state
/// lock, so it only reads or clones.
pub(crate) fn with_host_symbols<R>(
    body: impl FnOnce(&crate::lisp::types::symbols::HostSymbols) -> R,
) -> R {
    ensure_held();
    body(
        RUNTIME
            .state()
            .host_symbols
            .get_or_insert_with(crate::lisp::types::symbols::HostSymbols::new),
    )
}

/// An allocator's cold path: whether this OS thread has just taken the
/// runtime, a tenure, so that the allocation is made again from the
/// frontier it was given.
#[inline]
pub(crate) fn took_runtime() -> bool {
    if held() {
        return false;
    }
    take_tenure();
    true
}

#[cold]
#[inline(never)]
fn take_tenure() {
    // Its destructor gives the runtime back when the thread exits.  A
    // thread-local destructor that runs after it finds it gone.
    TENURE
        .try_with(|_| ())
        .expect("the Lisp runtime is taken after its thread's tenure ended");
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
/// thread it continues a test on, and for the process-test permit.  GNU's
/// collector scans every thread's stack (mark_threads) and ours scans the
/// collecting OS thread's, so the waiting thread's stacks are entered in
/// the heap-root registry while it waits.
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
    // alloc.c:mark_threads marks every thread's stack, and GNU's C globals
    // are the process's.  Ours mark the collecting OS thread's stacks and
    // thread-locals only: the lender's editor state, which it keeps, and
    // its stacks are entered in the heap-root registry for the wait.
    let mut roots = Vec::new();
    let mut keep = |value: &crate::lisp::types::Value| roots.push(*value);
    crate::lisp::primitives::profiler::mark_profiler(&mut keep);
    crate::lisp::primitives::atimer::mark_atimers(&mut keep);
    crate::lisp::primitives::mark_thread_local_roots(&mut keep);
    let roots = crate::lisp::alloc::RootedVec::from_vec(roots);
    let base = crate::lisp::eval::continuations::current_stack_base().map(|base| base as usize);
    // The runtime is taken again before the stacks leave the registry.
    let result = crate::lisp::alloc::with_stacks_lent(base, || {
        let entered = ENTERED.replace(Entered::NONE);
        RUNTIME.release(false);
        let _retake = Retake(entered);
        wait()
    });
    drop(roots);
    result
}

/// The process's Lisp runtime as the calling OS thread holds it: GNU's
/// global_lock, which emacs.c:main's init_threads takes for the main
/// thread before any Lisp.  Every public function that runs Lisp takes a
/// `&Runtime`, and a `Runtime` is neither `Send` nor `Sync`, so that a
/// call can only come from the thread that holds it.  `lock' waits for
/// another holder; on a thread that holds the runtime already it waits for
/// nothing and counts one more hold.  The thread holds the runtime until
/// its last handle goes and no call made through one is running.
///
/// An owned result of a call crosses to another thread:
///
/// ```
/// let runtime = emaxx::Runtime::lock();
/// let banner = emaxx::batch::version_banner(&runtime, None).expect("the banner");
/// std::thread::spawn(move || drop(banner)).join().expect("the thread");
/// ```
///
/// The runtime does not:
///
/// ```compile_fail
/// let runtime = emaxx::Runtime::lock();
/// let banner = emaxx::batch::version_banner(&runtime, None).expect("the banner");
/// std::thread::spawn(move || drop(banner)).join().expect("the thread");
/// std::thread::spawn(move || drop(runtime)).join().expect("the thread");
/// ```
///
/// Another thread may use what a call made:
///
/// ```
/// let runtime = emaxx::Runtime::lock();
/// let banner = emaxx::batch::version_banner(&runtime, None).expect("the banner");
/// std::thread::scope(|scope| {
///     scope.spawn(|| banner.len());
/// });
/// ```
///
/// but not call with this thread's runtime:
///
/// ```compile_fail
/// let runtime = emaxx::Runtime::lock();
/// let banner = emaxx::batch::version_banner(&runtime, None).expect("the banner");
/// std::thread::scope(|scope| {
///     scope.spawn(|| banner.len());
///     scope.spawn(|| emaxx::batch::version_banner(&runtime, None));
/// });
/// ```
pub struct Runtime {
    _holder_only: std::marker::PhantomData<*const ()>,
}

impl Runtime {
    /// Take the runtime for this OS thread, once no other one holds it
    /// (thread.c:acquire_global_lock); at once on a thread that holds it,
    /// as one more hold.  The runtime goes back when the thread's last
    /// hold does: every handle, and every call made through one, so no
    /// handle's drop gives it up while another or a call is alive.
    /// Uncontended, taking it makes no futex wait.
    pub fn lock() -> Self {
        let entered = ENTERED.get();
        if !entered.held() && RUNTIME.acquire(PATIENCE, &mut report_wait) {
            RUNTIME.release(false);
            // Do not recover a poisoned heap after a panic escapes the runtime.
            panic!("Lisp runtime panicked while active");
        }
        ENTERED.set(Entered {
            holds: entered.holds + 1,
            ..entered
        });
        Self {
            _holder_only: std::marker::PhantomData,
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        let_go(std::thread::panicking());
    }
}

/// The outermost scoped entry on an OS thread: one hold of the runtime
/// for its call, taken first if the thread holds none.
struct Scope;

impl Scope {
    fn enter() -> Self {
        let entered = ENTERED.get();
        let acquired = !entered.held();
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
            holds: entered.holds + 1,
            boundary: true,
            ..entered
        });
        Self
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        ENTERED.set(Entered {
            boundary: false,
            ..ENTERED.get()
        });
        let_go(std::thread::panicking());
    }
}

/// The editor state the runtime leaves in this OS thread's thread-locals
/// when it goes: GNU's C globals of the process, which a later holder
/// finds as a new process does.  Those that hold Lisp objects are marked
/// only by a collection on this thread (`mark_static_roots_into'), so one
/// left behind would name objects another thread's collection freed.
/// Each module resets its own (the anti-cheat gate
/// `thread_locals_are_released_with_the_runtime_or_per_os_thread' checks
/// that every thread-local is reset here or is the OS thread's own).  A
/// thread that lends the runtime keeps its state (`release_while').
fn release_thread_state() {
    crate::lisp::primitives::profiler::release_thread_state();
    crate::lisp::primitives::atimer::release_thread_state();
    crate::lisp::primitives::release_thread_state();
    crate::lisp::eval::terminal::release_thread_state();
    crate::lisp::eval::release_thread_state();
    crate::lisp::reader::release_thread_state();
    crate::lisp::types::release_thread_state();
    crate::tty::release_thread_state();
}

/// RESET the thread-local KEY of the runtime that leaves this OS thread,
/// unless the thread's exit has already destroyed it (its contents went
/// with it).
pub(crate) fn release_local<T: 'static>(
    key: &'static std::thread::LocalKey<T>,
    reset: impl FnOnce(&T),
) {
    if key.try_with(reset).is_err() {
        // Destroyed: nothing of it is left to release.
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
    use super::{
        GlobalLock, PATIENCE, RUNTIME, ensure_held, held, holder_name, release_while, with_runtime,
    };
    use crate::lisp::eval::continuations::current_stack_base;
    use crate::lisp::types::{Kind, Value};
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

    /// FIRST_USE, on a thread without the runtime while another thread
    /// holds it, does not return until the holder has exited.
    fn waits_for_the_holder(first_use: fn() -> bool) {
        let (held_tx, held_rx) = mpsc::channel();
        let (finish_tx, finish_rx) = mpsc::channel::<()>();
        let holder = std::thread::Builder::new()
            .name("frontier-holder-probe".into())
            .spawn(move || {
                let kept = Value::cons(Value::Integer(7), Value::Nil);
                assert!(held(), "the first allocation took the runtime");
                held_tx.send(()).expect("announce the tenure");
                finish_rx.recv().expect("wait to finish");
                assert_eq!(kept.car().expect("a cons"), Value::Integer(7));
            })
            .expect("spawn the holder");
        held_rx.recv().expect("the holder allocates");
        let (done_tx, done_rx) = mpsc::channel();
        let waiter = std::thread::spawn(move || {
            assert!(!held());
            let made = first_use();
            assert!(held(), "the first allocation took the runtime");
            done_tx.send(made).expect("announce the allocation");
        });
        assert!(
            done_rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "no allocation is made while another thread holds the runtime"
        );
        assert_eq!(holder_name().as_deref(), Some("frontier-holder-probe"));
        finish_tx.send(()).expect("let the holder finish");
        holder.join().expect("the holder exits");
        assert!(
            done_rx
                .recv_timeout(Duration::from_secs(60))
                .expect("the allocation is made once the holder has exited")
        );
        waiter.join().expect("the waiter exits");
    }

    #[test]
    fn a_thread_without_the_runtime_waits_at_its_first_allocation() {
        // Its frontier cells are empty, so its first cons reaches the cold
        // path, which takes the runtime before it reads any shared state.
        waits_for_the_holder(|| Value::cons(Value::Nil, Value::Nil).is_cons());
    }

    #[test]
    fn a_thread_without_the_runtime_waits_at_its_first_intern() {
        // A name no symbol has yet: interning it makes its string and its
        // symbol.
        waits_for_the_holder(|| Value::symbol("runtime-frontier-intern-probe").is_symbol());
    }

    /// The words of alloc.c's empty_unibyte_string, empty_multibyte_string
    /// and zero_vector as this OS thread finds them.
    fn empty_objects() -> [usize; 3] {
        let empty = |multibyte| {
            crate::lisp::types::StringObjectRef::from_storage_kind(
                &[],
                0,
                multibyte,
                crate::lisp::alloc::StringAllocation::Ordinary,
            )
            .map(|string| Value::StringObject(string).word())
            .expect("an empty string")
        };
        [empty(false), empty(true), Value::vector(Vec::new()).word()]
    }

    #[test]
    fn the_empty_strings_and_vector_are_the_runtimes() {
        // alloc.c's staticpro'd globals: one object each for the process,
        // which a thread without the runtime takes the runtime to reach.
        let first = std::thread::spawn(empty_objects)
            .join()
            .expect("the first thread");
        let second = std::thread::spawn(empty_objects)
            .join()
            .expect("the second thread");
        assert_eq!(first, second);
        waits_for_the_holder(|| Value::string("").is_string());
        waits_for_the_holder(|| Value::vector(Vec::new()).is_vectorlike());
    }

    /// This thread's newest cons: its cell, the cell its frontier gives
    /// next, the live conses and the mark epoch.
    fn cons_here() -> (usize, Option<usize>, usize, u32) {
        let cell = Value::cons(Value::Nil, Value::Nil);
        let Kind::Cons(cell) = cell.kind() else {
            unreachable!("a cons")
        };
        (
            cell.as_ptr() as usize,
            crate::lisp::alloc::next_cons_cell(),
            crate::lisp::alloc::live_conses(),
            crate::lisp::types::current_mark_epoch(),
        )
    }

    /// Whether the cons made at AFTER is the one the frontier at BEFORE gave
    /// next, as it must be when nothing else consed or collected between.
    fn continued(
        before: (usize, Option<usize>, usize, u32),
        after: (usize, Option<usize>, usize, u32),
    ) {
        if after.2 == before.2 + 1 && after.3 == before.3 && before.1.is_some() {
            assert_eq!(
                Some(after.0),
                before.1,
                "the next cell of the same frontier"
            );
        }
    }

    #[test]
    fn the_frontier_continues_across_os_threads() {
        // One heap, as GNU's is: a thread that exits returns its frontier
        // before another can take the runtime, and the next holder's first
        // cons is the cell the first thread's frontier would have given;
        // a thread that lends the runtime returns its frontier, its cells
        // left empty, and is given back the one its borrower returned.
        // (A thread whose first allocation takes the runtime makes a new
        // block for it; these take the runtime first.)
        let first = std::thread::spawn(cons_here)
            .join()
            .expect("the first holder");
        let second = std::thread::spawn(|| {
            ensure_held();
            let second = cons_here();
            let lent = std::thread::spawn(|| {
                ensure_held();
                cons_here()
            });
            let borrowed = release_while(|| {
                assert!(
                    crate::lisp::alloc::frontier_addresses().is_empty(),
                    "a lender keeps no frontier"
                );
                lent.join()
            })
            .expect("the borrower");
            continued(second, borrowed);
            continued(borrowed, cons_here());
            second
        })
        .join()
        .expect("the second holder");
        continued(first, second);
        // A scoped entry's thread keeps no frontier once it returns.
        std::thread::spawn(|| {
            with_runtime(|| {
                cons_here();
            });
            assert!(crate::lisp::alloc::frontier_addresses().is_empty());
        })
        .join()
        .expect("the scoped entry's thread");
    }

    #[test]
    fn mem_find_rejects_the_bump_and_free_list_words() {
        // A spawned thread's static thread-locals lie in its stack's
        // mapping, where the conservative scan can read the frontier's
        // words: none of them may be taken for an object.
        std::thread::spawn(|| {
            let mut interp = crate::lisp::eval::Interpreter::new();
            let garbage = "(let ((i 0)) (while (< i 2000) (cons i (number-to-string i)) \
                           (make-vector 3 i) (make-symbol \"probe\") (* 1.5 i) \
                           (setq i (1+ i))) i)";
            assert_eq!(fixnum(&mut interp, garbage), 2000);
            assert_eq!(fixnum(&mut interp, "(progn (garbage-collect) 0)"), 0);
            assert_eq!(fixnum(&mut interp, garbage), 2000);
            let words = crate::lisp::alloc::frontier_addresses();
            assert!(!words.is_empty());
            for word in words {
                // SAFETY: this thread holds the runtime; nothing collects.
                assert!(
                    unsafe { crate::lisp::alloc::mem_find(word) }.is_none(),
                    "{word:#x} is a frontier word, not an object"
                );
            }
        })
        .join()
        .expect("the frontier's thread completes");
    }

    #[test]
    fn four_test_threads_share_one_heap_in_turn() {
        // Each builds an editor, allocates 50,000 conses and strings,
        // collects and reads them back; they hold the runtime one at a
        // time, each from its first allocation until it exits.
        let start = Arc::new(Barrier::new(4));
        let workers: Vec<_> = (0..4)
            .map(|worker| {
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    let mut interp = crate::lisp::eval::Interpreter::new();
                    let program = format!(
                        "(let ((l nil) (i 0) (ok t))
                           (while (< i 50000)
                             (setq l (cons (cons i (number-to-string (+ i {worker}))) l))
                             (setq i (1+ i)))
                           (garbage-collect)
                           (while l
                             (setq i (1- i))
                             (if (and (= (car (car l)) i)
                                      (equal (cdr (car l)) (number-to-string (+ i {worker}))))
                                 nil
                               (setq ok nil))
                             (setq l (cdr l)))
                           (if (and ok (= i 0)) 1 0))"
                    );
                    fixnum(&mut interp, &program)
                })
            })
            .collect();
        for worker in workers {
            assert_eq!(worker.join().expect("a worker"), 1);
        }
    }

    #[test]
    fn a_host_symbol_clone_cannot_outlive_its_tenure() {
        // The host obarray's count changes only on the runtime's holder: a
        // release that finds a clone of it left refuses it, after giving
        // the runtime back.  On a lock and a table of the test's own.
        let refused = std::thread::spawn(|| {
            let lock = GlobalLock::new();
            let host = crate::lisp::types::symbols::HostSymbols::new();
            let _kept = host.share();
            lock.state().host_symbols = Some(host);
            lock.acquire(PATIENCE, &mut |_| ());
            lock.release(false);
        })
        .join()
        .expect_err("the release refuses a clone of the host obarray");
        let message = refused
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| refused.downcast_ref::<String>().map(String::as_str))
            .expect("the assertion's message");
        assert!(message.contains("outlived"), "{message}");
    }

    #[test]
    fn a_release_leaves_no_editor_state_on_the_thread() {
        // GNU's C globals are the process's: a scoped entry's editor state
        // goes with the runtime, as a later call finds a new process's.
        std::thread::spawn(|| {
            with_runtime(|| {
                let mut interp = crate::lisp::eval::Interpreter::new();
                assert_eq!(fixnum(&mut interp, "(progn (profiler-memory-start) 1)"), 1);
                assert!(!crate::lisp::primitives::profiler::profiler_memory_running_p().is_nil());
            });
            assert!(!held());
            assert!(crate::lisp::primitives::profiler::profiler_memory_running_p().is_nil());
        })
        .join()
        .expect("the scoped entry's thread");
    }

    #[test]
    fn a_lender_keeps_its_editor_state_and_its_objects() {
        // A thread that lends the runtime keeps its editor state, and the
        // objects only its stack and its thread-locals hold survive the
        // borrower's collections (alloc.c's mark_threads).
        std::thread::spawn(|| {
            let mut interp = crate::lisp::eval::Interpreter::new();
            assert_eq!(fixnum(&mut interp, "(progn (profiler-memory-start) 1)"), 1);
            let kept = std::hint::black_box(Value::cons(Value::Integer(41), Value::Nil));
            let Kind::Cons(cell) = kept.kind() else {
                unreachable!("a cons")
            };
            let address = cell.as_ptr() as usize;
            let borrower = std::thread::spawn(|| {
                let mut interp = crate::lisp::eval::Interpreter::new();
                fixnum(
                    &mut interp,
                    "(let ((i 0)) (while (< i 3) (make-list 5000 nil) (garbage-collect) (setq i (1+ i))) i)",
                )
            });
            assert_eq!(release_while(|| borrower.join()).expect("the borrower"), 3);
            // Still the cell's allocation: allocated where it was, and its
            // car its own.
            assert!(crate::lisp::alloc::cons_allocated_at(address));
            assert_eq!(kept.car().expect("a cons"), Value::Integer(41));
            assert!(!crate::lisp::primitives::profiler::profiler_memory_running_p().is_nil());
            std::hint::black_box(&kept);
        })
        .join()
        .expect("the lender's thread");
    }
}
