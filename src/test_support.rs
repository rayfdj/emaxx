use std::cell::RefCell;
use std::sync::{Condvar, Mutex, OnceLock};

const MAX_CONCURRENT_HOST_HEAVY_TESTS: usize = 2;

#[derive(Default)]
struct HostTestState {
    shared_active: usize,
    exclusive_active: bool,
    exclusive_waiters: usize,
}

struct HostTestGate {
    state: Mutex<HostTestState>,
    available: Condvar,
}

impl HostTestGate {
    const fn new() -> Self {
        Self {
            state: Mutex::new(HostTestState {
                shared_active: 0,
                exclusive_active: false,
                exclusive_waiters: 0,
            }),
            available: Condvar::new(),
        }
    }

    fn acquire_shared(&'static self) -> HostTestPermit {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while state.exclusive_active
            || state.exclusive_waiters > 0
            || state.shared_active >= MAX_CONCURRENT_HOST_HEAVY_TESTS
        {
            state = self
                .available
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        state.shared_active += 1;
        HostTestPermit {
            gate: self,
            kind: HostTestPermitKind::Shared,
        }
    }

    fn acquire_exclusive(&'static self) -> HostTestPermit {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.exclusive_waiters += 1;
        while state.exclusive_active || state.shared_active > 0 {
            state = self
                .available
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        state.exclusive_waiters -= 1;
        state.exclusive_active = true;
        HostTestPermit {
            gate: self,
            kind: HostTestPermitKind::Exclusive,
        }
    }
}

enum HostTestPermitKind {
    Shared,
    Exclusive,
}

/// Permit for tests that deliberately share scarce or process-wide host
/// resources. Dropping it releases the next marked test; unmarked Rust tests
/// continue to use the test runner's ordinary parallelism.
pub(crate) struct HostTestPermit {
    gate: &'static HostTestGate,
    kind: HostTestPermitKind,
}

impl Drop for HostTestPermit {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match self.kind {
            HostTestPermitKind::Shared => state.shared_active -= 1,
            HostTestPermitKind::Exclusive => state.exclusive_active = false,
        }
        self.gate.available.notify_all();
    }
}

fn host_test_gate() -> &'static HostTestGate {
    static GATE: OnceLock<HostTestGate> = OnceLock::new();
    GATE.get_or_init(HostTestGate::new)
}

fn process_test_gate() -> &'static HostTestGate {
    static GATE: OnceLock<HostTestGate> = OnceLock::new();
    GATE.get_or_init(HostTestGate::new)
}

thread_local! {
    /// Held until libtest tears down the current test thread. This makes the
    /// process boundary the single owner of process-test serialization: a new
    /// test cannot forget an annotation, while repeated process calls in one
    /// test do not deadlock by trying to acquire the gate again.
    static PROCESS_TEST_PERMIT: RefCell<Option<HostTestPermit>> = const { RefCell::new(None) };
}

/// Preserve the existing bounded parallelism for ordinary host-heavy tests.
pub(crate) fn acquire_host_test_permit() -> HostTestPermit {
    host_test_gate().acquire_shared()
}

/// Isolate a test proven sensitive to competition from other host-heavy tests.
pub(crate) fn acquire_exclusive_host_test_permit() -> HostTestPermit {
    host_test_gate().acquire_exclusive()
}

/// Serialize tests that cross Emaxx's host-process boundary.
///
/// The permit deliberately belongs to a separate gate from the bounded
/// host-heavy gate. Large-stack tests acquire the latter before their body
/// runs, so sharing a gate here would make nested process calls self-deadlock.
pub(crate) fn mark_process_test() {
    PROCESS_TEST_PERMIT.with(|slot| {
        if slot.borrow().is_none() {
            *slot.borrow_mut() = Some(process_test_gate().acquire_exclusive());
        }
    });
}

#[cfg(test)]
mod tests {
    use super::{HostTestGate, mark_process_test};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn host_test_gate_keeps_shared_capacity_and_exclusive_isolation() {
        static GATE: HostTestGate = HostTestGate::new();

        let first = GATE.acquire_shared();
        let second = GATE.acquire_shared();
        let (started_tx, started_rx) = mpsc::channel();
        let (entered_tx, entered_rx) = mpsc::channel();
        let third = thread::spawn(move || {
            started_tx.send(()).expect("announce shared waiter");
            let _permit = GATE.acquire_shared();
            entered_tx.send(()).expect("announce shared permit");
        });
        started_rx.recv().expect("shared waiter should start");
        assert!(entered_rx.recv_timeout(Duration::from_millis(25)).is_err());
        drop(first);
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("third shared test should enter at capacity");
        third.join().expect("shared waiter should finish");

        let (exclusive_started_tx, exclusive_started_rx) = mpsc::channel();
        let (exclusive_entered_tx, exclusive_entered_rx) = mpsc::channel();
        let exclusive = thread::spawn(move || {
            exclusive_started_tx
                .send(())
                .expect("announce exclusive waiter");
            let _permit = GATE.acquire_exclusive();
            exclusive_entered_tx
                .send(())
                .expect("announce exclusive permit");
        });
        exclusive_started_rx
            .recv()
            .expect("exclusive waiter should start");
        assert!(
            exclusive_entered_rx
                .recv_timeout(Duration::from_millis(25))
                .is_err()
        );
        drop(second);
        exclusive_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("exclusive test should enter alone");
        exclusive.join().expect("exclusive waiter should finish");
    }

    #[test]
    fn process_test_permit_lives_until_the_marked_thread_exits() {
        let (first_entered_tx, first_entered_rx) = mpsc::channel();
        let (release_first_tx, release_first_rx) = mpsc::channel();
        let first = thread::spawn(move || {
            mark_process_test();
            first_entered_tx.send(()).expect("announce first permit");
            release_first_rx.recv().expect("release first permit");
        });
        first_entered_rx
            .recv()
            .expect("first process test should enter");

        let (second_entered_tx, second_entered_rx) = mpsc::channel();
        let second = thread::spawn(move || {
            mark_process_test();
            second_entered_tx.send(()).expect("announce second permit");
        });
        assert!(
            second_entered_rx
                .recv_timeout(Duration::from_millis(25))
                .is_err()
        );

        release_first_tx
            .send(())
            .expect("release first process test");
        first.join().expect("first process test should finish");
        second_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second process test should enter after thread teardown");
        second.join().expect("second process test should finish");
    }
}
