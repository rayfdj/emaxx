use std::cell::RefCell;
use std::sync::{Condvar, Mutex, OnceLock};

use crate::lisp::eval::Interpreter;
use crate::lisp::reader::Reader;
use crate::lisp::types::{Env, LispError, Value};

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
    /// This thread already holds a real permit (a large-stack wrapper moved
    /// one in), so a nested acquisition must not book a second slot: with
    /// MAX_CONCURRENT_HOST_HEAVY_TESTS = 2, two tests each holding one
    /// permit and each waiting for a second deadlock the whole suite.  The
    /// gdb-verified hang: `run_with_large_stack' moves its permit into the
    /// big-stack thread, whose `upstream_batch_interpreter_with_features'
    /// then calls `acquire_host_test_permit' again.  An inherited permit
    /// keeps the invariant "one slot per concurrently-running marked test"
    /// and releases nothing on drop.
    Inherited,
}

thread_local! {
    static HOST_PERMIT_HELD_BY_THIS_THREAD: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

/// Mark the current thread as owning a moved-in host permit (the large-stack
/// wrappers call this from the spawned thread before running the test body).
pub(crate) fn note_host_permit_moved_to_this_thread() {
    HOST_PERMIT_HELD_BY_THIS_THREAD.with(|held| held.set(true));
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
            HostTestPermitKind::Inherited => {}
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

/// Source reconstruction tree-walks and macroexpands the entire GNU preload
/// image.  Running several copies at once only multiplies wall time and makes
/// libtest's 60-second warning look like a product hang.  Serialize that
/// setup phase while leaving both compiled startup and the test bodies under
/// the ordinary bounded-parallel scheduler.
pub(crate) fn acquire_batch_source_bootstrap_permit() -> std::sync::MutexGuard<'static, ()> {
    static GATE: OnceLock<Mutex<()>> = OnceLock::new();
    GATE.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

thread_local! {
    /// Held until libtest tears down the current test thread. This makes the
    /// process boundary the single owner of process-test serialization: a new
    /// test cannot forget an annotation, while repeated process calls in one
    /// test do not deadlock by trying to acquire the gate again.
    static PROCESS_TEST_PERMIT: RefCell<Option<HostTestPermit>> = const { RefCell::new(None) };
}

/// Preserve the existing bounded parallelism for ordinary host-heavy tests.
/// A thread that already holds a moved-in permit gets an inherited no-op
/// permit instead of booking a second slot (see HostTestPermitKind).
pub(crate) fn acquire_host_test_permit() -> HostTestPermit {
    if HOST_PERMIT_HELD_BY_THIS_THREAD.with(std::cell::Cell::get) {
        return HostTestPermit {
            gate: host_test_gate(),
            kind: HostTestPermitKind::Inherited,
        };
    }
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

/// Initialize GNU's early Lisp owners in their `loadup.el` order.
///
/// This is the smallest honest runtime for tests whose subject executes
/// portable definitions from subr.el (for example, compiled expansions of
/// `with-temp-buffer`).  Tests of the file-less C/Rust host must continue to
/// construct `Interpreter::new` directly.
pub(crate) fn initialized_gnu_early_lisp_interpreter() -> Interpreter {
    let upstream = crate::compat::project_root().join("../emacs");
    let mut interpreter = Interpreter::new();
    interpreter.set_load_path(
        crate::compat::emaxx_upstream_load_path(&upstream)
            .expect("resolve upstream GNU Lisp load path"),
    );
    interpreter.set_prefer_compiled_loads(crate::lisp::bytecode_vm_enabled());
    for library in [
        "emacs-lisp/debug-early",
        "emacs-lisp/byte-run",
        "emacs-lisp/backquote",
        "subr",
    ] {
        interpreter
            .load_target(library)
            .unwrap_or_else(|error| panic!("load GNU early owner {library}: {error}"));
    }
    interpreter
}

/// Replace a bare test interpreter with the same GNU Lisp image used by
/// normal Emaxx batch startup.
///
/// Tests must exercise the upstream Lisp owners directly.  In particular,
/// they must not load project-local compatibility facades that can mask a
/// missing primitive or move policy across the GNU C/Elisp boundary.
pub(crate) fn replace_with_gnu_batch_runtime(interpreter: &mut Interpreter) {
    let emacs_repo = crate::compat::project_root().join("../emacs");
    let options = crate::batch::BatchRunOptions {
        load_path: crate::compat::emaxx_upstream_load_path(&emacs_repo)
            .expect("resolve upstream GNU Lisp load path"),
        ..Default::default()
    };
    *interpreter = crate::batch::initialize_batch_interpreter_with_load_preference(&options, true)
        .expect("reconstruct compiled GNU batch Lisp image");
}

/// Initialize the same GNU-owned Lisp environment used by Emaxx batch mode.
/// Ownership-sensitive tests use this instead of rebuilding partial load
/// paths or quietly calling a dormant native dispatch arm.
/// The early-Lisp runtime plus additional GNU-owned libraries loaded from
/// the pinned checkout (help.el, keymap.el, cl-lib, ...), for tests whose
/// programs use those owners without paying for the full batch image.
pub(crate) fn initialized_gnu_early_lisp_interpreter_with(libraries: &[&str]) -> Interpreter {
    let mut interpreter = initialized_gnu_early_lisp_interpreter();
    for library in libraries {
        // loadup.el loads pcase with eager macro-expansion suspended: the
        // file's own pcase uses cannot expand until the file finishes.
        if *library == "emacs-lisp/pcase" {
            eval_lisp(
                &mut interpreter,
                &mut Vec::new(),
                "(let ((macroexp--pending-eager-loads '(skip))) (load \"emacs-lisp/pcase\"))",
            )
            .unwrap_or_else(|error| panic!("load GNU library {library}: {error}"));
            continue;
        }
        interpreter
            .load_target(library)
            .unwrap_or_else(|error| panic!("load GNU library {library}: {error}"));
    }
    interpreter
}

pub(crate) fn initialized_upstream_batch_interpreter() -> Interpreter {
    // EMAXX_IMAGE_TEMPLATE=1: reconstruct the image once per test thread
    // and clone it per test (issue #11).  The clone shares immutable Rc
    // structure; interpreter-owned tables are per-clone.  Validation
    // criterion: identical results to the reconstruct-per-test path,
    // stage for stage.
    if std::env::var("EMAXX_IMAGE_TEMPLATE").is_ok() {
        // Process-global template.  Interpreter holds Rc graphs, which are
        // sound across threads only when uses never overlap: libtest runs
        // each test on its own thread, so the serial gate's
        // --test-threads=1 is the required schedule.  The ACTIVE counter
        // makes a violation panic before any concurrent Rc touch: at most
        // one template-derived interpreter may be alive at a time.
        struct AssertSend<T>(T);
        unsafe impl<T> Send for AssertSend<T> {}
        static TEMPLATE: std::sync::Mutex<Option<AssertSend<Interpreter>>> =
            std::sync::Mutex::new(None);
        use crate::lisp::eval::{IMAGE_TEMPLATE_ACTIVE, ImageTemplateToken};
        let mut slot = TEMPLATE.lock().expect("image template lock");
        if IMAGE_TEMPLATE_ACTIVE.load(std::sync::atomic::Ordering::SeqCst) != 0 {
            panic!(
                "EMAXX_IMAGE_TEMPLATE requires --test-threads=1: a second \
                 template-derived interpreter was requested while one is live"
            );
        }
        if slot.is_none() {
            let started = std::time::Instant::now();
            *slot = Some(AssertSend(build_upstream_batch_interpreter()));
            if std::env::var("EMAXX_DEBUG_TEMPLATE").is_ok() {
                eprintln!("TEMPLATE build {:?}", started.elapsed());
            }
        }
        let started = std::time::Instant::now();
        let mut clone = slot
            .as_ref()
            .expect("image template built")
            .0
            .deep_clone_image();
        if std::env::var("EMAXX_DEBUG_TEMPLATE").is_ok() {
            eprintln!("TEMPLATE clone {:?}", started.elapsed());
        }
        IMAGE_TEMPLATE_ACTIVE.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        clone.image_template_token = Some(std::sync::Arc::new(ImageTemplateToken));
        return clone;
    }
    build_upstream_batch_interpreter()
}

fn build_upstream_batch_interpreter() -> Interpreter {
    let upstream = crate::compat::project_root().join("../emacs");
    let options = crate::batch::BatchRunOptions {
        load_path: crate::compat::emaxx_upstream_load_path(&upstream)
            .expect("upstream GNU Emacs load path"),
        ..Default::default()
    };
    crate::batch::initialize_batch_interpreter_with_load_preference(&options, true)
        .expect("initialize compiled GNU-compatible batch interpreter")
}

pub(crate) fn eval_lisp(
    interpreter: &mut Interpreter,
    environment: &mut Env,
    source: &str,
) -> Result<Value, LispError> {
    let forms = Reader::new(source).read_all()?;
    let mut result = Value::Nil;
    for form in forms {
        result = interpreter.eval(&form, environment)?;
    }
    Ok(result)
}

pub(crate) fn call_lisp_function(
    interpreter: &mut Interpreter,
    environment: &mut Env,
    name: &str,
    arguments: &[Value],
) -> Result<Value, LispError> {
    let function = interpreter.lookup_function(name, environment)?;
    interpreter.call_function_value(function, Some(name), arguments, environment)
}

pub(crate) fn run_with_large_stack(test: impl FnOnce() + Send + 'static) {
    let permit = acquire_host_test_permit();
    std::thread::Builder::new()
        .stack_size(128 * 1024 * 1024)
        .spawn(move || {
            let _permit = permit;
            note_host_permit_moved_to_this_thread();
            test();
        })
        .expect("spawn large-stack test thread")
        .join()
        .expect("join large-stack test thread");
}

/// Rewrite PROGRAM so every non-ASCII character becomes a `\N{U+XXXX}'
/// escape, keeping the text safe to hand the oracle through `--eval'.
///
/// GNU decodes command-line arguments with the locale's coding system, so a
/// program containing literal non-ASCII is corrupted under LANG=C -- often
/// badly enough that the reader rejects it outright with
/// `Invalid read syntax: "?"'.  LANG=C is exactly the environment the
/// compatibility harness runs its children in, so an oracle contract checked
/// through a literal `--eval' argument is checked in an environment where it
/// cannot work.
///
/// Loading the program from a file instead would fix the decoding but change
/// what is being measured: `-l FILE' leaves `last-coding-system-used' as
/// `prefer-utf-8-unix' in BOTH locales where `--eval' leaves it
/// `no-conversion' under LANG=C (and `utf-8-unix' under a UTF-8 locale), which
/// coding-sensitive contracts observe.  Escaping keeps the evaluation context
/// byte-for-byte identical to the original `--eval' call.  Verified against
/// the oracle: the escaped form yields identical output under both LANG=C and
/// a UTF-8 locale, and leaves each locale's own coding state untouched.
///
/// The escape is only valid where the Lisp reader accepts one -- inside a
/// string or after `?'.  That covers every non-ASCII character in these
/// contract programs; a non-ASCII SYMBOL name would need a different scheme.
pub(crate) fn oracle_program_ascii(program: &str) -> String {
    if program.is_ascii() {
        return program.to_string();
    }
    let mut escaped = String::with_capacity(program.len());
    let mut in_string = false;
    let mut in_comment = false;
    let mut previous = '\0';
    let mut before_previous = '\0';
    for character in program.chars() {
        if character.is_ascii() {
            match character {
                '\n' => in_comment = false,
                ';' if !in_string => in_comment = true,
                '"' if !in_comment && previous != '\\' => in_string = !in_string,
                _ => {}
            }
            escaped.push(character);
        } else {
            // Escaping is only MEANING-PRESERVING where the reader takes a
            // `\N{...}' escape.  Two positions would change the program
            // silently rather than fail: a non-ASCII character in a SYMBOL
            // name (`a\u{2018}b' reads as the symbol `aN{U+2018}b'), and one
            // preceded by a backslash inside a string (`"x\\\u{2018}y"' reads
            // as a literal backslash followed by the escape text).  Refuse
            // both loudly -- an oracle handed a subtly different question is
            // exactly the kind of defect this helper exists to prevent.
            let after_question = previous == '?' || (previous == '\\' && before_previous == '?');
            let escapable =
                in_comment || (in_string && previous != '\\') || (!in_string && previous == '?');
            assert!(
                escapable && !(after_question && previous == '\\'),
                "non-ASCII {character:?} (U+{:04X}) sits where a \\N{{U+XXXX}} escape would                  change the program's meaning; spell it explicitly or extend this helper.                  Context: {:?}",
                character as u32,
                program
                    .split(character)
                    .next()
                    .map(|head| head.chars().rev().take(40).collect::<String>())
                    .unwrap_or_default()
            );
            escaped.push_str(&format!("\\N{{U+{:04X}}}", character as u32));
        }
        before_previous = previous;
        previous = character;
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::{HostTestGate, acquire_batch_source_bootstrap_permit, mark_process_test};
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
    fn batch_source_bootstrap_gate_serializes_only_the_setup_region() {
        let first = acquire_batch_source_bootstrap_permit();
        let (started_tx, started_rx) = mpsc::channel();
        let (entered_tx, entered_rx) = mpsc::channel();
        let waiter = thread::spawn(move || {
            started_tx.send(()).expect("announce bootstrap waiter");
            let _permit = acquire_batch_source_bootstrap_permit();
            entered_tx.send(()).expect("announce bootstrap permit");
        });
        started_rx.recv().expect("bootstrap waiter should start");
        assert!(entered_rx.recv_timeout(Duration::from_millis(25)).is_err());
        drop(first);
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("bootstrap waiter should enter after setup releases");
        waiter.join().expect("bootstrap waiter should finish");
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
