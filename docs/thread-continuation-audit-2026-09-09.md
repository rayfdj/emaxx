# Selective thread continuation port: honesty and de-cheating audit

Base: main `85f0c2858eb99813081b8d0a9972c6b0589ee1e9`.
Reference: scheduler checkpoint `e479386f55b78e085c22d66c1b5ae9c57090ce87`
from `integrate-native-threads-startup`; live-constants control also inspected
at `a927d84`. This is a selective port, not a branch merge. Later native-comp
dump commits remain deferred.

## Review before broader validation

The review covers the tracked diff and the four new continuation, context,
root-registration and native-suspension files. Source checks are necessary
but cannot establish complete Rust aliasing safety or GNU equivalence.

- Removed the old `ThreadProgram` body classifier and yield-count deadlock
  fallback. Each worker calls its original function through ordinary funcall
  and retains actual execution frames. Blocker reporting and backtraces use
  that worker's recorded execution context, without recognizing test names.
- Compared signal delivery, join-entry error snapshots, recursive mutex
  release/reacquisition, variable-only binding swaps and live-thread roots
  with the pinned sibling GNU `thread.c` and `eval.c`. Finished-thread results
  are traced through reachable thread objects, not treated as permanent roots.
- The editor payload has one owning execution shell at a time. Scoped roots
  retain their source borrows until deregistration, including non-LIFO stack
  completion. Suspended native frames retain their machine stack ranges,
  handlers, pending errors, unwind actions and environment. The native heap
  still uses main's `NativeHeapOwner` allocation and integrated collector.
- Native activation locking remains held across suspended activations and is
  released after the last activation on the owning OS thread. Loader units
  retain stable Rc ownership across registry growth; compiler RefCell borrow
  exclusion remains shared. TLS and shared ownership are restored before
  original native frames resume, including the tested Rust panic path.
- Main's keyboard-terminal binding identity, frame identities, terminal event
  polling, native unwind corrections and dump refusal with live threads are
  preserved. Live execution stacks are not made clonable or dumpable.
- No upstream test, assertion, oracle expectation, timeout, test selector,
  native ABI constant or compatibility denominator is weakened. Oracle
  execution remains confined to test code. Production code does not delegate
  execution or results to GNU.

## Corrections found by this review

The initial port still carried unrelated old-branch edits: four native-loader
variable reads used `forwarded_c_value` instead of main's `lookup_var`, two
coding defaults changed vector representation, and coding initialization
moved. These have been removed. They were scope contamination, not needed for
continuation ownership; their correctness is not assumed from old results.

The root inventory previously inspected `Interpreter` directly. After the
ownership split it must inspect `InterpreterState`, or the audit could pass
without examining any payload fields. It now asserts that it found the real
globals field, checks registration of scoped/parked roots, and examines Lisp
object owners in `ThreadExecutionContext`. Its textual checks supplement the
GC behavioral controls; they are not a proof of collector completeness.

An earlier imported native test helper recognized only decoded cons cells,
missing current main's native-only allocations. Its membership check now
accepts either actual owner map. The expected retained `(17)` result and
collection assertion remain unchanged. The failed run remains in the logs.

## Evidence and limits

Artifacts: `/private/tmp/emaxx-thread-port-sept9`; fresh pre-port comparison:
`/private/tmp/emaxx-thread-review-sept9/results.json`.

Before the audit corrections, four low-level continuation/native-stack tests
and fifteen focused root, thread, timer, compiler and loader controls passed.
Those receipts are retained, not relabeled as a run of the corrected tree.
The fresh pre-port replay showed GNU passing all four target tests while main
failed them, and a finished-thread GC probe returned GNU `(1 0)` versus main
`(1 1)`.

Validation receipts are recorded below. Historical
34-test branch passes and the old 83-check checkpoint are not current-tree
validation. No fresh 7,883-test total or 100% compatibility claim is made.

The scheduler remains cooperative on one OS thread. The changes do not prove
GNU-equivalent scheduling for compute-only loops without a scheduling point.
The macOS ARM64 spill path is exercised locally; Linux x86-64 execution still
requires a Linux host. These tests do not establish portable stack unwinding
or arbitrary native panic safety. Existing live-thread dump refusal remains;
serializing continuations is outside this port.

## Validation receipts

- Corrected tree: all 22 `anti_cheat::gate_tests` passed, zero failures or
  ignores (`audit-01.log`). This includes fresh GNU manifest comparisons and
  the strengthened payload/context root inventory. Manual inspection also
  covers the new files; the oracle-delegation token test alone scans a limited
  list of files and would not establish that coverage.

- Corrected CLI, full unchanged thread selection: GNU and Emaxx each ran
  **36 tests: 34 passed, one failed, one skipped**. All four target mismatches
  passed. Both failed `threads-join-error`, tagged `:unstable` upstream, which
  expects a body error to be re-raised by join after the worker exits. The
  pinned GNU binary instead returns nil, consistently with the checked
  `Fthread_join` source. Both skipped the test for a build without threads.
  The historical 35-test selection excluded the unstable test. The expanded
  selection is retained, and the shared failure is not counted as passing.
  Logs: `upstream-{gnu,emaxx}.log`, per-test comparison:
  `upstream-outcomes.json`. An initial result parser missed lowercase
  `skipped`; only the parser was corrected, without rerunning either editor.

- The broader selection ran 198 cases: 197 passed; the local HTTP fixture
  failed at `TcpListener::bind` with sandbox `PermissionDenied`, before
  exercising the runtime. The same exact test passed with local socket
  access (`http-unsandboxed.log`). The 197 successful cases were retained,
  not rerun. This gives successful coverage of all 198 selected controls,
  including the original thirteen corrections and native/GC/VM boundaries.
  Both Eshell suite controls also passed. Rustfmt check passed.

- Strict `cargo clippy --offline --all-targets --all-features -- -D warnings`
  passed with zero warnings (`clippy.log`).
- Additional adversarial CLI probes matched GNU exactly. An early injected
  signal does not stop a body that finishes without another lock acquisition:
  both print `(t nil)`. `post_acquire_global_lock` explicitly defers that first
  signal until handlers exist; an eager entry check would invent behavior.
  No runtime change was made for this probe.
- Real native-compiled workers both retain weak keys across collection while
  parked. The first native activation returns while the second is suspended;
  the second resumes through a native error handler and cleanup. Both editors
  print `(t 2 (0 0) (1 caught 1) [1 1])`, with successful exit status. The
  leading t asserts actual native compilation; both cleanup counters are one.
  Inputs and logs: `early-signal.el`, `native-workers.el`,
  `early-{gnu,emaxx}.log`, `workers-{gnu,emaxx}.log`.

- Native artifact identity passed for nine unchanged GNU source inputs:
  eight artifacts were byte-identical, including the complete `comp.el`
  artifact (881,800 bytes), and the no-byte-compile policy case behaved as
  expected (`native-identity.log`).
- Full upstream native suite: **178/178 matching outcomes**, with **177
  passes and one failure in each editor**. `comp-tests-bootstrap` cannot
  find `comp.elc` in this isolated checkout; it remains a recorded shared
  setup failure, not a pass. Native cache suite: **3/3 passed in each**.
  Artifacts: `target/compat/run-1788921491603748000-80566` and
  `target/compat/run-1788935002254014000-83971`. The laptop slept during
  this run; the reported wall-time ratio is not performance evidence.
- All seven upstream server tests passed in both editors, using real local
  sockets and the GNU client (`server-{gnu,emaxx}.log`). The first PTY run
  stopped in GNU before reaching Emaxx: the driver supplied `LC_ALL=C` to a
  fixture that sends UTF-8 keyboard bytes. GNU produced `abXC`, not `abXéc`.
  The failed receipt is retained (`terminal-pty.log`); the follow-up uses an
  explicit UTF-8 locale, without changing assertions or editor code.

## Follow-up queued-signal correction

Further source review found four discrepancies in the imported signal path.
Fresh GNU probes established the expected behavior, and the pre-correction
Emaxx binary reproduced each discrepancy:

| Contract | GNU | Before correction |
| --- | --- | --- |
| Nil-condition signal data stays reachable through its thread object, then becomes collectible | `(1 0)` | `(0 0)` |
| Queued signal retains DATA identity and subsequent mutation | `(t after)` | `(nil before)` |
| Nil-condition signal broadcasts to both waiters on the same condition variable | `2` | Watchdog error: neither waiter woke |
| Signaling a blocked target checks the caller's already pending signal | `caught` | `returned` |

The implementation now stores condition and data independently, roots both
through the reachable thread object, and uses the original data as the caught
signal's cdr. Join retains its separate condition/data snapshot across waits.
Signaling a condition-variable waiter broadcasts to its fellow waiters;
signaling a target with a wait condition also checks the caller's pending
signal. These follow `thread.c:Fthread_signal`, `thread_signal_callback`,
`post_acquire_global_lock`, and `Fthread_join`, with the Lisp-owned fields
specified in `thread.h`. The root inventory now checks direct Lisp fields in
`ThreadState` as well.

The permanent integration fixture runs these four probes plus the early
signal and real native-worker controls in both editors, with identical Lisp,
isolated directories, exact complete stdout comparisons, successful exit
requirements and an external timeout that fails the test. The first draft
of the caller probe used `thread-blocker` rather than GNU's `thread--blocker`;
that fixture typo was corrected before using its results.

The completed broader/native/server receipts above predate this small signal
correction. Fingerprint comparison confirms the only subsequent production
source changes are in `eval.rs` and `eval/threads.rs`; the other changes are
the root audit and new tests. Native heap, compiler, loader, VM and ABI code
are unchanged. Final focused validation is recorded below rather than
relabeling those earlier receipts or rerunning the whole gate.

## Final validation after the signal correction

- The new `native_thread_continuations` integration test passed: all six
  fixtures matched the exact expected output in both GNU and Emaxx, including
  actual native compilation (76.33 seconds; `signal-integration.log`).
- **44 focused Rust tests passed, zero failures or ignores**: all 22
  de-cheating gate checks plus 22 continuation, GC, thread, native ownership,
  timer and dump-boundary controls (135.88 seconds;
  `signal-focused-final.log`, inventory `signal-final-selected.json`).
- The full unchanged 36-test thread selection was rerun in the corrected
  Emaxx binary and compared against the retained GNU outcomes: **34 passed,
  one failed, one skipped**, with the same shared `threads-join-error`
  failure. All four original mismatches still pass
  (`signal-upstream-final.log`).
- `cargo fmt --all -- --check` passed. Strict all-targets/all-features Clippy
  passed with **zero warnings** (`signal-fmt-final.log`,
  `signal-clippy-final.log`). Source and fixture hashes were checked before
  and after each final validation stage (`signal-final-fingerprints.json`).
- The real PTY comparison passed in both editors with `LC_ALL=en_US.UTF-8`:
  separate primary/client input and screens, fragmented arrow-key and UTF-8
  input, saving `abXéc\n`, normal client-frame closure and abrupt client
  disconnection all matched. The primary frame survived both closures.
  No fixture assertions were changed (`terminal-pty-utf8.log`,
  `pty-utf8/{oracle,subject}/result.json`).
