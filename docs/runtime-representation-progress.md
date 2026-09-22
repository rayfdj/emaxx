The full objective is retained in [runtime-representation-goal.md](runtime-representation-goal.md).
This record tracks evidence and outstanding work; no completion is claimed.

Starting revision: `45eb1531caabb35b816fc83e2e8a5b88090dae4e`, freshly fetched
from `origin/main` on 2026-09-21. Implementation branch: `compact-runtime`.
The pre-existing September 21 audit and all its raw artifacts are preserved.

| Goal requirement | Required completion evidence | Current status |
| --- | --- | --- |
| 1. Reproducible starting point | Source, binary, image, toolchain and oracle identities; fresh reproductions; original and corrected baselines | Starting identities recorded; all six original evaluator findings reproduced; corrected release and own image built; original/corrected/GNU diagnostic timing comparison preserved |
| 2. GNU architectural reference | Per-change C owner, semantic comparison, explained necessary Rust deviations | Evaluator repairs trace to `eval.c:eval_sub`, `apply_lambda`, `Fautoload_do_load`, and `fns.c:list_length` |
| 3. Authoritative representation | One-word values; two-word conses; shared interpreter/VM/native payload; no ordinary-path mirror lookup or synchronization; other object kinds reviewed | Open; current cons remains 80 bytes with duplicate payload and synchronization |
| 4. Allocation/GC/rooting | Correct accounting, live/dead-root controls, dumped-object treatment, enforced runtime ownership | Open; both formerly ignored root contracts pass in fresh processes; allocator ownership remains a finding |
| 5. VM/call efficiency | Fresh comparable profiles, reduced instructions/allocations, preserved call semantics | Open; correctness repair precedes optimization |
| 6. Adversarial de-cheating | Closed applicable September 21 findings, meaningful differential cases, audited runtime and measurement paths, reporting negative controls | Open; reproducing call semantics and platform dispatch first |
| 7. Equivalent performance measurement | Locked representative suite and noise criterion, independent normal editors/images, checked results, all samples and mode/GC/memory data | Expanded 16-case suite and 3% ceiling locked before optimization; two GNU self-comparisons retained, complete noise calibration and frozen-oracle certification still open |
| 8. Complete zero-warning validation | Required fmt/check/Clippy/diff/serial gate, release paths, full frozen comparison and affected integration contracts on Linux and macOS | Repaired baseline passes fmt/check/strict Clippy/diff and focused serial tests; normal release passes 51 GNU controls; full gates remain open |
| 9. Final proof | Current-source adversarial audit and requirement-by-requirement final evidence | Open |

The local GNU source matches the pinned revision, but its executable SHA-256
(`12c762a0…`) differs from the Darwin lock (`591acf7b…`). Diagnostic differential
results against that executable must remain qualified. Obtain the correct
oracle identity before ordinary frozen certification; do not silently repin.

The existing audit's current-revision executable is preserved as a diagnostic
baseline: it contains only the documented deletion of three Linux-only arms to
work around the macro's macOS build failure. Its SHA-256 is `ce60811f…`.
The goal requires repairing attribute propagation in the actual implementation,
then rebuilding normally; that diagnostic deletion is not the proposed fix.

Work order: reproduce and repair omitted GNU semantics and build correctness;
retain a corrected baseline and lock the expanded measurement contract; unify
representation and rooting; profile and reduce remaining VM/call work; perform
the complete adversarial review, then the required final gates and measurements.
Use focused checks during development and keep heavy processes serial.

2026-09-21 implementation evidence (in progress):

- Fresh copies of all six evaluator probes reproduce the audit's differences
  against the preserved original executable. Raw outputs are in
  `target/runtime-goal/baseline/probes/`. The circular case times out at eight
  seconds only in Emaxx.
- A fresh exact-source all-target build reproduces the six inotify errors and
  additionally finds two Darwin-only tests still passing `Vec` instead of `Env`.
  The initial network failure and subsequent offline build failure are retained
  separately. Fetching the locked missing dependencies succeeded.
- The dispatch repair preserves `cfg`/`cfg_attr` on each lifted entry, the
  supported-name probe, property probe and test inventory. Its compile control
  includes unavailable functions under false configurations and alternations.
  The two stale tests now construct the actual rooted environment type. The
  all-target/all-feature check passes without warnings after this build repair
  (`build-repair-check-2.log`). Linux execution remains unverified.
- The evaluator repair follows `eval.c:eval_sub` and `apply_lambda` separately:
  proper-list counting with the C cycle/quit cadence, both subr arity bounds,
  fixed subrs advancing after evaluation (including optional positions), MANY
  subrs stopping at the original bound or a shortened non-cons tail, and Lisp
  functions keeping the original count, padding a shortened list with nil.
  Seventeen new test inputs and expected outputs have been run in GNU; all
  agree (`baseline/new-evaluator-controls.gnu.json`). All six focused tests
  pass, followed by 40 broader arity/call/backtrace/handler tests in 198.38 s.
  Strict Clippy passes without warnings (`first-repair-clippy-2.log`). The
  exact tested diff, source hashes and test binary identity are retained in
  `first-repair-tested.patch` and `first-repair-source.json`.

2026-09-22 additional adversarial findings and work in progress:

- Invalid function cells evaluated arguments before rejection and reported the
  resolved object rather than the original callee. Cons-valued function cells
  could validate the argument list before classifying the callee. GNU rejects
  these before either step (`baseline/resolution-controls.json`).
- Function-position expressions were evaluated where GNU applies `Ffunction`,
  and a redefined `if` still selected the old special-form implementation.
  Source calls now resolve first, then select the actual function's convention.
- Ordinary autoloads ran after argument side effects; autoloaded macros counted
  arguments before loading. Five fresh controls establish GNU's ordering for
  successful loads, signaled errors, missing definitions, subr aliases and
  macros (`baseline/autoload-resolution-controls.json`). The initial probe's
  undeclared lexical log variable was invalid across a load; its raw failed
  evidence is preserved separately in `resolution-controls.json`.
- The new source resolution follows `eval_sub`'s retry after
  `Fautoload_do_load`, including malformed-stub errors, and classifies before
  counting arguments. Macro expansion retains its unevaluated call frame.
  These changes pass strict Clippy and all 50 affected serial tests in
  188.89 s (`resolution-repair-regressions.log`). The exact tested source is
  in `resolution-repair-tested.patch` and `resolution-repair-source.json`.
  The malformed-autoload test first exposed `nthcdr` reporting the tail
  instead of GNU's original improper list; that cause is fixed and has an
  additional GNU-verified identity/boundary control. The failed run remains
  in `resolution-repair-tests.log`. No performance result or final evaluator
  certification is claimed.
- Source review also found that `memory-use-counts` currently signals that
  allocation counters are unavailable. Baseline reporting must label that
  missing metric explicitly; final allocation evidence needs real counters.

Performance measurement repair in progress:

- Removed the private batch request interception and Rust marker/overlay/
  bare-interpreter adapters. Both editor commands now load the same helper
  and workload files and invoke `emaxx-perf-run-scenario` through normal
  startup. Image preparation builds the release executable's own dump.
- Source workloads return their computed result. The common Lisp runner
  checks it outside the timed region and obtains actual GC counts/times from
  GNU's `benchmark-run` path on both editors. A signaled error, wrong result,
  invalid timing, or partial sample set fails; each completed sample is also
  printed to the raw process log before final reporting.
- Host validation checks process success before accepting a report, verifies
  runner/scenario/parameters, exact case inventory and sample counts, finite
  positive durations, and recomputes summaries from raw samples. Rejections
  have separate files; the child's report is never overwritten with a
  synthesized success or failure.
- Fresh GNU inventory inspection found previously invented display/NOC case
  names. The display suite actually has 12 cases; NOC has three invocations,
  including the same function twice. The reporter executes all three and
  distinguishes the repeated occurrence with `#2`. Coding's 18 files and
  both modes now have 36 exact case IDs. Source inventory evidence is in
  `perf-inventory.gnu.json`; no upstream selector or benchmark was modified.
- The former test of private batch interception is replaced by a test that
  the same spelling calls its ordinary Lisp definition. The two interpreter
  performance contract tests now exercise the shared Lisp runner with the
  initialized GNU runtime. Added rejection controls cover corrupt/missing
  cases, samples and summaries, unsuccessful processes and missing reports.
  GNU has run the shared valid/incorrect-result/error/partial-sample controls
  (`perf-shared-runner.gnu.json`). All 13 selected library tests and both
  harness binary tests pass (`perf-repair-tests.log`); the real GNU NOC
  helper also executes and reports all three occurrences separately
  (`perf-duplicate-inventory.gnu.json`). The repaired baseline passes fmt,
  all-target/all-feature check, strict Clippy and diff checks without warnings.
  Exact patch/source/test executable identities are in `perf-repair-source.json`.
  This is harness repair, not the final expanded and calibrated performance
  suite. The normal release build and its own image completed successfully;
  identities are in `target/runtime-goal/corrected/environment.json`. All 51
  retained GNU semantic controls match through the release CLI, including
  output, stderr and exit status (`corrected/probes/summary.json`). Both
  editors also pass all six shared runner controls: a complete valid report,
  rejection of a wrong answer, and rejection of a partial sample set
  (`corrected/shared-runner/summary.json`). These are functional controls,
  not performance results or frozen-oracle certification.

Representation dependency discovered during source review: the two cons
payloads contain different word encodings, not merely duplicate storage.
`Value::Nil`/`T` and id-addressed kinds use tag 1; native nil is zero and native
symbols/other objects use anchors and `NativeHandle` allocations. Consequently,
removing the cons mirrors requires converging the value encodings and object
owners, not moving the existing fields into a per-cons map. The next design
review must cover all constructors and native accessors before changing the
cons layout. Symbol cells also live per interpreter because test templates
share interned symbol objects; a single authoritative symbol owner must resolve
that sharing explicitly rather than preserve a second mutable symbol table.

Expanded core measurements (still before representation optimization):

- Preserved a new interleaved comparison of GNU, the original audit binary,
  and the normally built corrected release in `corrected/measurements/`.
  Restoring the evaluator's required work costs approximately 15% on the
  lexical loop and 42% on the dynamic loop in this diagnostic comparison.
  Other unfavorable results, including explicit-GC variability, remain in
  the raw data; none are presented as frozen-oracle certification.
- Added a separate 16-case core contract, common source workloads and normal
  CLI process runner. It covers all requested modes, transitions and upstream
  workload categories, with real mode/result checks, per-sample GC and raw
  allocation counters, process/startup/body time, OS peak RSS, and exact input
  hashes. Native compilation is a separate preparation step using unchanged
  upstream fixture bytes and each editor's own compiler.
- Three GNU-only pilots fixed meaningful workload sizes and exposed automatic
  native loading in upstream libraries. The contract now explicitly loads
  sorting and bindat source and undo's unchanged bytecode. All relevant modes
  and results are recorded in the proposed manifest. Seven host-side reporting
  controls pass, including wrong answers, wrong mode, incomplete samples,
  failed processes, inconsistent raw data, missing metrics and incomplete
  inventory (`core-reporting-controls-2.log`).
- The Emaxx pilot completed all 16 functional result/mode checks and exposed
  a particularly large VM-to-native call cost. Its final input check correctly
  invalidated the run because the host validator was edited while it was in
  progress. Its raw observations remain under `core-pilot-emaxx/`; do not
  promote them to authoritative performance evidence.
- The first GNU self-comparison retained 288 process records. Host load rose
  above 100, and one Lisp wall-clock sample jumped to 632.9 seconds while the
  containing process's monotonic measurement was only 17.8 seconds. The host
  validator rejected it; the other samples also show unresolved noise in
  several cases. The second completed all 288 processes with unchanged inputs,
  but only three workloads resolved the 3% interval. Final certification
  requires a matching successful calibration revalidated against its raw
  reports. The suite, parameters, modes and 3% ceiling are now locked without
  widening that ceiling; `core-contract-lock.json` records exact hashes before
  any representation or VM optimization. The current host's calibration
  limitation remains open and cannot be used to declare parity.
- Removed the two GC contract ignores in source, retaining their exact GNU
  programs and both live-root and dead-root assertions. Each full contract
  now runs in a fresh child of the same Rust test executable to avoid unrelated
  prior-test conservative stack words. The parent requires one executed,
  passing, nonignored child test. The gate-profile build and both standalone
  contracts pass (`gc-contracts-build.log`, `gc-contracts-lexical.log`,
  `gc-contracts-bytecode.log`). Both also pass together with the neighboring
  constant-vector mutation and dead-thread-result contracts: four tests in
  40.45 seconds (`gc-contracts-neighbors.log`). The full gate remains open.
  Their names are also removed from the gate's
  allowed-ignore list, so reintroducing either ignore would fail inventory
  validation.

Runtime ownership work in progress:

- Added one process-wide execution lock at the public batch, terminal, ERT
  and audit entry points, following `thread.c:global_lock` and
  `acquire_global_lock`. Lisp coroutines continue on the owning OS thread.
  Nested host entry retains the outer lock; ordinary object operations add
  no lock or owner lookup. A panic escaping the boundary poisons the lock.
- Made raw heap/interpreter, buffer and overlay modules crate-private. Public
  editor results contain owned host data. `Value` retains its one-word ABI
  and now carries a zero-sized marker preventing `Send` and `Sync`. This is
  an intentional Rust API restriction: callers can no longer retain an
  unrooted Lisp handle outside runtime ownership. Existing binaries use the
  owned entry points already. Unused former public helpers were removed;
  helpers used only by internal tests now compile only for tests.
- The first public concurrency control passes: three OS threads make twelve
  batch calls with allocation, explicit GC and a nested version request while
  the outer interpreter is still alive. The independent lock/nesting control
  and the word-size/non-Send/non-Sync control also pass
  (`ownership-public-tests-1.log`, `ownership-boundary-tests.log`). Later
  entry-point changes require rebuilding and repeating these checks.
- Removing swallowed top-level ERT errors exposed a separate false-pass
  mechanism in the old library runner: it constructed a bare interpreter,
  ignored missing `ert`/`ert-deftest`, then allowed an empty passing inventory.
  All three existing integration files now expose those missing definitions
  (`ownership-ert-integration-1.log`). The replacement initializes normal GNU
  Lisp, uses strict loading and the same Lisp reporter as the compatibility
  CLI, and returns its full `BatchReport`, including skips and expectation
  evidence. Integration tests reject empty inventories. A load-error control
  and a two-test pass/fail control now pass. The old Rust ERT runner remains only in internal test helpers and
  still needs the final audit; it is no longer the public library route.
- The public ERT entry now requires the editor executable explicitly:
  `comp.el:comp--final` must launch the editor CLI, while an embedding host
  such as libtest need not implement that CLI. Its ordinary GNU invocation
  variables name that binary; native compilation remains enabled. The three
  original upstream integration files execute 431 tests: 430 passes, one
  unchanged upstream expected failure, zero unexpected outcomes
  (`ownership-ert-integration-3.log`). The earlier compiler-child failures
  remain in `ownership-ert-integration-2.log`.
- Strict all-target/all-feature Clippy, fmt, check, diff and the private-API
  compile-fail doctest pass at the float checkpoint below. All three public
  entry controls and the six ownership/GC checks pass on that build too.
  This boundary repair does not establish the soundness of every
  internal GC borrow: the duplicated `Box::from_raw` state views, escaped
  string borrows and conservative scanning of generic Rust buffers remain
  architectural review items. The shared compact representation is still open.
- Nine performance-reporting controls now pass, including calibration
  revalidation against the current inputs and original raw process/sample
  evidence (`core-reporting-controls-4.log`). Missing calibration, changed
  binaries/hosts/parameters, inconsistent aggregates, missing reports and an
  unresolved tolerance all reject certification. The locked workload and
  runner sources were not changed by these additional controls.

First shared-object conversion:

- Native floats now use the interpreter's existing tagged pointer directly,
  following `lisp.h:XFLOAT` and `XFLOAT_DATA`. Removed the float handle kind,
  allocation, identity-map entry and reverse-map access from ordinary native
  encode/decode. Checked host decoding validates the allocation; GC follows
  a tagged or base pointer as `alloc.c:live_float_holding` does. Those checks
  are absent from protected live reads.
- All 96 native runtime tests pass, including new exact-word, cross-runtime
  identity, signed-zero/NaN payload, invalid-word, native-only live-root and
  unreachable-float reclamation controls. The checkpoint's exact source,
  changed-file archive, build artifacts and verification are recorded in
  `target/runtime-goal/canonical-float-{source,build-artifacts,validation}.json`.
  This removes one adapter; floats still carry a separate in-cell mark word,
  conses remain 80 bytes, and other native handles/mirrors remain. No speedup
  or completed architecture is claimed.
- Reviewing the remaining cons mutation watchers found that the file-name
  handler cache both adds watcher state GNU does not need and changes lookup
  semantics. Ten normal-CLI controls differ from GNU in the checkpoint
  binary: eager dotted/circular-list rejection, regexp/operation ordering,
  structural equality used for inhibition, a spurious symbol-only operation
  restriction, and treating a nil handler as callable. Raw programs and both
  results are retained as `file-handler-controls*`. Replacing this cache with
  `fileio.c:Ffind_file_name_handler`'s direct walk is the next change.

File-handler and membership repair checkpoint:

- Removed the file-name-handler match cache and its mutation watchers. Lookup
  now follows `fileio.c:Ffind_file_name_handler` directly, including regexp
  ordering, lazy `Fmemq` tests, identity-based inhibition, improper alist tails,
  arbitrary operation values and nil handlers. The three old cache tests keep
  their functional assertions; only invented scan-count requirements were
  removed. The ten GNU controls also pass with changed names and paths.
- The shared interpreted `memq`/`memql`/`member` walk now follows
  `fns.c` and `lisp.h:FOR_EACH_TAIL`: no matching an improper tail as an element,
  original-list error identity, reached-cycle error data and GNU's quit cadence.
  All 115 affected tests pass, including the complete native runtime suite.
  `target/runtime-goal/file-handler-{source,build-artifacts,validation}.json`
  records the exact source and binary. The build completed without warnings;
  normal-CLI comparisons and subsequent validation continue with the symbol
  checkpoint. The earlier incorrect outputs remain preserved.

Symbol conversion checkpoint:

- Symbols and the builtin nil, true and unbound words now pass through native
  calls without a handle allocation, reverse-map entry or stored handle slot.
  Removed the separate true/type-symbol anchors. Fixnums also pass their
  existing word directly. Protected live reads use the canonical word; checked
  host reads and GC alone consult allocator metadata. `lisp.h:XBARE_SYMBOL`,
  `make_lisp_symbol_internal`, `EQ` and `alloc.c:live_symbol_holding` are the
  reference mechanisms.
- GNU uses offsets from `lispsym`; the remaining dynamically allocated Rust
  symbols still use absolute tagged addresses through the native symbol
  helpers. The builtin constants use GNU's 64-bit words 0, 48 and 96. This is
  an intermediate representation change, not a completed GNU symbol layout:
  symbol value/function/property cells still live in interpreter tables.
- Two explicit migration adapters remain for nil/t: construction recognizes
  their interned names, and GC keeps the allocated SymbolName/name objects
  owned by the obarray. Remove both when builtin symbol storage itself owns
  the authoritative mutable cells. Cached native value words also remain
  until all other object kinds use their canonical words. No new per-access
  identity map is introduced and no performance improvement is claimed yet.
- Replaced the local tests that required two native identities for one symbol
  with same-word/cross-runtime identity checks and actual live/dead symbol GC
  coverage. Added a control for builtin name survival across collections.
  Those old representation assertions were contrary to GNU's single-object
  identity; no upstream test or selector was changed. The first compiler
  check found a test helper spelling error; its failed log is retained.
  The corrected source passes all-target/all-feature check, strict Clippy,
  formatting and diff checks. Both optimized builds completed without
  warnings, with all 168 captured files unchanged.
- All 156 selected tests pass, including all 97 native runtime tests, 13
  dump tests and 27 value tests. All six ownership/GC checks pass too.
  The normal CLI, using its own freshly built image, matches GNU for startup,
  uninterned/private-obarray symbol identity and GC, and all ten file-handler
  controls. Exact sources, binaries, image, commands and results are in
  `target/runtime-goal/canonical-symbols-{source,build-artifacts,validation}.json`.
  Earlier image-less probes exceeded their limits; a separate startup run
  completed in 130 seconds. Those observations are retained and are not
  performance evidence. Full platform gates and parity remain open.

Quote and GC ownership changes (execution validation pending):

- Removed `plain_quote_templates`, its mutation snapshots and its GC roots.
  `quote` now returns its reader-owned argument directly, as `eval.c:Fquote`
  does, and rejects extra arguments with GNU's error identity. Vectors return
  directly from evaluation too. Missing reader-completion steps are performed
  once at embedded evaluation and buffer-read boundaries; test helpers use
  that same boundary without changing their Lisp inputs or assertions.
- The previous binary retained both temporary quoted objects in a weak-key
  test: `(2 t 2)` versus GNU's `(1 t 0)`. The new regression requires both
  live-object survival and dead-object reclamation. Sharing, mutation, cycles,
  literal objects, aliases and changed names/contents have additional GNU
  controls. Original raw results remain in `quote-and-lambda-controls-before-1.json`
  and `quote-live-dead-gnu-1.json` under `target/runtime-goal/`.
- Replaced the state-owning `Box` with one stable pointer owner. Its final
  drop alone reconstructs the original box; GC no longer creates second box
  owners. The root registry preserves the original writable pointer, and
  sweep cleanup reborrows the active interpreter through its existing owner.
  Parked-state views remain scoped to GC. This fixes specific ownership and
  pointer-provenance problems; broader field-borrow, string-lifetime and Rust
  buffer-scanning issues remain open. No ordinary value operation gains a
  lock or metadata lookup; cost measurement remains pending.
- A separate GNU control confirms that `make-interpreted-closure` must retain
  its actual body list. The current copied body and reconstructed inspection
  result produce `(nil 1 nil)` where GNU produces `(t 2 t)`. Removing that
  duplicate representation remains subsequent work. Source `lambda` expansion
  can legitimately copy its body through GNU Lisp, so its existing tests must
  not be changed to assume sharing without checking the actual entry point.
- The second source checkpoint passed formatting, compilation, strict Clippy
  and both development builds without warnings. Its execution batch aborted
  in the unchanged 250,000-cons native hot-path probe; a separate run reproduced
  the stack overflow with the existing 128 MB test stack. The quote-sharing,
  source-mutation and continuation tests passed before the abort. The complete
  selected batch, weak quote reclamation, public integration and image controls
  are not certified by that partial run. Commands, source and artifact hashes,
  and failures are preserved in `reader-owner-*-2.*` and
  `reader-owner-native-probe-alone-1.*` under `target/runtime-goal/`.
- Native cons decoding recursively traversed the duplicate representations.
  The current repair uses one explicit conversion work stack for decoding,
  encoding and reconciliation, with completion after descendants and a local
  cycle set. The persistent reconciliation set is removed. This conversion
  machinery is temporary and must disappear with the duplicate payloads;
  GNU's `lisp.h:XCAR/XCDR` need none of it. New small-stack tests cover deep car
  and cdr graphs, sharing, cycles, heap teardown and interpreter mutations.
  The unchanged failing probe and affected execution contracts must pass before
  this repair is validated. No performance benefit is claimed.


Closure integration (source checks and execution validation pending):

- The preceding native conversion checkpoint passed the unchanged 250,000
  cons probe and all 193 selected native/reader/quote/continuation/type/dump
  tests. The six GC/ownership contracts produced four passes and two failures:
  `suspended_bytecode_retains_operand_and_unwind_roots` and
  `threads_retain_lexical_caller_roots_across_separate_callee_environments`
  both returned `((1 t 1) (1 t 0))`, retaining one dead key in the first
  case. Their live-root checks passed. The same failures reproduce in the
  earlier reader/owner binary, before the stack repair and debug-symbol
  change. They remain unresolved; no assertion or input was changed.
  Raw inventories, outcomes and artifact identities are in
  `native-cons-stack-*-3.*` and `reader-owner-gc-triage-1.*` under
  `target/runtime-goal/`. These are development-profile diagnostics, not
  final gate, release, integration, frozen or performance certification.
- Interpreted closures now store their three to six actual Lisp slots inline
  in one vector allocation. Removed the parameter/body Rc vectors, source-body
  identity cache, reconstructed inspection lists, local-special-name cache,
  and separate dumped parameter/body records and loader construction maps.
  The header is still two words and native closures still use bridge handles;
  this does not complete the requested shared representation.
- Calls follow `eval.c:funcall_lambda`: walk the live parameter list once,
  bind arguments in order, retain errors and watcher effects, then execute
  the actual body. The reader, graph copier and dumper allocate closure
  identities before restoring fields that can lead back through cycles.
- Reader closure slot validation follows `lread.c:bytecode_from_rev_list`.
  Ordinary records named `interpreted-function` remain records. Equality,
  hashing and interactive metadata inspect stored slots directly.
- Removed sorting shortcuts that parsed a numeric lambda body or selected
  behavior from a predicate's symbol name. Sorting now calls the resolved
  function with ordinary bindings and errors. Rust's comparison schedule
  still differs from GNU Timsort and remains an explicit open finding.
- GNU controls for original and renamed closure sharing, mutation, binding
  order, cycles, reader types, and live/dead reclamation are preserved in
  `closure-authoritative-*-gnu-controls-1.json`. New regression tests and
  clone/dump graph checks exercise the corresponding Emaxx paths. Until
  those tests execute successfully, the implementation remains unvalidated.

- The first integrated compiler check found two stale users of the removed
  closure fields and one unused import. The text-property comparison now
  follows `intervals.c:intervals_equal` object identity, and `function-equal`
  follows `profiler.c:Ffunction_equal` code-slot identity for interpreted and
  bytecode closures. A local GNU differential control records the distinction
  between shared and copied bodies, interval boundaries, and self identity;
  Rust execution is pending. The failed check remains in
  `authoritative-closures-checks-and-build-1.json`.

- The next development build was intentionally stopped after 32 minutes in
  compilation. A one-second sample caught the compiler in local ThinLTO;
  no runtime tests ran from that incomplete artifact. Removed the obsolete
  binding-form name parser reported by the compiler. The retry uses
  `cargo --config profile.dev.package.emaxx.debug=0 rustc --profile test --lib`
  with `-C lto=off` only for the selected crate. A separate Cargo control
  verifies test mode, dependency reuse, optimization level 1, assertions and
  overflow checks. This is a build iteration setting, not a performance
  certification or a change to gate/release requirements. Raw controls and
  cancellation evidence are in `cargo-no-lto-control-*.json` and
  `authoritative-closures-build-cancellation-1.json`.


Shared vector access and reader consolidation (next checkpoint):

- Closure source 3 compiled without warnings. The exact 324-test inventory
  ran: 322 passed; two new reader contracts failed with invalid-read-syntax.
  All six GC/ownership contracts passed, including both previously failing
  suspended-root reclamation contracts with unchanged inputs and assertions.
  Evidence: authoritative-closures-source-3.json, dev-build-3.json and
  execution-4.json under target/runtime-goal. This is development validation;
  final gate/release/platform/frozen/performance requirements remain open.
- The reader failure exposed a second graph resolver that ran before the
  runtime could allocate record/closure identities. Removed that resolver,
  use the common object materializer at read boundaries, and publish vector
  identities before resolving their cyclic slots. GNU lread.c is the authority.
  Added a same-input GNU control across five reader entry points, three cyclic
  object classes and forced GC. GNU control 2 passes; control 1 records a test
  function-stream error (missing unread handling), not a runtime result.
- Vector slots now use Cell<Value> with direct loads and stores, following
  lisp.h:AREF/ASET without exposing aliased &Value or &mut Value references.
  Removed the mut_from_ref allowance and the graph copier's temporary slot
  vector. Corrected three tests whose raw-slice guards never enforced the
  exclusive borrow claimed by their comments; retain type assertions and use
  cyclic payloads. A retained iterator test exercises mutation and actual GC.
  This does not shrink the header or remove native vector bridge handles yet.
- Alternate-stack entry now registers its actual waiting caller's range with
  a scoped guard, sharing resume's existing driver-region mechanism. Removed
  the persistent OS-stack range and ensure panic cleanup. A new contract
  covers live OS/alternate parent roots and reclamation after normal and panic
  returns. No causal claim is made about the earlier two GC failures.
- Builds after this point can use a separate fixed validation checkout and
  the existing dependency cache, allowing implementation in the main working
  tree while the snapshot is checked. The next source remains unvalidated
  until its own checks and execution finish. No runtime speedup is claimed.

- The first consolidated all-target compiler check completed with no errors
  and two unused-code warnings; the validation driver correctly rejected it.
  Moved the now test-only unquote helper into the ERT test support module,
  and use the parser's existing placeholder flag to skip object-resolution
  walks for ordinary reads. No lint allowance was added. Source 2 reruns
  the required checks before generating the next development test binary.

- Source 2 passes fmt, all-target check, strict Clippy and diff checks without
  warnings. Its development build took 123 seconds (the preceding closure
  build took 1,134 seconds); cached dependencies and the fixed checkout were
  used, but this does not establish a cause or a runtime performance gain.
  All 386 selected tests executed: 384 passed, including cyclic closure
  identity, all five reader entry points, shared-vector mutation across GC,
  and nested alternate-stack normal/panic root reclamation. All six separate
  GC/ownership contracts passed again. The remaining type-of mismatch
  (interpreted closures reported as cons) is fixed against data.c:Ftype_of.
  The native relocation test failed before Emaxx execution because GNU's
  default compiler cache was unwritable. Both editors now receive the same
  explicit output filename in a unique temporary directory; GNU control 2
  passes unchanged native-function and interned-symbol identity assertions.
  Control 1 preserves an invalid directory-as-filename attempt. Source 3
  reruns validation; source 2's failed raw result remains preserved.

- Source 3 passes all 386 focused tests and all six GC/ownership contracts,
  with complete inventories, zero compiler/Clippy warnings and clean fmt/diff
  checks. The focused group took 805 seconds and the ownership group 52;
  these are validation durations under host load, not runtime benchmarks.
  Exact source and results are in shared-vector-reader-source-3.json and
  shared-vector-reader-execution-3.json. Full gate, public integration,
  release, both pinned platforms and the locked performance suite remain open.

Compact float allocation (implementation prepared; validation pending):

- FloatCell now contains only its eight-byte f64, following lisp.h:Lisp_Float.
  Allocation and mark bits occupy the containing 32 KiB aligned block; the
  collector derives their address by masking and indexing, as alloc.c's
  FLOAT_BLOCK/FLOAT_INDEX do. Release-build float reads remain direct payload
  loads, with no registry lookup or metadata access.
- GNU has one mark bitmap. The current Rust allocation block additionally
  uses one allocation bit per slot so checked native words can reject free
  slots, and one epoch per block to support the existing separate reachability
  passes. These replace per-float mark words. A block holds 3,969 payloads,
  1,016 bytes of metadata/padding and occupies 32,768 bytes; the previous
  implementation held 4,031 sixteen-byte cells in a 65,536-byte allocation.
  This records actual layout, not measured allocation volume, RSS or speedup.
  Allocation accounting for the complete runtime remains unfinished.
- The existing live/dead native-float GC contract is unchanged. Additional
  coverage retains arrays across several float blocks and repeated collection
  epochs. The native invalid-word test now changes the type tag instead of
  assuming that the following eight bytes cannot contain another live float.
  This change still needs its own compilation and execution checkpoint.

Vector headers and native words (separate draft; unvalidated):

- VectorHeader is one word, placing ordinary and interpreted-closure slots
  at lisp.h:Lisp_Vector's payload offset. Small-vector marks use a bitmap in
  their 4 KiB aligned allocation block; 4,048 bytes hold objects, with 40 bytes
  of metadata and eight padding bytes. Large vectors reserve 16 bytes past
  their rounded payload for the existing epoch mark. These costs must remain
  visible: the large-vector allocation does not necessarily shrink. Ordinary
  lengths, slot reads and stores consult no GC metadata.
- The block epoch supports the current independent reachability passes, as
  in the float draft. It is an integration constraint, not a claim that GNU
  needs epochs; eventual conversion of the collector to GNU's complete mark/
  sweep lifecycle must revisit these temporary metadata arrangements. No GC
  or allocation performance result is available for this draft.
- Native encoding now returns the actual word for ordinary vectors and
  interpreted closures, removing their NativeIdentity cases, handle allocation,
  identity-map entries and extra handle marking. The remaining bridge's existing tag
  word becomes a private header discriminator, without growing the handle.
  It must disappear with NativeHandle when all value encodings converge.
  Checked host decoding and conservative marking still validate allocation;
  live native decoding reads the object header directly. Native equality's
  positioned-symbol path likewise reads only the header of direct objects.
- New tests check GNU payload offsets and direct stores across collection,
  sizes around the small/large boundary, identity across native heaps, and
  both survival and reclamation of vector/closure cycles. A shared Lisp input
  compiles two differently named functions and varies sizes and element types
  through mutation, interpreted callbacks and forced GC. These tests have
  not run yet. Other object kinds still use different word encodings, so this
  does not establish the final uniform native slot ABI or the required
  two-word cons representation.

- The float source-1 checkpoint passes fmt, all-target check, strict Clippy
  and diff checks without warnings, all 45 focused tests and all six required
  GC/ownership contracts. The libtest artifact is preserved separately with
  SHA-256 d53d044467a5df62831a4cf1bc2d2cc330444891d2ff7ade39633965a7f79123.
  The first execution launcher ran before its build manifest existed and ran
  no tests; its failure is retained. Execution attempt 2 has complete passing
  inventories. Source/artifacts/results are compact-floats-*-1.json and
  compact-floats-execution-2.json under target/runtime-goal. No timing, release,
  full-gate or pinned-platform certification follows from this checkpoint.
- The vector source-1 targeted build failed in the pre-sweep diagnostic,
  whose old API read an in-object epoch word. The repair queries whether the
  object is marked in the specified epoch, preserving the same invariant
  check against unmarked children. No tests ran from the failed build. The
  failure remains in compact-vector-words-checks-and-build-1.json; source 2
  will retry. All-target check and strict Clippy are deferred to the next
  integration checkpoint for this vector change, not waived for completion.

Vector source-3 failure and follow-up:

- Source 3 compiled without warnings, but the 147-test focused group aborted
  after starting 117 tests in the new native vector fixture; no complete
  passing inventory exists. All six separate ownership/GC contracts passed.
  The exact-test replay also aborts and identifies a freed vectorlike read
  in native_handle_has_external_owner. Both failures and the artifact with
  SHA-256 d2a5fa933e9a37c143bb9f038f76a4324edf8e20465a0cdcce509654e9152cf8
  are retained under compact-vector-words-*-3 and native-fixture-backtrace-1.
- The next repair classifies the static builtin handles from their stored
  identity, without dereferencing weak payloads before tracing. When distinct
  buffer-reference allocations share a native identity, retaining that bridge
  also traces its exact stored reference. A new contract checks survival over
  repeated collections and reclamation after the equivalent Lisp root leaves.
  This is a temporary correctness repair for duplicate buffer references;
  canonical buffer objects and removal of their bridge remain required.
- The user authorized the repository's existing CI. Linux full-gate run
  https://github.com/rayfdj/emaxx/actions/runs/35765824945 targets 16da098bf8544b7449085456be68becb28100f9d,
  the previously passing float source-1 runtime checkpoint. One archived Lisp
  probe received whitespace-only cleanup because staging exposed whitespace
  errors not covered by unstaged git diff checks; its original bytes remain
  in the immutable float archive and linux-ci-original-thread-probe-1.el.
  Later vector/string/record/root-iterator drafts are outside that CI commit.
