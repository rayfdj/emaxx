The full objective is retained in [runtime-representation-goal.md](runtime-representation-goal.md).
This record tracks evidence and outstanding work; no completion is claimed.

Current checkpoint, 2026-09-26: the terminal migration has passed its selected
Linux and macOS controls; see [the terminal checkpoint](runtime-representation-terminal-checkpoint.md).
The evaluator correction is `2cf7984`, which replaces a redundant
`let` binding flag with GNU's direct environment comparison. Exact Linux machine
code identified the flag's spill as the source of both unwanted weak-key roots.
All19 unchanged Linux root controls now pass, including both reclamation
contracts, and the complete unchanged GNU eval file matches26/26 passing outcomes.
The prior failures and bytecode regression remain in the historical evidence.
`7c5fc2e` removes all temporary diagnostic tracing and passes the same Linux
19/19 root controls and26/26 GNU eval outcomes. Its fresh macOS binary passes
all25 affected evaluator/root controls. Formatting and strict all-target/
all-feature Clippy pass with zero Rust warnings on both validation paths.
Current-source full gates and GNU performance parity remain unproven.

Starting revision: `45eb1531caabb35b816fc83e2e8a5b88090dae4e`, freshly fetched
from `origin/main` on 2026-09-21. Implementation branch: `compact-runtime`.
The pre-existing September 21 audit and all its raw artifacts are preserved.

| Goal requirement | Required completion evidence | Current status |
| --- | --- | --- |
| 1. Reproducible starting point | Source, binary, image, toolchain and oracle identities; fresh reproductions; original and corrected baselines | Starting identities recorded; all six original evaluator findings reproduced; corrected release and own image built; original/corrected/GNU diagnostic timing comparison preserved |
| 2. GNU architectural reference | Per-change C owner, semantic comparison, explained necessary Rust deviations | Evaluator repairs trace to `eval.c:eval_sub`, `apply_lambda`, `Fautoload_do_load`, and `fns.c:list_length` |
| 3. Authoritative representation | One-word values; two-word conses; shared interpreter/VM/native payload; no ordinary-path mirror lookup or synchronization; other object kinds reviewed | Open; values are one word and buffers/terminals share object words; conses remain 80 bytes with duplicate payload and synchronization; marker/overlay/char-table/frame still use native bridges |
| 4. Allocation/GC/rooting | Correct accounting, live/dead-root controls, dumped-object treatment, enforced runtime ownership | Open; source65 passes all19 unchanged Linux root controls and25 macOS evaluator/root controls without temporary diagnostics; allocation accounting and Rust soundness still require completion |
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

- Vector prepared source 2 was not built. Source 3 retains its diagnostic
  repair and passes the empty vector as a fourth native argument in the new
  fixture, avoiding GNU's literal-vector equality warning without changing
  the 90 cases or assertions. GNU control 2 returns ((t t 45) (t t 45)) with
  no warnings; control 1, including its warnings, is retained. The targeted
  Rust source-3 build and runtime execution are still pending. This local
  GNU executable is diagnostic evidence, not the pinned macOS oracle.

Canonical string words (separate draft; unvalidated):

- Plain text and property-bearing strings now use their actual object
  address with Lisp_String's tag in all execution modes. Removed both native
  string identity variants, handle allocation, reverse lookup on live decode,
  and duplicate handle tracing. STRINGP is a direct tag test. Checked host
  decoding and conservative marking still validate allocated addresses.
- The two existing string storage forms remain. To distinguish them on a
  typed read, the existing plain-text storage-size word moves to offset zero;
  it cannot match the property-bearing form's private pseudovector header.
  GNU lisp.h:STRING_BYTES_BOUND bounds real lengths below that discriminator;
  the internal untracked-name sentinel also has a distinct header value.
  This adds no storage or lookup, but retains a header branch that must
  disappear when both forms become one Lisp_String allocation. StringCell
  remains 48 bytes, unlike GNU's 32; UTF-8/extended-character storage and
  mutable string-state ownership remain open. No measured speedup is claimed.
- New native contracts check canonical words in independent heaps, empty
  and untracked text, wrong-tag rejection, reachable property cycles, and
  eventual reclamation of previously reachable strings. Existing dead-string
  coverage is retained and supplemented with a remaining bridge kind so the
  handle reclamation assertion does not become vacuous. A shared Lisp fixture
  varies names, paths, lengths, character widths and callbacks through actual
  native, bytecode and interpreted execution with mutation and forced GC.
  Neither the new Rust tests nor this string GNU control has run yet.

Remaining allocated vectorlike words (following the string draft; unvalidated):

- Bignums, records and temporary reader forms now likewise expose their
  existing tagged allocation address to native code. Removed their native
  identity cases, handle allocation and duplicate handle marking. Ordinary
  live decode reads the actual header; only checked host decoding and GC
  inspect allocation metadata. GNU lisp.h:XUNTAG/PVEC_BIGNUM/PVEC_RECORD and
  alloc.c:mark_object are the references for object identity and tracing.
- Native positioned-symbol equality reads the record's actual fields after
  establishing its storage type, without a native handle or record-id lookup.
  The separate GET_SYMBOL_WITH_POSITION view is still present and must be
  removed when positioned-symbol fields have the GNU inline layout. General
  RecordState still uses a Vec and side tables for host pseudovector state;
  bignum payloads still use Rust BigInt, not GNU's GMP layout. This checkpoint
  removes word conversions but does not certify those payload ABIs.
- Added checks that separate interpreters' overlapping record-id spaces
  cannot merge their native identities, along with same-object words across
  native heaps and both survival and reclamation of record/reader/bignum
  graphs. Existing dead-string and dead-bignum coverage remains; a real marker
  additionally exercises reclamation of a remaining bridge handle. All of
  these edits are outside the immutable string source-1 checkpoint and await
  their own compilation and execution. No performance result is available.

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


Linux and vector checkpoint results (2026-09-23 local time):

- Linux float checkpoint 16da098bf8544b7449085456be68becb28100f9d
  passed runner build, formatting and strict Clippy. The full gate aborted
  in its first eval_01 test, a_store_into_a_full_keymaps_list_keeps_its_character_prefixes,
  with a freed-vector read through native run-hook-with-args. Its inventory,
  raw log and failed summary are retained under linux-ci-floats-artifacts-1.
  This invalidates any claim of full Linux correctness for that checkpoint.
- Vector source 4 builds locally without compiler warnings and completes its
  focused execution: 146 pass, two fail, and all six separate ownership/GC
  contracts pass. The original abort is absent from this run. The new buffer
  bridge regression incorrectly assumed the handle stores the second encoded
  reference; it actually retains the first. Source 5 roots only the second
  reference and requires survival of the first, exercising the intended GC
  edge with eventual reclamation unchanged. This is a correction to a newly
  introduced test setup, not a relaxation of a GNU semantic contract.
- The native vector fixture returns ((t t 45) (t nil 45)) instead of the GNU
  result ((t t 45) (t t 45)). All 90 cases still execute. Source 5 preserves
  the predicates and expected result and adds failure-only messages naming
  the predicate, function, length and replacement type. No passing outcome
  is claimed. Source 4's failed executable and complete raw inventory are
  preserved under compact-vector-words-source4 and compact-vector-words-*-4.
- The exact source-4 runtime is also under the unchanged Linux full-gate
  workflow in run https://github.com/rayfdj/emaxx/actions/runs/35778544417,
  commit 38aaa3137a413d9288e7cd5f09604f15f3c763ae. Formatting and strict
  Clippy passed; the validation step failed. Its raw failure is being examined.
  Later string, record and rooted-iterator drafts remain outside this run.


Between-call native relocation rooting (source 7):

- A new regression on source 6 fails before the runtime repair: a native
  relocation still points at a vector, but collection between generated
  calls frees that vector. The exact failing binary and raw assertion are
  preserved under compact-vector-words-source6 and
  native-relocation-roots-before-repair-1. This is an independently reproduced
  defect; it is not yet established as the cause of the Linux gate abort.
- Removed the separate no-active-native-stack collection branch. Collection
  now follows the same native/Lisp mark path in both states, preserving
  loaded relocation roots and bridge edges, while an absent native stack
  contributes no stack roots. GNU comp.c:load_comp_unit retains permanent
  data through its unit, and alloc.c marks all reachable objects independently
  of whether generated code is currently executing. No new root registry or
  per-operation lookup is introduced. The regression also requires collection
  of unrelated objects and reclamation after the relocation is cleared.
- The grouped gate now defaults RUST_BACKTRACE to 1 and records it, while
  respecting any explicitly supplied value. This retains the original panic
  stack before a native ABI abort, without changing selectors or outcomes.
- Source 5's corrected buffer regression passes, but its native fixture
  still aborts in vector allocation metadata. The two earlier predicates'
  failure-only diagnostics did not get a chance to report an equality
  mismatch in that run. All six separate ownership contracts again pass.
  Source 5's macOS replay of the first failed Linux gate test passes; Linux
  remains failed and cannot be certified by that cross-platform replay.


Standalone correctness milestone (not full-goal completion):

- The user asked for a committable/pushable milestone. The relocation-root
  defect also exists at the recorded remote-main baseline, so its repair
  and regression have been extracted directly onto that baseline in
  fix/native-relocation-root-lifetime. Only src/lisp/native_comp/runtime.rs
  and src/lisp/eval.rs change (78 additions, 27 removals). Representation
  drafts are preserved separately. The regression checks allocated payloads
  as well as successful bridge decoding, so an Ok result containing a freed
  object cannot satisfy survival coverage. Full CI validation is pending.
- The vector source-7 CI run stopped at strict Clippy: removing the second
  production collection path left weak_hash_reachability used only by tests.
  It is now compiled only for tests; no warning suppression is added. The
  original failure is retained in linux-ci-vectors-failure-2.log. Source 7
  passed the buffer and relocation GC regressions locally, but its vector
  fixture aborted while NativeMark followed a freed cons. Six ownership
  contracts passed. A draft regression for stale cons views is prepared,
  uncompiled, and excluded from the standalone milestone.
- Gate-tool checks exposed an old assertion expecting two workers after the
  gate was serialized to one. The assertion now requires the stricter serial
  setting; all twelve Python gate tests pass. Both outputs are retained under
  grouped-gate-python-tests-*-thread-expectation-repair-1. This correction is
  separate from the standalone runtime commit.


Standalone milestone published and macOS validated:

- fix/native-relocation-root-lifetime is pushed at
  e7e7bb08a7102baa2ddca243553141980b2a4d9f. Its three separate commits are
  697bacf (native relocation rooting), a609219 (platform dispatch attributes
  and the two Darwin Env constructors), and e7e7bb0 (twelve baseline Clippy
  warnings fixed without allowances). The complete branch changes ten files,
  237 additions and 80 deletions, relative to the immutable baseline.
- This exact source passes macOS cargo fmt, locked/offline all-target/all-feature
  check, strict Clippy -D warnings, and git diff --check, with no compiler
  or Cargo-cache warnings. All 109 focused native/GC/dump/dispatch tests and
  both affected Darwin interface/kqueue tests pass with complete inventories.
  Native-relocation-milestone-*-3 records retain source, compiler, artifact,
  commands and outputs; libtest SHA-256 is
  20b9675b79cb98c1825af0c56014b9af4b5e5e1bd621bb3739c8894bed6855d2.
  The execution build is diagnostic opt-level 1, assertions/overflow checks
  enabled, without crate debug symbols or LTO. No release or frozen claim.
- Exact-source Linux full-gate run:
  https://github.com/rayfdj/emaxx/actions/runs/35839862201.
  The earlier GC-only commit's run 35838466185 is separate evidence and was
  still executing its gate after passing Linux build/format/Clippy checks.
  Full Linux outcome, full macOS gate, frozen validation and the performance
  objective remain open. Finish this milestone before expanding the larger
  representation changes. The main workspace and its drafts remain intact.

Standalone milestone follow-up and return to representation validation:

- Both earlier milestone Linux gates failed in eval_01. Run 35838466185
  reported a freed-vector read through native set-buffer in the second test;
  run 35839862201 ended in that same test without a libtest result. Neither
  run is a passing gate. Their complete inventories and raw artifacts are
  retained under native-relocation-milestone-linux-artifacts-{1,3}.
- The exact-buffer-reference defect was reproduced independently against
  e7e7bb0: a live native bridge held a freed BufferRef allocation after only
  an equivalent buffer reference was rooted. Commit 62c4143 adds the missing
  tracing edge and a regression proving survival over three collections and
  reclamation of the bridge and both reference allocations. This repairs
  existing duplicate references; it does not replace the required canonical
  buffer object. GNU alloc.c marks its buffer allocation directly.
- Commit b3e74b7 preserves each gate process's exit code, command and expected
  inventory even when no libtest result is printed. Backtraces are enabled
  and recorded by default. Deliberate missing/incomplete reports and failed
  producers remain failures. No selector, timeout or ignored-test policy is
  relaxed. All 13 gate-tool tests pass.
- The pushed milestone at b3e74b729411713085b4096a649c3b07a0dd0218 passes
  114 focused macOS Rust tests, fmt, all-target check, strict Clippy and diff
  checks. Native-buffer-lifetime-source-4 preserves the failing control;
  source-5, macos-checks-5 and macos-controls-5 preserve the repaired source
  and results. The final commit tree was checked against that exact patch.
  Linux run https://github.com/rayfdj/emaxx/actions/runs/35841657491 is pending.
- The larger vector checkpoint is now building source 8 with the stale-cons
  regression and stronger payload survival/reclamation assertions. Its
  string, record and rooted-iterator successors remain separate drafts.
  No new performance measurement, full gate, pinned frozen result or final
  representation claim follows from the standalone milestone.

### 2026-09-23: loader GC ownership diagnosis

The isolated correctness branch was pushed through `b3e74b729411713085b4096a649c3b07a0dd0218`. Linux run [35841657491](https://github.com/rayfdj/emaxx/actions/runs/35841657491) passed build, formatting and strict Clippy, then completed the first 375-test group with 373 passes and two normal-mode selection failures. The remaining groups did not run. Commit `16cd957` adds GNU Lisp messages to those failure reports without changing the original Lisp inputs or expected results; full Linux run [35845247552](https://github.com/rayfdj/emaxx/actions/runs/35845247552) is pending.

Broad representation source8 reproduces a freed-cons read from stale native heap indexes; source9 rejects swept cons entries before marking and passes that regression. The full native vector fixture still fails. Source13 diagnostic evidence identifies a distinct ownership hole: native heap1 retains a buffer through mark epoch242, then temporary empty heap443 sweeps that same allocation at epoch243 during the next compile/load transition. Later native calls retain the stale bridge. Source13 ends with an ordinary failure in all three renamed buffer callback cases; earlier source12 aborts on the freed header. Exact sources, binaries, raw logs and inventories are under `target/runtime-goal/compact-vector-words-*`. These are diagnostic results, not performance or final-source certification.

Source14 removes all temporary diagnostics and adds a loader-scoped relocation regression with both survival and teardown reclamation checks. Its before-fix execution is pending. Shared 16-byte conses, remaining canonical object work, final validation, adversarial review and the locked performance criteria remain open.

The loader regression fails on source14 and passes on source15. Source15 completes **158** focused checks: 2 loader, 150 native/type/dump/reader/vector, and 6 ownership tests. The exact current 90-case fixture also passes local GNU control3 with unchanged inputs and empty stderr. This is the first passing full focused vector checkpoint in this series; broad all-target/Clippy/final validation remains open.

The repair is separately committed and pushed as `a64b79cd5c79654013efa3c92fdfe3b72af67ff0` on `fix/native-relocation-root-lifetime`. Its own source6 milestone binary passes **113** focused tests, including both normal-mode tests; macOS fmt, all-target check, strict Clippy and diff checks pass with zero warnings. Receipt: `target/runtime-goal/native-loader-runtime-commit-6.json`. Linux [35846754481](https://github.com/rayfdj/emaxx/actions/runs/35846754481) is pending. Prior diagnostic run35845247552 failed earlier than the mode tests, with a freed native unwind cleanup in `defvar_keymap_supports_custom_setters_toggling_bindings`; raw process outcome is abort(-6), not a missing or successful report. Source16 adds a focused pending-unwind liveness/execution/reclamation regression and is building before any unwind-root repair. No merge readiness or performance improvement is established.

The native-unwind regression fails on source16 (`a pending native cleanup was freed`). Source17 traces those roots and passes 159 focused checks, including survival, exactly-once cleanup execution and eventual reclamation. Review then found its new trace also followed historical binding-restore copies, which could retain values beyond GNU's canonical specbinding roots. Source18 removes that duplicate implementation and reuses the existing native frame root view for both executing and suspended calls; pending errors and handler match values are covered by that same view. This revision is prepared but not yet validated.

The a64b79c Linux run35846754481 failed at the same native-unwind cleanup, abort(-6); its raw artifacts are retained in `native-loader-runtime-linux-artifacts-a64b79c`. Milestone source7 did not build because the regression used the broad rewrite's Vec closure constructor; its compiler failure remains archived. Source8 adapts only that test constructor to the small branch's Rc API and is validating the shared native frame root view, including actual native stack-switch, non-LIFO return and teardown controls. It is not yet committed.

The corrected shared native frame trace is committed and pushed as `dab4750402aae1e5a2e730f9252095cc04af39bf`, following the loader ownership fix `a64b79cd5c79654013efa3c92fdfe3b72af67ff0`. On the exact small-branch source, **119 tests pass with zero failures/ignores**, including the pending-cleanup survival/execution/reclamation regression, actual native stack switches, non-LIFO returns, teardown, and macOS controls for the Linux failures. Formatting, all-target compilation, strict Clippy and diff checks pass with zero warnings. Receipt: `target/runtime-goal/native-frame-roots-commit-8.json`. The existing full Linux workflow is dispatched for this commit; no full-gate pass or merge readiness is claimed. Broad source18 validation is now running and remains distinct from the passing milestone.


Source18 validation is rejected: the shared Cargo target reused the small milestone binary, whose inventory omits broad tests. No execution from that artifact certifies source18. An attempted cache copy overlapped source19 compilation; both task-owned processes were stopped and reaped, and that attempt is explicitly aborted. Source20 preserves the same source patch (`404bcef2d6aadb22d1ae7bf52c8d82faab3b25da214fede6de272aaf3a57889b`), cleans only the new isolated `target/runtime-broad`, and requires a fresh target-crate compile plus the predeclared exact inventory. All **163** expected test executions (162 distinct tests; the one-word Value assertion is selected in two groups) pass with zero failures or ignores (4 native-thread, 2 loader, 151 native/type/dump/reader/vector, 6 ownership). Source identities are verified before and after execution; preserved binary SHA-256 is `2d184bcf0308f7e37fcb2045033d92aa3961250ce90286c8a5f76e87cedb0558`. Full gate, all-target/Clippy integration, release, frozen and performance remain open. The pushed correctness milestone remains `dab4750`; Linux run35850332057 is still executing its full gate after passing build/format/Clippy.


Linux run35850332057 finishes as a failure: eval_01 through eval_04 pass **1,234** tests, with zero failures/ignores; eval_05 aborts (-6) in `eieio_method_invocation_order_and_next_arguments_stay_coherent`. Its backtrace reaches `Value::native_handle_has_external_owner`, which inspects an already reclaimed vector payload in a weak bridge entry; unwinding then encounters a stale cons view. Later groups do not execute. Raw artifacts are under `native-frame-roots-linux-artifacts-dab4750`. The next bounded repair selects the existing immortal identities without dereferencing weak payloads and retires swept cons entries before field reads; both mechanisms already exist in the broader representation work. Added controls preserve a live vector across foreign collections, prove unreachable payloads are actually freed, then require bridge retirement and eventual reclamation after the live root is removed. It is not yet validated or committed.

The unchanged 72-case string fixture passes local GNU control1, with stdout `((t t t t 36) (t t t t 36))` and empty stderr. Broad source21 fails compilation in two new test constructors; source22 corrects those constructors and builds without warnings. Its fixed inventory runs **173 executions of 172 distinct tests**: 171 pass, two fail. The string fixture returns `((t t t nil 36) (t t t nil 36))`; no GNU agreement is claimed. The native record reclamation test also retains one record because its previously opaque native word is now the actual tagged object pointer on the scanned caller stack. An unvalidated correction confines that word to a separate live-root frame while preserving the exact live census, identity, unreachable-control and eventual-reclamation assertions. Rooted owning/drain iterator liveness/reclamation and panic/drop tests pass in source22. All failures remain preserved; broad final validation and performance remain open.


The bounded GC follow-up is committed and pushed as `9220d3a8c209de9b9b155c66914c9e5b03e2f2f5`, parent `dab4750`, on `fix/native-relocation-root-lifetime`. Exact tested patch SHA-256: `d5788c816dee33d47c1c23eac2190c9a5b561117d1d395763e32d0b9ee2f4d43`. **122 distinct tests pass with zero failures/ignores**, including the actual EIEIO crash control, live/dead weak bridge retirement after a foreign collection, stale cons addresses, native stack switching and dump restoration. Source9 preserves a test API compile error; source10 fixes that constructor. Validation rejects an inherited empty dispatch filter before execution, then runs the same freshly compiled binary with all 122 predeclared names unchanged. Receipt: `native-weak-handles-execution-10.json`; binary SHA-256 `f1b6b49fd6c740f7769f61929914b7b65542ba831842f30949c53d74eef392e0`.

Formatting passes. Local all-target compilation is still running, with strict Clippy/diff queued. To overlap those slow local checks with Linux, the published commit was created from an isolated index without moving the validation checkout's HEAD or changing any tested bytes. Commit patch equality and remote branch identity were verified. Existing full Linux CI [35854391416](https://github.com/rayfdj/emaxx/actions/runs/35854391416) runs at `9220d3a`; no complete gate or zero-Clippy claim is made for this latest candidate yet. The wider rewrite and measured performance objective remain open.


Checkpoint23 is published as diagnostic WIP commit `e7e3972e7f96086712d08128d93d79766d50717c` on `wip/compact-runtime-shared-object-words`. It preserves the original string fixture, adds a separate temporary predicate/case diagnostic, and keeps the corrected record live/dead assertions. Every captured changed/untracked file is checked against the committed tree; the existing workflow is byte-for-byte unchanged. Inputs are `validation=affected`, `file=test/src/data-tests.el`, `rust_filter=diagnose_native_string_mutation_predicates`; exactly one runtime test is required in its raw log. A failed runtime diagnostic stops the subsequent compatibility file and remains failed. This WIP is not a completed architecture/performance milestone.

Local resource inspection found 24 GiB RAM, about 14 GiB swap in use and substantial memory pressure while other applications run. No other application was changed. The pushed correctness candidate's macOS all-target check eventually passes after roughly22 minutes; strict Clippy is still running. CI supplies independent Linux build/test work while these local checks finish. No timing under this host load is performance evidence.

The exact `9220d3a` runtime source now passes all four macOS static checks: formatting, locked all-target/all-feature compilation, strict Clippy with `-D warnings`, and diff whitespace. Session35204 completed and was reaped. Its 122 focused tests remain passing. Full Linux run35854391416 fails with SIGSEGV in the EIEIO method-invocation test after the first four groups pass 1,234 tests. This remains a failed gate.

Existing-workflow diagnostic commit `ad3cf94958e5cec26a869f23014fe1052b1bb70a` changes no Rust runtime source. Linux [35857493614](https://github.com/rayfdj/emaxx/actions/runs/35857493614) replays the unchanged 351-test eval_05 inventory under GDB and reaches the same EIEIO crash after starting 130 tests. `LispReachability::enqueue` receives the allocator's `0xa5a5a5a5a5a5a5a5` poison during graph traversal. The artifact records a failed process, unchanged source and complete inventory. Follow-up diagnostic commit `79f80f050ea854d9eedf62fbe0e79985a241f6ae` enables line tables and the existing GC verifier only for this diagnostic mode; [35859878598](https://github.com/rayfdj/emaxx/actions/runs/35859878598) is pending. Neither replay certifies the full gate.

Broad source23's Linux diagnostic runs exactly one test and fails, preserving its full case output. Character stores and callback identity agree; text-property value identity differs. The cause is an existing `put-text-property` branch that copies a property-free string value before storing it. GNU `textprop.c:Fput_text_property` stores the original object and returns nil. Commit `2399c663f3427d83058948cdb1e3f444eb9f8973`, pushed on `wip/compact-runtime-shared-object-words`, removes that copy and restores the nil return. Twelve new differential cases cover renamed properties, string/current-buffer/explicit-buffer targets, unibyte/multibyte values, collection and subsequent mutation. Local GNU control passes all twelve with empty stderr. The original 72-case native fixture is unchanged, and the temporary predicate diagnostic has been removed.

Source24's Rust changes pass the changed-file rustfmt check and diff check. Existing Linux CI [35860044928](https://github.com/rayfdj/emaxx/actions/runs/35860044928) is dispatched with the predeclared 150-test `string_` inventory followed by the ordinary affected `test/src/data-tests.el` comparison. Full Rust outcomes are pending; the WIP workflow is unchanged. Conses remain 80 bytes, canonical representation work is unfinished, and no performance gain or merge readiness is established.


Linux source24 (`2399c66`) passes all **150** predeclared string tests, including the unchanged 72-case native fixture and the new 12-case property-identity differential test. Build, formatting and strict Clippy pass. The subsequent affected comparison stops at an obsolete audit requirement for the removed symbol handle cache. Source25 (`9dae6a0`) replaces only those requirements with four canonical-symbol identity, direct-access and survival/reclamation contracts; all other guards remain. Existing CI35862190750 is running the new cross-collector symbol test and ordinary affected data-tests.el comparison. No full compatibility pass is claimed.

The heap-verified Linux replay at `79f80f0` fails earlier, before the sixth eval_05 test completes: a marked cons still contains an unmarked buffer allocation. A new focused regression reproduces this missing tracing edge locally. Native and interpreter fields can reference distinct descriptors of the same logical buffer; skipping the typed field after tracing the native field frees an allocation the interpreter still uses. The repair traces both actual allocations after synchronizing reached views. GNU alloc.c marks the single actual cons payload; this extra traversal is temporary cost required by the existing duplicate representations and must disappear with canonical cons/buffer payloads.

A separate cross-collector uninterned-symbol regression reproduces SIGSEGV(-11) before repair and passes after removing ordinary symbols from unconditional immortal native roots. GNU alloc.c:survives_gc_p requires their mark bit. Both regressions check live allocations and eventual reclamation, including bridge retirement; no permanent retention is substituted. Before/after logs are in native-symbol-lifetime-{before,after}-1. The small correctness source11 freezes both repairs for the unchanged 122-test inventory plus these two new controls, followed by formatting, all-target checks, strict Clippy and diff validation. Main and the broad draft contain the cons repair too. This is correctness work; conses remain80 bytes and there is no verified performance improvement.


Source25 existing CI35862190750 completes successfully. Raw artifacts confirm exactly one passing cross-collector symbol test and **57/57** matching GNU data-tests.el outcomes, zero skips or mismatches, and successful subject/oracle processes. Provenance records clean subject commit9dae6a0 and both executable/image identities. Formatting and strict all-target Clippy pass in the workflow. The same raw run reports a **12.49x** Rust/GNU body-time ratio (1961ms/157ms), retained as an unfavorable single CI observation, not an authoritative repeated performance comparison. Receipt: compact-object-words-ci-25.json. This is a bounded compatibility checkpoint; complete platform frozen/gate/performance requirements remain open.


The two-file GC candidate is committed and pushed as `ee5c2f42cbc9fdede1ab02a4882d4ba81c27957a`, parent79f80f0, on `fix/native-relocation-root-lifetime`. Its committed patch exactly matches frozen source11 (`965c9ccb1d814d9b861ce1767fb61f0173d99193afa7934c559ff0b147ba4d0b`). Fresh macOS diagnostic compilation passes with zero warnings; binary SHA-256 is `8e78275b18d553ec81d4948e599e3a742e04daa889197b96d5a6875b6f6fc7b9`. The124-test run remains in progress, followed by queued static checks. A brief sample shows active Lisp loading/evaluation in the normal-mode control after earlier tests advance; it is not timing evidence. The candidate is sent to the existing full Linux gate at https://github.com/rayfdj/emaxx/actions/runs/35864645252 while macOS validation continues. CI reports the exact ee5c2f4 head. No full-gate, final zero-warning or merge-readiness claim is made for this candidate yet.


The exact pushed `ee5c2f4` source now passes **124/124** predeclared focused tests, zero failures or ignores, in591.73s. The before-fix buffer failure and symbol SIGSEGV are both followed by passing survival/reclamation controls; native mutation, current-field tracing, weak roots, suspended native stacks and dumping controls remain in the unchanged earlier122-test inventory. Source/commit patch equality is verified after execution. Formatting also passes. MacOS all-target checking is running, strict Clippy/diff are queued, and full Linux CI35864645252 remains active. Receipt: native-buffer-lifetime-source-11.json and native-lifetime-commit-11.json. Architecture and performance completion remain open.


All macOS static checks for exact `ee5c2f4` pass with zero warnings: fmt, locked all-target/all-feature check, strict Clippy and diff. The isolated checkout is clean at that commit after validation completes. Full Linux run35864645252 remains a failure: eval_01..04 exit successfully; eval_05 starts130 of351 tests and aborts in the EIEIO method-invocation case. The first error is NativeMark reconciliation rejecting an unknown native cons address; destructor cleanup triggers another rejection and abort. Heap-verified source-line replay35866819705 advances past the earlier sixth-test unmarked-buffer failure and stops at the same EIEIO error. Neither run is a full gate pass.

The GC cons-view repair is integrated into the representation branch as `51b9847fa5d65ce5e86c965e2b4f147b2888492c` (source26). Its202-file captured manifest is checked against the commit; only eval.rs and native_comp/runtime.rs differ from9dae6a0. The workflow is unchanged. Existing full Linux gate35867561222 runs that exact integrated source.

An additional adversarial control now exposes a direct representation defect. Source27 constructs a fresh Rust cons containing37, places it in a canonical vector, and passes only the vector through the native ABI. A native entry performs GNU-layout AREF then XCAR without an intervening primitive wrapper. It reads nil(word0), where the stored fixnum is word150. The ConsCell constructor still initializes a separate native prefix to zero, and encoding the vector leaves the nested cons unprepared. The failing assertion and fresh artifact are preserved in canonical-cons-controls-27.json; binary SHA-256 is0fe1f3ce281804770117b4da15bd5de1b18827602057bd0c05b98ba28d9dadbd. This proves the authoritative shared-payload requirement is not met; it does not by itself identify the Linux EIEIO cause.

The existing90-case native vector fixture is strengthened to read car/cadr of its list element, preserving names, sizes, replacements and expected outcomes. Both GNUcontrol4 and the Rust fixture pass; the raw native-layout control fails independently. Passing Lisp behavior therefore does not certify the underlying shared payload. Source27 passes formatting, diff and a fresh warning-free diagnostic build in the isolated broad target. Its overall two-control result remains FAILED(1pass,1failure), with exact inventories and source verification retained. These test changes are in main and the broad worktree but are not yet committed. They must remain requirements while removing duplicate cons storage and the remaining object conversions; no performance improvement is claimed.


Integrated source26 full Linux gate35867561222 fails in `cl_defmethod_supports_setf_function_names`: `(funcall #'(setf sample-slot)42 nil)` reports invalid-function after the method definition. Build, formatting and strict all-target Clippy pass; eval_01/02 pass, eval_03 reports319passes/1failure, and later groups do not run. The2769-test inventory retains only the two existing TTY ignores; both in-scope GC tests are restored but not reached. Raw evidence is under compact-words-linux-artifacts-51b9847; receipt compact-object-words-ci-26.json. A separate exact macOS control uses preserved source27 binary with image-template mode1; it is not a substitute for the Linux group outcome.


GNU also rejects the original setter test input with `(invalid-function (setf sample-slot))`; the local expectation of42 was invalid. Local GNU reference revision636f166c and executable SHA-25612c762a0 are unchanged. Source28 retains that exact input and verifies the error condition/data, then exercises both supported paths, `(funcall (gv-setter 'sample-slot)42 nil)` and `(setf (sample-slot nil)57)`. GNU and the fresh Rust artifact both return `(t 42 57)`. The existing test name and inventory are unchanged. Receipts: setf-generic-gnu-control-1.json, setf-generic-gnu-corrected-2.json, canonical-setf-controls-28.json. Formatting, diff checks and fresh local all-feature libtest compilation pass with zero warnings; the exact Rust test passes1/1, zero ignores. This corrects an invalid local test against GNU evidence; production behavior is unchanged.

The source27 raw native AREF/XCAR failure remains open and preserved. Passing the corrected setter control does not certify the compact architecture, Linux full gate, frozen comparisons or performance. The next CI checkpoint takes only the GNU-backed setter-test correction atop51b9847; the local failing cons regression and strengthened vector fixture remain explicit architecture acceptance controls.


The one-file GNU setter-contract correction is committed and pushed as `bfff40b5ab2333580c801d9e440462880919b24b`, parent51b9847, on `wip/compact-runtime-shared-object-words`. Only eval/tests/eval_03.rs changes; the workflow and all production bytes are unchanged. The source28 tested file is verified against the committed blob; worktree HEAD and other local changes are preserved. Existing full Linux [run35871733291](https://github.com/rayfdj/emaxx/actions/runs/35871733291) is in progress on the verified exact head. Receipt: compact-setf-contract-ci-29.json. This is a reviewable test correction; the known failing raw cons probe, 80-byte cons size, incomplete full validation and unverified performance remain explicit.


Full Linux run35871733291 at bfff40b5 passes the corrected setter group320/320 and the other first four groups, for1246 passes and zero ignores in those groups. Eval05 then aborts(-6) in EIEIO at NativeMark reconciliation of an unknown native cons address, followed by another rejection during destructor cleanup. The full gate remains FAILED; its2769-test inventory and raw logs are preserved in compact-setf-linux-artifacts-bfff40b5 and compact-setf-contract-ci-29.json.

The next architectural change replaces finalizer ids, callback lookup records and native bridge handles with an actual32-byte PVEC_FINALIZER allocation containing the callback and two weak list links. Active/doomed lists reference those objects; the callback is cleared in the object before invocation. The test image copier memoizes finalizer objects and cycles; dumping relocates actual links and callback slots. Finalizers are counted as one vector/four words and charge32 allocated Lisp bytes. The two per-interpreter list sentinels use stable shared ownership because shallow interpreter clones share the Lisp graph; this is host list metadata, never an ordinary object-access lookup. Ordinary reads and native transitions use the object's own tagged word. Shared conses and the other remaining bridge kinds are still unfinished.

Before-fix source30 adds the direct finalizer layout control to otherwise unchanged source28. It fails at the word-identity assertion (native2199039548485 versus interpreter321), with exit101 and zero passes. Exact binary SHA-256:a118512e743b2dcf2826cb1da543c4f4a47fda52b23d01d59b9883b703998767. The same test is retained unchanged in the rewrite. Source31 is frozen and compiling; no post-change test result or performance claim exists yet. New controls cover allocation census, foreign collection survival and eventual reclamation, and cyclic image-copy isolation. The collector queues all owners' doomed objects before marking their functions and before the weak-table fixed point, following alloc.c's phase order. A main-only collector branch correction found during review is reserved for source32 after the active source31 build finishes.


Source31 formatting, diff checks and the fresh all-feature libtest build pass with zero warnings. The reviewed collector dispatch follow-up and the foreign-GC regression with an existing builtin bridge are frozen as candidate32, committed and pushed as `fbb7b6e14a2b965d1f265cd423b7cd3d1c40b047`, parentbfff40b5. All203 captured files are verified against its tree, and the existing workflow is unchanged. The commit also retains the previously local failing cons-layout regression and the strengthened vector fixture, explicitly documented as unresolved WIP requirements. Linux [run35875353256](https://github.com/rayfdj/emaxx/actions/runs/35875353256) uses that exact head with `validation=affected`, `rust_filter=finalizer`, and unchanged `test/src/alloc-tests.el`. Exact candidate32 local build and48 focused controls are in progress; no post-change execution pass is claimed yet.


## Finalizer test corrections: pushed checkpoint 21fd82c

The canonical finalizer candidate fbb7b6e completed 46 of 48 focused local
controls. All four new finalizer controls passed. Linux 35875353256 stopped
at four new-test `unwrap` Clippy errors; its runtime controls did not execute.
The four calls now have explicit `expect` messages, with no lint allowances.

Both local failures have independent explanations and retained receipts:

- `canonical-finalizer-controls-33.json` preserves a diagnostic failure that
  first verifies all three unrelated record/reader/bignum objects are freed,
  then proves the comparison bignum reuses the dead bignum's address. The
  corrected test checks dead addresses before that allocation. All live-root,
  cycle, numeric-value and later-reclamation assertions remain. GNU alloc.c's
  vector free lists likewise reuse swept addresses.
- `finalizer-error-gnu-control-1.json` records actual GNU returning an empty
  log for the original immediate-collection program, then the expected error
  log and invocation counts `(1 1 ...)` for later collections. The corrected
  test executes the same program in GNU and Rust, retains the error-log
  assertion, and adds exactly-once coverage.

Commit 21fd82c7f592c713a4193a579ca54eff870b900f contains these test/lint
corrections only, on the existing WIP architecture branch. The repository's
Linux run https://github.com/rayfdj/emaxx/actions/runs/35879635346 was
dispatched with the unchanged `finalizer` filter and `test/src/alloc-tests.el`.
Its result is pending. No required full-gate inventory has been reduced.

Local source34 stopped at a formatting difference, preserved in its receipt.
Source35 compiled with zero warnings and passed the corrected reclamation
control; its full 48-control execution remains in progress. Review caught an
incorrect literal backslash-n in its GNU expected string. The pushed source36
uses the actual newline, checked byte-for-byte against retained GNU stdout;
it still needs exact-source local validation after the frozen source35 job.

Conses remain 80 bytes; the raw vector-to-cons native read still fails and
the broader Linux gate still has an EIEIO unknown-cons abort. This checkpoint
does not establish merge readiness, final architecture, or any speedup.


## Exact finalizer checkpoint validation: 21fd82c

The exact pushed candidate now passes macOS formatting, all-target checking,
strict Clippy, diff checking and a fresh all-feature test build with zero
warnings. All 48 expected focused tests pass with zero failures or ignores;
source hashes were verified before and after the suite. Receipts are
`compact-vector-words-checks-and-build-36.json` and
`canonical-finalizer-controls-36.json` under `target/runtime-goal`.

Linux 35879635346 passes all 7 expected finalizer controls and matches all
5 tests in unchanged GNU `test/src/alloc-tests.el`, with zero skipped tests.
The downloaded raw inventory, execution-receipt hashes, exact clean subject
commit, GNU revision and executable/image identities were checked. The
ordinary CI timing remains unfavorable: GNU body 8 ms versus Rust 51 ms
(reported ratio 6.374x), with setup 84 ms versus 636 ms. This short single
sample is retained and is not a locked-suite performance result.

The full Linux gate on the same commit is run 35882080795. Its result is
separate from the bounded finalizer pass. Full architecture, all required
gates, adversarial review and performance parity remain open.

Main has two new, not-yet-executed controls for the next step: a direct
GNU-layout builtin-word/subr-field/function-call regression and a
`cl-type-of` redefinition/alias/saved-subr regression. GNU returns
`(117 118 119 fixnum 121)` for the latter program. Its native-override
metadata currently precedes function-cell resolution and needs an actual
Rust before/after control. Source37 captures these tests with production
bytes unchanged from the validated finalizer candidate.

The next builtin conversion must eliminate ordinary reconstruction and
name lookup, not merely allocate a new wrapper. Current native funcall uses
`SUBR_INDICES`, rereads descriptor metadata, and reconstructs the function
value by name even on successful calls. Interpreter builtin resolution also
reconstructs values. Symbol value/function/property state remains in
per-interpreter tables; a canonical symbol identity alone does not certify
an authoritative GNU-layout symbol payload.


## Redefinition bypass reproduced; exact full Linux result

Full Linux run35882080795 at21fd82c is FAILED. The first four groups pass
1246 tests with zero ignores; eval05 aborts during
`eieio_method_invocation_order_and_next_arguments_stay_coherent`. NativeMark
rejects an unknown native cons address, followed by another rejection
during destructor cleanup. The raw2774-test inventory and two existing
TTY ignores are retained. This is separate from the passing finalizer checks.

Source37 compiles with zero warnings and both new adversarial controls fail.
The builtin probe fails before raw dereferencing: the native handle word
differs from the interpreter builtin word. The cl-type-of program returns
`(fixnum fixnum 119 fixnum fixnum)` instead of GNU's
`(117 118 119 fixnum 121)`: direct calls, symbolic funcall and aliases bypass
the redefined function cell, while fetching that cell observes the replacement.
Receipts: `canonical-builtin-controls-37.json`, preserved binary
SHA-256`de307349aca07f153069c3554c7d17de83769304c9cf6c7d99b529ff64452346`,
and `cl-type-redefinition-gnu-control-1.json`. No behavior is inferred from
test names; this is the same ordinary Lisp program in GNU and Rust.

Source38 removes the single production `builtin_override` annotation from
cl-type-of. The unchanged before-fix regression and17 related existing
redefinition, alias, arity, dispatch and type controls are being validated,
along with fmt, all-target checking and strict Clippy. The raw builtin and
cons representation regressions remain unignored in source and their
failures remain open. No post-change pass or performance gain is claimed yet.


## Builtin redefinition checkpoint db484d4 and representation draft

Source38 stopped after successful type checking because removal of the
last production override exposed an unused macro. Source39 makes that
selector test-only with cfg(test), retaining the synthetic platform-attribute
control and adding no lint allowance. Commit
`db484d46ab422dea16772511e97663c5418c52b1` is pushed to the existing WIP
branch. Its exact macOS fmt, all-target check, strict Clippy, diff and fresh
libtest build pass with zero warnings. The unchanged before-fix cl-type-of
regression now passes; the18-control supporting suite is still executing.
Linux35888335902 is verified at that exact commit; build/fmt/Clippy pass and
cl_type_of controls plus unchanged57-test GNU data-tests.el are running.
The first dispatch failed on a pre-dispatch GitHub GET TLS handshake; the
retry returned this run. Neither platform has a completed full gate here.

Main, separately from frozen source39, now has an uncompiled builtin draft:
GNU-layout static subr objects, BuiltinRef tagged words, direct native target
and arity reads, and stored builtin function cells. It removes the builtin
bridge identity, per-heap handles/static-handle retention, SUBR_INDICES map,
and successful-call reconstruction by name. Resolved interpreter calls
read dispatch metadata cached in the subr rather than symbol/name tables.
Six existing GC bridge controls use actual marker objects, and the handle
identity/reuse control retains cross-kind coverage with marker/overlay keys.
The generated tables were mechanically transformed without changing the
pinned registration order, names or arities; the generator emitter was
updated. Full GNU-source regeneration and compilation are still required.

This draft is not representation completion. The subr's Rust dispatch
extension and lazy initialization still require cost measurement; doc and
interactive metadata still use existing mechanisms and the corresponding
GNU prefix fields remain initialization values. Conses and the other bridge
kinds remain unfinished, and the retained raw builtin test has not yet run
against this draft. No performance result follows from these edits.


## Source39 validation complete; source40 builtin draft frozen

Exact-source macOS validation of db484d4 completed: all five static/build
phases have zero warnings, and all18 focused controls pass with zero ignores.
The slow cl-typep control passed unchanged in496.31seconds. Linux
35888335902 succeeds at exact clean db484d4:2 Rust controls and all57 GNU
data-tests outcomes pass in both editors, with no skips. Contract and raw
execution hashes, selected inventories, process exits and source identity
were checked after artifact download. This is affected validation, not a
full gate. The unfavorable CI timing remains visible: GNU156ms versus
Emaxx1875ms body(12.019x); setup128ms versus1406ms. It is a single diagnostic
sample, not an interleaved performance certificate.

The builtin generator compiled without warnings and generated a table from
the local GNU source, but comparison failed: that sibling is configured for
Darwin25.4 with D-Bus/ImageMagick and1449 subrs; the committed native ABI is
Darwin25.6 without them and1445 subrs. The full mismatch is retained in
builtin-source-regeneration-40. Nothing was copied over the pinned table,
and no oracle pin changed. The mechanically transformed table remains a
draft pending correct configuration regeneration. Existing Linux CI already
regenerates the table from its actual configured GNU source and compares it.

Source40 freezes only the19 builtin draft files on verified source39, leaving
other local work untouched. Rustfmt and diff checks pass; compiler and runtime
validation are pending. This is not yet another pushable representation
checkpoint and does not establish a performance improvement.


## Builtin integration checks and additional function-cell audit

Source40 failed compilation: the native compiler backend still copied the
old descriptors and read max_args as a field; an interior-mutable subr array
cannot use temporary promotion; handle classification omitted canonical
builtins. Source41 fixes those integration points, borrows descriptors and
C names during compiler setup, and adds the static-builtin-with-marker-bridge
GC regression. Ordinary symbol calls now resolve the actual function cell
first and carry the subr reference; bytecode callee checks no longer consult
symbol/name facts. No runtime pass is claimed for this draft.

Source41 fmt/diff pass; its all-target check currently reports three obsolete
native-form-cache warnings. Main removes the unused lookup, SymbolCell field
and accessor; a subsequent frozen source must validate that cleanup. The
45-name focused inventory is prepared, not executed.

A further audit found builtin-by-name fallback after a function cell becomes
absent. GNU's exact ordinary program removes identity and if with fmakunbound:
it returns (nil nil void-function 7 nil nil void-function), retaining the
saved identity subr while both names become undefined. Receipt:
builtin-unbound-gnu-control-1.json. The fallback's suspected resurrection
requires a Rust regression and repair; it is not counted fixed. Macro lookup,
symbol-function's special-form fallback, uninterned names and dump restoration
need the same function-cell review. The actual GNU control uses the diagnostic
local build, not the pinned frozen oracle.


Source41 all-target type checking completed with exit0 but three unused-cache
warnings, so the validation driver correctly stopped before Clippy and the
test build. The next snapshot42 removes their causes without allowances and
queues all checks, a fresh libtest, and the45 focused controls serially.
No new builtin runtime pass, commit, or performance claim is made yet.


## Shared builtin checkpoint 2aac0c4 and exact failures

Commit `2aac0c4eeebcbc5f3faab65a36c3f2025dc5d1d7` is pushed to the
existing WIP branch. Source42 macOS fmt, diff, all-target check, strict
Clippy and fresh libtest build pass with zero warnings. The unchanged raw
builtin word/layout/call regression now passes, as does static builtin GC
with a remaining marker bridge. All 45 selected controls executed: 35 pass,
10 fail, zero ignores; source identity was verified after execution. Nine
failures create a new builtin-name string during the dump writer's second
pass. The remaining failure loses a marker bridge rooted in another live
interpreter; finalizer survival, callback and reclamation assertions before
that final handle-count assertion pass. Neither failure is hidden.

Linux run35894806472 at exact 2aac0c4 failed. Its actual configured GNU
subr regeneration/comparison, runner build, formatting and strict Clippy
pass, as does the raw builtin ABI regression (1 pass, 0 failures/ignores).
The affected comparison does not begin: the anti-cheat inventory omits eql
from its scan after direct-target selection moved into the generator. Its
expected inventory remains unchanged. Raw logs and hashes are retained in
canonical-builtins-ci-42.json and canonical-builtins-linux-artifacts-42.

Main has uncompiled repairs: reuse the existing interned symbol-name
object during dumping; retain actual function cells through removal, nil
assignment and image restoration; remove symbol/name fallback and duplicate
metadata caches; reject funcall of special-form subrs before arity dispatch;
carry native marking into other live interpreter roots and doomed callbacks;
recognize canonical finalizers in the native-root handoff; include the new
generator selection in the existing fast-path audit. Three identical GNU
Lisp probes support function-cell and special-form semantics; their local
GNU executable is diagnostic, not the pinned macOS frozen oracle. Five
regressions and a native finalizer-root handoff control are added without
changing prior expectations. These edits need a separate frozen snapshot
and fresh execution. The full gate, compact conses, builtin metadata fields,
other bridge kinds, pinned macOS regeneration and performance remain open.


## Function-cell and integration followup e9807dc

Commit `e9807dc9d5bfeca05656a1683bcf3d3d91ef8441` is pushed. All52 focused
source43 controls pass with zero failures or ignores; every prior failed
builtin/dump/GC control remains unchanged and now passes. The seven added
controls cover removed/nil function cells, saved special forms, override
absence, image restoration, finalizer native-root handoff and the existing
fast-path audit gate. The initial driver used the wrong module path for the
audit test after six passes; its inventory failure is retained. The actual
compiled name is anti_cheat::gate_tests, and receipt43b reruns all52 on the
same unchanged source and binary.

Fmt, diff and fresh test compilation pass with zero warnings. The separate
all-target check exits0 but reports one unused dispatch_property macro, so
the zero-warning driver correctly rejects it and does not run Clippy.
Linux35901050382 is dispatched at exact e9807dc using existing affected
validation with builtin controls and unchanged data-tests.el. No Linux pass
is claimed. Source44 changes only that now test-only helper to cfg(test),
with no suppression and no test exclusion. It retains the52-name inventory
and is being validated afresh. Compact conses, full gates, pinned oracle
validation and performance remain open.


Source44 is committed as `81e8fe4c51a22470fef3e92a35b15824a7cd8617`,
parent e9807dc. It changes only cfg(test) on dispatch_property. Exact-source
fmt, diff, locked all-target/all-feature checking and fresh libtest compile
pass with zero warnings; all52 controls also pass with zero failures or
ignores and post-execution source identity verification. Strict Clippy is
running. Linux43 failed on the same unused macro under strict Clippy; its
raw log/artifacts are preserved. The source44 cleanup is being pushed for
a new run of the existing Linux affected workflow. This remains a bounded
builtin representation/de-cheating checkpoint, with no speedup claim.


The source44 checkpoint is now remotely verified at81e8fe4. All five macOS
static/build phases complete successfully, including strict all-target/
all-feature Clippy with -D warnings; compiler warnings are zero. The exact
52-test execution has zero failures/ignores and verified unchanged source.
Existing Linux CI35904605996 is dispatched and its head is verified at the
same commit; its result is pending. All local job handles are terminal.
Receipt: canonical-builtins-ci-44.json. No full-gate or performance claim.


## Linux checkpoint verified; remaining object words reproduced

Linux35904605996 succeeds at clean81e8fe4. Actual configured GNU native
table regeneration/comparison, runner build, formatting and strictClippy
pass. The27 Rust builtin controls exactly match the independently listed
source44 inventory and pass without ignores. Both ordinary editors execute
all57 selected data-tests.el tests and agree, with no skips or unsuccessful
processes. Contract/execution hashes, discovered/selected/result inventories,
binary/image identities and exact source commit were checked in downloaded
raw evidence. Receipt: canonical-builtins-ci-44.json. This is affected
validation, not the full frozen gate. Timing remains unfavorable: GNU148ms
and Rust1723ms body (reported11.570x); setup113/1223ms. This single CI sample
is not a repeated/interleaved performance result.

Source45 adds only two representation acceptance tests to unchanged81e8fe4
production. Fmt, diff and fresh all-feature libtest compile pass without
warnings; both tests then fail, with source verified after execution. The
first reaches all six constructors through ordinary primitives and confirms
buffer, marker, overlay, char-table, frame and terminal have different
ordinary/first-native/second-native words. The second confirms two ordinary
current-buffer calls return different BufferValue allocations. GNU
buffer.c:Fcurrent_buffer returns the address of the same current buffer.
Raw failures and the preserved binary are in
remaining-object-words-reproduction-45.json. No tests were ignored or
relabeled as passes.

The next representation change must address actual buffer/object ownership,
not add a name/id identity cache. Value::buffer currently allocates an id/name
wrapper each time while the editable Buffer lives elsewhere; marker and
other fields still carry private tagged IDs. NativeHeap maps those values
to heap-specific handles. A direct authoritative cons must be able to store
the same object words in every mode; shrinking its metadata while retaining
these conversions would not finish that architecture. The existing raw-cons
failure and80-byte ConsCell remain open. No production migration of these
six kinds or new speedup is claimed in source45.


Buffer ownership implementation (local source46, not pushed)

The editable Buffer now lives in BufferValue's RefCell; the interpreter holds its stable BufferRef. Existing-buffer primitives return that allocation, native encoding uses its ordinary word, and native marking/decoding accept it without a buffer bridge handle. GC traces the payload; image cloning memoizes each buffer before recursively copying its Lisp fields. Callers now hold explicit borrow guards, including mapped overlay borrows. Buffer equality/hashing use object addresses.

The frozen source passes locked all-target/all-feature checking and strict Clippy with zero warnings, and main passes formatting and diff checks. Receipts: target/runtime-goal/buffer-ownership-check-46g.json and buffer-ownership-check-46h.json. The failed intermediate checks are retained. A mistaken test-region edit was restored before the passing checks; its test function inventory was checked against the preceding source.

No runtime tests, Linux CI, or performance measurements have been run for this migration. It is not a pushable certificate. Follow-up includes actual-object test setup, borrow lifetimes across callbacks/GC, stale/dead buffer resolution, the killed-buffer filename table, census/dump identity, and the GNU buffer prefix. The last pushed checkpoint remains81e8fe4 with verified successful affected Linux CI; full representation, correctness and performance goals remain open. Detailed continuation: target/runtime-goal/buffer-ownership-working-state-46.json.


Buffer ownership runtime validation (source47, local and not pushed)

A fresh all-feature library test binary (SHA25670a1793c3aada8690c2aa07164a88c39ba64c2ffe1600817ff4f5fa7c7802ac7) passed70of71 selected controls. These include all52 prior builtin controls, both unchanged source45 acceptance probes, buffer/native/GC checks, all15 pdumper tests and two new GNU-backed buffer fixtures. The only failure is the unchanged all-kinds acceptance probe: marker, overlay, char-table, frame and terminal still have different native words. Buffer now passes both ordinary repeated-reference and native-word identity checks. Raw source46's58/69 failures remain preserved.

The follow-up fixes a missed canonical-buffer arm in native GC, removes the duplicate-wrapper tracing edge, rejects selecting a killed object after its name is reused, and roots the object returned by get-buffer-create across a hook that kills it and invokes GC. Old tests that manufactured separate allocations with the same id now use actual aliases; identity and survival/reclamation assertions remain. An overlapping-id-space test distinguishes separate buffer allocations. The two unchanged GNU/Rust fixtures cover rename/GC/hash identity/name reuse and creation-hook kill/GC; GNU receipts explicitly remain local diagnostics, not pinned Darwin certification.

Exactsource47 passes locked all-target/all-feature checking, strict Clippy, formatting and diff checks with zero warnings. Receipts are target/runtime-goal/buffer-ownership-check-47c.json, buffer-ownership-check-47d.json, buffer-ownership-build-47.json and buffer-ownership-controls-47.json. The432-file candidate preview is not a commit. Remaining buffer work includes removing the obsolete killed-buffer filename map, freeing killed-buffer payload as GNU does, broader editing/overlay controls, census and metadata review, and existing Linux CI. No performance or full-gate claim; last pushed head remains81e8fe4.


## Buffer ownership checkpoint pushed — source48

Pushed `e4f1c59a7b2d5c309399a74d8f912751f680f196` to the existing
`wip/compact-runtime-shared-object-words` branch. Buffers now own their editable
payload and share one ordinary/native object word; the killed-buffer filename
table is removed, and killed objects release text/undo/property storage.

Exact source48: formatting, diff, all-target/all-feature check, strict Clippy
and fresh library-test compilation pass with zero warnings. All73 focused
checks finished:72pass,1preserved failure for marker/overlay/char-table/frame/terminal
word identity. Both new lifetime controls pass, as do all15 pdumper tests.
Raw source/artifact identities, inventories and logs are retained under
`target/runtime-goal/buffer-ownership-*48*`. The failed acceptance assertion
remains unchanged; no ignores or timeouts were introduced.

Existing Linux CI is running at https://github.com/rayfdj/emaxx/actions/runs/36119568345
on the exact pushed commit, with the ordinary affected buffer validation.
The source inventory has304 Rust tests matching `buffer`; the unchanged GNU
file is `test/src/buffer-tests.el`. Outcome pending. No verified performance
improvement or full-gate pass is claimed; the full architecture/performance goal
remains open.


## GC control follow-up pushed — source49

Pushed `2f518a768bd4bdf62e6a067e8db23051a6e0a44e`. The broader source48
buffer run retained its301/304 failure:2socket permission failures and an
invalidly rooted GC negative control. Both socket tests pass with access.
GNU itself retains9,9,9 weak keys with setup in the caller frame, but9,8,7
when helper frames return first. The Rust control now ends setup and
inspection frames before marking; all9 entries and all survival/reclamation
assertions remain. An identicalGNU/Rust fixture checks9,8,7.

Source49 static/build checks pass with0warnings, and all304 buffer tests
pass together with0failures/ignores. Both exact GC controls also pass. No
production runtime changed after e4f1c59. Original failures and all raw
identities/logs remain in target/runtime-goal.

Linux follow-up36122311700 tests19root controls including both changed/new
tests, then the unchanged GNU buffer file. Original buffer run36119568345
is still running as last observed; both outcomes require raw review. The
terminal migration draft was parked before compilation to fix this failure;
it is not applied in the current working tree. Full goal remains active.

2026-09-26 evaluator and de-cheating follow-up:

- `5ac6558` removes the backtrace `mem::forget` path. A frame can own a Rust
  argument vector even when its function is immediate and it has no detail.
  Ordinary `Vec::truncate` now releases that storage. The local affected run
  passes 57/57 controls; Linux passes 15/15 backtrace controls. The unchanged
  full GNU eval file times out at 180 seconds in Emaxx while GNU passes all
  26 tests. Raw receipt: `target/runtime-goal/backtrace-ci-completed-59.json`.
- `4226e9e` evaluates all `let` initializers into Lisp-word storage before
  validating or binding names, then rereads the varlist and body. This follows
  `eval.c:Flet`, including initializer mutations and errors. `let*` now validates
  each saved name after its initializer and uses `FOR_EACH_TAIL`'s incremental
  cycle/quit checks, reading the next cdr after evaluation. GNU has executed
  the complete new fixture in `tests/fixtures/let-initializer-order.el`.
  Formatting and strict all-target/all-feature Clippy pass in Linux CI.
  All 39 selected Rust controls pass with zero ignored tests, and the complete
  unchanged GNU eval file now matches 26/26 passing outcomes without a timeout.
  Its test body takes 2417 ms versus GNU's 1379 ms in this single diagnostic run;
  this is not the locked performance-suite result. Receipt:
  `target/runtime-goal/let-ci-completed-61.json`.
- The ordinary root run for `4226e9e` passes 17/19 with zero ignored tests.
  Suspended bytecode reports `((1 t 1) (1 t 0))`; lexical callers report
  `((1 t 1) (1 t 1))`; both require `((1 t 0) (1 t 0))`. The source58 bytecode
  pass has regressed. No survival/reclamation assertion or timeout was changed.
  Receipt: `target/runtime-goal/let-roots-ci-completed-61.json`.
- Source60's local development predecessor passes 20/20 fixture/root controls
  with zero ignored tests after 878.67 seconds. Sampling located its long delay
  in upstream runtime initialization, before the new fixture assertions. It
  completed naturally before an attempted cancellation; no process was signaled.
  This predecessor does not certify final source61 or Linux reclamation.
- `7ce28c1` removes `prog1`'s vector-variable reread.
  GNU `Fprog1` returns its saved first value even when BODY mutates that object
  and rebinds the variable. The GNU-backed fixture checks vectors, bool-vectors,
  char-tables, replacement of BODY during FIRST, and propagation of BODY errors.
  Source62 formatting, fresh compilation, and local all-target/all-feature
  strict Clippy pass with zero warnings. The local affected inventory passes
  25/25 with zero ignored tests and verified source/executable identities.
  Linux CI passes formatting, strict Clippy, both `prog1` controls, and 26/26
  outcomes in the complete unchanged GNU eval file. This single diagnostic
  run's body takes 2537 ms versus GNU's 1395 ms; no performance gain is claimed.
  Receipts: `target/runtime-goal/evaluator-controls-62.json` and
  `target/runtime-goal/prog1-ci-completed-62.json`.

These are correctness and ownership checkpoints. They do not establish a
performance improvement. The complete unchanged GNU buffer file at the terminal
checkpoint matched 406/406 outcomes, but its single-run test body was 6.585 times
slower than GNU. The locked 16-case performance suite and its 3% tolerance remain
unchanged; full representation, allocation, validation and measurement goals
remain open.


2026-09-26 exact Linux GC diagnosis (source63):

The retained CI executable from run36218470711 has verified SHA256
`a8e341a9666340bbd4edb7bbcc6fc3a468f6eb4684b9e15c040506a6430880e4`.
The complete artifact also matches GitHub's published ZIP digest. Its unchanged
19 root controls pass17/fail2/ignore0. Bytecode reports `((1 t 1) (1 t 0))`;
lexical callers report `((1 t 0) (1 t 1))`. Earlier failures remain preserved.

Both unwanted roots originate at `let_with_values` frame offset0x48. Exact
x86-64 disassembly identifies that slot as the extra `lexical_bindings` boolean:
after `Value::cons` returns in RAX, instruction0x8aa585 sets only AL to1, then
0x8aa587 spills all of RAX to `[rsp+0x48]`. The collector recognizes the remaining
pointer bits as an interior pointer to an allocated cons. The slot persists
through the body evaluation at0x8aa76e. The optional-depth hypothesis was rejected
without applying it. Raw receipt and disassembly:
`target/runtime-goal/let-conservative-spill-diagnosis-63.json` and
`target/runtime-goal/let-exact-disassembly-63.log`.

The source64 correction follows GNU `eval.c:Flet` line1105: compare the constructed
lexical environment directly with the current one. It removes the redundant
boolean and uses Lisp-word equality instead of reconstructing environment kinds.
Validation is pending; tests, collector decisions and scan ranges are unchanged.
This addresses the identified spill without establishing general soundness of
conservative scanning over Rust storage. That broader issue remains open.


Source64 is committed and pushed as `2cf79844ef8f29175e644952c72819a4cf222efb`.
Exact formatted source passes all-target/all-feature strict Clippy on macOS
with zero warnings. Linux run36220232006 passes formatting, strict Clippy,
all19 unchanged root controls with zero ignores, and all26 GNU eval outcomes.
Both formerly failing survival/reclamation contracts pass. The Rust executed
inventory matches the prior failing inventory exactly; GNU discovered, selected
and executed inventories, process success, clean source identity, images and
artifact hashes were checked in raw evidence. Receipt:
`target/runtime-goal/let-environment-ci-completed-64.json`.

This run's test body takes2595ms versus GNU's1400ms (1.853x); setup is865ms versus
109ms. These single-run diagnostics remain unfavorable and do not establish a
performance change. The ordinary affected Rust workflow logs its fresh build
and binary path but does not retain that test executable's SHA; the GNU comparison
has executable/image hashes, and the prior diagnostic preserves its exact binary.

Source65 removes only the temporary GC stage/origin/frame tracing introduced in
sources51/54/56b, retaining whole-heap verification and every collector decision,
scan range and test assertion. Its exact-source strict Clippy check passes with
zero warnings. Fresh local runtime compilation and validation without tracing
are pending. The source64 diagnosis and all failed runs remain available; this
cleanup cannot be counted as passing until its own controls finish.


Source65 cleanup validation is complete at
`7c5fc2ec7937f319ec8afd376b5c7c723c9e47ac`:

- Fresh all-feature macOS compilation passes with zero compiler warnings.
  The saved test binary SHA256 is
  `bdbf3169a5988dc47b25715ce8b6ac1322daeee3d3a2529c7a941fef3ee02aca`.
  All25 affected evaluator/root controls pass with zero ignores and unchanged
  source/executable identities after execution. Both reclamation contracts pass.
- Linux run36221944778 passes formatting, strict Clippy, all19 unchanged root
  controls (zero ignores), and26/26 passing GNU eval outcomes (zero skips).
  The executed Rust inventory is identical to the previously failing inventory.
  Raw GNU inventories, process status, clean source identity and artifact hashes
  were verified. No temporary GC tracing remains in the runtime.
- This run remains slower than GNU: body2597ms versus1406ms (1.847x), setup905ms
  versus113ms. These single diagnostic samples are not a locked-suite result.
- Combined receipt: `target/runtime-goal/gc-reclamation-milestone-65.json`.
  Raw local and Linux receipts: `evaluator-controls-65.json` and
  `gc-cleanup-ci-completed-65.json` in the same directory.

The two specified reclamation failures are resolved for this selected checkpoint.
The full goal remains incomplete:80-byte conses, four bridge kinds, accounting,
public-API/rooting soundness, full gates, pinned Darwin certification and locked
performance parity still require work. The64KiB post-GC stack clearing predates
this goal in7e3b762 and has not been removed or justified by current-source evidence.
Reviewing it is not a validation result. No further runtime change is included
in this evidence update.


2026-09-26 shared marker milestone (source67g):

Markers now use one 48-byte GNU-layout allocation across Lisp, bytecode and
native code. Ordinary fields no longer consult an interpreter ID table or
native identity cache. Buffer mark slots, weak edit chains, GC cleanup, undo,
process/unwind roots and dump restoration now use actual marker objects.
GNU-confirmed repairs cover detached equality/hash behavior, error ordering,
return values, full buffer bounds, foreign-buffer marks at buffer killing and
detached markers in set-match-data. The exploratory bootstrap failure remains
in the evidence. See `docs/runtime-representation-marker-checkpoint.md`.

Corrected source67g passes formatting, strict Clippy and fresh compilation with
zero warnings. All33 selected marker controls pass with zero failures/ignores;
all19 unchanged root controls also pass. Broader buffer/dump controls and Linux
CI are still pending. Conses
remain80bytes and three object kinds still use native bridges. No performance
improvement, full validation or completion of the goal is claimed.


Marker follow-up: the source67g broader run passed321/324 with zero ignores.
Two failures are local socket permissions; the exact allocation-census test
omitted the buffer's newly real mark (one object, six words). The source68 test
requires both allocations and their full byte cost, following GNU. No runtime
code changes. Final source67g verification also failed after48 older files
vanished from the temporary checkout; surviving source and binary hashes match,
but that run is not a completed validation certificate. The raw failures remain.
A fresh workspace-contained checkout is being used for the rerun. Marker67 is
pushed as a1c6a17038db14cbc71f67959832f205927b48dd and its Linux CI is pending.


Marker67 Linux verification completed: run36257362542 passes33/33 Rust marker
controls plus3/3 GNU marker outcomes; run36257364649 passes19/19 unchanged roots
plus406/406 GNU buffer outcomes. Both pass formatting/strict Clippy. Raw artifact
digests, inventories, process success, clean source and editor/image identities
were checked. Both have zero ignored Rust tests and zero skipped GNU outcomes.
Unfavorable body timings remain visible: marker file2370ms/GNU174ms (13.620x,
including help rendering); buffer file2818ms/GNU430ms (6.553x). These single
samples do not measure an improvement or certify locked performance parity.
The test-only census correction is undergoing fresh workspace validation.


Source68 fresh workspace build/strict Clippy pass with zero warnings; the exact
census correction passes locally. Broader local controls are running with the
required socket access. Production runtime is unchanged from marker67; the
census assertion and updated evidence are the only follow-up changes.


2026-09-27 marker checkpoint validation completed (source68):

The fresh workspace rerun passes census1/1, markers33/33, roots19/19 and
buffer/dump324/324, zero failures/ignores. The overlapping groups cover360 distinct
tests. Both socket controls pass with permission. Source and saved binary hashes
match after execution; formatting, strict all-target/all-feature Clippy and fresh
compilation have zero warnings. Linux run36258556054 at2dfdfd3 also passes its
exact census test and406/406 unchanged GNU buffer outcomes with zero skips.
Raw archive/source/editor/image identities, inventories and process success were
verified. Combined receipt: target/runtime-goal/marker-milestone-68.json.

The additional buffer timing remains unfavorable:4139ms/GNU573ms for the body,
6073ms/GNU259ms for setup. It is another diagnostic sample, not a performance
improvement or regression measurement. Source67's earlier failures and failed
source verification remain preserved. The next architecture work removes the
remaining overlay/char-table/frame bridges and the80-byte cons representation;
the full goal remains active.
