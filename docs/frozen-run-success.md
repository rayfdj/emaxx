# Ordinary frozen-run success

Tracking: [correctness #69](https://github.com/rayfdj/emaxx/issues/69),
[runtime performance #70](https://github.com/rayfdj/emaxx/issues/70), and
[compiler latency #71](https://github.com/rayfdj/emaxx/issues/71).
Missing platform/build capabilities are tracked separately in
[#73](https://github.com/rayfdj/emaxx/issues/73), per the user's 2026-09-14 scope decision.
The issues retain the original run identity, all 363 files sorted by slowdown,
profiling findings, and acceptance criteria.

## Current validation status, 2026-09-14

The chronological evidence below includes failed intermediate attempts. This
section records the latest completed checks; no final frozen success is claimed.

- Linux frozen run `34826268382` at `03a3814` completes all 519 files and
  7,928 matching outcomes: 7,667 passes, 214 skips and 47 expected failures
  per editor, with zero unexpected outcomes. All 3,114 raw receipt hashes
  verify, as do all selected/result names and 1,038 successful process exits.
  This is a complete result for that revision, not the later primitive fixes.
  A named-case audit of the original 26 failures found 23 actual Linux
  passes and three shared server skips caused by the clean invocation
  omitting `TERM`. Those three skips do not close the original failures.
  The common test environment now supplies `TERM=xterm` for the real child
  PTYs used by both editors. This changes test preparation, with no editor
  runtime or upstream assertion changes. All seven unchanged server cases
  pass independently on Mac with that environment; the focused environment
  control, rustfmt and strict Clippy pass. Linux ordinary gate `34831517325`
  at `13a8546` also passes all seven tests, including the three original
  server cases, plus the environment control; all six raw receipt hashes
  verify. The original 214-skip artifact and named-case audit are retained.
- All four primitive fixes in `1ee9fce` pass their ordinary Linux files and
  accompanying Rust controls: Solar (`34829836880`), Eshell arguments
  (`34829839814`), ERC (`34829842376`), and Eshell unload (`34829845717`).
  Mac ordinary continuation `run-1789379366981112000-16201` passes Tramp's
  59 outcomes with the unchanged 180-second deadline: GNU's test phase takes
  122.711 seconds and Emaxx's 153.081 seconds. Its six receipts verify.
  The earlier timed-out attempt overlapped isolated Rust builds, so elapsed
  time differences cannot be attributed solely to primitive repairs.
  The complete continuation covers 201 files and 3,442 outcomes: 3,433
  match, three are unexpected runtime failures, and six retain the Mac
  capability diagnostic differences assigned to #73. All 1,206 raw receipt
  hashes verify. It is not combined with the earlier revision into a frozen
  certificate. The additional runtime failures expose the defects below.
- Pascal and Ruby indentation lose keyword identities when native relocation
  vectors contain symbols absent from the standard obarray. Both the reader's
  standard-obarray walk and its private-obarray identity mapping now traverse
  vectors, retaining shared and circular vector identity. An older `890becd`
  binary reproduces both failures, so they do not originate in the recent
  four-primitive repair. Three GNU-checked controls cover circular/shared
  vectors, a dynamically bound private obarray, and actual native relocation
  loading; adjacent symbol controls also pass.
- Native numeric predicates misclassify integers outside the 62-bit fixnum
  range when their values still fit Rust's `i64`. The bridge already represents
  these as GNU bignums; the pseudovector predicate now recognizes that same
  representation as well as arbitrary-precision integers. A real native-compiled
  control checks both representations and numeric-field sorting with explicit
  large integers, independently of random generation.
- Unrestricted `random` used the full signed 64-bit value range. It now retains
  and sign-extends the 62-bit payload, following `Frandom`/`make_ufixnum`.
  Noninteger, nonstring arguments follow GNU's unrestricted branch; positive
  integer limits retain the existing bounded generator. This repairs the value
  and argument contracts without claiming the underlying PRNG is GNU's exact
  algorithm. Four random-related controls pass, including deterministic reseeding
  and actual fixnum range checks. All 94 native-runtime controls, rustfmt, and
  all-target/all-feature Clippy with `-D warnings` pass for the combined candidate.
  Independent Mac checks of unchanged upstream files now match: Pascal has
  one pass, numeric sorting five, and Ruby 106 passes plus two declared
  expected failures in each editor. Raw outputs and input hashes verify in
  `vector-numeric-random-upstream-reviewed.json`. The first manual image
  command omitted the required test-directory environment; only that failed
  preparation step was continued. Ordinary cross-platform validation of the
  published three-fix implementation remains required.
- The ordinary Mac attempt at `03a3814`
  (`frozen-1789376872886057000-67964`) completed 318 file comparisons,
  including matching Comint and SHR, then stopped at Tramp: GNU completed
  59 outcomes (52 passes, seven skips), while Emaxx exceeded the unchanged
  180-second test deadline and produced no test results. Its raw timeout
  and receipt remain intact; this is incomplete evidence, not a certificate.
  The completed portion exposed four primitive behavior bugs with native
  GNU libraries. `atan` must treat an explicit nil X as omitted; buffer
  lookup must return a supplied buffer object even after rename or death;
  `read-string` must substitute only a non-nil default; `set-keymap-parent`
  must resolve a symbolic parent's function cell before installing it.
  The fixes follow `floatfns.c`, `buffer.c`, `minibuf.c`, and `keymap.c`,
  without application names or test selectors in runtime decisions.
- Independent Mac diagnostics pass the unchanged upstream Solar file
  (one test), Eshell arguments (17), ERC (94), and Eshell unload (three)
  in each editor. The Solar and Eshell-argument fixes were checked as they
  were implemented; ERC and unload use the combined four-fix candidate.
  All raw report hashes and actual selected/result rows verify. Evidence
  is retained under `target/compat-audit-20260914/` in
  `atan-nil-upstream-solar`, `buffer-identity-upstream-eshell`,
  `four-primitives-upstream-erc`, and `four-primitives-upstream-eshell-unload`.
  Eleven focused Rust checks pass, including six new controls for explicit
  nil arguments, native calls, renamed/dead buffer identity, uninterned
  symbolic keymap parents, and non-nil minibuffer defaults. Formatting and
  all-target/all-feature Clippy with `-D warnings` pass. These diagnostics
  do not substitute for ordinary validation of the published source.
- Linux continuation `34826271389` at `03a3814` passes three image/audit
  controls, 60 binary tests, and 39 tests across all six integration targets.
  Actual stage result counts and raw log hashes verify. This includes
  native compilation, native thread continuations, and batch exit/restart
  child-process behavior. The complete frozen result is recorded above.
- The ordinary Linux frozen attempt at `c469a18` (`34811016486`) completed
  457 matching files with zero unexpected outcomes, then stopped at
  `test/lisp/vc/vc-tests.el` because GNU selected seven Mercurial tests absent
  from the Linux manifest. Both editors actually passed all 14 Git/Mercurial
  tests. All 2,748 raw receipts across those 458 files verify. The run has no
  final summary or final input verification, so this remains incomplete
  evidence. The Linux inventory now adds those seven actual GNU names, keeping
  all prior names: 519 files, 7,928 outcomes. Darwin stays at 7,921.
- Linux ordinary continuation `34813818081` at `4e4160d` executes all 62
  remaining files: 61 match and only `x-dnd-tests.el` has unexpected failures
  (two). All 372 raw receipts verify. Its unchanged upstream Lisp supplies
  replacements for window-property primitives; missing original Emaxx
  bindings prevented the normal native trampoline hook from seeing those
  redefinitions. The window-property family now supplies GNU's live-frame
  validation and terminal-frame errors, with platform availability still
  taken from the host C manifest. The original tests and a direct GNU
  primitive contract will validate this repair on Linux.
- Mac image inspection found that the normal subject still used the old
  sibling GNU tree for source/data/DOC/native paths while loading Lisp from
  the pinned oracle. Its image had zero native units and bytecoded startup
  functions. The build-time `EMAXX_GNU_SOURCE_DIRECTORY` setting and ordinary
  runner now configure one installation tree consistently. The GNU-checked
  directory control passes for the non-sibling pinned Mac tree and confirms
  that hostile runtime overrides cannot redirect installation paths. This
  configuration correction is separate from the compact-tag memory pilot.
  Ordinary Mac X-DND now passes both unchanged tests; all six receipts and
  its contract verify (`run-1789369035708244000-51850`). Independent probes
  confirm the pinned source and native `command-line`/`normal-top-level` in
  both editors. Emaxx loads 174 native units; GNU loads 182, which is recorded
  as a preload difference rather than an equal-count claim. The initial
  attempt stopped before editor tests because the new pure compile-time
  path accessor needed an explicit audit-list entry; that reviewed entry
  retains all existing oracle-delegation bans.
- `run --from-file PATH` executes an inclusive ordinary continuation in the
  same canonical order, optionally ending at `--through-file`. A missing or
  reversed endpoint fails. These options are unavailable in `frozen`; they
  neither weaken resume identity checks nor certify partial frozen coverage.
- The original full Rust library runs at `37197db` completed every library
  group on both platforms. Each found the same missing detached-overlay
  image-state explanation in the final audit group. That documentation was
  corrected, and the three relevant image/audit controls passed without
  repeating the completed library groups. The continued binary tests pass
  58 cases on Mac and 59 on Linux. Linux continuation `34812783853`
  also passes all six integration targets (37 tests), including the actual
  large-list process and both command-line locales; its raw stage logs verify.
- The integration continuation exposed a separate native cons-encoding stack
  overflow. `34631ba` registers canonical cells and encodes their fields with
  an explicit worklist, preserving shared and cyclic identities. All 93
  native-runtime controls pass on Mac and Linux. The actual 8,000,005-element
  Mac CLI test passes, followed by all six integration targets (34 tests).
  This bounds initial typed-cons encoding; it is not a claim that every
  native decoding or dirty-mirror reconciliation path is iterative.
- Linux ordinary `comp-tests.el` at `34631ba` (`34811829450`) passes all
  177 tests independently in each editor, with zero skips or unexpected
  outcomes. The six raw receipts, contract and 93-control log verify.
  Uninstrumented test-body totals are 15.064615 seconds for GNU and 93.574951
  for Emaxx. Dedicated attribution does not replace these ordinary results.
- `68ed488` corrects reading from unibyte Lisp strings: GNU interprets source
  bytes as Latin-1 characters while unibyte buffer/marker sources retain
  byte8 behavior. The GNU-checked source-kind matrix and 45 adjacent reader
  controls pass. Ordinary Mac `lread-tests.el` passes all 52 tests, with six
  receipts and the contract verified. Actual command-line probes now match
  GNU under both C and UTF-8 locales, including arguments and exit status.
  Linux reader CI `34812781624` also passes the actual new reader control
  and all 52 ordinary tests; its six raw receipts and contract verify.
- The eight-case Linux compiler recording is retained despite its later
  report-permission failure. Offline job `34811621252` recovered reports
  using the exact original executable and profile hashes, without rerunning
  either editor. There are 27,265 recorded samples and zero reported lost
  samples; unavailable DSO/unwind details remain explicit limitations.
  Inclusive samples point to compiler class enumeration/native callbacks,
  symbol/value work, GC and image loading. Percentages overlap and cannot
  be added or extrapolated into an 80-case CPU total.
- Ordinary Mac `comp-tests.el` at `890becd` passes all 177 unchanged tests
  independently in both editors, with zero skips or unexpected outcomes.
  All six raw receipts and the contract verify for
  `run-1789369833919072000-52525`. This run uses the corrected native
  installation paths and compact internal image tags. Whole test phases
  take 98.030 seconds for GNU and 123.248 seconds for Emaxx; these are
  ordinary run timings, not a controlled before/after optimization trial.
- Compact internal dump tags retain the existing explicit 32-bit image
  encoding while reducing the loader's dense table footprint. A matched
  Mac pilot at `4e4160d` used the same configured GNU source, native preload,
  image layout and 41,465,448-byte image size. Across eight alternating pairs
  for each of startup and loading `comp`, median peak RSS fell from 222.09
  to 184.26 MiB (17.04%). Median wall times changed by less than 1%; this is
  a memory improvement, not evidence of a compiler latency improvement.
  All 32 processes completed with the expected output, and input and raw
  output hashes verify. The 17 existing dump-image controls and strict
  Mac Clippy pass. Linux validation remains pending.
- The Linux X-DND attempt at `8c023a2` (`34815944593`) stopped during Clippy
  before the editor tests: the new Linux-only control used prohibited
  `unwrap()` calls. These now have explanatory `expect()` messages. No
  runtime behavior or test assertion changed. Continued Linux job
  `34816493234` at `890becd` now passes the actual primitive control
  (one test) and both unchanged X-DND tests independently in each editor.
  All six raw receipts and the contract verify.
- Linux Semantic profiling (`34816503151`, `890becd`) retains 14,921
  process-tree samples with zero reported losses and all 17 unchanged
  tests passing in each editor. The six receipts, profile and exact
  executable hashes verify. Emaxx-only samples identify symbol lookup,
  regexp handling and native GC; inclusive percentages overlap, and
  unknown/truncated unwind roots remain visible.
- That profile exposed an unused, non-GNU side effect: `looking-at` and
  `posix-looking-at` copied every pattern into `last-looking-at-pattern`.
  Removing the assignment preserves unrelated Lisp bindings, verified by
  a direct GNU/Emaxx contract control. Four alternating Mac pairs of the
  complete unchanged Semantic file reduced median summed test-body time
  from 15.230375 to 14.743021 seconds (3.20%). GNU and all eight subject
  processes pass the same 17 tests: 153 independent outcomes, with all
  raw output and input hashes verified. This diagnostic comparison does
  not replace ordinary frozen validation. The new generic
  `tools/measure_upstream_pair.py` retains commands, private environments,
  reports, timings and hashes; it does not alter upstream tests or compiler
  passes. Formatting and strict Mac Clippy pass for the correction.
- Linux ordinary validation of that correction at `2e67b6a`
  (`34818540299`) passes the new GNU contract control and all 17 unchanged
  Semantic tests independently in both editors, with zero skips or unexpected
  outcomes. Its six raw receipts, contract and summary verify; formatting
  and strict Clippy pass. The branch runner checks also pass (`34818440161`).
- Linux Simple profiling at `890becd` (`34818140305`) completes all 53
  unchanged tests: 50 passes and three matching legitimate skips in each
  editor. All six receipts, contract, profile and exact executable hashes
  verify. The two dominant shell-command tests execute 50 and 20 fresh
  editor processes respectively. The profile identifies image loading,
  allocation and interpreter destruction as repeated work. Its instrumented
  timings are not substituted for the ordinary uninstrumented measurements.
- A compiler `mapatoms` shortcut was rejected: four alternating pairs of
  eight unchanged compiler tests increased median summed test-body time
  from 10.615654 to 10.982802 seconds (3.46%). All 72 independently executed
  GNU/subject outcomes passed and input/raw-output hashes verify. The rejected
  native shortcut is not part of the implementation. Investigation also
  found three independent GNU behavior differences: the dynamic default
  obarray, legacy vector conversion, and callback function-cell redefinition.
  Their separate correctness repair passes all four `mapatoms` controls,
  including GNU-checked interpreted and native callbacks, GC and nonlocal
  exit. Ordinary Linux validation at `a91caeb` (`34820686907`) passes
  these four controls and all 81 unchanged `test/src/fns-tests.el` tests
  independently in each editor. All six raw receipts and the contract verify.
- A GC candidate avoids searching conservative pointer candidates when the
  same attached cons has already been marked in the current pass. It retains
  the pointer-identity check, original root set, weak-table fixed point,
  thresholds and sweep. All 93 native-runtime controls and the existing
  weak-reference controls pass. Four alternating pairs of the three unchanged
  upstream sort tests reduce median summed body time from 12.209058 to
  11.360349 seconds (6.95%); one individual pair is slightly slower. The
  complete Semantic file changes from 15.407613 to 15.304161 seconds (0.67%),
  which does not establish a meaningful Semantic speedup. Both comparisons
  retain all raw reports and verify input/output hashes. These diagnostic
  selections do not replace the complete ordinary inventory.
- The same revision passes all 93 native-runtime controls and the three
  unchanged `test/lisp/emacs-lisp/comp-tests.el` native-cache tests on Linux
  (`34820694713`), with all six receipts and the contract verified. This is
  the separate cache test file, not the 177-test `test/src/comp-tests.el`
  compiler inventory. Full compiler job `34821076055` now passes all 177
  tests independently in each editor, with zero skips or unexpected outcomes.
  Its six receipts and contract verify, as does the 93-control stage. Ordinary
  test phases take GNU 15.214 seconds and Emaxx 93.052 seconds; no controlled
  compiler speedup is claimed from these run timings.
- The remaining Linux profiles at `2e67b6a` pass the one track-changes test
  (`34820154251`) and all 37 package tests (`34820160611`). Both sets of
  six receipts, contracts, profiles and exact executable hashes verify;
  both recordings report zero lost samples. Track-changes samples identify
  native time-value decoding and mirror reconciliation during timer work.
  The independent random seeds differ (GNU 12399781, Emaxx 16390989), so
  their observed times are not a controlled paired ratio. Package samples
  identify file-truename and file-name-handler lookup, with symbol/regexp
  work. Inclusive percentages overlap, and external GPG/wait time is not
  attributed by the Emaxx-only report. Full details and descending per-case
  gaps are retained in issue #70 and the local reviewed profile artifacts.
- A second compiler pilot resolved existing symbol-cell names once per
  lookup. Existing GNU variable/buffer-local/uninterned controls and strict
  Clippy passed, but four alternating pairs changed compiler body time by
  only 0.91% and Semantic by 0.15%. Neither establishes a meaningful gain,
  so the 16-line candidate is excluded. Its patch, executable, image and
  all 225 independent passing outcomes remain retained with verified hashes.
  An initial filename error selected zero GNU tests and was rejected before
  either subject ran; it is retained separately as failed diagnostic evidence.

- Process-tree profiles identified repeated heap destruction at the end of
  compiler and shell-command children. The batch CLI now keeps its interpreter
  alive through process exit or restart, after releasing suspended threads,
  process connections and terminal state. GNU similarly ends shutdown with
  exit/exec without walking the Lisp heap. Returning Rust library calls retain
  ordinary Drop, including bounded child cleanup; a failed restart still drops
  its interpreter. Unix CLI shutdown now sends SIGHUP to the child's process
  group without escalating an ignored signal, matching GNU's
  `kill_buffer_processes`. Direct GNU/Emaxx controls cover normal children and
  ignored-SIGHUP children through exit and restart.
  The first restart control wrongly assumed GNU flushed piped stdout before
  exec. Its failed log is retained; the corrected control records the PID in
  a temporary file and passes all four editor/exit-or-restart combinations.
  Emaxx's existing explicit stdout flush remains a separate observable restart
  difference, so these checks do not claim complete restart output parity.
  Four alternating pairs of eight unchanged compiler tests produce 72 actual
  passing outcomes and reduce median body time from
  9.340945 to 8.946665 seconds
  (4.22%). Four pairs of the two unchanged Simple shell-command cases
  produce 18 actual passing outcomes and reduce the median from
  21.141074 to 19.119371 seconds
  (9.56%). Every individual pair and raw result is retained; input and
  output hashes verify. These isolate the shutdown change against the same
  configured installation/native preload; they are Mac diagnostic selections,
  not complete frozen runs or measurements of combined optimization gains.
  Combined validation passes all 20 CLI cases and five of six parity checks.
  The copied-executable fingerprint fixture initially mirrored only the old
  sibling GNU native-library tree, so the configured Emaxx image could not
  resolve its own native units after the copy. The fixture now mirrors both
  actual native roots, deduplicating them when equal; its fingerprint equality
  assertion is unchanged. That single check now passes (9.91 seconds), as
  does the unfinished real-native-thread contract (5.54 seconds, six paired
  GNU/Emaxx scenarios). The 25 completed passing checks were retained. Rustfmt,
  strict all-target/all-feature Clippy (13.55 seconds) and the current runner
  build pass; the original failed fixture log remains available.

Still open in this effort: complete ordinary frozen results on both platforms,
remaining performance work and adversarial review. The six strict Mac
build-feature skip diagnostics belong to separate follow-up #73. They must
remain visible in final Mac output; a run containing them is not all-green.
This scope decision changes neither comparisons nor the selected inventory. Shared-library bytes are not yet covered by the
run provenance. A roughly three-minute Mac harness preparation delay was
observed before a reader run, but its attempted process sample arrived after
exit and did not establish the cause. The measured Mac vector-allocation
regression remains recorded. Formatting and strict Clippy passed for both
runtime fixes; the current inventory/continuation change has separate checks.

## Success and parity

`compat-harness run` and `frozen` share their setup and execution checks.
Outcome parity is still reported, but a file prints `PASS` only when both
editors completed successfully with no unexpected ERT outcomes. A matching
load error or unexpected failure fails the command. So does an unexpected
pass. Legitimate upstream expected failures and skips remain valid.

Platform skips require matching output and remain separate from passes.
Different feature-list diagnostics still fail comparison. A test that is
unsupported on Darwin must execute successfully on Linux when supported
there, and the converse applies to Darwin-only functionality.

The shared Lisp reporter asks `ert-test-result-expected-p` for each result,
including compound and predicate expectations. Rust retains that decision
when filtering reports and compares each test's declared expectation between
editors. It validates original reports before any filtering or summary
reconstruction, including runner/file/selector identity, missing expectation
evidence, inconsistent summaries, and incomplete/duplicate results. It also
rejects unsuccessful child exits. Per-file
`execution_issues` and aggregate `unsuccessful_files` record execution health
separately from matching/mismatching outcome counts. Frozen coverage checks
both selected and completed names against the manifest, including extra
selected tests that never produced a result.

## Resume evidence and message comparison

Each run writes `contract.json` before executing files. Resume requires that
contract to match the current binaries, helper, source/generated Lisp inputs,
dump images, effective environment, selector, file inventory, manifest, and
prepared external tools/module inputs.
Values of inherited environment variables are hashed, not published.

Each completed file has an `execution.json` receipt binding hashes of both
editors' separate raw reports, process evidence, and logs to the run contract.
Resume verifies those bytes and report identities, retains them in the new
artifact, and recomputes comparisons, execution health, timings, and exact
manifest coverage through the same path as fresh execution. It never reads
cached scores from `comparison.json`. A missing or invalid per-file receipt
causes an explicit replay of that file; other valid files remain reusable.
Older runs without the contract cannot certify a resume and remain available
for inspection. Hashes are integrity checks, not signed proof against someone
rewriting the entire artifact directory.

Before either child starts, the harness removes old report/marker files, so a
failed launch cannot reuse a prior pass. Reports changed after their producing
process returned are rejected when recording execution evidence.

Message normalization replaces only recorded checkout and private temporary
directory roots at path boundaries. Child names and arbitrary literals such
as `emacs-test-ABC123` retain their bytes. The former generic temporary-root
and random-name substitutions have been removed. A newly visible difference
must be investigated; it cannot be dismissed by its filename pattern.

## Fixture layout

Each editor receives its own short, canonical, private temporary directory.
On Unix this is below `/tmp`, with mode 0700 and a random name, leaving room
for upstream's nested Unix socket names even on macOS. TMPDIR, TMP, and TEMP
agree. Each child starts in its disposable GNU `test/` directory.

Both editors load the original pinned build's compiled Lisp. The disposable
checkout's `lisp/` directory links to those same libraries, so tests locating
libraries relative to their own filename resolve the real loaded files.
Writable test resources remain in the disposable `test/` tree. The harness
detaches the library link before reset/restoration and verifies original
source and generated-library fingerprints after execution. No expected test
paths, Lisp assertions, or editor results are rewritten.

Removing the old partial `source-directory` override alone did not solve
xref: the test derives its expected library paths from `load-file-name`.
The ordinary replay exposed that incomplete diagnosis before the library
layout was corrected.

## Initial Mac validation, 2026-09-14

The following unchanged upstream files passed through `compat-harness run
--file PATH`, sharing the normal frozen implementation:

| File | Passed | Expected failures | Skipped | Unexpected |
| --- | ---: | ---: | ---: | ---: |
| elisp-mode-tests | 58 | 1 | 4 | 0 |
| server-tests | 7 | 0 | 0 | 0 |
| wdired-tests | 7 | 0 | 0 | 0 |
| process-tests | 35 | 0 | 2 | 0 |
| uniquify-tests | 7 | 0 | 0 | 0 |

Both editors have the same results. These cover 20 of the original 26 shared
unexpected failures on this Mac. Evidence is retained under
`target/compat-review-20260914/ordinary-*.log`; the Elisp result after the
library correction is `ordinary-elisp-mode-final.log`. The other four
completed results were retained while correcting xref, rather than rerunning
the whole suite. This is incremental validation, not a final frozen score.

The initial 16 compatibility-report tests passed, as did all 41 harness checks using
the retained results and the affected checkout test after its update. The
real Unix socket test required execution outside the sandbox, which denied
`bind` in the initial invocation. A five-case GNU/Emaxx reporter control also
verified expected failure, unexpected pass, compound and predicate
expectations, and skip. Rustfmt and Clippy with `-D warnings` passed.

The follow-up audit repair passed 17 report tests, eight new adversarial
harness tests, and the affected manifest-name and checkout checks. Its
ordinary uniquify replay again passed all seven tests on both editors, with
raw receipts and unchanged-input verification enabled:
`target/compat/run-1789338447989900000-77677/`. A separate real-process control
deliberately failed only Emaxx; GNU passed, Emaxx retained its unexpected
failure, and the Rust scorer reported zero matching / one mismatching result.
These controls and logs are in `target/compat-audit-20260914/`. They verify
the targeted execution/scoring paths, not every possible runtime path. The
remaining completed upstream runs were not repeated.

## Ordinary prerequisites and inventory extension

The normal `run`, `frozen`, and discovery commands now prepare the same real
external prerequisites for both editors. Eglot uses Rust 1.75.0 with
rust-analyzer and rust-src, matching the pinned upstream fixture's
`rustAnalyzer/Indexing` protocol, and clangd with UTF-16 offsets. Install with:

```sh
rustup toolchain install 1.75.0 --profile minimal --component rust-analyzer --component rust-src
```

Clangd must be installed on PATH; `EMAXX_COMPAT_CLANGD` can select its actual
executable and `EMAXX_COMPAT_RUST_BIN` can select the 1.75 toolchain directory.
Missing prerequisites stop preflight rather than silently removing tests.
Tool executables, versions, sysroot contents, generated launchers, and module
build inputs are recorded and checked after execution. They also bind resume.

Module preparation runs the pinned source's configured GNU Makefile against
unchanged C source and headers. Each editor gets a separate short installation
containing its own unchanged executable and dump, the same compiled module,
and its original relative library layout. This preserves GNU's actual
`invocation-directory` lookup and portable-image native paths. Long artifact
paths caused the upstream help test to wrap its filename before abbreviation;
short installation paths remove that environmental failure without changing
Lisp variables, expectations, or report messages.

Ordinary Mac validation now includes Eglot (45 passes, seven upstream skips,
zero unexpected results on each editor) and modules (38 passes each):

- Eglot: `target/compat/run-1789340787561035000-79536/`.
- Modules: `target/compat/run-1789341049773817000-80892/`.

Together with the earlier file results, this provides incremental ordinary-run
evidence for all 26 original shared unexpected failures. It is not a final
frozen certificate. Earlier module attempts exposed missing native paths and
the help wrapping issue; their failures are retained. An invocation started
while the harness was rebuilding was stopped before tests and excluded
(`run-1789340337784221000-78881`).

The Darwin manifest deliberately adds the 38 names actually discovered by GNU,
replacing only its historical module load error. All previous 7,883 names
remain: 519 files, 7,921 outcomes, zero load errors. Frozen execution also
visits all files with zero recorded selected tests, so new selections cannot
hide in previously omitted files. Linux's inventory remains separately pinned
until actual Linux discovery is reviewed.

## Regexp correction and SHR diagnosis

The general bracket translator now keeps ASCII/non-ASCII and byte-class
membership separate from Unicode case folding. Ordinary letters and ranges
in the same bracket still fold; negation and quantifiers apply to the entire
combined atom. It also preserves the correct membership of characters with
syntax properties and raw byte8 characters. This removes the false ASCII
matches through long-s/Kelvin case equivalence without recognizing test names.

All 632 diagnostic rows match the actual GNU oracle, covering both folding
settings, mixed/negated/repeated brackets, bytes and syntax properties. The
new regression and 16 adjacent controls pass. Ordinary regex-emacs (34), search
(1), and diff-mode (7) results match with zero unexpected outcomes:

- `target/compat/run-1789341707802267000-85433/`
- `target/compat/run-1789341778094170000-87935/`
- `target/compat/run-1789341794149079000-88864/`

Both diff-mode font-lock tests now naturally declare `:passed`, as GNU does;
no expectation was rewritten. Rustfmt and strict Clippy pass.

SHR's missing callback has a parser cause: the upstream test supplies an
unfinished image tag. GNU links Homebrew libxml2 2.15.3 and returns an empty
body; Emaxx links macOS libxml2 2.9.13 and retains the image. Both invoke the
same documented parser options. A separate full build of unchanged GNU
source with Apple's libxml2 2.9.13 now passes SHR through the ordinary runner.
The build recipe is `tools/build_macos_oracle.py`; it preserves the committed
GNU configure options and exposes the actual SDK xml2-config flags through
pkg-config, without editing GNU sources or updating pins automatically.

The explicitly reviewed new Mac oracle hash is
`591acf7b5da3582dca93367b6a69935dbff9c69a9310f05a1927fa230b428f8d`.
The old `7d8944fe` build and its pin/configuration remain retained. The new
checkout is pristine revision `636f166c`; otool confirms system libxml2.
Regenerating the native ABI from the completed build changes only the host
configuration from Darwin 25.5.0 to 25.6.0. Subroutine order, ABI version and
configure options are unchanged. Emaxx and its image were rebuilt against
that actual ABI and GNU library tree.

Ordinary validation on the new pair: SHR 4/4, XML 1/1 and modules 38/38 pass
in each editor with zero unexpected outcomes. The receipts and contracts
validate in `run-1789344114951647000-22759`,
`run-1789344146348364000-22964`, and
`run-1789344164000459000-23126`. These replace the failing SHR setup with a
working dependency combination, not a matching failure or changed assertion.

## Still open

The runtime performance increment fixes two general problems. Vector
`length` now reads the slot count without cloning the entire vector, as
GNU's `Flength` reads its size. Overlay ownership now follows GNU's GC
behavior: attached overlays are buffer roots, reachable detached overlays
retain their properties, and unreachable detached overlays are swept after
the shared mark/weak-table fixed point. Killing a buffer or deleting all its
overlays detaches those objects rather than destroying still-reachable
properties. Detached objects no longer accumulate in buffer edit scans.

GNU probes confirmed both lifetime bugs before the fix: an attached overlay
incorrectly disappeared from an Emaxx weak-key table, while an unreachable
deleted overlay incorrectly retained its payload. A compiled helper removes
temporary stack references in the latter probe; a direct top-level GNU
probe can conservatively retain a temporary, so it is not treated as an
exact collection-time assertion. Buffer teardown and bulk deletion now
preserve properties as GNU does. Dump restoration and interpreter cloning
also preserve independent detached ownership.

The first IndexMap experiment regressed buffer timing by about 14% and was
removed, including its direct dependency. The replacement was measured
against the saved pre-change executable/image with unchanged upstream tests
in fresh independent processes. These are single paired diagnostic timings,
not a statistical benchmark or frozen certificate:

| Case | Before | After |
| --- | ---: | ---: |
| `test-overlay-randomly` | 3.544 s | 0.817 s |
| `replace-buffer-contents-bug31837` | 2.292 s | 0.466 s |
| `mule-cmds-tests--ucs-names-missing-names` | 7.309 s | 3.905 s |
| `mule-cmds-tests--ucs-names-old-name-override` | 3.645 s | 3.740 s |

The Unicode override case did not improve; it remains in the slow-case work.
All diagnostic outcomes were preserved. Subsequent ordinary-run artifacts
independently validate 406 buffer passes, 22 editfns passes plus one upstream
expected failure, and 16 Mule passes in each editor, with no unexpected
outcomes. Raw receipt and contract hashes were checked. Evidence is retained
in `target/compat-audit-20260914/ordinary-ownership-evidence.json` and its
referenced run directories. Forty-six focused overlay controls, fourteen
native-GC controls, module GC/reentry, and the dump round trip pass; formatting
and strict Clippy pass.

The inventory audit also now compares the complete freshly discovered file
set with the frozen manifest before execution. Added and missing files fail
preflight, including zero-selection files; discovery runs once rather than
once per manifest row. Final aggregate totals are printed only after the
input/provenance checks and durable summary succeed. The inventory controls
exercise added, missing and newly selected cases. These changes strengthen
coverage and reporting without modifying upstream selections or outcomes.

Linux ordinary run `34789196776` has been inspected at the raw-report level.
Module results are independently 37 passed, one upstream skip and zero
unexpected outcomes in both editors. Its GNU discovery explicitly extends
the Linux manifest by 38 names, preserving all 7,883 existing names. The
reviewed Linux lock now records the actual pristine `636f166c` build and
matches the existing native ABI configuration.

The same CI run is **not** an Eglot success. GNU has 44 passes, seven skips
and one unexpected `jsonrpc-error` in
`eglot-test-rust-completion-exit-function`; Emaxx has 45 passes and seven
skips. GNU sent the completion request at 23:38:01.348 and timed out at
23:38:11.351. Both language-server connections finished indexing by
23:38:04.165; a completion payload appears in the retained process output,
so further diagnosis must distinguish server readiness from message delivery.
Neither the upstream deadline nor the expected result has been changed.

Adversarial review found the CI wrapper lost the harness's failure status
through `tee`: GitHub's implicit shell did not enable `pipefail`. Both
compatibility workflows now select explicit Bash, which runs with `-e -o
pipefail`. The actual workflow branch was exercised with independent child
exit codes 0, 1 and 2; all failures reach the workflow exit. The original
green badge remains historical evidence of this defect, not evidence of
Eglot success. Existing raw files correctly recorded the failure throughout.
An explicit single-file workflow input permits continuing with Eglot while
retaining the valid module result. Dispatch controls when validation runs;
publishing a workflow correction does not silently restart all affected files.

Final cross-platform frozen validation and feature/skip diagnostics remain
open in #69. Runtime and native compilation measurements/fixes remain
tracked in #70 and #71.

Linux's single-file Eglot continuation at `ea61026` (CI run `34790872047`,
ordinary artifact `run-1789343728748794028-6642`) has 45 passes, seven skips
and zero unexpected outcomes in each editor; all 52 outcomes match. The
raw contract and six receipt hashes were verified. The original GNU timeout
remains recorded as intermittent; a successful continuation does not prove
its root cause is fixed or substitute for final frozen validation.

Linux Rust checks for commit `fdca75e` passed, but those checks are not an
ordinary Linux frozen-success certificate. The new `frozen-run.yml` workflow
first proposes an explicit Linux oracle pin update: the existing native-ABI
CI builds pristine GNU revision `636f166c`, whereas the old frozen Linux pin
names an Ubuntu repack revision. The proposed lock and actual ordinary
module/Eglot results were reviewed as described above before committing the Linux inventory.
Final frozen mode validates the committed lock; it does not repin it.
The workflow caches build outputs, never compatibility results.

Issue #69 remains open until complete ordinary frozen artifacts on both
platforms meet its acceptance criteria. Completed valid file evidence is
retained during iteration; final integration will receive its own full runs.

The platform policy remains strict after the user's September 14 clarification:
unsupported-platform tests may retain GNU's own skips only with matching
output; supported-platform tests must execute and succeed in both editors.
A proposed relaxation accepting equal skip predicates with different evaluated
diagnostics was removed before publication. A regression control keeps those
different messages, and pass-versus-skip in either direction, mismatching.
The aggregate now records each editor's actual pass/failure/skip/unexpected
counts separately from the parity count. In particular, the six Darwin
seccomp skips still differ in their feature-list diagnostics and remain open;
they have not been relabeled as passes or silently accepted.

The Linux setup review also found that isolated checkouts omitted GNU's two
generated seccomp BPF filters. Their absence could skip tests on the very
platform where they should execute. Both filters now enter the existing
isolated-copy, restore, and provenance checks. Linux preparation requires
them and bubblewrap, and CI installs bubblewrap. Missing filters fail setup
instead of reducing coverage. The setup is conditional on Linux; Darwin
continues to use the unchanged upstream platform guards.

Linux ordinary buffer validation at `44d4aee`, CI run `34791254409`, independently
reports 406 passed and zero skipped/unexpected in each editor. The contract,
six raw receipt hashes, outcome uniqueness, and expectedness were verified;
`linux-buffer-ownership-evidence.json` records that review. Linux formatting,
Clippy, and runner controls also passed at `3447fda` (CI `34791551669`).

Compiler-child profiling exposed a separate runtime defect before usable CPU
measurements could be collected: `current-cpu-time` returned wall-clock
nanoseconds in a one-element list, which `float-time` could not read. It now
returns a CPU `(TICKS . HZ)` pair independently of `current-time-list`. On
Unix it shares actual process resource accounting with `get-internal-run-time`;
that existing primitive retains its old-style list result. The GNU/interpreter
shape and conversion control passes in both time-list modes. Independent
running-binary probes after a 300 ms sleep recorded about 0.5 ms GNU / 1.0 ms
Emaxx CPU, followed by positive CPU time for actual work. Neither reports
sleep as CPU. Raw measurements and executable hashes are in
`cpu-time-behavior.json`; strict Clippy and formatting pass. Compiler profiling
continues from its one-case pilot, without rerunning all 177 tests.

The Linux sandbox continuation at `1eabcb9`, CI `34792206493`, executed
all seven tests with zero skips. GNU passed six and failed bubblewrap's
stdout test; Emaxx passed five and additionally received SIGSYS ("Bad system
call") in the seccomp stdout test. Matching unexpected bubblewrap failures
correctly fail the run. Contract and six raw receipt hashes were verified.
`linux-sandbox-1eabcb9/` retains the reports. These are newly exposed failures,
not resolved skips. The diagnostic workflow mode runs the real binaries and
unchanged filters under strace with empty environments, and separately probes
bubblewrap namespace creation. Diagnostic children set a zero core-size
resource limit; Linux's configured core handler can still delay termination.
Ordinary runs are unchanged. This mode emits
no compatibility certificate and returns failure for failed commands.

Linux diagnostic `34792804162` at `8186acc` identifies the first rejected
syscall: the original main thread blocks in
`futex(FUTEX_WAIT_BITSET|FUTEX_CLOCK_REALTIME)` joining the extra batch worker.
GNU's unchanged filter permits only `FUTEX_WAKE_PRIVATE`. Batch execution now
uses the already adopted corosensei stack backend on the calling OS thread,
preserving large virtual stack capacity and its guard page without the extra
worker/join. The thread-identity, TLS and panic-unwind control passes, as do
formatting and strict Clippy. Further Linux tracing and ordinary validation
remain required; this does not claim that no later syscall will be rejected.

The independent bubblewrap namespace probe fails with "setting up uid map:
Permission denied", while the Ubuntu runner enables restricted user namespaces.
The CI setup uses Ubuntu's documented application-profile mechanism to grant
`userns` to `/usr/bin/bwrap` only, then verifies actual namespace creation.
The general host restriction and GNU filters remain unchanged. The profile
is included in CI metadata. See
[Ubuntu's restricted user namespace design](https://ubuntu.com/blog/ubuntu-23-10-restricted-unprivileged-user-namespaces).

The calling-thread stack change passes the ordinary Mac module file at
`run-1789348711211568000-28393`: 38 passes, no skips or unexpected outcomes
in either editor. The contract and all six raw receipt hashes were verified.

Normal raw test outcomes now also preserve ERT's duration (converted from
its seconds value to integer nanoseconds) and its printed `ert-info` contexts.
These are diagnostic data, not extra timing precision or scoring inputs.
They retain subprocess output that previously disappeared behind a bare
assertion condition and allow descending per-test timing analysis directly
from ordinary artifacts. A live three-case control independently checks
pass, upstream expected failure, and skip in both editors, with the failure's
info retained and numeric durations on all cases. Eighteen comparison controls,
formatting and strict Clippy pass. Older reports without these optional fields
remain readable; provenance still rejects reuse across a reporter change.

Linux continuation `34795453212` at `16d7787` exposes an image-bootstrap
SIGSEGV before sandbox tracing. The alternate batch stack was not registered
with native GC, which therefore used the original pthread stack boundary.
The repair gives owned stacks a scoped physical bound, shared by batch
execution and continuation resumes, restoring it after suspension, return,
and panic. Two new controls pass locally, including actual collection of a
native-only stack root; all five continuation controls, 15 native-GC controls,
and the calling-thread control pass. Formatting and strict Clippy pass.
Linux confirmation is pending; the GDB diagnostic at `1bb749e` preserves the
unfixed bootstrap backtrace. The successful Mac module run alone did not
exercise this fault.

The saved-binary before/after comparison of the ownership/vector changes
completed all 80 native return-type tests with 80 passes each, using the same
older GNU source and matching ABI. Total test durations were 97.211254 and
95.492610 seconds respectively: 1.77% in one sequential pair, insufficient to
claim the compiler latency issue resolved. This comparison is separate from
the newer-oracle staged profile (GNU 75.60 seconds, Emaxx 79.36); differences
between those environments are not a measured code improvement. Raw results
and descending timings are in `perf-ownership-comparison/comp-tests/` under
the audit artifact directory.

Linux GDB diagnostic `34796053074` confirms the unfixed bootstrap SIGSEGV
in `NativeRuntime::collect_native_heap_now`, called from GNU's native
after-load code. At the repaired `73cdeab`, Linux runner checks pass and
sandbox diagnostic `34796264324` successfully builds the image. GNU now
passes both sandbox probes; the scoped bubblewrap prerequisite works.
Emaxx's next rejected calls are `statx` while locating the data directory
under `--seccomp`, and `sysinfo` during early process startup when bubblewrap
installs the filter before execution. These remain failures; neither filter
nor outcome expectations were changed. The traces are retained under
`linux-sandbox-diagnostics-73cdeab/`.

Ordinary Mac module and thread files at `73cdeab` independently pass 38/38
and 32/32 in both editors with no skips or unexpected results. Their runs
are `run-1789349643024911000-31483` and `run-1789349662394812000-31482`;
`ordinary-stack-bounds-evidence.json` records verification of their contracts
and all six raw receipt hashes each.

Startup diagnostic `34796859065` confirms the inherited-filter `sysinfo`
call comes from `_mi_prim_mem_init -> mi_process_init ->
_mi_auto_process_init`, before `main`. Linux now uses Rust's system allocator
(the host libc backend used by GNU), with no mimalloc constructor linked on
that target. Other targets retain their existing allocator. Linux performance
comparison remains required; no improvement is claimed for this change.

The runtime now reads file status through POSIX `stat`, `lstat`, and `fstat`,
as GNU does. Shared file reads retain the selected descriptor and avoid Rust's
extra `statx` birth-time query. The image mapping no longer requests
`MAP_POPULATE`, which GNU uses for a different anonymous-memory path rather
than its file mappings. These changes preserve the unchanged filters; they do
not interpose syscalls or manufacture failed queries to select a fallback.
Four host-filesystem controls, two existing descriptor/loading controls, and
the existing file-metadata/primitive controls pass on macOS. A standalone
image builds successfully; formatting and strict Clippy pass. The initial
raw-filename fixture was corrected after a direct host probe confirmed that
macOS rejects non-UTF-8 names with EILSEQ; Linux's raw-filename check remains.

At `7d03d07`, ordinary Mac file I/O has 13 passes and three matching skips;
modules have 38 passes in each editor. The respective runs are
`run-1789356578632207000-34575` and `run-1789356592583083000-34574`, with
receipt verification in `ordinary-posix-startup-evidence.json`. Linux runner
checks pass (`34802665509`). Its sandbox trace (`34802665698`) gets beyond
the earlier `statx` and allocator calls, then rejects `fcntl(F_GETFD)` when
Rust drops the image descriptor and `sched_getaffinity` during inherited-filter
startup. GNU still passes both probes. The trace also records the host's
systemd core-dump pipe; a zero RLIMIT_CORE did not prevent its delay.

`tools/measure_allocators.py` measures actual cons, vector, string and evaluator
work with alternating before/after order, per-case wall/CPU time, raw reports,
executable/image hashes and unchanged GNU library fingerprints. It emits no
frozen certificate. Linux's allocator diagnostic builds the actual pre-change
commit against the same GNU source and libraries. Both editors pass the local
measurement control. Linux comparison `34803486525` completed four alternating
pairs, with all actual workload assertions passing. Median wall times for
before/after are 0.566765/0.701159 seconds for conses (+23.71%),
0.319650/0.322279 for vectors (+0.82%), 0.198524/0.211833 for strings (+6.70%),
and 1.637066/1.630392 for evaluation (-0.41%). These measurements expose an
allocation regression from using libc on Linux; resolving it remains part of
the performance work. Raw evidence is in `linux-allocator-comparison-deabfbd/`.

Startup GDB run `34803484275` confirms `sched_getaffinity` is called by
`pthread_getattr_np` from Rust's `lang_start_internal`, before application
main. The Linux GNU executable now owns its C ABI entry while retaining
glibc initialization, std's [documented argument constructor](https://doc.rust-lang.org/std/env/fn.args_os.html), standard-stream
descriptor protection, the kernel's main-stack guard, and the registered
guarded stack for batch Lisp. Panics cannot unwind through the C entry;
ordinary return uses std's flushing/cleanup/exit boundary. GNU's inherited
SIGPIPE behavior is preserved in batch mode. Other target entry points remain
unchanged. Three real-binary Linux controls cover arguments/environment/exit,
both inherited SIGPIPE dispositions, and the actual access modes of all three
initially closed standard descriptors. Linux execution is still required.

Runtime files now retain std's file-opening and I/O implementations inside a
private owning wrapper. Its debug validity check uses the permitted F_GETFL
query, then closes exactly once. Descriptor transfers, clones, process pipes,
and terminal files use the same owner. Invalid ownership still aborts. Six
filesystem controls, including an isolated intentional invalid-owner child,
both existing loader-ownership controls, and four pipe/PTY/EOF controls pass
on macOS; strict Clippy and formatting pass. At `d0ac7d0`, ordinary Mac file
I/O passes 13 tests with three matching skips, and modules pass 38 tests in
each editor. Their runs are `run-1789358536418956000-36428` and
`run-1789358626191636000-36427`; `ordinary-owned-file-evidence.json` verifies
their contracts and all raw receipts. Linux sandbox tracing remains pending. No filter, upstream test, expected outcome, or comparison rule
was relaxed for either change.

At `d0ac7d0`, Linux runner checks pass (`34804575549`), as do all three
real-binary startup controls in `34804575854`. Both sandbox probes get past
descriptor cleanup and affinity initialization, then fail at libc's `mremap`
while growing a Rust allocation. GNU's unchanged probes still pass. Linux now
uses a small `GlobalAlloc` wrapper around `System`, retaining the standard
trait's allocate/copy/free reallocation instead of libc's optional `mremap`
path. The backend still supplies real allocation, alignment and zeroing;
there are no intercepted or fabricated syscall results. A control exercises
zeroing, growth, shrinkage and content/alignment preservation for four
alignments and allocations up to one MiB; it passes on macOS, with formatting
and strict Clippy clean. Linux execution remains required.

The separate iterative-cons experiment is retained, not published in the
runtime. It completes 100,000-cell car/cdr chains on a 256 KiB stack and passes
the 28 type and 15 native-GC controls, but the revised version still increases
the Mac list workload median from 0.268138 to 0.290957 seconds (+8.51%) over
four alternating pairs. The exact patch, saved binaries/images and verified
raw reports are under `cons-destruction-tail-*` in the audit directory.
The earlier +9.33% version is also retained. One premature measurement that
started before the rebuild finished is explicitly marked `REJECTED.md` and
excluded. Batch stack capacity has not changed. Allocation cost and Linux's
roughly 29-second crash-handler delay remain open performance work.

The allocator diagnostic also runs `tools/measure_core_latency.py`: independent
children reserve zero, 128 MiB or 8 GiB with the same guard/mapping permissions,
then deliberately abort. Reversed execution order, actual memory maps, limits,
host core policy, exits and timings are retained. This will test whether the
reservation causes the delay; it neither scores tests nor changes editor policy.

At `33e1f3a`, Linux runner checks pass (`34805538780`), as do all three
real-binary startup controls. Sandbox diagnosis `34805538676` passes allocation
growth and reaches native-unit discovery, where `Path::exists` issues the next
rejected `statx`. The diagnostic timed out during that process's crash handling,
so it did not run Emaxx's bubblewrap probe; this is not a sandbox pass. Native
unit existence now uses GNU's effective-identity `faccessat(F_OK)` check, and
native source/DOC file predicates use the existing POSIX metadata layer.
Diagnostic timeouts now stop the whole owned process group, record the timeout,
and continue to the other independent probes. Raw output hashes are retained.

Allocator/core diagnosis `34805541412` completes with all eight allocation
reports independently hash-checked. The core experiment's six raw stdout/stderr
receipts also verify: median abort-to-reap time is 0.214326 seconds without a
reservation, 0.465244 with 128 MiB and 27.674419 with 8 GiB. All children actually
terminate with SIGABRT under the same recorded systemd core policy and
`coredump_filter=00000033`. This establishes reservation size as a cause of the
runner's crash latency. It does not establish that a smaller editor stack is
safe: the cons teardown work and ordinary module validation remain required.

The case-table audit found that GNU's `characters.el` deliberately leaves
non-ASCII-to-ASCII mappings unset, while Emaxx supplied Unicode fallbacks.
Standard tables now start with GNU's ASCII entries and ordinary nil defaults;
unchanged GNU Lisp initializes the other mappings. Casing respects actual
table entries while retaining GNU special/titlecase properties, and final
sigma requires an actual case change. Lower/upper regexp classes use current
table predicates, with their union for case-folded matching. Cache keys include
the down/up tables and parent write stamps even when case folding is disabled.
Replacement-case classification uses these same predicates.

Six live-GNU/byte8/ASCII/property controls pass. Independent probes agree
byte-for-byte on the deliberately unset mappings and on switching/mutating
custom tables. Ordinary Mac regex tests pass 34/34, search 1/1, and casefiddle
10 passes plus one identical upstream skip, all with zero unexpected outcomes.
Runs are `run-1789360859451348000-38673`, `run-1789360913431602000-38896`,
and `run-1789360951970247000-39124`; `ordinary-case-table-evidence.json`
verifies all 18 raw receipts and their contracts. The first casefiddle launch
was refused by the subject lock before testing; only that unexecuted file was
retried. Formatting and strict Clippy pass; Linux validation remains required.

A further adversarial audit found a real success stub in SQLite extension
loading: Emaxx returned true for an existing allowed filename without loading
it. A plain-text `pcre.dylib` returns `t` in the published Emaxx binary and
`nil` in GNU, with both raw probes retained under `sqlite-extension-audit/`.
The upstream test can miss this because optional modules may be absent.
This remains open until actual loading and an executable extension control
replace the stub; it prevents a complete anti-cheating assurance.

Linux sandbox diagnosis `34806574593` at `7d79974` reaches actual Lisp
execution in both seccomp and bubblewrap: each prints `Hi`, then dies at
Emaxx's unconditional `ioctl(1, TCGETS)` during stdout cleanup. Every raw output
hash verifies. [glibc's buffer initialization](https://github.com/bminor/glibc/blob/glibc-2.39/libio/filedoalloc.c)
queries terminal state only for character devices. Emaxx now follows that
condition, and flushing an unused stream no longer allocates/probes its buffer.
The existing actual-process merged stdout/stderr, explicit-flush and traceback
control passes on Mac, with formatting and strict Clippy clean. Linux's normal
`emacs-tests.el` is the next validation, not a substituted diagnostic score.

Ordinary Linux run `34807373691` at `aa74d95` now passes the complete
`emacs-tests.el`: seven actual passes in each editor, no skips or unexpected
outcomes. All six raw receipts and the run contract verify. Emaxx still takes
59.125 seconds versus GNU's 0.500 seconds, so crash-handler/stack performance
remains open. Ordinary Linux regex `34807099781` and casefiddle `34807101531`
also pass at `2dc211f` (34 passes, and 10 passes plus one identical skip).
Their 12 raw receipts verify in `linux-case-table-receipts.json`.

SQLite extension loading now calls the real SQLite C API after GNU's basename
allowlist and Lisp filename expansion. A guard borrowing the live connection
disables C-API loading again; SQL extension loading is never enabled. Invalid
existing libraries return nil, and disallowed names signal `sqlite-error`.
The control compiles a genuine extension against the locked dependency's ABI
header, loads it separately in GNU and Emaxx, executes its registered SQL
function, and verifies inside that function that C-API loading is disabled.
It also covers invalid files, relative paths, trailing separators, DLL suffix
case and the disabled SQL loader. This passes on Mac; ordinary SQLite passes
all 12 upstream tests in each editor with zero unexpected outcomes
(`run-1789361635066000000-40091`, six verified raw receipts). Formatting,
strict Clippy and the fixture's C compiler warnings check pass.

The first SQLite control incorrectly expected `sqlite-select` to signal an
execution-time error which GNU suppresses. Only that control was corrected to
use `sqlite-execute` and rerun; its initial failed output remains retained.
Linux validation is pending. The CI affected-file path accepts an optional
quoted Rust test filter so the actual extension control can accompany the
normal upstream SQLite file without rerunning the full Rust suite.

The next cons-teardown revision empties uniquely owned cells in their existing
allocations, using `Rc::try_unwrap` only when weak observers prevent
`Rc::get_mut`. It preserves shared/native ownership and expires weak observers.
All 28 value controls and 15 native-GC controls pass, including 100,000-cell
car/cdr chains on a 256 KiB stack. This bounds teardown of cons trees; it does
not claim constant-stack destruction of arbitrary mixed object graphs.

Four alternating Mac pairs against the saved, normally validated `679bf45`
binary all complete with the expected workload values. Cons workload median
wall time changes from 0.345746 to 0.277783 seconds (19.66% lower), with the
candidate faster in each pair. Other workload medians do not regress, but their
variation does not establish a causal speedup. All eight raw report hashes
verify in `cons-in-place-mac-comparison/`. Both builds and image creation had
completed before this measurement. The batch reservation is now 128 MiB,
matching Lisp threads; guarded same-thread execution and native stack bounds
are retained. This teardown-only candidate did not yet pass the actual large
list process control: its crash frames show recursive `LispReachability::mark`
exhausting the 128 MiB stack. The failure is retained in
`cons-stack-large-list-crash-frames.log` and is not counted as a pass.

The collector now marks objects before queueing their fields and drains the
reachable graph before weak-table convergence or sweeping. This removes Rust
recursion across Lisp graph edges, preserves the current native words as the
authority, and terminates on cycles. All 21 targeted collector, native-GC,
weak-table, finalizer, marker and stack controls pass. The actual batch process
now constructs, reads and releases an 8,000,005-element list in both GNU and
Emaxx with the 128 MiB reservation (`iterative-gc-large-list-control.log`).
The combined implementation completes four alternating Mac allocation pairs
with all eight raw report hashes and expected values verified in
`iterative-gc-mac-comparison/`. Cons median changes 0.290779 to 0.267034 seconds
(-8.17%); vectors 0.115886 to 0.124855 (+7.74%); strings 0.079306 to 0.079728
(+0.53%); evaluation 0.759668 to 0.751860 (-1.03%). The vector cost remains
visible; this is not a uniform speedup. Ordinary Mac modules then pass 38/38
in both editors with zero unexpected outcomes at
`run-1789362997395408000-41819`; all six raw receipts and the contract verify.
Formatting and strict Clippy pass. Actual Linux crash-latency and allocation
validation remain required.

Linux SQLite control job `34807858244` stopped before runtime assertions:
unfiltered offline Cargo metadata requested an absent Android dependency.
`71af918` restricts the query to Cargo's actual target. The unchanged real
module control passes again on Mac, with formatting and strict Clippy clean;
Linux resumes only that control and the unexecuted ordinary SQLite file in
`34808702941`. The initial failed evidence remains retained.

Compiler attribution now has a portable diagnostic script with explicit GNU
source and Emaxx paths. It retains pass lists, raw independent test results,
child inputs and resource usage, process identity/CPU samples, and hashes of
inputs/outputs verified after execution. Linux reads descendants from each
task's `/proc` children; Darwin uses libproc. Sampling can miss short-lived
children, so actual `/usr/bin/time` usage remains available for each compiler
invocation. A real parent/child sampler control and one unchanged upstream
compiler test pass on Mac, with all output hashes verified. The diagnostic is
separate from normal frozen certification; Linux attribution remains pending.

Linux now validates the collector/stack change through ordinary
`emacs-tests.el` in CI `34809182319`: seven actual passes per editor, no skips
or unexpected results, with all six raw receipts and the contract verified.
Emaxx test-body time falls from the prior 59.125 seconds to 1.571324 seconds;
GNU takes 0.535897 seconds. The forbidden-subprocess case is 1.008877 seconds
versus GNU's 0.308253, and both allowed-stdout paths succeed. This measures the
real upstream process/filter behavior, not the standalone core diagnostic.

Linux's corrected SQLite continuation `34808702941` also passes the genuine
extension control and all 12 ordinary tests in each editor. Raw receipts,
the contract and control-log hash verify in `linux-sqlite-receipts.json`.

The complete Linux 80-case compiler attribution at `37197db` passes every
selected test independently. All raw output hashes and final input checks
verify. GNU test bodies total 9.204063 seconds versus Emaxx's 53.479885.
For the 80 compiler children inside those tests, aggregate elapsed time is
7.37 versus 42.99 seconds. Pre-instrumentation Lisp CPU totals 2.432252 versus
17.092326 seconds; loading `comp` uses 2.500924 versus 13.370486 CPU seconds.
Backend CPU is 1.417475 versus 5.260953 seconds. These are stage attributions,
not additive independent costs or complete sampled-process accounting.
The dominant Linux gap is repeated startup/loading CPU work. The next
diagnostic samples only the launched compiler process tree, keeps the editors
under the ordinary runner account, and records eight unchanged upstream cases
with `perf`; no global profiling policy or compatibility selector is changed.
