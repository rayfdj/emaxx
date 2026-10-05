# Direct bytecode dispatch checkpoint — 5 October 2026

The current [source289 call-entry checkpoint](runtime-representation-call-entry.md)
supersedes this historical source286 checkpoint.

The [complete runtime goal](runtime-representation-goal.md) remains incomplete.
**Source286 is applied and pushed as `654aca96`** on `runtime-char-tables`.
Main remains `21d20f0e`; PR79 stays draft. Continue measured VM/call work without
turning unrelated correctness exploration into another prerequisite.

The [selected evidence](handover/2026-09-30-shared-reader-draft/source286-vm-dispatch-selected-manifest.json)
verifies **522 inputs**, exact byte/mode replays from source284 and main,
warning-free formatting/check/Clippy/diff checks, **118 selected passes in both
gate and release**, and **43 ordinary byte-for-byte GNU comparisons**.
Both suspended-root contracts, active code/constant mutation, error fallback,
unwind and native transitions remain covered. No prior fixture or test is removed.
The generated reader/printer comparison still checks all 198 rows / 4,088,269 bytes.
These are bounded results, not complete-platform or universal correctness claims.

## Mechanism and corrected baseline

GNU `bytecode.c:FETCH/NEXT` dispatches on the actual opcode. Rust `run_fast`
now does the same, avoiding a decoded `Instr` and a second `Op` dispatch for hot
stack, constant, branch, arithmetic and array instructions. Checked operand widths
and a shared constant-index validator remain. Fallback decodes once and passes
that instruction to the full interpreter. Actual PC errors, untaken destinations,
backward-jump quit/GC cadence and current code-string borrowing are preserved.
No retained instruction cache, mirror, new unsafe block or benchmark-name branch
is introduced. This is an ordinary VM path improvement.

Finite changed-opcode coverage exposed an existing `%` defect: source284 accepts
a floating operand where GNU signals `integer-or-marker-p`. The repair follows
`data.c:Frem/Fmod`, their integer/number-or-marker coercion order, and
`marker.c:marker_position`. Only `mod` accepts floats; detached markers retain
GNU's error. Existing remainder arithmetic is unchanged.

**Source287 is the separate correctness-only baseline**, keeping both bytecode
files identical to source284 while applying that operand repair and the same
76-case fixture. It passes strict checks and all 43 ordinary GNU comparisons.
It has no separate gate/release libtest claim. Source286 adds direct dispatch.
The original baseline and its actual wrong remainder result remain retained.

## Unchanged 16-workload diagnostic comparison

The [performance evidence and profiles](handover/2026-09-30-shared-reader-draft/source286-vm-dispatch-performance-manifest.json)
retain three rotated repetitions of the unchanged 16-workload pilot for all three
builds: **288 ordinary processes, all actual results and modes equal GNU and
the locked expectations**. Each process has one sample and no warmup; every
body exceeds 100 ms. Each build uses its own executable/image/native artifact.
No build, validation or profiling job runs during these measurements.

Median Emaxx body time falls **18.2% for bytecode calls** and **4.4% for
bytecode-to-native** versus the corrected baseline. Several other medians rise,
including native-to-interpreted (+4.4%), explicit GC (+3.9%) and native-to-bytecode
(+3.5%). They remain unfavorable observations; this small pilot does not establish
their cause or statistical significance. The geometric mean of raw candidate /
corrected medians is **0.9961**: no meaningful overall speedup claim follows.

Geometric means of the per-case median Emaxx/GNU ratios are **3.0673 original,
3.0938 corrected, 3.0421 optimized**. The different aggregate uses paired GNU
samples and is not interchangeable with raw candidate/baseline time. GNU remains
materially faster. Emaxx allocation counters remain explicitly unavailable.
The calibrated nine-round, one-warmup, three-sample contract and unchanged 3%
ceiling are still unsatisfied. No tolerance, workload or expected result changed.

| Workload | Original / GNU | Corrected / GNU | Optimized / GNU | Optimized body vs corrected |
| --- | ---: | ---: | ---: | ---: |
| interpreted-lexical | 2.462× | 2.479× | 2.462× | +1.1% |
| interpreted-dynamic | 2.240× | 2.308× | 2.197× | -0.4% |
| interpreted-calls | 2.554× | 2.707× | 2.791× | +1.2% |
| bytecode-calls | 6.522× | 6.731× | 5.535× | -18.2% |
| native-execution | 3.401× | 3.347× | 3.374× | +1.2% |
| interpreted-to-native | 2.525× | 2.583× | 2.526× | -1.1% |
| bytecode-to-native | 9.676× | 9.879× | 9.004× | -4.4% |
| native-to-interpreted | 2.755× | 2.698× | 2.697× | +4.4% |
| native-to-bytecode | 2.877× | 2.893× | 2.878× | +3.5% |
| cons-allocation | 1.891× | 1.837× | 1.927× | +1.8% |
| list-traversal | 2.180× | 2.222× | 2.128× | -4.1% |
| mapcar | 2.351× | 2.368× | 2.303× | -0.0% |
| explicit-gc | 1.081× | 1.022× | 1.082× | +3.9% |
| upstream-sort | 5.012× | 5.166× | 5.215× | +3.1% |
| upstream-undo | 5.657× | 5.675× | 5.637× | +2.4% |
| upstream-bindat | 3.418× | 3.499× | 3.597× | +1.9% |

Separate post-change profiles repeat each locked-size bytecode workload 20 times;
all 40 results/modes pass and both call graphs are nonempty. Bytecode-call top
samples still concentrate in `run_fast` (835), `run_frames` (364),
`closure_program` (80) and fallback `fetch_instruction` (59). Native-transition
samples include `run_fast` (527), `run_frames` (219), `NativeRuntime::invoke`
(148), call dispatch (96), backtrace arguments (63), handler synchronization
(42) and fallback decoding (42). Sampling counts describe these runs, not direct
before/after cost ratios. Continue from these ordinary paths, preserving hooks,
errors, mutation, backtraces and roots. Native argument copying requires an
explicit ABI/aliasing review before removal; no such change is in source286.

## Full validation and preserved failures

The [closed source284 platform evidence](handover/2026-09-30-shared-reader-draft/source284-complete-platform-results-manifest.json)
now verifies **3,130 macOS Rust passes / two existing ignores**, including native
artifact identity, and **all 7,928 Linux frozen outcomes / 519 files / 1,038
successful processes**. macOS frozen remains failed: 7,915 matching outcomes and
six existing feature-diagnostic differences, with all processes successful.
Its original terminal run has eight matching scenarios/comparisons, then GNU
fails to display the `org-fold` startup target; 678 comparisons never execute.
A separate unchanged scenario retry, with 180-second readiness for both editors
and a 300-second total bound, finishes in **13.195 seconds with the screen equal**.
The original failed full run remains failed. Emaxx was never slowed to imitate GNU.

The [source284 Linux Rust failure](handover/2026-09-30-shared-reader-draft/source284-complete-linux-rust-failure-manifest.json)
remains 2,284 passes / one GNU reference census assertion failure (-9 versus zero),
with 756 library names and both Cargo stages unexecuted. The failed control stops
before Emaxx. Source276's earlier Emaxx census failure also remains unexplained.

[Source286 full-validation launches](handover/2026-09-30-shared-reader-draft/source286-full-validation-launch.json)
select exact `654aca96`: [Linux Rust37254821073](https://github.com/rayfdj/emaxx/actions/runs/37254821073),
[Linux frozen37254829602](https://github.com/rayfdj/emaxx/actions/runs/37254829602) and macOS supervisor
**10785** in `target/runtime-goal/recovered-2026-10-03/vm-dispatch-full/emaxx`.
All are closed. The [complete platform archive](handover/2026-09-30-shared-reader-draft/source286-complete-platform-results-manifest.json)
verifies 3,131 macOS Rust passes/two existing ignores, terminal 226 scenarios/686
matches and 7,928 Linux frozen matches. macOS frozen retains six diagnostic
failures. Linux Rust retains 2,285 passes/one GNU reference census failure, with
756 library names and both Cargo stages unrun. The GNU assertion precedes Emaxx;
its cause remains unresolved. These results do not certify source289. The
selected and corrected-baseline checkouts stay frozen.

Source285's initial fixture used invalid `#(...)` vector notation; GNU rejected
it before Emaxx ran. The corrected `[...]` fixture revealed the real remainder
mismatch (75 rows equal / one wrong). Source285 then failed strict Clippy on a
manual range pattern; no selected tests ran. Source286 uses the equivalent range
without a lint suppression. Source287 image preparation first omitted the
fingerprint helper, then `EMACS_TEST_DIRECTORY`; both setup failures remain.
The unchanged build later produces its own valid image and all comparisons pass.
An audit removal pattern wrongly expected a blank line after an EOF test, and a
benchmark preflight looked for measurement files in the runtime manifest. Both
helpers failed before claiming results; corrected helpers preserve their originals.
These setup mistakes are assistant errors, not runtime regressions.

The first local source284 platform package accidentally included four host tool
executables despite its exclusion label. The published successor excludes them,
retains their hashes and every comparison artifact, and records the correction.

## Continuation

Portable patches, source manifests, failed helpers, commands, exact outputs,
all timing samples and profiles are in the linked archives. Large executable and
image hashes identify local evidence; rebuild on another machine. Local files
remain under `target/runtime-goal/resume-2026-09-28`; source286 is
`recovered-2026-10-03/vm-opcode-dispatch/emaxx`, source287 is
`recovered-2026-10-03/remainder-corrected-baseline/emaxx`.

Continue measured call/VM improvements from the newer linked checkpoint.
Real intervals, unified pure storage, symbol authority, remaining host adapters,
physical accounting/counters, public ownership/serialization, final adversarial
review and calibrated performance acceptance remain open. The complete goal is
not replaced by this checkpoint, by green selected tests, or by the bytecode gain.
