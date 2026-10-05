# Integer and VM checkpoint — 5 October 2026

The [full runtime goal](runtime-representation-goal.md) is active and incomplete.
**Source290 is applied and pushed as `b870a848`.** Source291 is a validated,
measured alternative that remains unapplied. Main remains `21d20f0e`; PR79 stays draft.
Read the [applied call-entry checkpoint](runtime-representation-call-entry.md)
for its implementation and prior measurements.

## Closed source289 Linux Rust validation

The [complete Linux Rust archive](handover/2026-09-30-shared-reader-draft/source289-complete-linux-rust-manifest.json)
verifies exact `ae1fcb0f` in [run37265044228](https://github.com/rayfdj/emaxx/actions/runs/37265044228):
**3,145 passes / two existing ignores**. All 3,044 library names are checked
against actual verdicts: 3,042 pass; binaries add 61 and integration adds 42.
Native artifact identity, all four retained inputs and 526 source inputs verify.
The unchanged GNU census reference passes this time; earlier failures and the
source276 Emaxx census failure remain unexplained and preserved.
The [closed Linux frozen archive](handover/2026-09-30-shared-reader-draft/source289-complete-linux-frozen-manifest.json)
verifies [run37265047878](https://github.com/rayfdj/emaxx/actions/runs/37265047878):
519 files, **7,928 matching outcomes**, 1,038 successful processes, execution
hashes and retained GNU inputs. Each editor reports 7,670 passes, 47 expected
failures and 211 skips. Complete source289 macOS validation remains unstarted;
these predecessor results do not certify source290.

## Source290: correct selected results, unfavorable timings retained

Source290 outlines the unchanged bignum allocation arm of `Value::Integer` and
forces the immediate range/tagging path inline. GNU `lisp.h:make_int/make_fixnum`
is the reference. Ordinary ARM64 release disassembly verifies that `run_fast`
has zero calls to `Value::Integer`, versus two call sites in source289; bignum
allocation calls remain conditional. This is static evidence, not a speed ratio.

The VM also updates existing operand slots for boolean, cons, comparison,
arithmetic and vector operations, following GNU `bytecode.c`'s TOP/POP paths.
It removes the duplicate optimistic match between `run_fast` and full dispatch.
Full opcode/error arms, checked operands, callback boundaries and roots remain.
Values are Copy with no destructor; these slot changes do not add an allocation,
collection, cache, mirror, unsafe block or alternate representation.

The [closed selected archive](handover/2026-09-30-shared-reader-draft/source290-integer-vm-selected-manifest.json)
verifies **528 inputs**, exact portable replays from `fc7db1dc` and main,
warning-free strict checks, **325 selected passes in both gate and release**,
zero failures/ignores, and **46 ordinary actual GNU comparisons**. Existing
assertions and fixture bytes are unchanged. The added 588-row boundary fixture
checks fixnum/bignum results, types, overflow, operand errors and collection;
it matched GNU and source289 before candidate execution. The full generated
reader/printer file still matches all 198 rows / 4,088,269 bytes.

The [unchanged full16 diagnostic](handover/2026-09-30-shared-reader-draft/source290-integer-vm-performance-manifest.json)
verifies **192 ordinary processes**, three interleaved repetitions per build,
with actual equal GNU answers and modes. All bodies exceed 100 ms. Own valid
executable/image/native inputs, every sample, startup/total/RSS/GC data and
unavailable allocation reports remain. Builds, tests and profiles are separate
from timing. No workload, timeout, expectation or tolerance changes.

| Case | Source289 / GNU | Source290 / GNU | Candidate body vs 289 |
| --- | ---: | ---: | ---: |
| interpreted-lexical | 2.4639× | 2.4385× | -7.828% |
| interpreted-dynamic | 2.2351× | 2.1466× | -4.903% |
| interpreted-calls | 2.8172× | 2.7605× | -12.414% |
| bytecode-calls | 4.6170× | 4.1698× | -14.367% |
| native-execution | 3.3514× | 3.3619× | -3.581% |
| interpreted-to-native | 2.4871× | 2.4707× | -3.891% |
| bytecode-to-native | 8.7824× | 8.7995× | -2.912% |
| native-to-interpreted | 2.7250× | 2.7142× | -2.005% |
| native-to-bytecode | 2.8890× | 2.8746× | -0.917% |
| cons-allocation | 1.8495× | 1.8067× | -2.114% |
| list-traversal | 2.1121× | 2.1158× | -2.777% |
| mapcar | 2.3108× | 2.2576× | -0.234% |
| explicit-gc | 1.0724× | 1.0807× | +1.467% |
| upstream-sort | 5.2094× | 5.3456× | +3.764% |
| upstream-undo | 5.5420× | 5.7443× | +6.969% |
| upstream-bindat | 3.4464× | 3.5033× | +8.685% |

The raw median geometric ratio is 0.97505; the paired GNU ratio geometric means
are 2.98514× and 2.95864×. Those aggregates answer different questions.
No broad statistical gain or cause of the unfavorable cases is established by
this small pilot. The original decision held source290 for investigation; the
second complete cohort and application review below supersede that decision.
Real counters, calibrated nine-round
measurements and the unchanged every-case upper 95% ratio ≤1.03 remain required.

## Second cohort and source290 application

The [closed comparison and application review](handover/2026-09-30-shared-reader-draft/source290-inlining-review-and291-results-manifest.json)
retains all **288 additional processes**, three rotated full16 pilots for each of
source289/290/291. Every actual answer and execution mode matches GNU, and all
bodies exceed 100 ms. No builds, tests or profiles run during timing. The
paired GNU ratio geometric means are **3.05806× / 2.95393× / 3.00323×**.
All samples remain, including slower cases. The original audit receipt's prose
mistakenly names a predecessor cohort; its actual commands, inputs and samples
identify 289/290/291, as explicitly corrected in the archive qualification.

Across both cohorts' six samples per editor/source/case, source290's bytecode-call
median is **14.264% lower** than source289; the geometric mean of raw median body
ratios is **0.97158**. Sorting is 0.398% lower, undo **1.943% higher**, Bindat
**1.499% higher**. The earlier larger upstream increases do not repeat in the
second cohort. The combined paired GNU ratio geometric mean is **2.95454×**.
These are descriptive diagnostic results, without statistical acceptance or
an established explanation for variation between cohorts.

Source291 does not improve the overall tradeoff and stays unapplied. Source290
is applied after review of both complete cohorts, preserving the original hold,
all unfavorable results and the inlining alternative. The applied 528 inputs and
file modes match the validated candidate exactly. No workload, expectation,
timeout, tolerance, collector cadence or comparison rule changes.

[Source290 Linux CI](handover/2026-09-30-shared-reader-draft/source290-full-validation-launch.json)
selects exact `b870a848`: [Rust37269853253](https://github.com/rayfdj/emaxx/actions/runs/37269853253)
and [frozen37269857506](https://github.com/rayfdj/emaxx/actions/runs/37269857506).
The [closed Rust failure](handover/2026-09-30-shared-reader-draft/source290-complete-linux-rust-failure-manifest.json)
records 2,288 passes and one recurring GNU empty-record census reference failure
(−7 versus 2), before Emaxx. The remaining 756 library names and both Cargo stages
never run. Frozen remains in progress; full source290 macOS is unstarted. Read the
[latest buffer-line continuation](runtime-representation-buffer-lines.md) for the
next unapplied drafts and all current limits.

## Profile limits and next targets

Separate sample profiles cover bytecode calls, bytecode-to-native, interpreted
lexical execution, native execution, sorting and undo. All 120 reported results
and modes pass. The [profile-scope qualification](handover/2026-09-30-shared-reader-draft/source290-profile-scope-qualification.json)
is essential: sampling spans the driver, checks and explicit collection before
each body. All six profiles report zero body GC deltas. Collector stacks must
therefore not be described as timed-body costs. Instrumented durations are not
timing evidence, and these are not exclusive body profiles.

Non-GC stacks identify evaluator/list-length/binding work in interpreted execution,
native subr dispatch and recursive funcall transitions in the native workload,
rope slicing and evaluator work in sorting, and `Buffer::line_start_at` in undo.
These are leads for ordinary-path review; no new cause or repair is established.
The VM and call profiles remain in the archive as well. Preserve their full trees.

## Source291: closed isolated inlining comparison, unapplied

The [portable source291 draft](handover/2026-09-30-shared-reader-draft/source291-unfinished-inlining-draft-manifest.json)
changes only source290's `Value::Integer` annotation from forced to ordinary
inlining. The outlined bignum path, VM implementation, tests and fixtures remain
identical. That archive is the original unfinished launch snapshot; the closed
comparison archive above supplies its final evidence.

All **528 inputs and modes** replay exactly from `fc7db1dc` and main. Strict checks
pass with zero warnings; **325 selected tests pass in each profile**, no failures
or ignores. All **46 ordinary GNU comparisons** pass, including the 588 numeric
boundary rows and 198 generated reader/printer rows. Performance is closed as
reported above. Both validation supervisors have exited successfully.

Local frozen candidates are under `target/runtime-goal/recovered-2026-10-05/`:
`vm-integer-path/emaxx` (290), `vm-integer-inlining/emaxx` (291). Helpers and raw
evidence are under `target/runtime-goal/resume-2026-09-28`. The executed validation,
comparison, timing and independent audit helpers remain in the closed archive.
All local selected/profile/timing pipelines are closed. Preserve frozen candidates
and their own executable/image/native artifacts; do not rerun their write-once helpers.

Source290's original edit generator stopped at a text-pattern assertion after
writing only types.rs; the VM successor was written before validation. Its
selected wrapper later returned 1 because a final status-prefix check was stale,
although all three child stages returned zero. Both errors remain recorded.
The pre-timing audit also captured live orchestration state/stdout; the separate
closed audit reruns all raw checks after those pipelines finish and supersedes
that provisional evidence snapshot. No runtime/test/performance failure is hidden
or converted by these orchestration corrections.

Remaining full-goal requirements are unchanged: intervals, pure storage, symbol
authority/adapters, physical accounting and actual counters, public ownership,
VM stack layout/capacity and further measured call work, remaining audit findings,
final adversarial review, full exact-source validation and GNU performance parity.
