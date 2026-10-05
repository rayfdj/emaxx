# Integer and VM measurement drafts — 5 October 2026

The [full runtime goal](runtime-representation-goal.md) is active and incomplete.
**Applied runtime remains source289, `ae1fcb0f`.** Source290 and source291 below
are separate, unapplied candidates. Main remains `21d20f0e`; PR79 stays draft.
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
Linux frozen37265047878 is still running at publication. Complete source289
macOS validation remains unstarted; predecessor source286 results do not certify it.

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
this small pilot. **Hold source290 unapplied while investigating those cases.**
Do not select only its faster bytecode result. Real counters, calibrated nine-round
measurements and the unchanged every-case upper 95% ratio ≤1.03 remain required.

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

## Source291: unfinished isolated inlining comparison

The [portable source291 draft](handover/2026-09-30-shared-reader-draft/source291-unfinished-inlining-draft-manifest.json)
changes only source290's `Value::Integer` annotation from forced to ordinary
inlining. The outlined bignum path, VM implementation, tests and fixtures remain
identical. It tests whether compiler-chosen inlining avoids the unfavorable
upstream observations; that hypothesis is unproven.

All **528 inputs and modes** replay exactly from `fc7db1dc` and main. Strict checks
pass with zero warnings; selected validation is in progress at publication.
Ordinary comparisons and performance are pending. This launch snapshot is not a
passed candidate. Inspect `source291-validation-state.json`, its final result and
`inline-selection-state.json` before continuing. Do not restart active jobs or
apply either draft without reviewing its actual results.

Local frozen candidates are under `target/runtime-goal/recovered-2026-10-05/`:
`vm-integer-path/emaxx` (290), `vm-integer-inlining/emaxx` (291). Helpers and raw
evidence are under `target/runtime-goal/resume-2026-09-28`. Source291 validation
uses `run-source291-validation.py`; `finish-source291-selected.py` waits for it,
then performs the same ordinary comparisons and an independent audit. A new
interleaved baseline/forced/compiler-chosen measurement is still to be prepared
after selected validation. Preserve source289 and source290 binaries and their
own images/native artifacts for that comparison. No source291 timing is launched.

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
