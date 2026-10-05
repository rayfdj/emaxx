# Native and bytecode call entry — 5 October 2026

The latest [integer/VM checkpoint](runtime-representation-integer-vm-draft.md)
supersedes this runtime with applied source290 (`b870a848`). Source289's complete
Linux Rust and frozen runs are closed and passed; the linked checkpoint records
their evidence and the new source290 validation launches.

The [complete runtime goal](runtime-representation-goal.md) remains incomplete.
**Source289 is applied and pushed as `ae1fcb0f`** on `runtime-char-tables`.
Main remains `21d20f0e`; PR79 stays draft. This follows the
[source286 dispatch checkpoint](runtime-representation-vm-dispatch.md).

## Removed work and GNU reference

Fixed native calls now fill the existing `NativeInvocation.arguments` registers
directly from authoritative `Value::word()` values. This removes an intermediate
SmallVec and copy. The MANY convention retains its owned, writable SmallVec;
shared Rust argument slices are never cast to writable native pointers.
GNU `eval.c:funcall_subr` supplies the fixed/MANY ABI reference. The existing
trampoline remains necessary for Rust/nonlocal-exit boundaries. Null and eight-arg
checks, optional nil padding, handlers, roots, collection entry, returns and
unwinds are unchanged. Source288 isolates this change for measurement.

Source289 additionally reads each authoritative closure slot once in
`ByteCodeObject::from_fields`, retaining shape checks and argument/code/depth
error order. Those reads are ordinary checked accesses without Lisp callbacks;
no mutable payload is cached. The VM uses `ByteCodeObject` directly, removing
the duplicate transient `BytecodeActivation` and its infallible constructor.
Root tracing still covers the same fields. Direct dispatch recognizes Bcall
opcodes and checks their operand widths before using the unchanged full call
path, avoiding a second fetch/decode. GNU `bytecode.c:setup_frame/Bcall` is the
reference. No new unsafe block, mirror, registry, special benchmark path or
GC-cadence change is introduced.

## Selected correctness evidence

The [closed selected archive](handover/2026-09-30-shared-reader-draft/source289-native-vm-calls-selected-manifest.json)
verifies **526 inputs**, exact byte/mode replays from `d1120c70` and main, and
zero-warning formatting, all-target check, Clippy and diff checks.
**274 selected tests pass in each of gate and release**, with no failure or
ignore. Every previous test name remains. Both required suspended-root contracts
pass. The sole existing VM test edit removes its obsolete constructor call;
its semantic assertions and callbacks are unchanged.

**45 ordinary same-input comparisons match actual GNU output**, including the
entire generated 198-row / 4,088,269-byte reader/printer file. New fixtures cover
native fixed arguments 0–8, MANY arguments, optional/rest parameters, identity
and order through collection, and 78 bytecode call-width rows. Packed, byte and
word operands exercise interpreted, bytecode and native callees, including
arities 17, 255, 256 and 257 where encodable. GNU and the prior runtime matched
the call-width fixture before the VM edit. Expected bytes come from GNU.
Source288 separately passes 273 selected tests per profile and 44 ordinary
comparisons. These counts overlap; they are not summed.

## Unchanged full16 diagnostic measurement

The [performance archive](handover/2026-09-30-shared-reader-draft/source289-native-vm-calls-performance-manifest.json)
retains three rotated repetitions of the unchanged 16-workload pilot across
source286, source288 and source289: **288 ordinary processes, all actual answers
and execution modes verified against GNU**. Each build uses its own executable,
image and native artifact. Every timed body exceeds 100 ms. Builds, tests and
profiles are separate from timing. Startup, total time, RSS, GC reports and
allocation errors remain in the raw results. Emaxx allocation counters remain
explicitly unavailable.

Source289's median bytecode-call body is **15.350% lower than source286**;
bytecode-to-native is **3.540% lower**. The raw median geometric ratio is
**0.98799**. This small diagnostic does not establish a significant broad gain.
The paired Emaxx/GNU ratio geometric means are **3.03264, 3.03326 and 2.97399**
respectively. They are a different aggregate from candidate/baseline body time.

| Case | Source286 / GNU | Source288 / GNU | Source289 / GNU | Source289 body vs 286 |
| --- | ---: | ---: | ---: | ---: |
| interpreted-lexical | 2.4615× | 2.3699× | 2.4067× | -0.158% |
| interpreted-dynamic | 2.2666× | 2.2231× | 2.2295× | +1.271% |
| interpreted-calls | 2.7249× | 2.7508× | 2.7163× | +0.510% |
| bytecode-calls | 5.5170× | 5.5564× | 4.6277× | -15.350% |
| native-execution | 3.3779× | 3.5329× | 3.3887× | -1.382% |
| interpreted-to-native | 2.5211× | 2.5572× | 2.4963× | +0.553% |
| bytecode-to-native | 9.2980× | 9.1525× | 8.7821× | -3.540% |
| native-to-interpreted | 2.7384× | 2.7156× | 2.7154× | +0.149% |
| native-to-bytecode | 2.8601× | 2.8733× | 2.9004× | +0.199% |
| cons-allocation | 1.9041× | 1.8979× | 1.8721× | -1.037% |
| list-traversal | 2.1544× | 2.1228× | 2.0965× | -0.138% |
| mapcar | 2.3181× | 2.3361× | 2.3087× | +0.803% |
| explicit-gc | 1.0606× | 1.0813× | 1.0615× | +0.180% |
| upstream-sort | 5.1376× | 5.1065× | 5.1642× | +0.474% |
| upstream-undo | 5.5648× | 5.6069× | 5.6136× | +0.067% |
| upstream-bindat | 3.4124× | 3.3922× | 3.4033× | -0.516% |

The intermediate source288 native-execution body regresses 3.448% versus 286;
that result is retained. One sample/process, no warmup and three outer repeats
do not satisfy the calibrated nine-round/three-sample contract. The locked
16 cases and **every-case upper 95% ratio ≤1.03** remain unchanged and unmet.

Separate post-change sampling repeats each bytecode workload 20 times; all
40 results/modes pass. Bytecode-call self samples concentrate in `run_fast`
(708), `run_frames` (387), closure entry (69), frame exit (49) and backtrace
context (45). Bytecode-to-native includes `run_fast` (637), native invocation
(107), call dispatch (104), `run_frames` (100), backtrace arguments (72) and
handler synchronization (35). These are rankings from instrumented runs,
not timing ratios. Continue from the actual stack/constant, frame and transition
paths, preserving mutation, roots, errors, hooks and backtraces.

## Complete platform evidence and current launches

The [closed source286 platform archive](handover/2026-09-30-shared-reader-draft/source286-complete-platform-results-manifest.json)
verifies macOS Rust **3,131 passes / two existing ignores**, native artifact
identity, and terminal **226 scenarios / 686 exact comparisons** with none
unexecuted. macOS frozen remains **7,915 matches / six strict feature-diagnostic
differences**. Linux frozen verifies **519 files / 7,928 matches / 1,038 successful
processes**, including all 177 compiler tests in both editors.

Source286 Linux Rust remains failed: **2,285 passes / one GNU census reference
failure**, 756 library names and both Cargo stages unexecuted. GNU's first empty
vector sample is -9 instead of zero, identical to preceding failures; this
assertion stops before Emaxx. Its cause and source276's original Emaxx census
failure remain unresolved. Neither is converted into a pass. All source286
supervisors have exited; no GNU timeout retry was needed for this terminal run.
These complete results do not certify source289.

[Source289 Linux launches](handover/2026-09-30-shared-reader-draft/source289-full-validation-launch.json)
select exact `ae1fcb0f`: [Rust37265044228](https://github.com/rayfdj/emaxx/actions/runs/37265044228)
and [frozen37265047878](https://github.com/rayfdj/emaxx/actions/runs/37265047878).
The [closed Rust archive](handover/2026-09-30-shared-reader-draft/source289-complete-linux-rust-manifest.json)
verifies 3,145 passes/two existing ignores, all 3,044 library names and native
artifact identity. Frozen is still running; inspect its final receipts before
claiming a result. Complete source289 macOS validation has not been launched;
final exact-source validation remains required.

## Reproduction and continuation

The selected archive includes incremental and complete portable patches, source
manifests, commands, raw outputs, GNU mappings, helper code and application/push
receipts. Replays match all 526 bytes/modes inputs. Incremental SHA-256 is
`3fd147dd685a60f3874eda5190a6e9fe669060c301b5c86d66d3d02878fae974`;
complete-from-main is
`299ef0762fd02eebbdcca7119d9ea4a896ef78348247fa55480e98159ad2a22b`.
Large binaries/images are identified by hash, not packaged; rebuild elsewhere.
Local evidence stays under `target/runtime-goal/resume-2026-09-28`.
Frozen candidate checkouts are `recovered-2026-10-05/native-argument-words/emaxx`
and `recovered-2026-10-05/vm-call-entry/emaxx`. Use each executable at its original
path so it selects its own image and native artifact.

Original idle supervisors were withdrawn before timing/validation to batch
source289 checks while source286 terminal work completed. Their original helpers
and waiting receipts remain; `call-candidates-queue-reordering.json` supersedes
them. The replacement pipelines are closed. The initial application-helper
generator quoting error occurred before any source mutation and is retained.
The first native fixture lacked explicit lexical binding; both preliminary and
explicit-lexical comparisons remain. No failed run is relabeled successful.
The public timing evidence omits unrelated host application process listings;
it retains the local listing hash and checked absence of competing task jobs.

Sblocks are already applied in source279. Real intervals, unified pure storage,
allocated-symbol authority, remaining host adapters, physical accounting and
real counters, public ownership/serialization, VM stack layout/capacity, remaining
September 21 findings, final adversarial review and complete final validation
remain open. GNU performance parity remains open. This measured checkpoint does
not replace any requirement in the full goal.
