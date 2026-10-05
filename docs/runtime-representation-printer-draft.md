# Canonical printer/reader checkpoint and performance baseline — 3 October 2026

**Superseded by the [source286 VM checkpoint](runtime-representation-vm-dispatch.md).**
Source284 remains the preserved printer baseline. Its full runs are now closed:
3,130 macOS Rust passes/two ignores, 7,928 Linux frozen matches, six macOS frozen
diagnostic failures, and an incomplete terminal run after eight matches. The
unchanged GNU org-fold retry obtains an equal screen in 13.195 seconds. See the
[current closed evidence](handover/2026-09-30-shared-reader-draft/source284-complete-platform-results-manifest.json).


Read the [complete goal](runtime-representation-goal.md), [handover](../HANDOVER.md)
and [allocation recovery](runtime-representation-string-allocation-recovery.md).
The full goal remains incomplete. **Source284 is applied and pushed as `9c5b5356`**
on `runtime-char-tables`; main remains `21d20f0e`, and PR79 remains draft.
The user directs bounded correctness repairs followed by performance work from
measured profiles, without continually adding unrelated edge-case probes.

The [closed selected evidence](handover/2026-09-30-shared-reader-draft/source284-reader-printer-selected-manifest.json)
retains source282–284, commands, failures, actual outputs and portable patches.
All **520 source/test inputs and file modes** replay from `d64040a7` and main.
Source284 passes zero-warning formatting, all-target checking, Clippy and diff
checks, **six targeted controls in each profile**, and **42 ordinary exact GNU
comparisons**. These include the native-constant failure, all **198 generated
rows / 4,088,269 bytes**, and **679 reader bounds cases**. Every prior fixture and
test remains. These overlapping counts do not certify full-platform validation.

The frozen selected checkout is
`target/runtime-goal/recovered-2026-10-03/reader-bounds-ascii/emaxx`.
Receipts/helpers are under `target/runtime-goal/resume-2026-09-28`.
Preserve frozen inputs and executed helpers; develop changes in successors.

## Implementation and remaining adapters

GNU `print.c:print_object` reads actual character codes before choosing octal,
hexadecimal or literal output. The old Rust text view lost non-Unicode Emacs
codes. `PrintOutput` carries canonical bytes through recursive printing, string
results, buffer/marker insertion, callbacks, stdout and native serialization.
No retained Lisp mirror, identity cache or per-access registry is added.

The reader borrows encoded Lisp bytes for ordinary string sources, preserving
GNU character widths, BYTE8, genuine private-use characters and consumed character
positions. Native `read_static_object` follows GNU `comp.c:load_static_obj`: create
a real Lisp string with `make_string` semantics, then use the shared reader.
Valid canonical multibyte input stays multibyte; invalid/BYTE8/overlong C input is
wholly unibyte, following `alloc.c:make_string` and `character.c:parse_str_as_multibyte`.
The lossy UTF-8 conversion and fake length-only allocation notification are removed.

`read-from-string` reuses the unchanged `validate_subarray`, following GNU
`lread.c:read_internal_start` and `fns.c:validate_subarray`: check both index types
before the joint bounds and retain original arguments in range errors.

This remains a partial migration. Symbol tokens, positioned/buffer readers,
legacy formatting, echo capture and unreadable-object names retain host-text
adapters. Buffer insertion requires a temporary Unicode view plus exact extended
positions because Rust `str` cannot represent every Emacs code. Callback mutation
timing remains unreviewed. Whole-runtime timings do not isolate those adapters'
cost or certify public ownership soundness.

## Preserved failures

The [source280/281 archive](handover/2026-09-30-shared-reader-draft/source281-canonical-printer-unfinished-manifest.json)
retains source280's private-decoder compile error and source281's actual native
constant corruption. Source281's partial suite was withdrawn after sandbox socket
denials; identical permitted replays pass, but its original run remains incomplete
and release never starts. An earlier generated runner incorrectly used `--eval`
on a multi-form file and produced empty output from both editors; that is invalid
coverage. The corrected `-l` execution requires every output row and byte.

Source282 eventually passes **217 focused controls / two existing ignores and
1,073 affected controls in each profile**, plus 41 ordinary comparisons. Its first
focused run has 207 passes / ten missing-GNU-source-path failures; its affected run
is interrupted. A corrected-path helper then has log-filename errors. Its completed
1,073-test gate run is audited and reused; a separate runner finishes both focused
profiles and affected release. All failed/incomplete runs remain. These setup
mistakes cost time and must not be described as runtime regressions or passes.

Source282 is **never applied**: a later matrix confirms normalized bounds replacing
original negative error arguments. Source283 repairs that runtime defect, but its
new test fails at the GNU reference assertion: literal Greek argv in the expected
output generator was decoded differently under the `C` locale. Emaxx is not reached.
Source284 changes only that fixture to numeric character construction. Fresh GNU
output exactly matches the original test helper's GNU output; production bytes
are unchanged from283 and all 679 cases remain. Source282's larger test counts
are predecessor evidence, not final-source certification.

## Performance baseline and profiles

The [closed pilot and profiles](handover/2026-09-30-shared-reader-draft/source284-core-performance-pilot-manifest.json)
use the unchanged locked 16 workloads, sizes, modes, 600-second process limit and
3% tolerance. Native preparation uses identical upstream Lisp and each editor's
own compiled artifact. All **32 processes complete**; each actual result equals
GNU's answer and the locked expected value, and execution modes match. The adopted
GNU pin and all measured input hashes remain unchanged. No local build, validation
or profiling job runs concurrently with timing.

These are **one paired sample per workload**, not calibrated parity evidence.
Every body exceeds 100 ms. The geometric mean Emaxx/GNU body-time ratio is **3.05×**;
all 16 ratios are unfavorable. Emaxx allocation counters explicitly report
unavailable. Physical accounting and the full performance criterion remain open.

| Workload | GNU seconds | Emaxx seconds | Emaxx / GNU |
| --- | ---: | ---: | ---: |
| Interpreted lexical | 0.209 | 0.522 | 2.49× |
| Interpreted dynamic | 0.189 | 0.405 | 2.14× |
| Interpreted calls | 0.284 | 0.779 | 2.74× |
| Bytecode calls | 0.136 | 0.919 | 6.78× |
| Native execution | 0.616 | 1.969 | 3.20× |
| Interpreted to native | 0.221 | 0.514 | 2.33× |
| Bytecode to native | 0.163 | 1.564 | 9.58× |
| Native to interpreted | 0.213 | 0.606 | 2.85× |
| Native to bytecode | 0.204 | 0.605 | 2.97× |
| Cons allocation | 0.198 | 0.368 | 1.86× |
| List traversal | 0.293 | 0.621 | 2.12× |
| Mapcar | 0.142 | 0.326 | 2.30× |
| Explicit GC | 0.168 | 0.184 | 1.09× |
| Upstream sort | 0.335 | 1.742 | 5.21× |
| Upstream undo | 0.123 | 0.665 | 5.41× |
| Upstream bindat | 0.172 | 0.591 | 3.43× |

Separate two-second macOS samples profile bytecode calls and bytecode-to-native
calls, repeating exact locked-size bodies 20 times; all results/modes pass.
The first GNU native profile has no stacks despite sampler exit zero and is
retained as unusable. An 80-repeat GNU-only successor captures actual stacks and
passes every result check. Instrumented durations are excluded from timing.
Background GNU `__select` samples must not be mistaken for Lisp CPU work.

Emaxx samples concentrate in `run_fast` and `run_frames`, with closure decoding,
backtrace capture, function lookup/hashing, native invocation and handler bookkeeping
also visible. GNU samples concentrate in `exec_byte_code` and `funcall_subr`.
These are candidate costs, not permission to remove required error, rooting,
binding or unwind behavior. Continue from GNU `bytecode.c:Bcall`, `setup_frame`
and `eval.c:funcall_subr`, then validate ordinary-path improvements. No performance
optimization is included in this checkpoint.

## Full-platform state and GNU timeout retry

The [exact-source launches](handover/2026-09-30-shared-reader-draft/source284-full-validation-launch.json)
select `9c5b5356`. Linux [Rust37091391964](https://github.com/rayfdj/emaxx/actions/runs/37091391964)
has [closed failure evidence](handover/2026-09-30-shared-reader-draft/source284-complete-linux-rust-failure-manifest.json):
**2,284 passes / one GNU reference-assertion failure**. The first empty-vector
census is -9 instead of zero, byte-identical to source272's GNU failure; every later
row matches. The helper stops before Emaxx in that control. All four added
printer/reader controls pass. The remaining 756 library names (including two
existing ignores) and both Cargo stages never run. The actual pinned GNU inputs
are retained and verified unchanged; the cause remains unresolved and the original
expectation stays. [Linux frozen37091394500](https://github.com/rayfdj/emaxx/actions/runs/37091394500)
is complete. macOS supervisor **48303** has exited after full Rust, frozen and
an incomplete terminal run in
`target/runtime-goal/recovered-2026-10-03/reader-characters-full/emaxx`.
It starts after timing/profiling finish. Read actual `source284-full-*` and
`source284-linux-*` receipts before reporting completion; launches are not passes.

Source279's [complete Rust audits](handover/2026-09-30-shared-reader-draft/source279-complete-rust-platforms-manifest.json)
remain **3,126 macOS / 3,138 Linux passes**, two existing ignores each, including
native artifact identity. Its [Linux frozen audit](handover/2026-09-30-shared-reader-draft/source279-complete-linux-frozen-manifest.json)
has 519 files / 7,928 matches / 1,038 successful processes, including Eglot.
These certify that predecessor only and do not explain earlier census failures.

The [closed macOS run and bounded retry](handover/2026-09-30-shared-reader-draft/source279-macos-frozen-terminal-and-retry-manifest.json)
records supervisor77583's exit. Frozen has **7,915 matches / six strict feature
diagnostic mismatches**, 519 files and 1,038 successful processes. Terminal has
164 fully matching scenarios / 318 exact comparisons, then GNU startup times out
at `revert-buffer-decline`. There are 368 unexecuted comparisons and 61 unstarted
scenarios; the original full run remains incomplete. A separate unchanged-scenario
retry uses 180-second readiness for both editors and a 300-second overall bound.
It finishes in **14.224 seconds with all three screens equal**. This obtains the
actual GNU answer without slowing Emaxx or manufacturing a timeout.

Source276's original Emaxx census failure remains unexplained. Real intervals,
unified pure storage, symbol authority, physical accounting/counters, public
ownership, remaining adapters, VM/call optimization, final adversarial review,
full final-source validation and calibrated repeated performance remain required.
