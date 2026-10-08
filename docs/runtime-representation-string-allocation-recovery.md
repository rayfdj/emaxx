# String allocation recovery — 2 October 2026

The [full goal](runtime-representation-goal.md) remains active and incomplete.
This historical allocation checkpoint is **source279** (`d64040a7`); the current
[printer/reader checkpoint](runtime-representation-printer-draft.md) is source284
(`9c5b5356`). Main remains `21d20f0e`, and PR79 remains draft. Its original selected checkout remains frozen in
`target/runtime-goal/recovered-2026-09-30/string-allocation-recovery/emaxx`.
Its [portable draft and bounded evidence](handover/2026-09-30-shared-reader-draft/source279-string-allocation-recovery-draft-manifest.json)
retain failed predecessors and all 511 input identities. Receipts/helpers are in
`target/runtime-goal/resume-2026-09-28`; preserve frozen inputs and executed helpers.
The [closed selected validation and full-validation launches](handover/2026-09-30-shared-reader-draft/source279-string-allocation-selected-manifest.json)
record both profiles and 37 ordinary comparisons. The separate
[generated-output failure and bounded Eglot retry](handover/2026-09-30-shared-reader-draft/source279-varied-output-and-eglot-retry-manifest.json)
must be read with that passing selected result: **53 of 198 generated output
rows still differ from GNU**. This is an incremental allocator checkpoint,
not a complete correctness certificate.

The latest [printer/reader continuation](runtime-representation-printer-draft.md)
repairs all 198 generated rows and the native reload failure in applied source284.
It retains the original source279/281 failures, later reader/bounds repairs,
selected evidence and first diagnostic performance pilot. These successor passes
do not erase this allocation checkpoint's original failed observations.

## Recoverable allocation errors

Source276's new sdata allocator calls `handle_alloc_error` on allocation failure,
where the preceding runtime could return a Lisp error. Source278 adds three tests
without changing production code; **all three abort with SIGABRT**. Source277's
first test-only draft fails compilation on an `Option<i64>`/`Option<u32>` assertion;
source278 only adds the required lossless integer conversion.

Pinned ordinary GNU probes also expose two older differences: string bytes must
fit a 61-bit fixnum, and allocation failure must signal the existing dynamically
bound `memory-signal-data` object. Source270 instead constructs a generic message,
losing condition identity and message properties, and accepts an oversized
two-byte-character request past GNU's byte bound.

Source279 follows `lisp.h:STRING_BYTES_BOUND`, `alloc.c:STRING_BYTES_MAX`,
`allocate_string_data` and `memory_full`. Typed allocation failure reaches the
primitive body, including generated direct/native dispatch, and resolves the
existing Lisp condition there. A construction guard returns uninstalled headers
to the existing free list on error or unwind. Successful access gains no registry,
lookup or shadow payload.

The three unchanged abort controls now pass, covering the bound, native error
identity, unchanged string/block census after failure and subsequent allocation.
Both existing vector/pseudovector census controls also pass in fresh processes.
Formatting, all-target/all-feature checking, Clippy and diff checks pass with zero
warnings. All **511 bytes-and-mode input identities** replay from `b5a1558c` and main.
The first source279 validation-preparation helper miscounts changed files after
cloning its cache; its successor corrects 16 to 17 before any candidate validation.

These results cover oversized recoverable requests. General process-wide
exhaustion, reserve-memory handling, GC retuning and pre-existing infallible host
allocation APIs remain incomplete. The first relocated source270 ordinary probe
fails before Lisp because its image has relative native-library paths; the
corrected probe uses the identical original executable/image in place. Both
receipts remain, with no runtime verdict assigned to the startup failure.

## Broader failure and current validation

Source276 completes **208 focused passes / two existing ignores** and **1,063
affected passes / one failure**, with no missing or ignored affected test.
The failure is **in Emaxx, after GNU passes**: the first empty-vector census delta
is `-130` instead of zero. Every later vector/closure row matches. Release never
starts. Exact same-binary replays pass alone and with the two immediate predecessors;
that suggests an interaction but neither identifies its cause nor erases the failure.
The original fixture, expectations, timeouts and comparison rules remain unchanged.

Source279 passes **211 focused tests / two existing ignores** and all **1,067
affected controls in each profile**, with every raw verdict audited. Its
**37 ordinary comparisons** match GNU byte for byte in stdout, stderr and exit
status: all 33 preceding fixtures, both allocation probes and both census fixtures.
The preceding fixtures and expected-output digests are unchanged.
That later pass does not diagnose the earlier interaction.
Supervisor **72187 has exited**, and all eight selected stages and the final raw
audit pass. Source279 is applied and pushed as `d64040a7` with all 511 inputs/modes
unchanged. Full macOS supervisor **77583** has exited from the clean exact-commit checkout
`target/runtime-goal/recovered-2026-09-30/string-allocation-full/emaxx`.
Linux [Rust36950663403](https://github.com/rayfdj/emaxx/actions/runs/36950663403) and
[frozen36950667238](https://github.com/rayfdj/emaxx/actions/runs/36950667238)
are launched on that exact commit. Read `source279-full-*` and `source279-linux-*`
for their actual state. Their [closed Rust audit](handover/2026-09-30-shared-reader-draft/source279-complete-rust-platforms-manifest.json)
now verifies **3,126 macOS / 3,138 Linux passes**, two existing ignores each,
all library names, native artifact identity and retained actual inputs. The
[closed Linux frozen audit](handover/2026-09-30-shared-reader-draft/source279-complete-linux-frozen-manifest.json)
verifies **519 files / 7,928 matching outcomes / 1,038 successful processes**.
The original Eglot case completes and passes in both editors in this fresh full run.
Prior timeouts/census failures remain separate. The [closed macOS frozen/terminal
run and bounded GNU retry](handover/2026-09-30-shared-reader-draft/source279-macos-frozen-terminal-and-retry-manifest.json)
record 7,915 frozen matches/six strict feature-diagnostic differences and 318 exact
terminal comparisons before GNU readiness times out. The separate unchanged-scenario
retry finishes in 14.224 seconds with all three screens equal. The original run
remains incomplete, with 368 unexecuted comparisons and 61 unstarted scenarios.

## Additional adversarial output failure

A deterministic seed generates 96 varied string length/content/mutation cases
in each of interpreted and verified bytecode execution, plus six allocation-error
rows. It checks full printed strings, copies, aliases, properties and forced GC,
using changed names and contents. The first generator omits a closing parenthesis;
both editors reject it. Its corrected successor executes all **198 rows** but
**53 differ**. Both emit the same bytecompiler warning for an unused `make-string`
result. The original failed helpers, inputs, warnings and raw outputs remain intact.

The retained source270 executable runs the identical input and has **58 wrong
rows**. Its first 192 rows are byte-identical to source279's. Only the final six
allocation rows change: source279 repairs condition identity and bounds, leaving
five fewer whole-row differences. The earlier printing defect therefore predates
the allocator repair; its precise introduction remains unestablished.

The reviewed printer consumes `SharedStringState::text()` and iterates Rust `char`
values. That host view replaces extended Emacs characters with U+F8FF. GNU
`print.c:print_object` reads the actual character codes before escaping or emitting
them. Both escaped output and an unescaped extended-character stdout row fail.
The repair must carry actual character codes/bytes through rendering and output
destinations; adjusting expected output or only the generated fixture would not
repair the runtime. Formatting, string-returning/buffer/function streams, raw-byte
characters and native serialization also need review. No printer repair is applied.

The unused-result warning means the generated loop's extra discarded allocations
are not evidence that bytecode performed those allocations. Preserve this limit;
a successor can assert their live state while retaining all original value checks.
Neither the successful selected suite nor the bounded Eglot retry cancels this
new failure. Main and full-goal completion remain held.

## Other closed evidence and user steering

Source270 terminal validation now closes **226 scenarios / 686 exact comparisons**,
with no divergence or missing comparison. All 510 source inputs, ten execution
inputs and original labels are checked against the exact frozen-run executable/image.
This does not certify source279.

The user explicitly welcomes faster Emaxx execution when the work and answers are
correct. A GNU timeout must not be copied into Emaxx behavior. Source272 Linux
frozen still has 7,927 matching outcomes and one GNU Eglot completion timeout;
that original case establishes neither identical answers nor a speedup.

The bounded Eglot review verifies retained executable/command provenance, the
selected unchanged upstream test, the real pinned rust-analyzer identity and the
Emaxx log reaching completion. The test asserts exact final buffer contents after
choosing a completion. Rust source contains no special case for its name or its
input/result literals. No canned result or oracle forwarding was found in the
reviewed path. The original review performed no new Eglot execution or syscall
trace; the separately recorded retry below now adds a completed execution.

The user subsequently requested a reasonable longer wait to obtain GNU's answer.
`retry-source279-eglot-bounded.py` runs the unchanged upstream case with the same
**60-second JSON-RPC default and 180-second process bound** for both editors.
It uses their own executable/images and the real pinned Rust1.75 toolchain, and
passively captures the actual buffer after completion selection. Both editors
pass all original assertions, with no skip, in about 4.3 seconds of test execution.
Their captured buffer bytes are identical:
`fn test() -> i32 { let v: usize = 1; v.count_ones.1234567890;`.
All actual executable/image, fixture, observation helper and Rust toolchain inputs
are hashed and unchanged. This is a **new macOS diagnostic**, not a reclassification
of the original Linux timeout, a full-suite pass, a speed comparison or a universal
no-cheating proof. Future timeouts should receive bounded diagnostic retries to
obtain an answer when practical; keep the original timeout and retry separate.

Remaining requirements include final sblock validation, real property intervals,
unified pure storage, physical accounting/counters, public ownership, symbol
authority, VM/call profiling, full Linux/macOS/integration validation, final audit
and the locked 16-workload performance criterion with its unchanged 3% ceiling.
No timing or full-goal completion claim is made.
