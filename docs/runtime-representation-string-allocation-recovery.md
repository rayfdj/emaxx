# String allocation recovery — 2 October 2026

The [full goal](runtime-representation-goal.md) remains active and incomplete.
The applied runtime is **source272** (`eb286e23`), main is `21d20f0e`, and PR79
remains draft. **Source279 is isolated and unapplied**, in
`target/runtime-goal/recovered-2026-09-30/string-allocation-recovery/emaxx`.
Its [portable draft and bounded evidence](handover/2026-09-30-shared-reader-draft/source279-string-allocation-recovery-draft-manifest.json)
retain failed predecessors and all 511 input identities. Receipts/helpers are in
`target/runtime-goal/resume-2026-09-28`; preserve frozen inputs and executed helpers.

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

Source279 passes **211 focused gate tests / two existing ignores** and all **1,067
affected gate controls**, with [every raw verdict audited](handover/2026-09-30-shared-reader-draft/source279-string-allocation-gate-broad-manifest.json).
That later pass does not diagnose the earlier interaction.
Supervisor **72187** continues 1,067 affected release controls, 213 focused release
selectors, and **37 ordinary GNU comparisons**: all 33 preceding fixtures, both
allocation probes and both census fixtures. Read `source279-selected-*` for live
state. Failed stages remain failed while independent profiles/diagnostics continue.
A launch or fresh-process pass does not certify broader validation.

## Other closed evidence and user steering

Source270 terminal validation now closes **226 scenarios / 686 exact comparisons**,
with no divergence or missing comparison. All 510 source inputs, ten execution
inputs and original labels are checked against the exact frozen-run executable/image.
This does not certify source279.

The user explicitly welcomes faster Emaxx execution when the work and answers are
correct. A GNU timeout must not be copied into Emaxx behavior. Source272 Linux
frozen still has 7,927 matching outcomes and one GNU Eglot completion timeout;
that case establishes neither identical answers nor a speedup.

The bounded Eglot review verifies retained executable/command provenance, the
selected unchanged upstream test, the real pinned rust-analyzer identity and the
Emaxx log reaching completion. The test asserts exact final buffer contents after
choosing a completion. Rust source contains no special case for its name or its
input/result literals. No canned result or oracle forwarding was found in the
reviewed path. This is not a universal no-cheating proof: no new adversarial Eglot
execution or syscall trace was performed; the whole-runtime audit remains open.

Remaining requirements include final sblock validation, real property intervals,
unified pure storage, physical accounting/counters, public ownership, symbol
authority, VM/call profiling, full Linux/macOS/integration validation, final audit
and the locked 16-workload performance criterion with its unchanged 3% ceiling.
No timing or full-goal completion claim is made.
