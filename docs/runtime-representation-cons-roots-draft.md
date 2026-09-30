# Direct cons fields and conservative roots — 1 October 2026

Read the [complete goal](runtime-representation-goal.md), the
[reader handover](runtime-representation-shared-reader-draft.md) and the
[allocator continuation](runtime-representation-vector-allocation-draft.md).
**Source204 is the current task-branch candidate; complete Linux Rust fails two census assertions.**
All 410 inputs match the separately frozen `cons-roots/emaxx` checkout. Main remains
source174 and PR #79 remains draft. The preceding source201 passes complete macOS
and Linux Rust gates (3,056 / 3,068 tests, two existing ignores each), all 226
terminal scenarios / 686 comparisons, and the
[complete Linux frozen comparison](handover/2026-09-30-shared-reader-draft/source201-linux-frozen-manifest.json)
(519 files / 7,928 matching outcomes / 1,038 successful processes). These passes
do not establish a repair for source198's layout-sensitive defect below.

The [environment-controlled retained-artifact diagnosis](handover/2026-09-30-shared-reader-draft/source198-exact-environment-root-diagnosis-manifest.json)
uses the exact failed source198 executable and startup image. The ordinary
wrapper and direct child fail with `((1 t 0) (1 t 1))`; adding only the debugger
observer's environment variable makes the direct child return the required
`((1 t 0) (1 t 0))`. Keeping that variable out of the inferior reproduces the
original failure in all five debugger processes. All 398 compiled inputs and
both artifacts remain unchanged. No Python callback exceptions occur.

In every failing trace, the second key is marked through the cdr of a cons
whose base-plus-one address appears in exactly one captured native stack word.
At the final collection, that word is at offset 32 in `eval_call`. The retained
binary's disassembly shows `cons_values` calling `cons_cells`, constructing two
16-byte `ConsSlot` wrappers and writing the cdr selector with a one-byte store
at that offset. The upper bytes are not initialized by that store.

GNU `alloc.c:live_cons_holding` accepts only offsets 0 (raw car/object address),
3 (tagged cons) and 8 (raw cdr address). Emaxx clears all three tag bits before
`mem_find`, which accepts any address within an allocated cons. Consequently,
the malformed word can retain an unrelated live cell and its children. The
environment variable changes allocation placement; it is not a repair.

The [source202 negative baseline](handover/2026-09-30-shared-reader-draft/source202-cons-field-root-baseline-manifest.json)
keeps runtime201 unchanged and adds two architectural controls. Its fresh gate
build succeeds; both controls fail: a retained field occupies 16 rather than
8 bytes, and byte offset 1 wrongly retains a cons. The full 410-input patch
replays against main `21d20f0e`, including executable modes. The failed results
and original binary are preserved separately from the repair.

The candidate changes ordinary cons field handles to their actual
`NonNull<Cell<Value>>` address, one word. Reads and stores use that field
directly, and `cons_values` reads the two payload words without constructing
field handles. Cell identity follows the allocator's fixed two-word slot
geometry; compile-time checks retain its block start and car/cdr offsets.
Pointer subtraction stays within the same allocation. `Cell` preserves shared
mutation under the existing serialized runtime boundary, and the handle remains
non-Send. Weak field handles still use allocation serials to reject reused cells.
They do not add work to ordinary field reads and stores.

The collector validates a cons's original, unmasked offset before marking it,
following GNU's three permitted offsets. Other object kinds retain their
existing validation and still require review. The new controls cover all 16
byte offsets across varied contents, field identity and mutation, survival
while either car/cdr field remains held, and reclamation after release. Both
original suspended-thread and bytecode reclamation assertions remain unchanged.

The [source203 compiler failure](handover/2026-09-30-shared-reader-draft/source203-cons-field-root-compiler-failure-manifest.json)
is preserved: the new field type omitted its `std::ptr::NonNull` import, so no
runtime tests executed. Source204 corrects that import. Its
[portable focused results](handover/2026-09-30-shared-reader-draft/source204-cons-field-root-focused-manifest.json)
retain the complete 410-input patch and a smaller follow-up patch from
runtime201. Exact replay and executable modes are verified. Compiler, formatting,
Clippy and diff checks pass with zero warnings; all **seven focused gate
controls** pass, including both original reclamation assertions. The
[complete selected audit](handover/2026-09-30-shared-reader-draft/source204-cons-selected-validation-manifest.json)
verifies **596 gate / 596 release passes**, seven focused controls in each
profile, **39 ordinary batch comparisons and one ordinary terminal fixture**.
The raw inventories, executable/image identities and all 410 source inputs are
checked; five deliberately corrupt raw logs are rejected.

The original 40-fixture batch attempt remains failed. Both editors returned
identical purecopy results with zero message callbacks: GNU's batch message path
does not run the interactive callback required by this fixture. The unchanged
fixture and original expected counts (1, 1, 1, 2, 2) pass in both editors through
ordinary `-nw -Q` commands, including collection in every callback. This separate
replay does not turn the original batch attempt into a pass. Two incomplete audit
invocations are preserved: one omitted the expected fixture-file newline rule,
and one omitted the existing receipt-directory fixture path. Neither changed
runtime code, assertions or expected output.

Static ARM64 release disassembly shows `cons_values` using a direct payload load
without its former selector spills and stack frame (112 to 72 function-code
bytes). This is static evidence of removed wrapper work, not a measured speedup
or instructions-per-operation result. The prior and new binaries are retained.

The [complete validation launch](handover/2026-09-30-shared-reader-draft/source204-complete-validation-start-manifest.json)
records macOS Rust supervisor **43352** and terminal supervisor **43354** in
`target/runtime-goal/recovered-2026-09-30/cons-roots/emaxx`. The earlier selected
supervisor **41205** has exited. Preserve the frozen checkout, helpers and
artifacts while any associated child remains live. The [publication receipts](handover/2026-09-30-shared-reader-draft/source204-publication-manifest.json)
verify `d186d40bf014ca53d0a2652b4e2f2d921c5cb009` and draft PR #79.
[Complete macOS Rust](handover/2026-09-30-shared-reader-draft/source204-macos-complete-rust-manifest.json)
now passes 2,960 library, 60 binary and 39 integration tests: **3,059 passes**, two
existing ignores and native artifact identity. All 2,962 library names/verdicts,
410 source hashes and four retained executable/image files are audited.
Supervisor 43352 has exited. The
[complete terminal audit](handover/2026-09-30-shared-reader-draft/source204-complete-terminal-manifest.json)
also passes all 226 scenarios / 686 comparisons (658 screen, 28 filesystem),
with unchanged inventories, all 410 source hashes and eight execution inputs
verified. Supervisor 43354 has exited. GNU matches source/native ABI but remains
different from the frozen Darwin executable.

[Linux Rust](https://github.com/rayfdj/emaxx/actions/runs/36764911748) has finished
with **2,232 passes / two failures**. Its
[raw audit](handover/2026-09-30-shared-reader-draft/source204-linux-rust-failure-manifest.json)
preserves both vector/closure and pseudovector census failures. The first vector
delta is -47 instead of 0; the first record delta is -45 instead of 2. Every
later size/count matches. Both original reclamation assertions pass. Four later
groups (736 scheduled tests, including two existing ignores) and both Cargo
stages did not run; the new cons controls are in that unexecuted lightweight
group. The failed executable is `6ea03c1a…`, its retained image `85ebb975…`.
Their exact identities, inventory and raw verdicts are verified. The original
failed result stays failed.

`tools/diagnose_exact_census.py` uses the existing Linux workflow's
`rust-exact-census` mode to restore these artifacts and their source paths. It
replays both selected controls and, in the first diagnosis, the original
primitives group, then observes collections using the inspected executable's ABI.
No assertion or heap value is changed. Six corrupt retained-input cases are
rejected locally. The [published diagnostic launch](handover/2026-09-30-shared-reader-draft/source204-census-diagnosis-start-manifest.json)
records [run 36770142335](https://github.com/rayfdj/emaxx/actions/runs/36770142335)
on tooling/evidence commit `dca0453e`, with all runtime inputs still source204.
That job has finished. Its [audited evidence](handover/2026-09-30-shared-reader-draft/source204-exact-census-incomplete-manifest.json)
reproduces both failures in the selected pair and the full 626-test primitives
group (624 passes / two failures), with 403 compiled inputs and the exact
executable/image verified unchanged. The debugger's first control reaches four
collections. Its second sweep reclaims a 41-element vector and a host record
with four detached fields; its third and fourth reclaim no counted vectors.
The second control's initial sweep exceeds the observer's 4,096-object bound,
ending the debugger without a complete test result. This failure and all raw
data remain preserved. The retaining root is still unknown.

The revised observer retains first-collection roots but observes cleanup only
in collections 2..4, where the census difference occurs. It also records the
independent host conservative stack at the exact inspected `mark_stack` scan
boundary. The first version captured only native-heap roots. The new driver
requires both controls' first four host-stack/collection reports and rejects
missing traces. The already reproduced full group need not be rerun when the
same selected controls reproduce; `--replay-group` retains that option.
A retained-image replay
cannot reconstruct unrecorded inherited CI environment exactly, and a debugger
pass would not clear the original failure. The 47-slot cause is still unknown.

[Complete Linux frozen compatibility](handover/2026-09-30-shared-reader-draft/source204-linux-frozen-manifest.json)
now passes all 519 files / 7,928 matching outcomes / 1,038 successful processes
on source204. Raw inventories, result names, process exits and pinned GNU
identity are audited. Each editor reports 7,670 passes, 47 expected failures
and 211 skips; these categories remain separate. Receipts and reproduction helpers are in
`target/runtime-goal/resume-2026-09-28`. Complete candidate validation remains
required before promotion.

No timing, RSS or GNU performance-parity claim follows from the smaller field
handle. Full candidate validation, Linux confirmation of the repair, remaining
symbol/object authority, honest physical allocation and GC accounting, final
adversarial review and the locked performance criteria remain open.

The [accounting continuation](runtime-representation-accounting-draft.md) records
the subsequent same-input counter failure and the remaining representation costs.
No counter repair is included in this checkpoint.
