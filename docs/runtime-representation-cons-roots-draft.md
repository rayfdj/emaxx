# Direct cons fields and conservative roots — 1 October 2026

Read the [complete goal](runtime-representation-goal.md), the
[reader handover](runtime-representation-shared-reader-draft.md) and the
[allocator continuation](runtime-representation-vector-allocation-draft.md).
**Source204 is a separate unfinished candidate.** The task branch still contains
source201, and main remains source174. Source201 now passes complete macOS and
Linux Rust gates (3,056 / 3,068 tests, two existing ignores each). Its terminal
and Linux frozen comparisons remain running. Those passes do not resolve the
source198 reclamation failure described below.

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
controls** pass, including both original reclamation assertions. Broader gate,
release and 40 ordinary GNU comparisons run under supervisor **41205** in
`target/runtime-goal/recovered-2026-09-30/cons-roots/emaxx`. Receipts are in
`target/runtime-goal/resume-2026-09-28`, using `queue-source204-validation.py`
and the `*-cons204.py` helpers. Preserve the checkout and artifacts while any
associated process remains live. These focused passes do not replace complete
candidate validation or Linux confirmation of the repair.

No timing, RSS or GNU performance-parity claim follows from the smaller field
handle. Full candidate validation, Linux confirmation of the repair, remaining
symbol/object authority, honest physical allocation and GC accounting, final
adversarial review and the locked performance criteria remain open.
