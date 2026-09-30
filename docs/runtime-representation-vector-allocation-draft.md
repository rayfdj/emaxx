# Word-aligned vector allocation draft — 1 October 2026

**Source201 is the preceding allocator checkpoint.** The [cons repair](runtime-representation-cons-roots-draft.md) now occupies the task branch; main remains source174.
The allocator follows source198's reader/macro runtime in
[draft PR #79](https://github.com/rayfdj/emaxx/pull/79).
Read the [complete goal](runtime-representation-goal.md) and the
[shared-reader handover](runtime-representation-shared-reader-draft.md) first.
Source198 passes the complete macOS Rust gate but fails the original Linux
suspended-bytecode reclamation assertion. The allocator draft does not clear it.

The [portable source201 selected results](handover/2026-09-30-shared-reader-draft/source201-vector-selected-validation-manifest.json)
contain the complete patch from main `21d20f0eec08f3c013d5d3eb3bd6e9cfc16b3199`,
all 410 build/test input hashes, exact patch/mode replay, strict checks,
**454 gate / 454 release controls and 36 ordinary exact GNU comparisons**.
Eight focused controls pass in both profiles. Raw names, verdicts, process
results, source inputs and executable/image hashes are audited. Five corrupt-log
controls are rejected. Selected supervisor 31780 has exited; all 410 inputs are
now applied to the task worktree and
[published as **`320d0a2e379540db60201c0a486f37d11f77b8a1`**](handover/2026-09-30-shared-reader-draft/source201-publication-manifest.json).

The frozen validation checkout is
`target/runtime-goal/recovered-2026-09-30/macro-reader/emaxx`. Complete macOS Rust
supervisor **33431** and complete terminal supervisor **33432** have started
after those selected passes. Their [portable launch receipts and diagnostic controls](handover/2026-09-30-shared-reader-draft/source201-complete-validation-start-manifest.json)
retain exact helper hashes and the applied-source audit. The prior source198
terminal supervisor has exited.
Keep the checkout, helpers and artifacts frozen while these processes or their
children run. [Complete Linux Rust](https://github.com/rayfdj/emaxx/actions/runs/36756877353)
and [complete Linux frozen compatibility](https://github.com/rayfdj/emaxx/actions/runs/36756885097)
are dispatched on that exact published head. The
[complete macOS Rust audit](handover/2026-09-30-shared-reader-draft/source201-macos-complete-rust-manifest.json)
now verifies **2,957 library passes, 60 binary passes and 39 integration passes**:
3,056 passes, including native artifact identity, with two existing ignores.
Every one of the 2,959 library names/verdicts is checked against its inventory;
all 410 inputs match the isolated checkout, task worktree and published source.
The tested executable and four post-run images are retained with hashes. Those
images do not establish which test used each file. Supervisor 33431 has exited.
The [complete Linux Rust audit](handover/2026-09-30-shared-reader-draft/source201-linux-complete-rust-manifest.json)
also passes: 2,965 library, 61 binary and 42 integration tests, totaling **3,068**
passes, with the same two existing ignores. All 2,967 raw library names/verdicts,
native artifact identity and retained hashes are verified on clean `320d0a2e`.
Both original survival/reclamation assertions pass. The
[complete terminal audit](handover/2026-09-30-shared-reader-draft/source201-complete-terminal-manifest.json)
now verifies all **226 scenarios / 686 comparisons**: 658 screen and 28 filesystem
checks. Supervisor 33432 has exited; all 410 source inputs and eight execution
inputs remain unchanged. GNU matches source/native ABI but is not the frozen
Darwin executable. The [complete Linux frozen audit](handover/2026-09-30-shared-reader-draft/source201-linux-frozen-manifest.json)
now matches 519 files / 7,928 outcomes with all 1,038 processes successful on
published `320d0a2e`. It preserves 7,670 passes, 47 expected failures and 211 skips
per editor. This completes these source201 gates; no GNU performance parity or
complete source204 validation is established.

The [source200 broad failure](handover/2026-09-30-shared-reader-draft/source200-vector-selected-failure-manifest.json)
is preserved: **453 gate passes / one old record-footprint expectation failure**;
release and ordinary stages did not run. Same-input GNU evidence independently
checks all six original record sizes. Source201 preserves the allocator runtime
and all six original assertions, correcting the expected word counts for data
lengths 1, 9 and 257 from 4, 12 and 260 to GNU's 3, 11 and 259. Its ordinary
pseudovector fixture now also includes those boundary sizes and 4094.

GNU `alloc.c:roundup_size` uses the common alignment of Lisp words and allocated
object fields. Emaxx's three pointer tag bits, inline words and Rust payloads
require eight-byte alignment, already enforced for generic pseudovector fields.
The allocator nevertheless rounded every footprint to 16 bytes. Source201 uses
word alignment and adds static checks for its other typed metadata and hash
table layout. Header and payload offsets stay the same.

The collector retains its 40-byte block bitmap. Every nonempty allocated object
occupies at least 16 bytes, so two object starts cannot occupy the same 16-byte
bitmap interval even when their addresses have eight-byte alignment. The new
control checks distinct mark bits across mixed object sizes, odd word offsets,
all bitmap words, small/large boundaries and three collections. The existing
vector/closure cycle survival and eventual reclamation assertions also pass.

The [baseline receipts](handover/2026-09-30-shared-reader-draft/source199-vector-layout-baseline-manifest.json)
preserve two failing new architectural assertions on the unchanged source198
runtime and an ordinary GNU/Emaxx census difference. Both editors execute the
same program. Records with odd data-slot counts, char-tables with even extra-slot
counts, positioned symbols and hash tables report one excess vector slot in
Emaxx. Source201 removes the physical padding that caused those differences;
it does not replace the counters with invented smaller figures. The already
matching ordinary vector/closure census stays unchanged.

| Allocator quantity | Baseline | Source201 |
| --- | ---: | ---: |
| Two-slot vector footprint | 32 bytes | 24 bytes |
| Six-slot vector/closure footprint | 64 bytes | 56 bytes |
| Six-slot closure GC allocation charge | 64 bytes | 56 bytes |
| Per-block bitmap | 40 bytes | 40 bytes |
| Usable bytes in a 4,096-byte block | 4,048 | 4,056 |
| Separate large-vector mark reservation | 16 bytes | 8 bytes |
| Process-wide free-list head table | 1,024 bytes | 2,032 bytes |

These are verified allocator-requested footprints and charges. They do not
measure host allocator usable size, retained RSS, instruction counts or elapsed
performance. The larger free-list table and changed size-class search costs
remain part of the tradeoff. The earlier review's proposed 72-byte bitmap was
unnecessary and was not implemented.

The [source199 strict failure](handover/2026-09-30-shared-reader-draft/source199-vector-strict-failure-manifest.json)
is preserved: compiler/fmt/diff checks pass, but Clippy rejects a manual
`div_ceil` expression in the updated buffer-footprint test. No candidate runtime
tests execute in that run. Source200 uses `div_ceil` with the same exact expected
footprint, without a lint suppression. The baseline helper's initial relative
executable-path failure is also retained; its separate absolute-path comparison
is the actual GNU/Emaxx census evidence.

The [exact retained Linux replay](handover/2026-09-30-shared-reader-draft/source198-exact-retained-failure-manifest.json)
reproduces source198's original reclamation failure with its exact executable
and retained startup image. A separate debugger process passes but reports 498
address-read overflow errors; it does not establish the retaining root. The
diagnostic helper now records rejected name reads and preserves up to five
separate debugger processes, their outcomes, traces and image hashes. Its local
callback and malformed-artifact controls pass. The
[five-process Linux diagnosis](https://github.com/rayfdj/emaxx/actions/runs/36756891923)
uses source198's original retained artifacts. Its
[audited raw result](handover/2026-09-30-shared-reader-draft/source198-five-process-exact-diagnosis-manifest.json)
reproduces the ordinary failure, while all five debugger processes pass with
two watched keys and four collection-source snapshots, without callback
exceptions. The subsequent [environment-controlled run](https://github.com/rayfdj/emaxx/actions/runs/36758535929)
compares the exact direct child with and without the observer's environment
variable. It keeps that variable out of the inferior, preserves inherited display
dimensions and disables GDB's extra startup shell, using GDB's documented
[environment controls](https://www.sourceware.org/gdb/current/onlinedocs/gdb.html/Environment.html)
and [startup option](https://www.sourceware.org/gdb/current/onlinedocs/gdb.html/Starting.html).
Its [audited evidence](handover/2026-09-30-shared-reader-draft/source198-exact-environment-root-diagnosis-manifest.json)
reproduces the original failure in the ordinary wrapper, direct child and all
five debugger children. Adding only the observer variable makes the direct
child pass. The original executable, startup image and all 398 compiled inputs
remain unchanged; no debugger callback errors occur. Each failing trace reaches
the second weak key through a cons selected by a malformed stack word at offset
32 in `eval_call`. The word contains a one-byte cdr `ConsSlot` selector beside
old pointer bytes. GNU rejects that cons offset; Emaxx accepts it. The
[cons field and root continuation](runtime-representation-cons-roots-draft.md)
preserves the complete diagnosis, negative controls and separate repair draft.
Source201's passes do not establish that this layout-sensitive defect is fixed.

Complete source204 validation and Linux confirmation of its reclamation repair,
broader object authority and allocation accounting, final adversarial audit and
the locked performance criterion remain open. A focused allocator pass does not complete
the full runtime goal.
