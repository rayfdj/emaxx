# Word-aligned vector allocation draft — 1 October 2026

**Source200 is isolated unfinished work.** Main remains source174. The task
branch still contains source198's runtime in [draft PR #79](https://github.com/rayfdj/emaxx/pull/79).
Read the [complete goal](runtime-representation-goal.md) and the
[shared-reader handover](runtime-representation-shared-reader-draft.md) first.
Source198 passes the complete macOS Rust gate but fails the original Linux
suspended-bytecode reclamation assertion. The allocator draft does not clear it.

The [portable source200 draft and focused receipts](handover/2026-09-30-shared-reader-draft/source200-vector-allocation-focused-manifest.json)
contain the complete patch from main `21d20f0eec08f3c013d5d3eb3bd6e9cfc16b3199`,
all 410 build/test input hashes, exact patch/mode replay, strict checks and seven
focused gate passes. The isolated checkout is
`target/runtime-goal/recovered-2026-09-30/macro-reader/emaxx`. Supervisor 29761
is running its broader gate/release and 36 ordinary comparisons. Keep that
checkout and its artifacts frozen while the supervisor or its children run.

GNU `alloc.c:roundup_size` uses the common alignment of Lisp words and allocated
object fields. Emaxx's three pointer tag bits, inline words and Rust payloads
require eight-byte alignment, already enforced for generic pseudovector fields.
The allocator nevertheless rounded every footprint to 16 bytes. Source200 uses
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
Emaxx. Source200 removes the physical padding that caused those differences;
it does not replace the counters with invented smaller figures. The already
matching ordinary vector/closure census stays unchanged.

| Allocator quantity | Baseline | Source200 |
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

Complete source200 validation, the Linux reclamation diagnosis, broader object
authority and allocation accounting, final adversarial audit and the locked
performance criterion remain open. A focused allocator pass does not complete
the full runtime goal.
