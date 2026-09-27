# Canonical char-table continuation — 28 September 2026

The [complete runtime goal](runtime-representation-goal.md) remains open. This
revision continues the [27 September handover](runtime-representation-handover.md)
and applies its preserved three-file draft. Do not apply that draft again on this
working branch. Main was fetched and matched
`f79b7c73c85ae53aae3d747d883dfbda1b55a10c` before editing.

## Implementation under validation

Char tables now use the actual GNU vectorlike word and inline radix slots:
`PVEC_CHAR_TABLE` 32, `PVEC_SUB_CHAR_TABLE` 33, an eight-byte header,
68 root fields plus fixed extras, and subtable depth/minimum as two packed
32-bit integers followed by 16/32/128 Lisp slots. ASCII aliases the real leaf.
Allocation and GC census use the rounded allocation size; marking skips the
packed non-Lisp word and traces only actual fields.

Removed: char-table IDs and registry, append-only writes retaining overwritten
values, ASCII and resolved-range mirrors, per-table mutation generations,
separate category documentation, native char-table handles, and dump rebuilding
from a range log. Frame handles remain. Cons cells remain 80 bytes: the final
16-byte cons requirement is not satisfied.

`chartab.c` supplies get/set/default/parent/range, copy, optimize, live map
callback traversal and lazy Unicode decompression rules. Reader, printer,
substitution, image copying and dumping preserve the actual slots and sharing.
`syntax.c` supplies shared descriptor conses, rooted/dumped as its
`Vsyntax_code_object` vector; reads no longer convert descriptor strings.
`category.c` supplies bool-vector entries, the 95-slot documentation vector,
and the equal-key sharing hash table in extra slot 1. Existing record/bool-vector
representation limitations are separate unfinished runtime work.

Derived regexp caches now compare current graph fields, including mutable
leaves, so native stores cannot evade a Rust mutation counter. This scans and
allocates host-side snapshots on table-dependent cache probes. Its performance
cost is unmeasured and potentially significant; this is not GNU-equivalent
regexp matching architecture or a performance-parity result. Category update
boundary walks visit only intersecting radix nodes. Scoped Rust roots across
map/optimize callbacks are needed because Rust locals are not all conservative
GNU stack roots. Their cost also remains to be profiled.

Input auditing remains incomplete. Compressed run writes are bounded by the
actual 128-slot allocation. Reader roots are limited by the pseudovector size
field (4095 fields), while make-char-table retains GNU's property limit of ten
extras. Malformed subtable topology and cycles manufactured through reader/native
fields need additional adversarial review; do not claim general soundness from
selected tests or the serial runner.

## Reproduced evidence and limits

The original regression was reproduced before wiring the draft. GNU returned:

```
(initial initial default parent parent replacement-default range t (slot-initial slot-initial t) (replacement 0))
```

The fresh original Rust baseline failed with:

```
(parent parent nil nil nil nil parent nil (nil nil t) (replacement 1))
```

The weak-value expectation is unchanged. The repaired implementation passes it.
Intermediate failures are retained: missing built-in extra-slot declarations
blocked startup, then raw map callbacks exposed stored syntax strings. No tests
were ignored to bypass these failures.

Local evidence is under `target/runtime-goal/resume-2026-09-28/`:

- `provenance-baseline.json`, baseline build/source/test receipts and preserved
  `baseline-libtest` retain the starting experiment.
- Source 7: 10/10 selected char-table tests passed, no ignored tests; immutable
  binary SHA-256 `198ebf4def91892fea2be12aa28c3292fd331cec874c2a129fc7b25a6f25cd0c`.
- Source 8: 13/13 passed, including native inline stores, graph survival and
  reclamation, callbacks, identity and reader cycles. The pre-build diff capture
  overlapped formatter completion; retain the timing note and treat this run as
  diagnostic, not exact-source final evidence.
- Source 9: 14/14 passed from an exact hashed source manifest and immutable
  binary `a6f888207bb6d8aef0c4b0dc4d71f2b1f2ebfab41a5d69be61afd7263668f17a`.
  Both Unicode compression formats pass the GNU comparison. The run used a fresh
  fingerprinted startup image and checked binary hashes before/after execution.
- Source 10 adds reader coverage for more than ten literal extras, bounds malformed
  compressed input, and validates character arguments before radix access.
  All-target/all-feature check, strict Clippy, rustfmt and diff checks passed.
  The full serial gate is running against binary
  `55ac4ab8b303d4213cc8e3ac7096416d96c7a98e40b41dee4a6790b7a318ede1`;
  results belong in `full-gate-source10/summary.json`. Pending is not passing.

Local GNU is pristine source `636f166cfc86aa90d63f592fd99f3fdd9ef95ebd`, native
capable, but executable SHA-256
`7d8944fe2b2bdbd2856cfd4f47dbd5c80db90089ac20be641c10a348bf217e82` differs from
the locked Darwin oracle. The comparisons above are source-matched diagnostics,
not a pinned-oracle frozen compatibility certificate. The lock was not changed.
Builds use Rust 1.97.1. Selected builds use the existing opt-level-1 test profile;
the full serial gate uses the existing gate profile with assertions and overflow
checks. None of these runs establishes release performance.

## Remaining work

Finish the full gate, affected GNU chartab/syntax/case/keymap/native/dump/GC
coverage, both platforms' frozen comparison, release-specific paths, and the
adversarial audit. Inspect raw inventories and artifacts rather than only CI
status. Use the existing Linux workflow; keep the pinned oracle unchanged.
Then continue with the remaining frame bridge, compact cons payload, ownership
boundaries and the complete original audit requirements. Preserve the locked
16 workloads and 3% criterion. No GNU parity or measured speedup is claimed.
