# Overlay property-storage checkpoint — 2026-09-27

Commit `f2e23fc22e8a7d77c0c30733097509937926a838` stores an overlay's
properties in one Lisp plist. This is a precursor to canonical overlay objects;
overlay IDs, buffer scans, the interval vector and native bridge remain.

GNU source `636f166cfc86aa90d63f592fd99f3fdd9ef95ebd` supplies the mechanism:
`buffer.c:Foverlay_put` compares keys with EQ, prepends new properties and
updates an existing value cons in place. The old Rust pair vector appended
properties and treated distinct equal floats as one key. `Foverlay_properties`
and `copy_overlays` copy only the list spine, preserving key/value identities.
`alloc.c:mark_overlay` traces the actual plist; `pdumper.c:dump_overlay`
relocates it. Emaxx now follows that ownership and graph behavior. Display
callers that use Rust property names walk the list without interning a symbol
on every read. There is no new property cache or lookup table.

The shared GNU/Rust fixture checks generated keys, property order, replacement,
distinct equal floats, bytecode access, copying, deletion, forced collection,
cyclic property values and independent indirect-buffer updates. All 13 checks
pass in GNU and Emaxx. The existing image test retains its assertions and adds
a second reference to the actual plist, requiring identical restored conses.
The earlier test-compilation failure (an assertion around a unit-returning
setter) remains recorded in `buffer-ownership-check-70b.json`.

Exact source70d passes formatting, whitespace checks, all-target/all-feature
strict Clippy and fresh all-feature library-test compilation, with zero
warnings. Local controls pass 47 overlay tests, 19 root tests and 390
buffer/dump/TTY tests. The groups overlap: 438 distinct tests pass. Two existing
release-only TTY end-to-end tests remain unexecuted (`tty_smoke_end_to_end` and
`tty_differential_end_to_end`); they are not counted as passes. No ignores or
allowances were added. Source and binary hashes match after execution. The
saved test executable SHA256 is
`01d4cd3224c3dde28f00c120d794a0897a0c680bb2921ddda5f5a6156b17dd13`.

Existing Linux CI at the exact commit also passes formatting and strict Clippy:

- [Run36269589920](https://github.com/rayfdj/emaxx/actions/runs/36269589920):
  47/47 Rust overlay controls and 406/406 GNU buffer outcomes.
- [Run36269592554](https://github.com/rayfdj/emaxx/actions/runs/36269592554):
  19/19 unchanged Rust root controls and 5/5 GNU allocation outcomes.

Both runs have zero ignored Rust tests and zero skipped GNU outcomes. The
ordinary upstream selectors are unchanged. Raw archive digests, exact clean
commit identity, editor/image hashes, discovered/selected/executed inventories,
process exits and raw outcome totals were verified. The existing affected
workflow retains fresh Rust compilation and test binary paths but does not
retain their executable hashes; this limit is explicit in the receipts.

Unfavorable diagnostic timings are retained: buffer body2613ms/GNU307ms
(8.511x), setup4050ms/GNU201ms; allocation body45ms/GNU8ms, setup646ms/GNU98ms.
These single samples are not interleaved performance measurements; the 8ms
GNU allocation body is too short for authoritative ratio claims. They establish
neither improvement nor regression against earlier checkpoints.

The new plist uses real cons allocations. Conses remain 80 bytes, so this is
not a speedup claim or completion of the compact representation. Three native
bridge kinds, ordinary overlay ID scans, full allocation accounting, public
API/rooting soundness, the old 64 KiB post-GC stack clear, complete platform
and release gates, pinned Darwin validation and the locked 16-case performance
objective remain open. No performance workload, tolerance, upstream expectation,
timeout, comparison strictness or CI workflow changed in this checkpoint.

Combined receipt: `target/runtime-goal/overlay-plist-milestone-70.json`.
Related receipts in that directory: `overlay-plist-audit-70.json`,
`overlay-plist-oracle-70.json`, `overlay-plist-format-70.json`,
`buffer-ownership-check-70d.json`, `overlay-plist-build-70.json`,
`overlay-plist-controls-70.json`, `overlay-plist-ci-completed-70.json` and
`overlay-plist-roots-ci-completed-70.json`. This scoped audit does not certify
universal absence of cheating or completion of the full runtime goal.
