# Shared hash-table allocation — 29 September 2026

Hash tables now use one GNU-compatible allocation for interpreter, bytecode and
native operations. This replaces the host record and four per-interpreter maps
for standard entries, custom entries, callback mutation protection and immutable
tables. JSON, printing, CCL, GC, copying and image restoration use the same
allocated table. The [complete goal](runtime-representation-goal.md) remains open.

## Representation and GNU reference

GNU `lisp.h:Lisp_Hash_Table` and `fns.c:make_hash_table` define the layout: PVEC 14,
a 72-byte C-compatible payload, rounded to 80 bytes by this vector allocator.
Count, flags, descriptor and all four array pointers are actual fields. Native
stores and interpreter operations see the same keys, values, stored hashes and
bucket/free chains. The unchanged original native probe that failed on the
host-record tag now passes, together with direct descriptor callback controls.

For capacity N > 0, owned arrays occupy `24*N + 4*I` bytes, where I is the next
power of two strictly greater than N. Empty tables share one constant index.
Growth stages complete arrays before installing them; replacement and sweeping
release old arrays. The allocation census includes the rounded table and actual
live array bytes. The GNU allocation counter charges 72 logical header bytes and
array storage. No complete physical-memory accounting claim is made.

Copying preserves holes, stored hashes and both chains, following GNU
`copy_hash_table`. Lookup checks identity before the stored hash, following
`hash_lookup_with_hash`. Purecopy recursively replaces Lisp fields without
calling custom hash functions. Weak collection traces the authoritative arrays,
finds the reachability fixed point and removes rejected slots in bucket order.
The existing image format is retained; loading allocates real tables and delays
rehashing until object fields have been relocated.

GNU `get_hash_table_user_test` keeps custom descriptors and their three Lisp
roots for the process lifetime. Emaxx follows that policy. Standard descriptors
are reached directly without searching the custom list. Binding Rust's allocated
symbol words requires a first-use allocation instead of GNU static initializers.
The GNU 40-byte descriptor prefix is followed by an immutable kind byte: Rust
function addresses are not reliable identities across codegen units. Alignment
makes each shared standard descriptor 48 bytes and each custom node 56 bytes,
eight bytes more than the corresponding GNU structures, before allocator
metadata. These host bytes are not charged to GNU's GC allocation counter.

Constructor validation now captures the test descriptor before checking size,
uses the current positioned-symbol equality rules for weakness and callbacks,
and allocates only after keyword validation. Same-input GNU controls retain
first-keyword behavior, duplicate-keyword errors and captured callback identity.

## Validation and preserved failures

Source 133, commit `c7839c13` (task-branch integration `3a0f4d41`), passes 105 selected debug and 271 release tests, zero ignored, with
the GC verifier enabled. Both original suspended-root contracts are included.
Formatting, all-target/all-feature compiler checks, warnings-denied Clippy and
diff checks pass. A fresh ordinary executable and image match all eight GNU
fixtures exactly: copy, captured functions, captured symbols, actual buckets,
purecopy, constructor ordering, positioned callbacks and positioned-key copying.
Every process exits zero with empty stderr and unchanged source/artifact hashes.
These GNU comparisons use the source-matched candidate, not the frozen Darwin
executable. The complete Rust gates subsequently fail as recorded below.

The [portable receipts](handover/2026-09-29-hash-allocation/manifest.json) retain
commands, source manifests, original failures, exact binary/image identities,
complete selected inventories, raw outputs and reproduction drivers. They also
retain the main checkpoint's independent keymap reproduction; it is not evidence
of a new hash regression.

Sources 122, 123 and 126 failed compilation; 124 and 125 failed at remaining
host-record initialization and native decoding. Sources 127 and 128 failed
strict Clippy. Source 129 passed debug controls but failed one of 269 release
tests. The exact failed executable also failed the isolated weak-table control.
Retained ARM disassembly shows the test's positive survival check leaving a raw
value pointer in callee-saved x22 through the second collection. Source 130 uses
the existing non-inlined hidden-pointer helper for those positive checks,
retaining every survival, reclamation, membership and collection assertion.
It passes 103 debug and 269 release controls. This does not explain the earlier
Linux suspended-bytecode reclamation failures.

Source 131 passes 105 debug, 271 release and seven ordinary comparisons. Source
132 replaces function-address classification with the explicit descriptor kind;
it passes debug and static checks but fails one release assertion about toggling
positioned-symbol equality after insertion. The unchanged isolated binary passes.
Repeating the original Lisp body in GNU gives 526 identical-key hits and 3,570
misses in 4,096 trials: a changed hash can select the same bucket, where identity
wins before hash comparison. The old always-missing expectation is invalid.
Source 133 retains the count and overwrite checks, both enabled lookups and
restoration; its disabled scope checks that original and copy agree, both bare
lookups miss, and positioned/bare lists compare unequal. No test is ignored or
production lookup changed for this correction.

A separate GNU purecopy probe with unused slots aborts with exit -6. That failed
probe remains preserved; a distinct full-table control passes. No GNU crash is
counted as a pass or deliberately reproduced in Rust.

## Complete-gate failure and assertion repair

The original source-133 complete gates both fail at
`dump_emacs_portable_restores_context_and_reports_native_image_limit`: 2,174
passes on macOS and 2,182 on Linux precede the one failure. Four later library
groups and all later Cargo stages remain unexecuted. The
[Linux run](https://github.com/rayfdj/emaxx/actions/runs/36524987767) and
[portable failure receipts](handover/2026-09-29-hash-gate-repair/manifest.json)
retain the exact source, executable, image, logs and incomplete inventories.
The downloaded Linux executable and image were checked against their recorded
hashes.

The test still expected reopening 117 native units to advance `next-record-id`
by 117. GNU `pdumper.c:RELOC_NATIVE_COMP_UNIT` creates each unit's
`lambda_gc_guard_h` with `Fmake_hash_table`; these tables now have their own
allocation, so the observed record-counter change is correctly zero. Source 135
(audit commit `b04cd0bc`, integration `27effbdd`) removes the special counter
exception and requires the entire remembered-scalar group to match exactly.
Every other root, native-unit and restored-behavior assertion remains. This is
a test-only change. Both dump controls pass in debug and release, with zero
ignores, and all strict static checks pass. Its fresh complete macOS and
[Linux gates](https://github.com/rayfdj/emaxx/actions/runs/36527539384) then fail at
`value_less_selected_upstream_unordered_cases_match_emacs`: macOS has 2,260
passes and Linux 2,268 before the one failure. Three later library groups and
all later Cargo stages remain unexecuted. Those failures and their exact
artifacts are in the [ordering repair receipts](handover/2026-09-29-hash-ordering-repair/manifest.json).

GNU `fns.c:value_cmp` treats two hash tables as unordered. The representation
migration left the new hash kind outside this branch, causing `value<` to signal
`type-mismatch`. Source 139 (audit `736dd41e`, integration `75b69738`) restores
that dispatch; different object kinds still signal. No test assertion changes.
The unchanged original control covers both directions and nested comparisons.
All 154 selected debug and 154 release tests pass, with zero ignores, together
with strict static checks. A fresh ordinary executable and image pass all eight
previous hash comparisons plus a new same-input GNU comparison covering self
and distinct tables, differing capacities and tests, callback non-invocation,
forced GC, nested list/vector ordering and mixed-type error operand identities.
All 18 processes exit zero with empty stderr and unchanged source/artifact
hashes. The original ordinary executable fails the same new input; its output
is retained. Complete macOS and [Linux gates](https://github.com/rayfdj/emaxx/actions/runs/36530994726)
are running on this repaired source. PR #76 remains draft pending both results.

All 16 locked workloads also complete on the ordinary source-133 executable:
32 successful processes, exact results and execution modes, unchanged inputs.
Single-observation body ratios range from 1.31 to 8.55 times GNU, with geometric
mean 3.13. These diagnostics ran alongside correctness work and lack allocation
counters; they establish neither a controlled speedup nor performance parity.
Every raw sample is in the same follow-up bundle.

## Remaining work

The previous main checkpoint's full terminal comparison passes all 223 scenarios,
but its Linux frozen run remains failed and incomplete: two of 500 compared
files differ, the next keymap report has invalid UTF-8 and 18 later files never
execute in that run. A [separate tail run](https://github.com/rayfdj/emaxx/actions/runs/36524566529)
on the same main runtime passes those 18 files and 468 outcomes, with no
mismatches; it does not clear the failed full run. The same original keymap file reproduces nine Emaxx failures on macOS
while all 46 GNU tests pass. See the [compatibility record](runtime-representation-call-window-checkpoint.md).

Complete gates for this hash source, final pinned comparisons on both platforms,
remaining representation and ownership work, allocation accounting, the final
adversarial audit and the locked performance criterion remain required. Parked
interpreter states still retain their weak entries strongly under an existing
policy. Standard hash-number algorithms remain implementation-specific. No
controlled speedup or GNU performance parity is claimed.
