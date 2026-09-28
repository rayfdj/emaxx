# Authoritative cons payload — 28 September 2026

The [complete goal](runtime-representation-goal.md) remains incomplete. This
checkpoint follows the integrated [frame](runtime-representation-frame-checkpoint.md)
and [ordering](runtime-representation-ordering-checkpoint.md) checkpoints at
`76ad608fdd71fe4590042f601780e5ddfeea9e9a`. Source 65 passes 185 selected release
tests, strict static checks, and four ordinary batch comparisons against
source-matched GNU using a fresh executable and dump image. Source 64's broader
release failure remains recorded below. The full macOS gate subsequently fails:
2,833 library tests pass, three audit checks fail, and two existing opt-in TTY
tests remain ignored. Later Cargo stages do not execute. The audit repairs and
the next object migration are recorded in the
[positioned-symbol checkpoint](runtime-representation-positioned-symbol-checkpoint.md). This is
not a full validation or performance certificate. The [portable evidence
manifest](handover/2026-09-28-cons/manifest.json) preserves every source snapshot
and its successful or failed validation; binaries must be rebuilt elsewhere.

## Representation and GNU reference

`ConsCell` contains exactly two `Cell<Value>` fields at offsets 0 and 8. Its
16-byte payload is the storage used by the interpreter, bytecode, generated
native loads/stores, and dump relocation. `NativeCons` is an alias for this
same type. Encoding a live value returns its word, without graph preparation.
Distinct native heaps no longer impose ownership on a shared cons allocation.

This follows `lisp.h:struct Lisp_Cons`, `data.c:Fsetcar/Fsetcdr`, and
`alloc.c:Fcons`, `CONS_BLOCK/CONS_INDEX` and `sweep_conses` in GNU source
`636f166cfc86aa90d63f592fd99f3fdd9ef95ebd`. Ordinary reads and stores do not
consult a registry, verify a mirror, publish changes or notify watchers.
Native dirty queues, per-heap cons maps, owner registrations, touched sets,
recursive conversion work, mutation epochs and read barriers are removed.
The native symbol-word cache is removed too; readers return the current cell's
word. This does not complete symbol value/function/plist ownership migration.

The shared marker traces the actual fields once, car before cdr. Native roots
and conservative interior pointers use the allocator's common validator before
entering that traversal. The old second native traversal is gone. Checked
native decoding still rejects a cons word pointing inside a cell; a C pointer
to its cdr can conservatively root the containing allocation.

## Allocation and remaining costs

Each aligned 32,768-byte block has 1,350 cons slots: 21,600 bytes of payload and
11,168 bytes of metadata and padding. Metadata comprises weak-reference serials,
allocation and mark bitmaps, and a block epoch. Registry and system-allocator
overhead are additional; these block figures are not total process memory.
The old payload was 80 bytes. The new 16-byte payload is not a claim that each
cons costs only 16 bytes of retained memory.

GNU's allocator needs a mark bitmap; this runtime also validates weak Rust
cache references across slot reuse and checked native decoding. The serial and
allocation bits serve those contracts. Reached-cell marking and allocation
derive metadata by alignment and index. Ordinary field access never reads it.
The allocation charge remains 16 bytes per cons, as it was before this change;
the new implementation derives that charge from the actual payload size.
There is no deliberate change to GC thresholds or suppression of collection.
Collection-count equivalence still requires workload measurements.

Keymap and regexp views retain temporary weak snapshots of their input words.
Validation checks the allocation serial and current fields without keeping the
source cons alive. Their scan and lookup costs are unprofiled. Remove these
adapters with the remaining derived keymap/regexp state before declaring the
architecture complete. The keymap reverse index now records public roots only,
and is cleared when the associated record is swept.

The remaining `setcar`/`setcdr` primitive owner lookups and global macro-cache
invalidation are removed. A cached negative macro verdict missed raw native
stores, so macro dispatch now resolves the current function cell and reads its
tag directly. It allocates alias-cycle bookkeeping only for an actual alias.
Keymap parent and character-table readers use the public list; sparse binding
projections still require the temporary snapshot check described above.

## Evidence and test contracts

Source 57 reproduces all four representation failures: 80 versus 16 bytes,
unprepared raw fields containing zero, distinct native heaps rejecting the same
cons, and the prior vector-to-cons raw-read failure. Sources 58 and 59 preserve
the partial migration's 104 compiler errors and three fixture compiler errors.

Source 60 executes 177 selected debug tests: 173 pass and four fail. Three
reclamation controls still leave the supposedly unreachable address in a live
Rust frame, which correctly becomes a conservative root of the shared heap.
Their allocation is moved to a separate helper, the address is hidden until
after collection, and physical reclamation remains required. The fourth test
expects eager rebuilding of an internal keymap record; it now verifies the
direct reader and ordinary `lookup-key` result before checking the refreshed
record. All 177 controls and strict static checks pass on source 61.

Source 62 adds five probes. Interior-pointer validation, multi-block survival
and reclamation, and weak identity after identical-payload slot reuse pass.
The raw macro-verdict and keymap-reader probes fail and retain their original
assertions. Source 63 fails compilation because a cycle helper is named
incorrectly; no tests execute. Source 64 uses the existing constant-space
cycle guard. All 183 selected debug tests pass (100.93 seconds), including the
two new raw-store regressions and the same Lisp expression in GNU and Emaxx.
Rustfmt, all-target/all-feature compiler checks, strict Clippy and diff checks
pass, with unchanged software hashes through validation.

Source 64 also passes 32 GC controls with `EMAXX_GC_VERIFY=1`. Its broader
release selection executes 520 tests: **519 pass, one fails**, zero ignored
(624.01 seconds test time). Both original suspended-thread survival/reclamation
contracts pass. The new weak-slot reuse fixture still retains its victim at
the first reclamation assertion. The exact immutable release executable fails
the same assertion in a fresh process, ruling out prior-test history as its
cause. Source 65 moves all temporary pointer inspections into non-inlined
helpers outside the collection frame; every original assertion is retained.
All 185 selected release tests pass on source 65 (25.31 seconds test time),
including both original suspended-thread contracts, with clean strict static
checks. Source 65 changes only this fixture after source 64; its production
code is identical. Source 64's broad run remains a failure.

Source 65 also builds a fresh ordinary release executable and dump image.
Eight processes compare four identical Lisp inputs through GNU and Emaxx batch
entry points. Cons mutation, ordering, native vector words and native string
words all produce the exact expected output in both editors. The latter two
fixtures compile and execute their own native functions and exercise collection
of shared cyclic graphs. Software, executable and image hashes remain unchanged
through every comparison. These checks do not cover the complete ordinary or
terminal inventory.

Historical test selectors are retained. Assertions about deleted mirror,
ownership, epoch and watcher containers now check word identity, direct mutation
visibility, current cache inputs, trace order, and actual allocation survival
and reclamation. Deep 100,000-cell shared/cyclic controls remain. No ignore,
timeout, GNU golden, test selector or comparison policy is weakened. The two
original suspended-thread reclamation contracts retain both live-root and
post-exit reclamation assertions and their existing fresh-process boundary.

The local GNU executable is source-matched but differs from the pinned Darwin
binary. All live GNU comparisons here are diagnostics, not frozen certification.
No runtime speedup or GNU performance parity has been measured for this checkpoint.

## Continue here

Run the complete macOS gate on the later repaired source and ordinary compatibility/TTY comparisons. Preserve
every unsuccessful run, including the earlier completed source-36 full-gate
failure and the sixteen source-25 terminal divergences.

Record/hash/keymap and symbol authority, symbol-with-position native views,
dump treatment, the public ownership and serialization audit, original audit
findings, full Linux/macOS and pinned frozen validation, VM/call profiles,
allocation counters and the locked sixteen-workload performance evidence are
still required. The later checkpoint removes the symbol-with-position views.
The 3% criterion and full inventory are unchanged. The user subsequently
approved publication and Linux CI for `803e4326`; that earlier char-table
commit is pushed and its Linux run is recorded in the later checkpoint.
It does not certify this cons source.
