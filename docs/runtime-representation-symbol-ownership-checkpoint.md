# Weak symbol ownership across host threads — 28 September 2026

The [complete goal](runtime-representation-goal.md) remains incomplete. This
checkpoint follows the combined reader and GC source at
`2c055345ee2af24bba858cf8a373e3588f710e77`. Source 87 passes 189 selected debug
and 389 selected release tests, with zero failures or ignores and the optional
GC verifier enabled. Strict formatting, all-target/all-feature compiler checks,
Clippy with warnings denied, and diff checking pass on unchanged software.
All three existing public-runtime integration tests and all nineteen ordinary
reader/printing comparisons pass. The
[portable evidence](handover/2026-09-28-symbol-ownership/manifest.json) retains
both original failures and the final results. No full, frozen or performance
certification is claimed.

## Ownership repair

The uninterned-symbol weak lookup book was thread-local, while symbol allocation
and collection are process-wide. A later public runtime call can acquire the
existing runtime lock on another OS thread. Before the repair, that thread
cannot resolve a still-live private symbol key, and its collection cannot remove
entries from the creating thread's book. The latter leaves a stale handle after
sweep. The regression checks that entry's presence without dereferencing it.

The weak book now uses the same process-wide `ProcessTable` mechanism as the
interned-symbol table. Public entries already acquire the runtime lock; Lisp
threads run on the owning OS thread. No per-lookup lock, new object model or new
GC root is added. `sweep_symbol_cells` removes the weak entry before releasing
its symbol allocation. Values and symbol handles remain confined to the runtime;
the tests pass only owned strings, booleans and address bits across host threads.

GNU `thread.c` serializes Lisp execution with its global lock, `alloc.c` allocates
and sweeps symbols process-wide, and `lread.c` owns the standard obarray. GNU does
not need this private text-key book: it remains an adapter for Emaxx's name-keyed
symbol operations. Removing those operations and giving the symbol allocation
its value, function and property cells remain separate architectural obligations.
The table change has not been profiled and establishes no speedup.

## Preserved controls

Source 86 adds two tests to unchanged production at `a210f90c`; both fail. One
creates a symbol in a serialized host entry, exits that thread, then checks the
same live key and identity in another entry without an intervening collection.
The other keeps the creator alive while another host thread performs ordinary
`garbage-collect`, then requires the creator's weak entry to be absent. Its
waiting frame retains owned text only. Both controls are unchanged in source 87.

Source 87 combines the previously validated reader and GC repairs before changing
the weak book. The debug selection passes in 136.37 seconds and the release
selection in 233.44 seconds, as reported by libtest. Both original suspended-thread
survival/reclamation contracts pass, along with the existing concurrent and
nested runtime-lock test. The release inventory has 2,867 tests: every one of
source 85's 2,864, the GC verifier control, and the two new ownership controls.
No selector is removed.

The complete existing public-runtime integration inventory passes: three tests,
zero ignores, 61.55 seconds in libtest, with the verifier enabled. It exercises
twelve concurrent public editor calls with collection and nested entry, owned
ERT error reporting, and complete ERT inventory with an intentional unexpected
failure retained in the report. The integration executable and source remain
unchanged throughout.

A fresh ordinary executable and image match GNU on all nineteen identical
reader/printing inputs, including all three original stream failures. Every one
of the 38 processes exits zero; stdout and stderr match byte for byte, including
non-UTF-8 output. Software, executable and image hashes remain unchanged. The
Emaxx executable SHA-256 is `18e9b74c211126f198f118de052bb23a29e21ea6dd7039c35495e0102957336a`;
its image SHA-256 is `70b0261e5d0ef595f18cf6d2ce72f4e1f2a7de8e314af2aa796fa66d2c33cf65`.
The local GNU executable still differs from the pinned Darwin oracle.

## Remaining work

This fixes the demonstrated thread-local book mismatch; it does not prove every
unsafe borrow, conservative root or public ownership boundary sound. The older
macOS full gate and approved Linux run remain failed, with their later stages
unexecuted as documented in the [reader checkpoint](runtime-representation-reader-checkpoint.md).
The Linux text-conversion primitive, sixteen earlier terminal divergences,
generic records, symbol-cell authority, hash/keymap authority, full buffer/frame
ABI, dump mapping, real allocation counters, final adversarial review and measured
VM/call optimization remain open. Final Linux/macOS full, frozen and terminal
validation and the unchanged sixteen-workload 3% performance criterion remain
required. Local source-matched GNU diagnostics do not certify the pinned oracle.
