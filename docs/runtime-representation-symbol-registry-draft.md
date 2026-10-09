# Symbol registry removal — 9 October 2026

Source317 is a **separately packaged, unapplied continuation** of source316
`4dcf9f6a`. PR81's production runtime remains source316. The
[portable patch and selected evidence](handover/2026-10-09-symbol-fields/source317-symbol-registry-draft-manifest.json)
retain its original failures and the remaining public-instance ownership gap.
The full goal remains active and incomplete.

Each allocated symbol already owns its ID, and both name lookup tables are
already process-wide. The separate `SYMBOL_IDS` map therefore duplicates name
storage and adds a mutex, allocation-time lookup, and sweep-time count update.
The draft removes that map and reads the ID from the object found in the
existing table. It also removes the boxed copy of an uninterned symbol's key.
The weak lookup is now a set of symbol handles keyed by the object's immutable
host text, replacing another owned string copy. Sweep removes that entry before
freeing its object, checking identity if an internal constructor reused a key.

GNU `alloc.c:Fmake_symbol/init_symbol` creates a new object on allocation;
`lread.c:intern_driver` and the obarray retain its identity and name.
`sweep_symbols` retires unreachable objects. The remaining ID counters and weak
name lookup are migration adapters; direct allocated-symbol fields must remove
their need. The process-shared intern pool and per-interpreter field tables
remain. Logical GNU GC charges still do not measure the complete Rust footprint.
No timing improvement or completed physical-accounting claim is made.

Strict formatting, all-target/all-feature checking, Clippy and diff checks pass
without warnings. **All 175 selected controls pass in gate and release**, retaining
the preceding 128. The new control moves lookup across serialized OS-thread
entries, then collects after the creator threads exit: the interned object
survives, the weak lookup retires, and a replacement gets a new ID. Every old
test body and selector remains unchanged. The patch replays all **562 source
inputs and modes** from its published base.

The first cohort remains **143 passes / 32 failures**: the fresh worktree lacked
the required `../emacs` source link. After installing that link to the same
contracted GNU source, the exact same binary, selection, source bytes/modes and
environment overrides pass all 175. The failed run is preserved separately.
**Eighteen ordinary processes / nine comparisons match**, including actual
bytecode/native execution and native compilation of every redirect helper.
The launcher uses GNU's previously verified named-function compilation API.

A separate source316 public-API probe confirms the ownership work still needed.
Both editors execute the same Lisp with `purify-flag` explicitly nil. The first
instance attaches a property to an interned symbol's name and reports
`(t owner-one)`. A fresh GNU instance reports `(t nil)`; the next owned Emaxx
editor call reports **`(nil owner-one)`**, reusing the prior name object/property.
Both actual answers are captured through the public process entry's normal
flushed shutdown. Earlier stale-library/LTO launcher errors, a bare-purification
setup failure and an unflushed observation remain preserved; those attempts do
not establish the ownership result. Registry removal does not repair this gap.

Continue with instance-owned interning and graph-preserving image copying, then
move the mutable fields into their allocated symbols. Preserve canonical
identity and sharing within each instance, independent mutable names between
instances, and survival/reclamation across parked states. Owned-value field APIs
must preserve Rust aliasing when copied symbol handles access the same object.
Complete source317 platform gates, frozen comparisons, relevant terminal coverage
and timing have not run. Every other full-goal requirement, including final
ownership/audit work and the locked performance criterion, remains required.
