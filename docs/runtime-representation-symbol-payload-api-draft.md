# Copied symbol payload access — 9 October 2026

Source318 is a **separately packaged, unapplied continuation**. PR81's runtime
remains source316. Its [portable selected evidence](handover/2026-10-09-symbol-fields/source318-symbol-payload-api-draft-manifest.json)
contains an incremental patch on local source317 `c7185a61` and a cumulative
patch on published `4dcf9f6a`. Both reproduce all **562 inputs and modes**.
The full goal remains active and incomplete.

GNU `lisp.h:SYMBOL_VAL` and `SYMBOL_ALIAS` return the selected word;
`SET_SYMBOL_VAL` and `SET_SYMBOL_ALIAS` perform explicit stores. The Rust field
API now also returns copied value, function and alias words. No reference to a
mutable symbol payload escapes the table. Existing bound-only stores still use
one cell lookup; absent bindings keep the existing insertion path. Redirects,
watchers, assignment order and dynamic binding restoration are unchanged.
The image copier transforms owned words in the original traversal order using
its existing shared memo. There is no new payload store, lookup or temporary
collection. Immutable owner handles used for enumeration remain borrowed.

This prepares the API for interior mutation through copied symbol handles.
The allocated symbols remain process-shared, and their fields still live in
per-interpreter tables. **This does not repair the recorded instance-name
property leak**, implement per-instance interning/image copying, remove the
weak-field GC fixed point, or establish a complete ownership audit. No physical
memory saving or timing improvement is claimed for this access-API change.

Formatting, all-target/all-feature checking, strict Clippy and diff checks pass
without warnings. **All 208 selected controls pass in gate and release**,
retaining all 175 source317 controls and both complete test inventories. The
additional selection covers existing image-copy, binding, watcher and function
cell tests. Old symbol-cell assertions change only their Rust borrowed/copy
spelling. The removed test-only mutable-by-name accessor becomes the real
name-keyed store, adding a check of the previous value while retaining every
original transition and expected value. The independent audit checks those
bounded adaptations against the complete previous test module.

**Eighteen successful ordinary processes / nine comparisons match**, with the
same fixtures, assertions, deadlines and mode checks as source317. Both editors
execute real bytecode/native code; all three redirect helpers and their caller
are confirmed native. Raw outputs, compiler-selected test binary identities,
process receipts, source captures and both patch replays are retained.

The [complete source318 macOS Rust gate](handover/2026-10-09-symbol-fields/source318-macos-rust-results-manifest.json)
verifies **3,166 passes / two existing ignores**, including native artifact
identity. All 3,069 library names and raw verdicts, Cargo results, 562 input bytes
and modes, and retained artifact hashes are independently checked. It ran the
uncommitted API change on source317 `c7185a61`; both portable patch replays match.
Post-run retained images identify those artifacts, not per-test image use.
Linux, frozen, terminal and timing validation remain pending. The original
source317 setup failures and source316 ownership negative remain preserved.

Continue with instance-owned canonical symbols and graph-preserving image
copying, then direct allocated fields. The ID counters, name lookup adapters,
field/enumeration tables and GC fixed-point walk remain temporary. Real
intervals/pure storage, honest physical allocation/counters, final ownership
and adversarial reviews, final-source platform validation and the locked
16-workload/nine-round/every-case 3% GNU criterion all remain required.
