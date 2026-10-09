# Copied symbol payload access — 9 October 2026

PR82 carries **source318 after PR81 merged**. The previous source316 checkpoint
is retained at `30fa6e5b`; the archived unapplied state below is historical.
[Applied-source verification](handover/2026-10-09-symbol-fields/source318-applied-source-verification.json)
checks every runtime byte and mode against its completed validation. Its [portable selected evidence](handover/2026-10-09-symbol-fields/source318-symbol-payload-api-draft-manifest.json)
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
The [complete Linux evidence](handover/2026-10-09-symbol-fields/source318-linux-complete-results-manifest.json)
verifies **3,178 Rust passes / two existing ignores**, including native artifact
identity, plus **519 frozen files / 7,928 matching outcomes / 1,038 successful
processes**. Each editor has 7,670 passes, 47 expected failures and 211 skips;
these categories stay distinct. All 3,077 library names, raw outcomes, source
inputs and retained artifact hashes are checked.

The original macOS frozen run retains both Tramp timeouts at 180 seconds after
318 complete files / 4,479 matching outcomes. A separate 600-second observation
now obtains all 59 matching Tramp outcomes: GNU takes 188.635 seconds and Emaxx
223.763 seconds. It uses the same 562 runtime inputs and image with an
independently identified rebuilt executable. The [remaining 200-file tail](handover/2026-10-09-symbol-fields/source318-macos-frozen-continuation-results-manifest.json)
has completed at the original deadline: 3,377 matches, six existing diagnostic
differences and 400 successful processes, with no unexpected outcome. Every
native compiler and Eglot outcome matches. Across the separate attempts, all
519 files are observed with 7,915 matching outcome labels. The [six diagnostic
issue entries are unchanged from source316](handover/2026-10-09-symbol-fields/source318-existing-macos-diagnostics-audit.json).
The original run and six differences remain failed; this is not a single
passing frozen run. Full terminal validation is running separately. The original
source317 setup failures and source316 ownership negative remain preserved.

The [completed Linux timing diagnostic](handover/2026-10-09-symbol-fields/source318-linux-symbol-performance-results-manifest.json)
independently verifies all **192 actual answers and modes**. Three alternating
full16 pilots have body ratio **1.005265** against source316 main (0.53% slower).
Paired GNU aggregates are **2.857181× for source318** and 2.808045× for the baseline.
Explicit GC regresses 6.33%, bindat 5.28%, interpreted dynamic loops 4.26% and
mapcar 3.57%; every sample remains. This combined registry/API change has no
established timing improvement and does not meet calibrated nine-round/every-case
GNU acceptance. Physical allocation/counter requirements remain unresolved.

Continue with instance-owned canonical symbols and graph-preserving image
copying, then direct allocated fields. The ID counters, name lookup adapters,
field/enumeration tables and GC fixed-point walk remain temporary. Real
intervals/pure storage, honest physical allocation/counters, final ownership
and adversarial reviews, final-source platform validation and the locked
16-workload/nine-round/every-case 3% GNU criterion all remain required.
