# Symbol fields — 9 October 2026

Source314 continues the full goal; **the architecture and performance goal remain
incomplete**. The [selected evidence](handover/2026-10-09-symbol-fields/source314-selected-manifest.json)
identifies every changed input and retains the earlier failures. Complete Linux
and macOS Rust validation, Linux frozen comparison and Linux diagnostic timing
have now finished. The full macOS frozen comparison continues separately.

The existing per-interpreter symbol cell now owns its property list alongside
the value, function and alias. The separate name-keyed plist vector and both
position indexes are removed. `get`, `symbol-plist`, `setplist` and native `get`
read this payload. `put` and ordinary EQ `plist-put` share GNU's in-place walk:
updating a property retains the original conses; adding one allocates two conses.
The walk preserves EQ key identity, early successful updates, malformed-list
errors, cycle error data and quit checks (GNU `fns.c:plist_put/Fput` and
`lisp.h:FOR_EACH_TAIL_INTERNAL`).

Uninterned symbol fields now form edges from their owner instead of permanent
roots. GC follows reached owners, participates in weak-table marking, and removes
dead side cells before symbol sweep (`alloc.c` symbol marking and `sweep_symbols`).
The same constructor/lifetime fixture leaves all nine symbols retained in the
unchanged baseline; GNU and this draft reclaim all nine after their roots die.
Reachable children, cycles, parked interpreter fields and finalizers survive.

Following the actual edges exposed missing alias retention: `LOADHIST_ATTACH`
must accept an empty `current-load-list`. The repaired entry keeps an alias and
its target alive; removing it permits both to be collected, matching ordinary
GNU. The image reconstruction handoff also stops before `loadup.el` finishes, so
it must not commit a completed load-history entry or run after-load callbacks.
GNU's loader commits after EOF; loadup restores its filename only at its end.

Allocated-symbol authority is **not complete**. Allocated symbols still share
names/identity across interpreter instances; their mutable cells remain per
interpreter to preserve isolation. The temporary GC-only fixed-point walk must
disappear when each instance's allocated symbols own their fields. No ordinary
access gains a new hash lookup. The timing below measures the combined change;
it does not isolate the GC walk or enlarged dense cell. No speedup or GNU parity
is claimed.

The portable patch replays all **560 runtime/build/test inputs and checkout
modes** from main `b724bab0`; 102 auxiliary inputs are unchanged. Formatting,
all-target/all-feature checking and strict Clippy pass without warnings.
All **24 focused controls pass in gate and release**, including both original
suspended-root survival/reclamation tests. All six added library tests extend
the previous inventories; no old name, selector, timeout or assertion is removed.
The only old assertion adapted to storage removal still checks the same plist
enumeration count. **Twelve ordinary processes / six comparisons match**, using
each editor's own executable/image and actual interpreted, bytecode and native
execution. The GNU reference is the recorded rebuild, not the lost original
Darwin executable.

The [complete macOS Rust audit](handover/2026-10-09-symbol-fields/source314-macos-rust-results-manifest.json)
verifies **3,161 passes / two existing ignores**, including native artifact
identity, actual native thread continuations and public runtime ownership.
Every one of the 3,064 library names and every Cargo result agrees with its raw
log. The run started on a dirty `b724bab0` checkout; all 560 original and actual
post-run source bytes/modes match published `2712c057`. The detached wrapper
saved the successful command receipt but omitted its final source-after file;
the independent capture and that limitation are preserved. No test was rerun
to replace the result.

The [complete Linux evidence](handover/2026-10-09-symbol-fields/source314-linux-complete-results-manifest.json)
verifies **3,173 Rust passes / two existing ignores**, all 3,072 raw library
names, native artifact identity and the retained pinned GNU executable/image.
The full frozen comparison matches **519 files / 7,928 outcomes** across **1,038
successful processes**. Each editor has 7,670 passes, 47 expected failures and
211 skips. No previous library name, frozen selector or expected outcome is lost;
all six source314 tests extend the prior Linux library inventory.

The [Linux timing audit](handover/2026-10-09-symbol-fields/source314-linux-symbol-performance-results-manifest.json)
verifies all **192 actual answers and execution modes** across three alternating
full16 pilots of main `b724bab0` and source314. The raw body geometric ratio is
**1.025170** (2.52% slower); paired GNU aggregates are **2.896247× for314** and
2.822171× for main. Bytecode-to-native is the largest raw regression (+20.30%);
native-to-interpreted improves 6.94%. Every sample and slower case remains.
Both revisions include the source312 native-GC completion repair. The baseline
still retains unreachable symbol fields and copies plists on `put`, so these
diagnostics preserve the cost of changed correctness and allocation behavior.
They are neither calibrated nine-round acceptance nor evidence of equivalent
allocation accounting; real counters remain unavailable.

The archive preserves the initial compiler failure, intermediate warnings,
baseline mismatches, a **17-pass/one-failure** focused run, and a full-gate attempt
that stopped after **384 passes/two startup failures**, leaving 2,678 library
tests and both Cargo stages unstarted. The later 24-control cohort includes
both original startup failures unchanged. An initial replay audit also rejected
tar's 0664 modes; the corrected replay uses Git's normal checkout modes and
verifies every byte and mode. None of these earlier failures is relabeled a pass.

Reproduce from the packaged patch or this checkpoint's source, using the recorded
GNU source/configuration and native ABI. Run the goal's unchanged strict checks
and `python3 tools/serial_grouped_gate.py --scope full`; build release separately.
For ordinary execution, build `emaxx` and `make-fingerprint`, then run
`tools/build-image.sh` on that executable. The archive contains exact commands,
environments, fixture bytes, outputs, identities and the replay auditor.

Real allocation accounting/counters, remaining adapters, intervals and pure
storage, internal ownership review, final platform validation and the locked
16-workload/nine-round/every-workload 3% performance criterion remain required.
