# Symbol fields — 9 October 2026

Source314 continues the full goal; **the architecture and performance goal remain
incomplete**. The [selected evidence](handover/2026-10-09-symbol-fields/source314-selected-manifest.json)
identifies every changed input and retains the earlier failures. Full macOS Rust
validation is running separately; Linux validation and timing are still pending
at this selected-evidence checkpoint.

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
access gains a new hash lookup, but this walk and the enlarged dense cell still
need cost measurement. No speedup or GNU parity is claimed.

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
