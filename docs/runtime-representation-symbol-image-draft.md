# Symbol ownership in private image copies — 10 October 2026

This is **unfinished, separately packaged source319 draft6**, not an applied
runtime change. Main `265d5ba3` contains source318 through merged PR82. The
[portable patch and selected evidence](handover/2026-10-09-symbol-fields/source319-symbol-image-draft-results-manifest.json)
continue the [editor ownership draft](runtime-representation-symbol-owner-draft.md).
The patch independently reproduces all **565 source inputs and modes** from
both source318 `93b6546f` and current main `265d5ba3`.

GNU `lread.c` owns canonical symbols through its process obarray; `lisp.h`
stores the name and mutable fields on the allocated symbol. Independent editor
images therefore need independent symbol objects before Emaxx can move the
remaining fields out of per-interpreter tables. The new negative control
demonstrated that a private image clone still returned the original symbol word.
Its name-string properties could consequently point into the other editor.

The private graph copier now allocates symbols in the clone's owner and records
each placeholder before copying its Lisp name. This preserves cycles through
name properties, shared name strings and references from property lists.
The name remains one Lisp word, stored through `Cell<Value>` so completing a
recursive copy does not require mutable references to aliased object storage.
The copier remaps symbol-cell keys, aliases, buffer locals, watchers and retained
symbol references. It invalidates the old symbol enumeration cache. Owner
selection occurs at private host fixture boundaries, not on individual VM
operations.

A second fixture executes seventeen identity predicates. The identical Lisp
inputs return seventeen true values through ordinary GNU batch startup. The
first valid-template copy failed predicates **5, 6, 9 and 16**: private symbol
keys in name/string/buffer properties retained old encoded names, and a custom
hash table retained address-derived hashes. The repair relocates these keys,
including saved buffer undo properties, and recomputes hashes after copying the
complete graph. A custom table invokes its actual hash callback. This is private
fixture copying; the production dumper still rejects user-defined hash tests,
as GNU `pdumper.c` does. The existing guard against cloning live native relocation
state remains intact.

Strict formatting, all-target/all-feature checking, Clippy and diff checks pass
with **zero warnings**. All **230 earlier selected controls and both new controls
pass in gate and release**. Complete inventories preserve every earlier test
name: 3,072 gate names and 3,071 release names. The new graph control checks
private symbols, function/value/plist cells, obarrays, hash descriptors and
lookups, aliases, lambda parameters, buffer-local values, property keys and
watchers after forced collection; changing the clone's property leaves the
source unchanged.

Two existing clone fixtures needed explicit independent-editor expectations.
The plist fixture selects its source owner before its unchanged host-side
inspection. The positioned-symbol fixture checks the clone's own canonical
symbol and additionally requires a different symbol word; all previous shared
wrapper, field-position and native-store isolation assertions remain. Their
original failed results remain preserved alongside the initial shared-symbol
failure, a rejected map-key mutation at compilation, and the graph fixture's
rejected live-native setup. The corrected graph fixture loads unchanged GNU
early Lisp owners; it does not bypass the native-state guard.

The first ordinary-validation wrapper referenced draft5's old source manifest.
Its preflight correctly rejected the changed source before building or executing
comparisons. That script and traceback remain in the package. The corrected
`target/runtime-goal/symbol-obarray-2026-10-09/continue_image2.py` uses the exact
draft6 manifest. The [completed ordinary evidence](handover/2026-10-09-symbol-fields/source319-symbol-image-ordinary-results-manifest.json)
verifies **24 successful processes / twelve matching comparisons**: every
previous comparison plus the new graph fixture in interpreted, bytecode and
native modes. Both editors receive identical arguments and use their own valid
images; actual compiler-mode predicates pass. The complete macOS Rust gate is
now running. Consult `image-draft6-continuation2-state.json` and raw receipts;
the complete gate is not certified by the selected and ordinary results.

Private `Interpreter::new` still inherits the selected owner. Value, function,
plist and alias fields still live in per-interpreter cells. Direct allocated
fields, removal of ID/enumeration/weak-field GC adapters, descriptor reclamation,
other process-wide Lisp roots, physical allocation accounting and real counters
remain required. Broader platform, frozen, terminal and native contracts are not
certified by these selected controls. No performance improvement is claimed;
source318's diagnostic remains 2.857181 times paired GNU. The
[complete goal](runtime-representation-goal.md), including calibrated nine-round,
every-workload performance acceptance, remains active and incomplete.
