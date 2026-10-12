# Allocated symbol payload continuation — 12 October 2026

The full goal remains active and incomplete. This is a **separately packaged,
unapplied prototype**, outside the source320 debugger repair on the task branch.
The [portable draft7 patch and evidence](handover/2026-10-09-symbol-fields/allocated-symbol-payload-unfinished-results-manifest.json)
replay all **566 runtime inputs and modes** from main `265d5ba3`. The working
checkout is `target/runtime-goal/recovered-2026-10-10/symbol-fields/emaxx`.

GNU `lisp.h:Lisp_Symbol`, `data.c` and `alloc.c` provide the reference: fields
belong to the allocated symbol and marking follows those fields. Private
constructors now select independent canonical owners before initialization.
Function, plist, value/alias payload and flags live in the allocated header.
Ordinary reads and bound value stores access those fields directly. The alias
and value share one word, with a checked discriminator and copied accesses.
Image copying relocates their actual graph, including cycles and alias targets.
The per-interpreter field-tracing pass and its GC fixed-point walk are removed.

Formatting, all-target/all-feature checking, Clippy and diff checks pass with
zero warnings. **235 focused controls pass in both gate and release**, retaining
every earlier selected control and inventory name. The inventories contain
3,075 gate names and 3,074 release names. The unchanged new field fixture returns
nine true predicates in ordinary GNU and the Rust control. Selected tests cover
weak-field fixed points, finalizers, replaced alias reclamation, parked editors,+suspended bytecode and lexical caller roots. This is not full platform or
ordinary native validation of the prototype.

All failed attempts remain in the package. The initial Clippy findings were
fixed without suppressions. Four older fixtures assumed shared symbol objects
across independent editors; they now preserve the same field, image and lifetime
checks using their actual owners. A later debugger-context negative exposed a
process-wide cached symbol ID. The repair keeps an owner-specific Qsymbol
reference and is separately validated for source320 on the task branch.

The first value/alias migration retained 233 passes and two failed controls.
Copying only the enumeration adapter no longer copied Lisp fields; the control
now copies the actual owning image, retains its original value/flag/alias
assertions and additionally requires distinct symbol identities. The old weak
table liveness predicate considers canonical symbols live regardless of the
individual traversal. The alias-edge control now observes the actual GC walk,
preserving both exclusion of replaced targets and all positive field edges.
Neither correction changes runtime answers or removes a test.

The allocated header is **72 bytes**, with **24 bytes** of remaining metadata
per populated symbol: their combined 96 bytes are unchanged. This is an
authority/direct-access change, not a demonstrated memory or speed improvement.
The old GNU-formula allocation charge is still not honest physical accounting.

Remaining requirements include removing dense IDs, enumeration and weak metadata
cleanup, reviewing localized/forwarded storage, dropping host-name duplicates,
reclaiming hash descriptors, and checking process-wide Lisp roots. In particular,
the canonical-symbol shortcut in `LispReachability::contains` still assumes
permanent canonical liveness; owner retirement and weak references need a
targeted review. The host-owner fallback and private selected-owner lifetime
also remain migration concerns. Full ordinary/native/platform validation,
physical counters, profiles, final adversarial review and the unchanged
16-workload/nine-round/every-case performance criterion remain open.
