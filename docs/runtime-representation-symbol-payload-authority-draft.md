# Obarray-owned enumeration continuation — 12 October 2026

The latest **unapplied prototype is draft15**, with [portable patches, original
failures and raw evidence](handover/2026-10-09-symbol-fields/allocated-symbol-obarray-unfinished-results-manifest.json).
Its checkout is `target/runtime-goal/recovered-2026-10-10/symbol-fields/emaxx`.
All **568 input bytes/modes** replay from `265d5ba3`; strict checks have zero
warnings. **377 of 379 selected controls pass**; the full inventory preserves
all earlier names. Release, ordinary and full platform checks have not run for
this draft. Do not apply it as a validated runtime.

GNU `lread.c:map_obarray` enumerates the obarray, independently of field contents.
The old prototype's first-binding/definition histories reorder `mapatoms` after
field changes; identical ordinary programs return eight true predicates in GNU
and one false/seven true in the old Emaxx prototype. Draft15 removes the dense
and sparse per-interpreter metadata tables, all four history vectors and their
GC cleanup. Value/function/plist/alias stores update only the allocated object.
The remaining per-interpreter `SymbolCells` holds an owner and its Qsymbol
reference. No per-symbol side metadata remains, but the 72-byte allocated header
still has host-name/id and localized/forwarded adapters. Physical accounting and
whole-process memory/performance measurements remain unfinished.

Enumerating actual owner entries exposed a reader defect: a post-read rewrite
first created private-obarray names in the standard namespace. Text/encoded
reads now use the existing atom resolver; eval-buffer/region read each form in
the currently selected obarray before evaluating it. The new GNU observation
confirms private membership and standard non-membership for all four entries.
Both new obarray controls pass in draft15. A temporary bit in the existing state
byte preserves the startup distinction between Lisp definitions and defsubr;
it must disappear when its consumers use actual function capabilities. Alias
transitions retain independent classification bits; the existing union control
checks those transitions and all value/function/plist/GC edges.

The two remaining failures are
`eieio_persistence_recurses_through_container_values_with_expected_reader_policy`
and `dump_emacs_portable_restores_context_and_reports_native_image_limit`.
The first compares identical printed symbol values from different editor owners.
The second retains a native count of 3,865 but selects a different first function
from the reconstructed obarray. Preserve all serialization, native identity,
arity and lifetime requirements when resolving these; neither is waived or
counted as a pass. The earlier dump member check now captures the writer's
members at dump time, before diagnostic programs introduce additional names;
it still requires every captured member after restoration.

Draft12's compile errors, draft13's three failed controls, draft14's lifetime
compile error and draft15's two failed controls remain preserved, with each
cumulative patch replayed. The prior draft11 ordinary validation is now closed:
**30 successful processes / fifteen matching GNU comparisons** in real
interpreted/bytecode/native modes, with all four executable/image bytes retained.
Those earlier inputs do not validate draft15. Remaining ownership/name/id,
localized/forwarded, descriptor reclamation, allocation accounting, profiles,
full validation, final adversarial review and the unchanged performance criterion
remain open.

## Historical draft11 continuation

The latest **unapplied prototype is draft11**. Its
[portable patch and evidence](handover/2026-10-09-symbol-fields/allocated-symbol-authority-draft11-results-manifest.json)
replay all 566 inputs from main `265d5ba3`. Strict checks have zero warnings;
**237 selected controls pass in each profile**, with every older inventory name
preserved. The working checkout remains
`target/runtime-goal/recovered-2026-10-10/symbol-fields/emaxx`.

The new lifetime negative reproduced a weak key surviving its editor's retirement.
Weak survival now follows the actual symbol mark, matching GNU `alloc.c`.
The new control preserves parked-editor survival and requires removal after
retirement. This repair is separately validated and applied as source321 on the
task branch; the direct-field prototype remains separate.

The process-lifetime `NO_C_SLOT` thread-local ID hash set is removed. Its existing
negative name classification now occupies a spare bit in the allocated symbol's
state byte, removing TLS access and a hash probe from ordinary lexical binding
checks. The bit is a temporary adapter until dedicated C slots use the allocated
redirect; it adds no field. The header plus remaining per-symbol metadata still
totals 96 bytes. No speed or net memory improvement has been measured.

The original draft7 ordinary run now has an audited **15 matching GNU comparisons
/ 30 successful processes**, including actual bytecode/native modes and retained
executables/images. Those source inputs precede the retirement and C-slot changes;
they do not validate draft11. All failed attempts remain preserved. The existing
startup path fixture was corrected against the configured GNU root and actual
GNU environment-independence evidence, keeping its exact assertions.

Dense IDs, enumeration, weak metadata cleanup, localized/forwarded storage,
host text, descriptor reclamation, host-owner lifetime, physical allocation and
public counters remain open, along with full ordinary/platform/native/terminal
validation, profiles, final adversarial review and the unchanged performance
criterion. This checkpoint does not complete the architecture or the full goal.

## Historical draft7 continuation


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
