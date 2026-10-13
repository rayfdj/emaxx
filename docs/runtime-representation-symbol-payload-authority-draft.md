# Actual symbol identity continuation — 13 October 2026

The current task continuation is object-state draft17, with 584 exact input
bytes/modes reproduced from source324. Its [portable implementation](handover/2026-10-09-symbol-fields/symbol-object-state-draft17-results-manifest.json)
passes strict checks without warnings, all 407 affected controls in each profile
and 44 ordinary GNU comparisons / 88 successful processes. The original
40-predicate constant/store probe now agrees with GNU in interpreted, bytecode
and native execution. The 53 alias/default predicates, image copy/restore and
native boolean cell/plist controls pass too. All previous control names remain.

Actual object constant/C-slot state replaces name-derived restrictions;
assignment/default/let and alias creation follow their distinct GNU C entry
semantics. Native boolean value and property reads retain the owner objects.
The [draft15 native failure and earlier attempts](handover/2026-10-09-symbol-fields/symbol-object-state-drafts7-15-results-manifest.json)
remain preserved. The complete macOS Rust gate is running; clean platform,
frozen, terminal and performance validation remain pending. See the current
[handover](../HANDOVER.md) before changing either checkout. Main still carries
source324; this continuation is not completion of the full goal. The header
remains 64 bytes and is still charged only 48; remaining name/forwarding adapters,
public ownership/reclamation and other requirements below stay open.

## Published source324 and historical membership draft6

The published runtime is **source324 `99936262`**, with 572 inputs, on
[PR85](https://github.com/rayfdj/emaxx/pull/85), merged into main as `758d2138`
after PR84's `01189759`. Full Linux frozen matches all 7,928 outcomes; complete
Rust checks pass 3,190 on Linux and 3,178 on macOS, each with two existing ignores and exact
native artifacts. The original macOS frozen attempt matches 4,479 outcomes before
both Tramp processes time out. A separate 600-second observation obtains all
59 actual matching answers. The exact tail adds 3,377 matches and six unchanged
feature-diagnostic differences, covering all 519 files across separate attempts.
All 226 original terminal scenarios / 686 comparisons now pass, including
independent full-screen/attribute/cursor checks. Validation is closed with those
macOS qualifications; the original frozen verdict is still failed.
The [current handover](../HANDOVER.md) links the portable evidence and commands.

Three unchanged Linux pilots verify all 192 actual answers/modes. Source324 is
0.593% faster than previous main `01189759` overall, at 2.725557× paired GNU,
with an explicit-GC regression of 10.54%. The allocation and exact locked-GC profiles narrow the work
to investigate but do not explain that regression. The 64-byte header still has
an incorrect 48-byte allocation charge. No final performance acceptance exists.

The next **unapplied membership draft6** is retained in
`target/runtime-goal/recovered-2026-10-13/symbol-membership/emaxx`.
Its [portable cumulative patch](handover/2026-10-09-symbol-fields/symbol-membership-draft6-ownership-results-manifest.json)
replays all 580 inputs from `99936262`, with strict zero-warning checks and
403 selected passes in each of gate and release. Standard-obarray removal now
changes actual object membership, including preserved builtin nil/t identity,
printing, image copying and detached lifetime coverage. The process-global
captured Unicode menu table leaked the previous editor's graph into image roots;
the table now belongs to each editor and follows its marking/copying/dump path.
GNU's `keymap.c` has one process-owned table; a Rust process hosting multiple
editors needs the table under each editor's ownership. This adds one value word
per editor and removes the global optional table, with no measured net cost claim.

Draft5's three image failures, the exact startup-order reproductions and draft6's
formatting failure remain archived. Fresh same-name constant/store behavior is
still wrong (GNU's valid probe has 40 true predicates; production has 21 false
predicates). The [draft6 ordinary continuation](handover/2026-10-09-symbol-fields/symbol-membership-draft6-ordinary-results-manifest.json)
passes 38 comparisons / 76 processes in actual modes, preserving all 28 earlier
comparisons and adding ten membership cases. The unchanged constant/store probe
still has 20 false predicates in draft6 against GNU's 40 true predicates.
Complete library/platform certification remains open. Continue against actual
GNU constant initialization and `intern_sym` rather than
name-based exceptions. This draft must remain separate until repaired and
validated. Host-name and forwarded adapters, reclamation, public ownership,
physical accounting, real counters and the full goal remain unfinished.

## Historical applied identity draft5 and integration evidence

The original **identity draft5 prototype** is retained unchanged in
`target/runtime-goal/recovered-2026-10-12/symbol-identity/emaxx`.
Its [portable patches and raw results](handover/2026-10-09-symbol-fields/allocated-symbol-identity-draft5-results-manifest.json)
replay all **572 source inputs** from `265d5ba3`. Strict checks have zero warnings;
**397 selected controls pass in gate and release**. The complete local macOS Rust
gate passes **3,178 tests / two existing ignores**, including original native
artifact identity. **28 ordinary comparisons / 56 processes** match GNU, with
actual interpreted, bytecode and native execution modes verified. This dirty
prototype is not a clean published platform gate; Linux, frozen, terminal and
performance evidence remain pending for its runtime changes.

The [integration patch](handover/2026-10-09-symbol-fields/allocated-symbol-identity-integration-results-manifest.json)
replays all 572 inputs from source323 `0550a70c`. Production is byte/mode-identical
to validated draft5; the sole input difference retains source323's configured-GNU
native fixture. Fresh strict checks and **all 39 original integration tests**
pass, with all native byte assertions unchanged. Its isolated checkout is
`target/runtime-goal/recovered-2026-10-13/symbol-fields-integration/emaxx`.
The delta is applied after PR84's checkpoint merge, preserving the current
handover and evidence. Run the required checks on the clean published source.

GNU `lisp.h:Lisp_Symbol`, `data.c` and `alloc.c` remain the architectural reference.
Value/alias, function, plist and flags belong to the allocated symbol. Actual
obarray entries replace dense/sparse metadata and four field-history vectors;
the reader resolves atoms in the selected obarray before allocating. Actual
symbol identity replaces numeric IDs and both atomic ID counters. Local binding
keys are marked directly. Editor-owned keyboard references replace the global
ID cache. The 64-byte header and fourteen per-editor references (112 bytes) do
not establish net memory or speed improvement.

All 15 earlier failures remain in the evidence. Fourteen invalid fixture setups
now select the correct editor before comparing symbols or constructing calls;
their original value, native, lifetime and escaped-frame reclamation assertions
remain. The real Eshell regression came from `intern-soft` calling a function
lookup that created missing canonical symbols. The next lookup then found an
unbound qualified name and suppressed the real completion fallback. GNU
`lread.c:Fintern_soft` only probes the obarray. Draft5 removes the fallback;
the original Eshell test and new ordinary/keyword/Unicode miss controls pass.

A separate real bug copied the input string before calling programmed completion.
GNU `minibuf.c` passes the original Lisp string. Draft4/5 now preserve identity,
properties, encoding and callback mutations in all three completion primitives.
Nine GNU-backed cases cover ASCII, multibyte and unibyte strings. The same defect
was reproduced in source323 and is repaired in source324's integration delta.
This repair alone did not fix Eshell; that failed draft4 run
remains recorded alongside draft3's original Eshell failure.

Host strings and weak name bookkeeping, encoded private names, standard-obarray
membership tombstones, localized/forwarded and function-definition adapters,
descriptor reclamation and public ownership remain unfinished. The existing
48-byte allocation charge still undercounts the 64-byte header. Physical
allocation/public counters, profiles, final adversarial review, complete final
platform validation and the unchanged performance criterion remain required.
The full goal is active and incomplete.

## Historical identity draft2 — 12 October 2026

The latest **unapplied prototype is identity draft2** in
`target/runtime-goal/recovered-2026-10-12/symbol-identity/emaxx`.
Its [portable cumulative patches and evidence](handover/2026-10-09-symbol-fields/allocated-symbol-identity-unfinished-results-manifest.json)
replay all 568 inputs from `265d5ba3`. Strict checks have zero warnings;
**380 selected controls pass**, with all earlier inventory names retained.
Release, ordinary, complete platform validation and performance remain pending.

The allocated symbol no longer stores a numeric ID, and both symbol-ID atomic
counters are removed. Local bindings, alias-cycle checks, watchers and dump keys
use actual object identity. Local binding keys are marked directly, without a
duplicate symbol payload or an uninterned-ID test; existing binding order is
preserved. A spare state bit records uninterned construction. Standard-obarray
membership tombstones and the weak host-name book remain separate limitations.

The old process-wide keyboard ID cache fails when a second editor has its own
symbols. The new negative retains that failure, alongside the old 72-byte header
failing the unchanged 64-byte expectation. Keyboard variables now retain fourteen
Qsymbols in their own editor; the new owner test passes. The header is **64 bytes**,
but the per-editor references add 112 bytes, and physical accounting still uses
older estimates. No net memory or performance improvement is claimed. The first
repair's compile errors remain recorded.

The preceding [allocated-field draft16](handover/2026-10-09-symbol-fields/allocated-symbol-draft16-results-manifest.json)
passes 379 controls per profile and 21 ordinary GNU comparisons in real execution
modes. Its two prior fixture repairs keep symbol comparisons inside one editor
and compare every native name/arity without imposing an unsupported obarray
iteration order. Actual GNU observations support both contracts.

Draft16's full attempt fails after 689 passes/two failures. A separate continuation
executes all eight unstarted library groups: the combined inventory has **3,061
passes, 15 failures and two existing ignores across 3,078 names**. No Cargo stage
runs. Every failure is retained, including cconv/backquote/EIEIO and locale owner
comparisons, undo, bytecode switch, native calls, Eshell completion and escaped
frame reclamation. The ID repair does **not** fix the original Eshell assertion;
it fails in both before/after observations. These drafts must not be applied as
validated runtimes. Resolve the observed failures against GNU and the ownership
contract without weakening equality, dropping assertions or rewriting verdicts.

Remaining host-name adapters, membership, localized/forwarded fields, descriptor
reclamation, public ownership, physical allocation accounting, profiles, complete
validation and the full performance criterion remain open. The 64-byte layout
and selected controls do not complete the architecture or the full goal.

## Historical obarray-owned enumeration continuation

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
