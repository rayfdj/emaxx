# Text-conversion buffer field — selected checkpoint

The [complete goal](runtime-representation-goal.md) remains incomplete. This
checkpoint follows `2b8b329f` in the reused `runtime-audit-followup` worktree.
Source 104 passes 54 selected debug and 214 release tests, all strict static
checks and four ordinary exact GNU comparisons. Its
[portable evidence](handover/2026-09-28-text-conversion/manifest.json) retains
the failures and commands. Combined-source and Linux validation remain required;
this is not a full gate, frozen validation or a performance result.

The Linux full Rust run on `c3204ce5` failed after 999 passes because the
unchanged `map_y_or_n_p_honors_a_temporarily_overridden_read_event` test reaches
an unknown `set-text-conversion-style`. Seven later library groups and the
subsequent Cargo stages did not execute. Its original failure and the earlier
803e4326 failure remain retained; fixing a primitive does not turn either run
into a pass.

GNU `textconv.c:Fset_text_conversion_style` writes the current buffer's Lisp
field through `bset_text_conversion_style`. With no installed graphical text
interface it then returns nil. It accepts arbitrary Lisp values and an optional
second argument, does not create a buffer-local binding, and does not notify
variable watchers. The draft exposes this primitive only on Linux, retaining
the platform attribute in ordinary dispatch generation.

The buffer now owns one `Value` and a separate local flag. Lisp reads, native
field writes, dynamic/default bindings and indirect-buffer cloning use that
field. There is no duplicate payload in the local-variable table. The existing
global value owns the C default while forwarded; the existing detached-slot
storage owns that default if `makunbound` disconnects the Lisp symbol. The C
field and its flag survive disconnection independently of a subsequently plain
or newly localized Lisp variable, as in GNU `data.c` and `buffer.c`.

`data.c:set_internal`, `set_default_internal`, `Fmake_local_variable` and
`Fkill_local_variable`, plus `eval.c:specbind`/unbinding, determine the writes,
watcher operations, default propagation and flag changes. In particular, a
nonlocal `let` saves the actual C field even if a preceding C write differs
from its default. Killing that local during a local `let` prevents restoration
unless a new local binding exists. `let_shadows_buffer_binding_p` excludes
`SPECPDL_LET_LOCAL`, as the GNU source explicitly requires.

The existing buffer marker visits the field; deep image cloning rewrites it;
the dump contains an ordinary strong relocation and the local flag. Killing the
buffer clears the field along with other editing storage. These are additions
to the current Rust buffer state, not a claim that its complete layout now
matches GNU's buffer. The additional field/flag storage and default propagation
are real costs. No speedup or measured total-memory saving is claimed.

The same-input ordinary GNU before-probes show preexisting failures in source
89: a killed local reappears during unwind, permanent-local Lisp metadata
incorrectly preserves this native field, watcher sequences differ, and
`makunbound` leaves a local/default redirect instead of disconnecting it. A
separate alias/indirect-buffer probe preserves incorrect return identities for
both make-local-variable and kill-local-variable. GNU source is pristine and
source-matched locally, but its binary does not match the frozen Darwin pin.

An attempted source-96 before-probe fails before Lisp evaluation because the
copied candidate-configured image resolves its native library relative to its
original location. That startup failure is retained and is not a semantic
comparison. The usable source-89 before comparisons are separate receipts.

Source 100 compiles and passes all strict static checks. Its focused debug run
finishes with 23 passes, one failure, zero ignores (222.10 seconds). All three
new GNU variable-state contracts, the direct C-field control, the unchanged
map-y-or-n-p test on macOS, and the extended buffer GC control pass. The GC test
retains every previous assertion and adds actual collection survival and
eventual reclamation of a cons reachable only from the new field.

The one failure is a new image-test setup assertion that assumes a created
buffer has no local cells, overlooking its existing default-directory cell.
It stops before testing the new field's clone/dump sharing and cycle checks.
The correction must test absence of a duplicate text-conversion cell while
preserving all graph and locality assertions. The ordinary release build and fresh image pass (206.55 and 29.36 seconds).
Three of four exact stdout/stderr comparisons pass; the new alias case still
returns the resolved symbol from kill-local-variable instead of the supplied
alias. Source 104 repairs that forwarded return and corrects the image-test
setup assertion. All 54 selected debug tests pass (732.66 seconds), as do
214 release tests (113.78 seconds), with zero ignores and all strict static
checks clean. A fresh ordinary build and image pass (226.25 and 29.31 seconds);
all four identical-input comparisons match GNU's stdout and stderr exactly.
Source and artifact identities remain unchanged. The debug inventory grows
from 2,871 to 2,877, with six additions and none removed. The release inventory
omits only the existing debug-only descriptor-drop panic control; no selector
was changed. Linux-only dispatch and its GNU comparison require the existing
Linux CI; a macOS pass cannot certify that platform-specific primitive.

Validate the combined source with the generic-record change, then run Linux CI. Preserve the failed
raw evidence, complete inventory and the full goal's remaining architecture,
GC accounting, ownership, audit, VM/call profiles, frozen compatibility,
terminal validation and locked sixteen-workload 3% performance requirements.

The source-104 ordinary executable SHA-256 is
`c3aa20eb634b38bb2cb31bec25312d0397e95248b28d7f06d2f33b7c5c2d971a`;
its fresh image is
`d692fd442b701a80826bd7bad5fef13d9d1b6266002f84234fb739c53d337bf2`.
