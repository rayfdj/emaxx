# Buffer and marker ordering continuation — 28 September 2026

The [complete goal](runtime-representation-goal.md) remains incomplete. This
continues the [input checkpoint](runtime-representation-input-checkpoint.md).
It repairs the full gate's invalid local ordering fixture and a separately
reproduced ordinary-runtime ordering error. Software snapshot: source 48.

[The 71 portable receipts](handover/2026-09-28-ordering/manifest.json)
include commands, source manifests, patches, complete raw logs and failures.

## Completed full gate and the fixture correction

Source 36's unmodified full serial gate finished with **2,214 passes and one
failure**, exit 1, 3,513.43 seconds. All five evaluation groups and all 526
primitive tests passed. The compatibility group had 83 passes and one failure;
the later TTY, batch and lightweight groups did not run. The clean commit
`f69e1f0dade9ddceff294f465be663ae6618be9f` and its source hashes stayed unchanged.
This is a failed full gate, not a pass for source 48 or the frame draft.

The failing local `value_less_selected_upstream_ordered_cases_match_emacs`
fixture looked up an already killed buffer in the live-buffer index. It also
omitted the twenty characters that GNU's `test/src/fns-tests.el` inserts before
placing markers at positions 12 and 13; empty buffers correctly clamp both
positions to 1. The corrected fixture retains the killed buffer object and
inserts the original GNU text. Every case, comparison direction, vector tie
break, expected outcome and selector is preserved. Source 47 passes this
corrected fixture. The pristine local GNU passes its unchanged upstream
`fns-value<-ordered` test.

## Runtime repair and adversarial evidence

Inspection found that ordinary `value<` used interpreter-local buffer IDs for
ordering. It now reads the two actual buffer objects: current names determine
live-buffer order, killed buffers sort first, and two killed buffers compare
equal. Marker comparison reads actual buffer links, sorts detached markers
before attached ones, then compares buffer names and character positions.
This follows `fns.c:value_cmp`'s `PVEC_BUFFER` and `PVEC_MARKER` cases. It removes
live-buffer-index dependence from these comparisons and observes renaming.

The checked-in `tests/fixtures/runtime-value-ordering.el` runs the same Lisp
expression in GNU and Emaxx. It reverses creation/name order, renames a buffer,
compares attached and detached markers, kills both buffers and checks vector
tie breaks. GNU returns
`((nil nil t) (t t nil) (t t) (t nil t t nil t))`.
The unchanged source-39 ordinary executable instead returns
`((nil nil t) (t nil nil) (t t) (nil t nil nil nil t))`.
Source 47's new regression fails; source 48 passes it. A separate ownership
case uses distinct buffers with colliding interpreter-local IDs and verifies
that comparison observes their current names.

Source 48 passes seven selected debug ordering/arity tests, with zero ignores,
and clean rustfmt, all-target/all-feature compiler checks, strict Clippy and
diff checks. All 86 release compatibility tests pass (zero ignores), including the full
compatibility group that stopped source 36. This selected run does not replace
the failed full gate.
Source 46's fixture-method compilation mistake is preserved with the failing
source-47 comparison and the complete source-36 gate. No test expectation,
ignore, timeout, oracle pin, locked workload or performance tolerance changed.

## Remaining work

The separate frame migration has selected debug evidence but is unfinished and
is not part of source 48. The older source-25 terminal run's 16 divergences and
all goal-wide representation, ownership, audit, profiling and final-validation
requirements remain open. Local GNU is source-matched but has the previously
documented executable-hash mismatch with the Darwin lock. No frozen parity or
performance claim follows from these diagnostics. No push was performed;
Linux CI/publication still requires the pending explicit approval after the
previous automatic approval rejection.
