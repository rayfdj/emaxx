# Resume the compact runtime goal here

**Newest task-branch checkpoint:** read the [shared hash-table allocation](docs/runtime-representation-hash-allocation-checkpoint.md)
after the complete goal. Source 133 replaces the host record and four side maps
with one GNU-compatible allocation and owned arrays shared by all execution
engines, GC and image loading. Both complete gates exposed an obsolete dump
counter assertion; source 135 requires exact restoration of every remembered
scalar. Both fresh gates then exposed missing unordered hash-table comparison
handling. Source 139 repairs that production dispatch without changing the test:
154 debug and 154 release controls, strict checks and nine ordinary exact GNU
comparisons pass. Complete macOS and [Linux gates](https://github.com/rayfdj/emaxx/actions/runs/36530994726)
are running on PR #76, integration `75b69738`. The
[original failures](docs/handover/2026-09-29-hash-gate-repair/manifest.json) and
[ordering repair evidence](docs/handover/2026-09-29-hash-ordering-repair/manifest.json)
remain preserved. This checkpoint is not merged yet; the full goal remains open.

**Current task-branch follow-up:** the [native-call and window traversal checkpoint](docs/runtime-representation-call-window-checkpoint.md)
combines two measured ordinary-path improvements, with 232 debug and 232 release
passes, strict static checks and all 16 workload result/mode checks. Complete
gates for this newer source now pass: 2,993 tests on macOS and 3,005 on Linux,
with two existing ignores each. PR #75 integrates this follow-up at `49d0c25b`.
Its fresh full terminal run passes all 223 scenarios against source-matched GNU.
The Linux frozen comparison fails after 500 of 519 files: two differ, then the
keymap report's invalid UTF-8 stops parsing; 18 later files never run. Complete
raw evidence and limits are linked from the checkpoint. The previous checkpoint is already
merged into `main` at `bf483660` through PR #74. Full goal completion remains open.

**Newest continuation:** read the [complete Rust gates and main checkpoint](docs/runtime-representation-main-candidate.md)
after the complete goal. Source 115 now passes the complete macOS gate with
2,993 passes and the complete Linux gate with 3,005 passes; each retains the
two existing terminal-test ignores. Native artifact identity and the original
GC contracts pass. Earlier Linux reclamation failures remain preserved and
their cause is not established. Full pinned compatibility, allocation accounting,
the final audit and performance parity remain unfinished. The
[portable evidence](docs/handover/2026-09-29-main-candidate/manifest.json)
records the complete results. The full goal remains open.

The [regexp and VM checkpoint](docs/runtime-representation-regexp-checkpoint.md)
also records 152 debug and 152 release passes, strict static checks, four
ordinary GNU comparisons and all 11 original package installation checkpoints.
All 16 diagnostic workloads match results and execution modes but remain
substantially slower than GNU. Its earlier failures and measurements remain
in their original portable bundle.

**Previous results:** source 109 completes the entire macOS gate: 2,890 library
passes, two existing ignores, 60 binary passes and 39 integration passes,
including unchanged GNU native artifact identity. Linux completes with 2,174
passes and one suspended-bytecode weak-key reclamation failure; its later
groups and Cargo stages remain unexecuted. The [hash semantics continuation](docs/runtime-representation-hash-checkpoint.md)
repairs copying of mutated keys and capture of custom test functions. Its
three ordinary GNU comparisons pass, while the separately preserved native
hash-layout probe still fails. Read that continuation after the complete goal
and combined-source checkpoint. The full goal remains open.

**Latest continuation:** read the
[combined records, GC repair and profiling checkpoint](docs/runtime-representation-integration-checkpoint.md)
after the complete goal. Inline records are combined with the function-cell and
text-conversion repairs. A release-only eager-expansion GC failure is reproduced
against ordinary GNU and repaired; pending Lisp forms now remain reachable.
Source 108 passes 147 selected debug and 663 release tests, strict static checks
and ordinary comparisons. Full macOS and Linux validation subsequently failed;
the [completed full-run evidence](docs/runtime-representation-integration-checkpoint.md#completed-full-runs)
records their failures and unexecuted stages. The first 186 terminal scenarios
match; scenario 187 fails startup, leaving the run incomplete. The regexp
profile identifies and removes unnecessary scalar hashing, while measured pilot
Lisp bodies remain several times slower than GNU. Full goal completion is open.

**Earlier checkpoints:** the [function-cell checkpoint](docs/runtime-representation-function-cell-checkpoint.md)
removes two duplicate function payload tables and their name-position index.
Lookup, static tracing, cycle validation and image cloning use one existing
symbol cell. Source 89 passes 322 selected debug and 447 release tests, all strict
static checks, and 22 ordinary exact GNU comparisons. Allocated-symbol authority
and final-source validation remain incomplete. Generic inline records are now combined in the latest continuation above.

The callable reader now consumes bounded input,
creates authoritative positioned symbols, preserves callback/encoding semantics
and fixes marker-relative positions. Printer byte output and buffer hooks are
repaired too. Read the
[latest reader continuation](docs/runtime-representation-reader-checkpoint.md)
for 359 selected release passes, 19 ordinary exact comparisons, preserved prior
failures and remaining requirements. The earlier full gate failed at native
artifact identity after its library and binary stages passed. Its local GNU
ABI configuration differs from the committed target. The separately built pristine
GNU candidate now matches that configuration, and all nine unchanged native
artifact fixtures pass on the function-cell source; see the
[native configuration checkpoint](docs/runtime-representation-native-configuration-checkpoint.md).
Its executable still differs from the frozen pin. The approved older Linux run
also failed; the new [Linux run on c3204ce5](https://github.com/rayfdj/emaxx/actions/runs/36381574831)
finishes with 999 passes and one failure in the first three groups:
`set-text-conversion-style` still has no dispatch. The char-table key-range
failure is repaired. Later groups and Cargo stages did not execute; the
[raw Linux evidence](docs/handover/2026-09-28-linux-function-cells/manifest.json)
retains the failed result. None of these results is complete final-source validation.
The integrated [GC continuation](docs/runtime-representation-gc-checkpoint.md)
adds migrated-kind verifier coverage and removes inactive error payload storage
that retained a dead thread key. Its separate checkpoint passes 186 debug and
209 release controls plus cached-image and positioned-error checks. The combined source plus the [weak symbol ownership repair](docs/runtime-representation-symbol-ownership-checkpoint.md)
passes 189 selected debug and 389 release tests, three public-runtime integration
tests, all strict static checks and nineteen ordinary exact comparisons. The weak
uninterned-symbol book now follows its process-wide heap across serialized host
entries. Full final-source validation remains required. The newer function-cell
checkpoint preserves those repairs and its own failed baseline controls.
Do not reapply the old patch on this branch. The full goal remains incomplete.

**Active goal:** implement one compact, authoritative Lisp object representation
shared by interpreter, bytecode VM and native code, then reduce instruction/call
cost toward GNU C performance. Preserve GNU semantics, complete the adversarial
de-cheating audit, and require clean rustfmt/compiler/Clippy and full validation.
The goal is **not complete**.

Read these in order:

1. [Complete goal and completion requirements](docs/runtime-representation-goal.md).
   Then read the [shared hash-table allocation checkpoint](docs/runtime-representation-hash-allocation-checkpoint.md).
   Then read the [current task-branch follow-up](docs/runtime-representation-call-window-checkpoint.md).
   Then read the [complete Rust gates and main checkpoint](docs/runtime-representation-main-candidate.md).
   Then read the [newest regexp, VM and completed Linux continuation](docs/runtime-representation-regexp-checkpoint.md).
2. [Latest combined-source continuation and profiling limits](docs/runtime-representation-integration-checkpoint.md).
   Then read the [completed validation and hash semantics continuation](docs/runtime-representation-hash-checkpoint.md).
3. [Inline generic records and preserved failures](docs/runtime-representation-record-checkpoint.md).
4. [Text-conversion repair and Linux continuation](docs/runtime-representation-text-conversion-checkpoint.md).
5. [Function-cell payload consolidation and selected validation](docs/runtime-representation-function-cell-checkpoint.md).
6. [Host-thread symbol ownership and combined-source validation](docs/runtime-representation-symbol-ownership-checkpoint.md).
7. [Bounded reader, printing repair and completed gate failures](docs/runtime-representation-reader-checkpoint.md).
8. [Cons-edge checker, evaluator stack repair and saved-image evidence](docs/runtime-representation-gc-checkpoint.md).
9. [Authoritative positioned symbols and current failures](docs/runtime-representation-positioned-symbol-checkpoint.md).
10. [Authoritative cons payload and failed full gate](docs/runtime-representation-cons-checkpoint.md).
11. [Allocated frames and earlier cons failure](docs/runtime-representation-frame-checkpoint.md).
12. [Ordering repair and completed full-gate failure](docs/runtime-representation-ordering-checkpoint.md).
13. [Input repair and completed older terminal results](docs/runtime-representation-input-checkpoint.md).
14. [Keymap continuation and pending full validation](docs/runtime-representation-keymap-checkpoint.md).
15. [Char-table implementation and earlier evidence](docs/runtime-representation-char-table-checkpoint.md).
16. [27 September handover: original baseline and goal-wide obligations](docs/runtime-representation-handover.md).
17. [Original portable draft/evidence manifest](docs/handover/2026-09-27/manifest.json).

The original main checkpoint packaged its unfinished work as
[char-table-wip.patch](docs/handover/2026-09-27/char-table-wip.patch). That patch is
historical evidence on this branch: its allocator is now wired and the migration
is under validation. Commit `803e4326a16b7bfe13bb0b757ee85c8b02ac3998` contains
the first implementation. Rebuild on another machine; do not claim this host's
executable/image identity or test results as local.

Suggested instruction for the next instance:

> Read HANDOVER.md and the complete linked goal. Continue from the documented
> state without reapplying the historical patch. Preserve failures and all
> validation requirements. Use the repository's existing CI for Linux. The goal
> remains active; a milestone or a handover is not completion.
