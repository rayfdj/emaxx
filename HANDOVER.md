# Resume the compact runtime goal here

**Development continuation:** the callable reader now consumes bounded input,
creates authoritative positioned symbols, preserves callback/encoding semantics
and fixes marker-relative positions. Printer byte output and buffer hooks are
repaired too. Read the
[latest reader continuation](docs/runtime-representation-reader-checkpoint.md)
for 359 selected release passes, 19 ordinary exact comparisons, preserved prior
failures and remaining requirements. The earlier full gate failed at native
artifact identity after its library and binary stages passed. Its local GNU
ABI configuration differs from the committed target. The approved older Linux
run also failed. Neither result is complete final-source validation.
The integrated [GC continuation](docs/runtime-representation-gc-checkpoint.md)
adds migrated-kind verifier coverage and removes inactive error payload storage
that retained a dead thread key. Its separate checkpoint passes 186 debug and
209 release controls plus cached-image and positioned-error checks. The combined source plus the [weak symbol ownership repair](docs/runtime-representation-symbol-ownership-checkpoint.md)
passes 189 selected debug and 389 release tests, three public-runtime integration
tests, all strict static checks and nineteen ordinary exact comparisons. The weak
uninterned-symbol book now follows its process-wide heap across serialized host
entries. Full final-source validation remains required; function-cell duplicate
payload removal is the next architectural step.
Do not reapply the old patch on this branch. The full goal remains incomplete.

**Active goal:** implement one compact, authoritative Lisp object representation
shared by interpreter, bytecode VM and native code, then reduce instruction/call
cost toward GNU C performance. Preserve GNU semantics, complete the adversarial
de-cheating audit, and require clean rustfmt/compiler/Clippy and full validation.
The goal is **not complete**.

Read these in order:

1. [Complete goal and completion requirements](docs/runtime-representation-goal.md).
2. [Host-thread symbol ownership and combined-source validation](docs/runtime-representation-symbol-ownership-checkpoint.md).
3. [Bounded reader, printing repair and completed gate failures](docs/runtime-representation-reader-checkpoint.md).
4. [Cons-edge checker, evaluator stack repair and saved-image evidence](docs/runtime-representation-gc-checkpoint.md).
5. [Authoritative positioned symbols and current failures](docs/runtime-representation-positioned-symbol-checkpoint.md).
6. [Authoritative cons payload and failed full gate](docs/runtime-representation-cons-checkpoint.md).
7. [Allocated frames and earlier cons failure](docs/runtime-representation-frame-checkpoint.md).
8. [Ordering repair and completed full-gate failure](docs/runtime-representation-ordering-checkpoint.md).
9. [Input repair and completed older terminal results](docs/runtime-representation-input-checkpoint.md).
10. [Keymap continuation and pending full validation](docs/runtime-representation-keymap-checkpoint.md).
11. [Char-table implementation and earlier evidence](docs/runtime-representation-char-table-checkpoint.md).
12. [27 September handover: original baseline and goal-wide obligations](docs/runtime-representation-handover.md).
13. [Original portable draft/evidence manifest](docs/handover/2026-09-27/manifest.json).

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
