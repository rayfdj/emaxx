# Resume the compact runtime goal here

**Development continuation:** frames now use allocated object addresses, with
owned window/terminal/face references and no native bridge handles. Read the
[latest frame continuation](docs/runtime-representation-frame-checkpoint.md)
for selected validation, preserved failures and the pending 16-byte cons migration.
The ordering, input, char-table and keymap repairs are integrated too.
Do not reapply the old patch on this branch. The full goal remains incomplete.

**Active goal:** implement one compact, authoritative Lisp object representation
shared by interpreter, bytecode VM and native code, then reduce instruction/call
cost toward GNU C performance. Preserve GNU semantics, complete the adversarial
de-cheating audit, and require clean rustfmt/compiler/Clippy and full validation.
The goal is **not complete**.

Read these in order:

1. [Complete goal and completion requirements](docs/runtime-representation-goal.md).
2. [Allocated frames and remaining cons failure](docs/runtime-representation-frame-checkpoint.md).
3. [Ordering repair and completed full-gate failure](docs/runtime-representation-ordering-checkpoint.md).
4. [Input repair and completed older terminal results](docs/runtime-representation-input-checkpoint.md).
5. [Keymap continuation and pending full validation](docs/runtime-representation-keymap-checkpoint.md).
6. [Char-table implementation and earlier evidence](docs/runtime-representation-char-table-checkpoint.md).
7. [27 September handover: original baseline and goal-wide obligations](docs/runtime-representation-handover.md).
8. [Original portable draft/evidence manifest](docs/handover/2026-09-27/manifest.json).

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
