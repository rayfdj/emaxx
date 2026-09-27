# Resume the compact runtime goal here

**Development continuation:** the char-table draft has been applied and wired on
`runtime-char-tables`. Read the [28 September continuation](docs/runtime-representation-char-table-checkpoint.md)
for implementation, failures, exact selected evidence and pending validation.
Do not reapply the old patch on this branch. The full goal remains incomplete.

**Active goal:** implement one compact, authoritative Lisp object representation
shared by interpreter, bytecode VM and native code, then reduce instruction/call
cost toward GNU C performance. Preserve GNU semantics, complete the adversarial
de-cheating audit, and require clean rustfmt/compiler/Clippy and full validation.
The goal is **not complete**.

Read these in order:

1. [Complete goal and completion requirements](docs/runtime-representation-goal.md).
2. [Current handover: exact state, evidence, next implementation steps and Git status](docs/runtime-representation-handover.md).
3. [Portable draft/evidence manifest](docs/handover/2026-09-27/manifest.json).

Everything needed to resume is committed on **main**. From a clean checkout on
main, `git pull --ff-only origin main` gets this handover, the goal, the verified
runtime checkpoint, and the unfinished char-table work. No other branch or
original-machine build directory is required.

The unfinished work is deliberately saved as
[char-table-wip.patch](docs/handover/2026-09-27/char-table-wip.patch), not silently
counted as validated production code. The continuing agent should check and apply
that patch to recover the exact three-file development state, then resume the
char-table migration described in the handover. It includes an unwired allocator
module and the preserved failing GNU comparison. Rebuild on the new machine;
do not claim another host's executable/image identity or test results as local.

Suggested instruction for the next instance:

> Read HANDOVER.md and the complete linked goal. Continue from the documented
> state, restoring the saved char-table draft first. Preserve failures and all
> validation requirements. Use the repository's existing CI for Linux. The goal
> remains active; a milestone or a handover is not completion.
