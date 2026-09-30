# Direct call metadata and combined validation — 30 September 2026

Source174 combines the independent mutable TLS reports from source172 with
source173's evaluator repair. `eval_call` classifies each subr once and reads
its actual native descriptor for arity and UNEVALLED dispatch, following
GNU `eval.c:eval_sub`. It removes the copied optional `NameFacts` payload and
the second function-value decode before a special form. The original Linux
executable's debugger evidence locates a retaining stack word in that old
metadata spill: a one-byte store left stale pointer bytes in `eval_call`.
The unchanged survival and eventual-reclamation assertions remain intact.

All **345 compiled/test inputs** match the isolated combined checkout.
Compiler, formatting, warnings-denied Clippy and diff checks pass. **395 gate
and 395 release controls** pass, including both original lexical-thread and
suspended-bytecode contracts. All **12 ordinary exact GNU comparisons** pass:
ten evaluator/reader/unwind fixtures, the original TLS collection fixture and
the independent mutable report fixture. The
[portable receipts](handover/2026-09-30-combined-call-repair/manifest.json)
verify raw verdicts, complete selected inventories, process exits, source
hashes and executable/image identities. They also retain the ten completed
ordinary comparisons for the isolated source173 draft.

Complete source174 macOS Rust and terminal runs have started. Complete Linux
Rust and frozen compatibility remain required. This checkpoint is not yet
ready for main and does not establish a stable reclamation repair by itself.
The unchanged source170 macOS binary previously passed a standalone replay
after failing the complete gate; that limitation remains relevant.

Two completed predecessor Linux replays are also audited in this archive:

- Source170 [Eglot replay](https://github.com/rayfdj/emaxx/actions/runs/36669998865):
  all 52 outcomes match, with 45 passes and seven existing skips per editor.
- Source172 [TLS/network-stream validation](https://github.com/rayfdj/emaxx/actions/runs/36670346867):
  both Rust TLS controls pass and all 27 upstream outcomes match, with 26
  passes and one existing skip per editor. The existing affected workflow
  does not retain the Rust libtest executable hash; this remains an evidence
  limitation.

The successful Eglot replay does not change the original source170 full frozen
comparison's failed result. All earlier reclamation failures, TLS mutation
failures and unsuccessful helper invocations remain in their linked archives.
No existing assertion, selector, expected outcome, ignore or timeout changed
in source174. Darwin GNU matches the source and native ABI but differs from
the frozen executable pin; no pin changed.

Input decoding remains an isolated unfinished draft. Post-deinit TLS behavior,
symbol/object authority, physical allocation accounting, complete final-source
validation, the final adversarial audit and the locked performance criterion
remain open. This checkpoint makes no speedup or full-goal completion claim.
