# Regexp validation and remaining Linux reclamation failure — 29 September 2026

The [complete goal](runtime-representation-goal.md) remains open. The
[portable receipts](handover/2026-09-29-regexp-vm/manifest.json) retain the
commands, source and artifact identities, raw successes and failures, startup
diagnostics and all 16 workload pilots described below. Earlier failures and
the unfinished hash allocation draft remain in the
[previous continuation](runtime-representation-hash-checkpoint.md).

## Validated regexp checkpoint

Commit `d4950cf44016d32c016839429e26caebc970758f` captures a regexp dependency
graph on a cache miss and compares its actual payloads in place on a hit.
This removes repeated graph reconstruction, visited-set hashing and signature
allocation from a warm lookup. GNU reads its canonical tables directly;
Emaxx's existing backend embeds translated classes and must therefore detect
both Lisp and native field stores. Cache limits and regexp semantics are
unchanged. This is derived cache data, not a second mutable Lisp object model.

Snapshots hold unrooted words. Every ancestor is recorded and checked before
its descendants, so an altered edge stops validation before a potentially
collected old child is read. Matching complete ancestor payloads establishes
current reachability even after address reuse. The check makes no Lisp calls
and cannot collect; it relies on the existing serialized runtime boundary.
The packaged review states that safety argument and its remaining audit limits.

Source 115 passes 152 selected debug and 152 release tests, zero ignored,
with clean formatting, all-target/all-feature compiler checks, Clippy with
warnings denied and diff checks. The new same-input GNU fixture covers shared
mutable syntax/category leaves, replaced edges and collection. Existing native
store tests are unchanged. The debug inventory adds one test and removes none.
Four ordinary exact GNU comparisons pass using a fresh executable and image;
all processes exit zero with empty stderr and unchanged source and inputs.

The original `package-menu-install-refresh-remove` terminal scenario now passes
all 11 checkpoints in 61.56 seconds, including the original six-second install
deadline. This is a focused result, not a complete terminal-suite pass.

## Preserved full-run failures and VM checkpoint

Source 108's second complete terminal attempt starts all 223 scenarios:
222 match and package installation fails at checkpoint 5, with 23 differences.
Later actions within that scenario are unexecuted. An unchanged focused replay
reproduces the failure; the installed menu appears after 20 additional seconds.
Neither observation relaxes the original deadline or clears that failed run.

The diagnostic's synchronous sampler stopped draining the PTY. Its 1,345
blocked-write samples are therefore an observer artifact and cannot establish
that blocked output caused the original failure. Of its 391 CPU samples,
package-menu rebuilding and repeated regexp table validation dominate. The
qualification, raw sample and terminal states are all retained.

Commit `c9ce1b2dc23cf20a400e3c2aa15d6afb6aebdb96` removes six error unbox/rebox
sites across the evaluator and bytecode VM, preserving the boxed errors and
rooting a thrown value across comparison. Source 114 passes 169 debug and 169
release controls, all strict static checks, the three hash semantic comparisons
and the unchanged ordinary suspended-bytecode program. ARM disassembly shows
the VM `run` frame shrinking from 752 to 736 bytes and its instruction-dispatch
closure from 1,072 to 1,008 bytes; these figures are not Linux root evidence or
a speed measurement.

The [complete Linux run on that commit](https://github.com/rayfdj/emaxx/actions/runs/36431453774)
still fails: 2,177 passes and one failure across the first six groups. The
original suspended-bytecode contract returns `((1 t 1) (1 t 0))` instead of
`((1 t 0) (1 t 0))`. Both live-root assertions pass, but the first dead weak key
remains. Four later library groups and all Cargo stages are unexecuted. The
tested executable SHA-256 is
`a31760e841592d5df48a97fe27d49aa8ba64ba38f9fc60a6fb7aba76e8b7b383`.
The VM change does not resolve this full-run failure.

On the older source, both the [isolated default-profile replay](https://github.com/rayfdj/emaxx/actions/runs/36427648384)
and [all 569 original primitives](https://github.com/rayfdj/emaxx/actions/runs/36427656115)
pass with the exact executable SHA-256 of that source's failed full run,
`5287effe3d3b10a7b7da10ec89117c030bb157373b3f2c32a006309a082bb873`.
These passes do not clear the full failure. Startup image history is a candidate
difference, not a proven cause.

Commit `441d830b25ead161670d96ef26a051d94d2db699` adds an optional separate
prelude inventory to the existing Linux replay tool and preserves actual fixture
images alongside the executable. Six new diagnostic controls and 13 existing
gate-validator controls pass, as do AST parsing, help, workflow shell syntax
and diff checks. The production runtime, original tests, gate profile, timeouts
and strict outcome checks are unchanged. This enables a direct startup-history
experiment without altering the failing assertion.

## Performance remains substantially behind GNU

Both sources 114 and 115 run every one of the original 16 locked workloads
through the ordinary editor entry points. All 32 processes in each pilot
complete with matching results and actual execution modes. Both editors compile
the unchanged native fixture to the same artifact SHA-256,
`3bab9142001722127d0f13b50d45801b1353b0c1e8ac05fe1242c9c934a0466e`.
These are single-sample diagnostic pilots with no warmup. Source 114 also ran
alongside a correctness build. Neither pilot establishes the repeated parity
criterion, and neither has usable Emaxx allocation counters.

Source 115's observed Emaxx/GNU body ratios range from 1.33 to 10.32. Interpreted
calls are 3.39, bytecode calls 3.12, native execution 3.88, bytecode-to-native
transitions 8.19, sorting 4.86 and upstream undo 10.32. Every unfavorable result
is retained. The regexp change does not establish core execution parity.

The pristine GNU source and native ABI match the recorded target, but its
executable still differs from the frozen Darwin pin. No oracle pin, expected
output, timeout, ignore, workload inventory or parity tolerance changed.

Continue the Linux reclamation diagnosis, actual authoritative hash allocation,
remaining representation and ownership work, truthful allocation accounting,
profile-directed ordinary execution improvements, and the final adversarial
audit. Complete macOS/Linux gates and full pinned compatibility on the final
source, plus the unchanged performance criterion, remain required. A checkpoint
or a merge is not completion of the goal.
