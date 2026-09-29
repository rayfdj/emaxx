# Native-call and window traversal follow-up — 29 September 2026

The complete source-115 checkpoint is merged into `main` through
[PR #74](https://github.com/rayfdj/emaxx/pull/74), merge commit
`bf483660ab77da29f08baf33307e2f8233b2be3c`. Its
[complete Rust gates](runtime-representation-main-candidate.md) pass on macOS
and Linux. The [full goal](runtime-representation-goal.md) remains open.

The task branch now combines two separately tested, profile-directed changes:

- Native function dispatch passes the existing symbol to the existing
  function-cell resolver. This removes text conversion and another interned-name
  lookup. It follows GNU `indirect_function`'s use of the symbol in hand; aliases,
  redefinition, private symbols and errors still use the same resolver. It does
  not complete the migration of function cells into allocated symbol payloads.
- Window configuration save/restore visits the existing window index and reads
  each authoritative record, instead of scanning every host pseudovector. GNU
  `window.c` visits windows. The temporary host-window implementation retains
  record order, deleted windows, frame filtering and all saved slots. No new
  index or mutation-maintenance path was introduced.

Combined source 119, commit `545353e1`, passes 232 selected debug and 232 release
tests, zero ignored, with the GC verifier enabled. Formatting, all-target and
all-feature compiler checking, warnings-denied Clippy and diff checks pass.
Both original suspended-root contracts are included. A fresh ordinary binary
and image run every one of the 16 locked workloads; all 32 processes complete
with exact results and actual execution modes. The validated source is
integrated on the task branch at `1c91576c5300c8a90a4629670b25d603e0c92f5a`,
with all 362 recorded source inputs unchanged. Its complete macOS gate is
running; full Linux validation has been dispatched separately. Neither is yet
reported as a pass.

Source-115 whole-process profiles contained 118 self samples in native symbol
interning, and the undo profile contained 265/253 self samples in window
configuration save/restore. After the window change, neither save nor restore
appears in the profiler's list of functions with at least five self samples.
The same ordinary workload still passes its result checks. These are sampled
profiles including startup and preparation, not exact instruction counts.

The combined diagnostic pilot observes body ratios of 3.44 for native execution,
5.59 for upstream undo and 8.62 for bytecode-to-native transitions. All 16 results,
including regressions, are retained. These are single observations alongside
other correctness work; they do not establish a controlled speedup or the
unchanged parity criterion. Allocation counters remain unavailable.

Commit `f0ea449c31e7db2f80f6c33e998cfd307144715a` adds post-run collection of the
full Linux gate's actual executable and remaining fixture images. The collector
verifies the executable against the gate's recorded hash, rejects changed or
missing artifacts and preserves the original outcome summary. It describes
images as present after the run, without claiming per-test usage. Eight new
artifact controls, six existing replay controls and 13 gate-validator controls
pass, together with AST parsing, workflow shell syntax and a real exercise on
five completed source-115 artifacts. This changes evidence retention only.

The [portable receipts](handover/2026-09-29-call-window/manifest.json) retain the
combined commands, source manifests, raw selected checks, every workload pilot
and artifact-tool controls. The earlier
[bundle](handover/2026-09-29-main-candidate/manifest.json) preserves each separate
patch and profile. Earlier Linux reclamation failures remain unexplained. The
new complete source-115 terminal comparison is still running; full final-source
pinned comparisons, shared hash allocation, remaining representation and
ownership work, accounting, the adversarial audit and performance parity remain
unfinished.
