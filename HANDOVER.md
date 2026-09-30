# Resume the full compact-runtime goal here

The goal is **active and incomplete**. Read the [complete goal and all completion
requirements](docs/runtime-representation-goal.md) before editing. A validated
checkpoint, green gate or handover does not complete it. Current user instructions
take precedence over historical notes.

Read the [current cons continuation](docs/runtime-representation-cons-roots-draft.md),
[allocator continuation](docs/runtime-representation-vector-allocation-draft.md),
[shared-reader handover](docs/runtime-representation-shared-reader-draft.md),
[bytecode draft](docs/runtime-representation-bytecode-draft.md) and
[accounting continuation](docs/runtime-representation-accounting-draft.md).
The [requirement map](docs/runtime-representation-progress.md) tracks the full scope.

## Current production and candidate

The latest validated main checkpoint is **source174**, merge `21d20f0e` in
[PR #78](https://github.com/rayfdj/emaxx/pull/78). Its
[complete validation record](docs/runtime-representation-call-validation-complete.md)
remains separate from the unfinished task branch.

The task branch is **runtime-char-tables**, [draft PR #79](https://github.com/rayfdj/emaxx/pull/79).
Runtime **source204** was published as `d186d40bf014ca53d0a2652b4e2f2d921c5cb009`.
The current **source207** adds direct evaluator words and a separate cold error
constructor after the exact Linux diagnosis below. Its **410 runtime and test
inputs** match the isolated candidate; only `src/lisp/eval/core.rs` changes from
runtime204. The candidate also includes the shared command reader, authoritative
keyboard-macro array/state, word-aligned vector allocation and direct cons fields
with GNU-compatible conservative cons-root validation.

## Source204 evidence and live work

- [Selected validation](docs/handover/2026-09-30-shared-reader-draft/source204-cons-selected-validation-manifest.json):
  zero-warning strict checks, **596 gate / 596 release tests**, seven focused
  controls per profile, **39 ordinary batch comparisons and one terminal fixture**.
  The earlier purecopy batch attempt remains failed; the original fixture and
  expectations pass in the required interactive mode. Prior audit failures remain
  preserved.
- [Complete macOS Rust](docs/handover/2026-09-30-shared-reader-draft/source204-macos-complete-rust-manifest.json):
  **3,059 passes**, two existing ignores, native artifact identity and all input
  hashes verified. Supervisor **43352 has exited**.
- [Complete terminal comparison](docs/handover/2026-09-30-shared-reader-draft/source204-complete-terminal-manifest.json):
  **226 scenarios / 686 comparisons** (658 screen, 28 filesystem), unchanged
  inventories and all eight execution inputs verified. Supervisor **43354 has
  exited**. GNU matches source/native ABI but is not the frozen Darwin executable.
- [Complete Linux Rust failure](docs/handover/2026-09-30-shared-reader-draft/source204-linux-rust-failure-manifest.json),
  run **36764911748**: **2,232 passes / two census failures**. Both original
  reclamation assertions pass. Each first census delta is 47 slots below expected;
  later deltas match. Four later groups and both Cargo stages did not run. The
  exact executable `6ea03c1a…` and image `85ebb975…` are retained. The retaining
  stack word is now traced below; repair remains unverified.
- [Exact-artifact diagnosis](https://github.com/rayfdj/emaxx/actions/runs/36770142335)
  completed: the selected pair and original 626-test primitives group reproduce
  both failures. Its [audited incomplete trace](docs/handover/2026-09-30-shared-reader-draft/source204-exact-census-incomplete-manifest.json)
  shows a 41-element vector and four-field host record reclaimed on the second
  collection. The debugger then hits its startup-cleanup inventory limit, with no
  complete test verdict. That incomplete attempt remains preserved.
- [Revised exact diagnosis](https://github.com/rayfdj/emaxx/actions/runs/36772475709)
  reproduces both failures, ordinarily and under the debugger, with complete
  traces and unchanged artifacts. Its [audited retaining root](docs/handover/2026-09-30-shared-reader-draft/source204-exact-census-root-manifest.json)
  is the closure's tagged pointer in unused evaluator stack space at offset 8.
  The closure's actual constants slot names the 41-element vector. Both are
  reclaimed on collection two, explaining the 47-slot drop. Disassembly assigns
  that inactive stack space to error construction; no collection rule changes.
- [Complete Linux frozen comparison](docs/handover/2026-09-30-shared-reader-draft/source204-linux-frozen-manifest.json),
  run **36764918696**, passes **519 files / 7,928 matching outcomes / 1,038
  successful processes** on exact runtime `d186d40b`. Per editor: 7,670 passes,
  47 expected failures and 211 skips; the latter are not passes.

Do not promote PR #79 while the census failures or complete candidate validation
remain unresolved. The preceding source201 passed full Rust, terminal and Linux
frozen gates; those results do not certify source204. Preserve all failed results,
unexecuted stages, expected failures, skips, and artifact/environment limits.

## Current evaluator candidate

**Source207** in `target/runtime-goal/recovered-2026-09-30/eval-frames/emaxx`
uses actual car/cdr words during dispatch and puts CHECK_LIST error construction
in a separate cold function. The [portable draft and launch](docs/handover/2026-09-30-shared-reader-draft/source207-eval-frame-draft-manifest.json)
replay all 410 inputs from main, including file modes. Its
[audited focused validation](docs/handover/2026-09-30-shared-reader-draft/source207-eval-focused-manifest.json)
passes strict checks with zero warnings and all nine focused gate controls.
The ARM64 gate evaluator uses 176 rather than 208 stack bytes; this is static
evidence, not a timing or Linux repair result. Supervisor **52334** continues
broader gate/release controls and a fresh ordinary build. Preserve this
checkout and its helpers while that process or its children remain live.
The original census assertions and expectations are unchanged. This isolated
candidate now occupies the task branch. Linux confirmation and full candidate
validation remain required; source204's full passes do not certify source207.

## Separate unfinished bytecode draft

**Source206** in `target/runtime-goal/recovered-2026-09-30/bytecode-closure/emaxx`
adds only tests/fixtures to unchanged runtime204. Its
[portable patch and negative controls](docs/handover/2026-09-30-shared-reader-draft/source206-bytecode-negative-controls-manifest.json)
replay **416 inputs** from main `21d20f0e`. Fresh gate build succeeds: **one pass /
four failures**. Both layout controls and both code-string mutation controls fail;
constant sharing, cloning, GC and rejected closure `aset` pass. Supervisor
**51384 has exited**. No bytecode repair is applied to the task branch.

The bytecode closure still has detached 88-byte record/slot storage versus GNU's
40-byte four-slot closure. Mutating its code string is visible through Lisp but
execution uses stale decoded instructions, even during an active call. An
entry-only cache refresh cannot fix that case. The earlier
[source205 baseline](docs/handover/2026-09-30-shared-reader-draft/source205-bytecode-negative-baseline-manifest.json)
retains the original ordinary probes, including the invalid closure-aset attempt.

## Remaining full-goal requirements

Finish shared compact object authority (including bytecode closures and allocated
symbols), remove remaining adapters and unnecessary ordinary-path bookkeeping,
implement honest physical allocation/GC accounting and real counters, preserve
survival and reclamation, and complete VM/call optimization from profiles. The
final adversarial review, zero-warning Linux/macOS validation, pinned compatibility
and **locked 16-workload GNU performance criterion with its unchanged 3% ceiling**
remain required. No current timing or GNU-parity claim exists.

Receipts and reproduction helpers are under `target/runtime-goal/resume-2026-09-28`;
recovered checkouts and GNU are under `target/runtime-goal/recovered-2026-09-30`.
Rebuild on another machine; local executable/image identities are not portable
validation claims. Do not reapply historical patches over the existing task branch.

The [archived full continuation index](docs/runtime-representation-handover-history-2026-10-01.md)
retains every earlier checkpoint, failure, diagnosis and handover link. It includes
the original [27 September handover](docs/runtime-representation-handover.md) and
[complete September 21 audit](docs/adversarial-decheating-audit-2026-09-21.md).
