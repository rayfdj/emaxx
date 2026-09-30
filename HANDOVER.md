# Resume the full compact-runtime goal here

The goal is **active and incomplete**. Read the [complete goal and all completion
requirements](docs/runtime-representation-goal.md) before editing. A validated
checkpoint, green gate or handover does not complete it. Current user instructions
take precedence over historical notes.

Read the [current cons continuation](docs/runtime-representation-cons-roots-draft.md),
[allocator continuation](docs/runtime-representation-vector-allocation-draft.md),
[shared-reader handover](docs/runtime-representation-shared-reader-draft.md),
[bytecode draft](docs/runtime-representation-bytecode-draft.md) and
[string-byte draft](docs/runtime-representation-string-bytes-draft.md), plus the
[accounting continuation](docs/runtime-representation-accounting-draft.md).
The [requirement map](docs/runtime-representation-progress.md) tracks the full scope.

## Current production and candidate

The latest validated main checkpoint is **source174**, merge `21d20f0e` in
[PR #78](https://github.com/rayfdj/emaxx/pull/78). Its
[complete validation record](docs/runtime-representation-call-validation-complete.md)
remains separate from the unfinished task branch.

The task branch is **runtime-char-tables**, [PR #79](https://github.com/rayfdj/emaxx/pull/79),
now **ready for review** after the complete source207 checkpoint audits.
Runtime **source204** was published as `d186d40bf014ca53d0a2652b4e2f2d921c5cb009`.
The current **source207**, published as `092fef676e7667332349b12fd9b63539dd39072b`,
adds direct evaluator words and a separate cold error
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

Source207 now repairs both census failures on the complete Linux Rust run below.
Its complete Rust, terminal and Linux frozen results are audited below. The
preceding source201 passed full Rust, terminal and Linux
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
evidence, not a timing or Linux repair result. Its
[complete selected audit and full-gate launch](docs/handover/2026-09-30-shared-reader-draft/source207-eval-selected-validation-manifest.json)
verify **596 gate / 596 release passes**, nine focused controls per profile,
**40 exact ordinary comparisons**, and fresh executable/image identities.
Supervisors **52334 and 54295 have exited**. The
[complete macOS Rust audit](docs/handover/2026-09-30-shared-reader-draft/source207-macos-complete-rust-manifest.json)
verifies **3,059 passes**, two existing ignores, all 2,962 library names/verdicts,
410 source inputs and four retained executable/image files. The
[complete Linux Rust audit](docs/handover/2026-09-30-shared-reader-draft/source207-linux-complete-rust-manifest.json),
[run 36775563045](https://github.com/rayfdj/emaxx/actions/runs/36775563045), verifies
**3,071 passes**, two existing ignores, all 2,970 library names/verdicts and four
retained artifacts on exact `092fef67`. Native artifact identity passes on both.
Both original census assertions and both original suspended-root survival and
reclamation controls pass, unchanged. Only the evaluator source differs from
runtime204; the old failed results remain failed historical evidence.

The [complete terminal audit](docs/handover/2026-09-30-shared-reader-draft/source207-complete-terminal-manifest.json)
verifies all **226 scenarios / 686 comparisons** (658 screen, 28 filesystem),
the unchanged inventories and all eight execution inputs. Supervisor **54296
has exited**. GNU matches source/native ABI but is not the pinned Darwin
executable. The [complete Linux frozen audit](docs/handover/2026-09-30-shared-reader-draft/source207-linux-frozen-manifest.json),
[run 36775569711](https://github.com/rayfdj/emaxx/actions/runs/36775569711), verifies
**519 files / 7,928 matching outcomes / 1,038 successful processes** on exact
`092fef67`. Each editor reports 7,670 passes, 47 expected failures and 211 skips;
the latter categories are not passes. All source207 validation supervisors have
exited. These results certify this checkpoint's tested scope; the separate
bytecode draft, final pinned Darwin gate and performance goal remain unfinished.

## Separate unfinished bytecode draft

The current **source216** in
`target/runtime-goal/recovered-2026-09-30/bytecode-closure/emaxx` replaces detached
bytecode records with the same inline PVEC_CLOSURE used by interpreted functions.
The [source214 patch and audited controls](docs/handover/2026-09-30-shared-reader-draft/source214-shared-closure-draft-and-controls-manifest.json)
preserve **five passes / one failure** across the six original controls, with
zero-warning strict checks. Both layout controls now finish all 4/5/6-slot
checks, native stores and GC tracing; the four-slot allocation is **40 bytes**.
Constructor validation, shared constants/cloning and mutation between calls pass.
Active-call code mutation still returns `(41 194)` instead of `(67 194)`.

The draft removes closure host records, IDs, the per-record program cache and
slot-mutation notices. Interpreter/VM/native access, reading, copying, printing,
predicates, GC and dumping use the actual inline fields. Active and suspended
VM roots retain the actual function. A temporary decoded activation remains;
direct execution from authoritative string bytes is still required.

Review found source214's new vector-copy path omitted its live-vector census
increment. [Source215's portable draft and strict checks](docs/handover/2026-09-30-shared-reader-draft/source215-shared-closure-draft-manifest.json)
repair that increment and add twenty copy-census/identity cases. Supervisor
**61972 has exited** after the [seven audited controls](docs/handover/2026-09-30-shared-reader-draft/source215-shared-closure-controls-manifest.json):
**six pass / one fails**, including the passing new census/identity control.
Its [broader failure and diagnosis](docs/handover/2026-09-30-shared-reader-draft/source215-broad-failure-and-diagnosis-manifest.json)
preserve **839 passes / nine failures** across all 848 affected-module tests.
Five GNU-output checks pass unchanged when the helper restores the ordinary
gate's C locale. GNU rejects three old constructor fixtures; valid unibyte code
and actual vectors preserve their original call/identity/mutation contracts.
The probe wrapper's omitted text-property print notation also remains a failed
expectation. Active-call code mutation is still a runtime defect.

[Source216's portable draft](docs/handover/2026-09-30-shared-reader-draft/source216-bytecode-fixture-repair-draft-manifest.json)
changes only those three unit fixtures from source215, preserving every original
behavior assertion, and restores the helper locale. All 418 inputs and file modes
replay exactly; strict checks pass with zero warnings. Supervisor **64424 has
exited**. Its [audited results](docs/handover/2026-09-30-shared-reader-draft/source216-bytecode-results-manifest.json)
verify **nine passes / one failure** in the ten focused controls and **847 passes /
one failure** in the unchanged 848-test broader inventory, with zero ignores.
Only active-call code mutation fails. The executable and post-run image are
retained. These are gate-profile results, not complete or release validation.
Earlier failures and every intermediate patch remain preserved.
No bytecode repair is applied to the task branch.

The [current source223 string-byte and direct-VM draft](docs/runtime-representation-string-bytes-draft.md)
continues separately in `target/runtime-goal/recovered-2026-09-30/string-bytes/emaxx`.
Shared strings hold actual GNU bytes. Source221 repairs full-range `string`,
`make-string` and `char-to-string` construction without Rust-char conversion.
Its [complete affected-module audit](docs/handover/2026-09-30-shared-reader-draft/source221-string-constructor-repair-manifest.json)
verifies strict zero-warning checks, **14/15 focused passes** and **927/928 broader
passes**, zero ignores; only active-call mutation fails. Supervisor **70849 has
exited**. The preceding [source220 broader audit](docs/handover/2026-09-30-shared-reader-draft/source220-string-broad-results-manifest.json)
retains **925/927 passes and two failures**; supervisor **68772 has exited**.

The [source223 portable draft](docs/handover/2026-09-30-shared-reader-draft/source223-direct-bytecode-draft-manifest.json)
fetches the actual code bytes, uses byte offsets for branches and suspended
frames, and removes decoded-code caches/tables, per-call `Rc` activation and
obsolete string serial bookkeeping. All **425 inputs and modes** replay exactly;
strict checks pass with zero warnings. The
[focused audit](docs/handover/2026-09-30-shared-reader-draft/source223-direct-bytecode-focused-manifest.json)
verifies **17 gate passes**,
including the original active-call mutation failure and new GNU-confirmed
width/operand/branch/GC/suspended-caller cases. Supervisor **73057** is running
all **930 affected-module tests**. Supervisor **73532** runs release controls
and the same broader inventory. Preserve the checkout and helpers until both
and their children exit. Audit each outcome; a launch is not a pass.
Source222's strict obsolete-helper failure remains preserved, with no runtime
tests. A temporary non-ASCII plain-string Rust API byte adapter remains.
Plain-string migration, compact headers, physical accounting, complete validation
and the full performance goal remain open. No draft runtime is applied to the branch.

The preserved **source208** carries evaluator207
and adds GNU constructor field validation. Its [418-input portable draft](docs/handover/2026-09-30-shared-reader-draft/source208-bytecode-constructor-draft-manifest.json)
and [audited controls](docs/handover/2026-09-30-shared-reader-draft/source208-bytecode-constructor-results-manifest.json)
retain twelve ordinary negative constructor cases. Strict checks pass; six gate
controls yield **two passes / four failures**. Constructor validation and shared
constants pass; both layout and both live-code mutation controls still fail.
The result archive also preserves the wrapper's later log-print filename error.
Supervisor **53789 has exited**. No bytecode repair is applied to the task branch.

The preserved **source206** baseline adds only tests/fixtures to runtime204. Its
[portable patch and negative controls](docs/handover/2026-09-30-shared-reader-draft/source206-bytecode-negative-controls-manifest.json)
replay **416 inputs** from main `21d20f0e`. Fresh gate build succeeds: **one pass /
four failures**. Both layout controls and both code-string mutation controls fail;
constant sharing, cloning, GC and rejected closure `aset` pass. Supervisor
**51384 has exited**. No bytecode repair is applied to the task branch.

The source204 baseline has detached 88-byte record/slot storage versus GNU's
40-byte four-slot closure. Mutating its code string is visible through Lisp but
execution uses stale decoded instructions, even during an active call. An
entry-only cache refresh cannot fix that case; source214 still fails it. The earlier
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
