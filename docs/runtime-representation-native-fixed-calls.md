# Native fixed-call argument staging — 5 October 2026

The [full goal](runtime-representation-goal.md) remains active and incomplete.
**Source296 is applied** after the [LF-only checkpoint](runtime-representation-buffer-lines.md).
Main remains `21d20f0e`; PR79 stays draft. This checkpoint follows actual GNU
`eval.c:funcall_subr`: copy fixed arguments only when omitted optional arguments
need nil padding. Fully supplied fixed calls use their original words.

This removes SmallVec initialization, copying, resize and cleanup from that path.
Those small buffers were inline; no removed heap allocation is claimed. Arity
checks, function identity, optional padding, MANY ownership, target resolution,
backtrace/debugger, GC entry, handlers and nonlocal exits remain unchanged. No
fixture, test assertion, selector, timeout, expected byte or tolerance is weakened.

The [closed selected evidence](handover/2026-09-30-shared-reader-draft/source296-native-fixed-selected-manifest.json)
verifies all 532 inputs and file modes, exact incremental/main patch replays,
fresh gate/release/ordinary compilation and zero-warning strict checks. Both
profiles pass **326 selected tests, zero failures/ignores**. Both suspended-root
contracts remain. All **47 ordinary same-input GNU comparisons** pass, including
fixed 0–8/MANY/optional arguments, numeric boundaries and generated reader output.
The unchanged broader buffer probe retains its 60 original column-width failures.
Selected validation does not certify complete platform behavior or general ownership.

The [closed performance evidence](handover/2026-09-30-shared-reader-draft/source296-native-fixed-performance-manifest.json)
verifies **192 actual process answers/modes**, three alternating full16 pilots for
applied LF-correct baseline292 and candidate296. Every timed body exceeds 100 ms;
no builds, tests or profiles run during timing. All samples, startup/total/RSS/GC
reports and unavailable-allocation errors remain. Native execution's median body
is **14.167% lower**. The raw body geometric ratio is **0.950033**; the paired GNU
ratio geometric means are **2.996738× / 2.880160×**. Undo and Bindat remain slower.
This small diagnostic establishes no statistical acceptance or causal explanation
for every timing difference. The locked nine-round calibration and every-case
upper 95% GNU ratio of at most 1.03, with real counters, remain required.

| Case | Baseline292 / GNU | Source296 / GNU | Source296 body change |
| --- | ---: | ---: | ---: |
| interpreted-lexical | 2.4519× | 2.3023× | -5.997% |
| interpreted-dynamic | 2.2716× | 2.1108× | -5.334% |
| interpreted-calls | 3.0427× | 2.7046× | -12.411% |
| bytecode-calls | 4.0953× | 4.0837× | -2.073% |
| native-execution | 3.4259× | 3.0082× | -14.167% |
| interpreted-to-native | 2.5157× | 2.4199× | -1.484% |
| bytecode-to-native | 8.5996× | 8.4708× | -2.185% |
| native-to-interpreted | 2.8269× | 2.6881× | -7.298% |
| native-to-bytecode | 2.9119× | 2.8971× | -8.853% |
| cons-allocation | 1.8147× | 1.8682× | -2.485% |
| list-traversal | 2.1157× | 1.9867× | -4.374% |
| mapcar | 2.3756× | 2.2384× | -5.219% |
| explicit-gc | 1.0752× | 1.0777× | -10.644% |
| upstream-sort | 5.5022× | 5.3621× | -2.586% |
| upstream-undo | 5.5119× | 5.0515× | +2.916% |
| upstream-bindat | 3.3593× | 3.5869× | +4.265% |

The [application decision](handover/2026-09-30-shared-reader-draft/source296-application-decision.json)
retains those unfavorable cases. [Applied bytes/modes](handover/2026-09-30-shared-reader-draft/source296-applied-source-verification.json)
match the validated candidate exactly. All local validation/timing supervisors
have exited. Frozen source is
`target/runtime-goal/recovered-2026-10-05/native-fixed-funcall/emaxx`; helpers and
receipts are under `target/runtime-goal/resume-2026-09-28`. Do not rerun write-once
producers. The archive includes their executed source and all raw results.

[Predecessor292 Linux validation](handover/2026-09-30-shared-reader-draft/source292-full-validation-launch.json)
selects `93ed451d`: Rust37278007515 and frozen37278010760 are running at publication.
Its strict/selected evidence does not close those runs. Source290's frozen run
matches all 7,928 outcomes; its recurring GNU census reference failure remains
unresolved. Exact-source296 Linux and complete macOS validation remain required.

The next ordinary-path lead is the profile's repeated primitive-name routing
before the existing `funcall` handler. Keep synchronization, fallback and the
full call lifecycle intact. A separate recorded native boundary issue constructs
an empty Rust slice from a null pointer; its stated API permits null for zero
arguments, but Rust requires a nonnull reference. Neither lead is implemented
by this checkpoint. Remaining object authority/adapters, intervals/pure storage,
physical accounting, public ownership, final audits and GNU parity remain open.
