# Native call routing and argument boundaries — 5 October 2026

The [full goal](runtime-representation-goal.md) remains active and incomplete.
**Source297 is applied** after source296 below. Main remains `21d20f0e`; PR79 is draft.
The immutable `funcall` descriptor enters its existing guarded handler immediately
after handler synchronization, before unrelated primitive-name/type checks. All
fallback, debugger, backtrace, GC, roots, arity/error order and unwind behavior remain.
GNU native calls enter `Ffuncall` directly; no new lookup, cache or lifecycle is added.

The MANY argument boundary now uses `&[]` for zero arguments. Rust requires an
aligned nonnull address even for an empty slice; generated GNU ABI callers may
supply null. GNU `alloc.c:Flist` returns nil without reading storage for every
nonpositive count. One test invokes the real native trampoline with null storage
and counts 0, -1 and -7, checking nil and balanced call state. Positive counts and
all prior tests/assertions/fixtures are unchanged. Correctness-only source298 keeps
the complete original296 router so the cost of this repair remains visible.

The [closed selected evidence](handover/2026-09-30-shared-reader-draft/source297-298-native-dispatch-selected-manifest.json)
verifies both 532-input byte/mode replays, fresh gate/release/ordinary builds and
warning-free strict checks. Each candidate passes **327 selected tests per profile,
zero failures/ignores**, and **47 ordinary GNU comparisons**. The complete broad
probe still fails its same 60 display-column rows. Both suspended-root contracts
remain. The original helper-generation error and its correction are retained.

The [closed three-way diagnostic](handover/2026-09-30-shared-reader-draft/source297-298-native-dispatch-performance-manifest.json)
verifies **288 actual process answers and modes** across three rotated full16 pilots
per source. All bodies exceed 100 ms. Build, test and profile work is separate from
timing. Source297's raw body geometric ratio is **0.989164 versus corrected298**,
**0.996135 versus original296**. Native execution's median is 1.836% lower than298.
All unfavorable cases, startup/total/RSS/GC reports and unavailable-counter errors
remain. Three small samples do not establish statistical significance or explain
all differences. Source297's paired GNU ratio geometric mean is **2.886985×**;
calibration, real counters, prescribed nine rounds and every-case upper 95% ratio
at most 1.03 remain required.

| Case | Original296 / GNU | Corrected298 / GNU | Source297 / GNU | 297 body vs298 | 298 body vs296 |
| --- | ---: | ---: | ---: | ---: | ---: |
| interpreted-lexical | 2.4260× | 2.4680× | 2.4607× | -1.506% | +1.870% |
| interpreted-dynamic | 2.1371× | 2.1758× | 2.1732× | -0.287% | +1.163% |
| interpreted-calls | 2.6502× | 2.9125× | 2.6253× | -4.341% | +5.038% |
| bytecode-calls | 4.1905× | 4.1197× | 4.0144× | +0.212% | -1.742% |
| native-execution | 2.9133× | 2.9035× | 2.9223× | -1.836% | +1.264% |
| interpreted-to-native | 2.4323× | 2.5174× | 2.4811× | -0.008% | +0.807% |
| bytecode-to-native | 8.7659× | 8.5898× | 8.6808× | +0.093% | -0.408% |
| native-to-interpreted | 2.7521× | 2.8170× | 2.6444× | -2.609% | +2.625% |
| native-to-bytecode | 2.9689× | 2.9708× | 2.9743× | -0.104% | +0.508% |
| cons-allocation | 1.8124× | 1.7943× | 1.7278× | -1.493% | -0.250% |
| list-traversal | 2.0809× | 2.0128× | 2.0374× | -0.799% | -0.907% |
| mapcar | 2.3769× | 2.2260× | 2.2617× | -0.265% | -1.500% |
| explicit-gc | 1.0983× | 1.1249× | 1.0785× | -4.814% | +4.805% |
| upstream-sort | 5.2693× | 5.1351× | 5.2405× | +0.928% | -0.610% |
| upstream-undo | 5.3265× | 5.5721× | 5.4698× | +0.618% | -1.144% |
| upstream-bindat | 3.5066× | 3.4971× | 3.3911× | -0.917% | +0.062% |

The [application decision](handover/2026-09-30-shared-reader-draft/source297-application-decision.json)
retains this measured tradeoff without claiming final acceptance. [Applied bytes
and modes](handover/2026-09-30-shared-reader-draft/source297-applied-source-verification.json)
match all validated inputs. All selected/timing coordinators are closed. Frozen
checkouts are `target/runtime-goal/recovered-2026-10-05/native-funcall-dispatch/emaxx`
and `native-empty-arguments/emaxx`; receipts and helpers remain under
`target/runtime-goal/resume-2026-09-28`. Do not restart write-once producers.
The [original launch package](handover/2026-09-30-shared-reader-draft/source297-298-native-dispatch-drafts-manifest.json)
remains historical; the closed results above supersede its unfinished states.

[Complete source292 Linux results](handover/2026-09-30-shared-reader-draft/source292-complete-linux-results-manifest.json)
verify exact `93ed451d`, 3,147 Rust passes/two existing ignores, and all 7,928 frozen
outcomes across 519 files. Raw test names/verdicts, 1,038 successful frozen processes,
177 compiler controls, published source and retained GNU inputs are verified.
Earlier GNU census failures and source276's Emaxx negative remain unexplained.
[Source296 Linux runs](handover/2026-09-30-shared-reader-draft/source296-full-validation-launch.json)
select `68ca5d88`: [the closed Rust audit](handover/2026-09-30-shared-reader-draft/source296-complete-linux-rust-manifest.json)
verifies 3,147 passes/two existing ignores and native artifact identity. Frozen
run37280528176 is running. Exact297 Linux and full macOS remain required. [Three separate297 profiles](handover/2026-09-30-shared-reader-draft/source297-native-call-profiles-manifest.json)
verify all 60 body results and modes. Native dispatch/function resolution and
bytecode-to-native call entry remain costs to inspect. The whole-process samples
include setup and pre-body GC: every body GC delta is zero, so collector samples
are not timed-body costs. Instrumented durations are not performance evidence.
Object authority/adapters, intervals/pure storage, physical counters, general
Rust ownership, final audit and the complete performance criterion stay open.

## Earlier source296 checkpoint

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
