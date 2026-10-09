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
The latest architectural continuation is the separately packaged
[common-string draft](docs/runtime-representation-common-strings-draft.md).
Read the latest [correctness recovery and separate file-coding draft](docs/runtime-representation-correctness-recovery-draft.md) before continuing.
The [requirement map](docs/runtime-representation-progress.md) tracks the full scope.

## Source318 application after the main merge — 9 October 2026

PR [#81](https://github.com/rayfdj/emaxx/pull/81) is merged into main as
`30fa6e5b669c5cbd26e842e7675bfc18d306c2b4`. Main contains source316 and its
complete saved evidence, including both failed macOS Tramp processes and the
separate matching longer observation. The full goal remains incomplete.

PR [#82](https://github.com/rayfdj/emaxx/pull/82) carries the
[source318 continuation](docs/runtime-representation-symbol-payload-api-draft.md).
It includes source317's registry removal and
copied payload access; all 562 input bytes/modes match the archived patches,
strict/selected validation and the complete macOS Rust gate (3,166 passes / two
existing ignores). The earlier unapplied sections record historical states.
The [completed source318 Linux evidence](docs/handover/2026-10-09-symbol-fields/source318-linux-complete-results-manifest.json)
verifies **3,178 Rust passes / two existing ignores**, including native artifact
identity, and **519 frozen files / 7,928 matching outcomes / 1,038 successful
processes**. Every raw library name, frozen test inventory, verdict, process
receipt and retained artifact hash is checked. Its macOS frozen comparison
retains both original 180-second Tramp timeouts after 4,479 matching outcomes.
A separate 600-second observation obtains all 59 matching Tramp answers;
the ordinary 200-file tail adds 3,377 matches and six existing diagnostic
differences. The [combined macOS evidence](docs/handover/2026-10-09-symbol-fields/source318-macos-frozen-continuation-results-manifest.json)
observes all 519 files / 7,915 matching outcomes across separate attempts.
The [six diagnostic issue entries are unchanged](docs/handover/2026-10-09-symbol-fields/source318-existing-macos-diagnostics-audit.json)
from source316 and remain strict failures. This is not one complete passing
frozen run. Full terminal validation is running separately. This change
does not repair the known cross-instance symbol-name property leak. The separate
source319 ownership experiment is still local and unvalidated as a whole.

The [source318 Linux timing evidence](docs/handover/2026-10-09-symbol-fields/source318-linux-symbol-performance-results-manifest.json)
verifies all **192 actual answers and modes**. Three alternating full16 pilots
have a body aggregate **0.53% slower than source316 main**, and **2.857181×**
paired GNU. Explicit GC regresses 6.33% and bindat 5.28%; all samples remain.
This is an unfavorable diagnostic, not calibrated nine-round/every-case
performance acceptance. The other source318 platform runs remain separate.

The [source319 ownership draft](docs/runtime-representation-symbol-owner-draft.md)
is separately packaged and unapplied. It gives public editor entries their own
canonical symbols and hash-test descriptors. The original name-property leak
now matches GNU. True pointer equality exposed a second-editor descriptor
mismatch; the corrected owner-specific pool now matches the unchanged probe.
All earlier failures remain preserved. Strict checks and **229 gate controls**
pass in **both profiles**. The [selected continuation](docs/handover/2026-10-09-symbol-fields/source319-symbol-owner-selected-results-manifest.json)
adds nine ordinary GNU comparisons. The [public/native continuation](docs/handover/2026-10-09-symbol-fields/source319-public-native-results-manifest.json)
passes all three existing public integration tests and compares two native-compiling
editor instances restored from valid images. Original image/link/worker launcher
failures remain preserved. All local source319 processes have ended. Private image-clone
isolation, direct allocated fields, descriptor reclamation and other process-wide
Lisp roots remain unresolved. This is unfinished work, not ownership completion.

The [latest source319 draft5](docs/handover/2026-10-09-symbol-fields/source319-symbol-key-contract-results-manifest.json)
removes the inconsistent symbol/text borrowing trait and hashes symbol identity.
Strict checks, **230 controls in each profile** and **nine ordinary GNU comparisons**
pass. Every previous control remains, with a new nested-editor identity check.
Both patches replay all 563 inputs. Draft4's public/image/native results remain
separate historical evidence; the broader draft5 contracts have not run. All
local draft5 validation processes have ended. Source318's full 226-scenario
terminal comparison is running after its macOS frozen tail; the ordinary
harness, actions, comparisons and deadlines are unchanged.

## Main merge and source316 continuation — 9 October 2026

PR [#80](https://github.com/rayfdj/emaxx/pull/80) is merged into main as
`0610bf0c6aafbc45101d67efbd19cb058af4fc8f`. It carries source314's symbol
lifetime/plist repairs and the evidence and separately packaged drafts below.
The full goal remains active and incomplete. The existing Git credential works;
no new login or user approval is required for the authorized branch/CI work.

Source316 is now applied on the task branch as `ee0bddb9`, with all 562 inputs
identical to its selected validation. It combines source315's compact fields
and removed alias mirror with a shared value/alias word. The
[complete macOS Rust evidence](docs/handover/2026-10-09-symbol-fields/source316-macos-rust-results-manifest.json)
verifies **3,165 passes / two existing ignores**. The
[complete Linux evidence](docs/handover/2026-10-09-symbol-fields/source316-linux-complete-results-manifest.json)
verifies **3,177 Rust passes / two existing ignores** and **519 frozen files /
7,928 matching outcomes / 1,038 successful processes**. Raw names, verdicts,
source/artifact identities and native artifact identity are independently checked.
The [macOS frozen continuation](docs/handover/2026-10-09-symbol-fields/source316-macos-frozen-continuation-results-manifest.json)
observes all **519 files / 7,915 matching outcomes**, with **six existing platform
diagnostic differences**. The original run retains both Tramp timeouts at 180
seconds. A separate 600-second observation obtains all 59 matching Tramp answers;
the ordinary 200-file tail retains the original deadline. This is not one complete
passing frozen run. Source316 terminal validation remains pending. The [Linux timing evidence](docs/handover/2026-10-09-symbol-fields/source316-linux-symbol-performance-results-manifest.json)
verifies all 192 answers/modes. The three alternating full16 pilots are 2.41%
faster than current main by raw body aggregate, but still 2.659845× paired GNU.
Mapcar regresses 2.41%; all samples remain. This is not final performance acceptance.
The archived draft's earlier unapplied status is historical. Main's production
runtime remains source314 until the next validated merge.

The new [registry-removal draft](docs/runtime-representation-symbol-registry-draft.md)
is separately packaged as **source317, unapplied**. It removes the redundant
name-to-ID registry/mutex and two uninterned-name copies. Strict checks, all
175 controls per profile and nine ordinary comparisons pass; the initial
143-pass/32-failure source-layout attempt remains preserved. A source316
public-API probe confirms that mutable symbol-name properties leak into the
next editor instance. That negative remains open. Instance-owned interning,
image copying and allocated fields must resolve it; the cleanup alone does not.

The next [copied-payload API draft](docs/runtime-representation-symbol-payload-api-draft.md)
is separately packaged as **source318, unapplied**. It includes source317 and
replaces borrowed mutable payloads with copied words and explicit stores, keeping
the existing lookup and assignment paths. Strict checks, **208 controls per
profile** and **nine ordinary GNU comparisons** pass. Both patch replays verify
562 inputs; old assertions and all test inventories remain. Its full macOS Rust
gate [passes 3,166 tests / two existing ignores](docs/handover/2026-10-09-symbol-fields/source318-macos-rust-results-manifest.json),
including native artifact identity. Linux/frozen/terminal/timing remain pending.
Instance-owned interning and allocated
fields are still unimplemented; the public-instance name-property negative stays
open. This API preparation does not establish ownership or performance completion.

## Symbol-field checkpoint evidence — 9 October 2026

Read the [symbol-field continuation](docs/runtime-representation-symbol-fields-draft.md).
Source314 removes the separate plist store/indexes, preserves live `put` sharing,
and follows actual uninterned-symbol roots, including aliases and finalizers.
Its portable selected evidence verifies 560 source inputs, warning-free strict
checks, 24 focused passes in each profile and 12 matching ordinary GNU/Emaxx
processes, including real bytecode/native execution. Earlier failed drafts remain
preserved. The complete macOS Rust gate now verifies **3,161 passes / two existing
ignores**, including every raw library name and native artifact identity. Linux
Rust verifies **3,173 passes / two existing ignores**, and full Linux frozen
matches **519 files / 7,928 outcomes / 1,038 successful processes**. Each editor
has 7,670 passes, 47 expected failures and 211 skips.

The [macOS frozen failure and bounded Tramp continuation](docs/handover/2026-10-09-symbol-fields/source314-macos-frozen-incomplete-results-manifest.json)
retain **318 completed comparisons / 4,479 matching outcomes**, followed by both
editors timing out in Tramp at the original 180-second limit. The separate
600-second observation obtains all **59 matching Tramp outcomes**: each editor
has 52 passes and seven skips. Its subject was rebuilt from the same runtime
inputs; both executable identities are recorded and the image is unchanged.
The original failed run remains failed. The [ordinary tail and Eglot replay](docs/handover/2026-10-09-symbol-fields/source314-macos-tail-results-manifest.json)
now cover exactly the remaining **200 frozen files**, with the unchanged
180-second limit. The tail has 3,376 matching outcomes, six existing platform
diagnostic differences and one GNU Eglot reconnect expectation failure; Emaxx
passes that test. One separate whole-file Eglot replay matches all 52 outcomes
with identical executables/images and source inputs. The earlier GNU failure
remains unexplained and failed. Across the attempts, all 519 files are observed,
7,915 distinct outcome labels have matches, and six platform differences remain.
This is not one complete passing frozen run. Source315 validation is separate.

Three alternating Linux full16 pilots verify all **192 actual answers/modes**.
The body aggregate is **2.52% slower than main**, and the paired GNU aggregate is
**2.896247×**. Bytecode-to-native regresses 20.30%; all samples and regressions
remain in the linked evidence. Per-instance allocated-symbol ownership, honest
physical accounting/counters and every other full-goal requirement stay open.
This diagnostic does not establish calibrated nine-round/every-case GNU parity.

The next [compact-symbol draft](docs/runtime-representation-symbol-values-draft.md)
is **packaged separately and unapplied**. Source315 removes duplicated alias
names/targets and uses single-word value/function payloads. Its exact patch
replays 560 inputs; strict checks, 126 selected tests per profile and 12 ordinary
GNU/Emaxx processes pass. All old tests remain. Its complete macOS gate verifies
**3,163 passes / two existing ignores**, including native artifact identity.
Its own Linux/frozen/timing evidence has not run. PR80's runtime remains314.

The cumulative [value/alias-union draft](docs/runtime-representation-symbol-redirect-draft.md)
is now separately packaged as **source316, still unapplied**. Value and alias
share one word, and the per-interpreter cell shrinks from 64 to 56 bytes.
Its patch replays 562 inputs; strict checks and 128 controls per profile pass.
Eighteen successful ordinary processes give nine matching comparisons, including
actual native compilation of all three new helpers and their caller. Two earlier
native-launcher failures and a conservative-stack fixture mismatch remain
preserved. Its full macOS Rust gate is running at
`target/runtime-goal/symbol-ownership-2026-10-09/redirect-draft1-full`.
Complete source316 platform/frozen/timing validation is still required; the
per-instance allocated-symbol ownership work is not complete. The requested
main merge carries source314 runtime and these separately packaged drafts;
it does not complete the full goal.

## Main merge and terminal continuation — 8 October 2026

PR [#79](https://github.com/rayfdj/emaxx/pull/79) is **merged into main** as
`6ff4931467fa70acfe837d91245e5c76ddc6515f`. No new login was needed: the `gh` CLI's
stored token failed, but normal Git fetch/push worked. The full goal remains active.

The [post-merge terminal evidence](docs/handover/2026-09-30-shared-reader-draft/source313-postmerge-terminal-results-manifest.json)
retains an unchanged ordinary package replay with all **11 comparisons matching**,
followed by all **42 previously incomplete scenarios / 254 comparisons matching**.
The latter cohort observes the original harness and retains all 238 complete screen
snapshots: every row, cell attribute and cursor coordinate agrees; the other 16
comparisons check filesystem state. Both editors visibly install and remove the
package. Actions, deadlines, executable/image bytes and all 552 source inputs are
unchanged. All continuation processes have exited.

Across the separate attempts, all **226 scenarios / 686 distinct comparison labels**
now have matching observations. This is not one complete passing ordinary run.
The original full run and first tail remain failed; the intermittent package
divergence and GNU readiness failures remain unexplained. No runtime repair is
inferred from the later passes. Raw failed logs accompany the new results.

The requirement map now reflects the merged source312/313 checkpoint, including
the completed stack-capacity and native-GC repairs. Allocated-symbol authority,
intervals/pure storage, real allocation accounting/counters, remaining adapters
and internal ownership review still precede final validation and the unchanged
calibrated performance criterion. No performance or full-goal completion is claimed.

## Checkpoint for main — 8 October 2026

**Source313 is validated; the full goal remains active and incomplete.** The
current user instruction authorizes merging this checkpoint to main. PR79 carries
the implemented runtime work and the portable continuation; the remaining full-goal
requirements below continue after that merge. Earlier dated sections retain their
historical branch states and failures.

Production sources are unchanged from source312 (`7e894a66`). Source313 (`6ab33460`) only
adds two unconditional collections before each of the two object-census fixtures
starts measuring. The old whole-heap delta could include GNU reclaiming an
unrelated nine-word closure/constants pair. Every original constructor, size,
assertion and expected byte remains. No runtime special case, output adjustment,
retry-until-pass or coverage reduction was introduced. The original312 Linux
failure remains **2,297 passes / two GNU reference failures / 767 unstarted library
tests**, with both Cargo stages unstarted.

The [census correction evidence](docs/handover/2026-09-30-shared-reader-draft/source313-census-setup-results-manifest.json)
reproduces all 552 source bytes and modes, verifies fresh zero-warning strict checks,
both original controls in gate and release, **40 ordinary macOS GNU/Emaxx processes**,
and **18 Linux GNU processes plus 693 bounded Rust passes**. The complete new Linux
gate verifies **3,167 passes and two existing ignores**, including native artifact
identity and every one of the 3,066 library names. Production-byte identity connects
this fixture-only correction to source312's separately identified platform artifacts.

The [closed source312 platform evidence](docs/handover/2026-09-30-shared-reader-draft/source312-closed-platform-results-manifest.json)
independently verifies:

- Linux frozen: **519 files / 7,928 matching outcomes / 1,038 successful processes**;
  each editor has 7,670 passes, 47 expected failures and 211 skips.
- macOS Rust: **3,155 passes / two existing ignores**, including native artifact identity.
- macOS frozen: **519 files / 7,915 matching outcomes / six mismatches** across 1,038
  successful processes. Each editor has 7,623 passes, 46 expected failures and 252 skips.
  The six existing `system-configuration-features` skip diagnostics remain strict
  failures. They are neither passes nor new capability claims. The previous ERC
  crash is repaired; the complete ordinary run reaches every file.

The [terminal evidence](docs/handover/2026-09-30-shared-reader-draft/source312-macos-terminal-incomplete-results-manifest.json)
remains **incomplete: 173 scenarios / 374 comparisons match, 312 comparisons are
unexecuted**. GNU failed to display its startup target within the existing
120-second readiness deadline at scenario 174; no output divergence was reported.
The executable/image and all 11 execution inputs are unchanged. The original
relocated-image startup failure also remains preserved. Its retry changed only
the retained copies' location to the image's required directory depth.

The [separate tail attempt](docs/handover/2026-09-30-shared-reader-draft/source312-terminal-tail-incomplete-results-manifest.json)
adds 62 matches, but **package installation confirmation differs**: GNU displays
an installed package while Emaxx still displays the marked available entry. Its
six later checkpoints do not execute. GNU then misses startup readiness at
`org-fieldnotes-todo-cycle`. The cause of the package divergence remains open;
no later passing result is substituted for it.

Together the two attempts account for all 686 labels: **436 match, one differs,
249 remain unexecuted**. 184 scenarios fully match; 42 do not have a complete passing
result. Both processes have exited. Keep the original harness, artifacts, actions
and 120-second readiness deadline when continuing the outstanding terminal work.
This is an explicitly incomplete checkpoint for the requested main merge.

All 192 actual answers and execution modes in the three alternating full 16 timing
pilots match. The raw body geometric ratio312/311 is **1.034546** (3.45% slower);
paired GNU ratios are **2.829352× for source312** and 2.812642× for source311. The largest raw
regression is bindat (+13.57%); bytecode calls improve 4.60%. Every sample remains.
The predecessor omitted native automatic-GC completion and its statistics; source312
repairs it. These diagnostics preserve the correctness-repair cost and establish
neither equivalent baseline GC accounting nor calibrated GNU parity.

Allocated-symbol authority, real property intervals and unified pure storage,
remaining adapters, physical allocation/GC accounting and real counters, the
internal native borrow/ownership review, final adversarial review and final-source
validation remain required. The locked 16 workloads, calibration, nine rounds and
every-workload upper 95% GNU ratio at most 1.03 are unchanged. **Merging this checkpoint
does not complete the runtime or performance goal.**

## GC completion, call order and timer lifetime — 7 October 2026

**Source312 is applied; the full goal remains incomplete.** Native automatic GC
now runs finalizers, updates collection statistics and runs the inhibited
post-GC hook through the same completion path as ordinary collection
(GNU alloc.c:garbage_collect, lines 6698–6731). Interpreter, bytecode and native
funcall entries root their unresolved function/arguments, collect, then resolve
the current function cell (eval.c:Ffuncall, bytecode.c:Bcall). This also
removes duplicate dispatch-specific call frames. A callback can replace a
bytecode function with an interpreted function, primitive, alias or void cell;
the actual next call now agrees with GNU.

The source310 macOS ERC crash was a timer snapshot retaining unrooted Lisp
words across callbacks. A callback could cancel later timers and collect their
vectors; the later snapshot access read freed memory. The snapshot now uses
the existing RootedVec, retaining remaining entries until iteration consumes
them, corresponding to GNU's reachable copied timer lists in
keyboard.c:timer_check. The original crash, its layout dependence, instrumented
diagnosis and subsequent ordinary reproductions remain preserved.

The [selected evidence](docs/handover/2026-09-30-shared-reader-draft/source312-gc-call-timer-selected-manifest.json)
verifies **552 runtime inputs and modes**, **94 auxiliary inputs**, warning-free
strict checks, **1,068 passes and eight focused passes per profile**, and
**60 ordinary GNU comparisons**, including 198 generated rows. Three additional
call probes match actual GNU output, including native compilation, and the
unchanged upstream ERC case passes in GNU and312 across three environment
layouts. Both portable replays reproduce every runtime byte and mode.
All 1,065 predecessor selectors, 24 audit controls and original fixture/expected
bytes remain. The compaction test now guarantees an actual retired data entry
before the borrowed string; every original string and assertion remains.
The old setup incorrectly required movement even when data could already be at
its destination, which GNU's compact_small_strings does not require.
Both earlier broad failures and the smaller unchanged passing replay remain.

This is a [correctness publication](docs/handover/2026-09-30-shared-reader-draft/source312-application-decision.json)
with [exact applied-source verification](docs/handover/2026-09-30-shared-reader-draft/source312-applied-source-verification.json).
Full exact-source Linux/macOS validation and312 timing remain separate;
inspect the saved launch/queue receipts before starting another run.

Predecessor311's [Linux Rust run](docs/handover/2026-09-30-shared-reader-draft/source311-linux-rust-results-manifest.json)
passes **3,164 tests with two existing ignores**. Its
[full Linux frozen run](docs/handover/2026-09-30-shared-reader-draft/source311-linux-frozen-results-manifest.json)
matches **519 files / 7,928 outcomes / 1,038 successful processes**:
each editor reports 7,670 passes, 47 expected failures and 211 skips.
Its [paired full16 diagnostic](docs/handover/2026-09-30-shared-reader-draft/source311-linux-stack-performance-results-manifest.json)
verifies all 192 actual answers/modes. The raw body geometric ratio311/310
is **1.006777**; paired GNU aggregates are 2.903372× and 2.841965× respectively.
These were measured before the native GC completion repair: reported GC counts
and elapsed GC time omitted native automatic collections. Keep the timings and
regressions visible, but do not call them equivalent-work performance acceptance.

Source310's [macOS frozen run](docs/handover/2026-09-30-shared-reader-draft/source310-macos-frozen-abort-manifest.json)
remains failed: 182 complete comparisons / 2,951 matching outcomes, followed by
the ERC crash; 336 files never started. Its
[terminal raw audit](docs/handover/2026-09-30-shared-reader-draft/source310-macos-terminal-qualified-results-manifest.json)
finds 226 scenarios / 686 matching comparisons, but a diagnostic rebuild changed
the executable during the run, so unchanged-artifact acceptance is unestablished.
The [original frozen executable/image](docs/handover/2026-09-30-shared-reader-draft/source310-original-frozen-retention-verified.json)
are retained separately from the later crashing replay binary. The new full
validation checkout is isolated and its terminal stage uses retained copies.

Main remains 21d20f0e, PR79 remains draft. Allocated-symbol authority, real
intervals and unified pure storage, remaining adapters, physical accounting and
allocation counters, internal native borrow/ownership review, final adversarial
review, complete final-source validation and the unchanged calibrated nine-round
every-workload performance criterion remain open. No GNU performance parity or
full-goal completion is claimed. Earlier sections preserve historical states.

## Applied bytecode stack — 6 October 2026

**Source311 is applied; the full goal remains incomplete.** The VM now has GNU's
512K-word stack capacity, with each function's declared operand space followed
by its actual four-word frame footer. The footer holds the caller frame/top,
return PC and real function object. GC follows that chain; the separate function
root vector and duplicate host return PC are removed. Caller argument slices
remain stable while the callee uses disjoint storage.

The [selected evidence](docs/handover/2026-09-30-shared-reader-draft/source311-bytecode-stack-selected-manifest.json)
verifies all **548 runtime/build/fixture inputs**, **94 unchanged auxiliary inputs**,
warning-free strict checks, **1,065 passes per profile**, five focused controls per
profile, and **58 ordinary GNU comparisons**, including 198 generated rows.
Both portable patches reproduce every runtime byte and mode. All 1,036 predecessor
selectors and all 24 audit controls remain. GNU's maximum single declared depth
of 524280, nested reservations, arguments and recovery after overflow match.
The original capacity fixture is unchanged. Source310's actual stack-overflow
failure and the earlier moved-image setup failure remain separately recorded.

This is a [correctness publication](docs/handover/2026-09-30-shared-reader-draft/source311-application-decision.json),
with [exact applied-source verification](docs/handover/2026-09-30-shared-reader-draft/source311-applied-source-verification.json).
**Source311 timing and complete platform results are pending.** The added
`tools/core_runtime_perf_pair.py` builds both exact revisions before three alternating
full16 pilots on one Linux runner. It retains executable/image copies and every
actual process answer, execution mode, startup/body/total time, GC delta and RSS.
Its raw-evidence audit passed an existing real pilot and rejected seven deliberate
corruptions. The new orchestration still needs its actual Linux execution; this
is diagnostic preparation, not calibrated nine-round acceptance.

Predecessor source310 now has [complete Linux frozen evidence](docs/handover/2026-09-30-shared-reader-draft/source310-linux-frozen-results-manifest.json):
**519 files / 7,928 matching outcomes / 1,038 successful processes**. Each editor
reports 7,670 passes, 47 expected failures and 211 skips. Its
[complete macOS Rust evidence](docs/handover/2026-09-30-shared-reader-draft/source310-macos-rust-results-manifest.json)
verifies **3,150 passes / two existing ignores**, including native artifact identity.
Source310 macOS frozen and terminal remain separate; inspect the saved queue state.
These results certify310 only. The source310 Linux Rust run stays failed with
2,295 passes / one GNU reference failure and 765 library names unstarted.

The [Linux GNU census trace](docs/handover/2026-09-30-shared-reader-draft/source310-linux-gnu-census-trace-results-manifest.json)
now reproduces the original -9 vector difference in seven of ten ordinary GNU
runs and under the read-only debugger. Between the first two collections, GNU's
marked heap loses exactly one 40-byte closure and its 32-byte constants vector,
with no other marked-object change. A tagged closure pointer is on the C stack
in the first snapshot and absent in the second; it is a candidate retention root,
not a proven marking path. The record fixture reproduces -7 in four ordinary runs
but matches under GDB. The global-heap delta can therefore include unrelated
reclamation. Original fixtures and expectations are still unchanged; correcting
that invalid footprint assumption requires preserved equivalent coverage.

Host unwind/backtrace/handler watermarks and transient bytecode views remain.
Function resolution still precedes `maybe_gc`, unlike GNU. Separate same-input
source310 probes return the old function before its GC hook has run, while GNU's
hook replaces it and the new function runs. Identical compiled bytes/constants
rule out a compiler-shape difference; the missing hook's cause remains open, so
those outputs alone do not prove the resolution-order cause. The negative outputs
are preserved in the selected archive and are not counted as passing comparisons.

Main remains `21d20f0e`, PR79 is draft. Allocated-symbol authority, real intervals
and unified pure storage, remaining adapters, physical accounting/counters,
remaining GC/call semantics, final internal ownership/borrow and adversarial review,
complete final-source validation, and the unchanged performance criterion remain
required. Source310's last diagnostic remains **2.634352× GNU overall**; no source311
speedup or GNU parity is claimed. Historical checkpoint sections follow.

## Applied native compilation units — 6 October 2026

**Source310 is applied; the full goal remains incomplete.** A native compilation
unit is now one actual 88-byte GNU-layout object. Its seven Lisp fields own the
data graph, and the unreachable unit closes its library. The host record, strong
library registry and permanent relocation roots are removed. Repeated loads check
the library's actual runtime relocation before accepting another Rust editor;
ordinary calls acquire no new ownership lookup.

The [closed selected evidence](docs/handover/2026-09-30-shared-reader-draft/source310-native-unit-selected-manifest.json)
verifies all **546 runtime/build/fixture inputs** and **94 auxiliary tools/compat
inputs**, fresh warning-free strict checks, **1,036 passes in each profile**, and
**56 actual GNU comparisons**, including 198 generated printer rows. Both portable
patches reproduce the runtime bytes and modes. All 1,035 predecessor selectors
and all 24 audit controls remain. GNU and310 return identical actual values and
agree on native metadata, active-call survival and eventual unit reclamation;
308's differing retention results remain preserved.

The new two-editor control first reproduced309's missing owner check, then passed
in both profiles after repair. It also verifies that the first editor remains
usable and that the second can load the library after the first shuts down.
The earlier test-setup failure remains separate. The initial formal selection
also stays failed: **1,032 passes/four audit failures** because the sparse checkout
omitted unchanged generators. Restoring exact published tools/compat inputs made
all four controls and the complete selection pass on the same gate executable.
No runtime byte, assertion, selector or expected output changed for that recovery.

This is a [correctness publication](docs/handover/2026-09-30-shared-reader-draft/source310-application-decision.json),
with [exact applied-byte verification](docs/handover/2026-09-30-shared-reader-draft/source310-applied-source-verification.json).
The [closed source310 diagnostic](docs/handover/2026-09-30-shared-reader-draft/source310-native-unit-performance-manifest.json)
verifies all **192 actual answers and execution modes** in three alternating
unchanged full 16 pilots. The raw body geometric ratio to source308 is **0.909231**;
Source310's paired GNU aggregate is **2.634352×**. Native execution is **4.287% slower**,
cons allocation **3.007% slower**, and list traversal **0.445% slower** in these
raw medians; every sample remains. These small diagnostics establish neither
statistical significance nor calibrated GNU parity. Separate profiles retain
20 completed repetitions each for native execution, bytecode-to-native and
upstream sorting. Total process cost remains recorded; the owner-shutdown allocator walk has
no isolated cost measurement. The original correctness-publication decision remains unchanged,
accurately recording that timing was still pending when source310 was published.

The [exact source310 Linux Rust failure](docs/handover/2026-09-30-shared-reader-draft/source310-linux-rust-failure-manifest.json)
(run 37428917287) retains **2,295 passes/one failure**: GNU's first
empty-vector census reports **-9 instead of 0**, before Emaxx executes that
control. All later rows in that reference output match. The remaining 765 library
names and both Cargo stages never run. The actual native-unit metadata test
passes; the layout/tracing/owner unit tests are among the unstarted names.
The pinned GNU inputs were retained and verified unchanged. This recurring
reference failure remains unexplained; no assertion, fixture or expected output
was changed. Linux frozen and complete macOS source310 validation are running separately.
Inspect `source310-linux-collection-state.json` and `source310-full-queue-state.json`
before starting further validation jobs. Predecessor source308's
[terminal evidence](docs/handover/2026-09-30-shared-reader-draft/source308-macos-terminal-results-manifest.json)
now passes all 226 scenarios / 686 comparisons; it does not certify source310.

Main remains `21d20f0e`, PR79 stays draft. Allocated-symbol authority, real intervals
and unified pure storage, remaining adapters, physical accounting/counters,
VM stack capacity/layout, final internal ownership/borrow review, final audit,
complete final-source validation and the unchanged calibrated performance
criterion remain required. The following sections preserve earlier checkpoints.

## Native compilation-unit draft — 6 October 2026

The separately packaged [source309 selected draft](docs/handover/2026-09-30-shared-reader-draft/source309-native-unit-selected-draft-manifest.json)
is **unapplied**. It replaces the host unit record, strong library registry and
permanent relocation roots with one actual 88-byte GNU-layout unit. Its seven
Lisp fields own the data graph, and unreachable units close their library handle.
Both portable replays match all 546 inputs and modes. Fresh strict checks are
warning-free; all **1,035 selected tests per profile** and **56 ordinary GNU
comparisons** pass, including 198 generated printer rows.

Actual GNU, source308 and source309 run the same lifecycle programs. Source308
keeps file-loaded units after their functions are unbound; GNU and309 reclaim
them. Across interpreted, bytecode and native calls,309 also keeps a self-unbound
native function's unit alive during collection, returns the identical real data,
and permits reclamation after return. The separate anonymous-compilation probe
matches GNU's remaining count of two;308 retained three. The differing308 outputs
and every development failure remain preserved.

Review found a remaining **host ownership check**: a second Rust editor must not
reuse a library still relocated to the first editor. The removed registry rejected
that case;309's repeat-load path currently checks only the actual unit and live
handle. The isolated310 successor adds a regression control and will restore the
check using the library's actual current-thread relocation, with no new registry
or ordinary-call lookup.309's passing selection does not resolve that gap or
establish complete internal soundness. No309 timing or full-platform claim exists.
The applied runtime remains308; the full goal remains incomplete.

## Applied native function objects — 6 October 2026

**Source308 is applied; the full goal remains incomplete.** Native functions now
use one actual 88-byte GNU-layout subr for calls, metadata, identity and GC. The
old native-function record and three persistent descriptor/name maps are removed.
Direct calls read the function pointer and arities from the object. Five Lisp
fields are traced explicitly; unreachable subrs release their C names.

The [closed selected evidence](docs/handover/2026-09-30-shared-reader-draft/source308-native-object-selected-manifest.json)
verifies all 541 source inputs/modes, fresh warning-free strict checks,
**1,031 passes per profile**, including all 24 audit tests, and **52 ordinary GNU
comparisons**, including 198 generated printer rows. Original fixtures, assertions
and expected bytes remain. The original dabbrev regression is repaired by making
`commandp` recognize the native object's interactive field; native reader and
property identity and exact GNU C-name printing are covered too.

The [closed full16 diagnostic](docs/handover/2026-09-30-shared-reader-draft/source308-native-object-performance-manifest.json)
verifies all **384 actual process answers and execution modes**. The raw body
geometric ratio to304 is **0.951094**;
individual changes range from **-25.891% to +5.656%**.
308's paired GNU aggregate is **2.710317×**.
Every sample remains, including the first cohort’s slower undo and interpreted
cases. These six complementary pilots do not establish statistical
significance or GNU parity. Real counters, calibration, nine rounds and every-case
upper95% ratio at most1.03 remain required.

Predecessor304 platform results are closed and preserved: Linux frozen passes
519 files /7,928 equal outcomes; macOS Rust passes3,139 tests with two existing
ignores; [macOS terminal](docs/handover/2026-09-30-shared-reader-draft/source304-macos-terminal-results-manifest.json)
passes all226 scenarios /686 screen and filesystem comparisons. Linux Rust stays
failed after2,291 passes at the GNU first-record census (-7 instead of2), leaving
758 library names and both Cargo stages unstarted. macOS frozen stays failed with
7,915 exact matches and six existing build-feature skip-diagnostic differences.
These results certify304 only. The [closed exact308 platform evidence](docs/handover/2026-09-30-shared-reader-draft/source308-platform-validation-progress-manifest.json)
now verifies **3,157 Linux Rust passes** and **3,145 macOS Rust passes**, with the
two existing ignores on each platform. Every library name and Cargo result is
checked against raw output; both native artifact identity tests pass. Linux's
complete frozen comparison matches **519 files /7,928 outcomes** from **1,038
successful processes**: each editor reports 7,670 passes, 47 expected failures and 211
skips. No expected failure or skip is counted as a pass. The
[closed308 macOS frozen comparison](docs/handover/2026-09-30-shared-reader-draft/source308-macos-frozen-results-manifest.json)
retains **7,915 exact matches/six existing skip-diagnostic differences**, across
all519 files and1,038 successful processes. It remains a failed strict comparison;
terminal validation is still running separately. Earlier GNU census failures and source276's
Emaxx negative remain preserved and unresolved; this later pass does not erase them.

Main remains `21d20f0e`, PR79 stays draft. Symbol/unit authority, real intervals and
pure storage, physical accounting, final ownership/borrow review, final adversarial
review, complete final-source validation and the full performance criterion remain
open. See the native continuation for failed drafts and exact-source launch receipts.

## Applied column and native-audit repair — 6 October 2026

**Source304 is applied; the full goal is incomplete.** Ordinary buffer columns
now follow GNU `indent.c:scan_for_column` and `buffer.h:CHARACTER_WIDTH`: ASCII
controls honor `ctl-arrow`, unibyte non-ASCII characters occupy four columns,
and multibyte widths come from the live `char-width-table` with GNU sanitization.
All **338 rows of the original probe match actual GNU**, closing its recorded
60 column differences. No original fixture or expected byte changed. This does
not establish complete overlay, display-table or composition semantics.

Source302's full checks found an omission: its native `+`/`-` paths were absent
from the exact-contract inventory.304 adds the GNU owners and a real installed-ABI
boundary test; every audit assertion, negative control and the empty deviation
list remain. Native arithmetic production code is unchanged. The
[failed302 evidence](docs/handover/2026-09-30-shared-reader-draft/source302-full-validation-failures-manifest.json)
retains macOS **3,037 passes/one failure/two existing ignores** and Linux
**3,045 passes/one failure/two existing ignores**. All library names ran, but bins
and integration did not. Both frozen attempts stopped before any case; TTY never
started. The sole Rust failure was the inventory omission. Those runs stay failed.

[Source304 selected evidence](docs/handover/2026-09-30-shared-reader-draft/source304-native-audit-and-columns-selected-manifest.json)
verifies all **534 inputs and modes**, fresh builds, zero-warning strict checks,
**506 passes per profile**, including all24 audit tests, and **49 ordinary GNU
comparisons**. The prior [303 draft](docs/handover/2026-09-30-shared-reader-draft/source303-buffer-columns-selected-manifest.json)
remains unapplied: it passed482 tests/profile but inherited the inventory omission;
its timing queue stopped before measuring anything.

The [302/304 full16 diagnostic](docs/handover/2026-09-30-shared-reader-draft/source304-native-audit-and-columns-performance-manifest.json)
verifies all **192 actual process answers and execution modes**. The raw body
geometric ratio is **1.022383**; per-case changes range from
**-6.142% to +7.665%**.304's paired GNU aggregate is
**2.880732×**. Every sample and unfavorable case remains. These three-pilot
results establish neither statistical significance nor GNU parity; real counters,
calibration, nine rounds and every-case upper95% ratio at most1.03 remain required.

All selected/timing coordinators are closed. Exact304 Linux and full macOS checks
are next: inspect `source304-full-validation-launch.json` and
`source304-full-launch.json` under `target/runtime-goal/resume-2026-09-28`, then
their raw audits. Do not restart write-once jobs. Earlier GNU census failures and
source276's Emaxx negative remain unresolved. Main remains `21d20f0e`, PR79 is draft,
and authoritative symbols/native objects, intervals/pure storage, physical
accounting, public ownership, final validation and adversarial audit remain open.
The following sections preserve historical states.

## Applied native arithmetic checkpoint — 5 October 2026

**Source302 is applied.** The existing native two-fixnum dispatch now returns
in-range `+`/`-` results directly after the same handler synchronization, following
GNU `data.c:arith_driver`. This removes argument staging, repeated primitive
routing, coercion and normalization. Bignum results, other operand types and other
arities retain the original general path. No cache, representation or unsafe
access is added; the prior two-argument buffer was inline, so no removed heap
allocation is claimed.

The [closed selected evidence](docs/handover/2026-09-30-shared-reader-draft/source302-native-arithmetic-selected-manifest.json)
verifies all **534 source inputs and modes**, portable replays from `4af440a9` and
main, fresh builds and zero-warning strict checks. **328 selected tests pass in
each profile**, with no failures or ignores, and **48 actual GNU comparisons**
pass. Every original test/assertion/fixture remains. One new control forces native
workers through 108 varied arithmetic cases, overflow/promotion, invalid operands,
GC and marker coercion; it already matches GNU on the unchanged 297 baseline.

The [closed full16 diagnostic](docs/handover/2026-09-30-shared-reader-draft/source302-native-arithmetic-performance-manifest.json)
verifies all **192 process answers and execution modes**. Native execution's median
body is **22.893% lower** than 297; the raw body geometric ratio is **0.981839**.
Other cases range from 1.388% faster to 0.506% slower; every sample is retained.
The paired GNU geometric means are 2.836585× for 297 and 2.785707× for 302. These small
pilots establish no statistical significance or GNU-parity acceptance. Real
counters, calibration, nine rounds and the unchanged every-case upper 95% ratio
at most 1.03 remain required. Build/test/profile work is separate from timing.

All source302 selected/timing coordinators are closed. Exact-source Linux and full
macOS validation are next; predecessor297's complete Linux pass does not certify
this new source. Inspect `source302-full-validation-launch.json` and
`source302-full-launch.json` under `target/runtime-goal/resume-2026-09-28` for later
launches, then their complete raw audits. The prepared macOS helpers require the
applied commit and closed timing before starting. Do not restart write-once jobs.

The same 60 column-width failures, earlier GNU census failures and source276 Emaxx
negative remain open. Sources300/301 remain rejected and unapplied. Main stays
`21d20f0e`, PR79 stays draft, and the full architecture, real intervals/pure storage,
symbol authority, physical accounting/counters, public ownership, final validation
and adversarial audit remain incomplete. The following sections are historical.

## Native Bcall variants rejected; applied checkpoint passes Linux — 5 October 2026

**Applied runtime remains source297 (`4af440a9`). Sources 300 and 301 are rejected
and unapplied.** The [closed inline comparison](docs/handover/2026-09-30-shared-reader-draft/source301-native-bytecode-inline-performance-manifest.json)
verifies all **288 actual process answers and modes** across rotated full 16 pilots.
Source301 still improves bytecode-to-native by 9.624%, but native-to-bytecode is
12.098% slower, native execution 5.648% slower, and raw body geometric mean 3.178%
slower than 297. Default300 also retains slower native transitions. Forced inlining
does not establish a satisfactory tradeoff; this experiment is closed. Both full
cohorts and every unfavorable sample remain. These small diagnostics establish
neither statistical significance nor a causal explanation for all timing changes.

[Source301 selected evidence](docs/handover/2026-09-30-shared-reader-draft/source301-native-bytecode-inline-selected-manifest.json)
verifies all 532 input bytes/modes, fresh builds, warning-free strict checks,
**327 tests per profile / no failures or ignores**, and **47 actual GNU comparisons**.
Only the helper inline attribute differs from 300. No test, fixture, workload,
timeout, selector or tolerance was changed to obtain these results.

[Complete source297 Linux evidence](docs/handover/2026-09-30-shared-reader-draft/source297-complete-linux-results-manifest.json)
now verifies exact `4af440a9`: **3,148 Rust passes/two existing ignores** and
**519 frozen files/7,928 equal outcomes/1,038 successful processes**, native
artifact identity, 177 compiler controls and the retained pinned GNU inputs.
Runs 37284983243 and 37284989055 both passed. Full macOS for 297 remains unstarted.
The same 60 display-column failures, earlier GNU census failures and source276
Emaxx negative remain open; the later Linux passes do not erase those failures.

All source301 validation and timing coordinators are closed. Preserve frozen checkouts
and write-once receipts. The next profile-led candidate is native two-fixnum
addition/subtraction after existing handler synchronization, following
`data.c:arith_driver`; it is not yet applied or validated. Main remains `21d20f0e`,
PR79 remains draft, and the full architecture, ownership, honest accounting/counters,
final validation/audit and unchanged every-case 3% GNU criterion remain incomplete.
The sections below retain historical states.

## Native Bcall candidate held; inline-only comparison running — 5 October 2026

**Applied runtime remains source297 (`4af440a9`)**. Source300 shares the ordinary
native call body with Bcall after its existing function resolution, removing a
second symbol/alias lookup and general dispatch. All 532 inputs/modes replay;
strict checks are warning-free, **327 tests pass per profile**, and **47 actual
GNU comparisons** pass. Every original test, assertion and fixture remains.

The [closed source300 comparison](docs/handover/2026-09-30-shared-reader-draft/source300-native-bytecode-performance-manifest.json)
verifies all **192 process results/modes**. Bytecode-to-native median time falls
**11.579%**, but native-to-interpreted and native-to-bytecode are **4.652% and
6.609% slower**. The raw body geometric ratio is0.993933; GNU paired ratio is
2.886033×. Source300 is **held and unapplied** while the extracted helper's inlining
cost is checked. No causal explanation or final statistical acceptance is claimed.

[Source301's portable draft](docs/handover/2026-09-30-shared-reader-draft/source301-native-bytecode-inline-draft-manifest.json)
changes only that helper from `#[inline]` to `#[inline(always)]`. Its strict checks
are warning-free; selected validation is running and a rotated297/300/301 full16
comparison is queued. Inspect `native-bytecode-inlining-selection-closed-*` and
`native-bytecode-inlining-performance-sequence-closed-*` receipts under
`target/runtime-goal/resume-2026-09-28`. Preserve original300 timing and the frozen
checkouts; do not restart completed write-once producers. Source299's formatting
failure and source300's preparer assertion correction are retained separately.

[Complete source296 Linux results](docs/handover/2026-09-30-shared-reader-draft/source296-complete-linux-results-manifest.json)
now verify exact68ca5d88: **3,147 Rust passes/two existing ignores**, **519 frozen
files/7,928 equal outcomes/1,038 successful processes**, native artifact identity,
177 compiler controls and retained pinned GNU inputs. These results do not certify
later source. Exact297 Linux runs37284983243/37284989055 are in progress; full macOS
is unstarted. The60 original width failures, prior GNU census failures and source276
Emaxx negative remain open. Main, draft PR and all full-goal criteria remain unchanged.

## Applied native dispatch checkpoint — 5 October 2026

**Source297 is applied**, including the null/empty MANY argument repair. Read the
[native-call continuation](docs/runtime-representation-native-fixed-calls.md).
The existing guarded `funcall` path now precedes irrelevant primitive-name checks,
after the same handler synchronization. No call lifecycle or fallback is removed.
Correctness-only source298 stays preserved separately with the original router.

Both 532-input candidates pass warning-free strict checks, **327 selected tests
per profile / no failures or ignores**, and **47 ordinary GNU comparisons**.
All **288 process results and execution modes** verify in the closed three-way
full16 diagnostic. Source297's native median is **1.836% lower** than corrected298;
raw body geometric ratios are **0.989164 vs298 / 0.996135 vs original296**.
Bytecode calls, bytecode-to-native, sort and undo remain 0.212%, 0.093%, 0.928%
and 0.618% slower than298. These small diagnostics establish no statistical
acceptance. Source297's paired GNU ratio geometric mean is **2.886985×**.
All samples and the unchanged 3% every-case criterion remain.

[Source292 complete Linux results](docs/handover/2026-09-30-shared-reader-draft/source292-complete-linux-results-manifest.json)
are closed and raw-audited: **3,147 Rust passes / two existing ignores**, plus
**519 files / 7,928 equal frozen outcomes / 1,038 successful processes** at exact
`93ed451d`. Earlier GNU census failures and source276's Emaxx negative remain
unexplained; later success does not erase them. Source296 Linux Rust37280524716
also has a closed raw audit: 3,147 passes and two existing ignores. Its frozen
run37280528176 remains in progress.
Those predecessor results do not certify297. Exact297 Linux and complete macOS
validation remain required. The unchanged 60 display-column failures remain open.

All local297/298 validation and timing coordinators are closed; preserve their
frozen checkouts and write-once evidence. Separate source297 native/call/sort
profiles are closed: all 60 results/modes verify. Whole-process collector samples
are pre-body work; every measured-body GC delta is zero. Main remains `21d20f0e`, PR79 remains draft, and the full
architecture, honest counters, ownership, final validation/audit and GNU parity
requirements remain incomplete. The sections below retain historical states.

## Applied native fixed-call checkpoint — 5 October 2026

**Source296 is applied**. Read the [native-call continuation](docs/runtime-representation-native-fixed-calls.md).
Following GNU `funcall_subr`, fully supplied fixed native calls use their original
arguments; only missing optional arguments require copying and nil padding. All
arity/error/root/GC/handler/unwind paths and all tests/fixtures remain.

All 532 inputs/modes replay exactly. Strict checks are warning-free; **326 selected
tests pass per profile, no failures/ignores**, and **47 ordinary GNU comparisons**
pass. The complete paired full16 diagnostic verifies all **192 process answers and
modes**. Native execution's median body is **14.167% lower** than corrected292;
raw median body geometric ratio is **0.950033**. Undo remains **2.916% slower** and
Bindat **4.265% slower**. All results remain. The paired GNU ratio geometric mean
is **2.880160×**, so the unchanged every-case 3% criterion is far from achieved.
Real counters, calibration and prescribed final repetitions remain required.

All local native296 validation/timing coordinators are closed. Preserve their
frozen checkout and write-once receipts. The next measured lead is primitive-name
routing before the existing native `funcall` handler. The recorded null/empty
argument-slice boundary also needs a sound implementation; neither change is in296.

[Source292 Linux validation](docs/handover/2026-09-30-shared-reader-draft/source292-full-validation-launch.json)
selects `93ed451d`: Rust37278007515 and frozen37278010760 are running. They do not
certify296. Exact296 Linux validation is next; full macOS remains unstarted. The
60 original column-width failures and recurring GNU census failure stay open.
Main, draft PR status and every full-goal requirement remain unchanged.

## Applied LF correction; lookup experiments closed — 5 October 2026

**Source292 is applied** with 532 verified source inputs and file modes. Read the
[buffer-line checkpoint](docs/runtime-representation-buffer-lines.md). Rope metadata
now counts LF, matching GNU; the original backward scan remains. Warning-free
strict checks, 290 selected passes per profile/two existing TTY ignores, and
47 ordinary actual GNU comparisons pass. The full broad probe improves from
155 differing rows to 60 unchanged display-column failures; those remain failures.

**Source294 and source295 are rejected and unapplied.** Indexed294 slows undo
37.691% versus corrected292. The narrower295 shortcut shows no useful measured
advantage; its undo and bytecode medians are also slower than292. Both complete
three-way cohorts retain all 576 process answers/modes, all matching GNU. Across
six samples for original290 and corrected292, the raw median body geometric
ratio is 1.001869 and corrected292's paired GNU ratio geometric mean is 2.97884×.
All slower cases remain. This correctness repair makes no performance-gain claim;
calibrated every-case acceptance and real allocation counters remain required.

All local line validation/timing coordinators are closed. Do not restart their
write-once producers. The next measured lead is the redundant fixed-argument copy
in native `DirectFuncallTarget::invoke`; GNU copies only when padding is needed.
Its roots, error order, optional padding and nonlocal exits must remain intact.

Predecessor source290 (`b870a848`) has a
[closed Linux frozen audit](docs/handover/2026-09-30-shared-reader-draft/source290-complete-linux-frozen-manifest.json):
519 files, 7,928 equal outcomes and 1,038 successful processes. Its Linux Rust run
retains 2,288 passes/one GNU census reference failure before Emaxx. These results
do not certify the new LF configuration. Exact-source Linux validation is next;
complete macOS validation is unstarted. Main and all full-goal requirements remain
unchanged. The sections below preserve earlier publication states.

## Fresh-build correction — 5 October 2026

The [stale-cache evidence and source294 launch](docs/handover/2026-09-30-shared-reader-draft/source293-stale-cache-and294-launch-manifest.json)
supersede source293's validation status below. Cargo reused source292's exact
binaries in gate and release (`fresh=true`); those runs do **not** validate the
indexed lookup. Both v3 coordinators were terminated before any timing. Preserve
their outputs and withdrawal receipts; do not resume them.

**Source294 has exactly the same 532 source inputs as source293**, in
`target/runtime-goal/recovered-2026-10-05/buffer-line-lookup-fresh/emaxx`. It explicitly
cleans the package in gate and release and requires freshly compiled artifacts
and different test binaries from source292. Its build/validation is running;
no candidate speed or validation pass is claimed. Current coordinator receipts
are `buffer-selection-fresh-state.json` and `buffer-performance-sequence-fresh-state.json`,
followed by their final results, under `target/runtime-goal/resume-2026-09-28`.
Source292's separately verified fresh build and 290 selected passes per profile
remain valid. Applied source290 and all goal requirements are unchanged.

## Current buffer-line drafts — 5 October 2026

Read the [profile-led buffer continuation](docs/runtime-representation-buffer-lines.md)
first. Source292 isolates the LF-only line-metadata correction; source293 also
replaces repeated backward scanning with the rope's existing index. Both portable
532-input drafts remain unapplied. Source292 has zero-warning strict checks,
290 selected passes per profile, the two existing TTY ignores and its own fresh
ordinary executable/image. Source293 validation is running; ordinary comparisons
and the queued three-way full16 diagnostic remain pending. Inspect the v3
coordinator receipts before continuing. The original broad probe's display-column
failures and both wrapper/setup errors remain explicit; no speed or full pass is claimed.

**Applied runtime remains source290 (`b870a848`)**. Its
[closed Linux Rust failure](docs/handover/2026-09-30-shared-reader-draft/source290-complete-linux-rust-failure-manifest.json)
records 2,288 passes and one recurring GNU reference assertion, before Emaxx:
first empty-record census −7 versus 2. Remaining 756 library names and both Cargo
stages never run. The cause stays unresolved. Frozen37269857506 is still running;
full source290 macOS remains unstarted. Main and the full-goal requirements remain
unchanged. The records below preserve earlier checkpoint publication states.

## Applied integer/VM checkpoint — 5 October 2026

Read the [integer/VM checkpoint and profile qualification](docs/runtime-representation-integer-vm-draft.md)
first. **Source290 is applied and pushed as `b870a848`**, with 528 exact inputs,
warning-free strict checks, **325 selected passes per profile** and **46 ordinary
GNU comparisons**, including 588 numeric boundary rows. It removes redundant VM
dispatch and stack updates and outlines unchanged bignum allocation.

The [closed second comparison and application review](docs/handover/2026-09-30-shared-reader-draft/source290-inlining-review-and291-results-manifest.json)
verifies all 288 new process answers/modes, alongside the preserved first192.
Across both full16 diagnostic cohorts, bytecode-call median time falls **14.264%**
versus source289; the raw median body geometric ratio is **0.97158**. Undo and
Bindat remain 1.943% and 1.499% slower. All unfavorable samples remain. The paired
GNU ratio geometric mean is **2.95454×**; this does not satisfy GNU parity or the
unchanged calibrated every-case 3% criterion. **Source291 remains unapplied**:
its 325 passes per profile and46 ordinary comparisons are closed, but compiler
chosen inlining gives a worse overall measured tradeoff. The original source290
hold and unfinished291 snapshot remain historical, superseded by this review.

Source289's complete Linux results are closed: **3,145 Rust passes / two existing
ignores**, and [519 files /7,928 matching frozen outcomes](docs/handover/2026-09-30-shared-reader-draft/source289-complete-linux-frozen-manifest.json).
Earlier GNU census failures and source276's Emaxx negative remain unexplained.
[Source290 Linux CI](docs/handover/2026-09-30-shared-reader-draft/source290-full-validation-launch.json)
selects exact `b870a848`: Rust37269853253 and frozen37269857506 are in progress at
publication; full290 macOS is unstarted. All local selected/profile/timing jobs
are closed. Six separate profiles verify120 results/modes, but include pre-body
GC; all body GC deltas are zero, so collector samples are not timed-body costs.
Next follow the measured ordinary evaluator/native/buffer leads. Main remains
`21d20f0e`, PR79 stays draft, and every remaining full-goal requirement stays open.

## Applied native/bytecode call checkpoint — 5 October 2026

Read the [source289 call-entry checkpoint](docs/runtime-representation-call-entry.md)
first. **Source289 is applied and pushed as `ae1fcb0f`**, with 526 verified inputs,
exact portable replays, warning-free strict checks, **274 selected passes in each
profile** and **45 ordinary same-input GNU comparisons**. Fixed native arguments
avoid an intermediate copy; closure entry reads slots once; duplicate transient
activation storage and repeated Bcall decoding are removed. All earlier tests,
fixtures and both required suspended-root contracts remain.

The [closed performance archive](docs/handover/2026-09-30-shared-reader-draft/source289-native-vm-calls-performance-manifest.json)
verifies all **288 process results and modes** across three unchanged full16 pilots
for source286/288/289. Source289 bytecode-call median body time is **15.350% lower**
than source286, bytecode-to-native **3.540% lower**. Unfavorable cases are retained;
the small pilot establishes no broad statistical gain. The paired GNU ratio
geometric mean is **2.97399×**, so parity is still far from achieved. Counters,
calibration and the unchanged 3% every-case criterion remain open. New separate
profiles identify ordinary VM stack/constant, frame and native-transition costs.

[Closed source286 platform evidence](docs/handover/2026-09-30-shared-reader-draft/source286-complete-platform-results-manifest.json)
verifies **3,131 macOS Rust passes / two existing ignores**, **226 terminal
scenarios / 686 exact comparisons** and **7,928 Linux frozen matches**. macOS
frozen retains six strict feature-diagnostic differences. Linux Rust retains
2,285 passes / one GNU census reference failure, with 756 library names and both
Cargo stages unexecuted. The failure occurs before Emaxx; its cause and the
source276 Emaxx census negative remain unresolved. Supervisor 10785 has exited.
These predecessor results do not certify source289.

[Source289 Linux CI](docs/handover/2026-09-30-shared-reader-draft/source289-full-validation-launch.json)
selects exact `ae1fcb0f`: Rust **37265044228**, frozen **37265047878**. Both are
in progress at publication; inspect receipts before claiming completion. No
complete source289 macOS run has started. All local selected/profile/timing
pipelines are closed. Main remains `21d20f0e`, PR79 remains draft. Continue
profile-driven work without unrelated correctness expansion. Sblocks are already
applied in source279; remaining architecture, accounting, ownership, final validation and
the complete goal remain unfinished. The older sections below retain history.

## Preceding VM checkpoint — 5 October 2026

Read the [direct VM dispatch checkpoint](docs/runtime-representation-vm-dispatch.md)
before continuing. **Source286 is applied and pushed as `654aca96`**, with 522
inputs replayed from source284 and main. Warning-free strict checks, **118 selected
passes in each profile** and **43 ordinary exact GNU comparisons** pass. Both
suspended-root contracts and all prior fixtures remain. Direct canonical opcode
dispatch removes repeated decoding; GNU remainder operand coercion is repaired.
Source287 preserves a separately validated correctness-only performance baseline.

The [closed repeated full16 pilot](docs/handover/2026-09-30-shared-reader-draft/source286-vm-dispatch-performance-manifest.json)
verifies **288 processes with actual equal GNU results/modes**. Bytecode-call
median time falls **18.2%**, bytecode-to-native **4.4%** versus the corrected baseline.
Other workloads include slower medians; the overall raw median geometric ratio
is 0.9961, so no meaningful overall speedup is claimed. Optimized Emaxx remains
**3.0421× GNU** by the paired-ratio geometric mean. Counters, calibrated repeated
acceptance and the unchanged 3% criterion remain open. Post-change profiles still
show VM/call/frame/native-transition costs. Continue measured ordinary-path work,
without unrelated correctness expansion or weakening existing contracts.

[Closed source284 platform evidence](docs/handover/2026-09-30-shared-reader-draft/source284-complete-platform-results-manifest.json)
now verifies **3,130 macOS Rust passes / two existing ignores** and **7,928 Linux
frozen matches**. macOS frozen remains 7,915 matches / six strict diagnostic
differences. Its terminal run stops after eight matching comparisons at GNU
`org-fold` startup; 678 comparisons never run. A separate bounded longer retry
finishes in **13.195 seconds with the actual screen equal**. The original failure
remains. Its Linux Rust failure is still 2,284 passes / one GNU census reference
failure, with 756 library names and both Cargo stages unexecuted. No pass is
inferred for the failed or unrun work; source276's Emaxx census failure stays open.

[Source286 full validation](docs/handover/2026-09-30-shared-reader-draft/source286-full-validation-launch.json)
selected exact `654aca96`: Linux Rust **37254821073**, frozen **37254829602**, and
macOS supervisor **10785** in
`target/runtime-goal/recovered-2026-10-03/vm-dispatch-full/emaxx`. All are closed;
read the audited results above and the current call-entry checkpoint. Selected source286 and corrected source287
checkouts remain frozen. Main stays `21d20f0e`, PR79 stays draft, and the complete
goal remains incomplete. The linked checkpoint records original setup/fixture/lint
failures, all unfavorable timings and the portable reproduction evidence.

## Preceding printer checkpoint and performance baseline

Read the [printer/reader checkpoint and performance baseline](docs/runtime-representation-printer-draft.md)
before continuing. **Source284 is applied and pushed as `9c5b5356`**, with 520
verified inputs and exact portable replays from source279 and main. Its
[closed selected evidence](docs/handover/2026-09-30-shared-reader-draft/source284-reader-printer-selected-manifest.json)
verifies warning-free strict checks, six targeted controls in each profile and
42 ordinary byte-for-byte GNU comparisons. The native constant failure is repaired;
all 198 generated rows / 4,088,269 bytes and 679 reader bounds cases match.
Every prior fixture and test remains. This is a bounded checkpoint, not completion.

Canonical bytes now pass through printing and actual string/native reader input.
The loader follows GNU make_string/Fread rather than lossy UTF-8 conversion.
Bounds reuse GNU-compatible validate_subarray with original error arguments.
Remaining symbol, positioned-reader and formatting adapters, ownership/accounting,
real intervals, unified pure storage and the complete architectural goal stay open.

**Performance work has started.** The [closed unchanged 16-workload pilot and profiles](docs/handover/2026-09-30-shared-reader-draft/source284-core-performance-pilot-manifest.json)
verify all 32 processes, matching actual results and execution modes. One paired
sample per workload gives a 3.05× geometric mean Emaxx/GNU body-time ratio;
bytecode-to-native is 9.58× and bytecode calls 6.78×. All ratios are unfavorable
and retained. This is diagnostic, not parity certification; Emaxx allocation
counters explicitly report unavailable. The locked workloads and 3% ceiling
are unchanged. Profiles identify VM dispatch and call setup as the next targets.
Current user direction is to pursue that measured work after this bounded repair,
without continuously adding unrelated correctness probes. Preserve required semantics.

The [full-validation launches](docs/handover/2026-09-30-shared-reader-draft/source284-full-validation-launch.json)
select exact runtime `9c5b5356`. The [closed Linux Rust failure](docs/handover/2026-09-30-shared-reader-draft/source284-complete-linux-rust-failure-manifest.json)
retains **2,284 passes / one GNU reference-assertion failure**: the first empty-vector
census is -9 instead of zero, byte-identical to the earlier source272 GNU failure.
That control does not reach Emaxx; all four new printer/reader controls pass.
The remaining 756 library names and both Cargo stages never run. The cause stays
unresolved; no expectation or verdict changes. Linux frozen37091394500 and macOS supervisor **48303** are now closed;
read the platform results above and the linked current VM checkpoint. The original
macOS selected checkout and complete checkout retain their exact source284 inputs. The selected checkout
`target/runtime-goal/recovered-2026-10-03/reader-bounds-ascii/emaxx` stays frozen.
Main remains `21d20f0e`, PR79 remains draft and the full goal remains active.

Source282's 217 focused passes/two existing ignores and 1,073 affected passes per
profile are predecessor evidence. It is never applied because a subsequent bounds
negative is real. Source283 repairs that error but its new fixture fails at a GNU
reference assertion due to locale-dependent literal argv decoding. Source284
changes only that fixture to numeric character construction and matches the original
helper's actual GNU output. The source282 missing-GNU-path and log-path setup
failures, source283 failed reference, and earlier source280/281/native negatives
remain recorded in the linked archives. Do not reclassify failed or incomplete runs.

Source279's closed Rust validation remains 3,126 macOS / 3,138 Linux passes,
two existing ignores each, and its Linux frozen run matches all 7,928 outcomes.
The [closed macOS frozen/terminal audit and GNU retry](docs/handover/2026-09-30-shared-reader-draft/source279-macos-frozen-terminal-and-retry-manifest.json)
supersede supervisor77583's prior running state: 7,915 frozen matches / six strict
feature-diagnostic mismatches; terminal has 164 fully matching scenarios / 318
exact comparisons before GNU readiness times out. There are 368 unexecuted
comparisons and 61 unstarted scenarios. The unchanged failing scenario separately
completes in 14.224 seconds under a bounded longer wait, all three screens equal.
Never slow Emaxx to reproduce a GNU timeout; require actual equal answers.
Source276's original Emaxx census failure remains unexplained. Source270's prior
226-scenario / 686-comparison terminal pass does not certify the final runtime.

The preceding runtime **source272**, pushed as **`eb286e23`**, has **510 verified inputs**. Its
[closed borrow-root repair evidence](docs/handover/2026-09-30-shared-reader-draft/source272-string-borrow-selected-manifest.json)
verifies zero-warning strict checks, **206 focused passes / two existing ignores**
and **1,062 affected passes in each profile**. Every original selector, test,
GNU fixture and expected byte is preserved. Both portable repair patches and
the preceding test-only negative replay all 510 inputs and file modes exactly.

Source271 exposed a Rust ownership defect outside the frozen Lisp fixtures:
an off-stack shared string guard kept its header alive while its cons property
was reclaimed. Its original **two passes / one failure** remains preserved.
Source272 scans the existing borrow metadata before marking, traces shared
guards through the ordinary property graph and weak-table fixed point, and
rejects an exclusive guard before advancing the epoch or sweeping anything.
Controls verify survival, weak-key retention, reclamation after release and
exclusive-borrow rejection, including both permanent empty roots. No per-access
lookup or retained registry is added. The extra GC scan and temporary vectors
have not been timed; broader public ownership/serialization remains open.

The selected checkout `target/runtime-goal/recovered-2026-09-30/string-borrow-roots/emaxx`
is frozen. Receipts/helpers are under `target/runtime-goal/resume-2026-09-28`.
The [full Linux Rust failure](docs/handover/2026-09-30-shared-reader-draft/source272-complete-linux-rust-failure-manifest.json)
retains **2,279 passes / two GNU reference-assertion failures**. The first empty-record
and empty-vector GC samples are nine slots below their unchanged expectations;
all later samples match. Neither failed control reaches its Emaxx comparison.
The remaining 751 library tests and both Cargo stages never run. Actual GNU inputs
match the prior passing and failing runs; the cause remains unresolved.

The [full Linux frozen failure](docs/handover/2026-09-30-shared-reader-draft/source272-complete-linux-frozen-failure-manifest.json)
verifies **519 files / 7,927 matches / one mismatch / 1,038 successful processes**.
GNU reports an unexpected Eglot Rust-completion JSON-RPC timeout; Emaxx passes
that test and has no unexpected outcome across all 7,928 results. This is the
same failure signature seen in source244, without an established cause.
Both complete runs remain failed. No expectation, timeout or comparison rule changes.

MacOS supervisor **27731 was withdrawn while waiting, before any validation stage**,
to prioritize the newer draft below. `source272-full-withdrawal.json` supersedes
its retained original waiting-state receipt. Do not restart it implicitly.
The two source272 checkouts remain unchanged. Full validation is still required
on the final selected runtime; selected checks do not establish a full or
performance result. Main remains `21d20f0e`, PR79 remains draft, and the full goal
is active and incomplete.

## Separate GNU string-block draft: source276

The [portable source276 draft and original preparation failures](docs/handover/2026-09-30-shared-reader-draft/source276-string-sblocks-draft-manifest.json)
replay all **511 inputs and file modes** from `b5a1558c` and main. It is isolated in
`target/runtime-goal/recovered-2026-09-30/string-data-pools/emaxx`; it was prepared
against source272 and is superseded by source279. Strict formatting, all-target checking, Clippy and diff checks
pass with zero warnings. **Seven gate controls pass**, including actual compaction,
block reclamation, borrowed-pointer stability, aligned resizing, property roots,
weak references, release and pure-string survival. These are bounded controls.

Constructors now install a stable header before GNU-style sdata owner pointers.
Small bytes share 8,184-byte blocks; large bytes have separate blocks. GC retires
dead entries, compacts live unborrowed storage and frees unused blocks. Character
construction, copies, concatenation and resizing fill final storage directly.
A block containing a live Rust content borrow stays stationary until the guard
ends. Pure string bytes occupy separate permanent, immovable blocks. This concrete
Rust borrowing constraint and its unmeasured scan/retained-slack cost are recorded;
there is no new per-read lookup or registry. Real intervals, unified pure storage,
physical accounting, public ownership, symbol authority and VM work remain open.

**Queue67370 has exited.** It records 208 focused passes / two existing ignores
and 1,063 affected passes / one Emaxx first-vector census failure. Release never
starts. The [source279 recovery](docs/runtime-representation-string-allocation-recovery.md)
preserves that failure and repairs a separate allocation abort found by new
controls. Keep source276 and its executed helpers unchanged. Source273's two test argument-type errors and
source274/275's strict Clippy failures remain in their original checkouts and
portable patches; no runtime tests ran on those failed predecessors.

## Preceding storage checkpoint: source270

The preceding storage runtime **source270** was pushed as **`0ecf6e11`**, with
**510 verified inputs**. Its [closed selected evidence and full-validation launches](docs/handover/2026-09-30-shared-reader-draft/source270-string-storage-selected-manifest.json)
verify zero-warning strict checks, **203 focused passes / two existing ignores**
and **1,059 affected passes in each profile**, plus **33 ordinary exact GNU matches**.
All original controls and expected bytes remain, including all 12,600 prefix rows.
The archived ordering, version, range/casing, empty identity and pure-storage
negatives now match unchanged GNU output. Counts overlap and are not summed;
this does not establish universal correctness or a full-platform pass.

Strings now use a direct 32-byte GNU-layout header plus 16 bytes of checked-borrow
and allocator metadata, dedicated cells and packed property spans. Both normal
empty forms are canonical; pure copies are distinct permanent objects with GNU
write protection, copy/no-op/error order and dump/template identity behavior.
Comparison and casing follow actual GNU C paths and live tables. Data still uses
individual boxes; sblocks, real intervals, symbol authority, physical accounting,
public ownership, VM stack/profiling and the locked performance criteria remain open.

The [complete Rust audits on both platforms](docs/handover/2026-09-30-shared-reader-draft/source270-complete-rust-platforms-manifest.json)
verify **3,118 macOS / 3,130 Linux passes**, with **two existing ignores each**,
all 3,021/3,029 library names, native artifact identity and retained Rust/GNU inputs.
Linux [Rust36930970705](https://github.com/rayfdj/emaxx/actions/runs/36930970705)
is complete on exact `0ecf6e11`. The [complete frozen audits](docs/handover/2026-09-30-shared-reader-draft/source270-complete-frozen-platforms-manifest.json)
verify all 519 files and 1,038 successful processes per platform. Linux has
**7,928 matching outcomes**. Pinned macOS has **7,915 matches / six strict
build-feature skip-diagnostic differences**, which remain failures. Raw inventories,
paired outcomes, execution hashes and actual GNU inputs are checked; all 177
compiler tests pass in both editors. Expected failures and skips are not passes.
Supervisor **3079 has exited**. Its audited terminal result matches all 226
scenarios and 686 original comparisons in the unchanged clean checkout
`target/runtime-goal/recovered-2026-09-30/string-storage-full/emaxx`.
Read the latest recovery archive for the closed audit. These results do not certify source272 or
source276, memory or performance. The source271 borrow-root negative remains
preserved and is repaired separately in source272.

## Preceding source261 checkpoint and storage development

The preceding runtime **source261** was pushed as **`eddfe782`**, with **488 verified
inputs**. Its [closed selected evidence and full-validation launches](docs/handover/2026-09-30-shared-reader-draft/source261-common-string-selected-manifest.json)
verify zero-warning strict checks, **189 focused passes / two existing ignores**
and **962 affected passes in each profile**, plus **22 ordinary exact GNU matches**.
The common-string and comparison/hash changes below are now applied. Every earlier
failed draft, invalid fixture and evidence-check failure remains recorded.

The [complete macOS Rust audit](docs/handover/2026-09-30-shared-reader-draft/source261-complete-macos-rust-manifest.json)
verifies **3,107 passes / two existing ignores**, all 3,010 library names, native
artifact identity and four retained executable/image files. The clean checkout
`target/runtime-goal/recovered-2026-09-30/common-strings-full/emaxx` remains at
`eddfe782`, with all 488 inputs unchanged. Supervisor **31023** has completed
full terminal validation after the frozen comparison below. Linux
[Rust36904097575](https://github.com/rayfdj/emaxx/actions/runs/36904097575) and
[frozen36904105841](https://github.com/rayfdj/emaxx/actions/runs/36904105841) are
complete on exact `eddfe782`. The [closed Linux audit](docs/handover/2026-09-30-shared-reader-draft/source261-complete-linux-manifest.json)
verifies **3,119 Rust passes / two existing ignores**, all 3,018 library names,
native artifact identity and retained inputs, plus **519 frozen files / 7,928
matching outcomes / 1,038 successful processes**. Each editor reports 7,670 passes,
47 expected failures and 211 skips; all 177 original compiler tests pass. Every raw
execution hash and actual GNU input is checked. The
[complete macOS frozen audit](docs/handover/2026-09-30-shared-reader-draft/source261-complete-macos-frozen-manifest.json)
retains **7,915 matches / six strict mismatches**, all 519 files and 1,038 successful
processes. Both editors have 7,623 passes, 46 expected failures and 252 skips; all
177 compiler tests pass. The same six build-feature skip diagnostics differ as in
source249. Every raw outcome, execution hash and retained GNU/Emaxx input is checked.
No comparison rule or reported feature changes. The
[complete terminal audit](docs/handover/2026-09-30-shared-reader-draft/source261-complete-macos-terminal-manifest.json)
verifies **226 scenarios / 686 matching comparisons**: 658 screen and 28 filesystem
comparisons, all original labels, 488 source inputs and ten execution inputs. It uses
the exact executable/image from the frozen run. No source261 supervisor remains running.
Main remains `21d20f0e`, PR79 is draft, and the architecture/performance goal is open.

The [separate source262 ordering draft and negatives](docs/handover/2026-09-30-shared-reader-draft/source262-string-ordering-draft-manifest.json)
retain **154 wrong results among 1,024 symbol/storage ordering rows**, plus **56
wrong results among 400 version rows** on source261. Four ordering results regressed
after source207 and by source249; the exact introducing commit is unestablished.
Source261 and source249 agree throughout that ordering matrix. The version matrix
is unchanged from main. These defects remain open, despite earlier frozen passes.
Source262 follows actual `fns.c:string_cmp` and `lib/filevercmp.c`, uses canonical
bytes and Lisp symbol names, and removes the host-text comparison adapter. Its
**492 inputs** replay exactly and strict checks pass with zero warnings. Queue
**38430 was withdrawn while waiting, before any runtime stage**, in favor of the
successor below. Its source, helpers and original queued state remain unchanged.
No source262 runtime test ran; it remains an unapplied precursor.

The [source263 range/casing successor](docs/handover/2026-09-30-shared-reader-draft/source263-string-ranges-draft-manifest.json)
retains all source262 changes and adds GNU-confirmed `compare-strings` and numeric
casing repairs. New ordinary probes find **12 range/error differences**, **134
character-comparison differences**, **seven live-case-table comparison differences**
and **nine numeric casing differences**. Range, live-table and numeric outputs match
main exactly; main's character matrix exits255 at its unsupported extended-character
constructor, which is retained as a failure. The repair reads canonical bytes,
promotes octets before casing, follows live tables with actual keys and preserves
GNU's type/range validation order. All **500 inputs** replay exactly and strict
checks pass with zero warnings. Queue **90568** has exited: **197 focused passes /
two existing ignores**, then **1,053 affected passes / one failure**. The original
Unicode comparison expected dumped GNU case tables in a bare interpreter. Actual
GNU C initializes only ASCII there; loadup installs Unicode. The successor below
preserves the original dumped assertions and adds bare/live-table coverage.
Release and ordinary stages did not run. Keep failed source263 and its helpers unchanged.
The [additional boundary control](docs/handover/2026-09-30-shared-reader-draft/source263-ordering-boundary-control-manifest.json)
matches GNU/source261 on **12,600 comparisons**, covering 14 prefix lengths around
machine-word boundaries and 30 character/storage cases. Queue **90569** stopped
before any stage because90568 failed. No source263 prefix comparison or complete
selected pass is inferred. Its raw failure and executable/image identities are in the
[storage preparation archive](docs/handover/2026-09-30-shared-reader-draft/source268-string-storage-draft-manifest.json).

The [queue correction](docs/handover/2026-09-30-shared-reader-draft/source263-fixture-queue-repair-manifest.json)
preserves original waiting supervisors64601/87187, withdrawn before runtime execution.
Their wrapper named a nonexistent fixture; the corrected queue uses the unchanged
`string-ordering-symbol-storage` fixture. All 28 fixture paths are verified. Source,
tests, expected bytes, selectors, timeouts and comparison rules are unchanged.
The closed receipts are `source263-fixture-validation-*` and
`source263-boundary-fixtures-*`. Original helpers and waiting states remain
historical evidence; do not restart these failed or withdrawn queues.

The [current empty/pure string baseline](docs/handover/2026-09-30-shared-reader-draft/source261-string-storage-baseline-manifest.json)
finds **six wrong empty-identity rows among 12** and **57 wrong pure-storage rows
among 96**, including 28 operation/poststate differences. Compared with source249,
seven previously matching whole rows now differ and six now match. All seven new
whole-row regressions involve `purecopy` reusing the ordinary empty unibyte string;
the exact introducing source between249 and261 is not established. Nonempty pure
writes already differ in earlier checkpoints. Source174/main and207 agree exactly.
The original syntax/scope-invalid probes are preserved and excluded; the valid raw
and explicit-value fixtures overlap and their counts must not be added together.
Source263 does not repair these additional gaps. Subsequent string-storage work must
restore distinct pure allocation and write protection, supply the empty multibyte
singleton, and preserve GNU's empty `clear-string`, GC and dump behavior. A complete
frozen pass alone cannot certify those contracts or make this branch main-ready.

The storage successor **source270** was developed separately in
`target/runtime-goal/recovered-2026-09-30/string-storage/emaxx`. The
[source264–268 preparation and failures](docs/handover/2026-09-30-shared-reader-draft/source268-string-storage-draft-manifest.json)
and [source269 caller correction](docs/handover/2026-09-30-shared-reader-draft/source269-string-storage-caller-repair-manifest.json)
preserve the earlier drafts. The [current source270 patch and failures](docs/handover/2026-09-30-shared-reader-draft/source270-string-storage-empty-copy-manifest.json)
retain exact incremental/main patches and all **510 inputs**. Strings now have a
direct 32-byte GNU-layout header plus 16 bytes of checked-borrow/allocator metadata.
The implementation supplies two normal empty singletons, distinct permanent pure allocations,
write protection and GNU no-op/error order, and preserves dump/template identities.
Property spans use one packed allocation. Data still uses individual boxes, and
real sblocks, interval trees, physical accounting and public ownership remain open.

Source264/265 compiler failures and source266 Clippy failure are retained. Source267
passed strict checks but did not run. Source268 passed strict checks, then had
**199 focused passes / four failures / two existing ignores**. All four failures
were at the GNU precondition: the new expected-output callers retained one trailing
newline. Source269 changes only those four callers to the existing `trim_end`
convention; GNU fixtures and the shared comparator are unchanged. Source269 then
had **201 focused passes / two failures / two existing ignores**. Empty identity
and all 144 property rows pass; the 96-row pure fixture matches 94 rows. Copying an
empty pure string incorrectly retained its read-only header. A separate assertion
could not load its native image while the test still owned the first image.
Source270 follows GNU `Fcopy_sequence` through ordinary constructors for every
string and releases the first test interpreter before the independent image load.
All assertions remain. Its strict checks pass with zero warnings.
Queue **98240** has completed all selected stages: **203 focused passes / two
existing ignores and 1,059 affected passes in each profile**, plus **33 ordinary
GNU matches**. All source270 raw verdicts, complete inventories, retained artifacts
and source bytes are audited. The source261 and failed draft checkouts remain
unchanged. Source270 is now applied as described above; its earlier preparation
archives retain their original pending state and do not substitute for the closed audit.

## Preceding source249 validation

The preceding **source249**, pushed as **`99e61fca`**, has **476 inputs** matching the isolated
`coding-post-read/emaxx` candidate. Its [closed selected validation](docs/handover/2026-09-30-shared-reader-draft/source249-selected-and-setup-manifest.json)
verifies zero-warning strict checks, **172 focused passes / two existing ignores**
and **955 affected passes in each profile**, plus **fifteen ordinary GNU matches**.
The post-read hook now receives decoded character counts. ASCII string decoding
returns early, drops properties on a copied result and preserves NOCOPY identity,
following GNU.
The original negatives and the first checkout-setup failure remain retained.
Complete source249 Linux/macOS Rust, Linux frozen and terminal results are now audited
below. The preceding source248 results do not
certify this newer source.

The [closed Linux and Darwin frozen evidence](docs/handover/2026-09-30-shared-reader-draft/source249-linux-and-darwin-frozen-manifest.json)
verifies exact pushed runtime `99e61fca`. Full Linux
[Rust36879095484](https://github.com/rayfdj/emaxx/actions/runs/36879095484) and
[frozen36879103495](https://github.com/rayfdj/emaxx/actions/runs/36879103495) are
complete: **3,112 Rust passes / two existing ignores**, and **519 files / 7,928
matching frozen outcomes / 1,038 successful processes**. Each editor has 7,670
passes, 47 expected failures and 211 skips. Raw verdicts, complete inventories,
execution hashes and actual GNU inputs are verified; historical failures remain.

The [complete macOS Rust and oracle adoption record](docs/handover/2026-09-30-shared-reader-draft/source249-macos-and-oracle-adoption-manifest.json)
closes **64326** with **3,100 passes / two existing ignores**, native artifact
identity, all 3,003 raw library names and four retained inputs verified. The
[complete terminal audit](docs/handover/2026-09-30-shared-reader-draft/source249-complete-rebuilt-terminal-manifest.json)
closes **12347** with **226 scenarios / 686 matching comparisons**: 658 screen and
28 filesystem comparisons, every original label, 476 source inputs and ten execution
inputs verified. It uses the same gate executable/image as the rebuilt-oracle frozen
comparison. No source249 validation supervisor remains running.
Main remains `21d20f0e`, PR79 is draft and the full goal remains open.

GNU has been [rebuilt from the recorded recipe](docs/handover/2026-09-30-shared-reader-draft/gnu-macos-rebuild-20261001-manifest.json)
at pristine revision `636f166c`, with executable SHA-256 **`29cfb20d…`**.
Capabilities and configure options match the retained source/ABI-matched build;
fresh native ABI, C primitive and DEFSYM manifests are byte-identical to the
committed files. The binary, dump, configuration and Makefile are retained.
The task branch now explicitly adopts **`29cfb20d…`**, as already tested in
candidate **`1c0898fc`**. Its old pin and local configuration are retained.
Fresh GNU discovery from all 519 completed full-run reports renders the original
canonical inventory byte for byte. Runtime inputs, Linux pin, selectors and
timeouts are unchanged. Frozen retry **58661 has exited with a strict failure**:
**7,915 matching / six mismatching outcomes**, all 519 files and 1,038 processes
complete. Both editors have 7,623 passes, 46 expected failures and 252 skips.
The six differences are existing `system-configuration-features` text in skipped
seccomp diagnostics. Emaxx deliberately reports only actual capabilities; no
feature string or comparison rule was changed. All 177 original compiler tests
pass in both editors. This result is not relabeled a complete frozen pass.
The first launch's missing target-ownership marker failure and cache are preserved.

Separately, **source256** is an unapplied common-string draft in
`target/runtime-goal/recovered-2026-09-30/common-strings/emaxx`, with **480 inputs**.
Its [portable patches and evidence](docs/handover/2026-09-30-shared-reader-draft/source256-common-strings-draft-manifest.json)
replay exactly from `b8e5ad20` and main. It removes the plain-string arena,
binding/callback conversion copies and VM text adapter, and follows GNU's actual
`fillarray` byte-length/property rules. Two ordinary source249 negatives and
unchanged GNU expectations are retained. Source255's selected run has **181 passes,
one invalid Unicode bytecode fixture failure and two existing ignores**. GNU
confirms the fixture correction in source256; every original assertion remains,
plus the rejected Unicode case. Source256 passes all strict checks with zero
warnings. Queue **17749** has exited. Its [focused gate result](docs/handover/2026-09-30-shared-reader-draft/source256-focused-gate-manifest.json) is audited:
**182 passes / two existing ignores**, all 184 selectors and retained executable/image
verified. The [complete affected gate audit](docs/handover/2026-09-30-shared-reader-draft/source256-broad-failure-and259-focused-manifest.json)
records **956 passes / one native-bytecode fixture failure**, all 957 names checked.
GNU confirms that fixture's three intended opcodes were five bytes of Unicode;
the actual unibyte form returns 42 and the Unicode form is rejected. Release and
ordinary stages did not run. The failure and its executable/image remain retained.
Compact string headers, symbol authority, physical
accounting, ownership and measured performance remain unfinished.
The [new comparison negative](docs/handover/2026-09-30-shared-reader-draft/source249-string-comparison-gaps-manifest.json)
finds six wrong results in a 64-pair string matrix. The same fixture had twelve
wrong results on source174/main and source207, with no newly wrong result in
source249. The remaining `string-equal`/`equal-including-properties` projection
errors are still present in source256.

The [separate source259 comparison/hash draft](docs/handover/2026-09-30-shared-reader-draft/source259-string-comparison-draft-manifest.json)
has **488 exactly replayed inputs** and zero-warning strict checks. It compares
actual string bytes/counts, uses actual Lisp symbol names and GNU's positioned-symbol
policy, and compares/hashes interval values with ordinary equality. String hashing
also reads bytes directly. Four unchanged GNU fixtures retain the storage, symbol,
property and hash-table negatives; an existing dump test gains byte-distinct keys.
Source257's missing-test-import failure and source258's withdrawal before runtime
execution are retained. Queue **22381** has exited: its [focused gate](docs/handover/2026-09-30-shared-reader-draft/source256-broad-failure-and259-focused-manifest.json)
passes **188 tests / two existing ignores**; its complete broader gate records
**961 passes / one failure** in the same invalid native fixture. Release and ordinary
stages did not run. Every original selector and failed artifact remains retained.

The subsequent source261 corrects only that native test, following ordinary GNU
constructor evidence, with all original assertions and a Unicode rejection check.
Source260's new-test unwrap lint failure is preserved; source261 uses an explanatory
`expect_err`, with no lint allowance. Its **488 inputs** replay exactly and strict
checks have zero warnings. Both focused profiles pass **189 tests / two existing
ignores**; all **962 affected gate tests pass**. The first release auditor stopped
because retention included the preceding gate image as well as the release image.
The corrected auditor verifies all three retained inputs; no test or source changes.
Continuation **30525** has exited: all **962 affected release tests** and **22 ordinary
GNU comparisons** pass. The [complete selected audit](docs/handover/2026-09-30-shared-reader-draft/source261-common-string-selected-manifest.json)
checks both inventories, every raw verdict, original fixtures and retained inputs.
Source261 is applied and pushed as `eddfe782`; the full macOS/Linux runs above remain
active. No full-runtime, memory or performance pass is inferred from selected checks.
See the common-string continuation for exact evidence and remaining requirements.

The preceding **source248**, pushed as **`fa7578ac`**, has **472 inputs** matching the frozen
`target/runtime-goal/recovered-2026-09-30/coding-property-order/emaxx` checkout.
The two property repairs below follow the actual GNU C lookup paths; their
strict, selected and complete original compiler comparisons pass. Complete
macOS/Linux Rust and pinned Linux frozen comparisons now pass as detailed below.
Full terminal retry **14540** has exited and passes all **226 scenarios / 686 comparisons**.
The original GNU startup failure and pre-existing conversion gaps remain documented.
Further architecture work must start from this tested checkpoint and preserve its coverage.
The [original source249 post-read draft](docs/handover/2026-09-30-shared-reader-draft/source249-post-read-draft-manifest.json)
has **476 frozen inputs**, with both patches replayed exactly. Its two ordinary
negative fixtures expose byte-count hook arguments and a missing ASCII early
return; both also fail identically on retained source174/main and source207.
The repair follows `coding.c:decode_coding_object/code_convert_string`, preserves
the exact GNU outputs. Queue **15814** exited after strict
checks passed and selected gate tests reported 162 passes, ten setup failures and
two existing ignores. The new checkout lacked its required `../emacs` link.
Restoring that link makes the same executable and all 174 unchanged selectors pass:
172 passes / two existing ignores. The original failure and its executable/image
remain retained. Continuation **17624** has exited; all selected results are now
audited and the repair is applied to the root. Read `source249-*` receipts;
the original draft's prepared auditors are historical, not passing results.
The preceding **source246**, pushed as **`aaf31ce7`**, has **468 inputs** matching the frozen
`target/runtime-goal/recovered-2026-09-30/coding-translation/emaxx` checkout.
Its [portable repair and selected evidence](docs/handover/2026-09-30-shared-reader-draft/source246-coding-translation-selected-manifest.json)
retain source244's same-input negative probes and source245's four compiler
errors (no runtime tests executed). Source246 repairs live encoding-safety
translation tables, dynamically bound registration-list order/duplicates and a
source244 regression that interpreted U+E080–U+E0FF as internal raw-byte sentinels.
Strict checks have zero warnings; **168 focused passes / two existing ignores**,
**951 affected passes**, **eleven ordinary comparisons** and **177/177 original
compiler tests in both editors** are audited. All preceding assertions remain.

The [closed release and Linux Rust evidence](docs/handover/2026-09-30-shared-reader-draft/source246-release-and-linux-rust-failure-manifest.json)
closes release **86468**: **168 focused passes / two existing ignores** and
**951 affected passes**, matching gate. The [complete frozen/macOS/terminal audit](docs/handover/2026-09-30-shared-reader-draft/source246-complete-frozen-macos-terminal-manifest.json)
closes macOS **86467** and terminal **86469**: **3,096 macOS passes / two existing
ignores**, native artifact identity, all 2,999 raw library names, four retained
artifacts, and **226 terminal scenarios / 686 comparisons** verified. Both
supervisors have exited. Read `source246-*` receipts before acting.
The [ordinary audit](docs/handover/2026-09-30-shared-reader-draft/source246-linux-launch-and-ordinary-manifest.json)
closes all 57 preceding comparisons too: **68 distinct comparisons** match.
Linux [Rust36840211567](https://github.com/rayfdj/emaxx/actions/runs/36840211567)
fails on exact `aaf31ce7`: **2,260 passes / two failed GNU reference assertions**,
leaving 745 library tests and both Cargo stages unexecuted. GNU's first empty
record/vector samples are -7/-9 rather than 2/0; neither control reaches Emaxx.
All four GNU inputs are now retained and verified unchanged. The
[bounded diagnosis36842893035](https://github.com/rayfdj/emaxx/actions/runs/36842893035)
passes 18 GNU probes and unchanged Rust single/group/single controls (1/654/1)
with the **same Rust executable and GNU executable/dump/configuration/Makefile**.
The [raw diagnostic audit](docs/handover/2026-09-30-shared-reader-draft/source247-selected-and246-gnu-diagnosis-manifest.json)
does not establish the retaining root or environmental cause; the full run remains
failed. Emaxx images differ, but both failed assertions precede Emaxx comparison.
Linux [frozen36840217747](https://github.com/rayfdj/emaxx/actions/runs/36840217747)
now **passes all 519 files / 7,928 matching outcomes / 1,038 successful processes**
on exact `aaf31ce7`. Each editor has 7,670 passes, 47 expected failures and 211
skips; the latter are not passes. Every raw execution hash and paired outcome is
verified, together with the actual retained GNU executable/dump/configuration/
Makefile. This pass does not explain the earlier Eglot timeout or Linux Rust failure.
The [supplemental load markers](docs/handover/2026-09-30-shared-reader-draft/source246-frozen-load-markers-manifest.json)
complete the archive's raw file coverage; only the hashed GNU executable/dump are
excluded from the portable bundle and retained locally.
The separately validated CI change retains GNU's executable, dump, configuration
and Makefile before future Rust/frozen runs and verifies their bytes afterward.
Six synthetic corruption/overwrite controls reject invalid evidence; runtime
selectors, assertions, timeouts and outcome rules do not change.

Two same-input ordinary probes expose further source246 property defects. The
[separate portable property drafts](docs/handover/2026-09-30-shared-reader-draft/source248-property-drafts-manifest.json)
preserve both negative results. Source247 shares the existing `get` implementation
for translation-table symbols, including `nil`, `t` and overriding property lists.
Its **470 inputs**, zero-warning strict checks, **169 focused passes / two existing
ignores**, **952 affected passes**, twelve ordinary comparisons and original
compiler-file load are audited. Supervisor **90626** has exited.
Source248 adds GNU's first-match rule when an overriding property is nil before
a duplicate; the old helper incorrectly continues to the later value.
Its [closed selected validation](docs/handover/2026-09-30-shared-reader-draft/source248-selected-validation-manifest.json)
verifies **472 inputs/modes**, zero-warning strict checks, **170 focused passes /
two existing ignores**, **953 affected passes**, thirteen ordinary matches and
successful compiler-file load in both editors. Both original negative property
fixtures now match GNU without changed expectations. Queue **94807** has exited.
The [compiler audit and complete launches](docs/handover/2026-09-30-shared-reader-draft/source248-compiler-and-complete-launch-manifest.json)
close **1840**: all **177 original compiler tests pass in both editors**, with
the unchanged full inventory, raw process hashes and 472 source inputs checked.
Source248 is now applied to the root runtime. The [release audit and Linux launches](docs/handover/2026-09-30-shared-reader-draft/source248-release-and-linux-launch-manifest.json)
close **4529** with **170 focused passes / two existing ignores**, **953 affected
passes**, and all 57 preceding ordinary comparisons: **70 distinct ordinary
matches** including the thirteen selected fixtures. Release matches gate, with
the same existing debug-only file-descriptor test accounting for their inventory
difference. The [complete Rust/frozen and terminal-failure archive](docs/handover/2026-09-30-shared-reader-draft/source248-complete-rust-frozen-and-terminal-failure-manifest.json)
closes macOS **4528** with **3,098 passes / two existing ignores** and
[Linux Rust36849791338](https://github.com/rayfdj/emaxx/actions/runs/36849791338)
with **3,110 passes / two existing ignores**, on exact source248. Native artifact
identity, all 3,001/3,009 raw library names and four retained inputs per platform
are verified. The original GNU census assertions pass in this full Linux run;
their earlier failures remain preserved and unexplained.
[Linux frozen36849797068](https://github.com/rayfdj/emaxx/actions/runs/36849797068)
passes **all 519 files / 7,928 matching outcomes / 1,038 successful processes** on
exact `fa7578ac`. Each editor has 7,670 passes, 47 expected failures and 211 skips.
Every raw execution hash and paired outcome is audited, together with the actual
retained GNU executable/dump/configuration/Makefile. The archive includes all
1,038 load markers; only hashed binaries/images are excluded and retained locally.
Terminal **4530** has exited after GNU startup fails on scenario162:
**161 fully matching scenarios / 295 matching comparisons**, zero observed
divergences, **391 unexecuted comparisons**. The unchanged `recursive-minibuffer`
replay passes all seven comparisons after the local Rust gate finishes.
The [complete unchanged terminal retry](docs/handover/2026-09-30-shared-reader-draft/source248-complete-terminal-retry-manifest.json)
closes **14540** with all **226 scenarios / 686 comparisons** matching: 658 screen
and 28 filesystem comparisons, zero missing comparisons. All 472 source inputs,
eight execution inputs and every raw verdict are verified. Commands, selectors,
actions, timeouts and comparison strictness remain unchanged. The run followed
the local Rust gate's exit; it does not establish the first startup failure's
cause or erase that failure. GNU is source/native-ABI matched, not pinned Darwin.
Keep the source248 and source249 validation checkouts frozen. Continuation17624
has exited; rebuilt-Darwin frozen retry58661 remains active. The full goal and
complete replacement-oracle validation remain open.
The [C-source review and diagnostic failure](docs/handover/2026-09-30-shared-reader-draft/source248-c-reference-and246-trace-failure-manifest.json)
map the fixes to unchanged GNU `coding.c:get_translation_table`, `lisp.h:SYMBOLP`,
and `fns.c:plist_get/Fget`. Both original failing probes retain their exact bytes
and GNU results. No new representation, property cache or weakened assertion is added.

The same complete source246 archive preserves the new GNU GC trace preparation:
20 ordinary macOS smoke processes match, fifteen synthetic header/block decoder
controls pass, and all eight workflow shell blocks parse. `gnu-census-trace` runs
the unchanged census fixtures, records explicit environment-padding variants and
reads GNU's first eight GC inventories/possible stack references under GDB. It
makes no inferior calls/stores or warm-up collections. Linux diagnosis
[36848112001](https://github.com/rayfdj/emaxx/actions/runs/36848112001) fails on
`2dd1dea8`: ten ordinary first-fixture processes match, then GDB's argument quoting
with `startup-with-shell off` splits the Lisp argument. GNU stops with end-of-file
before any explicit collection; the second fixture never starts. Raw evidence
and the executed helper are preserved. No GC observation or root cause is claimed;
post-run oracle verification was not reached. The full Linux Rust failure remains open.
The separate observer correction restores GDB's default startup shell and checks
the actual Linux inferior argument vector against the original GNU command.
The [closed GNU trace and conversion-gap archive](docs/handover/2026-09-30-shared-reader-draft/source248-gnu-trace-and-conversion-gap-manifest.json)
verifies diagnosis [36850342459](https://github.com/rayfdj/emaxx/actions/runs/36850342459)
on `163064c9`: **twenty ordinary matches**, two successful GDB executions with
exact original argument vectors, and **sixteen marked inventories** independently
matching GNU's returned slot counts. No previously live object drops in those
observations; the earlier -7/-9 variation is not reproduced or explained. All four
GNU inputs match the prior failed full run and are verified unchanged afterward.
The first failed observer and raw arguments remain preserved. Runtime source248 is unchanged.

The same archive turns the documented general-translation limit into a concrete
negative fixture: actual encoding/decoding ignores live translation tables,
their mutation and composition with standard tables. GNU applies them. The
recorded source174/main and source207 executable/image pairs, run from their
original locations with hashes verified, produce source248's identical wrong
output. This specific gap therefore predates the later migrations; it still needs
repair. The initial relocated source174 launch failed before Lisp because of
relative native-library paths and remains preserved, with no runtime verdict.
Follow `coding.c:encode_coding/consume_chars` and `decode_coding/produce_chars`,
including their use of `get_translation_table`; do not infer complete translation
semantics from the current safety-only helper or the narrow negative fixture.

The [closed source244 evidence](docs/handover/2026-09-30-shared-reader-draft/source244-closed-validation-manifest.json)
records exact runtime **`578955b3`**, all 464 inputs, **3,094 complete macOS
passes / two existing ignores**, native artifact identity, **166 focused / 949
affected passes in each profile**, and the previously audited 66 ordinary matches.
Linux [frozen36830884261](https://github.com/rayfdj/emaxx/actions/runs/36830884261)
processes **all 519 files / 7,928 outcomes**, with **7,927 matches / one mismatch**.
All original 177 compiler tests now pass in both editors. The sole mismatch is
GNU's unexpected JSON-RPC timeout in `eglot-test-rust-completion-exit-function`;
Emaxx passes. Emaxx has 7,670 passes, 47 expected failures and 211 skips; GNU has
7,669 passes, 47 expected failures, one unexpected failure and 211 skips.
This full frozen run remains failed.
The retained GNU process-buffer dump contains the completion reply despite the
request timeout. Two connections and duplicated dumps prevent establishing which
connection/timing it belongs to. The same signature was documented before this
runtime work in `docs/frozen-run-success.md`; its cause remains unresolved.

Linux [Rust36830879269](https://github.com/rayfdj/emaxx/actions/runs/36830879269)
retains **2,258 passes / two failed GNU reference assertions**: first record
sample -7 rather than 2 slots, first vector sample -9 rather than 0. All later
samples match; neither failed control reaches its Emaxx comparison. Another
745 library tests and both Cargo stages never run. GNU's executable/dump were
not retained by that job, so the cause remains unresolved. Source244 full
terminal stops on scenario81 at GNU startup readiness: **80 complete matching
scenarios / 80 comparisons**, zero observed divergences, **145 unstarted
scenarios / 606 unexecuted comparisons**. The failed runs remain failed; earlier
source243 full terminal/Rust passes do not certify source246.

Known limits remain: source246 fixes the safety helper, while general encoding
and decoding translation behavior, normal registration-list authority/order,
legacy charset adapters and complete validation of the property repairs remain open.
Main remains source174; complete correctness, shared representation, accounting,
ownership, final audit, pinned Darwin and the locked 16-workload/3% performance
requirement remain unfinished. No current performance-parity claim exists.

The preceding **source243** was pushed as `8d4fcd39`, with its evidence below.
The preceding **source238**, `a7e12de8`, recovered the complete pinned Linux
frozen match: **519 files / 7,928 outcomes / 1,038 successful processes** in
[run 36812124904](https://github.com/rayfdj/emaxx/actions/runs/36812124904).
The [raw audit](docs/handover/2026-09-30-shared-reader-draft/source238-complete-frozen-and-terminal-failure-manifest.json)
verifies every inventory, paired outcome and execution hash. Each editor has
7,670 passes, 47 expected failures and 211 skips; the latter are not passes.

[Complete macOS and selected validation](docs/handover/2026-09-30-shared-reader-draft/source238-complete-macos-and-selected-manifest.json)
verify **3,085 Rust passes / two existing ignores**, native artifact identity,
101 focused release passes / two existing ignores, 940 affected release passes
and 57 ordinary comparisons. All source238 local supervisors have exited.
Its full terminal run remains failed: **225 of 226 scenarios wholly match**,
682 comparisons match, one coding-system indicator diverges after saving a new
alternate file and three later comparisons in that scenario never run. The
seven original quote-display divergences now match.

The [Linux Rust failure and replay](docs/handover/2026-09-30-shared-reader-draft/source238-linux-census-failure-and-replay-manifest.json)
retain **2,250 passes / one failed GNU reference assertion**; 745 later library
tests and both Cargo stages never run. The first empty-vector GNU census is -9
instead of 0, before the Emaxx comparison starts. An unchanged single-test replay
passes with the same Rust executable hash. This does not identify the cause or
erase the failed full run. The unchanged 643-test group diagnosis in
[run 36816077037](https://github.com/rayfdj/emaxx/actions/runs/36816077037)
now reproduces the same GNU failure: 642 passes / one failure, with the same
Rust executable hash. Its [closed audit](docs/handover/2026-09-30-shared-reader-draft/source240-release-and-open-failures-manifest.json)
retains every raw verdict. The retaining root or environment cause is unresolved.

The separate [source240 coding/copy draft](docs/handover/2026-09-30-shared-reader-draft/source240-coding-recovery-draft-manifest.json)
in `target/runtime-goal/recovered-2026-09-30/coding-repair/emaxx` has **454 inputs**.
Strict checks have zero warnings; **160 focused tests / two existing ignores**
and **944 affected tests pass in both gate and release profiles**. All **61
ordinary GNU comparisons** pass. It also
repairs source238's reproduced U+F8FF failure-report error while preserving the
known pass, failure, expected-failure and skip outcomes. The failed source239
draft remains preserved. The separately added save/revisit probe still fails:
GNU records `utf-8-unix` after save, Emaxx `prefer-utf-8-unix`; file bytes match.
The [complete macOS audit](docs/handover/2026-09-30-shared-reader-draft/source240-complete-macos-manifest.json)
now verifies **3,089 passes / two existing ignores**, native artifact identity,
all 2,992 raw library names/verdicts and four retained executable/image hashes.
Both supervisors **43216** and **43217** have exited.

The [source243 file-coding checkpoint](docs/handover/2026-09-30-shared-reader-draft/source243-file-coding-recovery-manifest.json)
follows GNU's Lisp-owned selection policy, preserves
narrowing across callbacks, uses the short overwrite prompt and checks exclusive
creation when opening the file. Source241's strict failure is preserved.
Source242 passes 163 focused tests with two existing ignores and one failed
selection fixture: its two invalid-coding errors contain a string instead of
GNU's offending symbol. The save/revisit, overwrite-order and prompt controls
pass. Source243 also retains the actual error object and adds a GNU-confirmed
uninterned-symbol identity control; its 462 inputs are frozen in
`target/runtime-goal/recovered-2026-09-30/file-coding/emaxx`.
Strict checks have zero warnings; **165 focused passes / two existing ignores**,
all **948 affected-module tests**, and eight ordinary GNU comparisons pass.
All **11 original file-lifecycle scenarios / 41 comparisons** match, including
the original indicator failure and all three previously unexecuted comparisons.
The actual interactive prompt probe also matches GNU. Every raw verdict and
artifact identity is audited. The first broad-audit helper stopped on an incorrect
562-input assertion; its separate corrected helper checks all 462 inputs without
rerunning or changing tests. The failed audit remains preserved.

Selected supervisors **53531** and **54143** have exited. The
[complete release audit](docs/handover/2026-09-30-shared-reader-draft/source243-release-complete-manifest.json)
also closes **55868**: **165 focused passes / two existing ignores** and all
**948 affected passes** in each profile, with exact artifacts and every raw
verdict verified. The [complete macOS audit](docs/handover/2026-09-30-shared-reader-draft/source243-complete-macos-manifest.json)
now closes **55867**: **3,093 passes / two existing ignores**, native artifact
identity, every one of 2,996 raw library names/verdicts and four retained input
hashes verified. The frozen checkout matches all 462 published runtime inputs.
The [complete terminal and frozen-failure audit](docs/handover/2026-09-30-shared-reader-draft/source243-complete-terminal-and-frozen-failure-manifest.json)
now closes terminal **55869**: all **226 scenarios / 686 comparisons** match,
with unchanged actions, inventories, timeouts and eight execution-input hashes.
GNU is source/native-ABI matched, not the pinned Darwin executable. The
[closed ordinary comparisons and Linux launches](docs/handover/2026-09-30-shared-reader-draft/source243-linux-launch-and-ordinary-manifest.json)
verify all 57 preceding ordinary comparisons as well: **65 distinct comparisons**
on source243. The [complete Linux Rust audit](docs/handover/2026-09-30-shared-reader-draft/source243-linux-complete-rust-manifest.json)
closes [run 36822371375](https://github.com/rayfdj/emaxx/actions/runs/36822371375)
on exact `8d4fcd39`: **3,105 passes / two existing ignores**, native identity,
all 3,004 raw library names/verdicts and four retained input hashes verified.
The original GNU census fixture, expectation and helper bodies are unchanged and
pass in this full run; this does not explain the earlier source238 variation.
[Full frozen 36822376061](https://github.com/rayfdj/emaxx/actions/runs/36822376061)
**fails** on exact `8d4fcd39` after **482 matching files / 6,911 outcomes**
(6,693 passes, 43 expected failures and 175 skips per editor). Emaxx exits 255
while loading `test/src/comp-tests.el`: writing a native-compiler temporary file
unexpectedly requests a coding system. All 177 selected outcomes are missing;
GNU runs and passes them. Another 36 files never start. The unchanged ordinary
compiler load reproduces the failure. Read the current `source243-*` receipts before acting. Do not edit executing
candidates or helpers. The published source243 checkpoint matches its 462 candidate inputs.
The same macOS archive preserves nine GNU-only workflow-environment probes and
nine GNU-only smoke probes of `tools/diagnose_gnu_census.py`; all match locally.
They do not resolve the Linux GNU reference failure. The new optional
`gnu-census` workflow mode holds the Rust controls' selector environment constant
and runs the original single test, complete primitives group and single test
again, retaining failures and GNU executable/image identities. Linux
[diagnosis 36825694384](https://github.com/rayfdj/emaxx/actions/runs/36825694384)
completed on `3a35ad00`, which preserves all 462 runtime inputs. Its **18 GNU
probes** and unchanged Rust **single/group/single controls (1/651/1 passes)**
all pass. Every raw verdict, inventory and eight retained input hashes are
audited in the same archive. This does not explain the earlier source238 GNU
census variation; those failures remain failed. It is diagnostic coverage,
not a full gate.
**Main is unchanged; complete correctness and the full
architecture/performance goal remain unfinished.**

## Source234 checkpoint and preceding history

The latest validated main checkpoint is **source174**, merge `21d20f0e` in
[PR #78](https://github.com/rayfdj/emaxx/pull/78). Its
[complete validation record](docs/runtime-representation-call-validation-complete.md)
remains separate from the unfinished task branch.

The task branch is **runtime-char-tables**, [draft PR #79](https://github.com/rayfdj/emaxx/pull/79).
This checkpoint advances source231 (`7cb2cae3`) to **source234**, matching all
442 runtime/test inputs in `target/runtime-goal/recovered-2026-09-30/display-tables/emaxx`.
Despite that checkout's name, it contains the interactive-metadata repair, with
no renderer change. The [portable draft](docs/handover/2026-09-30-shared-reader-draft/source234-interactive-metadata-draft-manifest.json)
and [focused audit](docs/handover/2026-09-30-shared-reader-draft/source234-focused-validation-manifest.json)
verify exact patch/mode replay, strict zero-warning checks and **37 gate passes**.
Release supervisor **8207** and complete macOS supervisor **12104** are active.
Linux validation of source234 has not yet been dispatched in this saved record.

Source231's [closed Linux failures](docs/handover/2026-09-30-shared-reader-draft/source231-linux-interactive-failure-manifest.json)
show **1,380 Rust passes followed by an Edebug abort**; 1,608 library tests and
both Cargo stages never run. Frozen comparison matches **79 files / 1,911 outcomes**
(1,901 passes, five expected failures and five skips per editor), then Emaxx
aborts in `edebug-tests.el` with 46 missing outcomes; 439 files never start.
Both traces reach the same malformed-environment assertion. An ordinary same-input
probe returns `17` in GNU but a constants-vector `listp` error in source231.
GNU `callint.c:Fcall_interactively` uses a closure environment only when its code
slot is a cons. Source234 follows that rule and delegates to existing `Feval`,
preserving captured mutations and isolating non-interpreted commands from caller
lexical bindings. Its new GNU fixture and four unchanged Edebug controls pass
locally; that does not yet establish Linux repair.

The included [source232 library repair](docs/handover/2026-09-30-shared-reader-draft/source232-library-control-repair-manifest.json)
corrects stale root metadata and the reader's closure-kind assertion. Its vector
control retains all 660 original mixed allocations and every original assertion,
then adds dense allocations to guarantee coverage of every bitmap word. The
original lightweight group passes **545 gate / 544 release tests**, including all
five previous failures; 31 focused controls also pass in each profile. Its first
[full-run setup failure](docs/handover/2026-09-30-shared-reader-draft/source232-fixture-relocation-failure-manifest.json)
retains 232 passes / 154 failures caused by a fixture image built under the relocated
test executable. The image and failed run are preserved. Supervisor **5593** reruns
the unchanged full gate with a fresh image from the normal executable; it remains
active in the string-ops checkout. Source233's formatting-only failure is retained;
it ran no runtime tests. Source234 corrects only that formatting from source233.

The [closed source229 terminal failure](docs/handover/2026-09-30-shared-reader-draft/source229-terminal-failure-manifest.json)
still contains **353 matching comparisons / seven quote-display divergences**, followed
by a GNU startup-readiness failure. Another 54 scenarios never start. Source234
has no renderer repair or complete terminal pass. Main remains source174.
Earlier source207 certification remains historical evidence below.
Runtime **source204** was published as `d186d40bf014ca53d0a2652b4e2f2d921c5cb009`.
The preceding **source207**, published as `092fef676e7667332349b12fd9b63539dd39072b`,
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
This source216 closure-only candidate remained isolated; source231 above now
carries the later closure, canonical-string and direct-execution work.

The [source229 string-byte and direct-VM history](docs/runtime-representation-string-bytes-draft.md)
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
width/operand/branch/GC/suspended-caller cases. Its
[completed gate/release audit](docs/handover/2026-09-30-shared-reader-draft/source223-direct-bytecode-results-manifest.json)
verifies **17 focused passes and 929/930 broader passes per profile**. The same
old raw-byte-code fixture fails in both; GNU proves its Unicode input does not
contain the intended opcodes. Supervisors **73057 and 73532 have exited**.
The [source224 repair](docs/handover/2026-09-30-shared-reader-draft/source224-raw-bytecode-fixture-repair-manifest.json)
changes only that test's inputs/comment, preserving both assertions and every
production byte. Strict checks and **18 focused tests per profile pass**;
supervisor **77687 has exited**. Earlier broader failures remain preserved.
Source222's strict obsolete-helper failure remains preserved, with no runtime
tests. A temporary non-ASCII plain-string Rust API byte adapter remains.
The [source225 control-flow draft](docs/handover/2026-09-30-shared-reader-draft/source225-control-flow-draft-manifest.json)
repairs eager checking of unused branch/handler destinations, following a new
ordinary GNU probe. It checks actual fetched byte offsets and carries each
instruction across fast/slow dispatch without decoding it twice. All **427 inputs
and modes** replay exactly; strict checks pass with zero warnings. Its
[audited selected results](docs/handover/2026-09-30-shared-reader-draft/source225-bytecode-string-selected-validation-manifest.json)
pass **20 focused and 932 affected-module tests per profile**, zero ignores,
and 48 distinct ordinary comparisons. The first ordinary wrapper run retains
44 passes/four failures from printing already self-printing fixtures twice;
four separate unchanged-fixture replays pass without that redundant print.
Supervisors **77913, 79760, 80241 and 81339 have exited**.

The [complete source225 failures](docs/handover/2026-09-30-shared-reader-draft/source225-complete-validation-failure-manifest.json)
retain **161 Rust passes followed by a native-call abort** in the original Tramp
test. The remaining 224 tests in that first group, every later group and both
Cargo stages did not run. The terminal comparison matches seven scenarios,
then aborts opening Org; 218 scenarios never start. Supervisors **82034 and
82035 have exited**. The Rust trace and a same-input ordinary probe diagnose
`try-completion` forcing non-byte characters into an unibyte result. The
terminal capture shows a related native `regexp-opt-group` panic but omits its
initial message. These are real failed runs, not complete validation.

The [source228 completion draft](docs/handover/2026-09-30-shared-reader-draft/source228-completion-storage-draft-manifest.json)
follows GNU's selected candidate and substring rules for encoding, properties,
extended characters and unchanged-input identity. All **429 inputs and modes**
replay exactly; strict checks pass with zero warnings. Its
[audited results](docs/handover/2026-09-30-shared-reader-draft/source228-completion-results-manifest.json)
retain **24/25 focused passes and 932/933 broader passes**, zero ignores.
The original Tramp test passes; the new completion fixture still fails because
`concat` loses extended characters before completion sees them. Supervisor
**82531 has exited**. The failed executable, image and original assertion remain.

The [source229 concat draft](docs/handover/2026-09-30-shared-reader-draft/source229-canonical-concat-draft-manifest.json)
follows `fns.c:concat_to_string`, copying actual encoded string bytes and
validating list/vector character codes over GNU's full range. It removes the
intermediate Rust-text concatenation. All **432 inputs and modes** replay exactly;
strict checks pass. Its [closed selected and full-Rust evidence](docs/handover/2026-09-30-shared-reader-draft/source229-selected-and-rust-failure-manifest.json)
verifies **26 focused / 934 affected tests per profile**, fifty ordinary controls,
zero ignores and exact source/artifact identities. Full Rust records **680 passes /
one failure**: an internal assertion still requires `Kind::Record` for quoted
bytecode, while unchanged ordinary GNU/source229 both show a callable four-slot
closure. The remaining 2,297 library tests and both Cargo stages never run.
Supervisors **84949, 86489, 86490 and 87395 have exited**. Terminal supervisor
**89147 has exited**. Its [audited partial failure](docs/handover/2026-09-30-shared-reader-draft/source229-terminal-failure-manifest.json)
has 164 fully matching scenarios, 353 matching comparisons and seven quote-display
divergences, including echo text and wrap/cursor consequences. Scenario 172 stops
at GNU startup readiness; 54 scenarios never start and 326 scheduled comparisons
never execute. Every observed verdict is checked against the unchanged inventory.
No complete terminal pass or Emaxx startup crash is inferred from that traceback.

The same archive retains three additional ordinary substring failures: extended
character loss, incorrect array/index error contracts, and a host abort for
reversed bounds. The [source230 draft](docs/handover/2026-09-30-shared-reader-draft/source230-substring-draft-manifest.json)
uses actual substring bytes and vector fields, validates GNU's index types before
their joint range, and shares the canonical slice with completion. The old quoted
bytecode test now checks the actual closure and GNU-confirmed fields/execution;
its original name and macro assertion remain. All **440 inputs and modes** replay
exactly. Strict checks pass. The [focused result and proposed setup correction](docs/handover/2026-09-30-shared-reader-draft/source230-focused-failure-and-proposal-manifest.json)
retain **30 passes / one failure**: the added normal-GNU fixture calls `cadr` in
an intentionally bare interpreter. All three substring controls pass; the
original quoted-reader and closure-kind assertions pass before that setup error.
Source230's [closed validation](docs/handover/2026-09-30-shared-reader-draft/source230-closed-validation-manifest.json)
verifies all **937 affected-module passes and 54 ordinary GNU comparisons**;
supervisors **90596/93555 have exited**. Its focused failure remains failed.

[Source231](docs/handover/2026-09-30-shared-reader-draft/source231-bytecode-fixture-startup-draft-manifest.json)
changes only the added fixture's startup, preserving the original bare-reader
assertions, every fixture/expected byte and all production source230 inputs.
All **440 inputs and modes** replay exactly. Its
[focused audit and complete launches](docs/handover/2026-09-30-shared-reader-draft/source231-focused-and-complete-launch-manifest.json)
verify strict zero-warning checks and **31 focused gate passes**. The task
checkpoint matches all 440 inputs. Supervisors **96184/96185 have exited**;
their [audited results](docs/handover/2026-09-30-shared-reader-draft/source231-release-and-ordinary-validation-manifest.json)
pass all 31 focused release controls, all 937 affected release tests and 54 fresh
ordinary GNU comparisons. Exact executables/images and raw verdicts are retained.
Supervisor **96183 has exited**. Its
[audited full-Rust failure](docs/handover/2026-09-30-shared-reader-draft/source231-full-rust-failure-manifest.json)
verifies every one of 2,981 library names: **2,974 pass, five fail, two retain their
existing ignore**. The binary and integration Cargo stages never run. The failures
are stale `bc_functions`/`bc_live_programs` root metadata (three controls), an old
host-record assertion in the no-slot-evaluation reader test, and a vector
allocation control that reaches only three of four bitmap words. Keep every
original semantic assertion while repairing those causes. Source231's
complete gate covers the full inventory instead of repeating the already passing
source230 affected-module gate after a test-only setup change. Its exact failed
executable/image and complete logs are retained before further changes. Sources226/227 are
unexecuted intermediate drafts; their patch history remains preserved.
The bundle also preserves an ordinary source207 capacity mismatch: GNU accepts
262144/393216 declared slots, but Emaxx overflows. Rust still reserves 256K words
with separate frames versus GNU's 512K words including frames. This remains open.
Plain-string migration, compact headers, stack layout/accounting, complete
validation and performance remain open. Source231 is a development checkpoint,
not completion of those requirements.

The preserved **source208** carries evaluator207
and adds GNU constructor field validation. Its [418-input portable draft](docs/handover/2026-09-30-shared-reader-draft/source208-bytecode-constructor-draft-manifest.json)
and [audited controls](docs/handover/2026-09-30-shared-reader-draft/source208-bytecode-constructor-results-manifest.json)
retain twelve ordinary negative constructor cases. Strict checks pass; six gate
controls yield **two passes / four failures**. Constructor validation and shared
constants pass; both layout and both live-code mutation controls still fail.
The result archive also preserves the wrapper's later log-print filename error.
Supervisor **53789 has exited**. This is the historical source208 failure;
the later source231 task checkpoint is described above.

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
remain required. Source284 has a diagnostic pilot and profiles; no GNU-parity claim exists.

Receipts and reproduction helpers are under `target/runtime-goal/resume-2026-09-28`;
recovered checkouts and GNU are under `target/runtime-goal/recovered-2026-09-30`.
Rebuild on another machine; local executable/image identities are not portable
validation claims. Do not reapply historical patches over the existing task branch.

The [archived full continuation index](docs/runtime-representation-handover-history-2026-10-01.md)
retains every earlier checkpoint, failure, diagnosis and handover link. It includes
the original [27 September handover](docs/runtime-representation-handover.md) and
[complete September 21 audit](docs/adversarial-decheating-audit-2026-09-21.md).
