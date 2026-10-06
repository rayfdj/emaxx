# Native call routing and argument boundaries — 5 October 2026

## Applied native compilation units — 6 October 2026

**Source310 is applied; the full goal remains incomplete.** A native compilation
unit is now one actual 88-byte GNU-layout object. Its seven Lisp fields own the
data graph, and the unreachable unit closes its library. The host record, strong
library registry and permanent relocation roots are removed. Repeated loads check
the library's actual runtime relocation before accepting another Rust editor;
ordinary calls acquire no new ownership lookup.

The [closed selected evidence](handover/2026-09-30-shared-reader-draft/source310-native-unit-selected-manifest.json)
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

This is a [correctness publication](handover/2026-09-30-shared-reader-draft/source310-application-decision.json),
with [exact applied-byte verification](handover/2026-09-30-shared-reader-draft/source310-applied-source-verification.json).
**310 performance is not yet measured.** Its unchanged full16 comparison is
queued after the running308 terminal suite; all samples, including regressions,
will remain. The required owner-shutdown allocator walk has no isolated cost
measurement yet. Full310 Linux and macOS validation follows publication; earlier
308 results do not certify310. Inspect `source310-full-validation-launch.json`,
`source310-full-launch.json` and `native-unit310-sequence-state.json` under
`target/runtime-goal/resume-2026-09-28` before starting any write-once job.

Main remains `21d20f0e`, PR79 stays draft. Allocated-symbol authority, real intervals
and unified pure storage, remaining adapters, physical accounting/counters,
VM stack capacity/layout, final internal ownership/borrow review, final audit,
complete final-source validation and the unchanged calibrated performance
criterion remain required. The following sections preserve earlier checkpoints.

## Native compilation-unit draft — 6 October 2026

The separately packaged [source309 selected draft](handover/2026-09-30-shared-reader-draft/source309-native-unit-selected-draft-manifest.json)
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

The [closed selected evidence](handover/2026-09-30-shared-reader-draft/source308-native-object-selected-manifest.json)
verifies all 541 source inputs/modes, fresh warning-free strict checks,
**1,031 passes per profile**, including all 24 audit tests, and **52 ordinary GNU
comparisons**, including 198 generated printer rows. Original fixtures, assertions
and expected bytes remain. The original dabbrev regression is repaired by making
`commandp` recognize the native object's interactive field; native reader and
property identity and exact GNU C-name printing are covered too.

The [closed full16 diagnostic](handover/2026-09-30-shared-reader-draft/source308-native-object-performance-manifest.json)
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
ignores; [macOS terminal](handover/2026-09-30-shared-reader-draft/source304-macos-terminal-results-manifest.json)
passes all226 scenarios /686 screen and filesystem comparisons. Linux Rust stays
failed after2,291 passes at the GNU first-record census (-7 instead of2), leaving
758 library names and both Cargo stages unstarted. macOS frozen stays failed with
7,915 exact matches and six existing build-feature skip-diagnostic differences.
These results certify304 only. The [closed exact308 platform evidence](handover/2026-09-30-shared-reader-draft/source308-platform-validation-progress-manifest.json)
now verifies **3,157 Linux Rust passes** and **3,145 macOS Rust passes**, with the
two existing ignores on each platform. Every library name and Cargo result is
checked against raw output; both native artifact identity tests pass. Linux's
complete frozen comparison matches **519 files /7,928 outcomes** from **1,038
successful processes**: each editor reports 7,670 passes, 47 expected failures and 211
skips. No expected failure or skip is counted as a pass. The
[closed308 macOS frozen comparison](handover/2026-09-30-shared-reader-draft/source308-macos-frozen-results-manifest.json)
retains **7,915 exact matches/six existing skip-diagnostic differences**, across
all519 files and1,038 successful processes. It remains a failed strict comparison;
terminal validation is still running separately. Earlier GNU census failures and source276's
Emaxx negative remain preserved and unresolved; this later pass does not erase them.

Main remains `21d20f0e`, PR79 stays draft. Symbol/unit authority, real intervals and
pure storage, physical accounting, final ownership/borrow review, final adversarial
review, complete final-source validation and the full performance criterion remain
open. See the native continuation for failed drafts and exact-source launch receipts.

## Source308 implementation and preserved failures

The validated source remains frozen at
`target/runtime-goal/recovered-2026-10-06/native-subr-callbacks/emaxx`.
The new payload follows `lisp.h:Lisp_Subr` and `comp.c:make_subr`: function pointer,
short arities, two owned C names, documentation index and the five actual Lisp
fields. Header/tag decoding distinguishes allocated subrs from static DEFUN
objects before making a Rust typed reference. `alloc.c` supplies the explicit
marking and C-name cleanup model. Direct/general calls, metadata, image copying,
dump restoration and native relocation decoding use the same object. The old
`NativeCompiledFunction` record kind and persistent function/name/C-name maps are
gone. The legacy dump discriminator remains a codec only; it creates a real subr.
Compilation-unit ownership and internal GC/borrow review remain open. Public
host entries already serialize through one process lock, and raw Lisp handles are
private. `source308-public-boundary-review.json` records that implemented boundary
and the unchanged304 concurrency evidence; final308 validation remains required.

The [source305 failure](handover/2026-09-30-shared-reader-draft/source305-native-object-decoder-failure-manifest.json)
passes actual layout and five-field survival/cyclic reclamation, but two focused
controls fail because checked word decoding omitted allocated subrs. Source306
adds that exact allocator-checked tag. The
[preserved306/307 results](handover/2026-09-30-shared-reader-draft/source306307-native-object-consumer-failures-manifest.json)
retain306's four focused passes and **821 broader passes / seven failures**.
Six failures are denied local sockets; one is a real dabbrev regression. An exact
original-build-location replay passes304 and fails306. Earlier retained-binary
replays fail during relative native-library setup and certify no semantic result.
307's four focused passes/one failure isolate GNU's signed C-char function-stream
arguments; no release or ordinary candidate comparison ran for306/307.

308 fixes that callback byte conversion while keeping Lisp string bytes valid,
NUL termination and exact GNU unibyte printing. The transient printer tracks
sparse exceptional callback arguments; its larger stack value and extra checks
are included in the ordinary full16 timing above; their separate contribution has not been isolated. This is a Rust encoding constraint, not persistent
Lisp storage. A temporary trace then proves `commandp` returned nil for actual
native commands, causing unchanged GNU isearch Lisp to exit after the first key.
The native arm now reads the actual interactive field (`eval.c:Fcommandp`).
Native identity is also recognized by interval property equality and reader
substitution (`intervals.c`, `lread.c`), and direct completion-table dispatch.
A non-placeholder subr correctly signals `sequencep` after prior substitutions,
as the preserved actual GNU probe shows. The added consumer fixture covers
native/bytecode calls, aliases/redefinition, property errors, varied prefixes,
cycles and partial mutation. Original dabbrev assertions and existing fixtures
remain. The temporary diagnostic test was removed before the final source freeze.

The first ordinary runner passed50 comparisons; its native C-name comparison
produced identical bytes in both editors but failed the saved expectation because
raw UTF-8 argv was decoded under the C locale. The unchanged fixture, loaded from
one UTF-8 file through normal `-l`, matches every original expected byte. No runtime
or fixture changed. The initial audit also used the gate inventory count in release;
the final audit verifies exactly all predecessor names plus six new controls:
3,048 gate /3,047 release, differing only by the unchanged debug-only descriptor
test. Both stopped coordinators and the audit path preflight error remain retained.

All selected and timing coordinators are closed under
`target/runtime-goal/resume-2026-09-28`. Inspect `source308-applied-commit.json`,
`source308-full-validation-launch.json` and `source308-full-launch.json` before
continuing exact-source Linux/macOS work. Do not restart write-once jobs or edit
frozen checkouts. A launch is not a validation result. The native units' host
ownership and final internal GC/borrow review remain unfinished; this subr change
does not certify them. Public host entry serialization and private handles are
already implemented; the recorded source308 boundary review separates that repair
from final-source validation and internal soundness. Earlier GNU census failures and source276's Emaxx negative
remain unresolved.

| Workload | Published304 / GNU | Native-object308 / GNU | 308 body vs304 |
| --- | ---: | ---: | ---: |
| interpreted-lexical | 2.4314× | 2.3307× | +0.812% |
| interpreted-dynamic | 2.1229× | 2.2378× | +1.861% |
| interpreted-calls | 2.6617× | 2.6628× | -5.446% |
| bytecode-calls | 4.1182× | 4.0067× | -6.246% |
| native-execution | 2.1660× | 1.7074× | -25.064% |
| interpreted-to-native | 2.4663× | 2.2500× | -9.976% |
| bytecode-to-native | 8.6302× | 6.6034× | -25.891% |
| native-to-interpreted | 2.7361× | 2.6103× | -5.207% |
| native-to-bytecode | 2.9144× | 2.7036× | -7.387% |
| cons-allocation | 1.7632× | 1.7938× | +1.450% |
| list-traversal | 2.0335× | 2.0784× | -2.163% |
| mapcar | 2.2957× | 2.3530× | +4.487% |
| explicit-gc | 1.0222× | 1.0169× | -0.498% |
| upstream-sort | 5.0352× | 5.0067× | -0.326% |
| upstream-undo | 5.4584× | 5.6685× | +5.656% |
| upstream-bindat | 3.3560× | 3.3616× | +3.249% |

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
[failed302 evidence](handover/2026-09-30-shared-reader-draft/source302-full-validation-failures-manifest.json)
retains macOS **3,037 passes/one failure/two existing ignores** and Linux
**3,045 passes/one failure/two existing ignores**. All library names ran, but bins
and integration did not. Both frozen attempts stopped before any case; TTY never
started. The sole Rust failure was the inventory omission. Those runs stay failed.

[Source304 selected evidence](handover/2026-09-30-shared-reader-draft/source304-native-audit-and-columns-selected-manifest.json)
verifies all **534 inputs and modes**, fresh builds, zero-warning strict checks,
**506 passes per profile**, including all24 audit tests, and **49 ordinary GNU
comparisons**. The prior [303 draft](handover/2026-09-30-shared-reader-draft/source303-buffer-columns-selected-manifest.json)
remains unapplied: it passed482 tests/profile but inherited the inventory omission;
its timing queue stopped before measuring anything.

The [302/304 full16 diagnostic](handover/2026-09-30-shared-reader-draft/source304-native-audit-and-columns-performance-manifest.json)
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

The [closed selected evidence](handover/2026-09-30-shared-reader-draft/source302-native-arithmetic-selected-manifest.json)
verifies all **534 source inputs and modes**, portable replays from `4af440a9` and
main, fresh builds and zero-warning strict checks. **328 selected tests pass in
each profile**, with no failures or ignores, and **48 actual GNU comparisons**
pass. Every original test/assertion/fixture remains. One new control forces native
workers through 108 varied arithmetic cases, overflow/promotion, invalid operands,
GC and marker coercion; it already matches GNU on the unchanged 297 baseline.

The [closed full16 diagnostic](handover/2026-09-30-shared-reader-draft/source302-native-arithmetic-performance-manifest.json)
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

| Case | Applied297 / GNU | Source302 / GNU | 302 body vs297 |
| --- | ---: | ---: | ---: |
| interpreted-lexical | 2.3512× | 2.3240× | +0.306% |
| interpreted-dynamic | 2.1009× | 2.1251× | -0.484% |
| interpreted-calls | 2.6023× | 2.6407× | -1.388% |
| bytecode-calls | 4.0728× | 4.0612× | -0.378% |
| native-execution | 2.8573× | 2.2066× | -22.893% |
| interpreted-to-native | 2.3758× | 2.4148× | -0.544% |
| bytecode-to-native | 8.6580× | 8.5120× | -0.170% |
| native-to-interpreted | 2.6924× | 2.6597× | -0.402% |
| native-to-bytecode | 2.7952× | 2.7850× | -0.328% |
| cons-allocation | 1.7799× | 1.7852× | +0.161% |
| list-traversal | 2.0996× | 2.0726× | -0.964% |
| mapcar | 2.2755× | 2.2637× | +0.321% |
| explicit-gc | 1.0134× | 1.0110× | +0.404% |
| upstream-sort | 4.8676× | 4.9434× | -0.249% |
| upstream-undo | 5.4765× | 5.3813× | -0.097% |
| upstream-bindat | 3.3029× | 3.2810× | +0.506% |

Three alternating pilots per source retain all startup/body/total/RSS/GC reports
and unavailable-counter errors; all measured bodies exceed 100 ms.

## Native Bcall variants rejected; applied checkpoint passes Linux — 5 October 2026

**Applied runtime remains source297 (`4af440a9`). Sources 300 and 301 are rejected
and unapplied.** The [closed inline comparison](handover/2026-09-30-shared-reader-draft/source301-native-bytecode-inline-performance-manifest.json)
verifies all **288 actual process answers and modes** across rotated full 16 pilots.
Source301 still improves bytecode-to-native by 9.624%, but native-to-bytecode is
12.098% slower, native execution 5.648% slower, and raw body geometric mean 3.178%
slower than 297. Default300 also retains slower native transitions. Forced inlining
does not establish a satisfactory tradeoff; this experiment is closed. Both full
cohorts and every unfavorable sample remain. These small diagnostics establish
neither statistical significance nor a causal explanation for all timing changes.

[Source301 selected evidence](handover/2026-09-30-shared-reader-draft/source301-native-bytecode-inline-selected-manifest.json)
verifies all 532 input bytes/modes, fresh builds, warning-free strict checks,
**327 tests per profile / no failures or ignores**, and **47 actual GNU comparisons**.
Only the helper inline attribute differs from 300. No test, fixture, workload,
timeout, selector or tolerance was changed to obtain these results.

[Complete source297 Linux evidence](handover/2026-09-30-shared-reader-draft/source297-complete-linux-results-manifest.json)
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

| Case | Applied297 / GNU | Default300 / GNU | Inline301 / GNU | 301 body vs297 |
| --- | ---: | ---: | ---: | ---: |
| interpreted-lexical | 2.3873× | 2.4210× | 2.4642× | +4.838% |
| interpreted-dynamic | 2.1692× | 2.0622× | 2.1738× | +2.417% |
| interpreted-calls | 2.8183× | 2.7796× | 2.7146× | +2.087% |
| bytecode-calls | 4.1159× | 4.1357× | 4.2720× | +1.445% |
| native-execution | 2.9230× | 2.9360× | 2.8742× | +5.648% |
| interpreted-to-native | 2.4005× | 2.4425× | 2.4164× | +1.269% |
| bytecode-to-native | 8.4911× | 7.5772× | 7.6582× | -9.624% |
| native-to-interpreted | 2.6990× | 2.6849× | 2.7316× | +2.446% |
| native-to-bytecode | 2.8120× | 2.8679× | 2.9796× | +12.098% |
| cons-allocation | 1.7801× | 1.7986× | 1.8509× | +2.214% |
| list-traversal | 1.9843× | 2.0364× | 2.1655× | +5.083% |
| mapcar | 2.2575× | 2.2521× | 2.2340× | +6.294% |
| explicit-gc | 1.0221× | 1.0366× | 1.0688× | +4.493% |
| upstream-sort | 4.9880× | 4.9747× | 5.1312× | +3.768% |
| upstream-undo | 5.6126× | 5.4661× | 5.4121× | +2.608% |
| upstream-bindat | 3.3207× | 3.4262× | 3.4375× | +5.188% |

This cohort’s GNU paired ratio geometric means are 2.864788× / 2.851701× / 2.896677×
for 297/300/301. All body intervals exceed 100 ms. No final parity claim is made.

## Native Bcall candidate held; inline-only comparison running — 5 October 2026

**Applied runtime remains source297 (`4af440a9`)**. Source300 shares the ordinary
native call body with Bcall after its existing function resolution, removing a
second symbol/alias lookup and general dispatch. All 532 inputs/modes replay;
strict checks are warning-free, **327 tests pass per profile**, and **47 actual
GNU comparisons** pass. Every original test, assertion and fixture remains.

The [closed source300 comparison](handover/2026-09-30-shared-reader-draft/source300-native-bytecode-performance-manifest.json)
verifies all **192 process results/modes**. Bytecode-to-native median time falls
**11.579%**, but native-to-interpreted and native-to-bytecode are **4.652% and
6.609% slower**. The raw body geometric ratio is0.993933; GNU paired ratio is
2.886033×. Source300 is **held and unapplied** while the extracted helper's inlining
cost is checked. No causal explanation or final statistical acceptance is claimed.

[Source301's portable draft](handover/2026-09-30-shared-reader-draft/source301-native-bytecode-inline-draft-manifest.json)
changes only that helper from `#[inline]` to `#[inline(always)]`. Its strict checks
are warning-free; selected validation is running and a rotated297/300/301 full16
comparison is queued. Inspect `native-bytecode-inlining-selection-closed-*` and
`native-bytecode-inlining-performance-sequence-closed-*` receipts under
`target/runtime-goal/resume-2026-09-28`. Preserve original300 timing and the frozen
checkouts; do not restart completed write-once producers. Source299's formatting
failure and source300's preparer assertion correction are retained separately.

[Complete source296 Linux results](handover/2026-09-30-shared-reader-draft/source296-complete-linux-results-manifest.json)
now verify exact68ca5d88: **3,147 Rust passes/two existing ignores**, **519 frozen
files/7,928 equal outcomes/1,038 successful processes**, native artifact identity,
177 compiler controls and retained pinned GNU inputs. These results do not certify
later source. Exact297 Linux runs37284983243/37284989055 are in progress; full macOS
is unstarted. The60 original width failures, prior GNU census failures and source276
Emaxx negative remain open. Main, draft PR and all full-goal criteria remain unchanged.

The [selected source300 archive](handover/2026-09-30-shared-reader-draft/source300-native-bytecode-selected-manifest.json)
contains the fresh builds, exact inventories, all47 ordinary GNU outputs and both
portable replays. The original broad probe still differs in its same60 column-width
rows. Extracting the call body does not close existing call-resolution/GC ordering,
native record ownership or any other full-goal gap. Source301 adds no semantic or
test changes. Its measurement changes no workload, timeout, selector or tolerance.

| Case | Applied297 / GNU | Held300 / GNU | 300 body vs297 |
| --- | ---: | ---: | ---: |
| interpreted-lexical | 2.4968× | 2.4946× | -4.266% |
| interpreted-dynamic | 2.1826× | 2.1418× | +2.382% |
| interpreted-calls | 2.8221× | 2.7143× | -0.282% |
| bytecode-calls | 4.0279× | 4.1092× | +1.075% |
| native-execution | 2.8936× | 2.8216× | +1.800% |
| interpreted-to-native | 2.4323× | 2.4945× | -0.420% |
| bytecode-to-native | 8.4464× | 7.5719× | -11.579% |
| native-to-interpreted | 2.7422× | 2.8969× | +4.652% |
| native-to-bytecode | 2.8566× | 2.9748× | +6.609% |
| cons-allocation | 1.8403× | 1.8280× | -0.364% |
| list-traversal | 1.9959× | 2.0427× | -1.473% |
| mapcar | 2.2377× | 2.2345× | -2.428% |
| explicit-gc | 1.0603× | 1.0549× | +1.795% |
| upstream-sort | 5.1048× | 5.2527× | -0.010% |
| upstream-undo | 5.4614× | 5.5163× | -3.582% |
| upstream-bindat | 3.4079× | 3.3187× | -2.310% |

Every sample remains. These three one-sample/no-warmup pilots per source are
separate from builds/tests/profiles, and all body intervals exceed100ms. Real
counters, calibrated nine-round distributions and every-case upper95% GNU ratio
at most1.03 remain required. The sections below retain earlier applied checkpoints.

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
