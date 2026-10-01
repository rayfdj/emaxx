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
Read the latest [correctness recovery and separate file-coding draft](docs/runtime-representation-correctness-recovery-draft.md) before continuing.
The [requirement map](docs/runtime-representation-progress.md) tracks the full scope.

## Current correctness recovery

The immediate priority is to restore complete correctness before further architecture work.
The task runtime is now **source248**, pushed as **`fa7578ac`**, with **472 inputs** matching the frozen
`target/runtime-goal/recovered-2026-09-30/coding-property-order/emaxx` checkout.
The two property repairs below follow the actual GNU C lookup paths; their
strict, selected and complete original compiler comparisons pass. Complete
macOS/Linux Rust and pinned Linux frozen comparisons now pass as detailed below.
Full terminal retry **14540** is running; the original GNU startup failure and a
pre-existing general conversion-table gap remain documented. Further architectural
changes remain on hold.
The [separate source249 post-read draft](docs/handover/2026-09-30-shared-reader-draft/source249-post-read-draft-manifest.json)
has **476 frozen inputs**, with both patches replayed exactly. Its two ordinary
negative fixtures expose byte-count hook arguments and a missing ASCII early
return; both also fail identically on retained source174/main and source207.
The repair follows `coding.c:decode_coding_object/code_convert_string`, preserves
the exact GNU outputs and remains unapplied. Queue **15814** waits for terminal
**14540** before building and validating it. Read `source249-*` receipts before
acting; the packaged queue snapshot and prepared auditors are not runtime passes.
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
The complete unchanged terminal retry **14540** is running without a simultaneous
local Rust gate. This does not establish the startup failure's cause or erase it.
Do not edit the frozen candidate or running retry helper; read its receipts before
another launch. Pinned Darwin and complete correctness remain open.
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
remain required. No current timing or GNU-parity claim exists.

Receipts and reproduction helpers are under `target/runtime-goal/resume-2026-09-28`;
recovered checkouts and GNU are under `target/runtime-goal/recovered-2026-09-30`.
Rebuild on another machine; local executable/image identities are not portable
validation claims. Do not reapply historical patches over the existing task branch.

The [archived full continuation index](docs/runtime-representation-handover-history-2026-10-01.md)
retains every earlier checkpoint, failure, diagnosis and handover link. It includes
the original [27 September handover](docs/runtime-representation-handover.md) and
[complete September 21 audit](docs/adversarial-decheating-audit-2026-09-21.md).
