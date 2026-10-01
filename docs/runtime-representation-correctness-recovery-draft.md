# Correctness recovery — 1 October 2026

The [full goal](runtime-representation-goal.md) is active and incomplete. The
user's latest question challenges the correctness regression. The immediate
work is to restore correctness before further architectural changes. The task branch did regress after the fully matching source207 Linux
frozen checkpoint; those historical results do not certify later shared-closure,
canonical-string and direct-bytecode changes. Main remains source174 at
`21d20f0e`. Do not merge the unfinished branch or declare the performance goal met.

## Verified recovery

Source234, commit `df42fda2a31144ecb25cd5102385a8a3660c173a`, now has complete
[Rust and selected validation](handover/2026-09-30-shared-reader-draft/source234-complete-rust-and-selected-manifest.json):

- macOS: 3,079 passes, two existing ignores; all 2,982 library names verified.
- Linux: 3,091 passes, two existing ignores; all 2,990 library names verified,
  [run 36808140965](https://github.com/rayfdj/emaxx/actions/runs/36808140965).
- Native artifact identity passes on both platforms. Exact executables and
  post-run images are retained locally, with identities in the portable archive.
- Release: 37 focused and 938 affected-module passes; 55 ordinary GNU comparisons.
- The preceding source232 repair also completes macOS with 3,078 passes and two
  ignores. Its first fixture-relocation failure remains failed historical evidence.
  Its initial post-run retention used the wrong image directory and retained only
  the correct executable; the supplemental receipt retains the actual cache's five
  images, explicitly including older entries. Neither receipt is overwritten.

Source234's [Linux frozen run](https://github.com/rayfdj/emaxx/actions/runs/36808153594)
still fails. Its [raw evidence audit](handover/2026-09-30-shared-reader-draft/source234-linux-rx-failure-manifest.json)
verifies 111 matching files / 2,578 matching outcomes: 2,555 passes, 16 expected
failures and seven skips per editor. All 46 Edebug outcomes now match GNU.
The next file, `rx-tests.el`, exits 255 in Emaxx while writing its report after
test execution has started. Its 36 outcomes are missing; 407 files never start.
The fallback `load_error` label is not a source-load diagnosis.

Unchanged ordinary local ERT reproduces 34 passes and two failures in that file:
`rx-char-any-raw-byte` and `rx-charset-or`. The unchanged structured runner also
reproduces the report-writing error. These are separate diagnostic runs, not
recovered Linux verdicts. The source/native-ABI-matched GNU here is not the pinned
Darwin executable. No current performance-parity evidence exists.

## Display repair: source237

The isolated source237 component adds `src/tty.rs` in
`target/runtime-goal/recovered-2026-09-30/tty-render/emaxx` to source234.
Its [portable patch and evidence](handover/2026-09-30-shared-reader-draft/source237-display-validation-and238-rx-draft-manifest.json)
identify 442 runtime/test inputs. Strict checks have zero warnings; all 62 TTY
unit tests pass, with the two pre-existing end-to-end ignores unchanged.
Four new controls cover table precedence, character/face translation, live
mutation, cursor remapping and echo clipping.

The ordinary terminal comparison passes all 11 selected scenarios / 39 comparisons:
the seven original failing scenarios and all four original glyphless controls.
Every original divergent label is now matching. No action, timeout, selector
meaning or comparison rule changed. This is not the complete 226-scenario gate.

The renderer follows `window.c:window_display_table` and
`xdisp.c:get_next_display_element` / `next_element_from_display_vector`: select
one valid window/buffer/standard table, translate vectors before glyphless fallback,
hide empty entries and avoid recursive table lookup of output glyphs. Buffer,
mode/header and echo paths share translation and position remapping. Blocking
event readers use their existing interpreter argument. No new interpreter
registry or mutable shadow state is added.

Further display-table differential review remains required, especially newline
translation, generated stretch spaces/alignment, special extra-slot glyphs,
control glyphs, non-Unicode glyph output and no-font face precedence. The current
tests do not certify every display-table behavior. Source235's compile failure,
source236's Clippy failure and the failed initial audit/helper launch are retained.

## Combined task checkpoint: source238

The task checkpoint, pushed as `a7e12de8bf4c9cf124e8982922c268f91e97aa6f`,
combines source237 with the reader/mapconcat repairs. The
same 446 inputs are frozen in
`target/runtime-goal/recovered-2026-09-30/rx-repair/emaxx`. Its
[focused audit and portable patch](handover/2026-09-30-shared-reader-draft/source238-focused-validation-manifest.json)
verify zero-warning strict checks and **101 focused gate passes**, with two
existing end-to-end ignores. This covers the 37 source234 controls, all TTY
unit tests and the two new GNU fixtures. Exact test executable and post-run
image are retained.

The [ordinary rx audit and full-run launches](handover/2026-09-30-shared-reader-draft/source238-rx-repair-and-complete-launch-manifest.json)
verify **all 36 upstream rx tests pass** through ordinary ERT and the unchanged
structured runner in both editors. Each of the four processes exits successfully;
all raw names and verdicts are checked. Both original local rx failures are
repaired. This closes their local reproduction, not the pending Linux frozen run.
Supervisor **27314** has completed all five stages successfully.

Supervisors **30236**, **31378** and **31379** have exited. The
[complete macOS and selected audit](handover/2026-09-30-shared-reader-draft/source238-complete-macos-and-selected-manifest.json)
verifies 3,085 Rust passes and two existing ignores: 2,986 library, 60 binary
and 39 integration passes, including native artifact identity. All 2,988 raw
library names/verdicts and four retained executable/image files are checked.
The first verifier stopped before artifact retention had run and wrote no
passing receipt; the separate retention receipt records the setup correction.
Release passes 101 focused controls with two existing ignores and all 940
affected controls without ignores. All 57 ordinary GNU comparisons match.

The [complete frozen and terminal evidence](handover/2026-09-30-shared-reader-draft/source238-complete-frozen-and-terminal-failure-manifest.json)
closes [Linux frozen run 36812124904](https://github.com/rayfdj/emaxx/actions/runs/36812124904)
on exact `a7e12de8`: **519 files / 7,928 matching outcomes / 1,038 successful
processes**. Each editor has 7,670 passes, 47 expected failures and 211 skips;
the latter are not passes. All inventories, paired name/status/expectation
outcomes, raw execution hashes and the pinned Linux oracle identity are verified.
Both Edebug and rx now match in the complete frozen run.

The full terminal run is still failed. All 226 scenarios start; 225 wholly
match, with 682 matching comparisons and one divergence:
`find-alternate-file-missing-revisit::6:verify-created-file`. After saving the
new file GNU's mode line shows `U` for the coding system, while Emaxx shows `-`.
The scenario returns early, leaving its final filesystem comparison and both
revisit comparisons unexecuted. All seven previous quote-display divergence
labels match. Preserve this failure; no timeout, action or comparison changed.

[Linux Rust 36812116311](https://github.com/rayfdj/emaxx/actions/runs/36812116311)
fails after 2,250 passes in `native_vector_and_closure_census_matches_gnu_word_layout`.
Its [failure and selected replay audit](handover/2026-09-30-shared-reader-draft/source238-linux-census-failure-and-replay-manifest.json)
verifies every executed name and verdict. The helper's GNU reference produces
`(0 -9)` for its first empty-vector sample instead of `(0 0)`; all remaining
vector and closure samples match. This assertion precedes Emaxx startup, so
the control has no Emaxx verdict in that run. Another 745 library tests and
both Cargo stages never run. No expectation has changed.

An [unchanged selected replay](https://github.com/rayfdj/emaxx/actions/runs/36814333394)
passes on documentation-only `6ee4bd1b` with the **same Rust executable hash**
`b872b12e92a71a4475231f7d001a74ec20a1ca5571ab547f441066716d7ba262`.
Its fresh Emaxx image differs; GNU executable hashes were not retained in these
Rust jobs. This does not identify the census variation's cause or certify the
failed full inventory. The unchanged 643-test primitives-group diagnosis
[36816077037](https://github.com/rayfdj/emaxx/actions/runs/36816077037) now
reproduces the same GNU failure: 642 passes and one failure with the same Rust
executable hash. The [closed group and draft release evidence](handover/2026-09-30-shared-reader-draft/source240-release-and-open-failures-manifest.json)
retain all 643 raw names/verdicts, executable and image identities. The passing
single-test replay and failing group narrow the investigation to an order or
environment dependent reproduction; the actual GNU retaining root is unproven.
Do not change the census expectation or relabel the complete failed run.

Helpers and receipts are under `target/runtime-goal/resume-2026-09-28`. Do not
modify these candidates or their executing helpers/artifacts. Read live receipts
before deciding whether a stage remains active; do not restart it.

The two diagnosed mechanisms are:

1. `mapconcat` projected source strings through Rust text, losing extended
   characters, and rejected valid vector/list results and separators. The draft
   collects the actual mapped Lisp values and calls the existing canonical
   `concat` primitive, following `fns.c:Fmapconcat`. Callback execution still
   precedes concatenation and validation; unused separators are not validated.
2. The string reader treated `#x3FFF00..#x3FFF7F` as raw bytes, although GNU's
   byte8 range begins at `#x3FFF80`. It also ignored the hexadecimal digit count:
   `\\x80` denotes a byte, whereas `\\x080` denotes a multibyte character.
   The draft follows those `lread.c` distinctions.

New fixtures retain GNU-derived expected outputs for character boundaries,
encoding, mixed sequences, properties, forced GC, callback mutation and errors.
Negative source234 probes are retained. The first reader probe used a literal
Unicode command-line prefix under C locale; its original bytes/results are
preserved separately. The final ASCII-only fixture constructs the prefix with
`string`, so ordinary and internal evaluation receive the same character.

## Separate coding/copy recovery: source240

The [portable source240 draft and evidence](handover/2026-09-30-shared-reader-draft/source240-coding-recovery-draft-manifest.json)
are frozen in `target/runtime-goal/recovered-2026-09-30/coding-repair/emaxx`.
The 454-input incremental patch replays from `6ee4bd1b`, and a complete patch
from main is also retained. Git checkout modes and every input byte are verified.
The first replay audit extracted archive modes verbatim and stopped on 0664
versus 0644; the replacement helper corrects extraction setup, not source or
executable bits. This draft was kept separate while being validated; the later
source243 task checkpoint includes it, as described below.

Four negative source238 ordinary probes expose character loss or incorrect
encoding/identity behavior. UTF-8/raw-text encoders now use actual Lisp character
codes, including surrogates, five-byte characters and byte8, following
`coding.c:encode_coding_utf_8`, `encode_coding_raw_text` and `code_convert_string`.
EOL/BOM behavior and GNU's ASCII identity fast path are preserved; conversion
buffers retain extended characters and input properties. File output retains
the selected region's character metadata. Copying and `propertize` clone the
actual canonical string bytes and shallow property values, following
`fns.c:Fcopy_sequence` and `editfns.c:Fpropertize`.

The prior source239 draft passed strict checks and 155 focused tests, but failed
two new controls because `propertize` had already lost the characters before
encoding. Its exact failed executable/image, logs, fixtures and patches remain
preserved. Source240 leaves those expected outputs unchanged and adds a separate
canonical-copy control plus both existing property-copy controls. It passes
zero-warning strict checks, **160 focused gate tests / two existing ignores**,
all **944 affected gate tests**, and four new ordinary GNU comparisons.

The unchanged structured reporter, given one passing test, one unexpected
failure, one expected failure and one skip carrying U+F8FF, exits 255 without a
report on source238. Source240 exits successfully and retains exactly GNU's
four outcomes and character-bearing messages. The audit compares all report
fields except editor labels and measured durations; raw reports remain unchanged.
This independently repairs the reproduced report-writing failure. It does not
claim every possible report or coding system is correct.

The [completed release and ordinary audit](handover/2026-09-30-shared-reader-draft/source240-release-and-open-failures-manifest.json)
now verifies **160 focused release passes / two existing ignores**, all **944
affected release passes**, and all **57 preceding ordinary comparisons**. Combined
with the four new coding/copy fixtures, that is 61 audited ordinary comparisons.
Every raw name, verdict, artifact identity and process result is checked.
Release/ordinary supervisor **43217** has exited. The
[complete macOS audit](handover/2026-09-30-shared-reader-draft/source240-complete-macos-manifest.json)
now closes supervisor **43216**: **3,089 passes / two existing ignores**,
including 2,990 library, 60 binary and 39 integration passes. All 2,992 raw
library names/verdicts, native artifact identity and four retained executable/image
hashes are verified. The candidate remains frozen as its tested baseline.

A separately added ordinary save/revisit probe still fails on source240. Both
editors begin with `undecided-unix` for the source file and nil for the new file;
after saving, GNU records `utf-8-unix`, Emaxx `prefer-utf-8-unix`. Both read identical
file bytes and revisit as `undecided-unix`. This provides a nonterminal reproduction
of the coding metadata distinction. `fileio.c:choose_write_coding_system` consults
`find-operation-coding-system` and the unchanged Lisp
`select-safe-coding-system-function`; Emaxx's `current_write_coding` falls back to
`prefer-utf-8` for ASCII. Repair the actual selection path with GNU Lisp ownership
preserved; do not special-case the scenario, force the displayed indicator or
weaken the terminal comparison. Freeze a new source version for any further edit.

Source240 has no complete Linux, frozen or terminal certification. The new-file
coding selection, unresolved GNU census variation, further display review,
remaining architecture/accounting/ownership work and locked 16-workload 3%
performance requirement remain open. No current performance-parity claim exists.

## File-coding repair after source240

The separate candidate in
`target/runtime-goal/recovered-2026-09-30/file-coding/emaxx` starts from task
commit `3060be47` and includes the source240 coding/copy changes. It follows
`fileio.c:choose_write_coding_system` and `coding.c:coding_inherit_eol_type`:
explicit coding and its warning callback, local buffer policy, file-name rules,
the unchanged GNU Lisp safe-coding selector, EOL inheritance and unibyte/raw
selection. It reads the payload after the callback, so callback mutations are
visible, and restores the original marker-backed restriction state on return.
No mode-line expectation or terminal scenario is changed.

GNU's overwrite check calls `y-or-n-p`, and the exclusive-file check happens
when opening the file after coding selection. The candidate follows that order,
including a file created by a coding callback. The old Rust prompt test expected
all of `no RET` to be consumed. An actual ordinary PTY reproduction establishes
GNU consumes `n` and leaves `o RET`; source240 consumed everything. The corrected
test preserves the declining error and original file-byte assertions, and checks
the remaining two events. Earlier batch prompt attempts did not exercise that
interactive route; their failed results remain preserved.

Three new same-input GNU fixtures cover 24 selection/restriction cases, eight
overwrite/append/exclusive-open cases and the original save/revisit lifecycle.
Source241's 460-input draft compiles with three unused-helper warnings and fails
strict Clippy; no runtime tests execute. Source242 reuses the existing ordinary
nonexclusive write helper and passes strict checks with zero warnings. Its
focused run has **163 passes, one failure and two existing ignores**. Every raw
verdict and its exact failed executable/image are retained. The new selection
fixture fails only on two invalid-coding payloads: Emaxx returns an error string,
GNU the offending symbol. Ordinary and affected-module stages never run.

The [portable source243 checkpoint and selected validation](handover/2026-09-30-shared-reader-draft/source243-file-coding-recovery-manifest.json)
change the shared coding-error constructor to retain the actual Lisp
object, following `coding.c:Fcheck_coding_system`. An additional ordinary probe
shows source240 also loses an uninterned invalid symbol's identity in both encode
and decode errors; GNU retains it. The new identity control adds that coverage
without changing any preceding fixture or expected output. All **462 inputs and
Git checkout modes** replay from both task commit `3060be47` and main `21d20f0e`.
Strict checks have zero warnings. **165 focused tests pass with two existing
ignores**, all **948 affected-module tests pass**, and all **eight ordinary
GNU comparisons** pass through each editor's normal executable and image.
Every raw verdict, expected output and input hash is verified. Both the original
selection fixture and the new error-object identity control pass unchanged.

All **11 original file-lifecycle scenarios / 41 terminal comparisons** match.
This includes the original new-file coding-indicator divergence and the three
filesystem/revisit comparisons that source238 never executed. No action, timeout,
selector or comparison rule changes. A fresh actual PTY prompt probe produces
`(file-already-exists (111 13) "old")` in both editors, closing the separately
captured prompt discrepancy. This is selected terminal validation; GNU's source
and native ABI match, but its executable is not the pinned Darwin oracle.

The first affected-module audit mistakenly required 562 source inputs and stopped
before checking results. That failed helper and its launch traceback remain in
the archive. A separately named correction verifies the frozen 462 inputs, all
2,996 inventory names and all 948 selected verdicts without rerunning or changing
runtime tests. No failed audit is relabeled as passed.

Selected supervisors **53531** and **54143** have exited. Complete macOS **55867**,
release plus 57 preceding ordinary comparisons **55868**, and the full unchanged
226-scenario terminal run **55869** have started. Their active logs are excluded
from this archive. Source243 is now pushed as **`8d4fcd39fafcac4d7f69987714164129a769e094`**.
The [subsequent ordinary audit and Linux dispatch receipts](handover/2026-09-30-shared-reader-draft/source243-linux-launch-and-ordinary-manifest.json)
verify all 57 preceding ordinary comparisons, for **65 distinct ordinary
comparisons** with the eight already audited. Linux
[Rust 36822371375](https://github.com/rayfdj/emaxx/actions/runs/36822371375) and
[full frozen 36822376061](https://github.com/rayfdj/emaxx/actions/runs/36822376061)
both target exact `8d4fcd39`; launch does not establish a passing result.
Consult the `source243-*` receipts for current results. Root task runtime
matches all 462 source243 inputs; main remains source174. Source238's
complete frozen result does not certify these later changes. The full Linux GNU
census failure and all architecture, accounting, ownership and performance
requirements remain open.

The [completed source243 release audit](handover/2026-09-30-shared-reader-draft/source243-release-complete-manifest.json)
now closes supervisor **55868**. It verifies **165 focused release passes / two
existing ignores**, all **948 affected release passes**, every raw name/verdict
and the retained release executable/image. The only gate/release inventory
difference is the pre-existing debug-only file-descriptor ownership control.
This completes the same selected coverage in both profiles; complete macOS,
terminal and Linux results remain pending. The archive includes prepared Mac
auditors, which have not run yet. No complete-platform result is inferred.

The [subsequent complete macOS audit](handover/2026-09-30-shared-reader-draft/source243-complete-macos-manifest.json)
now closes **55867** with **3,093 passes / two existing ignores**: 2,994 library,
60 binary and 39 integration passes, including native artifact identity. Every
one of 2,996 raw library names/verdicts, four retained executable/image hashes
and all 462 source inputs are verified against published `8d4fcd39`. This closes
the prepared auditors above; terminal and Linux results remain separate and pending.

Nine GNU-only macOS probes vary the recorded full/single/group workflow selector
variables over three interleaved rounds. All match the original census fixture.
The new bounded `tools/diagnose_gnu_census.py` repeats that matrix before and after
the unchanged Rust single/group/single controls; all Rust processes hold those
selector variables constant at the full-run values. It retains GNU executable,
image and configuration identity, every raw output and intermediate Emaxx image,
and continues after a failed group to obtain the after-controls. Any failed
process keeps its failure status and yields a nonzero diagnostic exit.
Its nine GNU-only smoke probes also match on macOS; the Rust branch and Linux
diagnosis have not yet executed in this record. Both macOS probe sets are in the
archive. They cannot clear the Linux failure or establish its retaining root.

The [complete source243 Linux Rust audit](handover/2026-09-30-shared-reader-draft/source243-linux-complete-rust-manifest.json)
closes [run 36822371375](https://github.com/rayfdj/emaxx/actions/runs/36822371375)
on exact `8d4fcd39`: **3,105 passes / two existing ignores**, comprising 3,002
library, 61 binary and 42 integration passes. Native artifact identity, all
3,004 raw library names/verdicts, four retained executable/image hashes and the
CI build/formatting/strict-Clippy output are checked. There are no compiler warnings.
The original census control and both original suspended/thread-root controls pass.
The census fixture, expectation and helper bodies are byte-identical to source238;
this pass does not identify why the preceding GNU reference produced -9. Those
earlier failed results remain failed. GNU executable/image hashes were not retained
in this complete Rust job, so no cross-job GNU artifact identity is inferred.

The bounded [Linux diagnosis 36825694384](https://github.com/rayfdj/emaxx/actions/runs/36825694384)
is now dispatched on `3a35ad00` with the same 462 runtime inputs. It explicitly
retains GNU executable/image identity and preserves failed controls. Its result,
the complete frozen comparison and full terminal comparison remain pending.

## Closed source243 comparisons and coding-candidate regression

The [complete terminal and frozen-failure archive](handover/2026-09-30-shared-reader-draft/source243-complete-terminal-and-frozen-failure-manifest.json)
closes both comparisons above. Terminal supervisor55869 finishes successfully:
all **226 scenarios / 686 comparisons** match, including 658 screen and 28
filesystem comparisons. Inventories, actions, timeouts and all eight execution
inputs remain unchanged. Its GNU executable is source/native-ABI matched, not
the pinned Darwin executable.

Linux frozen36822376061 on exact `8d4fcd39` fails after **482 completely matching
files / 6,911 outcomes**. Per editor these comprise 6,693 passes, 43 expected
failures and 175 skips. While loading `test/src/comp-tests.el`, Emaxx unexpectedly
prompts for a coding system and exits255 on batch EOF. No test in that file starts:
177 outcomes are missing, while GNU runs and passes all 177. Another36 files
never start. Every completed pair, execution hash and the original test inventory
is audited; the failed run remains failed.

The unchanged ordinary compiler-file load reproduces the same prompt. A Lisp
trace shows the compiler's temporary file requests `utf-8-emacs-unix`, but
`find_coding_systems_region_internal_value` enumerates only one representative
per detection category. This omits valid base codings, including `utf-8-emacs`.
Source243's move to GNU's actual Lisp-owned file-writing selector exposes this
incomplete helper. Source238's passing frozen run and source243's passing Rust
gates do not certify the resulting file-load behavior.

A same-input negative fixture captures the missing coding candidates, custom
bases/alias exclusions, actual extended characters, changed category priority,
markers outside narrowing and the real coding cookie. GNU's expected output is
retained unchanged. Initial trace/fixture syntax errors remain separately
preserved; the corrected probes do not alter the runtime or upstream test.

The same archive closes bounded Linux GNU census diagnosis36825694384 on
source243-equivalent `3a35ad00`: all18 GNU probes and unchanged Rust
single/group/single controls (1/651/1 passes) match. Every raw verdict,
3,004-name inventory and eight retained input hashes are verified. The exact
Rust executable matches the complete source243 Linux gate; GNU executable and
image identities are explicitly retained here. Earlier source238 GNU identities
were not retained, so their variation's cause remains unresolved. The initial
auditors' Python3.9 and multiline-output setup failures remain preserved; the
separate corrected auditor verifies existing raw results without rerunning tests.

## Source244 coding-candidate repair

The [portable source244 repair and selected evidence](handover/2026-09-30-shared-reader-draft/source244-coding-candidates-selected-manifest.json)
are frozen in `target/runtime-goal/recovered-2026-09-30/coding-selection/emaxx`.
Its464 inputs and Git checkout modes replay exactly from task `6da1448e` and
main `21d20f0e`. The task runtime matched this candidate before source246 below.

Following `coding.c:Ffind_coding_systems_region_internal`, candidates include
every registered base coding rather than one detection-category representative.
The helper deduplicates actual non-ASCII Emacs character numbers and checks each
coding's declared charsets, preserving the non-Unicode and byte8 boundaries.
It always includes GNU's raw-text/no-conversion fallbacks and reads full-buffer
marker bounds outside narrowing. File-writing policy remains in GNU Lisp.
The new fixture and its captured GNU expected output are unchanged from the
negative source243 reproduction. No upstream selector, timeout, test or result
expectation is modified.

Strict checks pass with zero warnings. All166 focused tests pass, with the same
two existing TTY ignores; all949 affected-module tests pass. Every preceding
selected name is retained. Nine ordinary coding/file-writing comparisons match
exactly, and the unchanged compiler-file load succeeds in both editors. The
complete original compiler file then passes **177/177 tests in each editor**.
Raw reports, process/input hashes, discovered metadata and selected names match
the original frozen Linux compiler inventory. This is local source/native-ABI
matched GNU evidence, not pinned Darwin or complete Linux certification.

Full macOS supervisor72034, release/57 preceding ordinary comparisons72035 and
the unchanged full terminal inventory72036 are active. Their active logs are
excluded from the archive; immutable launch helpers/receipts are included.
The [closed ordinary audit and Linux dispatch receipts](handover/2026-09-30-shared-reader-draft/source244-linux-launch-and-ordinary-manifest.json)
verify all 57 preceding ordinary comparisons as well: **66 distinct ordinary
comparisons** match on source244. The runtime is now pushed as **`578955b3`**.
Linux [Rust 36830879269](https://github.com/rayfdj/emaxx/actions/runs/36830879269)
and [frozen 36830884261](https://github.com/rayfdj/emaxx/actions/runs/36830884261)
are dispatched on that exact source. Launch is not a pass. Read actual receipts
before resuming or restarting any job.

This repair does not establish every safe-coding primitive contract. In
particular, GNU also applies encoding translation tables and walks its actual
coding-system registration list; the current helper still needs that broader
review. Preserve those limits along with all prior failures. Complete correctness,
remaining shared-object/accounting/ownership work, pinned Darwin, the final
adversarial audit and the locked 16-workload/3% performance criterion remain open.


## Closed source244 results and source246 recovery

The [closed source244 archive](handover/2026-09-30-shared-reader-draft/source244-closed-validation-manifest.json)
closes all three local supervisors: complete macOS has 3094 passes/two existing
ignores, native artifact identity and all 2997 raw library names verified. Release
has 166 focused passes/two ignores and 949 affected passes. All 464 runtime inputs
match published `578955b3`; all 66 ordinary comparisons remain audited.

Linux frozen 36830884261 processes the complete 519-file / 7,928-outcome inventory and
all 1,038 processes exit successfully. Exactly 7,927 paired outcomes match. GNU alone
has an unexpected JSON-RPC timeout in `eglot-test-rust-completion-exit-function`;
Emaxx passes. Both editors pass all 177 original compiler tests, closing the
source243 compiler-load failure on Linux. Emaxx has 7,670 passes/47 expected failures/
211 skips; GNU has 7,669 passes/47 expected failures/one unexpected failure/211 skips.
The full frozen run remains failed; no status is normalized away.

Linux Rust 36830879269 has 2,258 passes and two failed GNU reference assertions,
before either control reaches Emaxx: the first empty-record sample is -7 rather
than 2 slots; the first empty-vector sample is -9 rather than 0. All later samples
match. Another 745 library tests and both Cargo stages never execute. The actual
GNU executable and dump are missing from this job's retained inputs; the cause
is unresolved. Two initial audit helpers expected an older download-receipt
schema and failed before writing any result. Their scripts and failures are
preserved; the final auditor checks every actual mapping entry and raw verdict
without rerunning runtime tests.

Source244 terminal comparison fails at GNU startup readiness in scenario 81,
`yank-pop`, with GNU still displaying `*scratch*`. All 80 earlier scenarios and
comparisons match; no divergence is observed. Another 145 scenarios and 606
comparisons remain unexecuted. This is not a complete terminal pass or evidence
of an Emaxx crash. All 8 execution inputs and 464 source inputs are verified.

The [source246 selected repair](handover/2026-09-30-shared-reader-draft/source246-coding-translation-selected-manifest.json)
preserves two new same-input negative fixtures. Source244 ignored encoding-safety
translation tables and dynamically bound coding lists. Its actual-character
path also passed U+E080–U+E0FF through a legacy regex-sentinel conversion, falsely
rejecting real private-use Unicode characters. Source243's unchanged ordinary
probe accepts those characters for UTF-8, confirming this is a source244
regression, not merely new coverage. Source245's four invalid Value-accessor
calls fail compilation; its exact patches and logs remain preserved, with zero
runtime tests executed.

Source246 follows `coding.c:get_translation_table`,
`character.c:translate_char` and `coding.c:Ffind_coding_systems_region_internal`:
it reads live tables/properties and bound lists, applies translations before
charset membership, preserves registry order/duplicates and EQ exclusions, and
keeps actual character numbers separate from the legacy raw-byte adapter. The
original fixtures and expected GNU outputs do not change. All 468 inputs/modes
replay from `0f5e6ff8` and main `21d20f0e`. Strict checks have zero warnings;
168 focused passes/two existing ignores, 951 affected passes and 11 ordinary
comparisons are audited. Both normal editors pass all 177 original compiler
tests with exactly the frozen selected names/discovered metadata. This local
GNU is source/native-ABI matched, not the pinned Darwin executable.

Full source246 macOS 86467, release plus 57 preceding ordinary comparisons 86468,
and unchanged full terminal 86469 are running. Do not modify their frozen
candidate or helpers. Active logs are excluded from the selected archive.
Linux dispatch follows the checkpoint push. Current root runtime matches all 468
source246 inputs; launch is never counted as a result.

The separate CI evidence improvement retains the configured GNU executable,
dump, config.h and Makefile before Rust/frozen validation and checks both source
and retained bytes afterward. It runs no warm-up GC and changes no test or
verdict. Real-GNU capture/verification, shell/YAML parsing and six synthetic
rejection controls pass: changed original, changed retained copy, missing
original, incomplete inventory, malformed manifest and duplicate capture.

The safety-helper repair does not establish general encoder/decoder translation
semantics. The normal fallback registration list still comes from interpreter
metadata; its authority/order needs review. Legacy charset adapters and nil/t
translation-symbol or overriding-plist-environment behavior also need review.
Main remains unchanged; complete correctness, all remaining architecture/
accounting/ownership requirements, final pinned Darwin/adversarial validation
and the locked 16-workload/3% performance criterion remain open.


The [source246 ordinary audit and Linux launches](handover/2026-09-30-shared-reader-draft/source246-linux-launch-and-ordinary-manifest.json)
close all 57 preceding ordinary comparisons: **68 distinct comparisons** match,
with every process, input and output verified. The checkpoint is pushed as
**`aaf31ce70abc920f9fc3395c01db7ffc57328f81`**. Complete Linux
[Rust36840211567](https://github.com/rayfdj/emaxx/actions/runs/36840211567) and
[frozen36840217747](https://github.com/rayfdj/emaxx/actions/runs/36840217747)
are dispatched on that exact source. The raw launch receipts' final narrative
sentence mislabeled source246 selected counts as source244; their machine fields
identify the correct source and commit. A supplemental correction preserves both
original receipts without changing any result. Those launch-time states are
superseded by the closed results below. Main and the full goal are unchanged.

## Closed source246 release/Linux Rust and isolated property repairs

The [closed release and Linux Rust archive](handover/2026-09-30-shared-reader-draft/source246-release-and-linux-rust-failure-manifest.json)
closes release supervisor86468: 168 focused passes/two existing ignores and all
951 affected passes, matching gate. All names, verdicts, 468 inputs and retained
executable/image hashes are verified. The single existing debug-only descriptor
ownership control explains the gate/release inventory difference.

Full Linux Rust36840211567 fails on exact `aaf31ce7` with 2,260 passes and the same
two GNU reference assertions: first empty record -7 rather than 2 slots, first
empty vector -9 rather than 0. All later samples match. Neither control reaches
Emaxx; 745 library tests and both Cargo stages never run. This time the GNU
executable, dump, configuration and Makefile were retained before validation and
verified unchanged afterward. GNU executable/dump hashes are `5e1721732427d69d8af63211f1e3833ec21e40f97f4b7f6cd4cb224b72cebfcf`
and `bf4974228f3b3ea4374cec75a38eeb2bd0e7dd944da5bec4e6ad0387092502ae`.

The [closed bounded diagnosis and source247 selected evidence](handover/2026-09-30-shared-reader-draft/source247-selected-and246-gnu-diagnosis-manifest.json)
verify Linux diagnosis36842893035 on `de944b6a`, with all 468 source246 inputs.
All 18 ordinary GNU probes and unchanged Rust single/group/single controls pass
(1/654/1), with every raw name/verdict and all eight retained artifacts checked.
The Rust test executable and all four GNU inputs match the failed full run byte
for byte. Emaxx images differ, but the failed assertions precede either Emaxx
comparison. The retaining root or environmental cause remains unproven; the full
run remains failed. The first raw auditor assumed verdicts occupied one line and
missed thirteen blocks containing emitted runtime messages. Its separate repaired
auditor verifies each complete named block's terminal verdict; no runtime test,
log, inventory or expectation changes, and the first failed helper is preserved.

Two new ordinary GNU/source246 fixtures expose property defects. Translation-table
resolution omitted `nil` and `t` as symbols and bypassed dynamically overriding
properties. The public `get` helper also continues past a matching nil override
to a later duplicate. GNU instead stops at the first key, and a nil override
falls back to the symbol's own plist. Same-name uninterned symbol/key identity and
skipping atom entries already match; the probes do not establish an identity bug.

The [portable source247/248 drafts](handover/2026-09-30-shared-reader-draft/source248-property-drafts-manifest.json)
preserve both negative source246 results and exact incremental/complete patches.
Source247, in `coding-properties/emaxx`, shares the existing direct `get` helper
for translation-table properties and recognizes GNU's symbol types. Its 470
inputs have zero-warning strict checks, 169 focused passes/two existing ignores,
952 affected passes, twelve ordinary comparisons and a successful original
compiler-file load. The original GNU outputs remain unchanged. Supervisor90626
has exited and all selected evidence is audited. This is not a complete 177-test
compiler run or release/platform validation.

Source248, in `coding-property-order/emaxx`, includes source247 and changes only
the shared lookup's first-nil-match behavior, with an additional GNU fixture.
Its 472 inputs and modes replay from `de944b6a` and main. Queue94807 waited for
source247's actual successful exit and audit, copied its completed build cache,
and has started source248 selected validation. Do not edit either frozen candidate
or the executing helpers. Source248 has no audited runtime result yet; neither
draft is applied to root source246. Read current receipts rather than restarting.

The source244 Eglot trace retains a completion reply containing 91 items in the
GNU process-buffer dump despite the request timeout. Two connections and duplicated
dumps prevent proving its request association/timing. The same signature appears
in the historical `docs/frozen-run-success.md` record, including a later unchanged
single-file pass; no new Emaxx regression or GNU repair follows from this trace.
An additional read-only search of task-target and configured GNU source locations
found no pinned Darwin executable; that limited search does not prove universal
absence. No oracle identity or pin was changed.

Full source246 macOS86467, terminal86469 and frozen36840217747 remain active in
this saved record. Main remains source174. General encoder/decoder translations,
normal registry authority/order, legacy adapters, shared representation,
accounting, ownership, final validation/audit, pinned Darwin and the unchanged
16-workload/3% performance criterion remain open.

## Source246 complete frozen/macOS/terminal recovery and source248 selected pass

The [complete source246 archive](handover/2026-09-30-shared-reader-draft/source246-complete-frozen-macos-terminal-manifest.json)
closes macOS86467 and terminal86469, both now exited. Complete macOS has **3,096
passes / two existing ignores** (2,997 library, 60 binary and 39 integration
passes), native artifact identity, all 2,999 raw library names/verdicts and four
retained executable/image files verified. All 468 inputs match `aaf31ce7`.
Terminal comparison matches all **226 unchanged scenarios / 686 comparisons**
(658 screen, 28 filesystem), with all eight execution inputs rehashed. Local GNU
matches source/native ABI and remains different from the pinned Darwin executable.

Linux [frozen36840217747](https://github.com/rayfdj/emaxx/actions/runs/36840217747)
passes the complete **519-file / 7,928-outcome** inventory on exact `aaf31ce7`.
All **1,038 processes** succeed. GNU and Emaxx each have 7,670 passes, 47 expected
failures and 211 skips. All paired names/statuses/expectations, execution hashes,
inventory metadata and 5,204 downloaded artifact members are audited. The actual
configured GNU executable, dump, configuration and Makefile were captured before
the run and verified unchanged afterward; the supplemental raw audit adds this
actual-artifact evidence to the generic verifier's provenance-only qualification.
The original 177 compiler tests pass in both editors. No selector, expectation,
timeout or comparison rule changed. Earlier failed runs stay failed; this result
does not establish the cause of the GNU Eglot timeout or Linux Rust census failure.
The [load-marker supplement](handover/2026-09-30-shared-reader-draft/source246-frozen-load-markers-manifest.json)
preserves the 1,038 `.loaded` files and recorded AppArmor profile omitted by the
first archive's extension filter. Together both archives retain every downloaded
frozen member except the two GNU executable/dump files; their actual bytes remain
locally retained and verified. No raw result or earlier archive is rewritten.

The [closed source248 selected archive](handover/2026-09-30-shared-reader-draft/source248-selected-validation-manifest.json)
closes queue94807. All 472 inputs/modes are frozen; zero-warning strict checks,
**170 focused passes / two existing ignores**, **953 affected passes**, thirteen
ordinary comparisons and the original compiler-file load pass. The two retained
source246 negative property fixtures now match GNU unchanged. Source248 is still
isolated in `coding-property-order/emaxx`; original complete compiler comparison
1840 is running. Its selected results do not supply full platform, release,
frozen, pinned Darwin or performance certification. Root runtime remains source246.

The complete source246 archive also retains the new GNU-only GC diagnosis helper,
read-only GDB observer, static validator and ordinary macOS smoke. Twenty ordinary
processes use the two unchanged census fixtures, varying only a documented unused
environment padding value; all match their original expected output. Fifteen
synthetic checks validate normal/pseudo/bool vector sizes, mixed small/large/free
inventory traversal and rejection of invalid sizes/block chains. Both Python ASTs,
the workflow YAML and all eight shell blocks pass static checks. The observer
checks the actual GNU ABI, reads vector mark bits and possible C-stack references
at the first eight explicit collections, and requires its live-slot sum to match
GNU's returned census. It performs no inferior calls or writes and adds no GC.
Actual Linux GDB execution is required before claiming any observation; candidate
stack references alone would not establish the real marking root. The new workflow
mode is diagnostic only. No full Linux Rust repair or completed goal is claimed.
