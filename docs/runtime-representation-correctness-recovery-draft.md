# Correctness recovery — 1 October 2026

The [full goal](runtime-representation-goal.md) is active and incomplete. The
user's latest question challenges the correctness regression. The immediate
work is to restore correctness before further architectural changes. The task branch did regress after the fully matching source207 Linux
frozen checkpoint; those historical results do not certify later shared-closure,
canonical-string and direct-bytecode changes. Main remains source174 at
`21d20f0e`. Do not merge the unfinished branch or declare the performance goal met.

## Current checkpoint: source272

The [borrow-root repair](handover/2026-09-30-shared-reader-draft/source272-string-borrow-selected-manifest.json)
passes 206 focused controls and 1,062 affected controls in each profile, retaining
two existing ignores and every original test/fixture. The preserved source271
negative shows an off-stack Rust string guard retaining its header but losing a
cons property. The repair traces that root before weak tables/sweeping and rejects
exclusive guards before starting an epoch. New controls verify survival and
reclamation. This host-ownership case was outside the earlier frozen fixtures;
passing those fixtures did not establish it. Broader public ownership remains open.

The preceding source270 [complete Rust audits](handover/2026-09-30-shared-reader-draft/source270-complete-rust-platforms-manifest.json)
close 3,118 macOS / 3,130 Linux passes with two existing ignores each, all raw names,
native identity and retained inputs. Its frozen/terminal runs remain separate.
Full source272 validation is prepared but not launched at this checkpoint.
Read [HANDOVER.md](../HANDOVER.md) for live continuation state. All historical
failures below remain failures; no performance parity or final completion is claimed.

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

## Source248 compiler pass, GNU C review and applied checkpoint

The [compiler audit and complete launches](handover/2026-09-30-shared-reader-draft/source248-compiler-and-complete-launch-manifest.json)
close compiler1840 with all **177 original tests passing in both editors**.
The raw audit checks every name, selector and discovered metadata against the
original complete frozen compiler inventory, every process hash, and all 472
source inputs. Source248 is now applied to the root runtime. Full macOS4528,
release plus 57 preceding ordinary comparisons4529, and terminal4530 are running
from the unchanged isolated candidate. A launch is not a passing result.

The [C-reference review](handover/2026-09-30-shared-reader-draft/source248-c-reference-and246-trace-failure-manifest.json)
records unchanged GNU revision `636f166cfc86aa90d63f592fd99f3fdd9ef95ebd` and
hashes of the actual C sources read, with these narrow mappings:

| Repair | GNU C behavior | Emaxx change |
| --- | --- | --- |
| Translation-table symbol properties | `coding.c:get_translation_table` calls `Fget` for `SYMBOLP` entries; `lisp.h:SYMBOLP` includes enabled positioned symbols. Nil/t are symbols. | Use the existing `direct_get` path for coding-owned symbol/list entries, including nil/t and dynamic overriding properties. |
| Nil override before a duplicate | `fns.c:plist_get` returns the first EQ key's value. `Fget` falls back to the real plist when that value is nil. | Stop at the first matching key even when nil, instead of returning a later duplicate. |

Both original source246 negative fixtures, expected outputs and ordinary GNU
results are byte-identical; the source248 executions now match them. This patch
adds no representation, property cache or replacement Lisp policy. It does not
certify every surrounding coding/property behavior. Architecture changes remain
on hold until complete correctness validation closes.

The same archive preserves failed Linux GNU diagnosis
[36848112001](https://github.com/rayfdj/emaxx/actions/runs/36848112001) on `2dd1dea8`.
Ten ordinary first-fixture processes match, then GDB passes the Lisp argument
split and escaped because the observer disables its startup shell. GNU reports
end-of-file before any explicit collection. Zero inventories are observed; the
second fixture and final input/oracle verification never run. All 31 downloaded
members are verified, the exact executed tools are retained, and the two GNU
executable/dump files are retained locally with hashes in the portable evidence.
This is an observer setup failure, not an explanation or repair of the original
GNU census failures. Main, pinned Darwin and the full goal remain unchanged.

## Source248 pushed; release and preceding ordinary comparisons close

Source248 is pushed as `fa7578ac04a697d45acb59bb296a8365541b29c9`, and draft PR79
records its current evidence and open failures. The [release and Linux-launch archive](handover/2026-09-30-shared-reader-draft/source248-release-and-linux-launch-manifest.json)
closes supervisor4529, now exited: **170 focused passes / two existing ignores**
and **953 affected passes**, matching gate. All raw test names/verdicts and the
retained executable/image are checked. The one existing debug-only descriptor
ownership control is the sole gate/release inventory difference.

The same archive verifies all **57 preceding ordinary comparisons**, in addition
to the thirteen selected fixtures: **70 distinct matches**. Original fixture modes,
expected bytes, C locale and normal `-Q` entry points remain unchanged. Full Linux
[Rust36849791338](https://github.com/rayfdj/emaxx/actions/runs/36849791338) and
[frozen36849797068](https://github.com/rayfdj/emaxx/actions/runs/36849797068) run
on exact `fa7578ac`. Full macOS4528 and terminal4530 remain active; these launches
do not establish a complete platform result.

The observer setup correction restores the startup shell expected by GDB's
`--args` escaping and reads `/proc/PID/cmdline` at startup. The driver now requires
that actual argument vector to equal the original ordinary GNU command before
accepting any observation. Python syntax and diff checks pass. This corrects
the demonstrated tool setup mistake, follows the linked GDB manual, and still
needs actual Linux execution; it changes no runtime input, fixture, oracle byte,
workflow selector or failed gate verdict. The previous executed helper is preserved.

## Source248 complete Rust/frozen passes; terminal startup failure

The [complete result archive](handover/2026-09-30-shared-reader-draft/source248-complete-rust-frozen-and-terminal-failure-manifest.json)
closes macOS4528 and initial terminal4530; both supervisors have exited. Source248
has **3,098 complete macOS passes / two existing ignores** (2,999 library, 60
binary, 39 integration) and **3,110 complete Linux passes / two existing ignores**
(3,007 library, 61 binary, 42 integration) in
[run36849791338](https://github.com/rayfdj/emaxx/actions/runs/36849791338).
All 3,001/3,009 raw library names/verdicts, native artifact identity and four
retained inputs per platform are verified. All 472 runtime inputs match `fa7578ac`.
The unchanged original GNU census assertions pass in this full run. Its actual
GNU executable/dump/configuration/Makefile match the earlier failed source246
run and are verified unchanged; their historical variation remains unexplained.

The complete [Linux frozen run36849797068](https://github.com/rayfdj/emaxx/actions/runs/36849797068)
passes on exact `fa7578ac`: **519 files / 7,928 matching outcomes / 1,038 successful
processes**. Each editor has 7,670 passes, 47 expected failures and 211 skips.
All original compiler177 tests pass. Every raw execution hash, original inventory,
paired outcome and captured GNU input is verified. All 5,204 downloaded artifact
members are checked; all but the two GNU binary/dump files are portable, including
all 1,038 load markers. Binary/dump bytes remain locally retained with hashes.
The generic audit's provenance-only qualification is supplemented by the raw
actual-GNU-input audit. No earlier failed run is converted to a pass.

Initial terminal4530 remains failed: **161 fully matching scenarios / 295 matching
comparisons**, zero observed divergences, then GNU startup readiness fails before
scenario162 `recursive-minibuffer` begins comparing. Another 64 scenarios never
start, and **391 comparisons never execute**. Every verdict and all eight execution
inputs are verified. The stopped scenario's exact seven comparisons pass separately
after local Rust finishes. Full unchanged terminal retry14540 is now running
without a simultaneous local Rust gate. Its new helper/log/receipt names preserve
the initial failure; source, executables/images, actions, timeouts, settle rules,
inventory and comparison strictness are unchanged. No startup cause is established.

## GNU GC observation and established conversion-table gap

The [trace and ordinary-probe archive](handover/2026-09-30-shared-reader-draft/source248-gnu-trace-and-conversion-gap-manifest.json)
closes corrected GNU diagnosis36850342459 on `163064c9`. Twenty ordinary processes
match their original expectations. Two successful GDB processes receive exactly
the original argument vectors and produce the expected Lisp values. Sixteen
marked inventories independently sum to GNU's returned vector-slot counts, with
no previous live object disappearing during those observations. This verifies
the observer correction but does not reproduce or explain the earlier -7/-9
variation or identify a retaining root. Four actual GNU inputs match the prior
failed full run and are verified unchanged; all 55 downloaded members are checked.

The separate `coding-live-conversion-translation.el` fixture demonstrates the
already documented general conversion gap. GNU applies coding-owned encode/decode
tables, observes their mutation, composes standard tables and honors the enable
flag. Source248 leaves characters untranslated in all four cases; the disabled
case happens to match. The ordinary processes exit successfully without stderr
and all source/input hashes are unchanged. The same fixture in the recorded
source174/main and source207 executable/image pairs produces source248's exact
output. Both pairs are verified against their original immutable receipts and
run from their original locations, preserving relative native-library paths.
An earlier relocated source174 launch failed loading its dump before Lisp; that
failure and helper are preserved separately, not counted as a baseline verdict.

This specific gap predates the later architecture migrations and still needs
repair. GNU `coding.c:encode_coding` obtains translation tables before calling
`consume_chars`; `decode_coding` passes the tables to `produce_chars`. Read those
complete C mechanisms, including non-scalar translations, before implementing
the repair. The existing safety-character helper does not establish general
encoding/decoding semantics. Runtime source248 is unchanged; the full goal,
pinned Darwin, final audit and locked performance criterion remain open.

## Isolated source249 post-read repair

The [portable draft](handover/2026-09-30-shared-reader-draft/source249-post-read-draft-manifest.json)
preserves two exact negative fixtures and their GNU outputs. Source248 passes
Rust projection byte counts to post-read conversion: one four-character decoded
input produces a hook argument of ten. It also invokes the hook for an ASCII
string result, incorrectly copying the original properties and losing NOCOPY
identity. GNU returns from that ASCII path before conversion. Both recorded
source174/main and source207 executable/image pairs produce the same wrong
outputs as source248, with unchanged artifact hashes. These particular defects
predate the migrations; the real regressions documented above remain regressions.

The separate `coding-post-read/emaxx` candidate uses the decoded temporary buffer's
existing character count, following `coding.c:decode_coding_object`. Its ASCII
return follows `code_convert_string`: preserve the original object with NOCOPY,
otherwise make a fresh unpropertized multibyte string. Source regions and buffer
destinations still execute conversion hooks. No representation, cache or registry
is added. The fixtures retain the exact GNU output bytes, including raw bytes,
non-ASCII characters, EOL conversion, properties, identity and hook invocation.

All **476 inputs and Git checkout modes** replay from `97385010` and main.
Queue **15814** waits for source248 terminal **14540** to exit before cloning its
completed cache and running strict checks, selected gate/release controls and
fifteen ordinary comparisons. The archived snapshot precedes all build/runtime
validation; its prepared auditors are not passing results. Read actual receipts
before starting another job. Root runtime remains source248 and main is unchanged.

The C review also confirms that the ASCII string shortcut intentionally bypasses
translation tables. The general translation repair must respect that behavior,
table sequences, EOL ordering and raw-character handling; it is not implemented
here. Broader hook/destination contracts, adapters, ownership, accounting, complete
platform validation, pinned Darwin and the full performance goal remain open.

## Source248 terminal recovery complete

The [closed full retry](handover/2026-09-30-shared-reader-draft/source248-complete-terminal-retry-manifest.json)
verifies all **226 scenarios / 686 comparisons**: 658 screen and 28 filesystem
comparisons, with no divergence or missing comparison. Supervisor14540 has exited.
Every raw label/verdict, 472 runtime inputs and eight execution inputs is checked.
The commands, original scenarios/actions, timeouts, settling and comparison rules
match the first run. This new pass does not erase or explain its GNU startup
failure; both that failure and the selected replay remain in the archive.
GNU is source/native-ABI matched, not the pinned Darwin binary.

Source248 now has the complete Rust, pinned Linux frozen and terminal checkpoint
passes described above. The remaining architectural work can proceed from this
tested baseline, preserving those contracts. This supersedes the earlier pause
in architecture work; it does not establish universal correctness or completion
of the full goal. The updated [requirement map](runtime-representation-progress.md)
records remaining string/symbol authority, VM stack layout, ownership, accounting,
final validation/audit and all locked performance requirements.

Queue15814 subsequently passed strict checks and stopped at the selected gate:
162 passes, ten failures and two existing ignores. Six fixtures could not load
GNU early Lisp and four could not canonicalize the absent sibling `../emacs`.
The two new hook tests passed. The raw failure and its executable/image are
retained in `source249-setup-failure-audit.json`; the first audit helper's incorrect
assumption about all ten failure locations is also preserved.

Restoring only that checkout link makes the same executable, arguments, full
174-test selection and environment pass: 172 passes / two existing ignores.
Continuation17624 has exited. The [closed selected audit](handover/2026-09-30-shared-reader-draft/source249-selected-and-setup-manifest.json)
verifies strict zero-warning checks, 172 focused passes / two existing ignores
and 955 affected passes in both profiles, plus all fifteen ordinary GNU matches.
All previous selected controls remain, with exactly the two new post-read tests;
the gate/release full inventories differ only by the existing debug-only POSIX
test. Original fixture and expected-output bytes are unchanged. No runtime,
expectation, selector or timeout changed for the setup replay. All 476 validated
inputs are now applied to the root. Complete source249 validation remains open.

The [publication and launch snapshot](handover/2026-09-30-shared-reader-draft/source249-publication-and-launches-manifest.json)
verifies pushed runtime `99e61fca`, all 476 inputs and draft PR79. Full Linux
[Rust36879095484](https://github.com/rayfdj/emaxx/actions/runs/36879095484) and
[frozen36879103495](https://github.com/rayfdj/emaxx/actions/runs/36879103495) target
that exact commit. Their results remain pending. Full macOS Rust64326 is queued
after rebuilt-oracle frozen58661, against the same isolated runtime inputs and
new GNU installation. Complete source249 terminal validation remains unstarted.

## Recorded Darwin oracle rebuild

The user requested rebuilding the GNU reference from the recorded recipe.
`rebuild-gnu-macos-20261001.py` runs `tools/build_macos_oracle.py` on pristine
revision `636f166c` into `target/runtime-goal/rebuilt-2026-10-01/emacs`.
The [closed rebuild record](handover/2026-09-30-shared-reader-draft/gnu-macos-rebuild-20261001-manifest.json)
verifies successful supervisor17623 and verifier31221; both have exited.
The executable SHA-256 is
`29cfb20de2474c040d439d4bdf69b4dfcfb7921464f1f686b6e1f6bcca7c74e5`.
All recorded capabilities/configure options match the retained GNU build, including
native compilation and Apple's libxml2. Regenerated native ABI, C primitive and
DEFSYM manifests are byte-identical to the committed files. The executable, dump,
configuration and Makefile are retained and unchanged; no ABI edit is needed.

Preparation40013 also exited successfully. Local isolated source249 candidate
`1c0898fc` records the proposed binary pin; only its hash differs in the lock.
The task branch's lock and the original GNU tree remain unchanged. The first
frozen launch exits 2 before any test because a copied Cargo target lacks its
ownership marker. The failure and unclaimed cache are preserved. Retry58661 uses
the same command, candidate, GNU inputs, inventory, selectors and timeouts, letting
the unchanged harness create an empty target. It is active; no inventory/frozen
pass or full-goal completion is claimed by the rebuild archive.
The [oracle build contract](oracle-build-contract.md#darwin-build) now directly
documents the Apple-libxml2 recipe previously recorded in the frozen-run history.

## Source249 completed Linux and rebuilt-GNU validation

The [closed Linux and Darwin frozen archive](handover/2026-09-30-shared-reader-draft/source249-linux-and-darwin-frozen-manifest.json)
verifies both successful Linux workflows on exact runtime `99e61fca`.
[Rust36879095484](https://github.com/rayfdj/emaxx/actions/runs/36879095484) has
**3,112 passes / two existing ignores**: 3,009 library, 61 binary and 42 integration
passes. All 3,011 raw library names/verdicts, native artifact identity, four retained
inputs and all 476 published source inputs are verified. The actual GNU executable,
dump, configuration and Makefile match the earlier census-failure runs; their
historical -7/-9 variation remains unexplained.

[Frozen36879103495](https://github.com/rayfdj/emaxx/actions/runs/36879103495) matches
all **519 files / 7,928 outcomes**, with 1,038 successful processes. Each editor
has 7,670 passes, 47 expected failures and 211 skips. All 177 original compiler
tests pass. Every raw execution hash, paired name/status/expectation and GNU input
is verified. All 5,204 original artifact members match the retained downloaded ZIP;
all but the two large GNU executable/image files are portable. The original
failures are not relabeled by these later passes.

Rebuilt-GNU macOS frozen58661 finishes with **7,915 matching / six mismatching
outcomes**, all 519 files and 1,038 processes complete. Both editors report 7,623
passes, 46 expected failures and 252 skips, with no unexpected outcomes. All 177
compiler tests pass. The six `emacs-tests/seccomp/*` tests skip in both editors,
but their diagnostic strings include different `system-configuration-features`.
Emaxx's existing disclosed capability policy deliberately avoids advertising GNU
autoconf features it does not implement. No feature string or comparison rule is
changed. This is a failed strict comparison; skipped tests are not passes. Raw
unfavorable timings remain in the archive and are not locked-workload measurements.

The [complete macOS Rust and reference adoption archive](handover/2026-09-30-shared-reader-draft/source249-macos-and-oracle-adoption-manifest.json)
closes64326 with **3,100 passes / two existing ignores**: 3,001 library, 60 binary
and 39 integration passes. All 3,003 raw library names, native artifact identity
and four retained inputs are verified. The validated checkout is clean `1c0898fc`;
all 476 runtime inputs match published `99e61fca`.

The task branch now adopts the requested rebuilt Darwin reference `29cfb20d…`,
preserving the original `591acf7b…` lock and local configuration. Source revision,
configuration/capabilities, Apple libxml2 and generated native/C manifests agree
with the recorded contract. Fresh GNU discovery from all completed full-run
reports renders the original canonical inventory byte for byte; no separate
`list` command is claimed. The four actual GNU inputs are verified unchanged after
Rust validation. Only the Darwin executable-hash field changes in the lock;
Linux pin, runtime inputs, selectors and timeouts remain unchanged. The six
diagnostic failures remain visible after repinning.

The [closed full terminal archive](handover/2026-09-30-shared-reader-draft/source249-complete-rebuilt-terminal-manifest.json)
verifies all **226 scenarios / 686 comparisons** on the exact source249 gate
executable/image from the rebuilt-GNU frozen run. Every original label, all 658
screen and 28 filesystem matches, 476 source inputs and ten execution inputs are
checked. Supervisor12347 has exited after local full Rust. No divergence or
unexecuted comparison remains. The six strict macOS frozen diagnostic mismatches
remain failures; this terminal result does not certify the subsequent drafts.

## Separate common-string continuation

The [source256 draft](runtime-representation-common-strings-draft.md) removes the
separate plain-string representation, binding/callback copies and VM text adapter.
Two ordinary source249 probes expose lost mutation/identity/properties and incorrect
`fillarray` behavior. Exact fixtures and GNU expected outputs remain unchanged;
the probe does not establish these defects' historical origin.

Source255's strict checks pass, followed by 181 focused passes, one bytecode fixture
failure and two existing ignores. Both new GNU contracts pass. GNU confirms that
the failing local fixture used Unicode text where `make-byte-code` requires actual
unibyte storage. Source256 corrects construction, keeps every original assertion
and adds rejection of the old Unicode form. All strict checks pass with zero
warnings; all 480 inputs/modes replay exactly. Queue17749 has exited. Its focused
gate run [passes 182 tests / two existing ignores](handover/2026-09-30-shared-reader-draft/source256-focused-gate-manifest.json), with all 184 selectors and
retained inputs audited. The [complete broader gate audit](handover/2026-09-30-shared-reader-draft/source256-broad-failure-and259-focused-manifest.json)
records 956 passes and one invalid native-bytecode fixture failure. GNU confirms that
the intended three opcode bytes must be unibyte: that form returns 42 while the
five-byte Unicode form is rejected. Release/ordinary stages did not run. Separately,
source259 passes 188 focused tests / two existing ignores, including all four
comparison/hash GNU fixtures and the extended dump test. Its broader run has closed
with 961 passes and the same invalid native fixture failure, all 962 names audited.
After it exited, source261 corrected only that test with actual unibyte storage,
preserving every native assertion and adding Unicode rejection. Source260's new-test
lint failure remains recorded. Source261 strict checks have zero warnings; both
focused profiles pass 189 tests / two existing ignores, and all 962 affected gate
tests pass. A release retention-count audit failure is preserved and corrected by
verifying the release executable/image plus the older gate image. Continuation30525
has exited with all 962 affected release tests and 22 ordinary GNU comparisons
passing. The [closed selected audit and publication](handover/2026-09-30-shared-reader-draft/source261-common-string-selected-manifest.json)
verify both complete selected profiles and every original fixture/artifact. Source261
is now applied and pushed as `eddfe782`, with 488 exact inputs. Its
[complete macOS Rust audit](handover/2026-09-30-shared-reader-draft/source261-complete-macos-rust-manifest.json)
verifies **3,107 passes / two existing ignores**, all 3,010 library names, native
artifact identity, four retained inputs and unchanged source. Supervisor31023 has
completed terminal validation after the frozen comparison. Exact-head Linux
[Rust36904097575](https://github.com/rayfdj/emaxx/actions/runs/36904097575) and
[frozen36904105841](https://github.com/rayfdj/emaxx/actions/runs/36904105841) are complete.
The [closed Linux audit](handover/2026-09-30-shared-reader-draft/source261-complete-linux-manifest.json)
verifies **3,119 Rust passes / two existing ignores**, all 3,018 library names,
native artifact identity and **519 frozen files / 7,928 matching outcomes / 1,038
successful processes**. Every raw execution hash and actual GNU input is checked;
7,670 passes, 47 expected failures and 211 skips per editor remain distinct.
The [complete macOS frozen audit](handover/2026-09-30-shared-reader-draft/source261-complete-macos-frozen-manifest.json)
retains **7,915 matches / six strict mismatches**, all 519 files and 1,038 successful
processes. Both editors report 7,623 passes, 46 expected failures and 252 skips; all
177 compiler tests pass. The same six build-feature skip-diagnostic differences as
source249 remain failures, with unchanged comparison/capability rules. Raw outcomes,
hashes and actual GNU/Emaxx inputs are verified. The
[complete terminal audit](handover/2026-09-30-shared-reader-draft/source261-complete-macos-terminal-manifest.json)
verifies 226 scenarios / 686 matches (658 screen and 28 filesystem comparisons), all
original labels, 488 source inputs and ten execution inputs. Its executable/image
is the exact frozen-run pair. No source261 supervisor remains running.
Source255's failed executable/image, all compiler/lint/helper failures
and original negatives remain retained. Final-source validation remains required; no physical-memory,
ownership, performance or full-goal completion is claimed.

The [source262 ordering negatives and repair draft](handover/2026-09-30-shared-reader-draft/source262-string-ordering-draft-manifest.json)
retain **154 wrong source261 results among 1,024 symbol/storage ordering rows** and
**56 among 400 version rows**. Four ordering regressions are bracketed after
source207 and by source249; source261's whole ordering matrix matches source249.
The exact introducing commit is unestablished. Version output is unchanged from
main. Earlier frozen passes did not cover these defects. Source262 follows actual
GNU `string_cmp` and `filenvercmp`, replacing host-text ordering with canonical bytes
and Lisp symbol names. Its 492 inputs replay exactly and all strict checks pass.
Queue38430 was withdrawn while waiting, before any runtime stage, in favor of
source263. Its unchanged source, helpers and original queued state are retained;
no runtime pass or failure is inferred. Original negatives and GNU expectations remain.

The [unapplied source263 successor](handover/2026-09-30-shared-reader-draft/source263-string-ranges-draft-manifest.json)
adds repairs for 12 range/error differences, 134 character-comparison differences,
seven live-case-table comparison differences and nine numeric casing differences.
Main reproduces the range/live/numeric outputs; its character matrix fails before
comparison at an unsupported extended-character constructor, which stays recorded.
The repair follows actual GNU validation order, canonical byte cursors, octet
promotion before casing, and direct numeric case-table keys. All source262 code
and controls remain. Its 500 inputs replay exactly and strict checks have zero
warnings. Queue90568 has exited with 197 focused passes / two existing ignores,
then 1,053 affected passes / one failure. The Unicode comparison used bare ASCII
tables while expecting dumped GNU Unicode mappings. The actual C/loadup initialization
and an ordinary probe constrain the successor's corrected fixture. Release and
ordinary stages did not run. The
[additional prefix-boundary control](handover/2026-09-30-shared-reader-draft/source263-ordering-boundary-control-manifest.json)
matches all 12,600 GNU/source261 baseline rows. Queue90569 stopped before its first
stage because90568 failed. Source263 remains an unchanged failed candidate; no
complete selected pass is claimed.

The [queue correction](handover/2026-09-30-shared-reader-draft/source263-fixture-queue-repair-manifest.json)
records withdrawal of original queues64601/87187 while waiting, before runtime
execution. Their ordinary wrapper named a nonexistent fixture. The replacement
uses the original `string-ordering-symbol-storage`; all 28 fixture paths are checked.
Source, tests, expected bytes, selectors, timeouts and comparison rules are unchanged.
Current receipts are `source263-fixture-validation-*` and
`source263-boundary-fixtures-*`; all original helpers and waiting state remain.

The [subsequent empty/pure storage baseline](handover/2026-09-30-shared-reader-draft/source261-string-storage-baseline-manifest.json)
finds six wrong empty-identity rows among12 and57 wrong pure-storage rows among96
on source261, including28 operation/poststate differences. Seven whole rows that
matched GNU on source249 now differ; six other rows improve. The seven regressions
all involve the first pure copy of an ordinary empty unibyte string, incorrectly
returned by the shared normal constructor. The precise introducing source between249
and261 is unestablished. Pure-string writes already differ in preceding checkpoints;
source174/main and207 outputs match exactly. Original syntax/scope-invalid probes
are preserved and excluded, as are any claims from their outcomes. Raw and explicit
value pure fixtures overlap. Source263 is unchanged and does not repair these gaps;
they must be addressed before claiming full correctness or advancing to main.


The [source264–268 storage preparation](handover/2026-09-30-shared-reader-draft/source268-string-storage-draft-manifest.json)
packages the closed source263 failure and the initial compact-header/pure-storage
repair. Dedicated string cells contain a direct 32-byte GNU-layout header and 16
bytes of checked-borrow/allocator metadata. Normal empty forms are canonical;
pure copies allocate distinct permanent objects and obey write/no-op ordering.
Dump/template restoration preserves the relevant identities and ordinary mutability.
Packed property spans replace the nested vector allocation; actual intervals and
sblocks remain unfinished. A 144-row property baseline retains 106 wrong source261
whole rows, including 84 operation/poststate differences; counts overlap prior probes.
An ordinary case-table fixture gives GNU `((t t) (2 1))`, source261 `((t t) (t t))`.
Three bare-GNU startup failures are retained and excluded. Compiler failures264/265,
Clippy failure266 and the corrected source263 raw-verdict parser remain recorded.

The [source269 caller correction](handover/2026-09-30-shared-reader-draft/source269-string-storage-caller-repair-manifest.json)
retains source268's 199 focused passes / four failures / two existing ignores. All
four failures occurred at a GNU precondition because the new callers kept the
expected file's trailing newline. Source269 changes those four callers to the
existing `trim_end` convention; GNU files and shared comparison rules are unchanged.
It then has 201 focused passes / two failures / two existing ignores. Empty identities
and 144 property rows pass; 94 of 96 pure-storage rows match. Copying an empty pure
string retained its read-only header instead of using GNU's ordinary constructor.
The other failure occurs before an independent fixture assertion because the test
still owns a prior native image. All raw verdicts and failed artifacts are retained.

The [source270 repair](handover/2026-09-30-shared-reader-draft/source270-string-storage-empty-copy-manifest.json)
uses ordinary constructors for every string copy, removes the unreachable host-text
fallback and releases the test-owned interpreters before independent image loading.
All 510 inputs replay exactly and strict checks have zero warnings. Queue 98240 has
completed. Its [closed selected audit and full-validation launches](handover/2026-09-30-shared-reader-draft/source270-string-storage-selected-manifest.json)
verify 203 focused passes/two existing ignores and 1059 affected passes in each profile,
plus 33 ordinary exact GNU comparisons, preserving every previous expectation and
all 12,600 prefix rows. Every raw verdict, inventory and artifact identity is checked.
The result parser rejects eight deliberately corrupted reports without modifying
any actual log. This is bounded evidence-parser coverage, not the final audit.

Source270 is now applied and pushed as **`0ecf6e11`**. The eight ordinary matrices
have zero wrong rows, closing 154 ordering,56 version,12 range/error,134 character
comparison,9 numeric casing,6 empty-identity,57 pure-value and 106 pure-property
whole-row differences from source261. These observations overlap and must not be
summed. Live-table and initialization fixtures also match unchanged GNU output.
All earlier failures and original source261 negatives remain retained.

Full macOS supervisor 3079 runs from the clean exact-commit
`target/runtime-goal/recovered-2026-09-30/string-storage-full/emaxx`, after selected
re-audit, idle-cache cloning and GNU capture. Linux
[Rust 36930970705](https://github.com/rayfdj/emaxx/actions/runs/36930970705) and
[frozen36930977055](https://github.com/rayfdj/emaxx/actions/runs/36930977055) are launched
on exact 0ecf6e11. Keep both source270 checkouts unchanged and read the full-run
receipts for live state. No complete platform pass, memory saving, performance
parity or full-goal completion is inferred; main remains 21d20f0e and PR79 stays draft.
