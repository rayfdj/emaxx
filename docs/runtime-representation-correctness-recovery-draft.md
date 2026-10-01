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
executable bits. Root task-branch runtime remains source238.

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
Release/ordinary supervisor **43217** has exited. Full macOS **43216** remains
active; read its receipt before acting and do not modify the executing candidate.

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
