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

The task checkpoint combines source237 with the reader/mapconcat repairs. The
same 446 inputs are frozen in
`target/runtime-goal/recovered-2026-09-30/rx-repair/emaxx`. Its
[focused audit and portable patch](handover/2026-09-30-shared-reader-draft/source238-focused-validation-manifest.json)
verify zero-warning strict checks and **101 focused gate passes**, with two
existing end-to-end ignores. This covers the 37 source234 controls, all TTY
unit tests and the two new GNU fixtures. Exact test executable and post-run
image are retained.

Supervisor **30236** runs the complete macOS gate. Supervisor **27314** has
finished focused validation and is building the ordinary executable/image;
then both unchanged upstream rx runners execute against each editor. Helpers
and receipts are under `target/runtime-goal/resume-2026-09-28`. Do not modify
these candidates or their executing helpers/artifacts. Read live receipts before
deciding whether a stage remains active; do not restart it.

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

Only the focused source238 runtime pass is established here. The report-writing error must remain
visible unless independently resolved; a repaired rx suite alone does not prove
arbitrary failure reports are correct. After selected validation, run the complete
terminal and required platform/frozen gates on the final combined source, preserving
all earlier failures. Continue to the full architecture/accounting/ownership and
locked performance requirements only after recovering correctness.
