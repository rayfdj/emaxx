# Bounded callable reader — 28 September 2026

The [complete goal](runtime-representation-goal.md) remains incomplete. This
work continues from positioned-symbol commit
`a210f90cd65961bf4dc6aa2ccb87b529d46c4360`. It removes the callable reader's
whole-stream text adapter and repairs marker-relative symbol positions.
Source 78 passes 295 selected release tests and all strict static checks.
Source 80 adds GNU's omitted/nil input defaults and minibuffer dispatch and
passes 296 selected release tests and strict static checks. Its fresh ordinary
executable/image matches 15 of 16 comparisons; the original Unicode case still
fails on raw-byte printing. Source 83 repairs that printing behavior, but its
broader release run fails one dumper snapshot test (357 pass, one fail).
Source 85 repairs printer buffer change hooks and passes all 359 selected
release tests with zero ignores (258.98 seconds), plus strict static checks.
Its fresh ordinary executable/image matches all 19 exact-output comparisons. No frozen or performance
result is claimed. The [portable evidence](handover/2026-09-28-reader-streams/manifest.json)
retains every source snapshot, before-probe, failed result and final receipt.

## Reader mechanism and GNU reference

GNU `lread.c:readchar`, `unreadchar`, `read0`, `rdstack` and
`read_internal_start` in source `636f166cfc86aa90d63f592fd99f3fdd9ef95ebd`
are the references. The parser now accepts either borrowed text or a callable
character source. The latter retains at most four characters of lookahead;
it does not drain the source, copy the remaining input into a string, or
repeatedly reparse a growing prefix. Text input remains borrowed.

One exclusive host borrow owns both callback execution and symbol interning.
Every read and unread resolves a symbol callback again, preserving function
redefinition. Completed children occupy a temporary rooted vector until their
parent replaces them, so callbacks may collect while a list, vector or record
is only partly read. The reader releases that vector at the end of the read.
The separate weak-table lifetime control checks both live-result survival and
reclamation after return.

The parser returns terminators through the stream before interning a symbol,
and reads the current obarray and shorthands after that callback. Callable
symbol names follow GNU's unibyte-source behavior, retaining the low byte of
each source character. String and character literals preserve their actual
character codes. The bounded buffer keeps the original code as well as its
parser encoding, including byte8 and extended character cases. Positioned
symbols still use their one authoritative allocation. Marker sources start
their symbol offsets at zero for each read; buffer sources retain absolute
buffer positions.

Bytecode closures and other function values enter normal function dispatch.
Dot-prefixed tokens use GNU's read/unread lookahead even at the first list
position, in a vector, or at top level. Dotted tails use GNU's actual delimiter
set. An adjacent entry-point audit finds that ordinary `read` wrongly required
one argument and explicit nil did not select `standard-input`; source 80
repairs both readers' defaults and their `t`/`read-char` path through the current
`read-minibuffer` function cell.

The input branch, fallible parser operations and rooted vector have not been
profiled. Removing the full-source adapter is an architectural change, not
evidence of a speedup. Buffer and marker text extraction, other literal reader
forms and later materialization remain outside this migration.

## Preserved results and limitations

All three original source-72 stream comparisons fail. Their exact input and
raw output remain preserved, including the Unicode case whose GNU output is
not valid UTF-8. New fixtures first run against that immutable executable and
the local GNU executable; they retain exact read/unread events, source
remainders, errors, object identity, forced collections and execution modes.

Source 73 fails compilation on one missing fallible-peek propagation.
Source 74 also fails compilation because an expected string prematurely ends
a Rust raw string. Source 75 moves exact GNU output into separate expectation
files and passes 50 of 51 debug tests, including all 45 raw-reader controls.
Its one failure occurs in the existing oracle source-escaping helper, before
the runtime comparison: that helper cannot preserve the escaped Unicode
input in the fixture. Source 76 constructs the identical input from character
codes; a separate GNU comparison confirms byte-identical output. The failed
fixture and helper result remain preserved. Initial execution-mode fixture
warnings are also retained; initializing its declared variables removes the
warnings without suppressing diagnostics or changing GNU's output.

Source 76 passes 291 of 293 selected release tests. The two failures expose
bytecode callbacks incorrectly routed as strings and missing dot lookahead
at the first list element. All strict static checks pass. Source 78 repairs
those causes and adds dot and lifetime controls. All 295 selected release
tests pass, with zero ignores (116.88 seconds in libtest). Both original
suspended-thread survival/reclamation contracts pass in that release run.
Formatting, all-target/all-feature compiler checking, strict Clippy and diff
checking pass on unchanged source. Source 76 remains a failed run.

Source 80 passes all 296 selected release tests, with zero ignores (130.93
seconds in libtest), and all strict static checks. Its release test inventory
contains every one of source 72's 2,849 release tests plus the 13 new reader
controls; no existing selector is removed. The gate/debug inventory differs
because of an existing configuration-specific test.

The earlier source-72 complete macOS gate finishes failed at native artifact
identity, after 2,848 library passes, two existing terminal ignores, 60 binary
passes and 29 integration passes. Later integration/doc stages are unexecuted.
The local GNU native ABI differs only in the Darwin configuration version;
the full failed result and generated-table comparison are preserved. The source-65 complete gate remains failed: 2,833 pass, three fail,
two existing terminal ignores, with later Cargo stages unexecuted. The
[positioned-symbol checkpoint](runtime-representation-positioned-symbol-checkpoint.md)
retains the three audit repairs and their narrower validation.

Source 80's fresh ordinary executable and image build successfully. All 13
new reader fixtures and two of the three original comparisons match exact
stdout/stderr bytes. The remaining original Unicode case has the correct
symbol bytes and positions, but prints byte 177 as an octal escape where GNU
prints its raw Emacs byte8 encoding. Its failed raw comparison is preserved.

Source 83 distinguishes unibyte string bytes, multibyte raw bytes and Unicode
characters, following `print.c:print_object`, `print_string` and `print_prepare`.
Its byte-printing matrix covers string, function and buffer destinations with
both escaping flags. All reader and printing controls pass, as do both original
suspended-thread contracts. Its broader selection totals 357 passes and one
dumper snapshot failure; strict static checks pass. The dumper test snapshots
binding IDs before printing those roots, then writes an image after printing
has advanced the IDs. GNU's actual printer creates temporary escape bindings.
Source 85 captures the image before presenting the snapshot, retaining every
root comparison and assertion.

An additional before-probe exposes missing buffer change hooks in the older
printer. The new printer uses the existing insertion path, with the actual
output buffer selected for rendering and hooks. It restores escape bindings
and the original current buffer on errors. Marker printing advances the marker
and restores the buffer's adjusted point on success. Source 85 includes a
GNU control for both buffer encodings, hook errors and binding restoration.
All 359 selected release tests pass, including this control, the byte-printing
matrix, every selected reader/bytecode/native/image control and both original
suspended-thread contracts. Strict formatting, compiler, Clippy and diff checks
pass. Its complete release inventory adds 15 controls to all 2,849 previous
source-72 release tests; none is removed. The ordinary build and image creation
both pass, with software unchanged.

A fresh source-85 ordinary executable/image passes all 19 identical-input GNU
comparisons: the three original stream failures, all 15 new reader/printing
fixtures, and a separate buffer/marker destination control covering hooks,
errors, current-buffer restoration and adjusted point. All 38 processes exit
successfully with exact matching stdout and stderr, including raw non-UTF-8
bytes. Source, binary and image hashes stay unchanged throughout. The binary
SHA-256 is `04df4922ab4b7cb1283fc5f5a9e87792f3437937176037df85993251db98ef46`;
the image SHA-256 is `59d987d7a404b4f49b72f4829a2bd361ec18dfb33cd995bf78972d4824229715`.
These identify local artifacts; another host must rebuild. Source 80's failed
Unicode output and source 83's failed dumper snapshot check remain failures.

The broader printer still materializes rendered text before sending function
callbacks. This work does not establish GNU equivalence for arbitrary mutation
from such callbacks or every printer error path. Buffer/marker reader text
extraction and remaining parser forms/materialization also remain open.

The approved Linux run on older commit `803e4326` has completed failed:
998 pass and two fail, with remaining groups and later Cargo stages
unexecuted. The newer keymap code already guards its modified-character range
failure. Linux `set-text-conversion-style` still lacks implementation. Its GNU
direct buffer-slot store does not mark the variable local; an ordinary local
variable setter would not preserve that behavior. The
[Linux receipts](handover/2026-09-28-linux-char-tables/manifest.json) retain the
complete run and artifact identities. This result does not certify any later
source.

The local GNU executable is source-matched but differs from the pinned Darwin
executable. These comparisons are diagnostics, not frozen certification. Final
Linux/macOS full, frozen and terminal validation, the existing sixteen terminal
divergences, representation and ownership work, optional GC checker coverage,
real allocation counters, the final adversarial audit and measured VM/call
optimization remain open. The locked sixteen-workload suite and 3% criterion
are unchanged.
