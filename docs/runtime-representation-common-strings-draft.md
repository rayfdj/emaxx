# Common string representation draft — 2 October 2026

Read the [complete goal](runtime-representation-goal.md), [current handover](../HANDOVER.md)
and [correctness recovery](runtime-representation-correctness-recovery-draft.md).
The full goal is active and incomplete. The task runtime is now source261, pushed
as `eddfe782`, with 488 inputs and [closed selected validation](handover/2026-09-30-shared-reader-draft/source261-common-string-selected-manifest.json).
The earlier separately packaged drafts and failures remain historical evidence.
Main remains `21d20f0e`.

The [portable draft](handover/2026-09-30-shared-reader-draft/source256-common-strings-draft-manifest.json)
contains all 480 source/test input hashes, incremental and complete patches,
GNU C references, original negative fixtures, exact GNU output bytes, failed
intermediate checks and the source255 focused failure. Both patches replay every
input byte and Git checkout mode from `b8e5ad20` and main respectively.
The local candidate is `target/runtime-goal/recovered-2026-09-30/common-strings/emaxx`;
receipts and helpers are under `target/runtime-goal/resume-2026-09-28`.

## Mechanism and observed correctness

GNU's `Lisp_String` carries identity, actual bytes and intervals in one object.
Source249 still had immutable host strings and mutable canonical strings.
Storing a host-created string through a binding could copy it, while mutating
one held directly inside a vector could discard the write. Ordinary probes
expose different identity, `aset`, property, `fillarray` and `clear-string` results
from GNU. They do not establish when each defect first appeared.

Source256 routes host construction to the existing canonical byte payload.
`SharedText` is an alias to that handle. `Kind::String`, the `StringCell`/`TextRef`
arena and its GC path are removed, together with `stored_value` conversions and
the VM's owned-text adapter. `CodeBytes` contains the original one-word handle and
borrows actual bytes only while fetching an instruction. Symbol cells own their
host lookup text directly; the actual Lisp name object keeps its identity.
Dumping writes actual bytes, counts, flags and properties through one string key
and type. Both older string wire types restore the same canonical payload.

The `fillarray` repair follows `fns.c`: validate even an empty string, retain its
properties, store the low byte for an unibyte string, and reject a multibyte fill
that changes the byte length. `aset` and `clear-string` reach the existing mutable
payload. Pure copying preserves actual bytes and flags while dropping properties.
No selector, timeout, GNU expectation or comparison rule changes.

Source255 passes strict checks, then records **181 focused passes, one failure
and two existing ignores**. Both new GNU contracts pass. The failed local bytecode
fixture used Rust U+0089/U+0087 text for intended opcode bytes. An ordinary GNU
probe confirms that `make-byte-code` accepts the two unibyte bytes and rejects the
four-byte Unicode string. Source256 changes that fixture's construction, retains
all original assertions and adds rejection of the Unicode form. The failed
source255 executable, image and raw verdicts remain retained.

Source256 passes formatting, all-target checking, strict Clippy and diff checks
with zero warnings. Source249 terminal12347 has now exited with all 226 scenarios /
686 comparisons matching. **Queue17749** has exited. Its
[focused gate result](handover/2026-09-30-shared-reader-draft/source256-focused-gate-manifest.json)
is audited: **182 passes / two existing ignores**, with all 184 selectors
and both retained executable/image inputs verified. The
[broader gate audit](handover/2026-09-30-shared-reader-draft/source256-broad-failure-and259-focused-manifest.json)
verifies **956 passes / one failure**, all 957 names and the same retained inputs.
The failed native-call fixture used five bytes of Unicode text for intended opcode
bytes 137/84/135. Ordinary GNU accepts the unibyte form, retains its identity and
returns 42; it rejects the Unicode form. Release and seventeen ordinary comparisons
did not run. Every prior selector remains included. The failure is preserved; its
correction must retain the native dispatch/result/backtrace/depth assertions.

## Subsequent comparison and hash repair

The [source259 portable draft](handover/2026-09-30-shared-reader-draft/source259-string-comparison-draft-manifest.json)
replays all **488 inputs and Git checkout modes** from `3d7dd8f9` and main. Its
checkout is `target/runtime-goal/recovered-2026-09-30/string-comparisons/emaxx`.
It builds on source256 without changing that queued checkout.

GNU `fns.c:Fstring_equal` and `internal_equal` compare character counts, byte counts
and actual storage bytes. Source259 shares that direct contents path across the
Rust handles/values and Lisp equality primitives. `string-equal` reads a symbol's
actual Lisp name object and follows `lisp.h:SYMBOLP` for positioned symbols. The
including-properties path borrows the actual spans after checking identity and
contents; it no longer clones decoded text and its properties for comparison.

GNU `intervals.c:intervals_equal_1(..., true)` uses ordinary `Fequal` on property
values. GNU `fns.c:hash_interval` likewise uses ordinary `sxhash_obj`. The repair
uses that same relation for comparison and hashing, so nested string properties
do not change an equivalent key. String hashing reads actual bytes instead of
allocating a Rust text projection and extended-character list. The existing hash
mixing algorithm remains; GNU hash algorithm or cost parity is not established.
The dumper stores key/value pairs and reconstructs hashes after relocation. Its
existing test gains unibyte, byte8 and private-use keys while retaining all assertions.

Four exact ordinary GNU fixtures are preserved. Source249 has six wrong booleans
in the 64-pair storage matrix, 92 wrong results in the 512-row symbol/name matrix,
eight wrong property-matrix results plus three mutation observations, and thirteen
wrong hash/table observations. The symbol fixture's initial nonexistent-constructor
error is retained separately; its corrected fixture uses GNU `position-symbol`.
The property tests cover nested values, mutation, GC, interval segmentation and
custom table lookup/replacement/removal. No historical origin is inferred for the
newly probed symbol/property/hash gaps.

Source257 failed checking on eight missing-import diagnostics in the new Rust test;
source258 fixed only those imports and passed strict checks. C review then exposed
the related hash requirement before runtime execution. Its waiting queue21849 was
explicitly withdrawn, preserving the source and receipts. This is not a claimed
runtime failure or pass. Source259 includes the hash repair and passes formatting,
all-target checking, strict Clippy and diff checks with zero warnings. Both packaging
helper setup failures remain recorded too.

**Queue22381** has exited. Its [focused gate audit](handover/2026-09-30-shared-reader-draft/source256-broad-failure-and259-focused-manifest.json)
verifies **188 passes / two existing ignores**, all **190 selectors**, both retained
inputs, all four GNU fixtures and the extended dump test. The broader gate records
**961 passes / one failure**, with all 962 names audited. It fails on the same invalid
native-call fixture as source256. Release and ordinary stages did not run. Every
preceding 184/957 common-string control and both failed executable/image pairs remain.

## Corrected native fixture: source261

After source259 exited, source260 changed only the existing native-call test to
construct actual unibyte opcode bytes 137/84/135. It retains target classification,
the native result 42, empty backtrace and evaluation-depth assertions, and adds
rejection of the former Unicode form. Ordinary GNU `make-byte-code` confirms both
cases. Source260's new `.unwrap_err()` violates the existing lint; source261 changes
only that call to explanatory `expect_err`, with no suppression. Production code
is byte-identical to source259. Both intermediate patches and failures are retained.

Source261 has **488 exactly replayed inputs** and zero-warning strict checks. Both
focused profiles pass **189 tests / two existing ignores**, preserving all earlier
selectors and adding the corrected native fixture to the focused list. Its broader
gate passes **all 962 tests**. The first release evidence auditor incorrectly expected
only two retained inputs; the retention helper had also copied the preceding gate
image. A separate corrected auditor verifies the release executable/image and the
older gate image against their recorded hashes. The initial audit failure is retained;
no test rerun or changed expectation is used to repair that bookkeeping.

Continuation **30525** has exited. Its [closed selected audit](handover/2026-09-30-shared-reader-draft/source261-common-string-selected-manifest.json)
verifies **189 focused passes / two existing ignores and 962 affected passes in each
profile**, plus **22 ordinary exact GNU matches**: the 21 fixtures and unchanged native
constructor probe. Gate/release inventory differences are only the existing debug-only
file-descriptor control. All source249 selectors and every original GNU expectation
remain. The archive contains 368 evidence/helper files, both exact portable patches,
and the source259, source260 and initial source261 audit failures.

Source261 is now applied and pushed as `eddfe782`. The
[complete macOS Rust audit](handover/2026-09-30-shared-reader-draft/source261-complete-macos-rust-manifest.json)
verifies **3,107 passes / two existing ignores**: 3,008 library, 60 binary and 39
integration passes. All 3,010 raw library names, native artifact identity and four
retained executable/image files are checked. The original and retained GNU
executable/dump/configuration/Makefile also match. An initial bounded-input audit
used the wrong metadata key and wrote no passing receipt; its correction is recorded.
Supervisor **31023** has completed the unchanged full terminal comparison after frozen.
The clean full-validation checkout is
`target/runtime-goal/recovered-2026-09-30/common-strings-full/emaxx`, commit `eddfe782`,
with the same 488 inputs. Exact-head Linux
[Rust36904097575](https://github.com/rayfdj/emaxx/actions/runs/36904097575) and
[frozen36904105841](https://github.com/rayfdj/emaxx/actions/runs/36904105841) are complete.
Their [closed Linux evidence](handover/2026-09-30-shared-reader-draft/source261-complete-linux-manifest.json)
verifies **3,119 Rust passes / two existing ignores**, all 3,018 raw library names,
native artifact identity and four retained Rust inputs. Full frozen verifies
**519 files / 7,928 matching outcomes / 1,038 successful processes**; each editor
has 7,670 passes, 47 expected failures and 211 skips, including all 177 original
compiler tests passing. Every execution hash, paired outcome and actual GNU
executable/dump/configuration/Makefile is checked. Historical failures remain.
The [closed macOS frozen audit](handover/2026-09-30-shared-reader-draft/source261-complete-macos-frozen-manifest.json)
verifies all 519 files and 1,038 successful processes, with **7,915 matching / six
mismatching outcomes**. Both editors have 7,623 passes, 46 expected failures and
252 skips; all 177 original compiler tests pass. The six existing seccomp skip
diagnostics contain different `system-configuration-features` text, as in source249.
They remain strict failures. Every raw inventory, outcome, execution hash and actual
GNU/Emaxx input is checked. No capability string or comparison rule changed.
The retained gate executable is `aa99dd97…`, its image `ca97eca7…`; terminal validation
uses that exact pair. The [complete terminal audit](handover/2026-09-30-shared-reader-draft/source261-complete-macos-terminal-manifest.json)
verifies **226 scenarios / 686 matches**, including 658 screen and 28 filesystem
comparisons. All original labels, 488 source inputs and ten execution inputs match;
final verification of the four retained GNU files also passes. Keep both source261
checkouts unchanged. No measured saving or full-goal completion is implied.

## Subsequent ordering repair: source262

The [portable source262 draft and ordinary negatives](handover/2026-09-30-shared-reader-draft/source262-string-ordering-draft-manifest.json)
contain two unchanged GNU fixtures. The first has 1,024 rows across `string-lessp`,
`string-version-lessp`, actual symbol names, positioned-symbol policy, unibyte,
byte8, private-use text and invalid arguments. Source261 has **154 wrong results**.
Main/source174 has the same count but four different wrong rows. Retained source207
matches all main rows; source249 matches all source261 rows. Those four regressions
therefore appeared after source207 and by source249; the introducing commit is not
established. Four other rows improved in that interval. This does not establish
general regression absence. The second fixture has 400 version pairs including
dot names, suffixes, tilde, leading zeros, 96-digit numbers and embedded NUL.
Source261 and main give identical output, with **56 wrong results**. All negative
processes, exact executable/image identities and historical outputs remain retained.

Source262 uses canonical string bytes and actual Lisp symbol names for ordering.
It follows `fns.c:string_cmp`: direct bytes for unibyte/ASCII strings, a shared-byte
prefix followed by full character decoding for multibyte strings, and decoded
characters versus actual octets for mixed storage. Version ordering follows the
actual `lib/filevercmp.c:filenvercmp`, including dot, suffix, tilde and arbitrary
length digit handling. It adds no lexical tie-break absent from that C code. The
unused host-text comparison helper is removed. Two tests use the unchanged GNU
fixtures; the original compatibility version test remains byte-identical.

The isolated checkout is `target/runtime-goal/recovered-2026-09-30/string-ordering/emaxx`.
Its **492 inputs and modes** replay exactly from `7c6bb8ce` and main. Formatting,
all-target checking, strict Clippy and diff checks pass with zero warnings. Queue
**38430 was withdrawn while still waiting, before its first runtime stage**, in
favor of source263 below. The original source, helpers, launch and queued state
remain unchanged. This is neither a runtime pass nor a runtime failure. The
successor includes all 194 planned focused selectors, all 1,050 affected tests and
all 24 ordinary comparisons. The documented negatives remain open until an actual
successful comparison closes them.

## Range and numeric casing repair: source263

The [portable source263 successor](handover/2026-09-30-shared-reader-draft/source263-string-ranges-draft-manifest.json)
preserves four further ordinary GNU fixtures. Source261 has **12 differences among
22 range/error cases**, **134 among 1,024 character-comparison rows**, **seven wrong
comparisons across three live-table phases**, and **nine wrong numeric casing rows
among 30**. Main/source174 exactly reproduces the range, live-table and numeric
outputs. Its character matrix exits255 at `(string #x110000)` before producing any
comparison rows; that failure remains retained and supplies no historical matrix
attribution. GNU, source261 and main table contents agree in the numeric fixture;
the wrong character aliases are in lookup. All negative processes and hashes remain.

Following `fns.c:Fcompare_strings` and `validate_subarray`, the repair checks both
string types before either range, clamps only too-large positive fixnum ends and
preserves the original bounds in range errors. It uses sequential byte cursors on
canonical storage, removing decoded string, property and character-vector copies
from this path. `character.h:fetch_string_char_as_multibyte_advance` promotes
unibyte octets before casing. Only differing characters invoke numeric `upcase`,
with string borrows released first. That numeric path now follows actual
`buffer.h:upcase/downcase` table keys, defaults and parents, keeping the existing
modifier/C-integer rules. No raw-byte-to-Latin-1 alternate lookup is performed.
Legacy string-casing text consumers remain outside this bounded repair.

The separate checkout is `target/runtime-goal/recovered-2026-09-30/string-ranges/emaxx`.
All **500 inputs and modes** replay exactly from `dbf1fbb2` and main. Formatting,
all-target/all-feature checking, strict Clippy and diff checks pass with zero
warnings. The four tests use unchanged ordinary GNU output; all source262 code
and tests remain. Queue **90568** has exited with **197 focused passes / two existing
ignores**, then **1,053 affected passes / one failure**. All raw selected names and
retained executable/image hashes are audited. Release and ordinary stages did not
run. The candidate remains unapplied and unchanged. Its original Unicode casing
test used a bare interpreter while expecting dumped GNU case tables. GNU
`casetab.c:init_casetab_once` installs ASCII; loadup's `international/characters.el`
supplies Unicode. The successor preserves the original assertions in an initialized
interpreter and adds the actual bare-state result plus an ordinary live-table fixture.

An [additional ordinary prefix-boundary control](handover/2026-09-30-shared-reader-draft/source263-ordering-boundary-control-manifest.json)
matches GNU/source261 byte for byte on **12,600 comparisons**: 14 prefix lengths
around word boundaries and 30 character/storage cases, including non-Unicode,
surrogate, byte8, private-use and mixed storage. This specifically challenges the
new shared-byte-prefix loop rather than certifying it from single-character cases.
Queue **90569** stopped before any stage because90568 failed. It did not run the
selected auditor or prefix fixture. Neither the passing source261 baseline nor the
prepared follow-up is a source263 runtime or performance pass.

The [queue correction](handover/2026-09-30-shared-reader-draft/source263-fixture-queue-repair-manifest.json)
retains original supervisors64601/87187, withdrawn while waiting before any runtime
stage. Their wrapper misspelled the original ordering fixture as a nonexistent
`string-ranges-symbol-storage`. The replacement restores `string-ordering-symbol-storage`
and verifies every one of the 28 paths. No runtime, test, expectation, selector,
timeout or comparison rule changes. Read `source263-fixture-validation-*` and
`source263-boundary-fixtures-*`; the old helpers and queued state remain preserved.

## Compact header and pure storage continuation: source270

The [source264–268 preparation archive](handover/2026-09-30-shared-reader-draft/source268-string-storage-draft-manifest.json)
includes the closed source263 failure, ordinary GNU negatives, failed startup
diagnostics, compiler/lint failures and exact portable patches. The
[source269 correction archive](handover/2026-09-30-shared-reader-draft/source269-string-storage-caller-repair-manifest.json)
adds the closed source268 focused failure. The
[source270 correction archive](handover/2026-09-30-shared-reader-draft/source270-string-storage-empty-copy-manifest.json)
retains source269's two failed controls and the current **510-input** successor.
Its checkout is `target/runtime-goal/recovered-2026-09-30/string-storage/emaxx`, based
on `e6120bce`; both incremental and complete-main patches reproduce every byte and mode.

The shared string payload is now exactly GNU's four-field, 32-byte layout:
character count, byte count (`-1` for unibyte), property pointer and data pointer.
Native object words point directly at that header. Another 16 bytes per cell hold
checked-borrow and allocator metadata. Dedicated string blocks replace the vector
wrapper; native decoding, conservative roots and GC dispatch recognize string cells.
NUL-terminated bytes remain separately boxed, with a shared zero byte for empty data.
Property spans occupy a single packed allocation behind the header pointer; they
are still spans, not GNU interval trees. No total memory saving has been measured.

Normal empty unibyte/multibyte constructors return their two permanent singletons.
Pure allocation is distinct even at zero length; already-pure copies preserve
identity and survive collection outside the ordinary census. Mutation checks follow
GNU's pure-write and no-op/error ordering. Property mutation reads before borrowing
mutably, avoiding an unnecessary write for a no-op. Dump restoration preserves
distinct empty headers and makes restored pure strings ordinary, while explicit
empty-root metadata preserves the two singleton identities. Template cloning keeps
the same distinction. This wire metadata substitutes for GNU's static-root relocation;
the runtime still has one string representation.

New controls read native header fields/data/NUL directly and check pure GC/census,
dump restoration, template identity and ordinary empty/pure behavior. The additional
144-row property fixture finds **106 wrong whole rows / 84 operation or poststate
differences** on source261; identity observations overlap the previous fixtures and
must not be added. An ordinary initialization fixture gives GNU `((t t) (2 1))`
versus source261 `((t t) (t t))` for default and empty case tables. Three attempted
bare GNU startups failed before their fixture and remain excluded. The original
source263 broad-log auditor rejected a prompt sharing a line with its verdict;
a corrected parser verifies complete named blocks without changing test outcomes.

Source264 failed on five stale RefCell callers; source265 on one unchanged reader
assertion's container type. Source266 compiled but Clippy rejected `Box<Vec<Span>>`;
source267 replaces it with the single packed allocation and passes all strict checks.
Source267 did not run. Source268 also fixes image-template handling, passes strict
checks, and has **199 focused passes / four failures / two existing ignores**.
The native header, pure GC and dump/template controls passed. Each of the four
failures is at the GNU precondition: expected output has one extra trailing newline.
No Emaxx pass for those four fixture assertions can be inferred.

Source269 changes only those four expected-output callers to the existing
`trim_end` convention. All GNU fixture bytes and the shared comparator are unchanged;
strict checks have zero warnings. Its focused run has **201 passes / two failures /
two existing ignores**. Empty identity and all 144 property rows pass; the 96-row
pure fixture matches 94 rows. Its two wrong rows copy an empty pure string before
mutation: the old zero-length early return preserves purity instead of invoking
GNU's ordinary constructor. Source270 removes that return and the unreachable
host-text fallback. Its other failure occurs when the test loads a second native
image while still owning its first initialized interpreter. Source270 drops the
test-owned interpreters before the independent assertion, preserving every original
comparison. Source269's raw failure and artifacts remain; no later stage ran.

Source270 passes strict checks with zero warnings. Queue **98240** runs **205 focused selectors**,
**1,059 affected tests per profile** and **33 ordinary comparisons**, preserving
all original selectors, expected values and the full 12,600-row prefix fixture.
Read `source270-validation-*`; keep the candidate and executing helpers unchanged.
The published source261 and failed source263 remain unchanged. Preparation archives
do not establish a runtime, full-platform or performance pass.

## Remaining requirements

The [current empty/pure storage baseline](handover/2026-09-30-shared-reader-draft/source261-string-storage-baseline-manifest.json)
compares GNU and retained source174/main,207,249,261 executables. Source261 has six
wrong rows among 12 empty-identity cases; all three earlier Rust checkpoints have
twelve. It has 57 wrong rows among 96 pure-storage cases, including 28 operation and
poststate differences. Source249 has 56 wrong whole rows: seven previously matching
rows regress and six improve by261. All seven new whole-row differences concern
the first pure copy of an ordinary empty unibyte string. Pure write protection and
already-pure copy identity also fail in earlier checkpoints. The exact introducing
source between249 and261 is not established; main and207 outputs agree exactly.

GNU `make_clear_string`/`make_clear_multibyte_string` use two shared empty strings.
`make_pure_string` allocates a distinct header even for zero bytes, while `purecopy`
returns an already-pure object unchanged. The source261 pure-copy path incorrectly
uses the normal constructor's empty unibyte singleton. `STRING_SET_UNIBYTE` replaces
a zero-length local string value rather than changing the shared header; consequently
ordinary `clear-string` leaves an empty multibyte object's flag unchanged. These
actual C paths constrain the next storage change. Pure-write error ordering, both
empty identities, GC survival/accounting and image restoration must remain explicit.

The archive retains all 15 valid ordinary processes, including both raw output and
an additional fixture that describes returned/error strings by flags, lengths and
full character codes. The latter avoids lossy decoding of GNU's raw non-UTF8 error
data; the original bytes remain. Both pure fixtures contain the same 96 cases, so
their counts overlap. The first fixture's three syntax failures and the repaired
fixture's three scope-invalid runs are retained and excluded. No storage repair is
implemented by this baseline, and the frozen source263 candidate is unchanged.

Applied source261 unifies authority but does not supply GNU's 32-byte string header
or sblock allocation. Source270 implements the header and removes the vector wrapper,
but remains an unapplied draft under validation. Sblocks, real interval trees and
remaining borrow/allocation overhead are unfinished. Moving host keys grows `SymbolCell`; removing a separate key cell
does not establish a net memory reduction. Symbol value/function/plist authority
still resides in per-interpreter tables and registries.

The old host raw-byte/private-use text convention and many transient decoded
Rust views remain. The empty multibyte singleton and pure-string write protection
are implemented only in the unvalidated storage draft above. Public
runtime ownership, actual cumulative allocation counters, physical accounting,
VM stack layout, final platform validation and adversarial review remain open.
No speedup, memory saving or GNU performance parity has been measured. All sixteen
locked workloads and the 3% ceiling still apply.

The [subsequent comparison probe](handover/2026-09-30-shared-reader-draft/source249-string-comparison-gaps-manifest.json)
confirms six wrong results among a fixed matrix of 64 string pairs: four
`equal-including-properties` results conflate unibyte bytes with multibyte byte8,
and two `string-equal` results conflate byte8 with actual private-use Unicode.
Ordinary `equal` is correct throughout this matrix. The same fixture on recorded
source174/main and source207 executable/image pairs has twelve wrong boolean
results; source249 fixes six and introduces none in this matrix. This does not
excuse other recorded regressions or establish general correctness.

Those projection repairs are absent from frozen source256 and implemented separately
in source259/261 above. Keep the original negatives and both queued input sets unchanged
while validating. Published source261 ordering still uses host text; the separate
source262/263 drafts above are not yet validated or applied. Neither repair completes
the architecture.
