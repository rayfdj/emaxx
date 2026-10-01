# Canonical string bytes draft — 1 October 2026

Read the [complete goal](runtime-representation-goal.md),
[current handover](../HANDOVER.md) and
[shared-closure continuation](runtime-representation-bytecode-draft.md).
The full goal remains unfinished. **Source234** is the task checkpoint and matches
all 442 inputs in the isolated `display-tables/emaxx` checkout. PR #79 remains
draft and main remains source174. The checkout name does not indicate a renderer
repair. Source232 remains frozen in `string-ops/emaxx` during its complete macOS
rerun; source229 remains frozen in `string-bytes/emaxx`.

The [source231 Linux failure audit](handover/2026-09-30-shared-reader-draft/source231-linux-interactive-failure-manifest.json)
records 1,380 Rust passes before an Edebug abort and 79 frozen files / 1,911 matching
outcomes before the same assertion. There are 1,608 unstarted Rust tests and 439
unstarted frozen files. No later Cargo stage runs. GNU completes the edebug file;
Emaxx produces no final report for its 46 outcomes. The five expected failures and
five skips among preceding outcomes are not passes.

The [source234 draft](handover/2026-09-30-shared-reader-draft/source234-interactive-metadata-draft-manifest.json)
follows `callint.c:Fcall_interactively`: only a cons code slot identifies an
interpreted closure whose constants slot is an environment. Bytecode and other
commands pass nil to existing `Feval`; caller lexical bindings cannot leak in.
The original same-input source231 probe returns a vector/listp error where GNU
returns 17. The [focused audit](handover/2026-09-30-shared-reader-draft/source234-focused-validation-manifest.json)
passes all 37 gate controls, including the new GNU fixture, the four original
Edebug tests and captured-closure mutation. Strict checks pass with zero warnings.
Release supervisor 8207 and full macOS supervisor 12104 are active; Linux repair
is not yet verified. All 442 inputs and modes replay exactly.

The [source232 control repair](handover/2026-09-30-shared-reader-draft/source232-library-control-repair-manifest.json)
clears the five old local failures in the complete lightweight group: 545 gate
and 544 release passes, with all original behavioral assertions retained. Its
[first full run](handover/2026-09-30-shared-reader-draft/source232-fixture-relocation-failure-manifest.json)
fails because the relocated selected-test binary and normal gate reused an image
with incompatible relative native-library paths. That image and 232-pass/154-fail
run remain retained. Supervisor 5593 runs the unchanged full gate with a fresh
image from the normal executable. Source233 passes compiler/Clippy but fails one
new test's line wrapping; it runs no runtime tests. Source234 fixes that formatting.

The string-byte work starts from
source216, whose ten focused controls have nine passes/one failure and whose
unchanged 848-test broader inventory has 847 passes/one failure. The remaining
failure is active-call code mutation: `(41 194)` instead of `(67 194)`.
The old closure checkout and its executable/image remain intact.

## Constructor repair and direct VM candidate

The [source221 constructor repair](handover/2026-09-30-shared-reader-draft/source221-string-constructor-repair-manifest.json)
builds `string`, `make-string` and `char-to-string` directly from GNU character
codes, without a Rust-char conversion. It encodes repeated characters once and
preserves the multibyte flag at zero length. Length, character and arity checks
follow `alloc.c:Fmake_string`, `character.c:Fstring` and
`editfns.c:Fchar_to_string`. A new ordinary GNU fixture covers eighteen character
boundaries at lengths 0/1/3/17, actual bytes, flags, alias mutation and error order.

All **423 inputs and modes** replay exactly from main. Strict checks pass with
zero warnings. The original surrogate control now passes. The focused inventory
yields **14 passes / one failure**, and the full affected-module inventory yields
**927 passes / one failure / zero ignores** across 928 tests. Only active-call
bytecode mutation fails. Supervisor **70849 has exited**; the executable and
post-run image are retained. These are gate-profile results, not full validation.

The [source223 direct-bytecode draft](handover/2026-09-30-shared-reader-draft/source223-direct-bytecode-draft-manifest.json)
fetches current canonical string bytes with a byte-offset cursor. It removes
the decoded-code registry, retained instruction/offset tables and per-call `Rc`
activation. Suspended frames retain handles and byte offsets. Byte borrows end
before Lisp callbacks, collection, stores or frame changes; checked indexing
protects Rust access. The complete decoder remains in its diagnostic tests,
including the original corrupt-stream assertions, rather than running at closure
entry. Raw `byte-code` now follows GNU's conversion and closure-rooting path.
The obsolete plain-string cache serial field and allocation increments are gone.

The same-input GNU program verifies changed opcode widths, constant indices
2/17/257, branch operands at code lengths 17/257/513, forced GC, nested callers,
dead bytes after return and raw entry. Permanent controls retain that program
and require canonical execution to borrow the actual payload address. The
[focused audit](handover/2026-09-30-shared-reader-draft/source223-direct-bytecode-focused-manifest.json)
verifies **17 gate passes**, including both originally failing assertions.
The [completed gate/release audit](handover/2026-09-30-shared-reader-draft/source223-direct-bytecode-results-manifest.json)
verifies seventeen focused passes and **929 passes / one failure / zero ignores**
in each profile's unchanged 930-test broader inventory. Supervisors **73057 and
73532 have exited**. Both executables and post-run images are retained.

The sole failure is an old raw `byte-code` fixture whose Rust Unicode characters
encode as C3 80/C3 81, beginning with constant index 3 in a two-element vector.
GNU's ordinary `string-as-unibyte` confirms those bytes; the intended unibyte and
byte8 programs both produce the original expected 42. The
[source224 fixture correction](handover/2026-09-30-shared-reader-draft/source224-raw-bytecode-fixture-repair-manifest.json)
changes only that test's constructor inputs and explanatory comment. Both original
assertions and all production code are preserved byte for byte. Strict checks
and fresh **18/18 controls in both gate and release** pass; supervisor **77687
has exited**. Source223's broader failures remain failures, not a source224 pass.

All **425 inputs and modes** replay exactly from main, with zero-warning strict
checks. Source222's five obsolete-helper warnings and strict failure remain
preserved; it ran no runtime tests. The portable source223 draft assigns no
runtime or performance verdict. Those source223 results preceded the later uncommitted local source229 draft.

One temporary adapter remains: non-ASCII immutable `Kind::String` values supplied
through the old Rust API are converted once at entry to an owned byte slice.
It has no identity registry or mutation cache. Ordinary reader, constructor and
restored shared-string code uses its actual payload. Remove this adapter with
the plain-string representation before architectural completion.

## Current control-flow repair

Review found source223/224's instruction fetch still validated branch and handler
destinations before control actually used them. An ordinary GNU probe accepts all
four untaken conditional-branch forms and two unentered handler forms with unused
out-of-range destinations, through both closure and raw `byte-code` entry.

The [source225 draft](handover/2026-09-30-shared-reader-draft/source225-control-flow-draft-manifest.json)
checks the actual PC before reading code instead. The complete diagnostic decoder
retains all original invalid-target assertions. A separate Rust control requires
an actually taken out-of-range branch to signal without leaking stack or function
roots. Fast dispatch passes its already fetched instruction into slow dispatch,
removing the redundant second decode without holding a byte borrow across Lisp.
All **427 inputs and modes** replay exactly, and strict checks pass with zero
warnings. Its [audited selected results](handover/2026-09-30-shared-reader-draft/source225-bytecode-string-selected-validation-manifest.json)
pass **20 focused controls and all 932 affected-module tests in both profiles**,
zero ignores. All raw names/verdicts, binary/image hashes and source inputs are
verified; five deliberately corrupt logs are rejected. The original 48-fixture
ordinary run retains 44 passes/four redundant-print failures. Four separate
replays of those unchanged self-printing fixtures pass without an outer `prin1`,
giving 48 distinct passing ordinary comparisons. These are selected results;
the complete runs fail below. All selected supervisors have exited.

## Full-run failure and current completion repair

The [source225 complete failures](handover/2026-09-30-shared-reader-draft/source225-complete-validation-failure-manifest.json)
retain 161 Rust passes before the original Tramp test aborts in native completion.
The first group's remaining 224 tests, every later group and both Cargo stages
never run. The terminal comparison matches seven scenarios before Org startup
aborts; another 218 scenarios never start. Supervisors 82034/82035 have exited.
The failed executable and full-gate image are retained. The first audit helper
also fails on a truncated terminal-message expectation; its corrected successor
verifies the actual raw evidence without changing runtime or expectations.

`try-completion` constructs every result as unibyte, so the canonical byte
encoder rejects U+5BC6 in the full Rust trace. An ordinary same-input GNU/source225
probe separately reproduces an abort at U+03BB. GNU completes the entire probe,
including copied properties, character boundaries, original-input identity,
case-selected candidates, raw bytes and native `regexp-opt`. The terminal screen
captures a native `regexp-opt-group` non-unwinding panic but not its first message;
the initial diagnosis alone does not prove every terminal failure cause.

The [source228 draft](handover/2026-09-30-shared-reader-draft/source228-completion-storage-draft-manifest.json)
follows `minibuf.c:Ftry_completion`'s best-candidate selection and substring result.
It preserves that candidate's encoding, copied properties and extended characters,
returns the actual input for the unchanged case-folded result, and removes the
obsolete materialized common-prefix builder. The new permanent fixture uses the
unchanged ordinary GNU program and expected bytes. Sources226/227 are unexecuted
drafts: the first retains the obsolete helper, and the next cleanup accidentally
removes the neighboring filter helper too. Source228 restores that unchanged
neighbor. All three complete patches and modes replay exactly.

Its [closed audit](handover/2026-09-30-shared-reader-draft/source228-completion-results-manifest.json)
verifies **24/25 focused passes and 932/933 broader passes**, with zero ignores.
The original Tramp test and all twenty preceding bytecode/string controls pass.
The unchanged new completion assertion fails because `concat` first replaces
surrogate and beyond-Unicode characters with U+F8FF. Supervisor **82531 has
exited**; all raw verdicts and the exact failed executable/image are retained.

The [source229 draft](handover/2026-09-30-shared-reader-draft/source229-canonical-concat-draft-manifest.json)
follows `fns.c:concat_to_string`: it counts and copies canonical encoded bytes
directly, promotes unibyte octets only when the result requires multibyte storage,
and accepts GNU's full character range in lists and vectors. Proper-list spine
validation precedes character validation, with unchanged circular/improper-list
errors. Property offsets and copied plists follow the existing GNU-backed rules;
source strings retain their contents and identity across copying and collection.
The old Rust-text concat builders are removed. Unicode buffer completion now
also infers its encoding instead of forcing unibyte storage.

All **432 source229 inputs and modes** replay exactly. They matched the task
worktree before the later source231 update below.
Its [closed evidence](handover/2026-09-30-shared-reader-draft/source229-selected-and-rust-failure-manifest.json)
verifies strict checks, **26 focused and 934 affected-module passes per profile**,
fifty ordinary GNU comparisons and exact inputs/artifacts. Five corrupt-log
variants are rejected. Full Rust passes all 386 eval_01 tests, then records
294 passes/one failure in eval_02. The old quoted-bytecode assertion expects the
removed host record. The unchanged form returns identical GNU/source229 bytecode
predicate, non-record predicate, four-slot length, actual opcodes and execution.
The remaining 2,297 library tests and both Cargo stages never run. These full
results remain failed. Supervisors 84949/86489/86490/87395 have exited.

Full terminal supervisor **89147 has exited**. Its
[audited partial failure](handover/2026-09-30-shared-reader-draft/source229-terminal-failure-manifest.json)
contains 164 fully matching scenarios, 353 matching comparisons and seven
quote-display divergences. Scenario 172 stops in GNU startup readiness; 54 later
scenarios never start and 326 scheduled comparisons remain unexecuted. This is
not a complete pass or evidence of an Emaxx startup crash. A separate interactive
probe observes identical quote policy,
substituted character codes and display-table vectors in both editors, pointing
to display handling. Source229 and its exact artifacts remain preserved.
There is no complete, Linux, pinned Darwin or performance pass for this candidate.

Three additional same-input substring probes all fail on source229. Surrogates
become U+F8FF; bool-vector and no-properties vector checks differ from GNU;
index validation has wrong error order/data; and reversed bounds abort with a
Rust slice panic. Raw outputs, original fixtures and artifact hashes are retained.

The [source230 repair](handover/2026-09-30-shared-reader-draft/source230-substring-draft-manifest.json)
follows `fns.c:validate_subarray`, `Fsubstring` and `Fsubstring_no_properties`.
It validates both index types before testing the complete interval, signals the
original array/from/to values, and copies canonical byte ranges without a Rust
text/character vector. Vector slices copy actual Lisp fields. Completion uses
the same canonical slice, removing its result-side text conversion. The original
quoted-bytecode test retains its selector and macro assertion, now requires the
shared closure kind, and adds the ordinary GNU field/execution fixture.
All **440 inputs and modes** replay exactly. Compiler, fmt, strict Clippy and diff
checks pass. The [focused failure and setup proposal](handover/2026-09-30-shared-reader-draft/source230-focused-failure-and-proposal-manifest.json)
retain **30 passes / one failure / zero ignores** across 31 controls. All three
substring controls pass, as do the original quoted-reader macro and shared-closure
kind assertions. The added fixture then fails because its bare interpreter lacks
GNU's Lisp `cadr`; normal GNU/source229 already pass those unchanged inputs.
Proposed source231 uses the normal GNU batch preload for that added contract,
leaving the original bare-reader assertions and all fixture/expected bytes intact.
Source230's [closed results](handover/2026-09-30-shared-reader-draft/source230-closed-validation-manifest.json)
verify **937 affected-module passes and 54 ordinary GNU comparisons**, with all
raw names/verdicts, artifact and source hashes checked. Supervisors **90596/93555
have exited**. Its 30-pass/one-failure focused run stays failed.

The [source231 setup repair](handover/2026-09-30-shared-reader-draft/source231-bytecode-fixture-startup-draft-manifest.json)
is now applied in the string-ops checkout and source231 task checkpoint;
frozen string-bytes/source229 is unchanged. All **440 inputs and modes** replay
exactly. The [focused audit and complete launches](handover/2026-09-30-shared-reader-draft/source231-focused-and-complete-launch-manifest.json)
verify strict zero-warning checks and **31/31 focused gate passes**. Every
production input remains source230's. Supervisors **96184/96185 have exited**.
Their [closed release and ordinary audit](handover/2026-09-30-shared-reader-draft/source231-release-and-ordinary-validation-manifest.json)
verifies **31 focused / 937 affected release passes and 54 exact ordinary GNU
comparisons**, zero ignores, all raw verdicts, 440 source hashes and retained
artifact identities. Five corrupt-log variants are rejected. Supervisor **96183
has exited**. Its [full-Rust failure audit](handover/2026-09-30-shared-reader-draft/source231-full-rust-failure-manifest.json)
checks all 2,981 library names: **2,974 passes / five failures / two existing
ignores**. Three failures concern stale bytecode-root inventory metadata; the
reader's no-slot-evaluation test still expects a host record; and the vector
bitmap control covers only three of four words. Both later Cargo stages never
run. The failed executable/image and all original assertions remain preserved.
The later Linux run fails as recorded above; complete terminal validation remains open. The full gate supplies the
complete gate-profile inventory after this test-only change; the passing
source230 selected gate is not redundantly repeated. No complete pass is claimed.

The source229 terminal diagnosis reads the same curve-quote policy, U+2019 text
and display vectors `[92274784]`/`[92274727]` in both ordinary interactive editors.
GNU `xdisp.c:get_next_display_element` applies its display table before glyphless
fallback. Current `GlyphlessDisplayContext` reads glyphless rules but no standard,
buffer or window character display table. A general table translation repair,
including glyph face IDs and layout mappings, remains to be implemented and
checked; no quote-specific workaround or terminal pass is claimed.
Empty multibyte-string identity, other text adapters and the complete property/
header representation remain open.

A separate ordinary same-input source207 probe records another remaining gap:
GNU accepts declared frame depths 262144 and 393216, while Emaxx signals bytecode
stack overflow. Both accept depths 1 and 131072. `BC_STACK_VALUES` still reserves
256K Lisp words and stores frames separately; GNU's `BC_STACK_SIZE` reserves 512K
words including its frame headers. Source225 has not changed this limit. The
probe, exact editor/image identities and outputs are retained in the source225
bundle. Stack layout, capacity, physical accounting and frame overhead still need
repair; this is neither a timing result nor a source225 ordinary execution result.

## Implementation and GNU reference

`SharedStringState` now owns the actual unibyte octets or GNU multibyte bytes,
with their character count and multibyte flag. It no longer retains a Rust
`String` plus a separate extended-character payload. Encoding follows
`character.h` and the one-to-five-byte internal format, including surrogate,
non-Unicode and byte8 characters. `StringLike` text/extended-character views are
transient consumers, not a stored mutable mirror.

Unibyte `aref`, length and `aset` use the real byte array/count. Following
`data.c:Faset`, the shared-string path checks the existing index before the new
character, accepts the full GNU character range, and stores an unibyte character
without replacing its data allocation. Multibyte replacement changes only the
affected encoded bytes. ASCII unibyte strings can be promoted; non-ASCII unibyte
strings reject that promotion with the original string in the error data.

Raw-byte string producers now allocate the byte payload directly. Shared-string
`string-as-unibyte` follows `character.c:str_as_unibyte`: it preserves internal
bytes except that byte8 characters become single octets, and returns an already
unibyte object unchanged. Shared-string equality compares actual bytes and
character counts. Image cold data and restoration use the stored bytes; restored
character counts are validated against them. Graph copies copy that payload.

An ordinary same-input GNU probe verifies aliasing and property survival across
unibyte stores/GC, eighteen encoding boundary values, index/character error order
and error-data identity. It also shows `fns.c:Fclear_string` clears **storage
bytes**, makes the result unibyte, and **retains text properties**. The draft
implements that behavior; the previous text-based path cleared properties and
used the Rust text size. The permanent fixture additionally covers surrogates,
values beyond Unicode, five-byte characters and both ends of byte8.

Two new controls preserve that ordinary GNU program and check direct unibyte
payload contents, size and allocation address across actual `aset` calls at
lengths 1, 17, 257 and 4097. Their existence is not a passing test result.

## Evidence and live validation

The preserved [portable source220 draft](handover/2026-09-30-shared-reader-draft/source220-canonical-string-byte-draft-manifest.json)
replays all **421 source/test inputs** and file modes from main `21d20f0e`.
Compiler, formatting, strict Clippy and diff checks pass with zero warnings.
The [focused audit and broader launch](handover/2026-09-30-shared-reader-draft/source220-string-controls-and-broad-launch-manifest.json)
verify a fresh zero-warning gate build and **ten passes / two failures** across
all twelve controls. The direct payload/store control passes. Active-call code
mutation still fails. The new ordinary differential fails during Emaxx evaluation
with `Invalid character: 55296`: the unchanged `string` producer still routes
its characters through `char_for_codepoint`/Rust `char`. Later expressions in
that failed control are not passing coverage. Its expected GNU result is intact.
Source221 repairs this producer without changing the failed assertion.

Supervisor **67724 has exited**. Its continuation check used an old source219
receipt filename, so it never ran the intended broader stage. That failed
wrapper and all its outputs are preserved. The unexecuted stage ran once under
supervisor **68772**, which has exited. The
[complete broader audit](handover/2026-09-30-shared-reader-draft/source220-string-broad-results-manifest.json)
verifies all **927 tests** in the affected bytecode/native-runtime/pdumper/
primitives/reader/types modules: **925 pass / two fail / zero ignored**. Both
original failures remain. All source inputs, raw names/verdicts and the retained
executable and post-run image are verified; this does not certify later changes.

The bundle preserves source217's twelve compiler errors, source218's compiler
pass, and source219's three strict test-diagnostic lint errors. None of those
candidates ran runtime tests. Source220 replaces the three test `unwrap` calls
with descriptive expectations; no lint allowance or exclusion is introduced.
Every intermediate patch, manifest, raw compiler/lint log and the ordinary GNU
reference command/output is retained.

## Remaining architecture

The direct byte cursor and source225's flow repair pass selected tests in both
profiles; its full runs fail in completion. Source229 passes selected gate/release
and ordinary controls but fails full Rust and has a live terminal divergence.
Source231 passes selected gate/release and ordinary validation but fails complete macOS and Linux runs. Source234 passes 37 focused gate controls; its release and complete macOS runs are active. Instruction and call
costs need measured profiles; removing a cache is not a timing result. Complete
error, callback, rooting, suspended-frame and execution-mode coverage is required.

Plain `StringCell`/`Kind::String` storage still exists. The shared string still
has a Rust vector allocation and host property-span representation, rather than
the complete GNU string header/data/interval allocation layout. Unifying those
forms, removing temporary text adapters from ordinary paths, and accounting for
actual capacities, allocator metadata and retained memory remain required.
In particular, existing GNU-formula allocation charges are not measured physical
Rust allocation totals. Multibyte indexing and several text consumers still scan
or materialize a view. No timing, allocation-rate, RSS or GNU-parity claim is made.

The final task still includes allocated-symbol authority, real allocation
counters, full Linux/macOS validation, pinned Darwin certification, adversarial
review and the locked sixteen-workload criterion with its unchanged 3% ceiling.
