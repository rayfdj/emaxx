# Shared bytecode closure draft — 1 October 2026

Read the [complete goal](runtime-representation-goal.md) and
[current cons continuation](runtime-representation-cons-roots-draft.md).
The full goal remains unfinished. The source234 task checkpoint carries the
later closure/string work and GNU interactive-metadata repair; PR #79 is draft
and main is unchanged. Its 442 inputs replay exactly, strict checks pass and all
37 focused gate controls pass. Release and full macOS validation are active.
The linked string handover records the source231 Linux Edebug abort, the matching
GNU negative probe, source232 library-control repairs and every evidence limit.
The next [canonical string-byte draft](runtime-representation-string-bytes-draft.md)
continues from source216 in a separate checkout.

That continuation now has frozen **source229** and separate **source231**: canonical byte storage and full-character
constructors are implemented, and the VM fetches current bytes with byte-offset
cursors rather than decoded-program caches. Source224 passes eighteen focused
controls in both profiles after a GNU-backed fixture correction; source223's
broader runs each retain 929 passes and that one invalid-fixture failure.
Source225 checks only actual control-flow destinations and removes duplicate
fetching across fast/slow dispatch. Both profiles pass all 932 affected tests and
20 focused controls, but full Rust/terminal runs abort in native completion.
Source228 repairs the completion crash but retains a concat character-loss
failure (24/25 focused and 932/933 broader passes). Source229 copies canonical
concat bytes directly and passes strict checks plus all 26 focused gate controls;
both profiles pass all 934 affected tests and fifty ordinary comparisons pass.
Full Rust later fails the old quoted-bytecode `Kind::Record` assertion, while
ordinary GNU/source229 confirm the shared closure's fields and execution. The
terminal run has a quote-rendering divergence. Separate source230 repairs three
substring failures and that internal assertion; strict checks and all three
substring controls pass. Its 31 focused controls retain one added-fixture setup
failure (bare interpreter lacks GNU `cadr`), while all 937 affected tests and
54 ordinary comparisons pass. Source231 loads GNU only for that additional
contract and passes strict checks plus all 31 focused controls. The published source231 checkpoint
matched its 440 inputs before source234. Fresh release validation passes all 31 focused controls
and 937 affected tests; all 54 ordinary GNU comparisons pass. Complete macOS
library validation records 2,974 passes, five failures and two existing ignores;
both later Cargo stages never run. The linked string handover names the root
metadata, closure assertion and bitmap coverage failures. Linux later aborts in
Edebug; the source234 repair and its still-incomplete validation are recorded above.
Source229's closed terminal
run retains seven quote-display divergences and a later GNU startup-readiness
failure; 54 scenarios never start. No complete terminal or performance pass exists.
The linked
string-byte handover is authoritative for that job and the remaining
plain-string/header/stack-capacity/accounting limitations. The
source216 results below remain a preserved closure-only baseline.

## Main checkpoint continuation — 8 October 2026

The [current handover](../HANDOVER.md) closes source312 platform validation and the
fixture-only source313 census correction, including the complete 3,167-pass Linux gate
and the incomplete terminal comparison with its failed remaining-scenario run. The user has requested merging this checkpoint
to main. The full goal remains active; allocated symbols, intervals/pure storage,
physical accounting/counters, remaining adapters, internal ownership review and
the unchanged final validation/performance requirements continue after the merge.
The dated sections below preserve their original failures and evidence limits.

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

The [selected evidence](handover/2026-09-30-shared-reader-draft/source312-gc-call-timer-selected-manifest.json)
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

This is a [correctness publication](handover/2026-09-30-shared-reader-draft/source312-application-decision.json)
with [exact applied-source verification](handover/2026-09-30-shared-reader-draft/source312-applied-source-verification.json).
Full exact-source Linux/macOS validation and312 timing remain separate;
inspect the saved launch/queue receipts before starting another run.

Predecessor311's [Linux Rust run](handover/2026-09-30-shared-reader-draft/source311-linux-rust-results-manifest.json)
passes **3,164 tests with two existing ignores**. Its
[full Linux frozen run](handover/2026-09-30-shared-reader-draft/source311-linux-frozen-results-manifest.json)
matches **519 files / 7,928 outcomes / 1,038 successful processes**:
each editor reports 7,670 passes, 47 expected failures and 211 skips.
Its [paired full16 diagnostic](handover/2026-09-30-shared-reader-draft/source311-linux-stack-performance-results-manifest.json)
verifies all 192 actual answers/modes. The raw body geometric ratio311/310
is **1.006777**; paired GNU aggregates are 2.903372× and 2.841965× respectively.
These were measured before the native GC completion repair: reported GC counts
and elapsed GC time omitted native automatic collections. Keep the timings and
regressions visible, but do not call them equivalent-work performance acceptance.

Source310's [macOS frozen run](handover/2026-09-30-shared-reader-draft/source310-macos-frozen-abort-manifest.json)
remains failed: 182 complete comparisons / 2,951 matching outcomes, followed by
the ERC crash; 336 files never started. Its
[terminal raw audit](handover/2026-09-30-shared-reader-draft/source310-macos-terminal-qualified-results-manifest.json)
finds 226 scenarios / 686 matching comparisons, but a diagnostic rebuild changed
the executable during the run, so unchanged-artifact acceptance is unestablished.
The [original frozen executable/image](handover/2026-09-30-shared-reader-draft/source310-original-frozen-retention-verified.json)
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

The [selected evidence](handover/2026-09-30-shared-reader-draft/source311-bytecode-stack-selected-manifest.json)
verifies all **548 runtime/build/fixture inputs**, **94 unchanged auxiliary inputs**,
warning-free strict checks, **1,065 passes per profile**, five focused controls per
profile, and **58 ordinary GNU comparisons**, including 198 generated rows.
Both portable patches reproduce every runtime byte and mode. All 1,036 predecessor
selectors and all 24 audit controls remain. GNU's maximum single declared depth
of 524280, nested reservations, arguments and recovery after overflow match.
The original capacity fixture is unchanged. Source310's actual stack-overflow
failure and the earlier moved-image setup failure remain separately recorded.

This is a [correctness publication](handover/2026-09-30-shared-reader-draft/source311-application-decision.json),
with [exact applied-source verification](handover/2026-09-30-shared-reader-draft/source311-applied-source-verification.json).
**Source311 timing and complete platform results are pending.** The added
`tools/core_runtime_perf_pair.py` builds both exact revisions before three alternating
full16 pilots on one Linux runner. It retains executable/image copies and every
actual process answer, execution mode, startup/body/total time, GC delta and RSS.
Its raw-evidence audit passed an existing real pilot and rejected seven deliberate
corruptions. The new orchestration still needs its actual Linux execution; this
is diagnostic preparation, not calibrated nine-round acceptance.

Predecessor source310 now has [complete Linux frozen evidence](handover/2026-09-30-shared-reader-draft/source310-linux-frozen-results-manifest.json):
**519 files / 7,928 matching outcomes / 1,038 successful processes**. Each editor
reports 7,670 passes, 47 expected failures and 211 skips. Its
[complete macOS Rust evidence](handover/2026-09-30-shared-reader-draft/source310-macos-rust-results-manifest.json)
verifies **3,150 passes / two existing ignores**, including native artifact identity.
Source310 macOS frozen and terminal remain separate; inspect the saved queue state.
These results certify310 only. The source310 Linux Rust run stays failed with
2,295 passes / one GNU reference failure and 765 library names unstarted.

The [Linux GNU census trace](handover/2026-09-30-shared-reader-draft/source310-linux-gnu-census-trace-results-manifest.json)
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

## Current source216

The current checkout remains
`target/runtime-goal/recovered-2026-09-30/bytecode-closure/emaxx`. The
[source214 patch and audited controls](handover/2026-09-30-shared-reader-draft/source214-shared-closure-draft-and-controls-manifest.json)
replace detached bytecode `RecordState`/`Vec<Value>` storage with the same
`ClosureRef`/PVEC_CLOSURE allocation as interpreted functions. The public Rust
view is now `Kind::Closure`. `RecordKind::Closure`, its IDs/registry entries,
the dense per-record program cache, and slot-mutation cache invalidation are
removed. Constructor, reader, interpreter, VM/native entry, predicates, printing,
copying, GC and image restoration use the actual inline fields. `make-closure`
copies the actual closure and constants without a temporary outer slot vector.

Strict compiler, fmt, Clippy and diff checks pass with zero warnings. All six
original controls execute: **five pass / one fails**. The layout controls now
finish all four/five/six-slot cases, native word stores and current-child tracing.
The four-slot allocation is **40 bytes**, with no detached payload; block metadata
is separate. Constructor validation, original constant sharing/cloning/GC and
mutation between calls pass. Active-call code mutation still returns `(41 194)`
instead of GNU's `(67 194)`. It remains an ordinary failed assertion.

Active and suspended VM roots now retain the actual function word, including
across collecting redefinition of a named function. That implementation still
needs the broader original thread/GC tests. The VM currently builds a temporary
decoded activation at entry; the immutable-string decode cache also remains.
This does not establish the required direct byte cursor or instruction/call
performance. Mutable strings still use Rust text storage, so direct execution
must address authoritative code bytes without adding a mutable mirror or a
linear character scan per instruction.

Review after source214 found that its new direct vector copy called a census
helper whose ordinary-vector arm does nothing. The missing live-vector increment
is not covered by those six controls. The
[source215 portable draft and strict results](handover/2026-09-30-shared-reader-draft/source215-shared-closure-draft-manifest.json)
repair the real allocation-site increment and add twenty copy-census/identity
cases: zero/1/3/17/257 constants with 4/5/6/9 closure fields. Strict checks pass.
Its [audited focused results](handover/2026-09-30-shared-reader-draft/source215-shared-closure-controls-manifest.json)
verify **six passes / one failure** across all seven controls, including the new
copy-census/identity test. Supervisor **61972 has exited**. Its
[audited broader failure and diagnosis](handover/2026-09-30-shared-reader-draft/source215-broad-failure-and-diagnosis-manifest.json)
preserve **839 passes / nine failures**, with all 848 bytecode/native-runtime/
pdumper/primitives names and verdicts checked. The failed executable and post-run
image are retained by hash; the latter is not a per-test image-use transcript.

Five failures occur in GNU-output assertions before Emaxx executes. The helper
omitted the ordinary grouped gate's `LANG=C` and `LC_ALL=C`; an unchanged-source,
unchanged-assertion replay under those settings passes all five. Three old VM
tests construct bytecode with multibyte code strings; two also supply obsolete
list-form vector literals. Ordinary GNU rejects those inputs. A separate probe
confirms that actual unibyte strings and vectors preserve increment, repeated
argument identity and shared property/text mutation. Its wrapper initially
omits the property's printed notation from its expected string, fails, and is
preserved; the later audit verifies the actual property-bearing output. No
constructor validation is relaxed. Active-call mutation remains a real failure.

[Source216's portable draft](handover/2026-09-30-shared-reader-draft/source216-bytecode-fixture-repair-draft-manifest.json)
changes only those three unit-test constructors from source215, preserving every
original behavior assertion, and restores the established gate locale in its
helpers. Its 418-input patch and file modes replay exactly, and strict checks
pass with zero warnings. Supervisor **64424 has exited**. Its
[closed audit](handover/2026-09-30-shared-reader-draft/source216-bytecode-results-manifest.json)
verifies **nine passes / one failure** across all ten focused controls, and
**847 passes / one failure** across the unchanged 848-test broader inventory,
with zero ignores. Active-call mutation is the sole failure in both groups.
The exact executable and post-run image are retained. These gate-profile
results do not certify release paths or complete correctness. Source220 now
develops the required canonical string storage separately; source216 stays intact.

All **418 inputs** of sources209–215 replay exactly from main `21d20f0e`, including
executable modes. The source214 archive preserves source209's three compiler
errors, source210's missed thread-context rename, source211/212's obsolete-helper
warnings, and source213's strict single-match lint failure. None ran runtime tests.
Source214's original executable, all six raw verdicts and the later census review
finding remain distinct from source215. No later pass overwrites a failed attempt.

## Preserved baselines

Source205 adds
only two tests in `src/lisp/native_comp/runtime/record_layout_tests.rs`, in
`target/runtime-goal/recovered-2026-09-30/bytecode-closure/emaxx`. Its runtime is
identical to published source204 `d186d40b`. No associated process remains live.

The preserved **source206** retains those two tests
and adds three permanent GNU differential controls with the unchanged ordinary
programs and expected results below. Its
[portable patch and audited gate](handover/2026-09-30-shared-reader-draft/source206-bytecode-negative-controls-manifest.json)
replay all **416 inputs** from main `21d20f0e`, including file modes. The fresh
gate build succeeds; **one control passes and four fail**. Both representation
assertions and both mutation contracts fail. Constant sharing, cloning, GC and
rejected closure `aset` pass. Runtime204 is unchanged; only tests and six fixture
files differ. Supervisor **51384 has exited**. These are preserved failures,
not expected-failure annotations or a repair.

The preserved **source208** constructor candidate's
[418-input patch and constructor negative baseline](handover/2026-09-30-shared-reader-draft/source208-bytecode-constructor-draft-manifest.json)
carry evaluator207 and add GNU `alloc.c:Fmake_byte_code`'s four field checks
before allocation: fixnum/cons/nil arguments, an unibyte code string, a normal
constants vector and nonnegative fixnum stack depth. The original slots still
use detached host storage. No layout or live-code mutation repair is included.

The same ordinary input makes GNU reject twelve malformed constructors that
source204 accepts. Both editors exit zero with empty stderr; executable/image
and source identities are unchanged. Valid controls preserve seven descriptors,
including negative fixnums and dotted/cyclic conses; code/constant identity;
and closure lengths 4, 5, 6, 7 and 9. GNU accepts these descriptors and extra
slots at construction without validating their later execution. Its ASCII
`string-make-multibyte` case is also accepted. The new control preserves all
those distinctions rather than imposing stricter checks than GNU.

Source208's [closed control audit](handover/2026-09-30-shared-reader-draft/source208-bytecode-constructor-results-manifest.json)
verifies zero-warning strict checks, a fresh gate executable, all 418 unchanged
source inputs and **two passes / four failures** across all six controls.
Constructor validation and constant-sharing/cloning/GC pass. Both original
layout and both mutation assertions fail with their original actual/expected
values. Raw libtest exits 101. After saving those complete results, the wrapper
tries to print an obsolete baseline filename and exits 1 with FileNotFoundError;
that failure and the executed helper are preserved. The corrected
`test-bytecode-controls-next.py` helper is separately retained for future runs.
No test was rerun or converted to an expected failure to repair reporting.
Supervisor **53789 has exited**; this separate checkout is available for the
next representation change after verifying its current manifest.

The [portable evidence](handover/2026-09-30-shared-reader-draft/source205-bytecode-negative-baseline-manifest.json)
contains the complete patch from main `21d20f0e`, all 410 compiled/test input
hashes, exact patch/mode replay, a fresh gate build and both raw failures.
The four-slot bytecode object occupies an 88-byte Rust record/slot allocation,
versus GNU's 40-byte header and four Lisp slots; allocator metadata is separate.
Its current header is PVEC_RECORD (34), not PVEC_CLOSURE (31). Each test fails
at that first four-slot case. Later five/six-slot checks, native field stores
and GC tracing assertions in those tests did not execute. Do not count them as
validated coverage.

Ordinary commands use source204's unchanged executable and image. Both editors
run the same inputs with `-Q`; results, errors, process status and input hashes
are retained:

| Probe | GNU | Emaxx |
| --- | --- | --- |
| Four/five/six-slot bytecode calls, shared constant-vector mutation, cloning, forced GC and rejected `aset` on the closure itself | Passes original expected output | Identical output, zero exit, empty stderr |
| Change code-string opcode from constant 0 to constant 1 between calls | Returns 67 after mutation | Still returns 41 |
| A callback changes the next constant opcode during an active bytecode call | Returns 67 | Still returns 41 |

In the latter two probes Lisp reads the changed opcode in both editors. Emaxx's
VM nevertheless runs its old decoded instruction. `CachedProgram` retains the
decoded program; `execute_record` returns that cache without consulting the
mutable code string. An entry-only refresh would still fail the active-call
case. GNU `bytecode.c` fetches the next opcode from the actual code bytes.
The future representation/VM repair must preserve constant sharing and execute
current code, including mutation inside a call, without a second authoritative
payload or per-mutation mirror maintenance.

An earlier transition probe tried `aset` directly on a closure. Both editors
reject this with `wrong-type-argument arrayp` and exit 255; their backtraces
differ. That invalid probe is preserved as failed and is not evidence that GNU
supports Lisp closure-slot mutation. Do not broaden `aset` to make it pass.

Baseline bytecode is stored in `RecordState` plus a detached `Vec<Value>`;
interpreted closures already have inline PVEC_CLOSURE storage. A shared closure
layout must cover predicates, interpreter/VM/native access, printing, cloning,
GC and image restoration. Mutable unibyte strings currently use Rust text
storage, which also needs scrutiny before adopting a direct byte cursor.
The current draft repairs that layout and mutation between calls; active-call
mutation remains failed. Source207's complete Linux/macOS Rust, terminal and
Linux frozen results certify only the published checkpoint. Broader bytecode validation, remaining
symbol/string authority, physical accounting, final validation/audit and the
locked performance criterion all remain open.
