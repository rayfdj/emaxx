# Shared bytecode closure draft — 1 October 2026

Read the [complete goal](runtime-representation-goal.md) and
[current cons continuation](runtime-representation-cons-roots-draft.md).
The full goal remains unfinished. The source231 task checkpoint now carries the
later closure/string work beyond runtime207; PR #79 is draft and main is unchanged.
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
contract and passes strict checks plus all 31 focused controls. The task worktree
matches its 440 inputs. Fresh release validation passes all 31 focused controls
and 937 affected tests; all 54 ordinary GNU comparisons pass. Complete macOS
library validation records 2,974 passes, five failures and two existing ignores;
both later Cargo stages never run. The linked string handover names the root
metadata, closure assertion and bitmap coverage failures. Linux is pending.
Source229's closed terminal
run retains seven quote-display divergences and a later GNU startup-readiness
failure; 54 scenarios never start. No complete terminal or performance pass exists.
The linked
string-byte handover is authoritative for that job and the remaining
plain-string/header/stack-capacity/accounting limitations. The
source216 results below remain a preserved closure-only baseline.

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
