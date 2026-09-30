# Bytecode representation and mutation baseline — 1 October 2026

Read the [complete goal](runtime-representation-goal.md) and
[current cons continuation](runtime-representation-cons-roots-draft.md).
This is a separate unfinished draft, **not an applied task-branch repair**. Source205 adds
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

The current isolated checkout is **source208**. Its
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

Current bytecode is stored in `RecordState` plus a detached `Vec<Value>`;
interpreted closures already have inline PVEC_CLOSURE storage. A shared closure
layout must cover predicates, interpreter/VM/native access, printing, cloning,
GC and image restoration. Mutable unibyte strings currently use Rust text
storage, which also needs scrutiny before adopting a direct byte cursor.
Only constructor field validation is repaired in the separate draft; no layout
migration or live mutation repair has been implemented. Linux evaluator validation,
symbol authority, physical accounting, final validation/audit and the locked
performance criterion all remain open.
