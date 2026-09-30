# Bytecode representation and mutation baseline — 1 October 2026

Read the [complete goal](runtime-representation-goal.md) and
[current cons continuation](runtime-representation-cons-roots-draft.md).
This is an isolated negative baseline, **not an applied repair**. Source205 adds
only two tests in `src/lisp/native_comp/runtime/record_layout_tests.rs`, in
`target/runtime-goal/recovered-2026-09-30/bytecode-closure/emaxx`. Its runtime is
identical to published source204 `d186d40b`. No associated process remains live.

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
No migration or mutation repair has been implemented. Linux census diagnosis,
symbol authority, physical accounting, final validation/audit and the locked
performance criterion all remain open.
