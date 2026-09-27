# Allocated frame objects — 28 September 2026

The [complete goal](runtime-representation-goal.md) remains incomplete. This
checkpoint replaces frame IDs and their last native bridge handle with allocated
objects. Source 56 follows the [input repair](runtime-representation-input-checkpoint.md).
The separate [ordering repair](runtime-representation-ordering-checkpoint.md) is
integrated afterward; its source-36 full-gate failure remains a failure.

[Portable receipts](handover/2026-09-28-frames/manifest.json) preserve all source
manifests, patches, commands and raw logs from this migration, including failures.
Executable identities are provenance; rebuild on another machine.

## Object ownership and removed work

Frames now use their actual allocation address and GNU's `PVEC_FRAME=10` tag.
The common header and leading name word match GNU. The remaining Rust frame
state is not GNU's complete C frame layout; full native frame-struct ABI
compatibility has not been established. Ordinary access reads the object rather
than looking up an interpreter-local ID. Distinct frames with colliding local
labels have distinct Lisp/native words, including across interpreter owners.

Frame objects own their terminal, actual window references, focus link,
parameters and frame-local face vectors. Terminal top-frame links are actual
references too. GC traces those edges directly; cycles survive when reachable
and are reclaimed afterward. Image copying preserves graph identity and cycles.
Dumped nilled frames retain distinct address-based identities and shared
references; their reserved dump word is zero, not an address or local ID.

The live-frame list roots only live frames. Liveness follows
`frame.h:FRAME_LIVE_P`'s terminal-pointer rule. Terminal teardown closes the
device before deleting its frames; testing the terminal device's liveness there
would strand frames in the live list. The TTY routing reference is rooted only
during an active terminal session and cleared by the terminal guard.

All remaining `NativeHandle`, `NativeIdentity`, bridge-header and handle-map
machinery is removed. Cons mirrors and their heap bookkeeping remain pending.
The allocator counts the frame's actual rounded payload instead of an assumed
73-slot census term. The name cell is authoritative even after a raw native
store, following `frame.c`'s frame-parameter readers. The frame-local face table
is still a public view backed by the remaining hash-table/record machinery;
this checkpoint does not complete those migrations or remove all side tables.

## Validation and preserved failures

Source 40 reproduces equal native words for two distinct frames. That probe stops
at the identity assertion; its later bare-interpreter call to the Lisp helper
`set-frame-parameter` was invalid and was replaced with the native
`modify-frame-parameters` entry point. Sources 41/42 fail during the incomplete
type migration. Source 43 passes five focused tests but fails Clippy; source 44
contains test-fixture compilation errors. Source 45 passes fourteen focused
debug tests and all required static checks.

Source 49 fails compilation after removing redundant liveness state. A mistaken
main-thread dump edit is corrected before the successful source-50 build.
Source 50 has 145 selected release passes and one failure:
`native_reads_cons_fields_reached_through_canonical_vector_slots` reads zero
instead of fixnum 37's word from an unprepared cons. The exact failure also
reproduces on source 36's unchanged gate executable. Its 80-byte cons still has
a separate, initially empty native payload. Adding another preparation traversal
would retain the duplication; the next step is the authoritative 16-byte cons.

Source 51's terminal fixture has compilation errors; the corrected source-52
fixture fails because closed-device frames remain live. Sources 53/54 preserve
the face migration's compiler/Clippy failures. Source 54's thirteen focused
debug tests pass, including escaped frame/face survival and eventual reclamation.

Source 55's broader release selection executes 278 tests: **276 pass, two fail**,
zero ignored. Besides the cons failure above, saved-window restoration retains
a frame borrow across a mutable operation. Source 56 ends that borrow before
the update. All nineteen selected release frame, terminal, window-configuration
and dump tests pass (41.24 seconds test time), including the unchanged three
GNU terminal lifecycle contracts. Rustfmt, all-target/all-feature compiler
checks, strict Clippy and diff checks pass; source hashes stay unchanged.
Those later selected passes do not turn source 55 into a successful broad run.

Assertions about removed handle containers now check actual word identity and
reachable-object survival/reclamation. Existing semantic outcomes, GNU terminal
goldens, timeouts and ignores are unchanged. No frozen or performance claim
follows: local GNU is source-matched but differs from the pinned Darwin binary.

## Continue here

Finish cons payload unification and remove synchronization, registration,
duplicate tracing and per-store notifications. Preserve direct nested native
reads, cross-owner identity, deep/shared/cyclic graphs, conservative interior
roots, weak references, suspended execution and image restoration. Account for
allocator metadata separately from the two-word payload.

The older terminal divergences, full macOS/Linux gates, pinned frozen comparison,
record/keymap/hash and symbol authority, public ownership/serialization audit,
VM profiles and locked sixteen-workload performance evidence remain open. The
3% criterion and workload inventory are unchanged. Publication/Linux CI remains
pending explicit permission after the prior automatic push rejection.
