# Cons-edge verifier and evaluator stack repair — 28 September 2026

The [complete goal](runtime-representation-goal.md) remains incomplete. This
work follows positioned-symbol commit `a210f90cd65961bf4dc6aa2ccb87b529d46c4360`.
Source 84 passes 186 selected debug tests with the optional GC verifier enabled,
31 additional positioned-symbol controls and strict static checks. Both original
suspended-thread contracts retain their original live-root and reclamation
assertions. All 209 selected release tests pass with the verifier enabled
(53.07 seconds in libtest), with zero ignores. No full, frozen or performance
certification is claimed. The
[portable evidence](handover/2026-09-28-gc-verifier/manifest.json) retains the
baseline failure, broader failed run, diagnostic snapshots and final results.

## Checker coverage

The optional cons-edge checker previously omitted migrated char tables,
sub-char-tables, frames, terminals, records, finalizers and positioned symbols.
It now reads each kind's actual mark bit. A negative control marks a parent cons
without marking its positioned-symbol child, calls the real checker and requires
rejection. It then removes that edge and requires acceptance, restoring the
entire cons-block mark bitmap and epoch afterwards. Source 77 fails this test
before the implementation; the executable and source snapshot remain preserved.

This checker scans cons blocks, not every field of every allocation. Before
sweep it verifies cons-to-cons and recognized cons-to-vectorlike mark edges;
after sweep it verifies cons-to-cons allocation edges. The former documentation's
"whole-heap" description was inaccurate and is corrected. This follows the
purpose of GNU `alloc.c:GC_CHECK_MARKED_OBJECTS`, while retaining the narrower
implemented coverage explicitly. Ordinary runs with verification disabled do
not perform these scans.

## Preserved failure and stack diagnosis

Source 79 passes the negative control and strict static checks, but its broader
debug selection fails: 185 pass and the original lexical-caller thread contract
fails. A weak key remains after the first worker exits, producing
`((1 t 1) (1 t 0))` instead of `((1 t 0) (1 t 0))`. The same immutable executable
fails with the verifier both enabled and disabled. The verifier's execution is
therefore not a sufficient explanation.

The first source-bootstrap run can pass while a subsequent process loading the
same fixture image fails. Source 81/82 add temporary tracing that distinguishes
exact roots from conservative stack roots. Their first bootstrap runs pass;
the unchanged original contract fails again when those binaries load their
retained images. The trace identifies a direct word for the dead key in the
running OS stack, rather than a retained interpreter environment or registered
rooted-vector slot. All temporary tracing is removed before source 84.

LLDB on immutable source 82 places that word 24 bytes above the stack pointer
of an active `Interpreter::eval` frame. The disassembly reserves that space
for the large `LispErrorKind` payload used by the positioned-symbol branch's
`map_err(LispError::into_kind)`. The ordinary cons branch does not initialize
that error payload, leaving older pointer-shaped contents visible to conservative
scanning. The raw locals, stack locations, disassembly and failed original
contract are preserved. The first debugger attempt cannot launch in the sandbox;
a first unrestricted callback stops on optimized-out variables; both unsuccessful
attempts remain recorded alongside the successful observational trace.

Source 84 inspects the existing boxed error by reference. Non-void errors keep
the same box; the void-variable path still produces the exact positioned object
in GNU's error data. It avoids unboxing and reboxing the large error enum in the
inlined evaluator. On these two ARM debug executables, the evaluator's reserved
stack frame decreases from 224 to 192 bytes. That is a local code-generation
observation, not a timing result or a guarantee about every stack slot.

All 186 selected debug tests pass with verification enabled (144.08 seconds in
libtest), with zero ignores. The original lexical-caller contract also passes in
two separate processes using the retained fixture image, first with verification
enabled and then disabled. Both use the original parent/child entry point;
the image is not deleted to force a bootstrap pass. All 31 additional existing
positioned-symbol controls pass (267.39 seconds), including the exact
void-variable object identity check. Formatting, all-target/all-feature compiler
checking, strict Clippy and diff checking pass on unchanged source.

## Remaining goal

The repaired case does not establish that every conservative scan, inactive
stack slot or public ownership boundary is sound. The thread-local uninterned
symbol book remains under investigation while its heap is process-wide.
Generic records, symbol cells, hash/keymap authority, full buffer/frame ABI,
dump mapping, allocation counters, VM/call profiling and the locked sixteen
workloads with the unchanged 3% criterion remain open. Final Linux/macOS full,
frozen and terminal runs and an adversarial review of their actual final source
are still required. The local GNU executable is source-matched and differs from
the pinned Darwin executable; local comparisons remain diagnostics.
