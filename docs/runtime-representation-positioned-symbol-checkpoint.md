# Authoritative positioned symbols — 28 September 2026

The [complete goal](runtime-representation-goal.md) remains incomplete. This
checkpoint follows the [cons checkpoint](runtime-representation-cons-checkpoint.md)
at `5bb1cbbf758e85497b94b06325ea59f9fe7aabad`. Source 70 passes 287 selected
debug tests and strict static checks. Source 71 changes only root/image
audit checks and field lifecycle documentation. Its release selection fails:
292 pass and one root-inventory check fails. Source 72 corrects that inventory
and passes all 25 selected release audit/image controls and strict static
checks. A fresh ordinary executable/image passes eight identical-input GNU
comparisons; three additional stream-reader comparisons still fail. A complete
gate on this source is still required. No performance result is claimed. The
[portable evidence manifest](handover/2026-09-28-positioned-symbols/manifest.json)
retains the source snapshots, commands, raw results and unsuccessful attempts.

## Representation and GNU reference

A positioned symbol now consists of the GNU vector header followed by its
actual symbol and position words. The header identifies `PVEC_SYMBOL_WITH_POS`
(6), two traced fields and zero non-Lisp fields. Interpreter, bytecode, native
helpers and GC use this same allocation. The private `RecordKind` variant,
per-native-heap copied header/fields, address map and duplicate native roots
are removed. The native helper returns the address already in the Lisp word,
and native stores immediately change the fields Lisp reads.

This follows `lisp.h:struct Lisp_Symbol_With_Pos`,
`alloc.c:build_symbol_with_pos`, `data.c`'s positioning primitives and
`comp.c:helper_GET_SYMBOL_WITH_POSITION` in GNU source
`636f166cfc86aa90d63f592fd99f3fdd9ef95ebd`. The logical allocation charge is
24 bytes, as with GNU `allocate_vector(2)` and the previous Lisp allocation
charge. The vector allocator reserves 32 bytes, including padding; the live
census reports that physical footprint. Shared block and allocator overhead
are additional. No GC threshold is relaxed.

The reader allocates the final positioned object itself. Its temporary
`ReaderForm::PositionedSymbol` and extra conversion walk are removed. A
borrowed name resolver interns through the active obarray before deciding
whether the symbol is canonical nil and whether to attach a position. This
follows `lread.c:read0`'s `oblookup`/`intern_driver` before
`build_symbol_with_pos`. The callback is needed while obarray storage remains
interpreter-owned; its cost is unprofiled. Other literal reader forms and the
ordinary reader's existing interning passes remain outside this migration.

The migration corrects `type-of`: with `symbols-with-pos-enabled` non-nil,
GNU returns `symbol`; `cl-type-of` still reports `symbol-with-pos`. Bytecode
`eq` now recognizes the new tag before consulting that compatibility flag.
Disabled `eq`/`eql` retain identity, while disabled `equal` compares the fields,
as GNU does. The distinct address hashes with positions disabled are retained.
Private obarray identity, including that table's distinct `nil` and `t`, is
resolved before wrapping. `#:` names remain uninterned and unexpanded, numeric
names remain symbols, and nonempty occurrences receive their positions.

The collector traces the two actual fields. The graph copier preserves
sharing with distinct storage. The portable dumper rejects a reachable
positioned symbol, following `pdumper.c:dump_vectorlike`'s unsupported type 6,
rather than serializing the former private record. Fingerprints invalidate
images built from the previous software.

## Evidence and preserved failures

Source 66 builds the two original acceptance probes against the preceding
checkpoint. Both fail: the public word exposes record tag 34 instead of 6,
and the native helper returns a different allocation. Source 67 preserves an
incomplete-match compiler failure and an unused import. Source 68 compiles,
then runs 196 selected debug tests: 193 pass and three fail. The old bytecode
tag guard causes warning suppression and the shared execution-mode fixture
to fail; a native error test still expects the former record representation.
Clippy also rejects four obsolete explicit drops after the native view map's
removal and an unnecessary mutable reference.

Source 69 repairs those causes. All 233 selected debug tests pass (241.02
seconds), and formatting, all-target/all-feature compiler checks, strict
Clippy and diff checks pass. The error test still requires the exact original
object, and the teardown tests still read their objects after the native heap
scope has ended. Their historical selectors and assertions are retained.

Source 70 removes the reader adapter and adds direct raw-reader word checks
plus identical GNU/Emaxx reader inputs. All 287 selected debug tests pass
(358.67 seconds), with strict static checks clean. These include native raw
stores, invalid tags, reachable-object survival and unreachable-object/name
reclamation, graph copying, dumper refusal, and all reader/bytecode controls
selected by the retained command. The shared native fixture runs 72 cases
across interpreted, bytecode and native functions, using two names and fresh
native artifacts, varied symbols and positions, callbacks and forced GC.
It asserts the actual execution modes.

The initial native fixture had an incorrect enabled `type-of` expectation;
GNU failed that version too. That input and result remain preserved. The
corrected common input passes GNU and fails the immutable source-65 ordinary
Emaxx before the implementation. Other before-probes preserve the private
obarray and `#:` mismatches rather than silently treating them as passes.

## Completed cons gate and audit repair

The unchanged complete source-65 macOS library gate finishes **failed**:
2,833 pass, three fail and two existing opt-in terminal tests remain ignored
(3,634.67 seconds overall). All ten library groups execute. Later Cargo
binary, integration and documentation stages do not execute after the failed
lightweight group. The exact executable, source hashes, logs and failed
summary remain retained.

Two audit checks still searched for `weak_hash_reachability_with_native`,
which the cons migration replaced with the common `weak_hash_reachability`.
Source 71 scans that actual function, retaining the stack/context assertions
and adding an explicit native-root traversal assertion. The third failure
flags undocumented image treatment of `functions_index` and `next_frame_id`.
The former is rebuilt by `install_function_cell` through
`set_function_binding`; the latter advances when `install_dead_frame` gives
each nilled image frame a new process-local ID. Those lifecycle reasons are
now recorded in the checked inventory. No field or test is excluded.
Later selected results cannot change source 65's failed full-gate verdict.

Source 71's broader release selection runs 293 tests: 292 pass and one fails
(57.29 seconds). Both original suspended-thread survival/reclamation contracts
pass. All strict static checks pass. The remaining root inventory failure
identifies `selected_frame_id`, `old_selected_frame_id` and
`buffer_case_tables`: after the frame/char-table migrations these hold actual
object references, but the inventory still labels them non-root metadata.
Source 72 moves them to the documented root list. Frames are re-created at
startup; case tables are written in `BUFFER_CASE_TABLE` and restored through
`install_buffer_case_table`. This changes classification, not what the GC
marks or what the image carries. Source 71 remains a failed broader run.

All 25 selected release audit/image controls pass on source 72 (0.66 seconds),
including the three original full-gate failures, the reset-root inventory and
all portable-dumper tests. Formatting, all-target/all-feature compiler checks,
strict Clippy and diff checks pass. The broader release selection has not been
rerun on source 72; its production runtime is unchanged from source 70.

A fresh source-72 ordinary executable and image build successfully. Sixteen
processes compare eight common inputs: native positioned-symbol words, ordinary
positioned reading, private obarray identity, uninterned names, cons mutation,
ordering, native vectors and native strings. All eight outputs match exactly.
Software, executable and image hashes remain unchanged during each run. The
executable SHA-256 is `f668fb4a777d6c5ef9f9092ff1d3d4f6d33c0abc263ca4c92b43ed913c1734b6`;
the image SHA-256 is `28f5a21137d4f18d93d761308d1eeca548676cc87643d0b4b2d035574a33d22e`.
These are local provenance, not artifacts to reuse on another host.

## Known reader gaps and remaining goal

Additional source-65 before-probes expose unresolved reader behavior outside
the object representation: marker positions should be relative to the read,
callable streams should also return positioned symbols, and the callable
adapter drains the entire source instead of leaving the next expression.
It also differs from GNU on non-ASCII callable input. The initial Unicode
probe caused Python's text decoding to fail; that harness failure is recorded,
and a separate raw-byte rerun preserves both editors' bytes. An ASCII callable
variant isolates the position discrepancy without erasing the Unicode result.
All three additional reader comparisons fail again on the immutable source-72
ordinary executable. The raw bytes, including GNU's non-UTF-8 callable output,
are retained. Both editors exit successfully; their outputs differ. These
failures remain work to do.

Read-only inspection also finds that the optional `EMAXX_GC_VERIFY` cons-edge
checker does not recognize several migrated vectorlike kinds, including
positioned symbols, records, char tables, frames, terminals and finalizers.
The survival and reclamation tests above exercise the collector itself, but
the optional checker needs those cases and a negative control. It does not
verify every field of every vectorlike allocation and must not be described
as complete heap verification.

Generic records, hash/keymap and symbol-cell authority, full buffer/frame ABI,
dump treatment, ownership/serialization and the remaining original audit
findings are still open. So are complete final-source Linux/macOS, frozen and
terminal validation, real allocation counters, VM/call profiles and the locked
sixteen-workload performance criterion. The 3% criterion and inventories are
unchanged. The sixteen older terminal divergences remain recorded in the
[input checkpoint](runtime-representation-input-checkpoint.md).

Local GNU comparisons use the source-matched executable whose hash differs
from the pinned Darwin binary. They are diagnostics, not frozen certification.
The user approved publishing commit `803e4326a16b7bfe13bb0b757ee85c8b02ac3998`;
it is pushed to `runtime-char-tables`, and the existing
[Linux full Rust gate](https://github.com/rayfdj/emaxx/actions/runs/36359209336)
is running on that earlier char-table commit. Its result does not certify this
later cons/positioned-symbol source.
