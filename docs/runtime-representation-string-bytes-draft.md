# Canonical string bytes draft — 1 October 2026

Read the [complete goal](runtime-representation-goal.md),
[current handover](../HANDOVER.md) and
[shared-closure continuation](runtime-representation-bytecode-draft.md).
This is a separate unfinished draft. No string or bytecode runtime change is
applied to the task branch's validated source207.

The current **source220** checkout is
`target/runtime-goal/recovered-2026-09-30/string-bytes/emaxx`. It starts from
source216, whose ten focused controls have nine passes/one failure and whose
unchanged 848-test broader inventory has 847 passes/one failure. The remaining
failure is active-call code mutation: `(41 194)` instead of `(67 194)`.
The old closure checkout and its executable/image remain intact.

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

The [portable source220 draft](handover/2026-09-30-shared-reader-draft/source220-canonical-string-byte-draft-manifest.json)
replays all **421 source/test inputs** and file modes from main `21d20f0e`.
Compiler, formatting, strict Clippy and diff checks pass with zero warnings.
Supervisor **67724** runs a fresh gate build, twelve focused controls and all
tests in the bytecode/native-runtime/pdumper/primitives/reader/types modules.
The known active-call mutation failure remains in both inventories. Keep the
checkout and helpers frozen while that supervisor or its children run. No
runtime outcome is assigned by this draft archive.

The bundle preserves source217's twelve compiler errors, source218's compiler
pass, and source219's three strict test-diagnostic lint errors. None of those
candidates ran runtime tests. Source220 replaces the three test `unwrap` calls
with descriptive expectations; no lint allowance or exclusion is introduced.
Every intermediate patch, manifest, raw compiler/lint log and the ordinary GNU
reference command/output is retained.

## Remaining architecture

The VM still executes its decoded program. This draft provides authoritative
bytes for the next direct byte cursor; it does not repair active-call mutation
or establish lower instruction/call cost. The next VM change must preserve
current operands, byte-offset jumps, errors, callbacks, roots and suspended
frames without a second mutable code copy or mutation-notification cache.

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
