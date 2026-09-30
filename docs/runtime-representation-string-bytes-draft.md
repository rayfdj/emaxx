# Canonical string bytes draft — 1 October 2026

Read the [complete goal](runtime-representation-goal.md),
[current handover](../HANDOVER.md) and
[shared-closure continuation](runtime-representation-bytecode-draft.md).
This is a separate unfinished draft. No string or bytecode runtime change is
applied to the task branch's validated source207.

The current **source223** checkout is
`target/runtime-goal/recovered-2026-09-30/string-bytes/emaxx`. It starts from
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
The **930-test broader gate** is running under supervisor **73057**; release
controls and the same broader inventory run under supervisor **73532**. Keep
the checkout and helpers frozen until both supervisors and their children exit.
Their complete results must be audited separately.

All **425 inputs and modes** replay exactly from main, with zero-warning strict
checks. Source222's five obsolete-helper warnings and strict failure remain
preserved; it ran no runtime tests. The portable source223 draft assigns no
runtime or performance verdict. No draft runtime is applied to the task branch.

One temporary adapter remains: non-ASCII immutable `Kind::String` values supplied
through the old Rust API are converted once at entry to an owned byte slice.
It has no identity registry or mutation cache. Ordinary reader, constructor and
restored shared-string code uses its actual payload. Remove this adapter with
the plain-string representation before architectural completion.

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

Source223 implements the direct byte cursor and passes its focused mutation
controls, but broader correctness remains under validation. Instruction and call
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
