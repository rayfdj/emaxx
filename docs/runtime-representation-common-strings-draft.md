# Common string representation draft — 2 October 2026

Read the [complete goal](runtime-representation-goal.md), [current handover](../HANDOVER.md)
and [correctness recovery](runtime-representation-correctness-recovery-draft.md).
The full goal is active and incomplete. The task runtime is now source261, pushed
as `eddfe782`, with 488 inputs and [closed selected validation](handover/2026-09-30-shared-reader-draft/source261-common-string-selected-manifest.json).
The earlier separately packaged drafts and failures remain historical evidence.
Main remains `21d20f0e`.

The [portable draft](handover/2026-09-30-shared-reader-draft/source256-common-strings-draft-manifest.json)
contains all 480 source/test input hashes, incremental and complete patches,
GNU C references, original negative fixtures, exact GNU output bytes, failed
intermediate checks and the source255 focused failure. Both patches replay every
input byte and Git checkout mode from `b8e5ad20` and main respectively.
The local candidate is `target/runtime-goal/recovered-2026-09-30/common-strings/emaxx`;
receipts and helpers are under `target/runtime-goal/resume-2026-09-28`.

## Mechanism and observed correctness

GNU's `Lisp_String` carries identity, actual bytes and intervals in one object.
Source249 still had immutable host strings and mutable canonical strings.
Storing a host-created string through a binding could copy it, while mutating
one held directly inside a vector could discard the write. Ordinary probes
expose different identity, `aset`, property, `fillarray` and `clear-string` results
from GNU. They do not establish when each defect first appeared.

Source256 routes host construction to the existing canonical byte payload.
`SharedText` is an alias to that handle. `Kind::String`, the `StringCell`/`TextRef`
arena and its GC path are removed, together with `stored_value` conversions and
the VM's owned-text adapter. `CodeBytes` contains the original one-word handle and
borrows actual bytes only while fetching an instruction. Symbol cells own their
host lookup text directly; the actual Lisp name object keeps its identity.
Dumping writes actual bytes, counts, flags and properties through one string key
and type. Both older string wire types restore the same canonical payload.

The `fillarray` repair follows `fns.c`: validate even an empty string, retain its
properties, store the low byte for an unibyte string, and reject a multibyte fill
that changes the byte length. `aset` and `clear-string` reach the existing mutable
payload. Pure copying preserves actual bytes and flags while dropping properties.
No selector, timeout, GNU expectation or comparison rule changes.

Source255 passes strict checks, then records **181 focused passes, one failure
and two existing ignores**. Both new GNU contracts pass. The failed local bytecode
fixture used Rust U+0089/U+0087 text for intended opcode bytes. An ordinary GNU
probe confirms that `make-byte-code` accepts the two unibyte bytes and rejects the
four-byte Unicode string. Source256 changes that fixture's construction, retains
all original assertions and adds rejection of the Unicode form. The failed
source255 executable, image and raw verdicts remain retained.

Source256 passes formatting, all-target checking, strict Clippy and diff checks
with zero warnings. Source249 terminal12347 has now exited with all 226 scenarios /
686 comparisons matching. **Queue17749** has exited. Its
[focused gate result](handover/2026-09-30-shared-reader-draft/source256-focused-gate-manifest.json)
is audited: **182 passes / two existing ignores**, with all 184 selectors
and both retained executable/image inputs verified. The
[broader gate audit](handover/2026-09-30-shared-reader-draft/source256-broad-failure-and259-focused-manifest.json)
verifies **956 passes / one failure**, all 957 names and the same retained inputs.
The failed native-call fixture used five bytes of Unicode text for intended opcode
bytes 137/84/135. Ordinary GNU accepts the unibyte form, retains its identity and
returns 42; it rejects the Unicode form. Release and seventeen ordinary comparisons
did not run. Every prior selector remains included. The failure is preserved; its
correction must retain the native dispatch/result/backtrace/depth assertions.

## Subsequent comparison and hash repair

The [source259 portable draft](handover/2026-09-30-shared-reader-draft/source259-string-comparison-draft-manifest.json)
replays all **488 inputs and Git checkout modes** from `3d7dd8f9` and main. Its
checkout is `target/runtime-goal/recovered-2026-09-30/string-comparisons/emaxx`.
It builds on source256 without changing that queued checkout.

GNU `fns.c:Fstring_equal` and `internal_equal` compare character counts, byte counts
and actual storage bytes. Source259 shares that direct contents path across the
Rust handles/values and Lisp equality primitives. `string-equal` reads a symbol's
actual Lisp name object and follows `lisp.h:SYMBOLP` for positioned symbols. The
including-properties path borrows the actual spans after checking identity and
contents; it no longer clones decoded text and its properties for comparison.

GNU `intervals.c:intervals_equal_1(..., true)` uses ordinary `Fequal` on property
values. GNU `fns.c:hash_interval` likewise uses ordinary `sxhash_obj`. The repair
uses that same relation for comparison and hashing, so nested string properties
do not change an equivalent key. String hashing reads actual bytes instead of
allocating a Rust text projection and extended-character list. The existing hash
mixing algorithm remains; GNU hash algorithm or cost parity is not established.
The dumper stores key/value pairs and reconstructs hashes after relocation. Its
existing test gains unibyte, byte8 and private-use keys while retaining all assertions.

Four exact ordinary GNU fixtures are preserved. Source249 has six wrong booleans
in the 64-pair storage matrix, 92 wrong results in the 512-row symbol/name matrix,
eight wrong property-matrix results plus three mutation observations, and thirteen
wrong hash/table observations. The symbol fixture's initial nonexistent-constructor
error is retained separately; its corrected fixture uses GNU `position-symbol`.
The property tests cover nested values, mutation, GC, interval segmentation and
custom table lookup/replacement/removal. No historical origin is inferred for the
newly probed symbol/property/hash gaps.

Source257 failed checking on eight missing-import diagnostics in the new Rust test;
source258 fixed only those imports and passed strict checks. C review then exposed
the related hash requirement before runtime execution. Its waiting queue21849 was
explicitly withdrawn, preserving the source and receipts. This is not a claimed
runtime failure or pass. Source259 includes the hash repair and passes formatting,
all-target checking, strict Clippy and diff checks with zero warnings. Both packaging
helper setup failures remain recorded too.

**Queue22381** has exited. Its [focused gate audit](handover/2026-09-30-shared-reader-draft/source256-broad-failure-and259-focused-manifest.json)
verifies **188 passes / two existing ignores**, all **190 selectors**, both retained
inputs, all four GNU fixtures and the extended dump test. The broader gate records
**961 passes / one failure**, with all 962 names audited. It fails on the same invalid
native-call fixture as source256. Release and ordinary stages did not run. Every
preceding 184/957 common-string control and both failed executable/image pairs remain.

## Corrected native fixture: source261

After source259 exited, source260 changed only the existing native-call test to
construct actual unibyte opcode bytes 137/84/135. It retains target classification,
the native result 42, empty backtrace and evaluation-depth assertions, and adds
rejection of the former Unicode form. Ordinary GNU `make-byte-code` confirms both
cases. Source260's new `.unwrap_err()` violates the existing lint; source261 changes
only that call to explanatory `expect_err`, with no suppression. Production code
is byte-identical to source259. Both intermediate patches and failures are retained.

Source261 has **488 exactly replayed inputs** and zero-warning strict checks. Both
focused profiles pass **189 tests / two existing ignores**, preserving all earlier
selectors and adding the corrected native fixture to the focused list. Its broader
gate passes **all 962 tests**. The first release evidence auditor incorrectly expected
only two retained inputs; the retention helper had also copied the preceding gate
image. A separate corrected auditor verifies the release executable/image and the
older gate image against their recorded hashes. The initial audit failure is retained;
no test rerun or changed expectation is used to repair that bookkeeping.

Continuation **30525** has exited. Its [closed selected audit](handover/2026-09-30-shared-reader-draft/source261-common-string-selected-manifest.json)
verifies **189 focused passes / two existing ignores and 962 affected passes in each
profile**, plus **22 ordinary exact GNU matches**: the 21 fixtures and unchanged native
constructor probe. Gate/release inventory differences are only the existing debug-only
file-descriptor control. All source249 selectors and every original GNU expectation
remain. The archive contains 368 evidence/helper files, both exact portable patches,
and the source259, source260 and initial source261 audit failures.

Source261 is now applied and pushed as `eddfe782`. Complete macOS supervisor **31023**
has verified the selected audit, cloned the idle Cargo cache, cleaned only the new
target crate's gate artifacts and started the unchanged full Rust inventory. Frozen
and terminal gates follow sequentially. The clean full-validation checkout is
`target/runtime-goal/recovered-2026-09-30/common-strings-full/emaxx`, commit `eddfe782`,
with the same 488 inputs. Exact-head Linux
[Rust36904097575](https://github.com/rayfdj/emaxx/actions/runs/36904097575) and
[frozen36904105841](https://github.com/rayfdj/emaxx/actions/runs/36904105841) are dispatched.
Keep both source261 checkouts unchanged. No complete platform pass, measured saving
or full-goal completion is implied by selected checks or a queued command.

## Remaining requirements

This unifies authority but does not yet supply GNU's 32-byte string header or
sblock allocation. `RefCell`, `Vec` capacities, property vectors and vector arena
overhead remain. Moving host keys grows `SymbolCell`; removing a separate key cell
does not establish a net memory reduction. Symbol value/function/plist authority
still resides in per-interpreter tables and registries.

The old host raw-byte/private-use text convention and many transient decoded
Rust views remain. The empty unibyte string is canonical, but GNU's empty multibyte
singleton and general pure-string write protection remain unfinished. Public
runtime ownership, actual cumulative allocation counters, physical accounting,
VM stack layout, final platform validation and adversarial review remain open.
No speedup, memory saving or GNU performance parity has been measured. All sixteen
locked workloads and the 3% ceiling still apply.

The [subsequent comparison probe](handover/2026-09-30-shared-reader-draft/source249-string-comparison-gaps-manifest.json)
confirms six wrong results among a fixed matrix of 64 string pairs: four
`equal-including-properties` results conflate unibyte bytes with multibyte byte8,
and two `string-equal` results conflate byte8 with actual private-use Unicode.
Ordinary `equal` is correct throughout this matrix. The same fixture on recorded
source174/main and source207 executable/image pairs has twelve wrong boolean
results; source249 fixes six and introduces none in this matrix. This does not
excuse other recorded regressions or establish general correctness.

Those projection repairs are absent from frozen source256 and implemented separately
in source259/261 above. Keep the original negatives and both queued input sets unchanged
while validating. String ordering and version comparison still use host text; neither
the comparison repair nor the common-string draft completes the architecture.
