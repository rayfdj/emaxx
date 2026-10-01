# Common string representation draft — 2 October 2026

Read the [complete goal](runtime-representation-goal.md), [current handover](../HANDOVER.md)
and [correctness recovery](runtime-representation-correctness-recovery-draft.md).
The full goal is active and incomplete. The task runtime remains source249
(`99e61fca`); source256 is separate, unapplied work. Main remains `21d20f0e`.

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
with zero warnings. **Queue17749** waits for source249 terminal12347, then runs
all 184 focused selectors, all 957 affected tests, both gate/release profiles and
seventeen ordinary comparisons. The 174 preceding focused selectors and all 955
preceding affected tests remain included. Read current receipts before another
launch. The archived queue is not passing runtime evidence.

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
