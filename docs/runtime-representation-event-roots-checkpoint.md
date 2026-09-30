# Event and TLS root repairs — 30 September 2026

Source170 extends the [live keymap candidate](runtime-representation-live-keymap-checkpoint.md)
in draft [PR #78](https://github.com/rayfdj/emaxx/pull/78). Its 343 compiled/test
inputs match the isolated validation checkout. Strict compiler, formatting,
warnings-denied Clippy and diff checks pass, along with **482 debug and 482
release controls** and **27 exact ordinary GNU comparisons**. The
[portable receipts](handover/2026-09-30-event-roots/manifest.json) cross-check
raw test inventories, verdicts, process exits and artifact hashes.
The complete macOS Rust gate fails. Full Linux Rust and terminal validation
pass; the full frozen comparison has one unexpected GNU outcome. The goal
remains open.

The [complete full-run receipts](handover/2026-09-30-event-root-full-validation/manifest.json)
verify **3,034 Linux Rust passes** (2,931 library, 61 binary and 42 integration),
two existing terminal ignores, and native artifact identity. All **223 terminal
scenarios, 651 screen comparisons and 28 filesystem comparisons** match
source/ABI-matched GNU. Linux frozen executes all **519 files / 7,928 outcomes**
with 1,038 successful processes. **7,927 outcomes match**; Emaxx has 7,670 passes,
47 existing expected failures, 211 skips and no unexpected outcomes. GNU has
one unexpected `jsonrpc-error` timeout in
`eglot-test-rust-completion-exit-function`; that outcome differs and the full
comparison remains failed. Both source166 autorevert and network-stream
mismatch files now match in their complete original inventories.

The full macOS gate passes 2,195 tests and fails the unchanged
`suspended_bytecode_retains_operand_and_unwind_roots` assertion: the first key
remains present after thread completion (`((1 t 1) (1 t 0))` instead of
`((1 t 0) (1 t 0))`). Four later library groups and both Cargo stages do not run.
The [failure archive](handover/2026-09-30-gc-retention-diagnosis/manifest.json)
checks the raw group results, unchanged source and retained executable/image
hashes. Selected debug/release passes did not predict this gate-profile result.

The [exact original Linux executable](https://github.com/rayfdj/emaxx/actions/runs/36666863766)
also reproduces its source166 failure under GDB hardware watchpoints. The first
key is marked from `weak_hash_reachability`'s native-root array at epochs 2–5.
Further [root provenance](https://github.com/rayfdj/emaxx/actions/runs/36668646203)
identifies a stale lexical-environment pointer in the native stack, at
`eval_call` offset `0xa8`. The inspected original assembly writes only one
byte there at ELF address `0x97ea7e`, leaving old upper pointer bytes while
executing `setq` and `let`. Neither the debugger nor the
instrumented trace3 pass clears the ordinary failure. The archive preserves
all three exact-binary diagnoses and the inspected assembly. The unchanged
source170 macOS executable passes a standalone replay; that pass does not
establish a fix for its full-gate failure. Source173, an isolated pending
candidate, uses the authoritative subr's arity and UNEVALLED classification,
removing the copied optional metadata and repeated function decoding.

Two fresh source170 input-decoding probes fail against source/ABI-matched GNU
on menu-filter lookup. Another ordinary probe demonstrates that TLS peer-report
queries share a mutable cached result where GNU returns independent reports.
These negative comparisons are preserved in the same archive. A separate
source171 checkout removes that cache and passes 414 debug and 414 release
controls, plus the ordinary report-identity and original GC probes. A stronger
ordinary probe reveals that mutation of its returned cipher string silently
does nothing. Source172 uses mutable Lisp strings and extends the new fixture
to require the mutation itself, while preserving its independence and GC
checks. Source172 validation is pending; neither candidate has been applied
to this production candidate.

Queued file notifications now trace raw events, descriptors and callbacks.
A dispatch batch remains rooted after leaving the interpreter's queue, so
collection in one callback cannot free a later event. Errors preserve remaining
recipients and newly queued events. Dispatch removes the one-element callback
vector allocation. This follows GNU's rooted input queue without changing
ordering or suppressing errors. The new control checks queue ownership without
conservative Rust locals, collecting dispatch, error-report survival, requeueing
and eventual release.

Process-owned GnuTLS boot parameters and the retained peer report now receive
static tracing and graph-preserving cloning. GNU process.h marks pending Lisp
parameters; GNU constructs its peer report from the session. Emaxx's existing
cached report also needs tracing while retained. An ordinary synchronous and
asynchronous TLS probe loses all four protocol fields on source166 after GC;
GNU and source170 retain them. A preliminary probe also differs after
`gnutls-deinit`; that difference remains unresolved. These focused repairs
do not establish the cause of the earlier Linux frozen mismatches or replay
freed-cons aborts until the affected ordinary paths are revalidated.

Special events now use live keymap lookup. The actual event and map remain
rooted across collecting menu filters, and `command-execute` receives an actual
vector. GNU comparisons cover deep inheritance, ordinary filter errors and
ephemeral thread-event payloads; the earlier ordinary source166 probe differs.

Development failures are preserved. Source168 failed compilation on two new
test borrows. Source169 passed 412 controls but its new queue test demanded
release while an unconsumed batch error report still owned the argument.
Source170 asserts report survival, consumes it, then retains the release
assertion. No existing assertion, timeout, inventory or expectation is weakened.

The recovered **source166** macOS full gate passes **3,019 tests**: 2,920 library,
60 binary and 39 integration, with two existing terminal ignores and native
artifact identity passing. The archive includes complete raw receipts and
four rehashed retained artifacts. Its second terminal run was interrupted
during **112/223**, without a final result or known exit status. The driver
was absent after a tool daemon restart; the exact interruption cause is
unestablished. This partial run is not a pass. Source166's complete Linux
reclamation and frozen failures remain unresolved in the
[failure archive](handover/2026-09-30-keymap-linux-failures/manifest.json).

The first [instrumented Linux root trace](https://github.com/rayfdj/emaxx/actions/runs/36664022033)
passes the original reclamation assertion but changes compilation and
allocations, so cannot clear the ordinary failure. The test captures child
output, leaving no usable ownership trace. Revised diagnostic-only logging
writes separate child trace files and permits the exact earlier source revision.
Neither diagnostic changes production root decisions or the original assertion.

Darwin GNU matches the required source and native ABI but differs from the
frozen executable pin. No pin changed. Bounded input decoding, symbol authority,
physical allocation accounting, final adversarial audit, complete final-source
compatibility and locked performance criteria remain unfinished. This
checkpoint makes no timing claim.
