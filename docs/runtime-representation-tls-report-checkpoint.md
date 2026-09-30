# Independent mutable TLS reports — 30 September 2026

Source172 removes the process-owned Lisp peer-report cache. Like GNU
`gnutls.c:Fgnutls_peer_status`, a query builds a fresh report from the live
session and its recorded verification flags. A handshake no longer allocates
and retains a report that nobody requested. Process-owned pending boot
parameters keep their existing GC tracing; scalar verification state needs no
Lisp root. Report and certificate strings use the ordinary mutable Lisp string
allocator, following GNU's `build_string` and `make_string` behavior.

All **345 compiled/test inputs** match the isolated validation checkout.
Strict compiler, formatting, warnings-denied Clippy and diff checks pass.
**414 debug and 414 release controls** pass, including the original thread and
GC contracts. Two exact ordinary source/ABI-matched GNU comparisons pass: the
original synchronous/asynchronous TLS-GC fixture and a new report test covering
independent identities, mutation, collection and a subsequent query. The
[portable receipts](handover/2026-09-30-tls-peer-reports/manifest.json) audit
every selected test name, raw verdict, original inventory, process result,
source manifest and retained binary/image hash.

Negative evidence remains preserved. Source170 returns shared report objects
where GNU returns independent objects. Source171 removes that cache and passes
its initial controls, but a stronger ordinary probe shows that mutation of a
returned cipher string silently does nothing. Source172 adds the missing
mutation assertion to the new fixture and fixes the allocator choice. No
existing GC, identity or independence assertion is removed. The archived
source171 executable/image relocation failures occurred before Lisp evaluation;
replays at their original unchanged paths retain both passing and failing
outcomes. The first source172 check helper stopped on an existing manifest;
the corrected helper verifies it before the successful compiler check.

Complete validation of source172 remains required. Source170's complete
[full-run evidence](handover/2026-09-30-event-root-full-validation/manifest.json)
is earlier-source evidence: Linux Rust and terminal comparisons pass, macOS
Rust fails reclamation, and Linux frozen differs on one unexpected GNU Eglot
timeout. Its original failure is not cleared by these selected passes.
Source173 separately removes copied optional call metadata and repeated
function decoding after exact-binary stack diagnosis; it is still isolated
under validation. The ordinary input-decoding and post-deinit TLS differences
remain unresolved.

Darwin GNU matches the required source and native ABI but differs from the
frozen executable pin. No pin, timeout, selector or existing expectation
changed. Compact representation completion, physical allocation accounting,
final adversarial audit, complete final-source compatibility and the locked
performance criterion remain unfinished. This checkpoint makes no timing or
full-goal completion claim.
