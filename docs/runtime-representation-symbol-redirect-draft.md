# Symbol value and alias union — 9 October 2026

Source316 is a **separately packaged, unapplied draft**. Its
[cumulative patch and selected evidence](handover/2026-10-09-symbol-fields/source316-symbol-redirect-draft-manifest.json)
include source315 on base `2712c057`. PR80's runtime remains source314; the full
architecture and performance goal remains active and incomplete.

GNU `lisp.h:Lisp_Symbol` stores either the plain value or alias target in its
`val` union. `eval.c:Fdefvaralias` hands a former value to an unbound target
before `SET_SYMBOL_ALIAS` overwrites the old slot. `pdumper.c:dump_symbol`
records the selected union member. The Rust cell now follows that structure:
one eight-byte union replaces the separate value and alias payloads. The
existing alias enumeration position selects the alias member; a value position
never coexists with it. Mutation requires an exclusive cell borrow, and image
replacement initializes the selected member before it can be read.

The per-interpreter cell's measured size is **56 bytes**, versus source315's
64, asserted in gate and release. This is not the full physical symbol
footprint: allocated names, indexes, weak side entries and enumeration storage
remain additional costs. No ordinary read gains a hash lookup or lock. No
elapsed-time improvement or GNU parity is claimed.

This is a representation change, not a claim that ordinary source315 aliases
retained their old values: the previous primitive already removed the old
binding after setting its separate alias. The union removes that second slot
and prevents a hidden value from coexisting with the redirect. Function and
property fields remain independent, as GNU requires.

Strict formatting, checking, Clippy and diff checks pass without warnings.
**All 128 selected tests pass in each profile**, extending the previous 126;
every old selector remains. One historical storage test expected a test-only
alias removal to restore its former value. GNU's union cannot preserve that
value. The corrected test explicitly checks its absence, then reinstalls a
plain value before the original value/flag assertions. The evidence audits
that bounded correction and the otherwise unchanged previous test bodies.

**Eighteen successful ordinary processes / nine comparisons match.** The new
fixture covers discarded values, alias retargeting, handover to an unbound
target, live function/plist fields, varied sizes, forced collection and eventual
reclamation. Its native case confirms that the caller and all three helpers
are native functions in both editors. Each uses its own executable and image.

Two setup failures remain in the archive. The first lifetime fixture collected
while the frame that had held its live vector still existed: GNU retained six
children through conservative stack words, while Emaxx reclaimed them. The
revised fixture returns from that frame before its reclamation check, preserving
every case and survival assertion. GNU and source315 then agree. The first
ordinary source316 cohort has **16 passes / two failures** because the launcher
passed an interpreted function object to `native-compile`. GNU `comp.el` accepts
a function name, lambda form or filename. Only those two processes were repeated
with the named-function API; both pass with unchanged source, fixture bodies,
executables/images, environment and deadline. The failed processes remain failed.

The portable patch reproduces **562 source inputs and modes**; 102 auxiliary
inputs are unchanged. Apply it to `2712c057`, then run the full goal's original
checks and build each editor's own image. The archive contains exact selectors,
commands, raw outputs, artifact identities, replay auditor and GNU source hashes.

The complete source316 macOS Rust gate is running separately under
`target/runtime-goal/symbol-ownership-2026-10-09/redirect-draft1-full`; no result
is claimed yet. Linux/macOS frozen comparison, relevant terminal coverage and
timing have not run. Source315's complete macOS result does not
certify source316. Per-instance interning, image copying and allocated-symbol
authority still must remove the weak side table and GC fixed-point walk.
Intervals/pure storage, real allocation accounting/counters, internal ownership
review, final adversarial review and the locked 16-workload/nine-round/every-case
3% performance criterion remain required.
