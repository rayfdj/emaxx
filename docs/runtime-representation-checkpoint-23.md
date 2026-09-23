# Shared object checkpoint 23 — diagnostic WIP

This snapshot exists to run the shared-object implementation on the repository's existing Linux CI. It is not a completed architecture or performance milestone.

The preceding macOS snapshot (source22) ran 173 test executions covering 172 distinct names: 171 executions passed and two failed. The original 72-case string fixture returns `((t t t nil 36) (t t t nil 36))`; unchanged local GNU returns `((t t t t 36) (t t t t 36))`. The other failure is a reclamation test retaining a newly canonical tagged record pointer on its scanned caller stack.

This snapshot confines that record pointer to a separate live-root frame, preserving live identity, census and eventual-reclamation assertions. It adds `diagnose_native_string_mutation_predicates` and a separate temporary Lisp fixture that reports failing cases and each native predicate. The original GNU-comparison fixture remains unchanged. Remove the diagnostic test and fixture once the mismatch is explained and fixed; it is not final certification.

Use `.github/workflows/frozen-run.yml` with `validation=affected`, `file=test/src/data-tests.el`, and `rust_filter=diagnose_native_string_mutation_predicates`. The targeted runtime log must show exactly that one test executing. A failure remains a failure; if the runtime control fails, the later compatibility file does not run. The workflow, original tests, expectations, timeout and comparison policy are unchanged.

The broader draft shares native object words for symbols, floats, vectors, closures, both current string storage forms, records, bignums and reader objects. It also repairs owning/drain iterator roots. Several host object kinds still use native bridges, strings still have two storage forms, and ordinary conses are still 80 bytes. Shared 16-byte conses, remaining ownership/soundness work, measured VM/call optimization, full platform/frozen/release validation and the locked 16-case performance criterion all remain open. No performance gain is claimed.

The separately published correctness branch is `fix/native-relocation-root-lifetime` at `9220d3a8c209de9b9b155c66914c9e5b03e2f2f5`. Its 122 targeted tests pass. This WIP contains the broad versions of the native GC ownership/root repairs; it has not inherited that branch's validation result.
