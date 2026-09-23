Bring Emaxx’s core Lisp runtime toward GNU Emacs C performance by first establishing one compact, authoritative object representation shared by the interpreter,
bytecode VM, and native code, then reducing the work performed per VM instruction and function call. Preserve GNU semantics throughout. Deliver the implementation,
reproducible performance evidence, complete relevant validation, and an adversarial de-cheating audit.

Treat this as architectural work. Prioritize eliminating unnecessary representations, allocations, indirections, synchronization, and bookkeeping. Moving metadata
elsewhere only counts as progress when ordinary operations stop paying for it.

1. Establish an exact, reproducible starting point.

   Fetch remote main, record its exact commit, and preserve existing local work. Read the current repository instructions, oracle build contract, compatibility
   requirements, and docs/adversarial-decheating-audit-2026-09-21.md. Reproduce its findings against the starting revision; distinguish findings already fixed from
   those still present.

   Record the actual GNU source revision, executable, configuration, native compilation capability, Lisp sources, and image identity. Record equivalent provenance for
   Emaxx, including compiler and build settings. Historical reports establish context; they do not certify the current implementation.

   Preserve the original baseline. If correctness repairs change its performance, also establish a corrected baseline so the cost of restoring required behavior
   remains visible.

2. Use GNU C as the architectural reference.

   Trace each changed mechanism to its actual GNU implementation, including lisp.h, alloc.c, data.c, eval.c, bytecode.c, and the relevant native compilation and
   dumping code. Preserve the division between behavior owned by C primitives and behavior owned by unchanged GNU Lisp.

   Do not unnecessarily introduce mechanisms GNU does not need: parallel object models, shadow state, general synchronization layers, identity caches, registries, or
   replacement Lisp implementations. Every necessary deviation must identify the concrete Rust, ownership, platform, or integration constraint it solves, its
   correctness argument, and its measured cost.

   Preserve Rust soundness. GNU’s implementation is a behavioral and architectural reference, not justification for invalid Rust aliasing, lifetimes, thread access, or
   unwinding across incompatible boundaries.

3. Make object representation compact and authoritative.

   Keep Lisp values one machine word on supported 64-bit targets. Make ordinary conses two Lisp words, matching GNU’s 16-byte payload, with any necessary allocator
   metadata accounted for separately.

   Interpreter, bytecode, and native operations must observe and mutate the same authoritative objects. Remove duplicate mutable payloads and the agreement checks,
   conversion paths, mutation notifications, and mirror maintenance made necessary by those duplicates.

   Ordinary car, cdr, field stores, identity checks, and common value operations must have GNU-comparable direct paths. Shrinking a structure while adding a mandatory
   hash lookup on every access does not satisfy this requirement.

   Review symbols, strings, vectors, records, closures, and native handles for the same problem. Preserve object identity, sharing, cycles, mutation visibility,
   alignment, native/module ABI contracts, and dump restoration. Any temporary migration adapters must have explicit removal criteria and be removed before declaring
   the architecture complete.

4. Align allocation, garbage collection, and rooting with the representation.

   Follow GNU’s actual allocation, marking, sweeping, root handling, and dumped-object treatment where applicable. Remove duplicate tracing and unnecessary retention
   caused by mirrors, registries, and reconstructed state.

   Account honestly for allocated bytes, live objects, collection triggers, memory retained, and actual collection work. Explain collection-count differences under
   equivalent settings. Performance gains must not come from suppressing collection, undercounting allocations, retaining dead objects indefinitely, or shifting
   unreported work outside the measurement.

   Verify both survival of reachable objects and eventual reclamation of unreachable objects. Cover suspended bytecode stacks, native calls, lexical environments,
   dynamic bindings, unwind state, weak references, Lisp threads, and restored images.

   Enforce runtime ownership or serialization at the actual boundary. A serial test runner does not establish that public Rust APIs or process-global mutable state are
   sound.

5. Reduce VM and function-call overhead using measured profiles.

   After establishing the shared representation, profile interpreted execution, bytecode dispatch, primitive calls, Lisp calls, native transitions, variable access,
   and returns. Measure instructions and allocations per operation where supported, alongside elapsed time.

   Remove redundant decoding, type reconstruction, repeated lookups, frame construction, temporary allocations, argument copying, and bookkeeping where GNU performs
   less work. Preserve required evaluation order, arity checks, mutation behavior, errors, backtraces, quit checks, hooks, binding, and nonlocal exits.

   Optimize the ordinary execution paths. Preserve redefinition, advice, aliases, and dynamic behavior that invalidate assumptions. Introduce further specialization
   only when its validity conditions are explicit and adversarially tested.

6. Close the known de-cheating findings and challenge the complete execution path.

   Resolve all still-applicable findings from the September 21 audit, including argument-list validation, circular and dotted lists, mutation during argument
   evaluation, argument-buffer bounds, minimum-arity error timing, platform attributes lost by primitive dispatch generation, unsound concurrent runtime access, and
   misleading performance adapters.

   Restore the coverage of:
   threads_retain_lexical_caller_roots_across_separate_callee_environments and
   suspended_bytecode_retains_operand_and_unwind_roots.

   If a reclamation assertion needs a reproducible process boundary, retain its semantic contract and its live-root coverage. Do not replace it with a weaker
   assertion.

   Audit runtime code, caches, startup, images, native loading, test selection, comparison logic, and reporting. Look for test-name or benchmark-shape special cases,
   canned results, swallowed errors, fake capabilities, oracle forwarding, omitted work, stale artifacts, and silent execution-mode fallback.

   Add meaningful differential and adversarial tests using the same inputs in GNU and Emaxx. Include changed names and paths, varied sizes and contents, shared and
   cyclic structures, mutation, redefinition, forced collection, error paths, and transitions between execution modes. Use reproducible generated cases where useful.

   Verify that the measurement and reporting machinery rejects deliberately incorrect results, missing reports, unsuccessful processes, and incomplete inventories. A
   green summary is insufficient without consistent raw evidence.

7. Measure equivalent work and preserve unfavorable results.

   Both editors must execute the same Lisp workloads through their normal entry points with comparable startup state and execution modes. Each editor must use its own
   valid executable and image. Rust loops directly calling internal methods cannot substitute for Lisp loops used by GNU.

   Lock a representative suite before optimizing. Include interpreted lexical and dynamic loops, interpreted calls, bytecode calls, native execution and transitions,
   allocation, list traversal, mapcar, explicit GC, and unchanged upstream workloads covering sorting, undo, and substantial real Lisp execution.

   Check results and relevant final state outside timed regions. Record actual execution mode, GC counts, allocation volume, peak memory where available, startup time,
   and workload time. Separate startup and body measurements while retaining total cost.

   Use repeated, interleaved runs on the same host under controlled load, with enough work to avoid meaningless timer-scale comparisons. Retain every sample and report
   distributions, per-workload GNU ratios, regressions, and aggregate results. Build and profile separately from authoritative timing runs.

   Define the GNU-parity criterion and a small, evidence-based measurement tolerance before optimization. Do not widen that tolerance or change the workload selection
   to accommodate disappointing results. Persistent slowdowns must remain visible and receive profile-based explanations.

8. Require complete correctness and zero-warning validation.

   The final implementation must build and undergo the supported Linux and macOS validation paths. A locally patched diagnostic build does not certify the final
   source.

   Require clean results from:

   cargo fmt --all -- --check
   cargo check --locked --all-targets --all-features
   cargo clippy --locked --all-targets --all-features -- -D warnings
   git diff --check
   python3 tools/serial_grouped_gate.py --scope full

   Validate release-specific paths as well as the gate profile. Require zero formatting differences, zero Clippy warnings, and no unresolved compiler warnings. Fix
   causes instead of introducing lint suppressions, broad allowances, or exclusions to produce a clean report.

   Run the ordinary full frozen compatibility comparison against each platform’s pinned oracle, plus the affected native compilation, module, dumping, startup, thread,
   GC, and integration contracts. Preserve existing native artifact identity requirements.

   Preserve upstream tests, selectors, expected outcomes, timeouts, comparison strictness, and inventory coverage. Do not add ignores, convert failures to expected
   failures, reduce workloads, or normalize meaningful differences. Any demonstrably invalid local test must be corrected against GNU evidence while preserving
   equivalent coverage.

   Existing separately scoped platform differences must remain explicit in raw results and the final report. They must never be counted as passes or used to excuse new
   regressions.

9. Finish with independently checkable evidence.

   Maintain a concise record mapping each architectural change to the corresponding GNU mechanism, removed overhead, correctness evidence, and performance result.
   Retain commands, source and artifact identities, raw logs, profiles, all timing samples, and reproduction instructions.

   Conduct a final adversarial review of the actual final source and the complete evidence chain. Treat earlier checkpoints as historical evidence; rerun checks
   invalidated by subsequent changes. Report the audit’s coverage and remaining limitations without claiming universal absence of cheating.

   Completion requires the shared compact representation, removal of unnecessary ordinary-path bookkeeping, resolution of the in-scope correctness and de-cheating
   findings, clean required validation, and satisfaction of the predeclared core-runtime performance criteria.

   If the architecture is improved but GNU remains materially faster, report the remaining costs with profiles and keep the performance objective open. Continue
   through implementation, debugging, measurement, and validation; a plan, a smaller structure, or a faster isolated microbenchmark is not completion.
