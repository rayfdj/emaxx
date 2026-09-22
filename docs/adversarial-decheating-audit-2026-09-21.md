**Adversarial de-cheating audit of remote main — 2026-09-21**

Audited remote: `45eb1531caabb35b816fc83e2e8a5b88090dae4e`, fetched from `origin/main`. Focus: whether the recent Rust implementation work preserves GNU Emacs behavior and produces honest progress toward C performance.

**Verdict: there is real performance progress, but the latest work does not earn a clean de-cheating or C-parity sign-off.** Fresh measurements show useful improvements after the earlier GC-rate correction. However, the optimized evaluator still omits GNU checks and differs on ordinary Lisp mutation, two correctness contracts were disabled, and the documented performance scoreboard is both broken through the current CLI and methodologically unsuitable for an editor-performance claim. These are concrete implementation and measurement problems; this audit does not infer intent.

The runtime review concentrated on the changes since `556f228`: evaluator calls and bindings, primitive dispatch, the new allocator/representation, GC accounting, test exceptions, and measurement paths. That range changes 134 files, with 25,580 insertions and 15,259 deletions. This is a bounded audit of those consequential paths, not exhaustive verification of every subsystem.

**What the independent measurements establish**

The baseline is checkpoint 20k, `4068bb4`, **after** the correction for artificially infrequent GC. Both Rust revisions were built with the same compiler and release settings, and each received its own dumped image. Three rounds alternated GNU, baseline, and current processes. All subjects executed the same Lisp file; checks ran outside the timed regions. Default GC settings stayed at `800000` / `1.0`. The allocation-only probe records collections but does not validate every discarded allocation.

There is an important qualification: the exact remote head does not compile on this ARM Mac. Both Rust diagnostic copies remove only three Linux-gated inotify dispatch arms that the new macro incorrectly emits on macOS. The patches and build logs are retained. These are diagnostic measurements of that narrowly repaired implementation, not measurements of an unmodified shippable remote build. The local GNU source is the pinned `636f166c…`, but its executable hash differs from `compat/oracle.lock.json`; no oracle lock was changed and no frozen certification is claimed.

Median body time, seconds; a lower current/GNU ratio is better:

| Workload | GNU C | 20k | Current | Time reduction from 20k | Current/GNU |
| --- | ---: | ---: | ---: | ---: | ---: |
| Interpreted lexical loop, 2M | 0.980 | 2.271 | 1.780 | 21.7% | 1.82× |
| Interpreted dynamic loop, 2M | 0.410 | 0.894 | 0.786 | 12.1% | 1.92× |
| Interpreted function calls, 1M | 0.699 | 1.785 | 1.304 | 27.0% | 1.87× |
| Bytecode calls with checked sum, 2M | 0.0331 | 0.1079 | 0.1133 | **−5.0%** | 3.42× |
| Cons allocation in lexical wrapper, 6M | 2.400 | 8.076 | 6.104 | 24.4% | 2.54× |
| Mapcar/sequence workload, 2M elements | 0.176 | 1.036 | 0.771 | 25.6% | 4.39× |
| Ten explicit collections | 0.0794 | 0.1523 | 0.1083 | 28.9% | 1.36× |

These gains were **not** obtained by reducing GC frequency between the two Rust builds. Every round gave identical before/after counts: lexical loop 80, interpreted calls 80, cons workload 359, mapcar 41, and explicit collections 10. GNU gave 62, 62, 163, 10, and 10 respectively. Thus the broader allocation behavior still differs from C, even where the original simple cons probe once matched its collection count. The unchanged `tools/perf/gcs-per-conses.el` on this host gave 120 collections for both Rust revisions versus 94 for this GNU build; that host-specific result should not be silently substituted for the ledger's Linux 120/120 claim.

To check that improvements reach real Lisp, two alternating rounds also executed four **unchanged upstream ERT tests**, accepting only an actual expected pass from each editor. All 24 test executions passed. These are test-body durations, with process/startup time retained separately:

| Upstream test | GNU C | 20k | Current | Time reduction | Current/GNU |
| --- | ---: | ---: | ---: | ---: | ---: |
| `fns-tests-sort` | 1.104 | 2.293 | 1.767 | 22.9% | 1.60× |
| `undo-test4` | 0.655 | 3.839 | 3.115 | 18.9% | 4.76× |
| `mule-cmds-tests--ucs-names-missing-names` | 0.804 | 2.634 | 2.438 | 7.5% | 3.03× |
| `pcase-tests-macro` | 0.000096 | 0.000196 | 0.000321 | −63.9% | 3.36× |

The pcase row is retained, but its sub-millisecond scale is too small for a useful optimization conclusion from two samples. None of these small samples supports a universal “Emacs is N times faster” statement. They do support real improvement in the interpreted and allocating paths, with bytecode and substantial C-performance gaps remaining.

**Findings, in recommended repair order**

**1. P1 — The call-path optimization still skips GNU behavior, including the bound that made its stack allocation valid.**

Locations: [core.rs](../src/lisp/eval/core.rs#L749), especially lines 749–803; `ListForms::next` at line 237. Introduced across 20a/20i; the 20k repair fixed the initial oversized-call case but did not close these cases.

`ListForms::count()` is not GNU's `list_length`: it does not reject dotted tails or detect cycles. After that count, the evaluator walks the mutable list again without retaining a bound. An earlier argument may extend a later cell, so an initially two-argument call can evaluate and write twelve values into the eight-element array. The source's assertion that the initial count guarantees `argnum < 8` is false. The diagnostic release run returned all twelve values; this audit does not claim it crashed.

GNU also treats the fixed-arity and MANY walks differently. For fixed arity it reads the current cell's cdr **after** evaluating its car. The shared Rust iterator reads it beforehand. Finally, the early arity check only handles the maximum: a too-short call can perform side effects before signaling.

Fresh same-program comparisons:

| Probe | GNU | Current Rust |
| --- | --- | --- |
| Add one argument while evaluating a two-argument `list` call | `(1 2)` | `(1 2 3)` |
| Add ten arguments in that call | `(1 2)` | `(1 2 3 4 5 6 7 8 9 10 11 12)` |
| Replace a `cons` call's remaining argument during its first argument | `(1 . 3)` | `(1 . 2)` |
| `(eval '(list 1 . 2))` | `(wrong-type-argument listp 2)` | `(1)` |
| Evaluate a `list` call with circular arguments | `circular-list` | Still running at the 8-second cutoff |
| Too-few-argument `setcar`, with a side effect in its one argument | Signals before side effect | Signals after side effect |

For example, the last mutation comparison is simply:

```elisp
(setq audit-form
      '(cons (progn (setcdr (cdr audit-form) (list 3)) 1) 2))
(prin1 (eval audit-form))
```

GNU's reference is local `src/eval.c:2540` onward and `src/fns.c:118`. Restore its proper-list checks, both arity bounds, and the respective mutation/bounding rules. Then remeasure. Until then, the phrase “as eval_sub does” overstates the implementation. The fraction of the speedup attributable to these omitted checks has **not** been measured.

**2. P1 — The documented performance scoreboard cannot substantiate C-performance progress.**

Locations: [perf-harness.rs](../src/bin/perf-harness.rs#L299), [batch.rs](../src/batch.rs#L149), [perf.rs](../src/perf.rs#L798), and [perf_scenarios.json](../compat/perf_scenarios.json#L22).

The harness asks the executable to evaluate `emaxx-perf-run-batch`. The current CLI goes through GNU startup Lisp before reaching the old special-request interception. Reproducing that exact helper call on the diagnostic current build exits 255 with `void-function`. Therefore README's advertised performance command is not a working current scoreboard.

Even if that entry point were restored, the marker and overlay adapters are Rust-authored loops calling `Interpreter`/`Buffer` methods directly. GNU executes the upstream Lisp `dotimes`, `insert`, and `delete-char` calls. Both suites are nevertheless labeled `comparable`. The Rust overlay adapter additionally bypasses the ordinary primitive path, and delete errors are discarded. Its cases report completion without checking final buffer/marker/overlay state. The interpreter adapter starts a fresh bare `Interpreter::new()`, while GNU starts with its normal image; it reports zero GC counts without measuring them.

The adapter design predates the latest representation work. It is not evidence that today's checkpoint tables were generated from these replacement loops: the recent `tools/perf/` scripts use shared Lisp. It is a standing misleading measurement path, now also disconnected from the CLI. Replace it with identical Lisp workloads and semantic checks on both editors, rather than merely reconnecting the interception.

**3. P2 — The recent green gate became weaker, and the last full compatibility certificate is older than this implementation.**

Locations: [the two ignored contracts](../src/lisp/primitives/tests.rs#L24328), [the gate allowlist](../tools/grouped_gate.py#L51), and [the last checked-in full frozen report](frozen-run-result-2026-09-14.md).

20n ignored three GC contracts; 20o restored one and left these two ignored:

- `threads_retain_lexical_caller_roots_across_separate_callee_environments`
- `suspended_bytecode_retains_operand_and_unwind_roots`

The same changes also added their names to the gate's accepted ignore list. The ledger openly discloses this as weakening, which is better than hiding it, but the green result cannot certify those contracts. Each test verifies both survival of live roots and release of dead ones; disabling the whole test loses both protections even though the stated problem is excessive retention. Their Lisp programs matched GNU in fresh standalone release runs here. That does not reinstate their coverage in the gate profile or in the full test sequence where the recorded failures occurred.

Preserve the live-root assertions while resolving the retention issue or moving the release assertion to a reproducible process boundary. Do not make an exact reclamation claim solely from a passing release sample.

The checked-in complete frozen result is for `a5084e8`, before the new representation work. The recent checkpoint receipts list Rust groups plus focused/corpus probes; they do not supply a new complete frozen result for `45eb153`. Run the ordinary full comparison again after these repairs before carrying that older compatibility score forward. This audit did not rerun the entire Rust or frozen suite.

**4. P1 — The new primitive lifting broke the macOS build.**

Location: [dispatch_table](../src/lisp/mod.rs#L161), introduced by `6752598` (20d).

The macro captures arm attributes but omits them from its emitted function/table entries. Consequently, the three `#[cfg(target_os = "linux")]` inotify arms refer to Linux-only helpers on macOS. An exact-head release build fails with six compiler errors, including missing `file_notify_error_with_errno`, `validate_inotify_aspects`, and `register_inotify_file_notify_watch`. Preserve platform attributes in the emitted code and add a macOS build check to the normal validation path. The current Linux gate does not establish this platform still builds.

The diagnostic copies used for this audit delete only those three already-Linux-only arms on macOS. That local workaround is retained as evidence and is not proposed as the general implementation fix.

**5. P1 — Serializing the special gate hid an unmet runtime assumption from ordinary Rust tests.**

Locations: [the process-global allocator](../src/lisp/alloc.rs#L263), [ProcessTable](../src/lisp/types.rs#L723), [Value](../src/lisp/types.rs#L1973), and [.cargo/config.toml](../.cargo/config.toml#L6).

The new allocator and symbol tables presume only one Rust OS thread can use them. GNU enforces its corresponding assumption with its global Lisp lock. Here the grouped gate was changed to one test thread, but ordinary `cargo test` still defaults to two. `Value(usize)` also now automatically implements `Send` and `Sync` despite naming those unsynchronized objects.

A small program using only safe public Rust APIs allocated 100,000 floats on each of two threads, joined them, then read their values. **127,122 of 200,000 values had changed** in the recorded run. This is evidence that the scheduling assumption is not enforced, not evidence about GNU Lisp threads, which this implementation runs on one OS thread. It undermines default test reliability and the advertised Rust API. Align the ordinary test configuration immediately and enforce ownership/serialization at the runtime boundary instead of relying on the gate script alone.

**6. P2 — The recent microbenchmarks are timing probes, not correctness gates.**

Locations: [interp-loops.el](../tools/perf/interp-loops.el), [call-loop-10m.el](../tools/perf/call-loop-10m.el), and [consing.el](../tools/perf/consing.el#L10).

The committed scripts mostly print elapsed times and discard the computed results. `consing.el` labels `(car (cdr (assq 'conses (garbage-collect))))` as “conses live”, but that field is the object size, **16**, not the live count. A skipped traversal or allocation can therefore look fast without these scripts rejecting it. Add result/structural checks outside the timer, preserve per-sample GC counts, and retain raw results with binary/source identities. The checked probe in this audit improves that coverage but is still not a substitute for the compatibility suite.

**Historical inflation and the remaining distance to C**

The earlier 20g–20j allocation gains need an explicit asterisk. The ledger's own [20k correction](honesty-audit-2026-08-18.md#2026-09-19-checkpoint-20k-an-adversarial-audit-of-the-representation-phase-and-what-it-found-value-copy-the-collection-rate-as-alloccs-the-call-paths-count-the-parked-stacks-registers-cs-block-geometry) records three collections versus GNU's 120 on six million conses. Correcting the accounting changed that Rust result from about 3.17 to 7.63 seconds in its recorded experiment. Those four checkpoints were not measuring equivalent GC pressure. That specific distortion was corrected before the baseline used here; it should not be counted as a newly discovered active 40× GC suppression.

The subsequent word-sized values, register-sized results, cheaper cons field access, and reduced mark work do produce real gains. Remaining architectural differences are visible in the source: dumped objects are reconstructed into the swept heap; GC subtracts an image-byte estimate rather than separating a mapped dump region; conses retain native-view agreement/mutation bookkeeping; native mirrors and side tables remain; symbol value/function/plist ownership is still separate from the symbol cell. These are reasons to profile further, not automatic proof of cheating. The current bytecode regression and 3–5× gaps on some ordinary work argue for selecting the next change from representative profiles instead of the next representation milestone alone.

I found no new benchmark/test-name dispatch or oracle-result forwarding in the reviewed changed runtime paths. That bounded negative result does not make the omitted semantics, disabled tests, or stale scoreboard acceptable, and it cannot quantify an intent-based “percentage of cheating.”

**Evidence and reproduction**

The durable evidence is [audits/2026-09-21](audits/2026-09-21/README.md): all raw timing samples, ERT reports and process outputs, differential probe inputs/results, source revisions, executable/image hashes, compiler identity, and the two diagnostic patches. Larger build trees and binaries remain under `target/audit-2026-09-21/`.

No implementation fixes, commits, pushes, oracle repinning, test expectation changes, or new ignore entries were made as part of this audit. The added files are the report and its evidence.
