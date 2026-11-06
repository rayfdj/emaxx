# The audit every round passes before it is committed

Every round of the runtime goal is audited before its commit, and the
round's HANDOVER entry records the outcome in an **Audit** line. A round
whose audit finds a violation is not committed until the violation is
removed; a finding that is real but out of the round's scope is recorded
in HANDOVER's open findings, never left silent.

The audit has three parts. None of them certifies the absence of cheating;
together they make each round's changes answer to the rules below.

## The rules

1. **No cheating.** No special case for a test name, test file, benchmark
   shape or known input; no canned or hard-coded results; no error swallowed
   so that a failure reads as a pass; no fake capability; no silent fallback
   to another execution mode; no work omitted so that something looks
   correct or fast.
2. **No forwarding to GNU Emacs.** Runtime code never runs, reads the output
   of, or depends on the oracle executable or its results. The oracle is used
   only by the comparison harness, to produce the answers Emaxx is compared
   with.
3. **The C/Elisp boundary is GNU's.** What GNU implements in C is
   implemented natively with GNU's semantics: argument checks, error symbols
   and data, evaluation order, the Lisp objects used, the hooks run. What GNU
   implements in Lisp comes from GNU's unchanged Lisp, never a native
   reimplementation, and native code reaches a Lisp-owned function through
   its function cell, as GNU's C does. The set of primitives, symbols and C
   variables matches GNU's sources exactly (the manifest checks below).
4. **Faithfulness.** A changed mechanism matches the GNU function it names;
   each deviation is justified by a concrete Rust, ownership or platform
   constraint and stated in the round's record.
5. **No shadow state, no weakened tests.** No mirrors, registries, side
   tables or parallel object models GNU does not need; no ignores, changed
   expectations, deleted assertions, narrowed comparisons or lint
   suppressions.
6. **Rust soundness.** Lisp values held by Rust structures are traced by the
   collector and carried by image dump and restore; no unsound aliasing,
   lifetimes or thread access.

## The three parts

1. **Static checks, in every gate.** `src/anti_cheat.rs` holds 25 checks
   that run in the gate's lightweight group and at the start of every
   compatibility-harness run: no oracle shell-outs or batch-report
   delegation, no Lisp-owned definitions or fabricated Lisp-owned variable
   values in the bare runtime, no removed private Lisp state or non-GNU
   dispatch, native dispatch only to configured GNU C primitives, every
   interpreter field holding Lisp values traced, dumped and carried or
   documented, and the GNU C primitive, symbol, variable and arity
   manifests equal to a fresh regeneration from GNU's sources.
2. **The round's diff, mechanically.** `python3 tools/round_audit.py
   --base <the round's parent>` reads the round's added and removed lines,
   a deleted file's included. Hard findings (exit status 1): an oracle
   executable or ERT test name in runtime code; a test file named in
   runtime code; an ignored, should_panic or compiled-out test; a lint
   suppression (allow or expect, cfg_attr forms, crate-level forms, and
   allowances in Cargo.toml or .cargo); more tests or assertions removed
   than added (the comparison helpers such as `assert_same` counted); a
   trivially true assertion. Review findings list process spawning (exec
   and posix_spawn included), dropped results, fallback wording, new
   process- or interpreter-wide state, oracle mentions, embedded file
   contents, every removed or changed assertion, and changes to the
   test-selection, comparison and reporting infrastructure (the
   harness code in src/compat.rs and src/perf.rs, Cargo.toml and build.rs
   included) or to the audit itself, each for the review below to read.
   Its negative controls (`tools/test_round_audit.py`) run first and must
   pass. A name the scan does not see whole (split, or built at run time)
   escapes it: the review's to find.
3. **An independent adversarial review.** A reviewer that has not written
   the change reads the round's diff against these rules and against GNU's
   C and Lisp sources, with GNU and Emaxx binaries for probes, and reports
   each finding with its severity, location, the GNU source showing the
   correct behavior and a probe exposing it. The reviewer is given the code
   and the rules, not the author's conclusions.

The round's command-line comparisons (tests/cli_parity.rs) and the frozen
comparison in CI remain the evidence of behavior; the audit is the evidence
that the behavior was not obtained by breaking a rule.
