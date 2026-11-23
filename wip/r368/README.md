# Round 368, unfinished: a native function that takes a vector is called as funcall_subr calls it

`impl.patch` is a diff of `src/` and `tests/` against round 366's source
(`git apply wip/r368/impl.patch`).  It is packaged, not applied: `src/`
and `tests/` are round 366's, and the gate builds and tests nothing
under `wip/`.

The subject is the first of M2's call paths (D125 in docs/milestones.md).
A native-compiled function with lexical binding and `&rest' is a subr
whose maximum is MANY.  The interpreter, Ffuncall, Fapply and Bcall
reached it through the general native entry (an invocation record, the
trampoline, an encoded copy of the arguments); a fixed-arity function
already took a register call.  The patch gives the MANY functions the
same call with a count and the arguments' address (`many_registers',
`call_many_registers', `invoke_nested_many', `invoke_many_registers'),
gives eval_sub its SUBRP branch for them (`eval_many_native': the minimum
checked before any argument is evaluated, the vector stopping where the
list ends), and lets native code's own funcall call them directly.

State on 23 November 2026: the release build passes
`tests/cli_parity.rs`'s new test's program against GNU (50 forms), and
an interpreted loop of four `(ignore i)` forms runs 2,877 instructions an
iteration where it ran 4,502 (GNU 2,218; builds with line tables).  Not
yet done: lint, the grouped gate, the integration targets, the review and
the round audit on the final patch.
