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
`tests/cli_parity.rs`'s new test's program against GNU (54 forms), and
an interpreted loop of four `(ignore i)` forms runs 2,762 instructions an
iteration where it ran 4,486 (GNU 2,218), the other probes and the
interpreter loops as before.  The evaluator's helpers take the form by
value (`eval_sub', `eval_cons', `eval_cons_call' and the inlined call
helpers), which the interpreter loops' counts needed once the compiler
stopped promoting eval_sub's argument.  The independent review found no
cheat and no memory or collector fault, and one regression, fixed in
this patch: a call the debugger is to be entered at
(`debug-on-next-call') reads the function cell after the debugger, so
it takes the general call.  Not yet done: lint, the grouped gate and
the integration targets on the final patch, and CI.
