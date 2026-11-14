# Round 366, unfinished: C reads and stores of variables are dynamic

`impl.patch` is a diff of `src/` and `tests/` against round 365
(`git apply wip/r366/impl.patch`).  It is packaged, not applied: `src/`
and `tests/` are round 365's, and the gate builds and tests nothing
under `wip/`.

GNU's C reads a variable as the C variable a DEFVAR forwards to or with
Fsymbol_value, and stores with Fset: the dynamic binding.  Only the
evaluator's own reference to a symbol, and the special forms that bind
and set, look in the lexical environment.  Emaxx's primitives were
handed the caller's environment and read and stored through it, so an
entry of a special variable's name in the alist `eval' is given was
taken for the variable (HANDOVER's finding from round 364).  The patch
moves 711 runtime reads and stores to the dynamic API and removes the
lexical-first one.

Its author's checks passed (fmt and clippy under Rust 1.100.0, the
library gate's 3,530 tests, cli and cli_parity with a new test of 223
forms compared with GNU, the round audit with no hard finding).  It is
not yet reviewed independently, and the other integration targets and
CI have not run on it.
