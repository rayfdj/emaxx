# Round 367, unfinished: `nil' and `t' are symbols to eval_sub

`impl.patch` is a diff of `src/` and `tests/` against round 366
(`git apply wip/r367/impl.patch`).  It is packaged, not applied: `src/`
and `tests/` are round 366's, and the gate builds and tests nothing
under `wip/`.

The subject is HANDOVER's finding of that name (round 366's review):
GNU's eval_sub looks any symbol form up in the lexical environment,
`nil' and `t' among them, and evaluates a fixed-arity subr's omitted
arguments as references to `nil'.  `commit-msg-draft.txt` is its
author's description of the change.

Its author was stopped before reporting.  What is known: the patch is
one local commit with a clean tree; logs of a gate run, a sweep of GNU's
test files and the integration targets exist beside it, unread; its last
recorded instruction counts (callgrind-diff, an iteration) were 4,401
for the lexical interpreted loop, 3,177 dynamic, 380 for the byte-code
call loop and 171 for `get', against round 366's 4,402, 3,180, 380 and
171.  Nothing of it has been reviewed or re-run, and whether the round
is finished or dropped is decided when the scope of the remaining work
is fixed.
