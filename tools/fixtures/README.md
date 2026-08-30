# TTY differential fixtures

`fieldnotes.org` is the real 614-line notes fixture used for TTY parity testing,
checked in with the user's permission.  It retains the complete interaction
shape that exposed redisplay bugs: startup folding, long folded bodies, nested
headings, tables, source blocks, tagged headings, and TODO/DONE faces.

The fieldnotes scenarios in `tools/ttydiff.py` all load this file.  The default
gate covers startup folding, visibility cycles, faces and Occur, plus named
per-command checkpoints for TODO and priority changes, table and heading
motion, heading insertion, and narrow/widen behavior.  `tools/test_ttydiff.py`
also asserts that these scenarios remain wired to this checked-in fixture.

When a real editing journey exposes another bug, keep a representative shape
here and add the minimized keystroke sequence to the default differential
scenario set.  For broader reproducible exploration, use
`tools/ttydiff_explore.py`; it generates safe complete editing commands from a
seed and minimizes a divergent journey without dropping partial key prefixes.

Magit's fixture is generated instead of checked in because the Git object
database and index are mutable binary state.  `initialize_magit_repository` in
`tools/ttydiff.py` creates identical isolated repositories with host Git
configuration disabled, fixed identities and dates, two commits and branches,
and known staged, unstaged, and untracked files.  The repository-not-found
journey uses one shared empty directory and verifies that declining creation
leaves it empty of Git metadata.
