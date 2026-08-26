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
