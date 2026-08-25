# TTY differential fixtures

`fieldnotes.org` is the real 614-line notes fixture used for TTY round 33,
checked in with the user's permission.  It retains the complete interaction
shape that exposed redisplay bugs: startup folding, long folded bodies, nested
headings, tables, source blocks, tagged headings, and TODO/DONE faces.

The fieldnotes scenarios in `tools/ttydiff.py` all load this file.  When a
real editing journey exposes another bug, keep a representative shape here and
add the minimized keystroke sequence to the default differential scenario set.
