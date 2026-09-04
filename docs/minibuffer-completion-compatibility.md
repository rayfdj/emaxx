# Minibuffer and completion compatibility contract

This document records the permanent evidence for issue #31.  The target is
GNU Emacs 30.2, built from the pinned source commit
`636f166cfc86aa90d63f592fd99f3fdd9ef95ebd`; it is not an Emacs 31 contract.

## Upstream suite evidence

The compatibility harness sends the unedited default-selector tests to GNU
Emacs and Emaxx and compares every pass, failure, skip, and diagnostic record.
The issue gate covers these four upstream files:

- `test/lisp/minibuffer-tests.el`: 31 outcomes.
- `test/lisp/completion-preview-tests.el`: 11 outcomes.
- `test/lisp/completion-tests.el`: 6 outcomes.
- `test/src/minibuf-tests.el`: 65 outcomes.

Together these are 113 exact oracle comparisons covering minibuffer contents,
prompt properties, recursion, history, defaults, completion tables and
boundaries, metadata and categories, completion styles, case folding,
completion-at-point, completion preview, and error behavior.  Run them
serially with the optimized subject:

```sh
LANG=C LC_ALL=C target/gate/compat-harness run \
  --scope lisp --selector default --file test/lisp/minibuffer-tests.el
LANG=C LC_ALL=C target/gate/compat-harness run \
  --scope lisp --selector default --file test/lisp/completion-preview-tests.el
LANG=C LC_ALL=C target/gate/compat-harness run \
  --scope lisp --selector default --file test/lisp/completion-tests.el
LANG=C LC_ALL=C target/gate/compat-harness run \
  --scope src --selector default --file test/src/minibuf-tests.el
```

Each invocation enforces the repository anti-cheat checks before either
subject runs and writes a fresh provenance-bearing artifact under
`target/compat/`.  The optimized profile is the repository's release-derived
`gate` profile.

## Interactive terminal evidence

Six permanent scenarios in [`tools/ttydiff.py`](../tools/ttydiff.py) drive the
same keys through independent GNU Emacs and Emaxx pseudo-terminals.  Every
named action compares exact text, terminal attributes, mode line, echo area,
cursor position, and required semantic text:

- `minibuffer-default-history` accepts a real default, records it in a named
  history variable, recalls it, and proves both returned values.
- `completion-require-match-recovery` rejects an invalid submission, permits
  correction, accepts a member of the table, and proves cleanup.
- `completion-metadata-navigation` exercises case-folded input, completion
  metadata, annotation faces, the `*Completions*` window, highlighted
  navigation, and candidate selection.
- `completion-preview-capf` installs a real completion-at-point function,
  displays and cycles completion-preview overlays, inserts the preview, and
  proves overlay cleanup.
- `recursive-minibuffer` enters a second read through `eval-expression`,
  records both depths and return values, restores the outer prompt, and proves
  the stack fully unwinds.
- `keyboard-macro-minibuffer` records and replays a keyboard macro containing
  a require-match completion command and proves that the recorded event stream
  executes exactly once per replay.

Run only this contract, serially, with:

```sh
python3 tools/ttydiff.py \
  target/gate/emaxx /path/to/emacs-30.2 /path/to/emacs-30.2/lisp \
  minibuffer-default-history completion-require-match-recovery \
  completion-metadata-navigation completion-preview-capf \
  recursive-minibuffer keyboard-macro-minibuffer
```

The scenarios use ordinary Lisp entry points, real keymaps, the live recursive
command loop, real windows and overlays, and unmodified completion packages
from the pinned Emacs tree.  They do not call editor-specific setup functions,
normalize output, retry failures, or accept two equally empty results.  Final
records prove returned values, history/depth state, buffer contents, and
minibuffer/overlay cleanup so visual agreement alone is insufficient.

## Corrected compatibility boundaries

The issue repairs behavior at shared runtime boundaries rather than adding
scenario-specific answers.  A fresh minibuffer activation now starts a fresh
grow-only sizing lifecycle; accepted string defaults enter real history;
events consumed by a recursive read are recorded once in keyboard macros;
case-folded completion preserves unextended user spelling; and ASCII case
conversion retains string properties while character-count-changing Unicode
conversion drops them like GNU Emacs.  TTY redisplay now places point before
an overlay after-string, runs the Lisp pre-redisplay coordinator, gives the
hardware cursor to the actually selected window, and preserves a transient
printed value when it owns the echo area.

The focused Rust regressions include live-oracle probes for case-folded
completion and case conversion properties.  The terminal assertions remain
in the external differential runner, not in production dispatch, and the
production paths contain no fixture, scenario, editor-identity, or oracle
branch.

## Scope boundary

This contract covers the selected GNU Emacs 30.2 default-selector corpus and
the six deterministic terminal journeys above.  It does not claim every
third-party completion UI, every completion style combination, graphical
child frames, mouse-only completion interaction, platform input method, or
Emacs 31 feature.  Those require separate same-input oracle evidence.
