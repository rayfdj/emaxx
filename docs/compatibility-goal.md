# Compatibility Goal

This file records the active long-running goal so interrupted sessions can resume
from the same concrete state.

## Objective

Make `emaxx` 100% compatible with GNU Emacs at the Elisp API boundary by passing
the harness-selected GNU Emacs compatibility tests from the sibling checkout at
`../emacs`.

The canonical ordered manifest is:

- `compat/oracle_tests_all.txt`
- `compat/oracle_tests_all.md`

The denominator is 7080 selected tests. Do not use source-tree `ert-deftest`
counts as the progress denominator.

## Current State

- Tests through 600/7080 are verified locally.
- The latest compatibility batch is
  `Compat 600/7080: expose syntax ppss cache flush`.
- The 1..378 exact selected-test prefix was replayed after the
  `primitives.rs`/`eval.rs` split, after the SRecode/Semantic fixes, and again
  after the char-fold/regexp changes; all 378 passed. The same exact 1..378
  prefix was replayed again after the Completion Preview changes in the
  457/7080 batch; all 378 passed.
- Selectors 379..396 passed as a grouped Semantic IA replay after the
  char-fold/regexp changes. Selectors 397..407 passed as individual literal
  manifest selectors because `test/lisp/cedet/semantic-utest.el` is
  order-sensitive as a grouped run.
- Selectors 408..414 in `test/lisp/char-fold-tests.el` and selector 415
  `color-tests-cie-de2000` passed individually after adding Unicode
  decomposition support for char folding and the `time-to-seconds` time
  primitive.
- Selectors 416..446 passed individually after adding `color-values` and named
  color parsing for the existing color conversion path.
- Selectors 447..457 in `test/lisp/completion-preview-tests.el` passed
  individually after adding symbol bounds, `while-no-input`, pcase `seq`
  matching, mutable completion strings, and the completion metadata helpers
  needed by Completion Preview mode.
- Selectors 458..461 in `test/lisp/completion-tests.el` passed individually
  after adding the standard backup-retention defaults needed by
  `completion.el` and correcting `setcdr` to return the new cdr value.
- Selectors 462..463 in `test/lisp/completion-tests.el` passed individually
  after correcting regexp syntax-class translation for `\s ` and `\s_`.
- Selector 464, `cus-edit-test-bug63290` in `test/lisp/cus-edit-tests.el`,
  passed after loading real `cus-edit`, adding minimal widget accessors needed
  by `wid-edit`, and accepting marker positions in overlay range primitives.
- Selectors 465..470 in `test/lisp/cus-edit-tests.el` passed after adding
  standard obarray enumeration, `defconst` reinitialization, Custom group and
  version metadata, obsolete-variable metadata, basic batch display/window
  helpers, `dolist-with-progress-reporter`, `cl-letf` support for symbol
  property places, `setopt` type warnings, and `*Warnings*` buffer recording.
- Selectors 471..473 in `test/lisp/custom-tests.el` passed after restoring the
  built-in `user`/`changed` Custom themes, adding batch-safe frame/theme helper
  primitives, and honoring `defcustom :local` including permanent locals.
- Selector 474, `custom-test-no-saved-value-after-customizing-option` in
  `test/lisp/custom-tests.el`, passed after exposing runtime keymaps through
  their Lisp keymap-list view during sequence iteration, preserving preferred
  builtin toolbar stubs across loaded Lisp `defun`s, and defining the standard
  dynamic `inhibit-read-only` variable.
- The full 1..474 selected-test prefix was replayed after the 474 fix; all 474
  passed.
- After selector 474, the requested modularization pass moved evaluator
  bootstrap/static data into `src/lisp/eval/bootstrap.rs` and primitive
  window/scroll helpers into `src/lisp/primitives/window.rs`; the full gates
  and 1..474 compatibility prefix passed before advancing.
- Selectors 475..479 in `test/lisp/custom-tests.el` passed after adding the
  standard mark-ring Custom defaults, `make-empty-file`, dynamic
  `with-temp-file` writes, explicit-target `require` provide checks, and
  source-stub `.elc` fallback for `require-theme` support files.
- Selectors 480..481 in `test/lisp/dabbrev-tests.el` passed after adding a
  batch `execute-kbd-macro` path for parsed `kbd` vectors, dabbrev key
  bindings, interactive `*P` parsing, and the `dabbrev-capf`
  `completion-at-point` path needed by dabbrev completion.
- Selectors 482..491 in `test/lisp/dabbrev-tests.el` passed after adding
  standard minibuffer/window predicates and minibuffer contents helpers,
  formatted `user-error`, command-loop state tracking for keyboard macros,
  failed `looking-at` match-data preservation, lightweight minibuffer window
  selection, multi-key keyboard macro dispatch for search/mark/narrow commands,
  MRU `buffer-list` ordering, and `.el` auto-mode selection for dabbrev's
  same-major-mode buffer filter.
- Selectors 492..495 in `test/lisp/dabbrev-tests.el` passed after marking
  unwritable visited files read-only and enforcing `buffer-read-only` during
  insertion.
- Selectors 496..504 in `test/lisp/delim-col-tests.el` passed after matching
  GNU search `NOERROR` movement semantics and adding real window parameter
  storage for the rectangle helpers used by `delim-col`.
- Selectors 505..507 in `test/lisp/descr-text-tests.el`, 508..512 in
  `test/lisp/desktop-tests.el`, and selector 513
  `dired-guess-default` passed with the same batch.
- Selector 514, `dired-test-bug27496`, passed after adding `cl-callf`,
  keyword-aware `cl-member`, and routing `read-char-choice` through
  `read-char-from-minibuffer` when appropriate.
- Selectors 515..517 in `test/lisp/dired-aux-tests.el` passed after adding
  `rename-file`, dired destination directory coverage, minimal window buffer
  history/list helpers, `file-in-directory-p`, and property-preserving
  `split-string`.
- Selectors 518..520 in `test/lisp/dired-tests.el` passed after adding
  batch-compatible `delete-other-windows`, `switch-to-buffer-other-window`,
  `read-file-name`, and page motion helpers needed by Dired buffer setup.
  These selectors were verified individually because the local grouped
  `dired-tests.el` run is order-sensitive around Dired buffer/window state.
- Selectors 521..523 in `test/lisp/dired-tests.el` passed individually
  after routing directory visits through native Dired buffers, advertising
  native Dired buffers in `dired-buffers`, using parseable ls-style Dired
  listings, refreshing current Dired buffers after directory/file writes,
  giving native Dired buffers a native revert function, adding
  `file-name-sans-versions`, preserving `ert-with-temp-directory`'s trailing
  slash, and supporting wildcard `find-file` over directory entries.
- Selector 524, `dired-test-bug27631`, passed after adding wildcard
  directory recognition, wildcard expansion for `insert-directory`, shell
  `process-file` execution from dynamic `default-directory`, and the minimal
  Dired/window helpers needed by the wildcard listing path.
- Selector 525, `dired-test-bug27940`, passed after adding standard
  `read-answer`, Dired deletion prompt, no-dot directory matcher, and dead
  buffer cleanup semantics, plus GNU-compatible optional deletion arities.
- Selector 526, `dired-test-bug27968`, passed after making Dired buffer
  refresh on `make-directory` conditional on `dired-auto-revert-buffer` and
  preserving native Dired filename/position helpers across loaded Lisp.
- Selectors 527..528 passed after adding standard logical `line-move`
  behavior and related line-move defaults needed by Dired navigation over
  hidden detail lines.
- Selectors 529..530 passed after adding the callable
  `temporary-file-directory` helper and `directory-empty-p` over the native
  filesystem directory primitives.
- Selectors 531..535 passed after making `insert-directory` report
  `dired-free-space` for the target directory via the active
  `file-system-info` binding, independent of `default-directory`.
- Selector 536, `dnd-tests-begin-drag-files`, passed after loading real
  `ert-x`, supporting mock TRAMP local copies, fixing plain-vector/string
  predicates, and filling DND selection metadata helpers. Selector 537 is next.
- Selectors 537..542 in `test/lisp/dnd-tests.el` passed after preserving
  `dolist` binding identity for string list elements, keeping the `dolist`
  binding frame stable across nested empty lexical frames, adding `framep`,
  adding the `ascii` coding alias, and making `encode-coding-string` return
  unibyte encoded data with GNU-compatible `ascii`/`iso-8859-1` substitution.
- Selectors 543..568 in `test/lisp/dom-tests.el` passed after covering
  `cl-loop` append/collect forms used by DOM traversal, `setf` places rooted at
  `nthcdr`, HTML entity escaping, and destructive `delq` list edits.
- Selector 569 in `test/lisp/edmacro-tests.el` passed with the existing
  `edmacro-parse-keys` support.
- Selector 570, `electric-layout-control-reindentation`, passed after enabling
  electric local mode backing variables, self insertion hooks, recursive
  newline hooks, electric hook ordering, and the C-style indentation needed by
  electric layout.
- Selectors 571..580 in `test/lisp/electric-tests.el` passed after adding the
  standard RET binding for `newline` and minimal cc-mode brace layout helpers
  (`c-point-syntax`/`c-brace-newlines`) used by electric layout in C-derived
  modes.
- Selectors 581..600 in `test/lisp/electric-tests.el` passed after exposing
  the standard `syntax-ppss-flush-cache` helper used by `elec-pair.el` while
  checking string/comment syntax. Selector 601,
  `electric-pair-angle-brackets-everywhere-at-point-1-in-js-mode`, is next.

## Workflow

1. Start from the next unverified ordered test in `compat/oracle_tests_all.txt`.
2. Continue forward until the first compatibility mismatch that requires a code
   fix.
3. Fix the behavior honestly in Rust. Do not hardcode test answers, delegate to
   oracle Emacs, or add compatibility shortcuts that only recognize test data.
4. Before committing for test N, run targeted regression coverage for tests
   1..N-1 that the change could affect. Broaden the coverage when the change
   touches shared evaluator, primitive, reader, buffer, process, or file I/O
   behavior.
5. Every code-change batch must pass:
   - `cargo fmt --check`
   - `cargo clippy --all-targets --all-features -- -D warnings`
   - `cargo test`
   - `git diff --check`
   - relevant `compat-harness` runs
6. Commit and push each coherent passing batch before moving on.

## Notes From The 378 Batch

- `src/lisp/eval.rs` and `src/lisp/primitives.rs` were split into modules.
- All Rust files under `src/lisp/eval*` and `src/lisp/primitives*` are below
  3000 lines after the split.
- `test/lisp/calendar/todo-mode-tests.el` is order-sensitive as a full-file
  run. For the 1..378 verification, its 42 selected tests were verified as
  individual literal ERT selectors.
- `test/lisp/calc/calc-tests.el` passed with a longer timeout than the default
  short sweep timeout.
- `test/lisp/dabbrev-tests.el` is order-sensitive as a grouped full-file run;
  verify its selected tests as individual literal selectors when replaying the
  prefix.
