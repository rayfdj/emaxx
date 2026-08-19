# Honesty audit — 2026-08-18

Six independent adversarial audits of the emaxx tree, run after the de-cheat
phases 1-6 and the non-eval sweep triage.  Every finding recorded here was
re-verified by hand against the pinned GNU checkout (`../emacs`, commit
636f166cfc86aa90d63f592fd99f3fdd9ef95ebd) or its binary before being written
down; claims that could not be reproduced were dropped.

Severity key: **S1** deliberate recognition of the scoring suite; **S2**
fabricated GNU-owned data or a missing feature presented as working; **S3**
measurement integrity; **S4** disclosed-but-incomplete, or hygiene.

## S1 — oracle-conditioned production code

These are not approximations.  Production code recognises the upstream test
corpus and answers it.

1. `src/lisp/eval/threads.rs:2871-2955` — `make-thread` does not run the thread
   body.  It matches the *function names of GNU's own test file*
   (`threads-test-thread1`, `threads-test-io-switch`, `threads-test-mlock`,
   `thread-tests--thread-function`, all defined in
   `../emacs/test/src/thread-tests.el`) and replays hand-written interleavings,
   writing that file's globals (`threads-test-global`, `thread-tests-flag`)
   from Rust.  `thread_backtrace_frames` fabricates a frame named after the
   test's own function.  `thread_program_from_lambda` additionally matches
   lambda *shapes* containing the test file's literals.
2. `src/lisp/primitives/dispatch/misc_keymaps.rs:448-461` — `symbol-function`
   fabricates autoload objects, with invented docstrings, for exactly the two
   symbols `../emacs/test/src/doc-tests.el` probes (`benchmark-run`, `tetris`).
   GNU's real value names a different file and carries the genuine docstring.
3. `src/lisp/eval/loops.rs:33-53` — `same_frame_shape`, the *generic* closure
   frame-identity predicate used for every interpreted lambda, contains a
   special case for the symbol `sti`, which is the `cl-defmethod` argument in
   `../emacs/lisp/cedet/srecode/insert.el`.  GNU closure identity is structural
   and never name-based.
4. `src/lisp/eval/bindings.rs:835-839` — `ert-x.el`'s `defvar`s
   (`ert-resource-directory-format`, `-trim-left-regexp`, `-trim-right-regexp`)
   are pre-bound as if they were C DEFVARs; GNU leaves them void until
   `ert-x` loads.
5. `src/lisp/eval/bindings.rs:720-730` — `source-directory` is derived from
   `EMACS_TEST_DIRECTORY`, a variable set only by GNU's test Makefile and by
   emaxx's own harness.  GNU's is a build-time constant.

## S2 — fabricated GNU-owned data, or missing features that report success

6. `src/lisp/eval.rs:3372-3569` — `PRELOADED_LISP_INDENT`: 196
   `lisp-indent-function` properties installed by the bare host.  Verified:
   GNU's C sources contain **zero** occurrences of `lisp-indent-function`;
   every property originates in `(declare (indent N))`, `lisp-mode.el`'s `put`,
   or `loaddefs.el`'s `function-put`.  An exact 196/196 match with a running
   oracle is a copied snapshot, not a reimplementation.  Contributing cause:
   `compat/generate_dumped_autoloads.el` whitelists `put` but not
   `function-put`, so the generator silently drops 55 of them.
7. `src/lisp/eval/bootstrap.rs:20-52` — `default_mode_line_format()` is
   `../emacs/lisp/bindings.el:699` transcribed element-for-element.  GNU's C
   default is the string `"%-"` (`../emacs/src/buffer.c:4794`, with the comment
   "real setup is done in bindings.el").
8. `src/lisp/eval/bootstrap.rs:237-711` — 48 hardcoded coding systems.  GNU's C
   defines `no-conversion` and `undecided`; the rest come from
   `international/mule-conf.el` and `language/*.el`.
9. `src/lisp/eval/bindings.rs:254-895` — `builtin_var_value` gives native
   defaults to ~105 variables with no C DEFVAR (files.el, simple.el,
   minibuffer.el, isearch.el, fill.el, font-lock.el, ...).  `boundp` therefore
   answers `t` for state GNU leaves void, and a silently failed preload is
   masked.  `src/anti_cheat.rs:403` guards nine of them.
10. `src/lisp/eval/variables.rs:1513-1525` — any *unregistered* condition is
    catchable as `error`.  GNU probe: only `t` catches an undefined condition.
    This lets `ignore-errors` and `(should-error FORM)` absorb conditions emaxx
    never registered, converting failures into passes.
11. `src/lisp/primitives/dispatch/display.rs:1436-1448` — `yes-or-no-p` returns
    **t** when no input is available; GNU signals `end-of-file`.  With
    `hooks_overlays.rs:274-284`'s blanket `Err(_) => Ok(Value::T)` this makes
    `write-region` MUSTBENEW clobber a file GNU refuses to touch.
12. `src/lisp/primitives/completion.rs:1111-1160` — native `completing-read`
    invents an answer (initial input, else default, else the *first candidate*).
    GNU signals `end-of-file`.
13. Missing features reporting success: `kqueue-add-watch` never watches and
    never fails (`files_process.rs:1006`); `set-network-process-option` is
    `Ok(T)` with no validation (`files_process.rs:1713`); process output is
    never decoded with the process coding system (`processes.rs:777`).
14. `src/tty.rs:222` and `src/tty.rs:411` — disclosure comments state that the
    runtime "does not define" the prefix commands and `command-execute`, and
    native code substitutes for them.  Probe of the real image:
    `(command-execute universal-argument digit-argument negative-argument)` →
    `(t t t t)`; `batch.rs` preloads `simple.el`.  The comments are false.
15. `src/lisp/eval/loops.rs:24` — `(while)` with no arguments indexes `items[1]`
    unguarded and **panics the process**; GNU signals
    `wrong-number-of-arguments`.

## S3 — measurement integrity

16. **No tool computes the numerator.**  `AggregateReport`
    (`src/bin/compat-harness.rs:466`) records `total_files`, `matching_files`,
    `mismatching_files` — there is no test-level count anywhere.  Every
    "X/7080" or "X/7595" figure in the docs is prose derived by hand-summing
    `selected=` lines.  Only the all-or-nothing `frozen` mode proves it touched
    every named outcome.
17. **The one frozen artifact contradicts the headline claim.**
    `target/compat/frozen-7080-1786647587101484000-49062/summary.json`:
    453 files, **429 matching, 24 mismatching**, `compared_outcomes: 7080`,
    `subject_git_dirty: true` — written 26 minutes before commit `e9dac22`
    "Compat 7080/7080: finish ordered GNU compatibility".
18. **No large run was ever measured at a clean tree.**  Across 2824 summaries
    under `target/compat`, zero runs of >=100 files have a clean subject tree.
19. `EMAXX_*` environment variables leak into the subject.
    `configure_upstream_like_env` (`src/compat.rs:401`) removes a fixed list of
    `EMACS*` keys and never clears `EMAXX_*`, so `EMAXX_BYTECODE_VM`
    (execution engine) or `EMAXX_EMACS_VERSION` (reported identity) reach the
    subject, change results, and leave no trace in provenance.
20. Oracle and subject execute different artifact forms: the oracle resolves
    GNU's compiled `.elc` from the live tree, the subject reads `.el` from the
    isolated clone.  `.elc` bytes are in no fingerprint, so editing one moves
    the oracle invisibly.
21. The frozen manifest's sha256 is computed and emitted but never compared
    against a pinned constant; only three integers (515/4/7595) are enforced.
22. Failure *messages* are never compared (`src/compat.rs:914`), so any emaxx
    assertion failure matches any GNU assertion failure on the same test.
23. `compat/oracle_tests_all.md` documents `Selector: all`; the command it
    records actually used the default selector, which excludes
    `:expensive-test` and `:unstable`.  57 of the 515 files contribute zero
    outcomes.
24. `src/anti_cheat.rs` is `#[cfg(test)]` and largely a denylist of past
    incidents' literal spellings over an allow-listed file set; a rename or a
    new top-level module walks past it.  Its structural gates (manifest
    regeneration, dispatch inventory) are sound and are the model to follow.

## S4 — disclosed gaps, wrong expectations, hygiene

25. Assertions that contradict the pinned oracle (each verified): five `value<`
    large-int/float cases; `charset-priority-list`/`charset-list` (GNU: 179/203
    entries, emaxx: a 3-element list, and the two concepts aliased); `-b` is not
    a GNU option; `(length CHAR-TABLE)` off by one; `define-key` on a full
    keymap; a `message`-advice expectation justified by a false claim about GNU;
    `find-composition` in batch; `comp-el-to-eln-filename`'s version
    subdirectory; three native-comp `subrp` assertions that should use
    `subr-primitive-p`; `key-binding [127]`; `emacs-version` reporting
    `"30.2.0"` where GNU reports `"30.2"`, hidden behind a non-empty check;
    `max-lisp-eval-depth` scaled x384 with no test; `require`'s failure message
    dropping GNU's curly quotes.
26. Sweep triage deleted 27 of 103 compat_runtime tests in the commit declaring
    the module green (`4093638`).  The rationale — they asserted native
    implementations of Lisp-owned features — was sound, but several deleted
    expectations were GNU-correct (`count-lines`, `file-modes-number-to-symbolic`,
    `member-ignore-case`, `file-relative-name`) and the same commit series
    demonstrates the alternative: re-host on the full GNU image with
    `call_via_lisp`.  Re-host rather than delete.
27. Displacement inside legitimately C-owned primitives: the native minibuffer
    command loop executes four `simple.el` editing commands by ASCII code and
    ignores the keymap it was handed; the TTY loop intercepts prefix arguments
    before `key-binding`; the kbd-macro loop performs isearch natively.  These
    are on the documented backlog.
28. Unicode case data comes from Rust's own tables (Unicode 16 vs GNU's 15.1)
    with a 5-entry special-casing table against GNU's 151; `char-equal`
    truncates via `as u8` under case folding.
29. Hygiene: `generated_autoloads.rs` (4,199 lines) is dead — nothing consumes
    it and anti-cheat bans its use; `generated_builtin_arities.rs` is live but
    has no regeneration gate (its sibling manifest does); the
    `ComposedAccessor` route (`caar`..`cddddr`, subr.el names) is dead but
    invisible to the ownership test; `incf`/`decf` do not exist in GNU 30.2;
    an empty `test.elc` sits in the repo root.

## S1/S2 — introduced by this session's own work (self-audit)

30. **New fabrication, mine.** `src/lisp/primitives/case.rs:143-166` — the
    titlecase fix committed in `0fc8ca0` hardcodes 12 codepoints and justifies
    them with the comment "the only characters whose titlecase differs from
    their uppercase are the Latin digraphs".  GNU's `titlecase` uniprop table
    has **1444 entries**, 58 of which differ from upcase (the digraphs plus
    U+0131, U+017F and 46 Georgian mkhedruli letters).  Verified divergence:
    `(capitalize "აბ")` → GNU `"აბ"`, emaxx `"Ⴀბ"`.
    The table was sized to the one test slice the commit cites.
    Honest fix, and it is available today: emaxx already loads the real table —
    `(get-char-code-property ?ǆ 'titlecase)` → 453 — so consult
    `unicode-property-table-internal` as `casefiddle.c:74-85` does, including
    GNU's nil-table fallback.  The same applies to the 5-entry special-casing
    subset (GNU: 151 entries via `special-uppercase`/`-lowercase`/`-titlecase`).
31. **New silent fallback, mine.** `src/lisp/eval/treesit.rs:95-99` skips a
    non-string `user-emacs-directory`.  GNU `treesit.c:668-671` never skips: it
    expands against the symbol's value, so nil expands against
    `default-directory` and a non-string signals.  The test that motivated the
    skip should move to the full image instead.
32. **Message used as its own format string, mine.**
    `src/lisp/primitives/hooks_overlays.rs:219-231` pre-formats and passes the
    result as `message`'s only argument; GNU does
    `CALLN (Fmessage, "Error in %s (%S): %S", ...)` (`keyboard.c:1896`), so any
    `%` in prin1'd data is now reinterpreted.
33. **Blanket `inhibit-message`, mine.** `src/batch.rs:290` spans the whole
    ~600-line preload and is not restored on an early `?` return, so genuine
    Lisp warnings during reconstruction are swallowed.
34. **Commit-message overclaim, mine.** `4093638` says the deleted
    compat_runtime tests' "honest coverage is the oracle harness".  True for
    `count-lines`, `file-modes-number-to-symbolic` and the `value<`
    transcriptions; **false** for jka-compr sniffing, skeleton, `special-mode`,
    `member-ignore-case` and the `display-buffer` trio — GNU's test tree has no
    test for those, so coverage was net dropped.  Re-host them on the full
    image.
35. Latent, adjacent to this session's coding-system edit: emaxx gives
    `raw-text` `:mnemonic ?r`; GNU's is `?t`.

## Found during the fix round (2026-08-19)

36. **Keymap representation leaks through printing, while `type-of` denies it.**
    Emaxx represents keymaps as records that project list identity
    (`is_cons_value` treats a keymap record as a cons), so `keymapp` and
    `type-of` both answer as GNU does — but `prin1`/`%S` print the record:

        GNU:   (keymap (97 . ignore))        type-of => cons
        emaxx: #s(keymap nil nil (("a" ignore nil ("a"))) nil
                  (keymap (97 . ignore)))    type-of => cons

    So `type-of` reports `cons` for an object that is not one and does not
    print as one.  Any upstream test that prints a keymap — or a string
    carrying a `local-map` text property, e.g. bindings.el's
    `mode-line-buffer-identification` — diverges.  Two honest resolutions:
    represent keymaps as real cons lists (the correct fix, a deep change), or
    make the printer emit the list form the record already carries as its last
    slot, so the projection is at least consistent.  Leaving `type-of` claiming
    `cons` while printing a record is the one option that is not honest.

## Enumeration for finding 9 (2026-08-19)

`builtin_var_value` has 252 arms.  Diffed against every `DEFVAR_*` in
`../emacs/src/*.c` and `*.m`: **152 are legitimately C-owned, 98 have no C
DEFVAR at all** and therefore belong to GNU Elisp files (fill.el, files.el,
simple.el, font-lock.el, paragraphs.el, float-sup.el, minibuffer.el,
isearch.el, subr.el, ...).  On a bare host these make `boundp` answer `t` for
state GNU leaves void, and they mask a silently failed preload.  The exact
set, for the round that removes them:

    adaptive-fill-first-line-regexp adaptive-fill-mode adaptive-fill-regexp auto-compression-mode buffer-auto-revert-by-notification buffer-stale-function
    case-replace command-line-args-left command-switch-alist completion-styles completion-styles-alist current-language-environment
    custom-current-group-alist custom-file custom-versions-load-alist defun-declarations-alist delay-mode-hooks delayed-after-hook-functions
    delayed-mode-hooks delete-old-versions desktop-buffer-mode-handlers dir-locals-file directory-files-no-dot-files-regexp directory-listing-before-filename-regexp
    dired-kept-versions early-init-file emacs-build-time emacs-lisp-mode-syntax-table emacs-major-version emacs-minor-version
    eval-expression-debug-on-error file-local-variables-alist file-name-invalid-regexp filter-buffer-substring-function find-file-visit-truename find-program
    float-e float-pi font-lock-builtin-face font-lock-comment-delimiter-face font-lock-comment-face font-lock-constant-face
    font-lock-doc-face font-lock-doc-markup-face font-lock-function-name-face font-lock-keyword-face font-lock-negation-char-face font-lock-preprocessor-face
    font-lock-string-face font-lock-type-face font-lock-variable-name-face font-lock-warning-face gensym-counter grep-program
    hack-local-variables-hook ignored-local-variable-values ignored-local-variables image-load-path indent-line-function init-file-user
    insert-directory-program insert-directory-wildcard-in-dir-p kept-new-versions kept-old-versions line-move-ignore-invisible line-move-visual
    lisp-mode-syntax-table macroexpand-all-environment mail-host-address menu-bar-separator minor-mode-alist mode-require-final-newline
    mounted-file-systems non-essential null-device page-delimiter password-colon-equivalents password-word-equivalents
    prog-mode-syntax-table read-file-name-completion-ignore-case regexp-unmatchable remote-file-name-inhibit-cache require-final-newline revert-buffer-function
    safe-local-variable-values search-default-mode sentence-end sentence-end-double-space shell-command-switch site-run-file
    tab-stop-list text-mode-syntax-table this-single-command-keys tramp-mode use-hard-newlines user-mail-address
    version-control window-display-table

Note `this-single-command-keys` is a *function* in GNU (keyboard.c) with no
variable cell at all.

## Staging for finding 9 (2026-08-19)

Of the 98 arms with no C DEFVAR, only 19 are read anywhere in Emaxx's own
native code; the other 79 exist solely to answer `boundp'/`symbol-value' on a
bare host and can be deleted outright.  The 19 needing individual judgement:

    case-replace command-line-args-left command-switch-alist
    completion-styles completion-styles-alist custom-file
    delay-mode-hooks delayed-after-hook-functions delayed-mode-hooks
    desktop-buffer-mode-handlers find-program gensym-counter grep-program
    null-device require-final-newline shell-command-switch
    this-single-command-keys tramp-mode window-display-table

For each, the native reader must either tolerate the variable being void (as
it is in GNU before the owning file loads) or be shown to have a genuine C
owner this diff missed.  `this-single-command-keys' is the clearest case: GNU
has no variable of that name at all, only the keyboard.c function.

## Fix log (2026-08-19)

Applied and verified byte-identical against the pinned oracle:

- **30 (mine)** — the hardcoded titlecase subset AND the pre-existing 5-entry
  special-casing table are replaced by lookups into GNU's real uniprop tables
  through a `CasingContext` prepared once per operation, mirroring
  `casefiddle.c:70-85` including its nil-table fallback.  All nine hard cases
  now match GNU: Georgian, U+0131, U+017F, the digraphs, `ß`, `ﬁ`, final sigma.
- **1, 2, 3** — all three oracle-conditioning sites deleted: the name-keyed
  thread table and the entire canned-interleaving machinery (nine
  `ThreadProgram` variants, their driver arms, the fabricated backtrace frame,
  three dead helpers); the `sti` special case; the fabricated
  `tetris`/`benchmark-run` autoloads.  Deleting the last of these made Emaxx
  *more* correct — `(symbol-function 'tetris)` now returns GNU's real autoload,
  which the fake had been shadowing.
- **4, 5** — `ert-x.el`'s variables are no longer pre-bound (both report
  unbound, as GNU does); `source-directory` is derived from the pinned
  checkout, not `EMACS_TEST_DIRECTORY`, and matches GNU exactly.
- **6** — `PRELOADED_LISP_INDENT` deleted (209 lines).  All probed properties
  still match GNU, because they always came from GNU's own `declare` forms,
  `lisp-mode.el` and `loaddefs.el`.
- **10** — an unregistered condition is no longer catchable as `error`; only
  `t` catches it, as GNU does.  This exposed that Emaxx signals conditions it
  never registers, so `native-lisp-load-failed` (comp.c) and all **ten**
  treesit conditions are now registered with GNU's exact
  `error-conditions`/`error-message`, read from the oracle.
- **15** — `(while)` signals `wrong-number-of-arguments` instead of panicking.
- **31 (mine)** — the treesit silent skip is reverted to GNU's propagation.
  That exposed a real startup gap: `user-emacs-directory` was nil because
  subr.el's `defvar` sets nil deliberately and Emaxx never ran startup.el's
  `command-line`.  Emaxx now evaluates GNU's own two startup forms and reports
  `"~/.emacs.d/"` identically.
- **32, 33, 35 (mine)** — `safe_run_hooks` passes GNU's format string and
  arguments to `message`; `inhibit-message` is restored even when
  reconstruction fails; `raw-text`'s mnemonic is `?t`.

Prepared next, with evidence gathered:

- **7** — `default_mode_line_format()` is redundant as well as transcribed:
  `bindings.el` already sets the value in the reconstructed image (both sides
  print the same list).  Seed C's `"%-"` and delete the transcription and its
  `standard-value` put.
- **8** — the 48-entry coding table is *additive and wrong*: Emaxx lists 277
  coding systems to GNU's 271, inventing eight (`big5 dos euc-jp mac sjis unix
  utf8 utf-8-emacs`, which GNU treats as aliases rather than systems) and
  missing two (`utf-8-hfs`, `utf-8-nfd`).  Deleting it should yield GNU's exact
  set.
- **16, 19, 20, 21** — the harness work: tally matched/mismatched *outcomes*
  into `ComparisonReport`/`AggregateReport` so a numerator exists in
  `summary.json`; clear all `EMAXX_*` from the subject environment and record
  what remains; fingerprint the live tree's `lisp/**/*.elc` (the bytes the
  oracle actually executes); pin the frozen manifest by sha256 constant.

## What the audits confirmed as sound

- All nine `.el` files in the repo are infrastructure — the ERT reporter is
  loaded identically by oracle and subject, plus three generators, two perf
  runners, three benchmark kernels.  **None defines a name GNU owns.**  The old
  `faces_compat.el` / `simple_compat.el` facades are gone and their contents did
  not migrate into Rust.  No project-local Lisp load path; no runtime `.el`
  writes.
- All three `generated_*.rs` files regenerate byte-identically from their
  checked-in generators, with zero hand-edited entries.
- The C-primitive boundary is structurally enforced: a name is callable only if
  a dispatch module exists **and** the regenerated GNU manifest contains it;
  the dispatch inventory is macro-derived from the same match arms it audits;
  `prefer_override` is down to one legitimately C-owned name (`cl-type-of`).
- The GNU-contract test helpers really spawn the pinned binary and compare
  byte-for-byte.  A ~70-expectation sample across all suites matched GNU
  exactly, including many that looked like fabrications.
- The comparison core is strict: file status, discovered and selected sets,
  per-test status and condition type must all agree; a timeout on *either* side
  fails; expected failures and skips are never counted as passes; run inputs are
  re-fingerprinted before `summary.json` is written.
- The bytecode VM is clean: undefined opcodes error at decode time and the VM
  propagates rather than degrading to `eval`.
- `src/tty.rs:795` is the model of the right pattern: it paints
  `[mode-line render error: ...]` instead of a GNU-shaped fabrication, precisely
  because a fabrication would feed the differential tool.
