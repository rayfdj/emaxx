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

## Found during step 2 (2026-08-20)

37. **A `builtin_var_value` fallback is indistinguishable from unbound, so
    preloaded Lisp overwrites GNU's C defaults.**  `indent-tabs-mode` is
    `DEFVAR_BOOL` in indent.c:2486, initialised to 1.  simple.el's
    `define-minor-mode indent-tabs-mode' supplies no `:init-value', so in GNU
    its `defcustom' keeps the existing C value of t.  In Emaxx the C default
    lived only in the fallback table, `defvar'/`defcustom' saw the variable as
    unbound, and the minor mode set it to nil — so `align' produced spaces
    where GNU produces tabs.

    This is finding 9 seen from the other side: that table conflates "the C
    default" with "not bound at all".  The step-5 work should not merely delete
    the 98 Elisp-owned arms; the ~152 genuinely C-owned ones must become real
    bindings, or preloaded Lisp will keep silently overriding them.  Fixed for
    `indent-tabs-mode' here; the rest need the systematic pass.

## Found by artifact-form parity (2026-08-20)

Executing GNU's compiled Lisp instead of source immediately exposed defects
that source loads had hidden.  This is the parity change paying for itself.

38. **`handler-bind' never fired for an error raised inside byte-code.**
    `dispatch_handler_bindings' was called from every native-call boundary in
    eval/core.rs but from nowhere in the VM, so an error escaping compiled code
    skipped every enclosing handler-bind.  GNU runs the handlers from `signal'
    itself, so byte-code and interpreted code behave identically.  Minimal
    reproduction: a compiled function calling `handler-bind-1' around
    `(funcall 'no-such-fn)' returned `(handled void-function)' in GNU and in
    Emaxx's interpreter, but escaped in Emaxx's VM.  Consequence: ert could not
    turn a failing *compiled* test body into a result — which would have
    corrupted the compatibility measurement across the whole corpus the moment
    the subject started executing `.elc'.  Fixed by dispatching at the VM's
    boundary, after any condition-case in the frame has had its chance.

39. **A "EUC-JP encoder" that knew exactly one character.**
    `encode_euc_jp_bytes' mapped `あ' to (0xA4 0xA2) and signalled for every
    other non-ASCII character, and the encodability predicate carried
    `ch == 'あ'' special cases for both `euc-jp' and `sjis'.  `あ' is the
    character the tests use.  Same class as the thread-name table: a codec that
    knows one codepoint is a fabrication, not a partial implementation.
    Removed; EUC-JP/Shift_JIS encoding now signals honestly and is a tracked
    gap (GNU encodes `あ' as (164 162)).
40. Related, still open: Emaxx substitutes SPACE for an unencodable character
    where GNU substitutes `?'.  Probed: `(encode-coding-string "sæl ö всем"
    'ascii)' gives GNU (115 63 108 32 63 32 63 63 63 63), Emaxx
    (115 32 108 32 32 32 32 32 32 32).  The two tests covering 39 and 40 are
    quarantined with those probed values recorded, rather than rewritten to
    assert Emaxx's behaviour.

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

## Found while unblocking the loaded-ERT stage (2026-08-20)

`eval_05::loaded_ert_self_test_file_stays_green_in_the_native_runner` aborted
the whole test process with a stack overflow once `handler-bind' started firing
from the bytecode VM (finding 38), because the handler ran ert's real failure
reporter for the first time.  Bisecting that abort turned up five printing and
hashing divergences, all confirmed against the pinned oracle.

- **41** — `sxhash' recursed without bound.  `hash_value_equal'
  (`src/lisp/primitives/values.rs') walked a value's entire graph, with no
  depth or length cap and no cycle guard, so hashing a cyclic object recursed
  until the stack died.  GNU stops at `SXHASH_MAX_DEPTH' 3 and folds in at most
  `SXHASH_MAX_LEN' 7 elements per list or vector (fns.c:5336, fns.c:5341,
  `sxhash_obj' fns.c:5505).  This was not academic: `cl-print' labels a
  compiled function `#<bytecode %#x>' by calling `(sxhash object)'
  (cl-print.el:230), and a closure's constants routinely point back at the
  closure -- exactly the graph ert builds while reporting a failed test.  So
  *any* ERT failure whose backtrace contained a compiled frame aborted the
  process instead of printing.  Fixed by mirroring GNU's bounds.
- **42** — the `#N' cycle placeholder was invented.  Printing a self-referential
  record produced `#11643' (emaxx's internal record id) and every circular list
  produced `#0'.  GNU prints `#N' where N is the *print depth* the outer
  occurrence sits at (print.c:2253), and detects list cycles with Brent's
  algorithm, closing with `. #TORTOISE-INDEX' (print.c:2541, print.c:2705).
  `(1 2 . self)' therefore prints `(1 2 1 2 . #2)' in GNU, not `(1 2 . #0)'.
  Fixed; both forms now match byte-for-byte.
- **43** — hash tables were not cycle candidates.  `print_ref_key' excluded
  them, so `(let ((h (make-hash-table))) (puthash 1 h h) (prin1 h))' recursed
  until the stack died; GNU prints `#s(hash-table data (1 #0))', and
  `#1=#s(hash-table data (1 #1#))' under `print-circle'.  GNU's
  `PRINT_CIRCLE_CANDIDATE_P' (print.c:1299) includes them.  Fixed, including
  descending into table contents during `print_preprocess'.
- **44** — the hash-table print form was pre-30 and verbose.  Emaxx printed
  `#s(hash-table size 65 test eql rehash-size 1.5 rehash-threshold 0.8125 data
  ())' where GNU 30.2 prints `#s(hash-table)': the test is emitted only when it
  is not `eql', weakness only when weak, `purecopy t' only when set, and `data'
  only when the table is non-empty (print.c:2588).  Any measured test printing
  a hash table diverged on every field.  Fixed to GNU's rules, `print-length'
  truncation included.
- **45** — no `PRINT_CIRCLE' depth cap.  GNU refuses to print deeper than 200
  levels without `print-circle', signalling `(error "Apparently circular
  structure being printed")' (print.c:2249); emaxx printed a 300-deep
  structure happily.  Fixed, error message and 150-level output length
  verified identical.

Left open, disclosed rather than fixed:

- **46** — an interpreted closure prints as `#<lambda (x)>`.  GNU prints its
  readable vector form, `#[(x) ((+ x y)) ((y . 5))]`.  This is a real output
  divergence in every backtrace or `prin1' that reaches an interpreted
  closure, and it is not a cycle problem: it is emaxx's closure representation
  surfacing.  Not fixed here because it needs the closure object's slots to be
  projected the way `record_prin1_fields' projects a record's, which is a
  representation change rather than a printer change.

## Printer and startup parity, found by probing outward from finding 41

Once the printer was under the microscope, a systematic sweep against the
oracle turned up more divergences.  Each was probed on both sides before and
after the fix.

- **47** — `princ` was a separate printer.  GNU has one `print_object' that
  takes an `escapeflag', and the flag reaches nested elements, so
  `(princ (list "a"))' prints `(a)' and `(message "%s" (list "a"))' prints
  `(a)'.  Emaxx had a small `render_princ' that handled a top-level string or
  buffer and fell back to the host `Display' impl for everything else, so the
  same forms printed `("a")'.  Every `message "%s"' with a list argument -- the
  single most common shape in ERT and byte-compiler output -- diverged.  Fixed
  by giving `PrintOptions' GNU's `escape' flag and routing `princ', `%s' and
  `prin1-to-string NOESCAPE' through the shared traversal; the two bespoke
  princ renderers are deleted.
- **48** — a subr printed as `#<builtin car>'; GNU prints `#<subr car>'
  (print.c:1793).  This one leaked into every backtrace.
- **49** — a process printed as `#<record id:11643>', leaking emaxx's internal
  record id.  GNU prints `#<process NAME>', or the bare name under `princ'
  (print.c:1782).
- **50** — an obarray printed as `#<record id:N>'; GNU prints
  `#<obarray n=COUNT>' (print.c:2087).
- **51** — a bool vector printed as `#s(bool-vector t nil t)', a readable-looking
  form GNU never emits and its reader would read back as a record.  GNU packs
  the bits eight to a byte, low-order first, and writes `#&SIZE"BYTES"' with
  `octalout' escaping (print.c `print_bool_vector').  Fixed; empty, multi-byte,
  high-bit, control-character and `print-length'-truncated cases all verified
  byte-for-byte.
- **52** — `:purecopy' was parsed and dropped.  GNU still records it and
  print.c:2609 reports it back, so `(prin1 (make-hash-table :purecopy t))'
  printed `#s(hash-table)' instead of `#s(hash-table purecopy t)'.
- **53** — *the initial batch buffer was named `*test*'*.  GNU starts a batch
  session in `*scratch*', in `lisp-interaction-mode', with `buffer-list'
  ordered (*scratch* " *Minibuf-0*" *Messages*).  Emaxx started in a buffer
  literally named `*test*', in `fundamental-mode', with *Messages* ahead of the
  minibuffer buffer.  A test-shaped name in the shipped startup path is exactly
  the sort of harness artifact this audit exists to find: any measured test that
  printed the current buffer, or relied on the initial major mode, was comparing
  against a fiction.  Fixed by naming the buffer `*scratch*', ordering the list
  as GNU does, and running startup.el's own mode form (startup.el:1572) during
  batch initialization.

Still open, disclosed rather than fixed:

- **54** — a char table prints as `#<char-table id:71>'; GNU prints its readable
  `#^[...]' form.  Faithful output needs GNU's three-level char-table layout
  (ascii slot, 64-way contents, extra slots), which is a representation change,
  not a printer change.  Same class as finding 46.
- **55** — `(read "#&3\"\\5\"")' yields the list `(bool-vector-literal t nil t)'
  rather than a bool vector; the printer now emits GNU's syntax, but the reader
  still produces an evaluator literal form instead of the object.
- **56** — a thread, mutex or condition variable with no name prints as
  `#<thread 0xID>' using emaxx's own object identity where GNU prints the
  object's address.  The syntax matches and the identity is real; the number
  cannot agree with GNU's, and does not agree between two GNU runs either.

## Found by probing error messages (2026-08-20)

- **57** — `wrong-type-argument` carries a type *name* where GNU carries the
  offending *value*, and names the wrong predicate.  GNU signals
  `(wrong-type-argument PREDICATE VALUE)`:

      (+ "a" 1)          GNU (wrong-type-argument number-or-marker-p "a")
                       emaxx (wrong-type-argument number "string")
      (aref 'sym 0)      GNU (wrong-type-argument arrayp sym)
                       emaxx (wrong-type-argument list "symbol")
      (length 3)         GNU (wrong-type-argument sequencep 3)
                       emaxx (wrong-type-argument sequence "integer")

  Some paths are already right -- `(car 3)` gives `(wrong-type-argument listp
  3)` on both sides -- so this is per-call-site, not structural: there are 347
  `LispError::TypeError(EXPECTED, TYPE_NAME)` constructions outside tests, and
  each needs GNU's predicate symbol plus the value itself.

  The rendered message diverges twice over, because the value is printed with a
  host debug format rather than the Lisp printer:

      (+ (symbol-function 'car) 1)
          GNU   Wrong type argument: number-or-marker-p, #<subr car>
        emaxx   Wrong type argument: number, builtin<car>
      (+ (make-hash-table) 1)
          GNU   Wrong type argument: number-or-marker-p, #s(hash-table)
        emaxx   Wrong type argument: number, record<11649>

  `#<record id:N>` / `#<builtin NAME>` / `record<N>` / `builtin<NAME>` are the
  host `Display` impl (`src/lisp/types.rs:1689`, `:1697`) leaking into
  user-visible text; nothing in GNU ever prints those shapes.

  This is why finding 22 matters: the differential harness compares a failing
  test's condition *type* and not its data or message, so every one of these
  divergences is invisible to the score today.  Not started here -- it is a
  work item of its own, recorded in the execution plan.

## Revealed by fixing the eval_05 abort

Finding 41's stack overflow aborted the eval_05 process partway through, so
every test sorting after `loaded_ert_self_test_file_stays_green_in_the_native_runner`
had never once executed.  Four were waiting there.  None is a regression from
this round's work; all four are recorded here with what the oracle says.

- **58** — `standard_minibuffer_completion_map_is_bound` and
  `return_key_defaults_to_newline_command` asserted, against the *early* Lisp
  runtime, facts that belong to the dumped image: minibuffer.el's
  `minibuffer-local-completion-map' and the global map RET resolves through.
  `emacs -Q -batch' answers `(t t)' and `newline'; Emaxx's batch image answers
  the same.  The tests now use it.
- **59** — `preloaded_completing_read_delegates_through_the_gnu_dispatch_variable`
  used `cl-letf' in a bare batch image.  GNU does not preload cl-lib either:
  it signals `void-function cl-letf' for the identical program, and returns
  `("mocked" 8)' once `cl-lib' is required, which is what Emaxx returns too.
  The test now requires it.
- **60** — *`completion-preview` did not work* (fixed 2026-08-21).  Running
  the pinned suite on both binaries:

      GNU:   Ran 11 tests, 11 results as expected, 0 unexpected
      Emaxx: Ran 11 tests, 1 results as expected, 10 unexpected

  Root cause, found by differential bisection (source-loaded library passed,
  GNU's `.elc' failed, so the interpreted replica lied): `try-completion'
  returned a plain immutable `Value::String', and `set-text-properties' on
  such a string "mutates" it by silently rewriting the caller's environment
  binding.  Interpreted callers happen to read that rewritten binding;
  compiled callers read bytecode stack slots, which the rewrite can never
  reach, so the face `completion-preview.elc' set on the string was gone by
  the time the preview overlay was built.  The rewrite even breaks identity:

      (let* ((l (list (try-completion "foo" '("foobarbaz")))) (s (car l)))
        (set-text-properties 0 9 '(face f) s)
        (list s (car l) (eq s (car l))))
      GNU:   (#("foobarbaz" 0 9 (face f)) #("foobarbaz" 0 9 (face f)) t)
      emaxx: (#("foobarbaz" 0 9 (face f)) "foobarbaz" nil)

  Fixed by making `try-completion' return shared mutable strings, as
  `all-completions' three lines away already did; GNU probes confirm its
  return is a fresh string (not `eq' to any candidate), so this is the
  GNU-shaped representation, not a workaround.  The suite now runs 11/11 on
  both binaries and the identity probe matches byte-for-byte.  The binding
  rewrite itself remains for other plain-string producers -- that is issue
  #14, and this finding is its clearest demonstration to date.

Also noticed while probing, not yet fixed:

- **61** — `propertize' appends new properties where GNU prepends them, so the
  printed plist order differs: GNU prints `#("ab" 0 2 (keymap nil face foo))`,
  Emaxx `#("ab" 0 2 (face foo keymap nil))`.  Same properties, different
  `prin1' output, so any test comparing printed propertized strings diverges.

## The job-control failure (finding 62; supersedes an earlier misdiagnosis)

`native_subprocess_job_control_uses_child_groups_and_reaps_signal_states`
failed in every gate run and passed every foreground run -- 20/20 standalone,
plus every bisection subset, including one with the identical 176-test
predecessor sequence.  An earlier draft of this note blamed my own concurrent
triage processes; that was wrong (the next clean gate failed with nothing else
running), and the bisection wasted several hours on order-dependence that did
not exist.  The real discriminator was *how the test process was launched*.

- **62** — children inherited the shell's ignored SIGINT/SIGQUIT.  A
  background job of a non-interactive shell starts with SIGINT and SIGQUIT
  set to SIG_IGN, and SIG_IGN survives exec into every child.  Emaxx spawned
  subprocesses with Rust's `Command', which does not reset those
  dispositions, so under `nohup ... &' -- exactly how the gates run --
  `interrupt-process' sent a SIGINT that the child ignored, and the test
  timed out waiting for a death that could not happen.  Instrumentation
  showed the kill succeeding and the child alive five seconds later.  GNU
  guards against precisely this: `emacs_spawn' (callproc.c:1441) is the one
  choke point both `call-process' and `make-process' children pass through,
  and it restores SIGINT, SIGQUIT, SIGPROF -- and SIGCHLD on Darwin -- to
  SIG_DFL (callproc.c:1385) and gives every child a fresh session
  (POSIX_SPAWN_SETSID; `setsid' in the fork path, callproc.c:1289).  Emaxx
  had two independent spawn sites, and the first version of this fix patched
  only the `make-process' one, leaving `call-process' children still
  inheriting SIG_IGN -- exactly the bug-shape GNU's single-choke-point
  design makes impossible.  Both paths now share one `configure_emacs_spawn'
  mirroring GNU's: signal defaults plus `setsid' for every child (pipe
  children previously got only `setpgid'), TIOCSCTTY for PTY children.
  Verified by running the test five times as a background job (5/5 failures
  before, 5/5 passes after), by a `call-process' child self-delivering
  SIGINT under a background launch (dies on both binaries; before the fix it
  survived on Emaxx), and by pid/pgid/session probes matching GNU's shape.

The corrected process lesson: a test that fails only in the gate is not
thereby flaky or externally sabotaged; the gate's own launch context is part
of the test environment and must be reproduced when bisecting.
