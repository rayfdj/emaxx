# Findings index

Quick reference; each number links to its full entry below (search "**N**"
or the section named).  Status: FIXED, DISCLOSED (real divergence,
documented not faked), SCHEDULED (in the execution plan), OPEN QUESTION.

| # | One line | Status |
|---|---|---|
| 1-8 | Original audit S1: oracle-conditioned code (mode-line %-, coding table, ...) | FIXED |
| 9 | builtin_var_value fabrication table (251 entries) | FIXED (step 5b; dismantled) |
| 10-13 | Fabricated success: yes-or-no-p auto-t, completing-read invention, kqueue no-op | FIXED (step 5a) |
| 14 | tty.rs native prefix machinery + false disclosure comment | FIXED (5c: tty merge removed the comments; kbd-macro/minibuffer loops now dispatch prefix keys and isearch through the keymaps) |
| 15-21 | Measurement integrity: numerator, env leaks, fingerprints, manifest pin | FIXED |
| 22 | Comparison ignored failure messages | FIXED (step 4) |
| 23 | Selector documentation conflated scope with selector | FIXED (step 4) |
| 24 | Anti-cheat gates skippable (#[cfg(test)] only) | FIXED (step 4) |
| 25-35 | Fix-round and step-2 items (load-path, -L, user-emacs-directory, coding systems) | FIXED |
| 36 | Keymap type-of/prin1 disagreement | FIXED (5c: prin1 prints the public list view) |
| 37 | C-owned variables invisible to defvar | FIXED (step 5b + finding 69) |
| 38 | handler-bind never fired from the VM | FIXED |
| 39-40 | eval_04 quarantines (documented) | DISCLOSED |
| 41 | sxhash unbounded recursion (ert reporter abort) | FIXED |
| 42-45 | Printer: cycle placeholders, Brent, hash tables, PRINT_CIRCLE cap | FIXED |
| 46 | Interpreted closure prints #<lambda>, not #[...] | DISCLOSED |
| 47-53 | princ/escapeflag unification, subr/process/obarray/bool-vector printing, *scratch* startup | FIXED |
| 54 | Char table prints #<char-table>, not #^[...] | DISCLOSED |
| 55 | #& read as marker cons, not bool vector | FIXED (step 3) |
| 56 | Unnamed thread/mutex/condvar print emaxx identity, not GNU address | DISCLOSED |
| 57 | wrong-type-argument carried type name, not predicate+value | FIXED (waves 1-2; 104 sites remain, instrument-driven) |
| 58-59 | eval_05 tests pinned early-runtime facts / missing cl-lib | FIXED |
| 60 | completion-preview 1/11 (binding-rewrite invisible to VM slots) | FIXED (try-completion shared strings) |
| 61 | propertize property order differs in prin1 | DISCLOSED |
| 62 | Children inherited shell's ignored SIGINT/SIGQUIT (emacs_spawn) | FIXED |
| 63 | Subject image rebuilt from elc-less test checkout | FIXED (step 3) |
| 64 | call_named_function fabricated success (write-region clobber) | FIXED (Round A) |
| 65 | emacs-version wrong + identity env knobs | FIXED (Round A) |
| 66 | this-single-command-keys phantom variable | FIXED (Round A) |
| 67 | Dispatch gate blind to super::call (4 mode-line escapes) | FIXED (Round A) |
| 68 | Default-stack SIGABRT; oracle-build-specific gate | FIXED (Round A) |
| 69 | DEFVAR completeness: 229 oracle-bound names void | FIXED (218 seeded, 11 disclosed; dump-frozen values OPEN QUESTION) |
| 70 | Four *-consed counters frozen despite "zeroed" disclosure | FIXED (all seven zeroed) |
| 71 | find-composition string surface retired under false claim | DISCLOSED (no measured test exercises it) |
| 72 | Oracle binary pinned by self-report only | FIXED (lock pins binary sha256) |
| 73 | EMACSNATIVELOADPATH not stripped from children | FIXED |
| 74 | Frozen mode accepted --subject-root / dirty tree | FIXED (both refused) |
| 75 | Anti-cheat blind-spot catalogue (token splitting, unscanned files) | DISCLOSED |
| 76 | Probe-found runtime gaps (ppss elt-2/10, charset text prop, WTA tail) | DISCLOSED |
| 77 | comp-abi-hash/version-dir/pdumper-fingerprint copied the oracle's build identity | FIXED (comp vars void per no-native-comp model; fingerprint computed from this binary) |
| 78 | Profiler faked started-state and returned a print-mimic "#<hash-table>" string | FIXED (real state, real empty hash tables) |
| 79 | set-network-process-option fabricated success | FIXED (processp signal + network check) |
| 80 | command-error-default-function swallowed GNU's print-and-exit contract | FIXED (stderr + kill-emacs -1 in batch) |
| 81 | HOSTNAME/COMPUTERNAME/EMAXX_USER_FULL_NAME identity knobs | FIXED (removed; gethostname/$NAME only) |
| 82 | eq/eql/equal compared floats with IEEE ==, not GNU's representation equality | FIXED (to_bits at five sites; NaN self-eq restored, signed zeros distinct; boxed-float eq identity remains approximated) |
| 83 | Interpreted (+ FLOAT) seeded its accumulator with 0.0, losing the zero sign | FIXED (accumulate from the first argument, data.c arith_driver) |
| 84 | Cooperative thread model cannot suspend a thread mid-body | DISCLOSED (deadlock signals instead of spinning; scheduler no longer re-steps the active thread) |
| 85 | Batch reconstruction skipped startup.el's tty-color registration | FIXED (runs GNU's own tty-register-default-colors) |
| 86 | color-gray-p/color-supported-p/color-distance/color-values-from-color-spec bypass GNU's Lisp color path | OPEN (5-name Rust table; color-distance metric, list args and METRIC argument all diverge) |
| 87 | `\u{2620}` hardcoded into the word class to satisfy one upstream test | FIXED (removed; word/space now resolve through the syntax table everywhere) |
| 88 | `[[:space:]]` was a fixed Unicode property, not the whitespace syntax class | FIXED (regex-emacs.c:151) |
| 89 | `[:punct:]` still syntax-blind for non-ASCII | OPEN (disclosed) |
| 90 | text-quoting-style ignored the locale, so every quoted message diverged under the harness's LANG=C | FIXED |
| 91 | interactive-form/commandp missed compiled OClosures (advised functions) | FIXED |
| 92 | message, void-function/void-variable messages ignore text-quoting-style | OPEN (disclosed) |
| 93 | require's load-path branch names the feature where GNU names the resolved file | OPEN (disclosed) |
| 94 | harness let LC_ALL/LC_CTYPE override its own LANG=C, retiring the grave path from measurement | FIXED |
| 95 | default_to_grave_quoting_style's standard-display-table branch unimplemented | OPEN (disclosed) |
| 96 | no DEFVAR_BOOL coercion: bool-typed variables read back the raw value | OPEN (disclosed) |
| 97 | commandp returns t where GNU signals on an interactive-form property | OPEN (disclosed) |
| 98 | the 7595 denominator excluded 3 files dropped by a 20s inventory cap 9x tighter than the run's own 180s default | FIXED 2026-08-26 - regenerated to 7883 |
| 99 | make-thread body classifier pattern-matches three lambda shapes instead of running the body | OPEN |
| 100 | GnuTLS digest catalogue transcribed from the oracle while cipher/mac lists are queried live | OPEN |
| 101 | operating-system-release hardcodes this host's uname -r | OPEN |
| 102 | data-directory family derived from EMACS_TEST_DIRECTORY | OPEN |
| 103 | set-network-process-option fabricates success and never reads the option | OPEN |
| 104 | get-unused-iso-final-char returns a constant and swallows validation | OPEN |
| 105 | max-lisp-eval-depth ignored: let-bindings invisible, excessive-lisp-nesting never raised | OPEN |
| 106 | decode-coding-string falls back to identity for every unimplemented system | OPEN (deflating) |
| 107 | decode-sjis-char/encode-sjis-char implement exactly one probe value | OPEN |
| 108 | file-name-case-insensitive-p constant nil makes a self-comparing test pass trivially | OPEN |
| 109 | native keymap dispatch branches on add-keymap-witness, a symbol private to subr.el | OPEN |
| 110 | garbage-collect returns a correctly-shaped alist with every count fabricated as 0 | OPEN |
| 111 | network-interface-info is a bare nil beside a real network-interface-list | OPEN |
| 112 | intern-soft guesses interned-ness from value/function/plist cells | OPEN |
| 113 | the unit gate never ran under LANG=C, hiding a class of locale/coding divergence from the environment actually measured | OPEN (5 tests red) |
| 114 | a runner killed after writing its report still contributed every matching outcome to the headline numerator | FIXED |
| 115 | the frozen manifest has no fresh-regeneration gate, unlike the C and arities manifests | OPEN (disclosed) |

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
    [Step 6, 2026-08-23: fixed with oracle probes -- charset registration now
    maintains `charset-list' (203 entries, aliases prepended) and the ordered
    priority list (179, supplementary charsets after the rest) exactly as
    charset.c does, byte-identical to the oracle; `(length CHAR-TABLE)' is
    MAX_CHAR (4194303); `-b' is rejected as GNU rejects it (exit 255 path,
    cli test flipped); the three native-comp `subrp' assertions plus
    `function-get'/`remove-overlays' use `subr-primitive-p'; the invented
    five `value<` fixnum/float orderings moved to the unordered test and
    `value<` now implements fns.c value_cmp's numeric rules (double
    promotion for fixnum-vs-float, sign-only for fixnum-vs-bignum, exact
    mpz_cmp_d for float-vs-bignum) separately from exact arithcompare;
    `define-key' on a full keymap keeps single characters in the char-table
    only, so the public list carries no assoc pair (compat_01 expectation
    corrected); `find-composition' no longer fabricates automatic
    compositions from Rust grapheme clusters -- batch GNU reports nil and so
    does emaxx now; `require' quotes the feature with curly quotes; the
    `[127]`/`emacs-version`/`comp-el-to-eln-filename`/`define-key`
    range/`message`-advice items were re-probed and already agree with the
    oracle.  STILL OPEN: `max-lisp-eval-depth` -- GNU signals
    excessive-lisp-nesting at depth 1592/1600 in under a second; emaxx's
    x384-scaled check did not fire after minutes of the same probe (the
    self-call recursion path appears not to advance the guarded counter).
    Needs its own pass at the evaluator's depth accounting.  The five
    deleted compat_runtime re-hosts (finding 34) also remain.]
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
    [RESOLVED 2026-08-23, step 5c: the tty merge rebuilt the TTY loop on
    `key-binding`/`command-execute`; the kbd-macro and unread-events loops
    dropped their C-s isearch simulation and hardcoded C-u/digit/negative
    interception (oracle probes: a rebound C-u must run the rebinding, and
    `C-s Ind ESC` must trace isearch-forward + isearch-printing-char x3 --
    both now match); the kbd-macro minibuffer reader resolves every key
    through the active keymaps and dispatches the real commands inside a
    native `catch 'exit` boundary mirroring read_minibuf (oracle probes:
    `M-: 2 RET` traces read--expression-try-read, not a hardcoded
    exit-minibuffer; C-a/C-e/C-k/DEL edits and kill-line's end-of-buffer
    signal at eob all match GNU).]
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

    [RESOLVED 2026-08-23, step 7: generated_autoloads.rs and its generator
    generate_dumped_autoloads.el deleted (the runtime executes the real
    loaddefs.el, so improving the generator's function-put handling was
    moot); ComposedAccessor route deleted -- probes confirm caar..cddddr
    resolve through the preloaded subr.el definitions; incf/decf name
    special-case deleted, both now void exactly as in GNU 30.2;
    generated_builtin_arities.rs gained a byte-identity regeneration gate
    mirroring the C manifest's, registered in enforce_all (14 gates).]
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
    [RESOLVED 2026-08-23, step 7: all five re-hosted on the dumped image with
    oracle-probed expectations (including display-buffer's action-function
    return contract, which the old facade tests had wrong: a non-window
    truthy return makes display-buffer return nil, and a nil return falls
    through to the default actions).]
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

## Found by the step-3 smoke run (2026-08-21)

The first 13-file harness run after the artifact-form parity work scored
0/145 and earned its keep immediately:

- **63** — the subject rebuilt its image from the wrong tree.
  `installation_lisp_load_path' consulted `EMACS_TEST_DIRECTORY' first, and
  under the harness that names the isolated *test* checkout -- a fresh
  `git clone --shared' + `clean -ffdqx' tree with no compiled Lisp at all.
  The subject therefore reconstructed its dumped image from source `.el'
  while the oracle executes the pinned tree's 1,621 `.elc' -- the precise
  violation the artifact-form work exists to prevent -- and in fact the
  source-tree reconstruction failed outright (eager macro-expansion failure
  preloading elisp-mode), so every file scored `load_error'.  Image
  reconstruction is now anchored to `EMAXX_DUMP_SOURCE_DIRECTORY' (the tree
  the harness pins, with the oracle's own `.elc'), falling back to the
  pinned sibling; `EMACS_TEST_DIRECTORY' can never again choose the bytes
  the image is built from.

With 63 fixed the run scored 141/145 (the four misses being finding 55's
bool-vector literal in ansi-color.el, fixed above), and then

    TESTS 145/145 matching (0 mismatching) across 13 files

This is a plumbing smoke result, not a baseline: 13 files of 515.

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
  *Resolved 2026-08-21*: the step-3 smoke run promoted this disclosure into a
  measured failure -- ansi-color.el:974 has a literal `#&8"\0"' argument, so
  all four ansi-color-tests selectors signaled `(wrong-type-argument
  bool-vector "cons")' where GNU passes.  `#&' now reads as a
  `ReaderForm::BoolVector' materialized at the same read/evaluation boundary
  as `#s(...)' records, so quoted structure, bytecode constants and `read'
  itself all hand Lisp the object; the `bool-vector-literal' evaluator arm is
  deleted.  All three probes match GNU byte-for-byte.
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

## Round A: the second audit (2026-08-21)

An independent audit (credited throughout as "the second audit") reviewed the
tree after commit a48b84b.  Most of its findings reproduced entries already
recorded above and scheduled in the execution plan -- independent convergence
that the ledger matches the code.  Five items were new or sharper, each
verified against the oracle before fixing:

- **64** — `call_named_function' answered a *missing function* with
  `Ok(t)` -- fifteen call sites of fabricated success.  The sharpest
  consequence: `write-region's MUSTBENEW prompt treated "the asker is
  missing" as "the user said yes" and overwrote files GNU refuses to touch.
  GNU in batch prompts and signals `(end-of-file "Error reading from
  stdin")`, leaving the file alone; Emaxx now does exactly that,
  byte-identical prompt included (the prompt text was also wrong:
  "exists; overwrite?" vs fileio.c's "already exists; overwrite anyway?").
  A missing function now signals void-function, as GNU's call1 would.
- **65** — reported identity was both wrong and configurable.
  `emacs-version' answered "30.2.0" (the crate's three-component semver
  leaking through) where GNU says "30.2", behind a test that only checked
  non-emptiness; and EMAXX_EMACS_VERSION / EMAXX_SYSTEM_CONFIGURATION*
  environment knobs let a caller change the runtime's reported identity.
  The knobs are deleted from the runtime (the harness's EMAXX_* strip
  remains as defense in depth), the version is the GNU release constant,
  `system-configuration' uses config.guess's aarch64 spelling, and the
  test now asserts "30.2|30|2" exactly.
- **66** — `this-single-command-keys' existed as a *variable*.  In GNU it
  is only a keyboard.c function; `(boundp 'this-single-command-keys)' is
  nil.  Emaxx defined a fabricated variable (written by the interactive
  engine, exposed by the builtin fallback table) that no reader consumed
  -- the function already reads native keyboard state.  Removed; both
  binaries now answer `(nil t [])' for boundp/fboundp/call.
- **67** — the direct-native-dispatch gate only recognized the spelling
  `primitives::call(`, so the dispatch modules' `super::call(` alias was
  a blind spot.  Widening the pattern to every `call(interp, "...")`
  spelling caught four Lisp-owned names natively dispatched in the
  mode-line renderer: `buffer-narrowed-p' (replaced by xdisp.c:28812's
  pure accessibility checks, which also fixes ignoring end-narrowing),
  `file-remote-p' (now through the function cell, as xdisp.c:28909's
  dsafe_call1 does), `coding-system-eol-type-mnemonic' and
  `coding-system-mnemonic' (now computed from the C-owned
  `coding-system-eol-type' / `coding-system-plist' exactly as
  decode_mode_spec_coding reads the attribute vector).
- **68** — operational traps: `describe_char...` overflowed libtest's
  default 8 MiB stack and SIGABRTed the whole binary unless
  RUST_MIN_STACK was exported (now: the test carries run_with_large_stack
  AND .cargo/config.toml bakes the gate's stack size in, so a plain
  `cargo test` cannot abort); and the manifest-regeneration gate was
  silently oracle-build-specific (now: docs/oracle-build-contract.md
  states the pinned Darwin NS contract, and the gate detects an
  out-of-contract oracle and names the document instead of dumping a raw
  diff).

Also confirmed from the second audit's first list, scheduled rather than
fixed here: the builtin_var_value table's fabricated-defaults count is 100
(not 98) of 251; the native isearch/prefix/minibuffer approximations in the
kbd-macro engine await verification in step 5.

## Step 4: the measuring instrument (2026-08-22)

- **Finding 22 fixed** — failing outcomes now match only when their
  *messages* match, not merely their condition types.
  `compare_reports_normalized' takes a caller-supplied normalizer whose
  sole legitimate use is erasing environmental variance (each runner's
  isolated checkout root, the temp directory); the harness passes exactly
  that and nothing more.  A unit test pins that two `wrong-type-argument'
  failures with different data no longer count as matching.  The 13-file
  smoke prefix still scores 145/145 under the stricter instrument -- its
  tests pass on both sides, and passing outcomes carry no messages -- so
  the strictness will bite where it should: files with shared failures,
  starting with finding 57's entire class, which was invisible before.
- **Finding 24 fixed** — the anti-cheat gates are no longer only
  `#[cfg(test)]' tests someone remembers to run.  The module is compiled
  into the library, each gate is a callable check with a thin test
  wrapper, and `compat-harness run'/`frozen' execute every gate before
  producing any artifact: a tree that fails a gate cannot produce a
  summary at all.
- **Finding 23 fixed** — `compat/oracle_tests_all.md' said
  "Selector: `all'", conflating the `--scope all' flag with the ERT
  selector.  The manifest's own rows prove the 7,595 denominator was
  always the pinned default selector's selection (autorevert:
  discovered=16 selected=7); the document now says so.

## Step 5a: input and process fabrications (2026-08-22)

- **Finding 11 fixed** — `yes-or-no-p' answered t whenever no unread event
  supplied a `y' or `n': silence was consent.  It now implements
  fns.c:3521 -- `use-short-answers' honored through the function cell,
  `yes-or-no-prompt' appended, and a real `read-from-minibuffer' loop with
  GNU's ding/"Please answer yes or no."/sleep-for retry.  Five probe
  branches (EOF signal, yes, no, retry, short-answers) are byte-identical.
- **Finding 12 fixed** — the native `completing-read' fallback invented
  answers: initial input, then DEF, then *the first candidate*, then "".
  The measured CLI never reached it (minibuffer.el's
  `completing-read-function' owns the path there, and probes match GNU on
  EOF/piped/default cases), but the bare runtime returned fabricated
  values, and two lib tests pinned them -- one asserted the
  first-candidate invention outright.  The chain is now a real
  `read-from-minibuffer' call (DEF applies only when a real read submits
  empty input), and the tests type their input through
  `unread-command-events'.
- **Finding 13 sharpened and fixed** — `kqueue-add-watch' accepted any
  path and returned a live descriptor; kqueue.c signals file-missing
  first, and now Emaxx does, byte-identical.  The second audit's "watches
  nothing" was half right: watches DO fire for Emaxx-initiated file
  operations (seven generation sites; that model is what let
  autorevert-tests match the oracle 145/145), but kernel-level *external*
  changes are not observed -- that remains a disclosed architectural
  divergence, not faked.
- **Process output decoding fixed** — output bytes went through
  `String::from_utf8_lossy' unconditionally: the process's decoding
  coding system was never consulted, invalid UTF-8 became U+FFFD, and
  `:coding SYMBOL' (process.c's one-system-for-both-directions form) was
  rejected outright.  Output now decodes with the process's own coding
  system; `binary' yields GNU's unibyte raw bytes
  (`(99 97 102 195 169 10) nil` on both binaries), the default path is
  unchanged and still byte-identical.

## Step 5b: the variable table (2026-08-22)

Finding 9's fabrication surface is dismantled.  `builtin_var_value' held
251 entries; the audit's classification (verified name-by-name against the
pinned checkout's DEFVAR_* declarations) split them 152 C-owned / 99
Lisp-owned (plus nil/t):

- **96 Lisp-owned arms deleted** outright, with their dead support code
  (the hand-written ls-listing regexp, the version-component parser).  The
  bare runtime now reports these unbound, exactly like GNU before its Lisp
  loads; the batch image gets every one honestly from the files that own
  them.  The remaining Lisp-owned pair (`emacs-lisp-mode-syntax-table' et
  al.) went with them.
- **136 statically-valued C-owned names are now real bindings** installed
  at interpreter construction (finding 37 made general): `defvar' and
  `defcustom' see them bound, as they see GNU's C state.  A 152-variable
  mass comparison against the oracle shows zero regressions and zero
  movement -- the 34 names that differ, differed before, and are the known
  backlog (charset inventory, coding aliases, environmental timestamps).
- **16 C-owned names stay computed lookups** because they mirror live C
  state (buffer-locals, charset/coding registries, load bookkeeping,
  process-environment); freezing `load-path' at construction, for one,
  broke image reconstruction outright.

Discovery fallout: 25 tests.  Nineteen pinned deleted fabrications with
bare-runtime reads; each migrated to the batch image after probing GNU for
the very expression asserted -- all 17 probed expressions matched
byte-for-byte, including two new parities the fallback had been hiding
(`custom-versions-load-alist' is void in GNU batch too; `with-temp-buffer'
`indent-line-function' is `indent-relative' on both).  One pinned an
internal char-table id and became behavioral.  Four asserted silent
success where GNU *prompts*: `save-buffer' on a write-protected file and
bare `revert-buffer' both ask "(yes or no)" and, in batch, signal
end-of-file -- probed byte-identical on both binaries now that finding
11's auto-t is gone.  Those tests now assert the real contract.

## Finding 57, first wave (2026-08-22)

`wrong-type-argument' now carries what GNU carries.  A new
`WrongTypeArgument(predicate, value)' error variant holds the predicate
symbol the failed check names and the offending value itself, so
condition data matches structurally and messages render the value through
the real printer -- the host `Display' shapes (`#<record id:N>',
`builtin<car>') are gone from user-visible errors.  Migrated in this
wave: the core Value accessors, the arithmetic coercion funnel, and 109
mechanically-convertible sites, each family verified against an oracle
probe battery (arith, nth, aref, substring, symbol-name, string-match,
length, elt, car/setcar) that now matches byte-for-byte.  Two semantic
divergences surfaced beyond message text: `aref' silently indexed plain
lists where GNU signals `(wrong-type-argument arrayp ...)', and
`substring' named `stringp' where editfns.c checks `arrayp'.  223
complex-expression sites still construct the old value-less form; they
are honest but incomplete, and the message-level instrument (finding 22)
will surface each as it matters.  Fallout across the 2,094-test suite:
one test, which pattern-matched the old variant.

## Finding 69: the DEFVAR completeness question (2026-08-22)

The tty audit found 63 C-owned DEFVARs void in Emaxx and traced real tty
breakage to one of them (line-move reads `scroll-conservatively' on every
interactive motion; batch takes the noninteractive branch, so every gate
was structurally blind to the gap).  Verification here showed the gap was
never a step-5 regression -- git grep finds none of those names at any
commit; the fallback table simply never had them, and step 5b classified
only what the table contained.  The missed question was completeness:
enumerating the pinned checkout's 874 DEFVAR_* declarations against the
batch image found **229 oracle-bound names void in Emaxx**.

Resolution in this round: her 63 (cherry-picked; three value corrections
-- gc-cons-percentage and undo-outer-limit carry *dump-frozen* loadup
state (1.0, nil) rather than their C initializers, x-use-underline-
position-properties is C-false), a 153-name scalar tranche seeded from
the pinned dump's own post-load values, five portable list values, and
GNU's startup.el:1453 bar-mode clearing replayed in batch.  End state:
218 of 229 seeded and mass-verified byte-identical; three allocation
counters bound but zeroed (live telemetry Emaxx does not fabricate --
the first draft froze the oracle's own counter snapshots and was
corrected); eleven stay void by disclosure -- the native-comp comp-*
tables, comp-subr-list, terminal-frame and the redisplay cause tables
carry the NS build's filesystem paths and live object state.

The dump-frozen value class is an open question worth recording: the
pinned dump observably carries values (gc-cons-percentage 1.0,
undo-outer-limit nil) that neither the C initializers nor any Lisp file
on disk produce; pdumper snapshots loadup-time state.  Emaxx mirrors the
observable artifact.

## Full-codebase pre-7595 audit (2026-08-23)

Three parallel adversarial audits before the step-8 measurement: the
measurement pipeline, the d95b13e..HEAD diffs (51 seeded DEFVAR values,
both charset lists, all step-6 expectation flips, the five finding-34
re-hosts, and the keymap/prefix/isearch rework all re-probed against the
live oracle and confirmed), and the runtime at large.  Findings and
dispositions:

70. **Four `*-consed` counters were frozen oracle snapshots despite the
    "zeroed" disclosure** (floats-consed 350, intervals-consed 42,
    symbols-consed 18102, vector-cells-consed 990381 -- live telemetry in
    GNU, matching no current oracle run).  The tranche comment, commit
    131b843, and finding 69's note all said the counters were zeroed; only
    three of seven were.  FIXED: all seven now start at zero.  No test
    read any of them.
71. **find-composition's string surface diverges and was retired under a
    false blanket claim** (2adbbd4 said "batch GNU answers nil"; true only
    for buffer positions -- the batch oracle composes STRINGS through
    composition-function-table rules and terminal gstring shaping).
    DISCLOSED, not yet implemented: emaxx answers nil for the string case.
    No file in the pinned test tree calls find-composition, so the frozen
    measurement never exercises the gap.  The code comment now states the
    asymmetry instead of the false claim.
72. **Oracle binary identity was pinned only by self-report.**  The lock
    now records `emacs_binary_sha256` at pin time and `validate_oracle`
    refuses a binary whose hash differs (pipeline audit F1).  This also
    de-circularizes the two manifest regeneration gates, which previously
    trusted the same self-reporting binary they were regenerating from.
73. **`EMACSNATIVELOADPATH` was not stripped from the children's env** --
    an exported value could shadow pinned oracle Lisp with .eln files
    outside every fingerprint.  FIXED: added to UNSET_ENV_VARS (F2).
74. **Frozen mode accepted `--subject-root` and a dirty tree.**  The
    anti-cheat gates scan and behaviorally probe the harness's own tree,
    so a foreign subject root decoupled the gates from the measured
    binary; a dirty tree made the score non-commit-addressable.  FIXED:
    frozen mode now refuses both (F5/F6).
75. Anti-cheat blind spots catalogued for the record (pipeline audit):
    token gates are tripwires against known spellings, defeatable by
    concat!/format! splitting; `src/main.rs`, `src/lib.rs`, `src/perf.rs`
    and build.rs are outside the facade-gate file set; the native-dispatch
    literal-capture regex cannot see calls through a name variable; the
    regeneration gates hardcode `../emacs/src/emacs` rather than reading
    oracle.local.json.  These are verifiability limits, not active
    cheats; each was checked for current exploitation and none found.
    Ungated summary modes (`landed`, `regressions`, `compare-subjects`)
    are distinguishable by their `mode` field and must not be quoted as
    compatibility evidence.
76. Runtime gaps found by probe, no test pinning the wrong behavior:
    parse-partial-sexp elt-2 (last complete sexp start) and elt-10
    internal-state printing diverge from the oracle on open-string
    inputs; `decode-coding-string` drops the `charset` text property the
    oracle attaches; the wrong-type-argument tail (finding 57) includes
    aref/elt/upcase/lsh predicate names beyond the disclosed
    multi-type-contract set.  All DISCLOSED here as open gaps.

77. **The runtime carried the oracle binary's own identity**: `comp-abi-hash'
    "adba4e3f", `comp-native-version-dir' "30.2-adba4e3f" and
    `pdumper-fingerprint' were byte-copies of the pinned oracle's per-build
    values, while emaxx simultaneously (and honestly) reports
    `native-comp-available-p' nil and empty configuration strings.  FIXED:
    comp.c compiles only under HAVE_NATIVE_COMP, so both comp variables are
    now void exactly as in a GNU build without the native compiler (the
    eval_04 eln-filename expectation reverts to the bare path);
    `pdumper-fingerprint' -- documented by pdumper.c as "unique to each
    build" -- is now computed lazily as the sha256 of the running emaxx
    executable, never copied.
78. **Profiler fabrication**: profiler-*-start returned nil (GNU: t) while
    only flipping a bool, and the logs returned the literal STRING
    "#<hash-table>" -- spelled to survive printed-output comparison.  FIXED:
    starts return t with real state; logs return real empty equal-test hash
    tables (emaxx collects no samples; the empty table is the honest
    degenerate).  Probe now type-identical with the oracle.
79. **set-network-process-option returned t unconditionally.**  FIXED:
    non-processes signal (wrong-type-argument processp VALUE); non-network
    processes error "Process is not a network process", both probed against
    the oracle.
80. **command-error-default-function computed the error message and
    discarded it**, printing nothing and returning nil where batch GNU
    prints CONTEXT+message to stderr and kill-emacs's with -1.  FIXED and
    probed: both binaries now print "ctx: boom" and exit 255.  The
    interactive branch routes through `message'.
81. Identity dress-up knobs removed: HOSTNAME/COMPUTERNAME lookups ahead of
    gethostname in `system-name', and EMAXX_USER_FULL_NAME ahead of GNU's
    $NAME contract in `user-full-name' (finding 65's class; nothing in-repo
    set them).
    Also probed and REFUTED from the runtime sweep: the reported
    "infinity read/print hang" was a 5-second probe timeout against the
    ~10-second CLI image reconstruction; infinite floats read, compute and
    print correctly ("1.0e+INF" both binaries).  Remaining honest
    divergences from the sweep (network-interface-info nil,
    file-name-case-insensitive-p nil on APFS, libgnutls-version -1,
    timezone abbreviation, key-description modifier order, thread-join
    wording, overlay/char-table printed forms) recorded as open gaps in
    finding 76's class.

82. **Floats compared by IEEE == in eq, eql, equal and their helpers**
    where fns.c compares representations (same_float): (eql 0.0e+NaN
    0.0e+NaN) was nil, (eql 0.0 -0.0) was t -- both backwards.  The NaN
    case was fatal at scale: macroexp-macroexpand's fixpoint loop
    `(while (not (eq form (macroexpand-1 form))))' relies on (eq X X)
    holding for the atom it just got back, so loading ANY file with a
    NaN literal spun forever.  That single bug produced six of the
    eight zero-coverage files that kept aborting the frozen run
    (cl-lib-, data-, fns-, floatfns-, esh-util-, dbus-tests).  FIXED:
    representation equality at all five comparison sites; oracle-parity
    probes byte-identical.  Residual disclosed approximation: emaxx
    floats are immediates, so (eq A B) for two equal-bits floats is t
    where GNU's separately-boxed floats give nil -- (eq 1.5 1.5) class,
    pre-existing, now stated.
83. Exposed immediately by 82: the interpreted `+' seeded its float
    accumulator with 0.0, so (+ -0.0) returned +0.0 (IEEE 0.0 + -0.0);
    GNU's arith_driver starts from the first argument.  FIXED and the
    bytecomp signed-zero binding cases now pass with the honest
    `equal'.
84. **The cooperative thread scheduler runs a spawned thread's entire
    body inside one step from the parent's context.**  GNU's
    thread-tests.el has the child lock a mutex the parent holds across
    the child's lifetime; preemptive GNU blocks and resumes, emaxx
    span forever (the file produced zero outcomes).  DISCLOSED
    degraded behavior: when the mutex holder is the suspended parent,
    the lock attempt signals "Cooperative thread model deadlock"
    instead of spinning; the file completes with two honest
    mismatches.  A real fix needs resumable thread continuations.

85. **The batch image never registered the default TTY colors.**
    startup.el:1479 calls `tty-register-default-colors' inside
    `command-line' — deliberately outside every `unless noninteractive'
    guard, "regardless of whether the terminal supports colors" — so GNU's
    own batch session ends with 8 colors in `tty-color-alist'.  Emaxx's
    reconstruction replayed the neighbouring startup steps (bar modes at
    1453, scratch major mode at 1572) but not this one, leaving
    `tty-color-alist' empty; `color-values' then answered nil for every
    named color and color.el's arithmetic on that nil signalled
    (wrong-type-argument number-or-marker-p nil).  That single omission
    produced 21 mismatches in the 2026-08-25 baseline across color-tests,
    css-mode-tests and erc-nicks-tests.  FIXED by evaluating the same form
    startup.el evaluates.  Note the shape of the fix: the color database
    is Lisp-owned (`color-name-rgb-alist' is a 657-entry defconst in
    term/tty-colors.el, which emaxx already loaded and left unused), so
    transcribing it into Rust would have been the copied-snapshot cheat
    this audit exists to catch.  Emaxx runs GNU's registration function
    against GNU's own table.  Verified: the three files now match the
    oracle 82/82, and forcing `tty-defined-color-alist' back to nil
    reproduces exactly the 21 original failures.
86. **Four color primitives bypass the Lisp color database entirely.**
    Found by the adversarial audit of finding 85's fix, and pre-existing:
    `color-gray-p', `color-supported-p', `color-distance' and
    `color-values-from-color-spec' are served natively from a five-entry
    Rust name table (`named_color_spec', color_lcms.rs) rather than
    through GNU's `tty_lookup_color'/`tty-color-desc' path.  Measured
    divergences: the first two answer nil for every name outside those
    five (`gray50', `snow', `dark slate gray') where GNU answers t;
    `color-values-from-color-spec "red"' answers (65535 0 0) where GNU
    answers nil, since xfaces.c parses X specs only and never names;
    `color-distance' uses sum-of-squared-differences instead of
    xfaces.c:1208's Riemersma metric, rejects the documented (R G B) list
    arguments, and silently ignores its METRIC argument.  Upstream's
    `xfaces-color-distance' only asserts symmetry, so the 7595 baseline
    does not catch any of it.  OPEN: finding 85 is the prerequisite that
    makes routing these through the now-populated Lisp database possible.

87. **A fixture-keyed character literal in the word class — the cardinal
    sin, caught by the adversarial audit of finding 88's fix.**
    `REGEX_WORD_CLASS' and `skip_char_matches_class' both hardcoded
    U+2620 SKULL AND CROSSBONES into the set of word characters.  That is
    the single codepoint `test/src/regex-emacs-tests.el' uses as its
    word-character fixture (its docstring literally says "note: \u2620 is
    a word character"), and `git log -S' dates the literal to commit
    885ae16, titled "Advance compatibility for regex-emacs-tests.el".  The
    implementation was correct at exactly that codepoint and wrong at
    every neighbour: probes showed `skip-chars-forward "[:word:]"'
    answering 1 for U+2620 and 0 for U+2621, U+2622, U+263A and U+2600,
    where GNU answers 1 for all five.  The file scored 34/34 and the pass
    was bought by the literal.  FIXED, not by deleting the literal and
    accepting a mismatch, but by making the classes resolve through the
    syntax table the way GNU does: word boundaries (`\<', `\>', `\b',
    `\B') now mark a pattern syntax-dependent, and `skip-chars-forward'
    /`-backward' resolve `[:word:]' and `[:space:]' through
    `syntax_entry_for_code' (syntax.c:2258 routes skip-chars through the
    same `re_iswctype' the regexp engine uses).  The file is 34/34 again
    with the literal gone, and all five neighbouring codepoints now answer
    correctly.  The hardcoded `_' in the same table was wrong too --
    underscore has symbol syntax, not word -- and no longer decides
    anything on a live path; it survives only in the table-less fallback
    constants, whose one remaining caller is the coding-system operation
    patterns in primitives/coding.rs (no buffer, so no syntax table).
88. **`[[:space:]]` was translated to `\p{White_Space}`.**  regex-emacs.c:151
    defines `ISSPACE(c) = (BUFFER_SYNTAX (c) == Swhitespace)', and GNU's own
    comment at :2097 names SPACE and WORD as the two classes resolved
    through the syntax table.  The fixed Unicode property disagreed in both
    directions: it matched a newline in every mode that gives newline
    comment-end syntax (python, emacs-lisp, the C modes), where GNU does
    not, so `^[[:space:]]*\(.*\)[[:space:]]*$' ran past end-of-line and
    captured the following line; and it missed characters given whitespace
    syntax by `modify-syntax-entry'.  That single defect accounted for all
    13 `python-tests.el' mismatches in the 2026-08-25 baseline (navigation,
    indentation, hideshow and `python-info-current-line-empty-p' all rest
    on it); the file is now 366/366.  FIXED by rendering the class from the
    live syntax table, through the same outside-the-bracket alternation
    `[[:word:]]' already used, so an empty whitespace class yields nil
    rather than an invalid `[]'.  Syntax-table TEXT PROPERTIES are
    deliberately ignored for these two classes, which is what
    regex-emacs.c:139-141 specifies ("use the buffer-local syntax table and
    ignore syntax properties"); the first draft of this fix consulted the
    property class and so answered by Unicode for property-carrying
    characters.
    Two defects in the first draft of this fix were caught by its own
    adversarial audit and by upstream's PTESTS corpus before commit: an
    empty whitespace class emitted an invalid bracket where GNU returns
    nil, and the negated atom was emitted unwrapped so a following
    quantifier bound only part of it -- `[^[:space:]]*' meant "if the first
    character is not whitespace, match everything".
    A second adversarial pass then caught three more before commit: the
    same unwrapped-quantifier defect in the sentinel-guard path (so
    `[^[:space:]]*' ran through a property-marked character), the
    property-vs-table confusion above, and a performance regression where
    the skip-chars scan rebuilt its syntax snapshot per call -- 14.5 ms
    per call on a 140 KB buffer, ~430x its literal-spec path and quadratic
    in any scan loop.  The snapshot is now range segments cached on the
    interpreter and keyed by the char-table mutation generation, which
    brings `skip-chars-forward "[:space:]"' back to the literal path's
    cost (27 ms versus 23 ms for 2000 calls) while still invalidating on
    `modify-syntax-entry'.
89. `[:punct:]` remains syntax-blind for non-ASCII where GNU's ISPUNCT is
    `BUFFER_SYNTAX (c) != Sword'.  GNU does not set `used_syntax' for
    punct, so this is a separate and harder problem than 87/88; recorded
    here rather than left silent.  Probes: U+00A0, U+3000, U+200B and
    U+202F all match `[[:punct:]]` in GNU and not in emaxx.

90. **Every quoted message diverged in the environment that is actually
    measured.**  GNU sets `text_quoting_flag = using_utf8 ()' at startup
    (emacs.c:1665, the test being `mbrtowc' on the two bytes of U+0100), and
    doc.c:653/679 make a nil `text-quoting-style' mean grave quotes when
    that flag is false.  The compatibility harness runs every child under
    LANG=C, so GNU writes `like this' there while Emaxx -- which hardcoded
    the flag to t -- wrote curved quotes for every message carrying a
    quoted name.  FIXED: the flag is computed by replicating GNU's own
    libc test, `internal--text-quoting-flag' is a computed binding marked
    special so a `let' behaves like GNU's DEFVAR_BOOL, and
    `effective_text_quoting_style' consults it for a nil setting only.
    This also invalidated an earlier repair: step 6 changed `require's
    failed-to-provide message to curly quotes and verified it against the
    oracle WITHOUT LANG=C -- a probe run in the wrong environment, which
    reported a false match.  That message now derives its quotes from the
    effective style and agrees with the oracle in both locales, and the
    tests that asserted curved quotes literally now pin the style instead
    of inheriting the developer's LANG.
    Two defects in this change were caught by its own adversarial audit:
    routing every unmatched `text-quoting-style' value through the locale
    flag (doc.c treats any non-nil, non-grave, non-straight value as
    `curve', so a bogus style wrongly answered grave under LANG=C), and a
    second hardcoded U+2019 in the condition-variable mutex error --
    thread.c:499,558 spell it with an ASCII apostrophe and curl it only
    under `curve'.  Both fixed and probed.
    Disclosed approximations: on macOS GNU runs `ns_init_locale' before the
    test, synthesizing LANG from NSLocale when it is unset and falling back
    to en_US.UTF-8 when `setlocale' rejects it; Emaxx replicates the libc
    test but not that preprocessing, so the two differ when LANG is unset
    or names an unusable locale.  The harness always sets LANG=C, where
    they agree.  The decision is cached for the process, which matches GNU
    (its flag is set once in `main' and no later locale change moves it --
    verified: after `set-locale-environment' the oracle still reports the
    startup answer).
91. `interactive-form' and `commandp' recognised only interpreted lambdas as
    OClosures, so a COMPILED advice object -- what nadvice produces for an
    advised function -- was missed and `interactive-form' answered nil where
    GNU composes the advice's spec with the advised function's.  FIXED by
    falling back to the real Lisp `oclosure-type' owner, the idiom the
    autoload path already used.  27 advice shapes probed against the oracle.
    Disclosed: GNU inspects the docstring slot natively and never calls
    `oclosure-type', so advice or side effects on that function would be
    observable in Emaxx and not in GNU.
92. `message' and the `void-function'/`void-variable' diagnostics ignore
    `text-quoting-style' entirely.  They happen to agree with GNU under the
    harness's LANG=C (both grave) and diverge only under an explicit
    `curve' setting or a UTF-8 locale.  OPEN.
93. `require's load-path branch interpolates the FEATURE name where GNU
    names the resolved file ("Loading file qnp failed" against GNU's
    "Loading file /tmp/qlp/qnp.el failed").  Found while probing 90.  OPEN.

94. **The harness's own LANG=C was overridable, which would have quietly
    retired finding 90 from the measurement.**  `configure_upstream_like_env`
    sets `LANG=C` but stripped only the EMACS*/GREP/XDG variables.  POSIX
    gives `LC_ALL` precedence over `LANG', and `LC_CTYPE' overrides it for
    the character-type category specifically, so any operator with either
    exported -- ssh, iTerm, a `LC_ALL=C.UTF-8' container -- would hand both
    binaries a UTF-8 `LC_CTYPE' despite the `LANG=C'.  Probed: `LANG=C
    LC_CTYPE=en_US.UTF-8' yields `(t curve)' where `LANG=C' alone yields
    `(nil grave)'.  This never produced a false PASS, because both binaries
    move together -- but it meant the grave path, the entire subject of
    finding 90, could silently stop being exercised.  FIXED: `LC_ALL' and
    `LC_CTYPE' join `UNSET_ENV_VARS'.
    Still true of the unit-test oracle helper
    `assert_upstream_primitive_contract', which spawns the oracle with no
    environment control at all; the tests it backs are instead pinned
    individually (see 90).  Every test this change touches was run under
    both `LANG=C' and a UTF-8 locale; see finding 113 for why the GATE itself
    could not yet be green under `LANG=C'.
95. `default_to_grave_quoting_style' (doc.c:653-662) has a SECOND test after
    the locale flag: it reads the Lisp variable `standard-display-table' and
    answers grave when U+2018 is displayed as a one-element vector holding
    ?`.  That is a plain variable read, not a terminal capability, so it is
    observable in batch, and it is the branch a non-batch GNU session
    actually relies on -- startup.el:1466 forces the flag to t there, making
    the display table the only remaining route to grave.  Emaxx answers from
    the flag alone.  A comment in values.rs previously claimed this branch
    "cannot change the answer"; that was false and has been corrected.  OPEN.
96. GNU's `DEFVAR_BOOL' coerces on store (`store_symval_forwarding'): after
    `(setq internal--text-quoting-flag 42)' the variable reads back as `t',
    and `(let ((internal--text-quoting-flag 'foo)) ...)' binds `t'.  Emaxx
    has no such coercion for any bool-typed variable, so it reads back the
    raw value.  The effective quoting style is unaffected (both sides test
    truthiness), so this is currently cosmetic, but it is a whole missing
    mechanism rather than one variable.  OPEN.
    Related and also open, CORRECTED 2026-08-25 after an audit showed the
    original claim here was false.  `makunbound' on `text-quoting-style' does
    NOT make GNU's `(text-quoting-style)' signal: doc.c reads the C variable
    `Vtext_quoting_style' directly, never the symbol, so the function still
    answers grave/curve and Emaxx agrees with it exactly.  The real, separate
    divergence is in the VARIABLE: after `makunbound', GNU reports
    `(boundp 'text-quoting-style)' as nil and signals `void-variable' on a
    read, while Emaxx reports t and reads nil.  Probed in both locales.
97. `commandp' on a symbol carrying an `interactive-form' property SIGNALS in
    GNU (eval.c:2282-2291, "Found an 'interactive-form' property!"); Emaxx
    returns t.  Pre-existing, found while auditing 91.  OPEN.

**Correction to 91 (second).**  The property walk added below was landed with
a defensive 64-hop cap on the symbol-function alias chain, justified in a
comment as avoiding a hang.  That justification was false -- `defalias' signals
`cyclic-function-indirection' in both binaries, so no cyclic chain can reach
the walk -- and the cap was a measurable divergence: on a 99-link alias chain
carrying the property on its tail, GNU answers the property at every link
while Emaxx returned nil from the 64th onward.  The sibling `command-modes'
walk was already uncapped, so the tree contradicted itself.  The cap is
removed; the walk is now unbounded exactly as data.c:1144 is.

**Correction to 91.**  The widening described there was landed with an
ordering defect, caught by its own adversarial audit: `interactive-form'
consulted the OClosure path BEFORE the `interactive-form' property, where
data.c:1141-1151 consults the property first, unconditionally, walking the
symbol-function alias chain.  Before the widening the defect was unreachable
(a compiled advice object was not recognised as an OClosure at all), so the
change made a pre-existing inversion observable for every advised function.
Probed against the oracle: an advised symbol carrying the property returns
the property in GNU.  FIXED, and now pinned by
`interactive_form_prefers_the_property_over_advice_and_walks_aliases', which
checks seven shapes against the oracle.  The note that 91 rested on probes
alone was accurate when written and no longer is.

**Correction to 90.**  Two further defects in that change were caught by the
same audit round.  The tests for it were themselves LANG-dependent in two
ways: a condition-variable expectation still spelled a literal U+2019 with no
style pinned, and two test programs carried literal curved quotes INSIDE the
Lisp source handed to the oracle through `--eval', which GNU decodes with the
locale's coding system -- so the program text itself was corrupted under
`LANG=C'.  Both are fixed: the affected programs pin
`internal--text-quoting-flag' and spell non-ASCII as `\N{U+XXXX}' escapes, and
every touched test was re-run under both `LANG=C' and a UTF-8 locale.  A
comment claiming "GNU's own tests bind it" was also false -- GNU's Lisp
touches the flag in exactly one place, `setq' at startup.el:1466 -- and has
been corrected.  Finally, making the flag a `builtin_var_value' fallback
rather than a real binding regressed `default-boundp' to nil where GNU
answers t, which is precisely the hazard finding 37 warned about; it is now a
real global whose VALUE is still computed, never asserted.

## 2026-08-25 whole-tree sweep (findings 98-112)

A third adversarial sweep, commissioned to cover everything the 7595 run
touches rather than just the current diff.  It cleared large areas -- the ERT
assertion machinery is entirely GNU's, the scoring arithmetic closes exactly
(7144 mutual passes + 232 same-reason mutual skips + 47 same-message mutual
failures = 7423; 138 + 8 + 6 + 20 = 172), the manifest is sha256-pinned, no
`catch_unwind' exists in production code, no oracle paths appear in `src/',
and the two historical batch-input cheats are genuinely dead -- and returned
the following.  Items marked VERIFIED were re-checked directly against the
source or the oracle before being written here; the rest are recorded as
reported and still need confirmation.

98. **The headline denominator is an artifact of a tooling timeout.**
    VERIFIED.  `compat/oracle_tests_all.txt' marks three files
    `load-error process timed out during test': `test/lisp/net/tramp-tests.el'
    (4798), `test/lisp/progmodes/eglot-tests.el' (4993) and
    `test/src/comp-tests.el' (7276).  The inventory that produced that file
    was generated with `EMACS_TEST_TIMEOUT=20' (oracle_tests_all.md:6),
    while the frozen run allows 180 s per phase.  So three files the oracle
    runs fine were dropped from the denominator by a 20-second cap NINE TIMES
    tighter than the one the measurement itself applies.
    CORRECTED 2026-08-26: an earlier version of this entry said the frozen run
    "imposes NO timeout at all", citing `compat.rs:386 resolve_timeout'
    returning `None' by default.  That read only half the path --
    `resolve_run_timeout' (compat-harness.rs:2208 after this commit's edits)
    wraps it as
    `resolve_timeout()?.or(Some(DEFAULT_TIMEOUT_SECONDS))' with
    `DEFAULT_TIMEOUT_SECONDS = 180' (compat-harness.rs:36), so 180 s is the
    real default.  The conclusion is unchanged: all three files finish inside
    180 s.
    CORRECTED AGAIN 2026-08-26, and the first correction's risk analysis was
    itself misdirected.  The frozen procedure is invoked with
    `--timeout-seconds 3600' (docs/handover-2026-08-24.md:98), and the
    recorded baseline confirms it ran that way (provenance timeout_seconds
    3600), so the 180 s default governs nothing in practice -- it was already
    inadequate for the PRE-EXISTING manifest: edebug-tests.el spends 239.5 s in
    its TEST phase alone.  (Two exhibits originally cited here, ruby-mode-tests
    .el at 189 s and semantic-utest-ia.el at 178 s, were withdrawn on audit --
    those are TOTAL wall times whose largest single phase is 177.6 s and
    166.4 s, under the cap.  Citing them committed the very per-phase/total
    confusion the next sentence condemns.)  The "~36 s of headroom" figure was also wrong twice over: it
    subtracted total wall time from a PER-PHASE cap, when setup and test each
    get the full budget, so tramp's binding phase (~124 s of body) has closer
    to a minute even against the unused 180 s default.  The honest statement
    is that the operator's 3600 s covers every file in the manifest, old and
    new, so no timeout risk is introduced ON THE ORACLE SIDE.  That
    qualification is deliberate: every timing quoted for these three files is
    ORACLE time.  Emaxx has never run any of them, and comp-tests.el is 177
    `:nativecomp' outcomes it has no native compiler for, so the Emaxx-side
    cost is genuinely unknown until the first re-baseline.  An earlier draft
    said the change "does not move the timeout risk at all", which claimed
    more than the evidence supports.  The sweep re-measured them at 24.5 s / 52 outcomes
    (eglot), 153 s / 59 (tramp) and 177/177 passing (comp-tests) -- 288
    outcomes, an honest denominator of 7883.
    RE-VERIFIED INDEPENDENTLY 2026-08-25, running the oracle directly under
    the harness's own selector and LANG=C: eglot-tests.el ran 52 tests in
    30.2 s (39 expected, 6 unexpected, 7 skipped) with `clangd' present and
    connecting, and tramp-tests.el ran 59 tests in 143.5 s with 52 expected,
    ZERO unexpected and 7 skipped -- it passes outright.  Neither hangs, and
    both finish inside the frozen run's 180 s budget.  The documented
    rationale is false: eglot's LSP server (clangd) is installed and connects,
    and tramp's default method is local.  comp-tests.el ran 177 tests in
    132.0 s with 177 results as expected and ZERO unexpected -- it passes GNU
    outright.  All three exclusions are therefore re-verified as unjustified,
    288 outcomes in total.  A related consequence IS verified: zero
    `:nativecomp'-tagged outcomes survive in the 7595 even though the oracle
    lock sets `native_compilation: true' specifically to include them.
    This is not a scoring cheat -- nothing is counted that should not be --
    but the denominator is smaller than the project claims it is, and the
    documented rationale for the exclusions ("tramp needs remote access,
    eglot needs LSP servers") does not match the recorded reason.
    RESOLVED 2026-08-26 with the owner's approval: the inventory was
    regenerated without the cap and the manifest is now 518 files / 7,883
    outcomes / 1 load error, with every pinned constant and the sha bumped
    deliberately.  An auditor independently re-derived the whole manifest from
    the live pinned oracle and its sha matched byte-for-byte, which is the
    strongest evidence available that the contents came from the oracle rather
    than from an editor -- the gap finding 115 describes.
99. `thread_program_from_lambda' (eval/threads.rs:2878-2918) does not run an
    anonymous thread body at all: it pattern-matches three syntactic shapes
    -- a lone `sleep-for' call, exactly `(while t (thread-yield))', and a lone
    `thread-signal' call -- and signals "Unsupported anonymous thread entry
    point" for anything else.  VERIFIED by reading the function.  The middle
    shape appears nowhere in GNU outside thread-tests.el:319.  This is the
    machinery behind part of finding 84's disclosed cooperative model, but
    shape-matching specific test bodies goes beyond that disclosure.
100. dispatch/gnutls.rs:388-446 carries a 9-entry digest catalogue in the
     oracle's exact order, while the same file dlopens `gnutls_cipher_list'
     and `gnutls_mac_list' for its neighbours.  Reported; ~3 outcomes.
101. `("operating-system-release", "25.6.0")' at eval.rs:4109 is this host's
     `uname -r'.  VERIFIED (uname -r == 25.6.0).  `uname_value("-r")' already
     exists in-tree, so this is a transcription where a computation was
     available.  GNU computes it at editfns.c:140.
102. `data-directory', `doc-directory', `installation-directory' and
     `emacsclient-program-name' are derived from `EMACS_TEST_DIRECTORY'
     (system.rs:419-452 via bindings.rs:499,552,647), contradicting the rule
     stated in that same file at bindings.rs:489-492.  Reported.
103. `set-network-process-option' (files_process.rs:1731-1744) resolves the
     process, confirms it is a network process, and returns t without ever
     reading the option argument; GNU validates it and signals "Unknown or
     unsupported option".  It also accepts 2 arguments where GNU requires 3.
     The in-code comment credits finding 79 with repairing this arm; only the
     `processp' half was repaired.  Reported.
104. `get-unused-iso-final-char' (buffer_meta.rs:1124) returns the constant
     ?0 with its arguments unread.  Oracle: `(get-unused-iso-final-char 1 94)'
     is 54, `(... 2 94)' is 50, and an invalid CHARS signals.  Reported.
105. `max-lisp-eval-depth' is effectively ignored: eval/core.rs:244,324-330,
     378-388 reads the global only (a `let' is invisible), multiplies by 384,
     applies a 307200 floor, hardcodes `stack_headroom_remains()' to true, and
     signals a plain `error' though `excessive-lisp-nesting' is defined at
     eval.rs:732 and never raised.  Reported.
106. `decode-coding-string' (coding.rs:1204,1206) routes `euc-jp' to raw text
     and makes every unimplemented multibyte system an identity function via
     a `_' arm.  The encode direction signals honestly, which is the tell.
     Currently DEFLATING the score rather than inflating it.  Reported.
107. `decode-sjis-char'/`encode-sjis-char' (buffer_meta.rs:1368-1387)
     implement exactly the single pair 0x82A0 <-> U+3042 and signal
     otherwise.  Reported.
108. `file-name-case-insensitive-p' (files_process.rs:446-449) is a constant
     nil where GNU answers t on this APFS host.  The GNU test compares the
     handler and non-handler results OF THAT SAME PREDICATE, so a constant
     makes both sides agree and the test passes trivially -- one outcome
     currently INFLATING.  Reported.
109. Native keymap dispatch (values.rs:4057-4077) suppresses local and minor
     maps unless `add-keymap-witness' is present -- a symbol private to
     ../emacs/lisp/subr.el:6551 that GNU's keymap.c never consults.
     Reported.
110. `garbage-collect' (misc_keymaps.rs:1404-1425) returns a correctly shaped
     alist with every count fabricated as 0, while its neighbour
     `memory-use-counts' signals honestly.  Six GNU test files call it for
     effect only, so the shape is what keeps them green.  Reported.
111. `network-interface-info' (files_process.rs:1746) is a bare nil beside a
     genuinely implemented `network-interface-list'.  Reported.
112. `intern-soft' (completion.rs:402-424) infers interned-ness from the
     value, function, builtin and plist cells where GNU performs a pure
     `oblookup'.  Reported.

Lower-impact items the same sweep recorded without ranking: the
`locate-file'/`load-file-name' prefix remap under
`EMAXX_DUMP_SOURCE_DIRECTORY'; `case.rs:127' downcasing U+0130 where the
oracle leaves it unchanged; `script_contains' as seven hand-written ranges
against GNU's `char-script-table'; `current-cpu-time' returning wall-clock
nanoseconds in a one-element list rather than `(TICKS . HZ)'; `gap-size'
constant 0; `lock-file' writing a regular file where GNU writes a dangling
symlink; simulated kqueue watches; dead write-only `current_ert_test_name'
state; and the `landed'/`regression-add'/`regression-audit' modes writing a
summary.json without the anti-cheat gates that frozen mode enforces.

113. **The unit gate had never been run in the environment the project
     measures.**  Every gate to date inherited the developer's UTF-8 shell,
     while the compatibility harness runs its children under LANG=C.  Running
     the gate under LANG=C for the first time (v34) turned up nine failures
     out of 2211, none of which any previous green gate had shown.  (2211 is
     the whole serial gate -- the `cargo test --lib' suite plus the binary and
     integration stages; `--lib' alone currently lists 2150.)

     FOUR were defects in the tests themselves and are fixed here:
       - `select-safe-coding-system' had the UTF-8 answer hardcoded: GNU drops
         the `-unix' suffix under LANG=C because `set-locale-environment'
         leaves `buffer-file-coding-system' nil.  The test now pins that input.
       - three could not even be PARSED by the oracle.  The contract helpers
         hand programs to GNU through `--eval', and GNU decodes argv with the
         locale's coding system, so literal non-ASCII became
         `Invalid read syntax: "?"'.  The helpers now rewrite non-ASCII as
         `\N{U+XXXX}' escapes, and refuse -- loudly -- to escape a character
         sitting anywhere the escape would change the program's meaning (a
         symbol name, or after a backslash inside a string), so a future
         author cannot silently ask the oracle a different question.
         Loading from a file was tried first and REJECTED: `-l FILE' leaves
         `last-coding-system-used' as `prefer-utf-8-unix' where `--eval'
         leaves it `no-conversion' under LANG=C, which coding-sensitive
         contracts observe -- it would have fixed the decoding by silently
         changing what was measured.

     FIVE remain red under LANG=C and are NOT fixed here.  An audit corrected
     an earlier version of this entry which claimed `truncate-string-to-width'
     was among the fixed and double-counted it into both buckets; the true
     split is 4 + 5 = 9.  Of the five:
       - `truncate-string-to-width' ignores a bound `truncate-string-ellipsis'.
         Its expectation WAS repinned, and the pin is a correct spec (the
         oracle answers "h\u{2026}" under both locales with the variable
         bound), but Emaxx does not honour the binding, so the test is still
         red and the pin changed nothing about pass/fail.
       - `keyboard-coding-system' answers nil where GNU answers
         `no-conversion'.
       - two composite/font/mule contracts disagree on coding-system identity.
       - `native_composite_c_family_and_text_property_identity_match_gnu' is
         NOT an Emaxx divergence and was misclassified as one here.  It fails
         at the helper's own assertion, which compares the ORACLE's stdout to
         a hardcoded expectation: the oracle answers `[us-ascii 101 769]'
         under LANG=C against a stored `[utf-8-unix 101 769]'.  Emaxx's answer
         is never reached.  It belongs to the hardcoded-expectation class
         above and simply has not been repinned yet.
     All five pass under a UTF-8 locale, which is why they were invisible, and
     all are PRE-EXISTING: the same nine tests were run under both locales
     before and after this change.
     This matters beyond the unit suite: the harness measures under LANG=C,
     so these divergences are plausibly already inside the 172 mismatches.
     The gate for this commit was therefore run under UTF-8, the standard the
     tree currently meets, with every touched test additionally verified under
     both locales.  Making LANG=C the gate standard is tracked work.  OPEN.

**Note on the baseline's currency.**  `docs/baselines/frozen-7595-2026-08-25`
records `subject_git_head 3f59bbff`, and its arithmetic closes for that tree
(7423 + 172 = 7595).  The commit carrying findings 90-113 changes two
behaviours the GNU suite exercises -- `require's message quoting and
`interactive-form's property ordering -- so the 97.74% headline is STALE for
the tree it lands on.  It is not wrong about the run it describes; it simply
no longer describes HEAD.  Re-baselining is deliberately deferred until the
denominator question in finding 98 is settled, so the measurement is redone
once rather than twice.

114. **Timed-out runs could earn credit they had not established.**
     `invalidate_timed_out_comparison' (compat-harness.rs:1808) marked a
     timed-out comparison `matches = false' and attached an issue, but left
     `matching_outcomes' intact -- and `run_compat_files' accumulates that
     field into the headline numerator AFTER invalidation.  A child killed at
     the phase boundary AFTER its report file reached disk therefore passed
     the bilateral coverage gate (the file exists, so the real report is
     loaded rather than a synthesized load error), contributed every matching
     outcome to the score, and was demoted only at FILE level.  A run could
     print "7883/7883 matching" beside a non-zero mismatching-file count.
     Pre-existing, but made newly reachable by finding 98's re-inclusion of
     `tramp-tests.el' -- the very file whose mock shells keep a process tree
     alive past the report write, as the `run_command' comment already noted.
     Up to 59 outcomes of unearned credit.  FIXED: the outcomes are folded
     onto the mismatching side, idempotently, and pinned by
     `a_timed_out_runner_earns_no_matching_outcomes'.  The pre-existing test
     could not have caught this -- it started from zero outcomes.
     Found by the adversarial audit of the denominator change, not by me.
     Disclosed side effect: `comparison.json' is written AFTER invalidation,
     so per-file artifacts for a timed-out file now record
     `matching_outcomes: 0' where older artifact directories recorded N.
     Nothing reads that field back -- `compare-subjects' works from
     summary.json and the batch reports -- but old and new artifact trees are
     now semantically different with no version marker on them.
115. `anti_cheat::enforce_all' gates the GNU C manifest and the builtin
     arities table against fresh regeneration, but has no equivalent for the
     frozen compatibility manifest.  The sha256 pin catches an UNANNOUNCED
     edit; it cannot verify that the manifest's contents actually came from
     the pinned oracle, because regenerating legitimately means bumping the
     sha.  The 2026-08-26 regeneration was closed empirically instead -- an
     auditor re-derived all 288 added outcomes from the oracle, in the
     harness's own emission order -- but the asymmetry remains.  OPEN.

**Open question on the recorded timings.**  While re-checking finding 98's
per-phase evidence, `test/lisp/eshell/em-cmpl-tests.el` in the 2026-08-25
baseline reports `emaxx_test_duration_ms` 1,441,725 against an
`emaxx_duration_ms` of 45,034 -- a test phase thirty-two times longer than the
run it belongs to, which cannot be right.  Either the phase split or the total
is wrong for that row, and any argument resting on per-phase timings from this
artifact should be treated as unreliable until it is explained.  Not
investigated; recorded so the next reader does not build on it.  This is why
finding 98's corrected evidence now rests on `edebug-tests.el' alone, whose
239,452 ms test phase sits coherently inside a 251,765 ms total.
