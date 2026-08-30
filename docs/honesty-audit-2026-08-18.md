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
| 99 | make-thread: the classifier claim was stale; the real defects were join/bindings/handlers | FIXED 2026-08-27 - three semantics fixes; interleaving stays open as 84 |
| 124 | timers fire inside thread-join and see the joiner's let bindings; GNU runs none there | OPEN |
| 125 | thread-signal to the main thread prints eagerly and drops the data; GNU queues an event | OPEN |
| 126 | detect-coding-string still answers raw-text where the read path now answers iso-latin-1 | OPEN |
| 127 | supra-Unicode characters (private charset codepoints) cannot live in strings/buffers | OPEN (structural) |
| 128 | encode substitution rules for the generic/charset arms never swept against the oracle | OPEN |
| 129 | iso-2022-7bit detected by name but decoded as raw bytes; string-vs-file detection differs from GNU | OPEN |
| 130 | a failed oracle load-path probe silently falls back to the manual tree walk, changing what the harness boots without a trace | OPEN (recorded 2026-08-29) |
| 100 | GnuTLS digest catalogue was transcribed while cipher/mac lists were queried live | FIXED 2026-08-26 - dlopen'd gnutls_digest_list |
| 101 | operating-system-release hardcoded this host's uname -r | FIXED 2026-08-26 - reads uname(2); the entry states what its test can and cannot show |
| 102 | data-directory family derived from EMACS_TEST_DIRECTORY | FIXED 2026-08-28 - epaths-style sibling-checkout constants, oracle-matched |
| 103 | set-network-process-option fabricated success and never read the option | FIXED 2026-08-26 - real setsockopt, 20 cases oracle-matched |
| 104 | get-unused-iso-final-char returned a constant and swallowed validation | FIXED 2026-08-26 - scans the charset registry, 10 cases oracle-matched |
| 105 | max-lisp-eval-depth ignored: let-bindings invisible, excessive-lisp-nesting never raised | FIXED 2026-08-27 - mirrors eval.c:2504; funcall site tracked as 122 |
| 122 | the depth counter has no counterpart to GNU's second increment site in Ffuncall | OPEN (measured) |
| 123 | EMACS_TEST_DIRECTORY shadowed 11 core libraries (5 of them in the 397-name sweep), putting at least 324 measured outcomes (4.1%) at risk | FIXED 2026-08-27 - standard library ordered first, sweep 5 -> 0 |
| 106 | decode-coding-string falls back to identity for every unimplemented system | FIXED (euc-jp real; file reads consult the alist; one disclosed limit) |
| 107 | decode-sjis-char/encode-sjis-char implement exactly one probe value | FIXED 2026-08-28 (big5 twins included; two GNU crash/UB paths disclosed) |
| 108 | file-name-case-insensitive-p constant nil made a self-comparing test pass trivially | FIXED 2026-08-26 - pathconf walk, 18 cases oracle-matched |
| 109 | native keymap dispatch branches on add-keymap-witness, a symbol private to subr.el | FIXED 2026-08-28 - keymap.c:1657 rule ported; witness inert, 6 scenarios oracle-matched |
| 110 | garbage-collect returns a correctly-shaped alist with every count fabricated as 0 | FIXED 2026-08-28 - live reachability census; shape oracle-matched, counts are emaxx truth |
| 111 | network-interface-info was a bare nil beside a real network-interface-list | FIXED (macOS) 2026-08-26 - real ioctls; still nil on other platforms |
| 112 | intern-soft invents keywords nobody has interned; tightening it regresses 288 of GNU's 429 | FIXED 2026-08-29 - the mentioned-names hole is filled computed-not-copied; missing keywords 288 -> 7 (process.c socket-option table); see the obarray close-out |
| 119 | --eval did not intern the symbols it read, unlike file loading | FIXED |
| 120 | eval-region with a custom load-read-function re-interns symbols GNU leaves unintern'd | OPEN |
| 121 | the obarray is ~4400 symbols short of GNU's; intern-soft's inference is what hides it | FIXED 2026-08-29 - missing names 3,908 -> 124 vs the Linux oracle; four computed mechanisms; residual classes named in the close-out |
| 113 | the unit gate never ran under LANG=C, hiding a class of locale/coding divergence from the environment actually measured | FIXED 2026-08-29 - LANG=C is the gate standard; runtime defects and locale-dependent test inputs were fixed, not baselined |
| 114 | a runner killed after writing its report still contributed every matching outcome to the headline numerator | FIXED |
| 115 | the frozen manifest has no fresh-regeneration gate, unlike the C and arities manifests | FIXED 2026-08-29 - manifest sha pin (item 21) + frozen superset check: run ⊆ manifest enforced per file, both runners |
| 116 | system-configuration drifts from the oracle's build-time triple as the host OS updates | OPEN (disclosed) |
| 117 | the gate contains an intermittent test that fails up to 75% of runs under load, so "green" has always been partly luck | OPEN (rate revised UP) |
| 118 | network-interface-list omits most interfaces: 3 where GNU reports 11 on the same host | OPEN |

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

     **2026-08-29 closeout.**  The later LANG=C gate work corrected the
     classification above as well as the failures.  `keyboard-coding-system'
     now starts as keyboard.c's `no-conversion'.  Composite and font glyph
     strings now use term.c's effective terminal coder: nil, `no-conversion',
     `raw-text' and `undecided' select safe US-ASCII, while a real encoding
     coder is retained; an explicit three-row oracle contract pins that
     mechanism.  The two larger family contracts set their terminal coder to
     UTF-8 explicitly, so their unrelated assertions no longer depend on the
     process locale.

     The truncation row was a dump-membership mistake, not a broken dynamic
     binding.  `mule-util' is preloaded only by some window-system dumps; in
     the tty image its first autoload occurred inside the `let', after binding
     scope had already been chosen.  Loading the real GNU owner before the
     binding makes the function contract identical on tty, NS and X builds,
     and the bound ellipsis is then honored under both C and UTF-8.  No
     Emaxx-only owner or fallback was added.

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

## 2026-08-26 mechanical cheat fixes (findings 101, 103, 108)

The first tranche of finding 99-112 repairs -- the ones where the honest
implementation was small enough to land without a design.  All three were
probed against the pinned oracle before and after.  (None of the three has
locale-dependent output, so unlike findings 90-113 there is nothing here for a
second locale to exercise; claiming otherwise, as an earlier draft did, dressed
up a run that proved nothing.)

- **101 FIXED.**  `operating-system-release' now comes from the `uname'
  SYSCALL (editfns.c:136-141), not the literal "25.6.0" it had been
  transcribed to, and not from forking `uname(1)' -- an intermediate draft did
  fork, once per `Interpreter::new()', and answered nil under an empty PATH
  where GNU always answers a string.
  HONEST LIMIT OF THE TEST, corrected after an audit called the original
  claim here false: NO on-host test can distinguish a transcription of this
  host's release from a computed one.  The oracle says "25.6.0", `uname' says
  "25.6.0", and so did the literal.  The test pins the WIRING and would not
  fail if the literal were reintroduced today.  That this cheat is gone rests
  on code inspection, not on an assertion.  The earlier wording -- that
  asserting against the syscall was stronger than an oracle contract -- was
  wrong in exactly the way the cheat itself was.
- **108 FIXED.**  `file-name-case-insensitive-p' asks the filesystem through
  `pathconf(_PC_CASE_SENSITIVE)' and walks up the tree as fileio.c:2711-2722
  does -- though NOT fileio.c:2700's `Fexpand_file_name' first, so an
  unresolved ".." with a missing intermediate component still diverges
  ("/tmp/foo/../" is t in GNU, nil here); that is a pre-existing property of
  the shared path helper, shared with `file-exists-p' and ~30 other
  primitives, not of this change.  Including the detail that makes a missing path answer
  nil rather than inheriting the root's answer -- `file-name-directory' of
  "/nope/deep/" is itself, so the walk terminates on the second hop.  Eighteen
  cases byte-identical to GNU: the committed test pins
  four rows, five more were probed by hand, and auditors independently checked
  nine further paths
  (relative, empty, trailing-slash, "//", a non-string, and a bare missing
  directory whose parent IS reachable, which answers t where the deeper one
  answers nil).  The test also pins that an existing path and a missing one
  must DIFFER, so no future constant can satisfy it -- guarded to macOS, since
  on a case-sensitive volume or on Linux every answer is legitimately nil and
  the assertion would fail against a CORRECT implementation.
- **103 FIXED.**  `set-network-process-option' now reads the option: it looks
  it up in process.c:2839's table, applies it with `setsockopt' on the real
  descriptor behind the process, records an accepted option on the contact
  plist (process.c:2990), and signals "Unknown or unsupported option" -- or
  returns nil under NO-ERROR -- for anything else.  Its arity is GNU's 3-4
  rather than the 2-4 it had.  Twenty cases byte-identical to GNU across two probe sets -- including
  an over-long device name, an empty one, t/nil/0/negative/float/huge
  `:linger' values, an uninterned keyword, a foreign-obarray keyword, and
  the `wrong-number-of-arguments' and `wrong-type-argument' condition
  types.
  CORRECTED after an audit: an earlier draft of this entry claimed
  SO_BINDTODEVICE was "absent because GNU itself compiles it out on this
  platform".  That was false, and the oracle refutes it in one command --
  macOS sys/socket.h:190 defines SO_BINDTODEVICE as 0x1134 and GNU accepts
  `:bindtodevice', returning t and recording the device on the contact plist.
  The first draft of this FIX therefore REGRESSED that option: it signalled
  "Unknown or unsupported option" where the old cheat had returned t and
  matched GNU by accident.  A cheat that was accidentally right was replaced
  by an implementation that was deliberately wrong.  `:bindtodevice' is now
  implemented per process.c:2913-2925 -- zeroed IFNAMSIZ+1 buffer, at most
  IFNAMSIZ bytes copied, always IFNAMSIZ handed to the kernel so unbinding
  works -- with GNU's distinct "Bad option value for %s" for a non-string
  non-nil.  Only SO_PRIORITY is genuinely unavailable here; on Linux it would
  need adding.
  Three further defects in that first draft, all audit-found: the option was
  matched against Emaxx's RAW symbol name, so an uninterned or foreign-obarray
  `:broadcast' -- which GNU accepts, since process.c:2881 compares by name --
  would silently have become "Unknown or unsupported option"; a setsockopt
  failure raised a plain `error' where GNU raises a `file-error' carrying the
  option and value as DATA (process.c:2940 `report_file_errno'); and an
  out-of-int-range `:linger' was truncated into the kernel where GNU ignores
  it.

116. `system-configuration' is a BUILD-TIME constant in GNU: emacs.c:3625 sets
     it from `EMACS_CONFIGURATION', the triple autoconf recorded when that
     binary was configured.  The pinned oracle was built under macOS 25.5.0
     and reports "aarch64-apple-darwin25.5.0" forever; Emaxx computes the
     triple at runtime and reports "aarch64-apple-darwin25.6.0" on the same
     machine, because the host OS updated underneath it.
     This is NOT a cheat and must not be "fixed" by copying the oracle's
     string -- that is finding 77's family, transcribed build identity.  Emaxx
     is not configured by autoconf, so computing is the honest answer.  But
     the two WILL disagree for any GNU test comparing `system-configuration'
     against the oracle's value, and the gap widens every time the host OS
     updates while the pinned build stays put.  Recorded so a mismatch in that
     shape is recognised as environmental rather than chased as a defect.
     Pre-existing: the runtime computation predates the uname(2) change.
     OPEN (disclosed).

**Correction to the 103 fix, found by my own re-verification after the
scheduled audit stalled without reporting.**  The repair matched the option
name correctly but stored a RECONSTRUCTED symbol on the contact plist.  GNU's
`plist_put' compares with EQ and stores the caller's own symbol, so a
foreign-obarray `:keepalive' sets the socket option yet stays invisible to a
later `(plist-get (process-contact p t) :keepalive)' -- the interned keyword
is not EQ to it.  Emaxx answered t where GNU answers nil.  Fixed: the caller's
symbol is stored verbatim and existing entries match by raw-name identity,
deliberately NOT by the visible name used for the option-table lookup, because
GNU uses two different comparisons (strcmp for the table at process.c:2881, EQ
for the plist at :2990).
A FOURTH round of review then found the same identity bug surviving on the
ERROR path: `report_file_errno' (process.c:2954) puts `list2 (opt, val)' in
the file-error data, again the caller's symbol, and Emaxx was rebuilding it
there too -- so `(eq (nth 3 err) key)' answered nil where GNU answers t.  The
fix on the plist path had simply not been carried the few lines across to the
error path.  Both now pass the caller's symbol through, pinned by a test row
that asserts `eq' succeeds against the caller's keyword AND fails against the
interned one, which is what distinguishes a stored symbol from a rebuilt one.
The same round found `std::io::Error::last_os_error()' being read after two
allocating calls; process.c:2953 saves errno on the line after the syscall and
fileio.c:293 warns explicitly that building a Lisp string can clobber it.
Errno is now captured immediately.

**Process note.**  The three ledger corrections above were written once
before, reported as landed, and silently lost: the editing script verified
each substitution but wrote the file only at the end, so one failed assertion
discarded all of them.  An intermediate LOCAL commit was then made still
carrying the false SO_BINDTODEVICE claim, while the summary reported that
claim as corrected.  Nothing was pushed -- that commit was amended before it
left the machine, so `git log' shows the string only inside this refutation.
Calling it "published" would overstate the harm: the defect was the false
report, not a publication.  Ledger edits are now applied and re-read from disk
one at a time.  The irony is exact: an honesty ledger asserted a fix that had not
happened, which is the same defect finding 113 recorded about
`truncate-string-to-width'.

117. **The gate has been partly luck and nobody noticed.**
     `upstream_eshell_script_regressions_stay_green' (eval_05.rs:7779) wraps
     GNU's `em-script-test/source-script/background'
     (test/lisp/eshell/em-script-tests.el:70), which sources a script in the
     BACKGROUND and then compares the buffer.  When the check wins the race
     the buffer holds "hi\n" instead of "hi\nbye\n" and the test fails.
     Measured, not guessed: five consecutive runs of the identical binary gave
     four passes and one failure, and a sweep of every gate log kept this
     session shows the same test failing in v22, v23, v30 and v42 while
     passing in thirteen others -- roughly one run in five, stable across
     weeks and unrelated to whatever change was under test.
     The consequence is uncomfortable and worth stating plainly: every "gate
     is green" in this project's history carried about a 20% chance of this
     test failing instead, and a green gate was therefore never quite the
     proof it was presented as.  It also means a red gate can be noise, which
     is the more dangerous half -- it trains the reader to retry rather than
     investigate.
     Not caused by, and cannot be caused by, the socket-option work committed
     alongside this note: that code is reachable only from
     `set-network-process-option', which eshell scripts never call.
     The fix is not to retry until green.  Either the upstream test needs a
     deterministic wait for the background job before it reads the buffer, or
     Emaxx's background-source path completes later than GNU's and the race is
     an Emaxx defect wearing a flake's clothing -- which has NOT been
     determined.  Until it is, gate results should be read as "green modulo a
     known 1-in-5 flake in this one test".  OPEN.

**111 FIXED (2026-08-26).**  `network-interface-info' was `Ok(Value::Nil)' --
a bare constant sitting beside a genuinely implemented
`network-interface-list' in the same match.  It now mirrors process.c:4459:
a socket used purely as an ioctl handle, SIOCGIFFLAGS / SIOCGIFNETMASK /
SIOCGIFBRDADDR / SIOCGIFADDR for the four IPv4 components, and a `getifaddrs'
walk for the link-layer address, since macOS has no SIOCGIFHWADDR
(process.c:4532).  Six cases byte-identical to the oracle -- `lo0' (nil broadcast, nil hardware
address), `en0' and `bridge0' (both with real MACs), a nonexistent device
answering nil, "interface name too long", and `wrong-type-argument' for a
non-string.  All six are in the committed test, not merely hand-probed: an
earlier draft of this entry claimed "five cases including en0" when the test
contained four and `en0' was not among them, which is the finding-111 species
of defect reappearing inside finding 111's own repair.
Details worth recording because each is a place a plausible implementation
would have diverged:
  - The flag list is built by CONSING in process.c:4498, so it emerges in
    REVERSE table order -- `lo0' reads (multicast running loopback up), not
    (up loopback running multicast).
  - On a Cocoa build GNU spells IFF_NOTRAILERS "smart", not "notrailers"
    (process.c:4412), and the oracle is such a build.
  - process.c:4494 widens `ifr_flags' as UNSIGNED before testing bits,
    because IFF_MULTICAST can set the sign bit of a short.
  - `any' is set only by the ioctl branches; the getifaddrs hardware-address
    branch deliberately does NOT set it, so an interface that yields only a
    MAC still answers nil overall.  Mirrored rather than "improved".
  - The SIOCGIF* request numbers are NOT exposed by the libc crate on macOS.
    They are COMPUTED here from the `_IOWR' encoding in sys/ioccom.h -- group
    letter, ordinal, and payload size -- rather than transcribed as
    0xc0206911 and friends, which would be magic constants valid for exactly
    one struct layout on one platform, and would be the same species of
    defect as finding 101.
Disclosed: IPv6 is out of scope here exactly as it is in GNU, whose docstring
directs callers to `network-interface-list' for it.

**Correction to 111, found by adversarial audit before the commit landed.**
The first implementation PANICKED -- a hard crash, not a wrong answer -- on
any interface whose name is 7 bytes or longer and which has a 6-byte MAC.
`bridge0' exists on this very host and reproduced it immediately: GNU returns
`(nil nil nil (18 . [54 126 46 241 3 64]) (simplex multicast smart running
broadcast up))', Emaxx aborted with "index out of bounds: the len is 12 but
the index is 12".  The cause is that the kernel's `sockaddr_dl' is
VARIABLE-LENGTH: process.c:4548 reads the address through `LLADDR(sdl)', which
is pointer arithmetic `sdl_data + sdl_nlen' into a tail that extends past the
declared array, while Emaxx indexed the libc-declared `[c_char; 12]'.  A
7-byte name needs indices 7..=12 of a 12-long array.  Now read through a raw
pointer, and the struct itself is read with `read_unaligned' rather than by
forming a reference, since an entry shorter than the declared struct would
make a reference invalid.
The crash survived my own five-case probe because `lo0' has no hardware
address (its AF_LINK entry has `sdl_alen' 0, so the walk returns early) and
`en0' has a 3-byte name.  The ~35 lines of `getifaddrs' unsafe were reached by
NO test.  The committed test now names `bridge0' explicitly and asserts that
at least one named interface reports a hardware address, so the path cannot go
unexercised again.
Two smaller defects from the same audit: the flag table carried an
`IFF_ALTPHYS' -> "altphys" row that does NOT exist in GNU's table
(process.c:4386), under a comment claiming the table was GNU's in GNU's order
-- harmless, because on macOS IFF_ALTPHYS and IFF_LINK2 are the same bit and
the earlier row consumes it, but the fidelity claim was false, so the row is
deleted.  And the non-macOS arm's comment claimed GNU "compiles the whole body
out" on other platforms, which is the opposite of the truth: GNU/Linux defines
all five SIOCGIF* requests and returns full data including the hardware
address via `ifr_hwaddr' (process.c:4518).  Finding 111 is fixed for macOS
ONLY; the ledger row now says so, and the non-macOS arm is labelled as the
cheat it still is.

118. `network-interface-list' reports far fewer interfaces than GNU on the
     same host: 3 against 11, verified side by side.
     CORRECTED before this entry was committed: the first draft named the
     wrong cause and asserted something about GNU that is FALSE.  It claimed
     GNU "enumerates link-layer-only entries"; process.c:4344 is `else
     continue' -- GNU skips every address that is not AF_INET or AF_INET6,
     and the AF_LINK-only devices on this host (gif0, stf0, anpi0/1, en1-en4,
     bridge0, ap1) appear in NEITHER list.  It also blamed `if_addrs' for
     yielding "only interfaces carrying an IP address", which is not what that
     crate does.
     The real cause is a disabled cargo feature: if-addrs 0.15.0
     (`src/sockaddr.rs:45-49`) discards every fe80:: link-local address unless
     its `link-local' feature is on, and `Cargo.toml:67' pins
     `if-addrs = "=0.15.0"' without features.  All eight missing rows are
     fe80:: addresses -- lo0's fe80::1, en0's link-local, awdl0, llw0 and
     utun0-3 -- on interfaces that DO carry IP addresses.
     Enabling that feature is NOT the whole fix, though an earlier draft of
     this entry implied it was.  The list is ALSO built in the opposite order
     from GNU: Emaxx maps forward over the crate's iterator
     (files_process.rs:419) while GNU conses each row onto the front
     (process.c:4350ff), so the three rows the two currently share appear
     reversed.  Enabling the feature alone would yield eleven rows in the
     wrong order.  Both halves need doing, and the result checked against
     GNU's list element by element rather than by count.
     Found while writing the test for finding 111: driving that test by
     enumerating interfaces compared two different sets and failed loudly.
     Not a fabricated value, so not the same species as 111, but the list is
     materially incomplete and any GNU test that counts interfaces or looks
     for a specific device will diverge.  OPEN.

**Second correction to 111, from the verification round.**  The audit that
confirmed the LLADDR crash fix also found the repair had left two weaknesses
and one divergence.
The divergence: `need_args' checks only a MINIMUM, so
`(network-interface-info "lo0" "x")' returned data where GNU signals
`wrong-number-of-arguments'.  The generated arity table already recorded
(1, 1) and the sibling `network-interface-list', whose dispatch arm sits directly
beside this one, already used the bounded helper; only this call site
disagreed.  Now `need_arg_range(1, 1)',
and probed identical.  (Over-arity tolerance is systemic in Emaxx --
`(car '(1) 2)' also returns 1 rather than signalling -- so this is one instance
of a wider gap, not the whole of it.)
The weaknesses were both in the test written to prove the crash was fixed.
It asserted that SOME named interface reported a hardware address, which `en0'
satisfies -- and `en0' has a 3-byte name, so it never reaches the offsets that
overflowed.  The assertion would have passed on a host without `bridge0' while
the LLADDR arithmetic went unexercised, which is exactly how the crash shipped
the first time.  It now requires the 7-byte-named interface specifically and
fails loudly rather than testing less.  The test was also not `cfg'-gated to
macOS, so on GNU/Linux -- where the Emaxx arm is still the acknowledged cheat
and these devices do not exist -- it would have failed against a tree behaving
exactly as documented.
Independently confirmed by that round: all 18 interfaces on this host, plus
three nonexistent names and eight edge cases including an embedded-NUL name
and the 15/16-byte boundary, are byte-identical to the oracle.

**100 FIXED (2026-08-26).**  The `gnutls-digests' catalogue was a 9-entry
constant table in the oracle's exact order, in a file whose cipher and mac
catalogues were already queried live through dlopen -- the transcription sat
twenty lines from the machinery that would have replaced it.  It now loads
`gnutls_digest_list', `gnutls_digest_get_name' and `gnutls_hash_get_len'
alongside its neighbours (gnutls.c:402,403,434) and builds the plists exactly
as gnutls.c:2713 does, including the reversal that GNU's consing produces.
Byte-identical to the oracle, nine digests.  The remaining constant table is
NOT the catalogue: it maps a GnuTLS digest name to the internal hash
implementation `gnutls-hash-digest' uses, which has no library equivalent and
must live somewhere; its transcribed `length' field, now redundant, is gone.
Two honest limits.  The retained table still carries the oracle's `id' values
and its SET of nine names, used by `gnutls-hash-digest' -- so a host whose
GnuTLS listed a tenth digest would have `gnutls-digests' report it and
`gnutls-hash-digest' reject it.  And the test for this pins CORRECTNESS, not
liveness: the constant table was built from this host's GnuTLS, so restoring
it would leave the test green.  That the query is live rests on code
inspection, exactly as with finding 101.
Also disclosed: the digest symbols are loaded with the fallible loader, so a
GnuTLS older than 3.2.2 -- which lacks `gnutls_digest_list' -- would fail the
whole library load and take `gnutls-available-p' with it, where GNU falls back
to `gnutls_mac_list' (gnutls.c:2327-2333).  Pre-existing in kind: the cipher
tag/IV size symbols, gated at the same versions, already load fallibly.

**104 FIXED (2026-08-26).**  `get-unused-iso-final-char' returned the constant
?0 with both arguments unread, so it answered "0" even where that slot was
taken and never signalled for a bad DIMENSION or CHARS.  It now validates in
GNU's order -- DIMENSION first, and its real range is 1..=3 even though the
docstring says "1 or 2" -- and scans `0'..`?' against the charsets actually
registered, reading `:iso-final-char' from the same plists `charset-plist'
exposes and asking Lisp for each charset's dimension and chars, PLUS the
equivalence declarations `declare-equiv-charset' writes straight into the same
table (charset.c:1440) -- an earlier version derived a parallel table from
plists alone and diverged from Emaxx's own `iso_charsets' after any runtime
declaration: GNU went 54 -> 55 on a fresh declaration where Emaxx stayed 54.
Eleven cases byte-identical: all six dimension/chars pairs, both range errors,
three type errors, and the equivalence-declaration round trip.
One subtlety cost a wrong answer before it was found by comparing against the
oracle rather than by reasoning -- and a second, found by audit, was that
charset.c:1387 is CHECK_FIXNUM, which names `fixnump', while the obvious
`as_integer' helper names `integerp' (its own comment claimed otherwise).
Six type-error inputs signalled the wrong predicate, and the test compared
only `(car error)', which cannot see the difference.  A `as_fixnum' helper now
mirrors CHECK_FIXNUM and the test compares whole error objects.
The first subtlety: charset.c:1395 reduces CHARS to the BOOLEAN
`chars == 96', and ISO_CHARSET_TABLE is indexed by that flag rather than by
the number, so every charset whose `charset-chars' is not 96 shares the 94
bucket.  `arabic-digit' has a chars of 9 and claims final char ?2 there.
Comparing the numbers for equality skipped it and reported (1 94) as ?2 where
GNU answers ?6.  Emaxx's charset registry agrees with GNU on the charsets that matter here --
every charset whose final char falls in the scanned `0'..`?' range -- and only
the bucketing rule was wrong.  An earlier draft claimed agreement on "all 203
charsets and their final chars", which is FALSE: six differ, `ascii' most
clearly, with final char ?B in GNU and none recorded in Emaxx.  All six sit
outside the scanned range so the answers match anyway, but the sweeping claim
was untrue and is withdrawn.  Those six also lack `:dimension', so
`(charset-chars 'ascii)' signals in Emaxx where GNU answers 128 -- recorded
here rather than left for someone to rediscover.

**112 STAYS OPEN.  An attempted fix was measured, found to be a NET
REGRESSION, and reverted -- and the entry that announced it was wrong twice.**

The finding said `intern-soft' "infers interned-ness from the value, function,
builtin and plist cells where GNU performs a pure `oblookup'".  Across ten
symbol shapes, nine already agreed: a never-mentioned name, a name appearing
only inside a string, a read-quoted symbol, an explicit `intern', a `setq', a
`defun', a `put', an uninterned symbol's name, and an `unintern' round trip.
The inference is not arbitrary -- in GNU you cannot give a symbol a value,
function or property without interning it first.  An auditor later reproduced
that agreement across a further dozen shapes (plist-only, `makunbound',
`fmakunbound', nil-valued `defvar', `defalias', `##', `::', NUL and non-ASCII
names, 5000-character names, shorthands, symbols-with-position).

Exactly one shape diverges: a KEYWORD is bound to itself without being
interned, so the value-cell clause calls every conceivable keyword interned
and `(intern-soft ":never-mentioned")' answers the keyword where GNU answers
nil.

I tightened keywords to a real membership test.  THAT WAS WRONG, and the
number is the reason: GNU's preloaded obarray holds 429 keywords; Emaxx's
holds 141 (an earlier draft of this entry said 146, which was not reproducible
and did not even reconcile with the 288 below -- 429 - 141 = 288 does).  Feeding GNU's own 429 keyword names back as runtime strings,
Emaxx answered nil for 288 of them -- `:key', `:buffer', `:error', `:host'
and so on.  The permissive clause is accidentally RIGHT for every keyword GNU
actually preloads; requiring real membership traded one rare false positive
for 288 common false negatives.  Reverted.
The honest fix is to seed the missing keywords into Emaxx's obarray first --
generated from the oracle the way the arity tables are -- and only then
tighten this clause.  Until that happens 112 stays OPEN with the divergence
stated precisely: never-mentioned keywords, and only those.

**Correction to the entry that claimed 112 was fixed.**  It also asserted that
the failing test which prompted the change was "a TEST artifact" because
"loading a file or evaluating `--eval' runs `intern_symbols_in_value' over the
form afterwards".  The two citations given are the FILE LOADER and
`eval-region'.  Neither is `--eval', and `--eval' did not run the walk at all,
so that justification was false and the product was genuinely wrong on that
path -- see finding 119.  The half of the claim that held is that `-l FILE'
does intern, which is why the oracle probes agreed.

119. `--eval' did not intern the symbols it read.  GNU's reader interns as it
     reads, so `emacs --batch --eval "(progn 'foo (intern-soft \"foo\"))"'
     answers foo; Emaxx answered nil, for ordinary symbols as much as
     keywords, because `batch.rs' evaluated the reader's output without the
     `intern_symbols_in_value' walk that lisp/mod.rs:947 and loading.rs:401
     perform for files and `eval-region'.  `eval-buffer' with a custom
     `load-read-function' (loading.rs:546) LOOKED like the same asymmetry and
     was also given the walk -- which was wrong, and an audit caught it before
     this landed.  GNU interns nothing extra when reading is delegated to
     Lisp: the form's symbols are whatever that function produced.  Walking it
     resurrected a deliberately `unintern'-ed symbol that GNU leaves dead,
     regressing a case that had agreed.  That walk is reverted; only `--eval'
     changed.  A CLI test pins the fix end to end through the real binary,
     because the in-process harness (eval/tests.rs:61,85) already interns per
     form and is exactly what masked this bug.
     Disclosed: `intern_symbols_in_value' walks conses, symbols and string
     properties but NOT record or hash-table payloads, so symbols inside
     `#s(...)' are interned on no path at all -- pre-existing and unchanged
     here, but now inherited by `--eval' along with the rest.
     FIXED.  Found by an audit that was checking a different claim -- the one
     corrected above -- which is the second time this session that a false
     justification turned out to be concealing a real defect.

120. `eval-region' with a custom `load-read-function' runs
     `intern_symbols_in_value' over whatever that Lisp function returned
     (loading.rs:401).  GNU does not: when reading is delegated, it interns
     nothing beyond what the function itself interned.  A reader returning a
     deliberately `unintern'-ed symbol therefore has that symbol resurrected
     in Emaxx and left dead in GNU -- `(intern-soft "gone")' answers the
     symbol here and nil there.
     Found because I nearly copied it.  Fixing finding 119 I added the same
     walk to the `eval-buffer' twin "for symmetry", which regressed a case
     that had been agreeing; the audit caught it, that walk is reverted, and
     the remaining instance is recorded here rather than propagated.  The
     honest fix is to run the walk only when the built-in reader produced the
     form.  Narrow -- it needs a custom read function AND a deliberately
     unintern'd symbol -- but real.  OPEN.

121. **The obarray is thousands of symbols short, and finding 112 is a symptom
     of it rather than a defect of its own.**
     Chasing 112's remaining keyword gap led to the real shape of the problem.
     Measured by dumping GNU's own symbol names and feeding them back as
     RUNTIME STRINGS (a probe that lists them literally interns them by being
     read, which is how an earlier measurement was contaminated):
       - keywords: GNU 429, Emaxx 141, missing 288
       - symbols with NO value, function or property cell: GNU 4,238,
         Emaxx missing 2,340
     A symbol reaches GNU's obarray merely by being MENTIONED in preloaded
     Lisp -- `:key' comes from epg.el, `:host' from auth-source.el.  Emaxx's
     startup does not put those names in its obarray, so roughly 4,400 symbols
     GNU knows are absent.
     `intern-soft' hides this for the common cases by inferring membership
     from a value, function or property cell, which is why the suite never
     noticed: symbols that MATTER usually have a cell.  It is exactly the
     symbols with no cell -- names merely mentioned -- where the inference has
     nothing to go on and the gap shows.  That is also why tightening the
     keyword clause (see 112) collapsed: it removed the paper over a hole
     without filling the hole.
     This reframes 112.  Seeding a keyword list from the oracle would treat
     the visible symptom and transcribe oracle data to do it.  The honest fix
     is for Emaxx's preload to intern the names its Lisp mentions, the way
     GNU's reader does -- computed, not copied.  Until then `intern-soft'
     keeps its inference and 112 stays OPEN, now with the real cause attached.
     OPEN.

**105 FIXED (2026-08-27).**  `max-lisp-eval-depth' was read from the GLOBAL
cell, so `(let ((max-lisp-eval-depth 100)) ...)' was invisible and a runaway
recursion under a deliberately small binding ran to completion.  The value was
then multiplied by 384 and floored at 307,200, so the variable could not lower
the limit at all, and exceeding it raised a plain `error' where GNU raises
`excessive-lisp-nesting' -- a condition this tree already defined
(eval.rs:732) and had never once signalled.
It now mirrors eval.c:2504-2509: read the DYNAMIC binding, raise a sub-100
limit to 100 rather than rejecting it, and signal
`(excessive-lisp-nesting DEPTH)'.  The 384x scale is deleted.
The finding was WRONG about one clause and it is worth saying so: it claimed
`stack_headroom_remains()' is "hardcoded true".  On this platform it is a real
`pthread_get_stackaddr_np' probe; the `true' body is only the non-macOS arm.
That function is untouched -- but the guard beside it DID raise the same plain
`error' the finding describes, while its own comment claimed it signalled
`excessive-lisp-nesting'.  Now it signals what the comment always said.
The risk this change carried was that the 384x scale existed to let honest
deep recursion succeed, so removing it might make Emaxx signal where GNU
succeeds, or crash where it previously signalled.  An audit measured both and
neither happened.  Exact thresholds on the same recursion: GNU last succeeds
at 792 and signals `(excessive-lisp-nesting 1601)' at 793; Emaxx succeeds to
794 and signals the identical object at 795 -- two frames MORE permissive.
Roughly sixty programs matched byte-for-byte, including `cl-labels' 1000 deep,
`macroexpand-all' 400 deep, `cl-loop' to 20,000 and a 200,000-level nested
print.  Deliberate stack-overflow attempts (a million-deep non-tail `cons'
recursion under a 1e8 limit) produced no panic: with the scale gone the
counter trips at 1,601 instead of 614,400, so the native stack is reached
~384x LATER.  Removing the scale reduced crash risk rather than raising it.
Also fixed here, both found by that audit: a NEGATIVE limit became the 1600
default instead of flooring to 100, because the `usize' conversion ran before
the clamp -- making the limit larger than requested where GNU makes it
smaller; and a truncated comment left behind by the deletion, which ended
mid-sentence and asserted that this evaluator "nests several times deeper"
than GNU, a claim the threshold measurement above disproves.

122. Emaxx increments `lisp_eval_depth' at ONE site (core.rs, `eval'); GNU
     increments at TWO -- `eval_sub' (eval.c:2504) and `Ffuncall'
     (eval.c:3078).  Measured: a direct call costs 2 units per level in both,
     but a `funcall'/`apply' chain costs 3 in GNU and 2 here, so those paths
     trip at roughly 795 levels where GNU trips at 529.
     The divergence is in the SAFE direction -- Emaxx tolerates more, so no
     honest program fails that GNU accepts -- but `max-lisp-eval-depth' means
     something slightly different on those paths, and a GNU test that pins the
     depth at which a funcall chain fails would disagree.  Recorded rather
     than folded into finding 105, whose comment now states the gap instead of
     claiming eval.c is mirrored "exactly".  OPEN.

**Correction to 117: the failure rate is much worse than recorded, and it is
load-dependent.**  The entry says "roughly one run in five", from five
consecutive runs giving four passes and one failure, corroborated by four
failures across seventeen gate logs (~24%).  Re-measured on 2026-08-27 while
checking whether an evaluator change had worsened it:

    previous evaluator (HEAD~1 core.rs)   2 pass / 6 fail
    current evaluator                     4 pass / 4 fail

Eight runs each, back to back, same machine, load average ~5.  So the real
rate is somewhere between half and three quarters of runs when the machine is
busy, not one in five -- the original figure was taken on an idle machine and
generalised.  A "green gate" is therefore a much weaker statement than this
ledger has been treating it as, and every green gate reported during this
session should be read with that in mind.
The A/B also answers the question it was run for: the flake is NOT caused by
the `max-lisp-eval-depth' change (finding 105).  The older evaluator fails it
MORE often, so if anything the change helps; eight samples a side cannot
distinguish that from noise, but they comfortably exclude "the change made it
worse".
This strengthens the case that 117 is a genuine Emaxx defect rather than an
upstream test needing a wait, since a pure test-side race would not care which
evaluator is underneath.  Still OPEN, still not to be resolved by retrying
until green.

**117 DIAGNOSED (2026-08-27): a genuine Emaxx defect, not an upstream test
needing a wait.  The open question is answered.**
The entry left two possibilities open -- either GNU's test races and needs a
deterministic wait, or Emaxx's background path finishes later than GNU's.
Measured, running the single ERT test directly against each binary:

    GNU oracle          12 pass / 0 fail
    Emaxx (this tree)   ~4 pass / 4 fail, and 2/8 on the previous evaluator

GNU does not race at all.  (A first attempt at this measurement reported GNU
failing 10/10, which was my invocation: a relative path from the wrong
directory, so the test file never loaded.  Worth recording because a 10/10
failure looked like a dramatic result and was pure operator error.)
The test is NOT missing a wait.  It calls `(eshell-wait-for-subprocess t)',
and that helper (test/lisp/eshell/eshell-tests-helpers.el:106) waits until
`eshell-process-list' is EMPTY.  So the defect is an ordering one: Emaxx lets
a process leave `eshell-process-list' before its output has been delivered to
the redirection target, and the buffer is then read as "hi\n" instead of
"hi\nbye\n".  GNU flushes the output before the process is removed.
The script is `*echo hi' followed by `if {[ foo = foo ]} {*echo bye}' -- two
external commands, the second spawned from inside a conditional, which is
probably why the second one is the one lost.
That the rate MOVES when the evaluator changes (6/8 before finding 105's fix,
4/8 after) is consistent with this: anything altering the timing of Lisp
evaluation shifts the window, which a pure test-side race would not do.
Still OPEN, now with a mechanism rather than a shrug, and still not to be
resolved by retrying until green.

**117: one hypothesis tried and DISPROVED (2026-08-27).**
The diagnosis above said Emaxx lets a process leave `eshell-process-list'
before its output reaches the target, so the obvious fix was GNU's ordering:
`status_notify' delivers whatever is readable BEFORE running the sentinel,
while `pump_external_process_output' drained only the processes that were live
when it snapshotted them, then ran sentinels.  A process exiting between its
own poll and the sentinel loop would therefore be reported finished with
output still in the pipe.
That reasoning is sound and the fix was implemented -- poll and deliver
immediately before each sentinel.  It did NOT help: twelve runs gave 4 pass /
8 fail against 4/4 before, i.e. no improvement and possibly worse.  The change
was REVERTED rather than kept as a plausible-sounding improvement that fixes
nothing; unverified complexity is how a codebase acquires the sort of thing
this ledger records.
What that rules out: the loss is not simply unread pipe bytes at sentinel
time.
Next hypothesis, untested: the wait may return during a GAP.
`eshell-wait-for-subprocess t' waits for `eshell-process-list' to become
EMPTY, and the script runs two external commands in sequence -- `*echo hi',
then `*echo bye' from inside an `if' body.  If Emaxx removes the first process
before spawning the second, the list is momentarily empty, the wait returns,
and the buffer is read before "bye" is ever written.  That would explain why
the missing text is always the SECOND command's, and why flushing at sentinel
time changes nothing.  Testing it needs a way to observe `eshell-process-list'
over time; a first attempt at that instrumentation did not survive batch mode.

123. **`EMACS_TEST_DIRECTORY' corrupts the load-path ORDER, and the
     compatibility harness sets it for every child it measures.**
     Found while trying to reproduce finding 117 outside the gate: a probe
     that worked standalone failed with "Loading file debug failed to provide
     feature `debug'" as soon as the harness's environment was replicated.
     Measured, same machine, same probe:

         GNU     without EMACS_TEST_DIRECTORY   load-path 25 entries
         GNU     WITH    EMACS_TEST_DIRECTORY   load-path 25 entries
         Emaxx   without EMACS_TEST_DIRECTORY   load-path 25 entries
         Emaxx   WITH    EMACS_TEST_DIRECTORY   load-path 102 entries

     GNU's load-path does not respond to that variable at all.  Emaxx's
     `effective_batch_load_path' (batch.rs:670) appends every repo-local elisp
     directory when it is set, and the resulting ORDER differs from GNU's
     dumped one, so subdirectory libraries shadow core ones:

         (locate-library "debug")
           GNU    /Users/.../lisp/emacs-lisp/debug.elc
           Emaxx  /Users/.../lisp/cedet/semantic/debug.elc

     `semantic/debug.el' provides `semantic/debug', not `debug', so
     `(require 'ert)' -- which requires `debug' -- FAILS in Emaxx under the
     harness environment and succeeds without it.  A core library is
     unreachable purely because of a test-harness variable.
     `compat.rs:438' sets `EMACS_TEST_DIRECTORY' on every child, so every one
     of the 7,883 measured outcomes runs with this load-path.  How much it
     costs is unmeasured -- the harness passes `-l ert' on the command line,
     which evidently still works -- but any test whose Lisp `require's a name
     that collides with a subdirectory library resolves to the wrong file.
     This is adjacent to finding 102 (data-directory derived from the same
     variable) and worse in kind: 102 produces a wrong string, 123 loads the
     wrong CODE.
     The fix is to reproduce GNU's load-path order rather than appending
     discovered directories, and to stop letting a harness variable alter
     library resolution at all.  OPEN.

**123: the cost is no longer unmeasured, and it is large.**
The entry said "how much it costs is unmeasured".  Swept `locate-library'
over all 397 core library names under the harness environment, GNU against
Emaxx: FIVE resolve to the wrong file, all of them shadowed by CEDET
subdirectories.

    chart    emacs-lisp/chart.elc    ->  cedet/semantic/chart.elc
    comp     emacs-lisp/comp.elc     ->  cedet/semantic/wisent/comp.elc
    debug    emacs-lisp/debug.elc    ->  cedet/semantic/debug.elc
    generic  emacs-lisp/generic.elc  ->  cedet/ede/generic.elc
    map      emacs-lisp/map.elc      ->  cedet/srecode/map.elc

Requiring any of them FAILS in Emaxx under the harness environment and
succeeds without it; all four tested succeed in GNU with the same environment:

    (require 'map)      "Loading file map failed to provide feature `map'"
    (require 'comp)     "Loading file comp failed to provide feature `comp'"
    (require 'chart)    "Recursive `require' for feature `chart'"
    (require 'generic)  "Loading file generic failed to provide feature ..."

Five files in the frozen manifest require a shadowed library, and they are not
small:

    test/src/comp-tests.el              177 outcomes   (require 'comp)
    test/lisp/emacs-lisp/map-tests.el    62             (require 'map)
    test/lisp/json-tests.el              59             (require 'map)
    test/src/json-tests.el               23             (require 'map)
    test/lisp/emacs-lisp/comp-tests.el    3             (require 'comp)
                                        ---
                                        324 outcomes = 4.1% of 7,883

The 177 are the ones finding 98 restored to the denominator two commits ago.
They were re-included because the oracle runs them fine -- which it does -- but
Emaxx cannot even load that file in the environment it is measured in.  So the
denominator correction and this defect interact: the honest denominator grew,
and a self-inflicted harness artifact is positioned to fail most of what was
added.
This makes 123 the highest-value open item by a wide margin.  It is not a
scoring cheat -- nothing is counted that should not be -- but up to 324
outcomes may be failing for a reason that has nothing to do with Emaxx's
actual Lisp behaviour, and fixing it is honest work that could move the score
substantially.  Whether all 324 actually fail is still unverified; that needs
a frozen run, and this note should not be cited as if it were measured.

**123 FIXED (2026-08-27).**  `effective_batch_load_path' (batch.rs) appended
the repo-local test directories BEFORE the installation's own Lisp.  Those
come from a RECURSIVE `WalkDir' (compat.rs `repo_local_elisp_load_path'), so
77 extra directories -- 66 under `test/' and 11 under `lisp/' itself -- went in
ahead of the standard library (an earlier draft said 35, which matches nothing
measurable; GNU carries 25 entries and Emaxx carried 102) -- GNU's load-path holds nothing below one level under `lisp/', and
does not respond to EMACS_TEST_DIRECTORY at all.
The fix is ordering, not removal: the standard library now goes in first and
the test tree after it, so helper discovery still works while core names
resolve as GNU resolves them.  The 397-name `locate-library' sweep goes from
five differences to ZERO, and `(require 'map)', `(require 'comp)',
`(require 'chart)' and `(require 'generic)' all succeed under the harness
environment where they previously failed.
Pinned by `standard_library_precedes_the_discovered_test_tree' (batch.rs),
which asserts the ORDER directly and was verified to fail when the fix is
reverted ("core at 40, lisp/cedet/srecode at 23").
An audit caught the FIRST attempt at that test being vacuous: it drove
`initialized_upstream_batch_interpreter', which is constructed with
`load_path' already set to GNU's 25 directories, so they head the list under
either ordering and the test passed with the fix reverted.  It is kept, and
relabelled, for what it does show -- that those requires succeed and agree
with the oracle.
The shadow set was also larger than first recorded: ELEVEN names, not five.
Besides chart/comp/debug/generic/map, the walk shadowed `compile' and `cpp'
(cedet/srecode), `grep' and `python' (cedet/semantic), `emoji' (leim/quail),
and `etags' -- that last from
test/manual/etags/el-src/emacs/lisp/progmodes/etags.el, a PARSING FIXTURE that
would have shadowed the real etags.  The manifest exposure is correspondingly
larger than the 324 quoted below; 324 counts only the five originally found,
and the honest figure is not yet measured.
NOT CLAIMED: that this recovers 324 outcomes.  The 324 is the number of
manifest outcomes in files that require a shadowed library; how many of them
were failing FOR THIS REASON is unmeasured and needs a frozen run.  The honest
statement is that a defect which could break `(require 'map)' in 4.1% of the
denominator is gone, not that 4.1% has been recovered.
A smaller divergence remains and is deliberately not chased: Emaxx's
load-path still CONTAINS the test directories, where GNU's does not, so
`(locate-library "eshell-tests-helpers")' answers a path here and nil in GNU.
The reason is narrower than an earlier draft said.  GNU's runner supplies
ONE directory, APPENDED -- test/Makefile.in:64 is `-L "$(SEPCHAR)$(srcdir)"',
and the leading separator means append -- and GNU's tests do not find helpers
through load-path at all: they pass `require's FILENAME argument
(em-alias-tests.el:31), which is why `(locate-library "eshell-tests-helpers")'
is nil in GNU even with that `-L'.  Emaxx's harness passes NO `-L' at all
(compat.rs sets only environment variables), so the walk is how its children
find those helpers.  Now that the walk sits behind the standard library it
cannot shadow it -- and GNU appending rather than prepending means this
ordering is what upstream itself does.

**123, two further notes from the verification round.**
A concrete win that was not noticed when the fix was written: the ONE basename
colliding between `test/' and `lisp/' is `etags', and the test-tree copy is
`test/manual/etags/el-src/emacs/lisp/progmodes/etags.el' -- a PARSING FIXTURE.
`etags-tests.el:26' and `elisp-mode-tests.el:583' both `(require 'etags)' and
were getting that fixture instead of the real library.  The reorder fixes them
rather than breaking anything: there is no test-local override anywhere under
`test/' that legitimately needs to win.
And the evidence is stronger than the entry claimed: Emaxx's first 25
load-path entries are now byte-identical, in order, to GNU's ENTIRE 25-entry
load-path, and a 1,596-name sweep over every basename under `lisp/**' finds
ZERO cases where GNU resolves one file and Emaxx resolves a different one.
The 186 remaining differences are all `GNU nil / Emaxx a path' -- over-
permissive, never wrong.  The regression test now asserts that prefix
property rather than comparing two indices, because the index form only failed
under the old order thanks to `cedet' sorting before `emacs-lisp' in one
unsorted directory walk; the prefix form cannot hold under the old ordering on
any filesystem.

**99 FIXED (2026-08-27), and the finding's own description was stale.**
The entry said thread bodies were pattern-matched into three shapes and
anything else rejected.  That no longer reproduces: arbitrary bodies execute
(the classifier's unrecognised case falls through to a real call).  Probing
what actually diverged found three semantic defects, all now fixed and
GNU-verified:
  - `thread-join' of a thread whose BODY errored re-raised the error in the
    joining thread; GNU catches every body error inside the thread
    (thread.c:815 internal_condition_case), records it for
    `thread-last-error', and join returns NIL.  Only a `thread-signal'
    delivery re-raises out of the join (the error_symbol snapshot,
    thread.c:1081/1088) -- threads-mutex-signal requires the injected quit to
    come OUT of the join, and a first draft that returned nil for both broke
    it.  A `delivered' flag now separates the cases.  Disclosed shortcut: GNU
    returns nil when the target processed the delivery before join; Emaxx's
    cooperative kill is instant, so that window does not exist.
  - Dynamic bindings leaked into children: the parent's `let' stack was
    visible, and a child `setq' wrote the parent's let slot only to be undone
    at let-exit.  GNU gives each thread its own specpdl and SWAPS on switch
    (thread.c:87-100, watchers skipped per data.c SET_INTERNAL_THREAD_SWITCH).
    Implemented as a two-way swap over the live binding records; the child
    reads and writes GLOBALS, and the parent's let-exit restores the
    child-written value -- verified against the oracle, whose cell ends
    `child-wrote'.  The pre-commit audit then found the swap walked the WHOLE
    shared stack, so a GRANDCHILD saw the grandparent's lets again; a
    boundary stack now confines each swap to the suspending thread's own
    records, and the nested probe matches GNU both levels down.
  - The handler list was shared, so a child's error ran the PARENT's
    `handler-bind' handlers.  ERT was the proving case: ert.el:803 wraps every
    test body in handler-bind, and a child error inside an ERT test ran ERT's
    debugfun, whose cl-return-from died at the thread boundary as
    `(no-catch --cl-block-error-- nil)' -- recorded as the thread's error in
    place of the real one.  Children now start with an empty handler list.
Measured: test/src/thread-tests.el goes 4 -> 3 mismatches (threads-errors now
passes; threads-mutex-signal stays green).  The remaining three
(condvar-wait, mutex-contention, bug48990) are the cooperative-interleaving
gap and stay disclosed under finding 84 -- NOT claimed here.
The expectation for the new regression test was itself corrected by the
oracle: rows interact, because a child setq legitimately leaves the GLOBAL
changed, so a later grandchild reads the mutated value.  The first draft said
`global' where GNU answers `child'.

124. Timers fire inside `thread-join' and observe the JOINER's dynamic `let'
     bindings.  GNU's Fthread_join blocks without running timers at all
     (probed: a due timer never runs during the join).  Pre-existing --
     `run_pending_timer_events' predates the thread work -- and adjacent to
     it: whichever thread timers run in, they currently run WITHOUT the
     binding swap.  OPEN.
125. `thread-signal' aimed at the MAIN thread prints "Error ..." eagerly at
     delivery time and DROPS the data (probed: ("hi") became nil); GNU queues
     a THREAD_EVENT and batch prints nothing.  Pre-existing.  OPEN.

## 2026-08-28 coding batch (finding 106 closed; findings 126-128 recorded)

Finding 106 is FIXED.  The work grew as probes disproved my drafts; each
correction below is the oracle's, not mine.

  - `insert-file-contents' now consults `file-coding-system-alist' via the
    already-working `find-operation-coding-system' (fileio.c's third source,
    after coding-system-for-read and set-auto-coding-function).  A pure-ASCII
    .el file reads as prefer-utf-8-unix, not undecided-unix.  `prefer-utf-8'
    itself now detects like `undecided' except that a file which decides
    nothing keeps the prefer-utf-8 name.
  - euc-jp is a real codec: JIS X 0208 via the :unify-map table (unify-charset
    now records state instead of validating and forgetting; mule-conf.el's
    calls finally do something), halfwidth katakana behind SS2, JIS X 0212
    behind SS3, latin-jisx0201 designated with ESC ( J and restored before
    controls/eol, space for unencodable, raw-byte resync on invalid input.
    :subset charsets (both jisx0201 halves) convert through their parent, and
    the code-offset fallback now uses the :code-space INDEX (jisx0208's hole
    0x222F is offset+108, not offset+0x222F).  japanese.el's re-definition of
    japanese-iso-8bit had been shadowing the bootstrap euc-jp codec entirely
    -- every euc-jp decode was raw bytes -- fixed by keeping the internal
    codec discriminator on re-definition.  test/src/coding-tests.el: 9/9.
  - Detection and naming were wrong in ways my own draft tests exposed:
    a file with no eol byte anywhere detects as the BARE base (undecided /
    utf-8), and a bare-undecided read leaves buffer-file-coding-system nil;
    `last-coding-system-used' keeps the caller's own spelling (euc-jp stays
    euc-jp, unix stays unix, binary stays binary) unless the decoder actually
    resolved a charset or an eol the request left open; the pure-ASCII
    shortcut requires the coding to be :ascii-compatible-p, which for
    iso-2022 systems GNU RECOMPUTES from the initial G0 designation
    (japanese.el passes nil for euc-jp; coding.c:11285 overwrites it to t).
    Non-UTF-8 8-bit junk detects as iso-latin-1 (mojibake), except that a
    C1-control byte 0x80..0x9F rejects the latin-1 category and stays
    raw-text -- all probed row by row against the oracle.
  - `string-as-unibyte'/`string-as-multibyte' now expose and parse the
    INTERNAL (UTF-8) bytes; what stood there used the latin-1 byte below
    0x100, signalled above it, and as-multibyte PANICKED on any 8-bit byte
    (RAW_BYTE8_BASE + byte exceeds char::MAX).  `string-make-unibyte' takes
    the low byte of a character with no unibyte equivalent (GNU: ?B for
    U+3042); `string-make-multibyte' produces eight-bit characters, not
    latin-1.  This was why the coding-tests binary file emaxx generated
    differed byte-for-byte from GNU's.

Disclosed limitations (not claimed as fixed):
  - A JIS code missing from the unify table decodes in GNU to a
    supra-Unicode codepoint (0xA2 0xAF -> 1310828); emaxx strings are Rust
    strings and cannot hold characters beyond 0x10FFFF, so such bytes decode
    to raw-byte markers instead.  Affects only unmapped holes.
  - GNU's unify-charset early-return is gated on the lazily loaded
    deunifier: AFTER an encode through the charset, (unify-charset
    'japanese-jisx0208 42) is nil where a fresh session signals "Bad
    unify-map".  Emaxx always signals; the fresh-session behavior is the one
    regression-tested.

Gate v60 (the batch's first full gate) failed two pre-existing unit tests
sitting exactly on the changed behaviors; both were re-probed before
touching:
  - revert_buffer_reloads_non_utf8_file_as_raw_text: a UNIBYTE buffer
    suppresses every conversion except eol (fileio.c's comment, oracle
    rows byte-identical after the fix) -- the read is raw-text and even
    valid UTF-8 stays as its bytes.  The new latin-1 detection had leaked
    into unibyte reads, and the probe also exposed a PRE-EXISTING bug the
    fix removes: emaxx had been DECODING utf-8 into unibyte buffers
    (content (192 10) where GNU keeps (195 128 10)).
  - string_multibyte_conversion_helpers_match_fns_expectations: its
    latin-1 expectation for string-make-multibyte encoded the removed
    shortcut; the oracle answers the eight-bit character, and the
    roundtrip half of the test still holds.  Expectation corrected
    in-place with the reason.

New findings recorded while probing (all pre-existing, none fixed here):

126. `detect-coding-string' (detect_coding_names_for_text, coding.rs) still
     answers raw-text for non-UTF-8 8-bit text where the read-path detector
     now answers iso-latin-1; the two detectors should share the category
     logic.  OPEN.
127. Emaxx cannot represent supra-Unicode characters (private charset
     codepoints above 0x10FFFF) in strings or buffers at all; decode-char
     returns them as integers but text drops to raw-byte markers.  Structural
     -- same root as the 106 limitation above.  OPEN.
128. `encode-coding-string' of an unencodable character substitutes space
     for euc-jp (matching coding.c's iso-2022 default char) but the generic
     `_ '/charset arms still have their own substitution rules that were
     never swept against the oracle coding-by-coding.  OPEN.
129. iso-2022-7bit content is DETECTED by name (a hand-rolled ESC-window
     check) but never decoded: a file of ISO-2022 escapes reads back as its
     raw escape bytes where GNU decodes the kanji, the detected name misses
     the eol variant GNU appends for files, and the same detection fires for
     `decode-coding-string' where GNU's answers stay `undecided'.  All three
     pre-exist this batch (probed while auditing it).  OPEN.

## 2026-08-28 sjis/big5 batch (finding 107 closed)

Finding 107 is FIXED, and the big5 stub pair -- the same one-probe-value
disease next door -- went with it.  Everything below is oracle-probed.

  - decode-sjis-char/encode-sjis-char convert through the charsets of
    coding.c's Vsjis_coding_system, which is the LAST defined shift-jis
    system: japanese-shift-jis-2004 in a full load.  Their kanji bank is
    therefore JIS X 0213 plane 1 -- (decode-sjis-char #x8940) is 38498
    through JISX2131.map, and the euro sign, absent from JIS X 0208,
    ENCODES as #x8540 -- while the `sjis' STRING codec belongs to
    japanese-shift-jis and stays on JIS X 0208.  The same byte pair can
    answer differently between the primitive and the string decode; both
    answers are GNU's own.  Emaxx now tracks Vsjis/Vbig5 equivalents.
  - encode-sjis-char applies JIS_TO_SJIS to whatever code the charset
    search returns, halfwidth-katakana codes included: U+FF71's code
    0x31 becomes 0x70AF, an invalid SJIS code, exactly as GNU answers.
  - Charset conversion grew `:superset' support (jisx0213.2004-1 is the
    superset of jisx0213-a and jisx0213-1), riding the unify-charset
    state from the previous batch.
  - The sjis string codec: kana as code+0x80, JIS X 0208 through
    JIS_TO_SJIS, space for unencodable, raw-byte resync on invalid
    sequences; the big5 codec mirrors it through BIG5.map.
  - decode-big5-char reproduces coding.c's own bug: Fdecode_big5_char
    masks the second byte with 0x7F before validating, so half of Big5
    (#xA4A4 among them) signals "Invalid code" while encode-big5-char
    happily produces those codes.  Asymmetry oracle-confirmed.

Disclosed divergences (not claimed as fixed):
  - GNU's unencodable path in Fencode_sjis_char reads a NULL charset
    pointer: (encode-sjis-char #xA5) ABORTS the oracle binary (SIGABRT)
    and other unencodable characters return garbage.  Emaxx signals the
    error the docstring promises.  Untestable against the oracle.
  - Unmapped two-byte codes decode in GNU to supra-Unicode codepoints
    ((decode-coding-string "\xED\x40" 'sjis) is 1318992); emaxx strings
    cannot hold them and fall back to raw-byte markers -- the same
    finding-127 limitation as euc-jp.

## 2026-08-28 gate profile change (no findings; recorded for gate integrity)

The full serial gate moved from cargo's dev default to a dedicated
`[profile.gate]` (Cargo.toml): release-grade optimization with
`debug-assertions = true` and `overflow-checks = true` kept ON.  This
is a speed change, not a rigor change, and the claim was verified the
strong way: on the Linux container the IDENTICAL tree (finding-107
batch) ran the full gate under both profiles and produced the
identical 44-name environmental failure list, with everything else
green both times.  The overflow/assertion nets -- the reason the slow
profile was quietly valuable in a codebase full of ported C index
arithmetic -- are exactly what the new profile refuses to give up.
Wall clock on that container: ~3 h 28 m -> ~53 m.  The gate script in
docs/handover-2026-08-28.md now carries `--profile gate`.

## 2026-08-28 5b closes out (findings 110, 109, 102)

Finding 110, `garbage-collect': the fabricated zeros are replaced by
allocator bookkeeping -- GNU's own mechanism (gcstat), not a heap walk.
A first cut DID walk the reachable graph (the deep-clone root
enumeration, read-only); it was honest but cost ~55 ms per call, and
loadup.el runs `garbage-collect' after every file it loads, which
doubled boot and the full gate.  The landed design keeps the books at
the allocator instead: a live cons-cell counter maintained at
construction and Drop (Rust ownership IS the sweep), and Weak
registries of string allocations swept lazily at census time, with
amortized self-pruning so a session that never calls gc holds at most
~2x the live handles.  A census is ~5 ms; boot is within noise of the
old fabricated-zeros build.  What the numbers mean, stated rather than
implied:
  - USED counts are live allocations on this thread -- emaxx's truth,
    not GNU's heap state; no oracle row can pin them, so the regression
    test pins the SHAPE against the oracle (nine rows, order, arities,
    all integers) and the counts against sanity floors (a booted image
    holds >10k conses).
  - FREE columns are 0 truthfully: Rust ownership retains no free lists.
  - SIZE columns are this binary's real layout constants
    (size_of::<ConsCell>() and friends), so memory-report.el computes
    emaxx-true totals.
  - `floats' and `vectors'/`vector-slots' are 0 truthfully: emaxx
    floats are immediate f64s and vectors ride on tagged cons chains,
    so no float or vector HEAP OBJECTS exist -- their storage is cons
    cells, counted under `conses'.  `intervals' counts text-property
    spans (buffer and string).  Markers, overlays, frames, char-tables
    and records are id-indexed host state with no row of their own;
    records are never reclaimed, so what they reference stays counted.
  - Every value lives on one thread (Rc is !Send), so thread-local
    books are exact per interpreter thread.  All of this is in the code
    comments too.

Finding 109, keymap dispatch: the `add-keymap-witness' branch is gone.
The probe that motivated it was WRONG twice over: GNU 30.2's
current_active_maps (keymap.c:1657) never suppresses local or minor
maps under overriding-terminal-local-map (it rides on top), and
overriding-local-map suppresses them only while the terminal map is
nil.  Both `key-binding' and read_key_sequence share that one
construction (keymap.c:1840, keyboard.c:10200); where-is searches with
the overriding maps out of force (keymap.c:2653), and command-remapping
with them in force (keymap.c:1245).  Dispatch, the current-active-maps
primitive (whose OLP argument was previously ignored), where-is and
command-remapping now all route through one ported constructor, and the
`local-map' text property stands in for the buffer's local map as
get_local_map does.  Six scenarios oracle-matched, witness composition
included.

Finding 102, the directory family: data-directory, doc-directory and
installation-directory are epaths-style constants derived from the
pinned sibling GNU checkout -- the rule source-directory's own comment
already stated -- and EMACS_TEST_DIRECTORY no longer reaches any dumped
path variable.  emacsclient-program-name is the bare "emacsclient": the
oracle answers that even in an uninstalled build with lib-src/emacsclient
present, so the old lib-src derivation was wrong twice.  The regression
test sets a hostile EMACS_TEST_DIRECTORY at a fake repo layout and
asserts nothing moves, then matches the whole family against the oracle
row.


## 2026-08-29 finding 130: the silent load-path fallback

`emaxx_upstream_load_path' (compat.rs:461) asks the ORACLE BINARY for
its load-path and, if that probe fails for any reason, silently falls
back to `repo_local_elisp_load_path' -- a manual tree walk that does
not produce the same list (it missed `language/misc-lang' outright).
Discovered while standardizing the Linux gate environment: the
unprivileged gate user had a 1024 open-file limit, the parallel suite
exhausted it, oracle spawns failed with EMFILE, and four boot-heavy
tests flaked with "Cannot open load file" -- the fallback had changed
what the interpreter booted, with nothing in any log saying so.  The
fallback itself is legitimate (the standalone editor boots through it
when no oracle binary exists), but under the HARNESS a silent
degradation of the boot tree is a measurement hazard: a subject that
boots different Lisp than the oracle can mismatch or match for the
wrong reasons.  Not fixed here; recorded for the harness-integrity
queue.  The gate script now raises the fd limit, which removes the
trigger but not the hazard.


## 2026-08-29 finding 115 closed: the frozen superset check

The frozen battery pinned the manifest bytes (sha-256), the counts
(518 files / 1 load error / 7883 outcomes), and proved manifest ⊆ run
per file for both runners.  The un-checked direction was run ⊆
manifest: when upstream's pinned selector starts yielding outcomes the
manifest does not carry (a test added to an existing file),
`filter_report_by_exact_names' silently dropped them from both
reports before comparison -- drift that is score-inflating by
construction, since a new upstream test emaxx would fail simply
stopped being counted.  Frozen runs now refuse to proceed when either
runner produces an unmanifested selected outcome, naming the tests and
the regeneration recipe.  The check deliberately compares selected
OUTCOMES, not discovery: 53 manifest entries legitimately select
nothing (all-:expensive/:unstable files), and discovery still sees
those tests.  With the sha pin closing the "edited manifest" direction
and this check closing the "stale manifest" direction, finding 115's
regeneration-freshness gap is closed; the C-primitive and arities
manifests keep their separate fresh-regeneration gates.


## 2026-08-29 findings 112/121 closed: the obarray gap, by mechanism

Measured by dumping both binaries' `mapatoms' output to files under
LANG=C batch (never feeding those files back as expectations -- the
worklists steered WHERE to look; every fix below is a mechanism ported
from GNU source).  Starting point: 17,015 GNU names, 13,557 emaxx names,
3,908 missing.  Four mechanisms:

1. **Reader literals** (3,908 -> 1,925): `intern_symbols_in_value'
   walked conses, symbols and string properties but no ReaderForm --
   symbols living only inside `#[...]' compiled constant vectors,
   `#s(...)' hash tables/records, char-tables, or circular labels were
   interned on no path.  lread.c interns at every `read_symbol',
   whatever literal it is inside.  This was finding 119's own disclosed
   residue.
2. **Coding-system subsidiaries** (-793): coding.c's make_subsidiaries
   interns NAME-unix/-dos/-mac for every base coding system and alias
   with undecided eol; emaxx's define-coding-system(-alias) now does the
   same.
3. **defsubr names**: lread.c's defsubr interns every C primitive's
   name at image build; emaxx registers the same committed DEFUN
   contract surface its dispatch is gated against (arity-Some entries
   of generated_gnu_c_primitives.rs).
4. **DEFSYM names** (net effect with 3: 1,132 -> 124): a new generated
   manifest, generated_gnu_c_defsyms.rs, from the same source-contract
   convention as the DEFUN manifest ("GNU Emacs 30.2 src/*.c DEFSYM
   declarations", regeneration script in compat/).  Registration
   filters to files the oracle build compiles, derived from the DEFUN
   manifest's availability facts, with other window systems'
   sources (android*/w32*/haiku*/pgtk*, xfns.c, xmenu.c) excluded by
   the oracle-build-contract taxonomy.

En route, two REAL event divergences surfaced and are fixed with
oracle-probe verification (16-row table byte-identical):

- `define-key'/`lookup-key' did not convert Lucid-style event lists:
  `[(control meta shift kp-9)]' and `[C-M-S-kp-9]' named different
  bindings (GNU: the same one, keymap.c Fdefine_key/lookup_key_1), and
  a DEF vector opening with a cons (XEmacs-style macro) was stored
  unconverted where GNU converts each event.  bindings.el's keypad
  loop therefore produced a function-key-map unreachable by canonical
  event symbols -- 245 modifier event names absent, and lookups
  answering nil where GNU answers the translation.
- `event-convert-list' dropped the control modifier where keyboard.c's
  make_ctrl_char keeps it (C-9 = ?9 with the control bit, not bare
  ?9), lost the shift bit when folding control onto a shifted letter,
  and mis-folded `?', space and `@'.

Residual 124 (0.7% of GNU's names), by class, all C intern-loops
outside DEFSYM/DEFUN declarations: font.c style tables (weights,
slants, widths: `extrabold', `demibold', ...), coding.c's
`coding-category-*' name table, process.c's socket-option table (the 7
remaining keywords), inotify/kqueue event names, native-comp unit
names, and font family strings.  Each belongs to a subsystem port and
is left OPEN as the successor entry to 121's number.

Contract note: emaxx's image answers `(fboundp 'x-create-frame)' t per
the Darwin oracle contract, so its loadup replay includes the
`x-create-frame'-gated preloads (fringe, image, fontset, dnd,
tool-bar, mwheel, scroll-bar); the Linux comparison oracle is a
no-window-system build that skips them.  Their symbols therefore show
as "extra" against the LINUX dump (749 names) while matching the
pinned Darwin-image contract -- the same host-vs-contract policy
question as mule-util (see docs/oracle-build-contract.md), not
invented names: the original zz-/emaxx-contamination check stays
clean.


## 2026-08-29 tty merge audit (tty-frontend 16826b4 -> main)

All seven tty-side commits since the 196d80f merge-base were audited
before this merge: no oracle copying, no harness gaming, no boundary
violations found.  The new ttydiff comparator is stricter than its
predecessor (verbatim row/attribute/mode-line/echo/cursor comparison
plus filesystem snapshots), the package-lifecycle test runs BOTH
binaries live on identical synthetic fixtures and compares stdout
byte-for-byte, and the core fixes carry GNU C anchors (write-region
supersession under `create-lockfiles' nil, yes-or-no-p for overwrite
confirmation, insert-file-contents replacement point policy, where-is
candidate ordering, kbd-macro boundary truncation, minibuf.c prompt
interval copying).  Two items noted, kept, and worth future scrutiny:

- tty.rs `deferred_mode_line_point': reproduces GNU's stale mode-line
  redisplay artifact (point-based constructs keeping their pre-motion
  value after same-row motion inside invisible text, until the next
  input) by modeling WHEN GNU's incremental redisplay skips, not by
  porting the matrix machinery itself.  Faithful in effect and
  narrowly guarded (same buffer, cursor row, window start, and modiff
  required); a mis-generalization would diverge on new scenarios and
  the battery would catch it.  Mechanism-approximate, disclosed here.
- regexp.rs backward-search bound handling: a bounded-prefix retry
  guarded by `pattern_end_depends_on_following_context', which
  declines the shortcut for every end/word/symbol assertion rather
  than risk inventing context.  Self-limiting; failure mode is the
  prior full-context behavior.


## 2026-08-29 finding 131: the tty quote-display chain (homoglyph face)

The LANG=C differential battery's `hscroll-disabled' scenario (the one
that lands in *Disabled Command* help) shows GNU painting the
substituted apostrophe in "Here's" with the `homoglyph' face (fg1)
where emaxx paints default -- one cell, attribute-only, text equal.
The mechanism is a three-part GNU chain emaxx does not implement:
non-batch startup forces `internal--text-quoting-flag' t, so help text
keeps CURVED quotes in the buffer; `startup--setup-quote-display'
(startup.el:978) installs `standard-display-table' entries mapping
each curved quote to an ASCII glyph code carrying `homoglyph'; and the
display engine honors display-table glyph codes, emitting the
replacement char with its face.  Emaxx instead answers "grave" from
the locale flag, so its help buffers contain straight/grave quotes
directly -- the same visible glyphs with no face, which is why every
other row compares equal and only this attribute differs.  Finding 95
already recorded the doc.c side of this interplay.  OPEN: the honest
fix is the whole chain (flag, GNU's own setup function, display-table
glyph rendering in the tty renderer), not a face special-case on
quote characters.


## 2026-08-29 finding 132: interactive-session defects the LANG=C battery exposed

Running the full 210-scenario differential battery on the standardized
Linux environment (fresh HOMEs, LANG=C) surfaced four pre-existing
emaxx defects -- all verified against the PRE-merge tty tip (16826b4),
so none is a regression from the main merge:

1. **epg subprocess conversation hangs the tty command loop.**
   `package-import-keyring' (epg's gpg --import dialogue over
   accept-process-output) never returns inside an interactive `M-:',
   wedging the minibuffer; `call-process' and `epg-find-configuration'
   are fine.  This is why both package-menu scenarios diverge: their
   setup's `package-refresh-contents' imports the keyring on a fresh
   HOME.  A machine whose gpg state skips the import never sees it.
2. **`M-:' on a void variable wedges instead of erroring.**  GNU exits
   the minibuffer and shows "Symbol's value as variable is void";
   emaxx leaves the prompt stuck (a valid expression submits fine).
3. **copy-file ignored KEEP-TIME** (fileio.c copies the source's
   mtime/atime; dired-copy-preserve-time rides on it) -- FIXED in this
   batch; the dired-copy-rename-delete scenario pinned it.
4. **Unencodable characters print raw instead of glyphless escapes**:
   GNU renders o-umlaut on a LANG=C tty through
   `glyphless-char-display' (the default table produces `\u00F6');
   emaxx emits raw UTF-8 bytes.  Finding 131's display-substitution
   family.  RESOLVED 2026-08-29 by issue #50: tty redisplay now tests
   scalars against the active terminal coding, applies explicit and
   no-font char-table methods with the `glyphless-char' face, and maps
   the expanded cells through point motion, wrapping, hscroll, line
   numbers and face spans.  Four permanent interactive scenarios pin
   ten screen/cursor/attribute checkpoints; all ten match GNU exactly
   under both LANG=C and UTF-8.

Items 1 and 2 stay OPEN as tty-side work.  Finding 131 and the addendum
below are unchanged; this resolution removes only item 4 from the
remaining divergence list.

Addendum: a fifth pre-existing item in the same battery —
`find-alternate-file-missing-revisit' checkpoint 6 shows GNU deciding
utf-8 (mode-line `U') for a re-read file that emaxx leaves undecided
(`-'): revisit-time coding detection does not update
`buffer-file-coding-system' from the decoded content.  Verified
diverging pre-merge as well (at an earlier checkpoint, additionally
masked by the %z renderer defect fixed in this batch).  OPEN.


## 2026-08-29 per-platform oracle contracts (phase 1) and the oracle rebuild

The single-platform scoring contract is closed: each platform now pins
its own authoritative manifests, selected at gate time by the oracle's
OWN reported `system-configuration' — a Linux run can only score
against the Linux contract, a Darwin run against the Darwin one, and
any other configuration refuses.  The Linux oracle was rebuilt (same
pinned 30.2 source) from a tty-only build into an X11/cairo build with
HarfBuzz, tree-sitter, the full image stack and native-comp — the
honest Linux peer of the Darwin NS oracle — and the Linux C-primitive
manifest was regenerated from it (1,446 available primitives; Darwin
pins 1,420).  The arities manifest proved byte-identical when
generated from either oracle, so it remains ONE shared file whose
regeneration gate now runs on both platforms; if the platforms ever
drift it fails loudly and the manifest splits at that moment.  Scores
are per-platform and never comparable across platforms.  The gate
baseline entered its second era: 30 -> 16, with ten native_* probes
now real measurable divergences instead of oracle build gaps (each
recorded in the baseline doc's fix queue).  Environment note: the
regeneration gates run rustfmt, which the unprivileged gate user could
not reach — a latent gap the old always-refusing Linux path never
exercised; rustfmt is now installed system-wide for the gate.


## 2026-08-29 finding 133: where-is full-list ordering regressed by ae8f93b

The first authoritative post-merge Darwin frozen run (7,633/7,883)
carried exactly one regression against the pre-merge 7,620/263
baseline: `test-non-key-events' in test/src/keymap-tests.el.  Diffing
the two runs' artifacts proved everything else moved green (five files
fully fixed, and `semantic-utest-ia-texi' — briefly suspected as a
regression — was already failing pre-merge).

Root cause: tty-round commit ae8f93b changed `where_is_binding_rank'
from (length, symbolic) to (symbolic, length), citing keymap.c's
`preferred_sequence_p' as if it ordered the FULL result list.  It does
not: keymap.c's static where_is_internal walks Faccessible_keymaps
breadth-first, so the full list is ordered by sequence length no
matter what events a sequence carries (oracle: a symbolic [f7] answers
before the two-character C-c 8), and `preferred_sequence_p' is
consulted ONLY by the FIRSTONLY selection (where a character sequence
does beat a shorter symbolic one).  The tty commit fixed its FIRSTONLY
scenario by sorting the whole list — a mechanism misattribution the
merge audit passed because it carried a plausible C anchor; the frozen
run caught it within one cycle, which is the system working.

Fix (this batch): rank restored to (length, symbolic-tiebreak) with
the tie break now justified by the true mechanism (within one map the
char-table sweep answers before the symbol alist), and
`preferred_sequence_p' ported literally for the FIRSTONLY path
(including its ~CHAR_META masking and the rank-2 early answer that
makes a nil `where-is-preferred-modifier' still prefer character
sequences).  Oracle-verified: full-list order, FIRSTONLY selection
across lengths, nil-preferred-modifier selection, and the
test-non-key-events replay are all byte-identical now.  Residual
approximation, disclosed: same-length sequences from DIFFERENT prefix
maps tie-break by the stable sort's collection order (depth-first)
rather than GNU's breadth-first map order; no observed scenario
distinguishes them yet.


## 2026-08-29 finding 134: named :service strings resolved to port 0

The first Linux frozen run ABORTED at test/src/process-tests.el: the
file's top-level `(dns-query "google.com")' (dns.el passes
`:service "domain"') hit emaxx's make-network-process, which parsed a
non-numeric :service string with `.parse::<i64>().ok()' and fell back
to port 0 — process-send-string then died with sendto's EINVAL,
aborting the load, and frozen mode's outcome-coverage contract
correctly refused to score a 0-outcome file (finding-115 discipline
catching a real defect rather than hiding it).  GNU resolves service
names through the services database (getaddrinfo with the socket type
as hint when a host is given; getservbyname otherwise, process.c) and
signals "HOST/SERVICE Servname not supported for ai_socktype" for a
name the database does not know; emaxx silently built a
port-0/random-port socket instead, on every platform — Darwin's run
survived only because macOS's dns path happened not to explode the
load there.

Fix: :service strings that do not parse as integers now resolve via
getservbyname with "udp"/"tcp" chosen by :type, and unknown names
signal the getaddrinfo diagnostic (loopback-of-family host prefix when
no :host, as GNU's server default produces).  Oracle-verified
byte-identical: "domain" resolves to remote port 53, the unknown-name
error text matches, and upstream process-tests.el now loads all 37
tests.  The aborted Linux frozen run is rerun from scratch after this
fix; no score from the aborted run is recorded anywhere.


## 2026-08-30 second tty merge audit (tty 169dd51 into main eb2ee6c)

Three new substantive commits audited before merging, with finding
133's lesson applied — C anchors verified against source and oracle,
not trusted:

- f7a7d3a (live package archive canaries): the canary tool is honest
  by construction — opt-in `--live', never part of the deterministic
  gate, both editors fed identical live inputs, drift categorized,
  signature checking never disabled.  The bundled runtime work
  verified faithful: the gnutls peer-certificate details match
  gnutls.c exactly (gnutls_hex_string's colon-separated serial, UTC
  %Y-%m-%d validity dates, live libgnutls calls throughout), and the
  symbols-with-position evaluator dispatch was probed against the
  oracle byte-identical in both enabled and disabled modes (call,
  variable lookup, indirect-function).
- 2197a35 (package-vc/use-package): runtime changes are GNU error
  faithfulness ("Searching for program", "Setting current directory"
  file-error prefixes) plus contract batteries.
- 169dd51 (unencodable tty rendering): term.c's glyphless machinery —
  the \u%04X / \U%06X hex-code split, acronym bracketing, method
  names — matches produce_glyphless_glyph; scenarios are pinned by
  live GNU PTY comparison.  Resolves the finding-132 item 4
  (glyphless escapes) with its own close-out above.

Merge-commit forensics (the check that exists because substantive
changes can hide in merges): 3dbc9bf recomputes to its recorded tree
exactly.  fa76ad1 does NOT — it embeds 49 lines beyond the mechanical
merge: a DEFSYM-manifest fresh-regeneration anti-cheat gate.  The
content STRENGTHENS enforcement (hand-editing
generated_gnu_c_defsyms.rs could previously fabricate a GNU-owned
symbol past the DEFUN and arity gates) and is accepted; the practice
is flagged here — substantive code inside a merge commit evades
commit-by-commit review and was caught only by merge-tree
recomputation.  Open question carried to the gate: the new gate
demands byte-identity of the DEFSYM scan against `../emacs' source,
and the platforms pin different source commits (636f166c Darwin,
6ee5c13 Linux); the Linux gate run on this merge decides whether the
scan is cross-platform stable or the manifest must split per platform
like the primitives manifest did.

The finding-133 where-is fix survives the merge: the automatic merge
kept the corrected rank (her branch had not touched it since
ae8f93b), and the full 14-cell probe battery (non-key-events replay,
mixed-length ordering, FIRSTONLY selections) reruns byte-identical to
the oracle on the merged build.


## 2026-08-30 finding 135: frozen-run scratch file committed at repository root

The adversarial audit before merging main `81799ed` into tty found an
unreferenced repository-root file named from a long punctuation string.
Its contents are the `tramp-test33-file-name-substitute-in-file-name`
input at `test/lisp/net/tramp-tests.el:7712-7715`, and it entered main in
the Linux frozen-baseline commit `eb2ee6c` beside the intended baseline
documents.  No source, test, manifest, or baseline refers to it.  It was
a frozen-run scratch artifact swept up by broad staging, not a fixture.

The tty merge removes the artifact before committing.  The baseline JSON
and its documented provenance are left unchanged; neither depends on the
scratch path or file.


## 2026-08-30 finding 136: named-service fix hardcoded Linux and tested only GNU

The same pre-merge audit reran finding 134's named-service test against
the pinned Darwin oracle.  It failed before Emaxx was exercised because
the expected unknown-service diagnostic was Linux's
`Servname not supported for ai_socktype`; Darwin's `gai_strerror` says
`nodename nor servname provided, or not known`.  Production contained
the same Linux literal.  It also claimed to follow process.c's
getaddrinfo/getservbyname split while calling getservbyname for every
named service and resolving the host separately afterward.

There was a second audit defect: the new test invoked only GNU and
compared GNU with the hardcoded string.  It never evaluated the form in
Emaxx, so a platform whose GNU happened to print the pinned words could
pass even if the production result diverged.

Fixed before the merge gate.  Internet-family calls now follow
process.c's actual order: nil `:host` becomes the family loopback, then
host and named service are resolved together with getaddrinfo using the
socket type and family hints.  Resolver errors use the host platform's
gai_strerror text.  The permanent test obtains the complete result from
the local pinned GNU oracle, evaluates the identical form in Emaxx, and
compares the Lisp values without message normalization.  It covers a
named UDP service, an unknown client service, and an unknown server
service; all three rows match on Darwin after the correction.
