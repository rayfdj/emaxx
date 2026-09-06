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
| 86 | color-gray-p/color-supported-p/color-distance/color-values-from-color-spec bypass GNU's Lisp color path | FIXED 2026-09-04 (xfaces.c port: tty-defined-color, Riemersma distance with METRIC, parse_color_spec) |
| 87 | `\u{2620}` hardcoded into the word class to satisfy one upstream test | FIXED (removed; word/space now resolve through the syntax table everywhere) |
| 88 | `[[:space:]]` was a fixed Unicode property, not the whitespace syntax class | FIXED (regex-emacs.c:151) |
| 89 | `[:punct:]` still syntax-blind for non-ASCII | FIXED 2026-09-04 (regex-emacs.c ISPUNCT: non-word syntax beyond ASCII) |
| 90 | text-quoting-style ignored the locale, so every quoted message diverged under the harness's LANG=C | FIXED |
| 91 | interactive-form/commandp missed compiled OClosures (advised functions) | FIXED |
| 92 | message, void-function/void-variable messages ignore text-quoting-style | FIXED 2026-09-04 (format-message and substitute-command-keys on error messages) |
| 93 | require's load-path branch names the feature where GNU names the resolved file | FIXED 2026-09-04 (names `(car (car load-history))`) |
| 94 | harness let LC_ALL/LC_CTYPE override its own LANG=C, retiring the grave path from measurement | FIXED |
| 95 | default_to_grave_quoting_style's standard-display-table branch unimplemented | FIXED 2026-09-04 (display-table branch ported) |
| 96 | no DEFVAR_BOOL coercion: bool-typed variables read back the raw value | FIXED 2026-09-04 (177 DEFVAR_BOOL names coerced; makunbound detaches the slot) |
| 97 | commandp returns t where GNU signals on an interactive-form property | FIXED 2026-09-04 (Fcommandp order; signals on the property) |
| 98 | the 7595 denominator excluded 3 files dropped by a 20s inventory cap 9x tighter than the run's own 180s default | FIXED 2026-08-26 - regenerated to 7883 |
| 99 | make-thread: the classifier claim was stale; the real defects were join/bindings/handlers | FIXED 2026-08-27 - three semantics fixes; interleaving stays open as 84 |
| 124 | timers fire inside thread-join and see the joiner's let bindings; GNU runs none there | FIXED 2026-09-04 (child sleep-for runs due timers on its own specpdl) |
| 125 | thread-signal to the main thread prints eagerly and drops the data; GNU queues an event | FIXED 2026-09-04 (thread-event queued and dispatched through special-event-map) |
| 126 | detect-coding-string still answers raw-text where the read path now answers iso-latin-1 | FIXED 2026-09-04 (coding.c detect_coding_system ported: categories, representatives, all detectors, eol) |
| 127 | supra-Unicode characters (private charset codepoints) cannot live in strings/buffers | OPEN (structural) |
| 128 | encode substitution rules for the generic/charset arms never swept against the oracle | FIXED 2026-09-04 (ISO-2022 encoder ported; raw-text/undecided write internal bytes; unencodable-char-position real) |
| 129 | iso-2022-7bit detected by name but decoded as raw bytes; string-vs-file detection differs from GNU | FIXED 2026-09-04 (ISO-2022 decoder with charset properties; string fast path vs region/file detection as coding.c) |
| 130 | a failed oracle load-path probe silently falls back to the manual tree walk, changing what the harness boots without a trace | FIXED 2026-08-29 in code (`9c89a7c` refuses the silent fallback); row closed 2026-09-04 |
| 146 | rendering a non-selected mode line overwrote that window's independent point slot | FIXED 2026-09-02 |
| 147 | graphical fringe display strings leaked their source text onto TTY frames | FIXED 2026-09-02 |
| 148 | motion between edits left undo's point-before-command record stale when a boundary already existed | FIXED 2026-09-02 |
| 149 | the TTY differential reused the host temporary namespace across killed editor sessions until Org's 1000 `babel-stable-N` names were exhausted (harness defect, recorded in the 2026-09-03 tty-frontend merge section) | FIXED 2026-09-03 |
| 150 | the `supersession-accept-revisit` TTY scenario sent an extra `y` after a successful save and manufactured a divergence (harness defect, same section) | FIXED 2026-09-03 |
| 151 | status_notify's drain of an exited process's remaining output is not modeled: during a JUST-THIS-ONE wait GNU still delivers a distractor's leftover output once it exits, Emaxx never does | FIXED 2026-09-04 (status_notify drain during JUST-THIS-ONE waits) |
| 152 | `(sleep-for 0)` runs due Lisp timers; Fsleep_for returns without entering the wait for a non-positive duration | FIXED 2026-09-04 (Fsleep_for returns for a non-positive duration without the wait) |
| 153 | batch `read-from-minibuffer` reads stdin even while `executing-kbd-macro` is non-nil, where read_minibuf takes the full path; the accepted-default history push is gated on TTY-reader presence rather than that condition | FIXED 2026-09-04 (read_minibuf full path under a macro; history push follows it) |
| 154 | `expand_file_name_runtime` resolved a nil DEFAULT-DIRECTORY against the process cwd; exposed as a copy-family regression by the 2026-09-04 Linux frozen run | FIXED 2026-09-04 |
| 155 | Linux `process-attributes` returns 15 of GNU's 31 keys (no pcpu, pmem, utime/stime/cutime/cstime/ctime, page-fault counts, tpgid, ttname, nice, pri, thcount), so Proced's %CPU refinement fails with `(wrong-type-argument integerp nil)`; the Darwin skip hid it | FIXED 2026-09-04 (sysdep.c /proc port; proced-tests 6/6 on Linux) |
| 156 | `make-process` and `call-process` never searched `exec-path`/`exec-suffixes` (openp with X_OK): an empty `exec-path` still ran the program, misses surfaced as the spawn failure instead of "Searching for program" with openp's errno, a directory named as the program was a `(error "Permission denied (os error 13)")`, argv[0] was the bare name rather than the resolved path, EACCES rendered as `file-error` instead of `permission-denied` in every `report_file_errno` path, `file-executable-p` was nil for directories, an unexecutable absolute program was a synchronous host-error string instead of GNU's pty-path child exiting 127/126 (with the perror line) or the pipe-path "Doing vfork" signal, and glibc's `execvp` ran ENOEXEC files through `sh` where GNU's `execve` fails | FIXED 2026-09-04 |
| 157 | `process-attributes` of the Emaxx process itself reports `state` "S" and `thcount` 2 where GNU reads "R" and 1: Lisp runs on a spawned thread while /proc/PID/stat describes the blocked main thread | OPEN (architectural, recorded 2026-09-04) |
| 158 | `set-default-file-modes' only recorded a number: the process umask never changed, so `make-directory', `write-region' and subprocesses ignored `with-file-modes', and `make-temp-file' made 0644 files and 0755 directories where gen_tempname makes 0600/0700; server.el's server-ensure-safe-dir refused the 0755 temporary directory ("accessible by others"), failing all seven server-tests | FIXED 2026-09-05 (umask port; server-tests 4/7 matching, the rest are 159) |
| 159 | `make-terminal-frame' is a stub that signals "Unknown terminal type": Emaxx has one terminal and one frame (a single window tree), so `emacsclient -c' cannot get the tty frame GNU's init_tty opens on the client's pty; server-tests/emacsclient/create-frame, server-force-stop/keeps-frames and server-start/stop-prompt-with-client fail (the client gets `-error Unknown terminal type' and exits) | OPEN (structural: multi-terminal tty frames, recorded 2026-09-05) |
| 160 | `comp--install-trampoline' with a plain (non-native) subr as TRAMPOLINE signals `(wrong-type-argument subrp ...)'; comp.c's CHECK_SUBR accepts any subr and patches the link table with its C function pointer (a Rust primitive has no address to install) | OPEN (recorded 2026-09-05 at the native-comp merge) |
| 161 | `comp--compile-ctxt-to-file0' called without a compilation context signals `(native-ice "comp-ctxt is nil")'; comp.c reaches `comp-ctxt-speed' on nil and signals `void-function' (comp.el unloaded) or `wrong-type-argument' | OPEN (recorded 2026-09-05; error path only) |
| 162 | Batch startup takes about 20 s per process after the native-comp merge (about 9.5 s before it, GNU 0.03 s): the reconstructed image runs GNU's normal-top-level on every start because no portable dump exists | OPEN (structural, the branch's own handover names portable dumping as the missing milestone) |
| 163 | `(featurep 'x)' is nil; the Linux oracle (HAVE_X_WINDOWS) provides `x' at startup.  Lisp that branches on the feature takes the non-X path in Emaxx | OPEN (recorded 2026-09-05; not flipped mid-verification because Emaxx has no X primitives behind the feature) |
| 164 | An asynchronous pipe process without `:stderr' gets two pipes (stdout, stderr) whose bytes are appended per poll; process.c gives the child one descriptor (forkerr = forkout), so GNU delivers the two streams in the order written.  Synchronous `call-process' now shares one descriptor as callproc.c does | OPEN (recorded 2026-09-05; async path unchanged) |
| 165 | Backtraces record unevaluated frames only for `cond', `let', `let*', `setq', `while' and in-progress calls; eval_sub records one for every special form (`condition-case', `unwind-protect', `catch', `save-excursion', ...).  A handler-bind handler or `debug-early' sees those frames in GNU and not in Emaxx | OPEN (recorded 2026-09-05; shape divergence, no corpus test pins it) |
| 166 | After batch startup GNU's empty *Messages* buffer answers `(buffer-modified-p)' t (loadup's messages were logged and erased before the image was dumped); Emaxx's fresh *Messages* answers nil until the first message | OPEN (recorded 2026-09-06; the flag is a dump artefact, no corpus test pins it) |
| 167 | `time-convert' with FORM t (or nil under a nil `current-time-list') answers `(TICKS . HZ)' with the HZ GNU decoded from the input (1000000000000 for a four-element list, 2^k for a float, the pair's own HZ); Emaxx reduces the fraction first, so `(time-convert 1.5 t)' is `(3 . 2)' where GNU has `(6755399441055744 . 4503599627370496)'.  `time-add'/`time-subtract' results are reduced the same way | OPEN (recorded 2026-09-06; `current-time' and the nil-FORM list answer are fixed, the rational HZ needs timefns.c's lisp_time carried unreduced through the module) |
| 100 | GnuTLS digest catalogue was transcribed while cipher/mac lists were queried live | FIXED 2026-08-26 - dlopen'd gnutls_digest_list |
| 101 | operating-system-release hardcoded this host's uname -r | FIXED 2026-08-26 - reads uname(2); the entry states what its test can and cannot show |
| 102 | data-directory family derived from EMACS_TEST_DIRECTORY | FIXED 2026-08-28 - epaths-style sibling-checkout constants, oracle-matched |
| 103 | set-network-process-option fabricated success and never read the option | FIXED 2026-08-26 - real setsockopt, 20 cases oracle-matched |
| 104 | get-unused-iso-final-char returned a constant and swallowed validation | FIXED 2026-08-26 - scans the charset registry, 10 cases oracle-matched |
| 105 | max-lisp-eval-depth ignored: let-bindings invisible, excessive-lisp-nesting never raised | FIXED 2026-08-27 - mirrors eval.c:2504; funcall site tracked as 122 |
| 122 | the depth counter has no counterpart to GNU's second increment site in Ffuncall | FIXED 2026-09-04 (Ffuncall increment counted; contract expectation corrected to the oracle) |
| 123 | EMACS_TEST_DIRECTORY shadowed 11 core libraries (5 of them in the 397-name sweep), putting at least 324 measured outcomes (4.1%) at risk | FIXED 2026-08-27 - standard library ordered first, sweep 5 -> 0 |
| 106 | decode-coding-string falls back to identity for every unimplemented system | FIXED (euc-jp real; file reads consult the alist; one disclosed limit) |
| 107 | decode-sjis-char/encode-sjis-char implement exactly one probe value | FIXED 2026-08-28 (big5 twins included; two GNU crash/UB paths disclosed) |
| 108 | file-name-case-insensitive-p constant nil made a self-comparing test pass trivially | FIXED 2026-08-26 - pathconf walk, 18 cases oracle-matched |
| 109 | native keymap dispatch branches on add-keymap-witness, a symbol private to subr.el | FIXED 2026-08-28 - keymap.c:1657 rule ported; witness inert, 6 scenarios oracle-matched |
| 110 | garbage-collect returns a correctly-shaped alist with every count fabricated as 0 | FIXED 2026-08-28 - live reachability census; shape oracle-matched, counts are emaxx truth |
| 111 | network-interface-info was a bare nil beside a real network-interface-list | FIXED (macOS) 2026-08-26 - real ioctls; still nil on other platforms |
| 112 | intern-soft invents keywords nobody has interned; tightening it regresses 288 of GNU's 429 | FIXED 2026-08-29 - the mentioned-names hole is filled computed-not-copied; missing keywords 288 -> 7 (process.c socket-option table); see the obarray close-out |
| 119 | --eval did not intern the symbols it read, unlike file loading | FIXED |
| 120 | eval-region with a custom load-read-function re-interns symbols GNU leaves unintern'd | FIXED 2026-09-04 (readevalloop: load-read-function, no re-interning) |
| 121 | the obarray is ~4400 symbols short of GNU's; intern-soft's inference is what hides it | FIXED 2026-08-29 - missing names 3,908 -> 124 vs the Linux oracle; four computed mechanisms; residual classes named in the close-out |
| 113 | the unit gate never ran under LANG=C, hiding a class of locale/coding divergence from the environment actually measured | FIXED 2026-08-29 - LANG=C is the gate standard; runtime defects and locale-dependent test inputs were fixed, not baselined |
| 114 | a runner killed after writing its report still contributed every matching outcome to the headline numerator | FIXED |
| 115 | the frozen manifest has no fresh-regeneration gate, unlike the C and arities manifests | FIXED 2026-08-29 - manifest sha pin (item 21) + frozen superset check: run ⊆ manifest enforced per file, both runners |
| 116 | system-configuration drifts from the oracle's build-time triple as the host OS updates | FIXED 2026-09-04 (build-time config.guess triple embedded by build.rs) |
| 117 | the gate contains an intermittent test that fails up to 75% of runs under load, so "green" has always been partly luck | NOT REPRODUCIBLE on the Linux gate host 2026-09-04: 0 failures in 78 runs (40 of them at load 2.0-4.8, both on main `b50bdd2` before this delivery and on the finished tree); the 2026-08-27 rate stays on record as the earlier host's; reopen on recurrence |
| 118 | network-interface-list omits most interfaces: 3 where GNU reports 11 on the same host | FIXED 2026-09-04 (link-local rows, newest-first order) |

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



## 2026-08-30 finding 135: the load-error trace leaked into measured children

Six gv-tests frozen mismatches (and suspect siblings in bytecomp,
edebug, testcover, shortdoc — every test whose child process output is
compared byte-for-byte) shared one injected stderr line: `bytecode
operation Call(3) failed ... void: cl-no-applicable-method'.  The
harness sets EMAXX_TRACE_LOAD_ERRORS=1 on the measured emaxx runner
for its immutable-log diagnostics; test-spawned CHILD emaxx processes
inherit it, and the VM trace printed for errors that an outer
condition-case was about to absorb — errors GNU never shows.  The
line fired on EVERY emaxx boot: during the loadup replay of
cl-generic, a dispatch signals and is handled once per process
(invisible without the knob).

Fix: the trace now consults the active handler stack (GNU's
handlerlist mirror) and speaks only for errors no live
condition-case/handler-bind frame matches — errors headed to the
toplevel report, which is what the knob exists to explain.  Verified:
the boot line is gone under the knob, a genuinely unhandled load
error still traces, and a condition-case-wrapped one stays silent.
Disclosed tradeoff: a handler that matches but re-signals now
suppresses the trace for that error; acceptable for a
diagnostics-only channel.

OPEN sub-item (135b): the traced bootstrap event is itself a real
mechanism divergence.  GNU's cl-generic bootstrap resolves the
combine-methods circularity through the memoized under-construction
sentinel — cl--generic-build-combined-method signals
`cl--generic-cyclic-definition', caught by its own condition-case
(cl-generic.el:805-812) — while emaxx's dispatch concludes NO
applicable method and calls the not-yet-defined
cl-no-applicable-method, converging to the same built state by a
different error path.  Same observable output today, but the
dispatch/method-lookup difference that causes it is undiagnosed and
could surface elsewhere.  Queued.


## 2026-08-30 tractable-middle round 1: six mechanisms

Worked from the frozen artifacts' per-test details; every fix is a
GNU-source port verified against the oracle:

1. Finding 135 completed: beyond the handler-stack gating recorded
   above, main() now LATCHES EMAXX_TRACE_LOAD_ERRORS at startup and
   scrubs it from the environment — the harness sets the knob only on
   the measured emaxx runner, so Lisp getenv and child emacs processes
   now see exactly what the oracle runner's world shows, while the
   runner keeps its own diagnostics.  (EMAXX_DUMP_SOURCE_DIRECTORY is
   the remaining single-runner variable of this class; queued.)
2. Harness path-width artifact: ert explanations embed raw string
   LENGTHS, and "checkout-oracle" was one byte longer than
   "checkout-emaxx" — every length-sensitive explanation differed by
   exactly that byte.  Runner temp tags are now width-padded; all
   nine elisp-mode-tests xref mismatches were this and now match.
3. documentation now reads doc.c's autoload branch (third element of
   the autoload form, without resolving it) — shortdoc-tests' three
   failures were split-string on the nil this returned for
   `string-pad'; 5/5 matching now.
4. position-symbol accepts a symbol-with-pos as POS (data.c), which
   cconv's unused-variable rewrite relies on; the byte compiler's
   run-hook-with-args lexical-var warnings (bytecomp's trio) emit
   again because the compile no longer dies inside cconv.
5. The batch unhandled-error report is now debug-early.el's:
   "\nError: " + prin1 of symbol and data, then the backtrace under
   print-escape-newlines/control-characters/nonascii binds,
   bottoming out through load/command-line-1/command-line frames
   with canonical flag spellings.  Supporting print.c fidelity:
   octal control-character escapes now use octalout's exact width
   rule (following-octal-digit guard); the stray \r/\t/\b
   escape-newlines arms (GNU escapes only \n and \f there) are gone.
   eval.c parity: the funcall backtrace frame is recorded BEFORE
   function-cell resolution, so void-function reports carry the
   attempted call as their innermost frame.  A gv-tests child's
   entire failing-load output now diffs EMPTY against GNU's.
6. The load-path trace diagnostic now PEEKS at the captured
   backtrace instead of consuming it, so the toplevel report and the
   trace can coexist.


## 2026-08-30 round-1 self-audit (adversarial pass before the gate)

Where this batch could cheat, and why it does not:

- The harness's randomness canonicalization is the cheat-adjacent edge.
  Rules: (1) the harness's OWN per-runner scratch directory names
  (`emaxx-compat-<tag>-<pid>-<nanos>`) collapse to one token — these
  name which runner produced the path, so no faithful implementation
  could ever match on them; (2) `emacs-test-` followed by EXACTLY six
  alphanumerics collapses to `emacs-test-xxxxxx` — gen_tempname's
  shape, demanded of BOTH sides.  The second rule deliberately did NOT
  fire for emaxx until make-temp-name/make-temp-file-internal were
  ported to produce gen_tempname's six-character [a-zA-Z0-9] segment
  (they emitted 17-hex names; the shape divergence stayed a scored
  mismatch until the mechanism was fixed, which is the order the
  discipline requires).  Both rules apply identically to both
  runners' output; a wrong-shape name still scores as a divergence.
- make-temp-file-internal also gained fileio.c's creation semantics:
  O_EXCL-style create_new for files, mkdir-collision retry for
  directories, name-only for DIR-FLAG 0.
- The batch error reporter's bottom frames reconstruct command-line-1's
  argument list with canonical flag spellings (-l/--eval/-f).  The
  harness only ever passes those spellings; a user-typed variant
  (--load=X) would render canonically — disclosed approximation.
- The trace-knob scrub, pre-resolution backtrace frame, octalout port,
  position-symbol POS contract and autoload documentation branch are
  literal ports probed byte-identical against the oracle (the gv child
  scenario's entire failing-load output diffs empty).
- In-tree unit coverage for this batch is thin by design: the pinned
  upstream files themselves are the regression tests (gv-tests 8/8,
  elisp-mode 63/63, shortdoc 5/5 measured matching), and the oracle
  probes above are reproducible from the ledger.  Dedicated lib tests
  ride the next round.


## 2026-08-30 finding 134 corrected by collaborator audit (134b)

The named-:service port recorded above was mechanism-approximate in
two ways the round-1 self-audit missed, caught by the tty
collaborator's independent audit:

1. Production resolves the service via getservbyname UNCONDITIONALLY.
   GNU's split: with a host present, getaddrinfo resolves host and
   service together (fileio has no say); only the no-host path parses
   numerically and falls back to getservbyname.  The two agree on
   this box, which is how the shortcut survived its probes.
2. The unknown-name diagnostic is SYNTHESIZED from glibc's wording
   and the permanent unit test pins that literal — verified
   byte-identical against the Linux oracle only, where both sides
   are glibc.  Darwin's gai_strerror words EAI_SERVICE differently,
   so the test is Linux-blind and the message is not the live
   library's.  Same failure class as the Darwin-only contracts this
   project just dismantled, committed while dismantling them.

Fix ownership: the collaborator is implementing the
host-present/host-absent split with the live gai_strerror text and a
platform-oracle-driven test (no message normalization).  Audited on
arrival like any other commit; no parallel fix here.  Lesson recorded
for future self-audits: "byte-identical against the oracle" is only
evidence for the platform that oracle runs on — a pinned literal must
be platform-derived or the mechanism must produce it live.

## 2026-08-30 tractable-middle round 2 (mechanism ledger)

All items below are C-mechanism ports probed against the GNU oracle
before and after; none seed expectations from oracle runtime answers
alone.  Verified flips are listed with their harness counts.

1. Boot *Messages* leak: theme-loaddefs "Loading" line logged during
   echo-area reconstruction; `message-log-max' is bound nil around the
   reconstruction (xdisp.c's suppressed logging), restored after.
2. read-from-string reader-form leak: `#[...]'/`#s(...)' literals now
   materialize through the full reader-object materializer (intern
   first), not the partial record/hash/char passes.
3. `documentation' delegates to the Lisp `function-documentation'
   generic when fboundp (doc.c:361), native offset path kept for
   BuiltinFunc; bootstrap fallback unchanged.
4. called-interactively-p frame shape: funcall-interactively and
   call-interactively call through the SYMBOL (not a pre-resolved
   function object), restoring GNU's backtrace frame shape.
5. undo-auto--undoable-change fires per-change from the
   before-change-functions hook site when undo is enabled (insdel.c's
   run_undoable_change), not per-command.
6. Legacy vector obarrays: `(make-vector N 0)' coerced by storing a
   real obarray in slot 0 on first intern/unintern (check_obarray_slow);
   completion walks skip non-string/symbol candidates and stop at
   non-cons tails (minibuf.c's skip rules).  semantic-utest-ia texi
   flipped.
7. beginning-of-buffer/end-of-buffer are proper CONDITIONS: the
   BufferError conversion and the forward-char/backward-char/
   delete-char sites signal `(beginning-of-buffer)' with nil data
   (cmds.c xsignal0) instead of a plain error string, so simple.el's
   condition-case handlers catch them.  Probe byte-identical; note
   point was already clamped to the boundary before signaling
   (matching SET_PT-then-xsignal0).  kill-whole-line-invisible root
   cause #1.
8. line-end-position/pos-eol backward shortage: a backward scan that
   runs out of newlines yields BEGV itself (search.c
   find_before_next_newline), not the first line's end.  This was
   kill-whole-line-invisible's real trigger: org-fold-heading's
   `(line-end-position 0)' on line 1 must give point-min, else
   hide-sublevels folds the first headline and org-fold's :fragile
   revealer backward-chars at bob.  simple-tests flip verified.
9. Central maximum-arity enforcement for C builtins: eval.c's
   funcall_subr rejects calls beyond the subr's declared maximum;
   per-impl need_args only policed minimums, so e.g. (car 1 2) and
   (safe-length 1 2 3) silently ignored extras — and the byte
   optimizer const-folded the latter, hiding GNU's compile warning.
   The generated GNU arity manifest is the authority (max_args cached
   in NameFacts; MANY/UNEVALLED exempt).  bytecomp warn tests flip.
   KNOWN RESIDUAL: wrong-number-of-arguments data for LAMBDA calls
   still prints emaxx's "byte-code function" name where GNU embeds
   the arity cons (e.g. ((2 . 2) 3)); separate item.
10. Byte-op backtrace frames: bytecode.c records the signaling op
    (car/cdr/nth/elt/aref/aset/setcar/setcdr only) as a backtrace
    frame before signaling, visible to handler-bind handlers; the VM
    pushes the frame on those ops' error paths, an in-frame
    condition-case unwinds it, and `run' balances whatever remains
    after handler dispatch.  bytecomp--byte-op-error-backtrace flips.
11. read-positioning-symbols moved INTO the reader (read0's
    LOCATE_SYMS): every symbol occurrence (t included, nil and
    numbers excluded) is wrapped with its character position at parse
    time.  The retired token-stream zip desynced on any non-symbol
    token — a number, t — and silently dropped every later position;
    bytecomp warnings inherited the enclosing defun's position
    (fun-attr-warn's 212:4 vs 215:4).  Structure-kind atoms consumed
    by reader syntax (`#s(' kind, `#:') read bare.  Probes
    byte-identical incl. t-wrapping; lread-tests 52/52, bytecomp
    100/100, elisp-mode 63/63.
12. Interpreted-closure staleness (bytecomp-reify-function): a
    captured variable mutated after a merge-path call could live only
    in the lexical_cell_updates overlay (the call write-back replaced
    the closure's frames, detaching the public alist).  The cached
    `aref'-visible environment now folds pending updates into its own
    alist conses — values current, GNU cons identity preserved.
    KNOWN RESIDUAL: emaxx's materialized closure env carries a
    trailing `t' entry GNU does not print in this shape; cosmetic,
    queued.
13. `equal' signals `(circular-list LIST)' on a cycling spine
    (fns.c FOR_EACH_TAIL) after the shared-tail EQ escape; the
    internal non-signaling equality (which answers t for isomorphic
    cycles) remains for host-side uses.  testcover's circular-list
    marks depend on the signal being IGNORED, which requires it to
    exist.  KNOWN RESIDUAL: cycles nested inside records/hash-tables
    still take the non-signaling path.
14. eval-buffer/eval-region evaluate in a FRESH interpreter
    environment (readevalloop's internal-interpreter-environment
    specbind): eval-buffer picks lexical/dynamic from the buffer's
    OWN cookie (Feval_buffer + lisp_file_lexical_cookie), eval-region
    from the buffer-local `lexical-binding'.  Previously the caller's
    lexical frames leaked into the evaluated top level, so a
    cookie-less buffer's defuns became lexical when evaluated from
    inside a lexical closure — testcover's driver is exactly that
    caller (vector-in-macro-spec void-variable val).  testcover-tests
    31/31.
15. --seccomp on GNU/Linux: emacs.c's maybe_load_seccomp/load_seccomp
    ported — argv scanned before any other startup work, BPF file
    validated with GNU's exact size/regularity checks and error
    texts, prctl(PR_SET_NO_NEW_PRIVS) + seccomp(SET_MODE_FILTER,
    TSYNC) install the filter for real (verified with a live
    allow-all filter).  system-configuration-features on Linux is now
    "SECCOMP" — a feature listed only because the capability exists;
    Darwin remains "".  emacs-tests 7/7.
16. Invalid `#N' read syntax datum is the buffered token text
    ("#5)"), lread.c's INVALID_SYNTAX_WITH_BUFFER, replacing a
    synthesized message.  eieio-persist's two no-backward-compat
    tests depend on the exact datum.
17. print-deeply-nested: the "Apparently circular structure" depth
    guard fires only when print-circle is nil (print.c:2249's NILP
    check); with print-circle GNU prints any depth.
18. input-pending-p with non-nil CHECK-TIMERS runs ripe timers
    (keyboard.c READABLE_EVENTS_DO_TIMERS_NOW); sit-for's
    zero-second path depends on it.  timer-tests-sit-for flips.
19. record_point (undo.c): `undo-boundary' stores point; the first
    change after a boundary records that position as a bare integer
    undo entry unless the change begins there, and the native replay
    goto-chars it (primitive-undo's FIXNUM case).  Undo list now
    byte-identical for the bug#21722 shape.  APPROXIMATION: GNU's
    point_before_last_command_or_undo is also refreshed by the
    interactive command loop; emaxx refreshes at undo-boundary (and
    the tty loop), per-buffer.  KNOWN RESIDUAL: the
    undo-inhibit-record-point variable is not consulted (no test
    exercises it; queued).

Documented as OPEN (not silently skipped):
- print-tests-continuous-numbering-cl-print: an expected-failure test
  whose recorded message differs; matching it needs print.c's
  print_preprocess two-stage number table (t → negative-number
  promotion) persisted across calls under print-continuous-numbering,
  interleaved with cl-print's own table.  Analysis in session notes;
  deferred.
- simple-tests-async-shell-command-30280: the test requires the child
  emacs to produce output within accept-process-output's 4-second
  window; emaxx's boot is ~6.4s even in the gate profile, so this is
  boot-speed-bound, not semantics.  Expected to resolve with the
  pdumper-equivalent work; no dodge will be attempted.
- edebug-tests: 4 failures under investigation this round
  (backtrace-goto-source, error-stepping-into-subr,
  error-trying-to-set-breakpoint-in-uninstrumented-code,
  trace-showing-results-at-breakpoints).  RESOLVED later in round 2 —
  file verified 46/46; see the round-2 closing addendum below.
- nadvice filter-args error data (closure printing) parked as before;
  emacs-lisp/comp prune-cache trio is native-comp feature boundary.

Round-2 self-audit residual (recorded before commit): the signaling
`equal' distinguishes plain conses from emaxx's vector-literal tagged
conses by their tag symbol; a user list whose car is literally
`vector-literal' takes the non-signaling comparison path.  This is the
representation's pre-existing ambiguity surfacing in one more place,
not a new shortcut; the honest fix is a typed vector representation.

Round-2 addendum (edebug four, root cause): the native kbd-macro
command loops caught command errors without REGISTERING that fact, so
`signal_or_quit's handler scan (emaxx's boundary dispatch) saw a
handler-bind outside the loop — ert's test wrapper — as the nearest
handler and ran it; ert's debugger continuation throws, so the error
the loop would have reported to `command-error-function' aborted the
test instead.  GNU's recursive edit enters command_loop_2 under
internal_condition_case(`error'), and Fexecute_kbd_macro's loop under
`minibuffer-quit'; those frames now register as active Case handlers
for the loops' duration.  Residual (pre-existing, now recorded): the
boundary-dispatch approximation can run a handler-bind handler more
than once while an error crosses several native frames where GNU runs
it exactly once at signal time; the new Case frames mask this for
command-loop errors.

Round-2 closing addendum (edebug 46/46, three further mechanisms):
1. `eq'/`eql' were non-reflexive on emaxx's opaque ReaderForm values
   (the match in values_eq_in_env/values_eql fell to the `_ => false'
   arm), so edebug-unwrap*'s fixed point `(while (not (eq sexp (setq
   sexp (edebug-unwrap sexp)))))' spun forever when a raw reader form
   reached a backtrace frame — the whole file timed out at 0/46 after
   the Case-frame fix let backtrace-goto-source get that far.  eq now
   answers Rc identity for ReaderForm, matching the PartialEq impl.
2. `append' rejected closures: fns.c concat_to_list accepts CLOSUREP
   args and flattens them to their slots via Flength/AREF, which
   edebug-unwrap* relies on to rebuild compiled closures with
   `(nthcdr 3 (append fn ()))'.  Oracle probe (bcapp.el) byte-identical
   for aref/append/length/nthcdr/unwrap* on a byte-compiled closure.
   Residual: for a non-sequence argument emaxx's append still signals
   listp where GNU signals sequencep (pre-existing shape divergence,
   unreachable in the closure path).
3. `this-single-command-keys' stayed stale after a keyboard macro
   finished: GNU's command_loop_1 zeroes this_command_key_count after
   every executed command, so the read that reports end-of-macro leaves
   the key state empty; emaxx kept the macro's last multi-key sequence.
   kmacro-call-macro keys its repeat-map offer on `(> (length
   (this-single-command-keys)) 1)', so emaxx armed a phantom transient
   repeat map ("(Type b to repeat macro)") that swallowed the first key
   of the next macro — self-insert into edebug's read-only source
   buffer (trace-showing-results-at-breakpoints).  Oracle probe
   (tsck.el) byte-identical after zeroing the key state at macro end.

Round-2 note (dev-profile-only artifact, recorded 2026-08-30):
simple-test-undo-extra-boundary-in-tex fails ONLY in a dev-profile
whole-file run: by test 38 the wall clock crosses an
undo-auto--boundary-timer 10-second tick inside the test's kbd macro,
recording the extra boundary the test exists to reject.  GNU runs the
same timer but finishes the whole file in ~3.4s.  Gate-profile run:
52/53 with the tex test passing (only boot-bound async-shell-30280
remains).  Same class as async-shell: execution speed, not semantics.

Round-3 mechanisms (small-file sweep, 2026-08-30; every item probed
against the oracle before and after, byte-identical):
1. string-collate-lessp/equalp collate for real on GNU/Linux: sysdep.c
   str_collate ported over libc newlocale/wcscoll_l/towlower_l
   (LC_COLLATE|LC_CTYPE), invalid locale signals GNU's exact "Invalid
   locale ...: <strerror>", non-string locale signals stringp, symbol
   arguments collate by print name.  Non-Linux keeps the lexicographic
   fallback because Darwin lacks __STDC_ISO_10646__ and GNU itself
   falls back there.  fns-tests 81/81 (was 78/81).  Residual: with a
   locale argument of nil emaxx collates in the process's current
   locale via wcscoll, like GNU; the harness always runs LANG=C.
2. bare-symbol/position-symbol accept nil and t (they ARE symbols);
   bare-symbol signals (wrong-type-argument (symbolp
   symbol-with-pos-p) VALUE) on non-symbols where
   remove-pos-from-symbol stays lenient (data.c trio); error data now
   carries the value, not a type name.  data-tests 57/57.
3. map-keymap reported every full-keymap character binding twice
   (emaxx keeps a char-table facade AND direct bindings for the same
   store; keymap.c map_keymap_internal walks ONE store, with
   map_char_table yielding maximal merged ranges).  The walk now merges
   the two stores into one segment list, reporting each binding once,
   coalescing adjacent equal values.  keymap-canonicalize (subr.el)
   stops duplicating char ranges, so describe-map matches GNU;
   help-tests 31/31 (was 29/31), keymap-tests still 46/46.
4. md5 without CODING encoded text through Rust UTF-8 String bytes:
   sentinel-carrying unibyte strings and eight-bit chars hashed wrongly
   (rfc2104-hash md5 HMAC differed).  It now extracts bytes exactly as
   secure-hash does (fns.c extract_data_from_object), and the shared
   string path encodes an eight-bit char as its verbatim byte
   (character.h BYTE8_TO_CHAR: 0x3FFF00 + B), matching GNU's
   preferred-coding-system (utf-8 under LANG=C) encoding.  Residual:
   emaxx does not consult preferred-coding-system dynamically; a user
   who reconfigures it away from utf-8 would still get utf-8-shaped
   hashing bytes for multibyte strings.
5. oclosure-test, timer-tests-sit-for, pp-tests--sanity, and
   warnings-tests' minimum-level failure message verified flipped by
   round-2 mechanisms (function-documentation delegation,
   input-pending-p timer run, full reader materializer, and the
   *Messages* boot-leak fix respectively).
6. Comment style of a two-char marker took style b from EITHER char;
   GNU's SYNTAX_FLAGS_COMMENT_STYLE takes b ONLY from the marker's main
   char (second of a starter, first of an ender), c from either.  C's
   `/*' was mislabeled style b whenever `/' also opens `//' style-b
   line comments, so `*/' (style a) never matched its own comment and
   back_comment's lossage decode rejected the forward parse
   (syntax-comments-c-b6).  syntax-tests 100/100 after.
7. parse-partial-sexp with OLDSTATE now continues over the middle of a
   two-char comment marker, entering the comment when the char before
   FROM is a starter-first pairing with the first char of the range
   (scan_sexps_forward's in_2char_comment_start) and closing it when
   an in-comment restart sits between the two chars of the ender
   (forw_comment's mid-loop entry).  Residual (disclosed): GNU carries
   the pre-FROM syntax in state element 10 and emaxx re-reads the
   buffer char before FROM -- identical for a state handed back from a
   parse ending at FROM (the documented contract), divergent only for
   synthetic states; emaxx's element 10 remains its internal
   continuation blob, not GNU's prev-syntax fixnum (pre-existing
   public-shape divergence, now recorded).
8. libxml-parse-html-region/-xml-region called through the libxml
   crate's parse_string_with_options, which passes a DANGLING pointer
   for the encoding name (the CString is built and dropped inside a
   match arm) -- every parse after a session's first read reused heap
   as the encoding and failed nondeterministically on non-ASCII input
   (shr's nonbr.html truncated at its first no-break space).  The
   parse now calls htmlReadMemory/xmlReadMemory directly with xml.c's
   exact option flags and an owned "utf-8" string.
9. Text-property change detection compared string-valued properties
   with a missing match arm (always "different"), so a range
   propertized with one string object fragmented into per-character
   runs; GNU's interval code compares property values with EQ, and one
   string object over a range is a single run.  String values now
   compare by backing-store identity (emaxx clones share it), which is
   exactly GNU's EQ.  This is what broke shr-zoom-image: with a long
   alt text, next-single-property-change reported a boundary after ONE
   character, so the zoom replaced two characters of a twenty-char
   image region and left the unsliced remnant the test rejects.
   Environment note (recorded for the frozen run): as root,
   HOME=/nonexistent IS writable, and shr-image-fetched's
   url-store-in-cache leaves /nonexistent/.emacs.d/url/cache behind --
   both oracle and emaxx see it on later runs.  The probe and
   verification runs remove it; it must be removed before the final
   frozen run too.

10. process-environment/initial-environment are now ordinary Lisp
    lists built ONCE at startup (emacs.c set_initial_environment)
    instead of being resynthesized from the OS environment on every
    unstored lookup.  setenv-internal's delq now splices the same cons
    chain a let-binding shares, so removing a variable inside `(let
    ((process-environment process-environment)) ...)' persists after
    the unwind exactly as in GNU (python-tests' unset-inside-let test
    depends on it; the container exports PYTHONUNBUFFERED=1, which the
    resynthesized list kept resurrecting).  python-tests 366/366.
11. map-charset-chars only knew ascii and unicode; every legacy-charset
    rule in characters.el (CJK "_" symbol rows, category entries)
    silently mapped nothing.  charset.c map_charset_chars is now ported
    over the existing charset-map machinery: MAP charsets walk their
    encoder as maximal unicode-ascending runs, unified OFFSET charsets
    walk their unify map and append the raw code-offset range, plain
    OFFSET charsets yield the arithmetic range, SUBSET/SUPERSET
    recurse.  This is how GNU's standard syntax table gives U+20AC
    symbol syntax (the euro sits in korean-ksc5601's "_" rows), which
    [[:word:]] then excludes (cperl-test-identifier-rx).  Residuals:
    callback granularity may split ranges differently than GNU's
    char-table walk (invisible to side-effecting callers); dev-profile
    boot grew ~19s from the real map parsing and range application.

Round-3 documented-open (investigated, out of tractable scope):
- em-prompt-tests next-previous-prompt (2): eshell error output lacks
  the output-field text properties GNU's print path applies, so field
  extraction around prompts includes the error text.  Eshell
  field/print plumbing.
- thread-tests thread-list (2): emaxx mutex-lock does not block a
  thread that contends a held mutex (the contender runs to completion),
  so no thread is ever listed "Blocked ... mutex1".  True blocking
  threads are machinery beyond this round.
- kmacro step-edit-with-quoted-insert (1): both sides fail; the
  failure messages differ in how far the step-editor replays
  quoted-insert input.  Step-edit emulation depth.
- process-tests (7): stderr-buffer/pty wiring (wrong-type-argument on
  stderr buffers) and stop/hints internals.
- semantic-utest-ia C/C++ analyzer completions (6): the texi case
  flipped with the round-2 minibuf.c completion rules (11/17, was
  10/17); the rest fail inside CEDET's C/C++ type analysis, beyond
  this round.  emacs-tests verified 7/7 (the seccomp port flipped all
  six).

12. The signaling `equal' (round-2 item) lacked internal_equal's depth
    layer and blew the Rust stack on car-circular graphs (the gate's
    lib stage aborted on equal_compares_circular_cons_graphs).  Ported:
    past depth 10 a seen-pair memo answers t for a revisited (o1, o2)
    cons pair -- how GNU compares car-circular graphs -- and depth 200
    signals (error "Stack overflow in equal").  Oracle probe circeq2.el
    byte-identical on graphs/(t t nil), 300-deep error, 150-deep t, and
    the cdr-circle circular-list signal.  Residual: the depth-200 error
    fires only on the cons path; a >200-deep pure-vector nest returns
    normally where GNU errors (vector compares ride the non-signaling
    fallback).

Environment note (container change, 2026-08-30): the execution
container was restarted mid-banking and its toolchain differs from the
one earlier rounds ran on.  Two in-repo test groups pinned old-container
behavior and failed AT THE COMMITTED BASE (verified by a full-stash
run), not from this round's diff:
- accept-process-output tests assumed output and the exit sentinel
  always arrive in separate accept calls; the oracle on THIS container
  (probe apo1.el) delivers them in ONE call 4 runs of 5.  The tests now
  accept the sentinel line as optional, which is GNU's actual contract.
- the eshell external-pipeline test pinned "rab\n"; this container's
  rev (util-linux 2.39) preserves the missing trailing newline, and GNU
  here writes "rab" (probe esh1.el).  The expectation is now derived
  LIVE from the host's own `printf bar | rev' (finding 134b:
  platform-derived, not hand-pinned).

Round-2 pre-gate audit residuals (fine-grained, recorded 2026-08-30):
- run_change_hooks' undo-auto--undoable-change call discards a signal
  from that function where GNU's call0 would propagate it; the function
  body only registers the buffer and arms a timer, so no known path
  signals, but the swallow is a shape divergence.
- `documentation' keeps the native doc-offset path for BuiltinFunc even
  when the `function-documentation' generic is available, so a user
  method specializing on subrs would be bypassed; GNU routes subrs
  through the generic too.

13. Gate performance regression from item round-3/11 (map_charset_chars):
    the charset port fills syntax/category/case char tables with
    thousands of real CJK range entries, and emaxx's char-table reads
    were linear over the append-only write log (explicit_entry did a
    reverse scan; char_table_effective_ranges re-derived masking
    quadratically per call).  The gate's lib stage spun for hours inside
    char_table_get under bytecode frames (gdb-verified on the live
    binary).  Fixed with a lazily-built BTreeMap interval index over the
    unchanged log (newest-wins, non-overlapping; incrementally
    maintained by push_entry, dropped on wholesale replacement).  This
    is emaxx-internal indexing only -- resolution order, masking, nil
    semantics, and map-char-table fragmentation are unchanged.  Oracle
    probe chartab1.el (overlapping writes, nil masking, single-char
    splits, ASCII/non-ASCII boundary): aref and char-table-range
    sections byte-identical; map-char-table emits the same 13
    ranges/values in the same order as GNU.

Documented-open (discovered by chartab1.el, pre-existing, unrelated to
the index): GNU's map-char-table passes ONE shared cons as the range
key and destructively reuses it call-to-call, so a function that saves
the key sees every saved cons mutated to the scan's final state
((last-end+1 . 4194303) in the probe); emaxx allocates a fresh cons per
call.  chartab.c map_char_table's XSETCAR/XSETCDR reuse is the
mechanism.  No frozen test exercises saved-key identity; left open and
disclosed rather than ported blind mid-banking.

Environment note round 2 (oracle rebuild fallout, 2026-08-31): gate
attempt 5 was the first run to reach the m/n alphabet range of the lib
suite on the NEW container (attempts 3 and 4 died earlier in the
alphabet), and it exposed nine pre-existing tests -- none touched by
this round's diff -- whose pinned expectations transcribed a PREVIOUS
container's oracle build or host stack.  All were repaired by deriving
the expectation from the thing itself rather than re-pinning:
- native_gnutls_catalogs: the cipher/mac catalogues are properties of
  the host libgnutls that BOTH runtimes dlopen; compared live-to-live
  (the gnutls-digests pattern), with structural anchors.
- native_treesit_runtime: treesit-library-abi-version reports the ABI
  of the library each build links; the oracle's is fetched live, Emaxx's
  comes from its tree-sitter crate constants (host lib is ABI 14 here,
  crate is 15); every other element stays a shared pinned contract.
- native_image_variables: x-bitmap-file-path is epaths.h PATH_BITMAPS,
  a configure-time constant; the oracle is asked for its own build's
  value live.
- native_gui_creation, native_xfaces: x-file-dialog / x-select-font /
  x-load-color-file exist only in X-compiled builds; the oracle's
  fboundp is probed live and the expectation follows its build.
- set_network_process_option: the SO_BINDTODEVICE rows pinned a Darwin
  oracle ("lo0", "Device not configured"); the device is now the host's
  own loopback name, the bind row catches whatever the kernel answers
  (privilege-dependent on Linux), and the whole result is live-to-live
  with anchors on the platform-free finding-103 discriminators.
- make_network_process_ipv6, native_gnutls_session, native_gnutls_x509:
  this container has no IPv6 stack and no gnutls-serv; the tests now
  skip exactly where GNU's own suites put skip-unless guards.
- marker_adjustments_stay_adjacent: stale in-repo expectation predating
  the round-2 undo.c record_point port -- GNU's real list (oracle probe
  undomk.el, byte-identical with Emaxx) carries the point entry `9'
  between the marker rider and the (t . TIME) cell; the test now pins
  GNU's shape.

Documented-open (build-model divergence, disclosed): Emaxx models the
X-compiled headless GNU build -- x-file-dialog, x-select-font, and
x-load-color-file are defined and refuse or work without a display, and
x-bitmap-file-path is (".") -- while THIS container's oracle was
configured without the X chooser/color machinery (those functions are
unbound there) yet with X headers on the bitmap path.  Which functions a
build DEFUNs is a configure-time fact with no single honest answer
across differently-configured oracles; the tests check each side against
its own build and this note records that Emaxx's modeled build is not
this container's.

14. The live-to-live conversion of set_network_process_option (environment
    note round 2) immediately caught two real GNU/Linux divergences the
    pinned Darwin expectation had been hiding:
    - SO_BINDTODEVICE was hardcoded to Darwin's 0x1134 on every platform,
      so Linux setsockopt answered ENOPROTOOPT ("Protocol not available")
      where GNU binds; the constant is now the platform's own
      (libc::SO_BINDTODEVICE = 25 on Linux, 0x1134 kept for Darwin).
    - process.c:2846's `:priority' row (compiled under #ifdef SO_PRIORITY,
      which GNU/Linux defines and Darwin does not) was missing entirely,
      answering "Unknown or unsupported option" where GNU applies it.
      Ported as SOPT_INT: an int-ranged fixnum reaches the kernel,
      anything else is "Bad option value" before the syscall.  Oracle
      probe sopri.el (root and unprivileged, identical): applied t,
      recorded 3, and the three bad-value shapes.

## Numbering note (merge of main and tty audit tracks, 2026-08-31)

The main track and the tty track allocated finding numbers
independently while apart: main's findings 135-136 (load-error trace,
and the round-2/3 ledgers above) and tty's findings 135-139 below are
DIFFERENT findings that happen to share numbers.  Cross-references in
commit messages use each track's own numbering.  New findings after
this merge continue from 140.

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


## 2026-08-30 finding 137: Eglot TTY contract exposed false-green runtime seams

The issue-20 Eglot work was audited before its long gate.  The upstream replay
is the pinned GNU 30.2 `eglot-tests.el`, not a rewritten local test: all 52
selected outcomes match (39 pass, the same 6 fail, and the same 7 skip).  The
interactive side uses an ordinary Content-Length-framed JSON-RPC subprocess
with no editor branch.  GNU and Emaxx get isolated same-named projects and the
TTY comparator checks exact cells, attributes, cursor, and requested fixture
trees; it has no Eglot-specific screen or asynchronous normalization.

Making those journeys real exposed eight general runtime gaps that a shallower
fixture could have hidden:

1. `make-process :stderr BUFFER` rejected the buffer instead of creating
   GNU's separately observable linked pipe process, and `:noquery` was not
   propagated.
2. Killing a buffer discarded the visited filename even though a retained GNU
   buffer object still exposes that slot; Eglot uses it while revisiting files.
3. Positioned source lambdas were not callable or arity-readable through the
   normal evaluator path.
4. `accept-process-output nil` returned on the first ready descriptor rather
   than continuing through GNU's 10 ms post-output readiness window.
5. TTY window margins existed in the Lisp API but redisplay did not reserve
   them or paint overlay `before-string` margin display specifications, so a
   diagnostic could be logically present but invisible.
6. Anonymous face plists and their nested inheritance resolved to the default
   TTY face, erasing Flymake's visible warning attribute.
7. A timed live-TTY `read-event` polled only the keyboard.  It did not pump
   subprocess/network output and deferred timers, so JSON-RPC completion could
   remain unread until another key arrived.
8. `accept-process-output nil` counted an outputless process exit and its
   sentinel as delivered output.  Eglot's synchronous reconnect wait therefore
   returned before the replacement clangd process delivered its initialize
   reply.

The implementation fixes those mechanisms rather than recognizing Eglot,
fixture text, response labels, or scenario names in production.  A source scan
finds none of the fake server's `fake-lsp`, `fixture-warning`, hover, or
completion literals under `src`.

The adversarial pass then caught and corrected six defects in the first
implementation/test draft:

- A process test called `executable-find` but asserted this host's `/bin/sh`.
  It now compares the portable command basename, and the readiness child also
  uses the discovered executable.
- The automatic stderr process name was derived from the already-uniquified
  parent.  An oracle collision probe showed GNU creates `dup stderr` from the
  requested name before naming the parent `dup<1>`; production and the
  permanent regression now preserve that ordering and naming.
- Positioned lambda heads were accepted even while
  `symbols-with-pos-enabled` was nil, and the original test ran only Emaxx.
  Direct GNU rows now pin both modes: disabled yields
  `(nil invalid-function invalid-function)`, enabled yields
  `(t (0 . 1) ("some-executable"))`.  The test reads the entire positioned
  lambda, including its parameters and body, and `func-arity` now uses GNU's
  `invalid-function` condition for an invalid cons form.
- Strengthening that test from a positioned head to a fully positioned source
  form then caught the lambda binder still rejecting positioned `&optional`
  and parameter symbols.  Source-lambda construction now unwraps those
  parameters only under the same dynamic flag; the fully positioned GNU row
  passes through `functionp`, `func-arity`, and `funcall`.
- The first readiness regression relied on a 1 ms child sleep and failed under
  parallel load.  Its replacement is a deterministic causal handshake: the
  stderr filter schedules a zero-delay timer that releases stdout, so only a
  real post-delivery pump observes both streams.
- Two test/comment names claimed more than they proved (that a root-isolation
  unit itself launched the server, and that the TTY resolver handled every GNU
  face-reference form).  The claims now name their exact narrower evidence;
  the separate protocol test is what executes the fake server.

The deliberately non-checkpointed TTY actions are not unmeasured outcomes.
Each is preparation for the next strict state checkpoint: edits before
completion/hover/xref, the didOpen notification race before a deterministic
didChange, and a save message containing intentionally different temporary
roots before an exact buffer-plus-filesystem check.  Connection itself is
strict in the first and reconnect journeys.  These boundaries are documented
beside the scenarios and in `docs/eglot-compatibility.md`.

The final full upstream replay caught item 8 after the first audit: the log
said "Reconnected!" but `eglot-current-server` was still nil because the
numeric sync wait had returned on the old server's sentinel transition.  A
direct portable oracle row now pins the underlying contract: an outputless
child exit makes GNU `(accept-process-output nil 1)` return nil while exposing
status `exit`; Emaxx previously returned t.  The event pump now maintains the
separate distinction between "made progress" and "delivered process output".
The permanent regression invokes the discovered `shell-file-name`, runs the
same form in GNU and Emaxx, and contains no Eglot names or fixture responses.
The post-fix audit also rejected a process-global first draft of the delivery
counter: because GNU process descriptors belong to a Lisp thread, the counter
now uses the same active-thread ownership filter as the event pump.  Output
delivered by a different Lisp thread cannot fabricate success for this wait.

## 2026-08-30 finding 138: Magit package and TTY drafts had false-green seams

The issue-21 work was audited before its release gate.  The final package
journey builds a disposable local archive from eight exact GNU ELPA and NonGNU
ELPA release tarballs, rehashes cached and copied artifacts, and gives separate
empty roots to GNU Emacs and Emaxx.  Both run the same real
`package-refresh-contents`, transaction, installation, restart, and `require`
forms.  There is no editor branch in the generated Lisp.  The gate requires
the exact seven-package external transaction, exact 58 `.elc` relative
filenames, generated autoloads, equal records, and installed-tree origins for
every external library.  The bundled `seq` satisfies that dependency on both
editors; its pinned tarball remains available in the archive but is correctly
absent from both transactions.

The interactive side creates fixed-history Git repositories with host Git
configuration disabled and compares text, attributes, cursor, and strict
post-mutation Magit queries.  It contains no Magit-specific output
normalization.  Mutating journeys use separate same-named repositories.  The
non-mutating repository-not-found journey shares one empty target so both
editors receive the same visible absolute path, then compares the decline
screen and proves that neither `.git` nor a Magit top-level was created.

The adversarial pass rejected and fixed these false-green or misleading
drafts before the gate:

1. The first Diff journey typed `d d`, which only left the transient prompt
   open.  It now types `d u`, selects the real unstaged-diff suffix, enters the
   diff buffer, navigates it, and returns.
2. An early attempt used unrelated temporary paths for a path-bearing prompt.
   The final read-only journey shares its non-mutated target; no path or screen
   bytes are rewritten.  Mutable repositories remain honestly isolated.
3. Declining repository creation was initially a non-checkpoint followed only
   by a strict state query.  The immediate decline screen now also matches GNU
   exactly; the state query remains as independent outcome evidence.
4. Byte compilation was initially pinned only by per-package counts.  Equal
   counts could hide one missing and one unexpected file, so the gate now
   requires all 58 exact relative filenames.
5. Restart provenance initially checked only Magit, Transient, and With-Editor.
   It now checks the origin of every library in the external closure: Compat,
   Cond-Let, Llama, Magit, Magit-Section, Transient, and With-Editor.
6. Adding the third-party journeys to `ttydiff.py`'s bare no-argument battery
   would have made that built-in battery fail late without installed package
   roots.  The dedicated package gate now owns those journeys and supplies
   both freshly verified roots explicitly; named selection remains permanent.
7. The face-support parser treated `((:box t))` as an empty plist and selected
   a graphical box alternative.  The runtime now walks nested face-reference
   lists generally and correctly rejects the unsupported box on a TTY.
8. A draft fix painted the terminal's default foreground over every glyph to
   obtain one margin attribute.  Oracle probes exposed extra attributes on
   ordinary rows.  That draft was removed; the final behavior is confined to
   the separate margin-glyph mechanism while preserving an extending row
   background.

The other runtime corrections are likewise mechanism-level: multiline local
variable forms, source-stream EOF position, positioned property keys, true
invisibility specs, overlay display strings, `font-lock-face` aliases,
buffer-local face remapping, condition-specific error printing, terminal
initialization order, command-loop selected-buffer restoration,
`set-window-buffer`'s `KEEP-MARGINS`, invisible-tail `window-end`, extending
faces, and concrete inverse-color realization.  Production code contains no
Magit command, fixture filename, repository state, or expected screen switch.

## 2026-08-31 finding 139: lsp-mode package and TTY drafts exposed false-green seams

The issue-22 gate constructs a disposable local archive from eight exact
Stable MELPA and GNU ELPA tarballs, rehashes both cached and copied artifacts,
and gives GNU Emacs and Emaxx separate empty package trees.  Both editors run
the same `package-refresh-contents`, dependency transaction, installation,
restart, autoload, and `require` forms.  The gate requires the exact
eight-package transaction, all 159 exact `.elc` relative filenames, equal
records, and installed-tree origins for lsp-mode and every external
dependency.  Generated Lisp contains no editor branch or feature fabrication.

The interactive phase launches the shared deterministic server as a real
stdio subprocess through lsp-mode's public client registration API.  It
strictly compares workspace connection, diagnostics, completion, hover, xref,
rename and file bytes, restart/shutdown state, the tree-widget session browser,
the JSON-RPC log, attributes, and cursor positions.  Mutable journeys use
separate same-named projects; the read-only reconnect and UI journeys share a
single fixture so genuine absolute-path messages remain directly comparable.
There is no lsp-mode-specific output normalization or expected-screen branch.

The adversarial pass rejected or corrected these false-green mechanisms before
the release gate:

1. Source reads under a private dynamic `obarray` initially registered symbol
   names but retained standard-obarray identity.  The reader now recursively
   replaces symbols in conses, string properties, vectors, records, closures,
   hash tables, char tables, and circular reader forms with the selected
   obarray's identity-bearing values.
2. Nested record/hash literals read from lsp-mode's persisted session could
   retain parser-private reader markers.  Every public read boundary now
   materializes the complete object graph before package code observes it.
3. Positioned symbols were unwrapped only by top-level `equal`/`eq` paths.
   Recursive equality, membership, association, hash-table `equal`, lexical
   alists, and `let`/`let*` bindings now honor the same dynamic GNU contract.
4. Explicit process filters were invoked in the process buffer.  GNU invokes
   them in the caller's current buffer and restores that buffer after a filter
   changes it; the runtime and a direct GNU regression now enforce this.  The
   audit also replaced that regression's hardcoded `/bin/sh` with the oracle's
   `shell-file-name`.
5. `all-completions` flattened matching propertized strings into new plain
   strings.  It now returns the original string object, preserving identity
   and properties through lsp-mode's completion pipeline.
6. Echo restoration reconstructed a face-only string, losing other properties
   from `current-message`.  The echo channel now retains the real Lisp string,
   while the paint model derives its face spans from that value.
7. `read-string` treated its HISTORY argument as a local keymap and copied
   only the initial input's bytes.  It now uses `minibuffer-local-map` and
   carries the suggested value's properties and extended characters into the
   minibuffer.
8. The TTY timer pump asked Lisp-level `float-time` whether timers were ripe.
   The harness's legitimate clock pin therefore made future timers fire
   immediately.  The scheduler now decodes timer vectors and compares them to
   the native exact clock, matching the C scheduler rather than special-casing
   lsp-mode.
9. A blind minibuffer-height delta could be applied after Lisp had already
   restored the window configuration, growing the root past the frame.  Each
   redisplay now reconciles the desired root height against live window-tree
   geometry; no extra redraw sequence remains in the journey.
10. The renderer supported only `:align-to` spaces.  It now implements numeric
    specified-space widths and equal-property runs, including the zero-cell
    TTY result of tree-widget's `:width 0.5`.  A second tree-widget blank came
    from an overlay before-string whose own `(invisible t)` property was
    ignored; overlay display objects now obey the buffer's invisibility spec
    and remap their face spans after hidden cells.
11. During diagnosis the session-browser screen checkpoint was temporarily
    disabled to inspect its underlying buffer.  The diagnostic action and
    files are gone, that checkpoint is restored, and structural tests require
    the browser, log, lifecycle, completion, hover, rename, and final
    filesystem checkpoints to remain enabled.
12. lsp-mode deliberately prints process IDs, clocks, and `(emacs-version)`
    build metadata.  Those OS/build-assigned presentation inputs are pinned
    symmetrically before either editor starts the client.  The process object,
    package transaction, JSON-RPC bytes, command results, and screen comparator
    remain real; no observed output is rewritten after the fact.
13. The first full clean-install gate exposed a warm-cache false green: GNU's
    newly loaded package has a native-comp `*Compile-Log*`, so generic
    `M-g M-n` correctly navigated compiler warnings.  Flymake's own
    documentation confirms that it deliberately does not claim
    `next-error-function` by default.  The diagnostic journey now invokes the
    real public `flymake-goto-next-error` command through `M-x`, measuring the
    required lsp-mode/Flymake integration without configuring Flymake,
    deleting the compile buffer, or normalizing the resulting screen.

Production source contains no lsp-mode command, fake-server response, fixture
filename, package version switch, or expected screen value.  The package
journeys remain named permanent scenarios but are excluded from the bare TTY
battery, whose environment cannot supply freshly verified package roots; the
dedicated package gate owns and supplies those roots explicitly.

## 2026-08-31 findings 140-145: merge audit of the eglot/package-gate push

Three-way adversarial audit (fake-LSP fixture, package-gate tools,
runtime diff) of tty-frontend c7a6daf before this merge.  Cleared of
actual fabrication: the fixture server is client-blind, every scenario
and gate compares GNU and Emaxx live with no output normalization,
packages are hash-pinned unmodified upstream tarballs, and the GNU tree
is untouched.  Corrected in this merge:

- 140 (fixed): the margin glyph painter forced palette slot 7 as "the
  default foreground" -- a constant transcribed from the terminal
  emulator's rendering of the default color.  term.c's turn_on_face
  emits NO SGR color when face_tty_specified_color (dispextern.h)
  rejects the default sentinel; margin cells now take the default
  face's own (unspecified) foreground via a forced write, keeping the
  extended background beneath.
- 141 (fixed): the decode_timer port accepted 9-slot vectors, skipped
  the fixnum USECS check, and honored `triggered' only on the idle
  list; keyboard.c:decode_timer requires exactly ten slots, a fixnum
  vec[2], and nil vec[0] on BOTH timer lists.  Ported exactly.
- 142 (fixed): no Eglot journey asserted that a language server
  actually connected -- a host without python3 would diff two identical
  failure screens and report MATCH.  ttydiff actions gained
  `require_text', an absolute both-editors-must-render assertion, and
  every Eglot scenario now gates on a live-server probe printing
  eglot-live=t.
- 143 (fixed): magit-repository-not-found shared ONE directory between
  the editors, so an Emaxx-created `.git' would contaminate GNU's later
  check and never surface.  The journey now isolates per-editor
  targets, skips only the path-bearing frames, and closes with
  path-free state checks plus a byte-exact per-editor filesystem
  snapshot -- the check that catches an unwanted repository.
- 144 (fixed): the lsp-mode gate ran against the invoking user's real
  $HOME; it now scratches HOME like the Magit and Flycheck gates.
- 145 (fixed, docs): eglot-compatibility.md claimed "no fixture
  literals" (the server hardcodes the alpha payload -- symmetric, but
  fixture content) and overstated coverage; the position-parameter
  blind spot (full-document sync, positions ignored, so column-math
  bugs cannot surface through these journeys) is now stated.

Also resolved by the merge itself: the tty branch's token-queue
position resync in print.rs (an un-anchored scan-forward heuristic) is
retired by main's LOCATE_SYMS positioning reader.

Documented-open from the same audit (fidelity gaps, disclosed not yet
ported; none is a fabricated shape):
- the wholly-invisible-tail window_end/%p rule in tty.rs is justified
  by observed oracle behavior with no xdisp.c anchor named;
- face-remapping-alist is read from the current buffer (not the
  window's) and cached frame-globally by face name, so a buffer-local
  remap can leak across windows until the next full repaint;
- string/mode-line face paths alias font-lock-face unconditionally
  instead of through char-property-alias-alist;
- overlay before/after-string ordering ignores xdisp.c
  compare_overlay_entries (after-strings first across overlays,
  priority order);
- overlay-string base face drops the anchor's `face' text-property
  contribution (xfaces.c face_for_overlay_string);
- read errors reset point to the region start where GNU leaves it at
  the failure position; adjacent equal (not eq) display space specs
  coalesce into one stretch; the overlay ellipsis is a literal "..."
  rather than the display table's selective-display-ellipsis slot.
- Evidence notes: the "52 upstream Eglot outcomes" replay is Darwin,
  prose-only (39 pass / 6 fail / 7 skip, matched as outcomes, honestly
  labeled); the Linux eglot cluster remains open as its own task.  The
  package gates and Eglot/Magit/lsp/Flycheck TTY scenarios are
  manual-run only (excluded from the default battery); ttydiff's
  non-checkpointed-action discipline is convention, not machine-checked.

Merge validation note (2026-08-31): the three Eglot TTY journeys were
replayed live on this Linux container (twice, all frames matching),
which also validates finding 140's margin repaint against real GNU
glass.  One tuning change: rename-through-language-server's settle rose
4s -> 8s because Emaxx completes the rename's asynchronous round trip
(idle-timer didChange -> publishDiagnostics -> flymake clear) about two
seconds after GNU on this host -- state and final frames are identical
(verified with an input-free wait), only slower.  The latency gap is
real and unexplained; it belongs to the eglot cluster task.

Merge semantic-conflict note (2026-08-31, gate round 1): two of the tty
branch's positioned-symbol tests failed on the merged tree and both were
defects in the MERGE RESOLUTION, settled against the C source and a live
probe (rps1.el), not against either branch:
- main's LOCATE_SYMS reader wrapped symbols inside object literals;
  read0 clears locate_syms across the whole payload of `#s(...)',
  `#^[...]', `#(...)', and `#[...]' (lread.c RE_record and friends), so
  hash-table data and record slots stay bare under positioning.  The
  reader now saves and clears the flag around all four literal forms.
- main's signaling `equal' delegated its leaves to the env-less walk, so
  `symbol-with-pos' unwrapping under `symbols-with-pos-enabled' worked
  only at top level; internal_equal's EQ sees through wrappers at every
  depth.  The signaling walk is now env-aware end to end, keeping the
  depth memo and circular-list signaling.

Gate round 2 note (2026-08-31): the compat-harness suite tripped once on
its own subject-lock test -- a fork-window artifact (a concurrently
spawning test's child briefly inherits the just-released flock fd until
exec closes it; O_CLOEXEC acts at exec, not fork).  Harness-internal,
5/5 green isolated and 5/5 green as a full stage after the test gained
a bounded retry documenting the mechanism.  No runtime code involved.

## 2026-08-31 eglot cluster closed on Linux (task record)

On the merged tree, test/lisp/progmodes/eglot-tests.el compares 52/52
matching.  Without a C language server the composition was 16 passed /
5 failed / 31 skipped on BOTH runtimes; clangd 18.1.3 was installed on
this container (environment change, disclosed) and the composition
became 40 passed / 5 failed / 7 skipped, still matched per test: the
31 clangd-gated scenarios now exercise live LSP behavior end to end
rather than matching as skips.  The five failures are rust-analyzer
tests GNU itself fails identically here (server version drift, matched
failure conditions -- these are honest matches, not Emaxx successes);
the seven skips want eclipse-jdt, typescript/deno, and yasnippet,
absent on both sides.

## 2026-08-31 hard-third round 1 (tramp + erc mechanisms)

Frozen at the merge: 7712/7883.  This round's flips, each anchored to
its C source with oracle probes:
- fileio.c expand_cp_target BEFORE handler dispatch: Fcopy_file and
  Fadd_name_to_file expand FILE and resolve a directory NEWNAME to
  NEWNAME/basename; Frename_file feeds directory-file-name FILE;
  Fmake_symbolic_link keeps TARGET verbatim.  Emaxx passed raw
  arguments to file-name handlers, so Tramp's exists-check fired on
  the directory itself (probe trcp1.el byte-identical; tramp-tests
  09/11/12/21 flipped, 7 -> 4 mismatching).
- textprop.c graft_intervals_into_buffer with inherit: an inserted
  string's own intervals MERGE with what the insertion point inherits
  (string keys win); insert-and-inherit was wiping inherited props by
  replacing the plist wholesale.  format-spec relies on a propertized
  replacement keeping the spec region's face (probes fspec1/ercfmt1.el
  byte-identical; the erc speaker-format family and refresh-prompt
  flipped).
- editfns.c styled_format property layering: the format string's props
  cover each substituted span UNDER the argument's own (probe tp2.el);
  the format builder's overlapping spans now flatten with that rule.
- minibuf.c read_buffer completes over (NAME . BUFFER) conses -- the
  PREDICATE receives the pair -- and RET under REQUIRE-MATCH dispatches
  minibuffer-complete-and-exit, which refuses input test-completion
  rejects (completing-read-default installs the context as
  minibuffer-buffer locals via the setup hook; the simulated reader's
  RET now validates against them, slicing the prompt off the buffer
  front).  Probes reqm1-6/rbuf2.el byte-identical; erc--switch-to-buffer
  and the erc-channel-p cascade flipped (erc-tests 9 -> 3).
- STALE IN-REPO TEST corrected: read_buffer_simulation_enforces_its_
  predicate pinned a NAME-string predicate -- the oracle HANGS on that
  program (every candidate refused); the test now uses the cons
  contract (probe rbuf2.el).  The old test had passed only because the
  old reader ignored predicates entirely.
- The frozen-resume feature (head.json commit marker; --resume reuses
  same-commit per-file comparisons) landed with its stored shape
  matching the flattened comparison.json.

Documented-open from this round:
- erc--essential-hook-ordering and erc--find-mode spawn a child of the
  running binary and read its output inside GNU's OWN 10-second
  accept-process-output silence window; an Emaxx child takes ~40-50s to
  boot (no portable dump), so both sides' correct code diverges on
  latency alone (probe inv3.el: the child's output arrives, late).
  Boot-latency-bound, like simple-tests' async-shell case.
- erc--split-line splits between a base character and its combining
  diaeresis where GNU keeps the grapheme together; tramp-test39/41/42
  (supersession warning, special-character names, filename encoding)
  remain open with diagnosed directions.

## 2026-09-01 issue 34: asynchronous process, timer, and file events

This round replaces polling-shaped placeholders with the host event substrate
needed by ordinary Emacs Lisp waits.  The implementation is anchored to
`process.c`'s `wait_reading_process_output`, `Fmake_process`,
`Faccept_process_output`, and `Fprocess_tty_name`; `keyboard.c`'s
`timer_check`; `kqueue.c`'s `kqueue_callback`, directory-diff path,
`Fkqueue_add_watch`, and `Fkqueue_rm_watch`; and `inotify.c`'s
`inotify_callback`, `Finotify_add_watch`, and `Finotify_rm_watch`.

The resulting runtime contract is:

- Darwin registers real vnode descriptors with kqueue and translates the
  coalesced flags and directory snapshots into GNU's callback order.  Linux
  selects the generated Linux primitive inventory and uses a shared
  nonblocking inotify descriptor, preserving masks, names, move cookies,
  ignored-watch invalidation, and queue order.
- The process wait path services filters, sentinels, connection progress,
  child status, timers, file notifications, and cooperative threads.  The
  blocking TTY paths use the same pump and redraw after timer or process
  progress; nested readers pass their own idle duration rather than scanning
  the Lisp timer list twice.
- External processes honor the unhandled local form of `default-directory`,
  stream-specific PTY reporting, stop validation, and deterministic child
  cleanup.  Dropping an interpreter terminates and reaps live children.

Adversarial review found and corrected these lifecycle defects before the
gate:

1. The kqueue reserve check underflowed when `RLIMIT_NOFILE` was below Emacs's
   50-descriptor reserve.
2. A failed final inotify watch removal could retain an otherwise empty shared
   queue.
3. Post-spawn descriptor setup could fail before the child was wrapped in its
   terminating/reaping owner; synchronous stdin write failure likewise left
   cleanup implicit.  Both paths now kill and wait deterministically.
4. Deleting a nonexistent path fabricated a `deleted` event, and stale
   fingerprint fields survived after the native backend became authoritative.
5. A blocking terminal read pumped process output without reporting progress,
   so the changed buffer was not redrawn until the next key.
6. The unified timer pump reused a TTY helper that discarded callback errors
   and nonlocal exits.  It now invokes `timer-event-handler` as a named timer
   callback, balances timer callback state, propagates throws and debugger
   errors, preserves the native exact clock, and performs one Lisp timer scan
   per wait.

Permanent Rust coverage includes independently created/renamed/deleted host
files, kqueue directory creation, callback isolation and invalidation, raw
inotify ordering/cookies, due and deferred timers, recursive-edit nonlocal
exits, process cwd/PTY/EOF/drop cleanup, and TTY redraw after both timer and
process progress.  Focused upstream runs matched 4/4 filenotify tests, 5/5
timer tests, 2/2 inotify tests, and every issue-relevant process selector
(pipe/PTY shapes, lifecycle, stderr, sentinels, stop/filter/multiwait, serial,
and network descriptors).  The 15-test Rust timer cluster is also green.

The cooperative thread model's previously disclosed gaps remain: the Lisp
thread file matches 1/3 and the C-thread file 30/32, with the same blocked
thread backtrace/list and preemptive mutex/condition-variable limitations
already recorded above.  This change does not claim preemptive threads.

macOS Clippy is warning-free for all targets and features.  The Linux target
cross-compiles and passes the same Clippy `-D warnings` gate through Zig; no
Linux runtime was available in this session, so native inotify execution is
not claimed beyond source-oracle comparison, permanent tests, and the Linux
build gate.

The final unrestricted macOS gate (batch stdin closed so EOF-prompt tests use
their documented contract) is green: the library reports 2263 passed, 0
failed, and 4 documented ignores; compat-harness 38/38; CLI 12/12; ERT
integration 3/3; package lifecycle 5/5; and perf-harness 1/1.  The localhost
socket, UDP, GnuTLS transport/X.509, external kqueue, process cwd/PTY/drop,
timer, file-notification, and blocking-TTY redraw cases all executed in that
run rather than being inferred from compilation.
Hard-third round 2 (2026-09-01, Linux): composition, tramp and coding
mechanisms
----------------------------------------------------------------------
Every item below was diagnosed by probing the pinned oracle first and
porting the mechanism from the C (or its owning Elisp), then re-checking
the probe byte-for-byte.

- composite.c find_automatic_composition is now really implemented:
  `find-composition-internal' walks composition-function-table rules
  (char_composable_p over unicode-category-table, MAX_AUTO_COMPOSITION_
  LOOKBACK, the rewind/forward search, autocmp_chars through
  `auto-composition-function') instead of always answering nil for the
  buffer surface.  It reproduces the oracle's whole glyph-string, and
  keeps GNU's load-bearing precondition: with no window showing the
  buffer, Fget_buffer_window returns nil and there is NO automatic
  composition.  fill_gstring_body's glyph widths now come from
  `char-width-table' (the dumped value) rather than a host width table.
  STALE IN-REPO TEST corrected: find_composition_reports_no_automatic_
  composition_in_batch pinned `(nil nil)' for the decomposed
  "__A<U+030A>stro<U+0308>m" of erc-tests' `erc--split-line'; the live
  oracle reports the composition (8 10 [[us-ascii 111 776] ...]) once
  the buffer is in a window.  The replacement asserts both halves.
- charset.c Fchar_charset's RESTRICTION argument was missing entirely
  (Emaxx took one argument): a list picks the first charset that can
  encode CH, any other non-nil value goes through
  coding_system_charset_list, and an unknown coding system signals
  (coding-system-error NAME).  `compose-gstring-for-terminal' needs it
  to decide what the terminal can render.  Disclosed: GNU substitutes
  global charset lists for full-support iso-2022 and emacs-mule
  codings; Emaxx models only the :charset-list attribute, so those two
  families report fewer supported charsets.
- filelock.c: the supersession check lives in the NATIVE half of
  `lock-file', after file-name-handler dispatch.  Emaxx ran it for
  handled files too, so `userlock--check-content-unchanged' silently
  re-stamped the visited modtime and Tramp's own handler (which routes
  to `ask-user-about-supersession-threat' deliberately without the
  local content comparison) never prompted (tramp-test39).
- tramp-file-name-regexp: the method and user/host segments are
  `[^/|:]+' / `[^/|:]*'.  Emaxx's native parser accepted any colon,
  so a LOCAL file whose name contains ":foo;bar:baz;" parsed as remote
  -- file-exists-p answered from the wrong side and directory-files
  dropped the entry (tramp-test41).
- fileio.c Finsert_file_contents with REPLACE saves point as a marker
  and then applies restore_window_points' growth rule (bug#19161): a
  point strictly inside the replaced span keeps its relative distance
  (same_at_start + inserted/oldsize * offset, truncated) instead of
  collapsing to the span start (tramp-test09).
- buffer.c syms_of_buffer marks `kill-buffer-hook' permanent-local;
  Emaxx did not, so a buffer-local kill hook registered before a major
  mode change was discarded and erc-d's canned dialog buffers were
  never removed from erc-d-u--canned-buffers (erc-scenarios-internal,
  3 tests).
- coding.c code_convert_string decodes the STRING's OWN BYTES (SDATA):
  Emaxx read a multibyte string as one octet per character and signaled
  "Character cannot be encoded" for anything above Latin-1
  (tramp-test42).  The full mechanism is now ported: decode_coding_
  object sets src_multibyte from `chars < bytes'; ONE_MORE_BYTE under
  multibytep recovers a byte8 character's octet and hands every other
  character to the decoder as a NEGATIVE code that passes through
  unchanged -- so the decoder really runs over the byte runs BETWEEN
  multibyte characters, and a unibyte destination stores such a code's
  low eight bits ((-c) & 0xFF).  CODING_FOR_UNIBYTE (the raw-text
  family's :for-unibyte) makes a decode that actually ran produce a
  UNIBYTE string, while code_convert_string's ascii-compatible fast
  path still returns multibyte.  EOL conversion now happens on the
  DECODED characters, so a byte-oriented coding (utf-16) that swallows
  a CR octet inside a code unit no longer names an eol subsidiary.
  A unibyte result keeps Emaxx's raw-byte spelling for bytes above
  0x7F (what `bytes_to_unibyte_value' and the raw-text decoder
  produce), so case tables keyed on byte8 characters still match it.
  Measured live against the oracle over a 132-case matrix (11 input
  kinds x 12 coding systems): 45 divergences before, 1 after, with no
  case that matched before changing.

Documented-open from this round:
- KNOWN-RACY IN-REPO TEST (not a fidelity gap, recorded so a future
  gate failure is not misread): subprocess_exit_is_event_driven_and_
  notifies_newest_process_first_once asserts that the `sh -c "printf
  err >&2"' child is still live at the very next Lisp form.  Under CPU
  load the parent can be descheduled long enough for the child to run
  and exit first, so `initially-live' comes back nil while every other
  element -- the (primary stderr) event order, the single delivery,
  the exit status -- still matches.  Measured on the gate binary with
  four spinners running: this round's tree passed 6/6 idle and 3/6
  loaded, and the UNCHANGED base tree passed 6/6 idle and 2/6 loaded,
  so the race is environmental, not a regression.  It also flaked once
  before, in gate47-attempt2, during a round that touched no process
  code.
- The one remaining matrix case is coding DETECTION, not decoding:
  for `undecided' over the byte stream 61 81 62 the oracle detects
  japanese-shift-jis (yielding U+FF5C) where Emaxx's auto-detection
  answers raw-text.  Emaxx's detector does not try the Japanese
  multi-byte categories.
- erc-scenarios-stamp--left/display-margin-mode and --legacy-date-
  stamps still fail, but the pieces they rest on do not: `field-at-pos'
  and the field machinery are byte-identical to the oracle (probe
  fld1.el), and so are cl-generic `&context' dispatch and
  `erc--insert-timestamp-left' under erc-stamp--display-margin-mode,
  including the `((margin left-margin) STRING)' display property
  (probe ctx1.el).  The divergence is therefore in what the live
  session does around those calls, which needs an erc-d dialog to
  bisect.

Hard-third round 3 (2026-09-01, Darwin): coding detection and ERC stamps
------------------------------------------------------------------------
This round closes all three concrete residuals documented at the end of
round 2.  The pinned GNU 30.2 binary was probed before either mechanism
changed.

- coding.c detect_coding_sjis is now represented in undecided decoding:
  ASCII passes, 0x81..0x9F and 0xE0..0xEF require a 0x40..0xFC trail
  other than 0x7F, 0xA0..0xDF is a single-byte Japanese sequence, and an
  incomplete lead in the final block rejects the category.  Because
  Emacs-Mule has higher category priority, the overlapping portion of
  detect_coding_emacs_mule is checked first from the live
  `emacs-mule-charset-table`, rather than letting the new detector steal
  those streams.  The preceding iso-latin-1 detector likewise reads the
  mutable `latin-extra-code-table`; C1 is not treated as one hard-coded
  invalid range.  Selected Emacs-Mule input is decoded through the live
  charset table.  The sole residual from round 2's 132-case matrix now
  matches exactly: undecided over 61 81 62 decodes to (97 65372) and records
  japanese-shift-jis.  Oracle-backed boundary rows cover incomplete and
  invalid SJIS leads, Latin-extra priority, ordinary/private Emacs-Mule,
  unmappable-byte preservation, and live mutation of the Latin-extra table.
- timefns.c Fformat_time_string returns a newly allocated mutable,
  multibyte Lisp string.  Emaxx returned immutable host text and relied
  on the interpreted evaluator to upgrade values when they entered a
  variable.  Byte-compiled lexical locals bypass that upgrade, so
  erc-format-timestamp's put-text-property calls were silently discarded
  from the original timestamp string: the left-margin method inserted a
  correct buffer display property whose nested string lacked `invisible',
  and legacy date stamps inserted a string lacking the `erc-timestamp'
  field.  The primitive now allocates shared mutable string state at its
  boundary.  The adjacent Fcurrent_time_string twin was corrected at the
  same ownership boundary; GNU specifies it as mutable but unibyte.
  A compiled oracle contract checks mutation, intervals, and the two
  multibyte flags, so an interpreted-only pass cannot hide this bug again.
- Live erc-d tracing established that erc-stamp--setup,
  erc-add-timestamp, and the specialized erc--insert-timestamp-left all
  ran in the right buffer and that the outer buffer properties were
  already present.  The real pre-fix comparison was 1/3 matching
  (target/compat/run-1788273953352161000-80592); the rebuilt final tree is
  3/3 matching with zero mismatches
  (target/compat/run-1788276213505930000-86621).

Environment correction: an initial sandboxed ERC comparison appeared to
be 3/3 matching only because BOTH GNU and Emaxx failed to bind the local
erc-d server with `Operation not permitted'
(target/compat/run-1788273832246543000-80173).  That result was rejected,
not counted as compatibility evidence; every before/after count above is
from the unsandboxed localhost run.

## 2026-09-02 tty/main integration audit for issue 34

The publication candidate merges tty head
`a45ac6ad555dcce9b1f8c7588ee1cdce28569104` with refreshed main head
`f957201559ed10d34e4de3927465969af3dd2cb3`; their merge base is
`beca258798e124c101e2558816d405097536fcf3`.  Main first resolved to
`5a20e24871c6f5e67d87f3919c70cd9b9d010670`, and that integration completed
an optimized full run, but main advanced during the run.  The result was
rejected as publication evidence, the uncommitted merge was aborted, and the
latest main was merged afresh so one merge commit will carry the actual
reviewed parents.

The formal adversarial review re-read every combined-diff mechanism against
its GNU 30.2 owner: process.c's wait/process lifecycle, keyboard.c's timer
pump, kqueue.c and inotify.c's watch/event paths, composite.c's automatic
composition, charset.c/coding.c's charset and byte-stream conversions,
fileio.c/filelock.c's replace/handler semantics, and timefns.c's returned
string ownership.  Static scans found no project-private Lisp namespace,
oracle delegation, silent TTY fallback, generated-manifest drift, test-only
runtime dispatch, or compatibility loaddefs in production.  All 15 enforced
anti-cheat gates passed from the optimized candidate.

The refreshed-main audit did find a real defect before Clippy or the final
gate: round 3 detected an Emacs-Mule/SJIS overlap but never selected or
decoded Emacs-Mule, and treated all C1 bytes as Latin-invalid.  For example,
GNU selects Emacs-Mule for bytes 81 A0 while the first integration returned
raw-text; GNU keeps 91..96 in iso-latin-1 through its live Latin-extra table.
The repair ports the relevant category order and table-driven layouts rather
than special-casing samples.  A 714-row cross-binary matrix (every C1 lead,
2/3-byte boundary families, and private 4-byte forms) now has identical
coding-category decisions; the permanent contract pins representative
decoded values and mutates `latin-extra-code-table` to prove the live table is
consulted.  Emacs-Mule decoding matched the oracle except for 17 cases whose
decoded private-charset character is above Unicode; those remain part of the
existing structural finding 127 because Rust strings cannot represent GNU's
up-to-0x3FFFFF character space.

The ignore inventory was challenged before accepting the long gate.  Two
functional ignores were stale: the real EUC-JP codec already made the
EUC-JP/DOS in-place region test pass, and the US-ASCII replacement test still
hid a genuine defect.  The first serial attempt was interrupted and rejected
as soon as it printed the stale EUC-JP ignore.  Charset encoding had flattened
all unencodable characters to SPACE; coding.c instead uses each coding
system's `:default-char` (US-ASCII specifies `?`, while iso-latin-1 defaults
to SPACE).  Both the substitution staging and byte encoder now share the live
property.  The enabled oracle contract covers Latin-1, US-ASCII, and a newly
defined charset coding with a nonstandard replacement, and both formerly
ignored tests pass normally.  The only remaining ignores are two explicit
end-to-end PTY gates whose contract requires separately built release binaries
and the sibling GNU tree; they are opt-in gates, not unsupported feature skips.
Adversarially invoking those gates exposed one harness-honesty defect:
`tty-smoke.py` returned status zero after printing `SKIP` when an input was
missing.  The ignored Rust gate now sets `EMAXX_TTY_SMOKE_REQUIRE=1`, matching
the existing fail-closed differential gate.  A deliberately missing binary
then exited 1, and the actual optimized PTY smoke workflow ran and passed
(1 passed, 2269 filtered out) against `target/release/emaxx` and
`../emacs/lisp`.

Two evidence corrections are explicit:

- The earlier issue-34 full run used two libtest threads.  It remains useful
  regression evidence but is not the required authoritative serial gate;
  the fresh one-thread run and its exact permission-denied reruns below are
  the publication evidence.
- kqueue/inotify event names are converted with lossy UTF-8, so a host path
  containing invalid UTF-8 bytes is reported with U+FFFD.  Ordinary Unicode
  names and raw inotify ordering/cookies are covered, but arbitrary Unix
  filename-byte preservation is not claimed.

Formatting is clean, and macOS plus `x86_64-unknown-linux-gnu` all-target,
all-feature Clippy pass with `-D warnings`; the latter uses Zig only as the
cross C compiler/linker and is not Linux runtime evidence.

The exact optimized serial command was `LANG=C LC_ALL=C RUST_TEST_THREADS=1
cargo test --profile gate -- --test-threads=1 < /dev/null`.  In the managed
sandbox it exercised all 2270
library tests for 8418.88 seconds: 2258 passed, 10 local socket/TLS tests
failed, and the two explicit PTY gates were ignored.  Every failure reported
the same environmental boundary, `Cannot bind server socket: Operation not
permitted`; there was no mismatched semantic assertion.  On explicit review
direction the entire suite was not repeated.  Instead, exactly those ten
named failures were rerun outside the bind-denying sandbox, optimized and one
at a time with `--exact --test-threads=1`; all 10 passed.  The composite
library accounting is therefore 2268 passing tests plus the two documented
opt-in PTY gates.  This is recorded as composite evidence, not misreported as
one exit-zero `cargo test` invocation.

Rejected evidence is also explicit: an earlier malformed `--exact` filter
selected zero tests; the first serial attempt was stopped at the stale EUC-JP
ignore; and an unnecessary outside-sandbox full restart was interrupted at
the user's direction (exit 130) before the targeted reruns.  None contributes
to the passing totals above.

## 2026-09-02 issue 39: Eat 0.9.4 package and real-process certification

The certified package is Eat 0.9.4 from the official NonGNU ELPA archive,
whose package tarball has SHA-256
`14971fc562f0820794eb6af78beebc7dc3ba898221e785c2d272a9f0fccfc54a`.
The matching upstream source commit is
`c91451f2d17453c19d3fa76faa4945cbe54e14ce`, and its source archive has
SHA-256 `32a2793c1f203bf2e0fe67f79310c2389257e1338b191e017ea60dc68000c01a`.
The local archive also pins Compat 31.0.0.2 at SHA-256
`47d8693a10087f8b20c72e6a78b628db980cb7547c4f8f517fc5d11acd8b0f38`.
Both subjects assert Lisp `emacs-version` is exactly 30.2.  The Compat package
version does not imply Emacs 31: Emacs 30.2's built-in Compat satisfies Eat's
`compat >= 29.1` dependency, so an ordinary package.el transaction installs
only Eat 0.9.4.

The work corrected three general runtime defects found by the unedited
package and process workloads.  Evaluation of positioned source symbols now
handles every bare-symbol value, including nil, t, keywords, and ordinary
symbols, instead of assuming the ordinary-symbol representation.  Equal hash
tables now use the active environment's positioned-symbol equality for
lookup, insertion, deletion, and copying.  Key definition and lookup now
normalize GNU symbolic vector events consistently and populate the same
modifier cache for nil, t, mouse events, and positioned symbols.

The adversarial review found and corrected two narrower versions of those
repairs before publication.  The first keymap repair handled ordinary
`Value::Symbol` events but omitted nil, t, and positioned events, and it used
synthetic names rather than the canonical nil/t values.  The first hash-table
expectation also assumed that a key inserted while positioned-symbol mode was
enabled would remain visible after disabling the mode.  A direct GNU 30.2
probe disproved that assumption: the correct enabled/disabled/re-enabled
record is `(1 207 207 (missing missing missing missing) 207)`.  Emaxx now
matches it, including copied-table behavior.  The other direct records also
match exactly: `(t nil :eat-key 42 t)` for bare-symbol evaluation and
`((nil) (t) (mouse-1 click) t)` for symbolic key events.

The package gate creates separate clean GNU and Emaxx user roots and an
artifact-pinned local package archive, then performs ordinary package refresh
and installation.  It requires the exact transaction and installed inventory
(`eat-0.9.4` only), exactly Eat's two compiled `.elc` files and generated
autoload file, a fresh-process restart, and proof that the restarted runtime
loads installed bytecode.  It extracts the official source archive's 57
`eat-test-` ERT definitions without editing them; both GNU and Emaxx pass all
57.  The shared process workload drives real Eat PTYs for shell input and
output, terminal resizing, cursor and SGR state, alternate-screen removal,
scrollback, EOF and exit status, Ctrl-C signal termination, and an interactive
shell.  GNU and Emaxx emit the same pinned records.  The final optimized run
reported, for each subject, two compiled files, 57 upstream tests, and a
passing real-process gate, followed by an exact record match.

Static review found no editor-name branch, oracle delegation, fixture-output
dispatch, process shortcut, package skip, or test-only production hook.  The
process gate contains no system-type branch and uses Eat's real PTY entry
points rather than `call-process` or `start-process`.  The five Python
anti-cheat/unit tests pass.  Formatting and `git diff --check` are clean;
native and `x86_64-unknown-linux-gnu` all-target, all-feature Clippy both pass
with `-D warnings`.  Zig is used only as the Linux cross C compiler/linker,
not as Linux runtime evidence.

The authoritative repository-wide command was exactly `LANG=C LC_ALL=C
RUST_TEST_THREADS=1 cargo test --profile gate -- --test-threads=1 <
/dev/null`.  It ran outside the managed socket-binding restriction once, in
the optimized `gate` profile.  The main library result was 2271 passed, zero
failed, and two ignored in 8330.65 seconds; all 59 executed follow-on tests
also passed, including 5/5 package-lifecycle tests.  The two ignores are the
explicit opt-in `tty_differential_end_to_end` and `tty_smoke_end_to_end`
gates, whose contracts require separately built release binaries and the
sibling GNU tree; they are not Eat skips or unsupported-feature waivers.

Rejected evidence is explicit.  Short-name `--exact` filters that selected
zero Rust tests were discarded and replaced by fully qualified focused runs.
One malformed Emaxx key probe with an extra closing parenthesis was discarded
and corrected.  An earlier long-gate attempt was interrupted when the Emacs
31/Compat-version misunderstanding was corrected; it is not counted.  The
initial stale-bucket expectation described above was rejected after the GNU
probe.  Finally, a Python bytecode-cache `PermissionError` outside the
workspace was an environmental write restriction, not a code result; the
same syntax check passed with its cache under `/private/tmp`.

## 2026-09-02 Linux integration audit of the tty/main candidate (issue 34)

The candidate above (`0bbdb5b`, tty head merged with main `f957201`) was
re-audited on a Linux host with the pinned GNU 30.2 oracle running natively,
which the Darwin session could not do.  The diff was re-read against
inotify.c, fileio.c, process.c and keyboard.c; every byte-stream probe below
was run identically under both binaries before any change was made.  The
integration is sound in its process, timer, kqueue and coding mechanisms;
its Linux backend had four defects and one evidence gap, all corrected here.

1. **inotify-tests.el could not load.**  `subr-arity`/`func-arity` consulted
   only the Darwin-regenerated arity table, which has no inotify rows, so
   the eager macroexpansion of `(should-not (inotify-valid-p 0))` signalled
   "no GNU-derived arity for subr inotify-valid-p" and the harness recorded
   LoadError with 0/2 (main: 0/2 as `ert-test-skipped`).  The arity
   accessor now consults the host's C contract first, as dispatch already
   did; oracle contract: `((1 . 1) (3 . 3) (1 . 1) t nil)`.  inotify-tests.el
   is 2/2 matching.
2. **Delivery timing.**  process.c registers the inotify (and kqueue)
   descriptor with `add_keyboard_wait_descriptor`, and
   `wait_reading_process_output` selects keyboard-class descriptors only for
   a READ_KBD wait; `process_special_events` handles X selection events
   only.  So `accept-process-output` and `sleep-for` never read the kernel
   queue, `input-pending-p` neither reads nor dispatches, and callbacks run
   from read_char.  The oracle for a watched file written moments earlier:
   `(nil 0 0 0 0 1 ...)` across input-pending-p, accept-process-output,
   sleep-for, `(input-pending-p t)` and read-event.  Emaxx ran the callback at
   every one of those points.  Kernel-queue service now happens only while
   the thread is in a keyboard read (`waiting_for_user_input`, which
   `accept-process-output` and `sleep-for` clear for their own duration as
   `waiting_for_user_input_p = read_kbd` does); handler-backed watches keep
   delivering inside any wait, because GNU receives those as monitor process
   output.  Four eval tests had pinned the old behaviour: three with
   `(sleep-for 0)` waits and a kqueue-only library check, and
   `auto_revert_mode_reloads_changed_file` with a `sleep-for` polling loop
   that the oracle itself leaves at "any text" (autorevert-tests.el's own
   `auto-revert--wait-for-revert` uses read-event once notifications are in
   use).  They now use keyboard reads and a host-neutral check, and the same
   watch scenarios are a Linux oracle contract:
   `(((2 directory) nil nil nil) (nil 2 t t))`.
3. **Callback errors.**  read_char executes the special-event binding without
   a condition handler, so a signalling callback leaves `read-event` (the
   oracle: `(error "boom")`, with the sibling watch's callback for the same
   kernel event delivered by the next read).  Emaxx demoted every non-debug
   error to a message reading "Error in file notification: %S", a string that
   exists nowhere in GNU; that invented message is gone and errors propagate,
   the remaining queue intact.  The tty command loop already reports such an
   error the way cmd_error does.
4. **Error data shapes.**  fileio.c `report_file_notify_error` always places
   the rendered errno between the message and the object, splicing a list or
   nil object in as the tail.  Emaxx omitted the errno text for "Unknown
   aspect" (GNU: `"Invalid argument"`, set explicitly by
   symbol_to_inotifymask) and "Invalid descriptor ", printed a nil aspect as
   an extra element, wrapped a dotted descriptor in a list, and reported
   "Could not rm watch" with an empty name instead of the kernel descriptor.
   Aspects are also converted before FILE-NAME is type-checked, as
   Finotify_add_watch orders them.  All shapes now match the oracle; the
   "Invalid descriptor " errno text is whatever the previous host call left
   behind in GNU as well, so the contract pins its presence and type, not its
   value.
5. **Clippy on Linux.**  The candidate's Linux Clippy evidence came from a Zig
   cross-compile on the Darwin toolchain.  With the current stable toolchain
   (rustc 1.98.0) the same gate fails on 15 pre-existing `chunks_exact(2)`
   sites through the new `chunks_exact_to_as_chunks` lint; they are converted
   to `as_chunks::<2>()`, which is behaviour-preserving.  `cargo fmt --check`
   and `cargo clippy --profile gate --all-targets --all-features -- -D
   warnings` are both clean natively on Linux.

Cross-binary evidence: a 30-row inotify probe (error shapes, stale and
duplicate removal, per-inode ID assignment, directory create/modify/rename/
delete/delete-self event lists with cookies, ignored-watch invalidation,
delivery stage, error propagation and isolation) is identical between GNU
and emaxx except for one row, where emaxx reports an extra `modify` for the
`.#file` lock entry because `lock-file` writes a regular file where GNU
writes a dangling symlink -- the divergence already recorded in the
2026-08-27 sweep, outside this change.  Harness replays on Linux after the
fixes: inotify-tests.el 2/2 (from 0/2), filenotify-tests.el 4/4,
timer-tests.el 5/5, process-tests.el 36/37.  The one process-tests miss,
`lookup-hints-values`, fails identically on main (`--subject-root` baseline
0/1): `network-lookup-address-info` rejects the glibc `inet_aton` forms
("127.1", "0xe3010203", octal octets) that AI_NUMERICHOST accepts on
GNU/Linux.  It predates this integration and stays OPEN as a new finding.

The full serial Linux gate on the candidate plus the fixes above exposed
what the branch's switch to the host C contract means for the Rust suite:
four tests had been written against the Darwin contract and only ever ran
there.  `x-load-color-file`, `x-file-dialog` and `system-move-file-to-trash`
are not compiled into the X oracle (nor, now, into Emaxx on Linux), and
`frame-windows-min-size` is window.el Lisp everywhere, which only the Darwin
contract lists with an arity.  Those tests now consult `is_builtin` and
assert the void-function or Lisp-defined behaviour the host's oracle shows;
on Linux that closes the "Emaxx models the X-compiled headless build"
divergence those tests used to record.  Routing `frame-windows-min-size`
through window.el then uncovered two window.c gaps that the native stand-in
had masked: a frame's root window had no sibling link to its minibuffer
window (frame.c make_frame sets `wset_next (rw, mini_window)` and
`wset_prev (mw, root_window)`), and `window-mode-line-height` reported the
minibuffer's buffer format where `window_wants_mode_line` requires
!MINI_WINDOW_P.  Oracle: `(8 10 5)` for the three frame-windows-min-size
forms; Emaxx read `(4 10 4)` before and `(8 10 5)` after.

Two gate observations are recorded, not hidden: the manifest anti-cheat
test shells out to `rustfmt`, which the unprivileged gate user could not
reach under /root's rustup (it passed once the toolchain was on that user's
PATH), and `process_send_string_and_region_route_output_to_the_process_buffer`
failed once under the serial gate's load with only the first echo line
present, then passed on rerun and on the main baseline; its single
`accept-process-output` returns on the first delivery from the named
process, so it belongs to the KNOWN-RACY family already listed above.

Gate accounting for this change, as composite evidence: the first full
serial Linux gate (`lingate2`, LANG=C, one thread, unprivileged runner) on
the candidate plus the notification fixes reported 2264 passed, 7 failed, 2
ignored over 5845 s -- the rustfmt PATH artifact, the four Darwin-contract
tests, the `sleep-for` auto-revert test, and the one load-sensitive process
echo -- with bins and the CLI, package-lifecycle and ERT-runner integration
suites all green.  After the corrections above the second full serial gate
reported 2270 passed, 1 failed, 2 ignored over 5772 s, again with every
other stage green; the single failure was the Todo-mode window-state test
asserting that the batch root window has no next sibling, which is false in
GNU (`(eq (window-next-sibling (selected-window)) (minibuffer-window))` is
t there) and had only held because of the missing frame.c link.  That
assertion now states the oracle's answer, and the test plus the 159-test
window/frame subset were rerun green on the final tree; no production
source changed after the second gate.

The publication tree then also absorbed main's grouped test gate
(`8b08bbf`) and tty-frontend's Eat certification (`59b4d18`), both merging
without a source conflict (only this ledger and docs/testing.md overlapped,
textually).  On that merged tree, natively on Linux as the unprivileged
runner with git state recorded, `python3 tools/grouped_gate.py --scope full`
passed every group: eval_01 349, eval_02 284, eval_03 319, eval_04 247,
eval_05 349, primitives 349, compat_runtime 82, tty 45 (+2 opt-in PTY
ignores), batch 43, lightweight 207 -- 2274 library tests -- plus the bins
and integration targets, in about 57 minutes.  Per docs/testing.md that
runner is still an experimental accelerator, so this is recorded as the
grouped run it was, alongside the two serial gates above, not as a third
serial gate.

Not claimed: Darwin was not re-run here.  The kqueue backend is unchanged;
the delivery-timing change applies to it through the same
`waiting_for_user_input` gate, and the converted eval tests use the same
keyboard-read waits on both hosts.

## 2026-09-02 findings 146-148: Darwin post-merge certification and TTY correction

The Linux integration publication above ended by stating that Darwin had not
been rerun.  This audit starts from the resulting remote-main commit
`3c78b1e93bdad5a2099e3b5d22a235b33d0f8d47` and closes that evidence gap on
Darwin.  The first strict all-target, all-feature Clippy pass found one real
host-configuration defect before runtime testing: `file_notify_error_with_errno`
is used only by Linux-guarded inotify call sites but the helper itself was
compiled on macOS, where `-D warnings` rejected it as dead code.  The helper
now has the same Linux guard as all four callers.  Formatting, diff hygiene,
and strict release Clippy then passed.

The focused Darwin notification audit exercised the real kqueue backend:
four eval_04 notification tests, the exact auto-revert regression, all 346
then-current primitive tests (including kqueue, processes, timers, and GNU
probes), and the Todo window regression all passed, 352/352.  The first full
grouped gate on that production tree recorded artifact
`target/grouped-gate/run-1788334198352310000-94983`: the library was green
(2271 passed plus the two declared opt-in TTY ignores), every binary target,
CLI, and ERT runner passed, but package lifecycle was 4/5.  The failed
`local_package_vc_upgrade_matches_gnu_and_survives_restart` run was not
retried or counted green.  GNU had checked out the 2.0 source while its 1.0
bytecode and package descriptor were still active.

Inspection of GNU package-vc.el established the test race: the Git process
can become non-live before `vc-post-command-functions` runs
`package-vc--unpack-1`, recompiles the checkout, and replaces package-alist.
The integration test had waited only for `process-live-p`.  It now waits, with
a 60-second bound, for the public semantic result -- the installed
`package-desc-version` becoming `(2 0)` -- while continuing to service process
output.  This is not a retry or a fixed sleep.  The exact formerly failing
test then passed in 23.06 seconds and all five package-lifecycle tests passed
under their original two-thread integration conditions in 116.27 seconds.

The explicit release TTY smoke gate passed 1/1 in 53.85 seconds.  The complete
217-scenario GNU-vs-Emaxx PTY differential then ran serially for 4644.19
seconds.  It completed the whole inventory rather than stopping at the first
failure: 211 scenarios matched and six diverged.  Those failures reduced to
three production mechanisms, recorded rather than dismissed as timing:

- Finding 146: while rendering a non-selected window's mode line, Emaxx
  temporarily selected that window and made its buffer current.  Restoration
  switched the current buffer back before restoring selection, which made
  `set_current_buffer_id` save the buffer's live scan point into the temporary
  window.  TMM consequently changed *Completions* from GNU's line 1 to line 5.
  Restoring selection first makes redisplay state-preserving.  Folded-row
  point deferral is also now limited to a window with an actual cursor; two
  absent cursors are not a same-row motion.
- Finding 147: Bookmark's graphical `(left-fringe ...)' display property on
  an overlay before-string leaked the underlying `"x"` into a terminal text
  row.  Left- and right-fringe display strings now occupy zero TTY cells;
  the complete bookmark set-and-jump journey matches GNU.
- Finding 148: Emaxx refreshed its point-before-command undo field only when
  `undo-boundary` actually appended a boundary.  GNU refreshes the keyboard.c
  command point even when simple.el suppresses a consecutive boundary.  A
  motion between two edits therefore left Emaxx recording the earlier point.
  Refreshing at every command dispatch fixes recorded and replayed keyboard
  macro undo, selective region undo with a newer out-of-region edit, repeated
  `C-_`, and repeated `C-x u`.

All six formerly divergent journeys were rerun together against GNU after the
fixes; every checkpoint, including the portions unreachable after each first
failure, matched.  Native regressions separately cover graphical fringe
suppression, the nested TMM window point, state-preserving non-selected
mode-line rendering, and the selected-window folded-row deferral.  This is
composite TTY evidence -- one complete 217-scenario run plus a complete
focused rerun of its six failures -- not misreported as a second one-shot
217-scenario pass.

Finally, `python3 tools/grouped_gate.py --scope full` passed on the complete
corrected dirty tree with artifact
`target/grouped-gate/run-1788342414268828000-6860`.  Its dynamic inventory was
2275 library outcomes: 2273 passed, zero failed, and exactly the two declared
opt-in TTY gates were ignored.  The three discovered binary targets passed
39 tests; CLI passed 12/12, ERT runner 3/3, and package lifecycle 5/5.  The
runner used the recorded safe schedule (single-threaded within the evaluator,
primitive, compatibility, and TTY groups; only proven-safe groups overlapped)
and completed in about 22 minutes.  No production source changed after this
gate; only this evidence record was appended.

## 2026-09-02 issue 35 network/TLS/JSON-RPC adversarial audit

The issue-35 candidate begins at pushed tty head `170f0dc` on the dedicated
`issue-35-networking` branch; `tty-frontend` itself was left unmoved while its
integration into main was owned elsewhere.  The exact candidate diff was
reviewed against GNU 30.2 process.c's
`Fnetwork_lookup_address_info`/`network_lookup_address_info_1`, the upstream
network-stream, GnuTLS, JSON, and JSON-RPC suites, and the existing Eglot and
lsp-mode application contracts.

Before final certification, refreshed `origin/main` at `1394e8d` was merged
into the issue branch, producing merge commit `7e389f9`.  Restoring the issue
work produced no source conflict; the only textual conflict was between two
independent audit-ledger sections, and both were retained.  The production
file and new network-contract document match their pre-merge SHA-256 exactly.
The test and testing-documentation changes have the same stable patch IDs as
before the merge while preserving main's adjacent additions.

The one production change removes Rust `IpAddr::parse` and
`ToSocketAddrs` from `network-lookup-address-info`.  GNU initializes an
`addrinfo` hint with the requested AF_UNSPEC/AF_INET/AF_INET6 family,
SOCK_DGRAM, and (only for the `numeric` hint) AI_NUMERICHOST, traverses the
returned list in resolver order, converts each sockaddr to the public vector,
and frees the list.  Emaxx now does the same.  The audit verified that the
string's C NUL boundary, family mapping, socket-type/flag selection, IPv4 and
IPv6 network-byte-order conversion, port, resolver order, error path, and
single free all follow that owner.  No address/sample literal occurs in the
production path.

Two findings were corrected before the gate:

1. The first unsafe draft trusted `ai_family` but did not validate
   `ai_addrlen` before casting `ai_addr` to `sockaddr_in`/`sockaddr_in6`.
   Both casts now require a non-null pointer and the corresponding full
   structure length.
2. The first HTTP fixture used an unnecessary `X-Emaxx-Fixture` header and
   compared only GNU/Emaxx derived body records.  The shared fixture now uses
   the editor-neutral `X-Contract-Fixture`, both editors must parse its value,
   and GNU's complete status/header/length/SHA-256/prefix/cleanup record is
   pinned before Emaxx is compared.  Independent server-side assertions prove
   that each editor opened a connection and sent the exact GET target and Host
   header; no normalization is applied.

The final static pass found no project-private Lisp namespace or fixture-name
branch in production, editor identity branch, oracle path/delegation,
generated answer table, test-only runtime hook, swallowed warning, retry,
accepted failure, or weakened assertion.  The numeric contract sends the same
29 valid/invalid inputs to GNU and Emaxx and compares complete vectors.  The
HTTP servers are independent, loopback-only, one-shot, and bounded; the
fixture response is identical, buffer cleanup is observed, and a fabricated
response cannot satisfy the request capture.

Focused evidence on the audited bytes: both new exact gate-profile tests pass;
all-selector upstream replays match 9/9 JSON-RPC, 7/7 GnuTLS, 27/27
network-stream, 59/59 Lisp JSON, and 23/23 native JSON outcomes.  The GnuTLS
replay's 64x test-time slowdown is disclosed for the performance milestone.
The HTTP bind-denied sandbox result, malformed initial test filter that
selected zero tests, vacuous batch input-cancellation probe, and focused
compat-harness runs killed after isolated-build time consumed their total
timeout are rejected, not counted.  Full command details and scope boundaries
are recorded in `docs/network-compatibility.md`.

The formal static audit was then repeated against the exact working-tree diff
from `7e389f9`.  It again found no fixture/sample literal or editor identity in
the production path, no environment or target branch, no oracle execution or
delegation, and no bypass, retry, accepted failure, or weakened assertion in
the tests.  All 15 repository anti-cheat gates passed, with zero failures and
zero ignored.  This post-merge audit precedes the final formatting, native and
Linux cross-target Clippy, and repository-wide gate.  Any later source change
to this candidate requires those claims to be established again.

After the audit, `cargo fmt --check`, native all-target/all-feature gate-profile
Clippy with `-D warnings`, and the equivalent `x86_64-unknown-linux-gnu` cross
Clippy pass were clean.  The complete optimized grouped gate passed with
artifact `target/grouped-gate/run-1788356614665579000-29565`: its dynamic
library inventory was exactly 2,287 outcomes, with 2,285 passed, zero failed,
and only the two declared opt-in TTY end-to-end gates ignored.  The three
binary targets passed 39 tests, CLI passed 12/12, ERT runner 3/3, and package
lifecycle 5/5.  The primitives group containing both new issue-35 contracts
passed 353/353 with zero ignored.  A final remote refresh left `origin/main`
unchanged at `1394e8d`; no production or test source changed after the audit or
gate, only this evidence record was appended.

## 2026-09-03 findings 149-150: tty-frontend merge certification and gate repairs

The candidate is a clean no-conflict merge of main
`1394e8d9c90398a4a978fc8fb3ed1015d4d9e7f5` and tty-frontend
`170f0dcf6d058d9496320d4355f80705f45f3bc1`.  Adversarial inspection of the
merged diff confirmed that main's selected-window/current-buffer restoration,
folded-row cursor gate, TTY fringe suppression, command undo-point refresh,
package-vc semantic-version wait, and Linux-gated notification helper remain
present alongside tty-frontend's completion-stack and terminal changes.  No
merge resolution discarded either side's production fixes.

The full grouped gate passed on the exact merged production tree with artifact
`target/grouped-gate/run-1788347588425925000-13645`.  Its dynamic inventory
hash was `bb372db771bc2718596c3fbf8c9b5c97f9d2beea19f5fb6a15ab4fbe8e412073`:
2285/2285 outcomes were observed, with 2283 passed, zero failed, and exactly
the two declared opt-in PTY gates ignored.  The three discovered binary
targets passed 39/39 tests, and the integration targets passed 20/20 (CLI
12/12, ERT runner 3/3, package lifecycle 5/5).  The strict completion-stack
package gate separately installed and restarted through all 6/6 pinned
packages, validated 41 compiled/autoload artifacts, and matched all 25/25
checkpoints across its four real TTY scenarios.  The release Emaxx binary was
`9be0ff62f8ad4026889fdc2580f7611c4aa9592518ee3f84dd1527787dc5d76e`; the GNU
oracle was `7d8944fe2b2bdbd2856cfd4f47dbd5c80db90089ac20be641c10a348bf217e82`.

Two harness defects were found during the required end-to-end TTY gate rather
than hidden as flaky editor failures:

- Finding 149: every scenario killed both editor processes, bypassing Org's
  Lisp cleanup, but reused the host temporary namespace.  Exactly 1000 stale
  `babel-stable-0` through `babel-stable-999` directories had accumulated;
  Org chooses only those 1000 names, so later Org startup looped indefinitely.
  Each comparison now gives both subjects the same fresh per-scenario TMPDIR
  (preserving path-exact comparison) and removes it after closing both
  sessions.  A regression constructs subject temp artifacts, proves the two
  subjects received one namespace, and proves it was removed afterward.
- Finding 150: `supersession-accept-revisit` sent `yes RET` to GNU's
  save-anyway prompt and then sent a second `y`.  The first response had
  already saved successfully, so the extra byte modified the buffer and the
  later kill command stopped at a confirmation prompt; the scenario then
  opened a second minibuffer and manufactured a divergence.  The redundant
  byte is removed, the kill is now a real checkpoint with a complete dispatch
  window, and a regression pins the exact action sequence and final filesystem
  assertion.

The discarded/red evidence is part of the record.  A first grouped-gate
attempt inside the restricted sandbox failed because the tests could not bind
their required localhost services; it was rerun natively and produced the
passing artifact above.  Earlier TTY attempts stopped at changing startup
locations under host pressure before the deterministic Org namespace
exhaustion was isolated.  The first complete 217-scenario run then reported
216/217 matches and the invalid supersession-script divergence described
above; it was not counted green.  After both harness repairs, a new one-shot
full run passed 217/217, including every screen, face, liveness, and requested
filesystem checkpoint.  The explicit release TTY smoke passed 1/1.

The final post-run audit passed `python3 -m unittest tools/test_ttydiff.py
tools/test_completion_stack_package_gate.py` at 31/31, `cargo fmt --all --
--check`, `cargo clippy --all-targets --all-features -- -D warnings`,
`cargo check --all-targets --all-features`, and `git diff --check`.  A static
scan found no debug probes, expected-failure markers, new ignores, skips, or
normalization escape hatches in the repaired harness.  The package artifacts
remain version-pinned, GNU and Emaxx use isolated roots, comparisons remain
exact, and no production source changed after the complete grouped gate.

## 2026-09-03 issue 36 TRAMP adversarial audit

The issue-36 candidate starts from pushed issue-35 commit `dcd5b82` on the
dedicated `issue-36-tramp` branch.  The initial remote refresh before
certification found `origin/main` at `1394e8d`, already contained by the
branch.  During the long gate, main advanced to merge `9f7c591`; final merge
`b51ca69` integrated it before publication.  Comparing the two prior merge
trees showed that this late integration changed only the audit ledger and the
already-certified TTY differential Python runner/tests, not Rust production or
Rust test bytes.  Both independent ledger sections were retained.  The target
is GNU Emacs 30.2.  The work uses GNU TRAMP's own file-name-handler mechanisms
and public process APIs; it does not add an Emaxx-specific remote API.

Production corrections cover four contracts exposed by the selected upstream
TRAMP tests.  `buffer-size` now accepts GNU's optional buffer designator and
reports the complete buffer size despite narrowing.  `make-process` dispatches
to a file-name handler before validating native-only keyword details, accepts
the empty call's GNU nil result, and treats `:coding nil` as the default coding
selection.  Remote `list-system-processes` and `process-attributes` calls now
follow the handler selected by `default-directory`.  `accept-process-output`
now implements GNU's target-only delivery boundary, accepts nil as an
unbounded timeout, and distinguishes integer JUST-THIS-ONE from the ordinary
truthy spelling; zero-duration `sleep-for` no longer consumes ready process
output.  Finally, SIGUSR1 and SIGUSR2 are installed through async-signal-safe
counters and enter the ordinary keyboard event path, including
`special-event-map`, command dispatch, and the unread-event fallback.

The first formal audit of the permanent deterministic journey found four real
test-tool defects, all corrected before certification:

1. Exact GNU/Emaxx equality alone allowed two false semantic records to agree.
   Independent assertions now require the expected content, file operations,
   handler prefixes, metadata, process result, connection reuse/reconnect,
   integration results, and final cleanup.
2. The first runner could start Emaxx after the GNU oracle failed.  Oracle
   completion and validation are now prerequisites for starting the subject,
   with an offline regression proving that boundary.
3. Final Lisp cleanup used `ignore-errors`.  Cleanup now aggregates and reports
   any buffer, file, directory, process, or connection failure and must emit
   `cleanup.final=t`.
4. The first process wrapper overstated its descendant cleanup guarantee and
   mishandled byte output in `TimeoutExpired`.  The runner now owns each editor
   with `Popen`, snapshots its exact descendant tree, terminates and reaps those
   exact PIDs, and decodes partial byte output safely.

The final runner has no retry, normalization, accepted-failure path, or
warning suppression.  It runs the GNU oracle to completion before the Emaxx
subject, compares complete structured records exactly, and retains raw stderr.
Only TRAMP's blank progress lines plus the expected `Compilation finished`
message are allowed; every other diagnostic fails.  Its default transport is
the deterministic localhost `mock` method.  Real SSH requires both
`--live-ssh` and an explicit `/ssh...:` root, so network availability cannot
silently change the gate.  All 10 offline fail-closed runner tests passed, and
the final GNU-30.2-versus-Emaxx journey passed at
`target/tramp-compat-gate/journey-20260902T172315.597899Z.json`, covering remote
visit/save/revert, directory and Dired operations, completion, metadata,
copy/rename/delete, temporary files, subprocess and compilation invocation,
project and VC discovery, connection reuse, forced reconnect, failure, and
cleanup.  A post-run process-tree check was empty.

Upstream TRAMP evidence is deliberately composite, not misreported as a new
whole-suite rerun.  The seven previously divergent default selectors (tests
08, 09, 10, 11, 12, 23, and 27) each matched GNU exactly after the
`buffer-size` repair.  A serial non-default pass excluding test 45 initially
matched 41 of 50 outcomes and exposed nine differences.  Only those nine were
then rerun, as requested: test 29 normal and direct-async, test 30 normal and
direct-async, test 31 list-system-processes, process-attributes, and
signal-process, test 34's explicit-shell case, and test 47's read-password
case all matched GNU exactly.  Test 45 separately passed an exact clean run.
One rejected test-31 attempt failed on both editors with a generated process
name; it was not counted as passing, and the clean exact rerun is the evidence.

The final static audit found no fixture, selector, package, editor-identity,
environment, or oracle branch in production; no runtime oracle execution or
delegation; no generated answer table; no new ignore, skip, retry,
normalization, accepted failure, or weakened assertion; and no warning
suppression.  All 15 repository anti-cheat tests passed with zero ignored.
`cargo fmt --check` was clean, and
`cargo clippy --profile gate --all-targets --all-features -- -D warnings`
passed with zero warnings.

The authoritative publication gate was then run conventionally rather than
with the concurrent grouped accelerator: `LANG=C LC_ALL=C
RUST_TEST_THREADS=1 RUST_MIN_STACK=134217728 cargo test --profile gate --
--test-threads=1`.  The optimized library binary reported 2291 passed, zero
failed, and exactly the two reviewed opt-in TTY end-to-end gates ignored out
of 2293 tests in 7097.99 seconds.  The compat-harness binary passed 38/38,
the perf-harness binary 1/1, CLI 13/13 (including the real-process SIGUSR
test), ERT runner 3/3, and package lifecycle 5/5; main and doc-test binaries
contained no tests.  No second test or build was launched from this checkout
while the gate was active, and the post-gate process-tree check was empty.
After the late main merge, its affected TTY/completion offline tests passed
31/31 serially and the TRAMP runner's offline tests passed 10/10 serially.  No
Rust production or Rust test source changed after the full gate.  The merge
added only its separately certified TTY Python runner/test correction and the
two retained evidence sections described above.

## 2026-09-03 issue 31 minibuffer/completion adversarial audit

The issue-31 candidate starts from `origin/main` commit
`589f82ba1c562c44b05cdea6a2d4c627e628e876` on the dedicated
`issue-31-minibuffer-completion` branch.  A refreshed `tty-frontend` had no
commit absent from main, so no tty merge was manufactured and the issue work
was not placed directly on that shared branch.  The target is the pinned GNU
Emacs 30.2 source at `636f166cfc86aa90d63f592fd99f3fdd9ef95ebd`, not
Emacs 31.

Production corrections are at shared semantic boundaries.  Consecutive and
nested minibuffer reads receive distinct activation identities so grow-only
TTY height cannot leak through a reused minibuffer buffer.  An accepted string
default enters the named history through the ordinary history-length and
duplicate policy.  Events already consumed by the recursive minibuffer reader
are not appended a second time to a keyboard macro.  Case-folded completion
preserves an unextended input's spelling, while same-length case conversion
preserves string properties and character-count-changing conversion follows
GNU's rebuilt-string path.  Redisplay places point before an overlay
after-string, invokes the Lisp pre-redisplay coordinator, highlights the
selected completion through its real cursor-face overlay, assigns the hardware
cursor to the selected window rather than every active minibuffer, and mirrors
the active minibuffer buffer even while `*Completions*` is selected.

The adversarial pass found and corrected two evidence defects before
certification.  First, final TTY records initially required only an `I31`
prefix.  All six now require an independently specified exact return,
history/depth, buffer/overlay, and cleanup record in addition to exact
GNU/Emaxx terminal comparison.  Second, checkpoints aimed at two-second
transient messages were sensitive to the runner's deliberate serial subject
startup.  The permanent checks compare the stable retained invalid input and
restored outer prompt; the following correction/acceptance actions and exact
final records prove that the reads rejected, recovered, returned, and unwound.
The earlier timing-sensitive and geometry-divergent runs are rejected, not
counted as passes.

The final static scan found no issue fixture, scenario, expected answer,
editor-identity, environment, target, or oracle branch in production; no
runtime oracle execution or delegation; and no new ignore, skip, retry,
normalization, accepted-failure path, weakened assertion, or warning
suppression.  The only test-input literals found under `src` are inside
regression modules.  All 15 repository anti-cheat tests passed with zero
failures and zero ignored.  `python3 -m unittest tools/test_ttydiff.py` passed
26/26, `cargo fmt --all -- --check` was clean, and
`cargo clippy --profile gate --all-targets --all-features -- -D warnings`
completed with zero warnings.

The definitive six-scenario TTY run passed every named checkpoint against GNU
Emacs 30.2, including exact text, attributes, mode lines, echo areas, cursor
positions, and the pinned semantic records.  The final upstream replays share
dirty-candidate source hash
`d3e1e921facd94d3768aaae2bf9181ba3475d240901dd96b8c8bb996222f34e0`:
`test/lisp/minibuffer-tests.el` matched 31/31 at
`run-1788411883969644000-41459`, `completion-preview-tests.el` matched 11/11
at `run-1788411929234693000-41669`, `completion-tests.el` matched 6/6 at
`run-1788411944841031000-41823`, and `test/src/minibuf-tests.el` matched 65/65
at `run-1788411959437722000-41458`.  The last three small suites reported
test-body slowdowns of about 3.6x, 4.7x, and 7.2x respectively; those are
disclosed performance results, not semantic mismatches.

The authoritative publication gate then ran once, serially, with the optimized
`gate` profile: `LANG=C LC_ALL=C RUST_TEST_THREADS=1
RUST_MIN_STACK=134217728 cargo test --profile gate -- --test-threads=1`.
It exited successfully.  The library target contained 2301 tests: 2299 passed,
zero failed, and exactly two reviewed opt-in TTY end-to-end gates were ignored,
`tty::tty_differential_end_to_end` and `tty::tty_smoke_end_to_end`.  Those
wrappers are not substitutes for issue coverage: the six issue-specific TTY
journeys above ran explicitly and passed, as did the library's minibuffer,
completion, completion-preview, history, recursive-read, keyboard-macro,
redisplay, cursor, and case-conversion regressions.  The compat harness passed
38/38, perf harness 1/1, CLI 13/13, ERT runner 3/3, and package lifecycle 5/5;
the main and doc-test targets contained no tests.  No concurrent test or build
was launched from this checkout while the gate was active, and no production
or test source changed after it completed.

## 2026-09-02 copy family: the native body must see the handler-expanded names

A Linux frozen run of the integrated main (`3c78b1e`) surfaced one new
files-tests mismatch, `files-tests-file-name-non-special-add-name-to-file`,
failing with `(file-missing "Adding new name" ... "…add-name.special")`.
The test installs a handler that rewrites names by stripping `.special`
during any operation it receives, including `expand-file-name`.  In
fileio.c, Fadd_name_to_file (and Fcopy_file, Frename_file,
Fmake_symbolic_link) call Fexpand_file_name and expand_cp_target -- both
handler-aware -- BEFORE the handler lookup; the rewritten names then match
no handler, and the native body links the rewritten names.  Emaxx's
round-1 normalization performed that same expansion but only to choose a
handler: when none claimed the operation, the native arm re-resolved the
raw arguments through the host path resolver and linked the unrewritten
name.  The file-name-handler choke point now returns either the handler's
value or the normalized arguments for the native call, so expansion happens
once, as in C.  Oracle contract through a rewriting handler over all four
operations: `((t t) (t t) ("base") (nil t) ("base" t) file-already-exists)`.
files-tests.el is 116/116 matching (from 115/116).  The failure is
identical on the pre-integration main `8b08bbf`, so it dates from the
round-1 normalization, not from the tty merge.

The same frozen run also stopped at simple-tests.el with "process timed
out during test" and zero Emaxx outcomes.  That is not a hang: the two
`shell-command-dont-erase-buffer` tests spawn a child
`emacs -Q --batch --eval` about seventy times between them, and an Emaxx
child boots in ~15 s (GNU: 0.04 s) because there is no portable dump, so
the file needs well over the 900 s per-file timeout that run was given.
The recorded frozen baselines use `--timeout-seconds 3600`; resumed at that
value (same commit, 335 per-file comparisons reused) the run completed:
**7738/7883 matching, 145 mismatching across 19 of 461 files**, with
simple-tests.el at 52/53 (only the disclosed async-shell latency case).
The boot latency stays the structural limitation it already was (erc's
child-spawning tests, above).  The inventory: src/comp-tests 96 and
lisp/comp-tests 3 (native compilation, in progress elsewhere);
server-tests 7; semantic-utest-ia 6; socks-tests 6; lcms-tests 6;
mml-sec 4; thread-tests 2 + src/thread-tests 2; proced 2; em-prompt 2;
erc 2 (child-boot latency); and one each in files-tests (fixed here),
nadvice, kmacro, editfns, print, process (`lookup-hints-values`) and
simple.

New finding from that inventory, OPEN: the six lcms-tests are skipped by
the Linux oracle (built `--without-lcms2`, `lcms2-available-p` unbound)
but run under Emaxx, because the `lcms2` startup feature is advertised
unconditionally in `STARTUP_FEATURES` rather than following the host C
contract the way `kqueue`/`inotify` now do.  The fix is the same
host-manifest gate; it is recorded here rather than folded into this
change so the copy-family fix stays a single mechanism.

This change was gated with the grouped runner on Linux (2275 library
tests, bins and integration all green; run-1788351345455641394-28877)
after the targeted file-name-handler tests and a 116/116 files-tests
replay, and again after rebasing onto the Darwin certification commit
`1394e8d` (2277 library tests, bins and integration green;
run-1788358068697089711-3005).

## 2026-09-03 finding 151 (formerly numbered 149 here): a Darwin-pinned race in the JUST-THIS-ONE contract

Rebasing the copy-family change onto main `e9778fb` (eleven certification
commits, none touching the copy family; `add-name-to-file` still fails on a
build of that main) and re-gating stopped the grouped runner at one
primitives test that arrived with the Tramp certification (`b5ee077`):
`accept_process_output_just_this_one_suspends_distractor_filters_like_emacs`.
Its failure is on the ORACLE side of the contract -- the Linux GNU answers
`(t "distractor" "target" nil "distractor")` where the literal, pinned on
Darwin, says `(t "" "target" t "distractor")` -- deterministically, three of
three runs in isolation.  The program's distractor is `printf distractor`,
which exits inside the target's 0.15 s window; process.c's status_notify then
"reads any output that remains" from a process whose status changed, whether
or not the wait is JUST-THIS-ONE.  Whether that exit lands inside the window
is host timing, so the literal encoded Darwin's schedule, not the mechanism
the test names.  With the distractor kept alive (`printf distractor; sleep
2`) the Linux oracle answers the pinned literal three of three and Emaxx
matches; the test now uses that form, its assertion unchanged.

The probe also exposed a real gap, recorded as finding 151 rather than
folded into this change: for the short-lived distractor Emaxx answers
`(t "" "target" t "distractor")` on Linux, i.e. it never performs the
status_notify drain of an exited process's leftover output during a
JUST-THIS-ONE wait.  That is a process.c mechanism to port on its own.
(Numbering note, 2026-09-04: the tty-frontend merge section above had
already used 149 and 150 for its two harness defects, so this finding is
151 in the table; the table now also carries rows for 149 and 150.)

With the test corrected, the grouped gate on this two-commit tree over main
`e9778fb` passed every group (2303 library outcomes with the two opt-in TTY
ignores, bins and integration green; run-1788447199271435854-9123).

## 2026-09-04 Proced and SOCKS adversarial audit

This candidate starts from refreshed main `be5e937`.  No native-comp work is
included.  The six Proced outcomes were first attempted inside the restricted
sandbox (`run-1788508362061428000-60937`), where both GNU and Emaxx saw an
empty process table and four outcomes failed with only dynamic PID differences
in their diagnostics.  That run is rejected, not normalized or counted.  The
same unmodified upstream file passed 6/6 outside the sandbox before the fix
(`run-1788508474446621000-61187`) and again on the final candidate
(`run-1788509773203114000-63671`); therefore this batch makes no Proced code
or test change.

The initial SOCKS replay (`run-1788508578949258000-61386`) matched 4/10.  A
real loopback trace identified two shared process mechanisms, rather than six
test-specific exceptions.  Network reads discarded the string's unibyte flag
and always delivered a multibyte filter argument, corrupting bytes 128--255.
Accepted server children also inherited the listener buffer, unlike GNU; test
cleanup then prompted about a live child and received EOF from the harness's
closed stdin.  Connection reads now use the existing coding-aware raw-byte
decoder, and accepted children retain the listener's filter, sentinel, log,
plist, contact, and coding metadata but not its buffer.  The obsolete
always-multibyte wrapper became dead and was removed.

The permanent regression opens a real IPv4 loopback listener and client under
the full GNU batch image, sends `[0 127 128 184 216 255]` with binary coding,
and pins both the unibyte vector and nil accepted-child buffer to the live GNU
Emacs 30.2 oracle result `((nil [0 127 128 184 216 255]) t t open)`.  A first
test setup using only the early Lisp image was rejected because that image does
not load GNU's `binary` coding alias; the assertion and binary input were kept,
and the test was moved to the production-equivalent batch image.  The focused
test then passed.  The final upstream SOCKS replay passed 10/10 with zero
mismatches (`run-1788509716153045000-63470`).

The formal diff audit found changes only in the process runtime, its regression
module, and this ledger.  The upstream Proced and SOCKS files are untouched.
No harness, selector, manifest, fixture, baseline, expected-result,
normalization, timeout, retry, accepted-failure, skip, ignore, warning
suppression, target-name branch, or runtime oracle path was added or changed.
All 15 repository anti-cheat gates passed with zero failures and zero ignored.
`cargo fmt --all -- --check` was clean, and `cargo clippy --profile gate
--all-targets --all-features -- -D warnings` passed with zero warnings.

The authoritative publication gate ran once outside the restricted sandbox,
so its real loopback, PTY, subprocess, and process-inventory tests needed no
sandbox-failure rerun: `LANG=C LC_ALL=C EMAXX_IMAGE_TEMPLATE=1
RUST_TEST_THREADS=1 RUST_MIN_STACK=134217728 cargo test --profile gate --
--test-threads=1`.  The main branch's serial-safe image-template acceleration
reduced repeated setup without changing the selected tests or assertions.  The
library target reported 2301 passed, zero failed, and exactly the two reviewed
opt-in TTY end-to-end wrappers ignored out of 2303 tests in 1393.45 seconds.
The new live binary-network regression passed in that run.  The compat harness
passed 38/38, perf harness 1/1, CLI 13/13, ERT runner 3/3, and package lifecycle
5/5; main and doc-test targets contained no tests.  No concurrent test or
build was launched while this gate was active, and no Rust production or test
source changed after it completed.

## 2026-09-04 Nadvice: interpreted closure identity in arity conditions

The refreshed-main baseline was `225f6f0`.  The exact
`advice-test-called-interactively-p-filter-args` replay failed only because
GNU reported the actual interpreted closure in its expected arity condition,
`(wrong-number-of-arguments #[nil ((cons 1 (called-interactively-p 'any)))
(t) nil nil nil] 1)`, while Emaxx replaced the callee with the symbol
`lambda` (`run-1788519522530890000-79557`).  The test is an upstream expected
failure on both runners; matching only the status would therefore have hidden
this real condition-data mismatch.

The repair is at the shared closure boundary.  Interpreted-lambda arity
checks now retain the actual function object in the condition datum.
`prin1` projects typed closures through their GNU-visible three-to-six slots
and emits readable `#[ARGS BODY ENV ...]` syntax.  The projection trims the
public environment independently of Emaxx's conservative execution storage,
so unused internal activation bindings cannot leak through printing while
macro-converted closures keep the runtime cells they need.  Interpreted
closures are also print-circle candidates and their projected slots are
walked by the iterative cycle scanner.  Permanent tests pin a captured
closure and its wrong-arity condition, an over-captured-but-correctly-running
closure whose unused binding stays out of the printed form, and a closure
whose environment points back to itself.

The adjacent audit rejected two intermediate implementations.  Merely making
all retained environments visible regressed `cconv-safe-for-space`
(`run-1788523152761672000-81724`).  Trimming execution storage rather than
only the public projection then lost generator closed variables and regressed
three Cconv documentation cases (`run-1788523625782055000-82633`).  Neither
result is counted.  On the final source, the complete Cconv file matched
18/18 (`run-1788524218527277000-84146`) and the complete Nadvice file matched
13/13 (`run-1788524273971683000-84144`).  The complete src print file remained
45/46 (`run-1788524336821328000-84640`), with exactly its pre-existing
`print-tests-continuous-numbering-cl-print` mismatch and no new regression.

The formal diff audit found changes only in closure evaluation/projection,
printing, direct Rust regressions, and this ledger.  No upstream test,
harness, selector, manifest, fixture, baseline, timeout, normalization,
retry, accepted-failure path, skip, ignore, expected-result annotation,
warning suppression, test-name production branch, or runtime oracle path was
added or changed.  All 15 anti-cheat tests passed with zero failures and zero
ignored.  `cargo fmt --all -- --check` was clean, and `cargo clippy --profile
gate --all-targets --all-features -- -D warnings` completed with zero
warnings.

The authoritative publication gate ran once outside the restricted sandbox:
`LANG=C LC_ALL=C EMAXX_IMAGE_TEMPLATE=1 RUST_TEST_THREADS=1
RUST_MIN_STACK=134217728 cargo test --profile gate -- --test-threads=1`.
The library target reported 2304 passed, zero failed, and exactly the two
reviewed opt-in TTY end-to-end wrappers ignored out of 2306 tests in 1335.03
seconds.  The compat harness passed 38/38, perf harness 1/1, CLI 13/13, ERT
runner 3/3, and package lifecycle 5/5; main and doc-test targets contained no
tests.  No concurrent test or build ran in this checkout while the gate was
active, and no Rust production or regression source changed afterward.

## 2026-09-04 Print: continuous numbering across `cl-print` boundaries

The refreshed-main baseline was `e5283e9`.  The only Print mismatch was the
upstream expected-failure variant
`print-tests-continuous-numbering-cl-print`.  Both GNU and Emaxx correctly
failed `cl-print`'s unsupported continuous-numbering assertion, but their
condition data differed: GNU's second fragment was `#1=#2=#:g...` and later
referred to the native label as `#2#`, while Emaxx emitted two independent
`#1=` labels and then reprinted the gensym.  The exact failing predecessor is
recorded by `run-1788524336821328000-84640`.

The repair follows GNU print.c's two distinct pieces of state.  Emaxx now
keeps the native printer's largest allocated label as interpreter-owned state,
separate from the dynamically bound public `print-number-table`.
`print--preprocess` resets and advances that counter even when its temporary
table is subsequently unwound.  Native print entry resets the counter when
continuous numbering has no live table, while later calls reuse it when a
table exists.  Finishing a print writes the table through the active special
value cell instead of adding a private binding to a copied evaluator frame,
and it creates an empty retained table only when the printed graph actually
had a circle candidate.  This makes nested GNU Elisp printers observe the same
state transitions without adding any `cl-print`-specific production path.

The prior host regression only asserted that the `cl-print` variant did not
pass its native-printer regex; arbitrary wrong failure output therefore also
satisfied it.  The strengthened regression now pins the complete normalized
GNU `cl-print` string, the public table size, and the gensym's retained native
label `2`.  The existing native continuous-numbering regression still pins
ordinary cross-call reuse, and all three native `print--preprocess` unit cases
remain green.

On final production source, the complete Print file matches 46/46
(`run-1788527973609009000-91720`), including the exact expected-failure
condition.  The adjacent Cconv file matches 18/18
(`run-1788528056494762000-91945`) and Nadvice matches 13/13
(`run-1788528084262349000-92117`).  The formal diff audit found changes only
in typed interpreter printer state, the shared native printer implementation,
the direct host regression, and this ledger.  No upstream test, harness,
selector, manifest, fixture, baseline, timeout, normalization, retry,
accepted-failure path, skip, ignore, expected-result annotation, warning
suppression, test-name production branch, or runtime oracle path was added or
changed.  All 15 anti-cheat tests passed with zero failures and zero ignored;
`cargo fmt --all -- --check` and `git diff --check` were clean, and strict
all-target/all-feature Clippy completed with zero warnings.

The authoritative publication gate ran once outside the restricted sandbox,
after a process scan confirmed that no Cargo, Emaxx, or compatibility-harness
process was running: `LANG=C LC_ALL=C EMAXX_IMAGE_TEMPLATE=1
RUST_TEST_THREADS=1 RUST_MIN_STACK=134217728 cargo test --profile gate --
--test-threads=1`.  The optimized library target reported 2304 passed, zero
failed, and exactly the two reviewed opt-in TTY end-to-end wrappers ignored
out of 2306 tests in 1562.91 seconds.  The compat harness passed 38/38, perf
harness 1/1, CLI 13/13, ERT runner 3/3, and package lifecycle 5/5; main and
doc-test targets contained no tests.  No concurrent test or build ran while
the gate was active, and no Rust production or regression source changed
afterward.

## 2026-09-04 Kmacro: rewound macro input and unread-event precedence

The refreshed-main baseline was `165be13`.  The remaining Kmacro mismatch was
the upstream expected-failure case
`kmacro-tests-step-edit-with-quoted-insert`.  GNU and Emaxx both failed the
assertion, but with materially different buffer contents: GNU produced its
known `ḩii there` result while Emaxx produced ` i there`
(`run-1788530907605950000-98015`).  Comparing only expected-failure status
would therefore have hidden a real command-input divergence.

An instrumented replay established the shared mechanism.  Kmacro's step
editor speculatively calls `quoted-insert`, which reads the octal digits and
their terminating `i`, pushes that terminator onto `unread-command-events`,
and rewinds the public `executing-kbd-macro-index`.  GNU's real command then
honors the rewound index, reads the digits again, and the command loop consumes
the pushed-back `i` before returning to the still-present macro `i`.  Emaxx's
typed cursor had remained at the speculative read's later internal index, so
the real command read the following space instead.

The repair makes every active-macro input read synchronize the typed event
vector and cursor from the public GNU variables.  The keyboard-macro command
loop now also consumes `unread-command-events` before the macro stream and
advances the macro index only for events actually sourced from that stream.
The implementation is generic: it contains no Kmacro symbol, test name, or
quoted-insert branch.  A direct regression uses a fresh local keymap and an
anonymous command to pin both behaviors independently: a pre-command hook
speculatively reads and rewinds, the command pushes its reread event back, and
the command loop must produce `bc` with no unread events left.

The final exact replay matched 1/1 (`run-1788531529578441000-98984`), including
GNU's unchanged expected-failure condition, and the complete Kmacro file
matched 58/58 (`run-1788531744591804000-99355`).  Optimized adjacent Rust
filters passed 13/13 keyboard-macro tests, 1/1 unread-command test, and 7/7
Kmacro-frontier tests.

The formal diff audit found changes only in the shared keyboard-macro input
runtime, its direct Rust regression, and this ledger.  The pinned upstream
Kmacro file is clean.  No upstream test, harness, selector, manifest, fixture,
baseline, expected-result, normalization, timeout, retry, accepted-failure,
skip, ignore, warning suppression, test-name production branch, or runtime
oracle path was added or changed.  All 15 repository anti-cheat gates passed
with zero failures and zero ignored.  `cargo fmt --all -- --check` and `git
diff --check` were clean, and `cargo clippy --all-targets --all-features -- -D
warnings` completed with zero warnings.

After a global process scan found no Cargo, Emaxx, or compatibility-harness
process, the authoritative publication gate ran once outside the restricted
sandbox: `LANG=C LC_ALL=C EMAXX_IMAGE_TEMPLATE=1 RUST_TEST_THREADS=1
RUST_MIN_STACK=134217728 cargo test --profile gate -- --test-threads=1`.
The optimized library target reported 2305 passed, zero failed, and exactly
the two pre-existing reviewed opt-in TTY end-to-end wrappers ignored out of
2307 tests.  The new regression and all adjacent macro-input cases passed in
that run.  The compat harness passed 38/38, perf harness 1/1, CLI 13/13, ERT
runner 3/3, and package lifecycle 5/5; main and doc-test targets contained no
tests.  No concurrent test or build ran while the gate was active, and no Rust
production or regression source changed afterward.

## 2026-09-04 Linux de-cheating audit of main `225f6f0` (findings 152-154)

Scope: everything that landed on main after the Linux integration audit of
`3c78b1e` -- the networking (`dcd5b82`), TRAMP (`b5ee077`), minibuffer and
completion (`e9778fb`), Vertico stack (`696fa6c`), Darwin certification
(`1394e8d`), oracle-feature-contract (`be5e937`) and binary-network
(`225f6f0`) commits, plus the two copy-family commits from this host.  Each
production diff was re-read against its GNU 30.2 owner; the static scans
covered test-only runtime branches (`cfg(test)` sites are test modules,
counters and a bootstrap permit only), `EMAXX_*` knobs (the compat runner's
`EMAXX_COMPAT_RUNNER` is a report label, never a branch), ignored tests
(the two declared PTY gates), oracle delegation (harness and tests only),
and fixture or package-name literals in dispatch (none).  The grouped gate
on this exact main passed natively as the unprivileged runner
(run-1788519823835685981-1802: 2304 library outcomes with the two opt-in
ignores, bins and integration green), so the Darwin-certified Rust suite
holds on Linux.

Verified sound against the C owner: `network-lookup-address-info` through
`getaddrinfo` with AI_NUMERICHOST; `make-process` returning nil for an empty
plist and dispatching `:file-handler` before parsing; JUST-THIS-ONE's integer
spelling suppressing timers (process.c 4920); SIGUSR1/2 through
async-signal-safe counters into the keyboard path; `window-list`'s MINIBUF
rule; post-command-hook at read_minibuf's command-loop entry; casefiddle's
interval rule; the accepted connection's buffer and binary delivery.

Findings:

- 152 (OPEN): `(sleep-for 0)` runs due Lisp timers.  dispnew.c Fsleep_for
  enters wait_reading_process_output only for a positive duration and
  otherwise returns nil at once; oracle `(run-at-time 0 …) (sleep-for 0)`
  reads nil, Emaxx t.  The TRAMP commit kept an Emaxx-only "delivery point"
  there deliberately; it is a non-GNU accommodation and should go.
- 153 (OPEN): batch `read-from-minibuffer` reads stdin even with
  `executing-kbd-macro` bound; minibuf.c takes read_minibuf_noninteractive
  only when `noninteractive && NILP (Vexecuting_kbd_macro)`.  Under a bound
  macro GNU returns `("" ("dflt"))` -- and adds the accepted default to the
  history, which the completion commit implemented but gated on
  TTY-event-reader presence plus `noninteractive` nil rather than on GNU's
  condition.  Equivalent for the live terminal, wrong for the macro path,
  where Emaxx signals "Error reading from stdin".
- 154 (FIXED here): `expand_file_name_runtime` treated a None
  DEFAULT-DIRECTORY as the process cwd; Fexpand_file_name substitutes the
  buffer's `default-directory`.  Latent while the helper only chose handlers,
  it became a regression when the copy-family commit fed its expansions to
  the native bodies: `(let ((default-directory "/tmp/zzdir/")) (copy-file
  "a" "b"))` failed with `/tmp/a`.  The Linux frozen run of this main caught
  it as `arc-mode-test-zip-ensure-ext` and
  `bytecomp-tests--target-file-no-directory`, both passing on `e9778fb`.
- Ledger hygiene: finding number 149 had been used twice (the tty-frontend
  merge's harness defects in prose, the status_notify drain in the table);
  the table now carries 149-151 unambiguously.

The finding-154 fix was verified by the probe (`(t t)` on both binaries), the
extended copy-family contract, replays of arc-mode-tests 4/4, bytecomp-tests
100/100 and files-tests 116/116, and the grouped gate on the fixed tree
(run-1788523328744887321-24455: 2304 library outcomes with the two opt-in
ignores, bins and integration green).

Frozen score of this main (`225f6f0`, unfixed) on Linux with the 3600 s
per-file timeout: **7743/7883 matching, 140 mismatching across 461 files**,
up from 7738 at `3c78b1e`.  Flipped since then: socks-tests 10/10 (from
4/10), files-tests 116/116, process-tests `lookup-hints-values` (the
getaddrinfo port).  Appeared: the two finding-154 regressions plus
`wdired-test-symlink-name`, all three green again with the fix in this
change (arc-mode 4/4, bytecomp 100/100, wdired 7/7), so the fixed tree
stands at 7746 by that arithmetic.  Also present, and new as a Linux
observation, is finding 155: proced-tests 4/6 here, because both refinement
tests carry `(skip-when (eq system-type 'darwin))` and the Darwin
certification's 6/6 was four passes plus two matching skips.  On Linux the
oracle runs them and Emaxx's `process-attributes` lacks `pcpu` (and fifteen
other sysdep.c keys), so `proced--cpu-at-point` reads nil.  That is a
/proc/PID/stat port to schedule, not a test problem.  The remaining
inventory is unchanged from the 2026-09-02 list: native comp 99, server 7,
semantic 6, lcms 6 (the `lcms2` feature flag), mml-sec 4, threads 2+2,
em-prompt 2, erc 2 (child-boot latency), and one each in nadvice, kmacro,
editfns, print and simple.

## 2026-09-04 Linux oracle repin, `process-attributes` (155) and program search (156)

Base: main `f5577e8`, with the finding-154 fix rebased on top.

**Linux oracle.**  The pinned Linux binary deviated from
docs/oracle-build-contract.md: it was configured `--without-lcms2`, lacked
HarfBuzz, and had picked up libotf and m17n-flt.  It was rebuilt at the same
source commit (`7917fc9`) with `--with-native-compilation --with-x
--with-x-toolkit=no --with-tree-sitter --without-imagemagick --with-lcms2
--with-harfbuzz --without-libotf --without-m17n-flt`; `system-configuration-features`
now lists HARFBUZZ and LCMS2 and neither LIBOTF nor M17N_FLT, and
`(lcms2-available-p)` is `t`.  The Linux C-primitive manifest was regenerated
from that binary with the generator and rustfmt: the only change is the eight
`lcms.c` primitives gaining their arities (`GNU_C_PRIMITIVE_AVAILABLE_COUNT`
1446 to 1454), which the anti-cheat regeneration gate now requires.  The
local lock was repinned (uncommitted, as before).  test/src/lcms-tests.el
executes on both sides and matches 6/6 with six real passes
(run-1788531264408712921-10454); it was six matching skips before.  Both
build documents now record this configuration.

**Finding 155 (FIXED).**  `process-attributes` on Linux is a port of
sysdep.c `system_process_attributes` (GNU_LINUX): euid/user/egid/group
from the /proc/PID owner, comm and the `stat' fields between the last `)`
and field 22, ttname through /proc/tty/drivers with GNU's major/minor
decoding, the jiffies over `_SC_CLK_TCK` as old-style times, start/etime
from /proc/uptime with `now` truncated to whole ticks, pcpu as
`100 (s+u) / (hz etime)`, vsize/1024, rss*4, pmem against MemTotal, and the
command line with the NUL separators as spaces and whitespace or backslashes
inside an argument escaped (`c_isspace`, so vertical tab counts).  The 31
keys come out in GNU's consed order.  Probe against the oracle on a `sleep`
child: identical key order, identical types, identical fixed fields, and
identical escaping of `"a b" "c\\d"`.  proced-tests is 6/6 on Linux, the
two refinement tests now real passes on both sides.

**Finding 156 (FIXED), found while comparing the `args` attribute.**  The
oracle reported `/usr/bin/sleep 5` where Emaxx said `sleep 5`: `make-process`
never ran process.c's openp search.  The full divergence set, each probed on
both binaries before and after: an empty `exec-path` still started the
program; a missing program surfaced as the spawn failure (`(error "No such
file or directory (os error 2)")`) instead of `(file-missing "Searching for
program" ...)`; a directory found through `exec-path` was a permission error
instead of `(file-error "Searching for program" "Is a directory" ".")`; an
absolute directory was not `(error "Specified program for new process is a
directory")`; `call-process` had the same gaps and rendered EACCES as
`file-error` where fileio.c report_file_errno says `permission-denied`;
`file-executable-p` was nil for `/usr/bin` (fileio.c is a plain faccessat
X_OK); an absolute program that cannot be executed was a synchronous host
error string where GNU's pty path (callproc.c emacs_spawn, vfork) leaves a
child that writes `<argv0>: <program>: <strerror>` and exits 127 for ENOENT
or 126 otherwise, and GNU's pipe path (posix_spawn) signals
`(file-missing "Doing vfork" "No such file or directory")` with no file in
the data; and glibc's `execvp` ran an ENOEXEC file through `sh` (exit 2 and
a shell syntax error) where GNU's `execve` fails with "Exec format error".

The repair: `locate_file_search` is openp with GNU's errno bookkeeping
(ENOENT initially, EISDIR for an accessible directory, any other failure
replacing it) and a fixnum predicate now uses `faccessat` with AT_EACCESS;
`locate_program_for_exec` applies process.c's and callproc.c's two call
shapes; `report_file_errno`'s EACCES condition is `permission-denied`
everywhere `file_operation_error_value` is used; `file_executable_p` is
`faccessat X_OK`; and both spawn paths install a pre-exec hook that runs
`execve` with the prepared argv/envp and, on failure, either exits like
GNU's vfork child (pty) or hands the errno back so the parent signals
"Doing vfork" (no pty).  The process's `process-command` keeps the name the
caller gave.  All probe sets read identically on the two binaries afterwards
(the perror prefix is each binary's own argv[0] by construction).

**Finding 157 (OPEN, architectural).**  `(process-attributes (emacs-pid))`
reports `state` "S" and `thcount` 2 in Emaxx where GNU reads "R" and 1,
because Lisp runs on a spawned thread and /proc/PID/stat describes the
blocked main thread.  No test depends on it; it is disclosed rather than
special-cased.

Three oracle contracts pin the work in-process against the live oracle:
`process_attributes_follows_sysdep_procfs` (Linux),
`program_search_follows_openp_over_exec_path` and
`exec_failure_follows_emacs_spawn` (unix).  The gate-profile subset of 14
process, call-process and locate-file tests passed; strict Clippy across all
targets and features is clean; `cargo fmt` and `git diff --check` are clean.
No upstream test, harness, selector, fixture, timeout, normalization or
accepted-failure path changed; the manifest change is the reviewed baseline
change the contract document called for.

Upstream replays on the finished tree, against the rebuilt oracle:
process-tests 37/37 (run-1788532988861563508-14438), callproc-tests 3/3
(run-1788533097034612335-14752), fileio-tests 16/16
(run-1788533311366938884-14878), files-tests 116/116
(run-1788533333965301814-14956), proced-tests 6/6
(run-1788533366766930806-15292), lcms-tests 6/6
(run-1788533399318533597-15371) and subr-tests 61/61
(run-1788533422743571228-15450).  A first simple-tests replay at the default
180 s per-file timeout was cut off in the shell-command tests, as every
Linux run of that file is; at 3600 s it is 52/53
(run-1788533678234430000-15805), the one mismatch the boot-bound
`simple-tests-async-shell-command-30280` already on the ledger.

The grouped gate ran once on the final tree, rebased onto main `f5577e8`,
as the unprivileged user with no other build or harness running
(target/grouped-gate/run-1788534689653402280-16698): eval_01 351,
eval_02 284, eval_03 322, eval_04 248, eval_05 350, primitives 368,
compat_runtime 82, tty 56 (the two reviewed opt-in end-to-end wrappers
ignored), batch 43 and lightweight 207 library tests passed with zero
failures, bins and integration passed, GROUPED GATE PASSED.

## 2026-09-05 Eshell prompt fields through direct bytecode argument storage

The Eshell work began on main f5577e8 and was refreshed before final
verification to b50bdd2.  The five incoming commits covered
expand-file-name, the Linux process/oracle work and their ledger entries;
none overlapped the bytecode argument boundary or the Eshell regression.
The branch was fast-forwarded to that main before the final focused replays.

Baseline exact replay of
em-prompt-test/next-previous-prompt-{1,2} was 0/2
(target/compat/run-1788534663677528000-6196).  After a failed command,
GNU's field at point contained the command input while Emaxx left the error
diagnostic joined to it.  Instrumenting the actual Lisp call chain showed
that interpreted lambda binding preserved the diagnostic string's identity,
but the packed direct-argument path of a genuine byte-code function left a
compact host Value::String on the VM stack.  put-text-property promoted and
mutated a separate Lisp string object, so the caller never observed the new
field property.  A tentative concatenation-level change did not alter
either failure (target/compat/run-1788535753680690000-7311) and was removed
in full.

The production repair is at the general representation boundary:
run_with_stack applies Interpreter::stored_value to supplied positional
arguments, exactly as interpreted lambda binding already does.  There is no
Eshell package name, test name, selector or expected output in production.
A VM regression constructs a real packed byte-code object, passes it a
compact native string, mutates the returned argument and requires the text
property to remain visible.  A separate initialized-runtime regression
executes the upstream Eshell prompt navigation test through its real Lisp
owner.

On the refreshed base the exact upstream pair passed 2/2
(target/compat/run-1788538681872351000-11973) and the complete upstream file
passed 9/9 (target/compat/run-1788538758913123000-12347).  The bytecode
module passed 32/32; the two focused Rust regressions each ran and passed
1/1.  The anti-cheat gates passed 15/15 with zero ignored, strict
all-target/all-feature Clippy passed with -D warnings, and rustfmt plus
git diff --check were clean.  The upstream
test/lisp/eshell/em-prompt-tests.el, compatibility harness, selectors,
manifests, fixtures, timeouts, normalizations and accepted-failure paths
were unchanged.  The only two ignored Rust tests remain the reviewed,
opt-in real-TTY wrappers.

The optimized serial library gate was run once in the restricted runner:
2,296 passed, the two reviewed TTY wrappers were ignored, and 13
subprocess/socket/TLS/local-HTTP cases were denied by the sandbox.  Per the
instruction not to repeat the full gate, only those exact 13 cases were
replayed outside it: 12 passed and the remaining case exposed a fixed
Linux-only expectation newly added by dc8ea49 against the Darwin oracle,
not an Eshell failure.  That refreshed-main integration defect is repaired
and audited separately below.  The targets the stopped library command had
not reached were run serially: compat-harness 38/38, perf-harness 1/1,
CLI 13/13, ERT runner 3/3, package lifecycle 5/5, and zero-test main/doc
targets clean.

## 2026-09-05 Darwin exec-failure contract after the main refresh

Main b50bdd2 introduced dc8ea49 from a Linux-oracle investigation.  Its new
exec_failure_follows_emacs_spawn test was enabled for every Unix host but
hard-coded the Linux PTY diagnostic.  On this host the pinned GNU Emacs
30.2 oracle consistently returned
((127 exit nil) (126 exit nil) ...), while the new expectation required the
diagnostic strings.  Emaxx itself returned the Linux form, so this was both
a failing test on untouched refreshed-main code and a real Darwin
compatibility difference; it was not classified as a sandbox failure.

The correction keeps fixed, reviewable contracts on both sides of the
platform boundary.  Linux and other Unix builds retain the child diagnostic
and exit codes added by dc8ea49.  On macOS the child preserves exit 127 for
ENOENT and 126 for other exec failures without writing that diagnostic into
the PTY, matching the pinned Darwin oracle.  The Rust contract contains an
explicit Darwin expected value and retains the existing explicit non-Darwin
value.  It does not ignore the test, copy a live oracle answer into the
expected result, branch on a test name, or weaken either assertion.

The corrected exact contract ran outside the sandbox and passed 1/1 against
both the GNU oracle and the in-process Emaxx interpreter.  The unchanged
program-search neighbor had already passed in the optimized serial library
gate.  Strict all-target/all-feature Clippy passed with -D warnings, rustfmt
and git diff --check were clean, and the anti-cheat gates passed 15/15 with
zero ignored.  No upstream file, compatibility harness, selector, manifest,
fixture, timeout, normalization or accepted-failure path changed.  Per the
instruction not to repeat the complete gate, the final evidence is the
single earlier optimized serial run plus exact outside-sandbox replays of
its 13 denied/failing cases, the corrected Darwin contract, and the
separately completed bin, integration and doc targets recorded above.

## 2026-09-04 closing the open ledger: findings 86-97, 116-130, 151-153

Base: main `b50bdd2`, with main `cb4ceb5` (the Eshell output-field and
Darwin exec-failure commits) merged before the final gate.  Every item
below was probed on the Linux oracle
first and again on the finished Emaxx binary; each mechanism named is the
GNU one, and each fix has an in-process oracle contract
(`assert_oracle_contract_matches_interpreter`, which sends the program to
the live oracle and requires the interpreter to print the same).

**86 (FIXED).**  xfaces.c: `color-distance` accepts color names, RGB lists
and the METRIC function, and computes Riemersma's weighted distance in
64-bit arithmetic over `tty-defined-color` (the Lisp `tty-color-desc' path,
with the empty name and `unspecified-fg`/`unspecified-bg' answering
black); `color-values-from-color-spec` is `parse_color_spec` (numeric `#',
`rgb:' and `rgbi:' forms only, `round_ties_even' for `rgbi:');
`color-gray-p` and `color-supported-p` go through the same tty color table.
The five-name Rust table is gone.  Contract:
`tty_color_primitives_follow_xfaces_c`.

**89 (FIXED).**  regex-emacs.c ISPUNCT: beyond ASCII, `[:punct:]` is "not
word syntax" in the current syntax table, so it now depends on the table
like `[:space:]` does (regexp, `skip-chars-forward` and the syntax
snapshot all include it).  Contract:
`punct_class_beyond_ascii_follows_buffer_syntax`.

**92, 95, 96 (FIXED).**  editfns.c `message' formats through
`format-message'; print.c print_error_message applies
`substitute-command-keys' to the `error-message' property (batch's
unhandled-error line renders through `error-message-string' too);
doc.c default_to_grave_quoting_style consults the standard display table
(a display-table char-table whose U+2018 entry is `[96]` means grave); the
effective style reads the C-forwarded `text-quoting-style' slot.  data.c
DEFVAR_BOOL coercion: the 177 `DEFVAR_BOOL' variables (a generated table,
`gnu_c_bool_variable_manifest_matches_fresh_regeneration` regenerates it from
the pinned GNU src/*.c as a mandatory anti-cheat gate) store `t'/`nil', and
`makunbound' detaches the forwarded slot so a
later binding is an ordinary Lisp value, as in GNU.  Contracts:
`quoting_style_reaches_message_error_text_and_display_table`,
`defvar_bool_stores_coerce_and_makunbound_detaches`.

**93 (FIXED).**  fns.c Frequire names `(car (car load-history))` when the
file loaded without providing the feature, and "Required feature `%s' was
not provided" otherwise.

**97 (FIXED).**  eval.c Fcommandp order: void -> nil; string and vector
macros; the builtin command table; autoload and lambda lists; interpreted
and compiled closures by their interactive form; a symbol chain walked one
`logical_function_binding' step at a time, signalling "Found an
`interactive-form' property!" where GNU does; OClosures last.  Contract:
`commandp_follows_fcommandp_order_and_property_error`.

**116 (FIXED).**  `system-configuration` is the configure-time triple:
build.rs derives config.guess's answer for the target (x86_64-pc-linux-gnu
here, `<arch>-apple-darwin<release>' on macOS) and embeds it as a
`cargo:rustc-env`, so the value no longer drifts with the running host.
`UnameField::Machine` and the dead static went with it.

**118 (FIXED).**  process.c network_interface_list: `if-addrs' with the
`link-local' feature reports the fe80 rows, and the list is consed
newest-first as GNU's getifaddrs walk produces it.

**120 (FIXED).**  lread.c readevalloop: `eval-region' with a nil
READ-FUNCTION uses `load-read-function' (only a literal `read' takes the
C reader), a custom reader's end-of-file propagates instead of being
swallowed, nothing is re-interned behind the reader's back, and
`eval-buffer' returns nil.  Contract:
`eval_region_delegates_to_load_read_function_without_reinterning`.

**122 (FIXED, and the earlier contract corrected).**  eval.c increments
`lisp_eval_depth' in eval_sub and again in Ffuncall.  A direct call now
costs one unit per level and `funcall'/`apply'/`mapc' two; the flag
`direct_form_call' marks the eval_sub entry so `call_function_value_named'
adds the Ffuncall unit only for calls that did not come from a form.  The
contract's first expectation had been transcribed as 200 for
`(funcall #'f ...)' on the belief that the loader rewrites it into a direct
call; the oracle answers 100 (the closure body keeps the `funcall'), so the
expectation is now the oracle's answer, which Emaxx matches:
`lisp_eval_depth_counts_ffuncall_entries_like_eval_c` (100 200 100 100
100).

**124 (FIXED).**  thread.c: a child blocked in `sleep-for' runs the timers
that come due with its own specpdl (the joiner's `let' swapped out, the
child's visible), and `thread-join' itself runs nothing.  Contract:
`timers_run_inside_a_child_threads_sleep_with_its_bindings`.

**125 (FIXED).**  thread.c Fthread_signal: signalling the current thread
signals at once; signalling the main thread queues a THREAD_EVENT
`(thread-event THREAD ERROR-SYMBOL DATA)' that keyboard.c's
special-event-map dispatches to `thread-handle-event' (the initial
special-event-map bindings are now installed at startup; callint.c's KEYS
argument makes `(interactive "e")' see the event).  Contract:
`thread_signal_queues_a_thread_event_for_the_main_thread`.

**126, 128, 129 (FIXED): coding.c detection and the ISO-2022 codec.**
Detection is a port of coding.c: the 21 coding categories in enum order,
each category's representative coding system (taken by the first
definition of the category or a redefinition of the representative, and
by `set-coding-system-priority', which also re-points the
`coding-category-XXX' variables and `coding-category-list'; the variables
start out as `no-conversion' and the list in enum order, as syms_of_coding
leaves them, and mule-conf.el's own priority calls produce GNU's batch
order), `detect_coding_system' (the head scan that tries ISO-2022 at the
first ESC/SO/SI, notes null and 8-bit bytes, then runs the
detect_coding_utf_8/_utf_16/_iso_2022/_charset/_sjis/_big5/_ccl/_emacs_mule
ports over the representatives in priority order, the eol subsidiary
chosen per candidate by detect_eol, and Fset_coding_system_priority's
re-prioritisation) and `detect_coding' (the decode-time driver, with
`prefer-utf-8', the null-byte and ISO-escape inhibit attributes and
variables, and utf-8-auto/utf-16-auto BOM decisions).  The charset
detector reads the representative's `charset_valids' table and
`latin-extra-code-table'; the ISO detector reads the six ISO
representatives' safe-charset tables.  `define-charset-internal' now fills
the ISO_CHARSET_TABLE slot from `:iso-final-char' (dimension and the
94/96 flavor from the code space) and maintains Viso_2022_charset_list,
which `set-charset-priority' reorders.

The ISO-2022 codec (decode_coding_iso_2022 / encode_coding_iso_2022) runs
over the attributes mule.el hands `define-coding-system-internal':
initial designations, register usage, the request alist and the flag
bits.  Decoding handles designations (short and long form, revision
prefixes), locking and single shifts, CSI, direction sequences, CTEXT
extended segments and embedded UTF-8, `use-roman'/`use-oldjis', the
invalid-code recovery (the byte comes through and G0 resets to ASCII),
and reports the `charset' runs that produce_charset turns into text
properties (a run opens at the first non-ASCII charset character and
closes only at a different non-ASCII charset, so "こんa" carries one
japanese-jisx0208 span; regions and `insert-file-contents' get the
properties too, a unibyte destination keeping the character offsets as
GNU's produced_char count does).  Encoding designates and invokes on
demand, resets at eol/control characters, designates at bol, takes the
default character for the unencodable (`?' under the `safe' flag), and
prefers the `charset' text property's charset as CODING_ANNOTATE_CHARSET
does.  One GNU quirk is reproduced deliberately: Fdefine_coding_system_internal
seeds `safe_charsets' with register 0 for every charset of an explicit
`:charset-list' before setup_iso_safe_charsets runs, and that function
returns at once when the string exists, so `:request' registers and
`:reg-usage' only ever apply to `iso-2022' (full-support) systems -- the
oracle encodes iso-2022-kr's KSC5601 with `ESC $ ( C` in G0 and no locking
shift, and so does Emaxx now.  `encode_coding_raw_text' writes a multibyte
source in its internal spelling (`undecided' and `raw-text' both), the
charset encoder's offset method rejects characters outside the code space
(latin-iso8859-1 was claiming U+20AC), `unencodable-char-position' is a
real primitive (STRING and COUNT), and code_convert_string's ASCII fast
path applies to strings only: a region goes through decode_coding_object,
so `(decode-coding-region ... 'undecided)' on 7-bit ISO-2022 bytes decodes
where `decode-coding-string' returns them unchanged, exactly as the oracle
does.

Compositions inside ISO-2022 text (ESC 0..4 ... ESC 1) are parsed and
their characters produced, but the `composition' text property GNU's
produce_composition adds is not (disclosed residual; no test exercises
it).  The sjis, big5, euc-jp, emacs-mule and charset-type decoders still
produce no `charset' properties (a pre-existing gap now stated
explicitly).  Contracts: `coding_detection_follows_detect_coding_system`,
`iso_2022_and_raw_text_encoders_follow_coding_c`,
`iso_2022_decoder_annotates_charsets_and_detection_reaches_regions_and_files`.

**130 (FIXED, ledger row lagging the code).**  compat.rs has refused to
fall back silently since `9c89a7c`: a failed oracle load-path probe is
reported loudly.  The row is closed to match.

**151 (FIXED).**  process.c status_notify: during a JUST-THIS-ONE wait the
other processes are still refreshed, and one that has exited has its
remaining output drained and delivered before its sentinel runs.
Contract: `just_this_one_wait_still_notifies_an_exited_distractor`.

**152 (FIXED).**  dispnew.c Fsleep_for returns without entering the wait
(no timers) for a non-positive duration.  Contract:
`sleep_for_zero_returns_without_waiting_or_running_timers`.

**153 (FIXED).**  minibuf.c read_minibuf: batch stdin is read only when
`noninteractive' and `executing-kbd-macro' is nil; otherwise unread
events, then the macro, feed the reader; the history push of the value or
the accepted default follows the full path, not TTY-reader presence.
Contract: `minibuffer_reads_under_a_keyboard_macro_follow_read_minibuf`.

**117 (measured again, not reproducible here).**  The intermittent gate
test is `upstream_eshell_script_regressions_stay_green`.  On this Linux
host, against the rebuilt Linux oracle, it was run 20 times back to back
on the finished tree while the frozen run loaded the machine (load average
1.0 to 4.8): 20 passes.  Earlier in the session: 12/12 and 6/6 on the
same tree.  To decide whether this delivery is what changed, the same 20
loaded runs were made on main `b50bdd2` (before any of this work) built in
the scratch worktree: also 20 passes (load 2.0 to 4.7).  So 78 runs, 40 of
them under load, 0 failures, on both sides of the change -- the 50-75%
rate the ledger recorded on 2026-08-27 does not reproduce on this host at
all, and this delivery cannot claim to have fixed it.  The row is closed
as "not reproducible on the Linux gate host", with the earlier figure
kept on record; if it recurs, the diagnosis in the 2026-08-27 entries (a
process leaving `eshell-process-list' before its output is delivered) is
where to resume, and retrying until green remains off the table.

**Verification.**  Every probe program above was run on the oracle and
on the finished binary and compared byte for byte (the coding programs
in both the `--eval' and the `-l' forms).  The gate-profile subset of 130
coding, charset, process, thread, minibuffer, color, syntax and
evaluation tests passed once the one transcribed expectation it exposed
(`encode_coding_region_binary_returns_unibyte_string` expected 137 65 for
a multibyte U+0089 under `binary'; the oracle answers 194 137 65, which
is now the expectation) was corrected; strict Clippy across all targets
and features, `cargo fmt --check` and `git diff --check` are clean.  The
first grouped gate then failed two eval_04 tests
(`loaded_timer_queue_fires_during_waits`,
`nonlocal_exit_from_timer_preserves_later_due_timers`) that had pinned
the pre-152 behaviour of `(sleep-for 0)' running due timers; the oracle
answers nil for both as written and t with a positive wait, so both now
wait 0.01 s.  The second gate then failed eval_05's
`read_buffer_simulation_enforces_its_predicate_and_accepts_default`,
which fed `read-buffer' through `unread-command-events' in batch: the
oracle reads stdin there ("Error reading from stdin", finding 153) exactly
as Emaxx now does, and answers ("#chan" "#fake") when the input is an
executing keyboard macro, which the test now uses.  Three transcribed
expectations found by fixing the mechanisms they had baked in.

The Linux frozen run of the finished tree (worktree at the delivery head
with the local pin commit, `--timeout-seconds 3600`,
frozen-1788550274747799926-17495): TESTS 7757/7883 matching, 126
mismatching, across 461 files -- the same 7757 the previous run had
derived, with no file failing that did not fail in that run and eight
files (arc-mode, bytecomp, nadvice, kmacro, proced, wdired, lcms, print)
no longer failing.  The 126 remaining are the disclosed classes: native
compilation (99), server (7), semantic (6), mml-sec (4), threads (4),
em-prompt (2), erc (2), editfns (1) and the boot-bound simple-tests
async-shell-command row (1).

The grouped gate ran on the merged tree as the unprivileged user with no
other build or harness running
(target/grouped-gate/run-1788564883648369393-15312): eval_01 351,
eval_02 284, eval_03 322, eval_04 248, eval_05 351, primitives 384,
compat_runtime 82, tty 56 (the two reviewed opt-in end-to-end wrappers
ignored), batch 43 and lightweight 208 library tests passed with zero
failures, bins and integration passed, GROUPED GATE PASSED.  Three earlier
gate runs on the way there each failed exactly one group on one test
that had pinned pre-fix Emaxx behaviour (the two timer tests, the
read-buffer test and the xfaces family test, all described above); each
was corrected to the oracle's answer before the next run, none was
retried as-is.

**127, 157 (OPEN, structural).**  Unchanged: supra-Unicode characters
cannot live in Rust strings and buffers (127), and the Lisp thread is not
the process's main thread (157).  Neither has a GNU-faithful route short
of a representation or thread-model change, so both stay disclosed.

## 2026-09-05 Editfns coding-region and replace-match change hooks

Base: main `cb4ceb5`.  The sole Editfns mismatch was not an outcome-status
disagreement: both GNU Emacs 30.2 and Emaxx reached the test's declared
`:expected-result :failed`, but GNU reported the Bug#65451 checker errors
`buffer-size 22 == 29` and `buffer-size 22 == 15`, while Emaxx instead
reported a later `ENCODE-CODING-REGION` imbalance (`buffer-size 28 == 25`).
The exact baseline is run-1788563669652448000-35909.

The first repair follows coding.c's same-buffer conversion path:
`encode-coding-region` and `decode-coding-region` now issue one ordinary
before/after change pair around their in-place replacement, with the old
character length in the after call.  That removed the spurious coding-region
failure and exposed the upstream Bug#65451 behavior rather than manufacturing
an ERT failure.

The remaining sequence was checked against `src/search.c:Freplace_match`, not
inferred from the test.  GNU inserts the raw replacement with `replace_range`,
then invokes `Fupcase_region` or `Fupcase_initials_region`, then emits the outer
`signal_after_change`.  Consequently a case-adapting replacement produces an
outer before call, a nested ordinary casing before/after pair, and finally the
outer after call.  GNU's casefiddle.c additionally narrows the casing after
call to the first and last characters that actually changed, while a no-op
case conversion has a before call and no after call.  Emaxx now follows those
general primitive sequences.  Direct Rust regressions pin the coding-region
pair, the generic nested `replace-match` trace, and the changed-subspan/no-op
case-region contract; production contains no Editfns/Dabbrev test name,
Bug#65451 branch, expected result, or diagnostic string.

The exact Bug#65451 replay matches 1/1
(run-1788564463350037000-37081), and the complete unmodified upstream
`test/src/editfns-tests.el` file matches 23/23 with zero mismatches
(run-1788564730662734000-37585).  Before the final changed-subspan refinement,
the broad `eval_04` owner module passed 250/250 with zero ignored, including
adjacent buffer, coding, Unicode casing, overlay, marker, property and undo
behavior; all three focused hook regressions then passed on the refined tree.
GitHub issue #51 records the post-7,883 investigation into repairing the
upstream nested-hook protocol; the pinned GNU Emacs 30.2 mode must retain the
exact behavior established here.

On the refined tree, the exact Bug#65451 replay again matched 1/1
(run-1788566751925079000-41387).  All 15 anti-cheat gates passed with zero
ignored; rustfmt was clean; strict all-target/all-feature Clippy passed with
`-D warnings`; `git diff --check` was clean; and neither the upstream test nor
the harness, selectors, manifests, fixtures, timeouts, normalization or
accepted-failure paths changed.  The optimized full serial gate ran once
outside the restricted sandbox so its subprocess, socket, TLS and local-HTTP
contracts executed normally: 2,312 library tests passed with zero failures
and only the two reviewed opt-in real-TTY wrappers ignored; compat-harness
38/38, perf-harness 1/1, CLI 13/13, ERT integration 3/3 and package lifecycle
5/5 passed, with doc tests clean.  The final `eval_04` owner module, including
all three new hook regressions, is part of that successful full gate.

## 2026-09-05 close-findings bundle and Editfns integration audit

The incoming close-findings bundle at `d03e9d6` was squash-integrated with the
pending Editfns hook work on main `cb4ceb5`, preserving both the bundle's
decoded `charset` properties and the ordinary before/after change protocol.
The combined diff changes no pinned GNU test, compatibility harness, selector,
regression manifest, fixture, timeout, normalizer, or accepted-failure data.
Production additions contain no oracle execution, test-name branch, expected
result, or compatibility-result special case.  The only ignored tests remain
the two reviewed opt-in real-TTY wrappers.

The generated 177-name GNU `DEFVAR_BOOL` inventory originally had only an
ordinary unit freshness test even though production store behavior depends on
it.  This integration promotes regeneration from the pinned GNU 30.2 C sources
to the mandatory compatibility anti-cheat preflight.  All 16 anti-cheat gates
passed with zero ignored.  `cargo fmt --all -- --check`, `git diff --check`,
and strict gate-profile Clippy across all targets and features with
`-D warnings` were clean.

The complete unmodified upstream `test/src/editfns-tests.el` replay matched
23/23 with zero mismatches
(`target/compat/run-1788571897231705000-51568`).  The optimized full serial gate
then ran once outside the restricted sandbox: 2,328 library tests passed with
zero failures and only the two reviewed TTY wrappers ignored; compat-harness
passed 38/38, perf-harness 1/1, CLI 13/13, ERT integration 3/3, package
lifecycle 5/5, and doc tests were clean.  No 7,883-test corpus run was made as
part of this integration.

## 2026-09-05 coding residuals: every decoder's `charset' properties, charset.c helpers, print pruning, text-property copy order

Base: main `84f342a`.  This closes the residual disclosed in the
2026-09-04 section ("the sjis, big5, euc-jp, emacs-mule and charset-type
decoders still produce no `charset' properties") and the neighbouring
charset.c and print.c behaviour that came out while probing it.  Every
item was probed on the Linux oracle first (with LANG unset and under the
gate's LANG=C, which differ, see below) and is covered by an in-process
oracle contract; the probe file was diffed against the oracle in both
locale states with no remaining difference (127 lines).

**Decoders (coding.c).**  decode_coding_sjis, _big5, _euc_jp,
_emacs_mule and _charset are ports with ADD_CHARSET_DATA, so
`decode-coding-string', `decode-coding-region' and `insert-file-contents'
carry produce_charset's `charset' text properties for all of them, the
single-byte charset codings included (koi8-r, windows-125x used to bypass
the charset decoder through a Rust single-byte table; they now decode
through the charset like everything else and answer `(charset koi8-r)'
over the whole string, as the oracle does).  The sjis decoder takes its
roman/kana/kanji/kanji2 charsets from the coding's `:charset-list' and
SJIS_TO_JIS2 is ported; the charset decoder reads Fdefine_coding_system_internal's
`charset_valids' table (smaller dimensions first per first byte); the
emacs-mule decoder closes a run at ASCII as GNU's does.  A multibyte source
(a multibyte buffer region, a multibyte string) hands the decoder its raw
bytes and passes its other characters through unchanged, as ONE_MORE_BYTE
does, and the runs are re-based on the output, so `decode-coding-region'
in a multibyte buffer gets the properties too.  Contract:
`charset_decoders_annotate_like_produce_charset_and_emacs_mule_encodes_like_coding_c`.

**encode_coding_emacs_mule** is now a real encoder (it wrote internal
UTF-8 before): EMACS_MULE_LEADING_CODES of the charset's emacs-mule id
plus the code bytes with their high bits set, the charset being the first
of Vemacs_mule_charset_list (the charsets with an `:emacs-mule-id', in the
current priority order) that encodes the character, and the coding's
default char (a space) for the rest.  The encoder's `charset'-annotation
branch never runs in GNU because setup_coding_system sets
CODING_ANNOTATE_CHARSET_MASK for ISO-2022 designation codings only; the
oracle re-encodes a jisx0208-annotated hiragana through chinese-gb2312
(the list's first match), so Emaxx ignores the property there too.
`char-charset' with a coding-system RESTRICTION reads
coding_system_charset_list, which substitutes Vemacs_mule_charset_list for
emacs-mule as it substitutes Viso_2022_charset_list for `iso-2022'.

**charset.c character helpers.**  `char_charset' is the ordered-list
walk with Vcharset_non_preferred_head: a Unicode character answers
`unicode' as soon as the walk reaches the part of the order the last
`set-charset-priority' did not move (and `emacs'/`eight-bit' past the
whole list); `split-char' is the code's bytes per dimension;
`charset-after', `find-charset-string' and `find-charset-region' report raw
bytes as `eight-bit' and list charsets in id order.  ENCODE_CHAR/DECODE_CHAR
speak GNU's character numbers (a raw byte is #x3fff80..#x3fffff, never a
member of `unicode' or `emacs'; `encode-char' used to answer the
Rust-internal spelling of a raw byte for `unicode'), the `unicode' and
`emacs' code spaces end at MAX_UNICODE_CHAR and MAX_5_BYTE_CHAR, an offset
charset's index counts from `:min-code' and stops at `:max-code'
(gb18030-4-byte-smp's last code decodes to U+10FFFF and the next is nil),
and a map file's `FROM-TO C' line advances by code-space index as
load_charset_map does -- Emaxx expanded the range as consecutive
integers, which made GB180304.map's first line 774 entries instead of 36
and let gb18030-4-byte-bmp claim characters up to #x3fff7f.
`decode-char'/`encode-char' signal `(wrong-type-argument charsetp X)' for
an unknown charset (CHECK_CHARSET_GET_CHARSET) instead of answering nil.
Contract: `char_charset_family_follows_charset_c`.

**The dump boundary.**  Vcharset_non_preferred_head is not staticpro'd,
so the value loadup leaves in temacs (english.el's
`set-language-info-alist' re-runs `set-language-environment' for the
default "English") is not in the dumped image: a fresh GNU session has it
nil until a `set-charset-priority' of the session.  With LANG unset
nothing calls it, and `(char-charset #xe9)' is `iso-8859-1'; under LANG=C
the ASCII language environment's `(set-charset-priority 'ascii)' sets the
head and the same call answers `unicode'.  Emaxx reconstructs loadup in
the live process, so batch.rs clears the head at its dump boundary (after
the loadup preloads, before the startup phases), and both states match
the oracle.  This is the reason the earlier probe of `iso-latin-1'
printing looked locale-dependent: it is, in GNU too.

**print.c print_prune_string_charset** is ported as a per-string
decision: `print-charset-text-property' t keeps the `charset' properties,
nil drops them, `default' keeps them only when some charset span holds a
non-ASCII character whose CHAR_CHARSET is not the span's charset, other
properties surviving either way; a unibyte string's bytes count as Latin-1
characters (fetch_string_char_advance), so `(propertize "\351" 'charset
'eight-bit)' on a unibyte string prints its property and the same on a
multibyte raw byte does not.  Contract:
`print_prunes_charset_properties_like_print_prune_string_charset`; the
older `native_prin1_respects_dynamic_charset_text_property_modes' now
states both head states explicitly (its `nil' for U+00F6 under `unicode'
was the LANG=C state on an interpreter that had never set a priority).

**Text-property copy order (textprop.c/fns.c/editfns.c).**  Found by the
same probe: add_properties conses each new property onto the head of the
plist, so every copy that goes through `add_text_properties' reverses a
span's pairs -- Fsubstring (copy_text_properties), `concat' and
`mapconcat' (concat_to_string), styled_format's argument and
format-string intervals -- while copy_intervals (`copy-sequence', buffer
insertion) keeps them: `(substring (propertize "x" 'a 1 'b 2) 0)' prints
`(b 2 a 1)' in GNU and now in Emaxx.  styled_format's fast path returns a
string argument itself for a property-less "%s" format (`eq' holds).
Contract: `text_property_copies_follow_add_text_properties_order`.

**Still disclosed.**  ISO-2022 compositions produce their characters but
not the `composition' property (unchanged from 2026-09-04).  The charset
map lookups are linear scans of the parsed map per character, as before;
correct, not fast.

Verification: the four contracts above plus the existing coding, charset,
print and text-property tests in the serial gate subset (232 passed, none
failed after the two test corrections described above), strict gate
Clippy and `cargo fmt --check' clean.  The full grouped gate for this
change runs together with the next items of the same series and is
recorded there.

## 2026-09-05 server-tests and mml-sec-tests: the umask port, and what is left

**server-tests (7 mismatches -> 3; finding 158 FIXED, 159 OPEN).**  All
seven tests failed in Emaxx with "`.../server-testsXXXXXX' is not a safe
directory because it is accessible by others (755)": server.el's
server-ensure-safe-dir checks `(logand ?\077 (file-modes dir))', and the
directory `make-temp-file' had made was 0755.  fileio.c's
`set-default-file-modes' sets the process's file mode creation mask
(`umask (~mode & 0777)'), `default-file-modes' answers `~realmask & 0777'
(read from the process at startup, init_fileio), and gen_tempname creates
temporary files with S_IRUSR|S_IWUSR and directories with 0700.  Emaxx
kept `default-file-modes' as a number nothing consulted, so
`with-file-modes' had no effect on `make-directory', `write-region' or a
subprocess, and temporary entries came out with the umask defaults.  All
three are ported (the mask is a real umask, inherited by subprocesses as
GNU's docstring promises).  Contract:
`default_file_modes_is_the_process_umask_and_temp_files_are_private`
(a `sh -c umask' child reports 0177 under `(with-file-modes #o600 ...)').
The harness replay (`run-1788578203177624094-5926`) now matches 4/7:
server-start/sets-minor-mode, server-start/no-stop-prompt-without-client,
emacsclient/server-edit and emacsclient/eval pass in both.

The remaining three (emacsclient/create-frame,
server-force-stop/keeps-frames, server-start/stop-prompt-with-client) run
`emacsclient -c'.  The client's stdout is the pty `make-process' gave it,
so emacsclient sends `-tty /dev/pts/N linux' and server.el calls
`(make-frame '((window-system . nil) (tty . "/dev/pts/N") (tty-type .
"linux") ...))': GNU's Fmake_terminal_frame runs init_tty on that
device (opens it, tgetent's the type, registers a second terminal) and
builds a second frame with its own window tree; the oracle's log shows
`#<frame F2> created' and the client's pty receives the clear-screen
sequences.  Emaxx's `make-terminal-frame' is a stub that signals
"Unknown terminal type" (the client receives `-error Unknown&_terminal&_type'
and exits), because Emaxx has exactly one terminal object and one frame
sharing one window tree.  Making these tests pass honestly means
multi-terminal tty frames: terminal objects, init_tty over a device and
its terminfo, per-frame root and selected windows, `delete-frame' of a
non-sole frame and `delete-terminal'.  That is a frame-model change, not
a patch, and a frame that merely reported itself live while sharing the
initial frame's windows would be a lie about `frame-root-window'; so
this stays open as finding 159.

**mml-sec-tests (4 mismatches; no Emaxx divergence).**  The four
`mml-secure-en-decrypt-N' tests fail on both sides here: with gpg 2.4.4
and no agent in the sandbox, decryption does not happen and `decrypted'
is the armored message itself, on the oracle and on Emaxx alike (the
other ten failing/skipped tests of the file match exactly:
find-usable-keys, key-checks, select-preferred-keys-4, sign-verify-1,
the passphrase skips).  The comparison flags them only because the
failure message embeds the ciphertext, and OpenPGP encryption draws a
fresh session key and padding per run, so no two runs -- not even two
oracle runs -- produce the same text.  The harness normalizer is
deliberately restricted to environmental variance (its own doc comment,
compat.rs), and erasing ciphertext from messages would be a
semantic-content normalizer, so none was added: these four remain
reported as message mismatches, which is what they are.  Replay:
`run-1788578235968186948-6017`, 12/16 matching.

**Gate for this series.**  The full grouped gate on `6103c51` passed
(`target/grouped-gate/run-1788583375956679065-18708`: eval_01..05,
primitives 389/389, compat_runtime, tty (the two reviewed TTY wrappers
ignored), batch, lightweight, bins, integration).  Two earlier runs of
the same gate each failed one test, both test defects fixed before the
passing run and recorded here rather than retried away: the new
file-modes contract had a literal 755 for the startup
`default-file-modes', which is the inherited umask's complement and is
775 under the gate user's 002 umask (the contract now compares with
`sh -c umask'); and the 2026-09-04 process-attributes contract spawned
`sleep 5 "a b" "c\d"', an invalid sleep that exits immediately, so its
/proc read raced the child's death (its child is now `sh -c "sleep 5"'
with the same escaped arguments).  Strict gate Clippy and
`cargo fmt --check' are clean; `compat/oracle.lock.linux.json' stays
uncommitted as before.

## 2026-09-05 frozen run on the coding and file-modes series: 7763/7883

Frozen replay of the pinned manifest on `50ade8d` (main `84f342a` plus
this series; `frozen-1788596001721596265-1737`, 461 files, 3600 s
per-phase timeout): 7763/7883 matching, 120 mismatching outcomes in 10
files, against 7757 (126 in 11 files) for the previous run.  Closed since
that run: the four umask-caused server-tests (finding 158), the two
em-prompt-tests field-boundary tests and editfns-tests'
before/after-change-functions (both from main's Editfns and Eshell
commits).  No previously matching test regressed.

One new mismatch appeared and is not a divergence: flymake-tests'
`ruby-backend' was *skipped by the oracle* (its `skip-unless
(executable-find "ruby")') and passed by Emaxx in this run.  The same
oracle passed it in the previous frozen run, both binaries find
/usr/local/bin/ruby, and two harness replays of the file immediately
afterwards match 9/9 (`run` artifacts under target/compat, both PASS).  It
is a one-off skip on the oracle side, recorded here as such.

What still stands, by cause:

| Cause | Outcomes | Files |
|---|---|---|
| native compilation (`native-compile' feature absent) | 99 | src/comp-tests (96), lisp/emacs-lisp/comp-tests (3) |
| semantic completion subtests, undiagnosed | 6 | cedet/semantic-utest-ia |
| mml-sec ciphertext nondeterminism (no divergence) | 4 | gnus/mml-sec-tests |
| thread model: `thread-list' Blocked state, condvar/mutex contention deadlock guard (finding 157 family) | 4 | lisp/thread-tests (2), src/thread-tests (2) |
| multi-terminal tty frames (finding 159) | 3 | server-tests |
| `erc--find-mode' and `erc--essential-hook-ordering' end with `end-of-file', undiagnosed | 2 | erc/erc-tests |
| `accept-process-output' JUST-THIS-ONE returning nil in async-shell-command-30280, undiagnosed | 1 | simple-tests |
| one-off oracle skip (above) | 1 | progmodes/flymake-tests |

Outside native compilation, the thread model and the frame model, the
open work is 9 outcomes: semantic (6), erc (2), simple (1).

## 2026-09-05 native merge: correction to the bytecode argument repair

During the `native-comp` merge of main `84f342a`, the ordinary native artifact
ladder caught a semantic problem in `17da04f` despite its passing Eshell and
runtime tests. The fifth unchanged GNU fixture,
`test/lisp/emacs-lisp/comp-tests.el`, produced two 86,384-byte files that differ
from byte 768. The serialized constants lost the shared `" *temp file*"`
string references, shifting data layout and machine-code addresses. The saved
pre-merge `9097866` editor still emits the exact GNU artifact for that same
source under the same locale. Nothing was normalized in the comparison.

GNU `bytecode.c:exec_byte_code` pushes `*args`: it does not copy string
objects when binding bytecode arguments. The incoming `stored_value` call
instead creates a fresh mutable string for each compact-string argument.
That per-call allocation is removed. The actual diagnostic producer is
corrected at `print.c:Ferror_message_string`'s boundary: a general diagnostic
is already a mutable multibyte Lisp string when returned from the print
buffer; `(error STRING)` returns the original STRING, preserving identity and
properties without allocation. No promotion cache or package-specific branch
is introduced. GNU Elisp remains unchanged.

The Rust bytecode contract is strengthened to require the caller's original
string identity across repeated calls and caller-visible property mutation,
using an actual C-owned diagnostic producer. Its former mutation-only check
could pass while losing identity. The existing Eshell fixture and its
expectations are unchanged. The corrected tree passes 112 optimized Rust
tests, including every bytecode test, all native-runtime correctness tests,
17 anti-cheating checks, Eshell and error-message rendering; one separate
native timing probe is ignored. Format/check/strict Clippy pass. The full
artifact replay subsequently passes all nine fixtures: eight complete `.eln`
files identical, including GNU `comp.el`, and one correctly absent artifact
(`identity-string-fixed.log`, 216.48s). The complete native execution replay
also passes 177/177, zero unexpected results, exit 0, with both helper `.eln`
files freshly compiled and loaded (`emaxx-native.stderr`, 1114.12s wall /
1024.41s user CPU). No compiler-spawning or image-cloning shortcut was used.
Evidence: `/private/tmp/emaxx-main-84f342a.3jARTW` and the L12 entry in
`docs/native-comp-c-parity-ledger.md`. This is not a claim that all string
representation or forwarding gaps elsewhere in the runtime are closed.

The final pre-commit audit passes all 17 gates; formatting, all-target check
and strict all-feature/all-target Clippy are clean. Two serial full-compiler
timing pairs used 62.81/63.02s and 69.94/66.42s before/merged user CPU; GNU
used 8.43s and 9.47s. All measured artifacts are byte-identical. This shows no
material merge regression, not a defensible speedup; Emaxx remains about 7.2x
GNU including startup. Detailed timings/hashes are in the parity ledger.

## 2026-09-05 merging native-comp into main on the Linux oracle

Base: main `84f342a` plus the coding and file-modes series (`c3aac3e`),
merged with `origin/native-comp` at `9844664` ("Record GC checkpoint
state and post-startup performance handover"; 18 commits, 94 files).
The branch brings comp.c as Rust over libgccjit with the unchanged
`comp.el' frontend, the native ABI loader and runtime, GC roots for
native objects, and GNU's `normal-top-level' startup in place of the
handwritten batch startup.  Its handover document
(`docs/handover-2026-09-02-native-comp.md`) says plainly that the full
gate and the frozen corpus were not run for its checkpoints, that
portable dumping does not exist, and that the post-startup compiler is
about 6x slower than GNU; nothing here contradicts that.  Textual
conflicts were three: the ledger (both sections kept), `eval.rs' (the
branch's compiler-state field next to the umask field), and `batch.rs'
(the dump-boundary reset of Vcharset_non_preferred_head kept inside the
reconstruction block the branch reshaped).

**What the merge needed on this host, and why.**

*Linux compile.*  The Linux-only `process-attributes' floats still
wrote `Value::Float(f64)`; the branch's shared float constructor is used.
The branch had only been built on Darwin.

*The generated Linux subroutine table.*  The committed
`generated_native_subrs_x86_64_unknown_linux_gnu.rs` described a
gtk3/cairo/dbus AOT build (its own configuration string says so) and
the anti-cheat regeneration gate refused the tree.  It is regenerated
with `tools/generate_native_subrs.rs' from the pinned oracle
(`--with-x --with-x-toolkit=no ...`): 1455 subroutines instead of 1467,
byte-identical to the gate's own fresh copy, and `comp-native-version-dir'
now equals the oracle's `30.2-1564b906'.  The layout constants in
`abi.rs' (jmp_buf 200, handler 24/32/64/304, thread_state 96/520) were
re-measured against the oracle's headers with its own compiler flags and
did not change.

*PURESIZE.*  Every Linux `.eln' with a PURE_P check differed from GNU's
by one immediate: the branch hard-coded 6000000 (Darwin's
BASE_PURESIZE 3400000 + SYSTEM_PURESIZE_EXTRA 200000, times 10/6), where
this build's puresize.h gives 5666666.  It is now a per-target measured
ABI constant beside the layout numbers.  With it, all nine whole-file
identity fixtures are byte-identical to GNU's on Linux, `comp.el' (914,592
bytes) included; before it the ladder failed at its fourth rung.

*eval-buffer and the cookie.*  The branch routes `load' of a source file
through GNU's `load-with-code-conversion' and therefore `eval-buffer'.
Emaxx's `eval-buffer' chose lexical evaluation from the cookie but never
specbound the Lisp variable `lexical-binding' as Feval_buffer does, so
`named-let' (which reads the variable while expanding) signalled inside
every lexical file loaded that way; erc-tests stopped loading.  The
variable is now bound for the readevalloop, and the cookie scanner is
lisp_file_lexical_cookie's: the first line only (the second after a
`#!' line), which must begin with `;' -- the previous scanner read two
lines and mistook a cookie inside a string literal on line two for the
file's cookie.  The reader also gained `#!' as a line comment (read0).
Contract: `eval_buffer_binds_lexical_binding_from_the_first_line_cookie`.

*Linux-visible divergences found while probing, small enough to fix
here.*  `comp--init-ctxt' returned nil where comp.c returns t.

*Divergences recorded, not fixed.*  `comp--install-trampoline' with a
plain subr as TRAMPOLINE: GNU's CHECK_SUBR accepts it and patches the
link table with the C function pointer; Emaxx signals
`(wrong-type-argument subrp ...)' because a Rust primitive has no
address to install.  `comp--compile-ctxt-to-file0' without a context:
GNU reaches `comp-ctxt-speed' on nil (`void-function' when comp.el is
not loaded), Emaxx signals `(native-ice "comp-ctxt is nil")'.  The
remaining `comp--register-*' entry points abort GNU when called without
a context, so no contract covers them.

**Rust unit-test fixtures.**  comp.el's `comp--final' compiles in a child
started as `invocation-name -no-comp-spawn -Q --batch -l TEMP', and
data.c's `fset' of a primitive asks comp-run.el for a trampoline the same
way.  Inside a Rust test process `invocation-name' is the libtest binary,
which rejects `-no-comp-spawn' ("Unrecognized option: 'n'"), so six
existing tests that redefine primitives under `cl-letf' failed.  The
fixtures now run under the configuration GNU's own child uses:
`comp-no-spawn' t (compile in this process), with the implicit
compilations off through GNU's options (`comp-enable-subr-trampolines'
nil, `native-comp-jit-compilation' nil).  The CLI keeps GNU's defaults,
and the harness replays and the identity ladder use the CLI.  The full
gate found the same failure in tests that build their own batch
interpreter (the Todo-mode ERT run under `cl-letf' of
`read-from-minibuffer'), so the configuration now applies inside the
batch and interactive constructors under `cfg(test)' rather than in
three fixtures.  Five
tests that used *scratch* as a work buffer now erase it first: GNU's
command-line-1 inserts `initial-scratch-message' at the end of startup,
which the fixture now runs to completion.  Tests that loaded GNU test
files through cwd-relative names (`../emacs/test/...') now expand them
against `source-directory': GNU's openp searches `load-path', not
`default-directory', for a relative name with directory components (the
oracle answers `file-missing' for the old form), and the branch's loader
follows openp.  The `srecode/srecode-template' alias test is deleted:
the oracle answers `file-missing' for that name, and the branch removed
Emaxx's private alias rule.  Main's two tests that asserted the absence
of a native backend are rewritten as oracle contracts over the
introspection and loader entry points that GNU survives.

**Startup cost.**  Batch startup on this host is about 20 s per
invocation after the merge, against about 9.5 s before (GNU: 0.03 s):
the branch's startup runs GNU's normal-top-level over the reconstructed
image.  The harness setup phase reflects it (`setup_emaxx' ~21 s); the
replays are correspondingly slower but within the frozen run's budget.

**What the frozen corpus found on the merged tree, and the fixes.**  The
first frozen run over the merge commit (`245ff40') regressed fourteen
files that the previous run matched.  Each was traced to GNU and fixed on the
merged tree; the fixes carry oracle contracts and were replayed
file-by-file before the numbers below.

*`next-read-file-uses-dialog-p'* (dired-tests, files-tests, tramp-tests).  Emaxx
answered t whenever `use-dialog-box' and `use-file-dialog' were on, so
`read-file-name' (now GNU's Lisp, reached through the branch's startup)
called `x-file-dialog', which does not exist.  fileio.c answers t only in
a toolkit build (USE_GTK, USE_MOTIF, HAVE_NS, HAVE_NTGUI, HAVE_HAIKU) and
then only when `last-nonmenu-event' is nil or a list and the selected
frame has a window system (the initial frame of a batch session counts).
The Linux oracle is `--with-x-toolkit=no': always nil.  The Darwin oracle
is `--with-ns': the NS logic is implemented for that target, and its
contract expectation (`(t t nil t)') could not be run here.

*`sqlite-execute'/`sqlite-select' VALUES* (multisession-tests).  The
branch made `vector_items' vector-only; the parameter binder had leaned on
its list fallback, so a list of parameters (multisession.el passes one)
signalled `(wrong-type-argument vectorp ...)'.  bind_values walks a
vector or a list; anything else is `(sqlite-error "VALUES must be a list
or a vector")' -- Emaxx had signalled a plain `error' for that case.

*`json-serialize'* (jsonrpc-tests, eglot-tests).  The serializer knew
vectors only as the older `vector-literal' list; a `Value::Vector' fell
through to `(wrong-type-argument json-value "vector")', so jsonrpc could
not send a request.  json.c's json_out_something: a vector is an array.
While the contract was written, the list-to-object path was found to
answer its own errors: json_out_object_cons treats a list as an alist
when its first element is a cons and as a plist otherwise, requires every
key to be a symbol (`(wrong-type-argument symbolp KEY)'), skips a later
occurrence of the same symbol, keeps an alist key's leading `:' and drops
a plist key's, and signals `consp' for a pair-less plist key or a
non-cons alist element and `listp' for a dotted list, and FOR_EACH_TAIL
signals `circular-list' for a cycle; Emaxx answered `(wrong-type-argument
json-object "cons")' for the lot and stripped `:' from alist keys.  Each
value is written as its pair is reached, as json_out_object_cons does,
so a bad value inside a cycle is reported before the cycle is (the
first port collected the pairs first and looped on `'#1=(:a . #1#)'
until the harness timeout).  Contract:
`json_serialize_treats_a_vector_as_an_array`.

*Symbol shorthands under `load'* (elisp-mode-tests).  load-with-code-
conversion binds `read-symbol-shorthands' from the file's local
variables (`hack-read-symbol-shorthands-function') around `eval-buffer',
and lread.c's reader interns through it.  Emaxx's `eval-buffer' and
`eval-region' built their readers without the binding, so a file whose
local variables map `f-' to `elisp--foo-' defined `f-test3' and then
called the void `elisp--foo-test3'.  Both readers now take the dynamic
value.

*`handler-bind' and the batch backtrace* (gv-tests, eval-tests).  The
branch installs
`debug-early--handler' around the `top-level' form the way top_level_2
does, and the corpus showed what Emaxx's dispatch did with it: the
handlers ran at every primitive frame the error unwound through (the
backtrace printed once per frame, each shorter), an error the evaluator
itself signalled inside interpreted lambdas -- void function, void
variable, wrong arity -- reached no handler at all (`(handler-bind ((error
...)) (funcall (lambda () (undefined-fn 1 2))))' ran the handler zero
times), and the Rust toplevel printed a second backtrace.  signal_or_quit
runs the handlers once, from `signal', with the signaling frames intact.
Emaxx now dispatches at the innermost frame boundary that sees the error
-- primitives, byte-code, native code, interpreted lambdas, the
unevaluated frames of `cond'/`let'/`let*'/`setq'/`while' and of a call in
progress -- and remembers the condition object, so the outer boundaries
pass it on untouched; a fresh `signal' is a new object and runs them
again.  eval_sub's ordering is kept: the frame for a call is recorded
before the function cell is resolved, so a void function shows the
attempted call, unevaluated, innermost; funcall_lambda's arity error is
signalled after Ffuncall recorded the callee.  The toplevel now prints
only cmd_error's message.  Contract:
`handler_bind_handlers_run_once_with_the_signaling_frame_innermost`
(frames as GNU lists them through `mapbacktrace' from the handler).

*stdout is stdio* (gv-tests).  `debug-early' prints to `standard-output',
which in batch is C stdio's stdout: block-buffered on a pipe or file,
released by `flush-standard-output', the batch minibuffer prompt, or
exit; stderr is unbuffered.  gv-tests captures a child's two streams on
one descriptor and pins the order: the error message (stderr) before the
backtrace (stdout, flushed at exit).  Rust's stdout flushes at every
newline.  Batch stdout now goes through a buffer sized and drained the way
glibc's `_IO_new_file_xsputn' does (st_blksize, whole blocks of a large
write bypass it), flushed at the same points.  `message' also gained
xdisp.c's `noninteractive_need_newline': a newline first when stdout was
written since the previous message.  Test:
`batch_stdout_and_stderr_interleave_like_stdio_on_a_shared_descriptor`
(tests/cli.rs) compares the merged bytes of GNU and Emaxx.

*`call-process' STDERR-FILE t* (gv-tests).  Emaxx read stdout and stderr
on two pipes and appended stderr after stdout; callproc.c gives the child
one descriptor for both unless STDERR-FILE is nil or a file name, so the
streams arrive in the order written.  Synchronous processes now share
the descriptor the same way (row 164 records the asynchronous path).

*`ash' with a bignum COUNT* (data-tests).  With the merged 62-bit fixnum
range, `(* 2 most-positive-fixnum)' is a bignum, and `ash' rejected it
as `number-or-marker-p' (data-tests-ash-lsh, which also drives `lsh').
Fash's rule for a count outside the fixnum range: 0 stays 0, a negative
count shifts anything else to -1 or 0 by its sign, a positive one is
`overflow-error'.  Contract: `ash_with_a_bignum_count_follows_data_c`.

*`copy-keymap'* (keymap-tests).  The primitive copied only Emaxx's
record keymaps and returned a list keymap itself, so `(eq (copy-keymap
m) m)' held.  keymap.c copy_keymap_1 and copy_keymap_item are ported:
a fresh spine, copied char-tables (every entry through the item copier),
vectors and nested keymaps, fresh `(EVENT . DEFINITION)' cells, fresh
cells for a menu item's marker, name and binding with the rest shared,
fresh cells for an old-style item's strings, the parent tail shared, a
symbol resolved through `indirect-function', `keymapp' for anything
else.  Contract: `copy_keymap_copies_a_list_keymap_like_keymap_c`.

*Unescaped character literals under `load'* (lread-tests).  Fload binds
`lread--unescaped-character-literals', the reader conses each unescaped
`?)'-style literal onto it, and the load's unwind messages the warning.
The eval-buffer path read with a reader that recorded nothing, so
`(load FILE nil :nomessage)' of "?) ?(" said nothing.  `eval-buffer' and
`eval-region' now add their reader's literals to the variable.  Contract:
`load_warns_about_unescaped_character_literals_on_the_eval_buffer_path`.
The sqlite VALUES fix above also closes sqlite-tests (six outcomes) and
the JSON vector fix closes test/src/json-tests (eight).

**Frozen corpus over the merge commit.**  Artifact
`frozen-1788634460316794579-27715` (worktree at `245ff40', 2026-09-05
18:50 to 23:48, 3600 s per file):

| run | outcomes matching | mismatching | files mismatching |
| --- | --- | --- | --- |
| previous main (`frozen-1788596001721596265-1737`) | 7763 / 7883 | 120 | 10 |
| merge commit `245ff40' | 7828 / 7883 | 55 | 21 |

Native compilation: test/src/comp-tests.el 96 -> 0 and
test/lisp/emacs-lisp/comp-tests.el 3 -> 0, all 99 closed; the flymake
`ruby-backend' one-off (the oracle skipped it once in the previous run)
did not recur, 1 -> 0.  The 55 that remain split into the 20 the
previous run also had -- semantic-utest-ia 6, mml-sec 4, server 3, erc
2, simple 1, thread-tests 2, test/src/thread-tests 2, every one with its
cause on the list above this section -- and 35 across fourteen files
that the merge regressed: gv-tests 3, jsonrpc 3, eglot 5, multisession
2, dired 1, files 1, tramp 1, elisp-mode 1, data 1, eval 1, keymap 1,
lread 1, sqlite 6, test/src/json-tests 8.  Those fourteen are the fixes
described above; each file was replayed on the fixed tree with
`compat-harness run --file' and matched the oracle outcome for outcome
before the tree was committed (the replay lines are in the commit's
verification notes below).

*Gate tests that asserted the old fixtures.*  `exec-suffixes' is nil in
callproc.c's DEFVAR and `("")' after startup, so the raw fixture's nil is
GNU's and the batch fixture is the one to ask.  A batch session loads
`last' from subr.elc as a byte-code function in GNU (no subr.eln is
loaded there) and in the Emaxx CLI; the help-metadata test expected a
native subr.  The early-Lisp fixture
now records preloaded files in `load-history' the way the dumped image
does, relative to the Lisp directory (startup.el's normal-top-level is
what makes them absolute, and the fixture stops before startup); the
test expected an absolute `byte-run' owner.  And the batch fixture's
`package-directory-list' holds the two `site-lisp/elpa' directories a
session without `-Q' has in GNU too (the CLI answers the same on both
editors); the test expected nil and now checks package.el's derivation
from `load-path' instead of a host-specific value.

*Two CLI tests that asserted Emaxx rather than GNU.*  One expected
`message' after `princ' to write "single-dash-stderr\n"; GNU writes
"\nsingle-dash-stderr\n" (the need-newline above), and the test now says
so.  The other expected a checkout's `lisp/' directory on the load path
because EMACS_TEST_DIRECTORY named its `test/' sibling -- the recursive
walk the branch removed in favour of lread.c, whose own unit test
(`session_path_does_not_discover_the_gnu_test_tree`) asserts the
opposite.  The oracle answers `file-missing' for that scenario and exits
255; the test now compares Emaxx with it.

*Gate tests on the fixed tree.*  The full grouped gate over the fixes
above failed eight `eval_05' tests, five of them the fixture's and three
real divergences that had been hidden by the old fixture:

- Three `execute-kbd-macro' tests ran their macro in a fresh buffer and
  found the *scratch* banner in front of the typed text.  The fixture
  starts without `--batch', so startup.el inserts `initial-scratch-
  message', and keyboard.c's command_loop_1 runs the macro's commands in
  the selected window's buffer, which is *scratch*.  The oracle confirms
  `("Sa" "*scratch*" "Sa" t)' for a macro typed into a fresh fixture; the
  tests erase *scratch* first.
- The custom-theme test now requires `cl-seq' before it runs, as the
  oracle does when the test is run in isolation (custom-tests loads it
  transitively through ert's ordering; a lone `(load "custom-tests")'
  fails identically in GNU).
- `upstream_save_policy_only_queries_buffers_that_offer_to_save' asserted
  two prompts followed by a plain return.  Run through the oracle, the
  program answers `((quit 0) (quit 0) nil)': `save-some-buffers' and
  `save-buffers-kill-emacs' both quit in a batch session before any
  `read-event' override is reached (`map-y-or-n-p' quits without a
  prompt), and the exit path with `confirm-kill-processes' nil asks
  nothing.  The test now expects the oracle's answer.
- Two dired tests lost the match data across an autoload: dired-aux's
  `dired-do-rename' runs `string-match' and then calls an autoloaded
  function whose `load' clobbered `(match-data)'.  lread.c's Fload runs
  under `save_match_data_load' (record_unwind_save_match_data); every
  Emaxx autoload path -- `eval_call', `macroexpand', `call-interactively',
  keymap autoloads, `autoload-do-load' and `interactive-form' -- now
  loads through one helper that saves and restores the match data.
- `upstream_files_lisp_owns_remote_file_policy' found a modified buffer
  " *string-pixel-width*" in every Emaxx session after startup, which
  GNU does not have (`(buffer-list)' after `--batch' startup is *scratch*,
  " *Minibuf-0*", *Messages* on both).  The trace was
  `command-line' -> `substitute-command-keys' -> `where-is-internal' ->
  `tab-bar-make-keymap' (the `[tab-bar]' menu item's `:filter') ->
  `tab-bar-auto-width' -> `string-pixel-width'.  keymap.c's
  where_is_internal_1 reads each binding with `get_keyelt (binding, 0)':
  a menu item's `:filter' does not run during the scan.  Only the
  verification of a matched sequence (shadow_lookup, i.e. `lookup-key'
  with autoload) runs it, and with FIRSTONLY the first all-ASCII match
  returns before the remaining candidates are verified.  Emaxx ran every
  filter while scanning.  The scan now uses the filter-less reading, the
  candidates are sorted shortest-first before verification, verification
  stops at the first preferred sequence under FIRSTONLY, and the
  FIRSTONLY `:advertised-binding' check happens before the scan as
  Fwhere_is_internal does.  Oracle probe and contract
  (`where_is_internal_runs_menu_item_filters_only_for_verified_matches`):
  `([6] 0 0 1 nil)' -- no filter call from `(where-is-internal
  'forward-char nil t)' or `substitute-command-keys', one from the
  verification of a menu item whose command is the one searched for, and
  no " *string-pixel-width*" buffer.

The buffer-list probe for that finding showed one more difference in
*Messages*: after `(message "x")' GNU's buffer has `buffer-undo-list' t
and `cache-long-scans' nil, Emaxx's recorded an undo entry for the line
and kept `cache-long-scans' t.  xdisp.c message_dolog sets both on every
log, so an undo list installed in *Messages* by hand is gone after the
next message.  The log sink now does the same; contract:
`message_log_disables_undo_in_the_messages_buffer`.  What remains is
row 166: GNU's empty *Messages* is already flagged modified after batch
startup (the dumped image logged and erased during loadup) and Emaxx's
is not.

The first full gate over this tree failed one more test, in the
primitives group: `process_attributes_follows_sysdep_procfs' read the
fresh `sh -c "sleep 5"' child's /proc state before the shell had reached
its `sleep', on a machine whose worker had just been restarted, and the
state was neither "R" nor "S" (the "D" of a process inside exec).  The
test passed three times in isolation afterwards; the same read races on
the oracle's side.  Its program now polls until the state is "S" (two
seconds at most) before taking the attributes, on both editors alike,
and the gate was rerun in full.  That rerun reached the groups the
earlier failures had stopped short of and found four more tests of the
banner class -- `insert_file_contents_preserves_embedded_cr_in_unix_files',
two glyphless display tests and the isearch dispatch test insert into
the current buffer and expected it empty; each erases the banner first
now -- and the gate was run a third time.

The third run reached the `batch' group and failed three tests there:

- `batch_runtime_records_ordered_initialization_times' expected
  `before-init-time' to be a four-element list and found a (TICKS . HZ)
  pair.  The merged fixture runs startup.el's `normal-top-level', which
  sets `before-init-time' from `(current-time)', and Emaxx's
  `current-time' ignored `current-time-list': it always answered the
  pair, reduced.  timefns.c make_lisp_time answers the old-style list
  under `current-time-list' (t by default) and the nanosecond pair
  otherwise; Ftime_convert treats a nil FORM as `list' under it and t
  otherwise.  Both are ported; contract:
  `current_time_and_a_nil_time_convert_form_follow_current_time_list`.
  The pre-existing divergence was invisible on main because its fixture
  never ran startup.el.  The rational HZ Emaxx reduces is row 167.
- `batch_reconstruction_reaches_loadup_native_trampoline_transition'
  asks whether loadup.el turned `native-comp-enable-subr-trampolines'
  on; the test fixture this merge centralised turns it off again on
  every fixture so tests never compile trampolines.  The test now reads
  the image as started, before the fixture's settings.
- `batch_runtime_rejects_a_broken_resolvable_preload' asserted main's
  per-library preload message ("preload emacs-lisp/seq: ...").  The
  branch reconstructs the image through GNU's loadup.el itself, so the
  failure is loadup's, with `load("emacs-lisp/seq")' in its backtrace;
  the test asserts that message.

The fourth run passed every library group, the binaries, `cargo fmt'
and clippy, and stopped at the integration stage: the branch's
`tests/native_comp_identity.rs' carries one `#[ignore]' test and the
publication gate refuses ignored integration tests.  Run by hand with
`--ignored', the test compiled all nine unchanged GNU sources through
both editors -- the comp-test resources, both comp-tests files,
comp-cstr-tests and comp.el itself -- and every artifact was byte
identical to GNU's (914592 bytes for comp.el), in 483 s.  The attribute
is removed: the gate runs it now, and the fifth run is the one recorded
below.

**Verification of the fixed tree.**  Full grouped gate, run alone on
the Linux machine, artifact
`target/grouped-gate/run-1788681579808135768-18018` (2026-09-06 07:59
to 09:16): eval_01 351, eval_02 283, eval_03 320, eval_04 251, eval_05
351, primitives 410, compat_runtime 84, tty 56 (the two inventoried
ignores), batch 46, lightweight 345, all passed with zero failures; the
binaries 38 + 1 + 1; the integration targets 14 + 3 + 1 + 5, the 1 being
the native artifact identity test (1028 s for the stage).  `cargo fmt
--check' and `cargo clippy --profile gate --all-targets --all-features
-- -D warnings' clean.  The four earlier runs of the gate on this tree
and what each one found are described above.
