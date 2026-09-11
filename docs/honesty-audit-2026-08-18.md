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
| 84 | Cooperative thread model cannot suspend a thread mid-body | PARTIALLY FIXED 2026-09-09 (owned continuations suspend actual frames; four target cases pass; scheduling limits and validation in [thread audit](thread-continuation-audit-2026-09-09.md)) |
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
| 168 | `garbage-collect' reports GNU's C object layouts in its SIZE columns (conses 16, symbols 48, strings 32, vectors 16, vector-slots 8, floats 8, intervals 56, buffers 992) rather than this process's allocation sizes, so memory-report.el's byte totals describe a GNU Emacs heap; finding 110 had chosen the process's own sizes.  The same constants drive the GNU-style `gc-cons-threshold' accounting | OPEN (recorded 2026-09-06 on the second native-comp merge, PR #52; disclosed here, the choice is the merge's and is left for review) |
| 169 | Automatic garbage collection now happens where alloc.c's would (eval_sub's maybe_gc once `consing_until_gc' is negative against the retuned threshold, with the census, the weak-table sweep and `post-gc-hook'), but the consing tally is Emaxx's approximation of GNU's object sizes and the live-byte census is not GNU's mark phase, so the number of collections a given program provokes differs (GNU collects three times over 50000 interpreted conses in batch, Emaxx once over the same loop only after far more consing) | OPEN (recorded 2026-09-06; cadence, not the counters' semantics) |
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

**2026-09-09 correction to the historical claim below:** a fresh Linux
run exposed a real Emaxx ASCII conversion bug once GPG decryption worked.
The bug is now corrected and all four original tests pass in both editors.
See [the OpenPGP audit](openpgp-linux-investigation-2026-09-09.md). The old
agent/sandbox diagnosis was not established by the historical results.

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
`comp-no-spawn' t (this suppresses ordinary compilation, rather than
selecting in-process emission; corrected in the later merge audit), with the implicit
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

*Replays on the release build of the committed tree* (`compat-harness
run --file', 3600 s, 2026-09-06 09:20 to 10:11; every file the first
frozen run had regressed, plus the files the keymap, dired, help and
message-log fixes touch):

| file | outcomes |
| --- | --- |
| test/lisp/emacs-lisp/gv-tests.el | 8/8 |
| test/lisp/dired-tests.el | 16/16 |
| test/lisp/emacs-lisp/multisession-tests.el | 5/5 |
| test/lisp/files-tests.el | 116/116 |
| test/lisp/jsonrpc-tests.el | 5/5 |
| test/lisp/progmodes/elisp-mode-tests.el | 63/63 |
| test/lisp/progmodes/eglot-tests.el | 52/52 |
| test/lisp/net/tramp-tests.el | 59/59 |
| test/src/data-tests.el | 57/57 |
| test/src/eval-tests.el | 26/26 |
| test/src/keymap-tests.el | 46/46 |
| test/src/lread-tests.el | 52/52 |
| test/src/sqlite-tests.el | 12/12 |
| test/src/json-tests.el | 23/23 |
| test/lisp/json-tests.el | 59/59 |
| test/lisp/help-tests.el | 31/31 |
| test/lisp/dired-aux-tests.el | 5/5 |
| test/lisp/simple-tests.el | 52/53 (`simple-tests-async-shell-command-30280', on the list above) |
| test/lisp/tab-bar-tests.el | 2/2 |

## 2026-09-06 second native-comp merge (PR #52, `4397ffa'): audit

Main received twenty-three more native-comp commits after `9f95374'
(merged as PR #52, with the branch's own merge of main and a macOS pipe
portability fix on top).  They are: GNU `alloc.c' garbage-collect
layout constants and the `since_gc > gc_threshold / factor' boundary of
Fgarbage_collect_maybe (which was a constant nil before), eval_sub's
maybe_gc placement, Ffuncall's debug-on-exit `call_debugger', funcall
target and arity audits, `set'/`set-default' returning and notifying
with the caller's original NEWVAL and canonicalising the watcher
operation to `set', data.c's forwarded `symbols-with-pos-enabled' as
one live C boolean owned by the interpreter (the runtime's per-call
snapshot is gone; comp.c's relocation points at it), word-based native
EQ that unwraps only positioned symbols, bignum EQ by allocation
identity, Ftype_of without the old-struct policy (cl-lib.el's advice
owns it) and Fcl_type_of from object tags, reader-literal
materialisation on the eval-buffer path, and the pipe2-to-pipe
fallback.

*What was read.*  The handover document and both branch ledgers as
merged, and every line of the code diff (`git diff 9f95374..4397ffa --
src tests`).  Each change was checked against the C it names.

*De-cheating checks, all on this Linux machine.*

- No GNU delegation: the only `Command' sites that name the oracle are
  a unit test's locale probe and the harnesses; the runtime re-executes
  itself for `--restart'.  The anti-cheat suite
  (`runtime_code_does_not_shell_out_to_oracle_emacs' and its seventeen
  siblings) is part of the lightweight gate group and passed.
- `comp-abi-hash' is computed as comp.c computes it (ABI version,
  `emacs-version', configuration, options, the subr signature list,
  md5, eight hex digits); it is not a literal.  It equals the oracle's
  "1564b906" because the inputs are the same, which is what makes an
  Emaxx `.eln' loadable by name.
- The native-compilation corpus files run Emaxx's own compiler.  In the
  frozen artifact for test/src/comp-tests.el both editors record 177
  passed and no skipped test; Emaxx's log loads `.eln' files it
  compiled itself (their names carry the hash of Emaxx's private copy
  of the test tree, `comp-test-funcs-3def40ad-...', where the oracle's
  are `...-742562c3-...'), and `comp-async' cases spawn `emaxx'
  children (`invocation-name' is "emaxx", `invocation-directory' the
  release directory).  One hardening note: both editors receive
  HOME=/nonexistent as the GNU test Makefile does, so their user
  `eln-cache' directories coincide on disk; the differing path hashes
  keep the artifacts apart today, and a per-side HOME would make that
  independent of how the harness copies the test tree.
- The nine-source artifact identity test compiles unchanged GNU sources
  through both editors and compares whole `.eln' files; it now runs in
  every full gate (above), and comp.el's 914592-byte artifact is
  identical.
- Test edits in the merge replace fake lexical bindings of
  `symbols-with-pos-enabled' with the C cell in five fixtures; the
  expectations are unchanged.  No test was weakened or ignored.

*Findings.*  One disclosed divergence, row 168: the public
`garbage-collect' SIZE columns are GNU's constants, not this process's
sizes.  Finding 110 had resolved this the other way (real sizes, so
memory-report.el reports Emaxx-true totals); the merge reverses it with
the argument that Fgarbage_collect reports allocator layouts.  Both are
approximations of an ill-posed question (an Emaxx symbol is not a
struct with a size), so the choice is recorded here for review rather
than reverted.  Nothing else in the diff mimics an oracle value.

*What the Linux gate found in the merge.*  The full grouped gate over
`4397ffa' failed one primitives test,
`native_conditional_gc_and_memory_info_match_the_host_contract': the
oracle answers nil for `(garbage-collect-maybe 1)' in a fresh batch
session and Emaxx answered t.  Two C details were missing.
Fgarbage_collect_maybe compares `since_gc' with the `gc_threshold' the
last collection computed and never retunes it; the merged port called
bump_consing_until_gc first, so a let-bound `gc-cons-threshold' of
10000 made factor 1 true after 32000 bytes of consing.  And the
ordinary interpreter never collected on its own -- the merge added
eval_sub's maybe_gc only for active native calls -- so `since_gc' had
grown by the whole startup.  Both are ported: `garbage-collect-maybe'
reads the counters as they are, and eval_sub's maybe_gc collects for
the ordinary interpreter the way alloc.c:maybe_garbage_collect does
(the counter goes negative, the threshold is retuned from
`gc-cons-threshold' and `gc-cons-percentage', a collection follows if
it is still negative: the native heap's sweep, the live census, the
counter reset and `post-gc-hook', which `garbage-collect' had not run
either).  Oracle contract
`garbage_collect_maybe_reads_the_counters_the_last_collection_left`:
`(t nil nil t 1)'.  What still differs is the cadence, row 169.  The
cost on this machine: batch startup 20.3 s to 21.8 s (the startup
collections and the first census), a three-million-cons loop
unchanged.

*Verification of `4397ffa' with these two ports.*  Full grouped gate,
alone on the Linux machine, artifact
`target/grouped-gate/run-1788698294152772461-6679` (2026-09-06 12:38 to
14:05): eval_01 351, eval_02 284, eval_03 320, eval_04 251, eval_05 351,
primitives 411, compat_runtime 84, tty 56 (two inventoried ignores),
batch 46, lightweight 361 (the eighteen anti-cheat checks among them),
the binaries 38 + 1 + 1, the integration targets 14 + 3 + 1 + 5 with the
native artifact identity test (1208 s for the stage); zero failures,
`cargo fmt --check' and strict clippy clean.

Frozen corpus over the audit commit `ecb9ab3', artifact
`frozen-1788703772792632259-19742` (worktree at that commit plus the
local pin, 2026-09-06 14:06 to 19:29, 3600 s per file):

| run | outcomes matching | mismatching | files mismatching |
| --- | --- | --- | --- |
| merge commit `245ff40' (first run above) | 7828 / 7883 | 55 | 21 |
| audit commit `ecb9ab3' | 7863 / 7883 | 20 | 7 |

The fourteen files the first run had regressed all match now; test/src/
comp-tests.el and test/lisp/emacs-lisp/comp-tests.el match 177/177 and
3/3 with Emaxx's own compiler (its execution phase took 2086 s against
the oracle's 14 s).  The 20 that remain are exactly the list above this
section: semantic-utest-ia 6, mml-sec 4, server 3, erc 2, simple 1,
thread-tests 2, test/src/thread-tests 2.  Nothing in the second merge or
the GC ports changed a corpus outcome.

## 2026-09-06 the nine undiagnosed corpus outcomes

*semantic-utest-ia (6 outcomes): fixed.*  Every failing completion
subtest was a member lookup through a local variable declared by a
macro with arguments (`FOO(Test)' expanding to `Test *foo = ...').  The
probes: the top-level tags parse identically on both editors, but
Emaxx's analyzer context has the bare string "foo" where GNU has the
local variable tag, because Emaxx's lexer classifies `foo' as a
`semantic-list' token: `(semantic-lex-spp-symbol-p "foo")' is t.  Its
system macro table (`semantic-lex-spp-macro-symbol-obarray', built from
the parsed system headers) holds seven names GNU's does not -- alias,
cname, foo, name, prefix, proto, x -- all macro *parameter* names from
sys/cdefs.h and c++config.h.  lex-spp.el binds a parameter for one
expansion with `semantic-lex-spp-symbol-push' (`intern' into the
buffer's dynamic obarray, `set') and drops it with `-pop' (`unintern'),
and the next expansion pushes it again.  lread.c:intern_driver
allocates a new symbol on every obarray miss; Emaxx keyed a private
obarray's symbol by its name, so the re-interned name was the old
symbol, still bound.  The second push therefore saw a bound symbol and
stacked its value as an outer binding, the matching pop restored that
value instead of uninterning, and the parameter stayed in the table the
header's semanticdb entry saves.  Minimal reproduction, GNU then Emaxx:

    (let* ((ob (obarray-make 13)) (s1 (intern "alias" ob)))
      (set s1 5) (unintern "alias" ob)
      (let ((s2 (intern "alias" ob))) (list (eq s1 s2) (boundp s2))))
    => (nil nil)        ; GNU
    => (t t)            ; Emaxx before the fix

A private obarray's symbol now gets a fresh identity on every miss (the
uninterned symbol keeps its cells, as GNU's does).  Contract:
`interning_after_unintern_makes_a_fresh_symbol_in_a_private_obarray`.
With the fix, `(semantic-lex-spp-symbol-p "foo")' is nil, the
completion at the `foo->' test point is ("test") on both editors, and
the harness replay of test/lisp/cedet/semantic-utest-ia.el on the
release build matches 17/17 (`run-1788723512933282357-2005`).
Full grouped gate over the fix, alone on the machine, artifact
`target/grouped-gate/run-1788723917117973390-2305` (2026-09-06 19:45 to
21:11): eval_01 351, eval_02 284, eval_03 320, eval_04 251, eval_05
351, primitives 412, compat_runtime 84, tty 56 (two inventoried
ignores), batch 46, lightweight 361, the binaries and the integration
targets with the artifact identity test; zero failures, `cargo fmt
--check' and strict clippy clean.

Frozen corpus over the fix (`1b76c5a', the commit before its rebase onto
PR #53; artifact `frozen-1788729315968771008-15311`, 2026-09-06 21:13 to
2026-09-07 02:29, 3600 s per file):

| run | outcomes matching | mismatching | files mismatching |
| --- | --- | --- | --- |
| audit commit `ecb9ab3' | 7863 / 7883 | 20 | 7 |
| obarray fix `1b76c5a' | 7869 / 7883 | 14 | 6 |

test/lisp/cedet/semantic-utest-ia.el 17/17.  The 14 that remain: mml-sec
4 (ciphertext nondeterminism), thread-tests 2 and test/src/thread-tests
2 (the thread model), server-tests 3 (multi-terminal frames), and the
three startup-bound outcomes above (erc 2, simple-tests 1).  No other
file changed.

*Rebase onto PR #53.*  While that run was in progress main received PR
#53 (`c3f2abf': alloc.c's `symbols-with-pos-enabled' binding around the
mark phase, on top of the `garbage_collect_now' path above, and
funcall_subr's aMANY dispatch for subrs whose finite max_args exceeds
eight).  The obarray fix is rebased onto it as `e78332d' without
conflict.  Full grouped gate over the rebased tree, alone on the
machine, artifact `target/grouped-gate/run-1788748224030710472-27502`
(2026-09-07 02:30 to 03:59): eval_01 351, eval_02 284, eval_03 320,
eval_04 251, eval_05 351, primitives 412, compat_runtime 84, tty
56 (two inventoried ignores), batch 46, lightweight 362, the binaries and
the integration targets with the artifact identity test; zero failures,
`cargo fmt --check' and strict clippy clean.

Frozen corpus over the delivered tree (`cea5101', the rebased fix plus
its record; artifact `frozen-1788753809317920348-8882`, 2026-09-07
04:00 to 09:41, 3600 s per file): 7869 / 7883 matching, 14 mismatching
in six files, the same fourteen outcomes as the run over `1b76c5a'
above and no per-file change.  test/src/comp-tests.el and
test/lisp/emacs-lisp/comp-tests.el match 177/177 and 3/3 with Emaxx's
own compiler.  This is the number for main at `cea5101'.

*erc-tests (2) and simple-tests (1): startup time, not semantics.*
`erc--find-mode' and `erc--essential-hook-ordering' start an inferior
`emaxx -batch', wait with `(while (accept-process-output proc 10))' and
`read' the output; `simple-tests-async-shell-command-30280' starts
`emaxx -Q -batch -eval' and requires `(accept-process-output process 4
nil t)' to return non-nil.  On this machine `emaxx -Q -batch -eval
'(message "")'' takes 22.3 s and the erc child 22.7 s, against 0.03 s
for GNU: no output arrives inside the tests' 10 s and 4 s windows, the
erc parent reads an empty buffer (`end-of-file') and the simple-tests
`should' sees nil.  Verified the other way round: the two erc tests
pass on Emaxx (`Ran 2 tests, 2 results as expected') when the helper
waits 90 s per `accept-process-output' instead of 10, and the
simple-tests body on Emaxx answers nil for the 4 s wait and t for a 60
s wait on the same process, with every other `should' true.  These three outcomes close only when Emaxx
starts from a persistent image instead of replaying loadup.el; that is
the native-comp branch's dump milestone, recorded as open in its own
ledgers, and this document does not claim them.

## 2026-09-07 native-comp: main merged in, first checkpoint

Work moves to the `native-comp' branch for the dump prerequisites.
Main at `c7e4753' (the pushed PR #53 merge plus the obarray fix and its
records) is merged into the branch as `6166a12'; the branch's own eleven
commits since its last merge of main are the L08 census footprints for
GNU pseudovectors (process, window, window-configuration, thread, mutex,
condition-variable, frame, terminal, buffer, overlay, char-table), the
loader's n-ary arity split, and a harness change that redirects the
oracle's native-comp cache.  Full grouped gate over the merge, alone on
the machine, artifact `target/grouped-gate/run-1788775320151300929-22785`
(10:02 to 11:39): every group, the binaries, the integration targets
with the artifact identity test, `cargo fmt --check' and strict clippy
clean.

*sort_args, a CLI divergence found while placing a harness argument.*
emacs.c:main calls `sort_args' before anything reads argv: options are
reordered by the priority in `standard_args' (stable within a priority,
an option kept with its argument, a repeated argument-less option kept
once, "--" and what follows left at the end, an unambiguous prefix of a
long option recognised, an option missing its argument `fatal').
startup.el's option loop stops at the first argument it does not own,
so without the sort `emacs --eval FORM -Q' would leave `-Q' to
`command-line-1', which rejects it as an unknown option -- which is
exactly what Emaxx did ("Unknown option `-Q'"; GNU answers with `-Q'
honoured).  The table and the sort are ported for the configured Linux
build (HAVE_PDUMPER, HAVE_MODULES and SECCOMP_USABLE entries, no HAVE_NS
ones), applied before Clap and before `command-line-args' is built, and
an unambiguous long-option prefix is expanded to the spelling Clap
knows.  Oracle-compared CLI test
`startup_options_after_an_eval_are_sorted_ahead_of_it_like_emacs_c`:
`(nil nil ("--eval" ...))' for `--eval FORM --no-init-fil -Q --batch'
and "emacs: Option '--eval' requires an argument" with exit 1 for a
missing argument, both editors.

*Hardening: each runner's native-comp cache is its own.*  The branch had
redirected the oracle's `eln-cache' into the oracle run's temporary
directory (`startup-redirect-eln-cache' before `-l FILE', because
comp-tests.el compiles at load time); the subject still wrote into
HOME=/nonexistent.  The subject now receives the same redirect, so the
two editors' artifacts never share a directory and the earlier note
about path hashes keeping them apart is moot.

*Row 168 revisited.*  The plan had been to put this process's own sizes
back into `garbage-collect''s SIZE columns.  The branch's census commits
make that inconsistent: the *counts* in those rows are now GNU-modelled
too (a buffer contributes 123 vector slots, a frame 73, an overlay 3,
as alloc.c's sweep_vectors would count them), and Emaxx has no "real"
vector slot for a frame to report.  The rows therefore describe the GNU
heap this session would have, consistently, and the threshold model
reads the same figures.  Row 168 stays as the disclosure of that
choice; it is no longer marked for reversal.

*Gate for this checkpoint.*  Full grouped gate over the merge plus the
two items, alone on the machine, artifact
`target/grouped-gate/run-1788782244875590229-9765` (11:57 to 13:28):
eval_01 351, eval_02 284, eval_03 320, eval_04 251, eval_05 351,
primitives 415, compat_runtime 84, tty 56 (two inventoried ignores),
batch 46, lightweight 363, the binaries and the integration targets with
the artifact identity test; `cargo fmt --check' clean.  Strict clippy
then flagged the new `sort_args' test helpers (five `unwrap' calls under
the crate's `unwrap_used' denial and one indexed loop); those are test-
and loop-shape fixes with no behaviour change, after which strict clippy
is clean and the `sort_args' unit test and CLI contract pass again.  The
next checkpoint's gate covers the tree as committed.

## 2026-09-07 R02c, first boundary: a symbol carries its native word

*What changed.*  comp.c hands generated code a symbol as the object's
own address.  Emaxx's bridge assigned every non-cons object a boxed
`NativeHandle' and found it again through `handle_by_value', a hash map
keyed by the object's identity, on every crossing.  A symbol's
`SymbolNameState' now carries its handle as a packed slot (the owning
heap's id in the high half, the handle index plus one in the low half);
`encode' reads the word from the symbol when the slot names this heap
and the handle still holds this symbol, and falls back to the table
otherwise (a second heap on the same thread, or a stale slot).  The
sweep that frees an unmarked handle clears the slot through the handle's
own value, and a dropped heap clears every slot it owned.  Two Rust-only
controls, registered with the anti-cheat audit:
`symbol_carries_its_native_word_and_a_swept_handle_clears_it` and
`symbol_native_word_slot_is_per_heap_and_verified_against_the_handle`.
The native runtime module's 80 tests pass.

*Measured, and honestly no change.*  `emaxx -Q --batch -f
batch-native-compile comp.el' (fresh HOME and TMPDIR each run, `env
-i', user CPU): before 145.3, 139.3, 140.4 s; after 139.4, 142.8 s.  GNU
compiles the same file in 22.7 s user.  The artifact is byte-identical
to GNU's after the change.  The symbol lookup was therefore not where
the bridge's time goes for this workload; the slot stays because it is
the representation R02c asks for (the word lives in the object) and it
removes a map entry per symbol crossing, but no performance claim is
made for it.  The next boundary is chosen from a sampled profile of
this workload rather than from the contract list's order.

*Where the time goes when Emaxx compiles comp.el.*  A gdb sampler (100
backtraces of the worker thread at 0.7 s intervals, starting 30 s into
the run to skip startup, symbolised release build) over the same
`batch-native-compile comp.el' shows every sample inside the bytecode
VM executing comp.elc: `execute_record'/`run_with_stack' on 72 of 100
stacks, `call_function_value_inner' on 78, `dispatch_named_builtin' on
38.  The native bridge (`native_comp/runtime.rs') is on none.  Both
editors run the compiler as byte-code: the oracle's
`comp--native-compile' is not `subr-native-elisp-p' either, its
`load-history' names comp.elc, and its `../native-lisp/30.2-15987bd4'
was built for another ABI hash than the running binary (30.2-1564b906),
so it is never loaded.  The 5.3x for this workload (about 120 s of work
against GNU's 22.7 s, both interpreting the same comp.elc) is therefore
the Rust VM and its primitives against GNU's C VM, not native code
generation and not the bridge.  Leaf costs in the samples: allocator
calls 22 (realloc 15, alloc 6), variable access 18
(`direct_variable_alias' 5 -- the alias table is consulted on every
variable read -- `global_binding_value', `push_backtrace_frame_with_
locals' 3, `bind_special_variable'), bytecode `decode_program' 5,
`equal'-hash lookups 4, plist `get' through `overriding_plist_property'
5.  Those are the name-keyed storage contracts V02/V03 name (a symbol-
owned value cell and an alias stored in the symbol), so that work
serves the dump prerequisites and the compiler's speed at once, and
comes next.  R02c's remaining boundaries matter where native code
runs: test/src/comp-tests.el's execution phase (2086 s against 14 s) is
the bridge's workload, and it is measured separately when those
boundaries are touched.

*Bytecode variable access reaches the cell directly.*  bytecode.c's
`Bvarref', `Bvarset' and `Bvarbind' read and write a symbol's value
through `Fsymbol_value'/`set_internal'/`specbind' on the symbol object;
they never spell the symbol's name.  Emaxx's VM built a `String' per
`VarRef' and dispatched `symbol-value' and `set' by name through the
primitive table.  `Op::VarRef' now calls `symbol_value_cell_symbol' on
the constant symbol (a void cell signals `void-variable' with the
constant, as `Fsymbol_value' does), `Op::VarSet' calls a shared
`set_internal' (alias resolution, constant and read-only checks, the
buffer-local assignment target, watchers, then the cell write -- the
same path the `set' primitive takes, factored out so the two cannot
drift), and `Op::VarBind' binds through the symbol's name slice without
copying.  The fallbacks for non-symbol operands are unchanged.  Oracle
contract `compiled_variable_references_and_sets_follow_bytecode_c'
byte-compiles closures that reference an unbound variable, set a
constant, read and set a dynamic variable under a compiled `let',
trigger a `set' watcher, and `set' through an alias, and compares the
printed result with GNU; the 37 bytecode tests pass with it.

Measured as above, paired with the checkpoint-1 binary on the same
machine in the same hour: after 125.7, 128.5 s user; the checkpoint-1
binary 146.5 s in the paired run.  That is 13-14 % of the compile of
comp.el, artifact still byte-identical to GNU's, and it agrees with the
sampled profile (variable access was 18 of 100 leaves).  GNU's 22.7 s
is still 5.6x away, and the remaining leaves (allocator, alias table on
every read, `decode_program', plist `get') are the V02/V03 work.

*Checkpoint 2 gate.*  A first full gate over this tree (run 11, started
14:18) was invalidated by me: I built and edited sources while it ran,
which rewrites the binaries the later groups execute, so its result is
not evidence and is not cited.  The clean run, alone on the machine:
grouped gate run-1788792028897488641-26721, GROUPED GATE PASSED (every
group 0 failed, 0 ignored), then `cargo fmt --check' and strict clippy
both exit 0 on the tree as committed.

## 2026-09-07 V02/V03 stage 1: one cell per symbol, and Fdefvaralias as written

*What GNU does.*  data.c reads a variable through the `Lisp_Symbol'
object: `find_symbol_value' switches on `redirect' (PLAINVAL, VARALIAS,
LOCALIZED, FORWARDED) and returns `SYMBOL_VAL', follows `SYMBOL_ALIAS',
or swaps in the buffer-local cell; `declared_special' sits in the same
object.  The name is never consulted.

*What Emaxx did.*  Values lived in a name-keyed insertion-ordered map,
the alias redirect in a second name-keyed map (SipHash), the LOCALIZED
and special flags in two more name-keyed sets, so every symbol read paid
an alias probe, a localized probe and a value probe, each hashing the
name.

*What changed.*  `src/lisp/eval/symbol_cells.rs': one `SymbolCell'
(value, alias target, flags) per symbol, indexed by an id the
`SymbolName' now carries.  The four maps are gone; the ordered
enumeration the old map provided (GC roots, image cloning, the startup
special-marking passes, `known_symbol_names') is kept exactly, first-
binding order with a re-bound name moving last, through an order vector
with stale-position skipping and compaction.  Name-keyed callers (the
several hundred `&str' sites) reach the same cell through the interned
table, so a name and its symbol can never address different cells.  Ids
are process-wide (a mutex-guarded text-to-id registry consulted only
when a symbol state is created, never on a read): the test image
template is built on one thread and cloned into other test threads, and
a per-thread id would have addressed the wrong cells there -- the first
run signalled `void-variable noninteractive' from `display-warning'
inside a test, which is how that was found.  Uninterned symbols draw
ids from a separate counter with the high bit set and keep their cells
in a side table, so the dense vector is sized by the number of interned
names, not by every `make-symbol' ever evaluated; an uninterned text's
id is released when its last state drops, and a live uninterned text
re-entering through the `&str' boundary resolves to the live object
(the native-assq test's old control, which minted a second object for
the same private name, is rewritten to use a different `make-symbol').
The precomputed per-name FNV hash that the old map lookups needed is
removed from the symbol state.

*Fdefvaralias, seven divergences.*  The oracle contract for V03 was
written first and run against the checkpoint-2 binary; it found, in
eval.c's order: (1) no "Cannot make a constant an alias" (`(defvaralias
:kw ...)' succeeded); (2) the cycle error carried the new alias, GNU
carries BASE; (3) no "Cannot make a built-in variable an alias":
`(defvaralias 'load-path 'x)' destroyed `load-path' and Emaxx then died
at exit with `void-variable load-path'; (4) no "Don't know how to make
a buffer-local variable an alias" and no let-bound refusal; (5) an
unbound base did not receive the alias's value (the 2008 emacs-devel
hand-over) and both symbols were not declared special; (6) the
`losing-value' warning was called with one argument, GNU passes the
`format-message' text as well, and watchers were notified before the
checks that can still signal; (7) the return value was the alias, GNU
returns BASE, `variable-documentation' was not put when nil, and the
redirect was stored as the end of the base's chain, so re-pointing the
base later did not re-point the alias (GNU stores BASE itself and walks
the chain per read).  All seven are ported; `eval_sub'/`Fsymbol_value'
now also signal `void-variable' with the symbol they were given rather
than the end of the chain.  LOADHIST_ATTACH of the alias symbol is
recorded as for `defvar'.

*The forwarded-variable manifest.*  "Built-in" is `SYMBOL_FORWARDED',
which no source grep can decide alone (android, w32, haiku, pgtk, dbus
files are not compiled into this build, and `byte-code-meter' sits
under an ifdef), so
`src/lisp/primitives/generated_gnu_c_forwarded_variables_linux.rs' is
generated by taking every DEFVAR_* name in the pinned sources (876) and
asking the pinned oracle, through Fdefvaralias itself, which of them it
refuses: 749 forwarded, of which 29 already `SYMBOL_LOCALIZED' at `-Q
--batch' (buffer.c and keyboard.c call Fmake_variable_buffer_local on
them, e.g. `case-fold-search', `deactivate-mark') and six constants
(`enable-multibyte-characters', the three font tables, the fixnum
bounds -- now also in `is_constant_symbol'); 127 plain.  The anti-cheat
gate `gnu_c_forwarded_variable_manifest_matches_fresh_regeneration'
regenerates both lists against the oracle and requires byte identity.
Disclosed residuals: the manifest exists for the Linux oracle only, so
on macOS `gnu_c_forwarded_variables()' is empty and the built-in
refusal is not reproduced until the same probe is run against the
Darwin oracle; `trapped_write' is not modelled (an alias's own watchers
are cleared instead of trapped to the base's).  A suspected residual
did not survive its probe: `make-local-variable' or
`make-variable-buffer-local' on a DEFVAR_PER_BUFFER slot leaves it
"built-in" in GNU (data.c returns early for a BUFFER_OBJFWDP), while a
DEFVAR_LISP or a Lisp `defvar' made buffer-local becomes "buffer-
local"; both editors print the same four answers for `fill-column' (twice),
`load-path' and a Lisp variable.

*Contracts.*  `variable_cells_follow_data_c' (alias read/write/
indirect/boundp/default-boundp, alias to an unbound base and
`makunbound' through it, cycle payload, two `make-symbol's of one name,
lexical vs dynamic `let' of an uninterned symbol, buffer-local read/
default/other-buffer/kill, void payload through an alias, compiled
access), `defvaralias_follows_eval_c' (the twenty cases above),
`defvaralias_records_the_alias_in_load_history'; four Rust-only
`SymbolCells' tests registered with the anti-cheat audit; the manifest
gate.  Focused subset: 263 of the alias/special/buffer-local/watcher/
bytecode/native-runtime/anti-cheat tests passed and the one failure was
the native-assq control described above, fixed and re-run.

*Measured, and no change.*  `batch-native-compile comp.el' as before,
paired on the same machine in the same hour: this tree 125.4, 126.3 s
user; the checkpoint-2 binary 125.3 s.  The artifact is byte-identical
to GNU's.  The sampled profile had put the alias table on 5 of 100
leaves; replacing three hash probes with one index did not move the
total for this workload, and no performance claim is made.  The value
of the change is representational (V02/V03 rows moved to partial): the
cell is now the object the native word and the epoch's replacement will
attach to.

*Checkpoint 3 gate.*  Alone on the machine: grouped gate
run-1788800966130003373-15561, GROUPED GATE PASSED (every group 0
failed), `cargo fmt --check' and strict clippy exit 0 on the tree as
committed.

## 2026-09-07 V02 stage 2: the native word lives in the cell, the epoch is gone

*What changed.*  Generated code reads `SYMBOL_VAL' as a word.  The
bridge kept that word in the symbol's native handle, tagged with a
process-wide epoch that every `set', alias and localization anywhere
bumped, so one write to any variable retired every cached word in the
process.  The word now lives in the symbol's own cell beside the value
it belongs to, stamped with the heap id and the heap's collection
generation; the cell's own write transitions (`set_internal',
`makunbound', `defvaralias', the first buffer-local binding, the
special flag) clear it, and a sweep advances the generation so a word
whose bridge allocation may have been reclaimed is never returned.
The epoch field, its six bump sites and the per-handle cache are
removed; the native `symbol-value' fast path asks the interpreter's
cell.  Rust-only control
`a_cells_native_word_is_cleared_by_every_data_c_write_transition'
(registered with the anti-cheat audit) walks every transition and the
stamp discipline; the two native GC tests that proved a collection
discards cached words now prove it through the generation.  90 bridge,
cell and variable-contract tests pass.

*Measured on native execution.*  A native-compiled loop of three
million iterations reading two global variables and adding
(`$S/bench-native.el', `native-comp-speed' 2, `benchmark-run', best of
three, alternating binaries): this tree 5.01, 5.17 s; the checkpoint-3
binary 5.30, 5.30 s; GNU 0.178 s.  About 4 % on this micro-benchmark,
and GNU is 28x away: the per-call bridge cost around each helper
(`symbol-value', `+') dominates, not the value-word lookup this stage
removed.  That gap is R02c's remaining boundaries and R03, measured
here from now on.

*Checkpoint 4 gate.*  Alone on the machine: grouped gate
run-1788807756929107362-1200, GROUPED GATE PASSED (every group 0
failed), `cargo fmt --check' and strict clippy exit 0 on the tree as
committed.

## 2026-09-07 V04 stage 1: a buffer's local bindings by symbol, and a void local is still a binding

*What GNU does.*  A buffer's local bindings are its `local_var_alist',
`(symbol . value)' cells found with `assq_no_quit' on the symbol
object; the symbol's blv carries `local_if_set'.  A cell whose value is
`Qunbound' remains a binding: `local-variable-p' answers t, a read is
void, `buffer-local-variables' lists the bare symbol, and the default
is untouched.  `Fmakunbound' is `Fset (symbol, Qunbound)', so in a
buffer with a cell it voids the cell, and for a `local_if_set' symbol
without one (outside a let made for this buffer) it creates a void
cell.  `Fmake_local_variable' copies the default cell's value (void
stays void) and refuses a constant with VARIABLE as given;
`Fmake_variable_buffer_local' turns a void plain value into nil,
refuses a constant, and returns VARIABLE as given.

*What Emaxx did.*  The per-buffer table was keyed by name (three
name-keyed sets held `local_if_set', the per-buffer kind and the
always-local kind), and a local was either bound or absent: the probe
against the checkpoint-4 binary showed `(makunbound 'v)' on a local
deleting the cell (`boundp' t through the default, `local-variable-p'
nil, the symbol gone from `buffer-local-variables'),
`(make-local-variable 'void-var)' binding the local to nil,
`(make-variable-buffer-local 'zz-new)' leaving the symbol void (GNU:
nil) -- the extended contract program died on that read --
`(make-local-variable :kw)' and `(make-variable-buffer-local 'nil)'
succeeding, and `make-variable-buffer-local' of an alias returning the
base instead of the argument.

*What changed.*  `src/lisp/eval/local_cells.rs': one buffer's
bindings keyed by symbol id in first-binding order, `Value::Unbound'
for a void local; the three name-keyed sets are the LOCAL_IF_SET,
PER_BUFFER and ALWAYS_LOCAL flags in the symbol cell.  The read path
distinguishes "no cell" from "void cell" (`buffer_local_binding'),
and `set', `specbind', `makunbound', `local-variable-p',
`local-variable-if-set-p', `variable-binding-locus',
`buffer-local-value' and `buffer-local-variables' use that
distinction; `let_shadows_buffer_binding' is data.c's predicate over
the restore stack (a LET_LOCAL record for this buffer, or a
LET_DEFAULT one made here).  Oracle contract
`buffer_local_cells_follow_data_c' (three `with-temp-buffer' programs,
about forty observations including the alias cases and the lexical
`let' of a non-special `local_if_set' symbol) passes with the 173
buffer-local, special-binding, watcher, alias and bridge tests.  No
timing: the buffer-local branch is not on the compiler's path, and
none is claimed.

*Disclosed residual.*  The blv's `where'/`valcell' swap (GNU loads the
current buffer's binding into the symbol so a read is one cell
dereference) is not represented: every read of a LOCALIZED symbol
probes the current buffer's table.  DEFVAR_PER_BUFFER slots keep
Emaxx's model (`makunbound' of one still takes the previous path).

*Checkpoint 5 gate.*  Alone on the machine: grouped gate
run-1788814256001850440-16741, GROUPED GATE PASSED (every group 0
failed), `cargo fmt --check' and strict clippy exit 0 on the tree as
committed.

## 2026-09-07 V05 stage 1: forwarding kinds in the cell, and the oracle's own DEFVARs

*What GNU does.*  A DEFVAR_* symbol is `SYMBOL_FORWARDED'; a store goes
through `store_symval_forwarding' by the slot's kind (`Lisp_Fwd_Bool'
keeps `!NILP (newval)', `Lisp_Fwd_Int' is CHECK_INTEGER then
`integer_to_intmax' or `overflow-error'), and storing Qunbound makes
the symbol plain.  lread.c binds every DEFVAR of the build before any
Lisp runs.

*What the probes found.*  Emaxx coerced DEFVAR_BOOL stores by a name
lookup over all 178 source names (so an android-only name, plain in the
Linux oracle, was coerced too), had no DEFVAR_INT check except two
hand-written arms (`(setq undo-limit 1.5)' was kept, GNU signals
`wrong-type-argument integerp'; `(setq undo-limit (expt 2 70))' was
kept, GNU signals `overflow-error'; `gc-cons-threshold' accepted any
bignum), and left 73 of the oracle's 749 forwarded names void at `-Q
--batch': the X11, Cairo, font-table and text-conversion DEFVARs whose
C owners have no Emaxx counterpart (`(boundp 'x-selection-timeout)',
`font-weight-table', `cairo-version-string', `x-keysym-table', ...).

*What changed.*  Three flags in the symbol cell -- FORWARDED, FWD_BOOL,
FWD_INT -- set at interpreter construction for every name in the Linux
forwarded manifest, with the kind from the DEFVAR_BOOL manifest and a
new DEFVAR_INT manifest (`generated_gnu_c_int_variables.rs', 73 source
names, regeneration gate like the bool one).  `prepare_variable_
assignment' coerces or checks by flag, `is_forwarded_variable' is the
flag, and `makunbound' clears all three as set_internal does.  The 73
missing names are bound from
`generated_gnu_c_forwarded_defaults_linux.rs': the oracle's printed
`default-value' of each (the current buffer's value was wrong for
`multibyte-syntax-as-symbol', which lisp-interaction-mode sets locally
in *scratch* -- the first manifest recorded nil from a temp buffer and
the gate's top-level probe read t, which is how the `default-value'
rule was found), read back with `read-from-string' at construction and
declared special; the anti-cheat gate
`gnu_c_forwarded_defaults_manifest_matches_fresh_regeneration'
re-prints every listed name from the oracle and requires byte
identity.  Nothing about the C code behind those names is claimed: the
cells exist and hold what the oracle's do at `-Q --batch'.

*Contracts.*  `forwarded_c_variables_follow_store_symval_forwarding'
(bool coercion on set and let, int type and overflow errors, a plain
symbol after makunbound accepting any object, boundp of a forwarded
slot) and `oracle_only_forwarded_c_variables_are_bound_as_the_oracle_
binds_them' (all 73 names: bound, default-bound, special, not
local-if-set, and the default value itself, a hash table by count and
test).  Off Linux both manifests are empty: the 73 names stay void
there and the kind checks do not run, disclosed here and in the row.

*Checkpoint 6 gate.*  Alone on the machine: grouped gate
run-1788820575098554763-32733, GROUPED GATE PASSED (every group 0
failed), `cargo fmt --check' and strict clippy exit 0 on the tree as
committed.

## 2026-09-08 D06: finalizers run, the root set is audited, and the prerequisite rows close

*Finalizers never ran.*  `(make-finalizer F)' allocated an id and
dropped F; nothing ever called it, and `(make-finalizer 3)' succeeded
(GNU: `wrong-type-argument functionp').  alloc.c keeps every finalizer
object's function on the `finalizers' list, and `garbage_collect'
moves each unreached object with a non-nil function to
`doomed_finalizers' after the mark phase and before the weak-table
sweep (marking the doomed functions so they survive), then runs them
once the collection is complete -- before Fgarbage_collect's
`post-gc-hook' -- each under `inhibit-quit' with a signal caught and
logged as "finalizer failed: %S".  Emaxx now does exactly that: the
reachability walk marks a reached finalizer's function, both
collection paths queue the doomed ones before the weak sweep, and
`garbage_collect_now' runs them after the census and the specpdl
restore.  print.c's `#<finalizer>' replaces the id form.

*What the oracle can and cannot pin.*  GNU's conservative stack scan
keeps a just-created finalizer object alive through the next
collection: `(let ((ran nil)) (make-finalizer (lambda () (setq ran
t))) (garbage-collect) ran)' is nil in GNU and t in Emaxx, and which
later collection dooms it moved between two probe runs.  The contract
`finalizers_follow_alloc_c' therefore observes a finalizer only
through collections two `garbage-collect' calls after the release,
where three GNU runs agree, and covers the type check, the printed
form, creation order, a kept object not running, and one reachable only
from another finalizer's function running; the error-logging path is a
Rust-only test.  Emaxx's precise reachability is the documented
semantics ("after garbage collection when the returned finalizer
object becomes unreachable"), and the earlier collection is a
disclosed difference, not a fabrication.

*The contract helper printed with Display.*  The comparison helper for
oracle contracts rendered the Rust result with `Display', which marks a
shared sublist as `#<circular-list>' and does not escape embedded
quotes; it now prints through the interpreter's `prin1-to-string', as
the oracle side is compared through GNU's printer.  All 424
primitives-module tests pass under the stricter comparison.

*Root census.*  `interpreter_value_fields_are_gc_roots_or_documented'
(anti-cheat gate) parses the `Interpreter' struct, and for every field
whose type holds Lisp objects requires the reachability walk to mark
it or the field to be listed with its C reason: the two cleared stack
pools, the live-finalizer list (marked through the objects), the
function index (an index over the marked `functions'), and the
dispatched-signal identity memo (thread.c marks `handler->val' only
while a handler runs).  35 fields hold objects; 30 are marked.

*Prerequisite rows.*  With this checkpoint the pdump ledger's D03,
D04 and D06 rows are closed for image construction and D05's owner
decision is recorded (see the rows for the exact statements and what
each leaves open under other rows).  Not closed and not claimed: L08
census parity (the numbers `garbage-collect' prints), obarray bucket
order (L11), R03 mirror removal and the remaining R02c boundaries
(bridge cost), and the macOS manifests -- none of which the ledger's
own definition counts as a dumping blocker.

*Checkpoint 7 gate.*  Alone on the machine: grouped gate
run-1788827584304667786-18386, GROUPED GATE PASSED (every group 0
failed), `cargo fmt --check' and strict clippy exit 0 on the tree as
committed.

## 2026-09-08 R03: a cons generated code allocates is the evaluator's cons

*Two storages per cons.*  alloc.c:Fcons takes one `Lisp_Cons' from a
cons block and every primitive, the mark pass and generated code
address that one object.  Emaxx's bridge allocated a generated cons in
its own block arena (16K-cell blocks, a free list, a block map, mark
and occupancy bitmaps) and, the first time Rust read it, allocated a
second object -- a `ConsCell' whose two ABI words were attached to the
arena slot -- so the same cons had an arena slot and a Rust cell, with
the sweep consulting the arena's bit for one and the mirror flag for
the other, and the census adding an "arena-only" byte term to avoid
counting the pair twice.  The arena is gone.  `NativeHeap::cons' now
allocates the `ConsCell' itself (its ABI prefix is the two words
generated code stores through) and keeps the owning reference in a
map by address until a collection finds it unreachable; the first Rust
read attaches the typed view to that same cell.  The mark pass marks
the owner and, when present, the mirror at the same address, expanding
car before cdr as process_mark_stack does; the sweep drops the mirror
and then the owner that no mark reached, which is sweep_conses's free
unless a Rust value still holds the cell.  The live-bytes census needs
no arena term.  The 79 runtime tests are unchanged in what they assert;
ten that read the arena's own bookkeeping now ask the heap whether a
cons is live, how many generated conses it owns, or its collection
count.

*What is still not GNU.*  The typed view is still a view: when Rust
reads or writes a cons that generated code also holds, its two `Value'
fields are decoded from, and published back to, the two words inside
the same cell (`reconcile_mirror', `publish_interpreter_writes').  That
is R03b and is recorded as open in the native ledger.  Positioned-symbol
views for generated code are per-record boxes the heap roots and never
frees.

*Measurement, and a regression found on the way.*  The first build was
1.7x slower on a 300k-cons allocation loop (3.4-4.0 s against
1.5-2.4 s).  Sampling put the time in the address-keyed heap maps: the
identity hasher returned raw addresses, and 48-byte cells from one
allocator cluster in hashbrown's buckets.  The hasher now mixes the key
(a 64-bit finalizer); no lookup semantics changed.  The handle
identity word of R02b was already pre-mixed and is now mixed twice;
the paired timings below include that.  Paired on one
machine, the corrected build against the checkpoint-7 binary,
alternating runs: 300k-cons build 1.43-2.85 s against 1.47-2.59 s,
walk 0.68-0.75 s against 0.77-0.88 s, the two-global native loop
7.58-7.67 s against 7.87-8.13 s, batch-native-compile comp.el 173.5 s
user against 176.0 s, the artifact byte-identical to GNU's.  The comp.el
absolute times are higher than the checkpoint-3 pair (125 s) because
the machine was slower today; only the paired difference is claimed,
and it is within run-to-run noise: no speedup is claimed, and the
1.7x regression is gone.

*Checkpoint 8 gate.*  Alone on the machine: grouped gate
run-1788840508912446236-6692, GROUPED GATE PASSED (every group 0
failed), `cargo fmt --check' and strict clippy exit 0 on the tree as
committed.

## 2026-09-08 D07: the dumper's entry runs as pdumper.c writes it, up to the open

*What the entry did.*  `dump-emacs-portable' checked that its
argument was a string and signaled "Portable dumper backend is
unavailable".  Nothing of Fdump_emacs_portable's prelude ran: no
batch-mode refusal, no thread checks, no `load--fixup-all-elns', no
collection, no `command-line-processed' binding.

*What it does now (`primitives/pdumper.rs').*  In pdumper.c's order:
the batch-mode refusal with GNU's message; the main-thread refusal;
the other-threads refusal (`(cdr (all-threads))'); Ffuncall of the
Lisp `load--fixup-all-elns'; `garbage_collect' repeated while
`number_finalizers_run' is non-zero; specbind of
`command-line-processed' to nil, unbound on every exit; CHECK_STRING;
Fexpand_file_name; then the three variables `dump_unwind_cleanup'
restores -- purify-flag, post-gc-hook, process-environment -- cleared
around the writer and put back by direct cell writes (no watcher, as
`Vpurify_flag = Qnil' has none).  The oracle pinned every observable
of this order: the wrong-type error on 42 arrives after one
collection (post-gc-hook ran once), after the fixup, and after the
binding's `let' watcher event, with the `unlet' event on the way out;
the thread errors carry GNU's strings; a process with another live
thread is refused before the fixup runs.  Contract
`dump_emacs_portable_prelude_follows_pdumper_c' holds all of it.

*Where the boundary is now, and what that is not.*  GNU opens the
output file (`O_RDWR | O_TRUNC | O_CREAT', 0666) and writes a header
whose first magic byte is `!' until the dump completes; a failure
after that point leaves the incomplete file behind.  Emaxx signals
the unavailable error where the open would be, so no file is created
(Rust-only control
`dump_emacs_portable_restores_its_context_at_the_writer_boundary':
the three variables and the binding are back, no file exists).  A
missing directory therefore reports the unavailable error where GNU
reports `(file-missing "Opening dump output" "No such file or
directory" PATH)'; that case joins the contract with D08, when there
is a header to write.  Also not GNU and disclosed in the D07 row:
`will_dump_with_unexec_p' is false by configuration, `check_pure_size'
has no pure space, `block_input' has nothing to block, ENCODE_FILE is
the UTF-8 identity every file primitive uses.

*Startup.*  The image reconstruction at startup reaches this
primitive from loadup.el (dump-mode "pdump"), so the prelude now runs
there too: the fixup (a no-op without `--bin-dest'/`--eln-dest'), one
collection of the preloaded heap, the binding, and the unwind.  GNU's
temacs does the same at that point.  The boundary test that used a
bare interpreter now runs in the initialized batch image: a bare
interpreter has `noninteractive' nil and is refused as GNU refuses an
interactive session.  Startup cost of the added collection, paired and
alternating against the checkpoint-8 binary on one machine (`-Q --batch
--eval (kill-emacs)', fresh HOME): 30.96, 34.86, 32.24 s user against
35.19, 33.15, 31.46 s; within the run-to-run spread, and the absolute
numbers are this machine's today, not the 18 s baseline recorded
earlier.

*Checkpoint 9 gate.*  Alone on the machine: grouped gate
run-1788851939988389086-22849, GROUPED GATE PASSED (2609 tests, every
group 0 failed), `cargo fmt --check' and strict clippy exit 0 on the
tree as committed.

## 2026-09-08 D08: the image writer, and a unibyte string GNU could not hold

*What exists now (`primitives/pdumper/').*  `image.rs' is the file
layout: pdumper.c's 100-byte `dump_header' (magic with the `!' marker
until completion, the executable's SHA-256 as the fingerprint, table
locators, section starts), the object types and relocation kinds, and
the self-representing words (fixnums tagged as lisp.h tags them; nil,
t and the unbound marker as the symbol words Emaxx represents
specially).  `context.rs' is `dump_context': the in-memory buffer with
`dump_write', `dump_seek' and `dump_align_output'; `dump_object_start'
and `dump_object_finish'; `objects_dumped' with the normal, cold and
copied states; the `dump_queue' with its four tail queues, link
weights, sequence numbers and the distance score, ported clause by
clause from `dump_queue_enqueue' and `dump_queue_dequeue'; the fixup
list applied by `dump_do_fixups' in offset order; the cold queue
(strings' bytes in GNU's internal encoding, floats, bignum limbs) and
the copied queue (built-in functions, the discardable section); the
three relocation phases, the object-start table and the Emacs
relocations, whose targets are the interpreter's root slots rather
than C addresses.  `mod.rs' runs Fdump_emacs_portable's body from the
open to the completed header in the C order, with the same section
boundaries and the same stderr report.  `load.rs' (test builds) is the
validation half of `pdumper_load' -- size, magic, the incomplete
marker, the fingerprint -- and the object reconstruction that the
round-trip controls need; the process-level restore is D12/D13.

*What the controls prove.*  From explicit roots, an image holding a
shared sublist, a self-referential cons, a vector naming the list
twice, immutable and mutable strings (multibyte, unibyte with raw
bytes, text properties whose values are shared with the graph, a
character outside Unicode), one float object referenced twice next to
a second equal float, two bignums, an integer beyond the fixnum range,
the fixnum bounds, nil, t, the unbound marker, an interned and an
uninterned symbol, and a built-in function reads back as an isomorphic
graph at different addresses, sharing and cycle intact.  Symbol cells
read from a live interpreter (special flag, alias redirect, watcher
list with the trapped-write flag, plist, function, an unbound value)
come back as written.  A short file, the `!' marker, another magic and
another fingerprint are refused with pdumper_load's outcomes.  Object
starts are unique and ascending, and each object is written once.

*What a real dump does today.*  `(dump-emacs-portable FILE)' in a
batch session opens FILE as GNU does and stops at the first object the
writer does not cover with pdumper.c's "unsupported object type in
dump: KIND" (a record first in the current image, reached from the
`gud' custom-group plist; closures, char-tables, buffers, markers,
overlays, finalizers and reader forms are the others -- D09 to D11;
with TRACK-REFERRERS the referrer path is printed to stderr as
print_paths_to_root does), leaving
the truncated empty file GNU leaves when it fails before its single
write.  The startup reconstruction, which reaches the primitive from
loadup.el, hands off with the unavailable error where the open would
be: `image_reconstruction_handoff' marks that phase, since this
process has no temacs and loadup's dump call is where it stops.  No
image is loadable yet and none is claimed.

*Not GNU, disclosed in the D08 row.*  The records are Emaxx's objects
(a symbol record carries the cells GNU keeps in Lisp_Symbol plus the
watcher list and the Emaxx-only cell flags); table entries are two
32-bit words, not GNU's packed word; every symbol other than nil, t
and unbound is a heap record addressed by name; a built-in function's
copied record names it where GNU relocates to the subr's address.

*A string GNU could not hold.*  The first real dump stopped inside the
writer with a unibyte string holding characters above 255: the Burmese
composition regexp from burmese.el, which `replace-regexp-in-string'
builds by replacing ASCII keys in an ASCII pattern with multibyte
pieces.  search.c's Freplace_match with a STRING returns `concat3
(before, newtext, after)', so the result is multibyte when either is;
Emaxx's string branch kept STRING's flag.  `(string-bytes ...)' on that
entry signaled "Character cannot be encoded" where GNU returns 228.
Fixed at the site; contract
`replace_match_on_a_string_returns_concat3_multibyteness' pins the
flag for both argument orders and the two Burmese entries.  The writer
now refuses such a string with its own message ("unibyte string holds
character ..."), since GNU has no such state and the fault is at a
construction site, never in the image.

*Checkpoint 10 gate.*  Alone on the machine: grouped gate
run-1788863908506282633-10836, GROUPED GATE PASSED (2617 tests, every
group 0 failed), `cargo fmt --check' and strict clippy exit 0 on the
tree as committed.

## 2026-09-08 D09: closures, environments, char-tables, records and bool-vectors in the image

*What the writer covers now.*  Interpreted closures are written as
their GNU closure slots (parameters, body, environment, documentation,
interactive) plus the Emaxx fields that hold the exact Lisp objects
(`public_parameters', `public_environment'); the parameter vector, the
body vector and the captured environment are shared Rust objects, so
they are written once each through raw-pointer fixups -- as GNU writes
interval trees, blvs and fwds -- and a lexical frame carries its
bindings, identity, function-namespace flag, locally-special
declarations and the authoritative Lisp alist.  Char-tables are
written as Emaxx keeps them (subtype, default, parent, extra slots,
the range log, category docstrings), not as GNU's sub-char-table tree;
the observable table is the same and the row says so.  Records and
pseudovectors whose state is their slots (records, byte-code closures,
fonts, symbols with position, keymap facades) are written with their
ids, because the id is the identity every `Value::Record' carries and
the loader installs them under the same ids.  Bool-vectors go to the
cold section as bits.  Obarrays are records: the initial one with its
symbol list, a private one with its slot.  The main thread is an
object of the running process (GNU's DUMP_OBJECT_IS_RUNTIME_MAGIC).
Windows and processes are nilled as `dump_nilled_pseudovec' does.
The cells of `nil' and `t' -- self-representing words until now, so
their function cell and plist were lost -- are scanned with the roots
and written after the queue drains, where GNU writes the copied
symbols' hot parts.

*The loader.*  Records and char-tables are installed with the image's
ids (`install_record', `install_char_table' replace an existing id:
the image is the authority).  A closure is materialized on demand
when a field names it, with its environment created as an empty shell
first so a closure reachable through its own frame terminates, the
way the test template's graph copier already handles the same cycle;
a closure that names itself through its body or parameters is an
error.  `nil' and `t' arriving as immediate words are accepted
wherever a symbol is expected (obarray entries, parameters, property
names); the previous loader would have refused them.

*Control.*  Two closures over one `let' binding, a char-table with
ranges, a subtype, a default and an extra slot, a bool-vector with
three bits set, a record whose slot shares the closure list, and the
main thread go through an image into a second interpreter: the two
closures share one environment object there, calling the first
returns 1, the second adds 5 and the first then returns 6; the
char-table's fields, the bits, the record's slots and the shared
identity are as written; the main thread is the second interpreter's
own.  The referrer paths (TRACK-REFERRERS) named the object that
stopped the first attempt -- the standard obarray reaches every
symbol's value, and `comp-subr-arities-h' is a hash table -- so the
control does not include the obarray until D10.  print_paths_to_root
recurses without a guard in GNU; a referrer cycle made the printer
loop here, and each object's paths are now printed once.

*Still refused, with pdumper.c's error.*  Hash tables (D10); buffers,
markers, overlays, finalizers, frames, terminals (D11); native
compilation units and native functions (D14/D15); and what GNU
refuses too: threads other than the main one, window configurations,
mutexes, condition variables, tree-sitter objects, sqlite handles.
Reader forms are refused as well; none has been seen reachable from a
root after loading.  Not recorded: the interned-in-another-obarray
state (D09 row).

*Checkpoint 11 gate.*  Alone on the machine: grouped gate
run-1788876131748816838-30654, GROUPED GATE PASSED (2618 tests, every
group 0 failed), `cargo fmt --check' and strict clippy exit 0 on the
tree as committed.  Main was fetched after the gate and still carries
only merge commits of this branch (its tree is checkpoint 8's), so the
merge asked for waits for the next checkpoint.

## 2026-09-08 D10: hash tables frozen and thawed as fns.c and pdumper.c do

*The writer.*  `dump_object' now defers a hash table exactly as
`dump_hash_table' does under `defer_hash_tables': the first reference
scans the table (its keys and values are enqueued, nothing is written)
and puts it on the deferred list; the drain loop writes the deferred
tables before each normal drain, so the tables sit together in the
image.  A table's record is followed by what `hash_table_freeze'
keeps: the count, the weakness, the standard test and the mutability,
then the compact key/value contents in slot order.  The test is
`hash_table_std_test''s: `eq', `eql' or `equal', and a user-defined
test signals "cannot dump hash tables with user-defined tests" (GNU's
message, Bug#36769) as a Lisp error, not as an unsupported object.
`dump_hash_table_list' writes the vector of every table written at
`header.hash_list'.

*The loader.*  Each table on the hash list is thawed as
`hash_table_thaw' does: the runtime index is rebuilt from the compact
contents, the allocation is minimal (`count' entries, no room for
growth), and a table dumped immutable comes back immutable.  A table
missing from the list is an error.

*Control.*  An `eq' table, an `equal' table with a key removed and
another added, a key-weak table and an empty table go through an image
into a second interpreter, where `gethash' finds the keys through the
thawed index (and not the removed one), a value that was a shared
object is the same object as the graph's, `hash-table-weakness' and
`hash-table-test' answer as before, the thawed table accepts a
`puthash', and the entry order is GNU's: `remhash' freed slot 0 and
the next `puthash' reused it, so the compact contents walk that key
first.  I had written the opposite order as the expectation; the code
was right and the expectation was corrected.  The user-defined test
refusal is its own control.

*Not GNU, in the row.*  The contents follow the record inline rather
than through a separate packed array, and the hash list names each
table once where GNU's scan pass can list a table twice.  Weak tables
are written with their current contents; nothing is swept at load
until the restore (D13).

*Where a real dump stops now.*  Past every hash table: the first
refused object is an overlay (show-paren's context overlay), which is
D11's, where GNU itself signals "dumping overlays is not yet
implemented" for a buffer that has any.

*Checkpoint 12 gate.*  Alone on the machine: grouped gate
run-1788886573678285508-17120, GROUPED GATE PASSED (2620 tests, every
group 0 failed), `cargo fmt --check' and strict clippy exit 0 on the
tree as committed.  Main was fetched after the gate and still carries
only merge commits of this branch (its tree is checkpoint 8's); the
merge waits for the next checkpoint.

## 2026-09-08 D11 (object kinds): buffers, markers, overlays, finalizers, frames and terminals in the image

*The writer.*  `dump_object' dispatches the six remaining heap kinds
that were refused.  `dump_buffer' writes a live buffer's own fields as
pdumper.c copies the `struct buffer': name, file and truename, point,
mark and its activation, the narrowing, the modification counters,
the visited-file modtime, the multibyte flag and the hook inhibition;
`last_name' is cleared as `dump_buffer' clears `last_name_'.  The text
goes to the cold section as COLD_OP_BUFFER does, in GNU's internal
representation with the out-of-Unicode side list and a terminating
NUL (GNU also writes the zeroed gap; the rope has none), through a
raw-pointer fixup to `own_text.beg'.  The property spans follow the
record as the interval tree does; the markers pointing into the
buffer are written as the `own_text.markers' chain with WEIGHT_NORMAL;
the local bindings (`local_var_alist_', a void local as the unbound
word) and the local hook lists, the syntax and case tables (the
BVARs), the base buffer of an indirect buffer and the persistent mark
marker are Lisp fields with WEIGHT_STRONG; the undo entries are
written as Emaxx's typed entries (`undo_list_' is WEIGHT_STRONG in
GNU).  A buffer with a live overlay signals GNU's "dumping overlays is
not yet implemented" as a Lisp error; a killed buffer is written with
no text and a nil name, as BUFFER_LIVE_P false dumps.  `dump_marker'
writes the buffer with WEIGHT_NORMAL, the positions and the insertion
type.  `dump_overlay' writes the buffer field, the bounds, the advance
flags and the plist; only a deleted overlay can get through, because
a live one's buffer is a field, is dumped, and refuses -- which is how
GNU's writer behaves too.  `dump_finalizer' writes the function with
WEIGHT_NONE ("so we can give it a low weight") and the `prev'/`next'
neighbours with WEIGHT_NORMAL (a sentinel neighbour, an Emacs pointer
in GNU, is nil here), and `dump_roots' ends with
`dump_finalizer_list_head_ptr''s relocations for the list's last and
first finalizer, none for an empty list.  Frames and terminals are
nilled as `dump_nilled_pseudovec' writes them: the record is the id.

*The loader.*  Buffers are installed with their ids from the record
and the cold text; markers with the marker-buffer index and the mark
relation; deleted overlays on the list of the buffer that held them;
finalizers in chain order from the `finalizers.next' root (a
finalizer off the chain follows in image order); a frame as a dead
`FrameState'; a terminal as its object alone.

*Control.*  A buffer with text (an out-of-Unicode character in it), a
property span, a local variable, a local hook, its own syntax table,
a mark, a narrowing, insertion/deletion/boundary undo entries and a
modtime goes through an image into a second interpreter with every
field checked, the undo list printing identically, the marker index
the same size; a marker into it keeps its insertion type and a
detached marker stays detached; two finalizers come back in list
order with their functions; the frame is dead after the round trip
and the terminal's object is the same; a deleted overlay keeps its
properties and its holding buffer; a killed buffer is an object with
no buffer behind it.  The refusal of a buffer with a live overlay is
its own control.  Two expectations of mine were wrong and were
corrected against the code: the deletion at 1 had moved the mark and
the marker back one position, and `text_property_at' answers within
the narrowing only, so the spans are compared directly.

*Not GNU, in the row.*  The saved-text snapshot Emaxx compares for
`buffer-modified-p' is written beside the text (GNU compares
counters); the undo entries are typed entries rather than the
`buffer-undo-list' conses, so a list tail Lisp retained is not the
same object after a round trip; a marker's last position and
mark-buffer relation and a deleted overlay's holding buffer are Emaxx
fields written with the record; a killed buffer loads with an empty
name behind the object; a terminal's nilled record leaves the running
process's terminal state alone; the doomed-finalizer list, empty
after `Fdump_emacs_portable''s collection loop, is an error on the
explicit-root path rather than dumped, since Emaxx keeps only the
doomed functions.  The remaining `Interpreter' root groups are open as
D11b.

*A real dump completes.*  With the object kinds covered,
`dump-emacs-portable' in an initialized batch process no longer stops
at an unsupported object: the release binary wrote a complete image of
the loadup state (Dump complete; header=100 hot=5827884
discardable=11648 cold=8131360; relocs hot=363152 discardable=1455;
14029600 bytes), and the D07 boundary contract now asserts the
completing path instead of the refusal: `nil' returned, the three
variables and `command-line-processed' restored, the completed magic
in the file, and pdumper_load's validation and reconstruction reading
that whole image back into a second interpreter with an obarray of
the same size as the writer's.  That is an image of every object the
current roots reach; the root groups D11b tables are not in it yet,
and no process starts from it until D12/D13.

*Checkpoint 13 gate.*  Alone on the machine: grouped gate
run-1788899931881043684-12635, GROUPED GATE PASSED (2556 tests, every
group 0 failed), `cargo fmt --check' and strict clippy exit 0 on the
tree as committed.  Main was fetched before the gate and still carries
only merge commits of this branch (its tree is checkpoint 8's); the
merge waits for the next checkpoint.

## 2026-09-08 D11b: the remaining root groups, each against its GNU counterpart

*The table.*  `dump_roots' in GNU visits every staticpro'd slot.  The
mark phase's root list is Emaxx's inventory of the same thing, and
`eval/dump_roots.rs' now answers for each entry: written as the Lisp
value GNU keeps for the group, or not written with the C line that
re-creates it after a load.  The anti-cheat gate
`interpreter_roots_are_dumped_or_documented' scans the root-marking
function and requires every `self.<field>' it reads to appear in
`dump_root_values', in `dump_roots.rs', or in `ROOTS_RESET_AFTER_LOAD';
a new root that is neither fails the gate.

*Written.*  Under their own root slots, in GNU's shape (a vector per
coding system or charset as the hash tables hold them, an alist for
the buffers, vectors for the key buffers): `Vbuffer_alist', the three
keyboard vectors, the C slots of forwarded variables, the charset
tables (attributes, ordered list, `charset-list', ISO-2022 list,
aliases, `iso_charset_table', non-preferred head, sjis/big5), the
coding-system tables (attributes, aliases, priorities, category
representatives and priorities), `Vccl_program_table', the standard
syntax, category and case tables of `buffer_defaults' and the ASCII
case tables, `Vfontset_table', the global face vectors, the font
selection order and alternative alists, the fringe bitmaps,
`composition_hash_table', the ert registry, `labeled_restrictions',
`Vtimer_list', `last_thread_error', and the assigned captured lexical
cells.  The loader reinstalls each from its value.

*Not written, and why.*  Each with its GNU line: frames and the
selected frame (`init_frame_once_for_pdumper' resets `Vframe_list' and
`selected_frame'), the windows (`init_window_once_for_pdumper'), the
terminal's parameters (the terminal is nilled; `init_tty' makes the
initial one), the kboard's macro state and last event frame
(`syms_of_keyboard_for_pdumper'; kboards are not in the image),
`Vprocess_alist' (`init_process_emacs'), the inotify watch list, the
specpdl, handlerlist and backtrace (`init_eval_once_for_pdumper'
allocates a fresh specpdl; the dump runs inside its own specbind), the
current buffer (`init_buffer' selects `*scratch*'), and Emaxx's
quote-template cache.  Two transient Emaxx queues must be empty and
the writer signals otherwise (`pending_thread_events', the deferred
defsubst unbindings).  I first listed the captured lexical cell
updates as transient; the real dump refused with one entry pending,
and reading the field showed it is the storage of assigned captured
variables (GNU's environment conses), so it is written as a group and
the loader registers each restored closure's frames so assignments
stay shared.

*Control.*  A bare interpreter given a second buffer, keys, a
detached forwarded variable, a charset alias, a timer, an ert test, a
labeled restriction, a fringe bitmap and a composition is written from
the interpreter's own roots (the obarray included) and read back;
every group prints the same from the restored interpreter (the timer
with its remaining seconds elapsed) and the buffer list keeps its
order.  The pending-state refusal is its own control.  The real dump
of the initialized batch state is compared the same way: all groups
but the timer list print identically after the round trip.

*Not GNU, in the row.*  A frame's own face vectors are dropped with
the frame (GNU re-derives them at frame creation); `timer-list' is a
native table in Emaxx while the variable reads nil (a pre-existing
divergence of the timer implementation, not of the dump).  The native
scalars beside these groups (counters, next ids, the DOC offsets) are
GNU's remembered data, D13.

*Checkpoint 14 gate.*  Alone on the machine: grouped gate
run-1788910723379605551-10358, GROUPED GATE PASSED (2560 tests, every
group 0 failed), `cargo fmt --check' and strict clippy exit 0 on the tree as
committed.  Main was fetched during the gate and now carries content
of its own (the terminal and runtime parity work merged over
checkpoint 10); it is merged in the next commit, with the image code
adapted to its terminal and frame state, and gated again.

## 2026-09-08 Terminal frames: finding 159

The second-terminal/frame stub has been replaced by real device ownership
and separate frame/window trees. The port follows the pinned GNU `frame.c`,
`terminal.c`, `term.c`, `window.c`, `xfaces.c`, and keyboard binding and
deferred-hook paths. GNU `server.el` and its tests are unchanged. This does
not substitute a server implementation in Rust or special-case test names.

The seven unchanged server tests passed in both editors, including the
three failures recorded in finding 159. Three additional contracts call
the GNU oracle before checking device reuse, window lifetime and ownership,
independent window operations, face copying, keyboard-local bindings, and
configuration restoration. The real-client comparison additionally passes
primary/client input and screen isolation, fragmented arrow-key and UTF-8
input, saving, normal frame deletion, and abrupt PTY disconnection.
Artifacts: `/private/tmp/emaxx-terminal-interactive-6`.

The manual ownership review checked live-device closure, batch mode not
changing the device's input modes, frame-local face copies, window owners,
GC roots, image-copy restrictions, and both protected and deferred deletion
hooks. It also corrected a native symbol-word cache crossing keyboards and
dynamic unbinding after selection moved to another terminal. All 22
automated audit checks pass; strict all-target/all-feature Clippy and
formatting pass. The one native-runtime edit only restricts a Linux-only
`MaybeUninit` import to its existing Linux/x86-64 use site.

The first full grouped attempt could not clone the startup template:
`NativeCompilerState::clone` correctly rejected live native runtime state,
and the poisoned template lock caused subsequent failures. Its failed
result is retained at `/private/tmp/emaxx-terminal-full-gate`. The safety
check has not been weakened. The follow-up full gate uses fresh interpreters
and serial groups at `/private/tmp/emaxx-terminal-fresh-full-gate`.
That run passed all 351 tests in `eval_01`, then reported two `cl_getf_*`
failures in `eval_02`; the failed group was stopped to diagnose them rather
than continuing an already-failed checkpoint. Both failures reproduce in
an isolated, unmodified archive of starting main `450cb77`: the increment
returns 3 but the existing plist cell still contains 1. The same clean
baseline also reproduces the native template-clone guard failure.
Baseline logs: `/private/tmp/emaxx-terminal-cl-getf-baseline.log` and
`/private/tmp/emaxx-terminal-template-baseline.log`; current-tree diagnostic:
`/private/tmp/emaxx-terminal-cl-getf-diagnostic.log`. These are unresolved
baseline failures, not a passing full regression gate. No native mutation
or image-cloning behavior was changed to bypass them.

A separate affected-area run (frame/window/face/terminal/keyboard/coding
and TTY tests, excluding the already-passed `eval_01`) finished with 238
passed, 14 failed, and the two standard ignored TTY tests. Replaying its
14 failures on clean main gave 3 passed and 11 failed. The three new
regressions were corrected from GNU C: frame root/first/selected window
queries accept a valid window argument, and copying a face must create the
destination before registering its ID. The terminal lifecycle oracle
contract now covers window arguments belonging to an unselected frame.
The correction replay passed those three tests, all three terminal
contracts, and the frame-identity contract; its remaining 11 failures also
occur on clean main. The completion failures now have the same causes on
both trees (seven-argument `window-text-pixel-size` and popup height 11
versus expected 7), rather than the incorrect frame-argument rejection.
Logs: `/private/tmp/emaxx-terminal-affected-tests.log`,
`/private/tmp/emaxx-terminal-affected-baseline.log`, and
`/private/tmp/emaxx-terminal-corrections-tests.log`.

Final serial replay on the corrected binary: 22/22 automated audit checks,
7/7 unchanged server tests in GNU and Emaxx, and the real `emacsclient -t`
comparison all pass. Both editors produced `abXéc`, saved `abXéc\n`, exited
the client with status 0, and preserved the primary frame after normal
closure and abrupt client disconnection. Fragmented escape/UTF-8 input
and separate primary/client screen contents were checked. Final artifacts:
`/private/tmp/emaxx-terminal-interactive-final`; logs:
`/private/tmp/emaxx-terminal-final-{audit,gnu-server,emaxx-server,interactive}.log`.
Strict all-target/all-feature Clippy and the final build passed after the
regression corrections. No complete Rust-gate pass is claimed.

This finding's missing device/frame model is distinct from universal TTY
parity. Remaining limits, including repainting inactive terminals, terminal
suspend/resume, legacy termcap output, and non-UTF-8 streaming input, are
recorded in [terminal-frame-parity.md](terminal-frame-parity.md). No fresh
7,883-test corpus result or 100% compatibility is claimed here.

Follow-up attribution: all 13 starting-main test failures described above
pass before the first native-comp merge (`c3aac3e`) and fail at that merge
(`245ff40`) on the same macOS setup. "Baseline" here means before the
terminal work, not before native-comp. The compressed-file test's coding
values actually match; it fails its function-representation assertion, as
does the help metadata test. See the [comparison evidence and failure
classification](native-comp-regression-attribution-2026-09-08.md).

## 2026-09-08 Native-comp merge and adversarial regression review

This checkpoint merges `c5ec1b855f8a3535ff32c46bb038272d14bad005` into
main and retains the terminal-frame work above. The historical failures
and their attribution are recorded in
[native-comp-regression-attribution-2026-09-08.md](native-comp-regression-attribution-2026-09-08.md).
All 13 passed together with native-runtime, dumper, and terminal controls
in the first combined replay (112 passed, zero failed). That intermediate
result preceded the additional adversarial corrections below.

The review covered the entire merge diff and local corrections, the
source-ownership audit, test selection, native-state lifetime, cache
invalidation, dump validation, terminal/device ownership, and the
real-client comparison. GNU Lisp and upstream test files were not edited.
The fixes do not disable native execution or advice trampolines, substitute
builtin implementations for GNU Elisp functions, copy live native state,
consume oracle answers in production, or change compatibility selectors.
The two function-representation assertions now require either bytecode or
an actual native Elisp subr, as observed in GNU; a builtin facade fails.

Additional adversarial findings and corrections:

- A native-written cached tail stayed stale after return. The read barrier
  now reaches its stable owning heap outside an active native call. The
  existing mutation registration supplies that owner once per heap; the
  cons allocation layout and generated two-word ABI are unchanged. The
  heap uses explicit raw allocation ownership, and reconciliation/GC suppress
  recursive heap access. Non-native GC traverses roots without holding a
  heap borrow. Teardown reconciles before detaching; moving the runtime
  cannot invalidate the owner address. A second live heap cannot reattach
  the same canonical prefix.
- A cached source form could still evaluate old arguments after that tail
  fix. The new negative control first reproduced 3 instead of 11. Derived
  caches now watch their native-exposed cons dependencies as well as Rust
  mutation notifications. Crossing into native storage invalidates older
  Rust-only snapshots once. A separate control confirms checking one
  graph does not refresh an unrelated native cons.
- File-name-handler matches also retained a replaced handler. The negative
  control reproduced `first` instead of `second`. Its cache now watches the
  alist graph and handler property lists, including the operations filter,
  while preserving definition, string, and existing mutation guards.
- Reading an executable fingerprint could silently fall back to hashing
  empty bytes. It now requires a successful executable read. The normal
  digest is unchanged; an internal read failure cannot produce a false
  cross-binary identity.

The added dump control writes a native-modified cyclic cons after return,
loads the D08 image, and verifies both the changed value and cycle identity.
Existing writer controls cover sharing, supported object kinds, symbol
cells, queue deduplication, incomplete headers, and wrong fingerprints.
D08 still rejects unsupported objects and does not provide full-image
startup restoration. Its Rust object layout is not GNU `.pdmp` binary
interchange. These limitations remain explicit in the pdump ledger.

Final validation results follow after the serial gate completes.

The upstream native-comp comparison, before the final allocation-owner
correction described below, selected all 178 upstream cases. Both
editors passed all 177 normal cases, with no skips. The extra bootstrap
case failed in both because the harness's isolated checkout lacks the
explicitly loaded `lisp/emacs-lisp/comp.elc`. Thus the harness's reported
178 matching outcomes are **177 passes and one matching setup failure**,
not 178 passing tests. A separate bootstrap replay against the built tree
is required to exercise that case. Artifact:
`target/compat/run-1788876445995048000-42662`.

Before that allocation-owner correction, the source also passed 136 native-runtime/type/cache/audit
controls, including all 22 structural audit tests; strict all-target,
all-feature Clippy with `-D warnings`; rustfmt checking; and the gate-profile
CLI build, without warnings. Logs:
`/private/tmp/emaxx-native-final-controls.log`,
`/private/tmp/emaxx-native-merge-clippy.log`, and
`/private/tmp/emaxx-native-merge-build.log`.

That rebuilt Emaxx and the pinned GNU binary both passed all seven unchanged
server tests and the real client comparison. The first GNU server attempt was blocked by the
sandbox's Unix-socket restriction; the socket-enabled replay passed in
both editors. Logs retain the restriction failure separately. Final client
artifacts: `/private/tmp/emaxx-native-merge-terminal-gate`; server logs:
`/private/tmp/emaxx-native-merge-{gnu,emaxx}-server.log`.

The full Rust gate runs serially at
`/private/tmp/emaxx-native-merge-final-full-gate`. Its wrapper retains the official
inventory, selection, template settings, outcome checks, and ignored-test
policy, while limiting execution to one group and one test worker at a
time. The manifest records the override. It schedules all 2,558 library
tests; only the two existing standalone TTY tests are ignored. Binary and
integration stages follow, including the nine-source native artifact
identity test. No result is claimed for that gate until it completes.

The first full-gate attempt was stopped during `eval_01`, with no test
failure, for an additional ownership correction. Rust's documented
[Box aliasing rules](https://doc.rust-lang.org/std/boxed/index.html#considerations-for-unsafe-code)
make retaining the heap's Box alongside a persistent raw owner pointer
unsuitable across later moves and mutable borrows, even with UnsafeCell.
The owner now consumes the allocation with `Box::into_raw`, retains a
NonNull pointer, and reconstructs the Box exactly once at teardown. Normal
heap borrows still suppress reentrant cache reads. Existing move, teardown,
cross-heap, native mutation, and dump controls exercise that owner. The
interrupted artifact remains at `/private/tmp/emaxx-native-merge-full-gate`;
its manifest records the interruption rather than a passing result.

After the allocation-owner correction, the 136 native-runtime/type/cache/audit
controls passed again (zero failures or ignores), followed by a successful
gate-profile CLI build. Strict all-target/all-feature Clippy with
`-D warnings` and rustfmt checking also passed without warnings. These
checks cover the same source as the restarted full gate. Logs:
`/private/tmp/emaxx-native-raw-owner-controls.log`,
`/private/tmp/emaxx-native-merge-raw-owner-clippy.log`, and
`/private/tmp/emaxx-native-raw-owner-build.log`.

The restarted gate at `/private/tmp/emaxx-native-merge-final-full-gate`
passed all 351 `eval_01` tests, then finished `eval_02` with 283 passes and
one failure: `loaded_gnu_cl_generic_method_keeps_generic_documentation_public`.
The identical test also fails on starting main `450cb77`; the extracted
expression returns `(t t 11 t)` in pinned GNU. Its error was
`wrong-type-argument (char-table-p nil)` during help argument highlighting.
`syntax.c:Fmodify_syntax_entry` treats a nil optional table as the current
buffer's table. Rust accepted an omitted table but rejected explicit nil,
including the slot padded by native calls. Matching GNU's nil handling
fixes the unchanged help test. A new oracle control covers nil, omitted,
and explicit separate tables without changing either editor's Lisp.
The primitive probe failed before the correction and matches GNU's
`(nil 119 119 46 95)` afterward. Evidence:
`/private/tmp/emaxx-native-generic-documentation-repro` and
`/private/tmp/emaxx-native-generic-documentation-fixed.log`.
This is an additional starting-main mismatch, beyond the original 13.

After that correction, 137 native-runtime/type/cache/audit and syntax
controls passed, with zero failures or ignores. Log:
`/private/tmp/emaxx-native-syntax-controls.log`.
The next full run schedules previously unreached groups first and then
repeats `eval_01` and `eval_02` on the corrected source; no group or test is
removed. Its artifact root is
`/private/tmp/emaxx-native-merge-syntax-full-gate`.

That run was interrupted early in `eval_03` after two alignment cases
failed during fixture reconstruction, before executing alignment. Both
also fail on starting main: `replace_with_gnu_batch_runtime` constructed
the replacement while the old native session was still alive. The dynamic
loader reused live `.eln` handles whose saved compilation-unit words
belonged to the old interpreter's heap. The fixture now retires the old
interpreter before reconstructing GNU startup. This preserves native
execution and the existing test assertions; it does not share or overwrite
another live session's relocation words. Reproduction logs are in
`/private/tmp/emaxx-native-alignment-repro`.

All three C-alignment controls then passed in
`/private/tmp/emaxx-native-alignment-fixed.log`; strict Clippy and rustfmt
passed again (`/private/tmp/emaxx-native-session-clippy.log`). The subsequent
full run streamed per-test diagnostics and completed `eval_03` with
319 passes and one failure in script-mode ownership. Native-compiled
`sh-mode` and `python-mode` are subrs in both GNU and Emaxx, while the test
expected them not to be subrs. A focused review also reproduced the same
kind of failure for electric-mode producers (`tex-insert-quote`) and undo.
All three fail on starting main as well. GNU and Emaxx agree on the actual
representations: those four functions are native Elisp subrs; `tex-mode`
itself is bytecode on this build.

The four related tests now require bytecode or an actual native Elisp subr,
rejecting a C builtin substitute. Their derived-mode, key-binding,
indentation, interpreter-alist, and undo expectations are unchanged.
The already-passing `tex-mode` control uses the same ownership predicate.
The electric fixture's pre-call `tex-mode` autoload check remains unchanged;
only its loaded `tex-insert-quote` ownership assertion changes.
The before-fix replay was one pass and three failures in both current and
starting-main builds. GNU/Emaxx representation probes and replay logs are
retained at `/private/tmp/emaxx-native-mode-ownership`. The failed full-run
artifact remains at `/private/tmp/emaxx-native-merge-session-full-gate`.

The next run, `/private/tmp/emaxx-native-merge-ownership-full-gate`, exposed
`batch_native_lisp_callables_preserve_help_arglists`: its `zerop` check
expected a bytecode descriptor even when GNU loads a native Elisp subr.
The review also found a real public-type mismatch: `aref` exposed the
private slots of native functions, native compilation units, windows,
threads, and hash tables because their Rust storage uses `Value::Record`.
The `data.c:Faref/Faset` dispatch now checks the public object kind before
index bounds, rejects opaque objects with `wrong-type-argument arrayp`,
and retains the distinct GNU rules for readable closures and mutable
ordinary records. The help test retains its bytecode argspec assertion
and checks that native Elisp functions reject array access. A GNU-backed
control covers both reads, negative indexes, and writes, as well as a
real record tagged `subr` and a byte-compiled closure.

While building that control, the audit disproved the old fixture comment
that `comp-no-spawn` t requested in-process compilation. GNU's
`comp--native-compile` instead skips ordinary compilation under that flag;
the earlier advice replays could rely on already-cached trampolines.
The fixture now uses `comp-no-spawn` nil and
`comp-running-batch-compilation` t, matching GNU's batch compilation path
through `comp--final1`. Loaded native code, foreground compilation, and
missing trampolines remain enabled; only deferred file compilation is
disabled in the embedded fixture. The CLI's settings remain unchanged.
A new control starts with a fresh native cache, verifies no trampoline
exists, compiles a caller, replaces `file-system-info`, checks the native
caller sees that replacement, and verifies the newly compiled trampoline.
The Emaxx side deliberately uses the fixture settings without overriding
them. The GNU side explicitly uses the same batch-compilation settings.
Evidence and control logs are in `/private/tmp/emaxx-native-array-boundary`.

The adjacent `fns.c:Fcopy_sequence` review found the same internal-record
leak: copying a native function or compilation unit produced an ordinary
storage clone without native loader ownership. Public `copy-sequence` now
accepts GNU's sequence and ordinary-record kinds, rejects opaque objects
and closures with `wrong-type-argument sequencep`, and projects internal
keymaps through their public list representation. List copying uses the
existing proper-list walker, including dotted-tail and cycle checks.
The native-object oracle control also checks rejected copying and an
independent ordinary-record copy. It passed against both GNU and Emaxx;
see `copy-control.log` in the same evidence directory.

After that correction, 138 focused controls passed again, followed by
strict Clippy, rustfmt, and the CLI build. The next full run completed
`eval_04` with 250 passes and one failure in
`format_spec_renders_buffers_with_princ_semantics` (2424.53 seconds).
The same test fails on starting main. GNU's `print.c:PVEC_BUFFER` reads the
live buffer's name, while Emaxx printed a name snapshot retained in a Lisp
value, including native handles created before a rename. The printer now
looks up the buffer by identity for both escaped and unescaped output,
and prints `#<killed buffer>` after deletion. The original format-spec
expectation is retained, with additional rename/deletion checks. GNU and
before-fix Emaxx probes, plus the baseline reproduction, are retained in
`/private/tmp/emaxx-native-buffer-printing`. The failed full-run evidence
remains in `/private/tmp/emaxx-native-merge-array-full-gate`.

The printing correction passed 17 focused controls, including all 13
original failures, the three format-spec contracts, and the native-object
boundary control (319.32 seconds). Strict Clippy and rustfmt passed again.
The next serial wrapper runs every library group even after a group
failure, retaining the official selectors, inventory, and strict result
validation. Any caught validation failure prevents a passing final
summary; this guard was also checked with an injected synthetic failure.
This scheduling change collects all outstanding failures in one run
without turning failed tests into passes or excluding them.

The printing diagnostic sweep reached the remaining library groups and
found 15 failing tests: four Eshell cases, three macOS C-variable forwarding
cases, three native-Elisp ownership assertions, two platform/toolchain
assumptions, multisession, DND, and HTTP retrieval. Starting-main replays
with isolated homes and caches reproduce 14 of these; DND passes with the
old fixture because its `comp-no-spawn` setting suppresses missing
trampolines. The complete diagnostic sweep is not a passing gate: one
Eshell teardown hung after an assertion failure and was terminated, and
repeated evaluator groups were deferred until the accumulated fixes.
Evidence is in `/private/tmp/emaxx-native-printing-sweep-baseline` and
`/private/tmp/emaxx-native-merge-printing-full-gate`.

The macOS forwarding manifest now comes from the pinned GNU binary's
`defvaralias` classifications: 678 forwarded names, including 29 already
localized to buffers. The existing strict regeneration check now runs for
macOS as well as Linux. Diagnostic classification uses the C locale so
localized quotation marks cannot change the categories. This fixes boolean
coercion, integer stores, and built-in alias restrictions; it does not add
a macOS manifest of defaults for C variables lacking runtime owners.

The C-primitive generator had classified native-compiled Lisp as C-owned
when a Lisp function shared the name of a C DEFUN on another platform.
It now excludes `subr-native-elisp-p`. Fresh Darwin regeneration removes
exactly `frame-windows-min-size`, `x-begin-drag`, and `x-file-dialog`, leaving
1417 C primitives. This supersedes earlier sections that treated their
native Lisp representations as permission for Rust substitutes.
`frame-windows-min-size` now runs GNU's `window.el` computation; its Rust
substitute returned fixed numbers. The file-dialog substitute is removed.
`x-begin-drag` remains C-owned on the contracted Linux build and Lisp-owned
in `ns-win.el` on macOS. DND can therefore replace that function without
requesting an ABI trampoline for a nonexistent C subr. No trampoline hook
was bypassed or disabled, and the existing cold-cache native-caller control
remains required. The generator and arity manifests are regenerated, not
hand-edited to exempt a test.

The multisession fixture now runs its second editor in a separate process,
as GNU's workflow does. It retains the first live editor, verifies the
second reads 1 and writes 2, then verifies the first observes 2. The original
initial-value, invalid-value, file-existence, and SQLite assertions remain.
This avoids two independent heaps sharing one process's live `.eln`
relocation globals. The three remaining ownership assertions accept actual
native Elisp subrs while retaining their source and behavior assertions.
The loader test checks Darwin's Mach-O diagnostic on Darwin, and the
compiler-version check compares GNU to the configured libgccjit API's
actual version, rather than a fixed 14.2.0 literal.

HTTP retrieval exposed MD5's fifth `NOERROR` argument. Its implementation
now follows `fns.c:extract_data_from_object`: string slices apply after
encoding, buffer slices use accessible buffer positions, implicit coding
selection follows the buffer's write policy, invalid coding falls back only
where GNU permits it, and current-buffer state is restored on errors.
String hashing does not record a coding system; buffer encoding does.
The related arity review replaced `make-char`'s charset-ignoring placeholder
with `charset.c:Fmake_char` position-code assembly, dimension defaults,
ISO masking, range checks, and charset decoding. GNU-backed controls include
real native callers to both five-argument primitives. Five minimum-arity
mismatches are corrected. Additional controls compare errors through source
calls, `apply`, and actual native callers; GNU's resolved subr object is
retained in `funcall` arity errors before handlers observe them.

Eshell exposed two native unwinding defects. `eval.c:unwind_to_catch`
executes cleanup at each handler's saved depth before removing that handler;
the previous runtime removed inner handlers first. A small native-compiled
control returns `(t (cleanup handled))` in GNU but signals `no-catch` in the
old Emaxx binary. Matching GNU's order fixes that control. Eshell also
requires synchronization of handler removals performed directly by generated
code before `helper_unbind_n` executes cleanup. Without it, the trace shows
one cleanup being unbound twice after a deferred subprocess, consuming a
caller's entry. Synchronizing before cleanup and native error dispatch
makes both unchanged pipeline assertions pass. The dynamic-stack integrity
check remains unchanged; temporary tracing is removed. A related control
checks cleanup crossing into interpreted Lisp (that positive control also
passes before the fix and is not presented as a reproducer).

The 11 additional failures outside Eshell passed focused checks, as did
MD5/charset controls and both Eshell pipelines after the unwind correction.
These are focused results, not a completed full gate. The warning checks,
broader Eshell suites, and 176 unique regression controls on the final candidate
are recorded under `/private/tmp/emaxx-native-final-fixes/validation`.


The 176 unique focused controls completed with 176 passes, no failures or
ignores (576.00 seconds). Both broader Eshell suites, rustfmt, and strict
all-target/all-feature Clippy passed. The subsequent complete Rust run is
recorded at `/private/tmp/emaxx-native-merge-final-sept9-full-gate`.

That run found two primitive-test failures. The fixed C-mirror snapshot
still named 1420 entries after the documented regeneration removed three
native-Elisp owners. Independently comparing the generated inventories
confirms exactly `frame-windows-min-size`, `x-begin-drag`, and `x-file-dialog`
were removed, with no additions: 1417 entries and NUL-separated FNV-1a
10595795051582904188. Updating that snapshot retains its fixed fingerprint,
the empty missing-C-primitive inventory, and all arity/dispatch checks.

The second failure was introduced by removing the Rust `x-file-dialog`
substitute: `fboundp` previously returned t because that dispatcher existed,
although its body only signaled a window-system-unavailable error. Removing
it exposed a graphical-capability difference. A fresh pinned GNU invocation
returns `(nil nil nil)` for the three batch dialog eligibility queries,
and has both feature `ns` and `x-file-dialog`. Its actual function owner is
`term/ns-win.elc`, loaded by `loadup.el` only when `ns` is present. The Emaxx
terminal runtime has neither feature `ns` nor that Lisp dialog definition.
This is a lost public function binding; changing the test is not a claim
that NS API availability has been restored.

The user requested retaining backend-specific contracts. The corrected test
keeps identical GNU/Emaxx batch-predicate assertions and checks the oracle's
file-dialog availability and owner against its actual features: NS requires
a Lisp owner, PGTK and X11 with GTK/Motif require a C owner, and plain X11 or
terminal-only builds lack that definition. It reports the exercised backend;
this host exercises NS, not Linux graphical branches. Emaxx's separate
assertion pins its terminal-only capability boundary. No OS-based selection,
function substitution, new ignored test, or GUI-parity claim is introduced.

At the user's direction, repeated evaluator groups were stopped during
`eval_02`. The run remains recorded as interrupted, with its two primitive
failures retained. Completed `eval_01`/`eval_05` and other groups are retained;
earlier completed `eval_02`/`eval_03`/`eval_04` coverage and the successful
reruns of their fixed failures are reused. The record explicitly spans
multiple runs and is not relabeled as one passing full-gate execution.
Only the two test corrections, rustfmt, and strict Clippy are rerun;
production code is unchanged. Binary/integration and final native/server
verification remain pending. Evidence and the coverage ledger are under
`/private/tmp/emaxx-native-final-sept9-gate-corrections`.


The two corrected primitive contracts passed together (2/2, 27.01 seconds),
with rustfmt and strict all-target/all-feature Clippy clean. The coverage
ledger preserves the original group outcomes plus the successful targeted
corrections: 2564 library tests covered successfully and two pre-existing
TTY ignores across those recorded runs. This is not a fresh single-run
full-gate pass.

The subsequent CLI integration run passed 14 of 15 tests. The remaining
assertion rejected GNU's own output before comparing Emaxx: it assumed an
`eval-buffer` loader frame, while the current pinned GNU image reports
`load-with-code-conversion` immediately after the three probe call frames.
A standalone shared-descriptor replay produces byte-identical complete
GNU/Emaxx output and exit status 255. The correction retains the required
stdout/stderr ordering, error text, and probe frames in the fixed prefix,
and retains the unchanged exact comparison of the entire subject/oracle
output (including every loader/startup frame). No output is normalized.
The single corrected CLI test passed (13.03 seconds); rustfmt and strict
Clippy passed again. Evidence: `/private/tmp/emaxx-native-final-sept9-stdio`.
The 14 already-passing CLI cases and completed binary tests are retained;
only the other three integration targets continue.


The binary test targets passed all 42 tests. The remaining integration
stage passed all three ERT-runner tests, the native artifact identity test,
and all five package-lifecycle tests. Together with the retained CLI cases
and its corrected exact-comparison rerun, all 24 integration tests have
successful coverage. Native identity exercised all nine unchanged GNU
fixtures (eight byte-identical artifacts plus the no-byte-compile case),
including the full compiler frontend `comp.el`, in 237.17 seconds. The
five package-lifecycle tests passed in 396.33 seconds. Evidence:
`/private/tmp/emaxx-native-final-sept9-gate-corrections/cargo-stages` and
`/private/tmp/emaxx-native-final-sept9-gate-corrections/remaining-integrations`.
The subsequent native-comp replay is needed because its earlier result
predates the allocation-owner, native-unwind, MD5, and charset corrections;
the isolated later test corrections do not require another broad rerun.


The native replay on the final production source selected all 178 upstream
cases: GNU and Emaxx each passed 177, failed the same bootstrap setup case,
and skipped none. The isolated checkout lacks `comp.elc`; that matching
setup failure is not a pass. All three native-cache tests passed in both
editors. Artifacts: `target/compat/run-1788912698309078000-72391` and
`target/compat/run-1788914322598418000-76155`. Source fingerprints remained
unchanged throughout these runs.

The following GNU server attempt failed before exercising server behavior:
the runner's long temporary root exceeded the Unix socket pathname limit
(`Service name too long`). That failed receipt remains under
`/private/tmp/emaxx-native-final-sept9-gate-corrections/after-cli-validation`.
Only the uncompleted server, PTY, and standalone bootstrap stages resume
with the shorter `/private/tmp/ex-final` root. No production or upstream
test changes accompany this runner correction.

The corrected short-path server replay passed all seven unchanged tests
in each editor. The real PTY replay also passed in both: client and primary
input isolation, fragmented arrow-key and UTF-8 input, saving `abXéc`,
normal client closure, and abrupt disconnection preserving the primary
terminal. Evidence: `/private/tmp/ex-final/server-{gnu,emaxx}.log` and
`/private/tmp/emaxx-native-merge-terminal-final`.

Publication scope was reconfirmed after observing the newer native-comp
tip `68c0b22`: publish the validated `c5ec1b8` merge and local corrections
first, and review the three subsequent dump commits separately. Those
newer commits are not included in this checkpoint or its coverage claims.

The standalone bootstrap replay loaded the original built-tree `comp.elc`,
compiled stage one, loaded that generated compiler, and compiled stage two
in each editor. Both then failed the unchanged raw `cmp` assertion with
exit status 1. GNU took 114/115 seconds for the two compilation stages;
Emaxx took 75/76 seconds. Both pairs contain equally sized 881,824-byte
Mach-O files, with exactly 78 differing bytes per pair. Every difference
falls within `LC_ID_DYLIB`, `LC_UUID`, or the code-signature data; there
are zero differing bytes outside those regions. The library identity
contains the independently generated stage filename. This is a shared
macOS bootstrap reproducibility limitation, not a passing bootstrap test.
No bytes or test expectations were normalized. Full logs, original
artifacts, and read-only binary inspection results are retained under
`/private/tmp/emaxx-native-merge-bootstrap`. The validation driver preserves
both editor exit codes separately from its own successful orchestration.

Final review: the original 13 corrections pass, the 176 focused controls
pass, and the retained Rust coverage accounts for 2,564 successful library
cases, two pre-existing TTY ignores, 42 binary tests, and 24 integration
tests. Rustfmt and strict all-target/all-feature Clippy are clean. Native
comparison has 177 passes plus one shared setup failure, native-cache has
three passes, and the independent bootstrap reaches the same raw binary
comparison limitation in GNU and Emaxx. Both editors pass all seven server
tests and the real PTY comparison. Source fingerprints stayed unchanged
during final validation; only audit documentation was completed afterward.

The adversarial review and its negative controls are recorded above,
including allocation ownership, native cons/cache coherence, opaque array
boundaries, actual advice compilation, and native cleanup ordering. The
final isolated test corrections preserve complete subject/oracle comparison
and expose the NS capability gap instead of claiming a restored dialog
binding. There are no new skipped tests or patched GNU Lisp files. This
checkpoint does not claim a fresh single-run Rust gate, a fresh 7,883-test
corpus result, universal terminal/GUI parity, or complete image restoration.

## 2026-09-09 OpenPGP investigation: platform qualification

The four recorded `mml-secure-en-decrypt-1` through `-4` ciphertext
mismatches require a Linux reproduction. The current pinned Darwin source
explicitly rejects `system-type == darwin` in its `test-conf` helper before
these test bodies run. Fresh isolated runs on main `1b60d9b`, using identical
copies of the unchanged upstream test and its 35 tracked resource files,
produce four skips and zero passes in both GNU and Emaxx. These skips do not
close the four recorded Linux mismatches. The previously discussed
7,876/7,883 figure is an extrapolation from the historical corpus and focused
fixes, not a newly measured platform-independent total.

Source inspection shows that `mml2015-epg-clear-decrypt` catches a decryption
error and preserves the ciphertext buffer. That explains the failure's
appearance but does not establish the cause of the GPG error. The older
agent/sandbox diagnosis remains to be verified against the actual EasyPG
status/debug output. No runtime correction or ciphertext normalization has
been made.

The current macOS host has no GnuPG executable or available Linux container
runtime. Linux execution details have been requested. Prepared copies,
SHA-256 fixture inventory, separate GNU/Emaxx outputs and an EasyPG debug
capture driver are under `/private/tmp/emaxx-openpgp-sept9`. The capture
driver was checked against GNU: a skipped test returns a failing diagnostic
status, rather than being counted as a pass. The next step is to reproduce
on Linux with isolated test-key homes and capture the underlying decryption
error before choosing a correction. Pending native-comp dumping commits
remain separate from this investigation.

Follow-up: a Linux login is no longer required. The investigation now uses
an isolated GitHub Actions branch, `investigate/openpgp-linux`, checkpoint
`25a01b7`. Run `https://github.com/rayfdj/emaxx/actions/runs/34332448535`
builds GNU 30.2 and the current Emaxx source on Ubuntu 24.04, retains the
original four tests and test keys, and captures EasyPG debug output. Its
acceptance checks reject matching failures, skips and incomplete reports.
This diagnostic uses the publicly available upstream revision `636f166c...`
with the Linux capability set and records its actual identity; it does not
repin or certify the historical Linux frozen oracle. No decryption result is
claimed before the run completes.

### Completed Linux OpenPGP correction

The earlier environment-only explanation was incomplete. Linux run
34332448535 at `25a01b7` passed all four original tests in GNU and failed
all four in Emaxx, even though GPG reported successful decryption. Emaxx's
`set-buffer-multibyte` with non-t flag `to` converted ASCII to raw-byte
characters. GNU's `buffer.c:Fset_buffer_multibyte` preserves ASCII before
checking that flag. The corresponding generic branch is now corrected;
there is no GPG-specific runtime behavior or ciphertext normalization.

Run 34336177090 at `769577d` passes all four original tests in both editors,
in separate fresh fixture homes, for both short and historical long
fixture layouts. GPG used 59-byte sockets under `/run/user`; the proposed
long-socket explanation for the historical environment failure was not
reproduced and remains unproven. All 36 tracked GNU fixture hashes are
unchanged. No original test, oracle lock or corpus manifest was changed.

Validation covers 43 distinct focused Rust checks across the retained
40-check byte/audit run and the final 25-check audit/PTY run. All pass
serially without skips or ignores. The overlap is the 22 de-cheating
checks. Rustfmt, six Python acceptance checks and strict all-targets,
all-features Clippy pass; Clippy has zero warnings on Linux and Darwin.
The one Linux Clippy finding was an unnecessary mutable reference in an
existing PTY test helper; its pointer argument was corrected for both
platform signatures and all three affected PTY contracts passed.

Full provenance, binary hashes, negative controls and limitations are in
[the OpenPGP audit](openpgp-linux-investigation-2026-09-09.md). This closes
the four OpenPGP cases with actual Linux passes. The two known ERC startup
mismatches and one async-shell startup mismatch remain for dumping/startup
work. No subsequent native-comp commits were merged, and no fresh full
Rust or 7,883-test corpus result is claimed.

## 2026-09-09 Merge of main 85f0c28 into native-comp

*What came in.*  Main merged checkpoint 10 of this branch and added the
terminal and frame parity work: terminal states with their own
codings, parameters and keyboards, per-frame windows and face hash
tables, `pending_funcalls', the macOS forwarded-variable manifest,
and the test fixtures running native compilation as GNU's batch
child does (subr trampolines enabled).  The honesty audit's two
appended runs are kept in order; nothing else conflicted textually.

*Adapting the image code.*  A nilled frame loads with main's new
`FrameState' fields; a face's per-frame vectors (`frames') go with the
nilled frame; `ROOTS_RESET_AFTER_LOAD' lists `terminals' (nilled
terminal pseudovectors, `init_tty') and `pending_funcalls'
(`syms_of_keyboard_for_pdumper') in place of the fields main removed;
the native-comp dump control and the D11 control use the current
writer, loader and terminal table.  The root-inventory gate passed
unchanged on the merged tree, as did every image control and the
real dump with its load-back.

*The merged gate's one failure, and what the oracle said.*  The first
merged gate stopped in eval_04:
`lexical_onload_closure_can_define_a_function_in_a_dynamic_obarray'
signalled "Symbol's function definition is void: byte-code".  The
test binds `obarray' to a private obarray and `cl-letf's `require';
with main's fixture now leaving subr trampolines enabled, Emaxx's
`fset' of the primitive `require' called `comp-subr-trampoline-install'
(data.c:Ffset's hook), whose autoload loaded comp-run.el while the
private obarray was current, so its `byte-code' form named a private
symbol.  GNU does exactly the same: the oracle run of the program with
trampolines at their batch default answers `(void-function
byte-code)', and with `native-comp-enable-subr-trampolines' bound to
nil answers `("erc-lo2-mode" t)', the value the test expects.  The
test is about closures under a dynamic obarray, not trampolines, so it
now binds the trampolines off around its program, the state the
fixture had given it before.  Strict clippy on Linux also flagged
main's openpty control passing the window size as mutable; it is
passed by shared reference, as libc declares it.

*The second merged gate's failure: libgccjit's environment.*  With the
first test corrected, the gate stopped in eval_03:
`upstream_semantic_format_loads_with_complete_eieio_slots' failed with
"Wrong type argument: stringp, 1", and only when the whole group ran
in one process (alone it passed; each half of the preceding tests with
it passed; with the group's output uncaptured it passed).  Deleting
the cached `type-of' trampoline and running the two
cl-old-struct-compat-mode tests before it reproduced the failure every
time, and logging every `call-process' showed why: `semantic-gcc-query''s
gcc child exited 1 with "cannot execute 'cc1'", because its
environment carried `GCC_EXEC_PREFIX=/usr/lib/gcc/x86_64-linux-gnu/14',
libgccjit 14's prefix, while the system gcc is 13.  libgccjit's driver
exports that variable into the process environment during the
in-process trampoline compile the earlier tests trigger (main's
fixture runs with subr trampolines enabled), and the later test's
interpreter, made after it, read the process environment into its
`process-environment'.  GNU's children never see it: callproc.c builds
a child's environment from `process-environment', which
`init_callproc' took from the startup environment once, and the
oracle confirms gcc keeps working after a trampoline compile in the
same process.  Emaxx captures the environment when it makes an
interpreter (its test processes make many), so the compiler now
leaves `GCC_EXEC_PREFIX' as it found it around every in-process
compile; the three-test sequence passes with the trampoline compiled
in the process.  The diagnostic logging was not kept.

*Merge gate.*  Alone on the machine, on the merged tree with both
corrections: grouped gate run-1788932651828361257-12926, GROUPED GATE
PASSED (2579 tests, every group 0 failed), `cargo fmt --check' and strict clippy
exit 0 on the tree as committed.


## 2026-09-09 D12/D13/D16/D17: a process starts from the image

*What was built.*  The process-level loader (`pdumper_load'):
pdumper.c's result codes for a missing, short, foreign, incomplete or
other-build file, the refusal of a second load, and the point of no
return that rebuilds the objects, installs the symbols in the initial
obarray's order with their cells, aliases, flags, function cells,
plists and watchers, the built-in cells, the static root slots, the
root groups and the remembered scalars, and records the load for
`pdumper-stats'.  The startup path loads the image before every
init_* value as emacs.c:main does: `--dump-file' (both spellings,
consumed with its value, sorted at GNU's priority) or the
executable's own `<name>.pdmp'; a loaded process skips loadup and
keeps the image's `custom-delayed-init-variables' and eln load path
as emacs.c's `!initialized' branches do; `init_after_pdump_load'
applies the new process's environment lists, `command-line-args',
`exec-path' and TZ over the image and erases the pre-dump
`*Messages*'; `after-pdump-load-hook' runs through eval.c's
safe_run_hooks before the top level; a failed load is term.c's fatal
with emacs.c's reason strings and exit 1.

*What the first loads found.*  The mapatoms count of the restored
process was ten short: cl-macs's autoloaded symbols are interned in
GNU's obarray at dump time but Emaxx materializes an autoload lazily,
so the writer's obarray list omitted them and the installer wrongly
treated them as removed.  The writer now flags a symbol removed from
the initial obarray (`FLAG_UNINTERNED_FROM_OBARRAY') and the loader
interns every other symbol record; the restored obarray is a superset
of the writer's list, disclosed in the D12 row.  The first startup
from an image failed in `tty-set-up-initial-frame-faces' with
`terminal-live-p': the image's nilled frame was installed under its
dump-time id, which is the id of the live initial frame the new
process had just made, so the live frame became dead.  A nilled frame
or terminal now loads as a dead object with an id of its own (GNU's
init_frame_once_for_pdumper and init_tty make the live ones anew).
The second startup failed in normal-top-level with "Wrong type
argument: marker, marker<38>": the marker table is indexed by id, the
remembered next id (38) exceeded the markers the image carried
(markers dead at dump time are not written), and `make-marker' pushed
the new marker at the wrong index.  The marker, char-table and record
tables now fill the ids the image did not carry with empty entries
(records had a linear-scan fallback; char-tables had none and would
have failed on the first `make-char-table' after a load).  The unit
control's probes then matched, except for two of my own: a timestamp
and `custom-delayed-init-variables', which startup.el sets to `t'
after processing.  A hook control in a bare interpreter needed
`#'(lambda ...)' and `save-current-buffer' (the `lambda' and
`with-current-buffer' macros are subr.el's) and a buffer-local hook
value to exercise the local-then-global removal.

*Startup order.*  The load had first been placed inside the
reconstruction closure, after the constructor's process-derived
settings; GNU loads first and applies init_* afterwards, so the
image's dump-time `process-environment' (nil, as the writer leaves
it), `command-line-args' and `exec-path' would have stood.  The load
now precedes them and `init_after_pdump_load' is the second
application GNU makes; the D16 row lists what it covers and the two
candidates GNU has that Emaxx does not (`PATH_EXEC/emacs-VERSION.pdmp',
`--temacs').  The file-not-found reason was first written as "could
not open dump file"; emacs.c:dump_error_to_string says "could not
open file", and the CLI control compares the whole line.

*The field inventory.*  `interpreter_fields_are_carried_or_documented'
requires every field of `Interpreter' to be written, installed, or
listed in `FIELDS_NOT_CARRIED' with its reason; both documentation
gates now consult the constants rather than matching a quoted name
anywhere in the image code, and a unit test checks every documented
name is a declared field and that no field is documented twice.

*Timing.*  A batch process that evaluates `(kill-emacs 0)': 39.2 s
from the reconstruction, 1.0 s from the image (36023296 bytes), two
runs each, measured while a clippy build ran alongside; the loader's
own read of the image in the unit control is 0.63 s.  Nothing runs
from an image in the gate yet: producing the image once per build and
booting the fixtures from it is the next step (D16b).

*Verification.*  Focused: the pdumper controls, the anti-cheat gates,
the D12 boundary control, the D16 startup control, the hook control,
the CLI control, and a 213-test sweep over the marker, char-table,
record, frame, terminal and finalizer paths, all passing; `cargo fmt
--check' and strict clippy exit 0.  The full gate is run once, on the
tree with main 4311aa6 merged over this checkpoint, and its line is
recorded in the merge section that follows.

## 2026-09-09 Merge of main 4311aa6 into native-comp

*What came in.*  Five commits over the previous merge: the real Lisp
thread stacks resumed with the native runtime's ownership preserved
(`thread_context', the undo state's root visitor, the thread
continuation fixtures), ASCII preserved when a buffer is made
multibyte with a non-`t' flag, the warning-free PTY test setup, and
the OpenPGP investigation with its recovery record.

*Resolution.*  Four textual conflicts.  `buffer.rs': the buffer image
(this branch) and the undo root visitor (main) are both kept.
`eval.rs': `thread_context' joins the eval modules beside the image
exports.  The openpty control passes the window size as `&raw mut',
main's warning-free form, with the binding mutable again.  The honesty
audit keeps both appended sections in order, main's first.  One
non-textual adaptation: `install_record' reads the previous record's
type name by value, since main's `symbol_type_name' borrows the
record while the index is updated.

*The inventories after main's restructuring.*  Main made `Interpreter'
a shell (`state', the owned `InterpreterState' payload, and
`continuations', the suspended thread stacks) and added `stack_roots'
(the running Rust frames' registered Lisp roots) and
`new_thread_continuations', removing `fruitless_stepped_yields',
`thread_swap_boundaries' and `executing_thread_ids'.  The field and
root inventories now scan the shell and the payload as main's own
GC-root gate does; `stack_roots' is listed as reset after a load (the
main thread's stack scan, empty once the dump returns), the shell's
payload and the two continuation tables as not carried (no thread but
the main one exists at dump time), and the three removed fields are
gone from the list, which a unit test checks names declared fields only.

*Checking main's changes are all in.*  Every line main added since
the merge base (44 files) is present in the merged tree, checked
mechanically; the lines main has that the merged tree lacks are this
branch's own D08-era writer comments and control, the pre-D13 table
pushes and the old `DumpContext::new' signature, all replaced by later
checkpoints.  Main's OpenPGP gate unit tests pass on the merged tree
(6 tests); its Rust additions -- the multibyte-enabling ASCII control,
the byte-code prologue identity control, the continuation and stack
switch controls and the thread continuation fixtures -- are run below
with the image controls before the gate.

*Merge gate.*  Alone on the machine, on the merged tree as committed:
grouped gate run-1788954331026622077-28040, GROUPED GATE PASSED (2600
tests across the ten groups, every group 0 failed, the integration
binaries included), `cargo fmt --check' and strict clippy exit 0 both
before and after the run.  This is the one full gate of checkpoint 15
and the merge together.

## 2026-09-10 D16b: the harness boots from a shared loadup image

*What was built.*  With `EMAXX_FIXTURE_IMAGE_DIR' set, the first
startup that finds no image builds the loadup state as before and
then dumps it -- loadup.el's own final act, `(dump-emacs-portable
"emacs.pdmp")', performed by the Rust startup after the
reconstruction -- and every later startup in any process loads it as
emacs.c loads emacs.pdmp.  The file is named by this build's
fingerprint and the installation Lisp tree, so a rebuilt binary or
another tree gets its own file and the loader's fingerprint check
refuses a stale one (it is then removed and rebuilt); an exclusive
`flock' on a sibling lock file serializes the processes that check
and build it, and the dump lands under a temporary name renamed into
place so no process sees a partial image.  The grouped gate sets the
directory for its test processes; the compat harness passes it to
every emaxx runner it spawns, after its EMAXX_* strip, so the
corpus's per-file boots start from the image.  Production runs
without the variable are unchanged: `--dump-file' and the
executable's `<name>.pdmp' remain the only images they consider.

*What changed in the tests.*  Three controls asserted that a directly
initialized Emaxx reports no dump (`pdumper-stats' nil).  A process
the harness started from its image reports that file, as GNU's emacs
reports emacs.pdmp, so those assertions now follow
`pdumper_load_record': nil without a load, the image's name with one.
No test's Lisp answers changed: the D12 boundary control, the D16
startup control and the new fixture control compare an image-booted
process against a reconstructed one probe by probe, and the full
gate's pass count is the whole-suite check.

*What the first image-enabled gate found.*  It stopped in two
minutes: eval_02's tests found `backward-sexp', `beginning-of-defun-raw'
and `move-to-left-margin' void.  A plain CLI dump and load showed the
whole of lisp.el's functions void while simple.el's and subr-x's were
fine, and instrumenting the installer showed the image holding two
symbol records named `forward-sexp' with the same id: the initial
obarray's, with its byte-code function, and a second with value `t'
and no function, installed over it.  The second is a symbol interned
in a private obarray (Emaxx keys such a symbol by an internal name --
the Lisp name, the obarray's id and a serial -- and the loadup state
holds one named like the lisp.el command); the writer recorded it as
interned in the initial obarray under its Lisp name, the D09 row's
disclosed shortcut, and the loader resolved it to the standard symbol.
The writer now gives such a symbol lisp.h's SYMBOL_INTERNED and
appends its internal name after the watchers; the loader re-creates
it under that name, so its obarray's lookups find the same object,
and the installer keeps it out of the initial obarray.  The D16 and
fixture controls now probe lisp.el's functions, and a unit control
round-trips a private obarray whose symbol shares `car''s name.  The
D12 boundary control had not caught it because its probes never
named a lisp.el function, and the D16 control's did not either: both
gaps are closed by the new probes.

*What the second image-enabled gate found.*  eval_01's edmacro test
parsed `<<goto-line>>' as `[execute ...]' instead of `[?\M-x ...]':
`key-binding' answered nil for `M-x', `ESC x' and `C-x C-f' while
`lookup-key' on the global map answered, and `keymap-parent' of the
local map was nil.  Emaxx keeps an identity-bearing facade record
behind each keymap list it makes (parent, bindings, char-table and
the public view), found from the list through a view-to-record index;
the index is derived state and is not written, and the records
themselves were reachable from nothing the writer visits (Lisp holds
the lists), so 7 of them came through and the rest were lost.  The
records are now a root group of their own, disclosed as not GNU's,
and the loader rebuilds the index from their views; the CLI probes
and the startup controls' new key-binding probes answer as the
reconstruction does.  The first attempt rebuilt the index from
records typed by the `keymap' symbol and found the same 7: the
facade's kind is `RecordKind::Keymap', not a type tag.

*What the third found.*  `defvaralias' on `fill-column' after a load
said "Don't know how to make a buffer-local variable an alias" where
eval.c says "Cannot make a built-in variable an alias": a
DEFVAR_PER_BUFFER variable is SYMBOL_FORWARDED in GNU whether or not a
buffer has its own value, and Emaxx's cell keeps LOCALIZED beside
FORWARDED for it, which the writer's one redirect field encoded as
the localized kind.  The record now carries GNU's redirect (forwarded
first) and an Emaxx bit for the second flag, which the installer
restores.  The eval groups of that gate ran in 130, 137, 297, 74 and
300 seconds against 840, 884, 1187, 203 and 2630 for the previous
gate's, before the primitives group stopped on this.

*What the fourth found.*  Every library group passed (2602 tests in
27 minutes, against 2h20m for the previous gate's 2600), and the
integration stage stopped in tests/cli.rs: a test that compares the
process's stderr byte for byte with GNU's saw the fixture dump's
"Dumping fingerprint", "Dump complete" and byte-count lines, printed
by the first process of the run as it built the image.  GNU prints
those from a build's dump and a failed load, never from a session, so
the harness's dump and its load attempt hold them back while a guard
lives; `dump-emacs-portable' called from Lisp prints as pdumper.c does.

*Honesty.*  This is the harness's convenience, not GNU's flow:
loadup.el performs the dump in GNU, Emaxx's startup performs it, and
only under the variable.  The loadup state an image holds is the one
the reconstruction produces in the same environment (the same
`EMAXX_DUMP_SOURCE_DIRECTORY' or sibling tree, which the file name
hashes); the per-run isolation the harness applies (the test
directory, `source-directory', the eln cache) is applied after the
load in both paths, as startup.el and the runner's `--eval's apply it
in GNU.

*Gate.*  Alone on the machine, with the shared image on: grouped gate
run-1789024341050155106-30784, GROUPED GATE PASSED (2602 library tests
across the ten groups and the integration binaries, every group 0
failed), `cargo fmt --check' and strict clippy exit 0 before and after.
Wall time 07:12 to 07:52, forty minutes, against 2h20m for the gate of
checkpoint 15; the library groups took 41, 11, 148, 138, 300, 80, 323,
9, 682 and 11 seconds.

## 2026-09-10 Merge of main 08a4004 into native-comp

*What came in.*  Main merged native-comp 7a87362 (the state before
checkpoint 15) and validated it over seventeen commits: the completed
startup image control became a context-and-capability boundary
control (on Darwin the ordinary startup loads native functions and the
writer refuses them, the D14/D15 gap this branch's next checkpoints
close), `tools/serial_grouped_gate.py', the Linux oracle built with
the exact reference native ABI configuration, the dead-thread GC
release observed after the GNU loader returns, the public native
diagnostics, and the OpenPGP workflow.  The remote history was
rewritten in the meantime (every commit re-hashed with its tree,
author and date kept); the local branch was moved onto the rewritten
native-comp tip without a file changing, which a tree diff confirmed
empty.

*Resolution.*  Two textual conflicts, both kept in main's shape: the
buffer image follows the undo root visitor in buffer.rs, and the
honesty audit keeps its sections in order with the record of the
85f0c28 merge once.  The line check found 30 lines main has that the
merged tree lacks, all this branch's own pre-checkpoint-15 code that
main received through its merge of the older native-comp and that
checkpoints 15 and 16 replaced (the old loader's symbol creation, the
old dead-frame install by id, the old record install, the old
inventory match, the old handover pointer).

*The merged gate's one failure.*  The primitives group stopped in
`module_load_validates_real_libraries_without_fabricating_the_gnu_value_abi':
its C probe did not compile.  Main replaced this branch's
`ProcessEnvironmentGuard' (which put GCC_EXEC_PREFIX back after every
in-process libgccjit compile) with callproc.c's own approach, a
startup snapshot of environ that every interpreter's environment
lists are built from; the host environment itself now keeps what the
GCC driver exported, as GNU's does, and the test spawned the system
cc with `Command' inheriting that host environment (an earlier test in
the same two-worker process had compiled a trampoline).  The probe's
compiler now gets the startup snapshot, as a GNU child built from
`process-environment' would, and `init_after_pdump_load' takes the
same snapshot for the lists it rebuilds after a load.  Main's
serial gate had not met this because it runs one worker.

*Merge gate.*  Alone on the machine, on the merged tree as committed,
with the shared image on: grouped gate run-1789029688488327082-23803,
GROUPED GATE PASSED (2602 library tests across the ten groups and the
integration binaries, every group 0 failed), `cargo fmt --check' and
strict clippy exit 0 before and after; 08:41 to 09:20.

*The corpus from the image.*  The frozen corpus with
`EMAXX_FIXTURE_IMAGE_DIR' set (artifact
`frozen-1789032315130318069-30301', 09:20 to 10:25, one hour and five
minutes where the recorded run took five hours and forty-one; a
runner's setup 1.1 to 1.6 s where it was 23): 7873 / 7883 matching,
10 mismatching, against the recorded 7869 / 7883.  Ten outcomes came
right (erc, server, simple and the two thread files, main's thread
work and the earlier server fixes), and one file regressed:
gv-tests.el, 0 to 6.  Its tests write a file into a temporary
directory and spawn a child emacs there to byte-compile it; under
the image the child looked for the file in the harness's working
directory: the child was image-booted, and the `*scratch*' it
started in had the dumping process's `default-directory'.
buffer.c:init_buffer, which emacs.c runs after the load, selects
`*scratch*' and gives it and the first minibuffer the new process's
working directory (a separator appended, "/:" in front when a handler
would claim it); the other dumped buffers keep their dump-time
directories, as GNU's do.  That is now part of the after-load
initialization; the CLI control dumps in one directory and loads in
another and reads both buffers' directories.  The direct
reproductions had missed it because they ran the child in the
directory the image was dumped from.
gv-tests passes through the harness with the image on (8 / 8), and
the gate on the tree with this correction: grouped gate
run-1789037478153525215-14024, GROUPED GATE PASSED (2602 library tests
and the integration binaries, every group 0 failed), fmt and strict
clippy exit 0, 10:51 to 11:30.

## 2026-09-10 Merge of main 56dd60a into native-comp

*What came in.*  Main merged checkpoint 15 and validated the process
loader over eleven commits.  Its corrections match what this branch
found on its own in the meantime -- init_buffer, init_cmdargs and
init_callproc reapplied after the load, the environment lists from
the startup snapshot -- and add keyboard.c's safe_run_hooks details
(`inhibit-quit' bound, removal by function identity through `set' and
`set-default' so watchers see it, the global value read when a local
list reaches `t'), a fallible `init_after_pdump_load', the image
controls renamed to distinguish a supported round trip from the
native-image refusal, and `tools/dumped_startup_gate.py', the
positive acceptance gate that builds an image from an ordinary
startup and runs three original ERT tests from it in both editors.

*Resolution.*  Four textual conflicts: `init_after_pdump_load' is
main's version, the superset of this branch's follow-up (which had
added the same init_buffer port a few hours earlier); the D16 ledger
row keeps this branch's D16b text; the CLI control keeps main's
native-limit branch with this branch's change of working directory
between the dump and the load and its directory probes; the honesty
audit keeps its sections in order with the 85f0c28 merge record once.
The line check found 26 lines main has that the merged tree lacks,
all checkpoint-15-era lines that checkpoint 16 replaced (the old
symbol creation, the old keymap-cache reasons, the old nil
expectations for `pdumper-stats').

*Merge gate.*  Alone on the machine, on the merged tree as committed,
with the shared image on: grouped gate run-1789040516032552015-27701,
GROUPED GATE PASSED (2606 library tests across the ten groups and the
integration binaries, every group 0 failed), `cargo fmt --check' and
strict clippy exit 0 before and after; 11:41 to 12:20.

*The corpus from the image, on the merged tree.*  Frozen corpus with
`EMAXX_FIXTURE_IMAGE_DIR' set, artifact
`frozen-1789043156837100912-1662', 12:24 to 13:27: 7879 / 7883
matching, 4 mismatching, against the recorded 7869 / 7883 (14).  The
four are the OpenPGP decryption platform residual of mml-sec-tests
(`mml-secure-en-decrypt-1' to `-4', recorded above and in main's
audits); erc, server, simple and both thread files are at 0, and
gv-tests is back at 0 with the init_buffer correction.  One hour and
three minutes for the run that took five hours and forty-one.

## 2026-09-10 D14/D15: native compilation units and native functions in the image

*What was built.*  The last two open rows of the dump program.  The
writer no longer refuses a native compilation unit or a native
function: a unit is written as pdumper.c:dump_native_comp_unit writes
it (the Lisp fields with the file as the pair `load--fixup-all-elns'
made of it, the documentation vector nil, nothing for the handle, a
late relocation entry), a native function as dump_subr writes one
(no function pointer, the Lisp fields, the symbol name and the C
name GNU keeps as C strings, a very late relocation entry).  The
registry keeps each function's C name (Lisp_Subr.native_c_name) from
`comp--register-subr' on.  The process-level loader runs the two
phases pdumper_load runs after the Emacs relocations: the late one
reopens each unit (comp.c:load_comp_unit with loading_dump: the file
resolved against the executable's directory, installed or local
decided once by the first unit, `fixup_eln_load_path', dlopen, the
ABI hash and relocation symbols checked, the runtime pointers
linked, the data and impure relocations filled from the dumped data
vectors, no top-level run, the unit registered under its resolved
file), the very late one resolves each function by its C name in its
unit and, for an anonymous lambda, replaces the `lambda-fixup'
placeholder in the impure relocations and enters the function in the
unit's fresh GC guard.  The harness image dumps under loadup.el's
`load--bin-dest-dir' and `load--eln-dest-dir' (the binary's
directory and the source tree, Emaxx having no installation layout),
so a startup that loaded native units -- the Darwin case -- dumps
them as `make' dumps GNU's preloaded ones.

*What is disclosed.*  A unit whose file is still a string is refused
with GNU's "trying to dump non fixed-up eln file": a session dumps its
native units only under the two loadup variables, and a form
`native-compile' compiled into `temporary-file-directory' cannot be
fixed up there in GNU either (the ten-character `substring' of the
short directory name signals).  The documentation vector is written
nil without clearing the dumping process's own slot.  A dumped unit
the process already holds is refused where GNU's eassert fires (GNU
never loads a dump into the process that wrote it; the in-process
controls drop the writer first).  A failure in the native phases is
the startup's fatal "could not load dump file", where GNU's `error'
unwinds a process that has no handler yet; dlopen's failure is
preceded by GNU's "Error using execdir" line on stderr.  The function
pointer lives in the registry, not in the object, and the relocation
entries are consumed by kind rather than applied to mapped memory.
The template interpreter of `test_support' cannot be cloned once a
process holds native units (the runtime's permanent root ranges), so
an image-booted test process with native units reconstructs each
interpreter from the image instead; no Linux fixture has native
units, so the Linux gate is unchanged.

*Controls.*  `native_units_and_functions_round_trip_through_the_image'
compiles a file into the eln cache (a named function with a
docstring, a lambda returned by a function), refuses the dump with
the two loadup variables bound to nil (a process that started from
the harness image carries the values the image was dumped under, as
GNU's emacs.pdmp carries the build's; the gate found the control
dumping where it expected the refusal) and asserts the empty output,
dumps under them, drops the
writer, loads the image into a fresh process-state interpreter and
calls both functions, checks the arities, the unit's resolved file on
disk and in `comp-loaded-comp-units-h', the lazily loaded
documentation and a wrong-number-of-arguments, then moves the unit
away and asserts the load fails naming it.  The three startup
controls (in-process, batch startup, CLI) no longer branch into a
"native image refused" exit: they dump under the fixup when the
startup loaded native code and compare the native function count,
the first native function and its arity, and a preloaded call
(`string-trim') across the round trip; on Linux, whose startup loads
no native units, they run as before with a count of 0.  The
in-process control now takes every expectation from the writer
before dropping it, and the batch startup control its reference
answers before the restored startup, since a loaded image reopens the
units a live session in the same process would still hold.  The
Linux run of the three controls exercises only their count-of-zero
branch; the Darwin run, where the startup loads the preloaded units,
is the receipt still to be taken.

*What the gate found beside it.*  The second full run failed one
unrelated test once: `process_attributes_follows_sysdep_procfs' saw
`(user-login-name)' differ from the `user' field of the child's
attributes, both strings, in a process booted from the harness image
under the gate's load; the test passed in the first run and three
times alone.  The cause is a divergence from editfns.c:init_editfns,
which emacs.c:main runs once in every process (after load_pdump in a
dumped one) to set `user-login-name', `user-real-login-name' and
`user-full-name' from the environment and the account database, with
"unknown" where no account answers.  Emaxx synthesized the three on
every reference through `builtin_var_value', with a fabricated
"user" where a lookup failed, so one failed `getpwuid_r' under load
changed the answer between two references.  `Interpreter::init_editfns'
now computes the three once, at startup and in
`init_after_pdump_load', and keeps them in their variables; the
fallbacks are GNU's "unknown".  Control:
`init_editfns_computes_the_user_names_once_per_process' (LOGNAME set
at startup names the user after the variable is gone; the real name
is the account's; the process's initialization computes them again).

*Gate.*  Alone on the machine, on the tree as committed, with the
shared image on: grouped gate run-1789054270916670663-3315, GROUPED
GATE PASSED -- 2608 library tests across the ten groups (batch 49,
compat_runtime 84, eval_01 351, eval_02 284, eval_03 320, eval_04 251,
eval_05 351, lightweight 414, primitives 448, tty 56, every group 0
failed) and 68 in the binaries and integration tests; `cargo fmt
--check' and strict clippy exit 0 before and after; 15:31 to 16:20.
Two earlier full runs on this checkpoint's tree each failed one test:
the first the new native control (the refusal branch, corrected as
recorded above), the second the process-attributes test (the
init_editfns divergence, corrected as recorded above); a third run
was stopped for the correction.  Main's `8b6ff8a' is the merge of
checkpoint 16 into main with a tree identical to `63e6a42'; it is
merged after this commit for the ancestry, and the merged tree is
this gated tree.

*The Darwin receipt, first part (2026-09-11).*  On the Mac, from the
checkpoint 17 tree, `batch_startup_image_round_trip_or_explicit_native_image_limit'
passed: temacs with the preloaded native units dumped under the
fixup, a process started from the image reopened the units, resolved
3845 native functions and answered the ordinary startup's answers,
the native count and `string-trim' included.  The CLI control's
restored process did the same (3845, `a') and failed only on the
test's own expectation of its working directory: the process answers
getcwd's physical path, `/private/var/...' where the temporary
directory is named through Darwin's `/var' link (GNU's init_buffer
answers the same there, PWD not being updated for the child), and the
expectation now canonicalizes the directory it compares against.
The two in-process controls, run three to a process in parallel
threads, failed while building their loadup states ("unknown native
cons address" in `load("emacs-lisp/debug-early")'): the saved-unit
word of a shared object is process-global (D18), which is why the
grouped gate runs the image consumers serially; their serial run is
the receipt still to be taken, with the frozen run from an image
carrying the units.

*The Darwin receipt, second part (2026-09-11).*  The two in-process
controls, run serially on the Mac, failed on two expectations of the
Linux-written tests, neither on the loader.  The startup control's
comparison of the RememberedScalars group found `next-record-id' 87
higher in the restored process than in the writer: the late phase
gives each of the 87 reopened units a fresh `lambda_gc_guard_h'
(dump_do_dump_relocation's Fmake_hash_table), one record each, after
the remembered allocator was restored; the control now compares the
group without that entry and requires the difference to equal the
unit count.  The native round trip's own unit could not be reopened:
dump_do_dump_relocation decides installed-or-local once, by the first
unit, and on the Mac the first unit is a preloaded one whose installed
path under the source tree exists, so the control's cache unit was
sought there too and not found (GNU has the one-state rule; loadup
never dumps a cache unit beside the preloaded ones).  The control now
names an eln destination holding no units, so every unit resolves
through its build path.  The Linux run of both remains at a unit
count of zero.

## 2026-09-11 Checkpoint 18: the harness's checkout roots, the per-run floor, the cc-mode hot paths

*What prompted it.*  The Mac frozen run of checkpoint 17 showed three
things worth a patch before a re-run: a per-run floor of about 25 ms
in every ERT batch process, `csharp-mode-tests' at 361 times GNU's
time (one indentation test, 86.6 s on the Linux gate build against
GNU's 0.06 s), and `elisp-mode-tests' failing on nothing but the two
editors' checkout roots printed in a failure message.

*The harness.*  `compat-harness' now normalizes, before it compares
the two reports, each runner's isolated checkout root and the shared
temporary directory to `<checkout>' and `<tmp>', in the three
spellings a test can print a path in: as given, resolved through the
file system (Darwin's `/var' is a link to `/private/var', which
`file-truename' and getcwd answer), and downcased (a test comparing
case-folded names prints them so), longest spelling first so a
resolved root is not left half replaced by its unresolved prefix.
This is a harness rule, not editor behavior: it makes the two
editors' different working directories equal in the comparison and
nothing else; a path that differs in any other way remains a
divergence.  Control: `path_spellings_cover_resolved_and_downcased_forms'.

*The compiled-regexp cache handed out clones.*  The largest cost
found was not in cc-mode's patterns but in every regexp call: the
thread-local cache of compiled patterns returned a clone of the
compiled object, and a clone of a regex-automata regex (fancy-regex's
delegates included) starts with an empty cache pool, so every search
through the clone determinized its pattern's lazy DFA from nothing.
The cache now hands out an `Rc' to the one compiled object.  Measured
under `looking-at' on the Linux gate build, warm: `\(?:\sw\|\s_\)+'
25.6 to 8.1 us a call, a literal 7.9 to 7.0 us, `[[:alnum:]_$]\{,8\}'
305 to 8.9 us, cc-mode's 954-character keyword alternation 75 to 13 us
(GNU: 1 us for each).

*Large bounded repeats.*  cc-mode's `c-identifier-key' repeats a
Unicode class with `\{,1000\}'.  GNU's regex.c runs `X\{m,n\}' as a
counted loop (succeed_n, jump_n); the regex crate unrolls it into n
copies of X, which for that class cost hundreds of milliseconds to
compile and, matched through the unrolled automaton, 190 ms for one
`looking-at' (4.8 s the first).  The translation now wraps a bracket
expression under a bound of 16 or more in an atomic group, `(?>[..])
{0,1000}': one character with nothing to backtrack into, so the
language is unchanged, and a body fancy-regex runs itself compiles
as its counted loop (RepeatGr) instead of the delegate's unrolling.
Only a bracket expression immediately before the interval is
wrapped; a group, a literal, an unbounded `\{n,\}' keep the regex
crate's form.  Measured on `[[:alpha:]_@][[:alnum:]_$]\{,N\}': the
unrolled form compiles in 3 ms at N=4, 7.5 ms at N=16, 12.7 ms at
N=31 and matches in 9 us warm; the loop compiles in 2 ms at every N
and matches in 11 us (13 us at N=1000).  The match data of seven
bounded patterns (greedy, lazy, exact count, nested, with a
backreference) is the same as GNU's, pinned by
`large_bounded_repeats_over_a_bracket_expression_become_counted_loops'.
Not GNU: a pattern whose bounded repeat is looped has no
linear-boundary prefilter (the regex crate cannot parse the atomic
group; the coarse prefilter is an optimization, not a semantic).

*Syntax renderings key on the tables they read.*  A pattern with a
syntax class was keyed on one generation bumped by a write to any
syntax table (and any table that is neither a syntax nor a category
table), so a mode creating or writing tables of its own recompiled
every such pattern under the current table: 465 recompilations in
the csharp run, the largest patterns five times each.  Every char
table now carries a stamp taken from one process-wide counter when
it is made and again at each write through `find_char_table_mut';
the compiled-regexp key, the rendered syntax classes, the
syntax-segment cache and the mutable-entries answer key on the
signature of the current table's chain (the table and its parents,
each with its stamp).  A write to a table outside the chain changes
nothing; a write to a parent, or a change of a parent link, changes
the signature.  The category and case generations remain as they
were.  Disclosed and corrected beside it: the thread-local cache
outlives an interpreter, and a table of a later interpreter, or one
an image installs, under the same id could have met a stale entry
keyed on the same id and generation; the stamps are unique across
the process.  Controls: `syntax_class_rendering_survives_writes_to_tables_outside_its_chain'
(an unrelated table's write does not re-render; a parent's write
does) beside the existing invalidation and cache-hit controls.

*Function resolution keyed on the definition generation.*  The two
source-function resolution caches were keyed on `definition_generation',
which every `setcar', `setcdr' and plist write bumps, so cc-mode's
cons writes emptied them; they key on a function-binding generation
bumped where a function cell is bound, rebound or voided
(`note_function_binding_changed': the definition generation still
moves with it, the macro verdicts depending on both).  `looking-at'
after a `setcar' each call: 368 to 29 us then, 11 us now.

*`parse-sexp-lookup-properties'.*  With the variable set (cc-mode
sets it), every syntax-dependent match encoded the haystack for
`syntax-table' properties by walking the whole buffer; the encoding
is now skipped when no `syntax-table' property lies in the searched
range (144 to 29 us for a 2.8 KB buffer, 9 us now; with one such
property in range the encoding still runs, 132 us).

*The result.*  The csharp indentation test runs in 1.87 s on the
Linux gate build, 86.6 s before this checkpoint, GNU 0.061 s: 31
times GNU where it was 1400.  `looking-at' remains at 7 to 13 us a
call against GNU's 1 us; that floor is the per-call dispatch and
argument path, not the engine.

*The per-run floor, measured.*  On the Linux gate build booted from
the harness image, against GNU on the same machine: `ert-run-tests-batch'
on a trivial test 66 ms (GNU 9 ms), of which `mapatoms' over the
17,700 interned symbols costs 78 ms a pass (GNU 2 ms; the enumeration
now yields the interned symbols rather than their names, kept though
it gained nothing measurable: the cost is the per-symbol call, R02c's
dispatch by name); `garbage-collect' 132 ms (GNU 13 ms);
`ert-run-test' 0.33 ms a run (GNU 0.03); `message' 36 us (GNU 16 us).
The image load, `pdumper-stats' load-time, was 1.73 s (GNU 0.013 s, a
mapping): the loader's tables, one per image offset, hashed with
SipHash, were a third of it.  They hash by one multiplication now and
are sized from the header: load-time 0.45 to 0.51 s in three runs.  A
batch process that only exits takes about 1.3 s of wall time under
strace, GNU 0.05 s: 0.22 s before the image is opened (the reading and
SHA-256 of the 22 MB executable for the fingerprint check, where GNU
embeds its fingerprint at link time, and the interpreter's
construction), the load, and 0.6 s of startup after it.  The rest of
the load is the materialization of every object (a fifth of the
boot's CPU is the allocator's fresh pages) and the reading of the
image; a collection runs during startup.  Each is recorded in the ledger's D20 row;
none is closed.

*cperl-mode and perl-mode.*  `cperl-mode-tests' 23 s against GNU's
1.1 s, `perl-mode-tests' 12.8 s against 0.9 s, and in both one test,
`cperl-test-bug-37127', at 13 s and 10.8 s (GNU 0.84 s).  The test
advises `message' through `ert-with-message-capture'; an advised
primitive needs a native trampoline; `ert-run-tests-batch-and-exit'
redirects the eln cache to a fresh temporary directory, so the
trampoline is compiled in every such process; and comp.el's
`comp--final' runs the C side of a compilation in a child Emacs
unless the compilation is a batch or asynchronous one.  Emaxx
follows the same rule with the same comp.el, and its child is a full
boot: one trampoline cost 2.2 to 3.5 s before the loader change and
1.9 to 2.7 s after it (GNU 0.21 s), the libgccjit work itself under
1% of it.  Run directly,
without the redirected cache, the test takes 0.07 s.  The trampoline's
cost is the boot's; it is not corrected here.  `cperl-test-bug-10483'
fails on this Linux build: it starts a child Emacs under a two-second
timeout, and an Emaxx child's boot and indentation exceed it.

*Observed, not investigated.*  `elp-instrument-list' over a list
containing the autoloaded macro `cl-loop' after `(require 'comp)'
raised GNU's "ELP cannot profile the function" in GNU and not in
Emaxx; `elp-profilable-p' answers nil for it in both when asked
alone.  Whatever loads `cl-macs' earlier in Emaxx is not identified.

*Gate.*  Alone on the machine, on the tree as committed, with the
shared image on: grouped gate run-1789099214491500278-2818, GROUPED
GATE PASSED -- 2610 library tests across the ten groups (batch 49,
compat_runtime 84, eval_01 351, eval_02 284, eval_03 320, eval_04 251,
eval_05 351, lightweight 414, primitives 450, tty 56, every group 0
failed) and 69 in the binaries and integration tests; `cargo fmt
--check' and strict clippy exit 0 before and after; 04:00 to 04:49.
The focused run before it: 186 of the regexp, syntax-table, macro,
function-binding, image and startup controls passed and the new
chain-signature control failed once on its own use of subr.el's
`make-syntax-table' in a bare interpreter, rewritten to the primitives
it wraps; the harness control passed.

## 2026-09-11 Checkpoint 18b: the Mac frozen run's failures and slow files

*What prompted it.*  The Mac frozen run of checkpoint 18 reported
three Emaxx-only failures (`bytecomp-tests--not-writable-directory',
`bytecomp-tests--target-file-no-directory', `multi-test-files-simple'),
`context-menu-map-remove-consecutive-separators' later, two
environment mismatches (`shr-test/zoom-image' failing on the oracle
only; five eglot rust-analyzer tests failing with different condition
types on both sides), twenty-six files at 10 to 39 times GNU's test
time, and a stall after `ps-mode-tests'.

*The lock a byte compiler cannot create.*  Reproduced as an
unprivileged user (this container's root can write anywhere): the
byte compiler writes into a directory whose mode is 0500 (Bug#44631's
case), `write-region' locks the file first, and `lock-file' signalled
"Permission denied (os error 13)" when the lock file could not be
created there.  filelock.c:lock_file ignores the errno of a lock it
cannot create ("FIXME: This ignores errors when lock_if_free returns
an errno value"); the file stays unlocked and the write goes on.
Emaxx now does the same, and the test passes as `nobody'.  Beside it,
`make-temp-file-internal' reported a failed creation as a bare `error'
with Rust's text; it is report_file_error's `(file-missing "Creating
file with prefix" "No such file or directory" PREFIX)' now, with
"Creating directory with prefix" for a directory, as
Fmake_temp_file_internal names them.  Controls:
`lock_file_ignores_a_lock_it_cannot_create' (a lock under a plain
file, ENOTDIR for root too, is not an error and the write reaches the
file), `make_temp_file_internal_reports_a_failed_creation_as_a_file_error'.
The other three Emaxx-only failures pass here as root and as `nobody';
their Mac conditions are not in hand (the report's per-file
`comparison.json' carries them) and they remain open.

*A timed wait on an exited process.*  `(accept-process-output PROC
10)' waited its ten seconds after PROC had exited: the exited-process
return was applied only to the wait without a timeout.
process.c:wait_reading_process_output does not wait for output from a
process that is not running whichever the timeout ("Just read
whatever data has already been received"), so a loop
`(while (accept-process-output proc 10))' ends with the process.  Every
erc test that runs a child Emacs paid the ten seconds (`erc--find-mode'
12.8 s to 2.7 s, `erc--essential-hook-ordering' 12.6 s to 2.3 s), and
python-tests, which polls its inferior Python with timed waits, is
the file the Mac run stalled in.  Control:
`accept_process_output_with_a_timeout_returns_once_the_process_has_exited'.

*Backward regexp search.*  `re-search-backward' enumerated every match
start from the beginning of the accessible text and kept the latest,
a whole-buffer scan per call: 2.8 ms for a 6 KB buffer, 83 ms a call
in track-changes' buffers, 75 ms for fill.el's one call per line,
GNU under a microsecond.  re_search_2 with a negative range tries the
starts from point downward and stops at the first that matches; the
starts are now visited through windows growing backward from point
(64 characters, then four times as many), the latest start in a
window that holds any being the answer since every later start was
enumerated with it.  The same twelve patterns over a buffer of 300
`a', a `b' and 100 `c' answer as GNU does (start, match-beginning,
match-end), pinned by `backward_regexp_search_takes_the_latest_start_as_gnu_does';
6 KB buffer: 2849 to 19 us.  `subr-string-fill' 0.41 to 0.13 s,
`track-changes-tests--random' 18.1 to 7.2 s (GNU 0.67 s).

*Positions in a large haystack.*  Every buffer search converted point
to a byte offset and the match back to a position by walking the
haystack from its start: 410 us per `re-search-forward' in a
12,000-line dired listing (`ls-lisp-test-bug70271': 105,534 calls,
43 s).  The haystack keeps a sampled byte-to-character index now,
one entry per kilobyte, and the forward and backward searches and
their match data convert through it (an ASCII haystack needs no
table).  The match data of forward and backward searches over a
buffer mixing ASCII, Latin and CJK text equals GNU's; the dired test
68.5 s to 26.8 s (GNU 0.47 s), the rest of it below.

*Category patterns.*  A `\\c' pattern was keyed on the cons-mutation
generation, so fill.el's `[ \\t]\\|\\c|.\\|.\\c|' recompiled after every
`setcar' (89 ms each; the first compile 167 ms).  Category sets change
through `modify-category-entry', which writes the table (category.c
stores a fresh set), so the category generation covers them and the
pattern keys on that alone; the class renderer reads the table's
resolved ranges (54,000 for the standard table) once instead of
walking the quarter-million-entry write log with a lookup per
boundary: 167 to 120 ms for `\\c|', 22 to 8 ms for `\\cl', and no
recompilation after a cons write.  Not GNU: the first compile of
`\\c|' remains 120 ms (GNU 12 us), the class being handed to the regex
crate as tens of thousands of ranges.

*The compiled-regexp cache.*  icalendar's real-world test cycles
through 351 distinct patterns; with 256 entries every call recompiled
(`string-match' 69 us a call, `looking-at' 500 us).  The cache holds
1024 now (search.c holds twenty and compiles in microseconds; a
compilation here is tens of microseconds, a category class tens of
milliseconds): `icalendar-real-world' 15.3 s to 0.69 s, the file 35 s
to 3.5 s (GNU 0.36 s).

*Loading byte-compiled libraries.*  `org.elc' loaded in 1.10 s (GNU
0.09 s), `gnus-sum.elc' in 1.66 s (0.08 s): a redefinition scanned the
function list for the name and shifted its tail (`set_function_binding'
a tenth of a load), and each definition walked the file's
`current-load-list' for a duplicate.  A name's entry is found by
position and replaced in place; the load-history walk is gone, since
LOADHIST_ATTACH conses every definition, a repeated one included (a
file defining `f' twice lists `(defun . f)' twice in GNU, and Emaxx
listed it once: a divergence from the compat-3125 correction, now
removed).  `internal--define-uninitialized-variable' attaches its
symbol as Finternal__define_uninitialized_variable does, which Emaxx
omitted: a `defcustom' was missing from the file's load history.
`org.elc' 425 ms, `gnus-sum.elc' 755 ms, `erc.elc' 261 ms after; the
remaining five to nine times GNU are the interpretation of the
top-level forms, the load-path probing with its file-name-handler
matching, and the reader.  Control:
`load_history_lists_a_repeated_definition_twice_as_gnu_does'; the
load-history of a file with fifteen definition forms (defun, defmacro,
defvar, defcustom, defconst, defalias, defsubst, cl-defstruct,
define-minor-mode, defvar-local, defgroup, defface, autoload,
provide) is GNU's exactly.

*Measured, not corrected.*  The per-call floor decides the rest of the
list: bindat's signed-integer test (7.2 s, GNU 0.7) is `logand' and
`ash' at 3 us a call and `mapcar' at 10 us; comp-cstr (7.7 s, GNU 0.4)
and electric-tests (10.3 s over 874 tests, GNU 0.75) are ordinary
Lisp at that floor; mule's `ucs-names' tests (16.8 s, GNU 1.2) call
`get-char-code-property' over the code space; ls-lisp's remaining
27 s are `put-text-property' and `add-text-properties' at 0.7 ms a
call in a buffer of 12,000 intervals, `sort' with a Lisp predicate
(4 s for 13 calls), `indent-to' at 234 us and `time-less-p' at 7 us;
gnutls-tests (57 s, GNU 0.85) are `gnutls-ciphers' and
`gnutls-symmetric-encrypt' at 1.1 to 1.2 ms a call and the test's own
hex comparison in Lisp.  Each remains open in the ledger's D20 row.
`shr-test/zoom-image' fails on the Mac oracle alone and the eglot
rust-analyzer tests fail on both sides with different conditions:
environment, not editor divergences, and not corrected here.

*Gate.*  Alone on the machine, on the tree as committed, with the
shared image on: grouped gate run-1789124693509254921-11394, GROUPED
GATE PASSED -- 2615 library tests across the ten groups (batch 49,
compat_runtime 84, eval_01 351, eval_02 284, eval_03 320, eval_04 251,
eval_05 351, lightweight 414, primitives 455, tty 56, every group 0
failed) and 69 in the binaries and integration tests; `cargo fmt
--check' and strict clippy exit 0 before and after; 11:04 to 11:43.
The focused run before it: 126 of the process, search, load-history,
lock, regexp and inventory controls passed; two of the five new
controls failed once on their own use of a bare interpreter (no
`lambda' macro, no coding systems for `write-region') and run on the
initialized interpreters the neighbouring file tests use.

## 2026-09-11 Checkpoint 19a: a symbol inside a vector under the positioning reader

*What prompted it.*  The Mac frozen run of checkpoint 18b left one
Emaxx-only failure, `context-menu-map-remove-consecutive-separators',
whose record shows `context-menu-map' answering `(keymap (foo-item ...)
"Context Menu")': the separator and the bar item were gone.  The test
passes on Linux.  On the Mac the startup loads native units Emaxx
compiled itself from the preloaded sources, mouse.el among them.

*What was found.*  Native-compiling mouse.el on Linux produced a unit
whose data could not be read back ("Invalid read syntax #<"): its
constants held `[#<reader-form>]' wherever the source had a key
sequence such as `[mouse-1]', and byte-compiling mouse.el with Emaxx
wrote 36 such placeholders into the `.elc'.  Under
`read-positioning-symbols' (the byte compiler's reader) lread.c reads
a symbol inside a vector as a symbol with position, as it does inside
a list; Emaxx's pass that turns the reader's positioned-symbol
placeholders into `symbol-with-pos' objects walked conses, records and
hash tables and not vectors, so the placeholder stayed in the vector:
`type-of' on the element signalled "reader form escaped object
materialization", `symbol-with-pos-p' could not answer, and the
printer wrote `#<reader-form>'.  GNU does not strip positions out of
vectors either (byte-run.el's `byte-run--strip-vector/record' stores a
positioned element back unchanged); its `.elc' is correct because the
compiler prints under `print-symbols-bare'.  The pass now walks
vectors in place, cycle-safe, and the byte compiler's constants print
bare: `[mouse-1]'.  A mouse.elc compiled by Emaxx now loads and both
context-menu tests pass under it.  The Mac failure is inferred to be
this defect through the preloaded native unit, not observed here; the
next Mac run is the receipt.  Control:
`read_positioning_symbols_positions_the_symbols_inside_vectors'
(`symbol-with-pos-p' and `bare-symbol' on the vector's element as GNU
answers them; a byte-compiled `define-key' with `[mouse-1]' prints the
key bare and no placeholder reaches the file).

*Observed beside it, not corrected.*  A native compilation whose C
side runs in a child Emacs printed the compilation context with a
`#<' object before this fix; the in-process path (a batch or
asynchronous compilation) showed the same placeholders in the unit's
data.  Both come from the same vectors and are gone with it; no
separate change was made to the context's printing.

*Text property writes.*  `put-text-property' and `add-text-properties'
cost 0.5 to 1 ms a call in a dired listing of 12,000 files (the ls-lisp
and dired tests, and auto-revert's dired test reverting such a
listing): every write cloned the buffer's whole span list and rebuilt
it.  textprop.c splits and merges the intervals at the edges of the
write and leaves the rest alone; the write path now finds the spans
meeting the range by binary search, rewrites those, and merges only
with the neighbour on either side.  The observable results are the old
path's exactly: a sequence of 22 overlapping writes, removals and
undos over a 60-character buffer prints the same `text-properties-at'
answers under the old and the new path (and matches GNU's except that
Emaxx merges a split interval back into an equal neighbour eagerly,
which shows as the neighbour's plist order; GNU keeps the split until
a later merge -- an order-only difference that predates this change
and remains).  `dired-test-bug25609' 20.2 to 9.5 s, `ls-lisp-test-bug70271'
26.8 to 11.5 s (GNU 0.39 and 0.47 s; the remainder is
`insert-directory' and the per-call floor).  Control:
`text_property_writes_touch_only_the_spans_they_cover' (the answer is
GNU's for the same program).  Found while writing that control and
not corrected: GNU's `text-properties-at' returns the interval's own
plist, and a later `put-text-property' over the interval changes that
list in place (`add_properties' does `Fsetcar' on the plist cell), so
a plist taken before the write reads the new value afterwards and is
`eq' to the one taken after; Emaxx builds a fresh list per call, so
the earlier plist keeps the old value and the two are not `eq'.  A
probe over a six-character buffer shows `((face italic) t t)' in GNU
and `((face bold) nil nil)' here.  Recorded here as a divergence of
plist identity; the property values agree.

*The interpreted call floor.*  `semantic-fmt-utest' runs 6.9 million
`looking-at' calls that GNU finishes in 2.3 s; at 25 us a call here
they took 169 s.  In a tight interpreted loop a call to `eq' cost
5.3 us and `looking-at' 8.6 us (GNU 0.05 and 0.3 us).  Three fixed
costs are gone: the interned-name table and the process symbol
registry hashed each name-to-id resolution with SipHash (FNV now, as
the interpreter's other name tables); the resolution scanned each name
byte by byte for the uninterned marker (a one-character `contains'
uses memchr); and every ordinary call resolved its head's function
cell and searched the 1,700-entry C manifest to ask whether the head
is an alias of a special form (`(defalias 'inline 'progn)'), an answer
now kept per call site under the function-binding generation, for a
bare-symbol head when no local function frame is in force.  `eq' 5.3
to 3.5 us, `looking-at' 8.6 to 6.4 us.  What remains is the
name-keyed variable path (`set_global_binding' and the lookups resolve
a name to its cell by hashing it on each access, R02c) and the
allocation of every call's argument vector; recorded in D20.

*Gate.*  The full grouped gate on this tree passed: eval_01 351,
eval_02 284, eval_03 320, eval_04 251, eval_05 351, primitives 457,
tty 56 (2 ignored), compat_runtime 84, batch 49, lightweight 414, the
binaries, the integration group (1,332 s); `cargo fmt --check' and
`cargo clippy --profile gate --all-targets --all-features -D warnings'
clean before and after.  The Mac receipt for the vector fix is the
next frozen run's `context-menu-map-remove-consecutive-separators'.

## 2026-09-11 Checkpoint 19b: the batch boot, equal hash tables, span edits, the environment

*What prompted it.*  A per-test timing sweep of the GNU test tree
(6,765 tests, GNU against Emaxx, both from this box) ranked the excess seconds by file: tramp-tests 528 s over 44 tests,
semantic's format test 146 s, autorevert 98 s, simple-tests 86 s,
dired-aux 39 s, mule 35 s, dired 25 s, package 24 s.  Four themes
account for most of it; each is measured, corrected and controlled
below.  The numbers are this box's (a 4-core VM whose page zeroing is
slow and variable); GNU's are from the same runs.

*The batch boot.*  A process that only prints and exits took 1.4 to
2.4 s of wall time and 0.92 s of user time (GNU 0.04 s).  Four costs,
in the order the profile ranked them.  (1) The image loader kept its
relocation kinds (939,000 entries), object types (489,000) and objects
in hash tables keyed by image offset; the inserts alone, each a probe
into a table too large for the cache and a first touch of its pages,
were 250 ms of the boot, and every field read hashed twice.  Every key
the writer emits is a multiple of `DUMP_ALIGNMENT', so the three tables
are dense now: one slot per aligned position of the image (`OffsetTable'
for the kinds and types, `ObjectTable' with a dense index into a vector
of the objects), a read being one or two indexed loads.  (2) The
executable's SHA-256 fingerprint read the 22 MB file into a vector
(the copy and the first touch of every page, 10% of the boot) before
hashing; it is streamed through the hasher's buffer.  The hash itself
is 150 ms of software SHA-256 on this CPU, which has no SHA extensions
(the crate uses them where the CPU has them).  (3) The image file was read into a vector (37 MB
copied and zeroed); it is mapped read-only from the page cache, as
pdumper.c's dump_mmap_contiguous maps it (`ImageBytes', MAP_POPULATE on
Linux; the vector remains on non-Unix hosts).  (4) Every booted process
ran a garbage collection on its first evaluation: the loader's
allocations were counted as consing, so the first `maybe_gc' found the
threshold long exceeded (a 100 ms mark of the whole heap in every child
Emacs, every trampoline compilation, every fixture boot).  alloc.c's
`consing_until_gc', `gc_threshold' and `gcstat' are plain globals the
image does not carry, zero in a process that starts from a dump, and
pdumper's objects are not consing: the loader's allocations are now the
baseline (`baseline_after_image_load'), and the first collection comes
after `gc-cons-threshold' bytes of the process's own consing, as in GNU
(`gcs-done' is 0 after a GNU boot).  Beside these, the strings of the
image decode straight from their bytes when they are ASCII (unibyte)
or valid UTF-8 (multibyte), which every Unicode string is; the raw-byte
forms, the five-byte forms and the surrogates take the loop.  Wall
0.53 s (median of 7), user 0.47 s, system 0.06 s; GNU 0.039 s.  What
remains is the loader itself (one Rust object per image object, 0.3 s)
and the startup Lisp; a child that only exits still costs 13 GNU boots.
`simple-tests-shell-command-39067', forty child boots, 67 to 36 s
(GNU 2.4).  Control: the existing D12 load control asserts, after
`pdumper_load', that no collection is due before the process conses
(`garbage_collection_might_be_due' false).

*Equal hash tables.*  Loading tramp-tests.el took 22.5 s (GNU 1.0);
its `tramp-process-sentinel' flushed the connection cache with 4,375
`remhash' calls at 2.2 ms each.  The runtime's `equal' tables kept a
bucket index by entry position and rebuilt the whole index, hashing
every remaining key, on every removal and on every `puthash' into a
freed slot (fns.c removes an entry by unlinking it from its bucket and
pushing its slot onto the free list).  The state now keeps each entry's
hash code beside it, as fns.c keeps `hash' beside `key_and_value', and
the buckets hold slot numbers, which are stable across removals and
reused slots; a removal or a reused-slot insertion touches one bucket,
and the index is rebuilt only from the stored codes (a thaw, a clone,
the positioned-symbol reindex) without hashing.  The answers, the
count and the `maphash' order (slot order, freed slots reused last-in
first-out) are GNU's for the control's program.  tramp-tests.el loads
in 12.7 s now, `tramp-test07-file-exists-p' 15.6 to 5.3 s (GNU 0.85);
the rest of tramp's cost is its per-call floor, measured below.
Control: `equal_hash_table_answers_survive_removals_and_slot_reuse'.

*Buffer edits and the span list.*  `dired-test-bug30624' took 14.9 s
(GNU 0.33) listing this box's /tmp of 4,000 entries: `insert-directory'
decodes each file name and each gap between them with
`decode-coding-region', 8,000 replacements of a few characters in a
400 KB buffer carrying 4,000 property spans, and every insertion and
deletion cloned the whole span list, plist by plist, rebuilt, sorted
and re-merged it (0.36 ms an edit).  intervals.c adjusts the intervals
the edit meets and offsets the rest; `adjust_text_properties_for_insert'
and `_for_delete' now find the spans by binary search, split or cut
the one or two at the edges, shift the rest in place and merge only
at the cut, and `substring_property_spans' takes its spans by binary
search as well.  The observable results are unchanged: the differential
probe of checkpoint 19a prints the same, and the property controls
pass.  8,000 `decode-coding-region' calls in that buffer 2.9 to 0.29 s
(GNU 0.035; what remains is the replacement itself, 36 us a call);
`dired-test-bug30624' 14.9 to 4.5 s, `dired-test-bug27496' 6.5 to
1.5 s (GNU 0.19), `auto-revert-test04-auto-revert-mode-dired' 45 to
17.5 s (GNU 1.5).

*`looking-at' in a large buffer.*  70 us a call in a 300 KB buffer
whatever the pattern and whether it matched (GNU 0.5 to 1.7 us):
`looking-at' built its haystack from point to the end of the buffer on
every call, twice -- once for the POSIX matcher, before the branch
that decides whether POSIX matching is wanted at all, and once for the
ordinary matcher with one character of left context for `\='.  GNU's
re_match_2 reads the buffer in place.  The POSIX text is built on the
POSIX branch only, and without a `\=' in the pattern the ordinary
match now runs over the whole accessible region, the haystack every
search of the unchanged buffer shares through the cache, anchored at
point's byte offset (the match is required to start there, as
before); a pattern with `\=' keeps the one-character context, whose
lookbehind translation needs it.  The match data is converted through
the haystack's index instead of a character walk from its start.  9 to
14 us a call now; `dired-move-to-filename' calls it once per line.
`dired-test-bug30624' 4.5 to 3.9 s.

*The environment behind `expand-file-name'.*  45 us a call (GNU 3.6):
every expansion read HOME from `process-environment' by copying the
list's 140 entries into a vector of strings.  fileio.c reads the home
directory only for a `~'; the expansion asks for HOME only when the
name or the default directory starts with one, and `getenv' walks the
list comparing each entry in place, as callproc.c:getenv_internal_1
does.  7.4 us a call now.

*Measured after these and open.*  `find-file-name-handler' 21 us on a
name not in its match cache (GNU 1.8): a miss compiles every handler's
pattern and snapshots the alist's cons cells for mutation tracking.  A
`cl-defstruct' copy with two `setf's 17 us (GNU 1.8), `format' 5 us
(0.6), `string-match' 5 us (0.4): the per-call floor.  `macroexpand-all'
of a small form 116 us (GNU 12); loading tramp-tests.el spends 8.6 s
of its 12.7 s in the `ert-deftest' expansions.  `tramp-file-name-unify'
0.29 ms a call, 14,900 calls in one `directory-files' of the mock
directory.  The per-kilobyte replacement in `decode-coding-region'.

*Gate.*  The full grouped gate on this tree passed: eval_01 351,
eval_02 284, eval_03 320, eval_04 251, eval_05 351, primitives 458,
tty 56 (2 ignored), compat_runtime 84, batch 49, lightweight 414, the
binaries, the integration group (1,287 s); `cargo fmt --check' and
clippy clean before and after.  The focused runs before it: 349 tests
over the loader, the hash tables, the text properties, the searches
and the coding regions, then 38 over `looking-at', POSIX matching and
the match data, all passing.

## 2026-09-11 Checkpoint 19c: the assignment path by symbol, the allocator's purging

*What prompted it.*  With checkpoint 19b in place, the interpreted
call floor remained the theme behind most of the sweep's ratios: a
loop of a million calls of a three-line function with a `setq' of a
dynamic variable took 10.8 s (GNU 0.56).  Its profile put a fifth of
the time in `SymbolName::id_of' and the string hashing and comparison
under it, called from the assignment path; and a quarter of a
`macroexpand-all' loop's time was system time.

*The assignment path.*  A `setq' of a global resolved its variable's
name to its id seven times over -- `resolve_variable_name' through
`direct_variable_alias', the two forwarding-flag reads of
`prepare_variable_assignment', the buffer-local and auto-local reads of
`assignment_scope', and the value cell's read and write in
`set_global_binding' -- hashing the text each time, and copied the name
into a fresh String three times, where GNU's set_internal has the
symbol object in hand and reads its cells directly.  The evaluator's
`setq', the byte code's Bvarset and `set' now keep the `SymbolName'
(which carries its id) and go through symbol-keyed variants:
`resolve_variable_symbol' follows the alias chain by id,
`prepare_variable_assignment_symbol' reads the flags by id,
`assignment_scope_symbol' and `assignment_buffer_id_symbol' read the
buffer-local cell and the auto-local flag by id, and
`set_symbol_value_cell_resolved' and `set_global_binding_resolved'
write the cells by id; the name-keyed entry points resolve once and
delegate to them, and the shared bodies (the constant checks, the
forwarded-slot normalizations, the special names) are one copy each.
The loop 10.8 to 8.0 s.  The remaining floor is the call itself
(argument vectors, backtrace frames, the lexical scan) and the reads,
which still take the name-keyed `lookup_var' for names the source
analysis has not resolved; both stay in D20.  Controls: the 327
existing tests over `setq', `set', `let', aliases, buffer-local
values, watchers and the forwarded variables pass unchanged.

*The allocator's purging.*  mimalloc gives freed pages back to the
host after `purge_delay' (a second in the bundled mimalloc 3), so a
Lisp workload that frees and refills its working set paid a page fault
and a zeroed page for memory it had just released: 303 purges of
196 MB during a 3 s `macroexpand-all' loop, a quarter of its time in
the kernel, 0.4 s of the `setq' loop's 8.  glibc's malloc keeps GNU's
heap.  `tune_allocator', called first thing by both binaries, turns
purging off when the option reads a bundled default (the numeric
option identifier is guarded by that read, and an environment setting
is left alone): the `macroexpand-all' loop 4.2 to 2.8 s, the `setq'
loop 8.0 to 7.8 s, both with 0.05 s of system time; `mimalloc's
statistics report no purge.  The cost is that memory a Lisp program
frees stays with the process, as most of it does under GNU.  This is a
host-allocator policy, not GNU behaviour, and is disclosed as such.

*Measured after these and open.*  The `setq' loop's 7.8 s against
0.56 (14x): the per-call floor proper.  `macroexpand-all' of a small
form 111 us (GNU 12).  tramp-tests.el loads in 12.4 s (GNU 1.0), the
`ert-deftest' expansions most of it.  `dired-test-bug30624' 4.0 s
(GNU 0.33).  The boot 0.53 s (GNU 0.039).

*Gate.*  The full grouped gate on this tree passed: eval_01 351,
eval_02 284, eval_03 320, eval_04 251, eval_05 351, primitives 458,
tty 56 (2 ignored), compat_runtime 84, batch 49, lightweight 414, the
binaries, the integration group (1,104 s); `cargo fmt --check' and
clippy clean before and after.  The focused run before it: 327 tests
over `setq', `set', `let', aliases, buffer-local values, watchers and
the forwarded variables, all passing.

## 2026-09-11 Checkpoint 19d: `let' by symbol

*What prompted it.*  After checkpoint 19c the profile of the same
million-call loop put a fifth of its time under
`bind_special_variable' and its unwinding: the `let' of a dynamic
variable resolved the name (hashing it), prepared the value by name
(two more), read the buffer-local cell and the global cell by name,
wrote the global by name (resolving again), kept the name as a fresh
String in the restore record, and on the way out set or removed the
cell by name once more.

*What changed.*  `SpecialBindingRestore' carries the bound
`SymbolName', resolved, and `bind_special_symbol' does eval.c's
specbind by id: the alias chain (`resolve_variable_symbol'), the
forwarding flags (`prepare_variable_assignment_symbol'), the
buffer-local cell (`buffer_local_binding_symbol'), the global cell
(`global_binding_value_symbol', `set_global_binding_resolved'), the
auto-local flag (`has_flag'); `restore_special_binding' puts the
previous value back through `set_global_binding_resolved' or removes
the cell through `remove_global_binding_symbol'.  `let' and `let*'
keep the symbols they read from the form and decide dynamic against
lexical through `binding_is_dynamic_symbol' (the SPECIAL flag by id,
the C-slot registry and the local-special set by name as before).
The name-keyed `bind_special_variable' interns once and delegates.  The
loop 8.0 to 6.5 s (GNU 0.56); a `macroexpand-all' loop 2.8 to 2.5 s;
tramp-tests.el loads in 9.9 s (12.4 before, GNU 1.0); bindat-tests
7.0 s (GNU 0.7), comp-cstr-tests 8.5 s (GNU 0.6): the per-call floor
proper, still 10x, remains the theme.

*Measured after it and open.*  The call itself: the argument vector
(`Value::to_vec' of the argument list), the backtrace frame push and
pop, `function_executable_body', the lexical scan of
`set_lexical_variable_checked' comparing names frame by frame, and the
reads through `lookup_var' for names the source analysis has not
resolved.

*Gate.*  The full grouped gate on this tree passed: eval_01 351,
eval_02 284, eval_03 320, eval_04 251, eval_05 351, primitives 458,
tty 56 (2 ignored), compat_runtime 84, batch 49, lightweight 414, the
binaries, the integration group (1,203 s); `cargo fmt --check' and
clippy clean before and after.  The focused run before it: 354 tests
over `let', `let*', dynamic bindings, threads, buffer-local values,
watchers, aliases and `setq', all passing.

## 2026-09-11 Checkpoint 19e: the call itself

*What prompted it.*  With `setq' and `let' keyed by symbol the
million-call loop's profile flattened; four fixed costs of every call
remained visible.  (1) `function_executable_body' skipped a docstring,
`declare' and `interactive' forms by copying each candidate form into
a vector to look at its head, and `let' asked whether its binding list
was a vector literal the same way (`is_vector_literal'), a Vec per call
each.  (2) A dynamic lambda bound its parameters through the name-keyed
`bind_special_variable' although the parameter list holds the symbols.
(3) `capture_current_backtrace_context' ran on every call and asked,
by name, whether `edebug-entered' is special.  (4) `assignment_scope'
searched the active bindings by name.  Each is by symbol now: the head
checks read the car in place and walk the list only on a match
(`proper_list_headed_by'), the parameters go through
`bind_special_symbol', the `edebug-entered' flag is read by the id of
a symbol interned once per thread, and the active-binding search
compares symbols (`active_special_assignment_scope_symbol').  The loop
6.45 to 5.94 s (GNU 0.56); `macroexpand-all' of a small form 111 to
94 us (GNU 12); a `cl-defstruct' copy with two `setf's 15.4 to 11.8 us
(GNU 1.8).

*Measured after it and open.*  The profile is flat now: `eval_inner'
itself, the copying of argument and frame vectors, the buffer-local
and symbol-cell reads, the watcher check, the backtrace frame.  The
loop remains at 10x; the next step is structural (a call that does
not clone its environment or record a frame it will not inspect),
which is the ledger's per-call floor item.

*Gate.*  The full grouped gate on this tree passed: eval_01 351,
eval_02 284, eval_03 320, eval_04 251, eval_05 351, primitives 458,
tty 56 (2 ignored), compat_runtime 84, batch 49, lightweight 414, the
binaries, the integration group (1,042 s); `cargo fmt --check' and
clippy clean before and after.  The focused run before it: 268 tests
over `declare', `interactive', vector literals, lambdas, `funcall',
backtraces, edebug, dynamic bindings, `let' and closures, all passing.
