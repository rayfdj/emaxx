# AI Continuation Instructions - Do Not Skip

This document is the handoff for any AI agent continuing the GNU Emacs
compatibility goal in this repository. Read it before making changes.

## Active Objective

Make `emaxx` match GNU Emacs at the Elisp API boundary by passing the selected
compatibility tests from the sibling GNU Emacs checkout at `../emacs`.

The canonical ordered manifest is:

- `compat/oracle_tests_all.txt`
- `compat/oracle_tests_all.md`

The denominator is 7080 selected tests. Do not use source-tree `ert-deftest`
counts as the progress denominator.

## Current Resume Point

- Verified through selector 2100/7080: `faceup-test-basics.el'
  (2085..2099) and `faceup-test-files.el' (2100) pass their grouped
  `check-all` replays; the 87-file verified-prefix sweep is green
  (2026-07-05).
- The `Compat 2100/7080` batch: `forward-sexp'/`scan-sexps' honor
  `parse-sexp-ignore-comments' (native emacs-lisp-mode sets it), reach
  buffer end instead of signaling over trailing comments, and
  `syntax-propertize' is ported (auto-buffer-local
  `syntax-propertize--done'; the fontification engine runs it first, so
  `syntax-propertize-rules' modes get their `syntax-table' text
  properties).  Details in `docs/compatibility-goal.md`.
- The `Compat 2084/7080` batch (see `docs/compatibility-goal.md` for the
  full list): simple_compat ports of the ert-x helpers (ert-x is a
  preloaded feature; its macros were void), GNU message_dolog()
  semantics for `message'/`message-log-max', `ert--test-buffers' +
  GNU test-buffer naming in native `ert-with-test-buffer',
  unnamed `(:type vector)' cl-defstructs stored as plain vectors (ewoc
  nodes, timers — `timerp' accepts 10-slot vectors), handler-bind
  condition lists, `ert-info' dynamic `ert--infos' binding, `symbol-file'
  ert--test type, a macro-shadowing shield for the pcase family (GNU
  pcase.el registers its backquote pattern under `\`' but the native
  reader produces `backquote'), order-insensitive
  `equal-including-properties', GNU `indent-rigidly'/`indent-line-to'
  edge cases, `lisp-indent-line' as native elisp-mode's
  `indent-line-function', and `font-lock-mode' running the buffer's
  `font-lock-function'.
- IMPORTANT probe-environment fact (cost an hour): the harness passes the
  FULL oracle load-path (`emaxx_upstream_load_path`) plus `-l ert`, which
  LOADS GNU ert.el/ert-x deps (pcase.el, ewoc.el...) on top of the native
  preloads.  Probes must replicate that: build /tmp/probes/loadpath.txt
  from the ORACLE's `load-path` (batch `--eval` printing `load-path`,
  plus test/lisp dirs), not just the test directories, or `-l ert`
  fails to resolve and probe behavior diverges from harness behavior.
- The `Compat 2056/7080` batch built the native fontification engine and
  the repairs it exposed (see `docs/compatibility-goal.md` for the full
  list): `font-lock-ensure' runs a syntactic pass (comments/strings from
  `syntax-ppss') plus a full keyword pass (regexp/function matchers,
  subexp highlights with OVERRIDE/LAXMATCH, anchored highlighters,
  FACENAME expressions) over `font-lock-defaults' installed lazily for the
  native modes (requiring lisp-mode.el/js.el for their keyword variables);
  standard font-lock face variables are self-quoting defvars;
  `font-lock-defaults' is auto-buffer-local (font-core.el defvar-local) —
  sh-mode's plain `setq' used to leak globally and kill fontification
  everywhere; `font-lock-ensure' + the native major modes (c/c++/java/js/
  javascript) are in `prefer_builtin_override' because loading GNU
  font-lock.el/cc-mode.el/js.el shadows them with redisplay-dependent
  elisp (cc-mode's elisp `c-mode' dies on void `backtrace-frame');
  `javascript-mode' delegates to `js-mode' (GNU defalias); `ert-pass'
  throws `ert--pass' (native runner counts it a pass), `ert-fail' signals
  `ert-test-failed', `ert-set-test' registers the ert-font-lock deftest
  macros' tests; `\s<'/`\s>' resolve from the syntax table's explicit
  comment-class entries; `regexp-opt' honors PAREN (a shy-group PAREN bug
  silently killed every js keyword subexp match).
- Fontification-debugging leverage for future frontiers:
  `EMAXX_DEBUG_FONTLOCK=1' traces installer decisions, matcher attempts,
  and highlight applications; cross-test contamination in a suite is
  bisected fast with `(ert-run-tests-batch-and-exit '(member TEST-A ...
  TARGET))' — alphabetical run order, so test the failing target behind
  successive prefixes of its predecessors.
- The `Compat 2016/7080` batch finished eieio-tests.el on top of the
  groundwork+persist batches: `cl-no-next-method'/`cl-no-applicable-method'
  hooks (runtime helper `emaxx--cl-generic-apply-next' routes the dispatch
  chain's `ignore' sentinel to the hooks; single-method generics check
  their specializers; simple_compat lowers obsolete `no-next-method'/
  `no-applicable-method' defmethods per eieio-compat.el), in-place method
  REPLACEMENT for same-qualifier+specializer re-registration (splicing a
  duplicate wrapper made same-condition wrappers point at each other —
  infinite condition loop / Rust stack overflow once nothing matched),
  `defgeneric' over an existing non-generic errors (follows defalias
  chains; `generic-p' defined), native exact `same-class-p' + GNU `NAME-p'
  (exact) / `NAME--eieio-childp' (subclass) predicates,
  `eieio--class-children' returns symbols, `oref-default'/`oset-default'
  accept instances, class-allocated `oref-default' returns the
  `eieio--unbound' marker unsignaled.  Sweep-demanded repairs: the
  `(subclass CLASS)' dispatch condition resolves `eieio-defclass-autoload'
  stubs via `autoload-do-load' like GNU's subclass generalizer, and slot
  descriptors merge per GNU's storage model (each class's merged view is
  copied parent-by-parent, so a parent's own redeclarations of ancestor
  slots survive into subclasses — five cedet files depended on
  semanticdb's `tracking-symbol' initform).  Details in
  `docs/compatibility-goal.md`.
- Verified through selector 2106/7080: `find-func-tests.el` passes its
  grouped `check-all` replay.  The `Compat 2106/7080` batch finished the
  file with a simulated-minibuffer completion engine: `completing-read'
  consumes queued `unread-command-events' (ert-simulate-keys) as a key
  loop — self-inserting chars, RET submits, TAB completes via
  longest-common-prefix over the table, a trailing-slash retry (completes
  the component before a final "/"), and a component-wise
  partial-completion expander for "o/org"-style patterns;
  `filtered_completion_matches' accepts FUNCTION completion tables
  (calling (TABLE STRING PRED t)); `locate-file-completion-table' is
  ported to simple_compat.el (candidates carry the directory part of
  STRING so plain prefix matching reproduces GNU's boundaries behavior).
- KNOWN ENVIRONMENT FLAKE (do not chase):
  `auto-revert-test02-auto-revert-deleted-file-remote'
  (autorevert-tests.el) flips between PASS and FAIL depending on
  container state — it failed 3/3 on a PRISTINE parent-commit tree in the
  same session where it had passed an earlier sweep.  Compare against the
  parent commit before attributing it to your diff (an hour was lost
  bisecting simple_compat.el for it; a nonsense stub "reproduced" the
  failure because the test is simply unstable).  todo-mode-tests.el
  remains the other known retry-flake.
- Verified through selector 2199/7080: `float-sup-tests.el' (2107)
  passed as-is and `generator-tests.el' (2108..2199) passes all 92.
  Backquote templates now ALWAYS expand to list/append constructor code
  under macroexpand-all (backquote_template_code).  Two regressions that
  gate change caused and their fixes, for the record:
  - `',(f)' reads as (quote (comma (f))); quoted forms are only opaque
    to the converter when NO unquote appears inside
    (template_tree_unquotes) — otherwise the unquote silently became a
    literal `comma' form (bytecomp's dodgy-args warnings vanished
    because the compiled args stopped being literals).
  - Dotted unquote tails emit (append (list ...) TAIL); emaxx's `append'
    flattened a STRING as the last argument instead of reusing it
    verbatim as the tail — GNU: (append '(2) "b") => (2 . "b"); fixed in
    the native append (last arg now verbatim even for strings/vectors).
  - pcase forms keep their backquote PATTERNS: macroexpand-all walks
    only pcase clause bodies (patterns/bindings verbatim) — full opacity
    breaks cl-labels rewriting inside pcase bodies (ert-x-tests caught
    both mistakes).  The
  generator batch, beyond the cl-macrolet groundwork:
  - `macroexpand-all' now yields GNU shapes for the whole macro family
    the CPS transformer consumes: `cl-symbol-macrolet' substitutes
    variable references (shadowing-aware; that fixed the entire
    void-lexical-var cluster — the "doubled gensym" theory was wrong,
    the binding rename simply never applied), `setf' with symbol places
    → `setq', `push'/`pop'/`cl-incf'/`cl-decf', `prog2' →
    progn+prog1, and BACKQUOTE TEMPLATES now expand to list/append
    constructor code (backquote_template_code in macros.rs; both the
    try_macroexpand shield and macroexpand-all's template walk convert).
    The last is the most cross-cutting macro change yet — sweep suspect
    #1 for any regression.
  - `(signal SYM NON-LIST-DATA)' now produces the dotted condition value
    (SYM . DATA) like GNU (generator's iter-end-of-sequence carries the
    final value that way); LispError::condition_type reads the car of
    dotted values.
  - `condition-case' handler matching consults the signaled symbol's
    `error-conditions' property when present (define-error hierarchies;
    generator tests define cps-test-error with a custom condition list).
  - simple_compat: `inline' macro (byte-run.el progn marker), edebug
    autoloads (`edebug-defun'/`edebug-eval-top-level-form').
  - WARNING repeated from this batch's false trail: an EMPTY result JSON
    (load_error) makes "notpassed 0" look like success — always check
    the TOTAL, not just the failure count.
- Verified through selector 2207/7080: `gv-tests.el' (2200..2207, all 8
  selected) passes its grouped replay.  Lessons from this batch:
  - NEVER shadow a loadable GNU macro with a Rust special-form arm.  A
    `gv-define-setter' special-form shortcut looked harmless but it also
    intercepted gv.el's OWN internal registrations when gv.el loaded
    (gv-define-simple-setter expands to gv-define-setter), so `car'/`cdr'
    never got their `gv-expander' properties.  Downstream, GNU cl-macs'
    `cl-callf cdr (car cursor)' in `edebug-move-cursor' could not expand
    to `setcar' and silently fell back to a copying path — breaking the
    cons IDENTITY (`eq') that edebug's `&name' spec matcher asserts on
    (`gv-setter-edebug' failed in-suite with cl-assertion-failed only
    after anything loaded gv.el).  The fix: remove the shortcut and give
    `resolve_setf_place' a LAST-RESORT arm consuming the standard
    `gv-expander' property via gv-get's DO protocol (loops.rs); preload.rs
    already autoloads gv.el for the gv-define-* macros.  Place the arm
    after all native place arms — gv.el registers expanders for car/cdr/
    get/... that natives must outrank.
  - `plist-get' and `cl-getf' have DIFFERENT third arguments: plist-get
    takes a PREDICATE, cl-getf takes a DEFAULT (the setter evaluates but
    ignores it).  Passing cl-getf's default as a testfn funcalls an
    integer (caught by cargo test, not the compat suite).
  - GNU's gv expander for plist-get/cl-getf PREPENDS missing keys
    ((setf (plist-get l :d) v) => (:d v . l)); plist-put appends.  A
    stale Rust unit test asserting append order had to be updated.
  - `(setf (get S P) V)' routes to `put' (sf_setf arm).
- Verified through selector 2275/7080: `hierarchy-tests.el' (2208..2275)
  passes all 68.  Lessons from this batch:
  - 56 of 57 initial failures shared ONE root cause: `(require 'map)'
    no-oped because "map" sat in `is_compat_preloaded_feature', so GNU
    map.el (cl-generic `map-put!'/`map-insert', the `map-elt'
    gv-expander) never loaded.  GNU does NOT preload map.el (under `-l
    ert' the oracle has it loaded via the dependency chain).  Fix:
    require now prefers the real file when it resolves on the load-path
    and keeps the compat shim as the no-file fallback.  When a compat
    feature's tests fail with void-SOMETHING, check whether the shim is
    eclipsing a real GNU library before porting functions one by one.
  - `cl-defgeneric' now processes `(declare (gv-expander (lambda (do)
    ...)))' like `gv--defun-declaration': the expander takes DO plus the
    generic's own lambda list (map-elt's setf machinery needs it).
  - `setf' of `alist-get' must mutate a FOUND pair with `setcdr' — the
    alist stays `eq' and map-put! uses exactly that to detect in-place
    updates — and only assign the place when prepending a missing key
    (GNU prepends) or removing.
  - GNU text-property order: `add-text-properties'/`put-text-property'
    replace existing entries IN PLACE and cons NEW properties onto the
    interval-plist head, so `text-properties-at' lists later additions
    first ((add a b) then (add c) reads (c b a)); `propertize' and
    `set-text-properties' preserve the given plist order verbatim.
    hierarchy's make-text-button test asserts (car properties) eq
    'action because of this.  Cross-cutting: swept clean.
  - `tabulated-list-mode' autoloads tabulated-list.el (GNU preloads it
    through buff-menu.el).
  - `(kill-emacs 0)' is NOT defined in emaxx — probes using it as a
    final `--eval' exit 2 after the load completes; harmless for probe
    outputs written during load, but do not read that exit code as a
    load failure.
- Verified through selector 2287/7080: `icons-tests.el' (2276..2277),
  `let-alist-tests.el' (2278..2284), `lisp-mnt-tests.el' (2285..2287).
  Lessons:
  - GNU `pcase-let'/`pcase-let*' DESTRUCTURE ONLY — they never test.
    icons.el binds with `(,parent ,spec _ _): inside a backquote pattern
    a bare symbol (including `_') is a LITERAL eq-test in real pcase,
    but pcase-let drops all such membership tests.  emaxx's lenient
    binder (pcase_pattern_bindings_inner, eval.rs) now skips literal
    symbol/keyword comparisons inside backquote when lenient.
  - `let-alist' was ANOTHER special-form-shadows-GNU-macro case (same
    disease as gv-define-setter): the native form can't do nested
    `.sublist.foo' or `..outer' escapes and fails the exact
    macroexpansion test.  The core.rs arm now defers to a Lisp macro
    named let-alist when one is defined (the test file requires
    let-alist), falling back to sf_let_alist for file-less runs (cargo
    unit tests use it).  PATTERN: when adding a special-form arm for
    something GNU implements in loadable elisp, gate it on the Lisp
    definition being absent.
  - `gnutls-available-p' returns nil (GNU --without-gnutls build);
    package.el evaluates it at LOAD time so it must exist
    (lisp-mnt's lm-package-requires requires package).
- The next frontier is `test/lisp/emacs-lisp/lisp-mode-tests.el`
  (21 selected; manifest line 2384).  In-progress groundwork took it
  from 13 failing to 7: parse-partial-sexp now tracks element 2 (start
  of last complete sexp, per-level; token STARTS record it), supports
  STOPBEFORE (stop before any sexp start; open parens included —
  lisp-indent-specform needs it), stops on TARGETDEPTH crossings in
  BOTH directions, and honors COMMENTSTOP='syntax-table (stop after
  string/comment boundaries — lisp-indent-calc-next crosses multi-line
  strings with it); `backward-prefix-chars' is native; the native
  emacs-lisp-mode installs GNU lisp-data syntax entries (non-alnum
  ASCII = symbol constituent, `'``,#' prefix, `@' "_ p", `.' symbol)
  plus comment-start-skip ";+ *", comment-indent-function
  `lisp-comment-indent', comment-column 40; autoloads: newcomment.el
  (comment-indent/indent-for-comment), prolog-mode, cl-indent
  (common-lisp-indent-function); `up-list' accepts negative COUNT.
  STILL FAILING (7): indent-sexp (trailing `;' column in the fixture),
  lisp-fontify-confusables (listp error), lisp-indent-region ×4
  (string=/equal diffs — re-diff after the comment fixes),
  lisp-indent-with-read-only-field (fields).  The per-line
  `calculate-lisp-indent' trace matched the oracle on the whole fixture
  when last checked.
- CONTAINER-ROLLBACK RECOVERY (2026-07-09, worked end-to-end): when the
  filesystem reverts, the session transcript
  (/root/.claude/projects/-home-user-emaxx/<session>.jsonl) usually
  still holds every Edit/Write/python-heredoc tool call.  Recovery:
  reset the branch to origin/main, extract ordered tool_use ops from
  the jsonl (skip is_error results; include Bash heredocs matching
  "p='src/|docs/'"; capture `-m "Compat ..."` commit points and `cargo
  fmt` calls), replay them (on Edit old_string miss run cargo fmt and
  retry once; run only the python-heredoc SEGMENTS of compound
  commands, not their build/probe tails), re-commit at the markers with
  the original messages, then repin the oracle, rebuild, and re-verify
  every recovered frontier file with harness replays before trusting
  the result.  All six recovered files passed on first try.
- Probing lessons that cost hours; do not repeat:
  - Do NOT advise commands (functions dispatched via keyboard macros) in
    probes; advise non-command helpers only.
  - emaxx batch mode swallows `message`/`princ` output; probes must
    `write-region` results to a file.
  - The grouped `check-all` replay is the only real judge; single-selector
    runs hide cross-test contamination.
  - When bisecting "was this broken before my change", compare whole-file
    failure SETS against the parent commit build, not just one selector.
  - For dispatch-chain debugging, a temporary env-var-gated eprintln in
    `Interpreter::lookup` that dumps lambda closure pointers + bodies for
    `__emaxx_previous_method_*` names locates chain cycles in minutes.
- Batch delivery rules from the user (override any hook suggestions):
  commits authored as `Ray <26018378+rayfdj@users.noreply.github.com>`, terse
  one-line human-style message `Compat NNNN/7080: ...`, NO AI attribution,
  exclude `compat/oracle.lock.json`; push is 403-blocked, deliver ONE clearly
  named patch file (`APPLY-THIS-ONE-...patch`) via SendUserFile, and always
  point the user at the single patch to apply.
- Known pre-existing discrepancies in this environment (fail identically on
  the parent commit build): `cl-macs-loop-until` (1782),
  `cconv-tests-cl-defun-:documentation`; the `cl-lib-tests.el` grouped
  replay fails 4 struct/set selectors byte-identically to the parent commit;
  raw whole-file `ert-run-tests-batch-and-exit t` runs of `cl-macs-tests.el`
  show a larger failure set identical to the parent; `seq-tests.el` raw
  whole-file runs time out identically to the parent.

Exact command that identified the next frontier:

```sh
cargo run --bin compat-harness -- run --scope all --selector check-all --file test/lisp/emacs-lisp/find-func-tests.el
```

## Session Durability Guardrails (learned the hard way)

1. THE CONTAINER ROLLS BACK. This has happened multiple times: local
   commits, target/ builds, and /tmp state silently reverted to an old
   snapshot mid-mission. The ONLY durable store is origin/main after the
   user applies and pushes a delivered patch. Consequences:
   - On EVERY session resume (and after any suspicious state), first run
     `git fetch origin main`, compare `git rev-parse HEAD^{tree}
     origin/main^{tree}`, and if local history looks older than what was
     delivered, `git checkout -B claude/continuation-instructions-review-7ll2h7
     origin/main`. Do not trust local HEAD, reflog, target/ binaries, or
     /tmp/probes to have survived.
   - After any reset/rollback: repin the oracle (`chmod 666
     compat/oracle.lock.json`, then as dev `./target/release/compat-harness
     oracle pin --emacs /home/user/emacs/src/emacs --repo /home/user/emacs`),
     rebuild `cargo build --release`, and recreate /tmp/probes
     (prefix-files.txt regenerates from compat/oracle_tests_all.txt by
     taking every `^test/...: discovered=` line through the last verified
     file; sweep.sh loops compat-harness check-all over it as dev).
   - Commit and DELIVER the patch as soon as a batch is green. Unpushed
     work is one rollback away from oblivion; the delivered
     APPLY-THIS-ONE patch is the real backup.
2. When several commits accumulate before the user applies anything,
   regenerate ONE cumulative patch (`git format-patch origin/main..HEAD
   --stdout`) and tell the user explicitly that it SUPERSEDES earlier
   patch files. Never leave two live patches ambiguous.
3. The stop hook complaining about uncommitted changes when `git status`
   shows ONLY `compat/oracle.lock.json` is a false positive: that file is
   the container-local repin and must never be committed.
4. Cross-cutting changes (cl-generic dispatch, reader, printer, `equal',
   records, file-attributes, time formats) REQUIRE a full prefix sweep
   before committing, no matter how local they feel: the typed-oset work
   regressed three srecode files through `file-attributes' time shapes,
   and an eieio--object-class tweak broke persist. If the sweep is not
   green, the batch is not done.
5. Probe/debug hygiene that repeatedly wasted cycles:
   - `cmd | head; echo exit=$?` reports HEAD's exit code. Redirect to a
     file and echo the real code, then read the file.
   - DELETE stale probe artifacts (result JSONs, error-capture files)
     before reruns; a stale file has misdirected debugging twice.
   - emaxx exiting 2 with an EMPTY log is a LOAD error whose message was
     swallowed by batch; capture it with `(condition-case err (load ...)
     (error (write-region (format "%S" err) ...)))`.
   - Exit 134/SIGABRT with "stack overflow" during dispatch-heavy code is
     almost always a cl-generic wrapper chain cycle; an env-gated
     eprintln in the ERT runner loop (EMAXX_DEBUG_ERT) finds the test.
   - Run emaxx probes as dev via `bash /home/user/asdev.sh "..."`, never
     as root (root-owned /tmp lock files poison later oracle runs).

## Oracle Setup In A Fresh Container

If the sibling `../emacs` checkout is missing (fresh cloud container), rebuild
the oracle before anything else. GNU/GitHub sources are blocked by the network
policy, but `archive.ubuntu.com` is reachable:

1. `curl -O http://archive.ubuntu.com/ubuntu/pool/universe/e/emacs/emacs_30.2+1.orig.tar.xz`,
   extract to `../emacs`, `git init` + commit it.
2. The Ubuntu `+1` repack strips GFDL docs and `admin/unidata/IVD_Sequences.txt`;
   stub `doc/{emacs,lispintro,lispref,misc}/Makefile.in` with no-op targets
   (include a `.SUFFIXES:` override so `org.texi` matches the catch-all rule)
   plus minimal `emacs.texi`/`emacs-lisp-intro.texi`/`elisp.texi` files that
   carry a `@direntry`, and `touch admin/unidata/IVD_Sequences.txt`.
3. `apt-get install build-essential pkg-config texinfo autoconf libgnutls28-dev
   libncurses-dev libgccjit-13-dev libgmp-dev libxml2-dev libsqlite3-dev
   zlib1g-dev`, then `./autogen.sh && ./configure --with-native-compilation
   --without-x && make -j$(nproc)`.
4. Repin: `cargo run --bin compat-harness -- oracle pin --emacs
   ../emacs/src/emacs --repo ../emacs`. Leave the resulting
   `compat/oracle.lock.json` change uncommitted; the darwin pin in git history
   stays canonical.
5. Run the Rust test suite as a non-root user; several tests assert
   unwritable-file behavior that is vacuous under root.

## Non-Negotiable Rules

1. Preserve identical observable behavior to GNU Emacs for the selected tests.
2. Fix behavior honestly in Rust. Do not hardcode test answers, delegate to the
   oracle at runtime, or add special cases that only recognize test data.
3. Keep formatting clean. Run `cargo fmt`; final verification must include
   `cargo fmt --check`.
4. Keep clippy clean. Final verification must include
   `cargo clippy --all-targets --all-features -- -D warnings`.
5. Keep tests clean. Final verification must include `cargo test`.
6. Keep diffs clean. Final verification must include `git diff --check`.
7. Do not revert unrelated user or generated changes. If the worktree is dirty,
   inspect it and work with relevant changes.
8. Use focused, strategic patches that follow existing code patterns.
9. Add regression tests for behavioral changes when there is a local Rust test
   surface for the behavior.
10. Commit and push each coherent passing compatibility batch before moving to
    the next frontier.

## Compatibility Harness Usage

Use the harness to compare emaxx against the sibling GNU Emacs checkout. The
normal exact replay shape is:

```sh
cargo run --bin compat-harness -- run --scope all --selector SELECTOR --file PATH/TO/TEST.el
```

For the next known frontier, run:

```sh
cargo run --bin compat-harness -- run --scope all --selector check-all --file test/lisp/emacs-lisp/find-func-tests.el
```

After fixing a selector, exact-replay that selector. Then probe the next
canonical selector from `compat/oracle_tests_all.txt` and record the result in
`docs/compatibility-goal.md`.

## Prefix Replay Policy

Do not mechanically run the full selected prefix `1..N-1` before every selector
`N`. That is too expensive for small batches and has not been the working
cadence.

Required before committing a batch:

- Exact-replay every selector fixed by the batch.
- Run focused Rust regression tests for the changed behavior.
- Run targeted compatibility replays for nearby or impacted selectors when the
  change touches shared evaluator, reader, primitive, buffer, file, process, or
  byte-compiler behavior.
- Broaden to grouped file or prefix replays when the implementation risk is
  broad enough to justify it.
- Use full selected prefix replays strategically at larger milestones, after
  high-risk shared changes, or when a smaller targeted replay cannot establish
  confidence.

## Final Gates Before Commit

Run these commands before every pushed code-change commit:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
git diff --check
```

Also run the exact `compat-harness` replay commands for the selectors touched
by the batch and any targeted compatibility replays justified by the change.

If `cargo fmt --check` fails, run:

```sh
cargo fmt
```

Then rerun the final gates.

## Documentation Requirements

Update `docs/compatibility-goal.md` before committing:

- Current verified progress, for example `1786/7080`.
- Latest batch title.
- Next frontier selector, file, and observed mismatch.
- Exact replay commands run for the fixed selector.
- Exploratory command that identified the next frontier.
- Any important notes about targeted or broadened regression coverage.

Commit messages must include:

- The compatibility progress count.
- What behavior was fixed.
- Exact replay commands.
- Exploratory frontier command and result.
- Targeted regression tests.
- Final gate commands.

## Current Batch Context

The `Compat 2016/7080` run completed the eieio-tests directory in three
batches: the persist batch (slot descriptor protocol, record `equal',
hash-table reads, class-object tags), the groundwork batch (implicit
`eieio-default-superclass' parent activating eieio.el's real generic
methods, GNU slot-override merge + defclass validation, class-allocated
storage, typed `oset', record `aset', native slot machinery, plus the
`file-attributes' GNU time-list repair), and the finish batch
(cl-generic no-next/no-applicable hooks, in-place method re-registration,
`defgeneric' collision errors, exact `NAME-p'/`same-class-p'). The
83-file verified-prefix sweep is green. The next agent continues with
`test/lisp/emacs-lisp/ert-font-lock-tests.el` (selectors 2017..2056).
