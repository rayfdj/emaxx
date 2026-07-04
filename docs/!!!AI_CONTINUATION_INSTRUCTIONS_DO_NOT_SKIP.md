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

- Verified through selector 2056/7080: `ert-font-lock-tests.el` (selectors
  2017..2056) passes its grouped `check-all` replay; the 84-file
  verified-prefix sweep (now including ert-font-lock-tests.el) is green
  (2026-07-04).
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
- The next frontier is `test/lisp/emacs-lisp/ert-x-tests.el` (selectors
  2057..2084, 28 selected; manifest line 2142 of
  `compat/oracle_tests_all.txt`).  Start with the grouped `check-all`
  replay to enumerate the real mismatches.
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
cargo run --bin compat-harness -- run --scope all --selector check-all --file test/lisp/emacs-lisp/ert-x-tests.el
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
cargo run --bin compat-harness -- run --scope all --selector check-all --file test/lisp/emacs-lisp/ert-x-tests.el
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
