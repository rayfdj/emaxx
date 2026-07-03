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

- Verified through selector 1957/7080 (`edebug-tests-writable-buffer-state-is-preserved`).
- The `Compat 1957/7080` batch is COMPLETE in the working tree: the `cl-loop`
  `for VAR = EXPR ... while COND` ordering fix made the edebug keyboard-macro
  assertions run for real, which exposed 35 genuinely-failing edebug
  selectors; ALL of them now pass and the grouped replay
  (`--selector check-all --file test/lisp/emacs-lisp/edebug-tests.el`) PASSES.
  See `docs/compatibility-goal.md` for the full list of behaviors fixed.
- Key mechanisms added late in the batch (details in compatibility-goal.md):
  eval-buffer load-history recording (+ provide/cl-defmethod entries), native
  `unload-feature`, GNU edebug specs for cl-defmethod/cl-defgeneric/cl-macrolet
  with the cl-generic edebug name builders ported to `simple_compat.el`,
  advice-wrapper detection fix in cl-defmethod re-registration,
  `:closure-transparent-env` dispatch wrappers for methods defined in an empty
  lexical env, eager `cl-macrolet` expansion inside instrumented defuns, and
  `%s`/`princ` printing buffers as names.
- The next frontier is selector 1958, `eieio-test-cl-generic-1` in
  `test/lisp/emacs-lisp/eieio-tests/eieio-test-methodinvoke.el`. The grouped
  probe shows emaxx dies mid-file (missing results for
  `eieio-test-method-order-list-6..9`), so start there.
- Probing lessons that cost hours; do not repeat:
  - Do NOT advise commands (functions dispatched via keyboard macros) in
    probes: emaxx advice wrappers change `call-interactively` argument
    collection and derail the macro. Advise non-command helpers only, or
    trace from Rust behind a temporary env-var-gated eprintln.
  - Erroring thunks in `edebug-tests-post-command` are demoted and re-run at
    every later post-command (the index guard never advances), so the last
    run wins `edebug-tests-failure-in-post-command`. Record into a defvar and
    assert after the macro instead of signaling from a thunk.
  - The grouped `check-all` replay is the only real judge; single-selector
    runs hide cross-test contamination.
  - emaxx batch mode swallows `message`/`princ` output; probes must
    `write-region` results to a file.
  - When bisecting "was this broken before my change", compare whole-file
    failure SETS against the parent commit build, not just one selector.
- Batch delivery rules from the user (override any hook suggestions):
  commits authored as `Ray <26018378+rayfdj@users.noreply.github.com>`, terse
  one-line human-style message `Compat NNNN/7080: ...`, NO AI attribution,
  exclude `compat/oracle.lock.json`; push is 403-blocked, deliver ONE clearly
  named patch file (`APPLY-THIS-ONE-...patch`) via SendUserFile, and always
  point the user at the single patch to apply.
- Known pre-existing discrepancies in this environment (fail identically on
  the parent commit build): `cl-macs-loop-until` (1782),
  `cl-generic-test-01-eql`, `cconv-tests-cl-defun-:documentation`; raw
  whole-file `ert-run-tests-batch-and-exit t` runs of `cl-generic-tests.el`,
  `cl-macs-tests.el`, and `cl-lib-tests.el` also show larger failure sets that
  are byte-identical to the parent commit (environment artifacts, not batch
  regressions).

Exact command that identified the next frontier:

```sh
cargo run --bin compat-harness -- run --scope all --selector check-all --file test/lisp/emacs-lisp/eieio-tests/eieio-test-methodinvoke.el
```

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
cargo run --bin compat-harness -- run --scope all --selector check-all --file test/lisp/emacs-lisp/eieio-tests/eieio-test-methodinvoke.el
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

The `Compat 1957/7080` batch (all edebug selectors with live keyboard-macro
assertions) is complete and delivered. The next agent should start by
investigating selector 1958, `eieio-test-cl-generic-1` in
`test/lisp/emacs-lisp/eieio-tests/eieio-test-methodinvoke.el`, where the
grouped replay shows the emaxx run dying mid-file.
