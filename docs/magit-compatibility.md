# Magit compatibility contract

This document records the permanent package and TTY evidence added for issue
#21.  Magit is not bundled with GNU Emacs or Emaxx; the gate installs the
pinned third-party release and its external dependency closure through the
ordinary `package.el` lifecycle.

## Pinned package installation

[`tools/magit_package_gate.py`](../tools/magit_package_gate.py) constructs a
disposable local package archive from exact GNU ELPA and NonGNU ELPA release
tarballs.  Every downloaded or cached artifact is checked against the SHA-256
record in the gate before `package.el` can see it.  The archive contains:

- Magit 4.7.0 and magit-section 4.7.0
- compat 31.0.0.2
- cond-let 1.1.3
- llama 1.0.5
- seq 2.24
- transient 0.13.7
- with-editor 3.5.3

GNU Emacs and Emaxx receive separate empty user directories and execute the
same `package-refresh-contents`, dependency transaction, and `package-install`
forms.  The gate requires the exact seven-package external installation
closure (the bundled `seq` satisfies that dependency), the exact 58-file
byte-compiled inventory, generated Magit autoloads, and equal records from
both editors.  It then starts each editor again, requires Magit 4.7.0, and
proves that every library in the external closure resolves inside the new
installed package tree rather than a source checkout or host package
directory.

Run the package and interactive contract with release binaries:

```sh
python3 tools/magit_package_gate.py \
  target/gate/emaxx /path/to/pinned/emacs \
  --artifact-dir target/magit-package-gate/artifacts \
  --offline --tty --gnu-lisp-dir /path/to/pinned/emacs/lisp
```

Omit `--offline` for the first run to download the pinned release tarballs.
The hashes are mandatory in either mode.  Run the structural and anti-cheat
tests with:

```sh
python3 -m unittest tools/test_magit_package_gate.py tools/test_ttydiff.py
```

## Interactive TTY comparison

The optional package-gate phase passes the two fresh installed trees to four
dedicated [`tools/ttydiff.py`](../tools/ttydiff.py) scenarios:

- `magit-status-sections-stage` covers status rendering, staged, unstaged, and
  untracked state, stage/unstage, section expansion/collapse, and refresh.
- `magit-diff-log-transient` opens and selects real Diff and Log transient
  suffixes, checks their dismissal into the requested buffers, navigates a
  diff section and a log entry, and refreshes the log.
- `magit-process-error` runs a real invalid Git subcommand, checks its visible
  error, opens the process buffer, and returns to status.
- `magit-repository-not-found` checks the repository-creation prompt, declines
  it, and then strictly verifies that no `.git` directory or Magit top-level
  was created.

The mutable journeys use separately rooted but same-named deterministic Git
repositories.  The fixture generator clears host Git configuration and fixes
identity, commit dates, branches, commits, modes, staged files, unstaged files,
and untracked files.  Screen text, terminal attributes, and cursor positions
are compared without Magit-specific normalization.  The read-only
repository-not-found journey intentionally shares one target so the visible
absolute path is identical; both editors decline creation, so neither may
mutate it.

The search used to place point on the unstaged file is the only non-checkpoint:
it prepares the following stage command, whose screen is compared exactly.
Stage/unstage state is also checked by Magit itself after each mutation.  The
repository-decline keystroke is an exact screen checkpoint, preceded by an
exact prompt comparison and followed by an exact no-repository state
assertion.  No outcome is inferred from a skipped screen comparison.

## Scope boundary

This contract covers Magit 4.7.0 on the pinned Darwin GNU Emacs oracle and the
package and interactive mechanisms listed above.  It does not claim parity for
every Magit command, forge integration, remote repository, credential flow,
submodule, worktree, or platform.  Those require their own deterministic
oracle journeys rather than environment-dependent success assumptions.
