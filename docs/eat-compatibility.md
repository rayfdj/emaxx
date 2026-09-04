# Eat compatibility contract

This document records the permanent package and real-subprocess evidence for
issue #39.  Eat is not bundled with GNU Emacs or Emaxx; the gate installs the
official Eat 0.9.4 NonGNU ELPA release through the ordinary `package.el`
lifecycle and compares its observable behavior with the pinned GNU Emacs 30.2
oracle.

## Pinned package installation

[`tools/eat_package_gate.py`](../tools/eat_package_gate.py) builds a disposable
local package archive from three exact release artifacts.  Every downloaded or
cached file is checked against its SHA-256 record before `package.el` or the
test extractor can read it:

- Eat 0.9.4 package tar, SHA-256
  `14971fc562f0820794eb6af78beebc7dc3ba898221e785c2d272a9f0fccfc54a`
- Eat v0.9.4 source tar, commit
  `c91451f2d17453c19d3fa76faa4945cbe54e14ce`, SHA-256
  `32a2793c1f203bf2e0fe67f79310c2389257e1338b191e017ea60dc68000c01a`
- Compat 31.0.0.2 package tar, SHA-256
  `47d8693a10087f8b20c72e6a78b628db980cb7547c4f8f517fc5d11acd8b0f38`

GNU Emacs and Emaxx receive separate empty user and package directories.  Each
reports Lisp `emacs-version` exactly `30.2`, refreshes the same local archive,
computes Eat's real dependency transaction, and calls `package-install`.
Emacs 30.2 already satisfies Eat's `compat >= 29.1` requirement, so the
fail-closed ordinary transaction and installed inventory each contain only
`eat-0.9.4`; the Compat release remains present and pinned in the archive so
dependency resolution is genuine rather than edited out.  The gate requires
the exact two-file compiled inventory (`eat.elc` and `term/eat.elc`) and the
generated autoload file.

Both editors are then restarted.  The gate proves that `require` resolves
`eat-0.9.4/eat.elc` inside the editor's new package tree, extracts the unedited
`eat-tests.el` from the pinned upstream source tar, requires exactly the 57
`eat-test-*` tests in that file, and requires all 57 to complete as expected
with zero unexpected results.

Run the contract with optimized binaries:

```sh
python3 tools/eat_package_gate.py \
  target/gate/emaxx ../emacs/src/emacs \
  --artifact-dir target/eat-package-gate/artifacts
```

After the first download, add `--offline` to prove that only the cached,
rehashed artifacts are used.  Run the harness and anti-cheat regressions with:

```sh
python3 -m unittest tools/test_eat_package_gate.py
```

## Real subprocess comparison

The same editor-neutral [`tools/eat_process_gate.el`](../tools/eat_process_gate.el)
is loaded after the clean restart in both editors.  It uses Eat itself to drive
three real PTY sessions and emits structured records only after checking:

- a deterministic `/bin/sh -c` child: input, a 100-by-40 resize visible both
  to Eat and `stty`, cursor position, SGR red text, alternate-screen removal,
  200 scrollback rows, Ctrl-D EOF, exit status 7, and writable-buffer/process/
  terminal cleanup;
- a real foreground `/bin/sleep 30`: Ctrl-C delivery through Eat, signal 2,
  and complete process/terminal cleanup;
- a real interactive `/bin/sh -i`: command input, arithmetic output, SGR
  magenta text, exit status 3, and complete cleanup.

The Python driver pins the complete expected records and also requires GNU
Emacs and Emaxx to emit identical maps.  The Lisp file contains no editor
dispatch, expected-output branch, direct process shortcut, or reduced terminal
fixture.

## Scope boundary

This contract covers Eat 0.9.4 on the pinned Darwin GNU Emacs 30.2 oracle,
upstream's full 57-test inventory, and the PTY behaviors listed above.  It does
not claim parity for every shell, remote host, graphical input event, terminfo
database, Eshell integration, or platform.  Those require separate
deterministic oracle journeys rather than extrapolation from this gate.
