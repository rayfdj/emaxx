# lsp-mode compatibility contract

This document records the permanent package and TTY evidence added for issue
#22.  `lsp-mode` is not bundled with GNU Emacs or Emaxx; the gate installs the
pinned third-party release and its dependency closure through ordinary
`package.el` operations.

## Pinned package installation

[`tools/lsp_mode_package_gate.py`](../tools/lsp_mode_package_gate.py) builds a
disposable local archive from exact Stable MELPA and GNU ELPA release
tarballs.  Every downloaded, cached, and copied artifact is checked against
the SHA-256 digest recorded in the gate before `package.el` can use it.  The
archive contains:

- lsp-mode 10.0.0
- dash 2.20.0
- f 0.21.0
- ht 2.3
- lv 0.15.0
- markdown-mode 2.8
- s 1.13.1
- spinner 1.7.4

GNU Emacs and Emaxx receive separate empty user directories and run the same
`package-refresh-contents`, dependency transaction, `package-install`, and
`package-initialize` forms.  The gate requires the exact eight-package
transaction, the exact 159-file byte-compiled inventory, generated lsp-mode
autoloads, and identical records from both editors.  It then launches each
editor again, proves that `lsp` is an autoload before `require`, loads
lsp-mode 10.0.0, and verifies that every dependency resolves inside the fresh
installed package tree rather than a checkout or host package directory.

Run the package and interactive contract with release binaries:

```sh
python3 tools/lsp_mode_package_gate.py \
  target/release/emaxx /path/to/pinned/emacs \
  --artifact-dir target/lsp-mode-package-gate/artifacts \
  --offline --tty --gnu-lisp-dir /path/to/pinned/emacs/lisp
```

Omit `--offline` on the first run to download the pinned tarballs.  Hash
verification is mandatory in either mode.  Run the structural and anti-cheat
tests with:

```sh
python3 -m unittest \
  tools/test_lsp_mode_package_gate.py tools/test_ttydiff.py
```

## Interactive TTY comparison

The optional package-gate phase gives the two fresh installed trees to four
dedicated [`tools/ttydiff.py`](../tools/ttydiff.py) journeys:

- `lsp-mode-connect-diagnostics-completion-hover` starts a workspace, observes
  Flymake diagnostics, visits a diagnostic, completes through CAPF, and opens
  the hover buffer.
- `lsp-mode-xref-rename-edits` edits the document, follows an xref definition,
  performs a prompted rename, saves the result, and compares the final file
  bytes as well as the screen.
- `lsp-mode-reconnect-shutdown` restarts the real workspace process, verifies
  its live process object, shuts it down, and verifies that no workspace
  remains.
- `lsp-mode-ui-buffers` compares the tree-widget session browser, the genuine
  JSON-RPC log buffer, and log navigation.

The client uses lsp-mode's public registration and stdio connection paths to
launch [`tools/fixtures/fake_lsp_server.py`](../tools/fixtures/fake_lsp_server.py)
as an ordinary subprocess.  The server speaks framed JSON-RPC and shares the
deterministic protocol fixture already used by the Eglot contract; production
code has no fixture or lsp-mode branch.

Mutable journeys use isolated same-named files so one editor cannot observe
the other's writes.  Read-only reconnect and UI journeys share one fixture,
which keeps genuine absolute-path status text comparable without rewriting
screen output.  The harness fixes clocks, process identifiers, and Emacs build
metadata on both sides because lsp-mode deliberately renders those
OS/build-assigned presentation inputs.  It does not replace process objects,
server traffic, package APIs, or command results.

Non-checkpoint actions only prepare the next strict observation: connection
setup is checked by the workspace/diagnostic query, inserted prefixes by the
completion result, searches by hover/xref buffers, the rename prompt by the
rename result, and save by the buffer-plus-filesystem assertion.  The session
browser itself, the log buffer, and every state-bearing final command are exact
text, attribute, and cursor comparisons.

## Scope boundary

This is a smoke-level contract for lsp-mode 10.0.0 on the pinned Darwin GNU
Emacs oracle.  It covers the package lifecycle and command families above.  It
does not claim full lsp-mode compatibility, every optional lsp-mode extension,
remote/TRAMP workspaces, every language client, semantic tokens, lenses,
debug adapters, or real language-server/toolchain drift.  Those need separate
deterministic oracle journeys rather than inference from package loading.
