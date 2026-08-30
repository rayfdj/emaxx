# Eglot compatibility contract

This document records the permanent Eglot evidence added for issue #20.  It
does not claim that every external language server works.  It pins the built-in
Eglot test surface and exercises representative interactive LSP journeys over
a real JSON-RPC subprocess.

## Authoritative upstream replay

The 2026-08-30 replay used the repository's pinned Darwin oracle:

- GNU Emacs 30.2, source commit
  `636f166cfc86aa90d63f592fd99f3fdd9ef95ebd`
- native compilation enabled
- selector `(not (or (tag :expensive-test) (tag :unstable)))`

The release-profile compatibility harness command was:

```sh
target/gate/compat-harness run --scope all --selector default \
  --file test/lisp/progmodes/eglot-tests.el --timeout-seconds 300
```

All 52 selected outcomes match GNU exactly: 39 pass, 6 fail, and 7 skip on
both editors.  The matching failures and skips are part of the contract; they
must not be relabelled as Emaxx successes.

The six matching failures are:

- `eglot-test-lsp-abiding-column`: the test's oracle assertion expects 71 but
  the pinned GNU run produces 51.
- `eglot-test-project-wide-diagnostics-rust-analyzer`
- `eglot-test-rust-analyzer-hover-after-edit`
- `eglot-test-rust-analyzer-watches-files`
- `eglot-test-rust-completion-exit-function`
- `eglot-test-rust-on-type-formatting`

The five Rust failures all stop at the fixture's `cargo init`, which returns
status 1 in this environment.  Emaxx preserves the same outcome and condition;
it does not bypass the missing toolchain setup.

The seven matching skips are:

- `eglot-test-eclipse`: `jdtls` is unavailable.
- `eglot-test-javascript`: `typescript-language-server` or `tsserver` is
  unavailable.
- `eglot-test-json`: `vscode-json-languageserver` is unavailable.
- `eglot-test-path-to-uri-windows`: the oracle host is Darwin, not Windows.
- `eglot-test-project-wide-diagnostics-typescript`: the TypeScript language
  tools are unavailable.
- `eglot-test-snippet-completions`: `yas-minor-mode` is unavailable.
- `eglot-test-snippet-completions-with-company`: `yas-minor-mode` or `company`
  is unavailable.

The available native server was also recorded, rather than silently assumed:

```text
Apple clangd version 21.0.0 (clang-2100.1.1.101)
Features: mac+xpc
Platform: arm64-apple-darwin25.6.0
```

## Interactive TTY comparison

[`tools/fixtures/fake_lsp_server.py`](../tools/fixtures/fake_lsp_server.py) is a
small deterministic stdio LSP server.  It reads and writes ordinary
Content-Length-framed JSON-RPC and derives diagnostics and edits from the
opened document.  It has no GNU/Emaxx branches and production runtime code
contains no fixture responses.

Three default [`tools/ttydiff.py`](../tools/ttydiff.py) scenarios drive the
same server through GNU and Emaxx in separately rooted, same-named projects:

- `eglot-connect-diagnostics-completion-hover`
- `eglot-xref-rename-edits`
- `eglot-reconnect-shutdown`

Together they cover connection, diagnostics, completion, hover/Eldoc,
definition/xref, rename and workspace edits, save-to-disk, unexpected process
death and automatic reconnect, orderly shutdown, and stopped-server state.
Screens, cursors, attributes, and requested filesystem snapshots are compared
without LSP-specific normalization.

Run the protocol and scenario structure tests with:

```sh
python3 -m unittest tools/test_fake_lsp_server.py tools/test_ttydiff.py
```

Run the interactive contract with a release Emaxx binary and the pinned GNU
binary using the normal `ttydiff.py` invocation described by its `--help`,
selecting the three names above.

## Scope boundary

This contract covers the Eglot shipped with the pinned GNU tree and the LSP
mechanisms exercised above.  It does not substitute for tests of third-party
`lsp-mode`, Magit, uninstalled language servers, Windows URI behavior, or the
five unavailable Rust fixtures.  Those gaps remain explicit rather than being
filled with prerecorded responses, normalized asynchronous events, or
environment-specific success expectations.
