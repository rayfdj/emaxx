# Eglot compatibility contract

This document records the permanent Eglot evidence added for issue #20.  It
does not claim that every external language server works.  It pins the built-in
Eglot test surface and exercises representative interactive LSP journeys over
a real JSON-RPC subprocess.

## Authoritative upstream replay

The success gate uses GNU Emacs 30.2 at source commit
`636f166cfc86aa90d63f592fd99f3fdd9ef95ebd`, with native compilation enabled,
and selector `(not (or (tag :expensive-test) (tag :unstable)))`.
The unchanged upstream test file produces **45 passes, zero failures and seven
skips on both editors on macOS**. Matching failures do not satisfy this gate.

Run it with a built Emaxx and the configured GNU source tree:

```sh
rustup toolchain install 1.75.0 --profile minimal --component rust-analyzer --component rust-src
python3 tools/eglot_gate.py --output target/eglot-success
```

`--source`, `--oracle`, `--subject`, `--rust-bin`, and `--clangd` can select
explicit installations. The gate records input hashes, binary hashes, tool
versions and individual test results. It leaves the frozen manifest and
upstream fixtures unchanged. Linux runs the same gate in
[the compatibility workflow](../.github/workflows/module-eglot.yml).

### Why the GNU baseline failed

The earlier frozen environment had six failures even in GNU:

- Five Rust tests stopped at `cargo init`: the mise shim could not select a
  toolchain after the fixture changed `XDG_CONFIG_HOME`. The gate supplies
  direct toolchain executables to both editors.
- `eglot-test-lsp-abiding-column` expected a UTF-16 column of 71, but Apple
  clangd negotiated UTF-32 and returned 51. The gate starts the real clangd
  with `--offset-encoding=utf-16`, identically for both editors.

Running the Rust fixtures also required canonical temporary paths outside the
checkout. On macOS, `/var` aliases `/private/var`; mixing these spellings makes
Eglot miss diagnostic and watched-file matches. Temporary projects inside the
Emaxx checkout instead inherit its Git/Cargo project. The gate creates and
canonicalizes an external temporary directory before launching either editor.

The pinned fixture expects rust-analyzer's `rustAnalyzer/Indexing` progress
notification. Current rust-analyzer reports `rustAnalyzer/cachePriming` with
different cancellation metadata. Rust 1.75 supplies the protocol this fixture
expects. This is a test prerequisite, not a runtime restriction on servers.

### Emaxx fixes exposed by the working fixtures

- Native unwind cleanup now publishes interpreted cons mutations before native
  execution resumes. Otherwise JSON-RPC could reuse an old message length and
  parse the next message body as a header.
- Parsed JSON strings are mutable multibyte Lisp strings, as in GNU. Completion
  metadata attached to a string now survives, so the Rust completion exit
  function applies the server's edit instead of inserting the display label.

All six formerly failing tests must pass. The seven explicitly allowed skips
cover unavailable Java, JavaScript, JSON and TypeScript servers, Windows URI
behavior on Unix, and snippet packages (`yasnippet`/`company`). A missing Rust
or clangd prerequisite is a gate failure, not an allowed skip.

## Interactive TTY comparison

[`tools/fixtures/fake_lsp_server.py`](../tools/fixtures/fake_lsp_server.py) is a
small deterministic stdio LSP server.  It reads and writes ordinary
Content-Length-framed JSON-RPC and derives diagnostics and edits from the
opened document.  It has no GNU/Emaxx branches and production runtime code
contains no fixture responses.  The server itself does hardcode the
scenario's payload word ("alpha"/"alphaValue" and the fixture warning
text): those literals are served identically to both editors, so nothing
is masked, but they are fixture content, not derived output.  A known
structural blind spot: the server ignores LSP `position` parameters and
advertises full-document sync, so an Emaxx bug confined to position or
column arithmetic would produce identical screens on both editors and
these journeys could not surface it. The upstream
`eglot-test-lsp-abiding-column` success gate provides additional coverage.

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
mechanisms exercised above, at whole-document granularity (see the
position-parameter blind spot noted with the fixture server).  It does not substitute for tests of third-party
`lsp-mode`, Magit, uninstalled language servers, or Windows URI behavior.  Those gaps remain explicit rather than being
filled with prerecorded responses, normalized asynchronous events, or
environment-specific success expectations.
