# Completion-stack compatibility contract

This document records the permanent package and interactive terminal evidence
for issue #37.  Vertico, Consult, and Corfu are third-party packages, so the
gate installs their pinned releases and complete external dependency closure
through ordinary `package.el` operations in both GNU Emacs 30.2 and Emaxx.

## Pinned package installation

[`tools/completion_stack_package_gate.py`](../tools/completion_stack_package_gate.py)
constructs a disposable local archive from these exact release artifacts:

- Vertico 2.13, SHA-256
  `3ac95cd8f9159670b0fbbb7a3f1cfb0c0a9f44c437e44482106837334b422c3a`
- Consult 3.7, SHA-256
  `63f1724728fa7fbcab315e1aef2cf13d647774374b97fb27e8f862d528dbb1a7`
- Corfu 2.14, SHA-256
  `c6ec346e5666badce80e693ba7fbb9c0e0e02627c200b570f255ba84a4d91aa8`
- Corfu Terminal 0.7, SHA-256
  `946a63459c7255d0df7ebad0170f2b56f20c3bc798efc704b63146a2aa838128`
- Popon 0.13, SHA-256
  `abcda58b0bbfe3998140a47200ba0a1ef9ebe62dbccede192adcb50b863c157c`
- Compat 31.0.0.2, SHA-256
  `47d8693a10087f8b20c72e6a78b628db980cb7547c4f8f517fc5d11acd8b0f38`

Every downloaded, cached, and copied artifact is rehashed before use.  The
gate also checks release-matched source archives, independently pinned to
Consult commit `3ddec5493bce5445f099537be50b7a4f79c68321` (SHA-256
`666a663df5087d64ad44de732fd41bb6982f25bb88df776009dd8208c09f5c80`),
Corfu commit `75be36fe63e78c63ac71c32039ab07836bd532ac` (SHA-256
`161cc504e13d0870af38207a54f70ef6c7bb001eb56b0cba0db3a7e1e01092c7`),
Corfu Terminal commit `501548c3d51f926c687e8cd838c5865ec45d03cc` (SHA-256
`88402635bf4d967dba0238baed5a2a6a370591c730d6ba05de2be4680d33e334`),
Popon commit `bf8174cb7e6e8fe0fe91afe6b01b6562c4dc39da` (SHA-256
`5f7c3d31dd69370db031ebacb45432daa3dcce7827d9a77783772ed1d94c5978`),
and Vertico commit `a6874e3d8c74a9eea77967d702d608ebbd6b27ec` (SHA-256
`cbb94a61a490b6f1aba4a9f6441bbee7fad22a6731607fe7fa09917b34b07433`).
Those five source revisions contain no applicable upstream test files.  The
gate verifies that inventory and fails closed if a pinned source archive gains
one; it does not replace missing upstream tests with claimed package results.

GNU Emacs and Emaxx receive separate empty user/package roots.  Each reports
Lisp `emacs-version` exactly `30.2`, refreshes the same local archive, computes
the real dependency transaction, and calls `package-install`.  The exact
transaction and installed inventory are Compat 31.0.0.2, Consult 3.7, Corfu
2.14, Corfu Terminal 0.7, Popon 0.13, and Vertico 2.13.  The gate requires all
41 expected `.elc` files and all six generated autoload files.

Both editors then restart.  Every required feature must resolve from installed
bytecode inside its fresh package root, and the complete install/restart record
maps must match exactly.  Run the optimized, offline package and TTY contract
with:

```sh
python3 tools/completion_stack_package_gate.py \
  target/gate/emaxx /path/to/emacs-30.2 \
  --artifact-dir target/completion-stack-package-gate/artifacts \
  --offline --tty --gnu-lisp-dir /path/to/emacs-30.2/lisp
```

Omit `--offline` only for the first artifact download.  Run the structural and
anti-cheat regressions with:

```sh
python3 -m unittest \
  tools/test_completion_stack_package_gate.py tools/test_ttydiff.py
```

## Interactive terminal comparison

The package gate supplies the two fresh installed roots to four strict
[`tools/ttydiff.py`](../tools/ttydiff.py) journeys.  Every named action is an
exact text, terminal-attribute, mode-line, echo-area, and cursor checkpoint:

- `stack-vertico` opens a real `completing-read`, moves selection, filters,
  accepts, and verifies the returned value plus minibuffer cleanup.
- `stack-consult-line` filters a 30-line buffer, previews the next matching
  line with live scrolling, accepts it, and verifies point, line text, and
  minibuffer cleanup.
- `stack-consult-grep` starts Consult's real asynchronous grep subprocess over
  isolated same-named files, previews the next result, accepts it, and verifies
  the visited file, line, text, and minibuffer cleanup.
- `stack-corfu` installs a deterministic CAPF, opens Corfu through the official
  terminal frontend, previews the next candidate, inserts it, and verifies the
  resulting buffer text and popup cleanup.

The journeys do not normalize completion output, replace process objects, or
dispatch on editor identity.  Absolute-path-sensitive grep inputs live in
separate same-shaped roots so GNU and Emaxx cannot observe one another's state;
the visible prompt and result basenames remain identical.

## Emacs 30.2 terminal boundary

This is specifically an Emacs 30.2 terminal contract.  Corfu's built-in
terminal child-frame support belongs to Emacs 31, so claiming it here would be
incorrect.  The gate instead installs and exercises the official Corfu
Terminal 0.7 frontend and its Popon 0.13 dependency.  Compat's version number
also does not change the tested editor: both runtimes are required to report
`30.2`.

## Scope boundary

This contract covers the pinned package releases, dependency/install/restart
lifecycle, and the four interactive terminal journeys above on the pinned
Darwin GNU Emacs 30.2 oracle.  It does not claim every Vertico extension,
Consult source/command, Corfu frontend, graphical child frame, remote search,
completion style, or platform.  Those require additional deterministic oracle
journeys rather than inference from this gate.
