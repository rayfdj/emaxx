# Terminal frames and server clients

This work starts at main `450cb77` on branch `terminal-frame-parity`.
The behavioral reference is the pinned GNU Emacs 30.2 checkout in
`../emacs`, not an alternative server implementation. GNU's `server.el`
and the seven tests in `test/lisp/server-tests.el` are unchanged.

## C-owned behavior implemented

- `frame.c:make_terminal_frame` and `Fmake_terminal_frame`: open or reuse
  the named terminal, create a separate window tree and minibuffer window,
  prepend the frame, leave selection alone, and copy frame-local faces.
- `terminal.c` and `frame.c:delete_frame`: terminal identity and parameters,
  selection of a surviving frame, window invalidation, device closure,
  protected deletion hooks, and deferred `noelisp` hooks. The pending calls
  drain in `keyboard.c:timer_check_2` order through `eval.c:safe_funcall`.
- `window.c`: owning-frame identity, per-frame selected/root windows,
  window enumeration, saved configurations, and independent split/delete
  operations. Restoring a configuration retains its original selected
  frame, including the `dont-set-frame` alternative.
- `xfaces.c`: per-frame face vectors and hash tables, including copying
  between frames. `keyboard.c:init_kboard` and `eval.c:specbind`: separate
  keyboard variables and maps, and unwind restoration on the keyboard
  where a dynamic binding was established.
- `term.c:init_tty`, `term_get_fkeys_1`, and `sysdep.c:init_sys_modes`: real
  device validation, system termcap lookup, key mappings, terminal size,
  interactive input modes, and restoration when the device closes.
  Batch creation does not put the device into raw mode.

The existing renderer writes to the selected terminal. Device input enters
the ordinary Lisp keyboard path with switch-frame events. Keyboard and
terminal coding systems belong to each terminal. Partial UTF-8 and termcap
key sequences survive separate device reads. Closing a client device
retires its frames and leaves another terminal usable.

Live terminal descriptors cannot be copied into an interpreter image.
Frame windows, frame face values, terminal keyboard/parameter values, and
pending deletion calls participate in GC marking and image copying.

## Reproducible verification

`cargo test --profile gate --lib terminal_frames_ -- --test-threads=1`
runs three contracts against the pinned GNU binary and Emaxx. They cover
device reuse, frame and window lifetime, independent window trees, face
copying, keyboard isolation and dynamic unbinding after a frame switch,
and window-configuration restoration.

`python3 tools/terminal_frame_gate.py` runs the editors serially with real
PTYs, private Unix sockets, and GNU's `emacsclient`. It checks primary and
client input isolation, split arrow-key and UTF-8 input, display routing,
saving through `C-x C-s`, normal client-frame deletion, and abrupt client
disconnection. Raw terminal transcripts and decoded screens are retained.
GNU starts with `-nw`; Emaxx's interactive frontend is already a TTY and
its CLI does not yet accept that switch.

The unchanged server suite runs with:

```
TERM=xterm target/gate/emaxx -Q --batch \
  -l ../emacs/test/lisp/server-tests.el \
  -f ert-run-tests-batch-and-exit
```

## Checkpoint results

The final binary passes all seven unchanged server tests, as does the
pinned GNU oracle. The three new terminal contracts pass, along with the
corrected existing popup, quit-window, and face-registry contracts.
Formatting, strict all-target/all-feature Clippy, and the 22 automated
source audit checks pass.

The final real-client comparison also passes in both editors: separate
input/screens, fragmented key and UTF-8 input, save, normal client closure,
and abrupt disconnection. Artifacts are retained at
`/private/tmp/emaxx-terminal-interactive-final`.

After merging native-comp `c5ec1b8` and correcting the original 13 failures,
final verification again passes all seven unchanged server tests in each
editor and the complete real-client comparison. Current artifacts are
`/private/tmp/ex-final/server-{gnu,emaxx}.log` and
`/private/tmp/emaxx-native-merge-terminal-final`. A preceding server attempt
used a temporary path too long for GNU's Unix socket; that setup failure
is retained separately in the audit ledger.

Rust coverage totals 2,564 successful library cases (plus two existing
TTY ignores), 42 binary tests, and 24 integration tests across completed
runs and targeted corrections. Previously passing evaluator groups were
retained at the user's direction; this is not a fresh single-run full-gate
pass. Formatting and strict all-target/all-feature Clippy pass without
warnings. See [the audit ledger](honesty-audit-2026-08-18.md) for the original
failures, adversarial corrections, and exact coverage provenance.

## Limits of the claim

The server's three previously failing frame tests are the scope of this
checkpoint. Passing them does not establish universal terminal parity or
a new result for the full 7,883-test corpus. In particular, terminal
suspend/resume and output-buffer controls still need their GNU ports;
redisplay still services the selected terminal rather than repainting
every terminal's top frame after shared-buffer changes. The frontend uses
its existing ANSI renderer, so arbitrary legacy termcap output and padding
are not established by the xterm interactive test. Streaming input in
multibyte encodings other than UTF-8 needs further work. These limitations
remain visible; the server tests are not skipped or replaced to hide them.

Thread scheduling and dumping remain separate workstreams.
