# TRAMP compatibility gate

`tools/tramp_compat_gate.py` runs the same remote-editing journey through GNU
Emacs 30.2 and Emaxx, serially, and compares their structured records exactly.
The default transport is TRAMP's `mock` method: an ordinary interactive shell
and the real GNU TRAMP protocol, but confined to a fresh local temporary tree.
It does not open a network connection.

Build the optimized subject and run the deterministic journey from the
repository root:

```sh
cargo build --profile gate --bin emaxx
python3 tools/tramp_compat_gate.py \
  --gnu /path/to/emacs-30.2/src/emacs \
  --emacs-source /path/to/emacs-30.2
```

The journey covers visit/save/revert, listing and completion, copy/rename/
delete, metadata, nearby temporary files, synchronous and asynchronous remote
processes, connection reuse and cleanup/reconnect, missing-file errors, and
integration with file-name handlers, Dired, project.el, VC, and compilation.
Password/authentication behavior is additionally pinned by the selected
upstream `tramp-test47-*` comparisons; asynchronous cancellation and signal
handling are pinned by `tramp-test31-*` and `tramp-test45-*`.

Each invocation writes a JSON artifact under `target/tramp-compat-gate/` unless
`--report PATH` is supplied. A GNU failure remains a failure and prevents the
subject from starting, unexpected output is a protocol error, and record values
are not normalized or accepted through retries. Raw stderr is retained; blank
TRAMP progress lines and compilation's success line are allowed, while every
other warning or diagnostic fails the run.

## Opt-in real SSH canary

The runner cannot contact SSH merely because a remote-looking path was passed.
Both an explicit switch and a writable TRAMP root are required:

```sh
python3 tools/tramp_compat_gate.py \
  --gnu /path/to/emacs-30.2/src/emacs \
  --emacs-source /path/to/emacs-30.2 \
  --live-ssh \
  --remote-root '/ssh:user@example.net:/tmp/'
```

Authentication is whatever the ordinary local SSH/TRAMP configuration would
use; the canary does not disable host-key checks, inject credentials, or accept
an oracle failure. GNU runs first and exits before Emaxx starts. Each editor
creates and removes the same PID-suffixed remote directory, so no persistent
test payload is intended to remain.

The runner's offline unit tests never open the network:

```sh
python3 tools/test_tramp_compat_gate.py
```
