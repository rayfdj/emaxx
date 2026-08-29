# Live package archive canary

`tools/package_live_canary.py` is an explicitly opt-in comparison of GNU
Emacs and Emaxx against GNU ELPA, NonGNU ELPA, and MELPA. It is not part of
the deterministic test gate and cannot open the network unless `--live` is
present.

Build an optimized Emaxx binary and run it from the repository root:

```sh
cargo build --profile gate --bin emaxx
python3 tools/package_live_canary.py --live
```

Every editor/phase gets the same archive URLs and a fresh, isolated `HOME`
and `package-user-dir`. Both editors refresh through ordinary
`package-refresh-contents`, then install the pinned archive descriptors,
resolve dependencies, exit, restart, activate and load the features, remove
every installed package, and restart once more to prove removal persisted.

The current defaults are:

| Archive | URL | Package | Feature | Pinned version |
| --- | --- | --- | --- | --- |
| GNU ELPA | `https://elpa.gnu.org/packages/` | `compat` | `compat` | `31.0.0.2` |
| NonGNU ELPA | `https://elpa.nongnu.org/nongnu/` | `rainbow-delimiters` | `rainbow-delimiters` | `2.1.5` |
| MELPA | `https://melpa.org/packages/` | `ht` | `ht` | `20230703.558` |

The `ht` transaction also exercises dependency installation through `dash`.
If an archive legitimately advances a pin, pass exactly one replacement for
each archive, for example:

```sh
python3 tools/package_live_canary.py --live \
  --target gnu:compat:compat:31.0.0.2 \
  --target nongnu:rainbow-delimiters:rainbow-delimiters:2.1.5 \
  --target melpa:ht:ht:20230703.558
```

Each invocation writes a JSON artifact under `target/package-live-canary/`
unless `--report PATH` is supplied. The report retains raw stdout/stderr and
records:

- archive URLs and exact `archive-contents` SHA-256 values;
- archive signature presence and signature-file SHA-256 values;
- selected versions, package kinds, dependency requirements, and computed
  transactions;
- installed source-tree SHA-256 values and package signature evidence;
- the exact installed `.elc` inventory, so a silently failed package compile
  cannot pass merely because Emacs falls back to loading its source file;
- Emacs, OS, GnuTLS, TLS-program, GPG, network-security, and signature-policy
  assumptions for both editors;
- exact GNU/Emaxx record mismatches and one failure category.

`network_or_archive` means the GNU oracle could not complete the same live
phase on the same host. `archive_drift` means the editors received different
metadata/payloads or a committed pin moved. `emaxx_behavior` means GNU
completed with the pinned inputs but Emaxx did not, or their lifecycle records
diverged. TLS, signature, missing metadata, dependency, activation, and load
errors are fatal; the canary never disables signature checking or substitutes
another archive. Removal must clear both package activation state and every
versioned installation directory, before and after the final restart.

The offline unit test is safe for ordinary gates:

```sh
python3 tools/test_package_live_canary.py
```
