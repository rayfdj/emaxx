# emaxx

An Emacs reimplementation in Rust, aiming for 100% behavioral compatibility at the Elisp API boundary.

## Building

```
cargo build
```

## Testing

Unit tests run standalone:

```
cargo test --lib
```

The Rust integration tests are smoke coverage only. They still run actual Emacs `.el` test files (ERT) against the Rust implementation, and they expect a checkout of the Emacs source tree as a sibling directory:

```
../emacs/          # git clone https://git.savannah.gnu.org/git/emacs.git
../emaxx/          # this project
```

Then:

```
cargo test
```

The authoritative compatibility harness drives both a pinned real Emacs oracle and `emaxx` through near-matching batch invocations.

For a feature-rich local Emacs oracle build on Homebrew macOS, there is a helper script at `compat/build_emacs_homebrew.sh`.

Pin the oracle once:

```
cargo run --bin compat-harness -- oracle pin --emacs /path/to/emacs --repo ../emacs
```

Then list or run compatibility coverage:

```
cargo run --bin compat-harness -- selectors
cargo run --bin compat-harness -- list --scope all --selector default
cargo run --bin compat-harness -- run --scope all --selector default
```

Named selectors mirror upstream make/ERT usage for the pinned oracle, including `default`, `expensive`, `all`, `check`, `check-maybe`, `check-expensive`, and `check-all`. You can also pass any literal ERT selector expression to `--selector`.

Artifacts are written under `target/compat/`. The tracked oracle lock lives at `compat/oracle.lock.json`, while the local machine-specific oracle config is written to `compat/oracle.local.json`.

If the `emacs` directory isn't found, the smoke integration tests will fail with a clear error. The Emacs tests are never copied into this repo — they're always read from the sibling checkout so we stay in sync with upstream.
