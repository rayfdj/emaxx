# emaxx

An Emacs reimplementation in Rust, aiming for 100% behavioral compatibility at the Elisp API boundary.

## Building

```
cargo build
```

## Testing

There are three different test layers in this repo, and they serve different purposes:

- `cargo test --lib`: fast Rust unit tests for individual subsystems.
- `cargo test`: unit tests plus a small permissive smoke layer that runs a few upstream Emacs `.el` files through `emaxx`.
- `cargo run --bin compat-harness ...`: the authoritative oracle-backed compatibility runner that compares `emaxx` against a pinned real Emacs binary.

The detailed guide lives in [docs/testing.md](docs/testing.md).

### Unit Tests

Rust unit tests run standalone:

```
cargo test --lib
```

These are the fastest checks. They validate individual Rust modules like the reader, evaluator, buffer logic, overlay logic, and compatibility-harness helpers, but they do not compare against a real Emacs oracle.

### Smoke Integration Tests

The Rust integration tests are smoke coverage only. They still run actual Emacs `.el` test files (ERT) against the Rust implementation, and they expect a checkout of the Emacs source tree as a sibling directory:

```
../emacs/          # git clone https://git.savannah.gnu.org/git/emacs.git
../emaxx/          # this project
```

Then:

```
cargo test
```

Today that smoke layer only targets:

- `test/src/editfns-tests.el`
- `test/src/buffer-tests.el`
- `test/src/cmds-tests.el`

It is intentionally permissive and is not the compatibility scoreboard.

### Authoritative Compatibility Harness

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

This is the command path that should be used to judge behavioral compatibility. It discovers upstream tests recursively from `test/src` and `test/lisp`, compares file loading and per-test outcomes against real Emacs, and fails on mismatches.

Artifacts are written under `target/compat/`. The tracked oracle lock lives at `compat/oracle.lock.json`, while the local machine-specific oracle config is written to `compat/oracle.local.json`.

If the `emacs` directory isn't found, the smoke integration tests will fail with a clear error. The Emacs tests are never copied into this repo — they're always read from the sibling checkout so we stay in sync with upstream.
