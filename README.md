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

Integration tests run actual Emacs `.el` test files (ERT) against the Rust implementation. They expect a checkout of the Emacs source tree as a sibling directory:

```
../emacs/          # git clone https://git.savannah.gnu.org/git/emacs.git
../emaxx/          # this project
```

Then:

```
cargo test
```

If the `emacs` directory isn't found, the integration tests will fail with a clear error. The Emacs tests are never copied into this repo — they're always read from the sibling checkout so we stay in sync with upstream.