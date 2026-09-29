cargo clean --package emaxx --profile gate
cargo build --locked --profile gate --bin compat-harness -j2
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -j2 -- -D warnings
