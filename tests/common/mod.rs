//! Shared by the integration tests that execute the built `emaxx'.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

/// The Emaxx binary with its image built beside it the way GNU's Makefile
/// builds emacs.pdmp (tools/build-image.sh), so that every child starts
/// `initialized' like the oracle does.  The script is a no-op while the
/// image beside the binary loads; after a relink it dumps a new one --
/// emacs.c's load_pdump is fatal on a stale image beside the binary, so
/// every test binary that runs `emaxx' refreshes it first.  With FORCE the
/// fingerprint-and-dump build runs even when a previous test left a valid
/// image beside an unprocessed Cargo executable (cli_parity's fingerprint
/// controls need the binary fingerprinted, not only imaged).
pub fn emaxx(force: bool) -> &'static Path {
    static BINARY: OnceLock<PathBuf> = OnceLock::new();
    BINARY.get_or_init(|| {
        let binary = PathBuf::from(env!("CARGO_BIN_EXE_emaxx"));
        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tools/build-image.sh");
        let mut build = Command::new(&script);
        build.arg(&binary);
        if force {
            build.env("EMAXX_IMAGE_FORCE", "1");
        }
        let built = build.output().unwrap();
        assert!(
            built.status.success(),
            "tools/build-image.sh failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&built.stdout),
            String::from_utf8_lossy(&built.stderr)
        );
        binary
    })
}
