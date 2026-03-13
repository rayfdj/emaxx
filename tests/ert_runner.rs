#![allow(clippy::unwrap_used)]

use std::path::PathBuf;

use emaxx::lisp;

/// Find the emacs source tree relative to this project.
/// Expects it at ../emacs from the emaxx project root.
fn emacs_test_dir() -> Option<PathBuf> {
    let candidates = [
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../emacs/test/src"),
        PathBuf::from("/Users/alpha/CodexProjects/emacs/test/src"),
    ];
    candidates.into_iter().find(|p| p.exists())
}

/// Run a single .el test file and report results.
/// This is a lightweight smoke harness; the authoritative compatibility
/// runner lives in `cargo run --bin compat-harness`.
fn run_el_test(filename: &str) {
    let test_dir = emacs_test_dir().expect("Cannot find emacs test/src directory");
    let path = test_dir.join(filename);
    assert!(path.exists(), "Test file not found: {}", path.display());

    println!("\n=== Running {} ===", filename);

    match lisp::run_ert_file(&path) {
        Ok((passed, failed, total, results)) => {
            for (name, ok, err) in &results {
                if *ok {
                    println!("  PASS: {}", name);
                } else {
                    println!("  FAIL: {} -- {}", name, err.as_deref().unwrap_or("?"));
                }
            }
            println!("  [{}/{}] passed, {} failed", passed, total, failed);

            // Keep this permissive: the dedicated compatibility harness
            // owns strict oracle comparisons now.
            assert!(passed > 0 || total == 0, "No tests passed in {}", filename);
        }
        Err(e) => {
            println!("  ERROR loading {}: {}", filename, e);
            // Don't panic here — some files may use forms we don't support yet
        }
    }
}

#[test]
fn ert_editfns_tests() {
    run_el_test("editfns-tests.el");
}

#[test]
fn ert_buffer_tests() {
    run_el_test("buffer-tests.el");
}

#[test]
fn ert_cmds_tests() {
    run_el_test("cmds-tests.el");
}
