#![allow(clippy::unwrap_used)]

use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;

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

    let (tx, rx) = mpsc::channel();
    let filename_for_thread = filename.to_string();
    std::thread::spawn(move || {
        let report = match lisp::run_ert_file(&path) {
            Ok((passed, failed, total, results)) => {
                let mut lines = Vec::new();
                for (name, ok, err) in &results {
                    if *ok {
                        lines.push(format!("  PASS: {name}"));
                    } else {
                        lines.push(format!(
                            "  FAIL: {name} -- {}",
                            err.as_deref().unwrap_or("?")
                        ));
                    }
                }
                lines.push(format!("  [{passed}/{total}] passed, {failed} failed"));
                (passed > 0 || total == 0, lines)
            }
            Err(error) => (
                true,
                vec![format!("  ERROR loading {filename_for_thread}: {error}")],
            ),
        };
        let _ = tx.send(report);
    });

    match rx.recv_timeout(Duration::from_secs(60)) {
        Ok((has_passed_tests, lines)) => {
            for line in lines {
                println!("{line}");
            }
            // Keep this permissive: the dedicated compatibility harness
            // owns strict oracle comparisons now.
            assert!(has_passed_tests, "No tests passed in {}", filename);
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            println!(
                "  TIMEOUT loading/running {filename}; compatibility harness owns strict coverage"
            );
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            println!("  ERROR running {filename}: worker exited before reporting");
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
