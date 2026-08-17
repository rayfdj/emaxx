#![allow(clippy::unwrap_used)]

use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;

use emaxx::lisp;

/// Find the emacs source tree relative to this project.
/// Expects it at ../emacs from the emaxx project root.
fn emacs_test_dir() -> Option<PathBuf> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../emacs/test/src");
    path.exists().then_some(path)
}

/// Run a single upstream .el test file and fail on any dishonest outcome:
/// a load error, a timeout, a wedged worker, or any failing test.  The
/// compat harness owns oracle comparison; this target owns "these three
/// upstream files run clean through the library entry point".
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
                (failed == 0, lines)
            }
            Err(error) => (
                false,
                vec![format!("  ERROR loading {filename_for_thread}: {error}")],
            ),
        };
        let _ = tx.send(report);
    });

    match rx.recv_timeout(Duration::from_secs(300)) {
        Ok((clean, lines)) => {
            for line in lines {
                println!("{line}");
            }
            assert!(clean, "Load error or failing tests in {}", filename);
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("Timed out loading/running {filename}");
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("Worker exited before reporting for {filename}");
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
