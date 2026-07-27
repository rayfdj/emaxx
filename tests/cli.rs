#![allow(clippy::unwrap_used)]

use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_temp_path(stem: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "{stem}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn empty_batch_invocation_succeeds_like_gnu_emacs() {
    let output = Command::new(env!("CARGO_BIN_EXE_emaxx"))
        .arg("--batch")
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "empty batch invocation failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn batch_accepts_gnu_quick_and_long_load_options() {
    let source = unique_temp_path("emaxx-cli-load").with_extension("el");
    std::fs::write(&source, "(provide 'emaxx-cli-load-test)\n").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_emaxx"))
        .arg("--quick")
        .arg("--batch")
        .arg(format!("--load={}", source.display()))
        .output()
        .unwrap();

    std::fs::remove_file(&source).unwrap();
    assert!(
        output.status.success(),
        "GNU-compatible batch spelling failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn batch_child_reconstructs_repo_load_path_from_test_directory() {
    let repo = unique_temp_path("emaxx-cli-repo");
    let test_directory = repo.join("test");
    let lisp_directory = repo.join("lisp");
    std::fs::create_dir_all(&test_directory).unwrap();
    std::fs::create_dir_all(&lisp_directory).unwrap();

    let provider = lisp_directory.join("emaxx-cli-provider.el");
    let consumer = lisp_directory.join("emaxx-cli-consumer.el");
    std::fs::write(&provider, "(provide 'emaxx-cli-provider)\n").unwrap();
    std::fs::write(
        &consumer,
        "(require 'emaxx-cli-provider)\n(provide 'emaxx-cli-consumer)\n",
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_emaxx"))
        .env("EMACS_TEST_DIRECTORY", &test_directory)
        .arg("--quick")
        .arg("--batch")
        .arg(format!("--load={}", consumer.display()))
        .output()
        .unwrap();

    std::fs::remove_dir_all(&repo).unwrap();
    assert!(
        output.status.success(),
        "batch child did not recover its repo-local load path:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn batch_kill_emacs_runs_hooks_and_exits_without_lisp_unwinding() {
    let hook_result = unique_temp_path("emaxx-cli-kill-hook");
    let expression = format!(
        r#"(progn
             (add-hook 'kill-emacs-hook
                       (lambda ()
                         (write-region "hook" nil "{}" nil 'silent)))
             (unwind-protect
                 (condition-case nil
                     (kill-emacs 7)
                   (t (write-region "caught" nil "{}" t 'silent)))
               (write-region "cleanup" nil "{}" t 'silent)))"#,
        hook_result.display(),
        hook_result.display(),
        hook_result.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_emaxx"))
        .args(["--quick", "--batch", "--eval", &expression])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(7));
    assert_eq!(
        std::fs::read_to_string(&hook_result).unwrap(),
        "hook",
        "the shutdown hook must run, while catch and cleanup bodies must not"
    );
    std::fs::remove_file(&hook_result).unwrap();
    assert!(output.stdout.is_empty());
    assert!(
        output.stderr.is_empty(),
        "orderly shutdown wrote stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let negative = Command::new(env!("CARGO_BIN_EXE_emaxx"))
        .args(["--quick", "--batch", "--eval", "(kill-emacs -1)"])
        .output()
        .unwrap();
    assert_eq!(negative.status.code(), Some(255));
}

#[test]
fn batch_load_propagates_kill_emacs_instead_of_reporting_a_load_error() {
    let source = unique_temp_path("emaxx-cli-kill-emacs").with_extension("el");
    std::fs::write(&source, "(kill-emacs 6)\n").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_emaxx"))
        .arg("--quick")
        .arg("--batch")
        .arg(format!("--load={}", source.display()))
        .output()
        .unwrap();

    std::fs::remove_file(&source).unwrap();
    assert_eq!(output.status.code(), Some(6));
    assert!(
        output.stderr.is_empty(),
        "kill-emacs during load was reported as an error: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
