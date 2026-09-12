#![allow(clippy::unwrap_used)]

use std::io::Write;
use std::process::{Command, Stdio};
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

#[cfg(unix)]
#[test]
fn sigusr_events_follow_gnu_special_event_map_semantics() {
    let program = r#"(let ((seen nil))
      (define-key special-event-map [sigusr1]
        (lambda () (interactive) (setq seen (1+ (or seen 0)))))
      (call-process "kill" nil nil nil "-USR1" (number-to-string (emacs-pid)))
      (prin1 (list (read-event nil nil 0.05) seen last-input-event))
      (define-key special-event-map [sigusr1] nil)
      (call-process "kill" nil nil nil "-USR1" (number-to-string (emacs-pid)))
      (prin1 (list (read-event nil nil 0.05) seen last-input-event)))"#;
    let run = |binary: &std::path::Path| {
        Command::new(binary)
            .args(["--quick", "--batch", "--eval", program])
            .output()
            .unwrap()
    };
    let oracle = run(&std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../emacs/src/emacs"));
    assert!(
        oracle.status.success(),
        "GNU signal oracle failed: {}",
        String::from_utf8_lossy(&oracle.stderr)
    );
    let subject = run(std::path::Path::new(env!("CARGO_BIN_EXE_emaxx")));
    assert!(
        subject.status.success(),
        "Emaxx did not survive/dispatch SIGUSR1: {}",
        String::from_utf8_lossy(&subject.stderr)
    );
    assert_eq!(subject.stdout, oracle.stdout);
}

#[test]
fn dump_file_process_round_trip_or_explicit_native_image_limit() {
    // emacs.c:load_pdump: a batch session dumps itself with
    // `dump-emacs-portable' (command-line-processed bound to nil for the
    // dump, so the restored process runs startup again); `--dump-file'
    // starts a process from the image and startup.el runs as usual in
    // it; a dump file that cannot be loaded is term.c's fatal on stderr
    // with exit status 1.  The session dumps under loadup.el's
    // `load--bin-dest-dir' and `load--eln-dest-dir' (the Makefile's
    // --bin-dest and --eln-dest), so that the native units an ordinary
    // startup loaded are fixed up for the image and reopened by the
    // process that starts from it (D14/D15); the native functions are as
    // many in that process as in the session that dumped.
    let image = unique_temp_path("emaxx-cli-dump");
    // The dump happens in one directory and the load in another:
    // buffer.c:init_buffer gives the restored `*scratch*' and minibuffer
    // the new process's working directory.
    let dump_directory = unique_temp_path("emaxx-cli-dump-from");
    let load_directory = unique_temp_path("emaxx-cli-load-in");
    std::fs::create_dir_all(&dump_directory).unwrap();
    std::fs::create_dir_all(&load_directory).unwrap();
    // The restored process answers its working directory as getcwd
    // names it: the physical path (on Darwin `/var' is a link to
    // `/private/var', and PWD is not updated for the child).
    let load_directory_name = format!(
        "{}/",
        std::fs::canonicalize(&load_directory).unwrap().display()
    );
    let dump = Command::new(env!("CARGO_BIN_EXE_emaxx"))
        .current_dir(&dump_directory)
        .args([
            "--batch",
            "--eval",
            &format!(
                "(progn (let ((native 0)) \
                   (mapatoms (lambda (symbol) \
                     (when (native-comp-function-p (symbol-function symbol)) \
                       (setq native (1+ native))))) \
                   (princ (format \"native-functions=%d\\n\" native))) \
                   (let ((load--bin-dest-dir invocation-directory) \
                         (load--eln-dest-dir source-directory)) \
                     (dump-emacs-portable {:?})))",
                image.display()
            ),
        ])
        .output()
        .unwrap();
    assert!(
        dump.status.success(),
        "the batch session did not dump:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&dump.stdout),
        String::from_utf8_lossy(&dump.stderr)
    );
    let native_count = String::from_utf8_lossy(&dump.stdout)
        .lines()
        .find_map(|line| line.strip_prefix("native-functions=").map(str::to_owned))
        .expect("the session counted its native functions");
    let started = Command::new(env!("CARGO_BIN_EXE_emaxx"))
        .current_dir(&load_directory)
        .args([
            "--batch",
            "--dump-file",
            &image.display().to_string(),
            "--eval",
            "(princ (list (car (pdumper-stats)) command-line-processed \
             (featurep 'simple) (+ 1 2) (getenv \"HOME\") \
             (with-current-buffer \"*scratch*\" default-directory) \
             (with-current-buffer \" *Minibuf-0*\" default-directory) \
             (buffer-name) \
             (let ((native 0)) \
               (mapatoms (lambda (symbol) \
                 (when (native-comp-function-p (symbol-function symbol)) \
                   (setq native (1+ native))))) \
               native) \
             (string-trim \" a \")))",
        ])
        .output()
        .unwrap();
    let _ = std::fs::remove_file(&image);
    let _ = std::fs::remove_dir_all(&dump_directory);
    let _ = std::fs::remove_dir_all(&load_directory);
    assert!(
        started.status.success(),
        "the process did not start from the image:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&started.stdout),
        String::from_utf8_lossy(&started.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&started.stdout),
        format!(
            "((dumped-with-pdumper . t) t t 3 {} {load_directory_name} {load_directory_name} *scratch* {native_count} a)",
            std::env::var("HOME").unwrap_or_default()
        )
    );
    let missing = Command::new(env!("CARGO_BIN_EXE_emaxx"))
        .args(["--batch", "--dump-file", "/nonexistent-dir/none.pdmp"])
        .output()
        .unwrap();
    assert_eq!(missing.status.code(), Some(1));
    assert_eq!(
        String::from_utf8_lossy(&missing.stderr),
        "emacs: could not load dump file \"/nonexistent-dir/none.pdmp\": could not open file\n"
    );
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
fn batch_read_string_consumes_a_line_from_piped_stdin() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_emaxx"))
        .args([
            "--quick",
            "--batch",
            "--eval",
            r#"(prin1 (read-string "Input: "))"#,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(b"hello\n").unwrap();
    let output = child.wait_with_output().unwrap();

    assert!(
        output.status.success(),
        "batch read-string failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"Input: \"hello\"");
}

#[test]
fn batch_symbol_readers_answer_piped_stdin_like_gnu() {
    // GNU's minibuffer reads real stdin in batch, so this comparison needs a
    // process with piped input rather than an in-process interpreter.  The
    // expected output is byte-for-byte what `emacs -Q -batch' prints for the
    // same program and the same three empty answers.
    let program = r#"(progn
          (defun emaxx-test-readable-command () (interactive))
          (prin1
           (list
            (read-command "Command: " 'emaxx-test-readable-command)
            (read-command "Command: ")
            (read-variable "Variable: " 'tab-width)
            (subrp (symbol-function 'read-command))
            (subrp (symbol-function 'read-variable)))))"#;
    let mut child = Command::new(env!("CARGO_BIN_EXE_emaxx"))
        .args(["--quick", "--batch", "--eval", program])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(b"\n\n\n").unwrap();
    let output = child.wait_with_output().unwrap();

    assert!(
        output.status.success(),
        "batch symbol readers failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "Command: Command: Variable: (emaxx-test-readable-command ## tab-width t t)"
    );
}

#[test]
fn batch_accepts_gnu_single_dash_long_spellings_and_rejects_dash_b() {
    let output = Command::new(env!("CARGO_BIN_EXE_emaxx"))
        .args([
            "-batch",
            "-eval",
            r#"(progn
                 (princ "single-dash-stdout")
                 (message "single-dash-stderr")
                 (kill-emacs 23))"#,
        ])
        .output()
        .unwrap();

    assert_eq!(
        output.status.code(),
        Some(23),
        "GNU single-dash options did not reach batch evaluation:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"single-dash-stdout");
    // xdisp.c message_to_stderr: stdout was written since the last message
    // (`noninteractive_need_newline'), so the message starts a new line.
    assert_eq!(output.stderr, b"\nsingle-dash-stderr\n");

    // `-b' is not a GNU option: emacs.c leaves it to startup.el, whose
    // command-line-1 signals "Unknown option" (the message on stderr, the
    // batch backtrace on stdout) and the process exits 255 before any
    // later action runs.
    let rejected = Command::new(env!("CARGO_BIN_EXE_emaxx"))
        .args(["-b", "-batch", "-eval", "(princ \"hi\")"])
        .output()
        .unwrap();
    assert_eq!(
        rejected.status.code(),
        Some(255),
        "-b must be rejected as GNU rejects it:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&rejected.stdout),
        String::from_utf8_lossy(&rejected.stderr)
    );
    assert!(
        String::from_utf8_lossy(&rejected.stderr).starts_with("Unknown option `-b'\n"),
        "stderr: {}",
        String::from_utf8_lossy(&rejected.stderr)
    );
    // The batch backtrace on stdout quotes the unevaluated `-eval' form;
    // its own output would have come first.
    assert!(
        !String::from_utf8_lossy(&rejected.stdout).starts_with("hi"),
        "a rejected invocation must not evaluate forms: {}",
        String::from_utf8_lossy(&rejected.stdout)
    );
}

#[test]
fn batch_preserves_eval_and_load_action_order() {
    let source = unique_temp_path("emaxx-cli-action-order").with_extension("el");
    let compiled = source.with_extension("elc");
    std::fs::write(&source, ";;; -*- lexical-binding: t -*-\n(kill-emacs 31)\n").unwrap();
    let compile = format!("(byte-compile-file {:?})", source.display().to_string());

    let output = Command::new(env!("CARGO_BIN_EXE_emaxx"))
        .arg("-batch")
        .arg("-eval")
        .arg(compile)
        .arg("-l")
        .arg(&compiled)
        .output()
        .unwrap();

    std::fs::remove_file(&source).unwrap();
    if compiled.exists() {
        std::fs::remove_file(&compiled).unwrap();
    }
    assert_eq!(
        output.status.code(),
        Some(31),
        "batch actions did not run left-to-right:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn batch_funcall_receives_remaining_file_arguments() {
    let output = Command::new(env!("CARGO_BIN_EXE_emaxx"))
        .arg("--batch")
        .arg("--eval")
        .arg(
            "(defun emaxx-cli-funcall ()
               (unless (equal command-line-args-left '(\"remaining.el\"))
                 (kill-emacs 41)))",
        )
        .arg("-f")
        .arg("emaxx-cli-funcall")
        .arg("remaining.el")
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "GNU -f action did not see its remaining file argument:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn startup_options_after_an_eval_are_sorted_ahead_of_it_like_emacs_c() {
    // emacs.c:main sorts argv by option priority before Lisp sees it, so
    // `-Q' placed after `--eval' still reaches startup.el's option loop,
    // and an option missing its argument is emacs.c:fatal.  Emaxx handed
    // the arguments over unsorted; startup.el's `command-line-1' then
    // rejected the trailing `-Q' as an unknown option.
    let run = |binary: &std::path::Path, args: &[&str]| {
        Command::new(binary)
            .env("LANG", "C")
            .env("LC_ALL", "C")
            .args(args)
            .output()
            .unwrap()
    };
    let oracle = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../emacs/src/emacs");
    let subject = std::path::Path::new(env!("CARGO_BIN_EXE_emaxx"));
    let sorted = [
        "--eval",
        "(prin1 (list init-file-user site-run-file (cdr command-line-args)))",
        "--no-init-fil",
        "-Q",
        "--batch",
    ];
    let expected = run(&oracle, &sorted);
    let actual = run(subject, &sorted);
    assert_eq!(actual.status.code(), expected.status.code());
    assert_eq!(
        String::from_utf8_lossy(&actual.stdout),
        String::from_utf8_lossy(&expected.stdout)
    );
    assert_eq!(
        String::from_utf8_lossy(&expected.stdout),
        "(nil nil (\"--eval\" \"(prin1 (list init-file-user site-run-file (cdr command-line-args)))\"))"
    );

    let missing = ["--batch", "--eval"];
    let expected = run(&oracle, &missing);
    let actual = run(subject, &missing);
    assert_eq!(actual.status.code(), expected.status.code());
    assert_eq!(actual.status.code(), Some(1));
    assert_eq!(
        String::from_utf8_lossy(&actual.stderr),
        String::from_utf8_lossy(&expected.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&expected.stderr),
        "emacs: Option '--eval' requires an argument\n"
    );
}

#[test]
fn batch_eval_error_uses_gnu_stderr_and_exit_status() {
    let output = Command::new(env!("CARGO_BIN_EXE_emaxx"))
        .args([
            "-Q",
            "-batch",
            "-eval",
            "(let ((backtrace-on-error-noninteractive nil)) (funcall 'not-defined))",
        ])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(255));
    assert!(output.stdout.is_empty());
    assert_eq!(
        String::from_utf8_lossy(&output.stderr),
        "Symbol's function definition is void: not-defined\n"
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

/// lread.c:init_lread does not read EMACS_TEST_DIRECTORY: a checkout's
/// `lisp/' directory is not on the load path because the harness names its
/// `test/' sibling, so a library only that directory holds cannot be
/// required.  (Emaxx once walked the checkout recursively; the merged
/// startup follows GNU, and this test used to assert the walk.)
#[test]
fn batch_child_load_path_ignores_the_test_directory_like_lread() {
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

    let run = |binary: &std::path::Path| {
        Command::new(binary)
            .env("EMACS_TEST_DIRECTORY", &test_directory)
            .env("LANG", "C")
            .env("LC_ALL", "C")
            .arg("--quick")
            .arg("--batch")
            .arg(format!("--load={}", consumer.display()))
            .output()
            .unwrap()
    };
    let oracle = run(&std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../emacs/src/emacs"));
    let subject = run(std::path::Path::new(env!("CARGO_BIN_EXE_emaxx")));

    std::fs::remove_dir_all(&repo).unwrap();
    assert_eq!(oracle.status.code(), Some(255));
    assert_eq!(
        String::from_utf8_lossy(&oracle.stderr),
        "Cannot open load file: No such file or directory, emaxx-cli-provider\n"
    );
    assert_eq!(subject.status.code(), oracle.status.code());
    assert_eq!(subject.stderr, oracle.stderr);
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

#[test]
fn batch_eval_interns_the_symbols_it_reads_like_gnu() {
    // GNU's reader interns as it reads, so anything `--eval' mentions is in
    // the obarray afterwards.  Emaxx ran the interning walk for file loading
    // and `eval-region' but NOT for `--eval', so
    // `(progn 'foo (intern-soft "foo"))' answered nil where GNU answers foo --
    // for ordinary symbols as well as keywords.
    //
    // Found by an audit while it was checking a DIFFERENT claim: an earlier
    // ledger entry asserted that `--eval' already interned, and used that to
    // dismiss a failing test as an artifact.  The assertion was false and the
    // product was genuinely wrong on that path.
    let program = r#"(prin1 (list (progn 'zz-cli-plain (intern-soft "zz-cli-plain"))
                                  ;; NOT discriminating for this fix: the
                                  ;; permissive keyword clause answers any
                                  ;; `:name' regardless of the walk.  Kept as a
                                  ;; finding-112 pin -- it will start meaning
                                  ;; something once the obarray is seeded.
                                  (progn ':zz-cli-kw (intern-soft ":zz-cli-kw"))
                                  (intern-soft "zz-cli-never-mentioned")))"#;
    let output = Command::new(env!("CARGO_BIN_EXE_emaxx"))
        .args(["--quick", "--batch", "--eval", program])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "emaxx --eval failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "(zz-cli-plain :zz-cli-kw nil)",
        "--eval must intern what it reads, and must not invent what it never read"
    );
}

/// GNU's batch stdout is C stdio, block-buffered when it is a pipe or a
/// file; stderr is unbuffered.  A child whose two streams feed one
/// descriptor -- what `call-process' with a merged DESTINATION captures --
/// shows every `message' before the `princ' text written earlier, a
/// `flush-standard-output' releases the text written so far, and the
/// `debug-early--handler' backtrace, printed to `standard-output', follows
/// the error message that cmd_error printed to stderr.  `message' also
/// starts with a newline of its own when stdout was written since the
/// previous message (xdisp.c `noninteractive_need_newline').  The frames
/// are GNU's: the unevaluated call for the void function, then each
/// interpreted caller.
#[test]
fn batch_stdout_and_stderr_interleave_like_stdio_on_a_shared_descriptor() {
    let program = unique_temp_path("emaxx-cli-stdio").with_extension("el");
    std::fs::write(
        &program,
        "(princ \"one\\n\")\n\
         (message \"two\")\n\
         (princ \"three\")\n\
         (message \"four\")\n\
         (flush-standard-output)\n\
         (message \"five\")\n\
         (defun cli-probe-g () (cli-probe-undefined 1 2))\n\
         (defun cli-probe-f () (cli-probe-g))\n\
         (cli-probe-f)\n",
    )
    .unwrap();
    // Keep the loader call interpreted in both processes. Native compilation
    // can optimize away the funcall trampolines for `load' and `eval-buffer'
    // in preloaded startup.el/mule.el (comp.el:comp--call-optim-form-call).
    // Those build-dependent frames are not a stdio contract. This ASCII
    // fixture uses lread.c's direct reader and an explicit Lisp caller, so
    // the complete trace, including loader/startup frames, remains comparable.
    let setup = format!(
        "(progn (setq load-source-file-function nil) \
         (defun cli-probe-load () (load {:?} nil t)))",
        program.display().to_string()
    );
    let run = |binary: &std::path::Path| {
        let merged = unique_temp_path("emaxx-cli-stdio-merged");
        let file = std::fs::File::create(&merged).unwrap();
        let status = Command::new(binary)
            .env("LANG", "C")
            .env("LC_ALL", "C")
            .args(["-Q", "--batch", "--eval", &setup, "-f", "cli-probe-load"])
            .stdout(Stdio::from(file.try_clone().unwrap()))
            .stderr(Stdio::from(file))
            .status()
            .unwrap();
        let text = std::fs::read_to_string(&merged).unwrap();
        let _ = std::fs::remove_file(&merged);
        (status.code(), text)
    };
    let oracle = run(&std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../emacs/src/emacs"));
    let subject = run(std::path::Path::new(env!("CARGO_BIN_EXE_emaxx")));
    let _ = std::fs::remove_file(&program);
    assert_eq!(oracle.0, Some(255), "GNU oracle exit status:\n{}", oracle.1);
    // Compare every output byte and every frame; do not strip loader frames.
    assert!(
        oracle.1.starts_with("\ntwo\n\nfour\none\nthreefive\nSymbol's function definition is void: cli-probe-undefined\n\nError: void-function (cli-probe-undefined)\n  (cli-probe-undefined 1 2)\n  cli-probe-g()\n  cli-probe-f()\n"),
        "unexpected GNU oracle output:\n{}",
        oracle.1
    );
    assert_eq!(subject, oracle);
}
