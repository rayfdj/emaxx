use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Exercise generated native frames, not only hand-written ABI probes.
/// Each editor receives identical Lisp and its own HOME and temporary files.
#[test]
fn thread_suspension_and_signals_match_gnu_with_real_native_code() {
    let cases = [
        (
            "early-signal",
            include_str!("fixtures/thread-early-signal.el"),
            None,
            "(t nil)\n",
        ),
        (
            "native-workers",
            include_str!("fixtures/thread-native-suspension.el"),
            None,
            "(t 2 (0 0) (1 caught 1) [1 1])\n",
        ),
        (
            "nil-signal-roots",
            include_str!("fixtures/thread-nil-signal-roots.el"),
            Some(
                "(progn (garbage-collect) \
                 (prin1 (list nil-signal-retained (hash-table-count nil-signal-table))) \
                 (terpri))",
            ),
            "(1 0)\n",
        ),
        (
            "signal-data-identity",
            include_str!("fixtures/thread-signal-data-identity.el"),
            None,
            "(t after)\n",
        ),
        (
            "signal-condition-broadcast",
            include_str!("fixtures/thread-signal-condition-broadcast.el"),
            None,
            "2\n",
        ),
        (
            "signal-caller-boundary",
            include_str!("fixtures/thread-signal-caller-boundary.el"),
            None,
            "caught\n",
        ),
    ];
    run_cases("native-thread-contract", &cases, false);
}

/// A native helper that calls Lisp makes its references to the
/// interpreter, the runtime and the environment again after the call:
/// the callee parks its thread, another thread runs, and the helper then
/// goes on with its own frame.  One case for each such helper: Ffuncall
/// (each kind of callee, and one that signals), helper_unbind_n (a
/// cleanup, a watcher), Fget under `overriding-plist-environment' (the
/// debugger entered for a quit in Fassq) and helper_sanitizer_assert.
/// The collector's verifier is on in each editor's environment; GNU
/// reads no such variable.
#[test]
fn native_helpers_go_on_after_their_callee_parks_the_thread() {
    let cases = [
        (
            "funcall-yield",
            include_str!("fixtures/thread-native-funcall-yield.el"),
            None,
            "(t t ((40 (0 40)) (80 (1 40))) ((40 (0 40)) (80 (1 40))) \
             ((40 (0 40)) (80 (1 40))) ((40 (0 40)) (80 (1 40))) \
             ((40 (0 40)) (80 (1 40))))\n",
        ),
        (
            "unbind-yield",
            include_str!("fixtures/thread-native-unbind-yield.el"),
            None,
            "(t t ((40 unbound) (80 unbound)) ((40 unbound unbound) 40) 40 unbound unbound)\n",
        ),
        (
            "get-yield",
            include_str!("fixtures/thread-native-get-yield.el"),
            None,
            "(t ((280 nil) (280 nil)) 80)\n",
        ),
        (
            "sanitizer-yield",
            include_str!("fixtures/thread-native-sanitizer-yield.el"),
            None,
            "(t (120 120) 80)\n",
        ),
    ];
    run_cases("native-helper-yield", &cases, true);
}

/// Each case's Lisp run by GNU Emacs and by Emaxx, each in a directory of
/// its own under one named PREFIX, with the standard output compared
/// with the case's expectation.  VERIFY sets `EMAXX_GC_VERIFY'.
fn run_cases(prefix: &str, cases: &[(&str, &str, Option<&str>, &str)], verify: bool) {
    let project = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let oracle = project.join("../emacs/src/emacs");
    let subject = PathBuf::from(env!("CARGO_BIN_EXE_emaxx"));
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("timestamp")
        .as_nanos();
    let work = std::env::temp_dir().join(format!("{prefix}-{}-{nonce}", std::process::id()));
    for &(name, program, observer, expected) in cases {
        let case = work.join(name);
        std::fs::create_dir_all(&case).expect("create contract directory");
        let source = case.join("contract.el");
        std::fs::write(&source, program).expect("write identical Lisp fixture");
        for (editor, binary) in [("gnu", &oracle), ("emaxx", &subject)] {
            let root = case.join(editor);
            let home = root.join("home");
            let temporary = root.join("tmp");
            for directory in [&home, &temporary] {
                std::fs::create_dir_all(directory).expect("create isolated editor directory");
            }
            let stdout = root.join("stdout.log");
            let stderr = root.join("stderr.log");
            let mut command = Command::new(binary);
            command.args(["-Q", "--batch", "-l"]).arg(&source);
            if let Some(observer) = observer {
                // Both editors observe GC after their loader frames unwind.
                command.args(["--eval", observer]);
            }
            command.env_clear();
            if verify {
                command.env("EMAXX_GC_VERIFY", "1");
            }
            let mut child = command
                .env(
                    "PATH",
                    std::env::var_os("PATH").expect("compiler toolchain PATH"),
                )
                .env("HOME", home)
                .env("TMPDIR", temporary)
                .env("LANG", "C")
                .env("LC_ALL", "C")
                .stdout(Stdio::from(
                    std::fs::File::create(&stdout).expect("stdout log"),
                ))
                .stderr(Stdio::from(
                    std::fs::File::create(&stderr).expect("stderr log"),
                ))
                .spawn()
                .expect("run real editor");
            let deadline = Instant::now() + Duration::from_secs(300);
            let status = loop {
                if let Some(status) = child.try_wait().expect("poll editor") {
                    break status;
                }
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("{editor} {name} timed out; artifacts: {}", root.display());
                }
                std::thread::sleep(Duration::from_millis(20));
            };
            assert!(
                status.success(),
                "{editor} {name}: {status}\n{}",
                std::fs::read_to_string(&stderr).expect("read stderr")
            );
            assert_eq!(
                std::fs::read(&stdout).expect("read stdout"),
                expected.as_bytes(),
                "{editor} {name}; artifacts: {}",
                root.display()
            );
        }
    }
}
