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

/// eval.c:probably_quit inhibits the collection while a quit is
/// processed.  Native code sets `quit-flag' and calls `string-match' with
/// no call entry between, so the matcher's own maybe_quit finds the quit
/// and the debugger is entered for it; the debugger frees the strings
/// allocated just before the subject, calls `garbage-collect' and
/// allocates again.  A collection there would move the subject's data
/// under the matcher, which goes on reading when the debugger returns.
#[test]
fn a_quit_processed_inside_a_string_match_collects_nothing() {
    let cases = [(
        "string-match-quit",
        include_str!("fixtures/thread-native-string-match-quit.el"),
        None,
        "(((0 61) (0 61) (0 61) (0 61) (0 61) (0 61) (0 61) (0 61)) 8)\n",
    )];
    run_cases("native-string-match-quit", &cases, true);
}

/// A collection in the windows where the collector reads what another
/// stack or an outer native call holds.  In the first case a Lisp thread
/// is parked in each way a thread parks (thread-yield, mutex-lock,
/// condition-wait, thread-join, sleep-for), in native code, in the
/// evaluator and in byte code, each holding a list and a string that
/// nothing else holds as the evaluated arguments of a pending call, and
/// the main thread collects, allocates and collects.  Native code's
/// arguments are found by the parked stack's scan and again among its
/// suspended native words; the evaluator's are words of the parked
/// machine stack and nothing else; byte code's are on the byte-code
/// stack.  In the second case three native calls are nested, an
/// interpreted and a byte-compiled function between them, and the
/// innermost collects; each then reads what its own frame holds and its
/// special binding.  The weak table's count there is compared as a
/// bound: how many keys of the rounds before are still in it depends on
/// the words those rounds left on the machine stack, in GNU Emacs too.
#[test]
fn collections_keep_what_parked_threads_and_outer_native_calls_hold() {
    let cases = [
        (
            "parked-kinds",
            include_str!("fixtures/thread-native-parked-kinds.el"),
            None,
            concat!(
                "((native interpreted byte-code) 15)\n",
                "(0 1 \"aaaaaaaaaaaaaaaaaaaa\" 0 waited)\n",
                "(1 2 \"bbbbbbbbbbbbbbbbbbbbb\" 1 waited)\n",
                "(2 3 \"cccccccccccccccccccccc\" 2 waited)\n",
                "(3 4 \"ddddddddddddddddddddddd\" 3 waited)\n",
                "(4 5 \"eeeeeeeeeeeeeeeeeeeeeeee\" 4 waited)\n",
                "(5 6 \"fffffffffffffffffffffffff\" 5 waited)\n",
                "(6 7 \"gggggggggggggggggggggggggg\" 6 waited)\n",
                "(7 8 \"hhhhhhhhhhhhhhhhhhhhhhhhhhh\" 7 waited)\n",
                "(8 9 \"iiiiiiiiiiiiiiiiiiiiiiiiiiii\" 8 waited)\n",
                "(9 10 \"jjjjjjjjjjjjjjjjjjjjjjjjjjjjj\" 9 waited)\n",
                "(10 11 \"kkkkkkkkkkkkkkkkkkkkkkkkkkkkkk\" 10 waited)\n",
                "(11 12 \"lllllllllllllllllllllllllllllll\" 11 waited)\n",
                "(12 13 \"mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm\" 12 waited)\n",
                "(13 14 \"nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn\" 13 waited)\n",
                "(14 15 \"oooooooooooooooooooooooooooooooooo\" 14 waited)\n",
            ),
        ),
        (
            "nested-collect",
            include_str!("fixtures/thread-native-nested-collect.el"),
            None,
            concat!(
                "(t t t top (",
                r#"((1 "ooooooooooooooooooooo") outer 1 (2 "mmmmmmmmmmmmmmmmmmmmmm" middle 2 "#,
                r#"(3 "iiiiiiiiiiiiiiiiiiiiiii" inner 3 t))) "#,
                r#"((1 "ooooooooooooooooooooo") outer 1 (2 "mmmmmmmmmmmmmmmmmmmmmm" middle 2 "#,
                r#"(3 "iiiiiiiiiiiiiiiiiiiiiii" inner 3 t))) "#,
                r#"((1 "ooooooooooooooooooooo") outer 1 (2 "mmmmmmmmmmmmmmmmmmmmmm" middle 2 "#,
                r#"(3 "iiiiiiiiiiiiiiiiiiiiiii" inner 3 t)))"#,
                "))\n"
            ),
        ),
    ];
    run_cases("native-collection-windows", &cases, true);
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
