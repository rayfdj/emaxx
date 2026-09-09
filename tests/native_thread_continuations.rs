use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Exercise generated native frames, not only hand-written ABI probes.
/// Each editor receives identical Lisp and its own HOME and temporary files.
#[test]
fn thread_suspension_and_signals_match_gnu_with_real_native_code() {
    let project = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let oracle = project.join("../emacs/src/emacs");
    let subject = PathBuf::from(env!("CARGO_BIN_EXE_emaxx"));
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("timestamp")
        .as_nanos();
    let work = std::env::temp_dir().join(format!(
        "native-thread-contract-{}-{nonce}",
        std::process::id()
    ));
    let cases = [
        (
            "early-signal",
            include_str!("fixtures/thread-early-signal.el"),
            "(t nil)\n",
        ),
        (
            "native-workers",
            include_str!("fixtures/thread-native-suspension.el"),
            "(t 2 (0 0) (1 caught 1) [1 1])\n",
        ),
        (
            "nil-signal-roots",
            include_str!("fixtures/thread-nil-signal-roots.el"),
            "(1 0)\n",
        ),
        (
            "signal-data-identity",
            include_str!("fixtures/thread-signal-data-identity.el"),
            "(t after)\n",
        ),
        (
            "signal-condition-broadcast",
            include_str!("fixtures/thread-signal-condition-broadcast.el"),
            "2\n",
        ),
        (
            "signal-caller-boundary",
            include_str!("fixtures/thread-signal-caller-boundary.el"),
            "caught\n",
        ),
    ];
    for (name, program, expected) in cases {
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
            let mut child = Command::new(binary)
                .args(["-Q", "--batch", "-l"])
                .arg(&source)
                .env_clear()
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
