#![allow(clippy::unwrap_used)]
//! emacs.c command-line parity: the same argument vectors are given to the
//! GNU oracle (`../emacs/src/emacs`) and to Emaxx, and their stdout,
//! stderr and exit status must agree.  The switches consumed in C
//! (emacs.c:main, sort_args, argmatch, init_cmdargs) and the ones left to
//! the unchanged startup.el are both covered: the probe file prints what
//! `command-line-args' and `command-line-args-left' startup.el received.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::OnceLock;

fn oracle() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../emacs/src/emacs")
}

/// The Emaxx binary with its image built beside it the way GNU's Makefile
/// builds emacs.pdmp (tools/build-image.sh), so that every child starts
/// `initialized' like the oracle does.
fn emaxx() -> &'static Path {
    static BINARY: OnceLock<PathBuf> = OnceLock::new();
    BINARY.get_or_init(|| {
        let binary = PathBuf::from(env!("CARGO_BIN_EXE_emaxx"));
        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tools/build-image.sh");
        let built = Command::new(&script).arg(&binary).output().unwrap();
        assert!(
            built.status.success(),
            "tools/build-image.sh failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&built.stdout),
            String::from_utf8_lossy(&built.stderr)
        );
        binary
    })
}

struct Corpus {
    directory: PathBuf,
}

impl Corpus {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "emaxx-cli-parity-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join("probe.el"),
            "(princ (format \"args=%S left=%S ni=%S\\n\" command-line-args command-line-args-left noninteractive))\n",
        )
        .unwrap();
        std::fs::write(
            directory.join("script.el"),
            "(princ (format \"script-args=%S\\n\" command-line-args))\n",
        )
        .unwrap();
        Self { directory }
    }

    fn probe(&self) -> String {
        self.directory.join("probe.el").display().to_string()
    }

    fn script(&self) -> String {
        self.directory.join("script.el").display().to_string()
    }

    /// Run BINARY with ARGS from the corpus directory; the binary's own
    /// path (argv[0], echoed by messages and `command-line-args') and
    /// the corpus directory are replaced by placeholders, and bytecode
    /// object addresses in backtraces by one token.
    fn run(&self, binary: &Path, args: &[&str]) -> (String, String, Option<i32>) {
        let Output {
            status,
            stdout,
            stderr,
        } = Command::new(binary)
            .args(args)
            .current_dir(&self.directory)
            .env("HOME", &self.directory)
            .env("LC_ALL", "C")
            .stdin(std::process::Stdio::null())
            .output()
            .unwrap();
        let normalize = |bytes: &[u8]| {
            let text = String::from_utf8_lossy(bytes)
                .replace(&binary.display().to_string(), "EMACS")
                .replace(&self.directory.display().to_string(), "CORPUS");
            replace_addresses(&text)
        };
        (normalize(&stdout), normalize(&stderr), status.code())
    }
}

impl Drop for Corpus {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

/// The oracle's preloaded Lisp is native code, which calls a C primitive
/// directly, without a backtrace frame; Emaxx runs the byte code of the
/// same files, whose every call is a frame.  The frames of C primitives
/// (`signal' under `error', `read-from-string' under `command-line-1',
/// `string-match' under `command-line-normalize-file-name') are dropped
/// from both outputs before they are compared: that difference is the
/// evaluator's, not the command line's.  Which frame names are
/// primitives, the oracle itself says.
fn without_primitive_frames(texts: &mut [String]) {
    let names = texts
        .iter()
        .flat_map(|text| text.lines())
        .filter_map(frame_function)
        .collect::<std::collections::BTreeSet<_>>();
    if names.is_empty() {
        return;
    }
    let query = format!(
        "(dolist (name '({})) (when (and (fboundp name) (subr-primitive-p (indirect-function name))) (princ name) (terpri)))",
        names.iter().cloned().collect::<Vec<_>>().join(" ")
    );
    let answer = Command::new(oracle())
        .args(["-Q", "--batch", "--eval", &query])
        .output()
        .unwrap();
    assert!(
        answer.status.success(),
        "{}",
        String::from_utf8_lossy(&answer.stderr)
    );
    let primitives = String::from_utf8_lossy(&answer.stdout)
        .lines()
        .map(str::to_owned)
        .collect::<std::collections::BTreeSet<_>>();
    for text in texts.iter_mut() {
        *text = text
            .lines()
            .filter(|line| !frame_function(line).is_some_and(|name| primitives.contains(&name)))
            .map(|line| format!("{line}\n"))
            .collect();
    }
}

/// The function of a backtrace frame line ("  name(args)"), when the
/// name is a plain symbol.
fn frame_function(line: &str) -> Option<String> {
    let rest = line.strip_prefix("  ")?;
    let name = &rest[..rest.find('(')?];
    (!name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '*' | '+' | '/' | ':')))
    .then(|| name.to_string())
}

/// `#<bytecode 0x...>' in GNU's backtrace names a heap address.
fn replace_addresses(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(index) = rest.find("0x") {
        out.push_str(&rest[..index]);
        out.push_str("0xADDR");
        let after = &rest[index + 2..];
        let digits = after
            .find(|c: char| !c.is_ascii_hexdigit())
            .unwrap_or(after.len());
        rest = &after[digits..];
    }
    out.push_str(rest);
    out
}

type Run = (String, String, Option<i32>);

/// Both editors' runs, with the frames of C primitives dropped.
fn both(corpus: &Corpus, args: &[&str]) -> (Run, Run) {
    let (expected_out, expected_err, expected_status) = corpus.run(&oracle(), args);
    let (actual_out, actual_err, actual_status) = corpus.run(emaxx(), args);
    let mut texts = [expected_out, expected_err, actual_out, actual_err];
    without_primitive_frames(&mut texts);
    let [expected_out, expected_err, actual_out, actual_err] = texts;
    (
        (actual_out, actual_err, actual_status),
        (expected_out, expected_err, expected_status),
    )
}

fn assert_same(corpus: &Corpus, args: &[&str]) {
    let (actual, expected) = both(corpus, args);
    assert_eq!(
        actual, expected,
        "Emaxx (left) and GNU (right) differ for arguments {args:?}"
    );
}

#[test]
fn c_owned_switches_and_startup_arguments_agree_with_gnu() {
    let corpus = Corpus::new();
    let probe = corpus.probe();
    let script = corpus.script();
    let cases: Vec<Vec<&str>> = vec![
        // Switches emacs.c consumes, in every spelling and abbreviation.
        vec!["--script", &script],
        vec!["-x", &script],
        vec!["-x"],
        vec!["-batch", "-nl", "--eval", "(princ \"hi\")"],
        vec![
            "--batch",
            "-chdir",
            "/nonexistent",
            "--eval",
            "(princ \"hi\")",
        ],
        vec![
            "--batch",
            "--chdir=/tmp",
            "--eval",
            "(princ default-directory)",
        ],
        vec!["--batch", "-t", "/dev/null", "--eval", "(princ \"hi\")"],
        vec!["--batch", "-t", "/nonexistent", "--eval", "(princ \"hi\")"],
        vec!["--batch", "--module-assertions", "--eval", "(princ \"hi\")"],
        // `--display=NAME' is not here: the oracle is an X build, whose
        // emacs.c rewrites it to `-d NAME' inside `#ifdef HAVE_X_WINDOWS';
        // Emaxx, like a `--without-x' build, leaves it to startup.el.
        vec!["--batch", "-d", "foo", "--eval", "(princ \"hi\")"],
        vec!["-nw", "--batch", "--eval", "(princ \"hi\")"],
        vec!["--no-windows", "--batch", "--eval", "(princ \"hi\")"],
        vec!["--batch", "--no-site-lisp", "--eval", "(princ \"hi\")"],
        vec![
            "--batch",
            "-nsl",
            "-nw",
            "-no-build-details",
            "-Q",
            "-Q",
            "-l",
            &probe,
        ],
        vec![
            "--batch",
            "--fg-daemon=x",
            "--eval",
            "(princ (list (daemonp) \"hi\"))",
        ],
        vec!["--batch", "--temacs=foo", "--eval", "(princ \"hi\")"],
        vec![
            "--batch",
            "--dump-file",
            "/nonexistent",
            "--eval",
            "(princ \"hi\")",
        ],
        vec!["--batch", "--seccomp", "--eval", "(princ \"hi\")"],
        vec!["--batch", "--eval"],
        vec![
            "--batch",
            "--no-comp-spawn",
            "-Q",
            "--eval",
            "(princ noninteractive)",
        ],
        vec![
            "--batch",
            "-no-comp-spawn",
            "-Q",
            "--eval",
            "(princ comp-no-spawn)",
        ],
        // Abbreviations: unambiguous ones are the option, ambiguous ones
        // are sort_args's stderr note and startup.el's error.
        vec!["--batch", "--no-i", "--eval", "(princ \"hi\")"],
        vec!["--batch", "--no", "--eval", "(princ \"hi\")"],
        vec!["--batch", "--quick=1", "--eval", "(princ \"hi\")"],
        // Unknown options are startup.el's errors, with its backtrace.
        vec!["--batch", "--bogus", "--eval", "(princ \"hi\")"],
        vec!["--batch", "-b", "--eval", "(princ \"hi\")"],
        vec!["--batch", "-bogus", "--eval", "(princ \"hi\")"],
        vec!["--batch", "-no-loadup", "--eval", "(princ \"hi\")"],
        vec!["--batch", "--scriptload", &script],
        // Priority sorting, repeated options, "--", option values.
        vec!["--eval", "(princ \"hi\")", "--batch"],
        vec!["--batch", "--eval", "(princ \"hi\")", "--", "--batch"],
        vec!["-batch", "-l", &probe, "--", "-Q", "foo"],
        vec!["-batch", "-q", "-l", &probe, "foo", "bar"],
        vec!["-batch", "-L", "/tmp", "-L", ":/usr", "-l", &probe],
        vec!["-batch", "-l", &probe, "-kill"],
        vec![
            "-batch",
            "-no-site-file",
            "-no-init-file",
            "-nbc",
            "-l",
            &probe,
        ],
        vec!["-batch", "--init-directory=/tmp", "-l", &probe],
        vec!["-batch", "-u", "nobody", "-l", &probe],
        vec!["-batch", "-f", "emacs-version"],
        vec!["-batch", "--funcall=emacs-version"],
        vec!["-batch", "-execute", "(princ \"exec\")"],
        vec!["-batch", "--eval="],
        vec![
            "--batch",
            "--debug-init",
            "--iconic",
            "--eval",
            "(princ \"hi\")",
        ],
        vec![
            "--batch",
            "-Q",
            "--eval",
            "(princ (list site-run-file init-file-user inhibit-x-resources))",
        ],
        vec![
            "--batch",
            "--eval",
            "(princ (list (daemonp) (boundp 'comp-no-spawn) system-name))",
        ],
        vec![
            "--batch",
            "--no-build-details",
            "--eval",
            "(princ (list system-name (system-name)))",
        ],
    ];
    for case in &cases {
        assert_same(&corpus, case);
    }
}

#[test]
fn help_text_is_emacs_c_usage_message() {
    let corpus = Corpus::new();
    assert_same(&corpus, &["--help"]);
    assert_same(&corpus, &["-help"]);
    assert_same(&corpus, &["--batch", "--he"]);
    assert_same(&corpus, &["--help", "--version"]);
}

#[test]
fn version_report_is_emacs_c_main_s() {
    let corpus = Corpus::new();
    for args in [
        vec!["--version"],
        vec!["-version", "--batch", "--eval", "(princ \"hi\")"],
        vec!["--batch", "--vers"],
        vec!["--batch", "--fingerprint", "--version"],
    ] {
        let (expected_out, expected_err, expected_status) = corpus.run(&oracle(), &args);
        let (actual_out, actual_err, actual_status) = corpus.run(emaxx(), &args);
        assert_eq!((actual_err, actual_status), (expected_err, expected_status));
        // The "Development version" line names each binary's own
        // repository revision and build date; its shape must agree.
        let strip = |text: &str| {
            text.lines()
                .filter(|line| !line.starts_with("Development version "))
                .collect::<Vec<_>>()
                .join("\n")
        };
        assert_eq!(
            strip(&actual_out),
            strip(&expected_out),
            "arguments {args:?}"
        );
        let development = |text: &str| {
            text.lines()
                .find(|line| line.starts_with("Development version "))
                .map(|line| {
                    line.ends_with('.')
                        && line.contains(" on ")
                        && line.contains(" branch; build date ")
                })
        };
        assert_eq!(development(&actual_out), development(&expected_out));
    }
}

#[test]
fn fingerprint_is_the_executable_s_own() {
    let corpus = Corpus::new();
    let (out, err, status) = corpus.run(emaxx(), &["--fingerprint"]);
    let (expected_out, expected_err, expected_status) = corpus.run(&oracle(), &["--fingerprint"]);
    assert_eq!((err, status), (expected_err, expected_status));
    let hex = |text: &str| {
        text.len() == expected_out.len()
            && text.ends_with('\n')
            && text.trim_end().chars().all(|c| c.is_ascii_hexdigit())
    };
    assert!(hex(&out), "{out:?}");
    assert!(hex(&expected_out), "{expected_out:?}");
}

#[test]
fn script_with_equals_rewrites_the_argument_before_it_like_emacs_c() {
    // emacs.c's FIXME: `--script=FILE' replaces the argument before the
    // option with `-scriptload' -- here argv[0] itself -- and un-skips two
    // positions, so startup.el sees the option unchanged and rejects it.
    // The oracle's invocation name is then `-scriptload', which its
    // installation-layout warnings (no Emaxx counterpart) mention on
    // stderr before that error, so only the error and the status agree.
    let corpus = Corpus::new();
    let script = format!("--script={}", corpus.script());
    let ((actual_out, actual_err, actual_status), (expected_out, expected_err, expected_status)) =
        both(&corpus, &[&script]);
    assert_eq!((actual_out, actual_status), (expected_out, expected_status));
    let error_line = |text: &str| {
        text.lines()
            .find(|line| line.starts_with("Unknown option"))
            .map(str::to_owned)
    };
    assert_eq!(error_line(&actual_err), error_line(&expected_err));
    assert_eq!(
        error_line(&actual_err).as_deref(),
        Some("Unknown option `--script=CORPUS/script.el'")
    );
}
