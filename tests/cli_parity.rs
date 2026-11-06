#![allow(clippy::unwrap_used)]
//! emacs.c command-line parity: the same argument vectors are given to the
//! GNU oracle (`../emacs/src/emacs`) and to Emaxx, and their stdout,
//! stderr and exit status must agree.  The switches consumed in C
//! (emacs.c:main, sort_args, argmatch, init_cmdargs) and the ones left to
//! the unchanged startup.el are both covered: the probe file prints what
//! `command-line-args' and `command-line-args-left' startup.el received.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn oracle() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../emacs/src/emacs")
}

mod common;

/// The fingerprint-and-dump build runs even when a previous test left a
/// valid image beside an unprocessed Cargo executable: the fingerprint
/// controls below need the binary fingerprinted, not only imaged.
fn emaxx() -> &'static Path {
    common::emaxx(true)
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
    /// object hashes in backtraces by one token.
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

/// `#<bytecode 0x...>' in GNU's backtrace is cl-print's `%#x' of the
/// object's `sxhash', which mixes in object addresses; `format' prints a
/// negative one as `-0x...', so either sign becomes the one token.
fn replace_addresses(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(index) = rest.find("0x") {
        let before = &rest[..index];
        out.push_str(before.strip_suffix('-').unwrap_or(before));
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
    // `--version' wins over `--help' and prints each binary's own
    // "Development version" line (its revision and build date, which
    // agree only when both were built the same day): compare without it,
    // as `version_report_is_emacs_c_main_s' does.
    let args = ["--help", "--version"];
    let ((actual_out, actual_err, actual_status), (expected_out, expected_err, expected_status)) =
        both(&corpus, &args);
    assert_eq!((actual_err, actual_status), (expected_err, expected_status));
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
#[cfg(unix)]
fn dumped_fingerprint_survives_copying_and_resigning_the_executable() {
    let corpus = Corpus::new();
    // Dumped native units are relative to each executable directory.
    // Mirror their relative layout under the corpus without changing either
    // editor's original executable, dump, or native libraries.
    let native_roots = [
        oracle().parent().unwrap().join("../native-lisp"),
        emaxx::compat::configured_gnu_source_root().join("native-lisp"),
    ]
    .map(|path| path.canonicalize().unwrap())
    .into_iter()
    .collect::<std::collections::BTreeSet<_>>();
    for native in native_roots {
        let native_link = corpus.directory.join(native.strip_prefix("/").unwrap());
        std::fs::create_dir_all(native_link.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(&native, &native_link).unwrap();
    }
    for (name, binary) in [("gnu", oracle()), ("emaxx", emaxx().to_path_buf())] {
        let expected = corpus.run(&binary, &["--fingerprint"]);
        assert_eq!(expected.2, Some(0), "{name}: {expected:?}");
        let original = binary.canonicalize().unwrap();
        let copied = corpus
            .directory
            .join(original.strip_prefix("/").unwrap())
            .with_file_name(format!("{name}-copied"));
        std::fs::create_dir_all(copied.parent().unwrap()).unwrap();
        std::fs::copy(&binary, &copied).unwrap();
        // Change the file's digest while preserving its executable format.
        // GNU's embedded build fingerprint must still identify its dump.
        #[cfg(not(target_os = "macos"))]
        {
            use std::io::Write;
            std::fs::OpenOptions::new()
                .append(true)
                .open(&copied)
                .unwrap()
                .write_all(b"build fingerprint regression\n")
                .unwrap();
        }
        #[cfg(target_os = "macos")]
        {
            let signed = Command::new("codesign")
                .args(["--force", "--sign", "-"])
                .args(["--identifier", &format!("fingerprint-regression-{name}")])
                .arg(&copied)
                .output()
                .unwrap();
            assert!(signed.status.success(), "codesign: {signed:?}");
        }
        assert!(std::fs::read(&copied).unwrap() != std::fs::read(&binary).unwrap());
        let image = binary.with_extension("pdmp");
        assert!(image.is_file(), "missing image {}", image.display());
        let actual = corpus.run(
            &copied,
            &["--dump-file", image.to_str().unwrap(), "--fingerprint"],
        );
        assert_eq!(actual, expected, "{name} must retain its build fingerprint");
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

#[test]
fn open_file_limit_is_init_process_emacs_s() {
    // process.c:init_process_emacs lowers the soft RLIMIT_NOFILE to
    // FD_SETSIZE for Emacs itself; a child made through posix_spawn
    // (call-process, a pipe process) keeps that limit, and one made
    // through a pty's vfork gets the limit Emacs started with back
    // (callproc.c:child_setup's restore_nofile_limit).
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(let ((own (with-temp-buffer
             (insert-file-contents "/proc/self/limits")
             (re-search-forward "^Max open files +\\([0-9]+\\|unlimited\\)")
             (match-string 1)))
      (child (lambda (type)
               (let* ((buf (generate-new-buffer "out"))
                      (p (make-process :name "l" :buffer buf :sentinel #'ignore
                                       :command '("sh" "-c" "ulimit -Sn")
                                       :connection-type type)))
                 (while (process-live-p p) (accept-process-output p 0.05))
                 (accept-process-output nil 0.05)
                 (with-current-buffer buf (string-trim (buffer-string)))))))
  (princ (format "%s %s %s %s\n" own
                 (with-temp-buffer (call-process "sh" nil t nil "-c" "ulimit -Sn")
                                   (string-trim (buffer-string)))
                 (funcall child 'pipe) (funcall child 'pty))))"#,
        ],
    );
}

#[test]
fn profiler_is_profiler_c_s() {
    // profiler.c: the start and stop protocol and its errors, a CPU log of
    // a busy loop (each key a vector of `profiler-max-stack-depth'
    // functions, innermost first, padded with nil), a full log's eviction
    // into `Discarded Samples', the memory profiler's call stacks and its
    // `Automatic GC' entry, and `function-equal'.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
  (defun pp-work (n) (let ((i 0)) (while (< i n) (setq i (1+ i)))))
  (defun pp-keys (log)
    (let (keys) (maphash (lambda (k v) (push (cons (append k nil) v) keys)) log) keys))
  (let (out)
    (dolist (interval '(0 -5 1.5 nil))
      (push (condition-case e (profiler-cpu-start interval) (error e)) out))
    (push (condition-case e (profiler-cpu-start (expt 2 70)) (error e)) out)
    (push (profiler-cpu-running-p) out)
    (push (profiler-cpu-start 1000000) out)
    (push (condition-case e (profiler-cpu-start 1000000) (error e)) out)
    (push (profiler-cpu-running-p) out)
    (pp-work 3000000)
    (let ((keys (pp-keys (profiler-cpu-log))))
      (push (profiler-cpu-running-p) out)
      (push (seq-uniq (mapcar (lambda (k) (length (car k))) keys)) out)
      (push (seq-every-p (lambda (k) (and (natnump (cdr k)) (> (cdr k) 0))) keys) out)
      (push (and (seq-find (lambda (k) (equal (seq-take (car k) 4) '(< while let pp-work))) keys) t)
            out)
      (push (seq-every-p (lambda (k) (let ((tail (memq nil (car k))))
                                       (seq-every-p #'null tail)))
                         keys)
            out))
    (push (profiler-cpu-stop) out)
    (push (profiler-cpu-stop) out)
    (push (profiler-cpu-running-p) out)
    (push (hash-table-p (profiler-cpu-log)) out)
    (let ((profiler-log-size 3) (profiler-max-stack-depth 2))
      (profiler-cpu-start 100000)
      (dotimes (_ 5)
        (pp-work 200000)
        (let ((i 0)) (while (< i 200000) (setq i (+ i 1))))
        (let ((i 0)) (while (< i 200000) (setq i (- i -1))))
        (let ((i 0)) (while (< i 200000) (setq i (1+ i)) (car nil))))
      (let ((keys (pp-keys (profiler-cpu-log))))
        (profiler-cpu-stop)
        (push (seq-uniq (mapcar (lambda (k) (length (car k))) keys)) out)
        (push (and (assoc '(Discarded\ Samples nil) keys) t) out)
        (push (<= (length (seq-remove (lambda (k) (memq (caar k) '(Discarded\ Samples Automatic\ GC))) keys)) 3) out)))
    (push (condition-case e (progn (profiler-memory-start) (profiler-memory-start)) (error e)) out)
    (push (profiler-memory-running-p) out)
    (let ((l nil)) (dotimes (_ 300000) (push (make-string 10 ?a) l)) (setq l nil))
    (garbage-collect)
    (let ((keys (pp-keys (profiler-memory-log))))
      (push (profiler-memory-running-p) out)
      (push (and (seq-find (lambda (k) (eq (caar k) 'make-string)) keys) t) out)
      (push (and (assoc '(Automatic\ GC nil) keys) t) out)
      (push (and (seq-find (lambda (k) (eq (caar k) 'garbage-collect)) keys) t) out))
    (push (list (profiler-memory-stop) (profiler-memory-stop) (profiler-memory-running-p)) out)
    (push (list (function-equal 'car 'car)
                (function-equal (lambda () 1) (lambda () 1))
                (let ((f (lambda (x) (lambda () x)))) (function-equal (funcall f 1) (funcall f 2))))
          out)
    (princ (format "%S\n" (nreverse out)))))"#,
        ],
    );
}

#[test]
fn huge_years_keep_glibcs_daylight_saving_rules() {
    // glibc's tzset.c:compute_change counts the days to a year's January
    // 1st in int; from year 5,881,581 the count wraps, and the daylight
    // saving time changes it gives, wherever they land, are what
    // `decode-time' and `format-time-string' report, for a TZ rule as for
    // a TZif file's footer.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(dolist (zone '("EST5EDT,M3.2.0,M11.1.0" "America/New_York" "Australia/Sydney"
                         "NZST-12NZDT,M9.5.0,M4.1.0/3" "EST5EDT,J60/2,J300/2"))
  (dolist (time '(1690000000 157722601487760 157722622223760 185650509191760
                  1125899906842624 -1125899906842624 36028797018963968))
    (princ (format "%S %S %S %S\n" zone time (decode-time time zone)
                   (format-time-string "%Y-%m-%d %T %Z %z" time zone)))))"#,
        ],
    );
}

#[test]
#[cfg(target_os = "linux")]
fn atimer_is_atimer_c_s() {
    // atimer.c and keyboard.c: `funcall-with-delayed-message' starts a
    // relative atimer whose expiry only marks a pending signal; the next
    // maybe_quit, in a busy loop or in `sleep-for''s wait, shows the
    // message (on standard error and in *Messages* in batch).  A call
    // finishing first cancels it; a zero or negative timeout expires at
    // once.  init_atimer leaves its timerfd at descriptor 3.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
  (dolist (params '((5 0.05) (0.05 1.5) (0 0.3) (-1 0.3)))
    (let ((timeout (nth 0 params)) (work-time (nth 1 params)))
      (with-current-buffer "*Messages*"
        (let ((inhibit-read-only t)) (erase-buffer))
        (let ((stop (+ (float-time) work-time)))
          (funcall-with-delayed-message timeout "timed out"
            (lambda () (while (< (float-time) stop)) (message "finished"))))
        (princ (format "%S %S\n" params (string-trim (buffer-string)))))))
  (princ (format "%S\n" (condition-case e (funcall-with-delayed-message "x" "m" #'ignore) (error e))))
  (princ (format "%S\n" (condition-case e (funcall-with-delayed-message 1 'm #'ignore) (error e))))
  (princ (format "%S\n" (condition-case e (funcall-with-delayed-message 1 "m" 'no-such-fn) (error e))))
  (princ (format "%S\n" (funcall-with-delayed-message 5 "m" (lambda () 42))))
  (funcall-with-delayed-message 0.05 "during sleep" (lambda () (sleep-for 1.5)))
  (princ (format "%S\n" (file-symlink-p "/proc/self/fd/3"))))"#,
        ],
    );
}

#[test]
#[cfg(target_os = "linux")]
fn byte_code_calls_check_for_pending_signals() {
    // bytecode.c's Bcall runs maybe_quit before every call, however the
    // callee is entered: a delayed message's atimer that expires during a
    // byte-code recursion with no loop in it shows at the next call, of a
    // lexical-binding function entered in place or of a dynamic-binding
    // one through funcall_lambda.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
  (defun pend-lex (n) (if (< n 2) n (+ (pend-lex (- n 1)) (pend-lex (- n 2)))))
  (eval '(defun pend-dyn (n) (if (< n 2) n (+ (pend-dyn (- n 1)) (pend-dyn (- n 2))))) nil)
  (require 'bytecomp)
  (setq byte-compile-warnings nil)
  (byte-compile 'pend-lex)
  (byte-compile 'pend-dyn)
  (dolist (f '(pend-lex pend-dyn))
    (with-current-buffer "*Messages*"
      (let ((inhibit-read-only t)) (erase-buffer))
      (princ (format "%S %S" f (funcall-with-delayed-message 0.01 (format "%s ran long" f)
                                   (lambda () (funcall f 30)))))
      (princ (format " %S\n" (string-trim (buffer-string)))))))"#,
        ],
    );
}

#[test]
fn dynamic_binding_byte_code_binds_as_funcall_lambda() {
    // eval.c:funcall_lambda's binding of a dynamic-binding function's
    // formals, for byte code called from byte code: each specbound for the
    // call and its callees, &optional ones to nil, the first after &rest to
    // the list of the rest, unbound on return, signal and throw; the
    // arity and arglist errors, a constant's `setting-constant', a C
    // variable's and a watched variable's bindings, a binding made local
    // inside, the backtrace's locals, the depth limit, and the live arglist.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
  (require 'bytecomp)
  (setq byte-compile-warnings nil)
  (eval '(progn
    (defvar dz-a 'outer-a) (defvar dz-b 'outer-b) (defvar dz-w 1) (defvar dz-log nil)
    (defun dz-peek () (list dz-a dz-b))
    (defun dz-basic (dz-a dz-b) (list dz-a dz-b (dz-peek)))
    (defun dz-opt (dz-a &optional dz-b c) (list dz-a dz-b c (dz-peek)))
    (defun dz-rest (dz-a &rest dz-b) (list dz-a dz-b (dz-peek)))
    (defun dz-optrest (dz-a &optional dz-b &rest c) (list dz-a dz-b c))
    (defun dz-setq (dz-a) (setq dz-a (* dz-a 10)) (list dz-a (dz-peek)))
    (defun dz-err (dz-a) (if (> dz-a 0) (dz-err (1- dz-a)) (signal 'error (list "bottom" (dz-peek)))))
    (defun dz-throw (dz-a) (if (> dz-a 0) (dz-throw (1- dz-a)) (throw 'dz-tag (dz-peek))))
    (defun dz-local (dz-a) (make-local-variable 'dz-a) (setq dz-a 99) (list dz-a))
    (defun dz-cfs (case-fold-search) (list case-fold-search (string-match "A" "a")))
    (defun dz-locals (dz-a dz-b)
      (let ((n 0) found)
        (while (and (< n 40) (not found))
          (when (eq (cadr (backtrace-frame n)) 'dz-locals) (setq found (backtrace--locals n)))
          (setq n (1+ n)))
        (assq 'dz-a found)))
    (defun dz-deep (n) (if (= n 0) 0 (1+ (dz-deep (1- n)))))
    (defun dz-many (a b c d e f g h i j) (list a b c d e f g h i j))
    (defun dz-watch (dz-w) (list dz-w))
    (defun dz-mut (p q) (list p q))
    (defun dz-caller (x) (list (dz-basic x 'y) (dz-opt x) (dz-rest x 1 2) (dz-optrest x 1 2 3)
                               (dz-setq 5) (condition-case e (dz-err 3) (error e)) (catch 'dz-tag (dz-throw 3))
                               (dz-local 1) (dz-cfs nil) (dz-cfs t) (dz-locals 1 2) (dz-deep 100)
                               (dz-many 1 2 3 4 5 6 7 8 9 10) (dz-watch 5) (dz-mut 1 2) (dz-peek))))
    nil)
  (dolist (f '(dz-peek dz-basic dz-opt dz-rest dz-optrest dz-setq dz-err dz-throw dz-local dz-cfs
               dz-locals dz-deep dz-many dz-watch dz-mut dz-caller))
    (byte-compile f))
  (add-variable-watcher 'dz-w (lambda (s n o w) (push (list s n o w) dz-log)))
  (dolist (form '((dz-caller 'x) (list dz-a dz-b case-fold-search (local-variable-p 'dz-a))
                  (dz-basic 1) (dz-basic 1 2 3) (dz-opt 1 2 3 4) (dz-many 1 2)
                  (let ((max-lisp-eval-depth 100)) (dz-deep 500))
                  (progn (setcar (aref (symbol-function 'dz-mut) 0) 'pp) (dz-mut 1 2))
                  (list (boundp 'pp) (boundp 'p))
                  (progn (defalias 'dz-bad (make-byte-code '(a &rest) "\300\207" [nil] 2)) (dz-bad 1))
                  (progn (defalias 'dz-bad2 (make-byte-code '(a . b) "\300\207" [nil] 2)) (dz-bad2 1))
                  (progn (defalias 'dz-bad3 (make-byte-code '(a 5) "\300\207" [nil] 2)) (dz-bad3 1 2))
                  (progn (defalias 'dz-kw (make-byte-code '(:k) "\300\207" [nil] 2)) (dz-kw 1))
                  (progn (defalias 'dz-nil (make-byte-code '(nil) "\300\207" [nil] 2)) (dz-nil 1))
                  (funcall (byte-compile (lambda (x) (dz-basic x 3))) 4)
                  (reverse dz-log)))
    (princ (format "%S\n" (condition-case e (eval form t) (error (list 'error e)))))))"#,
        ],
    );
}

#[test]
fn set_buffer_multibyte_is_fset_buffer_multibyte() {
    // buffer.c:Fset_buffer_multibyte: the undo list is t before the
    // narrowed buffer's error, which leaves it so; each eight-bit
    // character converted is a one-byte insertion (insert_1_both) or
    // deletion (del_range_2) that advances the modification ticks; an
    // unmodified buffer is restored to unmodified; a list kept for undo
    // gets one `apply' entry for the conversion.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(dolist (text (list "abc" "aé" (string-to-unibyte "a\377b") (string-to-multibyte "a\377\200b")))
  (dolist (flag '(nil t to))
    (dolist (undo '(t nil))
      (dolist (modified '(nil t))
        (with-temp-buffer
          (when undo (buffer-enable-undo))
          (unless (multibyte-string-p text) (set-buffer-multibyte nil))
          (insert text)
          (unless modified (set-buffer-modified-p nil))
          (let ((tick (buffer-modified-tick)) (chars (buffer-chars-modified-tick)) (marker (copy-marker 3)))
            (goto-char 2)
            (let ((value (condition-case e (set-buffer-multibyte flag) (error e))))
              (princ (format "%S %S %S %S: %S %S %S %S %S %S %S %S %S\n" text flag undo modified value
                             enable-multibyte-characters (buffer-string) (point) (buffer-modified-p)
                             (- (buffer-modified-tick) tick) (- (buffer-chars-modified-tick) chars)
                             buffer-undo-list (marker-position marker)))
              (narrow-to-region 1 2)
              (princ (format "  %S %S\n" (condition-case e (set-buffer-multibyte (not enable-multibyte-characters)) (error e))
                             buffer-undo-list)))))))))"#,
        ],
    );
}

#[test]
fn text_property_analysis_precedes_the_buffer_hooks() {
    // textprop.c: add_text_properties_1, Fremove_text_properties and
    // Fremove_list_of_text_properties split the interval at START (the
    // list form also the first interval with a property at END), and
    // validate_interval_range makes the root interval, before
    // modify_text_properties runs the hooks, so a hook's error leaves
    // them; a malformed plist signals before the hooks run.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
  (with-temp-buffer
    (insert "abcdefghij")
    (put-text-property 2 6 'p 1)
    (let ((ticks (buffer-modified-tick)) (calls 0))
      (add-hook 'before-change-functions (lambda (&rest _) (setq calls (1+ calls))) nil t)
      (print (condition-case e (remove-text-properties 1 8 '(q . b)) (error e)))
      (print (condition-case e (remove-text-properties 1 8 '(p . b)) (error e)))
      (print (condition-case e (add-text-properties 1 8 '(p 1 . b)) (error e)))
      (print (list calls (- (buffer-modified-tick) ticks) (object-intervals (current-buffer))))))
  (dolist (op (list (lambda () (put-text-property 3 5 'p 1))
                    (lambda () (add-face-text-property 3 5 'bold))
                    (lambda () (set-text-properties 3 4 '(q 1)))
                    (lambda () (set-text-properties 3 4 nil))))
    (with-temp-buffer
      (insert "abcdefghij")
      (add-hook 'before-change-functions (lambda (&rest _) (error "no")) nil t)
      (print (list (condition-case e (funcall op) (error e)) (object-intervals (current-buffer))))))
  (with-temp-buffer
    (insert "abcdefghij")
    (put-text-property 1 11 'p 1)
    (dolist (op (list (lambda () (remove-list-of-text-properties 3 5 '(p)))
                      (lambda () (remove-text-properties 6 8 '(p nil)))
                      (lambda () (add-text-properties 9 10 '(p 1 q 2)))
                      (lambda () (put-text-property 7 9 'p 1))))
      (add-hook 'before-change-functions (lambda (&rest _) (error "no")) nil t)
      (print (list (condition-case e (funcall op) (error e)) (object-intervals (current-buffer))))))
  (with-temp-buffer
    (insert "abcdefghij")
    (add-text-properties 5 7 '(display 1 read-only "s"))
    (print (list (condition-case e (add-text-properties 6 8 '(face bold)) (error e))
                 (buffer-string)))))"#,
        ],
    );
}

#[test]
fn combined_after_change_calls_as_insdel() {
    // insdel.c: a deferred change's unchanged end counts a deletion's
    // text as unchanged, so the combined change can end before it starts
    // and its counts go negative, as signal_after_change passes them;
    // Fcombine_after_change_execute leaves `combine-after-change-calls'
    // as it is, deferring the change again under a non-nil flag; another
    // buffer's change joins the pending list after it is executed.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
(defvar cacc-log nil)
(let ((a (generate-new-buffer "a")) (b (generate-new-buffer "b")))
  (dolist (buf (list a b))
    (with-current-buffer buf
      (insert "hello world")
      (add-hook 'after-change-functions
                (lambda (beg end len) (push (list (buffer-name) beg end len) cacc-log)) nil t)))
  (with-current-buffer a
    (goto-char 4)
    (combine-after-change-calls (insert "c") (delete-region 3 6))
    (goto-char 2)
    (combine-after-change-calls (insert "xyz") (delete-region 1 9))
    (combine-after-change-calls (delete-region 1 2) (delete-region 1 2))
    (combine-after-change-calls (insert "abc") (goto-char 1) (insert "q")))
  (let ((combine-after-change-calls t))
    (with-current-buffer a (goto-char 1) (insert "1"))
    (push 'switch cacc-log)
    (with-current-buffer b (goto-char 3) (insert "2") (delete-region 1 2))
    (push 'execute cacc-log)
    (combine-after-change-execute)
    (push 'executed cacc-log))
  (combine-after-change-execute)
  (print (nreverse cacc-log))
  (print (list (with-current-buffer a (buffer-string)) (with-current-buffer b (buffer-string))))))"#,
        ],
    );
}

#[test]
fn arithmetic_is_data_c_arith_driver() {
    // data.c:arith_driver folds the integers before the first float
    // exactly (in intmax_t, then a bignum) and converts that once, so
    // (* -1 0 3.0) is 0.0 and a bignum's sum survives; `/' is all floats
    // when a later divisor is one; floatop_arith_driver's NaN is the later
    // operand's for `+' and `*' and the accumulator's for `-' and `/'.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(let ((p 0.0e+NaN) (n (/ 0.0 0.0)))
  (dolist (f '(+ - * / min max))
    (print (list f (funcall f p n) (funcall f n p) (funcall f p p 1.0 n) (funcall f n 2 p) (funcall f 1 n))))
  (print (list (+ (1- most-negative-fixnum) most-positive-fixnum 0.5)
               (+ (1- most-negative-fixnum) most-positive-fixnum 5e-324)
               (+ most-positive-fixnum 1 0.5) (* (expt 2 70) 2 0.5) (- (expt 2 70) 1 0.5)
               (* -1 0 3.0) (* -1 0.0 3) (- 0 0 0.0) (* 0 -1.0) (* -1.0 0)
               (/ 6 4 2.0) (/ 6 4.0 2) (/ 7 2 0.5) (/ (expt 2 70) 3 2.0) (/ 7 (expt 2 70))
               (- 5 (expt 2 70) 1.0) (+ 1 (expt 2 62) 0.5) (* 3 (expt 2 61) 1.5)
               (condition-case e (/ 5 0 1.0) (error e)) (condition-case e (/ 5 0.0 1) (error e))
               (condition-case e (+ 1.0 'x 2) (error e)) (condition-case e (* (expt 2 70) 'y) (error e))
               (/ 5) (/ 5.0) (/ (expt 2 70)) (- (expt 2 70)) (- 0.0) (* 2) (+ 2.5))))"#,
        ],
    );
}

#[test]
fn ash_is_data_c_fash() {
    // data.c:Fash: a bignum COUNT keeps 0 and sends the rest to -1, 0 or
    // `overflow-error' by its sign; a fixnum one shifts right in a machine
    // integer and left through emacs_mpz_mul_2exp, whose limb limit
    // refuses a count past it even for 0.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
  (dolist (v (list 0 1 -1 7 -7 most-positive-fixnum most-negative-fixnum (1+ most-positive-fixnum) (- (expt 2 70))))
    (print (mapcar (lambda (s) (condition-case e (ash v s) (error e)))
                   (list 0 1 -1 2 61 62 63 64 65 -61 -62 -63 -64 -65 200 -200 most-negative-fixnum
                         (1+ most-positive-fixnum) (1- most-negative-fixnum)))))
  (print (mapcar (lambda (s) (condition-case e (ash 0 s) (error e)))
                 (list 137438953407 137438953408 most-positive-fixnum (- 137438953408))))
  (print (mapcar (lambda (s) (condition-case e (ash 1 s) (error e)))
                 (list 137438953408 most-positive-fixnum)))
  (print (list (condition-case e (ash 1.0 1) (error e)) (condition-case e (ash 1 1.0) (error e))
               (condition-case e (ash (point-marker) 1) (error e)))))"#,
        ],
    );
}

#[test]
fn aref_of_a_char_table_is_char_table_ref() {
    // data.c:Faref's CHECK_CHARACTER and CHAR_TABLE_REF, interpreted, from
    // byte code and through get-char-code-property's native code: a
    // table's own ranges, its parent's values, a Unicode property table's
    // compressed entries, and indices that are no character.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(let* ((parent (make-char-table 'ct-test 'from-parent))
       (table (make-char-table 'ct-test))
       (uni (unicode-property-table-internal 'general-category))
       (compiled (byte-compile (lambda (tb c) (condition-case e (aref tb c) (error e)))))
       (chars (list ?a #x7f #x80 #x3042 #x10000 #x10ffff #x3fff80 #x3fffff -1 #x400000)))
  (set-char-table-parent table parent)
  (set-char-table-range table '(#x3000 . #x30ff) 'kana)
  (aset table ?b 'bee)
  (set-char-table-range parent '(#x10000 . #x10fff) nil)
  (print (mapcar (lambda (c) (condition-case e (aref table c) (error e))) chars))
  (print (mapcar (lambda (c) (funcall compiled table c)) chars))
  (print (mapcar (lambda (c) (funcall compiled uni c)) chars))
  (print (mapcar (lambda (c) (condition-case e (get-char-code-property c 'general-category) (error e)))
                 (list ?a #x3042 #x1f600 #xe000 #x10ffff)))
  (print (mapcar (lambda (c) (condition-case e (get-char-code-property c 'name) (error e)))
                 (list ?a #x3042 #x1f600 #xac00 #x4e00)))
  (print (list (funcall compiled (make-char-table 'x 0) ?z) (funcall compiled (make-char-table 'x) #x2000)
               (condition-case e (aref table 'x) (error e)) (condition-case e (aref table 1.0) (error e)))))"#,
        ],
    );
}

#[test]
fn bindings_and_unwind_records_share_one_specpdl() {
    // eval.c's one specpdl: a throw or a signal undoes the bindings and
    // the unwind records above its catch innermost first, so an
    // unwind-protect's handler runs after the inner bindings are gone and
    // before the outer ones, and a watcher hears each `unlet' in that
    // order; Bunbind over a saved current buffer, a buffer-local binding
    // undone from another buffer, save-excursion and save-restriction in
    // recursion, interpreted and byte-compiled.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
  (defvar sp-1 'g1) (defvar sp-2 'g2) (defvar sp-3 'g3) (defvar sp-4 'g4)
  (defvar sp-log nil)
  (add-variable-watcher 'sp-3 (lambda (sym new op where)
                                (push (list 'watch sym new op (and where (buffer-name where))) sp-log)))
  (defun sp-f1 (b2)
    (catch 'k
      (let ((sp-1 'l1))
        (unwind-protect
            (let ((sp-2 'l2) (sp-3 'l3))
              (save-current-buffer
                (set-buffer b2)
                (push (list 'inner sp-1 sp-2 sp-3 (buffer-name)) sp-log)
                (throw 'k (list sp-1 sp-2 sp-3))))
          (push (list 'cleanup sp-1 sp-2 sp-3 (buffer-name)) sp-log)))))
  (defun sp-f2 (b2)
    (let ((sp-1 1) (sp-2 2))
      (save-current-buffer
        (set-buffer b2)
        (let ((sp-4 4))
          (list sp-1 sp-2 sp-4 (buffer-name))))))
  (defun sp-f3 (b2)
    (with-temp-buffer
      (rename-buffer "sp-home" t)
      (setq-local sp-4 'local)
      (list (let ((sp-4 'bound)) (set-buffer b2) (list sp-4 (buffer-name)))
            sp-4 (buffer-name) (with-current-buffer "sp-home" sp-4))))
  (defun sp-f4 ()
    (condition-case err
        (let ((sp-1 'e1))
          (unwind-protect (let ((sp-2 'e2) (sp-3 'e3)) (signal 'error (list sp-1 sp-2 sp-3)))
            (push (list 'f4-cleanup sp-1 sp-2 sp-3) sp-log)))
      (error (list 'caught err sp-1 sp-2 sp-3))))
  (defun sp-f5 (n)
    (if (= n 0) (list sp-1 sp-2)
      (let ((sp-1 n))
        (save-excursion
          (let ((sp-2 (* 2 n)))
            (save-restriction
              (sp-f5 (1- n))))))))
  (let ((b2 (get-buffer-create "b2")) results)
    (dolist (compiled '(nil t))
      (when compiled (dolist (f '(sp-f1 sp-f2 sp-f3 sp-f4 sp-f5)) (byte-compile f)))
      (setq sp-log nil)
      (push (list compiled (sp-f1 b2) (sp-f2 b2) (sp-f3 b2) (sp-f4) (sp-f5 3) (nreverse sp-log)
                  sp-1 sp-2 sp-3 sp-4 (buffer-name))
            results))
    (dolist (r (nreverse results)) (print r))))"#,
        ],
    );
}

#[test]
fn generated_dynamic_bindings_match_across_buffers() {
    // Forty programs generated from one `random' seed: `let', `setq',
    // `set-default', `make-local-variable', `kill-local-variable' and
    // `setq-local' of two global variables and a buffer-local one in two
    // buffers, under buffer switches, unwind-protect, catch and throw and
    // condition-case, each run interpreted and byte-compiled, its states
    // and GNU's warnings printed.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
  (defvar fz-a 'ga) (defvar fz-b 'gb) (defvar-local fz-c 'gc) (defvar fz-out nil)
  (defun fz-log (x) (push x fz-out))
  (defun fz-pick (l) (nth (random (length l)) l))
  (defun fz-state ()
    '(fz-log (list (buffer-name) fz-a fz-b fz-c (default-value 'fz-a)
                   (default-value 'fz-c) (local-variable-p 'fz-a) (local-variable-p 'fz-c))))
  (defun fz-stmt (d)
    (let ((k (random (if (< d 4) 14 7))) (v (fz-pick '(fz-a fz-b fz-c)))
          (x (fz-pick '('x 'y 'z 1 2 3))) (b (fz-pick '("fz1" "fz2"))))
      (if (< k 7)
          (nth k `((setq ,v ,x) (set-default ',v ,x) (make-local-variable ',v)
                   (kill-local-variable ',v) (set-buffer ,b) ,(fz-state) (setq-local ,v ,x)))
        (let ((body (let (l) (dotimes (_ (1+ (random 3))) (push (fz-stmt (1+ d)) l)) l)))
          (nth (- k 7)
               `((let ((,v ,x)) ,@body)
                 (let ((,v ,x) (,(fz-pick '(fz-a fz-b fz-c)) ,x)) ,@body)
                 (with-current-buffer ,b ,@body)
                 (unwind-protect (progn ,@body) ,(fz-state))
                 (catch 'fz-tag ,@body (throw 'fz-tag nil))
                 (condition-case nil (progn ,@body (error "fz")) (error ,(fz-state)))
                 (save-current-buffer ,@body)))))))
  (get-buffer-create "fz1") (get-buffer-create "fz2")
  (defun fz-run (f)
    (dolist (b '("fz1" "fz2")) (with-current-buffer b (kill-all-local-variables)))
    (set-buffer "fz1")
    (setq-default fz-a 'ga fz-b 'gb fz-c 'gc)
    (setq fz-out nil)
    (condition-case e (funcall f) (error (fz-log (list 'err e))))
    (set-buffer "fz1")
    (nreverse fz-out))
  (random "emaxx-specpdl")
  (let ((programs (let (l) (dotimes (_ 40)
                             (push `(lambda () ,@(let (s) (dotimes (_ (+ 3 (random 4))) (push (fz-stmt 0) s)) s)
                                       ,(fz-state))
                                   l))
                    (nreverse l))))
    (dolist (p programs)
      (let ((f (eval p t)))
        (print (list (fz-run f) (fz-run (byte-compile f))))))))"#,
        ],
    );
}

#[test]
fn matcher_backtracks_and_folds_as_regex_emacs() {
    // re_match_2_internal's failure stack (failure points, saved
    // registers, `\{n,m\}' counters) and a case-folded EXACTN over ASCII
    // and other text, in multibyte and unibyte buffers and strings.
    // No pattern ends in a loop: a POSIX one has no final `succeed', and
    // GNU's forall_firstchar reads the stale byte past it there.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(dolist (case-fold-search '(nil t))
  (dolist (s '("abAB!=->foo" "FOOfooFoO" "éÉéa xXy" "ıİiI kK" "ßSSss" "ABABAB" "->->!=" "x.y.z"))
    (dolist (re '("\\(?:!=\\|##\\|->\\|[-+*/]\\)" "\\(ab\\|AB\\)\\{1,3\\}" "\\(fo\\|FO\\)+o" "\\(É\\|é\\)\\{0,2\\}a"
                  "\\(K\\|\\w\\|SS\\)?\\(K\\)\\{0,2\\}ı*[ k]" "\\(x\\|X\\)\\(\\.\\|y\\)*?z" "ß\\|SS\\|ss" "\\(a\\|b\\)*\\1"))
      (print (list case-fold-search s re (string-match re s) (match-data t)
                   (with-temp-buffer
                     (insert s)
                     (let (r) (dotimes (i (1+ (buffer-size))) (goto-char (1+ i)) (push (and (looking-at re) (match-data t)) r)) r))
                   (with-temp-buffer
                     (set-buffer-multibyte nil)
                     (insert (encode-coding-string s 'utf-8))
                     (let (r) (dotimes (i (1+ (buffer-size))) (goto-char (1+ i)) (push (and (looking-at re) (match-data t)) r)) r))
                   (posix-string-match re s) (match-data t))))))"#,
        ],
    );
}

#[test]
fn unescaped_character_literals_are_recorded_as_read_char_literal() {
    // read_char_literal conses each unescaped `(', `)', `[', `]', `"' or
    // `;' onto `lread--unescaped-character-literals' as it reads it, after
    // CHECK_LIST and unless Fmemq finds it, by a C assignment: in reading
    // order, before a later character fails the read, onto the list it
    // found, seen by a function stream mid-read, in the current buffer's
    // binding; and `load' warns of a file's as it unwinds, even when the
    // file's read fails.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
  (let ((lread--unescaped-character-literals nil) (out nil))
    (push (condition-case e (read-from-string "`?]_") (error e)) out)
    (push lread--unescaped-character-literals out)
    (push (condition-case e (read-from-string "(?; ?[x") (error e)) out)
    (push lread--unescaped-character-literals out)
    (with-temp-buffer
      (insert "(?( . ?\")")
      (goto-char 1)
      (push (condition-case e (read (current-buffer)) (error e)) out)
      (push lread--unescaped-character-literals out)
      (insert " ?)")
      (goto-char (point-max))
      (insert " ?]a")
      (let ((m (copy-marker 10)))
        (push (condition-case e (read m) (error e)) out)
        (push (marker-position m) out)))
    (push lread--unescaped-character-literals out)
    (let ((chars (string-to-list "?;z")))
      (push (condition-case e
                (read (lambda (&optional ch) (if ch (push ch chars) (pop chars))))
              (error e))
            out))
    (push lread--unescaped-character-literals out)
    (print (nreverse out)))
  (let* ((tail (list 40)) (lread--unescaped-character-literals tail) (seen nil)
         (chars (string-to-list "(?) ?( ?[ ?\\( ?))")))
    (print (list (read-from-string "?) ?(") lread--unescaped-character-literals
                 (eq (cdr lread--unescaped-character-literals) tail)))
    (setq lread--unescaped-character-literals nil)
    (print (list (read (lambda (&optional ch)
                         (if ch (push ch chars)
                           (push lread--unescaped-character-literals seen)
                           (pop chars))))
                 (nreverse seen))))
  (dolist (v '(5 "x" (1 . 2) (41 . 2) (1 2 . 3)))
    (let ((lread--unescaped-character-literals v))
      (print (list v (condition-case e (read-from-string "(?) . ") (error e))
                   lread--unescaped-character-literals))))
  (let ((lread--unescaped-character-literals nil))
    (read-from-string "?;")
    (with-temp-buffer
      (setq-local lread--unescaped-character-literals (list 1))
      (read-from-string "?[")
      (print (list lread--unescaped-character-literals
                   (default-value 'lread--unescaped-character-literals))))
    (print lread--unescaped-character-literals))
  (with-temp-file "lit-a.el" (insert "(setq lit-a '(?( ?] ?\\[))\n"))
  (with-temp-file "lit-b.el" (insert "(setq lit-b '(?; ?\" . \n"))
  (print (list (load (expand-file-name "lit-a.el") nil t t) (symbol-value 'lit-a)))
  (print (condition-case e (load (expand-file-name "lit-b.el") nil t t) (error e)))
  (print lread--unescaped-character-literals))"#,
        ],
    );
}

#[test]
fn variable_watchers_hear_stores_as_data_c_tells_them() {
    // set_internal, set_default_internal, specbind and do_one_unbind as
    // they notify the watchers of a plain variable, one local when set,
    // one made local in a buffer, an alias, C variables (a Lisp object,
    // an integer, a boolean), buffer slots, a C variable made local when
    // set (`case-fold-search'), and a keyboard's slot:
    // the operation, the value as given before it is checked or
    // converted, the buffer notify_variable_watchers finds when the
    // caller names none, the second notifications set_default_internal
    // and a buffer slot's set make, a let whose store signals, and
    // make-local-variable and make-variable-buffer-local refusing a
    // keyboard's slot; once interpreted and once byte-compiled.
    let corpus = Corpus::new();
    let program = r#"(progn
  (defvar w-plain 'p0)
  (defvar-local w-local 'l0)
  (defvar w-ml 'm0)
  (defvar w-base 'b0)
  (defvaralias 'w-alias 'w-base)
  (defun w-subst (tree)
    (cond ((eq tree 'V) w-var) ((consp tree) (cons (w-subst (car tree)) (w-subst (cdr tree)))) (t tree)))
  (defvar w-var nil)
  (with-current-buffer (get-buffer-create "wk") (make-local-variable 'w-ml))
  (let ((log nil)
        (ops '((setq V 1)
               (set-default 'V 2)
               (let ((V 3)) (setq V 4))
               (let ((V 5)) (set-default 'V 6))
               (with-temp-buffer (rename-buffer "wt1") (let ((V 7)) (setq V 8)))
               (let ((V 9)) (with-temp-buffer (rename-buffer "wt2") (setq-local V 10)))
               (with-temp-buffer (rename-buffer "wt3") (setq-local V 11)
                                 (let ((V 12)) (setq V 13))
                                 (let ((V 14)) (kill-local-variable 'V))
                                 (setq V 15))
               (with-temp-buffer (rename-buffer "wt4")
                                 (let ((V 16)) (make-local-variable 'V) (setq V 17)))
               (with-current-buffer "wk" (setq V 18) (let ((V 19)) (setq V 20)))
               (let ((V 21)) (with-current-buffer "wk" (setq V 22)))
               (list (local-variable-if-set-p 'V) (local-variable-if-set-p 'V (get-buffer "wk")))
               (make-variable-buffer-local 'V)
               (let ((V 'sym)) 23)
               (set 'V 'sym)
               (setq V nil))))
    (dolist (v '(w-plain w-local w-ml w-alias lread--unescaped-character-literals
                 gc-cons-threshold cursor-in-echo-area fill-column case-fold-search
                 left-margin last-command))
      (let* ((target (indirect-variable v))
             (watcher (lambda (sym nv op where)
                        (push (list sym op nv (and (bufferp where)
                                                   (or (buffer-name where) 'dead)))
                              log))))
        (add-variable-watcher target watcher)
        (setq log nil)
        (let ((saved (default-value v)))
          (dolist (op ops)
            (setq w-var v)
            (condition-case e (push (list 'value (eval (w-subst op) t)) log)
              (error (push (list 'error e) log)))
            (push '| log))
          (remove-variable-watcher target watcher)
          (with-current-buffer "wk" (kill-local-variable v))
          (set-default v saved))
        (print (cons v (nreverse log)))))))"#;
    for program in [
        program.to_owned(),
        program.replace(
            "(eval (w-subst op) t)",
            "(funcall (byte-compile (list 'lambda nil (w-subst op))))",
        ),
    ] {
        assert_same(&corpus, &["--batch", "-Q", "--eval", &program]);
    }
}

#[test]
fn defvar_and_buffer_local_value_are_eval_c_and_data_c_s() {
    // eval.c:defvar asks Fdefault_boundp first, then
    // Finternal__define_uninitialized_variable refuses a symbol an
    // environment an inner binding saved binds lexically (lexbound_p),
    // and the value goes to the default when it is void or to the value
    // the outermost let of the symbol as given saved when that is void;
    // `defvar-1', byte code's, is the same.  buffer_local_value of a
    // buffer without a binding is the default, void when that is.  A
    // hundred programs generated in Lisp from one `random' seed mix
    // let, setq, set-default, makunbound, make-local-variable,
    // kill-local-variable, kill-all-local-variables, defvar,
    // set-default-toplevel-value and an alias over two buffers with
    // nonlocal exits, interpreted and byte-compiled, printing values,
    // defaults, top-level values, localness and buffer-local-variables.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
(print (condition-case e (let ((zq1 1)) (defvar zq1 2) zq1) (error e)))
(print (condition-case e (let ((zq2 1)) (let ((y 2)) (defvar zq2 3)) zq2) (error e)))
(print (condition-case e (let ((zq3 1)) (let ((y 2)) (internal--define-uninitialized-variable 'zq3)) zq3) (error e)))
(defvar zq4) (makunbound 'zq4)
(print (list (let ((zq4 5)) (defvar-1 'zq4 7 "doc") zq4) (symbol-value 'zq4) (get 'zq4 'variable-documentation)))
(with-temp-buffer (setq-local zq5 1) (defvar-1 'zq5 9 nil) (print (list zq5 (default-value 'zq5) (special-variable-p 'zq5))))
(defvar zq6 'a0) (makunbound 'zq6)
(print (list (let ((zq6 5)) (defvar zq6 8) zq6) (symbol-value 'zq6)))
(defvar zq7 'a) (defvaralias 'zq7-al 'zq7) (makunbound 'zq7)
(print (list (let ((zq7 1)) (defvar zq7-al 2) zq7) (boundp 'zq7) (get 'zq7-al 'variable-documentation)))
(defvar zq8 'a0) (get-buffer-create "zq-2")
(with-temp-buffer (makunbound 'zq8) (make-local-variable 'zq8) (setq zq8 'y)
  (print (list (condition-case e (buffer-local-value 'zq8 (get-buffer "zq-2")) (error e))
               (condition-case e (default-value 'zq8) (error e)))))
(let ((zq9 1)) (makunbound 'zq9) (defvar zq9 3) (print (list zq9 (default-boundp 'zq9))))
(random "var-fuzz")
(defvar vz-a 'a0) (defvar vz-b 'b0) (defvar-local vz-c 'c0) (defvar vz-p 'p0)
(put 'vz-p 'permanent-local t)
(defvaralias 'vz-al 'vz-a)
(get-buffer-create "vz1") (get-buffer-create "vz2")
(defun vz-state ()
  (list (buffer-name)
        (mapcar (lambda (v) (if (boundp v) (symbol-value v) 'UNBOUND)) '(vz-a vz-b vz-c vz-p vz-al))
        (mapcar (lambda (v) (condition-case nil (default-value v) (void-variable 'VOID))) '(vz-a vz-c vz-p))
        (mapcar (lambda (v) (condition-case nil (default-toplevel-value v) (void-variable 'VOID))) '(vz-a vz-c))
        (mapcar (lambda (v) (local-variable-p v)) '(vz-a vz-b vz-c vz-p vz-al))
        (seq-filter (lambda (c) (memq (car-safe c) '(vz-a vz-b vz-c vz-p))) (buffer-local-variables))
        (condition-case e (buffer-local-value 'vz-a (get-buffer "vz2")) (error (car e)))))
(defun vz-stmt (r d)
  (let* ((k (random (if (< d 3) 22 14)))
         (v (nth (random 5) '(vz-a vz-b vz-c vz-p vz-al)))
         (x (nth (random 5) '('x 'y 1 2 nil))))
    (cond
     ((= k 0) `(setq ,v ,x))
     ((= k 1) `(set-default ',v ,x))
     ((= k 2) `(make-local-variable ',v))
     ((= k 3) `(kill-local-variable ',v))
     ((= k 4) `(makunbound ',v))
     ((= k 5) `(set-buffer ,(nth (random 2) '("vz1" "vz2"))))
     ((= k 6) `(vz-log (vz-state)))
     ((= k 7) `(kill-all-local-variables))
     ((= k 8) `(set-default-toplevel-value ',v ,x))
     ((= k 9) `(setq-local ,v ,x))
     ((= k 10) `(vz-log (list 'special (special-variable-p ',v) (boundp ',v) (default-boundp ',v))))
     ((= k 11) `(vz-log (list 'locus (variable-binding-locus ',v))))
     ((= k 12) `(defvar ,v ,x))
     ((= k 13) `(vz-log (list 'ifset (local-variable-if-set-p ',v))))
     (t (let ((body (let (l) (dotimes (_ (1+ (random 3))) (push (vz-stmt r (1+ d)) l)) l)))
          (nth (- k 14)
               `((let ((,v ,x)) ,@body)
                 (let ((,v ,x) (vz-b 'lb)) ,@body)
                 (with-current-buffer ,(nth (random 2) '("vz1" "vz2")) ,@body)
                 (unwind-protect (progn ,@body) (vz-log (vz-state)))
                 (catch 'vz (progn ,@body (throw 'vz nil)))
                 (condition-case e (progn ,@body (error "vz")) (error (vz-log (list 'caught (cadr e)))))
                 (save-current-buffer ,@body)
                 (let* ((,v ,x)) (setq ,v 'inner) ,@body))))))))
(defvar vz-out nil)
(defun vz-log (x) (push x vz-out))
(defun vz-reset ()
  (dolist (b '("vz1" "vz2")) (with-current-buffer b (kill-all-local-variables) (kill-local-variable 'vz-p)))
  (set-buffer "vz1")
  (setq-default vz-a 'a0 vz-b 'b0 vz-c 'c0 vz-p 'p0)
  (setq vz-out nil))
(dotimes (i 100)
  (let ((form `(progn ,@(let (l) (dotimes (_ (+ 3 (random 6))) (push (vz-stmt nil 0) l)) l) (vz-log (vz-state)))))
    (dolist (compiled '(nil t))
      (vz-reset)
      (condition-case e
          (if compiled (funcall (byte-compile `(lambda () ,form))) (eval form t))
        (error (vz-log (list 'err (car e)))))
      (prin1 (list i compiled (nreverse vz-out))) (terpri)
      (set-buffer "vz1"))))
)"#,
        ],
    );
}

#[test]
fn defalias_leaves_the_cycle_check_to_fset() {
    // data.c:defalias hands an advised symbol's new definition to its
    // `defalias-fset-function' (nadvice's), which wraps it before Ffset
    // checks for a cycle: a definition naming the symbol itself is no
    // cycle there.  A hundred and fifty programs generated in Lisp from
    // one `random' seed defalias, fset, advise, unadvise and fmakunbound
    // four symbols and print each one's fboundp, arity, commandp,
    // interactive form, documentation, alias chain, advice and the
    // results of calling it.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn

(random "fn-fuzz")
(defun fz-try (f &rest args) (condition-case e (apply f args) (error (list 'err (car e)))))
(defvar fz-log nil)
(defun fz-describe (s)
  (list s (fboundp s)
        (fz-try (lambda () (func-arity s)))
        (fz-try (lambda () (commandp s)))
        (fz-try (lambda () (car-safe (interactive-form s))))
        (fz-try (lambda () (let ((d (documentation s t))) (and d (car (split-string d "\n"))))))
        (fz-try (lambda () (function-alias-p s)))
        (fz-try (lambda () (subrp (indirect-function s))))
        (fz-try (lambda () (advice-member-p 'fz-adv s)))
        (fz-try (lambda () (funcall s 1 2)))
        (fz-try (lambda () (apply s '(3))))
        (fz-try (lambda () (car-safe (symbol-function s))))))
(let ((names '(fz-f1 fz-f2 fz-f3 fz-f4))
      (targets (list (lambda (a &optional b) "Doc A." (list 'l a b))
                     (lambda (&rest r) (interactive "p") (cons 'rest r))
                     #'+ #'car 'fz-f1 'fz-f2 'list
                     (byte-compile (lambda (x y) "Compiled doc." (list 'bc x y)))
                     (lambda (x) (interactive (list 1)) (list 'int x)))))
  (dotimes (i 150)
    (setq fz-log nil)
    (dolist (n names) (fmakunbound n) (put n 'function-documentation nil))
    (dotimes (_ (+ 2 (random 8)))
      (let ((n (nth (random 4) names)) (k (random 9)))
        (condition-case e
            (cond ((< k 3) (defalias n (nth (random (length targets)) targets) (and (zerop (random 3)) "Alias doc.")))
                  ((= k 3) (fset n (nth (random (length targets)) targets)))
                  ((= k 4) (advice-add n (nth (random 4) '(:around :before :after :filter-return))
                                       (lambda (&rest r) (push (list 'adv n (length r)) fz-log) (if (functionp (car r)) (apply (car r) (cdr r)) (car r)))
                                       '((name . fz-adv))))
                  ((= k 5) (advice-remove n 'fz-adv))
                  ((= k 6) (fmakunbound n))
                  ((= k 7) (put n 'function-documentation "Prop doc."))
                  (t (defalias n (symbol-function (nth (random 4) names)))))
          (error (push (list 'op-err (car e)) fz-log)))))
    (prin1 (list i (mapcar #'fz-describe names) (nreverse fz-log)))
    (terpri)))
)"#,
        ],
    );
}

/// One batch run of BINARY on PROGRAM in a fresh directory holding
/// `f.txt', with no core files and with each of IGNORED inherited ignored:
/// its standard output, the first line of its standard error, its exit
/// status or the signal that ended it, and the directory's files after.
#[cfg(target_os = "linux")]
fn signal_run(
    binary: &Path,
    program: &str,
    ignored: &[i32],
) -> (String, String, String, Vec<String>) {
    use std::os::unix::process::{CommandExt, ExitStatusExt};
    let corpus = Corpus::new();
    std::fs::write(corpus.directory.join("f.txt"), "text\n").unwrap();
    let ignored = ignored.to_vec();
    let mut command = Command::new(binary);
    command
        .args(["--batch", "-Q", "--eval", program])
        .current_dir(&corpus.directory)
        .env("HOME", &corpus.directory)
        .env("LC_ALL", "C")
        .stdin(std::process::Stdio::null());
    // SAFETY: setrlimit and signal are async-signal-safe.
    unsafe {
        command.pre_exec(move || {
            let none = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            libc::setrlimit(libc::RLIMIT_CORE, &none);
            for signal in &ignored {
                libc::signal(*signal, libc::SIG_IGN);
            }
            Ok(())
        });
    }
    let output = command.output().unwrap();
    let ended = match (output.status.code(), output.status.signal()) {
        (Some(code), _) => format!("exit {code}"),
        (None, Some(signal)) => format!("signal {signal}"),
        _ => "unknown".to_string(),
    };
    let first_error_line = String::from_utf8_lossy(&output.stderr)
        .lines()
        .next()
        .unwrap_or("")
        .to_string();
    let mut files = std::fs::read_dir(&corpus.directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    files.sort();
    (
        String::from_utf8_lossy(&output.stdout).into_owned(),
        first_error_line,
        ended,
        files,
    )
}

#[cfg(target_os = "linux")]
#[test]
fn signals_are_init_signals_s() {
    // sysdep.c:init_signals ignores SIGPROF and atimer.c catches SIGALRM,
    // so neither ends Emacs; SIGTERM is caught even when inherited
    // ignored; SIGQUIT and SIGXFSZ (process_fatal_action) and the faults
    // (thread_fatal_action) run terminate_due_to_signal: shut_down_emacs
    // auto-saves and unlocks, and emacs_backtrace writes "Backtrace:"
    // before the signal ends the process (no "Fatal error" line without a
    // controlling terminal).  The backtraces' frames are each binary's own.
    let edit = "(progn (find-file \"f.txt\") (auto-save-mode 1) (insert \"more\") (princ \"edited\\n\") (setq kill-emacs-hook (list (lambda () (princ \"hook\\n\")))))";
    let mut cases: Vec<(String, Vec<i32>)> = vec![
        (
            "(progn (signal-process (emacs-pid) 'SIGPROF) (signal-process (emacs-pid) 'SIGALRM) (sleep-for 0.1) (princ \"survived\\n\"))".to_string(),
            vec![],
        ),
        (
            "(progn (signal-process (emacs-pid) 'SIGTERM) (sleep-for 1) (princ \"survived\\n\"))".to_string(),
            vec![libc::SIGTERM],
        ),
        // kill-emacs's shut_down_emacs auto-saves and unlocks too.
        (format!("(progn {edit} (princ \"done\\n\"))"), vec![]),
    ];
    for signal in ["SIGTERM", "SIGQUIT", "SIGXFSZ", "SIGSEGV", "SIGABRT"] {
        cases.push((
            format!("(progn {edit} (signal-process (emacs-pid) '{signal}) (sleep-for 1) (princ \"survived\\n\"))"),
            vec![],
        ));
    }
    for (program, ignored) in cases {
        assert_eq!(
            signal_run(emaxx(), &program, &ignored),
            signal_run(&oracle(), &program, &ignored),
            "Emaxx (left) and GNU (right) differ for {program}"
        );
    }
}

#[test]
fn let_binds_what_specbind_accepts_and_properties_follow_symbols() {
    // eval.c's let and let* leave the constants to specbind's set_internal:
    // a keyword bound to itself is no change, and a symbol that only looks
    // like a keyword, an uninterned or private obarray one, is an ordinary
    // variable.  A text property's `category' symbol gives its properties
    // as Fget of the object, and `buffer-local-variables' lists an
    // uninterned variable as itself.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
(defun t3-try (f) (condition-case e (funcall f) (error (list 'err e))))
(defvar t3-dyn 5)
(dolist (lexical '(nil t))
  (dolist (form (list '(let ((:k :k)) (list :k))
                      '(let* ((:k :k) (:j :j)) (list :k :j))
                      '(let ((:k 2)) 1)
                      '(let* ((:k nil)) 1)
                      '(let ((nil nil)) 1)
                      '(let* ((t t)) 1)
                      '(let ((t3-dyn 1) (:k :k)) t3-dyn)
                      '(let ((t3-dyn 1) (:k 3)) t3-dyn)
                      (let ((x (make-symbol ":zz"))) (list 'let (list (list x 1)) x))
                      (let ((x (make-symbol ":zz"))) (list 'let* (list (list x 1)) (list 'list x)))
                      (let ((x (intern ":zz" (obarray-make)))) (list 'let (list (list x 1)) x))
                      (let ((x (make-symbol "nil"))) (list 'let (list (list x 2)) x))
                      (let ((x (make-symbol "t"))) (list 'let* (list (list x 3)) x))))
    (prin1 (list lexical (t3-try (lambda () (eval form lexical)))))
    (terpri)))
(prin1 (list 'after t3-dyn (t3-try (lambda () (symbol-value :k)))))
(terpri)
;; A category symbol's plist by the symbol object, uninterned or interned.
(dolist (cat (list (make-symbol "t3-cat") 't3-cat (intern "t3-cat" (obarray-make)) nil))
  (when cat (put cat 'face 'bold) (put cat 'help-echo "help"))
  (with-temp-buffer
    (insert (propertize "abc" 'category cat 'mouse-face 'highlight))
    (prin1 (list (get-text-property 1 'face) (get-char-property 2 'help-echo)
                 (get-text-property 1 'mouse-face) (text-property-not-all 1 4 'face 'bold)
                 (next-single-char-property-change 1 'face)))
    (terpri)))
;; buffer-local-variables lists an uninterned variable as itself.
(with-temp-buffer
  (let ((v (make-symbol "t3-local")) (w (make-symbol "t3-void")))
    (set (make-local-variable v) 7)
    (make-local-variable w)
    (let ((locals (buffer-local-variables)))
      (prin1 (list (eq (car (assq v locals)) v) (cdr (assq v locals))
                   (and (memq w locals) t) (local-variable-p v) (buffer-local-value v (current-buffer)))))
    (terpri)))
)"#,
        ],
    );
}

#[test]
fn overlay_categories_and_symbol_properties_are_read_by_object() {
    // buffer.c:Foverlay_get and lookup_char_property read an overlay's
    // `category' symbol with Fget of the object, whether it is uninterned,
    // interned or a private obarray's; `featurep''s subfeatures and
    // `find-operation-coding-system''s target index are the symbol's own
    // properties.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
;; An overlay's `category' symbol gives its properties as Fget of the
;; object: uninterned, interned, a private obarray's, nil.
(dolist (cat (list (make-symbol "t4-cat") 't4-cat (intern "t4-cat" (obarray-make)) nil))
  (when cat (put cat 'face 'italic) (put cat 'priority 5))
  (with-temp-buffer
    (insert "abcdef")
    (let ((o (make-overlay 2 5)))
      (overlay-put o 'category cat)
      (overlay-put o 'mouse-face 'highlight)
      (prin1 (list (overlay-get o 'face) (overlay-get o 'priority) (overlay-get o 'category)
                   (get-char-property 3 'face) (get-pos-property 3 'face)
                   (next-single-char-property-change 1 'face) (previous-single-char-property-change 6 'face)
                   (get-char-property-and-overlay 3 'face)))
      (terpri))))
;; A symbol's properties read by the object where the C code holds one.
(let ((sub (make-symbol "t4-sub")))
  (put 't4-feature 'subfeatures (list sub 'b))
  (provide 't4-feature)
  (prin1 (list (featurep 't4-feature 'b) (featurep 't4-feature sub) (featurep 't4-feature 'c)
               (find-operation-coding-system 'insert-file-contents "x.el")))
  (terpri))
)"#,
        ],
    );
}

#[test]
fn defconst_stores_with_set_default_and_uninterned_symbols_are_kept_as_objects() {
    // eval.c:Fdefconst_1 declares the symbol as given and stores with
    // Fset_default, so into a `let''s binding of the variable; and an
    // uninterned symbol keeps its identity through `kill-local-variable',
    // `kill-all-local-variables', `defalias' with advice and an
    // `interactive-form' alias chain, each reading the object rather than
    // its name.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
(defvar t5-log nil)
(defun t5-watch (sym val op where) (push (list (symbol-name sym) val op (and where t)) t5-log))
;; kill-local-variable and kill-all-local-variables of an uninterned
;; variable tell its own watchers and return it.
(let ((v (make-symbol "t5-local")))
  (add-variable-watcher v #'t5-watch)
  (with-temp-buffer
    (set (make-local-variable v) 1)
    (prin1 (list (eq (kill-local-variable v) v) (local-variable-p v)))
    (set (make-local-variable v) 2)
    (kill-all-local-variables)
    (prin1 (list (local-variable-p v) (nreverse t5-log))))
  (terpri))
;; defalias of an uninterned symbol: its history, docstring and advice.
(let ((f (make-symbol "t5-fn")))
  (defalias f (lambda (x) (* x 2)) "Doubles.")
  (advice-add f :around (lambda (orig x) (1+ (funcall orig x))))
  (defalias f (lambda (x) (* x 3)))
  (prin1 (list (funcall f 2) (documentation f) (and (advice-member-p (lambda (orig x) (1+ (funcall orig x))) f) t)
               (eq (car-safe (get f 'function-history)) nil)))
  (terpri))
;; interactive-form along an alias chain of uninterned symbols.
(let ((a (make-symbol "t5-a")) (b (make-symbol "t5-b")))
  (defalias b (lambda () (interactive "p") 1))
  (defalias a b)
  (put b 'interactive-form '(interactive "P"))
  (prin1 (list (interactive-form a) (commandp a)))
  (terpri))
;; defconst stores with set-default, and declares the symbol as given.
(defvar t5-c 0)
(prin1 (list (let ((t5-c 1)) (defconst t5-c 2) t5-c) t5-c))
(terpri)
(defvaralias 't5-al 't5-base)
(defconst t5-al 7 "Alias doc.")
(prin1 (list t5-base (get 't5-al 'variable-documentation) (get 't5-base 'variable-documentation)
             (get 't5-al 'risky-local-variable) (get 't5-base 'risky-local-variable)))
(terpri)
(prin1 (list (defconst-1 't5-c1 3 "C1 doc.") t5-c1 (get 't5-c1 'variable-documentation) (special-variable-p 't5-c1)))
(terpri)
)"#,
        ],
    );
}

#[test]
fn void_errors_carry_their_symbol_through_a_collection() {
    // eval.c's `(void-variable SYMBOL)' and `(void-function SYMBOL)' hold
    // the symbol object: an uninterned one is `eq' to itself in the
    // handler, interpreted or byte-compiled, and the error alone keeps it
    // through a collection while `unwind-protect' unwinds.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
;; A void uninterned function's or variable's error names the symbol itself.
(let ((f (make-symbol "t6-nf")))
  (prin1 (list (condition-case e (funcall f) (void-function (eq (cadr e) f)))
               (condition-case e (symbol-value f) (void-variable (eq (cadr e) f)))
               (condition-case e (eval f t) (void-variable (eq (cadr e) f)))
               (condition-case e (eval (list f) t) (void-function (eq (cadr e) f)))
               (condition-case e (funcall (byte-compile (list 'lambda () (list f)))) (void-function (eq (cadr e) f)))))
  (terpri))
;; The error alone holds the symbol across a collection during unwinding.
(let ((r (condition-case e
             (unwind-protect (funcall (make-symbol "t6-gone"))
               (garbage-collect) (make-list 10000 nil) (garbage-collect))
           (void-function e))))
  (prin1 (list (car r) (symbolp (cadr r)) (symbol-name (cadr r)) (intern-soft (symbol-name (cadr r)))
               (error-message-string r)))
  (terpri))
(prin1 (list (condition-case e (funcall nil) (error e)) (condition-case e (t6-undefined-fn 1) (error e))
             (condition-case e t6-unbound-var (error e)) (error-message-string '(void-function \(setf\ x\)))))
(terpri)
)"#,
        ],
    );
}

#[test]
fn hooks_run_as_eval_c_walks_them() {
    // eval.c:run_hook_with_args reads the hook symbol's value as C reads
    // a variable: a lambda value is the one function, a `t' (in a local
    // or a global value) runs the default's functions in its place, a void
    // default signals, the conses are walked as they stand while the
    // functions run, and every argument of `run-hooks' must be a symbol.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
(defvar zlog nil)
(defun zl (&rest a) (push (cons 'zl a) zlog) nil)
(defun zm (&rest a) (push (cons 'zm a) zlog) nil)
(defun zt (&rest a) (push (cons 'zt a) zlog) t)
(defmacro zrun (&rest body) `(progn (setq zlog nil) (let ((r (condition-case e (progn ,@body) (error (list 'err e))))) (list r (reverse zlog)))))
(defvar zh1 nil) (defvar zh2 nil) (defvar zh3 nil)
;; A lambda value is the one function.
(setq zh1 '(lambda (&rest a) (push (cons 'lam a) zlog)))
(prin1 (zrun (run-hook-with-args 'zh1 1 2))) (terpri)
;; A t in the global value runs the default in its place.
(setq zh2 '(zl t zm))
(prin1 (zrun (run-hooks 'zh2))) (terpri)
;; A non-symbol argument.
(prin1 (zrun (run-hooks 'zh2 1))) (terpri)
(prin1 (zrun (run-hook-with-args 2))) (terpri)
;; A local list with t, a default that is a lambda.
(with-temp-buffer
  (setq-default zh3 '(lambda () (push 'deflam zlog)))
  (setq-local zh3 '(zl t zm))
  (prin1 (zrun (run-hooks 'zh3))) (terpri)
  (setq-default zh3 'zm)
  (prin1 (zrun (run-hooks 'zh3))) (terpri)
  (setq-default zh3 nil)
  (prin1 (zrun (run-hook-with-args-until-success 'zh3))) (terpri))
;; A local t with a void default.
(let ((s (make-symbol "zgen")))
  (with-temp-buffer
    (set (make-local-variable s) (list 'zl t))
    (prin1 (zrun (run-hooks s))) (terpri)
    (prin1 (zrun (run-hook-with-args s 7))) (terpri))
  (set s (list 'zm 'zl))
  (prin1 (zrun (run-hook-with-args s 8))) (terpri)
  (prin1 (zrun (run-hook-with-args-until-failure s 8))) (terpri)
  (prin1 (zrun (run-hook-wrapped s (lambda (f &rest a) (push (cons 'wrap f) zlog) nil) 9))) (terpri))
;; Destructive change of the list while it runs.
(defvar zh4 nil)
(defun zcut () (push 'cut zlog) (setcdr zh4 (list 'zm)))
(setq zh4 (list 'zcut 'zl 'zl))
(prin1 (zrun (run-hooks 'zh4))) (terpri)
;; A hook that removes itself.
(defvar zh5 nil)
(defun zself () (push 'self zlog) (remove-hook 'zh5 'zself))
(setq zh5 (list 'zself 'zl))
(prin1 (zrun (run-hooks 'zh5) (run-hooks 'zh5))) (terpri)
;; until-success / until-failure with t
(setq zh2 '(zl zt zm))
(prin1 (zrun (run-hook-with-args-until-success 'zh2 3))) (terpri)
(prin1 (zrun (run-hook-with-args-until-failure 'zh2 3))) (terpri)
;; Lexical binding of the hook name is not read.
(prin1 (zrun (funcall (eval '(lambda () (let ((zh-lex '(zl))) (run-hooks 'zh-lex))) t)))) (terpri)
;; dotted list, non-list value
(setq zh1 (cons 'zl 'zm))
(prin1 (zrun (run-hooks 'zh1))) (terpri)
(setq zh1 "str")
(prin1 (zrun (run-hooks 'zh1))) (terpri)
(setq zh1 '(t))
(prin1 (zrun (run-hooks 'zh1))) (terpri)
;; void hook
(prin1 (zrun (run-hooks 'zh-void-never))) (terpri)
(prin1 (zrun (run-mode-hooks 'zh2))) (terpri)
)"#,
        ],
    );
}

#[test]
fn safe_run_hooks_binds_inhibit_quit_and_drops_a_failing_function() {
    // keyboard.c:safe_run_hooks runs `kill-emacs-hook' in batch with
    // `inhibit-quit' bound to t; a function's error is reported and the
    // function deleted from the hook, the others still running.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
(defun zk1 () (princ (format "k1 %S\n" inhibit-quit)))
(defun zk2 () (error "boom %d" 2))
(defun zk3 () (princ (format "k3 %S\n" kill-emacs-hook)))
(setq kill-emacs-hook (list 'zk1 'zk2 'zk3))
(let ((s (make-symbol "zg")))
  (set s (list 'zk1 'zk2 'zk3))
  (setq post-gc-hook nil))
(kill-emacs 0))"#,
        ],
    );
}

#[test]
fn an_invalid_function_error_names_the_function_called() {
    // eval.c:funcall_general signals `invalid-function' with ORIGINAL_FUN:
    // the symbol called (an alias included) when its definition is a
    // macro, a cons that is no lambda or any other non-function, and the
    // definition itself only when that was what was called.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
(defalias 'zz-when 'when)
(defalias 'zz-junk '(foo . bar))
(defalias 'zz-junk2 "str")
(defalias 'zz-junk3 '((x) 1))
(defalias 'zz-sf 'if)
(dolist (form '((funcall 'when 1) (funcall 'zz-when 1) (funcall (symbol-function 'when) 1) (apply 'lambda '(5)) (funcall 'zz-junk) (funcall 'zz-junk2) (funcall 'zz-junk3)
                (funcall 'zz-sf 1) (funcall 'if 1) (mapcar 'when '(1)) (zz-junk) (zz-junk3) (funcall (byte-compile '(lambda () (zz-junk3)))) (funcall (byte-compile '(lambda () (funcall 'when 1))))
                (run-hooks 'zz-junk) (funcall '(macro . car) 1) (funcall '(closure) 1)))
  (prin1 (list form (condition-case e (eval form t) (error (if (memq (car e) '(invalid-function void-function)) (list (car e) (if (symbolp (cadr e)) (cadr e) (type-of (cadr e)))) e))))) (terpri)))"#,
        ],
    );
}

#[test]
fn generated_hook_programs_run_as_gnu_runs_them() {
    // 150 generated programs that add, remove and set hook functions
    // locally and globally (`t' markers, lambdas, a function that edits
    // the list it is run from, one that removes itself, one that signals),
    // run them by every runner and print each run's answer and calls.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
(random "hook-fuzz")
(defvar hk-log nil)
(defun hk-try (f &rest args) (condition-case e (apply f args) (error (list 'err (car e) (cdr e)))))
(dolist (n '(hk-a hk-b hk-c hk-d))
  (defalias n (let ((n n)) (lambda (&rest args) (push (cons n args) hk-log) (eq n 'hk-c)))))
(defalias 'hk-x (lambda (&rest _) (push 'x hk-log) (error "hk-x failed")))
(defalias 'hk-cut (lambda (&rest _) (push 'cut hk-log) (when (consp hk-h) (setcdr hk-h nil)) nil))
(defalias 'hk-self (lambda (&rest _) (push 'self hk-log) (remove-hook 'hk-h 'hk-self) (remove-hook 'hk-h 'hk-self t) nil))
(defvar hk-h nil)
(defun hk-fn () (nth (random 8) '(hk-a hk-b hk-c hk-d hk-x hk-cut hk-self (lambda (&rest a) (push (cons 'lam a) hk-log) nil))))
(defun hk-run ()
  (setq hk-log nil)
  (let ((r (pcase (random 7)
             (0 (hk-try #'run-hooks 'hk-h))
             (1 (hk-try #'run-hook-with-args 'hk-h 1 2))
             (2 (hk-try #'run-hook-with-args-until-success 'hk-h 3))
             (3 (hk-try #'run-hook-with-args-until-failure 'hk-h 4))
             (4 (hk-try #'run-hook-wrapped 'hk-h (lambda (f &rest a) (push (list 'w f a) hk-log) (eq f 'hk-b)) 5))
             (5 (hk-try #'run-hooks 'hk-h 'hk-h))
             (_ (hk-try #'run-mode-hooks 'hk-h)))))
    (list r (reverse hk-log))))
(dotimes (i 150)
  (makunbound 'hk-h) (defvar hk-h nil) (setq-default hk-h nil)
  (with-temp-buffer
    (let ((log nil))
      (dotimes (_ (+ 1 (random 6)))
        (push (pcase (random 9)
                (0 (hk-try #'add-hook 'hk-h (hk-fn)))
                (1 (hk-try #'add-hook 'hk-h (hk-fn) (nth (random 4) '(nil t 90 -90)) t))
                (2 (hk-try #'remove-hook 'hk-h (hk-fn) (zerop (random 2))))
                (3 (hk-try #'set 'hk-h (nth (random 5) (list nil (hk-fn) (list (hk-fn) t (hk-fn)) (cons (hk-fn) (hk-fn)) '(t)))))
                (4 (hk-try #'set-default 'hk-h (nth (random 5) (list nil (hk-fn) (list (hk-fn) t) '(lambda () (push 'deflam hk-log)) '(t hk-a)))))
                (5 (hk-try #'kill-local-variable 'hk-h))
                (6 (hk-try #'make-local-variable 'hk-h))
                (7 (hk-run))
                (_ (hk-try #'makunbound 'hk-h)))
              log))
      (prin1 (list i (nreverse log) (hk-run) (hk-try #'symbol-value 'hk-h) (hk-try #'default-value 'hk-h) (local-variable-p 'hk-h)))
      (terpri))))
)"#,
        ],
    );
}

#[test]
fn a_name_interned_again_after_obarray_clear_is_a_new_symbol() {
    // lread.c:intern_driver makes a new symbol on every obarray miss: after
    // `obarray-clear' (here through `clear-abbrev-table', on every table
    // the image holds) the name is interned as a new object, not the one
    // the obarray held before.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
;; A symbol interned after `clear-abbrev-table' (obarray-clear) is a new
;; object, in the tables the image holds as in new ones.
(let ((same nil))
  (dolist (name abbrev-table-name-list)
    (let* ((tab (symbol-value name)) (old (obarray-get tab "")))
      (clear-abbrev-table tab)
      (when (eq old (obarray-get tab "")) (push name same))))
  (prin1 (list 'same-after-clear same)) (terpri))
(let* ((tab (obarray-make)) (a (intern "x" tab)))
  (put a 'p 1)
  (obarray-clear tab)
  (let ((b (intern "x" tab)))
    (prin1 (list (eq a b) (get b 'p) (eq (intern-soft "x" tab) b) (intern-soft "x"))) (terpri))))"#,
        ],
    );
}

#[test]
fn uninterned_symbols_keep_their_roles_by_object() {
    // A symbol `make-symbol' made, as a function (its arity), a macro
    // (`macroexpand' with and without an environment entry for it), a
    // property owner, a hash-table test, a char-table purpose, a coding
    // system and a command with `interactive-args': each role is read
    // from the symbol itself, not from a name, which names none.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
(let ((s (make-symbol "zf")) (m (make-symbol "zm")) (p (make-symbol "zp")))
  (fset s 'car)
  (defalias m (cons 'macro (lambda (x) (list 'quote x))))
  (put p 'event-kind 'mouse-click)
  (prin1 (list (condition-case e (func-arity s) (error e))
               (condition-case e (macroexpand (list m 1)) (error e))
               (condition-case e (macroexpand-1 (list m 2) (list (cons m (lambda (x) (list 'quote (list x x)))))) (error e))
               (get p 'event-kind)
               (progn (define-hash-table-test (make-symbol "zt") 'eq 'sxhash-eq) 'ok)
               (let ((h (make-symbol "zh"))) (define-hash-table-test h 'equal 'sxhash-equal) (hash-table-count (make-hash-table :test h)))
               (let ((ct (make-symbol "zct"))) (put ct 'char-table-extra-slots 2) (char-table-extra-slot (make-char-table ct) 1))))
  (terpri))
(let ((cs (make-symbol "zcs")))
  (prin1 (list (coding-system-p cs) (condition-case e (check-coding-system cs) (error (car e))))) (terpri))
(let ((f (make-symbol "zcmd")))
  (fset f (lambda (x) (interactive "p") (* x 2)))
  (put f 'interactive-args '((x 21)))
  (prin1 (list (commandp f) (call-interactively f))) (terpri)))"#,
        ],
    );
}

#[test]
fn primitives_check_and_name_their_arguments_as_gnu_does() {
    // A feature, an algorithm and a backtrace base named by an uninterned
    // symbol, and the argument checks of primitives given a symbol where
    // GNU expects a number, an obarray, an array or a proper list: what
    // GNU reads (SYMBOL_NAME, EQ, CHECK_FIXNUM, check_obarray first) and
    // what it signals, in its order.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
(let ((f (make-symbol "cl-lib"))) (prin1 (condition-case e (require f) (error (list (car e) (and (string-match-p "failed to provide feature .cl-lib.\\'" (cadr e)) t)))))) (terpri)
(let ((f (make-symbol "zfeat"))) (provide f) (prin1 (list (featurep f) (featurep 'zfeat) (eq (car features) f)))) (terpri)
(prin1 (list (condition-case e (secure-hash (make-symbol "md5") "a") (error e)) (condition-case e (secure-hash 'sha3-256 "a") (error e)) (secure-hash 'sha224 "a"))) (terpri)
(prin1 (list (markerp (copy-marker)) (marker-position (copy-marker)) (marker-insertion-type (copy-marker nil t)))) (terpri)
(prin1 (list (condition-case e (bidi-resolved-levels 'x) (error e)) (condition-case e (bidi-resolved-levels 1.0) (error e)))) (terpri)
(prin1 (list (condition-case e (lookup-image-map '((x)) 'a 1) (error e)) (lookup-image-map nil 'a 'b))) (terpri)
(prin1 (list (condition-case e (intern-soft 5 'not-an-obarray) (error e)) (condition-case e (intern-soft 5) (error e)))) (terpri)
(prin1 (list (with-temp-buffer (insert "abc") (put-text-property 1 2 'display '((height 2) (raise 1))) (list (get-display-property 1 'raise) (get-display-property 1 'height) (get-display-property 1 'x)))
             (get-display-property 0 'raise (propertize "a" 'display [(raise 3)]))
             (get-display-property 0 'raise nil '(raise 4))
             (get-display-property 0 'raise nil '((raise 5) . 7))
             (condition-case e (get-display-property 'a 'raise) (error e))
             (condition-case e (get-display-property 1 'raise 'not-a-buffer) (error e))
             (condition-case e (get-display-property 1 'raise nil 5) (error e)))) (terpri)
(prin1 (condition-case e (make-terminal-frame '(a . b)) (error e))) (terpri)
(prin1 (list (mapbacktrace (make-symbol "zcb") (cons (make-symbol "x") "y")) (backtrace-frame--internal (make-symbol "zcb") 0 (make-symbol "none")))) (terpri)
(prin1 (list (condition-case e (pos-visible-in-window-p 'x) (error e)) (pos-visible-in-window-p) (pos-visible-in-window-p t))) (terpri)
(prin1 (with-temp-buffer (condition-case e (serial-process-configure) (error e)))) (terpri)
(prin1 (let ((current-load-list nil)) (provide 'zfeat2) (provide 'zfeat2) current-load-list)) (terpri)
(let ((last-kbd-macro '(1 2))) (prin1 (condition-case e (start-kbd-macro t) (error e)))) (terpri))"#,
        ],
    );
}

#[test]
fn coding_systems_are_found_by_their_symbols() {
    // Vcoding_system_hash_table is an `eq' table: an uninterned alias is
    // a coding system, the interned symbol of its name is none, and the
    // alias's subsidiaries are interned from its name.  Then the argument
    // checks of primitives a fuzz of uninterned arguments found apart.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
(let ((u (make-symbol "zutf")))
  (define-coding-system-alias u 'utf-8)
  (prin1 (list (coding-system-p u) (coding-system-p 'zutf) (encode-coding-string "é" u) (decode-coding-string "\303\251" u)
               (check-coding-system u) (coding-system-base u) (coding-system-eol-type u) (coding-system-type u)
               (coding-system-get u :mime-charset) (memq u (coding-system-aliases 'utf-8)) (car (coding-system-aliases u))
               (coding-system-p 'zutf-unix) (coding-system-base 'zutf-unix) (and (memq u coding-system-list) t) (assoc "zutf" coding-system-alist)
               (coding-system-change-eol-conversion u 'dos) (with-temp-buffer (set-buffer-file-coding-system u) buffer-file-coding-system)))
  (terpri))
(let ((u (make-symbol "zlat")))
  (define-coding-system-alias u 'latin-1-unix)
  (prin1 (list (encode-coding-string "é" u) (coding-system-eol-type u) (coding-system-get u :charset-list))) (terpri))
(let ((u (make-symbol "utf-8")))
  (prin1 (list (coding-system-p u) (condition-case e (check-coding-system u) (error e)) (condition-case e (encode-coding-string "a" u) (error e)))) (terpri))
(prin1 (list (condition-case e (describe-vector 'x) (error e))
             (condition-case e (local-variable-p [x] 'not-a-buffer) (error e))
             (condition-case e (menu-bar-menu-at-x-y 0 0 'not-a-frame) (error e))
             (condition-case e (set-input-meta-mode t 'not-a-terminal) (error e))
             (condition-case e (set-output-flow-control t 'not-a-terminal) (error e))
             (condition-case e (tab-bar-height 'x) (error e))
             (condition-case e (tool-bar-height 5) (error e))
             (condition-case e (lcms-cie-de2000 'a '(1 2 3)) (error e))
             (condition-case e (lcms-cie-de2000 '(1 2 3) 'b) (error e))
             (condition-case e (lcms-cam02-ucs '(1 2 3) 'b) (error e))
             (condition-case e (lcms-xyz->jch '(1 . 2)) (error e))
             (condition-case e (lcms-temp->white-point 'x) (error e))
             (condition-case e (lcms-temp->white-point 100) (error e))
             (let ((f (make-symbol "zauto")))
               (autoload f "zz-nonexistent-file")
               (list (autoloadp (symbol-function f)) (condition-case e (funcall f) (error (car e)))))))
(terpri))"#,
        ],
    );
}

#[test]
fn symbols_are_read_by_name_or_identity_as_gnu_reads_them() {
    // keyboard.c's parse_solitary_modifier and parse_modifiers read a
    // symbol's SYMBOL_NAME, so an uninterned `control' is a modifier;
    // Fget_buffer_create's CHECK_STRING names what make-process and
    // call-process were given; the variables read through cached
    // symbols are the ones GNU reads.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
(dolist (f (list
  (lambda () (event-convert-list (list (make-symbol "control") 'a)))
  (lambda () (event-convert-list (list (make-symbol "meta") (make-symbol "shift") ?x)))
  (lambda () (event-convert-list (list 'control (make-symbol "a"))))
  (lambda () (internal-event-symbol-parse-modifiers (make-symbol "s-H-x")))
  (lambda () (event-modifiers (make-symbol "C-M-down-mouse-1")))
  (lambda () (event-basic-type (make-symbol "C-f1")))
  (lambda () (let ((where-is-preferred-modifier (make-symbol "meta"))) (where-is-internal 'forward-char nil t)))
  (lambda () (make-process :name "x" :command '("true") :buffer 'foo))
  (lambda () (make-process :name "x" :command '("true") :stderr 'foo))
  (lambda () (make-process :name "x" :command '("true") :stderr 5))
  (lambda () (make-pipe-process :name "p" :buffer 5))
  (lambda () (call-process "true" nil 'foo))
  (lambda () (with-temp-buffer (setq-local display-line-numbers t) (list display-line-numbers (buffer-local-value 'display-line-numbers (current-buffer)))))
  (lambda () (let ((undo-limit 10) (undo-strong-limit 20)) (with-temp-buffer (buffer-enable-undo) (insert "abc") (undo-boundary) (length buffer-undo-list))))
  (lambda () (format-mode-line "%Z" nil nil (current-buffer)))
  (lambda () (let ((eol-mnemonic-unix "U")) (format-mode-line "%Z")))
  (lambda () (coding-system-eol-type 'utf-8-dos))
  (lambda () (coding-system-eol-type 'latin-1-mac))
  ))
  (prin1 (condition-case e (funcall f) (error (list 'ERR e)))) (terpri)))"#,
        ],
    );
}

#[test]
fn charsets_are_found_by_their_symbols() {
    // charset.c's Vcharset_hash_table is an `eq' table: an uninterned
    // alias or charset is one, `charset-list' gains the symbol itself, the
    // interned symbol of its name is none, and the conversions over the
    // ordered and ISO-2022 lists find each charset by its symbol.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
(dolist (f (list
  (lambda () (let ((u (make-symbol "zz"))) (define-charset-alias u 'ascii) (list (charsetp u) (charsetp 'zz) (eq (car (memq u charset-list)) u) (charset-plist u) (encode-char ?a u) (decode-char u 98) (charset-id-internal u))))
  (lambda () (let ((u (make-symbol "ascii"))) (list (charsetp u) (condition-case e (charset-plist u) (error e)) (condition-case e (encode-char ?a u) (error e)))))
  (lambda () (let ((u (make-symbol "zcs"))) (define-charset-internal u [1] [0 127 0 0 0 0 0 0] nil nil nil nil nil nil nil nil nil nil nil nil nil) (list (charsetp u) (charsetp 'zcs) (eq (car charset-list) u) (decode-char u 65) (encode-char ?A u) (and (memq u (charset-priority-list)) t))))
  (lambda () (let ((u (make-symbol "zcs2"))) (define-charset-internal u [1] [0 127 0 0 0 0 0 0] nil nil nil nil nil nil nil nil nil nil nil nil nil) (set-charset-priority u) (list (eq (car (charset-priority-list)) u) (eq (car (charset-priority-list t)) u) (eq (char-charset ?a) 'ascii) (sort-charsets (list 'ascii u)))))
  (lambda () (let ((u (make-symbol "zcs3"))) (define-charset-internal u [1] [32 127 0 0 0 0 0 0] nil nil nil nil nil nil nil nil nil nil nil nil nil) (list (condition-case e (unify-charset u) (error e)) (condition-case e (declare-equiv-charset 1 94 ?~ u) (error e)) (eq (iso-charset 1 94 ?~) u))))
  (lambda () (charset-priority-list t))
  (lambda () (let ((u (make-symbol "latin-iso8859-1"))) (condition-case e (encode-char ?é u) (error e))))
  (lambda () (sort-charsets (list 'ascii (make-symbol "x"))))
  (lambda () (find-charset-string "aé"))
  (lambda () (with-temp-buffer (insert "aé") (find-charset-region 1 3)))
  (lambda () (char-charset ?é))
  (lambda () (char-charset ?é (list 'latin-iso8859-1 'unicode)))
  (lambda () (condition-case e (char-charset ?é (list (make-symbol "latin-iso8859-1"))) (error e)))
  (lambda () (encode-coding-string "é" 'iso-latin-1))
  (lambda () (decode-coding-string "\351" 'iso-latin-1))
  (lambda () (encode-coding-string "日本" 'iso-2022-jp))
  (lambda () (decode-coding-string "\e$BF|K\\\e(B" 'iso-2022-jp))
  (lambda () (encode-coding-string "日本" 'euc-jp))
  (lambda () (encode-coding-string "日本" 'shift_jis))
  (lambda () (encode-coding-string "中文" 'big5))
  (lambda () (encode-coding-string "é" 'emacs-mule))
  (lambda () (get-text-property 0 'charset (decode-coding-string "\e$BF|\e(B" 'iso-2022-jp)))
  (lambda () (split-char ?é))
  (lambda () (make-char 'latin-iso8859-1 105))
  (lambda () (charset-plist 'latin-iso8859-1))
  ))
  (prin1 (condition-case e (funcall f) (error (list 'ERR e)))) (terpri)))"#,
        ],
    );
}

#[test]
fn string_arguments_reach_primitives_as_their_bytes() {
    // A sweep of every subr with multibyte, unibyte, eight-bit, NUL,
    // propertized and wide strings: an error formatted from SDATA (NAME)
    // takes the bytes up to the first NUL and make_string's choice of
    // multibyte (nsberror, the process and fontset lookups); a face name
    // is Fintern'd, raw bytes included; base64 decoding reads the string's
    // own bytes, a non-ASCII character's being no base64 data.  A buffer's
    // name is the Lisp string BVAR (b, name): the object `buffer-name'
    // returns and `rename-buffer' installs as given, found by
    // `string-equal' and printed and reported from its own bytes.  Color
    // names are C strings; a nil locale coding system encodes a time
    // format as `no-conversion'; columns and `format' step over and
    // register compositions as scan_for_column and lisp_string_width do.
    // A process's name is p->name, found by `equal' and made unique by
    // concat2; a minibuffer is one Vminibuffer_list holds.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r##"(progn
(dolist (f (list
  (lambda () (let ((eight (string ?a (unibyte-char-to-multibyte 200) ?b)) (nul (string ?a 0 ?b)) (uni (string-to-unibyte (copy-sequence "a\377b"))))
               (mapcar (lambda (s) (list (condition-case e (buffer-hash s) (error (list (cadr e) (multibyte-string-p (cadr e)) (string-bytes (cadr e)))))
                                         (condition-case e (process-type s) (error (list (cadr e) (multibyte-string-p (cadr e)))))
                                         (condition-case e (fontset-font s 65) (error (list (cadr e) (multibyte-string-p (cadr e)))))))
                       (list eight nul uni (string 233) (make-string 2 #x4e2d)))))
  (lambda () (let ((eight (string ?a (unibyte-char-to-multibyte 200) ?b)))
               (list (condition-case e (face-font eight) (error (list (car e) (cadr e) (append (symbol-name (nth 2 e)) nil) (eq (nth 2 e) (intern eight)))))
                     (condition-case e (internal-lisp-face-empty-p eight) (error (append (symbol-name (nth 2 e)) nil))))))
  (lambda () (list (condition-case e (base64-decode-string (make-string 3 #x4e2d)) (error e)) (condition-case e (base64-decode-string (string #x1f600 ?x)) (error e))
                   (condition-case e (base64-decode-string (make-string 3 #x4e2d) nil t) (error e)) (condition-case e (base64-decode-string (concat "YQ" (string #x4e2d) "==") nil t) (error e))
                   (condition-case e (base64-decode-string (string ?Y (unibyte-char-to-multibyte 200) ?Q ?= ?=) nil t) (error e))
                   (with-temp-buffer (insert "YQ" (string #x4e2d) "==") (list (condition-case e (base64-decode-region (point-min) (point-max)) (error e)) (condition-case e (base64-decode-region (point-min) (point-max) nil t) (error e)) (buffer-string)))
                   (with-temp-buffer (insert "/w==") (base64-decode-region (point-min) (point-max)) (list (buffer-string) (multibyte-string-p (buffer-string)) (append (buffer-string) nil)))))
  (lambda () (let* ((u (string-to-unibyte (copy-sequence "x\377"))) (b (get-buffer-create u)) (s (copy-sequence "probe")))
               (list (eq (buffer-name) (buffer-name)) (multibyte-string-p (buffer-name b)) (eq (buffer-name b) u)
                     (get-buffer (string ?x (unibyte-char-to-multibyte 255))) (eq (get-buffer u) b)
                     (progn (with-current-buffer b (rename-buffer s)) (eq (buffer-name b) s))
                     (progn (aset s 0 ?P) (list (buffer-name b) (buffer-last-name b)))
                     (progn (with-current-buffer b (rename-buffer (propertize "propped" 'face 'bold))) (buffer-name b))
                     (buffer-name (get-buffer-create (propertize "newp" 'face 'italic)))
                     (with-current-buffer b (rename-buffer "*scratch*" t))
                     (generate-new-buffer-name (propertize "*scratch*" 'face 'bold))
                     (generate-new-buffer-name "*scratch*" "*scratch*<2>") (generate-new-buffer-name "*scratch*" 'foo)
                     (condition-case e (generate-new-buffer-name "*scratch*" 5) (error e))
                     (multibyte-string-p (generate-new-buffer-name (string-to-unibyte "*scratch*")))
                     (with-current-buffer b (eq (rename-buffer (buffer-name)) (buffer-name)))
                     (let ((k (get-buffer-create "killme"))) (kill-buffer k) (list (buffer-name k) (buffer-last-name k)))
                     (prin1-to-string (get-buffer-create (string ?y (unibyte-char-to-multibyte 200))))
                     (sort (list (get-buffer-create "zz") (get-buffer-create "aa")) #'value<))))
  (lambda () (let ((nul (string ?a 0 ?b)) (eight (string ?a (unibyte-char-to-multibyte 200) ?b)))
               (get-buffer-create nul) (get-buffer-create eight)
               (mapcar (lambda (name)
                         (mapcar (lambda (f) (condition-case e (funcall f) (error (list (cadr e) (multibyte-string-p (cadr e))))))
                                 (list (lambda () (make-indirect-buffer "*scratch*" name)) (lambda () (make-indirect-buffer name "fresh-ind"))
                                       (lambda () (process-type (get-buffer name))) (lambda () (with-temp-buffer (rename-buffer name)))
                                       (lambda () (buffer-enable-undo (concat name "-gone"))))))
                       (list nul eight))))
  (lambda () (list (color-gray-p (string 0 ?x)) (color-supported-p (string ?r ?e ?d 0 ?z)) (color-distance (string ?r ?e ?d 0 ?z) "black")
                   (color-gray-p "unspecified-fg") (condition-case e (color-gray-p "red" 'foo) (error e)) (condition-case e (color-supported-p 'red) (error e))
                   (let ((locale-coding-system nil)) (format-time-string (string ?a (unibyte-char-to-multibyte 200) ?b ?% ?Y ?\N{U+e9}) 0 t))))
  (lambda () (list (with-temp-buffer (insert "ab") (insert "xyz") (compose-region 3 6 ?λ) (end-of-line) (current-column))
                   (with-temp-buffer (insert "abcdef") (compose-region 2 5 "Q") (list (progn (end-of-line) (current-column)) (progn (move-to-column 2) (point)) (progn (move-to-column 3) (point)) (progn (move-to-column 1) (point))))
                   (with-temp-buffer (insert "a\tb") (compose-region 1 3 "Z") (end-of-line) (current-column))
                   (with-temp-buffer (insert "abc") (put-text-property 1 3 'invisible t) (compose-region 2 4 "R") (end-of-line) (current-column))
                   (let ((s (copy-sequence "abc"))) (compose-string-internal s 0 3 "X") (format "%s" s) (prog1 (object-intervals s) (format "x%s" s)))
                   (let ((s (copy-sequence "abc"))) (compose-string-internal s 0 3 "X") (format "x%s" s) (object-intervals s))
                   (let ((s (copy-sequence "abc"))) (compose-string-internal s 0 3 "X") (face-attributes-as-vector s) (object-intervals s))))
  (lambda () (let* ((n (copy-sequence "pp")) (pr (make-pipe-process :name n :noquery t)) (u (make-pipe-process :name (string-to-unibyte "u\377") :noquery t))
                    (q (make-pipe-process :name (propertize "propn" 'face 'bold) :noquery t)) (z (make-pipe-process :name (string ?z 0 ?y) :noquery t)))
               (list (eq (process-name pr) n) (process-name (make-pipe-process :name n :noquery t)) (process-name (make-pipe-process :name n :noquery t))
                     (eq (get-process n) pr) (eq (get-process (string-to-multibyte "pp")) pr)
                     (multibyte-string-p (process-name u)) (get-process (string ?u (unibyte-char-to-multibyte 255))) (eq (get-process (string-to-unibyte "u\377")) u)
                     (process-name q) (process-name (make-pipe-process :name (propertize "propn" 'face 'bold) :noquery t))
                     (process-name z) (prin1-to-string z)
                     (value< (make-pipe-process :name "bbb" :noquery t) (make-pipe-process :name "aaa" :noquery t))
                     (condition-case e (get-process 5) (error e)) (process-status "nonexistent") (process-status (copy-sequence "pp"))
                     (mapcar #'process-name (seq-filter (lambda (x) (string-prefix-p "pp" (process-name x))) (process-list))))))
  (lambda () (list (minibufferp (get-buffer " *Minibuf-0*")) (minibufferp " *Minibuf-0*") (minibufferp (get-buffer " *Minibuf-0*") t)
                   (minibufferp (get-buffer-create " *Minibuf-9*")) (minibufferp (get-buffer-create " *Minibuf-1*")) (minibufferp "nonexistent")
                   (condition-case e (minibufferp 5) (error e)) (innermost-minibuffer-p (get-buffer " *Minibuf-0*")) (innermost-minibuffer-p)
                   (minibuffer-innermost-command-loop-p (get-buffer " *Minibuf-0*"))
                   (let ((b (get-buffer " *Minibuf-0*"))) (with-current-buffer b (rename-buffer "renamed-mini")) (list (minibufferp b) (minibufferp "renamed-mini") (buffer-name b)))
                   (window-buffer (minibuffer-window))))
  ))
  (prin1 (condition-case e (funcall f) (error (list 'ERR e)))) (terpri)))"##,
        ],
    );
}

#[test]
fn numeric_edge_arguments_answer_as_gnu_does() {
    // A sweep of every subr with -0.0, NaNs, infinities, the fixnum
    // bounds, bignums and the character bound: a position is fix_position's
    // (CHECK_FIXNUM_COERCE_MARKER), so any bignum is the fixnum bound in its
    // direction; a line count is bol's and eol's, a bignum as many lines as
    // a buffer can hold; `obarray-make''s range error carries SIZE itself;
    // an hscroll is clipped to the fixnum range; `move-point-visually'
    // reads its direction as a C int.  A sweep with lists, vectors,
    // records, closures and symbols: the alternative font alists are
    // CHECK_LIST'd copies whose elements Fintern or Fdowncase takes, and a
    // face attribute is the interned keyword itself.  A sweep with live
    // objects: Fapply stores a one-element spread list's element into the
    // vector its frame records and pads a subr's call with nils to its
    // MAXARGS, and `line-number-at-pos' takes another buffer's marker
    // without the fixnum check.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r##"(progn
(dolist (f (list
  (lambda () (with-temp-buffer (insert "abcdef\nghi")
               (let ((big (expt 2 70)) (nbig (- (expt 2 70))))
                 (mapcar (lambda (g) (condition-case e (funcall g) (error e)))
                         (list (lambda () (make-overlay big big)) (lambda () (overlays-at big)) (lambda () (overlays-in big big)) (lambda () (next-overlay-change big)) (lambda () (previous-overlay-change nbig))
                               (lambda () (overlay-recenter big)) (lambda () (narrow-to-region big nbig)) (lambda () (prog1 (list (point-min) (point-max)) (widen)))
                               (lambda () (get-byte big)) (lambda () (position-bytes big)) (lambda () (compare-buffer-substrings nil big nil nil 1 2))
                               (lambda () (pos-bol big)) (lambda () (pos-eol nbig)) (lambda () (line-beginning-position big)) (lambda () (line-end-position nbig))
                               (lambda () (pos-visible-in-window-p big)) (lambda () (posn-at-point big)) (lambda () (check-coding-systems-region big nil nil))
                               (lambda () (goto-char big)) (lambda () (char-after big)) (lambda () (put-text-property 1 big 'face 'bold)))))))
  (lambda () (list (condition-case e (obarray-make most-positive-fixnum) (error e))
                   (let ((w (selected-window))) (set-window-hscroll w 0)
                     (list (scroll-left most-positive-fixnum) (scroll-left 5) (window-hscroll w) (scroll-right most-positive-fixnum) (scroll-right 3) (scroll-left most-negative-fixnum)))
                   (mapcar (lambda (d) (with-temp-buffer (condition-case e (move-point-visually d) (error e))))
                           (list 1 -1 0 most-positive-fixnum (ash most-positive-fixnum -1) (ash 1 32) (1+ (ash 1 31)) most-negative-fixnum))))
  (lambda () (list (condition-case e (internal-set-alternative-font-registry-alist '((((1))))) (error e))
                   (internal-set-alternative-font-registry-alist (list (list "ABC" ?Q (string #x130 ?X))))
                   (let ((r (internal-set-alternative-font-family-alist (list (list (string-to-unibyte "a\377") "Mono"))))) (list r (eq (car (car r)) (intern (string-to-unibyte "a\377")))))
                   (condition-case e (internal-set-alternative-font-family-alist '((1))) (error e))
                   (condition-case e (internal-set-alternative-font-family-alist '(1)) (error e))
                   (condition-case e (internal-set-alternative-font-registry-alist '(("a" . "b"))) (error e))
                   (let ((s (make-symbol ":foreground"))) (condition-case e (internal-set-lisp-face-attribute 'default s "red" t) (error (list (cadr e) (eq (nth 2 e) s)))))
                   (condition-case e (internal-set-lisp-face-attribute 'default :nonesuch 1 t) (error e))))
  (lambda () (let ((frames (lambda (name) (seq-take (seq-drop (seq-drop-while (lambda (f) (not (eq (cadr f) name))) (backtrace-frames)) 1) 1)))
                   (seen nil))
               (fset 'ap-frames (lambda (&rest _) (funcall frames 'ap-frames)))
               (fset 'ap-grab (lambda (x) (setq seen (seq-take (seq-drop-while (lambda (f) (not (eq (cadr f) 'mapconcat))) (backtrace-frames)) 1)) x))
               (list (apply #'ap-frames '(x)) (apply #'ap-frames 1 '(x)) (apply #'ap-frames '(x y)) (apply #'ap-frames '()) (apply 'ap-frames 1 2 '(3))
                     (progn (apply #'mapconcat (list #'ap-grab '("a"))) seen) (progn (apply #'mapconcat #'ap-grab '(("a") "-")) seen)
                     (progn (apply 'mapconcat 'ap-grab (list '("a" "b"))) seen))))
  (lambda () (with-temp-buffer (insert "a\nb\nc")
               (let ((m (copy-marker 5)))
                 (with-temp-buffer (list (line-number-at-pos m) (condition-case e (line-number-at-pos m t) (error e)) (condition-case e (line-number-at-pos 5) (error e)))))))
  ))
  (prin1 (condition-case e (funcall f) (error (list 'ERR e)))) (terpri)))"##,
        ],
    );
}

#[test]
fn identity_and_symbol_value_answer_as_gnu_does() {
    // lisp.h:EQ is BASE_EQ of the words, a symbol with position reckoned
    // its bare symbol only while `symbols-with-pos-enabled' is set;
    // fns.c:Feql adds same_float for floats and the value for bignums.
    // Bsymbol_value is Fsymbol_value of TOP whatever its variable's kind;
    // `type-of' and `cl-type-of' answer the interned symbols; Fget_buffer
    // compares a name's characters and bytes.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r##"(progn
(defvar idt-plain 1) (defvar-local idt-local 2)
(dolist (f (list
  (lambda () (let ((sv (byte-compile (lambda (s) (symbol-value s)))))
     (with-temp-buffer (setq idt-local 5)
       (mapcar (lambda (s) (condition-case e (funcall sv s) (error e)))
               (list 'idt-plain 'idt-local 'fill-column 'case-fold-search nil t (make-symbol "void") 'idt-unbound 3 "x")))))
  (lambda () (let ((sv (byte-compile (lambda (s) (symbol-value s)))) (p (position-symbol 'idt-plain 5)))
     (list (condition-case e (funcall sv p) (error (car e)))
           (let ((symbols-with-pos-enabled t)) (condition-case e (funcall sv p) (error (car e)))))))
  (lambda () (let ((p (position-symbol 'foo 3)) (q (position-symbol 'foo 9)) (b1 (expt 2 70)) (b2 (expt 2 70))
                   (n1 (/ 0.0 0.0)) (w (selected-window)) (eqf (byte-compile (lambda (a b) (eq a b)))))
     (list (eq p 'foo) (eql p 'foo) (eq p q) (funcall eqf p 'foo)
           (let ((symbols-with-pos-enabled t))
             (list (eq p 'foo) (eq 'foo q) (eq p q) (eql p q) (funcall eqf p 'foo) (funcall eqf q p)
                   (consp (memq p '(foo))) (consp (assq q '((foo . 1)))) (consp (rassq p '((1 . foo))))))
           (eq b1 b2) (eql b1 b2) (eql 1.0 1.0) (eql 0.0 -0.0) (eql n1 n1) (eql n1 (/ 0.0 0.0)) (eql n1 (- n1)) (eql 1 1.0)
           (eq (current-buffer) (current-buffer)) (memql 2.5 (list 1 2.5)) (consp (assq w (list (cons w 1))))
           (funcall eqf w (selected-window)) (funcall eqf b1 b2) (consp (memq w (window-list))))))
  (lambda () (mapcar (lambda (o) (list (eq (type-of o) (intern (symbol-name (type-of o)))) (type-of o) (cl-type-of o)))
     (list nil t 1 (expt 2 70) 1.5 "s" 'a (position-symbol 'a 1) [1] '(1) (symbol-function 'car) (lambda ()) (byte-compile (lambda ()))
           (current-buffer) (point-marker) (make-overlay 1 1) (make-char-table 'x) (make-hash-table) (selected-frame) (selected-window)
           (current-window-configuration) (make-bool-vector 2 t) (obarray-make) (record 'foo 1))))
  (lambda () (let ((b (get-buffer-create "idt-é")))
     (prog1 (list (eq (get-buffer (copy-sequence "idt-é")) b) (get-buffer (string-to-unibyte "idt-\303\251")) (get-buffer "idt-e")
                  (eq (get-buffer b) b) (string-equal "idt-é" (buffer-name b)) (string-equal (string-to-unibyte "idt-\303\251") (buffer-name b)))
       (kill-buffer b))))
  ))
  (prin1 (condition-case e (funcall f) (error (list 'ERR e)))) (terpri)))"##,
        ],
    );
}

#[test]
fn window_configurations_save_and_restore_gnu_s_fields() {
    // window.c:save_window_save saves the frame's window tree (the
    // minibuffer window the root's next) with the fields of struct
    // saved_window alone, the persistent parameters among them; and
    // Fset_window_configuration revives the saved windows from
    // delete_all_child_windows and writes those fields back, leaving a
    // window's previous and next buffers, cursor type, other parameters
    // and a window split after the save (deleted, its parameters kept)
    // as they are.
    let corpus = Corpus::new();
    for script in [
        r##"(defvar wc-r nil)
(defun wc-push (&rest xs) (setq wc-r (append wc-r xs)))
(let ((w (selected-window)) (m (get-buffer-create "*wc-other*")) (c nil))
  (set-window-prev-buffers w nil)
  (set-window-next-buffers w nil)
  (set-window-parameter w 'wc-p 1)
  (set-window-parameter w 'wc-q 1)
  (set-window-dedicated-p w nil)
  (setq c (current-window-configuration))
  (set-window-prev-buffers w (list (list m 1 1)))
  (set-window-next-buffers w (list m))
  (set-window-parameter w 'wc-p 2)
  (set-window-parameter w 'wc-new 3)
  (set-window-display-table w (make-display-table))
  (set-window-dedicated-p w t)
  (set-window-hscroll w 4)
  (set-window-cursor-type w 'hbar)
  (set-window-configuration c)
  (wc-push (length (window-prev-buffers w)) (length (window-next-buffers w))
           (window-parameter w 'wc-p) (window-parameter w 'wc-q) (window-parameter w 'wc-new)
           (char-table-p (window-display-table w)) (window-dedicated-p w)
           (window-hscroll w) (window-cursor-type w))
  (let ((w2 (split-window)))
    (set-window-parameter w2 'kept 7)
    (set-window-configuration c)
    (wc-push (window-live-p w2) (window-valid-p w2) (window-parameter w2 'kept)
             (window-buffer w2) (length (window-list))))
  (let ((w3 (split-window)))
    (set-window-parameter w3 'kept 8)
    (delete-window w3)
    (setq c (current-window-configuration))
    (set-window-configuration c)
    (wc-push (window-live-p w3) (window-valid-p w3) (window-parameter w3 'kept) (length (window-list)))))
(prin1 wc-r) (terpri)
"##,
        r##"(defvar wc-r nil)
(defun wc-push (&rest xs) (setq wc-r (append wc-r (list xs))))
(defun wc-tree () (let ((tr (window-tree))) (car tr)))
(defun wc-shape (w)
  (if (windowp w) (list (buffer-name (window-buffer w)) (window-total-height w) (window-total-width w))
    (cons (if (car w) 'h 'v) (mapcar #'wc-shape (cddr w)))))
(let* ((a (get-buffer-create "wa")) (b (get-buffer-create "wb")) (c (get-buffer-create "wc"))
       (w0 (selected-window)) conf conf2)
  (switch-to-buffer a) (insert "aaaa\nbbbb\ncccc\n") (goto-char 3)
  (let ((w1 (split-window w0 nil 'right)))
    (set-window-buffer w1 b)
    (let ((w2 (split-window w1 nil 'below)))
      (set-window-buffer w2 c)
      (set-window-parameter w2 'clone-of 'x)
      (set-window-parameter w2 'other 'y)
      (set-window-dedicated-p w2 t)
      (setq conf (current-window-configuration))
      (wc-push 'saved (wc-shape (wc-tree)) (window-configuration-equal-p conf (current-window-configuration)))
      (delete-other-windows w0)
      (wc-push 'after-delete (wc-shape (wc-tree)) (window-live-p w1) (window-live-p w2) (window-configuration-equal-p conf (current-window-configuration)))
      (set-window-configuration conf)
      (wc-push 'restored (wc-shape (wc-tree)) (window-live-p w1) (window-live-p w2)
               (window-parameter w2 'clone-of) (window-parameter w2 'other) (window-dedicated-p w2)
               (eq (selected-window) w0) (point) (window-point w0)
               (window-configuration-equal-p conf (current-window-configuration))
               (eq (window-configuration-frame conf) (selected-frame)))
      ;; kill a shown buffer, then restore
      (setq conf2 (current-window-configuration))
      (kill-buffer b)
      (wc-push 'killed (wc-shape (wc-tree)))
      (set-window-configuration conf2)
      (wc-push 'restored-killed (wc-shape (wc-tree)) (window-live-p w1) (window-live-p w2))
      ;; save-window-excursion keeps point of the current buffer
      (goto-char 5)
      (save-window-excursion (delete-other-windows) (goto-char 9) (switch-to-buffer c))
      (wc-push 'swe (wc-shape (wc-tree)) (buffer-name) (point))
      ;; restoring twice, and a configuration across a resplit
      (let ((conf3 (current-window-configuration)))
        (split-window (selected-window) nil 'below)
        (set-window-configuration conf3)
        (set-window-configuration conf3)
        (wc-push 'twice (wc-shape (wc-tree)) (length (window-list)))))))
(dolist (x wc-r) (prin1 x) (terpri))
"##,
    ] {
        assert_same(
            &corpus,
            &["--batch", "-Q", "--eval", &format!("(progn {script})")],
        );
    }
}

#[test]
fn window_points_starts_and_selections_answer_as_window_c_does() {
    // window.c: set-window-start and set-window-point of nil (or of a
    // marker pointing nowhere) leave a non-selected window's marker
    // pointing nowhere, which window-start answers as nil and
    // window_point as the charpos it kept, and which unshow_buffer
    // refuses ("Marker does not point anywhere") when the window is
    // deleted or shows another buffer, as set_point_from_marker does
    // when it is selected.  The selected window's point is its buffer's,
    // moved in that buffer when another is current (POS coerced by
    // fix_position).  unshow_buffer leaves the buffer's point alone while
    // the window the buffer was last selected in still shows it, and the
    // buffer-local window-configuration-change-hook runs with its window
    // selected as Fselect_window does.  select-window and
    // resize-mini-window-internal take no nil, an inactive minibuffer
    // window is followed by the first window and preceded by the last,
    // and it has no mode line.
    let corpus = Corpus::new();
    for script in [
        r##"(defvar ws-r nil)
(defun ws-push (&rest xs) (setq ws-r (append ws-r (list xs))))
(defmacro ws-try (form) `(condition-case e ,form (error (list 'ERR (car e) (if (windowp (cadr e)) 'W (cadr e))))))
(let* ((sel (selected-window)) (mini (minibuffer-window)) (b (get-buffer-create "ws-b")))
  (switch-to-buffer (get-buffer-create "ws-a"))
  (insert "line one\nline two\nline three\n") (goto-char 5)
  (with-current-buffer b (insert "other\ntext\n"))
  (let* ((right (split-window sel nil 'right)) (below (split-window right nil 'below)))
    (set-window-buffer below b)
    (ws-push 'start (ws-try (set-window-start right nil)) (window-start right)
             (ws-try (set-window-start right 4)) (window-start right))
    (ws-push 'point (ws-try (set-window-point below nil)) (window-point below)
             (ws-try (set-window-point below 3)) (window-point below)
             (ws-try (set-window-point right 7)) (window-point right) (point)
             (ws-try (set-window-point sel 2)) (point) (window-point sel))
    (ws-push 'select (ws-try (select-window nil)) (ws-try (resize-mini-window-internal nil))
             (eq (selected-window) sel))
    (ws-push 'cycle (eq (next-window mini) (car (window-list nil 'nomini)))
             (eq (previous-window mini) (car (last (window-list nil 'nomini))))
             (eq (next-window mini nil) (next-window mini 'nomini)))
    (ws-push 'sizes (window-body-height mini) (window-text-height mini) (window-total-height mini)
             (window-margins sel)
             (window-text-width right) (window-total-width right))
    (set-window-margins right 2 1)
    (ws-push 'margins (- (window-total-width right) (window-text-width right)) (window-body-width right))
    (ws-push 'detached (ws-try (set-window-point below nil)) (ws-try (delete-other-windows sel))
             (length (window-list)))))
(dolist (x ws-r) (prin1 x) (terpri))
"##,
        r##"(defvar ws-r nil)
(defun ws-push (&rest xs) (setq ws-r (append ws-r (list xs))))
(defmacro ws-try (form) `(condition-case e ,form (error (list 'ERR (car e) (if (windowp (cadr e)) 'W (cadr e))))))
(let* ((x (get-buffer-create "ws-x")) (y (get-buffer-create "ws-y")) (a (selected-window)) b c)
  (with-current-buffer x (insert "0123456789abcdefghij\n"))
  (with-current-buffer y (insert "yyyyyyyyyy\n"))
  (switch-to-buffer x) (goto-char 5)
  (setq b (split-window a nil 'below))
  (setq c (split-window b nil 'below))
  (set-window-buffer c y)
  (select-window b) (goto-char 9)
  (select-window a) (goto-char 3)
  (select-window c)
  (ws-push 'points (window-point a) (window-point b) (with-current-buffer x (point)))
  (set-window-point b 15)
  (delete-window b)
  (ws-push 'after-delete-b (with-current-buffer x (point)) (length (window-list)))
  (setq b (split-window a nil 'below))
  (set-window-buffer b x)
  (set-window-point b 12)
  (select-window b) (select-window c)
  (set-window-point b 17)
  (delete-window b)
  (ws-push 'after-delete-b2 (with-current-buffer x (point)))
  ;; hooks see the window's point
  (with-current-buffer x
    (setq-local window-configuration-change-hook
                (list (lambda () (ws-push 'hook (eq (selected-window) a) (point) (window-point a) (buffer-name))) t)))
  (set-window-point a 7)
  (run-window-configuration-change-hook)
  (ws-push 'after-hook (eq (selected-window) c) (buffer-name) (with-current-buffer x (point)) (window-point a))
  (with-current-buffer x (kill-local-variable 'window-configuration-change-hook))
  ;; set-window-point of the selected window from another buffer
  (select-window a)
  (with-current-buffer y (ws-push 'swp (set-window-point nil 4) (buffer-name) (point)))
  (ws-push 'swp-after (point) (window-point a))
  (with-current-buffer y (ws-push 'swp-err (ws-try (set-window-point nil (make-marker))) (ws-try (set-window-point nil 1.5)) (ws-try (set-window-point nil (1+ most-positive-fixnum)))))
  ;; a start pointing nowhere
  (setq b (split-window a nil 'below))
  (set-window-start b nil)
  (ws-push 'start-nowhere (window-start b) (ws-try (delete-window b)) (window-live-p b) (length (window-list)))
  (ws-push 'start-nowhere-2 (ws-try (set-window-buffer b y)) (window-buffer b)))
(dolist (x ws-r) (prin1 x) (terpri))
"##,
    ] {
        assert_same(
            &corpus,
            &["--batch", "-Q", "--eval", &format!("(progn {script})")],
        );
    }
}

#[test]
fn a_deleted_selected_window_decodes_as_window_c_does() {
    // A point marker set to point nowhere makes delete-other-windows fail
    // in unshow_buffer, which can leave the selected window deleted.
    // decode_live_window, decode_valid_window and decode_any_window then
    // answer it for nil unchecked: its fields read as they are, and
    // Fset_window_buffer signals "Window is deleted"; window_loop's walk
    // from it (get-buffer-window, other-buffer) is window_list_1's
    // decode_live_window, which signals window-live-p.  The second
    // script detaches the kept window's start, which
    // Fdelete_other_windows_internal's marker_position refuses before it
    // changes anything.
    let corpus = Corpus::new();
    for script in [
        r##"(defvar ws-r nil)
(defun ws-push (&rest xs) (prin1 xs) (terpri))
(defun ws-st (tag)
  (ws-push tag (window-live-p (selected-window)) (window-valid-p (selected-window))
           (and (window-buffer (selected-window)) (buffer-name (window-buffer (selected-window))))
           (condition-case e (length (window-list)) (error (list (quote ERR) (car e)))) (condition-case e (mapcar (lambda (w) (buffer-name (window-buffer w))) (window-list)) (error (list (quote ERR) (car e))))
           (condition-case e (eq (frame-first-window) (car (window-list))) (error (list (quote ERR) (car e)))) (buffer-name)))
(defmacro ws-try (tag form) `(ws-push ,tag (condition-case e (progn ,form 'ok) (error (list 'ERR (car e) (if (windowp (cadr e)) 'W (cadr e)))))))
(defvar ws-objs nil)
(defun ws-setup (n)
  (ws-try (list n 'sel) (select-window (frame-first-window)))
  (ws-try (list n 'dow) (delete-other-windows))
  (ws-st (list n 'after-dow))
  (ws-try (list n 'stb) (switch-to-buffer (get-buffer-create "wsA")))
  (erase-buffer) (insert "line one\nline two\nline three\n") (goto-char 5)
  (ws-try (list n 'split)
          (let* ((sel (selected-window))
                 (right (split-window sel nil 'right))
                 (below (split-window right nil 'below))
                 (dead (split-window below nil 'below)))
            (set-window-buffer below (get-buffer-create "wsB"))
            (delete-window dead)
            (setq ws-objs (list (cons 'selected sel) (cons 'right right) (cons 'below below) (cons 'dead dead)))))
  (ws-st (list n 'after-setup)))
(defun ws-w (w) (list (window-live-p w) (window-valid-p w) (and (window-buffer w) (buffer-name (window-buffer w))) (and (window-parent w) t)))
(ws-setup 0)
(ws-push 'swp (condition-case e (set-window-point (cdr (assq 'below ws-objs)) nil) (error e)))
(ws-setup 1)
(ws-setup 2)
(ignore-errors (select-window (frame-first-window)))
(ignore-errors (delete-other-windows))
(defmacro ws-t (form) `(condition-case e ,form (error (list 'ERR (car e) (if (windowp (cadr e)) 'W (cadr e))))))
(ws-push 'dead-selected (window-live-p (selected-window)) (window-valid-p (selected-window))
         (ws-t (window-minibuffer-p)) (ws-t (window-dedicated-p)) (ws-t (window-buffer))
         (ws-t (window-prev-buffers)) (ws-t (window-next-buffers)) (ws-t (window-parameters))
         (ws-t (set-window-buffer nil (get-buffer-create "wsA"))) (ws-t (window-parameter nil 'x))
         (ws-t (window-hscroll)) (ws-t (window-margins)) (ws-t (window-fringes))
         (ws-t (window-parent)) (ws-t (window-normal-size)) (ws-t (window-combination-limit nil))
         (ws-t (window-new-total)) (ws-t (window-old-point)) (framep (window-frame)))
(let ((w (frame-root-window)))
  (ws-push 'root (window-live-p w) (window-valid-p w) (ws-t (frame-first-window))))
(ws-push 'gbw (ws-t (get-buffer-window "wsA")) (ws-t (other-buffer)) (ws-t (get-buffer-window-list "wsA")))
(kill-emacs 0)
"##,
        r##"(defvar ws-objs nil)
(defun ws-push (&rest xs) (prin1 xs) (terpri))
(defmacro ws-try (tag form) `(ws-push ,tag (condition-case e (progn ,form 'ok) (error (list 'ERR (car e) (if (windowp (cadr e)) (format "%s" (cadr e)) (cadr e)))))))
(defun ws-id (w) (and w (string-to-number (replace-regexp-in-string "#<window \\([0-9]+\\).*" "\\1" (format "%s" w)))))
(defun ws-tree (w) (when w (list (ws-id w) (window-valid-p w) (window-live-p w)
   'parent (condition-case nil (ws-id (window-parent w)) (error 'E)) 'prev (condition-case nil (ws-id (window-prev-sibling w)) (error 'E)) 'next (condition-case nil (ws-id (window-next-sibling w)) (error 'E))
   'kids (condition-case nil (let ((c (window-child w)) r) (while c (push (ws-id c) r) (setq c (window-next-sibling c))) (nreverse r)) (error 'E)))))
(defun ws-setup (n)
  (ws-try (list n 'sel) (select-window (frame-first-window)))
  (ws-try (list n 'dow) (delete-other-windows))
  (ws-push (list n 'state) (ws-id (selected-window)) (ws-id (frame-root-window)) (ws-tree (frame-root-window)) (ws-tree (selected-window)))
  (switch-to-buffer (get-buffer-create "wsA"))
  (erase-buffer) (insert "line one\nline two\nline three\n") (goto-char 5)
  (ws-try (list n 'split)
   (let* ((sel (selected-window))
         (right (split-window sel nil 'right))
         (below (split-window right nil 'below))
         (dead (split-window below nil 'below)))
    (set-window-buffer below (get-buffer-create "wsB"))
    (delete-window dead)
    (setq ws-objs (list (cons 'selected sel) (cons 'right right) (cons 'below below) (cons 'dead dead))))))
(dolist (k '(selected right below))
  (ws-setup k)
  (ws-push 'sws k (condition-case e (set-window-start (cdr (assq k ws-objs)) nil) (error e))))
(ws-setup 'last)
(kill-emacs 0)
"##,
    ] {
        assert_same(
            &corpus,
            &["--batch", "-Q", "--eval", &format!("(progn {script})")],
        );
    }
}

#[test]
fn labeled_restrictions_and_stream_modes_answer_as_editfns_c_does() {
    // editfns.c keeps a buffer's labeled restrictions innermost first,
    // the outermost recording the bounds current when the first was
    // entered: narrow-to-region checks its bounds against the whole
    // buffer and keeps them within the innermost labeled restriction's,
    // widen goes back to that restriction's bounds (dropping the
    // outermost one when it is reached), a nil label counts as none, and
    // internal--labeled-narrow-to-region narrows as narrow-to-region does
    // before recording its bounds.  fileio.c's set-binary-mode checks a
    // symbol and the three streams by EQ and answers t on a Posix
    // system; set-default-file-modes takes a fixnum.
    let corpus = Corpus::new();
    for script in [
        r##"(defun lr-b () (list (point-min) (point-max) (point)))
(defmacro lr-try (form) `(condition-case e ,form (error (list 'ERR e))))
(with-temp-buffer
  (insert "0123456789abcdefghijklmnopqrstuvwxyz\n")
  (let (r)
    (narrow-to-region 6 30)
    (push (list 'a (lr-b)) r)
    (with-restriction 10 20 :label 'foo
      (push (list 'b (lr-b)) r)
      (widen) (push (list 'c (lr-b)) r)
      (narrow-to-region 1 38) (push (list 'd (lr-b)) r)
      (narrow-to-region 12 15) (push (list 'e (lr-b)) r)
      (save-restriction (widen) (push (list 'f (lr-b)) r))
      (push (list 'g (lr-b)) r)
      (without-restriction :label 'foo (push (list 'h (lr-b)) r))
      (push (list 'i (lr-b)) r)
      (without-restriction :label 'bar (push (list 'j (lr-b)) r))
      (with-restriction 14 18 :label 'baz
        (push (list 'k (lr-b)) r)
        (widen) (push (list 'l (lr-b)) r)
        (without-restriction :label 'baz (push (list 'm (lr-b)) r))
        (push (list 'n (lr-b)) r))
      (push (list 'o (lr-b)) r)
      (goto-char 11) (insert "XX") (push (list 'p (lr-b)) r)
      (widen) (push (list 'q (lr-b)) r))
    (push (list 'r (lr-b)) r)
    (widen) (push (list 's (lr-b)) r)
    (internal--labeled-narrow-to-region 3 9 nil)
    (push (list 't (lr-b)) r)
    (widen) (push (list 'u (lr-b)) r)
    (internal--labeled-widen nil) (push (list 'v (lr-b)) r)
    (widen) (push (list 'w (lr-b)) r)
    (push (list 'x (lr-try (narrow-to-region 0 5)) (lr-try (narrow-to-region 5 (1+ most-positive-fixnum))) (lr-try (narrow-to-region (make-marker) 3)) (lr-b)) r)
    (dolist (x (nreverse r)) (prin1 x) (terpri))))
"##,
        r##"(prin1 (list (set-binary-mode 'stdout nil) (set-binary-mode 'stderr t) (set-binary-mode 'stdin nil)
             (condition-case e (set-binary-mode 1 nil) (error e))
             (condition-case e (set-binary-mode 'stdfoo nil) (error e))
             (condition-case e (set-binary-mode (make-symbol "stdout") nil) (error e))
             (condition-case e (set-default-file-modes nil) (error e))
             (condition-case e (set-default-file-modes 1.5) (error e))
             (condition-case e (set-default-file-modes (1+ most-positive-fixnum)) (error e))
             (let ((m (default-file-modes))) (prog1 (list (set-default-file-modes #o750) (default-file-modes)) (set-default-file-modes m)))))
(terpri)
"##,
    ] {
        assert_same(
            &corpus,
            &["--batch", "-Q", "--eval", &format!("(progn {script})")],
        );
    }
}

#[test]
fn describe_insertions_are_insert_string_s() {
    // syntax.c's internal-describe-syntax-value and keymap.c's
    // describe-vector and describe-map insert with insert_string and its
    // relatives, a piece at a time: a read-only buffer signals before
    // anything is inserted, each piece runs the change hooks, and
    // describe_vector_princ indents with Findent_to (16, 1), as
    // indent-tabs-mode and tab-width have it.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            &format!(
                "(progn {})",
                r##"(defun dv-run (ro thunk)
  (with-temp-buffer
    (insert "x")
    (setq buffer-read-only ro)
    (let ((after nil) (before nil))
      (add-hook 'after-change-functions (lambda (b e l) (push (list b e l) after)) nil t)
      (add-hook 'before-change-functions (lambda (b e) (push (list b e) before)) nil t)
      (let ((r (condition-case e (funcall thunk) (error (list 'ERR (car e))))))
        (list r (buffer-string) (length after) (length before) (car after) (car (last before)))))))
(dolist (ro '(nil t))
  (prin1 (list ro
               (dv-run ro (lambda () (internal-describe-syntax-value '(1))))
               (dv-run ro (lambda () (internal-describe-syntax-value nil))) (dv-run ro (lambda () (internal-describe-syntax-value (cons (logior 4 (ash 1 20) (ash 1 16)) ?\)))))
               (dv-run ro (lambda () (let ((standard-output (current-buffer))) (describe-vector [a nil c] 'princ))))
               (dv-run ro (lambda () (let ((indent-tabs-mode nil) (standard-output (current-buffer))) (describe-vector [aa bb] 'princ))))
               (dv-run ro (lambda () (let ((tab-width 4) (standard-output (current-buffer))) (describe-vector [aa bb] 'princ))))
               (dv-run ro (lambda () (let ((m (make-sparse-keymap))) (define-key m "a" 'forward-char) (define-key m [?b] 'backward-char) (describe-map-tree m) nil)))))
  (terpri))
"##
            ),
        ],
    );
}

#[test]
fn symbols_with_position_decode_as_lisp_h_symbolp_xsymbol_and_eq() {
    // lisp.h: while `symbols-with-pos-enabled' is set, CHECK_SYMBOL takes
    // a symbol with position and XSYMBOL reaches its bare symbol, EQ
    // compares the bare symbols, and NILP stays BASE_EQ.  Ffset's cycle
    // check, indirect_function, the fringe, CCL, char-table, coding
    // system, face and key decoders, json's `true' and where_is_internal
    // all read them so; with the flag off each refuses them.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            &format!(
                "(progn {})",
                r##"(defmacro ps-try (form) `(condition-case e ,form (error (list 'ERR e))))
(let ((print-symbols-bare t))
  (dolist (on '(nil t))
    (let* ((symbols-with-pos-enabled on)
           (p (lambda (s) (position-symbol s 5))))
      (prin1 (list on
        (ps-try (fset (funcall p 'ps-a) (funcall p 'ps-a)))
        (ps-try (progn (fset 'ps-b 'car) (fset 'ps-c (funcall p 'ps-b)) (fset 'ps-b (funcall p 'ps-c))))
        (ps-try (defalias (funcall p 'ps-d) (funcall p 'ps-d)))
        (ps-try (fmakunbound (funcall p t)))
        (ps-try (list (indirect-function (funcall p t)) (indirect-function t) (indirect-function (funcall p 'ps-c))))
        (ps-try (let ((ct (make-char-table (funcall p 'ps-purpose)))) (list (char-table-subtype ct) (eq (char-table-subtype ct) 'ps-purpose))))
        (ps-try (list (define-fringe-bitmap (funcall p 'ps-bitmap) [1 2]) (integerp (get 'ps-bitmap 'fringe)) (eq (car fringe-bitmaps) 'ps-bitmap)
                      (set-fringe-bitmap-face (funcall p 'ps-bitmap) 'error) (destroy-fringe-bitmap (funcall p 'ps-bitmap)) (get 'ps-bitmap 'fringe)))
        (ps-try (progn (defvar ps-special 1) (internal-make-var-non-special (funcall p 'ps-special)) (special-variable-p 'ps-special)))
        (ps-try (internal-event-symbol-parse-modifiers (funcall p 'C-M-ps-key)))
        (ps-try (list (register-ccl-program (funcall p 'ps-ccl) nil) (register-ccl-program 'ps-ccl nil) (get 'ps-ccl 'ccl-program-idx)))
        (ps-try (list (register-code-conversion-map (funcall p 'ps-map) [1]) (register-code-conversion-map 'ps-map [2]) (get 'ps-map 'code-conversion-map)))
        (ps-try (list (single-key-description (funcall p 'C-home)) (single-key-description (funcall p 'home) t)))
        (ps-try (list (string-collate-lessp (funcall p 'a) (funcall p 'b)) (string-collate-equalp (funcall p 'a) "a")))
        (ps-try (internal-default-signal-process (emacs-pid) (funcall p 'ps-nosig)))
        (ps-try (set-binary-mode (funcall p 'ps-stream) nil))
        (ps-try (list (json-serialize (vector (funcall p t))) (json-serialize (list (funcall p :k) 1))))
        (ps-try (check-coding-system (funcall p 'utf-8)))
        (ps-try (check-coding-system (funcall p nil)))
        (ps-try (coding-system-base (funcall p 'utf-8-unix)))
        (ps-try (internal-lisp-face-attribute-values (funcall p :underline)))
        (ps-try (list (coding-system-eol-type (funcall p 'utf-8-unix)) (consp (where-is-internal (funcall p nil)))))))
      (terpri))))
"##
            ),
        ],
    );
}

#[test]
fn file_names_reach_errors_and_buffers_as_the_lisp_strings_gnu_keeps() {
    // fileio.c's report_file_error is given the Lisp file names (expanded,
    // or as given for access-file and load), so a name's text properties
    // and raw bytes reach the error data, and its errno text is the
    // string code_convert_string_norecord makes; BVAR (b, filename) and
    // file_truename hold the string `buffer-file-name' was set to; and a
    // C string from SSDATA (a hostname, a module file, access-file's
    // message) carries the string's own bytes.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            &format!(
                "(progn {})",
                r##"(let* ((dir (file-name-as-directory (make-temp-file "fe" t)))
       (default-directory dir)
       (show (lambda (x) (if (stringp x)
                             (let ((s (if (string-prefix-p dir x) (substring x (length dir)) x)))
                               (list s (multibyte-string-p s) (text-properties-at 0 s)))
                           x)))
       (err (lambda (thunk) (condition-case e (progn (funcall thunk) 'ok)
                              (error (cons (car e) (mapcar show (cdr e)))))))
       (p (propertize "nd/foo" 'face 'bold))
       (r (string-to-multibyte "nd/\377a")))
  (unwind-protect
      (dolist (f (list p r))
        (prin1 (list (funcall err (lambda () (with-temp-buffer (insert-file-contents f))))
                     (funcall err (lambda () (with-temp-buffer (insert "x") (write-region nil nil f nil 0))))
                     (funcall err (lambda () (copy-file f "dst")))
                     (funcall err (lambda () (rename-file f "dst")))
                     (funcall err (lambda () (add-name-to-file f "dst")))
                     (funcall err (lambda () (make-symbolic-link "x" f)))
                     (funcall err (lambda () (delete-directory f)))
                     (funcall err (lambda () (make-directory f)))
                     (funcall err (lambda () (set-file-modes f #o644)))
                     (funcall err (lambda () (set-file-times f)))
                     (funcall err (lambda () (access-file f (string-to-multibyte "m\377"))))
                     (funcall err (lambda () (load f nil t)))))
        (terpri))
    (delete-directory dir t)))
(with-temp-buffer
  (setq buffer-file-name (propertize "/tmp/x" 'a 1))
  (setq buffer-file-truename (propertize "/tmp/y" 'b 2))
  (prin1 (list (eq buffer-file-name buffer-file-name) buffer-file-name (buffer-file-name)
               (eq buffer-file-truename buffer-file-truename) buffer-file-truename
               (condition-case e (setq buffer-file-name 5) (error e))
               (let ((r (string-to-multibyte "/tmp/\377a"))) (setq buffer-file-name r)
                    (list (eq (buffer-file-name) r) (append buffer-file-name nil)))))
  (terpri))
(let ((r (string-to-multibyte "\377a")))
  (prin1 (list (condition-case e (network-lookup-address-info r) (error (list (multibyte-string-p (cadr e)) (append (cadr e) nil))))
               (condition-case e (network-lookup-address-info r 'bogus) (error e))
               (condition-case e (module-load (string-to-multibyte "/nonexistent/\377a"))
                 (error (list (car e) (multibyte-string-p (nth 2 e)) (append (substring (nth 2 e) 0 16) nil))))))
  (terpri))
"##
            ),
        ],
    );
}

#[test]
fn file_error_data_is_get_file_errno_data_s() {
    // fileio.c:get_file_errno_data: the condition, the message, the errno
    // text through code_convert_string_norecord (a multibyte copy of an
    // ASCII text under an ASCII-compatible `locale-coding-system', nil's
    // `no-conversion' included), then the Lisp names -- call-process's
    // output and stderr files, make-temp-file's prefix, write-region's and
    // load's files among them.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            &format!(
                "(progn {})",
                r##"(let ((show (lambda (e) (list (car e) (cadr e) (multibyte-string-p (nth 2 e)) (nthcdr 3 e)))))
  (prin1 (list (with-temp-buffer (insert "x") (condition-case e (write-region nil nil "/nonexistent-dir/f" nil 0) (error (funcall show e))))
               (condition-case e (call-process "true" nil '(:file "/nonexistent-dir/out")) (error (funcall show e)))
               (condition-case e (call-process "true" nil (list t "/nonexistent-dir/err")) (error (funcall show e)))
               (condition-case e (make-temp-file "/nonexistent-dir/pfx") (error (funcall show e)))
               (condition-case e (make-temp-file "/nonexistent-dir/pfx" t) (error (funcall show e)))
               (condition-case e (load "/nonexistent-dir/l" nil t) (error (funcall show e)))))
  (terpri))
"##
            ),
        ],
    );
}

#[test]
fn file_errors_name_the_lisp_objects_gnu_names() {
    // The errno text is code_convert_string_norecord's under each
    // `locale-coding-system': a bad one signals, `utf-16le' decodes the
    // ASCII bytes.  call-process names DECODE_FILE (ENCODE_FILE) of each
    // file without its "/:" (the expanded string itself for an ASCII name,
    // properties and all; a decoded copy of a unibyte one), the program
    // search names the program given, get_current_directory names
    // `default-directory' after file_accessible_directory_p, and
    // redirect-debugging-output, serial_open, make-temp-file-internal's
    // GT_NOCREATE lstat, create_temp_file's pattern and a network
    // connection report what GNU's report_file_errno does.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            &format!(
                "(progn {})",
                r##"(defmacro show (form) `(condition-case err (prin1 (list ',form '=> ,form)) (error (prin1 (list ',form 'ERR err (mapcar (lambda (x) (if (stringp x) (list (multibyte-string-p x) (text-properties-at 0 x)) x)) (cdr err)))))))
(dolist (lcs (list nil 'bogus-cs "str" 'utf-16le 'utf-8 'latin-1))
  (let ((locale-coding-system lcs))
    (show (rename-file "/nonexistent-dir/a" "/nonexistent-dir/b")) (terpri)))
(let ((p (propertize "/nonexistent-dir/out" 'face 'bold)))
  (show (call-process "true" nil (list :file p))) (terpri)
  (show (call-process "true" nil (list t p))) (terpri)
  (show (call-process "true" nil (list :file (concat "/:" p)))) (terpri)
  (show (call-process "true" nil (list :file (string-to-unibyte "/nonexistent-dir/\351")))) (terpri)
  (show (call-process "true" nil (list t (string-to-unibyte "/nonexistent-dir/\351")))) (terpri)
  (show (call-process "true" nil (list :file "/nonexistent-dir/\u00e9"))) (terpri)
  (show (redirect-debugging-output p)) (terpri)
  (show (call-process (propertize "no-such-program-xyz" 'face 'bold))) (terpri)
  (show (make-process :name "x" :command (list (propertize "no-such-program-xyz" 'face 'bold)))) (terpri)
  (show (make-serial-process :port p :speed nil)) (terpri))
(let ((default-directory nil)) (show (call-process "true"))) (terpri)
(let ((default-directory "/etc/passwd/")) (show (call-process "true"))) (terpri)
(let ((default-directory (propertize "/nonexistent-dir/" 'face 'bold))) (show (call-process "true"))) (terpri)
(show (make-temp-file-internal "/etc/passwd/pfx" 0 "" nil)) (terpri)
(show (make-temp-file-internal "/etc/passwd/pfx" nil "" nil)) (terpri)
(show (make-network-process :name "c" :host "127.0.0.1" :service 1)) (terpri)
(show (with-temp-buffer (insert "abc") (call-process-region (point-min) (point-max) "cat" nil t))) (terpri)
(let ((temporary-file-directory "/nonexistent-dir/")) (show (with-temp-buffer (insert "abc") (call-process-region (point-min) (point-max) "cat" nil t)))) (terpri)
(defvar zz-log nil)
(defun zz-handler (op &rest args)
  (push op zz-log)
  (let ((inhibit-file-name-handlers (cons 'zz-handler inhibit-file-name-handlers))
        (inhibit-file-name-operation op))
    (if (eq op 'write-region) nil (apply op args))))
(let ((file-name-handler-alist (list (cons "\\.zzh\\'" 'zz-handler))))
  (let ((f (make-temp-file-internal "/tmp/zzp" nil ".zzh" "hello")))
    (prin1 (list (reverse zz-log) (file-attribute-size (file-attributes f))))
    (delete-file f)))
(terpri)
(show (make-serial-process :port "/nonexistent-dir/port" :speed "x")) (terpri)
(show (make-serial-process :port "/nonexistent-dir/port" :speed nil :name 5)) (terpri)
"##
            ),
        ],
    );
}

#[test]
fn the_audit_of_rounds_323_to_328_is_answered_as_gnu_answers() {
    // The probes of the independent review of rounds 323 to 328, each in
    // its own session: describe-vector as keymap.c's describe_vector (its
    // elements read afresh, a change hook that drops one and collects
    // included; a range as three insertions; get_keyelt; a char-table's
    // runs, 8-bit split, default and no parent), coding systems and labels
    // and properties given as symbols with position, unshow_buffer's start
    // recorded before its point is read, set_marker_restricted on window
    // markers, the configuration-change hook over a window deleted under
    // it, get-file-buffer's Fstring_equal, scan_for_column's window
    // lookup, a syntax description's matching character, and the
    // hostname failure message cut at its NUL.
    let corpus = Corpus::new();
    for program in [
        r##"(progn
(defvar v nil)
(setq v (vector (list 'a) (list (make-string 3 ?b) 'bb) (list (make-string 3 ?c) 'cc)))
(with-temp-buffer
  (add-hook 'after-change-functions (lambda (&rest _) (aset v 2 (list 'hooked)) (garbage-collect)) nil t)
  (describe-vector v)
  (message "%S" (buffer-string))))"##,
        r##"(progn
(defmacro try (f) `(condition-case e ,f (error (list 'ERR e))))
(defvar log nil)
(with-temp-buffer
  (add-hook 'after-change-functions (lambda (b e l) (push (buffer-substring-no-properties b e) log)) nil t)
  (describe-vector [a a a nil b])
  (message "range hooks: %S" (reverse log))
  (message "text: %S" (buffer-string)))
(with-temp-buffer
  (describe-vector [("label" . forward-char) nil (menu-item "x" backward-char)])
  (message "keyelt: %S" (buffer-string)))
(with-temp-buffer
  (let ((ct (make-char-table nil)))
    (set-char-table-range ct '(?a . ?b) (list 1))
    (set-char-table-range ct ?c (list 1))
    (describe-vector ct)
    (message "ct equal: %S" (buffer-string))))
(with-temp-buffer
  (let ((ct (make-char-table nil)))
    (set-char-table-range ct '(#x3FFF70 . #x3FFF90) 'z)
    (describe-vector ct)
    (message "ct 8bit: %S" (buffer-string))))
(with-temp-buffer
  (let ((ct (make-char-table nil 'dflt)))
    (set-char-table-range ct '(?a . ?b) 'q)
    (describe-vector ct)
    (message "ct default: %S" (substring (buffer-string) 0 (min 200 (length (buffer-string)))))))
(with-temp-buffer
  (let* ((p (make-char-table nil)) (ct (make-char-table nil)))
    (set-char-table-range p '(?a . ?c) 'par)
    (set-char-table-parent ct p)
    (set-char-table-range ct ?b 'own)
    (describe-vector ct)
    (message "ct parent: %S" (buffer-string)))))"##,
        r##"(progn
(defun p (s) (position-symbol s 3))
(let ((symbols-with-pos-enabled t))
  (message "a %S" (condition-case e (encode-coding-string "a" (p 'utf-8)) (error (list (car e) (cdr e) (type-of (cadr e)) (bare-symbol (cadr e)) (eq (cadr e) 'utf-8))))))
(message "b %S" (condition-case e (let ((symbols-with-pos-enabled t)) (encode-coding-string "a" (p 'utf-8))) (error (list (car e) (cdr e) (type-of (cadr e))))))
(message "c %S" (condition-case e (encode-coding-string "a" (p 'utf-8)) (error e)))
(message "d %S" (condition-case e (let ((symbols-with-pos-enabled t)) (decode-coding-string "a" (p 'utf-8))) (error e)))
(message "e %S" (condition-case e (let ((symbols-with-pos-enabled t)) (check-coding-system (p 'utf-8))) (error e)))
(message "f %S" (condition-case e (let ((symbols-with-pos-enabled t)) (with-temp-buffer (insert "a") (encode-coding-region 1 2 (p 'utf-8)))) (error e)))
(message "g %S" (condition-case e (let ((symbols-with-pos-enabled t)) (let ((coding-system-for-read (p 'utf-8))) (with-temp-buffer (insert-file-contents "/etc/passwd") (buffer-size)))) (error e))))"##,
        r##"(progn
(defun show (tag form) (message "%s: %S" tag (condition-case e (funcall form) (error (list 'ERR e)))))
(let* ((b (get-buffer-create "B"))
       (w1 (selected-window))
       w2)
  (with-current-buffer b (insert (make-string 200 ?x)))
  (setq w2 (split-window w1))
  (set-window-buffer w2 b)
  (set-window-start w2 50)
  (set-window-point w2 nil)
  (show "wp" (lambda () (window-point w2)))
  (show "ws" (lambda () (window-start w2)))
  (show "del" (lambda () (delete-window w2)))
  (show "live" (lambda () (list (window-live-p w2) (window-buffer w2) (length (window-list)))))
  (show "set-wb" (lambda () (set-window-buffer w1 b)))
  (show "ws1" (lambda () (window-start w1)))))"##,
        r##"(progn
(defmacro try (f) `(condition-case e ,f (error (list 'ERR e))))
(let* ((b (get-buffer-create "B")) (c (get-buffer-create "C"))
       (w1 (selected-window)) (w2 (split-window w1)))
  (with-current-buffer b (insert (make-string 100 ?x)))
  (with-current-buffer c (insert (make-string 300 ?y)))
  (set-window-buffer w2 b)
  (with-current-buffer b (narrow-to-region 10 50))
  (let ((m (with-current-buffer b (copy-marker 80)))
        (mc (with-current-buffer c (copy-marker 250))))
    (message "1 %S" (try (list (set-window-start w2 m) (window-start w2))))
    (message "2 %S" (try (list (set-window-start w2 mc) (window-start w2))))
    (message "3 %S" (try (list (set-window-point w2 m) (window-point w2))))
    (message "4 %S" (try (list (set-window-point w2 mc) (window-point w2))))
    (message "5 %S" (try (list (set-window-start w2 200) (window-start w2))))
    (message "6 %S" (try (list (set-window-start w2 1.5) (window-start w2))))
    (message "7 %S" (try (list (set-window-start w2 (1+ most-positive-fixnum)) (window-start w2))))
    (message "8 %S" (try (list (set-window-point w2 (1+ most-positive-fixnum)) (window-point w2))))
    (message "9 %S" (try (list (set-window-point w2 -5) (window-point w2))))
    (with-current-buffer b (widen))
    (message "10 %S" (try (list (set-window-start w2 m) (window-start w2) (set-window-start w2 3) (window-start w2))))
    (with-current-buffer b (narrow-to-region 10 50))
    (message "11 %S" (try (list (set-window-start w2 m) (window-start w2))))
    (message "12 %S" (try (list (set-window-point w2 m) (window-point w2))))
    ;; selected window, other buffer current
    (select-window w2)
    (message "13 %S" (try (with-current-buffer c (list (set-window-point nil m) (point) (window-point w2)))))
    (message "14 %S" (try (with-current-buffer c (list (set-window-point nil mc) (window-point w2)))))
    (message "15 %S" (try (with-current-buffer b (list (set-window-point nil mc) (window-point w2)))))
    )))"##,
        r##"(progn
(defvar log nil)
(let* ((a (get-buffer-create "A")) (b (get-buffer-create "B")) (c (get-buffer-create "C"))
       (w1 (selected-window)) (w2 (split-window w1)) (w3 (split-window w2)))
  (set-window-buffer w1 a) (set-window-buffer w2 b) (set-window-buffer w3 c)
  (with-current-buffer a (setq-local window-configuration-change-hook
     (list (lambda () (push (list 'a (buffer-name) (eq (selected-window) w1)) log) (delete-window w3) (garbage-collect)))))
  (with-current-buffer b (setq-local window-configuration-change-hook
     (list (lambda () (push (list 'b (buffer-name)) log)))))
  (with-current-buffer c (setq-local window-configuration-change-hook
     (list (lambda () (push (list 'c (buffer-name) (window-live-p (selected-window))) log)))))
  (setq-default window-configuration-change-hook (list (lambda () (push (list 'g (buffer-name)) log))))
  (message "r %S" (condition-case e (run-window-configuration-change-hook) (error e)))
  (message "%S" (reverse log))))"##,
        r##"(progn
(defmacro try (f) `(condition-case e ,f (error (list 'ERR e))))
(defun p (s) (position-symbol s 3))
(let ((symbols-with-pos-enabled t))
  (message "8 %S" (try (with-temp-buffer (insert (propertize "x" 'face 'bold)) (list (get-text-property 1 (p 'face)) (get-char-property 1 (p 'face)) (text-property-any 1 2 (p 'face) 'bold) (next-single-property-change 1 (p 'face)) (next-single-char-property-change 1 (p 'face))))))
  (message "19 %S" (try (with-temp-buffer (insert "abc") (narrow-to-region 2 3) (internal--labeled-narrow-to-region 2 3 'L) (internal--labeled-widen (p 'L)) (widen) (list (point-min) (point-max)))))
  (message "22 %S" (try (with-temp-buffer (insert "x") (overlay-get (let ((o (make-overlay 1 1))) (overlay-put o 'q 9) o) (p 'q)))))
  (message "24 %S" (try (frame-parameter nil (p 'name))))
  (message "24b %S" (try (frame-parameter nil (p 'nonexistent-param))))
  (message "31 %S" (try (network-lookup-address-info "127.0.0.1" (p 'ipv4) (p 'numeric))))
  (message "32 %S" (try (network-lookup-address-info "127.0.0.1" nil (p 'numeric))))
  (message "33 %S" (try (length (network-interface-list nil (p 'ipv4)))))
  (message "34 %S" (try (let ((proc (make-network-process (p :name) "e" (p :server) t :family (p 'ipv4) :service 0 :host "127.0.0.1" :noquery t))) (prog1 (list (process-name proc) (process-contact proc :family)) (delete-process proc))))))
(message "35 %S" (try (let ((proc (make-network-process :name "a" :name "b" :server t :service 0 :host "127.0.0.1" :noquery t))) (prog1 (process-name proc) (delete-process proc)))))
(message "36 %S" (try (with-temp-buffer (setq buffer-file-name "/tmp/xyz-q") (list (eq (get-file-buffer "/tmp/xyz-q") (current-buffer)) (get-file-buffer 5)))))
(message "37 %S" (try (get-truename-buffer 5))))"##,
        r##"(progn
(defmacro try (f) `(condition-case e ,f (error (list 'ERR e))))
(let ((b (get-buffer-create "rb")))
  (with-current-buffer b (setq buffer-file-name (string-to-unibyte "/tmp/x\377")))
  (message "1 %S" (try (list (get-file-buffer (string-to-unibyte "/tmp/x\377")) (get-file-buffer (string-to-multibyte "/tmp/x\377")) (get-file-buffer "/tmp/xÿ"))))
  (with-current-buffer b (setq buffer-file-name "/tmp/xÿ"))
  (message "2 %S" (try (list (get-file-buffer (string-to-unibyte "/tmp/x\377")) (get-file-buffer (string-to-multibyte "/tmp/x\377")) (get-file-buffer "/tmp/xÿ"))))))"##,
        r##"(progn
(defun ws-push (&rest xs) (prin1 xs) (terpri))
(defmacro ws-try (tag form) `(ws-push ,tag (condition-case e (progn ,form 'ok) (error (list 'ERR (car e) (if (windowp (cadr e)) 'W (cadr e)))))))
(defvar ws-objs nil)
(defun ws-setup (n)
  (ws-try (list n 'sel) (select-window (frame-first-window)))
  (ws-try (list n 'dow) (delete-other-windows))
  (ws-try (list n 'stb) (switch-to-buffer (get-buffer-create "wsA")))
  (erase-buffer) (insert "line one\nline two\nline three\n") (goto-char 5)
  (ws-try (list n 'split)
          (let* ((sel (selected-window))
                 (right (split-window sel nil 'right))
                 (below (split-window right nil 'below))
                 (dead (split-window below nil 'below)))
            (set-window-buffer below (get-buffer-create "wsB"))
            (delete-window dead)
            (setq ws-objs (list (cons 'selected sel) (cons 'right right) (cons 'below below) (cons 'dead dead))))))
(ws-setup 0)
(ws-push 'swp (condition-case e (set-window-point (cdr (assq 'below ws-objs)) nil) (error e)))
(ws-setup 1)
(ws-setup 2)
(ignore-errors (select-window (frame-first-window)))
(ignore-errors (delete-other-windows))
(ws-push 'dead-selected (window-live-p (selected-window)) (buffer-name))
(ws-try 'cc (current-column))
(ws-try 'cc2 (progn (insert "\t\tx") (current-column)))
(ws-try 'mtc (move-to-column 3))
(ws-try 'gbw (get-buffer-window))
(kill-emacs 0))"##,
        r##"(progn
(dolist (m (list #xD800 #x3FFF80 #x110000))
  (with-temp-buffer (internal-describe-syntax-value (cons 4 m)) (message "%x %S" m (buffer-string))))
(with-temp-buffer (internal-describe-syntax-value (cons 4 ?\))) (set-buffer-multibyte nil) (message "%S" (buffer-string))))"##,
        r##"(progn
(network-lookup-address-info "a\0b" nil (quote numeric)))"##,
    ] {
        assert_same(&corpus, &["--batch", "-Q", "--eval", program]);
    }
}

#[test]
fn symbols_with_position_compare_as_eq_where_gnu_compares() {
    // lisp.h's EQ while `symbols-with-pos-enabled' is set, a symbol with
    // position standing for its symbol, in the comparisons C makes: text
    // property writes and removals, stickiness lists, face merging and
    // interval merging, overlay plists and `evaporate', frame and window
    // parameter alists, keymaps, features, catch tags, faces, font keys,
    // invisibility specs and lexical environments; setq's XSYMBOL, its
    // value evaluated before CHECK_SYMBOL; and code_convert_string's fast
    // path storing the coding system as given in `last-coding-system-used'.
    let corpus = Corpus::new();
    for program in [
        r##"(progn
(defmacro try (&rest f) `(condition-case e (progn ,@f) (error (list 'ERR e))))
(defun p (s) (position-symbol s 3))
(let ((symbols-with-pos-enabled t))
  (with-temp-buffer
    (insert "abcdef")
    (put-text-property 2 4 'face 'bold)
    (message "1 %S" (try (put-text-property 3 5 (p 'face) 'italic) (buffer-string)))
    (message "2 %S" (try (add-face-text-property 1 3 'underline nil) (get-text-property 2 'face)))
    (message "3 %S" (try (add-face-text-property 1 3 (p 'x) t) (get-text-property 2 'face)))
    (message "4 %S" (try (text-property-not-all 1 7 (p 'face) nil)))
    (message "5 %S" (try (next-single-property-change 1 (p 'face))))
    (message "6 %S" (try (put-text-property 1 2 'rear-nonsticky (list (p 'k))) (put-text-property 1 2 'k 1) (goto-char 2) (insert-and-inherit "Z") (get-text-property 2 'k)))
    (message "7 %S" (try (put-text-property 3 4 'front-sticky (list (p 'm))) (put-text-property 3 4 'm 2) (goto-char 3) (insert-and-inherit "Y") (get-text-property 3 'm)))
    (message "8 %S" (try (remove-list-of-text-properties 1 (point-max) (list (p 'k) (p 'm))) (text-properties-at 2)))
    (message "9 %S" (try (let ((s (concat (propertize "a" (p 'q) 1) (propertize "b" 'q 1)))) (list (next-single-property-change 0 'q s) (equal-including-properties s (propertize "ab" 'q 1))))))
    (message "10 %S" (try (let ((s (propertize "xy" 'face 'bold))) (add-face-text-property 0 2 'italic t s) (get-text-property 0 'face s))))
    (message "11 %S" (try (let ((o (make-overlay 1 1))) (overlay-put o (p 'evaporate) t) (overlay-buffer o))))
    (message "12 %S" (try (let ((o (make-overlay 1 3))) (overlay-put o 'z 1) (overlay-put o (p 'z) 2) (overlay-properties o))))
    (message "13 %S" (try (modify-frame-parameters nil (list (cons (p 'zz1) 7))) (frame-parameter nil 'zz1)))
    (message "14 %S" (try (set-frame-parameter nil (p 'zz2) 8) (set-frame-parameter nil 'zz2 9) (let ((n 0)) (dolist (c (frame-parameters) n) (when (eq (car-safe c) 'zz2) (setq n (1+ n)))))))
    (message "15 %S" (try (set-window-parameter nil (p 'wq) 3) (list (window-parameter nil 'wq) (window-parameter nil (p 'wq)))))
    (message "16 %S" (try (let ((s (propertize "ab" 'composition nil))) (put-text-property 0 1 (p 'composition) '(1 . 2) s) (text-properties-at 0 s))))
    (message "17 %S" (try (let ((s "a")) (list (encode-coding-string s (p 'utf-8)) last-coding-system-used (decode-coding-string s (p 'latin-1)) last-coding-system-used))))
    (message "18 %S" (try (encode-coding-string "a\n" (p 'utf-8-dos)) last-coding-system-used))
    (message "19 %S" (try (with-temp-buffer (encode-coding-string "a" (p 'utf-8) nil (current-buffer)) last-coding-system-used)))
    (message "20 %S" (try (let ((cs (make-symbol "myalias"))) (define-coding-system-alias cs 'utf-8) (encode-coding-string "a" cs) (eq last-coding-system-used cs))))
    )))"##,
        r##"(progn
(defmacro try (&rest f) `(condition-case e (progn ,@f) (error (list 'ERR e))))
(defun p (s) (position-symbol s 3))
(put 'mycat 'zap 'from-cat)
(let ((symbols-with-pos-enabled t))
  (with-temp-buffer
    (insert (propertize "abc" 'category 'mycat 'face 'bold) "def")
    (let ((o (make-overlay 2 5)))
      (overlay-put o 'ov 'ovval) (overlay-put o 'category 'mycat)
      (message "1 %S" (try (get-text-property 1 (p 'zap))))
      (message "2 %S" (try (let ((char-property-alias-alist '((alias face)))) (get-text-property 1 (p 'alias)))))
      (message "3 %S" (try (let ((default-text-properties '(dtp 7))) (get-text-property 5 (p 'dtp)))))
      (message "4 %S" (try (get-char-property 3 (p 'ov))))
      (message "5 %S" (try (overlay-get o (p 'zap))))
      (message "6 %S" (try (get-char-property-and-overlay 3 (p 'ov))))
      (message "7 %S" (try (text-property-any 1 7 (p 'face) 'bold)))
      (message "8 %S" (try (text-property-not-all 1 7 (p 'face) 'bold)))
      (message "9 %S" (try (next-single-property-change 1 (p 'face))))
      (message "10 %S" (try (previous-single-property-change 7 (p 'face))))
      (message "11 %S" (try (next-single-char-property-change 1 (p 'ov))))
      (message "12 %S" (try (previous-single-char-property-change 7 (p 'ov))))
      (message "13 %S" (try (get-pos-property 3 (p 'ov))))
      (message "14 %S" (try (get-pos-property 2 (p 'face))))
      (message "15 %S" (try (let ((s (concat (propertize "x" 'face 'b) "y"))) (put-text-property 0 1 (p 'face) 'i s) (text-properties-at 0 s))))
      (message "16 %S" (try (let ((s (concat (propertize "x" 'face 'b) "y"))) (remove-text-properties 0 1 (list (p 'face) nil) s) (text-properties-at 0 s))))
      (message "17 %S" (try (let ((s (concat (propertize "x" 'face 'b) "y"))) (remove-list-of-text-properties 0 1 (list (p 'face)) s) (text-properties-at 0 s))))
      (message "18 %S" (try (let ((s (concat (propertize "x" 'face 'b) "y"))) (add-text-properties 0 1 (list (p 'face) 'q) s) (text-properties-at 0 s))))
      (message "19 %S" (try (let ((s (propertize "x" 'face 'b))) (get-text-property 0 (p 'face) s))))
      (message "20 %S" (try (let ((s (propertize "xy" 'face 'b))) (next-single-property-change 0 (p 'face) s))))
      (message "21 %S" (try (let ((s (propertize "x" 'face 'b))) (text-property-any 0 1 (p 'face) 'b s))))
      (message "22 %S" (try (overlay-properties o)))
      (message "23 %S" (try (overlays-in 1 7)))
      (message "24 %S" (try (let ((inhibit-read-only (list (p 'ro)))) (put-text-property 4 5 'read-only 'ro) (goto-char 5) (insert "Z") (buffer-string))))
      (message "25 %S" (try (let ((inhibit-read-only (list 'ro))) (put-text-property 4 5 'read-only (p 'ro)) (goto-char 5) (delete-char 1) (buffer-string))))
      (message "26 %S" (try (get-char-property 3 (p 'zap))))
      (message "27 %S" (try (plist-get (text-properties-at 1) (p 'face))))
      (message "28 %S" (try (next-char-property-change 1)))
      ))))"##,
        r##"(progn
(defmacro try (&rest f) `(condition-case e (progn ,@f) (error (list 'ERR e))))
(defun p (s) (position-symbol s 3))
(let ((symbols-with-pos-enabled t))
  (with-temp-buffer
    (insert "abcdef")
    (let ((o (make-overlay 2 5)))
      (overlay-put o 'ov 'ovval)
      (message "1 %S" (try (overlay-put o (p 'ov) 'new) (overlay-properties o)))
      (message "2 %S" (try (modify-frame-parameters nil (list (cons (p 'zzparm) 1))) (frame-parameter nil 'zzparm)))
      (message "3 %S" (try (frame-parameter nil (p 'zzparm))))
      (message "4 %S" (try (frame-parameter nil (p 'height))))
      (message "5 %S" (try (frame-parameter nil (p 'buffer-list))))
      (message "6 %S" (try (frame-parameter nil (p 'minibuffer))))
      (message "7 %S" (try (frame-parameter nil 3)))
      (message "8 %S" (try (frame-parameter nil nil)))
      (message "9 %S" (try (frame-parameter nil (p 'menu-bar-lines))))
      (message "10 %S" (try (frame-parameter nil (p 'foreground-color))))
      (message "11 %S" (try (frame-parameter nil (p 'background-mode))))
      (message "12 %S" (try (frame-parameter nil (p 'font))))
      (message "13 %S" (try (frame-parameter nil (p 'cursor-color))))
      (message "14 %S" (try (frame-parameter nil (p 'window-system))))
      (message "15 %S" (try (frame-parameter nil (p 'title))))
      (message "16 %S" (try (frame-parameter nil (p 'tty-type))))
      (message "17 %S" (try (frame-parameter nil (p 'display-type))))
      )))
(message "n1 %S" (try (frame-parameter nil (p 'name))))
(message "n2 %S" (try (frame-parameter nil (p 'height)))))"##,
        r##"(progn
(defun p (s) (position-symbol s 3))
(let ((symbols-with-pos-enabled t))
  (with-temp-buffer
    (insert "abc")
    (let ((o (make-overlay 2 2)))
      (overlay-put o (p 'evaporate) t)
      (message "1 %S %S" (overlay-buffer o) (overlay-properties o)))
    (let ((o (make-overlay 1 2)))
      (overlay-put o 'k 1)
      (overlay-put o (p 'k) 2)
      (message "2 %S %S" (overlay-get o 'k) (overlay-properties o))))))"##,
        r##"(progn
(defmacro try (&rest f) `(condition-case e (progn ,@f) (error (list 'ERR e))))
(defun p (s) (position-symbol s 3))
(let ((symbols-with-pos-enabled t))
  (modify-frame-parameters nil (list (cons (p 'zzparm) 1)))
  (message "1 %S" (try (frame-parameter nil 'zzparm)))
  (message "2 %S" (try (frame-parameter nil (p 'zzparm))))
  (message "3 %S" (try (cdr (assq 'zzparm (frame-parameters)))))
  (message "4 %S" (try (seq-find (lambda (c) (eq (car c) 'zzparm)) (frame-parameters))))
  (modify-frame-parameters nil (list (cons 'yyparm 2)))
  (message "5 %S" (try (frame-parameter nil (p 'yyparm))))))"##,
        r##"(progn
(defmacro try (&rest f) `(condition-case e (progn ,@f) (error (list 'ERR e))))
(defun p (s) (position-symbol s 3))
(let ((symbols-with-pos-enabled t))
  (set-window-parameter nil (p 'wq) 1)
  (message "1 %S" (try (list (window-parameter nil 'wq) (window-parameter nil (p 'wq)))))
  (set-window-parameter nil 'wq 2)
  (message "2 %S" (try (window-parameters)))
  (set-frame-parameter nil (p 'fz) 1)
  (set-frame-parameter nil 'fz 2)
  (message "3 %S" (try (seq-filter (lambda (c) (eq (car c) 'fz)) (frame-parameters))))
  (message "4 %S" (try (let ((s (propertize "ab" 'face 'bold))) (set-text-properties 0 1 (list (p 'face) 'x) s) (list (get-text-property 0 'face s) (next-single-property-change 0 'face s)))))
  (message "5 %S" (try (let ((s (concat (propertize "a" (p 'face) 'x) (propertize "b" 'face 'x)))) (list (next-single-property-change 0 'face s) (next-property-change 0 s) s))))
  ))"##,
        r##"(progn
(defun p (s) (position-symbol s 3))
(let ((symbols-with-pos-enabled t))
  (encode-coding-string "a" (p 'latin-1))
  (message "1 %S" last-coding-system-used)
  (decode-coding-string "a" (p 'utf-8))
  (message "2 %S" last-coding-system-used)
  (decode-coding-string "\351" (p 'latin-1))
  (message "3 %S" last-coding-system-used))
(let ((cs (make-symbol "utf-8")))
  (define-coding-system-alias cs 'utf-8)
  (decode-coding-string "a" cs)
  (message "4 %S %S" last-coding-system-used (eq last-coding-system-used cs))))"##,
        r##"(progn
(defmacro try (&rest f) `(condition-case e (progn ,@f) (error (list 'ERR e))))
(defun p (s) (position-symbol s 3))
(provide 'zzfeat)
(let ((symbols-with-pos-enabled t))
  (message "1 %S" (try (keymapp (list (p 'keymap)))))
  (message "2 %S" (try (let ((m (list (p 'keymap)))) (define-key m "a" 'forward-char) (lookup-key m "a"))))
  (message "3 %S" (try (memql (p 'x) '(a x b))))
  (message "4 %S" (try (featurep (p 'zzfeat))))
  (message "5 %S" (try (char-table-subtype (make-char-table (p 'syntax-table)))))
  (message "6 %S" (try (let ((ct (make-char-table (p 'syntax-table)))) (syntax-table-p ct))))
  (message "7 %S" (try (catch 'tg (throw (p 'tg) 5))))
  (message "8 %S" (try (catch (p 'tg) (throw 'tg 6))))
  (message "9 %S" (try (eql (p 'a) 'a)))
  (message "10 %S" (try (font-get (font-spec :family "x" (p :weight) 'bold) :weight)))
  (message "11 %S" (try (let ((f (font-spec :family "x"))) (font-put f (p :size) 12) (font-get f :size))))
  (message "12 %S" (try (with-temp-buffer (insert "abc") (put-text-property 1 2 'invisible 'zz) (setq buffer-invisibility-spec (list (p 'zz))) (invisible-p 1))))
  (message "13 %S" (try (let ((lx 1)) (ignore lx) (eval `(let ((,(p 'lv) 2)) lv) t))))
  (message "14 %S" (try (eval `(let ((lv 3)) ,(p 'lv)) t)))
  (message "15 %S" (try (face-attribute (p 'default) :inverse-video)))
  (message "16 %S" (try (assq (p 'a) '((a . 1)))))
  (message "17 %S" (try (plist-get (list (p 'k) 1) 'k)))
  (message "18 %S" (try (with-temp-buffer (insert "a\tb") (setq-local tab-width (p 8)))))
  (message "19 %S" (try (with-temp-buffer (insert "xyz") (put-text-property 1 3 (p 'field) 'f) (field-beginning 2))))
  (message "20 %S" (try (let ((m (make-sparse-keymap))) (define-key m [remap kill-line] 'ignore) (key-binding [remap kill-line] nil nil) (lookup-key m (vector (p 'remap) 'kill-line)))))
  )
(with-temp-buffer
  (switch-to-buffer (current-buffer))
  (dotimes (i 60) (insert (format "line\t%d\n" i)))
  (goto-char (point-min)) (forward-line 10) (forward-char 6)
  (let ((scroll-preserve-screen-position t)) (message "21 %S" (try (scroll-up 3) (list (line-number-at-pos) (current-column)))))))"##,
        r##"(progn
(defmacro try (&rest f) `(condition-case e (progn ,@f) (error (list 'ERR e))))
(defun p (s) (position-symbol s 3))
(defvar zzv 0)
(message "1 %S" (try (eval '(setq 3 (progn (message "evaluated 1") 1)) t)))
(message "2 %S" (try (eval '(setq zzv 5 "str" (progn (message "evaluated 2") 1)) t) zzv))
(message "3 %S" (let ((symbols-with-pos-enabled t)) (try (eval `(setq ,(p 'zzv) 7) t) zzv)))
(message "4 %S" (let ((symbols-with-pos-enabled t)) (try (eval `(let ((lx 1)) (setq ,(p 'lx) 9) lx) t))))
(message "5 %S" (try (eval `(setq ,(p 'zzv) 8) t) zzv))
(message "6 %S" (let ((symbols-with-pos-enabled t)) (try (eval `(setq ,(p nil) 1) t))))
(message "7 %S" (let ((symbols-with-pos-enabled t)) (try (eval `(setq ,(p t) 1) t)))))"##,
    ] {
        assert_same(&corpus, &["--batch", "-Q", "--eval", program]);
    }
}

#[test]
fn the_review_of_round_331_is_answered_as_gnu_answers() {
    // The probes of round 331's review, each in its own session: keymaps
    // reached through a symbol with position, setq and set of keywords
    // with EQ values and the object `setting-constant' names, face and
    // font keys, category values and symbol plists by EQ, defvar's
    // declaration as given, overlays evaporating after a deletion through
    // their category, lexical scopes, and the flag across buffer-local
    // values, makunbound, unwinding and threads.
    let corpus = Corpus::new();
    for program in [
        r##"(progn
(defmacro try (&rest f) `(condition-case e (progn ,@f) (error (list 'ERR e))))
(defun p (s) (position-symbol s 3))
(defvar zmap (make-sparse-keymap))
(fset 'zmap zmap)
(define-key zmap "a" 'forward-char)
(defun zfn () (interactive) 1)
(let ((symbols-with-pos-enabled t))
  (message "1 %S" (try (keymapp (p 'zmap))))
  (message "2 %S" (try (indirect-function (p 'zfn))))
  (message "3 %S" (try (lookup-key (p 'zmap) "a")))
  (message "4 %S" (try (let ((m (make-sparse-keymap))) (define-key m "b" (p 'zmap)) (lookup-key m "ba"))))
  (message "5 %S" (try (let ((m (make-sparse-keymap))) (set-keymap-parent m (p 'zmap)) (lookup-key m "a"))))
  (message "6 %S" (try (with-temp-buffer (use-local-map (p 'zmap)) (key-binding "a"))))
  (message "7 %S" (try (commandp (p 'zfn))))
  (message "8 %S" (try (functionp (p 'zfn))))
  (message "9 %S" (try (fboundp (p 'zfn))))
  (message "10 %S" (try (funcall (p 'zfn))))
  (message "11 %S" (try (apply (p 'zfn) nil)))
  (message "12 %S" (try (mapcar (p 'car) '((1) (2)))))
  (message "13 %S" (try (where-is-internal (p 'forward-char) zmap)))
  (message "14 %S" (try (keymap-parent (let ((m (make-sparse-keymap))) (set-keymap-parent m zmap) m))))
  (message "15 %S" (try (map-keymap (lambda (k v) (message "  mk %S %S" k v)) (p 'zmap))))
  (message "16 %S" (try (copy-keymap (p 'zmap))))
  (message "17 %S" (try (funcall (p 'zundefined))))
  (message "18 %S" (try (symbol-function (p 'zfn))))
  ))"##,
        r##"(progn
(defmacro try (&rest f) `(condition-case e (progn ,@f) (error (list 'ERR e))))
(defun p (s) (position-symbol s 3))
(let ((symbols-with-pos-enabled t))
  (message "1 %S" (try (set :kw (p :kw))))
  (message "2 %S" (try (set (p :kw) (p :kw))))
  (message "3 %S" (try (set (p :kw) 5)))
  (message "4 %S" (try (eval `(setq ,(p :kw) ,(p :kw)) t)))
  (message "5 %S" (try (eval `(setq ,(p :kw) (quote ,(p :kw))) t)))
  (message "6 %S" (try (set-default (p :kw) 5)))
  (message "7 %S" (try (set-default :kw (p :kw))))
  (message "8 %S" (try (eval `(setq ,(p 'nil) 1) t)))
  (message "9 %S" (try (eval `(let ((,(p 'nil) 1)) 1) t)))
  (message "10 %S" (try (eval `(defvar ,(p 'zzdv) 1) t)))
  (message "11 %S" (try (makunbound (p :kw))))
  ))"##,
        r##"(progn
(defmacro try (&rest f) `(condition-case e (progn ,@f) (error (list 'ERR e))))
(defun p (s) (position-symbol s 3))
(let ((symbols-with-pos-enabled t))
  (message "1 %S" (try (face-attribute-relative-p (p :height) 1.5)))
  (message "2 %S" (try (merge-face-attribute (p :height) 1.5 100)))
  (message "3 %S" (try (face-attribute-relative-p :weight (p 'unspecified))))
  (message "4 %S" (try (merge-face-attribute :weight (p 'unspecified) 'bold)))
  (message "5 %S" (try (let ((tool-bar-style (p 'text))) (tool-bar-get-system-style))))
  (message "6 %S" (try (progn (set-face-attribute 'default nil (p :height) 120) (face-attribute 'default :height))))
  (message "7 %S" (try (face-attribute 'default (p :foreground))))
  (message "8 %S" (try (internal-lisp-face-p (p 'default))))
  (message "9 %S" (try (facep (p 'bold))))
  (message "10 %S" (try (face-font (p 'default))))
  (message "11 %S" (try (with-temp-buffer (insert "ab") (put-text-property 1 2 'face (list (p :weight) 'bold)) (get-text-property 1 'face))))
  (message "12 %S" (try (format "%S" (font-spec (p :weight) 'bold))))
  (message "13 %S" (try (font-get (font-spec :weight 'bold) (p :weight))))
  (message "14 %S" (try (font-spec (p :size) 10 :size 12)))
  ))"##,
        r##"(progn
(defmacro try (&rest f) `(condition-case e (progn ,@f) (error (list 'ERR e))))
(defun p (s) (position-symbol s 3))
(put 'zcat 'zprop 42)
(let ((symbols-with-pos-enabled t))
  (with-temp-buffer
    (insert "abcd")
    (put-text-property 1 3 'category (p 'zcat))
    (message "1 %S" (try (get-text-property 1 'zprop)))
    (put-text-property 1 3 (p 'category) 'zcat)
    (message "2 %S" (try (get-text-property 1 'zprop)))
    (let ((o (make-overlay 1 3)))
      (overlay-put o 'category (p 'zcat))
      (message "3 %S" (try (overlay-get o 'zprop))))
    (let ((o (make-overlay 1 3)))
      (overlay-put o (p 'category) 'zcat)
      (message "4 %S" (try (overlay-get o 'zprop))))
    (message "5 %S" (try (get (p 'zcat) 'zprop)))
    (message "6 %S" (try (progn (put (p 'zcat) (p 'zp2) 1) (list (get 'zcat 'zp2) (symbol-plist 'zcat)))))
    (message "7 %S" (try (plist-put (list 'a 1) (p 'a) 2)))
    (message "8 %S" (try (plist-member (list 'a 1) (p 'a))))
    (message "9 %S" (try (let ((h (make-hash-table :test 'eq))) (puthash 'k 1 h) (gethash (p 'k) h))))
    (message "10 %S" (try (rassq (p 'v) '((1 . v)))))
    (message "11 %S" (try (delq (p 'a) (list 'a 'b 'a))))
    (message "12 %S" (try (remq (p 'a) (list 'a 'b 'a))))
    (message "13 %S" (try (alist-get (p 'a) '((a . 1)))))
    )))"##,
        r##"(progn
(defmacro try (&rest f) `(condition-case e (progn ,@f) (error (list 'ERR e))))
(defun p (s) (position-symbol s 3))
(put 'zcat 'zprop 42)
(let ((symbols-with-pos-enabled t))
  (with-temp-buffer
    (insert "abcd")
    (put-text-property 1 3 (p 'category) 'zcat)
    (message "1 %S" (try (list (text-properties-at 1) (get-text-property 1 'zprop) (get-char-property 1 'zprop) (get-text-property 1 'category)))))
  (with-temp-buffer
    (insert "abcd")
    (put-text-property 1 3 'category 'zcat)
    (message "2 %S" (try (list (text-properties-at 1) (get-text-property 1 'zprop)))))
  (message "3 %S" (try (get-text-property 0 'zprop (propertize "ab" (p 'category) 'zcat))))
  (message "4 %S" (try (get-text-property 0 'zprop (propertize "ab" 'category 'zcat)))))
(with-temp-buffer
    (insert "abcd")
    (put-text-property 1 3 'category 'zcat)
    (message "5 %S" (try (list (text-properties-at 1) (get-text-property 1 'zprop))))))"##,
        r##"(progn
(defun p (s) (position-symbol s 3))
(put 'zcat 'zprop 42)
(let ((symbols-with-pos-enabled t))
  (with-temp-buffer
    (insert "abcd")
    (put-text-property 1 3 'category (p 'zcat))
    (message "1 %S" (list (text-properties-at 1) (get-text-property 1 'zprop)))
    (put-text-property 1 3 (p 'category) 'zcat)
    (message "2 %S" (list (text-properties-at 1) (get-text-property 1 'zprop)))
    (put-text-property 1 3 'category 'zcat)
    (message "3 %S" (list (text-properties-at 1) (get-text-property 1 'zprop))))))"##,
        r##"(progn
(defmacro try (&rest f) `(condition-case e (progn ,@f) (error (list 'ERR e))))
(defun p (s) (position-symbol s 3))
(message "1 %S" (let ((symbols-with-pos-enabled t)) (try (eval `(let ((zx 1)) (defvar ,(p 'zx)) (let ((symbols-with-pos-enabled nil)) (let ((zx 2)) (list zx (boundp 'zx))))) t))))
(message "2 %S" (let ((symbols-with-pos-enabled t)) (try (eval `(let ((zy 1)) (defvar ,(p 'zy)) (let ((zy 2)) (list zy (boundp 'zy)))) t))))
(message "3 %S" (let ((symbols-with-pos-enabled t)) (try (eval `(let ((zw 1)) (defvar ,(p 'zw)) (lambda () zw)) t))))
(message "4 %S" (let ((symbols-with-pos-enabled t)) (try (eval `(let ((,(p 'zv) 1)) (lambda () zv)) t))))
(message "5 %S" (let ((symbols-with-pos-enabled t)) (try (eval `(function (lambda (,(p 'a1)) a1)) t))))
(message "6 %S" (let ((symbols-with-pos-enabled t)) (try (eval `(let ((,(p 'zu) 1)) (let ((symbols-with-pos-enabled nil)) zu)) t)))))"##,
        r##"(progn
(defmacro try (&rest f) `(condition-case e (progn ,@f) (error (list 'ERR e))))
(defun p (s) (position-symbol s 3))
(with-temp-buffer
  (insert "abcdef")
  (put 'zcat 'evaporate t)
  (let ((o (make-overlay 2 4)))
    (overlay-put o 'category 'zcat)
    (delete-region 2 4)
    (message "1 cat %S" (overlay-buffer o))))
(let ((symbols-with-pos-enabled t))
 (with-temp-buffer
  (insert "abcdef")
  (let ((o (make-overlay 2 4)))
    (overlay-put o (p 'evaporate) t)
    (delete-region 2 4)
    (message "2 swp %S" (overlay-buffer o)))
  (let ((o (make-overlay 2 4)))
    (overlay-put o (p 'evaporate) t)
    (move-overlay o 3 3)
    (message "3 swp-move %S" (overlay-buffer o)))
  (let ((o (make-overlay 1 3)))
    (overlay-put o (p 'face) 'bold)
    (message "4 %S" (list (overlay-get o 'face) (get-char-property 1 'face) (get-pos-property 1 'face))))
  (let ((o (make-overlay 1 3)))
    (overlay-put o (p 'priority) 5)
    (overlay-put o 'pz 1)
    (message "5 %S" (try (mapcar (lambda (x) (overlay-get x 'priority)) (overlays-at 1 t)))))
  (put-text-property 1 3 (p 'invisible) t)
  (message "6 %S" (list (invisible-p 1) (get-text-property 1 'invisible) (text-properties-at 1)))
  (put-text-property 1 3 'invisible nil)
  (message "7 %S" (text-properties-at 1))
  (add-text-properties 1 3 (list 'kk 1))
  (add-text-properties 1 3 (list (p 'kk) 2))
  (message "8 %S" (text-properties-at 1))
  (set-text-properties 1 3 (list (p 'q1) 1 'q1 2))
  (message "9 %S" (list (get-text-property 1 'q1) (text-properties-at 1)))
  (remove-text-properties 1 3 (list (p 'q1) nil))
  (message "10 %S" (text-properties-at 1))
  )))"##,
        r##"(progn
(defmacro try (&rest f) `(condition-case e (progn ,@f) (error (list 'ERR e))))
(defun p (s) (position-symbol s 3))
(defvar zf1 (let ((symbols-with-pos-enabled t)) (eval `(let ((,(p 'lv) 2)) (lambda () lv)) t)))
(message "1 %S" zf1)
(message "2 %S" (try (funcall zf1)))
(message "3 %S" (let ((symbols-with-pos-enabled t)) (try (funcall zf1))))
(defvar zf2 (eval `(let ((lv 5)) (lambda () ,(p 'lv))) t))
(message "4 %S" (try (funcall zf2)))
(message "5 %S" (let ((symbols-with-pos-enabled t)) (try (funcall zf2))))
(defvar zf3 (let ((symbols-with-pos-enabled t)) (eval `(let ((,(p 'lv) 2)) (lambda () (setq lv 9) lv)) t)))
(message "6 %S" (try (funcall zf3)))
(message "7 %S" zf3)
(message "8 %S" (let ((symbols-with-pos-enabled t)) (try (eval `(let ((,(p 'lv) 2) (lv 3)) lv) t))))
(message "9 %S" (let ((symbols-with-pos-enabled t)) (try (eval `(let* ((lv 2) (,(p 'lv) 4)) lv) t))))
(message "10 %S" (let ((symbols-with-pos-enabled t)) (try (eval `(funcall (lambda (,(p 'av)) av) 7) t))))
(message "11 %S" (let ((symbols-with-pos-enabled t)) (try (eval `(funcall (lambda (av) ,(p 'av)) 7) t))))
(message "12 %S" (let ((symbols-with-pos-enabled t)) (try (eval `(let ((x 1)) (defvar ,(p 'x)) (let ((x 2)) (list x (symbol-value 'x)))) t))))
(message "13 %S" (let ((symbols-with-pos-enabled t)) (try (eval `(condition-case ,(p 'err) (error "boo") (error err)) t))))
(message "14 %S" (let ((symbols-with-pos-enabled t)) (try (eval `(condition-case ,(p 'err) (error "boo") (error (lambda () err))) t)))))"##,
        r##"(progn
(defmacro try (&rest f) `(condition-case e (progn ,@f) (error (list 'ERR e))))
(defvar zs (position-symbol 'foo 3))
(message "0 %S" (list (eq zs 'foo) (local-variable-if-set-p 'symbols-with-pos-enabled) (special-variable-p 'symbols-with-pos-enabled)))
(with-temp-buffer
  (setq-local symbols-with-pos-enabled t)
  (message "1 %S" (list (eq zs 'foo) (memq zs '(foo)) (assq zs '((foo . 1))) (get-text-property 0 'foo (propertize "a" zs 1))))
  (let ((b (current-buffer)))
    (with-temp-buffer
      (message "2 %S" (list symbols-with-pos-enabled (eq zs 'foo) (get-text-property 0 'foo (propertize "a" zs 1)))))
    (message "3 %S" (list symbols-with-pos-enabled (eq zs 'foo)))))
(message "4 %S" (list symbols-with-pos-enabled (eq zs 'foo)))
(setq symbols-with-pos-enabled t)
(makunbound 'symbols-with-pos-enabled)
(message "5 %S" (list (boundp 'symbols-with-pos-enabled) (eq zs 'foo) (memq zs '(foo)) (get-text-property 0 'foo (propertize "a" zs 1))
   (with-temp-buffer (insert "ab") (put-text-property 1 2 zs 1) (get-text-property 1 'foo))))
(setq symbols-with-pos-enabled nil)
(message "6 %S" (list (eq zs 'foo)))
(let ((symbols-with-pos-enabled t))
  (message "7 %S" (list (eq zs 'foo) (catch 'a (let ((symbols-with-pos-enabled nil)) (throw 'a (eq zs 'foo)))) (eq zs 'foo)))
  (message "8 %S" (try (let ((symbols-with-pos-enabled nil)) (error "x")) ))
  (message "9 %S" (eq zs 'foo)))
(message "10 %S" (eq zs 'foo)))"##,
        r##"(progn
(defvar zs (position-symbol 'foo 3))
(defvar zr nil)
(let ((symbols-with-pos-enabled t))
  (let ((th (make-thread (lambda () (setq zr (list symbols-with-pos-enabled (eq zs 'foo) (get-text-property 0 'foo (propertize "a" zs 1))))))))
    (thread-join th)
    (message "1 %S %S" zr (list symbols-with-pos-enabled (eq zs 'foo)))))
(let ((th (make-thread (lambda () (let ((symbols-with-pos-enabled t)) (thread-yield) (setq zr (list (eq zs 'foo) (memq zs '(foo)))))))))
  (thread-yield)
  (message "2 %S" (list symbols-with-pos-enabled (eq zs 'foo) (memq zs '(foo)) (get-text-property 0 'foo (propertize "a" zs 1))))
  (thread-join th)
  (message "3 %S" zr)))"##,
    ] {
        assert_same(&corpus, &["--batch", "-Q", "--eval", program]);
    }
}

#[test]
fn argument_types_are_checked_and_answered_as_gnu_does() {
    // A sweep of every subr with ordinary values of each type in each
    // argument found these: abs, max, min and the one-argument logical
    // operations return their argument itself (data.c and floatfns.c),
    // compose-string-internal takes nil ends (validate_subarray),
    // list_length names a dotted list's tail, signal_error splices a
    // proper-list datum (font-shape-gstring, md5, secure-hash,
    // internal-set-font-selection-order), goto-char refuses a bignum,
    // indirect-function reads t's function cell, posn-at-x-y is
    // CHECK_FIXNAT's, and window margins and new pixel sizes are
    // check_integer_range's.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r##"(progn
(dolist (f (list
  (lambda () (let ((x 1.5) (b (1+ most-positive-fixnum)) (n -2.5)) (list (eq (abs x) x) (eq (abs b) b) (eq (max b) b) (eq (min b) b) (eq (logand b) b) (eq (logior b) b) (eq (logxor b) b) (eq (abs n) n) (eq (max x) x) (eq (+ b) b) (eq (* b) b) (eq (- b) b))))
  (lambda () (let ((s (copy-sequence "abc"))) (list (compose-string-internal s nil nil) (get-text-property 0 'composition s) (condition-case e (compose-string-internal s 1 0) (error e)))))
  (lambda () (list (condition-case e (composition-sort-rules '(1 . 2)) (error e)) (condition-case e (composition-sort-rules '(1 2)) (error e)) (composition-sort-rules '(x)) (condition-case e (composition-sort-rules 5) (error e))))
  (lambda () (list (condition-case e (font-shape-gstring '(1 2) nil) (error e)) (condition-case e (md5 '(1 2)) (error e)) (condition-case e (secure-hash 'md5 '(1 2)) (error e)) (condition-case e (md5 '(1 . 2)) (error e))))
  (lambda () (with-temp-buffer (insert "abc") (list (condition-case e (goto-char (1+ most-positive-fixnum)) (error e)) (condition-case e (goto-char 1.5) (error e)) (point))))
  (lambda () (list (indirect-function t) (indirect-function nil) (indirect-function 'car) (indirect-function 5) (indirect-function "x")))
  (lambda () (list (condition-case e (internal-set-font-selection-order '(1 . 2)) (error e)) (condition-case e (internal-set-font-selection-order '(1 2)) (error e)) (condition-case e (internal-set-font-selection-order 5) (error e))))
  (lambda () (list (condition-case e (posn-at-x-y 0 nil) (error e)) (condition-case e (posn-at-x-y -1 nil) (error e)) (condition-case e (posn-at-x-y -2 0) (error e)) (condition-case e (posn-at-x-y nil 0) (error e))))
  (lambda () (list (condition-case e (set-window-margins nil (1+ most-positive-fixnum)) (error e)) (condition-case e (set-window-margins nil 0 (1+ most-positive-fixnum)) (error e)) (condition-case e (set-window-margins nil -1) (error e)) (condition-case e (set-window-margins nil 1.5) (error e))))
  (lambda () (list (condition-case e (set-window-new-pixel nil -1) (error e)) (condition-case e (set-window-new-pixel nil (1+ most-positive-fixnum)) (error e)) (condition-case e (set-window-new-pixel nil 1.5) (error e)) (condition-case e (set-window-new-pixel nil -5 t) (error e))))
  ))
  (prin1 (condition-case e (funcall f) (error (list 'ERR e)))) (terpri)))"##,
        ],
    );
}

#[test]
fn dead_objects_reach_primitives_as_gnu_decodes_them() {
    // A killed buffer, a deleted window, a deleted process, a deleted
    // overlay and a marker pointing nowhere in each argument of the
    // primitives a sweep of every subr found answering otherwise: a killed
    // buffer's freed text is empty (buffer-hash, buffer-line-statistics,
    // object-intervals, current-bidi-paragraph-direction), a window
    // argument is decode_live_window'd (font-at, the bidi scan), each
    // argument is checked in GNU's order (posn-at-point's fix_position,
    // fringe-bitmaps-at-pos's window point, internal-complete-buffer's
    // flag, the default sentinel's Finsert), get-process returns only a
    // process itself, and make-hash-table signals with signal_error.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r##"(progn
(setq print-gensym nil print-circle nil print-length 20 print-level 4)
(defvar dd-deny-prefixes '("kill-emacs" "suspend" "recursive-edit" "top-level" "exit-" "abort-" "read" "sleep-" "sit-for" "accept-process" "sqlite" "module" "native-" "comp-" "dump" "pdumper" "unexec" "daemon" "x-" "w32" "ns-" "dbus" "gnutls" "debug" "prin" "terpri" "write-char" "external-debugging" "open-dribble" "open-termscript" "redirect-debugging" "send-string-to-terminal" "set-input-mode" "set-output-flow" "set-quit-char" "resume-tty" "suspend-tty" "delete-terminal" "delete-frame" "make-frame" "make-terminal-frame" "garbage-collect" "memory-" "malloc" "call-process" "make-process" "make-network" "make-serial" "make-pipe" "start-kbd-macro" "end-kbd-macro" "execute-kbd-macro" "call-last-kbd" "command-execute" "call-interactively" "execute-extended" "yes-or-no" "y-or-n" "play-sound" "ding" "beep" "unix-sync" "thread" "mutex" "condition-" "make-thread" "treesit" "invocation-" "list-system" "current-cpu" "get-internal-run" "current-time" "float-time" "time-" "random" "emacs-pid" "getenv" "setenv" "system-" "user-" "group-" "file-" "directory-" "delete-file" "rename-file" "copy-file" "make-directory" "delete-directory" "set-file" "add-name-to-file" "make-symbolic-link" "insert-file" "write-region" "find-file" "do-auto-save" "lock-" "unlock-" "inotify" "sxhash" "re--describe" "process-attributes" "num-processors" "network-" "current-idle" "recent-keys" "lossage" "discard-input" "input-pending" "track-mouse" "profiler" "Snarf" "init-image" "lookup-image" "load" "require" "autoload" "eval" "funcall" "apply" "throw" "signal" "mapatoms" "maphash" "map-" "mapc" "mapcar" "mapconcat" "mapcan" "fset" "defalias" "setplist" "makunbound" "fmakunbound" "set-default" "setq-default" "defvar" "defconst" "internal--define-uninitialized" "set-standard" "set-locale" "set-time-zone" "define-" "unintern" "obarray-clear" "set-keyboard-coding" "set-terminal-coding" "set-safe-terminal" "set-binary-mode" "kill-buffer" "kill-all-local" "set-buffer-multibyte" "set-window-configuration" "redisplay" "force-mode-line" "message" "minibuffer-message" "set-message" "display-warning" "run-hook" "run-mode-hooks" "byte-code" "make-byte-code" "make-closure" "internal-make-closure" "make-interpreted-closure" "frame-" "set-frame-" "select-frame" "handle-switch-frame" "make-char-table" "bury-buffer" "set-window-buffer" "switch-to" "pop-to" "set-buffer" "other-window" "delete-other-windows" "delete-window" "split-window" "select-window" "set-frame-selected-window" "set-minibuffer-window" "next-frame" "previous-frame" "process-send" "signal-process" "kill-process" "interrupt-process" "stop-process" "continue-process" "quit-process" "set-process" "store-kbd" "set-match-data" "store-match-data"))
(defun dd-denied (name) (seq-some (lambda (p) (string-prefix-p p name)) dd-deny-prefixes))
(defvar dd-dead nil)
(defun dd-make-dead ()
  (list (cons 'DEAD-BUFFER (let ((b (generate-new-buffer "dd"))) (kill-buffer b) b))
        (cons 'DEAD-WINDOW (let ((w (split-window))) (delete-window w) w))
        (cons 'DEAD-PROCESS (let ((p (make-pipe-process :name "dd" :noquery t))) (delete-process p) p))
        (cons 'DEAD-OVERLAY (with-temp-buffer (let ((o (make-overlay 1 1))) (delete-overlay o) o)))
        (cons 'DETACHED-MARKER (make-marker))))
(defun dd-walk (v depth)
  (let ((d (rassq v dd-dead)))
    (cond (d (car d))
          ((> depth 4) '...)
          ((consp v) (let ((out nil) (n 0))
                       (while (and (consp v) (< n 20)) (push (dd-walk (car v) (1+ depth)) out) (setq v (cdr v) n (1+ n)))
                       (let ((tail (if (consp v) '... (dd-walk v (1+ depth))))) (nconc (nreverse out) tail))))
          ((and (vectorp v) (< (length v) 20)) (apply #'vector (mapcar (lambda (x) (dd-walk x (1+ depth))) v)))
          (t v))))
(defun dd-show (v)
  (condition-case nil
      (let ((s (prin1-to-string (dd-walk v 0))))
        (if (string-match-p "#<\\|0x[0-9a-f]\\{6,\\}" s) (list 'opaque (type-of v)) s))
    (error 'unprintable)))
(setq dd-dead (dd-make-dead))
(let ((subrs '(bidi-find-overridden-directionality buffer-hash buffer-line-statistics current-bidi-paragraph-direction font-at fringe-bitmaps-at-pos get-process internal-complete-buffer internal-default-process-sentinel make-hash-table object-intervals posn-at-point)))
  (dolist (f subrs)
    (when (fboundp f)
      (let* ((arity (func-arity f)) (min (car arity)) (max (if (numberp (cdr arity)) (cdr arity) 3))
             (npos (min 3 max)))
        (dotimes (pos npos)
          (dolist (dead dd-dead)
            (let ((args (make-list (max min (1+ pos)) nil)))
              (setcar (nthcdr pos args) (cdr dead))
              (message "%s" (format "%s %d %s => %s" f pos (car dead)
                                    (dd-show (condition-case e (with-temp-buffer (apply f args)) (error (list 'ERR e))))))
              (ignore-errors (delete-other-windows))))))))))"##,
        ],
    );
}

#[test]
fn window_kinds_and_links_are_read_from_the_window_record() {
    // window.c tells a window's kind from w->contents and its flags, so
    // no symbol names a kind; Fwindow_parent and its siblings read the
    // link of decode_valid_window, and Fwindow_top_child and
    // Fwindow_left_child the first child of a combination of their
    // direction.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
(dolist (f (list
  (lambda () (let ((w (split-window))) (list (mapcar (lambda (n) (and (intern-soft n) t)) '("internal-vertical" "internal-horizontal" "deleted" "leaf")) (windowp w) (window-valid-p w) (window-live-p w) (eq (window-parent w) (window-parent)) (window-live-p (window-parent w)) (window-valid-p (window-parent w)) (eq (window-top-child (window-parent w)) (selected-window)) (window-left-child (window-parent w)) (eq (window-next-sibling) w) (eq (window-prev-sibling w) (selected-window)))))
  (lambda () (let ((w (split-window nil nil t))) (list (mapcar (lambda (n) (and (intern-soft n) t)) '("internal-vertical" "internal-horizontal")) (window-top-child (window-parent w)) (eq (window-left-child (window-parent w)) (selected-window)) (window-combination-limit (window-parent w)) (condition-case e (window-combination-limit w) (error e)) (progn (delete-window w) (list (windowp w) (window-valid-p w) (window-live-p w))))))
  (lambda () (let ((w (split-window))) (delete-window w) (list (condition-case e (window-parent w) (error (car e))) (condition-case e (window-next-sibling w) (error (car e))) (condition-case e (window-prev-sibling w) (error (car e))) (condition-case e (window-top-child w) (error e)) (condition-case e (window-left-child w) (error e)) (condition-case e (window-frame w) (error (car e))) (condition-case e (window-buffer w) (error (car e))))))
  (lambda () (list (window-minibuffer-p (minibuffer-window)) (window-minibuffer-p) (eq (window-next-sibling (frame-root-window)) (minibuffer-window)) (eq (window-prev-sibling (minibuffer-window)) (frame-root-window)) (window-parent (minibuffer-window)) (window-live-p (minibuffer-window)) (window-valid-p (minibuffer-window)) (window-top-child) (window-left-child) (window-parent)))
  (lambda () (list (condition-case e (window-parent 1) (error e)) (condition-case e (window-top-child 'x) (error e)) (windowp 1) (window-valid-p nil) (window-live-p nil) (condition-case e (windowp) (error (car e))) (condition-case e (window-live-p 1 2) (error (car e)))))
  (lambda () (let* ((w (split-window)) (c (current-window-configuration))) (delete-window w) (set-window-configuration c) (list (window-live-p w) (length (window-list)) (mapcar (lambda (n) (and (intern-soft n) t)) '("internal-vertical" "deleted")))))
  (lambda () (let ((w (split-window))) (prog1 (list (frame-windows-min-size nil nil nil t) (frame-windows-min-size nil t nil t) (window-min-size w) (window-min-size (window-parent w))) (delete-window w))))
  (lambda () (let ((w1 (split-window)) (w2 (split-window nil nil t))) (prog1 (list (length (window-list)) (eq (window-parent w1) (window-parent (window-parent w2))) (window-live-p (window-parent w2)) (window-combination-p (window-parent w2) t) (window-combination-p (window-parent w1))) (delete-other-windows))))
  ))
  (prin1 (condition-case e (unwind-protect (funcall f) (delete-other-windows)) (error (list (quote ERR) e)))) (terpri)))"#,
        ],
    );
}

#[test]
fn window_primitives_decode_their_window_as_window_c_does() {
    // Each window primitive decodes its WINDOW argument as window.c's
    // does (decode_any_window, decode_valid_window or decode_live_window,
    // CHECK_VALID_WINDOW where nil is no window), and checks its other
    // arguments in GNU's order.  `window-system-for-display' is left out:
    // GNU's dump preloads term/x-win, which this build has no use for.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(let (names (dead (let ((w (split-window))) (delete-window w) w)))
  (mapatoms (lambda (s) (when (and (subrp (symbol-function s)) (not (special-form-p s))
                                    (string-match-p "\\`\\(set-\\)?window-\\|\\`\\(coordinates-in\\|pos-visible-in\\|delete\\|select\\|next\\|previous\\|scroll\\|recenter\\|move-to\\|other\\)-window" (symbol-name s))
                                    (not (memq s '(delete-window delete-other-windows window-configuration-to-register window-system-for-display))))
                           (push s names))))
  (setq names (sort names (lambda (a b) (string< (symbol-name a) (symbol-name b)))))
  (dolist (s names)
    (let* ((arity (func-arity s)) (min (car arity)))
      (dolist (arg (list dead 1 'x))
        (princ (format "%s %S: %S\n" s (if (eq arg dead) 'dead arg)
                       (condition-case e (progn (apply s arg (make-list (max 0 (1- min)) nil)) 'ok)
                         (error (list (car e) (if (eq (nth 1 e) dead) 'DEAD (nth 1 e)) (if (eq (nth 2 e) dead) 'DEAD (nth 2 e))))))))
      (ignore-errors (delete-other-windows)))))"#,
        ],
    );
}

#[test]
fn frame_and_window_list_primitives_decode_windows_as_gnu_does() {
    // The frame, list, configuration, position and internal window
    // primitives with a deleted window, a fixnum and a symbol in each
    // window argument.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(let ((dead (let ((w (split-window))) (delete-window w) w))
      (cases '((frame-root-window 0) (frame-first-window 0) (frame-selected-window 0) (frame-old-selected-window 0)
               (set-frame-selected-window 1) (set-frame-selected-window 0) (minibuffer-window 0) (set-minibuffer-window 0)
               (window-list 0) (window-list 2) (window-list-1 0) (get-buffer-window 1) (split-window-internal 0)
               (delete-window-internal 0) (delete-other-windows-internal 0) (resize-mini-window-internal 0)
               (run-window-scroll-functions 0) (window-text-pixel-size 0) (set-window-buffer 0) (force-window-update 0)
               (current-window-configuration 0) (window-configuration-frame 0) (posn-at-point 1) (posn-at-x-y 2)
               (window-lines-pixel-dimensions 0) (window-point 0) (set-window-point 0) (set-window-start 0)
               (pos-visible-in-window-p 1) (window-line-height 1) (window-end 0) (window-hscroll 0) (window-margins 0)
               (set-window-margins 0) (window-text-width 0) (window-text-height 0) (window-old-point 0) (window-use-time 0)
               (window-bump-use-time 0) (select-window 0) (next-window 0) (previous-window 0) (window-buffer 0)
               (window-old-buffer 0) (window-parameter 0) (set-window-parameter 0) (window-frame 0)
               (window-old-pixel-width 0) (window-mode-line-height 0) (window-header-line-height 0) (window-tab-line-height 0)
               (window-vscroll 0) (scroll-other-window 0) (recenter 0) (window-swap-states 0) (frame-windows-min-size 0)
               (window-system 0) (window-inside-edges 0) (window-edges 0) (window-body-pixel-edges 0) (window-absolute-pixel-edges 0)
               (window-font-width 0) (window-font-height 0) (window-max-chars-per-line 0) (window-screen-lines 0))))
  (dolist (c cases)
    (let* ((s (car c)) (pos (cadr c)) (arity (func-arity s)) (n (max (1+ pos) (car arity))))
      (dolist (arg (list dead 1 'x))
        (let ((args (make-list n nil)))
          (setcar (nthcdr pos args) arg)
          (princ (format "%s@%d %S: %S\n" s pos (if (eq arg dead) 'dead arg)
                         (condition-case e (progn (apply s args) 'ok)
                           (error (list (car e) (if (eq (nth 1 e) dead) 'DEAD (nth 1 e)) (if (eq (nth 2 e) dead) 'DEAD (nth 2 e)))))))))
      (ignore-errors (delete-other-windows)))))"#,
        ],
    );
}

#[test]
fn killed_buffers_stay_buffer_objects_in_windows_and_variables() {
    // buffer.c's Fkill_buffer clears the buffer's name and leaves the object,
    // which every reference keeps: BUFFER_LIVE_P reads the name, a window
    // shows a buffer object (window.c's w->contents), and a killed buffer is
    // never live again whatever later buffers are made.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
(dolist (f (list
  (lambda () (let ((b (get-buffer-create "x1"))) (list (buffer-live-p b) (progn (kill-buffer b) (buffer-live-p b)) (buffer-name b) (condition-case e (set-buffer b) (error e)))))
  (lambda () (let ((b (get-buffer-create "x2"))) (set-window-buffer nil b) (list (eq (window-buffer) b) (progn (kill-buffer b) (eq (window-buffer) b)) (buffer-live-p (window-buffer)))))
  (lambda () (let ((b (get-buffer-create "x3")) (c (current-window-configuration))) (set-window-buffer nil b) (kill-buffer b) (set-window-configuration c) (buffer-name (window-buffer))))
  (lambda () (let ((b (get-buffer-create "x4"))) (save-window-excursion (switch-to-buffer b) (list (eq (window-buffer) b) (eq (current-buffer) b))) ))
  (lambda () (with-temp-buffer (list (buffer-live-p (current-buffer)) (eq (window-buffer) (current-buffer)))))
  (lambda () (let ((b (generate-new-buffer "x5"))) (prog1 (list (get-buffer-window b) (progn (display-buffer b) (window-live-p (get-buffer-window b)))) (kill-buffer b))))
  (lambda () (let ((b (get-buffer-create "x6"))) (set-window-buffer nil b) (list (window-point) (window-start) (with-current-buffer b (insert "hello")) (progn (set-window-point nil 3) (window-point)))))
  (lambda () (buffer-live-p (window-buffer (minibuffer-window))))
  (lambda () (let ((b (get-buffer-create "x7"))) (kill-buffer b) (list (buffer-live-p b) (condition-case e (with-current-buffer b 1) (error e)) (condition-case e (set-window-buffer nil b) (error e)))))
  (lambda () (mapcar (lambda (w) (buffer-name (window-buffer w))) (window-list)))
  (lambda () (let ((b (get-buffer-create "x8"))) (switch-to-buffer b) (kill-buffer b) (list (buffer-name (current-buffer)) (buffer-name (window-buffer)))))
  ))
  (prin1 (condition-case e (funcall f) (error (list 'ERR e)))) (terpri)))"#,
        ],
    );
}

#[test]
fn fringe_bitmaps_are_found_by_their_fringe_property() {
    // fringe.c's lookup_fringe_bitmap reads the number BITMAP's `fringe'
    // property holds, so a symbol given another's number is that bitmap;
    // Fset_fontset_font takes a script or charset symbol by EQ and names
    // it by SYMBOL_NAME.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
(dolist (f (list
  (lambda () (list (define-fringe-bitmap 'zz [1 2 3]) (get 'zz 'fringe) (fringe-bitmap-p 'zz) (get 'left-arrow 'fringe) (fringe-bitmap-p 'left-arrow)))
  (lambda () (let ((u (make-symbol "zz"))) (list (define-fringe-bitmap u [1]) (get u 'fringe) (fringe-bitmap-p u) (progn (destroy-fringe-bitmap u) (get u 'fringe)))))
  (lambda () (progn (put 'yy 'fringe 3) (condition-case e (set-fringe-bitmap-face 'yy 'bold) (error e))))
  (lambda () (condition-case e (destroy-fringe-bitmap 'nosuch) (error e)))
  (lambda () (list (progn (define-fringe-bitmap 'a1 [1]) (get 'a1 'fringe)) (progn (destroy-fringe-bitmap 'a1) (define-fringe-bitmap 'a2 [1]) (get 'a2 'fringe)) (progn (put 'a3 'fringe (get 'a2 'fringe)) (define-fringe-bitmap 'a3 [2]) (list (get 'a3 'fringe) (and (memq 'a3 fringe-bitmaps) t)))))
  (lambda () (condition-case e (define-fringe-bitmap 'a4 [1] nil nil 'center) (error e)))
  (lambda () (condition-case e (define-fringe-bitmap 'a5 [1] nil nil (make-symbol "top")) (error e)))
  (lambda () (progn (put 'a6 'fringe 2) (destroy-fringe-bitmap 'a6) (get 'a6 'fringe)))
  (lambda () (condition-case e (define-fringe-bitmap "x" [1]) (error e)))
  (lambda () (condition-case e (set-fringe-bitmap-face 'nosuch2) (error e)))
  (lambda () (progn (put 'a7 'fringe 999) (list (condition-case e (set-fringe-bitmap-face 'a7) (error e)) (define-fringe-bitmap 'a7 [1]) (get 'a7 'fringe))))
  (lambda () (condition-case e (set-fontset-font t (make-symbol "latin") "Mono") (error e)))
  (lambda () (condition-case e (set-fontset-font t (make-symbol "nosuchscript") "Mono") (error e)))
  ))
  (prin1 (condition-case e (funcall f) (error (list 'ERR e)))) (terpri)))"#,
        ],
    );
}

#[test]
fn cached_symbol_keys_name_symbols_gnu_has_interned() {
    // Every cached key's symbol is interned when an editor is constructed,
    // as lisp.h's lispsym are: each must name a symbol GNU's own startup
    // has interned, or `intern-soft' would find one GNU does not have.
    let source = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lisp/types/symbols.rs"),
    )
    .unwrap();
    let start = source.find("const KEY_NAMES: &[&str] = &[").unwrap();
    let end = start + source[start..].find("];").unwrap();
    let names: Vec<&str> = source[start..end].split('"').skip(1).step_by(2).collect();
    assert!(names.len() > 300, "{} key names", names.len());
    let program = format!(
        "(dolist (n '({})) (unless (intern-soft n) (princ (format \"%s \" n))))",
        names
            .iter()
            .map(|name| format!("\"{name}\""))
            .collect::<Vec<_>>()
            .join(" ")
    );
    let corpus = Corpus::new();
    assert_same(&corpus, &["--batch", "-Q", "--eval", &program]);
}

#[test]
fn frame_parameters_and_frame_state_follow_frame_c_and_dispnew_c() {
    // frame.c:Fframe_parameter reads a stored parameter without building
    // Fframe_parameters' alist, and matches by EQ; dispnew.c's
    // Fframe_or_buffer_changed_p records every frame's name and sizes its
    // vector by all live buffers; its variable is any symbol.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
(defvar fbv nil)
(prin1 (list (frame-or-buffer-changed-p 'fbv) (length fbv) (aref fbv 1) (eq (aref fbv 0) (selected-frame)) (frame-or-buffer-changed-p 'fbv)
             (progn (get-buffer-create "x") (frame-or-buffer-changed-p 'fbv)) (length fbv)
             (progn (get-buffer-create " hidden") (frame-or-buffer-changed-p 'fbv)) (length fbv)
             (progn (with-current-buffer "x" (set-buffer-modified-p t)) (frame-or-buffer-changed-p 'fbv))
             (frame-or-buffer-changed-p 'fbv)
             (progn (with-current-buffer "x" (setq buffer-read-only t)) (frame-or-buffer-changed-p 'fbv))
             (frame-or-buffer-changed-p 'fbv)
             (let ((s (make-symbol "fbw"))) (set s 7) (list (frame-or-buffer-changed-p s) (length (symbol-value s)) (frame-or-buffer-changed-p s)))
             (condition-case e (frame-or-buffer-changed-p 'unbound-fbv) (error e))
             (condition-case e (frame-or-buffer-changed-p 3) (error e))
             (let ((old fbv)) (setq fbv (make-vector 200 'lambda)) (list (frame-or-buffer-changed-p 'fbv) (eq old fbv) (length fbv)))))
(terpri)
(prin1 (list (frame-parameter nil (make-symbol "name"))
             (progn (modify-frame-parameters nil (list (cons (make-symbol "name") "zz"))) (frame-parameter nil 'name))
             (frame-parameter nil 'height) (frame-parameter nil 'width) (frame-parameter nil 'buffer-list)
             (frame-parameter nil 'foreground-color) (frame-parameter nil 'background-color)
             (frame-parameter nil 'font) (frame-parameter nil 'modeline) (frame-parameter nil 'unsplittable)
             (frame-parameter nil 'menu-bar-lines) (frame-parameter nil 'tab-bar-lines) (frame-parameter nil 'nosuch)
             (progn (modify-frame-parameters nil '((nosuch . 3))) (frame-parameter nil 'nosuch))
             (frame-parameter nil 'line-spacing) (frame-parameter nil 'vertical-scroll-bars)
             (let ((u (make-symbol "height"))) (modify-frame-parameters nil (list (cons u 9))) (list (frame-parameter nil u) (frame-parameter nil 'height)))
             (let ((u (make-symbol "foreground-color"))) (modify-frame-parameters nil (list (cons u "red"))) (list (frame-parameter nil u) (frame-parameter nil 'foreground-color)))
             (progn (modify-frame-parameters nil '((buffer-predicate . ignore))) (frame-parameter nil 'buffer-predicate))
             (progn (modify-frame-parameters nil (list (cons (make-symbol "buffer-list") (list (current-buffer))))) (frame-parameter nil 'buffer-list))
             (progn (modify-frame-parameters nil '((buffer-predicate . nil))) (frame-parameter nil 'buffer-predicate))
             (frame-parameter nil 'display-type) (frame-parameter nil 'background-mode)
             (condition-case e (frame-parameter nil "name") (error e))))
(terpri))"#,
        ],
    );
}

#[test]
fn generated_uninterned_symbol_programs_run_as_gnu_runs_them() {
    // 150 generated programs that give a fresh uninterned symbol one of 26
    // roles (function, variable, local variable, property owner, hash
    // test, char-table purpose, error, hook, macro, key, text and overlay
    // property, catch tag, command, watched variable, compiled callee)
    // and print what each role answers.
    let corpus = Corpus::new();
    assert_same(
        &corpus,
        &[
            "--batch",
            "-Q",
            "--eval",
            r#"(progn
(random "uninterned-fuzz")
(defun un-try (f) (condition-case e (funcall f) (error (list 'err (car e) (and (cdr e) (if (symbolp (cadr e)) (symbol-name (cadr e)) (cadr e)))))))
(defvar un-log nil)
(defun un-case (s n)
  (pcase n
    (0 (fset s (lambda (&rest a) (cons 'called a))) (list (funcall s 1) (apply s '(2 3)) (func-arity s) (functionp s) (fboundp s) (commandp s)))
    (1 (set s 41) (list (symbol-value s) (boundp s) (default-value s) (progn (makunbound s) (boundp s))))
    (2 (put s 'zk 7) (list (get s 'zk) (symbol-plist s) (function-get s 'zk) (progn (function-put s 'zj 8) (get s 'zj))))
    (3 (define-hash-table-test s 'equal 'sxhash-equal) (let ((h (make-hash-table :test s))) (puthash "a" 1 h) (list (gethash "a" h) (hash-table-count h) (eq (hash-table-test h) s))))
    (4 (put s 'char-table-extra-slots 1) (let ((ct (make-char-table s 0))) (set-char-table-extra-slot ct 0 'x) (list (char-table-extra-slot ct 0) (eq (char-table-subtype ct) s) (aref ct ?a))))
    (5 (define-error s "Zz failure") (list (condition-case e (signal s '(1)) (error (list (eq (car e) s) (cdr e) (error-message-string e)))) (get s 'error-message)))
    (6 (set s nil) (add-hook s (lambda () (push 'h1 un-log))) (setq un-log nil) (run-hooks s) (list un-log (symbol-value s)))
    (7 (defalias s (cons 'macro (lambda (x) (list 'quote (list x x))))) (list (macroexpand (list s 1)) (eval (list s 2) t) (macrop s)))
    (8 (let ((m (make-sparse-keymap))) (define-key m (vector s) 'ignore) (list (lookup-key m (vector s)) (where-is-internal 'ignore (list m)))))
    (9 (with-temp-buffer (insert "abc") (put-text-property 1 3 s 'v) (list (get-text-property 1 s) (text-properties-at 1) (next-single-property-change 1 s))))
    (10 (with-temp-buffer (insert "abc") (let ((o (make-overlay 1 3))) (overlay-put o s 9) (list (overlay-get o s) (get-char-property 1 s)))))
    (11 (list (plist-get (list s 1) s) (assq s (list (cons s 2))) (memq s (list 'a s)) (intern-soft (symbol-name s)) (eq s (intern (symbol-name s)))))
    (12 (fset s (lambda (x) (interactive "p") (* x 2))) (list (commandp s) (interactive-form s) (call-interactively s)))
    (13 (with-temp-buffer (set (make-local-variable s) 5) (list (local-variable-p s) (buffer-local-value s (current-buffer)) (symbol-value s) (default-boundp s))))
    (14 (eval (list 'defvar s 3)) (list (special-variable-p s) (eval (list 'let (list (list s 4)) (list 'symbol-value (list 'quote s))) t)))
    (15 (fset s 'car) (list (funcall (byte-compile (list 'lambda () (list s ''(1 2))))) (func-arity s) (indirect-function s)))
    (16 (put s 'variable-documentation "Doc of zz.") (list (documentation-property s 'variable-documentation)))
    (17 (let ((log nil)) (set s 1) (add-variable-watcher s (lambda (&rest a) (push (nth 2 a) log))) (set s 2) (list log (get-variable-watchers s))))
    (18 (list (prin1-to-string s) (let ((print-gensym t)) (prin1-to-string s)) (format "%S" (list s)) (symbol-name s)))
    (19 (defalias s 'list) (list (funcall s 1 2) (symbol-function s) (indirect-function s) (eq (symbol-function s) 'list)))
    (20 (set s 10) (list (eval s t) (eval (list 'setq s 11) t) (symbol-value s) (eval (list '1+ s) t)))
    (21 (fset s (lambda () 'z)) (list (funcall (byte-compile (list 'lambda () (list s)))) (eval (list s) t) (mapcar s nil)))
    (22 (with-temp-buffer (make-local-variable s) (list (local-variable-p s) (boundp s) (local-variable-if-set-p s))))
    (23 (list (catch s (throw s 'thrown)) (condition-case e (throw s 1) (no-catch (list (eq (cadr e) s))))))
    (24 (let ((tbl (obarray-make))) (list (intern-soft (symbol-name s) tbl) (eq (intern (symbol-name s) tbl) s))))
    (25 (set s 1) (list (eval (list 'let* (list (list s 2)) s) t) (eval (list 'let (list (list s 3)) (list 'boundp (list 'quote s))) nil)))))
(dotimes (i 150)
  (let* ((s (make-symbol (format "un-%d" (random 5))))
         (n (random 26)))
    (prin1 (list i n (un-try (lambda () (un-case s n))))) (terpri)))
)"#,
        ],
    );
}

#[test]
fn kill_buffer_and_the_selected_windows_buffer_as_gnu_answers() {
    // window_loop's REPLACE_BUFFER_IN_WINDOWS_SAFELY when
    // `replace-buffer-in-windows' is not fbound: other_buffer_safely's
    // buffer, undedicated, with its start, point, hscroll and margins; the
    // readers of the selected window's buffer with a live window (the
    // line-number width's variable, the scroll commands' excursion) and
    // with a deleted one, GNU's answer where its C has one.
    let corpus = Corpus::new();
    for program in [
        r##"(progn
;; kill-buffer when replace-buffer-in-windows is not fboundp: window_loop's
;; REPLACE_BUFFER_IN_WINDOWS_SAFELY replaces the dying buffer in windows.
(defun show (tag form)
  (message "%s %S" tag (condition-case err (funcall form) (error (list 'ERR err)))))

(fmakunbound 'replace-buffer-in-windows)

;; 1. the selected window shows a dedicated buffer that is not current;
;; the replacement is other_buffer_safely's, not the current buffer.
(show "k1"
 (lambda ()
   (let ((b (get-buffer-create "kb1"))
         (h (get-buffer-create " hidden")))
     (with-current-buffer b (insert "hello world\nsecond line\n") (goto-char 5))
     (set-window-buffer nil b)
     (set-window-dedicated-p nil t)
     (set-window-hscroll nil 3)
     (set-buffer h)
     (let ((r (kill-buffer b)))
       (list r (buffer-name (window-buffer)) (window-dedicated-p)
             (window-start) (window-point) (window-hscroll)
             (markerp (window-start)) (buffer-name (current-buffer))
             (buffer-live-p b))))))

;; 2. the dying buffer is current and shown: Fother_buffer becomes
;; current first, then the window shows other_buffer_safely's buffer.
(show "k2"
 (lambda ()
   (let ((b (get-buffer-create "kb2")))
     (with-current-buffer "*scratch*" (erase-buffer) (insert "abc\ndef\n") (goto-char 3))
     (set-window-buffer nil b)
     (set-buffer b)
     (let ((r (kill-buffer b)))
       (list r (buffer-name (window-buffer)) (buffer-name (current-buffer))
             (window-point) (window-start))))))

;; 3. the minibuffer window shows the dying buffer.
(show "k3"
 (lambda ()
   (let ((b (get-buffer-create "kb3"))
         (old (window-buffer (minibuffer-window))))
     (set-window-buffer (minibuffer-window) b)
     (let ((r (kill-buffer b)))
       (prog1 (list r (buffer-name (window-buffer (minibuffer-window))))
         (when (buffer-live-p old) (set-window-buffer (minibuffer-window) old)))))))

;; 4. the replacement window's start is the new buffer's last window start
;; and `inhibit-quit' is restored after the kill.
(show "k4"
 (lambda ()
   (let ((b (get-buffer-create "kb4")) (seen nil))
     (set-window-buffer nil b)
     (set-buffer "*scratch*")
     (list (kill-buffer b) inhibit-quit (buffer-name (window-buffer))
           (window-start) (window-margins)))))

;; 5. a window showing a buffer whose margins are set gets the new
;; buffer's margins.
(show "k5"
 (lambda ()
   (let ((b (get-buffer-create "kb5")))
     (with-current-buffer "*scratch*" (setq left-margin-width 2))
     (set-window-buffer nil b)
     (set-window-margins nil 5 1)
     (set-buffer "*Messages*")
     (prog1 (list (kill-buffer b) (buffer-name (window-buffer)) (window-margins))
       (with-current-buffer "*scratch*" (setq left-margin-width 0))))))

;; 6. buffer-list-update-hook and kill-buffer-hook see the window state.
(show "k6"
 (lambda ()
   (let* ((b (get-buffer-create "kb6")) (log nil))
     (set-window-buffer nil b)
     (with-current-buffer b
       (add-hook 'kill-buffer-hook
                 (lambda () (push (list 'kbh (buffer-name (window-buffer))) log)) nil t))
     (set-buffer "*scratch*")
     (kill-buffer b)
     (list (reverse log) (buffer-name (window-buffer)))))))"##,
        r##"(progn
;; Selected-window buffer readers with a live selected window.
(defun show (tag form)
  (message "%s %S" tag (condition-case err (funcall form) (error (list 'ERR err)))))
(defun fill-buf (name n)
  (with-current-buffer (get-buffer-create name)
    (erase-buffer)
    (dotimes (i n) (insert (format "line %d\n" i)))
    (goto-char (point-min))
    (current-buffer)))
;; line-number-display-width reads display-line-numbers in the current buffer.
(show "ln1" (lambda ()
  (let ((a (fill-buf "lnA" 50)) (b (fill-buf "lnB" 5)))
    (set-window-buffer nil a)
    (with-current-buffer a (setq display-line-numbers t))
    (with-current-buffer b (setq display-line-numbers nil))
    (list (with-current-buffer b (line-number-display-width))
          (with-current-buffer a (line-number-display-width))
          (with-current-buffer b (line-number-display-width 'columns))))))
(show "ln2" (lambda ()
  (let ((a (fill-buf "lnA" 50)) (b (fill-buf "lnB" 5)))
    (with-current-buffer a (setq display-line-numbers nil))
    (with-current-buffer b (setq display-line-numbers t))
    (list (with-current-buffer b (line-number-display-width))
          (with-current-buffer a (line-number-display-width))))))
;; scroll-up scrolls the selected window's buffer, in an excursion.
(show "sc1" (lambda ()
  (let ((a (fill-buf "scA" 200)) (b (fill-buf "scB" 200)))
    (set-window-buffer nil a)
    (set-window-start nil 1)
    (set-buffer b)
    (goto-char 30)
    (let ((r (condition-case e (scroll-up 3) (error (list 'ERR e)))))
      (list r (current-buffer) (point) (window-start)
            (with-current-buffer a (point)) (window-point))))))
(show "sc2" (lambda ()
  (let ((a (fill-buf "scA" 200)) (b (fill-buf "scB" 200)))
    (set-window-buffer nil a)
    (set-window-start nil 1)
    (set-buffer b)
    (let ((r (condition-case e (scroll-down 2) (error (list 'ERR e)))))
      (list r (current-buffer) (point) (window-start) (window-point))))))
(show "sc3" (lambda ()
  (let ((a (fill-buf "scA" 200)) (b (fill-buf "scB" 200)))
    (set-window-buffer nil a)
    (set-window-start nil 1)
    (set-buffer b)
    (condition-case e (scroll-up 'x) (error (list 'ERR e)))))))"##,
        r##"(progn
(defun ws-push (&rest xs) (prin1 xs) (terpri))
(defmacro ws-try (tag form) `(ws-push ,tag (condition-case e (progn ,form 'ok) (error (car e)))))
(defvar ws-objs nil)
(defun ws-setup (n)
  (ws-try (list n 'sel) (select-window (frame-first-window)))
  (ws-try (list n 'dow) (delete-other-windows))
  (ws-try (list n 'stb) (switch-to-buffer (get-buffer-create "wsA")))
  (erase-buffer) (insert "line one\nline two\nline three\n") (goto-char 5)
  (ws-try (list n 'split)
          (let* ((sel (selected-window))
                 (right (split-window sel nil 'right))
                 (below (split-window right nil 'below))
                 (dead (split-window below nil 'below)))
            (set-window-buffer below (get-buffer-create "wsB"))
            (delete-window dead)
            (setq ws-objs (list (cons 'selected sel) (cons 'right right) (cons 'below below) (cons 'dead dead))))))
(ws-setup 0)
(ws-push 'swp (condition-case e (set-window-point (cdr (assq 'below ws-objs)) nil) (error e)))
(ws-setup 1)
(ws-setup 2)
(ignore-errors (select-window (frame-first-window)))
(ignore-errors (select-window (frame-first-window)))
(ignore-errors (delete-other-windows))
(ws-push 'dead-selected (window-live-p (selected-window)) (buffer-name) (window-buffer (selected-window)))

(defun show (tag form)
  (message "%s %S" tag (condition-case err (funcall form) (error (list 'ERR err)))))
(set-buffer "wsA")
(show "d-start" (lambda () (window-start)))
(show "d-end" (lambda () (window-end)))
(show "d-body" (lambda () (list (window-body-height) (window-text-height) (window-mode-line-height) (window-header-line-height) (window-tab-line-height))))
(show "d-recenter" (lambda () (recenter 0)))
(show "d-mtwl" (lambda () (move-to-window-line 0)))
(show "d-scroll" (lambda () (scroll-up 1)))
(show "d-scroll-x" (lambda () (scroll-up 'x)))
(show "d-lndw" (lambda () (line-number-display-width)))
(show "d-pos" (lambda () (pos-visible-in-window-p)))
(show "d-fml" (lambda () (format-mode-line "%b")))
(show "d-wcch" (lambda () (let ((window-configuration-change-hook (list (lambda () (princ "HOOK\n")))))
                            (run-window-configuration-change-hook))))
(show "d-cur" (lambda () (list (buffer-name) (window-live-p (selected-window)))))
)"##,
    ] {
        assert_same(&corpus, &["--batch", "-Q", "--eval", program]);
    }
}

#[test]
fn the_review_of_round_332_is_answered_as_gnu_answers() {
    // The probes of round 332's review, each in its own session: a
    // sentinel, a buffer predicate or an unlock handler making the dying
    // buffer current again, `save-current-buffer' and a second
    // `kill-buffer' while the windows are replaced, an error there,
    // kill-buffer's excursion, move-point-visually's buffer, the scratch
    // buffer's creation and its log, and the replacement's window state.
    let corpus = Corpus::new();
    for program in [
        r##"(progn
;; A pipe process's sentinel, run by kill_buffer_processes's Fdelete_process,
;; kills the buffer now current; its Fkill_buffer makes the dying buffer
;; current.  GNU goes on to kill the dying buffer while it is current.
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(show "s1"
 (lambda ()
   (let* ((b1 (get-buffer-create "dying"))
          (b2 (get-buffer-create "other"))
          (p (make-pipe-process :name "pp" :buffer b1 :noquery t
                                :sentinel (lambda (proc ev)
                                            (princ (format "sentinel cur=%s\n" (buffer-name)))
                                            (when (buffer-live-p b2) (kill-buffer b2))
                                            (princ (format "after kill cur=%s\n" (buffer-name)))))))
     ;; buffer-list order: dying first, so other-buffer of b2 is b1.
     (switch-to-buffer "*scratch*")
     (bury-buffer "*scratch*") (bury-buffer "*Messages*")
     (set-buffer b2)
     (princ (format "list=%S\n" (mapcar #'buffer-name (buffer-list))))
     (let ((r (kill-buffer b1)))
       (list r (buffer-live-p b1) (buffer-live-p b2) (buffer-name (current-buffer))
             (buffer-live-p (current-buffer)))))))
(princ "survived\n"))"##,
        r##"(progn
;; The frame's buffer-predicate, called by Fkill_buffer's second Fother_buffer
;; (the minibuffer-buffer check), makes the dying buffer current.
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(defvar dying (get-buffer-create "dying"))
(show "p1"
 (lambda ()
   (set-frame-parameter nil 'buffer-predicate
                        (lambda (b) (when (buffer-live-p dying) (set-buffer dying)) t))
   (let ((r (with-current-buffer (window-buffer (minibuffer-window))
              (prog1 (kill-buffer dying)
                (princ (format "inner cur=%S\n" (current-buffer)))))))
     (set-frame-parameter nil 'buffer-predicate nil)
     (list r (buffer-live-p dying) (buffer-name)))))
(princ "survived\n"))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(defvar dying (get-buffer-create "dying"))
(defun zz-handler (op &rest args)
  (if (eq op 'unlock-file)
      (progn (princ (format "handler %S\n" op)) (set-buffer dying) nil)
    (let ((inhibit-file-name-handlers (cons 'zz-handler inhibit-file-name-handlers))
          (inhibit-file-name-operation op))
      (apply op args))))
(push '("\\`/zz:" . zz-handler) file-name-handler-alist)
(show "u1"
 (lambda ()
   (with-current-buffer dying
     (setq buffer-file-name "/zz:/tmp/nofile.txt")
     (setq buffer-file-truename "/zz:/tmp/nofile.txt")
     (insert "modified"))
   (set-buffer "*scratch*")
   (let ((r (kill-buffer dying)))
     (list r (buffer-live-p dying) (buffer-name)))))
(princ "survived\n"))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(defvar dying nil)
(defvar log nil)
(advice-add 'get-scratch-buffer-create :before
            (lambda (&rest _) (push (condition-case e (progn (set-buffer dying) (list 'ok (eq (current-buffer) dying))) (error e)) log)))
(advice-add 'get-scratch-buffer-create :after
            (lambda (&rest _) (push (list 'after (eq (current-buffer) dying) (buffer-live-p (current-buffer))) log)))
(show "m3"
 (lambda ()
   (fmakunbound 'replace-buffer-in-windows)
   (kill-buffer "*Messages*")
   (setq dying (get-buffer "*scratch*"))
   (set-window-buffer nil dying)
   (set-buffer " *load*")
   (let ((r (kill-buffer dying)))
     (list r (reverse log) (eq (current-buffer) dying) (buffer-live-p (current-buffer))))))
(set-buffer " *load*")
(show "msgs" (lambda () (and (get-buffer "*Messages*") (with-current-buffer "*Messages*" (buffer-string))))))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(fmakunbound 'replace-buffer-in-windows)
(defvar b (get-buffer-create "dying"))
(defvar o (get-buffer-create "repl"))
(dolist (n (list "*scratch*" "*Messages*")) (bury-buffer n))
(with-current-buffer o (setq-local left-margin-width -2))
(with-current-buffer b (insert "text in dying") (setq-local foo-local 7))
(defvar m (with-current-buffer b (copy-marker 3)))
(set-window-buffer nil b)
(set-buffer o)
(show "e1" (lambda () (kill-buffer b)))
(setq inhibit-quit nil)
(show "e1b" (lambda () (list (buffer-live-p b) (marker-buffer m) (buffer-local-value 'foo-local b)
                             (buffer-name (window-buffer)) (eq (window-buffer) b)
                             (window-start) (window-point)
                             (with-current-buffer b (list (buffer-string) (point)))
                             (get-buffer-window b)
                             (length (buffer-list)))))
(with-current-buffer o (setq-local left-margin-width 1))
(show "e1c" (lambda () (list (kill-buffer b) (buffer-live-p b) (marker-buffer m)
                             (buffer-name (window-buffer)) (window-margins))))
(show "e1d" (lambda () (list (set-buffer b))))
(show "e1e" (lambda () (with-current-buffer b (rename-buffer "zz")))))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(defvar a (get-buffer-create "A"))
(with-current-buffer a (insert "0123456789abcdef") (goto-char 10))
(show "x1"
 (lambda ()
   (let ((b (get-buffer-create "B")))
     (with-current-buffer b
       (add-hook 'kill-buffer-hook (lambda () (with-current-buffer a (goto-char 3))) nil t))
     (set-buffer a)
     (list (kill-buffer b) (buffer-name) (point)))))
(show "x2"
 (lambda ()
   (let ((b (get-buffer-create "B2")))
     (with-current-buffer b
       (add-hook 'kill-buffer-query-functions (lambda () (with-current-buffer a (goto-char 5)) t) nil t))
     (set-buffer a) (goto-char 12)
     (list (kill-buffer b) (buffer-name) (point))))))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(defvar a (get-buffer-create "A"))
(defvar b (get-buffer-create "B"))
(with-current-buffer a (insert "aaaaaaaaaa\nbbbbbbbbbb\n") (goto-char 3))
(with-current-buffer b (insert "xxxxxxxxxxxxxxxxxxxx\nyyyy\n") (goto-char 8))
(set-window-buffer nil a)
(set-buffer b)
(show "mpv" (lambda () (list (move-point-visually 1) (buffer-name) (point) (with-current-buffer a (point)))))
(show "mpv-1" (lambda () (list (move-point-visually -1) (buffer-name) (point) (with-current-buffer a (point)))))
(show "cont" (lambda () (list (display--line-is-continued-p) (buffer-name) (point) (with-current-buffer a (point))))))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(advice-add 'get-scratch-buffer-create :override
            (lambda (&rest _) (error "no scratch for you")))
(show "l2"
 (lambda ()
   (fmakunbound 'replace-buffer-in-windows)
   (kill-buffer "*Messages*")
   (set-window-buffer nil "*scratch*")
   (set-buffer " *load*")
   (let* ((old (get-buffer "*scratch*"))
          (r (kill-buffer old)))
     (list r (buffer-live-p old) (buffer-name (window-buffer))
           (buffer-name) inhibit-quit
           (mapcar #'buffer-name (buffer-list))
           (with-current-buffer (window-buffer) major-mode)
           (and (get-buffer "*Messages*") (with-current-buffer "*Messages*" (buffer-string))))))))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(fmakunbound (quote get-scratch-buffer-create)) (ignore
            (lambda (&rest _) (error "no scratch for you")))
(show "l3"
 (lambda ()
   (fmakunbound 'replace-buffer-in-windows)
   (kill-buffer "*Messages*")
   (set-window-buffer nil "*scratch*")
   (set-buffer " *load*")
   (let* ((old (get-buffer "*scratch*"))
          (r (kill-buffer old)))
     (list r (buffer-live-p old) (buffer-name (window-buffer))
           (buffer-name) inhibit-quit
           (mapcar #'buffer-name (buffer-list))
           (with-current-buffer (window-buffer) major-mode)
           (and (get-buffer "*Messages*") (with-current-buffer "*Messages*" (buffer-string))))))))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(fmakunbound 'replace-buffer-in-windows)
(setq frame-resize-pixelwise nil)
;; non-selected windows, display counts, prev-buffers, fringes, scroll bars,
;; vscroll, point insertion type.
(show "r1"
 (lambda ()
   (let* ((b (get-buffer-create "kbx"))
          (o (get-buffer-create "repl"))
          (w1 (selected-window))
          (w2 (split-window)))
     (with-current-buffer b (insert (make-string 300 ?x)) (goto-char 50))
     (with-current-buffer o (insert "repl text\nmore\n") (goto-char 4)
                          (setq buffer-display-count 7)
                          (setq-local window-point-insertion-type t)
                          (setq-local left-fringe-width 3)
                          (setq-local scroll-bar-width 5)
                          (setq-local left-margin-width 1))
     (set-window-buffer w1 b)
     (set-window-buffer w2 b)
     (set-window-dedicated-p w2 t)
     (set-window-hscroll w2 2)
     (set-window-start w2 20)
     ;; o first in the buffer list
     (dolist (n (list "*scratch*" "*Messages*")) (bury-buffer n))
     (switch-to-buffer o t t) (set-window-buffer w1 b)
     (set-buffer o)
     (princ (format "list=%S\n" (mapcar #'buffer-name (buffer-list))))
     (let ((before (buffer-local-value 'buffer-display-count o))
           (r (kill-buffer b)))
       (list r before
             (buffer-local-value 'buffer-display-count o)
             (buffer-name (window-buffer w1)) (buffer-name (window-buffer w2))
             (window-dedicated-p w2) (window-start w2) (window-point w2)
             (window-hscroll w2) (window-margins w2) (window-fringes w2)
             (window-scroll-bars w2)
             (window-vscroll w2)
             (let ((p (window-point w2))) (with-current-buffer o (save-excursion (goto-char p) (insert "Z"))) (list p (window-point w2)))
             (mapcar (lambda (e) (buffer-name (car e))) (window-prev-buffers w2))
             (window-next-buffers w2)
             (eq (get-buffer-window o) w1)
             (buffer-name (current-buffer)) (point))))))
(princ "done\n"))"##,
    ] {
        assert_same(&corpus, &["--batch", "-Q", "--eval", program]);
    }
}

#[test]
fn make_process_and_make_pipe_process_as_process_c_makes_them() {
    // Fmake_process's order of checks (the current directory before
    // `:name', the program search before each argument's CHECK_STRING,
    // `:stop' and `:connection-type' before the coding systems, whose
    // setup comes last), `:stderr''s pipe-process test, create_pty for a
    // nil `:command', the command list kept as given, and
    // Fmake_pipe_process as its own DEFUN (`:stop', `:plist', no
    // `:command'), each probe in its own session.
    let corpus = Corpus::new();
    for program in [
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
;; directory before :name
(show "m1" (lambda () (let ((default-directory "/nonexistent-dir-xyz/")) (make-process :name 7 :command '("true")))))
;; :name before :command program
(show "m2" (lambda () (make-process :name 7 :command '(8))))
;; program CHECK_STRING
(show "m3" (lambda () (make-process :name "p" :command '(8))))
;; program search before args check
(show "m4" (lambda () (make-process :name "p" :command '("no-such-program-xyz" 9))))
(show "m5" (lambda () (make-process :name "p" :command '("true" 9))))
;; odd args
(show "m6" (lambda () (make-process :name "p" :command)))
;; stderr not a pipe process
(show "m7" (lambda () (make-process :name "p" :command '("true") :stderr (make-process :name "q" :command '("true")))))
;; stderr string with nil program
(show "m8" (lambda () (make-process :name "p" :command nil :stderr "x")))
(show "m9" (lambda () (make-process :name "p" :command '("true") :buffer 5)))
(show "m10" (lambda () (make-process :name "p" :command '("true") :coding 'no-such-coding)))
(show "m11" (lambda () (make-process :name "p" :command "true")))
(show "m12" (lambda () (let ((default-directory 5)) (make-process :name "p" :command '("true")))))
(show "procs" (lambda () (mapcar #'process-name (process-list)))))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(show "n1" (lambda () (let ((p (make-process :name "p0" :command nil)))
  (list (process-status p) (process-type p) (stringp (process-tty-name p)) (process-id p) (process-command p)
        (process-buffer p) (process-contact p) (process-coding-system p) (process-live-p p)
        (condition-case e (process-send-string p "hi\n") (error e))
        (progn (delete-process p) (process-status p))))))
(show "n2" (lambda () (let ((p (make-process :name "p1" :command nil :buffer "pb" :connection-type 'pipe)))
  (list (process-status p) (process-type p) (process-tty-name p) (process-buffer p)))))
(show "n3" (lambda () (make-pipe-process :name "pp" :command))))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(defun pinfo (p) (list (process-name p) (process-status p) (process-type p) (process-command p)
                       (and (process-buffer p) (buffer-name (process-buffer p)))
                       (process-plist p) (process-contact p) (process-query-on-exit-flag p)
                       (process-filter p) (process-sentinel p) (process-id p)))
(show "q1" (lambda () (pinfo (make-pipe-process :name "q1"))))
(show "q2" (lambda () (pinfo (make-pipe-process :name "q2" :buffer "qb" :noquery t :stop t :plist '(a 1)))))
(show "q3" (lambda () (pinfo (make-pipe-process :name "q3" :command '("true") :stderr "x" :file-handler t))))
(show "q4" (lambda () (pinfo (make-pipe-process :name "q4" :plist 5))))
(show "q5" (lambda () (make-pipe-process :buffer "x")))
(show "q6" (lambda () (make-pipe-process :name 'x)))
(show "q7" (lambda () (make-pipe-process)))
(show "q8" (lambda () (pinfo (make-pipe-process :name "q8" :buffer 7))))
(show "q9" (lambda () (pinfo (make-pipe-process :name "q9" :coding 'no-such-cs))))
(show "q10" (lambda () (let ((p (make-pipe-process :name "q10" :filter t))) (list (process-status p) (process-filter p)))))
(show "q11" (lambda () (let ((p (make-pipe-process :name "q11" :stop t))) (list (process-status p) (progn (continue-process p) (process-status p))))))
(show "q12" (lambda () (let* ((out nil) (p (make-pipe-process :name "q12" :noquery t :filter (lambda (_ s) (push s out))))) (process-send-string p "hi\n") (accept-process-output p 0.2) (list out (process-status p)))))
(show "m1" (lambda () (make-process :name "m1" :command '("no-such-program-xyz") :coding 'no-such-cs)))
(show "m2" (lambda () (make-process :name "m2" :command '("true") :connection-type 'bogus :coding 'no-such-cs)))
(show "m3" (lambda () (make-process :name "m3" :command '("true") :stop t :connection-type 'bogus)))
(show "m4" (lambda () (pinfo (make-process :name "m4" :command nil :connection-type 'pipe :buffer "m4b"))))
(show "m5" (lambda () (let ((p (make-process :name "m5"))) (list (process-status p) (process-type p) (stringp (process-tty-name p)) (process-id p) (process-contact p)))))
(show "m6" (lambda () (make-process :name "m6" :command '("true") :stderr 5)))
(show "procs" (lambda () (mapcar #'process-name (process-list)))))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(show "c1" (lambda () (let* ((s (propertize "true" 'face 'bold)) (c (list s "a")) (p (make-process :name "c1" :command c :noquery t)))
   (list (eq (car (process-command p)) s) (eq (process-command p) c) (process-command p)))))
(show "c2" (lambda () (let* ((p (make-process :name "c2" :command '("sh" "-c" "echo out; echo err >&2") :stderr "c2err" :buffer "c2out" :noquery t)))
   (while (accept-process-output p 0.2))
   (sleep-for 0.2)
   (list (mapcar #'process-name (process-list))
         (with-current-buffer "c2out" (buffer-string))
         (with-current-buffer "c2err" (buffer-string))
         (process-status (get-process "c2 stderr"))
         (condition-case e (process-send-string (get-process "c2 stderr") "x") (error e))))))
(show "c3" (lambda () (let ((inherit-process-coding-system t)) (list (process-inherit-coding-system-flag (make-pipe-process :name "c3" :noquery t))
   (process-inherit-coding-system-flag (make-process :name "c3b" :command '("true") :buffer "c3b" :noquery t))))))
(show "c4" (lambda () (let ((p (make-process :name "c4" :command '("cat") :noquery t :connection-type 'pipe)) (out nil))
   (set-process-filter p (lambda (_ s) (push s out)))
   (process-send-string p "hello\n") (process-send-eof p)
   (while (accept-process-output p 0.2))
   (list out (process-status p) (process-exit-status p)))))
(show "c5" (lambda () (let ((p (make-process :name "c5" :command nil :noquery t)) (out nil))
   (set-process-filter p (lambda (_ s) (push s out)))
   (process-send-string p "abc\n")
   (accept-process-output p 0.3)
   (list out (process-status p)))))
(show "c6" (lambda () (let ((default-directory "/nonexistent-zz/")) (make-pipe-process :name "c6" :noquery t))))
(show "procs" (lambda () (mapcar #'process-name (process-list)))))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(show "k1" (lambda () (let ((p (make-process :name "k1" :command nil :buffer "k1b" :noquery t))) (list (kill-buffer "k1b") (process-status p) (process-live-p p)))))
(show "k2" (lambda () (let ((p (make-process :name "k2" :command nil :noquery t))) (list (condition-case e (process-send-eof p) (error e)) (process-status p)))))
(show "k3" (lambda () (let ((p (make-process :name "k3" :command nil :noquery t))) (list (condition-case e (signal-process p 'TERM) (error e)) (condition-case e (interrupt-process p) (error e)) (process-status p)))))
(show "k4" (lambda () (let ((p (make-process :name "k4" :command nil :noquery t :connection-type 'pipe))) (list (condition-case e (process-send-string p "x") (error e)) (process-tty-name p) (delete-process p) (process-status p)))))
(show "k5" (lambda () (let ((p (make-pipe-process :name "k5" :noquery t))) (list (condition-case e (process-send-eof p) (error e)) (process-status p) (condition-case e (process-send-string p "x") (error e))))))
(show "k6" (lambda () (let ((p (make-process :name "k6" :command nil :noquery t))) (list (process-contact p) (process-contact p t) (process-type p) (process-running-child-p p))))))"##,
    ] {
        assert_same(&corpus, &["--batch", "-Q", "--eval", program]);
    }
}

#[test]
fn the_review_of_round_333_is_answered_as_gnu_answers() {
    // The probes of round 333's review and of its fixes, each in its own
    // session: create_pty's coding setup only with a pty, the `:stderr'
    // pipe's input closed before the coding setup, a childless pty's
    // signals, send-eof and window size, set-process-window-size through
    // p->infd, process buffers through Fget_buffer_create,
    // `default-process-coding-system' read after find-operation, a send
    // that blocks while timers run, and a childless pty's EIO once
    // another opener of its tty closes it.
    let corpus = Corpus::new();
    for program in [
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(show "e1" (lambda () (let* ((ev nil) (p (make-process :name "e1" :noquery t :sentinel (lambda (_ s) (push s ev)))))
  (list (process-running-child-p p) (progn (dotimes (_ 3) (accept-process-output nil 0.05)) (process-status p)) (reverse ev)
        (condition-case e (process-send-string p "x\n") (error e))))))
(show "e2" (lambda () (let* ((ev nil) (p (make-process :name "e2" :noquery t :sentinel (lambda (_ s) (push s ev)))))
  (let ((s (make-serial-process :port (process-tty-name p) :speed nil :noquery t)))
    (delete-process s))
  (list (progn (dotimes (_ 3) (accept-process-output nil 0.05)) (process-status p)) (reverse ev) (process-exit-status p)))))
(show "e3" (lambda () (let* ((ev nil) (p (make-process :name "e3" :noquery t :sentinel (lambda (_ s) (push s ev)))))
  (call-process "sh" nil nil nil "-c" (format "echo hi > %s" (process-tty-name p)))
  (list (progn (dotimes (_ 3) (accept-process-output nil 0.05)) (process-status p)) (reverse ev)))))
(show "e4" (lambda () (let* ((out nil) (p (make-process :name "e4" :noquery t :filter (lambda (_ s) (push s out)))))
  (process-send-string p "abc\n")
  (dotimes (_ 3) (accept-process-output nil 0.05))
  (list (process-status p) (reverse out)))))
(show "e6" (lambda () (let* ((ev nil) (p (make-process :name "e6" :noquery t :sentinel (lambda (_ s) (push s ev)))))
  (call-process "sh" nil nil nil "-c" (format "true < %s" (process-tty-name p)))
  (dotimes (_ 3) (accept-process-output nil 0.05))
  (list (process-status p) (condition-case e (process-send-string p "x") (error e))
        (progn (delete-process p) (accept-process-output nil 0.05) (process-status p)) (process-exit-status p) (reverse ev) (memq p (process-list))))))
(show "e7" (lambda () (let* ((ev nil) (p (make-process :name "e7" :noquery t :connection-type 'pipe :sentinel (lambda (_ s) (push s ev)))))
  (list (process-status p) (process-tty-name p)))))
(show "e8" (lambda () (let* ((out nil) (p (make-process :name "e8" :noquery t :filter (lambda (_ s) (push s out)))))
  (with-temp-buffer (call-process "stty" nil t nil "-F" (process-tty-name p) "size"))
  (dotimes (_ 3) (accept-process-output nil 0.05))
  (list (process-status p) (set-process-window-size p 5 6))))))"##,
        r##"(progn
;; create_pty sets up coding systems only with a pty.
(condition-case e
    (let ((p (make-process :name "q1" :connection-type 'pipe :coding 'no-such-coding)))
      (message "q1 ok %S %S %S" (process-status p) (process-coding-system p) (process-tty-name p))
      (delete-process p))
  (error (message "q1 err %S" e)))
(condition-case e
    (let ((p (make-process :name "q1b" :connection-type 'pty :coding 'no-such-coding)))
      (message "q1b ok %S" (process-status p)))
  (error (message "q1b err %S %S" e (mapcar #'process-name (process-list))))))"##,
        r##"(progn
;; set-process-window-size through p->infd.
(let ((p (make-process :name "q2a" :command '("sleep" "3") :connection-type 'pty)))
  (message "pty child %S" (set-process-window-size p 10 20)) (delete-process p))
(let ((p (make-process :name "q2b" :command '("sleep" "3") :connection-type 'pipe)))
  (message "pipe child %S" (set-process-window-size p 10 20)) (delete-process p))
(let ((p (make-process :name "q2c" :command '("sleep" "3") :connection-type '(pty . pipe))))
  (message "pty-in child %S" (set-process-window-size p 10 20)) (delete-process p))
(let ((p (make-process :name "q2d" :command '("sleep" "3") :connection-type '(pipe . pty))))
  (message "pty-out child %S" (set-process-window-size p 10 20)) (delete-process p))
(let ((p (make-process :name "q2e" :connection-type 'pty)))
  (message "childless pty %S" (set-process-window-size p 10 20)) (delete-process p))
(let ((p (make-process :name "q2e2" :connection-type 'pipe)))
  (message "childless no pty %S" (set-process-window-size p 10 20)) (delete-process p))
(let ((p (make-pipe-process :name "q2f")))
  (message "pipe proc %S" (set-process-window-size p 10 20)) (delete-process p))
(let ((p (make-network-process :name "q2g" :server t :service t :host 'local)))
  (message "server %S" (set-process-window-size p 10 20)) (delete-process p))
(let ((p (make-process :name "q2h" :command '("true") :connection-type 'pty)))
  (while (process-live-p p) (accept-process-output p 0.05))
  (message "exited pty child %S" (set-process-window-size p 10 20)))
(let ((p (make-process :name "q2i" :command '("sleep" "3") :connection-type 'pty)))
  (delete-process p)
  (message "deleted pty child %S" (set-process-window-size p 10 20))))"##,
        r##"(progn
;; The :stderr pipe's input is closed before the coding systems' setup.
(let* ((errp (make-pipe-process :name "q3err")))
  (condition-case e
      (make-process :name "q3" :command '("true") :stderr errp :coding 'no-such-coding)
    (error (message "q3 err %S" e)))
  (message "procs %S" (mapcar #'process-name (process-list)))
  (message "stderr status %S" (process-status errp))
  (condition-case e (progn (process-send-string errp "x") (message "send ok"))
    (error (message "send err %S" e)))))"##,
        r##"(progn
;; Process buffers through Fget_buffer_create.
(setq inherit-process-coding-system t)
(let ((b (generate-new-buffer "q4dead")))
  (kill-buffer b)
  (let ((p (make-pipe-process :name "q4a" :buffer b)))
    (message "pipe dead buffer %S %S %S" (process-buffer p) (marker-buffer (process-mark p))
             (process-inherit-coding-system-flag p))
    (delete-process p))
  (let ((p (make-process :name "q4b" :command '("true") :buffer b)))
    (message "proc dead buffer %S %S %S" (process-buffer p) (marker-buffer (process-mark p))
             (process-inherit-coding-system-flag p))
    (delete-process p))
  (let ((p (make-process :name "q4b2" :buffer b)))
    (message "childless dead buffer %S %S" (process-buffer p) (process-inherit-coding-system-flag p))
    (delete-process p)))
(condition-case e (make-pipe-process :name "q4c" :buffer "") (error (message "empty %S" e)))
(condition-case e (make-process :name "q4d" :command '("true") :buffer "") (error (message "empty2 %S" e)))
(condition-case e (make-process :name "q4d2" :command '("true") :stderr "") (error (message "empty3 %S" e)))
(defvar q4-n 0)
(add-hook 'buffer-list-update-hook (lambda () (setq q4-n (1+ q4-n))))
(let ((p (make-pipe-process :name "q4e" :buffer "q4-new-buffer")))
  (message "hook ran %d %S" q4-n (buffer-live-p (process-buffer p)))
  (delete-process p))
(setq q4-n 0)
(let ((p (make-process :name "q4e2" :command '("true") :buffer "q4-new-buffer2")))
  (message "hook ran2 %d" q4-n)
  (delete-process p))
(condition-case e (make-pipe-process :name "q4f" :buffer 42) (error (message "num %S" e)))
(condition-case e (make-process :name "q4g" :command '("true") :buffer 'sym) (error (message "sym %S" e))))"##,
        r##"(progn
;; Vdefault_process_coding_system read after find-operation-coding-system.
(setq default-process-coding-system '(utf-8 . utf-8))
(defun q5-fn (&rest _) (setq default-process-coding-system '(latin-1 . latin-1)) 0)
(let ((process-coding-system-alist (list (cons "true" 'q5-fn))))
  (let ((p (make-process :name "q5" :command '("true"))))
    (message "q5 %S" (process-coding-system p))))
(setq default-process-coding-system '(utf-8 . utf-8))
(defun q5-net (&rest _) (setq default-process-coding-system '(latin-1 . latin-1)) 0)
(let* ((server (make-network-process :name "q5s" :server t :service t :host 'local))
       (port (process-contact server :service))
       (network-coding-system-alist (list (cons port 'q5-net))))
  (let ((c (make-network-process :name "q5c" :host 'local :service port)))
    (message "q5 net %S" (process-coding-system c))
    (delete-process c))
  (delete-process server)))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
;; childless pipe connection with a bad coding: create_pty sets nothing up
(show "a1" (lambda () (let ((p (make-process :name "a1" :command nil :connection-type 'pipe :coding 'no-such-cs :noquery t)))
   (list (process-status p) (process-coding-system p)))))
;; childless pty with a bad coding: setup_process_coding_systems signals, process removed
(show "a2" (lambda () (make-process :name "a2" :command nil :coding 'no-such-cs :noquery t)))
;; :stderr given as a string naming an existing process
(show "a3" (lambda () (let ((q (make-process :name "a3q" :command '("sleep" "3") :noquery t)))
   (let ((p (make-process :name "a3" :command '("true") :stderr "a3q" :noquery t)))
     (list (process-name p) (mapcar #'process-name (process-list)))))))
;; :stderr given as a buffer that has a process
(show "a4" (lambda () (let ((q (make-process :name "a4q" :command '("sleep" "3") :buffer "a4b" :noquery t)))
   (let ((p (make-process :name "a4" :command '("true") :stderr (get-buffer "a4b") :noquery t)))
     (list (process-name p) (mapcar #'process-name (process-list)))))))
;; :stderr pipe left behind when make-process fails later
(show "a5" (lambda () (list (condition-case e (make-process :name "a5" :command '("true") :stderr "a5err" :stop t) (error e))
   (mapcar #'process-name (process-list)))))
(show "a6" (lambda () (list (condition-case e (make-process :name "a6" :command '("no-such-prog-zz") :stderr "a6err") (error e))
   (mapcar #'process-name (process-list)))))
(show "procs" (lambda () (mapcar (lambda (p) (list (process-name p) (process-status p))) (process-list)))))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
;; send to a :stderr pipe process whose input create_process closed, child alive
(show "b1" (lambda () (let* ((pp (make-pipe-process :name "b1e" :noquery t))
                             (p (make-process :name "b1" :command '("sleep" "2") :stderr pp :noquery t)))
   (list (process-status pp) (condition-case e (process-send-string pp "x") (error e))))))
;; same with the buffer form of :stderr
(show "b2" (lambda () (let* ((p (make-process :name "b2" :command '("sleep" "2") :stderr "b2e" :noquery t)))
   (list (process-status (get-process "b2 stderr")) (condition-case e (process-send-string (get-process "b2 stderr") "x") (error e))))))
;; a make-process with :stderr pipe that fails in setup_process_coding_systems (create_process
;; closes the pipe's input before it)
(show "b3" (lambda () (let* ((pp (make-pipe-process :name "b3e" :noquery t)))
   (list (condition-case e (make-process :name "b3" :command '("true") :stderr pp :coding 'no-such-cs) (error e))
         (process-status pp) (condition-case e (process-send-string pp "x") (error e))))))
;; ...and that fails at the program search (before create_process): pipe still writable
(show "b4" (lambda () (let* ((pp (make-pipe-process :name "b4e" :noquery t)))
   (list (condition-case e (make-process :name "b4" :command '("no-such-prog-zz") :stderr pp) (error e))
         (process-status pp) (condition-case e (process-send-string pp "x") (error e))))))
;; reuse of a :stderr pipe process for a second child
(show "b5" (lambda () (let* ((pp (make-pipe-process :name "b5e" :noquery t)) (out nil))
   (set-process-filter pp (lambda (_ s) (push s out)))
   (make-process :name "b5a" :command '("sh" "-c" "echo one >&2") :stderr pp :noquery t)
   (make-process :name "b5b" :command '("sh" "-c" "echo two >&2; echo two-out") :stderr pp :noquery t :buffer "b5bout")
   (let ((n 0)) (while (and (< n 20) (accept-process-output nil 0.1)) (setq n (1+ n))))
   (sleep-for 0.3) (accept-process-output nil 0.1)
   (list (sort out #'string<) (process-status pp) (with-current-buffer "b5bout" (buffer-string)))))))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(defun mk (name &rest more) (let ((out nil)) (list (apply #'make-process :name name :command nil :noquery t
   :filter (lambda (_ s) (push s (car (process-get _ 'outbox)))) more))))
(defun outs (p) (accept-process-output p 0.2) (reverse (car (process-get p 'outbox))))
(defun fresh (name &rest more)
  (let ((p (apply #'make-process :name name :command nil :noquery t more)))
    (process-put p 'outbox (list nil))
    (set-process-filter p (lambda (pr s) (push s (car (process-get pr 'outbox)))))
    p))
(show "d1" (lambda () (let ((p (fresh "d1"))) (list (interrupt-process p t) (outs p) (process-status p)))))
(show "d2" (lambda () (let ((p (fresh "d2"))) (list (quit-process p t) (outs p) (process-status p)))))
(show "d3" (lambda () (let ((p (fresh "d3"))) (list (stop-process p t) (outs p) (process-status p) (continue-process p t) (process-status p)))))
(show "d4" (lambda () (let ((p (fresh "d4"))) (list (stop-process p) (process-status p) (continue-process p) (process-status p)))))
(show "d5" (lambda () (let ((p (fresh "d5"))) (list (kill-process p) (outs p) (process-status p)))))
(show "d6" (lambda () (let ((p (fresh "d6" :connection-type 'pipe))) (list (condition-case e (interrupt-process p) (error e)) (condition-case e (stop-process p) (error e)) (condition-case e (process-running-child-p p) (error e)) (process-status p)))))
(show "d7" (lambda () (let ((p (fresh "d7"))) (list (interrupt-process p 'lambda) (outs p)))))
(show "d8" (lambda () (let ((p (fresh "d8"))) (process-send-string p "abc") (list (interrupt-process p t) (outs p)))))
(show "d9" (lambda () (let ((p (fresh "d9"))) (list (process-send-eof p) (outs p) (process-send-string p "q\n") (outs p))))))"##,
        r##"(progn
;; Sending more than a pipe's capacity to a pipe process nobody reads:
;; GNU's send_process waits in wait_reading_process_output, which runs timers.
(let ((p (make-pipe-process :name "h1" :noquery t)))
  (run-at-time 0.5 nil (lambda () (princ "timer ran\n") (kill-emacs 3)))
  (princ "sending\n")
  (condition-case e (process-send-string p (make-string 200000 ?x)) (error (princ (format "err %S\n" e))))
  (princ "sent\n")))"##,
        r##"(progn
;; A real child that never reads its input: same send loop.
(let ((p (make-process :name "h4" :command '("sleep" "30") :connection-type 'pipe :noquery t)))
  (run-at-time 0.5 nil (lambda () (message "timer ran") (kill-emacs 3)))
  (message "sending")
  (condition-case e (process-send-string p (make-string 200000 ?x)) (error (message "err %S" e)))
  (message "sent")))"##,
        r##"(progn
;; Same for a childless pty: the slave's input queue fills.
(let ((p (make-process :name "h2" :command nil :noquery t)))
  (run-at-time 0.5 nil (lambda () (princ "timer ran\n") (kill-emacs 3)))
  (princ "sending\n")
  (condition-case e (process-send-string p (make-string 200000 ?x)) (error (princ (format "err %S\n" e))))
  (princ "sent\n")))"##,
        r##"(progn
;; A timer that deletes the pipe process while send_process waits.
(let ((p (make-pipe-process :name "h3" :noquery t)))
  (run-at-time 0.5 nil (lambda () (princ "timer deletes\n") (delete-process p)))
  (princ "sending\n")
  (princ (format "result %S\n" (condition-case e (process-send-string p (make-string 200000 ?x)) (error e))))
  (princ "after\n")))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(show "t1" (lambda () (let ((p (make-pipe-process :name "t1" :stop t :noquery t))) (list (process-send-string p "abc") (process-status p)))))
(show "t2" (lambda () (let ((p (make-pipe-process :name "t2" :noquery t))) (delete-process p) (condition-case e (process-send-string p "abc") (error e)))))
(show "t3" (lambda () (let ((p (make-process :name "t3" :command nil :noquery t))) (delete-process p) (condition-case e (process-send-string p "abc") (error e)))))
(show "t4" (lambda () (let ((p (make-process :name "t4" :command nil :noquery t :connection-type 'pipe))) (delete-process p) (list (condition-case e (process-send-string p "abc") (error e)) (process-status p) (process-live-p p) (memq p (process-list))))))
(show "t5" (lambda () (with-temp-buffer (insert "héllo\n") (let ((p (make-process :name "t5" :command nil :noquery t)) (out nil))
   (set-process-filter p (lambda (_ s) (push s out)))
   (process-send-region p (point-min) (point-max)) (accept-process-output p 0.2) (list out (process-coding-system p))))))
(show "t6" (lambda () (let ((p (make-process :name "t6" :command nil :noquery t :connection-type 'pipe))) (list (condition-case e (process-send-eof p) (error e)) (condition-case e (process-send-string p "x") (error e))))))
(show "t7" (lambda () (let ((p (make-pipe-process :name "t7" :noquery t))) (list (process-send-string p (make-string 1000 ?a)) (process-send-eof p) (process-send-string p (make-string 100000 ?a)))))))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(show "u1" (lambda () (let* ((ev nil) (p (make-process :name "u1" :command nil :noquery t :sentinel (lambda (_ s) (push s ev)))))
   (continue-process p) (accept-process-output nil 0.1) (list (reverse ev) (process-status p)))))
(show "u2" (lambda () (let* ((ev nil) (p (make-process :name "u2" :command nil :noquery t :connection-type 'pipe :sentinel (lambda (_ s) (push s ev)))))
   (list (condition-case e (continue-process p) (error e)) (reverse ev)))))
(show "u3" (lambda () (let* ((ev nil) (p (make-process :name "u3" :command '("sleep" "5") :noquery t :sentinel (lambda (_ s) (push s ev)))))
   (stop-process p) (accept-process-output nil 0.2) (continue-process p) (accept-process-output nil 0.2)
   (prog1 (list (reverse ev) (process-status p)) (delete-process p)))))
(show "u4" (lambda () (let* ((ev nil) (p (make-process :name "u4" :command nil :noquery t :sentinel (lambda (_ s) (push s ev)))))
   (continue-process p t) (accept-process-output nil 0.1) (list (reverse ev) (process-status p))))))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(show "w1" (lambda () (let ((p (make-process :name "w1" :command nil :noquery t)))
   (list (set-process-window-size p 33 77)
         (with-temp-buffer (call-process "stty" nil t nil "-F" (process-tty-name p) "size") (buffer-string))))))
(show "w2" (lambda () (let ((p (make-process :name "w2" :command nil :noquery t :connection-type 'pipe))) (set-process-window-size p 33 77))))
(show "w3" (lambda () (let ((p (make-pipe-process :name "w3" :noquery t))) (set-process-window-size p 33 77))))
(show "w4" (lambda () (let ((p (make-process :name "w4" :command nil :noquery t :connection-type 'pipe))) (set-process-coding-system p 'utf-8 'utf-8))))
(show "w5" (lambda () (let ((p (make-process :name "w5" :command nil :noquery t))) (list (set-process-coding-system p 'latin-1 'latin-1) (process-coding-system p)))))
(show "w6" (lambda () (let ((p (make-process :name "w6" :command nil :noquery t :connection-type 'pipe))) (list (set-process-filter p t) (set-process-filter p nil) (process-filter p)))))
(show "w7" (lambda () (let ((p (make-process :name "w7" :command '("sleep" "3") :noquery t))) (prog1 (set-process-window-size p 20 50) (delete-process p))))))"##,
    ] {
        assert_same(&corpus, &["--batch", "-Q", "--eval", program]);
    }
}

#[test]
fn make_network_process_as_process_c_makes_it() {
    // Fmake_network_process's order of steps and connect_network_socket,
    // each probe in its own session: getaddrinfo's errors as
    // "HOST/SERVICE gai_strerror" for clients, servers and datagram
    // sockets, a family the address has not, the port string, a non-ASCII
    // host; `:nowait''s name lookup answered later, its failure "Name
    // lookup of HOST failed", a datagram client left connecting;
    // `:remote' and `:local' address objects, a malformed one, (FAMILY .
    // VECTOR); a local socket's name and its ignored host; servers'
    // options, backlog, `:stop', `:noquery' and `:plist'; the datagram
    // address of a client and a server; and the errors after
    // make_process, which remove the process.
    let corpus = Corpus::new();
    for program in [
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(defun noport (v) (if (and (vectorp v) (memq (length v) '(5 9))) (vconcat (butlast (append v nil)) '(0)) v))
(show "a1" (lambda () (make-network-process :name "a1" :host "no-such-host.invalid" :service 80)))
(show "a2" (lambda () (make-network-process :name "a2" :host "no-such-host.invalid" :service t :server t)))
(show "a3" (lambda () (make-network-process :name "a3" :host "no-such-host.invalid" :service 80 :type 'datagram)))
(show "a4" (lambda () (make-network-process :name "a4" :host "no-such-host.invalid" :service 80 :type 'datagram :server t)))
(show "a5" (lambda () (make-network-process :name "a5" :host "127.0.0.1" :service 80 :family 'ipv6)))
(show "a6" (lambda () (make-network-process :name "a6" :host "::1" :service 80 :family 'ipv4)))
(show "a7" (lambda () (make-network-process :name "a7" :host "127.0.0.1" :service t :server t :family 'ipv6)))
(show "a8" (lambda () (make-network-process :name "a8" :host "" :service 80)))
(show "a9" (lambda () (make-network-process :name "a9" :host "no-such-host.invalid" :service 70000)))
(show "a10" (lambda () (make-network-process :name "a10" :host "no-such-host.invalid" :service -1)))
(show "a11" (lambda () (let* ((ev nil) (p (make-network-process :name "a11" :host "no-such-host.invalid" :service 80 :nowait t
                         :sentinel (lambda (_ s) (push s ev)))))
   (list (process-status p) (progn (dotimes (_ 40) (accept-process-output nil 0.05)) (process-status p)) (reverse ev)
         (process-exit-status p) (process-contact p :remote)))))
(show "a12" (lambda () (make-network-process :name "a12" :host "localhost" :service 1 :family 'ipv4)))
(show "a13" (lambda () (open-network-stream "a13" nil "no-such-host.invalid" 80)))
(show "a14" (lambda () (make-network-process :name "a14" :host 'local :service t :server t :family 'ipv6)))
(show "a15" (lambda () (let ((s (make-network-process :name "a15" :host "localhost" :service t :server t)))
   (prog1 (list (noport (process-contact s :local)) (process-contact s :host)) (delete-process s)))))
(show "a16" (lambda () (let ((s (make-network-process :name "a16" :host "127.0.0.1" :service t :server t)))
   (prog1 (let ((c (make-network-process :name "a16c" :host "localhost" :service (process-contact s :service) :family 'ipv4)))
            (prog1 (list (process-status c) (noport (process-contact c :remote))) (delete-process c))) (delete-process s))))))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(defun noport (v) (if (and (vectorp v) (memq (length v) '(5 9))) (vconcat (butlast (append v nil)) '(0)) v))
(show "w1" (lambda () (let ((p (make-network-process :name "w1" :host "127.0.0.1" :service t :server t :nowait t))) (list (process-status p) (process-contact p :service)))))
(show "w2" (lambda () (let ((p (make-network-process :name "w2" :host "127.0.0.1" :service 9 :type 'datagram :nowait t)))
  (list (process-status p) (progn (dotimes (_ 10) (accept-process-output nil 0.05)) (process-status p)) (process-contact p :remote) (process-contact p :local)))))
(show "w3" (lambda () (let ((p (make-network-process :name "w3" :host "127.0.0.1" :service 9 :type 'datagram)))
  (list (process-status p) (process-contact p :remote) (process-contact p :local)))))
(show "w4" (lambda () (let ((p (make-network-process :name "w4" :host "127.0.0.1" :service t :type 'datagram :server t)))
  (list (process-status p) (numberp (process-contact p :service)) (process-contact p :remote) (length (process-contact p :local))))))
(show "w5" (lambda () (let ((s (make-network-process :name "w5" :host "127.0.0.1" :service t :server t :keepalive t :reuseaddr nil)))
  (list (process-status s) (process-contact s :reuseaddr)))))
(show "w6" (lambda () (make-network-process :name "w6" :host "127.0.0.1" :service t :server t :bogus-option 5)))
(show "w7" (lambda () (make-network-process :name "w7" :host "127.0.0.1" :service t :server t :linger 'x)))
(show "w8" (lambda () (let ((s (make-network-process :name "w8" :host "127.0.0.1" :service t :server t)))
  (make-network-process :name "w8b" :host "127.0.0.1" :service (process-contact s :service) :server t :reuseaddr nil))))
(show "w9" (lambda () (let* ((ev nil) (p (make-network-process :name "w9" :host "127.0.0.1" :service 1 :nowait t :sentinel (lambda (_ s) (push s ev)))))
  (list (process-status p) (progn (dotimes (_ 20) (accept-process-output nil 0.05)) (process-status p)) (reverse ev)))))
(show "w10" (lambda () (make-network-process :name "w10" :host "127.0.0.1" :service "80x")))
(show "w11" (lambda () (make-network-process :name "w11" :host 5 :service 80)))
(show "w12" (lambda () (make-network-process :name "w12" :host "127.0.0.1" :service 'foo)))
(show "w13" (lambda () (make-network-process :name "w13" :host "héllo" :service 80))))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(defun noport (v) (if (and (vectorp v) (memq (length v) '(5 9))) (vconcat (butlast (append v nil)) '(0)) v))
(show "c1" (lambda () (let ((p (make-network-process :name "c1" :host "127.0.0.1" :service t :server t :noquery t :stop t :plist '(a 1))))
  (prog1 (list (process-status p) (process-query-on-exit-flag p) (process-plist p) (process-command p) (process-type p)) (delete-process p)))))
(show "c2" (lambda () (let* ((s (make-network-process :name "c2s" :host "127.0.0.1" :service t :server t))
                             (port (process-contact s :service))
                             (c (make-network-process :name "c2c" :remote (vector 127 0 0 1 port))))
  (prog1 (list (process-status c) (noport (process-contact c :remote)) (process-contact c :host) (equal (process-contact c :service) nil)) (delete-process c) (delete-process s)))))
(show "c3" (lambda () (make-network-process :name "c3" :remote [1 2 3])))
(show "c4" (lambda () (make-network-process :name "c4" :remote "/nonexistent/sock")))
(show "c5" (lambda () (let ((s (make-network-process :name "c5" :server t :local (vector 127 0 0 1 0))))
  (prog1 (list (process-status s) (numberp (process-contact s :service)) (aref (process-contact s :local) 0)) (delete-process s)))))
(show "c6" (lambda () (let* ((s (make-network-process :name "c6s" :host "127.0.0.1" :service t :server t :type 'datagram))
                             (port (process-contact s :service))
                             (got nil)
                             (c (make-network-process :name "c6c" :host "127.0.0.1" :service port :type 'datagram)))
   (set-process-filter s (lambda (_ str) (push str got)))
   (process-send-string c "hello")
   (dotimes (_ 10) (accept-process-output nil 0.02))
   (list got (equal (process-datagram-address s) (process-contact c :local)) (vectorp (process-contact s :remote))
         (set-process-datagram-address s [127 0 0 1 9]) (process-datagram-address s)
         (set-process-datagram-address s [0 0 0 0 0 0 0 0 9]) (noport (process-contact c :remote))))))
(show "c7" (lambda () (let* ((f (make-temp-name "/tmp/emx-sock-")) (s (make-network-process :name "c7s" :family 'local :service f :server t)))
   (prog1 (let ((c (make-network-process :name "c7c" :family 'local :service f)))
      (list (process-status s) (equal (process-contact s :local) f) (equal (process-contact c :remote) f) (process-contact c :local) (process-status c)))
     (delete-file f)))))
(show "c8" (lambda () (make-network-process :name "c8" :family 'local :host "x" :service "/tmp/emx-no-such-sock")))
(show "c9" (lambda () (make-network-process :name "c9" :family 'local :service (make-string 200 ?a))))
(show "c10" (lambda () (make-network-process :name "c10" :family 'local :service 5)))
(show "c11" (lambda () (make-network-process :name "c11" :host "127.0.0.1" :service t :server t :plist 5)))
(show "c12" (lambda () (list (make-network-process :name "c12" :host "127.0.0.1" :service t :server t :tls-parameters 5) (mapcar #'process-name (process-list)))))
(show "c13" (lambda () (make-network-process :name "c13" :host "127.0.0.1" :service t :server t :coding 'no-such)))
(show "c14" (lambda () (mapcar #'process-name (process-list))))
(show "c15" (lambda () (make-network-process :name "c15" :host "127.0.0.1" :service t :server 'x :type 'foo)))
(show "c16" (lambda () (make-network-process :name "c16" :host "127.0.0.1" :service 1)))
(show "c17" (lambda () (let ((s (make-network-process :name "c17" :host "127.0.0.1" :service t :server 2)))
   (prog1 (process-status s) (delete-process s)))))
(show "c18" (lambda () (make-network-process :name "c18" :remote '(2 . [0 80 127 0 0 1]))))
(show "c19" (lambda () (let ((s (make-network-process :name "c19" :host "127.0.0.1" :service "0" :server t)))
   (prog1 (numberp (process-contact s :service)) (delete-process s))))))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(defun noport (v) (if (and (vectorp v) (memq (length v) '(5 9))) (vconcat (butlast (append v nil)) '(0)) v))
(show "d1" (lambda () (let* ((s (make-network-process :name "d1s" :host "127.0.0.1" :service t :server t))
                             (port (process-contact s :service)) (ev nil)
                             (c (make-network-process :name "d1c" :host "127.0.0.1" :service port :nowait t
                                  :sentinel (lambda (_ e) (push e ev)))))
  (list (process-status c) (progn (dotimes (_ 10) (accept-process-output nil 0.02)) (process-status c)) (reverse ev)
        (prog1 (vectorp (process-contact c :local)) (delete-process c) (delete-process s))))))
(show "d2" (lambda () (let* ((s (make-network-process :name "d2s" :host "localhost" :service t :server t :family 'ipv4))
                             (port (process-contact s :service)) (ev nil)
                             (c (make-network-process :name "d2c" :host "localhost" :service port :nowait t :family 'ipv4
                                  :sentinel (lambda (_ e) (push e ev)))))
  (list (process-status c) (noport (process-contact c :remote)) (progn (dotimes (_ 10) (accept-process-output nil 0.02)) (process-status c)) (reverse ev)
        (prog1 (noport (process-contact c :remote)) (delete-process c) (delete-process s))))))
(show "d3" (lambda () (let* ((c (make-network-process :name "d3c" :host "localhost" :service 1 :nowait t :family 'ipv4)))
  (list (process-status c) (process-contact c t t) (progn (dotimes (_ 10) (accept-process-output nil 0.02)) (process-status c)) (process-exit-status c)))))
(show "d4" (lambda () (let* ((f (make-temp-name "/tmp/emx-dg-")) (s (make-network-process :name "d4s" :family 'local :service f :server t :type 'datagram)) (got nil))
  (set-process-filter s (lambda (_ str) (push str got)))
  (prog1 (let ((c (make-network-process :name "d4c" :family 'local :service f :type 'datagram)))
     (process-send-string c "dg")
     (dotimes (_ 10) (accept-process-output nil 0.02))
     (list (process-status s) got (equal (process-contact s :local) f) (equal (process-datagram-address c) f) (equal (process-contact c :remote) f)))
   (delete-file f))))))"##,
    ] {
        assert_same(&corpus, &["--batch", "-Q", "--eval", program]);
    }
}

#[test]
fn the_review_of_round_334_is_answered_as_gnu_answers() {
    // The review of round 334, each probe in its own session: names made
    // unique as make_process makes them; a failed status's data and its
    // message; the mark placed by connect_network_socket's
    // update_process_mark, not at creation; the *Messages* lines of the
    // socket and connection waits; the inherit flag a `:coding' clears;
    // the contact changed in place by plist_put (`:remote' of a datagram
    // process, `:filter', `:sentinel'); a (FAMILY . VECTOR) longer than a
    // sockaddr_storage; a unibyte host's raw byte in the lookup's error;
    // set-process-datagram-address of a process that has no datagram
    // address.
    let corpus = Corpus::new();
    for program in [
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(defvar srv (make-network-process :name "srv" :host "127.0.0.1" :service t :server t))
(defvar port (process-contact srv :service))
(defvar b (get-buffer-create "pb"))
(with-current-buffer b (insert "abc"))
(show "n1" (lambda () (let ((x (make-network-process :name "srv" :host "127.0.0.1" :service t :server t)))
   (prog1 (process-name x) (delete-process x)))))
(show "k1" (lambda () (let* ((c (make-network-process :name "k1" :host "127.0.0.1" :service port :nowait t :buffer b)))
   (let ((m0 (marker-position (process-mark c))))
     (with-current-buffer b (insert "defgh"))
     (dotimes (_ 5) (accept-process-output nil 0.02))
     (prog1 (list m0 (marker-position (process-mark c)) (marker-buffer (process-mark c))) (delete-process c))))))
(show "k2" (lambda () (let* ((c (make-network-process :name "k2" :remote '(99 . [0 0]) :nowait t :buffer b)))
   (list (process-status c) (marker-position (process-mark c)) (marker-buffer (process-mark c))))))
(show "k3" (lambda () (let* ((c (make-network-process :name "k3" :host "127.0.0.1" :service port :buffer b)))
   (prog1 (list (marker-position (process-mark c))) (delete-process c)))))
(show "k4" (lambda () (let* ((c (make-network-process :name "k4" :host "no-such-host.invalid" :service port :nowait t :buffer b)))
   (dotimes (_ 20) (accept-process-output nil 0.05))
   (list (process-status c) (process-exit-status c) (marker-position (process-mark c)) (marker-buffer (process-mark c))))))
(show "k5" (lambda () (let* ((s (make-network-process :name "k5" :host "127.0.0.1" :service t :server t :buffer b)))
   (prog1 (list (marker-position (process-mark s))) (delete-process s))))))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(defun noport (v) (if (and (vectorp v) (memq (length v) '(5 9))) (vconcat (butlast (append v nil)) '(0)) v))
(defvar srv (make-network-process :name "srv" :host "127.0.0.1" :service t :server t))
(defvar port (process-contact srv :service))
(defun msgs () (with-current-buffer (messages-buffer) (prog1 (buffer-string) (let ((inhibit-read-only t)) (erase-buffer)))))
(msgs)
(show "m5" (lambda () (let* ((a (make-network-process :name "a5" :remote (vector 127 0 0 1 port) :nowait t)))
   (list (process-send-string a "x") (msgs)))))
(show "m6" (lambda () (let* ((a (make-network-process :name "a6" :remote (vector 127 0 0 1 port) :nowait t)))
   (list (set-process-datagram-address a [1 2 3]) (process-datagram-address a) (msgs))))))"##,
        r##"(progn
(defun show (tag form)
  (princ (format "%s %S\n" tag (condition-case err (funcall form) (error (list 'ERR err))))))
(setq inherit-process-coding-system t)
(show "h1" (lambda () (let ((s (make-network-process :name "h1" :host "127.0.0.1" :service t :server t :buffer "hb" :coding 'utf-8)))
   (prog1 (process-inherit-coding-system-flag s) (delete-process s)))))
(show "h3" (lambda () (let ((s (make-network-process :name "h3" :host "127.0.0.1" :service t :server t :buffer "hb")))
   (prog1 (process-inherit-coding-system-flag s) (delete-process s)))))
(show "v1" (lambda () (let ((p (make-network-process :name "v1" :remote (cons 2 (make-vector 200 0)))))
   (list (process-status p) (let ((r (process-contact p :remote))) (if (consp r) (list (car r) (length (cdr r))) r))))))
(show "s11" (lambda () (make-network-process :name "s11" :host (string-to-unibyte "h\377x") :service 1)))
(show "e1" (lambda () (let* ((ev nil) (c (make-network-process :name "e1" :host "127.0.0.1" :service 1 :nowait t
                                        :sentinel (lambda (_ e) (push e ev)))))
   (dotimes (_ 10) (accept-process-output nil 0.02))
   (list (process-status c) (process-exit-status c) (reverse ev)))))
(show "i3" (lambda () (let* ((d (make-network-process :name "d" :host "127.0.0.1" :service 9 :type 'datagram))
                             (held (process-contact d t)))
   (set-process-datagram-address d [127 0 0 1 7])
   (let ((now (process-contact d t)))
     (list (eq held now) (plist-get now :remote))))))
(show "f1" (lambda () (let* ((s (make-network-process :name "f1" :host "127.0.0.1" :service t :server t)))
   (set-process-filter s #'ignore)
   (set-process-sentinel s #'ignore)
   (prog1 (list (plist-get (process-contact s t) :filter) (plist-get (process-contact s t) :sentinel))
     (delete-process s))))))"##,
    ] {
        assert_same(&corpus, &["--batch", "-Q", "--eval", program]);
    }
}
