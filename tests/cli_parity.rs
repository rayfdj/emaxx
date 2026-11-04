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
