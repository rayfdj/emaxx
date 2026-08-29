#![allow(clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

struct TempTree(PathBuf);

impl TempTree {
    fn new(stem: &str) -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{stem}-{}-{unique}", std::process::id()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempTree {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct GitPackage {
    repository: PathBuf,
    revision_one: String,
}

fn run_git(repository: &Path, args: &[&str]) -> Output {
    Command::new("git")
        .current_dir(repository)
        .env("LANG", "C")
        .args(args)
        .output()
        .unwrap()
}

fn git_ok(repository: &Path, args: &[&str]) -> Output {
    let output = run_git(repository, args);
    assert!(
        output.status.success(),
        "git {} failed in {}:\nstdout: {}\nstderr: {}",
        args.join(" "),
        repository.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn git_package_source(version: u8) -> String {
    format!(
        ";;; journey-vc.el --- deterministic package-vc fixture -*- lexical-binding: t; -*-\n\
         ;; Version: {version}.0\n\
         ;;; Code:\n\
         ;;;###autoload\n\
         (defun journey-vc-command () (interactive) 'vc-{version})\n\
         (defun journey-vc-value () 'vc-{version})\n\
         (provide 'journey-vc)\n\
         ;;; journey-vc.el ends here\n"
    )
}

fn commit_git_package(repository: &Path, version: u8) -> String {
    std::fs::write(
        repository.join("journey-vc.el"),
        git_package_source(version),
    )
    .unwrap();
    git_ok(repository, &["add", "journey-vc.el"]);
    let timestamp = format!("2000-01-0{version}T00:00:00Z");
    let output = Command::new("git")
        .current_dir(repository)
        .env("LANG", "C")
        .env("GIT_AUTHOR_DATE", &timestamp)
        .env("GIT_COMMITTER_DATE", &timestamp)
        .args(["commit", "--quiet", "-m", &format!("version {version}")])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git commit failed in {}:\nstdout: {}\nstderr: {}",
        repository.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(git_ok(repository, &["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .to_string()
}

fn initialize_git_package(root: &Path) -> GitPackage {
    let repository = root.join("repository");
    std::fs::create_dir_all(&repository).unwrap();
    git_ok(&repository, &["init", "--quiet", "--initial-branch=main"]);
    git_ok(&repository, &["config", "user.name", "Emaxx Test"]);
    git_ok(
        &repository,
        &["config", "user.email", "emaxx-test@example.invalid"],
    );
    let revision_one = commit_git_package(&repository, 1);
    GitPackage {
        repository,
        revision_one,
    }
}

fn write_tar_field(field: &mut [u8], value: &[u8]) {
    field[..value.len()].copy_from_slice(value);
}

fn write_tar_octal(field: &mut [u8], value: u64) {
    let digits = format!("{:0width$o}", value, width = field.len() - 1);
    write_tar_field(field, digits.as_bytes());
}

fn append_tar_entry(archive: &mut Vec<u8>, name: &str, contents: &[u8], kind: u8) {
    let mut header = [0u8; 512];
    write_tar_field(&mut header[0..100], name.as_bytes());
    write_tar_octal(
        &mut header[100..108],
        if kind == b'5' { 0o755 } else { 0o644 },
    );
    write_tar_octal(&mut header[108..116], 0);
    write_tar_octal(&mut header[116..124], 0);
    write_tar_octal(&mut header[124..136], contents.len() as u64);
    write_tar_octal(&mut header[136..148], 946_684_800);
    header[148..156].fill(b' ');
    header[156] = kind;
    write_tar_field(&mut header[257..263], b"ustar\0");
    write_tar_field(&mut header[263..265], b"00");
    let checksum = header.iter().map(|byte| u64::from(*byte)).sum::<u64>();
    let checksum = format!("{checksum:06o}\0 ");
    write_tar_field(&mut header[148..156], checksum.as_bytes());
    archive.extend_from_slice(&header);
    archive.extend_from_slice(contents);
    let padding = (512 - contents.len() % 512) % 512;
    archive.resize(archive.len() + padding, 0);
}

fn package_source(name: &str, version: u8, requires: Option<&str>, body: &str) -> String {
    let requires = requires
        .map(|dependency| format!(";; Package-Requires: (({dependency} \"{version}.0\"))\n"))
        .unwrap_or_default();
    format!(
        ";;; {name}.el --- deterministic package lifecycle fixture -*- lexical-binding: t; -*-\n\
         ;; Version: {version}.0\n\
         {requires}\
         ;;; Code:\n\
         {body}\n\
         (provide '{name})\n\
         ;;; {name}.el ends here\n"
    )
}

fn write_archive(root: &Path, version: u8) {
    let archive = root.join("archive");
    std::fs::create_dir_all(&archive).unwrap();
    let contents = format!(
        "(1\n\
         (journey-single . [({version} 0) nil \"Single-file lifecycle fixture\" single])\n\
         (journey-dep . [({version} 0) ((journey-single ({version} 0))) \"Dependency lifecycle fixture\" single])\n\
         (journey-multi . [({version} 0) ((journey-dep ({version} 0))) \"Multi-file lifecycle fixture\" tar]))\n"
    );
    std::fs::write(archive.join("archive-contents"), contents).unwrap();

    let single = package_source(
        "journey-single",
        version,
        None,
        &format!(
            ";;;###autoload\n(defun journey-single-command () (interactive) 'single-{version})\n\
             (defgroup journey-single nil\n\
               \"Deterministic package fixture.\"\n\
               :group 'applications)\n\
             (defcustom journey-single-option 7\n\
               \"Custom value for the deterministic package fixture.\"\n\
               :type 'integer\n\
               :group 'journey-single)\n\
             (defvar journey-single-hook-log nil)\n\
             ;;;###autoload\n\
             (defun journey-single-on-probe ()\n\
               (push 'probe journey-single-hook-log))\n\
             (defun journey-single-value () 'single-{version})"
        ),
    );
    std::fs::write(
        archive.join(format!("journey-single-{version}.0.el")),
        single,
    )
    .unwrap();

    let dependency = package_source(
        "journey-dep",
        version,
        Some("journey-single"),
        &format!(
            "(require 'journey-single)\n\
             (defun journey-dep-value () (list 'dep-{version} (journey-single-value)))"
        ),
    );
    std::fs::write(
        archive.join(format!("journey-dep-{version}.0.el")),
        dependency,
    )
    .unwrap();

    let directory = format!("journey-multi-{version}.0/");
    let package_file = format!(
        "(define-package \"journey-multi\" \"{version}.0\" \
         \"Multi-file lifecycle fixture\" '((journey-dep \"{version}.0\")))\n"
    );
    let main = package_source(
        "journey-multi",
        version,
        Some("journey-dep"),
        &format!(
            "(require 'journey-dep)\n\
             (require 'journey-multi-extra)\n\
             ;;;###autoload\n(defun journey-multi-command () (interactive) 'multi-{version})\n\
             (defun journey-multi-value ()\n\
               (list 'multi-{version} (journey-dep-value) (journey-multi-extra-value)))"
        ),
    );
    let extra = package_source(
        "journey-multi-extra",
        version,
        None,
        &format!("(defun journey-multi-extra-value () 'extra-{version})"),
    );
    let mut tar = Vec::new();
    append_tar_entry(&mut tar, &directory, &[], b'5');
    append_tar_entry(
        &mut tar,
        &format!("{directory}journey-multi-pkg.el"),
        package_file.as_bytes(),
        b'0',
    );
    append_tar_entry(
        &mut tar,
        &format!("{directory}journey-multi.el"),
        main.as_bytes(),
        b'0',
    );
    append_tar_entry(
        &mut tar,
        &format!("{directory}journey-multi-extra.el"),
        extra.as_bytes(),
        b'0',
    );
    tar.resize(tar.len() + 1024, 0);
    std::fs::write(archive.join(format!("journey-multi-{version}.0.tar")), tar).unwrap();
}

fn lisp_string(path: &Path) -> String {
    serde_json::to_string(&path.display().to_string()).unwrap()
}

fn package_program(root: &Path, archive: &Path, body: &str) -> String {
    let user_dir = lisp_string(&root.join("home"));
    let package_dir = lisp_string(&root.join("packages"));
    let archive = lisp_string(archive);
    format!(
        "(progn\n\
           (setq user-emacs-directory (file-name-as-directory {user_dir})\n\
                 package-user-dir {package_dir})\n\
           (require 'package)\n\
           (setq package-archives\n\
                 (list (cons \"local\" (file-name-as-directory {archive}))))\n\
           {body})"
    )
}

fn run_phase(binary: &Path, root: &Path, archive: &Path, body: &str) -> Output {
    std::fs::create_dir_all(root.join("home")).unwrap();
    Command::new(binary)
        .env("HOME", root.join("home"))
        .env("LANG", "C")
        .args([
            "--no-init-file",
            "--no-site-file",
            "--no-site-lisp",
            "--batch",
            "--eval",
            &package_program(root, archive, body),
        ])
        .output()
        .unwrap()
}

fn vc_program(root: &Path, body: &str) -> String {
    let user_dir = lisp_string(&root.join("home"));
    let package_dir = lisp_string(&root.join("packages"));
    let custom_file = lisp_string(&root.join("custom.el"));
    format!(
        "(progn\n\
           (setq user-emacs-directory (file-name-as-directory {user_dir})\n\
                 package-user-dir {package_dir}\n\
                 custom-file {custom_file}\n\
                 package-archives nil\n\
                 package-native-compile nil)\n\
           (require 'package)\n\
           (package-initialize)\n\
           (require 'package-vc)\n\
           (setq package-vc-register-as-project nil)\n\
           {body})"
    )
}

fn run_vc_phase(binary: &Path, root: &Path, body: &str) -> Output {
    std::fs::create_dir_all(root.join("home")).unwrap();
    Command::new(binary)
        .env("HOME", root.join("home"))
        .env("LANG", "C")
        .args([
            "--no-init-file",
            "--no-site-file",
            "--no-site-lisp",
            "--batch",
            "--eval",
            &vc_program(root, body),
        ])
        .output()
        .unwrap()
}

fn compare_vc_phase(
    label: &str,
    oracle: &Path,
    oracle_root: &Path,
    oracle_body: &str,
    emaxx: &Path,
    emaxx_root: &Path,
    emaxx_body: &str,
) {
    let oracle_output = run_vc_phase(oracle, oracle_root, oracle_body);
    let emaxx_output = run_vc_phase(emaxx, emaxx_root, emaxx_body);
    assert!(
        oracle_output.status.success(),
        "GNU VC phase {label} failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&oracle_output.stdout),
        String::from_utf8_lossy(&oracle_output.stderr)
    );
    assert!(
        emaxx_output.status.success(),
        "Emaxx VC phase {label} failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&emaxx_output.stdout),
        String::from_utf8_lossy(&emaxx_output.stderr)
    );
    assert_eq!(
        emaxx_output.stdout,
        oracle_output.stdout,
        "VC phase {label} stdout differed:\nGNU stderr: {}\nEmaxx stderr: {}",
        String::from_utf8_lossy(&oracle_output.stderr),
        String::from_utf8_lossy(&emaxx_output.stderr)
    );
}

fn vc_install_form(repository: &Path, revision: Option<&str>) -> String {
    let repository = lisp_string(repository);
    let revision =
        revision.map_or_else(|| "nil".to_string(), |value| lisp_string(Path::new(value)));
    format!(
        "(package-vc-install\n\
           (cons 'journey-vc (list :url {repository} :vc-backend 'Git))\n\
           {revision})"
    )
}

fn vc_failure_body(marker: &str, install_form: &str) -> String {
    format!(
        "(let* ((outcome\n\
                  (condition-case error\n\
                      (progn {install_form} 'unexpected-success)\n\
                    (error (car error))))\n\
                (generated\n\
                 (and (file-directory-p package-user-dir)\n\
                      (directory-files-recursively\n\
                       package-user-dir\n\
                       \"\\\\(?:-autoloads\\\\|-pkg\\\\)\\\\.el\\\\'\\\\|\\\\.elc\\\\'\"))))\n\
           (unless (and (not (eq outcome 'unexpected-success))\n\
                        (not (package-installed-p 'journey-vc))\n\
                        (not (assq 'journey-vc package-alist))\n\
                        (not (featurep 'journey-vc))\n\
                        (not (fboundp 'journey-vc-command))\n\
                        (not generated))\n\
             (kill-emacs 78))\n\
           (prin1 (list '{marker} outcome\n\
                        (package-installed-p 'journey-vc)\n\
                        (assq 'journey-vc package-alist)\n\
                        (featurep 'journey-vc)\n\
                        (fboundp 'journey-vc-command)\n\
                        (and generated t))))"
    )
}

fn vc_failure_restart_body(marker: &str) -> String {
    format!(
        "(let ((generated\n\
                (and (file-directory-p package-user-dir)\n\
                     (directory-files-recursively\n\
                      package-user-dir\n\
                      \"\\\\(?:-autoloads\\\\|-pkg\\\\)\\\\.el\\\\'\\\\|\\\\.elc\\\\'\"))))\n\
           (unless (and (not (package-installed-p 'journey-vc))\n\
                        (not (assq 'journey-vc package-alist))\n\
                        (not (featurep 'journey-vc))\n\
                        (not (fboundp 'journey-vc-command))\n\
                        (not generated))\n\
             (kill-emacs 79))\n\
           (prin1 (list '{marker}\n\
                        (package-installed-p 'journey-vc)\n\
                        (assq 'journey-vc package-alist)\n\
                        (featurep 'journey-vc)\n\
                        (fboundp 'journey-vc-command)\n\
                        (and generated t))))"
    )
}

fn compare_phase(
    label: &str,
    oracle: &Path,
    oracle_root: &Path,
    emaxx: &Path,
    emaxx_root: &Path,
    archive_relative: &str,
    body: &str,
) {
    let oracle_output = run_phase(
        oracle,
        oracle_root,
        &oracle_root.join(archive_relative),
        body,
    );
    let emaxx_output = run_phase(emaxx, emaxx_root, &emaxx_root.join(archive_relative), body);
    assert!(
        oracle_output.status.success(),
        "GNU phase {label} failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&oracle_output.stdout),
        String::from_utf8_lossy(&oracle_output.stderr)
    );
    assert!(
        emaxx_output.status.success(),
        "Emaxx phase {label} failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&emaxx_output.stdout),
        String::from_utf8_lossy(&emaxx_output.stderr)
    );
    assert_eq!(
        emaxx_output.stdout,
        oracle_output.stdout,
        "phase {label} stdout differed:\nGNU stderr: {}\nEmaxx stderr: {}",
        String::from_utf8_lossy(&oracle_output.stderr),
        String::from_utf8_lossy(&emaxx_output.stderr)
    );
}

#[test]
fn local_package_archive_lifecycle_matches_gnu_across_restarts_and_failures() {
    let oracle = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../emacs/src/emacs");
    assert!(oracle.is_file(), "missing GNU oracle: {}", oracle.display());
    let emaxx = PathBuf::from(env!("CARGO_BIN_EXE_emaxx"));
    let oracle_root = TempTree::new("emaxx-package-oracle");
    let emaxx_root = TempTree::new("emaxx-package-subject");
    write_archive(&oracle_root.0, 1);
    write_archive(&emaxx_root.0, 1);

    compare_phase(
        "install",
        &oracle,
        &oracle_root.0,
        &emaxx,
        &emaxx_root.0,
        "archive",
        r#"
          (unless (eq package-check-signature 'allow-unsigned) (kill-emacs 61))
          (package-initialize)
          (package-refresh-contents)
          (let ((available (cadr (assq 'journey-multi
                                       package-archive-contents))))
            (unless (and (eq (package-desc-kind available) 'tar)
                         (equal (package-desc-archive available) "local")
                         (equal (package-desc-reqs available)
                                '((journey-dep (1 0)))))
              (kill-emacs 69)))
          (package-install 'journey-multi)
          (require 'journey-multi)
          (let* ((single (cadr (assq 'journey-single package-alist)))
                 (multi (cadr (assq 'journey-multi package-alist)))
                 (signature-error
                  (condition-case error
                      (progn
                        (package--check-signature-content
                         "not an OpenPGP signature\n" "package payload"
                         "journey-invalid.sig")
                        'accepted)
                    (error (car error)))))
            (unless (and (equal (journey-multi-value)
                                '(multi-1 (dep-1 single-1) extra-1))
                         (file-exists-p
                          (expand-file-name "journey-single-autoloads.el"
                                            (package-desc-dir single)))
                         (file-exists-p
                          (expand-file-name "journey-multi-autoloads.el"
                                            (package-desc-dir multi)))
                         (file-exists-p
                          (expand-file-name "journey-multi.elc"
                                            (package-desc-dir multi)))
                         ;; Which error a garbage signature raises depends
                         ;; on the host's gpg (epg-error on some, package.el's
                         ;; bad-signature on others).  Assert only that
                         ;; verification REFUSED; the prin1 below feeds the
                         ;; exact symbol to the GNU-vs-emaxx stdout
                         ;; comparison, which pins it per host.
                         (not (eq signature-error 'accepted))
                         (equal (sort (directory-files
                                       package-user-dir nil "\\`journey-" t)
                                      #'string<)
                                '("journey-dep-1.0"
                                  "journey-multi-1.0"
                                  "journey-single-1.0"))
                         (eq package-check-signature 'allow-unsigned))
              (kill-emacs 62))
            (prin1 (list 'installed (journey-multi-value)
                         (package-desc-version multi)
                         'archive-metadata-verified
                         signature-error package-check-signature)))
        "#,
    );

    compare_phase(
        "restart",
        &oracle,
        &oracle_root.0,
        &emaxx,
        &emaxx_root.0,
        "archive",
        r#"
          (package-initialize)
          (require 'journey-multi)
          (unless (and (package-installed-p 'journey-single '(1 0))
                       (package-installed-p 'journey-dep '(1 0))
                       (package-installed-p 'journey-multi '(1 0))
                       (equal (journey-multi-value)
                              '(multi-1 (dep-1 single-1) extra-1)))
            (kill-emacs 63))
          (prin1 (list 'restarted (journey-multi-value)
                       (fboundp 'journey-multi-command)))
        "#,
    );

    write_archive(&oracle_root.0, 2);
    write_archive(&emaxx_root.0, 2);
    compare_phase(
        "upgrade-reinstall",
        &oracle,
        &oracle_root.0,
        &emaxx,
        &emaxx_root.0,
        "archive",
        r#"
          (package-initialize)
          (package-refresh-contents)
          (mapc #'package-upgrade
                '(journey-single journey-dep journey-multi))
          (require 'journey-multi)
          (package-reinstall 'journey-multi)
          (unless (and (package-installed-p 'journey-single '(2 0))
                       (package-installed-p 'journey-dep '(2 0))
                       (package-installed-p 'journey-multi '(2 0))
                       (equal (journey-multi-value)
                              '(multi-2 (dep-2 single-2) extra-2))
                       (equal (sort (directory-files
                                     package-user-dir nil "\\`journey-" t)
                                    #'string<)
                              '("journey-dep-2.0"
                                "journey-multi-2.0"
                                "journey-single-2.0")))
            (kill-emacs 64))
          (prin1 (list 'upgraded (journey-multi-value)
                       (mapcar (lambda (name)
                                 (package-desc-version
                                  (cadr (assq name package-alist))))
                               '(journey-single journey-dep journey-multi))))
        "#,
    );

    std::fs::write(oracle_root.0.join("archive/archive-contents"), "(").unwrap();
    std::fs::write(emaxx_root.0.join("archive/archive-contents"), "(").unwrap();
    compare_phase(
        "corrupt-archive-rollback",
        &oracle,
        &oracle_root.0,
        &emaxx,
        &emaxx_root.0,
        "archive",
        r#"
          (package-initialize)
          (let ((before (copy-tree package-archive-contents)))
            (package-refresh-contents)
            (require 'journey-multi)
            (unless (and (equal before package-archive-contents)
                         (equal (journey-multi-value)
                                '(multi-2 (dep-2 single-2) extra-2)))
              (kill-emacs 65))
            (prin1 (list 'corrupt
                         (mapcar #'car before)
                         (mapcar #'car package-archive-contents)
                         (journey-multi-value))))
        "#,
    );

    compare_phase(
        "unreachable-archive-rollback",
        &oracle,
        &oracle_root.0,
        &emaxx,
        &emaxx_root.0,
        "missing-archive",
        r#"
          (package-initialize)
          (let ((before (copy-tree package-archive-contents)))
            (package-refresh-contents)
            (require 'journey-multi)
            (unless (and (equal before package-archive-contents)
                         (equal (journey-multi-value)
                                '(multi-2 (dep-2 single-2) extra-2)))
              (kill-emacs 66))
            (prin1 (list 'unreachable
                         (mapcar #'car before)
                         (mapcar #'car package-archive-contents)
                         (journey-multi-value))))
        "#,
    );

    write_archive(&oracle_root.0, 2);
    write_archive(&emaxx_root.0, 2);
    compare_phase(
        "delete",
        &oracle,
        &oracle_root.0,
        &emaxx,
        &emaxx_root.0,
        "archive",
        r#"
          (package-initialize)
          (dolist (name '(journey-multi journey-dep journey-single))
            (package-delete (cadr (assq name package-alist)) 'force))
          (unless (not (or (package-installed-p 'journey-single)
                           (package-installed-p 'journey-dep)
                           (package-installed-p 'journey-multi)
                           (directory-files package-user-dir nil
                                            "\\`journey-" t)))
            (kill-emacs 67))
          (prin1 (list 'deleted
                       (mapcar #'package-installed-p
                               '(journey-single journey-dep journey-multi))))
        "#,
    );

    compare_phase(
        "restart-after-delete",
        &oracle,
        &oracle_root.0,
        &emaxx,
        &emaxx_root.0,
        "archive",
        r#"
          (package-initialize)
          (unless (not (or (package-installed-p 'journey-single)
                           (package-installed-p 'journey-dep)
                           (package-installed-p 'journey-multi)
                           (directory-files package-user-dir nil
                                            "\\`journey-" t)))
            (kill-emacs 68))
          (prin1 (list 'restart-after-delete
                       (mapcar #'package-installed-p
                               '(journey-single journey-dep journey-multi))))
        "#,
    );
}

#[test]
fn local_use_package_workflows_match_gnu_across_defer_and_failure() {
    let oracle = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../emacs/src/emacs");
    assert!(oracle.is_file(), "missing GNU oracle: {}", oracle.display());
    let emaxx = PathBuf::from(env!("CARGO_BIN_EXE_emaxx"));
    let oracle_root = TempTree::new("emaxx-use-package-oracle");
    let emaxx_root = TempTree::new("emaxx-use-package-subject");
    write_archive(&oracle_root.0, 1);
    write_archive(&emaxx_root.0, 1);

    let journey_body = r#"
      (package-initialize)
      (package-refresh-contents)
      (require 'use-package)
      (use-package journey-single
        :ensure t
        :defer t
        :commands journey-single-command
        :hook (journey-probe . journey-single-on-probe)
        :bind ("C-c j" . journey-single-command)
        :custom (journey-single-option 42))
      (let ((deferred (not (featurep 'journey-single))))
        (unless (and deferred
                     (package-installed-p 'journey-single '(1 0))
                     (autoloadp (symbol-function 'journey-single-command))
                     (autoloadp (symbol-function 'journey-single-on-probe))
                     (eq (lookup-key global-map (kbd "C-c j"))
                         'journey-single-command)
                     (memq 'journey-single-on-probe journey-probe-hook)
                     (not (boundp 'journey-single-option)))
          (kill-emacs 82))
        (run-hooks 'journey-probe-hook)
        (unless (and (featurep 'journey-single)
                     (equal journey-single-hook-log '(probe))
                     (eq (journey-single-command) 'single-1)
                     (equal journey-single-option 42))
          (kill-emacs 83))
        (prin1 (list 'use-package-journey
                     deferred
                     (featurep 'journey-single)
                     (journey-single-command)
                     journey-single-hook-log
                     (lookup-key global-map (kbd "C-c j"))
                     journey-single-option)))
    "#;
    compare_phase(
        "use-package-ensure-defer",
        &oracle,
        &oracle_root.0,
        &emaxx,
        &emaxx_root.0,
        "archive",
        journey_body,
    );

    let restart_body = r#"
      (package-initialize)
      (require 'use-package)
      (use-package journey-single
        :ensure t
        :defer t
        :commands journey-single-command
        :hook (journey-probe . journey-single-on-probe)
        :bind ("C-c j" . journey-single-command)
        :custom (journey-single-option 42))
      (let ((deferred (not (featurep 'journey-single))))
        (unless (and deferred
                     (package-installed-p 'journey-single '(1 0))
                     (autoloadp (symbol-function 'journey-single-command))
                     (eq (lookup-key global-map (kbd "C-c j"))
                         'journey-single-command)
                     (memq 'journey-single-on-probe journey-probe-hook)
                     (not (boundp 'journey-single-option))
                     (eq (journey-single-command) 'single-1)
                     (featurep 'journey-single)
                     (equal journey-single-option 42))
          (kill-emacs 84))
        (prin1 (list 'use-package-restarted
                     deferred
                     (featurep 'journey-single)
                     (journey-single-command)
                     journey-single-option)))
    "#;
    compare_phase(
        "use-package-restart",
        &oracle,
        &oracle_root.0,
        &emaxx,
        &emaxx_root.0,
        "archive",
        restart_body,
    );

    let absent_body = r#"
      (package-initialize)
      (require 'cl-lib)
      (require 'use-package)
      (let (warnings)
        (cl-letf (((symbol-function #'display-warning)
                   (lambda (type message &optional level _buffer-name)
                     (push (list type level
                                 (and (string-match-p
                                       "Failed to install journey-absent"
                                       message)
                                      t))
                           warnings))))
          (use-package journey-absent
            :ensure t
            :defer t
            :commands journey-absent-command))
        (setq warnings (nreverse warnings))
        (let ((call-condition
               (condition-case error
                   (progn (journey-absent-command) 'unexpected-success)
                 (error (car error)))))
          (unless (and (equal warnings '((use-package :error t)))
                       (eq call-condition 'file-missing)
                       (not (package-installed-p 'journey-absent))
                       (not (assq 'journey-absent package-alist))
                       (not (featurep 'journey-absent))
                       (autoloadp
                        (symbol-function 'journey-absent-command))
                       (not (directory-files package-user-dir nil
                                             "\\`journey-absent" t)))
            (kill-emacs 85))
          (prin1 (list 'use-package-absent
                       warnings call-condition
                       (package-installed-p 'journey-absent)
                       (featurep 'journey-absent)
                       (autoloadp
                        (symbol-function 'journey-absent-command))))))
    "#;
    compare_phase(
        "use-package-absence",
        &oracle,
        &oracle_root.0,
        &emaxx,
        &emaxx_root.0,
        "archive",
        absent_body,
    );

    let absent_restart_body = r#"
      (package-initialize)
      (unless (and (not (package-installed-p 'journey-absent))
                   (not (assq 'journey-absent package-alist))
                   (not (featurep 'journey-absent))
                   (not (fboundp 'journey-absent-command))
                   (not (directory-files package-user-dir nil
                                         "\\`journey-absent" t)))
        (kill-emacs 86))
      (prin1 (list 'use-package-absent-restart
                   (package-installed-p 'journey-absent)
                   (featurep 'journey-absent)
                   (fboundp 'journey-absent-command)))
    "#;
    compare_phase(
        "use-package-absence-restart",
        &oracle,
        &oracle_root.0,
        &emaxx,
        &emaxx_root.0,
        "archive",
        absent_restart_body,
    );
}

#[test]
fn local_package_vc_lifecycle_matches_gnu_across_restarts_and_deletion() {
    let oracle = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../emacs/src/emacs");
    assert!(oracle.is_file(), "missing GNU oracle: {}", oracle.display());
    let emaxx = PathBuf::from(env!("CARGO_BIN_EXE_emaxx"));
    let oracle_root = TempTree::new("emaxx-package-vc-oracle");
    let emaxx_root = TempTree::new("emaxx-package-vc-subject");
    let oracle_package = initialize_git_package(&oracle_root.0);
    let emaxx_package = initialize_git_package(&emaxx_root.0);
    let oracle_revision_two = commit_git_package(&oracle_package.repository, 2);
    let emaxx_revision_two = commit_git_package(&emaxx_package.repository, 2);
    assert_eq!(oracle_package.revision_one, emaxx_package.revision_one);
    assert_eq!(oracle_revision_two, emaxx_revision_two);

    let install_body = |package: &GitPackage| {
        format!(
            "{}\n\
             (require 'journey-vc)\n\
             (let* ((desc (cadr (assq 'journey-vc package-alist)))\n\
                    (main (expand-file-name \"journey-vc.el\"\n\
                                            (package-desc-dir desc))))\n\
               (unless (and (package-vc-p desc)\n\
                            (equal (package-desc-version desc) '(1 0))\n\
                            (string= (vc-working-revision main) {})\n\
                            (eq (journey-vc-value) 'vc-1)\n\
                            (eq (journey-vc-command) 'vc-1)\n\
                            (file-exists-p\n\
                             (expand-file-name \"journey-vc-autoloads.el\"\n\
                                               (package-desc-dir desc)))\n\
                            (file-exists-p\n\
                             (expand-file-name \"journey-vc-pkg.el\"\n\
                                               (package-desc-dir desc)))\n\
                            (memq 'journey-vc package-selected-packages))\n\
                 (kill-emacs 71))\n\
               (prin1 (list 'vc-installed\n\
                            (package-desc-kind desc)\n\
                            (package-desc-version desc)\n\
                            (journey-vc-value))))",
            vc_install_form(&package.repository, Some(&package.revision_one)),
            lisp_string(Path::new(&package.revision_one))
        )
    };
    compare_vc_phase(
        "pinned-install",
        &oracle,
        &oracle_root.0,
        &install_body(&oracle_package),
        &emaxx,
        &emaxx_root.0,
        &install_body(&emaxx_package),
    );

    let restart_body = r#"
      (require 'journey-vc)
      (let ((desc (cadr (assq 'journey-vc package-alist))))
        (unless (and (package-vc-p desc)
                     (package-installed-p 'journey-vc '(1 0))
                     (eq (journey-vc-value) 'vc-1)
                     (eq (journey-vc-command) 'vc-1))
          (kill-emacs 72))
        (prin1 (list 'vc-restarted (package-desc-version desc)
                     (journey-vc-value))))
    "#;
    compare_vc_phase(
        "restart-activation",
        &oracle,
        &oracle_root.0,
        restart_body,
        &emaxx,
        &emaxx_root.0,
        restart_body,
    );

    let delete_body = r#"
      (let ((desc (cadr (assq 'journey-vc package-alist))))
        (package-delete desc 'force)
        (unless (and (not (package-installed-p 'journey-vc))
                     (not (assq 'journey-vc package-alist))
                     (not (file-exists-p (package-desc-dir desc))))
          (kill-emacs 73))
        (prin1 (list 'vc-deleted
                     (package-installed-p 'journey-vc)
                     (assq 'journey-vc package-alist))))
    "#;
    compare_vc_phase(
        "delete",
        &oracle,
        &oracle_root.0,
        delete_body,
        &emaxx,
        &emaxx_root.0,
        delete_body,
    );

    let restart_after_delete_body = r#"
      (unless (and (not (package-installed-p 'journey-vc))
                   (not (assq 'journey-vc package-alist))
                   (not (locate-library "journey-vc")))
        (kill-emacs 74))
      (prin1 (list 'vc-restart-after-delete
                   (package-installed-p 'journey-vc)
                   (featurep 'journey-vc)))
    "#;
    compare_vc_phase(
        "restart-after-delete",
        &oracle,
        &oracle_root.0,
        restart_after_delete_body,
        &emaxx,
        &emaxx_root.0,
        restart_after_delete_body,
    );
}

#[test]
fn local_package_vc_upgrade_matches_gnu_and_survives_restart() {
    let oracle = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../emacs/src/emacs");
    assert!(oracle.is_file(), "missing GNU oracle: {}", oracle.display());
    let emaxx = PathBuf::from(env!("CARGO_BIN_EXE_emaxx"));
    let oracle_root = TempTree::new("emaxx-package-vc-upgrade-oracle");
    let emaxx_root = TempTree::new("emaxx-package-vc-upgrade-subject");
    let oracle_package = initialize_git_package(&oracle_root.0);
    let emaxx_package = initialize_git_package(&emaxx_root.0);
    assert_eq!(oracle_package.revision_one, emaxx_package.revision_one);

    let install_body = |package: &GitPackage| {
        format!(
            "{}\n\
             (require 'journey-vc)\n\
             (let* ((desc (cadr (assq 'journey-vc package-alist)))\n\
                    (main (expand-file-name \"journey-vc.el\"\n\
                                            (package-desc-dir desc))))\n\
               (unless (and (string= (vc-working-revision main) {})\n\
                            (eq (journey-vc-value) 'vc-1))\n\
                 (kill-emacs 75))\n\
               (prin1 (list 'vc-before-upgrade\n\
                            (package-desc-version desc)\n\
                            (journey-vc-value))))",
            vc_install_form(&package.repository, None),
            lisp_string(Path::new(&package.revision_one))
        )
    };
    compare_vc_phase(
        "install-upgrade-source",
        &oracle,
        &oracle_root.0,
        &install_body(&oracle_package),
        &emaxx,
        &emaxx_root.0,
        &install_body(&emaxx_package),
    );

    let oracle_revision_two = commit_git_package(&oracle_package.repository, 2);
    let emaxx_revision_two = commit_git_package(&emaxx_package.repository, 2);
    assert_eq!(oracle_revision_two, emaxx_revision_two);
    let upgrade_body = |revision: &str| {
        format!(
            "(let ((desc (cadr (assq 'journey-vc package-alist))))\n\
               (package-vc-upgrade desc)\n\
               (while (seq-some #'process-live-p (process-list))\n\
                 (accept-process-output nil 0.05)))\n\
             (require 'journey-vc)\n\
             (let* ((desc (cadr (assq 'journey-vc package-alist)))\n\
                    (main (expand-file-name \"journey-vc.el\"\n\
                                            (package-desc-dir desc))))\n\
               (unless (and (equal (package-desc-version desc) '(2 0))\n\
                            (string= (vc-working-revision main) {})\n\
                            (eq (journey-vc-value) 'vc-2))\n\
                 (kill-emacs 76))\n\
               (prin1 (list 'vc-upgraded\n\
                            (package-desc-version desc)\n\
                            (journey-vc-value))))",
            lisp_string(Path::new(revision))
        )
    };
    compare_vc_phase(
        "upgrade",
        &oracle,
        &oracle_root.0,
        &upgrade_body(&oracle_revision_two),
        &emaxx,
        &emaxx_root.0,
        &upgrade_body(&emaxx_revision_two),
    );

    let restart_body = r#"
      (require 'journey-vc)
      (let ((desc (cadr (assq 'journey-vc package-alist))))
        (unless (and (package-installed-p 'journey-vc '(2 0))
                     (equal (package-desc-version desc) '(2 0))
                     (eq (journey-vc-value) 'vc-2))
          (kill-emacs 77))
        (prin1 (list 'vc-upgrade-restarted
                     (package-desc-version desc)
                     (journey-vc-value))))
    "#;
    compare_vc_phase(
        "upgrade-restart",
        &oracle,
        &oracle_root.0,
        restart_body,
        &emaxx,
        &emaxx_root.0,
        restart_body,
    );
}

#[test]
fn local_package_vc_failures_match_gnu_without_activation_state() {
    let oracle = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../emacs/src/emacs");
    assert!(oracle.is_file(), "missing GNU oracle: {}", oracle.display());
    let emaxx = PathBuf::from(env!("CARGO_BIN_EXE_emaxx"));

    let run_failure = |label: &str,
                       oracle_root: &TempTree,
                       oracle_body: String,
                       emaxx_root: &TempTree,
                       emaxx_body: String| {
        compare_vc_phase(
            label,
            &oracle,
            &oracle_root.0,
            &oracle_body,
            &emaxx,
            &emaxx_root.0,
            &emaxx_body,
        );
        let restart_marker = format!("{label}-restart");
        let restart_body = vc_failure_restart_body(&restart_marker);
        compare_vc_phase(
            &restart_marker,
            &oracle,
            &oracle_root.0,
            &restart_body,
            &emaxx,
            &emaxx_root.0,
            &restart_body,
        );
    };

    let missing_git_oracle_root = TempTree::new("emaxx-vc-missing-git-oracle");
    let missing_git_emaxx_root = TempTree::new("emaxx-vc-missing-git-subject");
    let missing_git_oracle_package = initialize_git_package(&missing_git_oracle_root.0);
    let missing_git_emaxx_package = initialize_git_package(&missing_git_emaxx_root.0);
    let missing_git_install = |package: &GitPackage| {
        format!(
            "(progn (require 'vc-git)\n\
                    (let ((vc-git-program \"emaxx-definitely-missing-git\"))\n\
                      {}))",
            vc_install_form(&package.repository, None)
        )
    };
    run_failure(
        "missing-git",
        &missing_git_oracle_root,
        vc_failure_body(
            "vc-missing-git",
            &missing_git_install(&missing_git_oracle_package),
        ),
        &missing_git_emaxx_root,
        vc_failure_body(
            "vc-missing-git",
            &missing_git_install(&missing_git_emaxx_package),
        ),
    );

    let invalid_revision_oracle_root = TempTree::new("emaxx-vc-invalid-revision-oracle");
    let invalid_revision_emaxx_root = TempTree::new("emaxx-vc-invalid-revision-subject");
    let invalid_revision_oracle_package = initialize_git_package(&invalid_revision_oracle_root.0);
    let invalid_revision_emaxx_package = initialize_git_package(&invalid_revision_emaxx_root.0);
    let invalid_revision_body = |package: &GitPackage| {
        format!(
            "{}\n\
             (require 'journey-vc)\n\
             (let* ((desc (cadr (assq 'journey-vc package-alist)))\n\
                    (main (expand-file-name \"journey-vc.el\"\n\
                                            (package-desc-dir desc))))\n\
               (unless (and (package-vc-p desc)\n\
                            (string= (vc-working-revision main) {})\n\
                            (eq (journey-vc-value) 'vc-1))\n\
                 (kill-emacs 80))\n\
               (package-delete desc 'force)\n\
               (unless (and (not (package-installed-p 'journey-vc))\n\
                            (not (file-exists-p (package-desc-dir desc))))\n\
                 (kill-emacs 81))\n\
               (prin1 (list 'vc-invalid-revision-fell-back-to-head\n\
                            (journey-vc-value)\n\
                            (package-installed-p 'journey-vc))))",
            vc_install_form(
                &package.repository,
                Some("refs/heads/emaxx-invalid-revision"),
            ),
            lisp_string(Path::new(&package.revision_one))
        )
    };
    compare_vc_phase(
        "invalid-revision",
        &oracle,
        &invalid_revision_oracle_root.0,
        &invalid_revision_body(&invalid_revision_oracle_package),
        &emaxx,
        &invalid_revision_emaxx_root.0,
        &invalid_revision_body(&invalid_revision_emaxx_package),
    );
    let invalid_restart = vc_failure_restart_body("invalid-revision-restart");
    compare_vc_phase(
        "invalid-revision-restart",
        &oracle,
        &invalid_revision_oracle_root.0,
        &invalid_restart,
        &emaxx,
        &invalid_revision_emaxx_root.0,
        &invalid_restart,
    );

    let clone_failure_oracle_root = TempTree::new("emaxx-vc-clone-failure-oracle");
    let clone_failure_emaxx_root = TempTree::new("emaxx-vc-clone-failure-subject");
    run_failure(
        "clone-failure",
        &clone_failure_oracle_root,
        vc_failure_body(
            "vc-clone-failure",
            &vc_install_form(
                &clone_failure_oracle_root.0.join("missing-repository"),
                None,
            ),
        ),
        &clone_failure_emaxx_root,
        vc_failure_body(
            "vc-clone-failure",
            &vc_install_form(&clone_failure_emaxx_root.0.join("missing-repository"), None),
        ),
    );

    let build_command_oracle_root = TempTree::new("emaxx-vc-build-command-oracle");
    let build_command_emaxx_root = TempTree::new("emaxx-vc-build-command-subject");
    let build_command_oracle_package = initialize_git_package(&build_command_oracle_root.0);
    let build_command_emaxx_package = initialize_git_package(&build_command_emaxx_root.0);
    let build_command_body = |package: &GitPackage| {
        format!(
            "(let ((package-vc-allow-build-commands t))\n\
               (package-vc-install\n\
                (cons 'journey-vc\n\
                      (list :url {} :vc-backend 'Git\n\
                            :shell-command\n\
                            \"printf build-attempted; exit 17\"))\n\
                nil))\n\
             (let ((build-log\n\
                    (with-current-buffer \" *package-vc make journey-vc*\"\n\
                      (buffer-string))))\n\
               (require 'journey-vc)\n\
               (let ((desc (cadr (assq 'journey-vc package-alist))))\n\
                 (unless (and (string-match-p \"build-attempted\" build-log)\n\
                              (package-vc-p desc)\n\
                              (eq (journey-vc-value) 'vc-1))\n\
                   (kill-emacs 87))\n\
                 (package-delete desc 'force)\n\
                 (unless (and (not (package-installed-p 'journey-vc))\n\
                              (not (file-exists-p (package-desc-dir desc))))\n\
                   (kill-emacs 88))\n\
                 (prin1 (list 'vc-build-command-failure\n\
                              (and (string-match-p\n\
                                    \"build-attempted\" build-log) t)\n\
                              (journey-vc-value)\n\
                              (package-installed-p 'journey-vc)))))",
            lisp_string(&package.repository)
        )
    };
    compare_vc_phase(
        "build-command-failure",
        &oracle,
        &build_command_oracle_root.0,
        &build_command_body(&build_command_oracle_package),
        &emaxx,
        &build_command_emaxx_root.0,
        &build_command_body(&build_command_emaxx_package),
    );
    let build_command_restart = vc_failure_restart_body("build-command-failure-restart");
    compare_vc_phase(
        "build-command-failure-restart",
        &oracle,
        &build_command_oracle_root.0,
        &build_command_restart,
        &emaxx,
        &build_command_emaxx_root.0,
        &build_command_restart,
    );

    let build_failure_oracle_root = TempTree::new("emaxx-vc-build-failure-oracle");
    let build_failure_emaxx_root = TempTree::new("emaxx-vc-build-failure-subject");
    let build_failure_oracle_package = initialize_git_package(&build_failure_oracle_root.0);
    let build_failure_emaxx_package = initialize_git_package(&build_failure_emaxx_root.0);
    let build_failure_install = |package: &GitPackage| {
        format!(
            "(package-vc-install\n\
               (cons 'journey-vc\n\
                     (list :url {} :vc-backend 'Git\n\
                           :lisp-dir \"missing-lisp-directory\"))\n\
               nil)",
            lisp_string(&package.repository)
        )
    };
    run_failure(
        "build-failure",
        &build_failure_oracle_root,
        vc_failure_body(
            "vc-build-failure",
            &build_failure_install(&build_failure_oracle_package),
        ),
        &build_failure_emaxx_root,
        vc_failure_body(
            "vc-build-failure",
            &build_failure_install(&build_failure_emaxx_package),
        ),
    );
}
