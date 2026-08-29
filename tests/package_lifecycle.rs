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
