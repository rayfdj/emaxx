#!/usr/bin/env python3
"""Opt-in GNU/Emaxx package.el canary against live public archives.

Nothing in this module opens the network unless ``--live`` is supplied.
Each editor gets a fresh HOME and package directory, refreshes the same three
HTTPS archives, and runs the same pinned install/restart/remove journey.
"""

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
from typing import Dict, Iterable, List, Mapping, NamedTuple, Optional, Sequence, Tuple


ARCHIVES = {
    "gnu": "https://elpa.gnu.org/packages/",
    "nongnu": "https://elpa.nongnu.org/nongnu/",
    "melpa": "https://melpa.org/packages/",
}


class Target(NamedTuple):
    archive: str
    package: str
    feature: str
    version: str


TARGETS = (
    Target("gnu", "compat", "compat", "31.0.0.2"),
    Target("nongnu", "rainbow-delimiters", "rainbow-delimiters", "2.1.5"),
    Target("melpa", "ht", "ht", "20230703.558"),
)

IDENTIFIER = re.compile(r"\A[a-zA-Z0-9+*/_:.<>=!?-]+\Z")
MARKER = "CANARY\t"
ERROR_MARKER = "CANARY_ERROR\t"


class PhaseResult(NamedTuple):
    editor: str
    phase: str
    returncode: int
    duration_seconds: float
    stdout: str
    stderr: str
    records: Dict[str, List[str]]
    protocol_error: Optional[str]

    @property
    def ok(self) -> bool:
        return self.returncode == 0 and self.protocol_error is None


def lisp_string(value: object) -> str:
    """Return VALUE as an Emacs Lisp string literal."""
    return json.dumps(str(value), ensure_ascii=True)


def validate_target(target: Target) -> None:
    if target.archive not in ARCHIVES:
        raise ValueError("unknown archive in target: %s" % target.archive)
    for label, value in (
        ("package", target.package),
        ("feature", target.feature),
    ):
        if not IDENTIFIER.fullmatch(value):
            raise ValueError("invalid %s name: %s" % (label, value))
    if not re.fullmatch(r"[0-9]+(?:\.[0-9]+)*", target.version):
        raise ValueError("invalid target version: %s" % target.version)


def parse_target(value: str) -> Target:
    parts = value.split(":")
    if len(parts) != 4:
        raise argparse.ArgumentTypeError(
            "targets must be ARCHIVE:PACKAGE:FEATURE:VERSION"
        )
    target = Target(*parts)
    try:
        validate_target(target)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error
    return target


def validate_targets(targets: Sequence[Target]) -> None:
    for target in targets:
        validate_target(target)
    archives = [target.archive for target in targets]
    if sorted(archives) != sorted(ARCHIVES):
        raise ValueError("exactly one target is required for each public archive")


def target_lisp(targets: Sequence[Target]) -> str:
    rows = []
    for target in targets:
        rows.append(
            "(%s %s %s %s %s)"
            % (
                lisp_string(target.archive),
                lisp_string(ARCHIVES[target.archive]),
                target.package,
                target.feature,
                lisp_string(target.version),
            )
        )
    return "'(%s)" % " ".join(rows)


def common_lisp(root: Path, targets: Sequence[Target]) -> str:
    package_dir = root / "packages"
    home = root / "home"
    archives = " ".join(
        "(cons %s %s)" % (lisp_string(name), lisp_string(url))
        for name, url in ARCHIVES.items()
    )
    return r"""
      (setq user-emacs-directory (file-name-as-directory %s)
            package-user-dir %s
            package-archives (list %s))
      (require 'cl-lib)
      (require 'package)
      (defconst canary-targets %s)
      (defun canary-emit (key value)
        (princ (format "CANARY\t%%s\t%%s\n" key value)))
      (defun canary-file-sha256 (file)
        (with-temp-buffer
          (set-buffer-multibyte nil)
          (insert-file-contents-literally file)
          (secure-hash 'sha256 (current-buffer))))
      (defun canary-desc (archive package)
        (cl-find-if
         (lambda (desc) (equal (package-desc-archive desc) archive))
         (cdr (assq package package-archive-contents))))
      (defun canary-desc-record (desc)
        (format "%%s@%%s:%%s:%%S:%%S"
                (package-desc-name desc)
                (package-version-join (package-desc-version desc))
                (or (package-desc-archive desc) "")
                (package-desc-kind desc)
                (package-desc-reqs desc)))
      (defun canary-installed-descs ()
        (let (descs)
          (dolist (entry package-alist)
            (dolist (desc (cdr entry))
              (when (and (stringp (package-desc-dir desc))
                         (file-in-directory-p (package-desc-dir desc)
                                              package-user-dir))
                (push desc descs))))
          (sort descs
                (lambda (left right)
                  (string< (package-desc-full-name left)
                           (package-desc-full-name right))))))
      (defun canary-source-sha256 (directory)
        (let ((files
               (sort
                (cl-remove-if
                 (lambda (file)
                   (string-match-p "-autoloads\\.el\\'" file))
                 (directory-files-recursively directory "\\.el\\'"))
                #'string<)))
          (with-temp-buffer
            (set-buffer-multibyte nil)
            (dolist (file files)
              (insert (file-relative-name file directory) "\0")
              (insert-file-contents-literally file)
              (goto-char (point-max))
              (insert "\0"))
            (secure-hash 'sha256 (current-buffer)))))
      (defun canary-emit-installed ()
        (let ((descs (canary-installed-descs)))
          (canary-emit
           "installed.packages"
           (mapconcat #'package-desc-full-name descs ","))
          (dolist (desc descs)
            (let* ((full-name (package-desc-full-name desc))
                   (signature (expand-file-name
                               (concat full-name ".signed")
                               package-user-dir)))
              (canary-emit
               (concat "source." full-name ".sha256")
               (canary-source-sha256 (package-desc-dir desc)))
              (canary-emit
               (concat "compiled." full-name ".files")
               (mapconcat
                (lambda (file)
                  (file-relative-name file (package-desc-dir desc)))
                (sort (directory-files-recursively
                       (package-desc-dir desc) "\\.elc\\'")
                      #'string<)
                ","))
              (canary-emit
               (concat "signature." full-name ".present")
               (if (file-exists-p signature) "true" "false"))
              (when (file-exists-p signature)
                (canary-emit
                 (concat "signature." full-name ".sha256")
                 (canary-file-sha256 signature)))))))
    """ % (
        lisp_string(str(home) + os.sep),
        lisp_string(str(package_dir)),
        archives,
        target_lisp(targets),
    )


def wrapped_lisp(root: Path, targets: Sequence[Target], body: str) -> str:
    return """(progn
%s
  (condition-case canary-error
      (progn
%s)
    (error
     (princ "CANARY_ERROR\\t")
     (prin1 canary-error)
     (terpri)
     (kill-emacs 70))))
""" % (common_lisp(root, targets), body)


def refresh_lisp(root: Path, targets: Sequence[Target]) -> str:
    return wrapped_lisp(
        root,
        targets,
        r"""
        (package-initialize)
        (package-refresh-contents)
        (canary-emit "runtime.emacs-version" emacs-version)
        (canary-emit "runtime.system-type" system-type)
        (canary-emit "runtime.system-configuration" system-configuration)
        (canary-emit
         "runtime.system-configuration-features"
         (if (boundp 'system-configuration-features)
             system-configuration-features "unbound"))
        (canary-emit
         "runtime.gnutls-available"
         (if (and (fboundp 'gnutls-available-p) (gnutls-available-p))
             "true" "false"))
        (canary-emit
         "runtime.gnutls-version"
         (if (boundp 'libgnutls-version) libgnutls-version "unbound"))
        (canary-emit
         "runtime.network-security-level"
         (if (boundp 'network-security-level) network-security-level "unbound"))
        (canary-emit
         "runtime.tls-program"
         (if (boundp 'tls-program) (prin1-to-string tls-program) "unbound"))
        (canary-emit
         "runtime.epg-gpg-program"
         (if (boundp 'epg-gpg-program) epg-gpg-program "unbound"))
        (canary-emit
         "runtime.package-check-signature"
         (prin1-to-string package-check-signature))
        (canary-emit
         "runtime.package-unsigned-archives"
         (prin1-to-string package-unsigned-archives))
        (when (null package-check-signature)
          (error "package-check-signature was disabled"))
        (dolist (spec canary-targets)
          (let* ((archive (nth 0 spec))
                 (url (nth 1 spec))
                 (package (nth 2 spec))
                 (expected-version (nth 4 spec))
                 (archive-file
                  (expand-file-name
                   (concat "archives/" archive "/archive-contents")
                   package-user-dir))
                 (signature-file (concat archive-file ".signed"))
                 (desc (canary-desc archive package)))
            (unless (file-exists-p archive-file)
              (error "archive refresh did not produce metadata: %s %s"
                     archive url))
            (unless desc
              (error "archive metadata lacks target: %s %s" archive package))
            (canary-emit (concat "archive." archive ".url") url)
            (canary-emit
             (concat "archive." archive ".contents-sha256")
             (canary-file-sha256 archive-file))
            (canary-emit
             (concat "archive." archive ".signature-present")
             (if (file-exists-p signature-file) "true" "false"))
            (when (file-exists-p signature-file)
              (canary-emit
               (concat "archive." archive ".signature-sha256")
               (canary-file-sha256 signature-file)))
            (canary-emit
             (concat "archive." archive ".target")
             (canary-desc-record desc))
            (canary-emit
             (concat "archive." archive ".expected-version")
             expected-version)))
        """,
    )


def install_lisp(root: Path, targets: Sequence[Target]) -> str:
    return wrapped_lisp(
        root,
        targets,
        r"""
        (package-initialize)
        (package-read-all-archive-contents)
        (dolist (spec canary-targets)
          (let* ((archive (nth 0 spec))
                 (package (nth 2 spec))
                 (expected-version (nth 4 spec))
                 (desc (canary-desc archive package)))
            (unless desc
              (error "cached archive metadata lacks target: %s %s"
                     archive package))
            (unless (equal (package-version-join (package-desc-version desc))
                           expected-version)
              (error "pinned version drift for %s: wanted %s got %s"
                     package expected-version
                     (package-version-join (package-desc-version desc))))
            (let ((transaction (package-compute-transaction (list desc) nil)))
              (canary-emit
               (concat "transaction." archive)
               (mapconcat #'canary-desc-record
                          (sort (copy-sequence transaction)
                                (lambda (left right)
                                  (string< (canary-desc-record left)
                                           (canary-desc-record right))))
                          ",")))
            (package-install desc)))
        (dolist (spec canary-targets)
          (let ((feature (nth 3 spec)))
            (condition-case error
                (require feature)
              (error
               (error "loading installed feature %s failed: %S"
                      feature error)))))
        (canary-emit
         "loaded.features"
         (mapconcat (lambda (spec) (symbol-name (nth 3 spec)))
                    canary-targets ","))
        (canary-emit-installed)
        """,
    )


def restart_remove_lisp(
    root: Path, targets: Sequence[Target], installed: Sequence[str]
) -> str:
    names = installed_names(installed)
    installed_symbols = "'(%s)" % " ".join(names)
    installed_full_names = "'(%s)" % " ".join(
        lisp_string(name) for name in installed
    )
    return wrapped_lisp(
        root,
        targets,
        r"""
        (package-initialize)
        (defconst canary-installed-names %s)
        (defconst canary-installed-full-names %s)
        (dolist (spec canary-targets)
          (let ((package (nth 2 spec))
                (version (version-to-list (nth 4 spec))))
            (unless (package-installed-p package version)
              (error "pinned package missing after restart: %%s" package)))
          (require (nth 3 spec)))
        (canary-emit
         "restart.loaded-features"
         (mapconcat (lambda (spec) (symbol-name (nth 3 spec)))
                    canary-targets ","))
        (canary-emit-installed)
        (dolist (name
                 (append (mapcar (lambda (spec) (nth 2 spec)) canary-targets)
                         canary-installed-names))
          (when-let* ((desc (cadr (assq name package-alist))))
            (package-delete desc 'force)))
        (dolist (name canary-installed-names)
          (when (assq name package-alist)
            (error "package remained after removal: %%s" name)))
        (dolist (full-name canary-installed-full-names)
          (let ((directory (expand-file-name full-name package-user-dir)))
            (when (file-directory-p directory)
              (error "package directory remained after removal: %%s"
                     directory))))
        (canary-emit
         "removed.packages"
         (mapconcat #'symbol-name canary-installed-names ","))
        """ % (installed_symbols, installed_full_names),
    )


def verify_removed_lisp(
    root: Path, targets: Sequence[Target], installed: Sequence[str]
) -> str:
    names = installed_names(installed)
    installed_symbols = "'(%s)" % " ".join(names)
    installed_full_names = "'(%s)" % " ".join(
        lisp_string(name) for name in installed
    )
    return wrapped_lisp(
        root,
        targets,
        r"""
        (package-initialize)
        (defconst canary-installed-names %s)
        (defconst canary-installed-full-names %s)
        (dolist (name canary-installed-names)
          (when (assq name package-alist)
            (error "package returned after removal restart: %%s" name)))
        (dolist (full-name canary-installed-full-names)
          (let ((directory (expand-file-name full-name package-user-dir)))
            (when (file-directory-p directory)
              (error "package directory returned after removal restart: %%s"
                     directory))))
        (canary-emit "restart-after-remove.packages" "")
        """ % (installed_symbols, installed_full_names),
    )


def parse_protocol(stdout: str) -> Tuple[Dict[str, List[str]], Optional[str]]:
    records: Dict[str, List[str]] = {}
    errors = []
    unexpected = []
    for line in stdout.splitlines():
        if line.startswith(MARKER):
            parts = line.split("\t", 2)
            if len(parts) != 3 or not parts[1]:
                errors.append("malformed protocol record: %r" % line)
                continue
            records.setdefault(parts[1], []).append(parts[2])
        elif line.startswith(ERROR_MARKER):
            errors.append(line[len(ERROR_MARKER) :])
        elif line.strip():
            unexpected.append(line)
    if unexpected:
        errors.append("unexpected stdout: %r" % unexpected)
    return records, "; ".join(errors) if errors else None


def run_phase(
    editor: str,
    binary: Path,
    phase: str,
    program: str,
    root: Path,
    timeout: int,
) -> PhaseResult:
    root.mkdir(parents=True, exist_ok=True)
    (root / "home").mkdir(parents=True, exist_ok=True)
    program_path = root / (phase + ".el")
    program_path.write_text(program, encoding="utf-8")
    environment = os.environ.copy()
    environment.update(
        {
            "HOME": str(root / "home"),
            "LANG": "C",
            "LC_ALL": "C",
        }
    )
    started = time.monotonic()
    try:
        completed = subprocess.run(
            [
                str(binary),
                "--no-init-file",
                "--no-site-file",
                "--no-site-lisp",
                "--batch",
                "--load",
                str(program_path),
            ],
            cwd=str(root),
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
            check=False,
        )
        records, protocol_error = parse_protocol(completed.stdout)
        return PhaseResult(
            editor,
            phase,
            completed.returncode,
            time.monotonic() - started,
            completed.stdout,
            completed.stderr,
            records,
            protocol_error,
        )
    except subprocess.TimeoutExpired as error:
        stdout = error.stdout or ""
        stderr = error.stderr or ""
        if isinstance(stdout, bytes):
            stdout = stdout.decode("utf-8", "replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", "replace")
        records, protocol_error = parse_protocol(stdout)
        timeout_error = "phase timed out after %ss" % timeout
        if protocol_error:
            timeout_error += "; " + protocol_error
        return PhaseResult(
            editor,
            phase,
            124,
            time.monotonic() - started,
            stdout,
            stderr,
            records,
            timeout_error,
        )


def result_json(result: PhaseResult) -> dict:
    return {
        "returncode": result.returncode,
        "duration_seconds": round(result.duration_seconds, 3),
        "records": result.records,
        "protocol_error": result.protocol_error,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def comparable_records(records: Mapping[str, List[str]]) -> Dict[str, List[str]]:
    return {
        key: value
        for key, value in records.items()
        if not key.startswith("runtime.")
    }


def record_mismatches(
    oracle: Mapping[str, List[str]], subject: Mapping[str, List[str]]
) -> List[dict]:
    mismatches = []
    for key in sorted(set(oracle) | set(subject)):
        if oracle.get(key) != subject.get(key):
            mismatches.append(
                {"key": key, "gnu": oracle.get(key), "emaxx": subject.get(key)}
            )
    return mismatches


def installed_full_names(records: Mapping[str, List[str]]) -> List[str]:
    values = records.get("installed.packages")
    if not values or len(values) != 1:
        raise ValueError("install phase did not emit exactly one installed package list")
    return [value for value in values[0].split(",") if value]


def package_name(full_name: str) -> str:
    match = re.match(r"\A(.+)-[0-9]+(?:\.[0-9]+)*\Z", full_name)
    if not match or not IDENTIFIER.fullmatch(match.group(1)):
        raise ValueError("invalid installed package full name: %s" % full_name)
    return match.group(1)


def installed_names(full_names: Iterable[str]) -> List[str]:
    return [package_name(value) for value in full_names]


def phase_failure_classification(
    oracle: PhaseResult, subject: PhaseResult
) -> Tuple[str, str]:
    if oracle.ok and not subject.ok:
        return (
            "emaxx_behavior",
            "GNU completed %s but Emaxx did not" % oracle.phase,
        )
    if not oracle.ok:
        return (
            "network_or_archive",
            "GNU could not complete %s on this host" % oracle.phase,
        )
    return ("internal", "unexpected phase result state")


def write_report(report: dict, requested: Optional[Path]) -> Path:
    if requested is None:
        stamp = report["started_at"].replace(":", "").replace("-", "")
        stamp = stamp.replace("+0000", "Z")
        requested = Path("target/package-live-canary") / (
            "live-canary-%s.json" % stamp
        )
    requested.parent.mkdir(parents=True, exist_ok=True)
    requested.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return requested.resolve()


def finish(report: dict, report_path: Optional[Path], status: str, message: str) -> int:
    report["status"] = status
    report["message"] = message
    report["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    path = write_report(report, report_path)
    print("%s: %s" % (status.upper(), message))
    print("Report: %s" % path)
    return 0 if status == "pass" else 1


def run_canary(args: argparse.Namespace) -> int:
    targets = tuple(args.target or TARGETS)
    validate_targets(targets)
    gnu = args.gnu.resolve()
    emaxx = args.emaxx.resolve()
    for path, label in ((gnu, "GNU Emacs"), (emaxx, "Emaxx")):
        if not path.is_file():
            raise ValueError("missing %s binary: %s" % (label, path))

    started = dt.datetime.now(dt.timezone.utc).isoformat()
    report = {
        "schema_version": 1,
        "started_at": started,
        "host": {
            "platform": sys.platform,
            "python": sys.version,
            "gnu_binary": str(gnu),
            "emaxx_binary": str(emaxx),
        },
        "archives": ARCHIVES,
        "targets": [target._asdict() for target in targets],
        "phases": {},
    }

    with tempfile.TemporaryDirectory(prefix="emaxx-live-package-canary-") as temp:
        temp_root = Path(temp)
        roots = {
            "gnu": temp_root / "gnu",
            "emaxx": temp_root / "emaxx",
        }

        def paired(phase: str, builder) -> Tuple[PhaseResult, PhaseResult]:
            oracle = run_phase(
                "gnu",
                gnu,
                phase,
                builder(roots["gnu"]),
                roots["gnu"],
                args.timeout,
            )
            subject = run_phase(
                "emaxx",
                emaxx,
                phase,
                builder(roots["emaxx"]),
                roots["emaxx"],
                args.timeout,
            )
            report["phases"][phase] = {
                "gnu": result_json(oracle),
                "emaxx": result_json(subject),
            }
            return oracle, subject

        refresh = paired("refresh", lambda root: refresh_lisp(root, targets))
        if not refresh[0].ok or not refresh[1].ok:
            category, message = phase_failure_classification(*refresh)
            report["failure_category"] = category
            return finish(report, args.report, "fail", message)
        refresh_mismatches = record_mismatches(
            comparable_records(refresh[0].records),
            comparable_records(refresh[1].records),
        )
        report["phases"]["refresh"]["mismatches"] = refresh_mismatches
        if refresh_mismatches:
            report["failure_category"] = "archive_drift"
            return finish(
                report,
                args.report,
                "fail",
                "the editors received different live archive metadata",
            )
        pin_mismatches = []
        for target in targets:
            record = refresh[0].records.get("archive.%s.target" % target.archive, [])
            expected = "%s@%s:" % (target.package, target.version)
            if len(record) != 1 or not record[0].startswith(expected):
                pin_mismatches.append(
                    {
                        "archive": target.archive,
                        "expected": target.version,
                        "observed": record,
                    }
                )
        report["pin_mismatches"] = pin_mismatches
        if pin_mismatches:
            report["failure_category"] = "archive_drift"
            return finish(
                report,
                args.report,
                "fail",
                "one or more live archives no longer carry the pinned version",
            )

        install = paired("install", lambda root: install_lisp(root, targets))
        if not install[0].ok or not install[1].ok:
            category, message = phase_failure_classification(*install)
            report["failure_category"] = category
            return finish(report, args.report, "fail", message)
        install_mismatches = record_mismatches(
            install[0].records, install[1].records
        )
        report["phases"]["install"]["mismatches"] = install_mismatches
        if install_mismatches:
            drift = any(
                item["key"].startswith(("source.", "signature.", "transaction."))
                for item in install_mismatches
            )
            category = "archive_drift" if drift else "emaxx_behavior"
            report["failure_category"] = category
            return finish(
                report,
                args.report,
                "fail",
                "the installed package transaction differed from GNU",
            )
        try:
            full_names = installed_full_names(install[0].records)
            names = installed_names(full_names)
        except ValueError as error:
            report["failure_category"] = "protocol"
            return finish(report, args.report, "fail", str(error))

        restart = paired(
            "restart-remove",
            lambda root: restart_remove_lisp(root, targets, full_names),
        )
        if not restart[0].ok or not restart[1].ok:
            category, message = phase_failure_classification(*restart)
            report["failure_category"] = category
            return finish(report, args.report, "fail", message)
        restart_mismatches = record_mismatches(
            restart[0].records, restart[1].records
        )
        report["phases"]["restart-remove"]["mismatches"] = restart_mismatches
        if restart_mismatches:
            report["failure_category"] = "emaxx_behavior"
            return finish(
                report,
                args.report,
                "fail",
                "restart, activation, loading, or removal differed from GNU",
            )

        removed = paired(
            "restart-after-remove",
            lambda root: verify_removed_lisp(root, targets, full_names),
        )
        if not removed[0].ok or not removed[1].ok:
            category, message = phase_failure_classification(*removed)
            report["failure_category"] = category
            return finish(report, args.report, "fail", message)
        removed_mismatches = record_mismatches(
            removed[0].records, removed[1].records
        )
        report["phases"]["restart-after-remove"][
            "mismatches"
        ] = removed_mismatches
        if removed_mismatches:
            report["failure_category"] = "emaxx_behavior"
            return finish(
                report,
                args.report,
                "fail",
                "post-removal restart differed from GNU",
            )

        report["installed_package_names"] = names
        return finish(
            report,
            args.report,
            "pass",
            "GNU and Emaxx completed equivalent live package journeys",
        )


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--live",
        action="store_true",
        help="required opt-in acknowledgement that public HTTPS will be used",
    )
    result.add_argument(
        "--gnu", type=Path, default=Path("../emacs/src/emacs"), help="GNU oracle binary"
    )
    result.add_argument(
        "--emaxx", type=Path, default=Path("target/gate/emaxx"), help="Emaxx binary"
    )
    result.add_argument(
        "--target",
        action="append",
        type=parse_target,
        help="override all defaults with ARCHIVE:PACKAGE:FEATURE:VERSION",
    )
    result.add_argument(
        "--timeout", type=int, default=900, help="seconds allowed per editor phase"
    )
    result.add_argument("--report", type=Path, help="JSON report path")
    return result


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parser().parse_args(argv)
    if not args.live:
        print(
            "REFUSING: live network canary requires the explicit --live flag",
            file=sys.stderr,
        )
        return 2
    if args.timeout <= 0:
        print("ERROR: --timeout must be positive", file=sys.stderr)
        return 2
    try:
        return run_canary(args)
    except (OSError, ValueError) as error:
        print("ERROR: %s" % error, file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
