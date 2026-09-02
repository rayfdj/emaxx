#!/usr/bin/env python3
"""Compare a real TRAMP workflow between GNU Emacs and Emaxx.

The default journey uses TRAMP's deterministic local ``mock`` method and does
not open a network connection.  A real SSH endpoint is accepted only with the
explicit ``--live-ssh`` flag.  Oracle and subject are always run serially in
fresh homes, and their structured records are compared without normalization.
"""

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time
from typing import Dict, List, NamedTuple, Optional, Sequence, Tuple


MARKER = "TRAMP_JOURNEY\t"
ERROR_MARKER = "TRAMP_JOURNEY_ERROR\t"
SSH_ROOT = re.compile(r"\A/(?:ssh|sshx|scp|sftp):[^:]+:.+/?\Z")
TRUE_RECORDS = {
    "cleanup.final",
    "compilation.output",
    "connection.cleaned",
    "connection.reconnected",
    "connection.reused",
    "delete.absent",
    "dired.visible",
    "handler.present",
    "metadata.modes",
    "metadata.regular",
    "project.root",
    "reconnect.file",
    "vc.registered",
}
EXACT_RECORDS = {
    "completion.vis": ['"visited.txt"'],
    "copy-rename.contents": ['"external\\n"'],
    "metadata.size": ["9"],
    "missing.condition": ["file-missing"],
    "process.async": ['"async-ok"'],
    "process.sync": ['"sync-ok"'],
    "revert.contents": ['"external\\n"'],
    "visit.contents": ['"initial\\n"'],
}


class EditorResult(NamedTuple):
    editor: str
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


def mock_method_lisp() -> str:
    return r"""
      (add-to-list
       'tramp-methods
       `("mock"
         (tramp-login-program ,tramp-default-remote-shell)
         (tramp-login-args (("-i")))
         (tramp-direct-async ("-c"))
         (tramp-remote-shell ,tramp-default-remote-shell)
         (tramp-remote-shell-args ("-c"))
         (tramp-connection-timeout 10)))
      (add-to-list
       'tramp-default-host-alist
       `("\\`mock\\'" nil ,(system-name)))
    """


def journey_lisp(remote_root: str, mock: bool) -> str:
    setup = mock_method_lisp() if mock else ""
    return r"""(progn
  (require 'cl-lib)
  (require 'tramp)
  (require 'dired)
  (require 'project)
  (require 'compile)
%s
  (setq tramp-verbose 0
        tramp-persistency-file-name nil
        remote-file-name-inhibit-cache nil)
  (defun tramp-journey-emit (key value)
    (princ (format "TRAMP_JOURNEY\t%%s\t%%s\n"
                   key
                   (let ((print-escape-newlines t)
                         (print-escape-control-characters t))
                     (prin1-to-string value)))))
  (let* ((root (file-name-as-directory %s))
         (work (expand-file-name "workflow/" root))
         (visited (expand-file-name "visited.txt" work))
         (copy (expand-file-name "copy.txt" work))
         (renamed (expand-file-name "renamed.txt" work))
         (marker (expand-file-name ".project" work))
         (tracked (expand-file-name "tracked.txt" work))
         (vec (tramp-dissect-file-name root))
         first-connection second-connection async-buffer async-process
         compile-buffer compile-process temp-file cleanup-errors)
    (condition-case journey-error
        (unwind-protect
            (progn
              (make-directory work t)
              (write-region "initial\n" nil visited nil 'silent)
              (let ((buffer (find-file-noselect visited)))
                (unwind-protect
                    (with-current-buffer buffer
                      (tramp-journey-emit "visit.remote"
                                          (file-remote-p buffer-file-name))
                      (tramp-journey-emit "visit.contents" (buffer-string))
                      (erase-buffer)
                      (insert "saved\n")
                      (save-buffer)
                      (write-region "external\n" nil visited nil 'silent)
                      (revert-buffer t t)
                      (tramp-journey-emit "revert.contents" (buffer-string)))
                  (kill-buffer buffer)))

              (tramp-journey-emit
               "directory.entries"
               (sort (directory-files work nil "\\`[^.]") #'string<))
              (tramp-journey-emit
               "completion.vis" (file-name-completion "vis" work))

              (copy-file visited copy)
              (rename-file copy renamed)
              (tramp-journey-emit
               "copy-rename.contents"
               (with-temp-buffer
                 (insert-file-contents renamed)
                 (buffer-string)))
              (delete-file renamed)
              (tramp-journey-emit "delete.absent" (not (file-exists-p renamed)))

              (let ((attributes (file-attributes visited)))
                (tramp-journey-emit "metadata.regular" (null (car attributes)))
                (tramp-journey-emit "metadata.size" (file-attribute-size attributes))
                (tramp-journey-emit "metadata.modes"
                                    (stringp (file-attribute-modes attributes))))

              (setq temp-file (make-nearby-temp-file
                               (expand-file-name "near-" work)))
              (tramp-journey-emit "temp.remote" (file-remote-p temp-file))
              (delete-file temp-file)
              (setq temp-file nil)

              (let ((default-directory work))
                (tramp-journey-emit
                 "process.sync"
                 (with-temp-buffer
                   (process-file shell-file-name nil t nil
                                 shell-command-switch "printf sync-ok")
                   (buffer-string))))

              (setq async-buffer (generate-new-buffer " *tramp-journey-async*")
                    async-process
                    (let ((default-directory work))
                      (start-file-process
                       "tramp-journey-async" async-buffer shell-file-name
                       shell-command-switch "printf async-ok")))
              (set-process-sentinel async-process #'ignore)
              (while (process-live-p async-process)
                (accept-process-output async-process nil nil t))
              (while (accept-process-output async-process 0 nil t))
              (tramp-journey-emit
               "process.async"
               (with-current-buffer async-buffer (buffer-string)))

              (file-attributes visited)
              (setq first-connection (tramp-get-connection-process vec))
              (file-attributes visited)
              (setq second-connection (tramp-get-connection-process vec))
              (tramp-journey-emit
               "connection.reused"
               (and (processp first-connection)
                    (eq first-connection second-connection)))

              (write-region "" nil marker nil 'silent)
              (let ((default-directory work))
                (process-file "git" nil nil nil "init" "-q")
                (write-region "tracked\n" nil tracked nil 'silent)
                (process-file "git" nil nil nil "add" "tracked.txt"))
              (require 'vc-git)
              (tramp-journey-emit "vc.registered"
                                  (and (vc-registered tracked) t))
              (let ((project (project-current nil work)))
                (tramp-journey-emit "project.root"
                                    (and project (file-equal-p
                                                  (project-root project) work))))

              (tramp-journey-emit
               "dired.visible"
               (let ((buffer (dired-noselect work)))
                 (unwind-protect
                     (with-current-buffer buffer
                       (goto-char (point-min))
                       (and (search-forward "visited.txt" nil t) t))
                   (kill-buffer buffer))))

              (let ((default-directory work)
                    (compilation-buffer-name-function
                     (lambda (_mode) "*tramp-journey-compilation*")))
                (setq compile-buffer
                      (compilation-start "printf compilation-ok")))
              (setq compile-process (get-buffer-process compile-buffer))
              (while (and compile-process (process-live-p compile-process))
                (accept-process-output compile-process nil nil t))
              (while (and compile-process
                          (accept-process-output compile-process 0 nil t)))
              (tramp-journey-emit
               "compilation.output"
               (with-current-buffer compile-buffer
                 (and (string-match-p "compilation-ok" (buffer-string)) t)))

              (tramp-journey-emit
               "handler.present"
               (functionp (find-file-name-handler root 'file-exists-p)))
              (tramp-journey-emit
               "missing.condition"
               (condition-case error-data
                   (progn (insert-file-contents
                           (expand-file-name "missing" work))
                          'none)
                 (error (car error-data))))

              (tramp-cleanup-connection vec)
              (tramp-journey-emit
               "connection.cleaned"
               (not (process-live-p first-connection)))
              (tramp-journey-emit "reconnect.file" (file-exists-p visited))
              (let ((reconnected (tramp-get-connection-process vec)))
                (tramp-journey-emit
                 "connection.reconnected"
                 (and (processp reconnected)
                      (process-live-p reconnected)
                      (not (eq reconnected first-connection))))))
          (condition-case cleanup-error
              (when (processp async-process) (delete-process async-process))
            (error (push cleanup-error cleanup-errors)))
          (condition-case cleanup-error
              (when (buffer-live-p async-buffer) (kill-buffer async-buffer))
            (error (push cleanup-error cleanup-errors)))
          (condition-case cleanup-error
              (when (processp compile-process) (delete-process compile-process))
            (error (push cleanup-error cleanup-errors)))
          (condition-case cleanup-error
              (when (buffer-live-p compile-buffer) (kill-buffer compile-buffer))
            (error (push cleanup-error cleanup-errors)))
          (condition-case cleanup-error
              (when temp-file (delete-file temp-file))
            (error (push cleanup-error cleanup-errors)))
          (condition-case cleanup-error
              (delete-directory root t)
            (error (push cleanup-error cleanup-errors)))
          (condition-case cleanup-error
              (tramp-cleanup-connection vec)
            (error (push cleanup-error cleanup-errors)))
          (if cleanup-errors
              (error "TRAMP journey cleanup failed: %%S"
                     (nreverse cleanup-errors))
            (tramp-journey-emit "cleanup.final" t)))
      (error
       (princ "TRAMP_JOURNEY_ERROR\t")
       (prin1 journey-error)
       (terpri)
       (kill-emacs 70)))))
""" % (setup, lisp_string(remote_root))


def parse_protocol(stdout: str) -> Tuple[Dict[str, List[str]], Optional[str]]:
    records: Dict[str, List[str]] = {}
    errors: List[str] = []
    for line in stdout.splitlines():
        if line.startswith(MARKER):
            parts = line.split("\t", 2)
            if len(parts) != 3 or not parts[1]:
                errors.append("malformed journey record: %r" % line)
                continue
            records.setdefault(parts[1], []).append(parts[2])
        elif line.startswith(ERROR_MARKER):
            errors.append(line[len(ERROR_MARKER) :])
        elif line.strip():
            errors.append("unexpected stdout: %r" % line)
    return records, "; ".join(errors) or None


def lisp_load_args(source: Path) -> List[str]:
    lisp = source / "lisp"
    directories = [lisp]
    if lisp.is_dir():
        directories.extend(sorted(path for path in lisp.iterdir() if path.is_dir()))
    args: List[str] = []
    for directory in directories:
        args.extend(["-L", str(directory)])
    return args


def descendant_pids(root_pid: int) -> List[int]:
    """Snapshot descendants of ROOT_PID without matching command text."""
    if os.name != "posix":
        return []
    snapshot = subprocess.run(
        ["ps", "-axo", "pid=,ppid="],
        text=True,
        capture_output=True,
        check=False,
    )
    if snapshot.returncode != 0:
        return []
    children: Dict[int, List[int]] = {}
    for line in snapshot.stdout.splitlines():
        fields = line.split()
        if len(fields) != 2:
            continue
        pid, parent = (int(field) for field in fields)
        children.setdefault(parent, []).append(pid)
    descendants: List[int] = []
    pending = list(children.get(root_pid, []))
    while pending:
        pid = pending.pop()
        descendants.append(pid)
        pending.extend(children.get(pid, []))
    return descendants


def terminate_timed_out_editor(process: subprocess.Popen) -> None:
    """Terminate the timed-out editor and the exact descendants it created."""
    descendants = descendant_pids(process.pid)
    for pid in [process.pid, *reversed(descendants)]:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    for pid in reversed(descendants):
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def output_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def run_editor(
    editor: str,
    binary: Path,
    emacs_source: Path,
    root: Path,
    remote_root: str,
    mock: bool,
    timeout: int,
) -> EditorResult:
    home = root / "home"
    home.mkdir(parents=True)
    program = root / "journey.el"
    program.write_text(journey_lisp(remote_root, mock), encoding="utf-8")
    command = [
        str(binary),
        "--no-init-file",
        "--no-site-file",
        "--no-site-lisp",
        "--batch",
        "--eval",
        "(setq source-directory %s)" % lisp_string(str(emacs_source) + os.sep),
        *lisp_load_args(emacs_source),
        "--load",
        str(program),
    ]
    environment = os.environ.copy()
    environment.update(
        {
            "HOME": str(home),
            "LANG": "C",
            "LC_ALL": "C",
            "EMACS_TEST_DIRECTORY": str(emacs_source / "test"),
        }
    )
    started = time.monotonic()
    process = subprocess.Popen(
        command,
        cwd=root,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=os.name == "posix",
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout)
        returncode = process.returncode
    except subprocess.TimeoutExpired as error:
        terminate_timed_out_editor(process)
        returncode = 124
        stdout = output_text(error.stdout)
        stderr = output_text(error.stderr) + "\njourney timed out"
    records, protocol_error = parse_protocol(stdout)
    return EditorResult(
        editor,
        returncode,
        time.monotonic() - started,
        stdout,
        stderr,
        records,
        protocol_error,
    )


def record_mismatches(
    oracle: Dict[str, List[str]], subject: Dict[str, List[str]]
) -> List[dict]:
    return [
        {"key": key, "gnu": oracle.get(key, []), "emaxx": subject.get(key, [])}
        for key in sorted(set(oracle) | set(subject))
        if oracle.get(key, []) != subject.get(key, [])
    ]


def semantic_failures(records: Dict[str, List[str]]) -> List[str]:
    """Return unmet journey assertions without weakening oracle comparison."""
    failures = [
        "%s expected [t], got %r" % (key, records.get(key, []))
        for key in sorted(TRUE_RECORDS)
        if records.get(key, []) != ["t"]
    ]
    failures.extend(
        "%s expected %r, got %r" % (key, expected, records.get(key, []))
        for key, expected in sorted(EXACT_RECORDS.items())
        if records.get(key, []) != expected
    )
    for key in ("temp.remote", "visit.remote"):
        observed = records.get(key, [])
        if len(observed) != 1 or observed[0] == "nil":
            failures.append("%s did not report a remote prefix: %r" % (key, observed))
    entries = records.get("directory.entries", [])
    if len(entries) != 1 or '"visited.txt"' not in entries[0]:
        failures.append("directory.entries omitted visited.txt: %r" % entries)
    return failures


def diagnostic_error(stderr: str) -> Optional[str]:
    """Reject every nonblank diagnostic except compilation's success line."""
    lines = [line for line in stderr.splitlines() if line]
    if lines == ["Compilation finished"]:
        return None
    return "unexpected stderr lines: %r" % lines


def result_json(result: EditorResult) -> dict:
    return {
        "returncode": result.returncode,
        "duration_seconds": result.duration_seconds,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "records": result.records,
        "protocol_error": result.protocol_error,
    }


def write_report(report: dict, requested: Optional[Path]) -> Path:
    if requested is None:
        stamp = report["started_at"].replace(":", "").replace("-", "")
        stamp = stamp.replace("+0000", "Z")
        requested = Path("target/tramp-compat-gate") / ("journey-%s.json" % stamp)
    requested.parent.mkdir(parents=True, exist_ok=True)
    requested.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return requested.resolve()


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--gnu", type=Path, default=Path("../emacs/src/emacs"))
    result.add_argument("--emaxx", type=Path, default=Path("target/gate/emaxx"))
    result.add_argument("--emacs-source", type=Path, default=Path("../emacs"))
    result.add_argument("--timeout", type=int, default=300)
    result.add_argument("--report", type=Path)
    result.add_argument(
        "--live-ssh",
        action="store_true",
        help="allow a real SSH transport (never implied by --remote-root)",
    )
    result.add_argument(
        "--remote-root",
        help="existing writable /ssh:USER@HOST:/path/ base for --live-ssh",
    )
    return result


def validate_args(args: argparse.Namespace) -> None:
    if args.timeout <= 0:
        raise ValueError("--timeout must be positive")
    if args.live_ssh:
        if not args.remote_root or not SSH_ROOT.fullmatch(args.remote_root):
            raise ValueError(
                "--live-ssh requires an explicit writable /ssh:USER@HOST:/path/ root"
            )
    elif args.remote_root:
        raise ValueError("--remote-root is accepted only with --live-ssh")


def run_gate(args: argparse.Namespace) -> int:
    validate_args(args)
    gnu = args.gnu.resolve()
    emaxx = args.emaxx.resolve()
    source = args.emacs_source.resolve()
    for path, label in ((gnu, "GNU Emacs"), (emaxx, "Emaxx")):
        if not path.is_file():
            raise ValueError("missing %s binary: %s" % (label, path))
    if not (source / "lisp").is_dir():
        raise ValueError("missing GNU Emacs source tree: %s" % source)

    started = dt.datetime.now(dt.timezone.utc).isoformat()
    report = {
        "schema_version": 1,
        "started_at": started,
        "mode": "live-ssh" if args.live_ssh else "mock-localhost",
        "gnu_binary": str(gnu),
        "emaxx_binary": str(emaxx),
        "emacs_source": str(source),
    }
    with tempfile.TemporaryDirectory(prefix="emaxx-tramp-journey-") as temporary:
        temp = Path(temporary)
        roots = {"gnu": temp / "gnu", "emaxx": temp / "emaxx"}
        for root in roots.values():
            root.mkdir()
        if args.live_ssh:
            suffix = "emaxx-tramp-journey-%d/" % os.getpid()
            remote_roots = {
                "gnu": args.remote_root.rstrip("/") + "/" + suffix,
                "emaxx": args.remote_root.rstrip("/") + "/" + suffix,
            }
        else:
            remote_roots = {
                label: "/mock::%s/remote/" % root for label, root in roots.items()
            }

        # Deliberately serial: the subject starts only after the oracle has
        # reported strict cleanup and its editor process has exited.
        oracle = run_editor(
            "gnu",
            gnu,
            source,
            roots["gnu"],
            remote_roots["gnu"],
            not args.live_ssh,
            args.timeout,
        )
        oracle_semantic_failures = semantic_failures(oracle.records)
        oracle_diagnostic_error = diagnostic_error(oracle.stderr)
        if not oracle.ok:
            status = "fail"
            message = "GNU oracle could not complete the TRAMP journey"
        elif oracle_semantic_failures:
            status = "fail"
            message = "GNU oracle did not satisfy the TRAMP journey assertions"
        elif oracle_diagnostic_error:
            status = "fail"
            message = "GNU oracle emitted unexpected TRAMP diagnostics"
        else:
            subject = run_editor(
                "emaxx",
                emaxx,
                source,
                roots["emaxx"],
                remote_roots["emaxx"],
                not args.live_ssh,
                args.timeout,
            )
            subject_semantic_failures = semantic_failures(subject.records)
            subject_diagnostic_error = diagnostic_error(subject.stderr)
            mismatches = record_mismatches(oracle.records, subject.records)
            report["results"] = {
                "gnu": result_json(oracle),
                "emaxx": result_json(subject),
            }
            report["mismatches"] = mismatches
            report["diagnostic_errors"] = {
                "gnu": oracle_diagnostic_error,
                "emaxx": subject_diagnostic_error,
            }
            report["semantic_failures"] = {
                "gnu": oracle_semantic_failures,
                "emaxx": subject_semantic_failures,
            }
            if not subject.ok:
                status = "fail"
                message = "Emaxx could not complete the TRAMP journey"
            elif subject_semantic_failures:
                status = "fail"
                message = "Emaxx did not satisfy the TRAMP journey assertions"
            elif subject_diagnostic_error:
                status = "fail"
                message = "Emaxx emitted unexpected TRAMP diagnostics"
            elif mismatches:
                status = "fail"
                message = "GNU and Emaxx TRAMP journey records differ"
            else:
                status = "pass"
                message = "GNU and Emaxx completed equivalent TRAMP journeys"

        if "results" not in report:
            report["results"] = {"gnu": result_json(oracle), "emaxx": None}
            report["mismatches"] = []
            report["diagnostic_errors"] = {
                "gnu": oracle_diagnostic_error,
                "emaxx": None,
            }
            report["semantic_failures"] = {
                "gnu": oracle_semantic_failures,
                "emaxx": [],
            }

    report["status"] = status
    report["message"] = message
    report["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    path = write_report(report, args.report)
    print("%s: %s" % (status.upper(), message))
    print("Report: %s" % path)
    return 0 if status == "pass" else 1


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parser().parse_args(argv)
    try:
        return run_gate(args)
    except ValueError as error:
        parser().error(str(error))
    return 2


if __name__ == "__main__":
    sys.exit(main())
