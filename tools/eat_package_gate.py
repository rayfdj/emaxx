#!/usr/bin/env python3
"""Certify the pinned Eat 0.9.4 package against GNU Emacs and Emaxx.

The gate hash-verifies official release artifacts, exposes them through a
disposable local package archive, and gives both editors separate empty
package roots.  Each editor performs the same package.el install, restarts,
loads only the installed Eat bytecode, runs all 57 upstream tests, and then
executes the shared real-process gate in ``tools/eat_process_gate.el``.
"""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
from typing import Dict, Mapping, NamedTuple, Optional, Sequence, Tuple
import urllib.request


MARKER = "EAT_GATE\t"
ERROR_MARKER = "EAT_GATE_ERROR\t"


class Artifact(NamedTuple):
    filename: str
    url: str
    sha256: str
    package_archive: bool


ARTIFACTS = (
    Artifact(
        "compat-31.0.0.2.tar",
        "https://elpa.gnu.org/packages/compat-31.0.0.2.tar",
        "47d8693a10087f8b20c72e6a78b628db980cb7547c4f8f517fc5d11acd8b0f38",
        True,
    ),
    Artifact(
        "eat-0.9.4.tar",
        "https://elpa.nongnu.org/nongnu/eat-0.9.4.tar",
        "14971fc562f0820794eb6af78beebc7dc3ba898221e785c2d272a9f0fccfc54a",
        True,
    ),
    Artifact(
        "eat-v0.9.4-source.tar.gz",
        "https://codeberg.org/akib/emacs-eat/archive/v0.9.4.tar.gz",
        "32a2793c1f203bf2e0fe67f79310c2389257e1338b191e017ea60dc68000c01a",
        False,
    ),
)


ARCHIVE_CONTENTS = """(1
 (compat . [(31 0 0 2) ((emacs (25 1))) "Emacs Lisp Compatibility Library" tar])
 (eat . [(0 9 4) ((emacs (26 1)) (compat (29 1))) "Emulate A Terminal, in a region, in a buffer and in Eshell" tar]))
"""


EXPECTED_INSTALLED = ("eat-0.9.4",)
EXPECTED_TRANSACTION = ("eat-0.9.4",)

EXPECTED_COMPILED = (
    "eat-0.9.4/eat.elc",
    "eat-0.9.4/term/eat.elc",
)


EXPECTED_PROCESS_RECORDS = {
    "deterministic": (
        "(((100 . 40) (4 12) "
        "(:foreground \"red3\" :inherit (eat-term-font-0)) "
        "200 t t t) t exit 7 t t t)"
    ),
    "signal": "(signal 2 t t t t)",
    "shell": (
        "(t (:foreground \"magenta3\" :inherit (eat-term-font-0)) "
        "exit 3 t t t)"
    ),
}


class PhaseResult(NamedTuple):
    editor: str
    phase: str
    returncode: int
    stdout: str
    stderr: str
    records: Mapping[str, str]


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_artifact(path: Path, artifact: Artifact) -> None:
    if not path.is_file():
        raise ValueError("missing pinned artifact: %s" % path)
    actual = file_sha256(path)
    if actual != artifact.sha256:
        raise ValueError(
            "SHA-256 mismatch for %s: expected %s, got %s"
            % (artifact.filename, artifact.sha256, actual)
        )


def obtain_artifact(cache: Path, artifact: Artifact, offline: bool) -> Path:
    cache.mkdir(parents=True, exist_ok=True)
    destination = cache / artifact.filename
    if destination.exists():
        verify_artifact(destination, artifact)
        return destination
    if offline:
        raise ValueError("offline mode cannot fetch %s" % artifact.filename)
    temporary = destination.with_suffix(destination.suffix + ".part")
    try:
        with urllib.request.urlopen(artifact.url, timeout=120) as response:
            with temporary.open("wb") as output:
                shutil.copyfileobj(response, output)
        verify_artifact(temporary, artifact)
        os.replace(temporary, destination)
    finally:
        if temporary.exists():
            temporary.unlink()
    return destination


def extract_upstream_tests(source_archive: Path, destination: Path) -> Path:
    with tarfile.open(source_archive, "r:gz") as archive:
        matches = [
            member
            for member in archive.getmembers()
            if member.isfile() and member.name.endswith("/eat-tests.el")
        ]
        if len(matches) != 1:
            raise ValueError(
                "source archive contains %d eat-tests.el files" % len(matches)
            )
        extracted = archive.extractfile(matches[0])
        if extracted is None:
            raise ValueError("could not read pinned eat-tests.el")
        payload = extracted.read()
    if not payload or b"(ert-deftest eat-test-" not in payload:
        raise ValueError("pinned eat-tests.el has no Eat ERT inventory")
    destination.write_bytes(payload)
    return destination


def build_inputs(
    cache: Path, archive: Path, source_dir: Path, offline: bool
) -> Path:
    archive.mkdir(parents=True, exist_ok=False)
    source_dir.mkdir(parents=True, exist_ok=False)
    source_archive: Optional[Path] = None
    for artifact in ARTIFACTS:
        source = obtain_artifact(cache, artifact, offline)
        if artifact.package_archive:
            copied = archive / artifact.filename
            shutil.copyfile(source, copied)
            verify_artifact(copied, artifact)
        else:
            source_archive = source
    if source_archive is None:
        raise ValueError("no pinned Eat source archive configured")
    (archive / "archive-contents").write_text(ARCHIVE_CONTENTS, encoding="utf-8")
    return extract_upstream_tests(source_archive, source_dir / "eat-tests.el")


def lisp_string(value: object) -> str:
    import json

    return json.dumps(str(value), ensure_ascii=True)


def common_lisp(root: Path, archive: Path) -> str:
    return r"""
      (setq user-emacs-directory (file-name-as-directory %s)
            package-user-dir %s
            package-archives (list (cons "pinned" (file-name-as-directory %s)))
            package-check-signature 'allow-unsigned)
      (require 'cl-lib)
      (require 'package)
      (defun eat-gate-emit (key value)
        (princ (format "EAT_GATE\t%%s\t%%s\n" key value)))
      (defun eat-gate-installed-descs ()
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
      (defun eat-gate-compiled-record ()
        (let (compiled)
          (dolist (desc (eat-gate-installed-descs))
            (dolist (file (directory-files-recursively
                           (package-desc-dir desc) "\\.elc\\'"))
              (push (concat (package-desc-full-name desc) "/"
                            (file-relative-name file (package-desc-dir desc)))
                    compiled)))
          (mapconcat #'identity (sort compiled #'string<) ",")))
    """ % (
        lisp_string(str(root) + os.sep),
        lisp_string(root / "packages"),
        lisp_string(str(archive) + os.sep),
    )


def wrapped_lisp(root: Path, archive: Path, body: str) -> str:
    return """(progn
%s
  (condition-case eat-gate-error
      (progn
%s)
    (error
     (princ (format "EAT_GATE_ERROR\\t%%S\\n" eat-gate-error))
     (kill-emacs 1))))
""" % (common_lisp(root, archive), body)


def install_lisp(root: Path, archive: Path) -> str:
    return wrapped_lisp(
        root,
        archive,
        r"""
        (package-initialize)
        (eat-gate-emit "emacs-version" emacs-version)
        (package-refresh-contents)
        (let* ((desc (car (cdr (assq 'eat package-archive-contents))))
               (transaction
                (package-compute-transaction
                 (list desc) (package-desc-reqs desc))))
          (unless (and desc
                       (equal (package-desc-version desc) '(0 9 4))
                       (equal (package-desc-archive desc) "pinned"))
            (error "Pinned Eat descriptor was not selected: %S" desc))
          (eat-gate-emit
           "transaction"
           (mapconcat #'package-desc-full-name
                      (sort (copy-sequence transaction)
                            (lambda (left right)
                              (string< (package-desc-full-name left)
                                       (package-desc-full-name right))))
                      ","))
          (package-install desc))
        (package-initialize)
        (eat-gate-emit
         "installed"
         (mapconcat #'package-desc-full-name
                    (eat-gate-installed-descs) ","))
        (eat-gate-emit "compiled" (eat-gate-compiled-record))
        (let* ((installed (car (cdr (assq 'eat package-alist))))
               (autoload-file
                (and installed
                     (expand-file-name "eat-autoloads.el"
                                       (package-desc-dir installed)))))
          (eat-gate-emit
           "autoload-file"
           (if (and autoload-file (file-exists-p autoload-file))
               "true" "false")))
        """,
    )


def restart_lisp(root: Path, archive: Path, upstream_tests: Path) -> str:
    return wrapped_lisp(
        root,
        archive,
        r"""
        (package-initialize)
        (require 'eat)
        (let* ((desc (car (cdr (assq 'eat package-alist))))
               (origin (locate-library "eat")))
          (unless (and desc origin
                       (file-in-directory-p origin package-user-dir)
                       (string-suffix-p "/eat-0.9.4/eat.elc" origin))
            (error "Eat resolved outside installed bytecode: %%S" origin))
          (eat-gate-emit "version"
                         (package-version-join (package-desc-version desc)))
          (eat-gate-emit "origin"
                         (file-relative-name origin package-user-dir)))
        (load %s nil nil t)
        (let ((selected (ert-select-tests "^eat-test-" t)))
          (unless (= (length selected) 57)
            (error "Expected 57 upstream Eat tests, got %%S" selected)))
        (let ((stats (ert-run-tests-batch "^eat-test-")))
          (eat-gate-emit "tests.total" (ert-stats-total stats))
          (eat-gate-emit "tests.expected"
                         (ert-stats-completed-expected stats))
          (eat-gate-emit "tests.unexpected"
                         (ert-stats-completed-unexpected stats)))
        """ % lisp_string(upstream_tests),
    )


def parse_records(output: str) -> Dict[str, str]:
    records: Dict[str, str] = {}
    for line in output.splitlines():
        if not line.startswith(MARKER):
            continue
        parts = line.split("\t", 2)
        if len(parts) != 3 or parts[1] in records:
            raise ValueError("malformed or duplicate Eat gate record: %r" % line)
        records[parts[1]] = parts[2]
    return records


def run_phase(
    editor: str,
    binary: Path,
    phase: str,
    script: Path,
    root: Path,
    extra_environment: Optional[Mapping[str, str]] = None,
) -> PhaseResult:
    home = root / "home"
    home.mkdir(exist_ok=True)
    environment = os.environ.copy()
    environment.update({"HOME": str(home), "LANG": "C", "LC_ALL": "C"})
    if extra_environment:
        environment.update(extra_environment)
    completed = subprocess.run(
        [str(binary), "--batch", "-Q", "-l", str(script)],
        cwd=root,
        env=environment,
        text=True,
        capture_output=True,
        timeout=900,
        check=False,
    )
    try:
        records = parse_records(completed.stdout)
    except ValueError as error:
        records = {"protocol-error": str(error)}
    return PhaseResult(
        editor,
        phase,
        completed.returncode,
        completed.stdout,
        completed.stderr,
        records,
    )


def run_generated_phase(
    editor: str,
    binary: Path,
    phase: str,
    lisp: str,
    root: Path,
) -> PhaseResult:
    script = root / (phase + ".el")
    script.write_text(lisp, encoding="utf-8")
    return run_phase(editor, binary, phase, script, root)


def require_success(result: PhaseResult) -> None:
    if (
        result.returncode != 0
        or ERROR_MARKER in result.stdout
        or "protocol-error" in result.records
    ):
        raise RuntimeError(
            "%s %s failed (exit %d)\nstdout:\n%s\nstderr:\n%s"
            % (
                result.editor,
                result.phase,
                result.returncode,
                result.stdout,
                result.stderr,
            )
        )


def split_csv(value: str) -> Tuple[str, ...]:
    return tuple(item for item in value.split(",") if item)


def validate_install(result: PhaseResult) -> None:
    require_success(result)
    expected_keys = {
        "emacs-version",
        "transaction",
        "installed",
        "compiled",
        "autoload-file",
    }
    if set(result.records) != expected_keys:
        raise RuntimeError(
            "%s install emitted keys %r, expected %r"
            % (result.editor, sorted(result.records), sorted(expected_keys))
        )
    if split_csv(result.records["transaction"]) != EXPECTED_TRANSACTION:
        raise RuntimeError(
            "%s transaction inventory differs: %r"
            % (result.editor, result.records["transaction"])
        )
    if split_csv(result.records["installed"]) != EXPECTED_INSTALLED:
        raise RuntimeError(
            "%s installed inventory differs: %r"
            % (result.editor, result.records["installed"])
        )
    if split_csv(result.records["compiled"]) != EXPECTED_COMPILED:
        raise RuntimeError(
            "%s compiled inventory differs: %r"
            % (result.editor, result.records["compiled"])
        )
    if result.records["autoload-file"] != "true":
        raise RuntimeError("%s did not generate eat-autoloads.el" % result.editor)
    if result.records["emacs-version"] != "30.2":
        raise RuntimeError(
            "%s used Emacs %r instead of the pinned 30.2 oracle contract"
            % (result.editor, result.records["emacs-version"])
        )


def validate_restart(result: PhaseResult) -> None:
    require_success(result)
    expected = {
        "version": "0.9.4",
        "origin": "eat-0.9.4/eat.elc",
        "tests.total": "57",
        "tests.expected": "57",
        "tests.unexpected": "0",
    }
    if dict(result.records) != expected:
        raise RuntimeError(
            "%s restart records %r, expected %r"
            % (result.editor, dict(result.records), expected)
        )


def validate_process(result: PhaseResult) -> None:
    require_success(result)
    if dict(result.records) != EXPECTED_PROCESS_RECORDS:
        raise RuntimeError(
            "%s process records %r, expected %r"
            % (result.editor, dict(result.records), EXPECTED_PROCESS_RECORDS)
        )


def compare_results(left: PhaseResult, right: PhaseResult) -> None:
    if dict(left.records) != dict(right.records):
        raise RuntimeError(
            "%s record mismatch:\nGNU: %r\nEmaxx: %r"
            % (left.phase, dict(left.records), dict(right.records))
        )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("emaxx_binary", type=Path)
    parser.add_argument("gnu_binary", type=Path)
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=Path("target/eat-package-gate/artifacts"),
        help="verified release/source artifact cache",
    )
    parser.add_argument("--offline", action="store_true", help="forbid downloads")
    return parser.parse_args(argv)


def resolve_binary(value: Path) -> Path:
    text = str(value)
    if os.sep not in text:
        located = shutil.which(text)
        if located:
            return Path(located).resolve()
    return value.resolve()


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    repository = Path(__file__).resolve().parent.parent
    process_gate = repository / "tools" / "eat_process_gate.el"
    emaxx = resolve_binary(args.emaxx_binary)
    gnu = resolve_binary(args.gnu_binary)
    cache = args.artifact_dir.resolve()
    for binary, label in ((emaxx, "Emaxx"), (gnu, "GNU Emacs")):
        if not binary.is_file():
            print("ERROR: no %s binary at %s" % (label, binary), file=sys.stderr)
            return 2
    if not process_gate.is_file():
        print("ERROR: missing process gate at %s" % process_gate, file=sys.stderr)
        return 2

    try:
        with tempfile.TemporaryDirectory(prefix="emaxx-eat-package-gate-") as temp:
            work = Path(temp)
            archive = work / "archive"
            upstream_tests = build_inputs(
                cache, archive, work / "source", args.offline
            )
            roots = {name: work / name for name in ("gnu", "emaxx")}
            for root in roots.values():
                root.mkdir()

            phases: Dict[str, Dict[str, PhaseResult]] = {
                "install": {},
                "restart": {},
                "process": {},
            }
            for name, binary in (("gnu", gnu), ("emaxx", emaxx)):
                install = run_generated_phase(
                    name,
                    binary,
                    "install",
                    install_lisp(roots[name], archive),
                    roots[name],
                )
                validate_install(install)
                phases["install"][name] = install

                restart = run_generated_phase(
                    name,
                    binary,
                    "restart",
                    restart_lisp(roots[name], archive, upstream_tests),
                    roots[name],
                )
                validate_restart(restart)
                phases["restart"][name] = restart

                process = run_phase(
                    name,
                    binary,
                    "process",
                    process_gate,
                    roots[name],
                    {"EAT_GATE_ROOT": str(roots[name])},
                )
                validate_process(process)
                phases["process"][name] = process
                print(
                    "PASS: %s install, 2 compiled files, 57 upstream tests, process gate"
                    % name
                )

            for phase in ("install", "restart", "process"):
                compare_results(phases[phase]["gnu"], phases[phase]["emaxx"])
            print("PASS: GNU/Emaxx Eat package records match exactly")
    except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
        print("ERROR: %s" % error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
