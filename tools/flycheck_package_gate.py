#!/usr/bin/env python3
"""Install pinned Flycheck and compare real package, spec, and TTY journeys.

The runtime package and its test dependency are served from a disposable local
archive after every tarball has passed an exact SHA-256 check.  GNU Emacs and
Emaxx independently install Flycheck through package.el, restart from their
fresh installed trees, and run a fixed subset of Flycheck's own Buttercup
specs from the official release source.  The optional TTY phase reuses the
runtime roots and drives the deterministic command checker through ttydiff.
"""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import tarfile
import tempfile
from typing import Dict, Mapping, NamedTuple, Optional, Sequence, Tuple
import urllib.request


MARKER = "FLYCHECK_GATE\t"
ERROR_MARKER = "FLYCHECK_GATE_ERROR\t"


class Artifact(NamedTuple):
    filename: str
    url: str
    sha256: str


PACKAGE_ARTIFACTS = (
    Artifact(
        "buttercup-1.40.tar",
        "https://stable.melpa.org/packages/buttercup-1.40.tar",
        "849555794365ec0719331e8137dad4d6557208f0cef5e0de8cdc7cae4d050267",
    ),
    Artifact(
        "flycheck-39.0.tar",
        "https://stable.melpa.org/packages/flycheck-39.0.tar",
        "bd5f03913bac3e7e256b9032c202ad8e8a04621daafa9fe213d1bc126b5f9e2f",
    ),
    Artifact(
        "seq-2.24.tar",
        "https://elpa.gnu.org/packages/seq-2.24.tar",
        "8693439fd9bc447345aa6e1b5a4121107a474c4e7de5a511bbd2b8586aa0a88f",
    ),
)

SOURCE_ARTIFACT = Artifact(
    "flycheck-v39.0-source.tar.gz",
    "https://api.github.com/repos/flycheck/flycheck/tarball/v39.0",
    "e60395e8411c81c694ed988f96c6f51b4e6d237f3f27798fc1a373c91f4eaae3",
)

ARTIFACTS = PACKAGE_ARTIFACTS + (SOURCE_ARTIFACT,)

ARCHIVE_CONTENTS = """(1
 (buttercup . [(1 40) ((emacs (24 4))) "Behavior-Driven Emacs Lisp Testing" tar])
 (flycheck . [(39 0) ((emacs (28 1)) (seq (2 24))) "On-the-fly syntax checking" tar])
 (seq . [(2 24) nil "Sequence manipulation functions" tar]))
"""

EXPECTED_RUNTIME_INSTALLED = ("flycheck-39.0",)
EXPECTED_RUNTIME_COMPILED = ("flycheck-39.0/flycheck.elc",)
EXPECTED_SPEC_INSTALLED = ("buttercup-1.40", "flycheck-39.0")
EXPECTED_SPEC_COMPILED = (
    "buttercup-1.40/buttercup-compat.elc",
    "buttercup-1.40/buttercup.elc",
    "flycheck-39.0/flycheck.elc",
)

UPSTREAM_SPEC_FILES = (
    "test-error-filters.el",
    "test-error-parsers.el",
    "test-mode-line.el",
)

# The three pinned upstream files define exactly forty specs.  Keeping this
# explicit prevents a load failure from looking like a smaller green run.
EXPECTED_UPSTREAM_SPEC_COUNT = 40

FLYCHECK_TTY_SCENARIOS = (
    "flycheck-diagnostics-navigation",
    "flycheck-clean-idle-teardown",
    "flycheck-malformed-missing-tool",
    "flycheck-cancellation",
)


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


def build_local_archive(cache: Path, destination: Path, offline: bool) -> None:
    destination.mkdir(parents=True, exist_ok=False)
    for artifact in PACKAGE_ARTIFACTS:
        source = obtain_artifact(cache, artifact, offline)
        copied = destination / artifact.filename
        shutil.copyfile(source, copied)
        verify_artifact(copied, artifact)
    (destination / "archive-contents").write_text(ARCHIVE_CONTENTS, encoding="utf-8")


def extract_source(archive: Path, destination: Path) -> Path:
    destination.mkdir(parents=True, exist_ok=False)
    with tarfile.open(archive, "r:gz") as source:
        members = source.getmembers()
        roots = set()
        for member in members:
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts or not path.parts:
                raise ValueError("unsafe source archive member: %s" % member.name)
            if not (member.isfile() or member.isdir()):
                raise ValueError(
                    "unsupported source archive member: %s" % member.name
                )
            roots.add(path.parts[0])
        if len(roots) != 1:
            raise ValueError("source archive has unexpected roots: %r" % sorted(roots))
        source.extractall(destination)
    root = destination / roots.pop()
    required = [root / "flycheck.el"] + [root / "test" / "specs" / name for name in UPSTREAM_SPEC_FILES]
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise ValueError("source archive is missing required files: %r" % missing)
    return root


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
      (defun flycheck-gate-emit (key value)
        (princ (format "FLYCHECK_GATE\t%%s\t%%s\n" key value)))
      (defun flycheck-gate-installed-descs ()
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
      (defun flycheck-gate-installed-record ()
        (mapconcat #'package-desc-full-name
                   (flycheck-gate-installed-descs) ","))
      (defun flycheck-gate-compiled-record ()
        (let (compiled)
          (dolist (desc (flycheck-gate-installed-descs))
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
  (condition-case flycheck-gate-error
      (progn
%s)
    (error
     (princ (format "FLYCHECK_GATE_ERROR\\t%%S\\n" flycheck-gate-error))
     (kill-emacs 1))))
""" % (common_lisp(root, archive), body)


def install_lisp(root: Path, archive: Path) -> str:
    return wrapped_lisp(
        root,
        archive,
        r"""
        (package-initialize)
        (package-refresh-contents)
        (let* ((desc (car (cdr (assq 'flycheck package-archive-contents))))
               (transaction
                (package-compute-transaction
                 (list desc) (package-desc-reqs desc))))
          (unless (and desc
                       (equal (package-desc-version desc) '(39 0))
                       (equal (package-desc-archive desc) "pinned"))
            (error "Pinned Flycheck descriptor was not selected: %S" desc))
          (flycheck-gate-emit
           "transaction"
           (mapconcat #'package-desc-full-name
                      (sort (copy-sequence transaction)
                            (lambda (left right)
                              (string< (package-desc-full-name left)
                                       (package-desc-full-name right))))
                      ","))
          (package-install desc))
        (package-initialize)
        (flycheck-gate-emit "installed" (flycheck-gate-installed-record))
        (flycheck-gate-emit "compiled" (flycheck-gate-compiled-record))
        (let* ((installed (car (cdr (assq 'flycheck package-alist))))
               (autoload-file
                (and installed
                     (expand-file-name "flycheck-autoloads.el"
                                       (package-desc-dir installed)))))
          (flycheck-gate-emit
           "autoload-file"
           (if (and autoload-file (file-exists-p autoload-file)) "true" "false")))
        (flycheck-gate-emit
         "autoload-before-restart"
         (if (autoloadp (symbol-function 'flycheck-mode)) "true" "false"))
        """,
    )


def restart_lisp(root: Path, archive: Path) -> str:
    return wrapped_lisp(
        root,
        archive,
        r"""
        (package-initialize)
        (flycheck-gate-emit
         "autoload-after-restart"
         (if (autoloadp (symbol-function 'flycheck-mode)) "true" "false"))
        (require 'flycheck)
        (flycheck-gate-emit "version" (flycheck-version))
        (let ((library (locate-library "flycheck")))
          (unless (and library (file-in-directory-p library package-user-dir))
            (error "Flycheck resolved outside package-user-dir: %S" library))
          (flycheck-gate-emit
           "origin.flycheck"
           (file-name-nondirectory
            (directory-file-name (file-name-directory library)))))
        """,
    )


def upstream_lisp(root: Path, archive: Path, source: Path) -> str:
    spec_dir = source / "test" / "specs"
    loads = "\n".join(
        "        (load %s nil nil t)" % lisp_string(spec_dir / name)
        for name in UPSTREAM_SPEC_FILES
    )
    body = r"""
        (package-initialize)
        (package-refresh-contents)
        (dolist (name '(flycheck buttercup))
          (let ((desc (car (cdr (assq name package-archive-contents)))))
            (unless (and desc (equal (package-desc-archive desc) "pinned"))
              (error "Pinned %%S descriptor was not selected: %%S" name desc))
            (package-install desc)))
        (package-initialize)
        (require 'flycheck)
        (require 'buttercup)
        (require 'flycheck-buttercup)
        (setq buttercup-suites nil)
%s
        (let ((index 0)
              (passed t))
          (setq buttercup-reporter
                (lambda (event value)
                  (when (eq event 'spec-done)
                    (setq index (1+ index))
                    (let ((status (buttercup-spec-status value)))
                      (unless (eq status 'passed) (setq passed nil))
                      (flycheck-gate-emit
                       (format "spec.%%04d" index)
                       (format "%%s|%%s" status
                               (buttercup-spec-full-name value)))))))
          (buttercup-run t)
          (flycheck-gate-emit "spec-count" (number-to-string index))
          (flycheck-gate-emit "installed" (flycheck-gate-installed-record))
          (flycheck-gate-emit "compiled" (flycheck-gate-compiled-record))
          (unless passed (error "One or more pinned Flycheck specs failed")))
        """ % loads
    return wrapped_lisp(root, archive, body)


def parse_records(output: str) -> Dict[str, str]:
    records: Dict[str, str] = {}
    for line in output.splitlines():
        if not line.startswith(MARKER):
            continue
        parts = line.split("\t", 2)
        if len(parts) != 3 or parts[1] in records:
            raise ValueError("malformed or duplicate Flycheck gate record: %r" % line)
        records[parts[1]] = parts[2]
    return records


def run_phase(editor: str, binary: Path, phase: str, script: str, root: Path) -> PhaseResult:
    script_path = root / (phase + ".el")
    script_path.write_text(script, encoding="utf-8")
    home = root / "home"
    home.mkdir(exist_ok=True)
    environment = os.environ.copy()
    environment.update({"HOME": str(home), "LANG": "C", "LC_ALL": "C"})
    completed = subprocess.run(
        [str(binary), "--batch", "-Q", "-l", str(script_path)],
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
        "transaction",
        "installed",
        "compiled",
        "autoload-file",
        "autoload-before-restart",
    }
    if set(result.records) != expected_keys:
        raise RuntimeError(
            "%s install emitted keys %r, expected %r"
            % (result.editor, sorted(result.records), sorted(expected_keys))
        )
    for key in ("transaction", "installed"):
        actual = split_csv(result.records[key])
        if actual != EXPECTED_RUNTIME_INSTALLED:
            raise RuntimeError(
                "%s %s closure %r, expected %r"
                % (result.editor, key, actual, EXPECTED_RUNTIME_INSTALLED)
            )
    compiled = split_csv(result.records["compiled"])
    if compiled != EXPECTED_RUNTIME_COMPILED:
        raise RuntimeError(
            "%s compiled inventory %r, expected %r"
            % (result.editor, compiled, EXPECTED_RUNTIME_COMPILED)
        )
    if result.records["autoload-file"] != "true":
        raise RuntimeError("%s did not generate flycheck-autoloads.el" % result.editor)
    if result.records["autoload-before-restart"] != "true":
        raise RuntimeError("%s did not register the Flycheck autoload" % result.editor)


def validate_restart(result: PhaseResult) -> None:
    require_success(result)
    expected = {
        "autoload-after-restart": "true",
        "version": "39.0",
        "origin.flycheck": "flycheck-39.0",
    }
    if dict(result.records) != expected:
        raise RuntimeError(
            "%s restart records %r, expected %r"
            % (result.editor, dict(result.records), expected)
        )


def validate_upstream(result: PhaseResult) -> None:
    require_success(result)
    if split_csv(result.records.get("installed", "")) != EXPECTED_SPEC_INSTALLED:
        raise RuntimeError("%s upstream install closure differs: %r" % (result.editor, result.records))
    if split_csv(result.records.get("compiled", "")) != EXPECTED_SPEC_COMPILED:
        raise RuntimeError("%s upstream compiled inventory differs: %r" % (result.editor, result.records))
    count_text = result.records.get("spec-count")
    if count_text is None or not count_text.isdigit():
        raise RuntimeError("%s did not report a numeric spec count" % result.editor)
    count = int(count_text)
    if count != EXPECTED_UPSTREAM_SPEC_COUNT:
        raise RuntimeError(
            "%s ran %d upstream specs, expected %d"
            % (result.editor, count, EXPECTED_UPSTREAM_SPEC_COUNT)
        )
    spec_keys = tuple(sorted(key for key in result.records if key.startswith("spec.")))
    expected_keys = tuple("spec.%04d" % index for index in range(1, count + 1))
    if spec_keys != expected_keys:
        raise RuntimeError("%s upstream spec record sequence has gaps" % result.editor)
    failures = [result.records[key] for key in spec_keys if not result.records[key].startswith("passed|")]
    if failures:
        raise RuntimeError("%s upstream specs did not pass: %r" % (result.editor, failures))


def compare_results(left: PhaseResult, right: PhaseResult) -> None:
    if dict(left.records) != dict(right.records):
        raise RuntimeError(
            "%s GNU/Emaxx record mismatch:\nGNU: %r\nEmaxx: %r"
            % (left.phase, dict(left.records), dict(right.records))
        )


def run_tty_gate(
    repository: Path,
    emaxx: Path,
    gnu: Path,
    gnu_lisp_dir: Path,
    gnu_root: Path,
    emaxx_root: Path,
    scenarios: Sequence[str],
) -> None:
    environment = os.environ.copy()
    environment.update(
        {
            "EMAXX_TTYDIFF_REQUIRE": "1",
            "EMAXX_TTYDIFF_FLYCHECK_GNU_ROOT": str(gnu_root),
            "EMAXX_TTYDIFF_FLYCHECK_EMAXX_ROOT": str(emaxx_root),
            "EMAXX_TTYDIFF_FLYCHECK_CHECKER": str(
                repository / "tools" / "flycheck_fixture_checker.py"
            ),
        }
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(repository / "tools" / "ttydiff.py"),
            str(emaxx),
            str(gnu),
            str(gnu_lisp_dir),
            *scenarios,
        ],
        cwd=repository,
        env=environment,
        timeout=900,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError("Flycheck TTY gate failed with exit %d" % completed.returncode)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("emaxx_binary", type=Path)
    parser.add_argument("gnu_binary", type=Path)
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=Path("target/flycheck-package-gate/artifacts"),
        help="verified package and source tarball cache",
    )
    parser.add_argument("--offline", action="store_true", help="forbid downloads")
    parser.add_argument(
        "--tty",
        action="store_true",
        help="also run the strict interactive Flycheck TTY scenarios",
    )
    parser.add_argument(
        "--gnu-lisp-dir",
        type=Path,
        help="GNU Lisp source tree required by ttydiff",
    )
    parser.add_argument(
        "--tty-scenario",
        action="append",
        choices=FLYCHECK_TTY_SCENARIOS,
        help="run only this Flycheck TTY scenario (repeatable)",
    )
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
    emaxx = resolve_binary(args.emaxx_binary)
    gnu = resolve_binary(args.gnu_binary)
    cache = args.artifact_dir.resolve()
    for binary, label in ((emaxx, "Emaxx"), (gnu, "GNU Emacs")):
        if not binary.is_file():
            print("ERROR: no %s binary at %s" % (label, binary), file=sys.stderr)
            return 2
    if args.tty and not args.gnu_lisp_dir:
        print("ERROR: --tty requires --gnu-lisp-dir", file=sys.stderr)
        return 2

    try:
        with tempfile.TemporaryDirectory(prefix="emaxx-flycheck-package-gate-") as temp:
            work = Path(temp)
            archive = work / "archive"
            build_local_archive(cache, archive, args.offline)
            source_archive = obtain_artifact(cache, SOURCE_ARTIFACT, args.offline)
            source = extract_source(source_archive, work / "source")
            roots = {name: work / name for name in ("gnu", "emaxx")}
            spec_roots = {name: work / (name + "-specs") for name in ("gnu", "emaxx")}
            for root in tuple(roots.values()) + tuple(spec_roots.values()):
                root.mkdir()

            install_results = {}
            restart_results = {}
            upstream_results = {}
            for name, binary in (("gnu", gnu), ("emaxx", emaxx)):
                install = run_phase(
                    name,
                    binary,
                    "install",
                    install_lisp(roots[name], archive),
                    roots[name],
                )
                validate_install(install)
                install_results[name] = install
                restart = run_phase(
                    name,
                    binary,
                    "restart",
                    restart_lisp(roots[name], archive),
                    roots[name],
                )
                validate_restart(restart)
                restart_results[name] = restart
                upstream = run_phase(
                    name,
                    binary,
                    "upstream-specs",
                    upstream_lisp(spec_roots[name], archive, source),
                    spec_roots[name],
                )
                validate_upstream(upstream)
                upstream_results[name] = upstream
                print(
                    "PASS: %s clean Flycheck install, restart, and %d upstream specs"
                    % (name, EXPECTED_UPSTREAM_SPEC_COUNT)
                )

            for phase_results in (install_results, restart_results, upstream_results):
                compare_results(phase_results["gnu"], phase_results["emaxx"])
            print("PASS: GNU/Emaxx Flycheck package and upstream-spec records match")

            if args.tty:
                run_tty_gate(
                    repository,
                    emaxx,
                    gnu,
                    args.gnu_lisp_dir.resolve(),
                    roots["gnu"],
                    roots["emaxx"],
                    args.tty_scenario or FLYCHECK_TTY_SCENARIOS,
                )
                print("PASS: strict Flycheck TTY scenarios match")
    except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired, tarfile.TarError) as error:
        print("ERROR: %s" % error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
