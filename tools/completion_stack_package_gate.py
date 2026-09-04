#!/usr/bin/env python3
"""Certify pinned Vertico, Consult, and Corfu releases on GNU and Emaxx.

The gate hash-verifies official package and source archives, exposes only the
package archives through a disposable local package archive, and gives both
editors separate empty package roots.  Each editor performs the same ordinary
package.el transaction, restarts, and loads only installed bytecode.  The
optional TTY phase then drives the shared interactive completion journeys in
``tools/ttydiff.py`` through those installed roots.

Corfu uses the official corfu-terminal/Popon frontend because this contract is
for Emacs 30.2 terminals.  Corfu's built-in terminal child-frame support is an
Emacs 31 feature and is deliberately not claimed here.
"""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
from typing import Dict, Mapping, NamedTuple, Optional, Sequence, Tuple
import urllib.request


MARKER = "COMPLETION_STACK_GATE\t"
ERROR_MARKER = "COMPLETION_STACK_GATE_ERROR\t"


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
        "consult-3.7.tar",
        "https://elpa.gnu.org/packages/consult-3.7.tar",
        "63f1724728fa7fbcab315e1aef2cf13d647774374b97fb27e8f862d528dbb1a7",
        True,
    ),
    Artifact(
        "corfu-2.14.tar",
        "https://elpa.gnu.org/packages/corfu-2.14.tar",
        "c6ec346e5666badce80e693ba7fbb9c0e0e02627c200b570f255ba84a4d91aa8",
        True,
    ),
    Artifact(
        "corfu-terminal-0.7.tar",
        "https://elpa.nongnu.org/nongnu/corfu-terminal-0.7.tar",
        "946a63459c7255d0df7ebad0170f2b56f20c3bc798efc704b63146a2aa838128",
        True,
    ),
    Artifact(
        "popon-0.13.tar",
        "https://elpa.nongnu.org/nongnu/popon-0.13.tar",
        "abcda58b0bbfe3998140a47200ba0a1ef9ebe62dbccede192adcb50b863c157c",
        True,
    ),
    Artifact(
        "vertico-2.13.tar",
        "https://elpa.gnu.org/packages/vertico-2.13.tar",
        "3ac95cd8f9159670b0fbbb7a3f1cfb0c0a9f44c437e44482106837334b422c3a",
        True,
    ),
    Artifact(
        "consult-3.7-source.tar.gz",
        "https://codeload.github.com/minad/consult/tar.gz/3ddec5493bce5445f099537be50b7a4f79c68321",
        "666a663df5087d64ad44de732fd41bb6982f25bb88df776009dd8208c09f5c80",
        False,
    ),
    Artifact(
        "corfu-2.14-source.tar.gz",
        "https://codeload.github.com/minad/corfu/tar.gz/75be36fe63e78c63ac71c32039ab07836bd532ac",
        "161cc504e13d0870af38207a54f70ef6c7bb001eb56b0cba0db3a7e1e01092c7",
        False,
    ),
    Artifact(
        "corfu-terminal-0.7-source.tar.gz",
        "https://codeberg.org/akib/emacs-corfu-terminal/archive/501548c3d51f926c687e8cd838c5865ec45d03cc.tar.gz",
        "88402635bf4d967dba0238baed5a2a6a370591c730d6ba05de2be4680d33e334",
        False,
    ),
    Artifact(
        "popon-0.13-source.tar.gz",
        "https://codeberg.org/akib/emacs-popon/archive/bf8174cb7e6e8fe0fe91afe6b01b6562c4dc39da.tar.gz",
        "5f7c3d31dd69370db031ebacb45432daa3dcce7827d9a77783772ed1d94c5978",
        False,
    ),
    Artifact(
        "vertico-2.13-source.tar.gz",
        "https://codeload.github.com/minad/vertico/tar.gz/a6874e3d8c74a9eea77967d702d608ebbd6b27ec",
        "cbb94a61a490b6f1aba4a9f6441bbee7fad22a6731607fe7fa09917b34b07433",
        False,
    ),
)


ARCHIVE_CONTENTS = """(1
 (compat . [(31 0 0 2) ((emacs (25 1))) "Emacs Lisp Compatibility Library" tar])
 (consult . [(3 7) ((emacs (29 1)) (compat (31))) "Search and navigate via completing-read" tar])
 (corfu . [(2 14) ((emacs (29 1)) (compat (31))) "COmpletion in Region FUnction" tar])
 (corfu-terminal . [(0 7) ((emacs (26 1)) (corfu (0 36)) (popon (0 13))) "Corfu popup on terminal" tar])
 (popon . [(0 13) ((emacs (25 1))) "Pop floating text on a window" tar])
 (vertico . [(2 13) ((emacs (29 1)) (compat (31))) "VERTical Interactive COmpletion" tar]))
"""


EXPECTED_TRANSACTION = (
    "compat-31.0.0.2",
    "consult-3.7",
    "corfu-2.14",
    "corfu-terminal-0.7",
    "popon-0.13",
    "vertico-2.13",
)
EXPECTED_INSTALLED = EXPECTED_TRANSACTION

EXPECTED_COMPILED = tuple(
    sorted(
        (
            "compat-31.0.0.2/compat-26.elc",
            "compat-31.0.0.2/compat-27.elc",
            "compat-31.0.0.2/compat-28.elc",
            "compat-31.0.0.2/compat-29.elc",
            "compat-31.0.0.2/compat-30.elc",
            "compat-31.0.0.2/compat-31.elc",
            "compat-31.0.0.2/compat.elc",
            "consult-3.7/consult-compile.elc",
            "consult-3.7/consult-flymake.elc",
            "consult-3.7/consult-imenu.elc",
            "consult-3.7/consult-info.elc",
            "consult-3.7/consult-kmacro.elc",
            "consult-3.7/consult-org.elc",
            "consult-3.7/consult-register.elc",
            "consult-3.7/consult-xref.elc",
            "consult-3.7/consult.elc",
            "corfu-2.14/corfu-auto.elc",
            "corfu-2.14/corfu-echo.elc",
            "corfu-2.14/corfu-history.elc",
            "corfu-2.14/corfu-indexed.elc",
            "corfu-2.14/corfu-info.elc",
            "corfu-2.14/corfu-mouse.elc",
            "corfu-2.14/corfu-popupinfo.elc",
            "corfu-2.14/corfu-quick.elc",
            "corfu-2.14/corfu.elc",
            "corfu-terminal-0.7/corfu-terminal.elc",
            "popon-0.13/popon.elc",
            "vertico-2.13/vertico-buffer.elc",
            "vertico-2.13/vertico-directory.elc",
            "vertico-2.13/vertico-flat.elc",
            "vertico-2.13/vertico-grid.elc",
            "vertico-2.13/vertico-indexed.elc",
            "vertico-2.13/vertico-mouse.elc",
            "vertico-2.13/vertico-multiform.elc",
            "vertico-2.13/vertico-quick.elc",
            "vertico-2.13/vertico-repeat.elc",
            "vertico-2.13/vertico-reverse.elc",
            "vertico-2.13/vertico-sort.elc",
            "vertico-2.13/vertico-suspend.elc",
            "vertico-2.13/vertico-unobtrusive.elc",
            "vertico-2.13/vertico.elc",
        )
    )
)

EXPECTED_AUTOLOADS = (
    "compat-31.0.0.2/compat-autoloads.el",
    "consult-3.7/consult-autoloads.el",
    "corfu-2.14/corfu-autoloads.el",
    "corfu-terminal-0.7/corfu-terminal-autoloads.el",
    "popon-0.13/popon-autoloads.el",
    "vertico-2.13/vertico-autoloads.el",
)

EXPECTED_ORIGINS = {
    "origin.compat": "compat-31.0.0.2/compat.elc",
    "origin.consult": "consult-3.7/consult.elc",
    "origin.corfu": "corfu-2.14/corfu.elc",
    "origin.corfu-terminal": "corfu-terminal-0.7/corfu-terminal.elc",
    "origin.popon": "popon-0.13/popon.elc",
    "origin.vertico": "vertico-2.13/vertico.elc",
}

COMPLETION_STACK_TTY_SCENARIOS = (
    "stack-vertico",
    "stack-consult-line",
    "stack-consult-grep",
    "stack-corfu",
)

_TEST_FILE = re.compile(r"(?:^|/)(?:tests?/|[^/]*[-_]tests?\.el)$", re.IGNORECASE)


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


def source_test_inventory(source_archives: Sequence[Path]) -> Tuple[str, ...]:
    tests = []
    for source_archive in source_archives:
        with tarfile.open(source_archive, "r:gz") as archive:
            tests.extend(
                "%s:%s" % (source_archive.name, member.name)
                for member in archive.getmembers()
                if member.isfile() and _TEST_FILE.search(member.name)
            )
    return tuple(sorted(tests))


def build_inputs(cache: Path, archive: Path, offline: bool) -> Tuple[str, ...]:
    archive.mkdir(parents=True, exist_ok=False)
    source_archives = []
    for artifact in ARTIFACTS:
        source = obtain_artifact(cache, artifact, offline)
        if artifact.package_archive:
            copied = archive / artifact.filename
            shutil.copyfile(source, copied)
            verify_artifact(copied, artifact)
        else:
            source_archives.append(source)
    (archive / "archive-contents").write_text(ARCHIVE_CONTENTS, encoding="utf-8")
    if len(source_archives) != 5:
        raise ValueError("expected five pinned upstream source archives")
    return source_test_inventory(source_archives)


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
      (defun completion-stack-gate-emit (key value)
        (princ (format "COMPLETION_STACK_GATE\t%%s\t%%s\n" key value)))
      (defun completion-stack-gate-installed-descs ()
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
      (defun completion-stack-gate-compiled-record ()
        (let (compiled)
          (dolist (desc (completion-stack-gate-installed-descs))
            (dolist (file (directory-files-recursively
                           (package-desc-dir desc) "\\.elc\\'"))
              (push (concat (package-desc-full-name desc) "/"
                            (file-relative-name file (package-desc-dir desc)))
                    compiled)))
          (mapconcat #'identity (sort compiled #'string<) ",")))
      (defun completion-stack-gate-autoload-record ()
        (let (autoloads)
          (dolist (desc (completion-stack-gate-installed-descs))
            (let ((file (expand-file-name
                         (format "%%s-autoloads.el" (package-desc-name desc))
                         (package-desc-dir desc))))
              (when (file-exists-p file)
                (push (concat (package-desc-full-name desc) "/"
                              (file-name-nondirectory file))
                      autoloads))))
          (mapconcat #'identity (sort autoloads #'string<) ",")))
    """ % (
        lisp_string(str(root) + os.sep),
        lisp_string(root / "packages"),
        lisp_string(str(archive) + os.sep),
    )


def wrapped_lisp(root: Path, archive: Path, body: str) -> str:
    return """(progn
%s
  (condition-case completion-stack-gate-error
      (progn
%s)
    (error
     (princ (format "COMPLETION_STACK_GATE_ERROR\\t%%S\\n"
                    completion-stack-gate-error))
     (kill-emacs 1))))
""" % (common_lisp(root, archive), body)


def install_lisp(root: Path, archive: Path) -> str:
    return wrapped_lisp(
        root,
        archive,
        r"""
        (package-initialize)
        (completion-stack-gate-emit "emacs-version" emacs-version)
        (package-refresh-contents)
        (let* ((names '(vertico consult corfu-terminal))
               (descs
                (mapcar (lambda (name)
                          (car (cdr (assq name package-archive-contents))))
                        names))
               (requirements
                (apply #'append (mapcar #'package-desc-reqs descs)))
               (transaction
                (package-compute-transaction descs requirements)))
          (unless (equal (mapcar #'package-desc-version descs)
                         '((2 13) (3 7) (0 7)))
            (error "Pinned completion descriptors were not selected: %S" descs))
          (unless (cl-every (lambda (desc)
                              (equal (package-desc-archive desc) "pinned"))
                            descs)
            (error "A completion descriptor came from another archive: %S" descs))
          (completion-stack-gate-emit
           "transaction"
           (mapconcat #'package-desc-full-name
                      (sort (copy-sequence transaction)
                            (lambda (left right)
                              (string< (package-desc-full-name left)
                                       (package-desc-full-name right))))
                      ","))
          (dolist (desc descs)
            (package-install desc)))
        (package-initialize)
        (completion-stack-gate-emit
         "installed"
         (mapconcat #'package-desc-full-name
                    (completion-stack-gate-installed-descs) ","))
        (completion-stack-gate-emit
         "compiled" (completion-stack-gate-compiled-record))
        (completion-stack-gate-emit
         "autoloads" (completion-stack-gate-autoload-record))
        """,
    )


def restart_lisp(root: Path, archive: Path) -> str:
    return wrapped_lisp(
        root,
        archive,
        r"""
        (package-initialize)
        (dolist (feature '(compat vertico consult corfu popon corfu-terminal))
          (require feature)
          (let ((library (locate-library (symbol-name feature))))
            (unless (and library
                         (file-in-directory-p library package-user-dir)
                         (string-suffix-p ".elc" library))
              (error "%S resolved outside installed bytecode: %S"
                     feature library))
            (completion-stack-gate-emit
             (format "origin.%s" feature)
             (file-relative-name library package-user-dir))))
        (completion-stack-gate-emit
         "versions"
         (mapconcat
          (lambda (name)
            (package-desc-full-name (car (cdr (assq name package-alist)))))
          '(compat consult corfu corfu-terminal popon vertico)
          ","))
        """,
    )


def parse_records(output: str) -> Dict[str, str]:
    records: Dict[str, str] = {}
    for line in output.splitlines():
        if not line.startswith(MARKER):
            continue
        parts = line.split("\t", 2)
        if len(parts) != 3 or parts[1] in records:
            raise ValueError(
                "malformed or duplicate completion-stack gate record: %r" % line
            )
        records[parts[1]] = parts[2]
    return records


def run_phase(
    editor: str,
    binary: Path,
    phase: str,
    script: str,
    root: Path,
) -> PhaseResult:
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
        "emacs-version",
        "transaction",
        "installed",
        "compiled",
        "autoloads",
    }
    if set(result.records) != expected_keys:
        raise RuntimeError(
            "%s install emitted keys %r, expected %r"
            % (result.editor, sorted(result.records), sorted(expected_keys))
        )
    if result.records["emacs-version"] != "30.2":
        raise RuntimeError(
            "%s used Emacs %r instead of 30.2"
            % (result.editor, result.records["emacs-version"])
        )
    for key, expected in (
        ("transaction", EXPECTED_TRANSACTION),
        ("installed", EXPECTED_INSTALLED),
        ("compiled", EXPECTED_COMPILED),
        ("autoloads", EXPECTED_AUTOLOADS),
    ):
        actual = split_csv(result.records[key])
        if actual != expected:
            raise RuntimeError(
                "%s %s inventory differs\nactual: %r\nexpected: %r"
                % (result.editor, key, actual, expected)
            )
def validate_restart(result: PhaseResult) -> None:
    require_success(result)
    expected = dict(EXPECTED_ORIGINS)
    expected["versions"] = ",".join(EXPECTED_INSTALLED)
    if dict(result.records) != expected:
        raise RuntimeError(
            "%s restart records %r, expected %r"
            % (result.editor, dict(result.records), expected)
        )


def compare_results(left: PhaseResult, right: PhaseResult) -> None:
    if dict(left.records) != dict(right.records):
        raise RuntimeError(
            "%s record mismatch:\nGNU: %r\nEmaxx: %r"
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
            "EMAXX_TTYDIFF_COMPLETION_GNU_ROOT": str(gnu_root),
            "EMAXX_TTYDIFF_COMPLETION_EMAXX_ROOT": str(emaxx_root),
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
        timeout=1200,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "completion-stack TTY gate failed with exit %d" % completed.returncode
        )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("emaxx_binary", type=Path)
    parser.add_argument("gnu_binary", type=Path)
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=Path("target/completion-stack-package-gate/artifacts"),
        help="verified package/source artifact cache",
    )
    parser.add_argument("--offline", action="store_true", help="forbid downloads")
    parser.add_argument(
        "--tty", action="store_true", help="run the strict interactive TTY journeys"
    )
    parser.add_argument(
        "--gnu-lisp-dir", type=Path, help="GNU Lisp tree required by ttydiff"
    )
    parser.add_argument(
        "--tty-scenario",
        action="append",
        choices=COMPLETION_STACK_TTY_SCENARIOS,
        help="run only this completion scenario (repeatable)",
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
        with tempfile.TemporaryDirectory(
            prefix="emaxx-completion-stack-package-gate-"
        ) as temp:
            work = Path(temp)
            archive = work / "archive"
            upstream_tests = build_inputs(cache, archive, args.offline)
            if upstream_tests:
                raise RuntimeError(
                    "pinned upstream archives unexpectedly ship tests: %r"
                    % (upstream_tests,)
                )
            roots = {name: work / name for name in ("gnu", "emaxx")}
            for root in roots.values():
                root.mkdir()

            phases: Dict[str, Dict[str, PhaseResult]] = {
                "install": {},
                "restart": {},
            }
            for name, binary in (("gnu", gnu), ("emaxx", emaxx)):
                install = run_phase(
                    name,
                    binary,
                    "install",
                    install_lisp(roots[name], archive),
                    roots[name],
                )
                validate_install(install)
                phases["install"][name] = install
                restart = run_phase(
                    name,
                    binary,
                    "restart",
                    restart_lisp(roots[name], archive),
                    roots[name],
                )
                validate_restart(restart)
                phases["restart"][name] = restart
                print(
                    "PASS: %s install, 41 compiled files, restart, no upstream tests shipped"
                    % name
                )

            for phase in ("install", "restart"):
                compare_results(phases[phase]["gnu"], phases[phase]["emaxx"])
            print("PASS: GNU/Emaxx completion package records match exactly")

            if args.tty:
                run_tty_gate(
                    repository,
                    emaxx,
                    gnu,
                    args.gnu_lisp_dir.resolve(),
                    roots["gnu"],
                    roots["emaxx"],
                    args.tty_scenario or COMPLETION_STACK_TTY_SCENARIOS,
                )
                print("PASS: strict completion-stack TTY journeys match")
    except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
        print("ERROR: %s" % error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
