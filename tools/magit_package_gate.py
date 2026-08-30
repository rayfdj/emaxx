#!/usr/bin/env python3
"""Install the pinned Magit closure through package.el in GNU Emacs and Emaxx.

The inputs are the exact GNU ELPA and NonGNU ELPA release tarballs named in
``ARTIFACTS``.  Every cached or downloaded tarball is SHA-256 verified before
it is exposed through a disposable local package archive.  Both editors then
perform the same real ``package-refresh-contents``/``package-install`` journey
in fresh roots, restart, and load Magit exclusively from the installed tree.

Use ``--offline`` to forbid downloads.  The optional TTY phase reuses those
fresh installed roots for the Magit scenarios in ``tools/ttydiff.py``.
"""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from typing import Dict, Mapping, NamedTuple, Optional, Sequence, Tuple
import urllib.request


MARKER = "MAGIT_GATE\t"
ERROR_MARKER = "MAGIT_GATE_ERROR\t"


class Artifact(NamedTuple):
    filename: str
    url: str
    sha256: str


ARTIFACTS = (
    Artifact(
        "compat-31.0.0.2.tar",
        "https://elpa.gnu.org/packages/compat-31.0.0.2.tar",
        "47d8693a10087f8b20c72e6a78b628db980cb7547c4f8f517fc5d11acd8b0f38",
    ),
    Artifact(
        "cond-let-1.1.3.tar",
        "https://elpa.nongnu.org/nongnu/cond-let-1.1.3.tar",
        "8efd86f6023f53b030a1d62bb722544ef6b5270c352cd5734444df3c70ceb17f",
    ),
    Artifact(
        "llama-1.0.5.tar",
        "https://elpa.nongnu.org/nongnu/llama-1.0.5.tar",
        "54f02ff7fcb4aa373e673ede1f3c762dd97fb462ee79987b4cd745759488da83",
    ),
    Artifact(
        "magit-4.7.0.tar",
        "https://elpa.nongnu.org/nongnu/magit-4.7.0.tar",
        "3f64b12e5e4403769109cbb85008026a12646333b839153b0b819ca88e4776ac",
    ),
    Artifact(
        "magit-section-4.7.0.tar",
        "https://elpa.nongnu.org/nongnu/magit-section-4.7.0.tar",
        "221be8ea0920e5ab429f259a9c808052300f4b1a0fadbfe85a750564d0baa889",
    ),
    Artifact(
        "seq-2.24.tar",
        "https://elpa.gnu.org/packages/seq-2.24.tar",
        "8693439fd9bc447345aa6e1b5a4121107a474c4e7de5a511bbd2b8586aa0a88f",
    ),
    Artifact(
        "transient-0.13.7.tar",
        "https://elpa.gnu.org/packages/transient-0.13.7.tar",
        "9b03d2f20b7bd34d89d12f778d07aa6b993ac37b5ecfb6e5f2e152fb1403ac52",
    ),
    Artifact(
        "with-editor-3.5.3.tar",
        "https://elpa.nongnu.org/nongnu/with-editor-3.5.3.tar",
        "8701de1a9adaf0704609c24b26e90f3f98427a17c573c4e856bef0abdc076dbb",
    ),
)


ARCHIVE_CONTENTS = """(1
 (compat . [(31 0 0 2) ((emacs (25 1))) "Emacs Lisp Compatibility Library" tar])
 (cond-let . [(1 1 3) ((emacs (28 1))) "Additional and improved binding conditionals" tar])
 (llama . [(1 0 5) ((emacs (26 1)) (compat (31 0))) "Compact syntax for short lambda" tar])
 (magit . [(4 7 0) ((emacs (28 1)) (compat (31 0)) (cond-let (1 1)) (llama (1 0)) (magit-section (4 7)) (seq (2 24)) (transient (0 13)) (with-editor (3 5))) "A Git porcelain inside Emacs" tar])
 (magit-section . [(4 7 0) ((emacs (28 1)) (compat (31 0)) (cond-let (1 1)) (llama (1 0)) (seq (2 24))) "Sections for read-only buffers" tar])
 (seq . [(2 24) nil "Sequence manipulation functions" tar])
 (transient . [(0 13 7) ((emacs (28 1)) (compat (31 0)) (cond-let (1 1)) (llama (1 0)) (seq (2 24))) "Transient commands" tar])
 (with-editor . [(3 5 3) ((emacs (28 1)) (compat (31 0)) (cond-let (1 1)) (llama (1 0))) "Use the Emacsclient as $EDITOR" tar]))
"""


EXPECTED_INSTALLED = (
    "compat-31.0.0.2",
    "cond-let-1.1.3",
    "llama-1.0.5",
    "magit-4.7.0",
    "magit-section-4.7.0",
    "transient-0.13.7",
    "with-editor-3.5.3",
)

EXPECTED_COMPILED = (
    "compat-31.0.0.2/compat-26.elc",
    "compat-31.0.0.2/compat-27.elc",
    "compat-31.0.0.2/compat-28.elc",
    "compat-31.0.0.2/compat-29.elc",
    "compat-31.0.0.2/compat-30.elc",
    "compat-31.0.0.2/compat-31.elc",
    "compat-31.0.0.2/compat.elc",
    "cond-let-1.1.3/cond-let.elc",
    "llama-1.0.5/llama.elc",
    "magit-4.7.0/git-commit.elc",
    "magit-4.7.0/git-rebase.elc",
    "magit-4.7.0/magit-apply.elc",
    "magit-4.7.0/magit-autorevert.elc",
    "magit-4.7.0/magit-base.elc",
    "magit-4.7.0/magit-bisect.elc",
    "magit-4.7.0/magit-blame.elc",
    "magit-4.7.0/magit-bookmark.elc",
    "magit-4.7.0/magit-branch.elc",
    "magit-4.7.0/magit-bundle.elc",
    "magit-4.7.0/magit-clone.elc",
    "magit-4.7.0/magit-commit.elc",
    "magit-4.7.0/magit-core.elc",
    "magit-4.7.0/magit-diff.elc",
    "magit-4.7.0/magit-dired.elc",
    "magit-4.7.0/magit-ediff.elc",
    "magit-4.7.0/magit-extras.elc",
    "magit-4.7.0/magit-fetch.elc",
    "magit-4.7.0/magit-files.elc",
    "magit-4.7.0/magit-git.elc",
    "magit-4.7.0/magit-gitignore.elc",
    "magit-4.7.0/magit-log.elc",
    "magit-4.7.0/magit-margin.elc",
    "magit-4.7.0/magit-merge.elc",
    "magit-4.7.0/magit-mode.elc",
    "magit-4.7.0/magit-notes.elc",
    "magit-4.7.0/magit-patch.elc",
    "magit-4.7.0/magit-process.elc",
    "magit-4.7.0/magit-pull.elc",
    "magit-4.7.0/magit-push.elc",
    "magit-4.7.0/magit-reflog.elc",
    "magit-4.7.0/magit-refs.elc",
    "magit-4.7.0/magit-remote.elc",
    "magit-4.7.0/magit-repos.elc",
    "magit-4.7.0/magit-reset.elc",
    "magit-4.7.0/magit-sequence.elc",
    "magit-4.7.0/magit-sparse-checkout.elc",
    "magit-4.7.0/magit-stash.elc",
    "magit-4.7.0/magit-status.elc",
    "magit-4.7.0/magit-submodule.elc",
    "magit-4.7.0/magit-subtree.elc",
    "magit-4.7.0/magit-tag.elc",
    "magit-4.7.0/magit-transient.elc",
    "magit-4.7.0/magit-wip.elc",
    "magit-4.7.0/magit-worktree.elc",
    "magit-4.7.0/magit.elc",
    "magit-section-4.7.0/magit-section.elc",
    "transient-0.13.7/transient.elc",
    "with-editor-3.5.3/with-editor.elc",
)

MAGIT_TTY_SCENARIOS = (
    "magit-status-sections-stage",
    "magit-diff-log-transient",
    "magit-process-error",
    "magit-repository-not-found",
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
    for artifact in ARTIFACTS:
        source = obtain_artifact(cache, artifact, offline)
        copied = destination / artifact.filename
        shutil.copyfile(source, copied)
        verify_artifact(copied, artifact)
    (destination / "archive-contents").write_text(ARCHIVE_CONTENTS, encoding="utf-8")


def lisp_string(value: object) -> str:
    import json

    return json.dumps(str(value), ensure_ascii=True)


def common_lisp(root: Path, archive: Path) -> str:
    return r"""
      (setq user-emacs-directory (file-name-as-directory %s)
            package-user-dir %s
            package-archives (list (cons "pinned" (file-name-as-directory %s)))
            ;; The disposable mirror contains verified release tarballs but
            ;; not detached archive signatures.  Hash verification happens
            ;; before package.el can see any artifact.
            package-check-signature 'allow-unsigned)
      (require 'cl-lib)
      (require 'package)
      (defun magit-gate-emit (key value)
        (princ (format "MAGIT_GATE\t%%s\t%%s\n" key value)))
      (defun magit-gate-installed-descs ()
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
      (defun magit-gate-compiled-record ()
        (let (compiled)
          (dolist (desc (magit-gate-installed-descs))
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
  (condition-case magit-gate-error
      (progn
%s)
    (error
     (princ (format "MAGIT_GATE_ERROR\\t%%S\\n" magit-gate-error))
     (kill-emacs 1))))
""" % (common_lisp(root, archive), body)


def install_lisp(root: Path, archive: Path) -> str:
    return wrapped_lisp(
        root,
        archive,
        r"""
        (package-initialize)
        (package-refresh-contents)
        (let* ((desc (car (cdr (assq 'magit package-archive-contents))))
               (transaction
                (package-compute-transaction
                 (list desc) (package-desc-reqs desc))))
          (unless (and desc
                       (equal (package-desc-version desc) '(4 7 0))
                       (equal (package-desc-archive desc) "pinned"))
            (error "Pinned Magit descriptor was not selected: %S" desc))
          (magit-gate-emit
           "transaction"
           (mapconcat #'package-desc-full-name
                      (sort (copy-sequence transaction)
                            (lambda (left right)
                              (string< (package-desc-full-name left)
                                       (package-desc-full-name right))))
                      ","))
          (package-install desc))
        (package-initialize)
        (magit-gate-emit
         "installed"
         (mapconcat #'package-desc-full-name
                    (magit-gate-installed-descs) ","))
        (magit-gate-emit "compiled" (magit-gate-compiled-record))
        (let* ((installed (car (cdr (assq 'magit package-alist))))
               (autoload-file
                (and installed
                     (expand-file-name "magit-autoloads.el"
                                       (package-desc-dir installed)))))
          (magit-gate-emit
           "autoload-file"
           (if (and autoload-file (file-exists-p autoload-file))
               "true" "false")))
        (magit-gate-emit
         "autoload-before-restart"
         (if (autoloadp (symbol-function 'magit-status)) "true" "false"))
        """,
    )


def restart_lisp(root: Path, archive: Path) -> str:
    return wrapped_lisp(
        root,
        archive,
        r"""
        (package-initialize)
        (magit-gate-emit
         "autoload-after-restart"
         (if (autoloadp (symbol-function 'magit-status)) "true" "false"))
        (require 'magit)
        (magit-gate-emit "version" (magit-version))
        (dolist (feature '(compat cond-let llama magit magit-section
                           transient with-editor))
          (let ((library (locate-library (symbol-name feature))))
            (unless (and library (file-in-directory-p library package-user-dir))
              (error "%S resolved outside package-user-dir: %S" feature library))
            (magit-gate-emit
             (format "origin.%s" feature)
             (file-name-nondirectory
              (directory-file-name (file-name-directory library))))))
        """,
    )


def parse_records(output: str) -> Dict[str, str]:
    records: Dict[str, str] = {}
    for line in output.splitlines():
        if not line.startswith(MARKER):
            continue
        parts = line.split("\t", 2)
        if len(parts) != 3 or parts[1] in records:
            raise ValueError("malformed or duplicate Magit gate record: %r" % line)
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
    environment.update(
        {
            "HOME": str(home),
            "LANG": "C",
            "LC_ALL": "C",
        }
    )
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
        return PhaseResult(
            editor,
            phase,
            completed.returncode,
            completed.stdout,
            completed.stderr,
            {"protocol-error": str(error)},
        )
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
    required = {
        "transaction",
        "installed",
        "compiled",
        "autoload-file",
        "autoload-before-restart",
    }
    if set(result.records) != required:
        raise RuntimeError(
            "%s install emitted keys %r, expected %r"
            % (result.editor, sorted(result.records), sorted(required))
        )
    installed = split_csv(result.records["installed"])
    if installed != EXPECTED_INSTALLED:
        raise RuntimeError(
            "%s installed %r, expected %r"
            % (result.editor, installed, EXPECTED_INSTALLED)
        )
    transaction = split_csv(result.records["transaction"])
    if transaction != EXPECTED_INSTALLED:
        raise RuntimeError(
            "%s resolved transaction %r, expected %r"
            % (result.editor, transaction, EXPECTED_INSTALLED)
        )
    if result.records["autoload-file"] != "true":
        raise RuntimeError("%s did not generate magit-autoloads.el" % result.editor)
    compiled = split_csv(result.records["compiled"])
    if compiled != EXPECTED_COMPILED:
        raise RuntimeError(
            "%s compiled inventory differs\nactual: %r\nexpected: %r"
            % (result.editor, compiled, EXPECTED_COMPILED)
        )


def validate_restart(result: PhaseResult) -> None:
    require_success(result)
    expected = {
        "autoload-after-restart": "true",
        "version": "4.7.0",
        "origin.compat": "compat-31.0.0.2",
        "origin.cond-let": "cond-let-1.1.3",
        "origin.llama": "llama-1.0.5",
        "origin.magit": "magit-4.7.0",
        "origin.magit-section": "magit-section-4.7.0",
        "origin.transient": "transient-0.13.7",
        "origin.with-editor": "with-editor-3.5.3",
    }
    if dict(result.records) != expected:
        raise RuntimeError(
            "%s restart records %r, expected %r"
            % (result.editor, dict(result.records), expected)
        )


def compare_results(left: PhaseResult, right: PhaseResult) -> None:
    if dict(left.records) != dict(right.records):
        raise RuntimeError(
            "%s/%s record mismatch:\nGNU: %r\nEmaxx: %r"
            % (left.phase, right.phase, dict(left.records), dict(right.records))
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
            "EMAXX_TTYDIFF_MAGIT_GNU_ROOT": str(gnu_root),
            "EMAXX_TTYDIFF_MAGIT_EMAXX_ROOT": str(emaxx_root),
        }
    )
    command = [
        sys.executable,
        str(repository / "tools" / "ttydiff.py"),
        str(emaxx),
        str(gnu),
        str(gnu_lisp_dir),
        *scenarios,
    ]
    completed = subprocess.run(
        command,
        cwd=repository,
        env=environment,
        timeout=900,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError("Magit TTY gate failed with exit %d" % completed.returncode)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("emaxx_binary", type=Path)
    parser.add_argument("gnu_binary", type=Path)
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=Path("target/magit-package-gate/artifacts"),
        help="verified release-tarball cache",
    )
    parser.add_argument("--offline", action="store_true", help="forbid downloads")
    parser.add_argument(
        "--tty",
        action="store_true",
        help="also run the strict interactive Magit TTY scenarios",
    )
    parser.add_argument(
        "--gnu-lisp-dir",
        type=Path,
        help="GNU Lisp source tree required by ttydiff",
    )
    parser.add_argument(
        "--tty-scenario",
        action="append",
        choices=MAGIT_TTY_SCENARIOS,
        help="run only this Magit TTY scenario (repeatable)",
    )
    return parser.parse_args(argv)


def resolve_binary(value: Path) -> Path:
    """Resolve either an explicit path or a command available through PATH."""
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
        with tempfile.TemporaryDirectory(prefix="emaxx-magit-package-gate-") as temp:
            work = Path(temp)
            archive = work / "archive"
            build_local_archive(cache, archive, args.offline)
            roots = {name: work / name for name in ("gnu", "emaxx")}
            for root in roots.values():
                root.mkdir()

            install_results = {}
            restart_results = {}
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
                print(
                    "PASS: %s clean install, 58 byte-compiled payloads, restart"
                    % name
                )

            compare_results(install_results["gnu"], install_results["emaxx"])
            compare_results(restart_results["gnu"], restart_results["emaxx"])
            print("PASS: GNU/Emaxx package.el records match exactly")

            if args.tty:
                run_tty_gate(
                    repository,
                    emaxx,
                    gnu,
                    args.gnu_lisp_dir.resolve(),
                    roots["gnu"],
                    roots["emaxx"],
                    args.tty_scenario or MAGIT_TTY_SCENARIOS,
                )
                print("PASS: strict Magit TTY scenarios match")
    except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
        print("ERROR: %s" % error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
