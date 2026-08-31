#!/usr/bin/env python3
"""Install pinned lsp-mode through package.el and compare real LSP journeys.

The gate exposes only the SHA-256-verified release tarballs named in
``ARTIFACTS`` through a disposable local archive.  GNU Emacs and Emaxx each
perform the same package refresh, dependency transaction, install, restart,
and load from separate fresh package roots.  The optional TTY phase drives the
installed package against the ordinary deterministic stdio LSP fixture used by
the built-in Eglot contract.
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


MARKER = "LSP_MODE_GATE\t"
ERROR_MARKER = "LSP_MODE_GATE_ERROR\t"


class Artifact(NamedTuple):
    filename: str
    url: str
    sha256: str


ARTIFACTS = (
    Artifact(
        "dash-2.20.0.tar",
        "https://stable.melpa.org/packages/dash-2.20.0.tar",
        "b77386b4a103913c7b4bf3dfe066f412a86489841678f9dff89937cff4b8662e",
    ),
    Artifact(
        "f-0.21.0.tar",
        "https://stable.melpa.org/packages/f-0.21.0.tar",
        "7f6f1e7e4835a481698b196996c1372b7c508664896bd8071323528dcbdd1438",
    ),
    Artifact(
        "ht-2.3.tar",
        "https://stable.melpa.org/packages/ht-2.3.tar",
        "158984a989cf0db7f9e7151b2003ccd2e584209ee016c30c6340dc529cb35653",
    ),
    Artifact(
        "lsp-mode-10.0.0.tar",
        "https://stable.melpa.org/packages/lsp-mode-10.0.0.tar",
        "ad7d46d6bb5b2f840f73c5884cf86dd2678ff6ce68d1bede9ba6b9b60b5668ba",
    ),
    Artifact(
        "lv-0.15.0.tar",
        "https://stable.melpa.org/packages/lv-0.15.0.tar",
        "88feada6365c15f1037d5ca4daf005315dc28fa42a7e98f18794f7c3399b1c4c",
    ),
    Artifact(
        "markdown-mode-2.8.tar",
        "https://stable.melpa.org/packages/markdown-mode-2.8.tar",
        "74220b9337e064a185123dfa1f9e307ded19159aa42917d7c568b77807664ba2",
    ),
    Artifact(
        "s-1.13.1.tar",
        "https://stable.melpa.org/packages/s-1.13.1.tar",
        "8730ac9005a8629a674fc882782211240c7dc7e95910ae42ad448abf1743234f",
    ),
    Artifact(
        "spinner-1.7.4.tar",
        "https://elpa.gnu.org/packages/spinner-1.7.4.tar",
        "d9a82b8cc7ac6960a65d5e9b6822b6b64143cb73ca5f74530ac8aa8285c10853",
    ),
)


ARCHIVE_CONTENTS = """(1
 (dash . [(2 20 0) ((emacs (24))) "A modern list library for Emacs" tar])
 (f . [(0 21 0) ((emacs (24 1)) (s (1 7 0)) (dash (2 2 0))) "Modern API for working with files and directories" tar])
 (ht . [(2 3) ((dash (2 12 0))) "The missing hash table library for Emacs" tar])
 (lsp-mode . [(10 0 0) ((emacs (28 1)) (dash (2 18 0)) (f (0 21 0)) (ht (2 3)) (spinner (1 7 3)) (markdown-mode (2 3)) (lv (0)) (eldoc (1 11))) "LSP mode" tar])
 (lv . [(0 15 0) nil "Other echo area" tar])
 (markdown-mode . [(2 8) ((emacs (28 1))) "Major mode for Markdown-formatted text" tar])
 (s . [(1 13 1) nil "The long lost Emacs string manipulation library" tar])
 (spinner . [(1 7 4) ((emacs (24 3))) "Add spinners and progress-bars to the mode-line for ongoing operations" tar]))
"""


EXPECTED_INSTALLED = (
    "dash-2.20.0",
    "f-0.21.0",
    "ht-2.3",
    "lsp-mode-10.0.0",
    "lv-0.15.0",
    "markdown-mode-2.8",
    "s-1.13.1",
    "spinner-1.7.4",
)

_LSP_MODULES = (
    "lsp-actionscript",
    "lsp-ada",
    "lsp-angular",
    "lsp-ansible",
    "lsp-asm",
    "lsp-astro",
    "lsp-autotools",
    "lsp-awk",
    "lsp-bash",
    "lsp-beancount",
    "lsp-bufls",
    "lsp-c3",
    "lsp-camel",
    "lsp-clangd",
    "lsp-clojure",
    "lsp-cmake",
    "lsp-cobol",
    "lsp-completion",
    "lsp-copilot",
    "lsp-crates",
    "lsp-credo",
    "lsp-crystal",
    "lsp-csharp",
    "lsp-css",
    "lsp-cucumber",
    "lsp-cypher",
    "lsp-d",
    "lsp-dhall",
    "lsp-diagnostics",
    "lsp-dired",
    "lsp-dockerfile",
    "lsp-dot",
    "lsp-earthly",
    "lsp-elixir",
    "lsp-elm",
    "lsp-emmet",
    "lsp-erlang",
    "lsp-eslint",
    "lsp-fennel",
    "lsp-fish",
    "lsp-fortitude",
    "lsp-fortran",
    "lsp-fsharp",
    "lsp-futhark",
    "lsp-gdscript",
    "lsp-gleam",
    "lsp-glsl",
    "lsp-go",
    "lsp-golangci-lint",
    "lsp-graphql",
    "lsp-groovy",
    "lsp-hack",
    "lsp-haxe",
    "lsp-headerline",
    "lsp-html",
    "lsp-hy",
    "lsp-icons",
    "lsp-ido",
    "lsp-idris",
    "lsp-iedit",
    "lsp-inline-completion",
    "lsp-javascript",
    "lsp-jq",
    "lsp-json",
    "lsp-jsonnet",
    "lsp-just",
    "lsp-kotlin",
    "lsp-kubernetes-helm",
    "lsp-lens",
    "lsp-lisp",
    "lsp-lua",
    "lsp-magik",
    "lsp-markdown",
    "lsp-marksman",
    "lsp-matlab",
    "lsp-mdx",
    "lsp-meson",
    "lsp-mint",
    "lsp-mode",
    "lsp-modeline",
    "lsp-mojo",
    "lsp-move",
    "lsp-nextflow",
    "lsp-nginx",
    "lsp-nim",
    "lsp-nix",
    "lsp-nushell",
    "lsp-ocaml",
    "lsp-odin",
    "lsp-openscad",
    "lsp-perl",
    "lsp-perlnavigator",
    "lsp-php",
    "lsp-pls",
    "lsp-postgres",
    "lsp-prolog",
    "lsp-protocol",
    "lsp-purescript",
    "lsp-pwsh",
    "lsp-pyls",
    "lsp-pylsp",
    "lsp-python-ty",
    "lsp-qml",
    "lsp-r",
    "lsp-racket",
    "lsp-remark",
    "lsp-rf",
    "lsp-roc",
    "lsp-ron",
    "lsp-roslyn",
    "lsp-rpm-spec",
    "lsp-rubocop",
    "lsp-ruby-lsp",
    "lsp-ruby-syntax-tree",
    "lsp-ruff",
    "lsp-rust",
    "lsp-semantic-tokens",
    "lsp-semgrep",
    "lsp-sml",
    "lsp-solargraph",
    "lsp-solidity",
    "lsp-sorbet",
    "lsp-sql",
    "lsp-sqls",
    "lsp-steep",
    "lsp-svelte",
    "lsp-terraform",
    "lsp-tex",
    "lsp-tilt",
    "lsp-toml-tombi",
    "lsp-toml",
    "lsp-trunk",
    "lsp-ts-query",
    "lsp-ttcn3",
    "lsp-typeprof",
    "lsp-typespec",
    "lsp-typos",
    "lsp-typst",
    "lsp-v",
    "lsp-vala",
    "lsp-verilog",
    "lsp-vetur",
    "lsp-vhdl",
    "lsp-vimscript",
    "lsp-volar",
    "lsp-wat",
    "lsp-wgsl",
    "lsp-xml",
    "lsp-yaml",
    "lsp-yang",
    "lsp-zig",
    "lsp",
)

EXPECTED_COMPILED = tuple(
    sorted(
        (
            "dash-2.20.0/dash.elc",
            "f-0.21.0/f.elc",
            "ht-2.3/ht.elc",
            "lv-0.15.0/lv.elc",
            "markdown-mode-2.8/markdown-mode.elc",
            "s-1.13.1/s.elc",
            "spinner-1.7.4/spinner.elc",
        )
        + tuple("lsp-mode-10.0.0/%s.elc" % module for module in _LSP_MODULES)
    )
)

LSP_MODE_TTY_SCENARIOS = (
    "lsp-mode-connect-diagnostics-completion-hover",
    "lsp-mode-xref-rename-edits",
    "lsp-mode-reconnect-shutdown",
    "lsp-mode-ui-buffers",
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
            package-check-signature 'allow-unsigned)
      (require 'cl-lib)
      (require 'package)
      (defun lsp-mode-gate-emit (key value)
        (princ (format "LSP_MODE_GATE\t%%s\t%%s\n" key value)))
      (defun lsp-mode-gate-installed-descs ()
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
      (defun lsp-mode-gate-compiled-record ()
        (let (compiled)
          (dolist (desc (lsp-mode-gate-installed-descs))
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
  (condition-case lsp-mode-gate-error
      (progn
%s)
    (error
     (princ (format "LSP_MODE_GATE_ERROR\\t%%S\\n" lsp-mode-gate-error))
     (kill-emacs 1))))
""" % (common_lisp(root, archive), body)


def install_lisp(root: Path, archive: Path) -> str:
    return wrapped_lisp(
        root,
        archive,
        r"""
        (package-initialize)
        (package-refresh-contents)
        (let* ((desc (car (cdr (assq 'lsp-mode package-archive-contents))))
               (transaction
                (package-compute-transaction
                 (list desc) (package-desc-reqs desc))))
          (unless (and desc
                       (equal (package-desc-version desc) '(10 0 0))
                       (equal (package-desc-archive desc) "pinned"))
            (error "Pinned lsp-mode descriptor was not selected: %S" desc))
          (lsp-mode-gate-emit
           "transaction"
           (mapconcat #'package-desc-full-name
                      (sort (copy-sequence transaction)
                            (lambda (left right)
                              (string< (package-desc-full-name left)
                                       (package-desc-full-name right))))
                      ","))
          (package-install desc))
        (package-initialize)
        (lsp-mode-gate-emit
         "installed"
         (mapconcat #'package-desc-full-name
                    (lsp-mode-gate-installed-descs) ","))
        (lsp-mode-gate-emit "compiled" (lsp-mode-gate-compiled-record))
        (let* ((installed (car (cdr (assq 'lsp-mode package-alist))))
               (autoload-file
                (and installed
                     (expand-file-name "lsp-mode-autoloads.el"
                                       (package-desc-dir installed)))))
          (lsp-mode-gate-emit
           "autoload-file"
           (if (and autoload-file (file-exists-p autoload-file))
               "true" "false")))
        (lsp-mode-gate-emit
         "callable-before-restart"
         ;; Byte compilation can load lsp-mode in the install process.  The
         ;; separate restart phase is what proves generated-autoload use.
         (if (fboundp 'lsp) "true" "false"))
        """,
    )


def restart_lisp(root: Path, archive: Path) -> str:
    return wrapped_lisp(
        root,
        archive,
        r"""
        (package-initialize)
        (lsp-mode-gate-emit
         "autoload-after-restart"
         (if (autoloadp (symbol-function 'lsp)) "true" "false"))
        (require 'lsp-mode)
        (lsp-mode-gate-emit "version" (lsp-package-version))
        (dolist (library '(dash f ht lsp-mode lv markdown-mode s spinner))
          (let ((path (locate-library (symbol-name library))))
            (unless (and path (file-in-directory-p path package-user-dir))
              (error "%S resolved outside package-user-dir: %S" library path))
            (lsp-mode-gate-emit
             (format "origin.%s" library)
             (file-name-nondirectory
              (directory-file-name (file-name-directory path))))))
        """,
    )


def parse_records(output: str) -> Dict[str, str]:
    records: Dict[str, str] = {}
    for line in output.splitlines():
        if not line.startswith(MARKER):
            continue
        parts = line.split("\t", 2)
        if len(parts) != 3 or parts[1] in records:
            raise ValueError("malformed or duplicate lsp-mode gate record: %r" % line)
        records[parts[1]] = parts[2]
    return records


def run_phase(editor: str, binary: Path, phase: str, script: str, root: Path) -> PhaseResult:
    script_path = root / (phase + ".el")
    script_path.write_text(script, encoding="utf-8")
    # Same hermeticity as the Magit and Flycheck gates: the invoking
    # user's real $HOME must not leak configuration into either editor.
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
    required = {
        "transaction",
        "installed",
        "compiled",
        "autoload-file",
        "callable-before-restart",
    }
    if set(result.records) != required:
        raise RuntimeError(
            "%s install emitted keys %r, expected %r"
            % (result.editor, sorted(result.records), sorted(required))
        )
    for key in ("transaction", "installed"):
        actual = split_csv(result.records[key])
        if actual != EXPECTED_INSTALLED:
            raise RuntimeError(
                "%s %s closure %r, expected %r"
                % (result.editor, key, actual, EXPECTED_INSTALLED)
            )
    compiled = split_csv(result.records["compiled"])
    if compiled != EXPECTED_COMPILED:
        raise RuntimeError(
            "%s compiled inventory differs\nactual: %r\nexpected: %r"
            "\nphase stdout:\n%s\nphase stderr:\n%s"
            % (
                result.editor,
                compiled,
                EXPECTED_COMPILED,
                result.stdout,
                result.stderr,
            )
        )
    if result.records["autoload-file"] != "true":
        raise RuntimeError("%s did not generate lsp-mode-autoloads.el" % result.editor)
    if result.records["callable-before-restart"] != "true":
        raise RuntimeError("%s did not make lsp callable after install" % result.editor)


def validate_restart(result: PhaseResult) -> None:
    require_success(result)
    expected = {
        "autoload-after-restart": "true",
        "version": "10.0.0",
        "origin.dash": "dash-2.20.0",
        "origin.f": "f-0.21.0",
        "origin.ht": "ht-2.3",
        "origin.lsp-mode": "lsp-mode-10.0.0",
        "origin.lv": "lv-0.15.0",
        "origin.markdown-mode": "markdown-mode-2.8",
        "origin.s": "s-1.13.1",
        "origin.spinner": "spinner-1.7.4",
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
            "EMAXX_TTYDIFF_LSP_MODE_GNU_ROOT": str(gnu_root),
            "EMAXX_TTYDIFF_LSP_MODE_EMAXX_ROOT": str(emaxx_root),
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
        raise RuntimeError("lsp-mode TTY gate failed with exit %d" % completed.returncode)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("emaxx_binary", type=Path)
    parser.add_argument("gnu_binary", type=Path)
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=Path("target/lsp-mode-package-gate/artifacts"),
        help="verified release-tarball cache",
    )
    parser.add_argument("--offline", action="store_true", help="forbid downloads")
    parser.add_argument(
        "--tty",
        action="store_true",
        help="also run the strict interactive lsp-mode TTY scenarios",
    )
    parser.add_argument(
        "--gnu-lisp-dir",
        type=Path,
        help="GNU Lisp source tree required by ttydiff",
    )
    parser.add_argument(
        "--tty-scenario",
        action="append",
        choices=LSP_MODE_TTY_SCENARIOS,
        help="run only this lsp-mode TTY scenario (repeatable)",
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
        with tempfile.TemporaryDirectory(prefix="emaxx-lsp-mode-package-gate-") as temp:
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
                    "PASS: %s clean install, %d byte-compiled payloads, restart"
                    % (name, len(EXPECTED_COMPILED))
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
                    args.tty_scenario or LSP_MODE_TTY_SCENARIOS,
                )
                print("PASS: strict lsp-mode TTY scenarios match")
    except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
        print("ERROR: %s" % error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
