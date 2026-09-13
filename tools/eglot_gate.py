#!/usr/bin/env python3
"""Require GNU's unchanged Eglot tests to succeed with shared prerequisites.

Rust 1.75 supplies the analyzer protocol expected by this pinned GNU fixture.
The six previously failing tests must pass; missing optional servers may skip.
"""
import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile

from emacs_module_gate import digest
from openpgp_gate import run


OPTIONAL = {"eglot-test-eclipse-connect", "eglot-test-javascript-basic",
            "eglot-test-json-basic", "eglot-test-path-to-uri-windows",
            "eglot-test-project-wide-diagnostics-typescript",
            "eglot-test-snippet-completions", "eglot-test-snippet-completions-with-company"}


def gate_succeeded(results):
    if set(results) != {"gnu", "emaxx"}:
        return False
    outcomes = []
    for result in results.values():
        report = result["report"]
        if result["exit_code"] != 0 or not report or report["file_status"] != "loaded":
            return False
        tests = {test["name"]: test["status"] for test in report["results"]}
        if len(report["results"]) != 52 or len(tests) != 52 or any(
                status != "passed" and not (name in OPTIONAL and status == "skipped")
                for name, status in tests.items()):
            return False
        outcomes.append(tests)
    return outcomes[0] == outcomes[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path("../emacs"))
    parser.add_argument("--oracle", type=Path, default=Path("../emacs/src/emacs"))
    parser.add_argument("--subject", type=Path, default=Path("target/gate/emaxx"))
    parser.add_argument("--rust-bin", type=Path,
                        help="Rust 1.75 toolchain bin directory, with rust-analyzer and rust-src installed")
    parser.add_argument("--clangd", type=Path, default=shutil.which("clangd"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = args.source.resolve()
    rust_bin = args.rust_bin or Path(subprocess.check_output(
        ["rustup", "which", "--toolchain", "1.75.0", "rustc"], text=True).strip()).parent
    rust_bin = rust_bin.resolve()
    if not args.clangd or not args.clangd.is_file():
        parser.error("a real clangd executable is required")
    if not (rust_bin.parent / "lib/rustlib/src/rust/library").is_dir():
        parser.error("install rust-src for the selected Rust toolchain")
    rustc_version = subprocess.check_output([str(rust_bin / "rustc"), "--version"], text=True)
    if not rustc_version.startswith("rustc 1.75.0 "):
        parser.error("this pinned GNU fixture requires the Rust 1.75 toolchain")
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    # Eglot discovers projects through parent directories. Test projects
    # must live outside the Emaxx checkout, even if reports live in target/.
    temporary = Path(tempfile.mkdtemp(prefix="emaxx-eglot-")).resolve()
    (root / "temporary-directory.txt").write_text(str(temporary) + "\n")
    (root / "bin").mkdir()
    for name in ("cargo", "rustc", "rustdoc", "rust-analyzer"):
        binary = rust_bin / name
        if not binary.is_file():
            parser.error(f"missing toolchain component: {binary}")
        (root / "bin" / name).symlink_to(binary)
    # Eglot's column test asserts UTF-16 positions. Configure that encoding
    # on the actual server, identically for both editors.
    clangd = root / "bin/clangd"
    clangd.write_text("#!/bin/sh\nexec " + shlex.quote(str(args.clangd.resolve()))
                      + ' --offset-encoding=utf-16 "$@"\n')
    clangd.chmod(0o755)
    env = os.environ.copy()
    env.update(PATH=str(root / "bin") + os.pathsep + env["PATH"],
               TMPDIR=str(temporary), LANG="C", LC_ALL="C")
    versions = {name: subprocess.check_output([str(root / "bin" / name), "--version"],
                                              env=env, text=True).strip()
                for name in ("cargo", "rustc", "rust-analyzer", "clangd")}
    (root / "tool-versions.json").write_text(json.dumps(versions, indent=2) + "\n")
    fixture = source / "test/lisp/progmodes/eglot-tests.el"
    hashes = {str(path.relative_to(source)): digest(path) for path in
              (fixture, source / "lisp/progmodes/eglot.el", source / "lisp/jsonrpc.el")}
    (root / "input-hashes.json").write_text(json.dumps(hashes, indent=2) + "\n")
    helper = Path(__file__).resolve().parents[1] / "compat/emacs_compat_runner.el"
    results = {}
    for editor, binary in [("gnu", args.oracle.resolve()), ("emaxx", args.subject.resolve())]:
        work = root / editor
        work.mkdir()
        env.update(EMAXX_BATCH_RESULT_FILE=str(work / "results.json"),
                   EMAXX_COMPAT_RUNNER=editor,
                   EMAXX_COMPAT_RELATIVE_FILE=str(fixture.relative_to(source)),
                   EMACS_TEST_DIRECTORY=str(source / "test"))
        code = run([str(binary), "-Q", "--batch", "-l", str(helper), "-l", str(fixture),
                    "--eval", "(emaxx-compat-run '(not (or (tag :expensive-test) (tag :unstable))))"],
                   env, work / "stdout.log", work / "stderr.log", timeout=300)
        report = work / "results.json"
        results[editor] = {"exit_code": code, "binary_sha256": digest(binary),
                           "report": json.loads(report.read_text()) if report.exists() else None}
        print(editor, results[editor]["report"]["summary"] if report.exists() else code,
              flush=True)
    assert hashes == {name: digest(source / name) for name in hashes}, "fixture changed"
    (root / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
    succeeded = gate_succeeded(results)
    if succeeded:
        shutil.rmtree(temporary)
    return 0 if succeeded else 1


if __name__ == "__main__":
    raise SystemExit(main())
