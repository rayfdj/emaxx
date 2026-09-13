#!/usr/bin/env python3
"""Build GNU's unchanged module fixture and require its tests to succeed.

One library, built against the configured oracle's public header, is loaded by
both editors. Results and input hashes are retained outside the frozen corpus.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys

from openpgp_gate import run


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def gate_succeeded(results, platform):
    allowed_skips = {"module-darwin-secondary-suffix"} if platform != "darwin" else set()
    if set(results) != {"gnu", "emaxx"}:
        return False
    outcomes = []
    for result in results.values():
        report = result["report"]
        if result["exit_code"] != 0 or not report or report["file_status"] != "loaded":
            return False
        tests = {test["name"]: test["status"] for test in report["results"]}
        if len(report["results"]) != 38 or len(tests) != 38 or any(
                status != "passed" and not (name in allowed_skips and status == "skipped")
                for name, status in tests.items()):
            return False
        outcomes.append(tests)
    return outcomes[0] == outcomes[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path("../emacs"))
    parser.add_argument("--oracle", type=Path, default=Path("../emacs/src/emacs"))
    parser.add_argument("--subject", type=Path, default=Path("target/gate/emaxx"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cc", help="C compiler command (default: configured GNU test compiler)")
    parser.add_argument("--timeout-seconds", type=float, default=300,
                        help="whole-file budget, including assertion subprocess startup (default: 300)")
    args = parser.parse_args()
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")
    if sys.platform not in ("darwin", "linux"):
        parser.error("this gate currently supports macOS and Linux")
    source = args.source.resolve()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    module_dir = root / "test/src/emacs-module-resources"
    module_dir.mkdir(parents=True)
    (root / "src").mkdir()
    inputs = ["test/src/emacs-module-tests.el", "test/src/emacs-module-resources/mod-test.c",
              "src/emacs-module.h", "src/config.h", "lib/mini-gmp.c", "lib/mini-gmp.h"]
    hashes = {name: digest(source / name) for name in inputs}
    (root / "input-hashes.json").write_text(json.dumps(hashes, indent=2) + "\n")
    (root / "oracle-revision.txt").write_bytes(subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"]))
    makefile = (source / "test/Makefile").read_text()
    cc = args.cc or re.search(r"^CC = (.+)$", makefile, re.M)[1]
    # GNU's fixture supports its bundled mini-GMP on both platforms. Compile
    # it from source with PIC, just as test/Makefile does for mini-GMP builds.
    (module_dir / "gmp.h").write_text(
        '#include ' + json.dumps(str(source / "lib/mini-gmp.h")) + '\n')
    suffix = ".dylib" if sys.platform == "darwin" else ".so"
    module = module_dir / ("mod-test" + suffix)
    command = shlex.split(cc) + ["-shared", "-fPIC", "-pthread", "-I" + str(module_dir),
                                "-I" + str(source / "src"), "-o", str(module),
                                str(source / inputs[1]), str(source / "lib/mini-gmp.c")]
    (root / "compile-command.json").write_text(json.dumps(command, indent=2) + "\n")
    with (root / "compile.log").open("wb") as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
    selector = "(not (or (tag :expensive-test) (tag :unstable)))"
    helper = Path(__file__).resolve().parents[1] / "compat/emacs_compat_runner.el"
    # ERT's listener runs outside test bodies. Keep the shared listener's
    # cleanup and record which test is active if a subprocess times out.
    progress = '''(advice-add 'emaxx-compat--test-listener :after
      (lambda (event &rest args)
        (when (memq event '(test-started test-ended))
          (message "module-gate %.3f %s %s" (float-time) event
                   (ert-test-name (nth 1 args))))))'''
    results = {}
    for editor, binary in [("gnu", args.oracle.resolve()), ("emaxx", args.subject.resolve())]:
        work = root / editor
        work.mkdir()
        env = os.environ.copy()
        env.update(EMAXX_BATCH_RESULT_FILE=str(work / "results.json"),
                   EMAXX_COMPAT_RUNNER=editor, EMAXX_COMPAT_RELATIVE_FILE=inputs[0],
                   EMAXX_DUMP_SOURCE_DIRECTORY=str(source),
                   EMACS_TEST_DIRECTORY=str(source / "test"))
        # The test derives the module path from invocation-directory. Point
        # that fixture lookup at our build tree, then give its assertion
        # subprocesses the real binary path, preserving image relocation.
        load = (f'(let ((invocation-directory {json.dumps(str(root / "src") + "/")})) '
                f'(load {json.dumps(str(source / inputs[0]))} nil t))')
        timed_out = False
        try:
            code = run([str(binary), "-Q", "--batch", "-l", str(helper), "--eval", load,
                        "--eval", f'(setq mod-test-emacs {json.dumps(str(binary))})',
                        "--eval", progress, "--eval", f"(emaxx-compat-run '{selector})"], env,
                       work / "stdout.log", work / "stderr.log", timeout=args.timeout_seconds)
        except subprocess.TimeoutExpired:
            code, timed_out = 124, True
        report = work / "results.json"
        results[editor] = {"exit_code": code, "binary_sha256": digest(binary),
                           "module_sha256": digest(module),
                           "timed_out": timed_out, "timeout_seconds": args.timeout_seconds,
                           "report": json.loads(report.read_text()) if report.exists() else None}
        print(editor, results[editor]["report"]["summary"] if report.exists() else code,
              flush=True)
    assert hashes == {name: digest(source / name) for name in inputs}, "fixture changed"
    (root / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
    return 0 if gate_succeeded(results, sys.platform) else 1


if __name__ == "__main__":
    raise SystemExit(main())
