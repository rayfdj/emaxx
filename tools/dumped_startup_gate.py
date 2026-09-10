#!/usr/bin/env python3
"""Run the original startup-sensitive tests from real, ordinary saved images.

Each editor builds its own image before any test is loaded. Fresh processes
discover that image beside a copy of the unchanged executable, including the
children started by the original tests. No original timeout is changed.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time


SUITES = {
    "test/lisp/erc/erc-tests.el": ["erc--find-mode", "erc--essential-hook-ordering"],
    "test/lisp/simple-tests.el": ["simple-tests-async-shell-command-30280"],
}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(command, environment, directory, label, timeout=600, working_directory=None):
    started = time.monotonic()
    with (directory / f"{label}.stdout").open("wb") as out:
        with (directory / f"{label}.stderr").open("wb") as err:
            child = subprocess.Popen(command, env=environment, stdout=out, stderr=err,
                                     cwd=working_directory or directory, start_new_session=True)
            try:
                status = child.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
                status = 124
    result = {"exit_code": status, "elapsed_seconds": time.monotonic() - started,
              "working_directory": str(working_directory or directory),
              "command": [str(arg) for arg in command]}
    (directory / f"{label}.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def gate_succeeded(results):
    expected = {name: "passed" for names in SUITES.values() for name in names}
    return set(results) == {"gnu", "emaxx"} and all(
        result.get("image_built") and result.get("startup_probe_passed")
        and result.get("test_exit_codes") == [0, 0]
        and result.get("outcomes") == expected
        for result in results.values())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path("../emacs"))
    parser.add_argument("--oracle", type=Path, default=Path("../emacs/src/emacs"))
    parser.add_argument("--subject", type=Path, default=Path("target/gate/emaxx"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = args.source.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, mode=0o700, exist_ok=False)
    paths = subprocess.check_output([
        "git", "-C", str(source), "ls-files", "-z", "test/lisp/erc",
        "test/lisp/simple-tests.el", "test/lisp/simple-resources",
    ]).decode().strip("\0").split("\0")
    assert set(SUITES).issubset(paths), "missing original test files"
    manifest = {path: digest(source / path) for path in paths}
    native_manifest = {str(path.relative_to(source)): digest(path)
                       for path in (source / "native-lisp").rglob("*.eln")}
    (output / "fixture-hashes.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (output / "native-library-hashes.json").write_text(
        json.dumps(native_manifest, indent=2) + "\n")
    (output / "oracle-revision.txt").write_bytes(subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"]))
    results = {}
    for editor, original in [("gnu", args.oracle.resolve()),
                             ("emaxx", args.subject.resolve())]:
        directory = output / editor
        for name in ["bin", "build-home", "build-tmp", "home", "tmp"]:
            (directory / name).mkdir(parents=True, mode=0o700)
        # Preserve the uninstalled GNU build layout, including init_lread's
        # discovery of lisp/ and init_cmdargs's discovery of etc/.
        for name in ["lisp", "etc", "lib-src", "native-lisp"]:
            (directory / name).symlink_to(source / name, target_is_directory=True)
        for name in paths:
            target = directory / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source / name, target)
        # GNU resolves .eln files relative to the executable. Use the same
        # built libraries as ordinary startup, with hashes checked below.
        # Copying those files changed GNU's first-use loading time on Darwin
        # enough to exceed ERC's unchanged deadline; shared installation
        # libraries passed with the same saved image and executable bytes.
        binary = directory / "bin" / ("emacs" if editor == "gnu" else "emaxx")
        shutil.copy2(original, binary)
        assert digest(binary) == digest(original)
        image = Path(str(binary) + ".pdmp")
        locale = "en_US.UTF-8" if sys.platform == "darwin" else "C.UTF-8"
        environment = {"PATH": os.environ["PATH"], "HOME": str(directory / "build-home"),
                       "TMPDIR": str(directory / "build-tmp"), "LANG": locale,
                       "LC_ALL": locale, "TERM": "xterm", "EMAXX_DUMP_PROBE": "builder"}
        # This uses the ordinary startup, with native compilation untouched.
        # The marker is general saved state, created before loading any tests.
        expression = ("(progn (defvar dumped-startup-gate-marker 'saved) "
                      f"(dump-emacs-portable {json.dumps(str(image))}))")
        build = run([str(original), "-Q", "--batch", "--eval", expression],
                    environment, directory, "build-image",
                    working_directory=directory / "build-home")
        result = {"binary_sha256": digest(binary), "build": build,
                  "image_built": build["exit_code"] == 0 and image.is_file()
                  and image.stat().st_size > 0}
        results[editor] = result
        if result["image_built"]:
            result["image_sha256"] = digest(image)
            environment.update(HOME=str(directory / "home"), TMPDIR=str(directory / "tmp"),
                               EMAXX_DUMP_PROBE="restored")
            probe = run([str(binary), "-Q", "--batch", "--eval",
                         "(prin1 (list (cdr (assq 'dumped-with-pdumper (pdumper-stats))) "
                         "dumped-startup-gate-marker command-line-processed "
                         "(getenv \"EMAXX_DUMP_PROBE\") "
                         "(expand-file-name invocation-name invocation-directory) "
                         "default-directory))"],
                        environment, directory, "startup-probe")
            result["startup_probe_passed"] = (
                probe["exit_code"] == 0 and (directory / "startup-probe.stdout").read_text()
                == f'(t saved t "restored" {json.dumps(str(binary))} '
                   f'{json.dumps(str(directory) + "/")})')
            result["outcomes"] = {}
            result["test_exit_codes"] = []
            if result["startup_probe_passed"]:
                for number, (file, names) in enumerate(SUITES.items()):
                    label = f"tests-{number}"
                    test = run([str(binary), "-Q", "--batch", "-l", str(directory / file),
                                "--eval", "(ert-run-tests-batch-and-exit '(member "
                                + " ".join(names) + "))"], environment, directory, label)
                    result["test_exit_codes"].append(test["exit_code"])
                    raw = (directory / f"{label}.stderr").read_text()
                    raw += (directory / f"{label}.stdout").read_text()
                    for status, name in re.findall(
                            r"(?:^|\n)\s*(passed|FAILED|skipped)\s+\d+/\d+\s+(\S+)", raw):
                        assert name not in result["outcomes"], "duplicate test outcome"
                        result["outcomes"][name] = status
            assert digest(image) == result["image_sha256"], "tests mutated the saved image"
        assert manifest == {path: digest(directory / path) for path in paths}
        assert native_manifest == {path: digest(directory / path) for path in native_manifest}
        (output / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
        print(editor, json.dumps(result), flush=True)
    assert manifest == {path: digest(source / path) for path in paths}
    assert native_manifest == {path: digest(source / path) for path in native_manifest}
    assert native_manifest == {str(path.relative_to(source)): digest(path)
                               for path in (source / "native-lisp").rglob("*.eln")}
    return 0 if gate_succeeded(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
