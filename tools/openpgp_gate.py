#!/usr/bin/env python3
"""Run the four original GNU OpenPGP tests with isolated, public test keys.

Skips and shared failures are failures of this gate. This focused diagnostic
never changes the corpus manifest, oracle lock, test bodies, or expectations.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys

TESTS = [f"mml-secure-en-decrypt-{n}" for n in range(1, 5)]
RELATIVE = Path("test/lisp/gnus")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(command, env, stdout, stderr, timeout=600):
    with stdout.open("wb") as out, stderr.open("wb") as err:
        child = subprocess.Popen(command, env=env, stdout=out, stderr=err,
                                 start_new_session=True)
        try:
            return child.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait()
            raise


def gate_succeeded(results):
    expected = {name: "passed" for name in TESTS}
    return set(results) == {"gnu", "emaxx"} and all(
        r["exit_code"] == 0 and r["outcomes"] == expected
        for r in results.values())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path("../emacs"))
    parser.add_argument("--oracle", type=Path, default=Path("../emacs/src/emacs"))
    parser.add_argument("--subject", type=Path, default=Path("target/gate/emaxx"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--prelaunch-agent", action="store_true",
                        help="Launch each fixture's own agent before running its tests")
    args = parser.parse_args()
    if not sys.platform.startswith("linux"):
        parser.error("the unchanged upstream tests skip Darwin; use a Linux host")
    programs = {name: shutil.which(name) for name in ["gpg", "gpgconf", "gpgsm"]}
    if not all(programs.values()):
        parser.error(f"GnuPG tools required: {programs}")
    source = args.source.resolve()
    root = args.output.resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    files = subprocess.check_output(
        ["git", "-C", str(source), "ls-files", "-z", str(RELATIVE / "mml-sec-tests.el"),
         str(RELATIVE / "mml-sec-resources")]).decode().strip("\0").split("\0")
    if not files or any(not (source / name).is_file() for name in files):
        raise RuntimeError("unchanged GNU test and tracked resources required")
    manifest = {name: digest(source / name) for name in files}
    (root / "fixture-hashes.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (root / "oracle-revision.txt").write_bytes(subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"]))
    results = {}
    for editor, binary in [("gnu", args.oracle.resolve()), ("emaxx", args.subject.resolve())]:
        work = root / editor
        for name in ["home", "tmp"]:
            (work / name).mkdir(mode=0o700, parents=True)
        for name in files:
            target = work / Path(name).relative_to(RELATIVE)
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            shutil.copyfile(source / name, target)
            assert digest(target) == manifest[name]
        keys = work / "mml-sec-resources"
        env = {"PATH": os.environ["PATH"], "HOME": str(work / "home"),
               "TMPDIR": str(work / "tmp"), "GNUPGHOME": str(keys),
               "LANG": "C", "LC_ALL": "C", "OPENPGP_OUTPUT": str(work)}
        agent = [programs["gpgconf"], "--homedir", str(keys)]
        driver = work / "capture.el"
        driver.write_text('''(require 'json)
(let ((directory (getenv "OPENPGP_OUTPUT")) results)
  (dolist (name '(mml-secure-en-decrypt-1 mml-secure-en-decrypt-2
                 mml-secure-en-decrypt-3 mml-secure-en-decrypt-4))
    (ert-run-tests-batch name)
    (let* ((result (ert-test-most-recent-result (ert-get-test name)))
           (status (cond ((ert-test-passed-p result) "passed")
                         ((ert-test-skipped-p result) "skipped")
                         (t "failed"))))
      (push (cons (symbol-name name) status) results)
      (when (get-buffer " *epg-test*")
        (with-current-buffer " *epg-test*"
          (write-region (point-min) (point-max)
                        (expand-file-name (format "%s.epg.log" name) directory)
                        nil 'silent)))))
  (with-temp-file (expand-file-name "results.json" directory)
    (insert (json-encode (nreverse results)))))
''')
        try:
            if args.prelaunch_agent:
                subprocess.run(agent + ["--launch", "gpg-agent"], env=env,
                               check=True, timeout=30)
            code = run([str(binary), "-Q", "--batch", "-l", str(work / "mml-sec-tests.el"),
                        "-l", str(driver)], env, work / "stdout.log", work / "stderr.log")
            report = work / "results.json"
            outcomes = json.loads(report.read_text()) if report.exists() else None
            results[editor] = {"exit_code": code, "binary_sha256": digest(binary),
                               "outcomes": outcomes}
            print(editor, json.dumps(results[editor]), flush=True)
        finally:
            # The home is a newly created copy of public fixture keys. Never
            # use the developer's keyring or kill agents outside this home.
            subprocess.run(agent + ["--kill", "gpg-agent"], env=env, timeout=30,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    (root / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
    return 0 if gate_succeeded(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
