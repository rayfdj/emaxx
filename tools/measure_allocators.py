#!/usr/bin/env python3
"""Compare real allocation workloads; diagnostic measurements, not frozen results."""

import argparse
import hashlib
import json
import math
import os
import platform
from pathlib import Path
import re
import statistics
import subprocess
import tempfile
import time


WORKLOAD = r''';; -*- lexical-binding: t; -*-
(require 'json)
(defvar allocation-measurements nil)
(dolist (entry
         '((conses 10000000 . (lambda ()
                              (let ((sum 0))
                                (dotimes (i 200 sum)
                                  (let ((items (make-list 50000 i)))
                                    (setq sum (+ sum (length items))))))))
           (vectors 20480000 . (lambda ()
                               (let ((sum 0))
                                 (dotimes (i 5000 sum)
                                   (setq sum (+ sum (length (make-vector 4096 i))))))))
           (strings 81920000 . (lambda ()
                               (let ((sum 0))
                                 (dotimes (_ 20000 sum)
                                   (setq sum (+ sum (length (make-string 4096 ?a))))))))
           (evaluation 499999500000 . (lambda ()
                                     (let ((sum 0))
                                       (dotimes (i 1000000 sum)
                                         (setq sum (+ sum i))))))))
  (let* ((wall (float-time))
         (cpu (float-time (current-cpu-time)))
         (value (funcall (cddr entry)))
         (cpu-seconds (- (float-time (current-cpu-time)) cpu))
         (wall-seconds (- (float-time) wall)))
    (unless (= value (cadr entry))
      (error "Allocation workload %s returned %S, expected %S"
             (car entry) value (cadr entry)))
    (push (list (cons 'case (symbol-name (car entry)))
                (cons 'result value)
                (cons 'wall_seconds wall-seconds)
                (cons 'cpu_seconds cpu-seconds))
          allocation-measurements)))
(with-temp-file allocation-report
  (insert (json-encode (vconcat (nreverse allocation-measurements)))))
'''


def digest(path):
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for data in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(data)
    return hasher.hexdigest()


def library_digest(source):
    hasher = hashlib.sha256()
    for name in ("lisp", "native-lisp"):
        for path in sorted((source / name).rglob("*")):
            if path.is_file():
                hasher.update(str(path.relative_to(source)).encode() + b"\0")
                hasher.update(digest(path).encode() + b"\0")
    return hasher.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", action="append", required=True, help="LABEL=/absolute/binary")
    parser.add_argument("--oracle-source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=4, help="alternate execution order each round")
    args = parser.parse_args()
    if args.rounds < 1:
        parser.error("rounds must be positive")
    binaries = {}
    for value in args.binary:
        label, separator, path = value.partition("=")
        if not separator or not re.fullmatch(r"[A-Za-z0-9_-]+", label) or label in binaries:
            parser.error("each binary needs a unique simple LABEL=PATH")
        binaries[label] = Path(path).resolve(strict=True)
    source = args.oracle_source.resolve(strict=True)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    workload = output / "allocation-workload.el"
    workload.write_text(WORKLOAD)
    inputs = [workload]
    for binary in binaries.values():
        inputs.extend([binary, binary.with_suffix(".pdmp")])
    identities = {str(path): digest(path) for path in inputs}
    evidence = {
        "kind": "diagnostic_only",
        "host": platform.uname()._asdict(),
        "source_libraries_sha256": library_digest(source),
        "source_revision": subprocess.check_output(
            ["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip(),
        "source": str(source),
        "input_sha256": identities,
        "runs": [],
    }
    for round_index in range(args.rounds):
        order = list(binaries.items())
        if round_index % 2:
            order.reverse()
        for label, binary in order:
            root = output / (str(round_index + 1) + "-" + label)
            root.mkdir()
            report = root / "measurements.json"
            command = [str(binary), "--quick", "--batch", "--eval",
                       "(setq allocation-report " + json.dumps(str(report), ensure_ascii=False) + ")",
                       "--load", str(workload)]
            with tempfile.TemporaryDirectory(prefix="emaxx-allocator-") as temporary:
                environment = {
                    "HOME": temporary, "TMPDIR": temporary, "TMP": temporary, "TEMP": temporary,
                    "PATH": os.environ.get("PATH", os.defpath), "LANG": "C",
                    "EMAXX_DUMP_SOURCE_DIRECTORY": str(source),
                    "EMACS_TEST_DIRECTORY": str(source / "test"),
                }
                start = time.monotonic()
                with (root / "stdout").open("wb") as stdout, (root / "stderr").open("wb") as stderr:
                    process = subprocess.run(command, cwd=source / "test", env=environment,
                                             stdin=subprocess.DEVNULL, stdout=stdout,
                                             stderr=stderr, timeout=180)
                run = {
                    "label": label, "round": round_index + 1, "command": command,
                    "environment_sha256": hashlib.sha256(json.dumps(environment, sort_keys=True).encode()).hexdigest(),
                    "exit_code": process.returncode, "process_seconds": time.monotonic() - start,
                }
            evidence["runs"].append(run)
            if process.returncode == 0:
                run["measurements"] = json.loads(report.read_text())
                run["report_sha256"] = digest(report)
            (output / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
            print(label, "round", round_index + 1, "exit", process.returncode, flush=True)
            if process.returncode:
                return 1
            rows = run["measurements"]
            if sorted(row["case"] for row in rows) != ["conses", "evaluation", "strings", "vectors"]:
                raise RuntimeError("Incomplete or duplicate allocation measurements")
            for row in rows:
                for metric in ("wall_seconds", "cpu_seconds"):
                    value = row[metric]
                    if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                        raise RuntimeError("Invalid timing measurement: " + metric)
    for path, expected in identities.items():
        if digest(Path(path)) != expected:
            raise RuntimeError("Measurement input changed: " + path)
    if library_digest(source) != evidence["source_libraries_sha256"]:
        raise RuntimeError("Source libraries changed during measurement")
    summary = {}
    for label in binaries:
        cases = {}
        for run in evidence["runs"]:
            if run["label"] == label:
                for item in run["measurements"]:
                    cases.setdefault(item["case"], []).append(item)
        summary[label] = {
            case: {metric + "_median": statistics.median(item[metric] for item in samples)
                   for metric in ("wall_seconds", "cpu_seconds")}
            for case, samples in cases.items()
        }
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))
    print("Diagnostic only: no compatibility outcomes are certified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
