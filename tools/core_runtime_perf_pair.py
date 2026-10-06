#!/usr/bin/env python3
"""Build two exact revisions, then run unchanged full-core diagnostic pilots.

All builds and native preparation finish before timing. This is a diagnostic,
not the calibrated nine-round acceptance run. Raw results are never repaired.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import sys
import time

import core_runtime_perf as perf
import retain_oracle_artifacts

ROOT = Path(__file__).resolve().parents[1]
MEASUREMENT_FILES = ("tools/core_runtime_perf.py", "compat/core_runtime_perf.json", "compat/core_runtime_perf.el")


def audit_pilot(directory, contract):
    """Recheck each actual process/report and the complete two-editor inventory."""
    read = lambda path: perf.parse_json(path.read_text())
    cases = {case["id"]: case for case in contract["cases"]}
    perf.validate_contract(contract)
    if read(directory / "contract.json") != contract:
        raise ValueError("pilot changed the locked contract")
    records = read(directory / "runs.json")
    expected = {(name, editor) for name in cases for editor in ("oracle", "emaxx")}
    seen = set()
    results = {}
    for record in records:
        key = (record["case_id"], record["runner"])
        if key not in expected or key in seen or record["round"] != 0:
            raise ValueError("unexpected or duplicate pilot outcome")
        seen.add(key)
        location = directory / "round-00" / key[0] / key[1]
        process = read(location / "process.json")
        report = read(location / "report.json")
        observations = perf.validate_report(report, process, cases[key[0]], contract,
                                            (location / "stdout.txt").read_text(), 0, 1)
        if (record["validation"] != "completed" or record["process"] != process
                or record["observations"] != observations or record["modes"] != report["modes"]
                or read(location / "validation.json") != record):
            raise ValueError("aggregate differs from an actual pilot result")
        results[key] = dict(observation=observations[0], modes=report["modes"], process=process)
    if seen != expected:
        raise ValueError("incomplete pilot inventory")
    provenance = read(directory / "provenance.json")
    final = read(directory / "final-inputs.json")
    if not final["unchanged"] or final["input_sha256"] != provenance["input_sha256"]:
        raise ValueError("pilot input identity changed")
    if any(perf.sha256(Path(name)) != digest for name, digest in final["input_sha256"].items()):
        raise ValueError("actual pilot input differs from its recorded identity")
    for name in cases:
        left, right = (results[name, editor] for editor in ("oracle", "emaxx"))
        if (left["observation"]["result"] != right["observation"]["result"]
                or left["modes"] != right["modes"]):
            raise ValueError("actual GNU and Emaxx result or mode differs: " + name)
    return {name: {editor: results[name, editor] for editor in ("oracle", "emaxx")} for name in cases}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--oracle-source", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.baseline):
        parser.error("--baseline must be an exact commit")
    if perf.git(ROOT, "cat-file", "-t", args.baseline) != "commit":
        parser.error("--baseline is not a commit")
    if perf.git(ROOT, "status", "--porcelain", "--untracked-files=no"):
        parser.error("candidate tracked source is not clean")
    oracle = args.oracle_source.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    candidate = perf.git(ROOT, "rev-parse", "HEAD")
    revisions = {"baseline": args.baseline, "candidate": candidate}
    work = ROOT / "target" / ("core-runtime-pair-" + candidate[:12])
    work.mkdir(exist_ok=False)
    environment = dict(os.environ, LANG="C", LC_ALL="C", CARGO_BUILD_JOBS="2",
                       EMAXX_GNU_SOURCE_DIRECTORY=str(oracle), EMAXX_DUMP_SOURCE_DIRECTORY=str(oracle),
                       EMACS_TEST_DIRECTORY=str(oracle / "test"))
    environment.pop("GH_TOKEN", None)
    state = dict(status="preparing", revisions=revisions, commands=[], pilots=[], goal_complete=False,
                 schedule=[["baseline", "candidate"], ["candidate", "baseline"], ["baseline", "candidate"]],
                 qualification="Three alternating full16 diagnostic pilots on one Linux runner. No concurrent build or profile stages. Every actual process/report, startup/body/total time, GC delta and RSS remains. This is not calibrated nine-round/every-case3% certification; missing real Emaxx allocation counters remain explicit.")
    def save():
        perf.write_json(output / "summary.json", state)
    def execute(label, command, cwd, env=None, timeout=3600):
        row = dict(stage=label, command=[str(v) for v in command], cwd=str(cwd), timeout_seconds=timeout)
        state["commands"].append(row)
        state["status"] = "running " + label
        save()
        start = time.monotonic()
        with (output / (label + ".log")).open("xb") as log:
            try:
                result = subprocess.run(row["command"], cwd=cwd, env=env or environment,
                                        stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, timeout=timeout)
                row.update(exit_code=result.returncode, timed_out=False)
            except subprocess.TimeoutExpired:
                row.update(exit_code=None, timed_out=True)
        row["elapsed_seconds"] = time.monotonic() - start
        save()
        if row["exit_code"] != 0 or row["timed_out"]:
            raise ValueError("stage failed: " + label)
    save()
    try:
        capture = retain_oracle_artifacts.capture(oracle, output / "oracle-inputs")
        lock = json.loads((ROOT / "compat/oracle.lock.linux.json").read_text())
        if (capture["source_commit"] != lock["emacs_repo_commit"]
                or capture["files"][0]["sha256"] != lock["emacs_binary_sha256"]):
            raise ValueError("GNU source/executable differs from the platform pin")
        inputs = {name: perf.sha256(ROOT / name) for name in MEASUREMENT_FILES}
        state["measurement_inputs"] = inputs
        state["helper_sha256"] = perf.sha256(Path(__file__))
        subjects = {}
        for label, revision in revisions.items():
            checkout = work / label / "emaxx"
            execute(label + "-worktree", ["git", "worktree", "add", "--detach", "--no-checkout", checkout, revision], ROOT)
            execute(label + "-sparse", ["git", "sparse-checkout", "set", "--cone", "src", "tests", "tools", "compat", ".cargo"], checkout)
            execute(label + "-checkout", ["git", "checkout", "--detach", revision], checkout)
            if any(perf.sha256(checkout / name) != digest for name, digest in inputs.items()):
                raise ValueError("measurement inputs differ between revisions")
            (checkout.parent / "emacs").symlink_to(oracle, target_is_directory=True)
            target = checkout / "target" / "pair-release"
            env = dict(environment, CARGO_TARGET_DIR=str(target))
            execute(label + "-build", ["cargo", "build", "--locked", "--release", "--all-features", "--bin", "emaxx", "--bin", "make-fingerprint", "--message-format=json"], checkout, env)
            raw = (output / (label + "-build.log")).read_text()
            messages = [json.loads(line) for line in raw.splitlines() if line.startswith("{")]
            if (re.search(r"^warning(?:\[.*\])?:", raw, re.M)
                    or any(m.get("message", {}).get("level") in ("warning", "error") for m in messages)):
                raise ValueError("compiler warning or error in " + label)
            artifacts = [m for m in messages if m.get("reason") == "compiler-artifact" and m["target"]["name"] == "emaxx" and m.get("executable")]
            if len(artifacts) != 1 or artifacts[0]["fresh"]:
                raise ValueError("editor was not freshly built")
            binary = target / "release" / "emaxx"
            execute(label + "-image", ["sh", "tools/build-image.sh", binary], checkout, dict(env, EMAXX_IMAGE_FORCE="1"))
            native = output / (label + "-native")
            common = [sys.executable, "-B", "tools/core_runtime_perf.py"]
            common_args = ["--source", oracle, "--oracle", oracle / "src/emacs", "--emaxx", binary, "--diagnostic"]
            execute(label + "-native", [*common, "prepare", *common_args, "--output", native], checkout)
            source_files = subprocess.check_output(["git", "ls-files", "-z", "--", "src", "tests", "tools", "compat", "Cargo.toml", "Cargo.lock", "build.rs", ".cargo"], cwd=checkout).decode().split("\0")
            source = {name: perf.sha256(checkout / name) for name in source_files if name}
            artifacts = {str(f): perf.sha256(f) for f in (binary, binary.with_suffix(".pdmp"))}
            retained = output / "artifacts" / label
            retained.mkdir(parents=True)
            copies = {}
            for name, digest in artifacts.items():
                copy = retained / Path(name).name
                shutil.copy2(name, copy)
                if perf.sha256(copy) != digest:
                    raise ValueError("retained executable/image differs from its original")
                copies[str(copy)] = digest
            subjects[label] = dict(checkout=checkout, command=common, arguments=common_args, native=native,
                                   source=source, artifacts=artifacts, retained=copies)
            perf.write_json(output / (label + "-inputs.json"), dict(revision=revision, source=source, artifacts=artifacts,
                            retained=copies,
                            rustc=subprocess.check_output(["rustc", "-Vv"], text=True),
                            cargo=subprocess.check_output(["cargo", "-V"], text=True)))
        # Any build/native preparation for either revision has now finished.
        process_list = subprocess.check_output(["ps", "-axo", "pid,ppid,args"], text=True)
        competitors = [line for line in process_list.splitlines() if any(token in line for token in (
            "cargo build ", "cargo test ", "/rustc ", "tools/serial_grouped_gate.py", "tools/ttydiff.py", "compat-harness frozen", "perf record "))]
        perf.write_json(output / "process-audit.json", dict(competing_task_processes=competitors,
                        process_listing_sha256=hashlib.sha256(process_list.encode()).hexdigest()))
        if competitors:
            raise ValueError("competing task process before timing")
        contract = perf.parse_json((ROOT / "compat/core_runtime_perf.json").read_text())
        samples = {label: [] for label in revisions}
        for repetition, order in enumerate(state["schedule"]):
            for label in order:
                subject = subjects[label]
                location = output / (label + "-pilot-" + str(repetition))
                execute(label + "-pilot-" + str(repetition), [*subject["command"], "pilot", *subject["arguments"], "--pilot-runner", "both", "--native-dir", subject["native"], "--output", location], subject["checkout"])
                samples[label].append(audit_pilot(location, contract))
                state["pilots"].append(dict(label=label, repetition=repetition, processes=32, raw_directory=location.name))
                save()
        for label, subject in subjects.items():
            if (perf.git(subject["checkout"], "rev-parse", "HEAD") != revisions[label]
                    or perf.git(subject["checkout"], "status", "--porcelain", "--untracked-files=no")
                    or any(perf.sha256(subject["checkout"] / n) != h for n, h in subject["source"].items())
                    or any(perf.sha256(Path(n)) != h for n, h in {**subject["artifacts"], **subject["retained"]}.items())):
                raise ValueError("subject inputs changed: " + label)
        results = []
        for case in contract["cases"]:
            name = case["id"]
            bodies = {label: [sample[name]["emaxx"]["observation"]["seconds"] for sample in group] for label, group in samples.items()}
            medians = {label: statistics.median(values) for label, values in bodies.items()}
            ratios = {label: statistics.median(sample[name]["emaxx"]["observation"]["seconds"] / sample[name]["oracle"]["observation"]["seconds"] for sample in group) for label, group in samples.items()}
            results.append(dict(case=name, body_samples=bodies, body_medians=medians, gnu_ratios=ratios,
                                candidate_over_baseline=medians["candidate"] / medians["baseline"],
                                samples={label: [sample[name] for sample in group] for label, group in samples.items()}))
        state.update(status="all 192 actual process results and modes verified", results=results,
                     raw_body_geometric_ratio=math.exp(statistics.mean(math.log(r["candidate_over_baseline"]) for r in results)))
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        state.update(status="diagnostic stopped", error=repr(error))
    finally:
        try:
            state["oracle_verification"] = retain_oracle_artifacts.verify(output / "oracle-inputs")
        except (OSError, ValueError, KeyError) as error:
            state.update(status="diagnostic stopped", oracle_verification_error=repr(error))
        save()
    return 0 if state["status"] == "all 192 actual process results and modes verified" else 1


if __name__ == "__main__":
    raise SystemExit(main())
