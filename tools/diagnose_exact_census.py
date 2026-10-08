#!/usr/bin/env python3
"""Diagnose source204's census failure with its retained executable and image.

This is a bounded diagnosis of one inspected x86-64 executable, not a passing
gate or a repair. The original assertions, inventory and timeout stay intact.
"""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile

from diagnose_rust_gate import execute
from diagnose_weak_roots import compiled_inputs
import serial_grouped_gate


REVISION = "d186d40bf014ca53d0a2652b4e2f2d921c5cb009"
BINARY_SHA = "6ea03c1ac6c36eea7cdbdbd8a32369954b5a079b266d4950dd4e1a942aa663bc"
IMAGE_SHA = "85ebb975393d794a1b8fdb1a27ba835161ab7b99484f4cae70c217298c52827e"
FILTER = "census_matches_gnu_word_layout"
TESTS = [
    "lisp::primitives::tests::native_pseudovector_census_matches_gnu_word_layout",
    "lisp::primitives::tests::native_vector_and_closure_census_matches_gnu_word_layout",
]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_retained(directory):
    original = json.loads((directory / "summary.json").read_text())
    retained = directory / "retained-inputs"
    manifest = json.loads((retained / "manifest.json").read_text())
    if (original["scope"] != "full" or original["status"] != "failed"
            or original["git"] != {"head": REVISION, "dirty": False}):
        raise ValueError("expected source204's original failed full gate")
    if (manifest["status"] != "retained" or manifest["gate_status"] != "failed"
            or manifest["gate_summary_sha256"] != sha(directory / "summary.json")):
        raise ValueError("retained manifest does not identify the failed summary")
    names = serial_grouped_gate.gate.parse_inventory((directory / "inventory.txt").read_text())
    digest = hashlib.sha256(("\n".join(names) + "\n").encode()).hexdigest()
    if digest != original["inventory"]["sha256"] or [n for n in names if FILTER in n] != TESTS:
        raise ValueError("original test inventory differs")
    artifacts = {}
    for entry in manifest["files"]:
        relative = Path(entry["artifact"])
        if relative.is_absolute() or ".." in relative.parts or str(relative) in artifacts:
            raise ValueError("invalid retained artifact path")
        artifact = retained / relative
        if artifact.is_symlink() or artifact.stat().st_size != entry["bytes"] or sha(artifact) != entry["sha256"]:
            raise ValueError("retained artifact hash or size differs")
        artifacts[str(relative)] = entry
    if set(artifacts) != {"libtest", "fixture-images/loadup-6ea03c1a-988ea42f.pdmp"}:
        raise ValueError("unexpected retained inputs")
    binary = artifacts["libtest"]
    image = artifacts["fixture-images/loadup-6ea03c1a-988ea42f.pdmp"]
    if (binary["sha256"] != BINARY_SHA or image["sha256"] != IMAGE_SHA
            or original["test_binary"] != {"path": binary["original"], "sha256": BINARY_SHA}):
        raise ValueError("artifacts differ from the inspected source204 ABI")
    return original, names, binary, image


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--artifact-run", default="36764911748")
    parser.add_argument("--replay-group", action="store_true",
                        help="also repeat the already reproduced complete primitives group")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    record = {
        "status": "preparing", "artifact_run": args.artifact_run,
        "source_revision": REVISION,
        "qualification": "Exact executable/image and source inputs; original assertions. Remaining inherited CI environment was not fully recorded by the original gate. Debugger timing/address changes are diagnostic only. No repair or performance claim.",
    }

    def save():
        (output / "summary.json").write_text(json.dumps(record, indent=2) + "\n")

    save()
    try:
        subprocess.run(["gh", "run", "download", args.artifact_run, "--repo", "rayfdj/emaxx",
                        "--dir", str(output / "original")], check=True)
        summaries = list((output / "original").rglob("rust/summary.json"))
        if len(summaries) != 1:
            raise ValueError("expected one failed full-gate summary")
        directory = summaries[0].parent
        original, inventory, binary_entry, image_entry = read_retained(directory)
        retained = directory / "retained-inputs"
        environment = serial_grouped_gate.gate.gate_environment(True)
        environment.pop("GH_TOKEN", None)
        environment.update(original["environment"])
        binary = Path(binary_entry["original"])
        image = root / image_entry["original"]
        for destination in [binary, image]:
            if not destination.resolve().is_relative_to(root / "target"):
                raise ValueError("original execution path leaves the disposable target")
            destination.parent.mkdir(parents=True, exist_ok=True)
        if image.parent != Path(environment["EMAXX_FIXTURE_IMAGE_DIR"]):
            raise ValueError("fixture directory differs from the original path")
        shutil.copy2(retained / "libtest", binary)
        binary.chmod(0o755)
        shutil.copy2(retained / image_entry["artifact"], image)
        paths = ["src", "tests", "Cargo.toml", "Cargo.lock", "build.rs", ".cargo"]
        subprocess.run(["git", "restore", "--source", REVISION, "--staged", "--worktree", "--", *paths], cwd=root, check=True)
        inputs = compiled_inputs(root)
        archived = subprocess.check_output(["git", "archive", REVISION, "--", *inputs], cwd=root)
        with tarfile.open(fileobj=io.BytesIO(archived)) as archive:
            expected = {name: hashlib.sha256(archive.extractfile(name).read()).hexdigest() for name in inputs}
        if inputs != expected:
            raise ValueError("compiled inputs differ from the original source")
        (output / "source-before.json").write_text(json.dumps(inputs, indent=2) + "\n")
        record.update(source_inputs=len(inputs), binary_sha256=sha(binary), image_sha256=sha(image),
                      recorded_environment=original["environment"], fixture_directory=str(image.parent))

        def run(label, command, names):
            result = execute(command, environment, output / (label + ".log"))
            raw = (output / (label + ".log")).read_text(errors="replace")
            result.update(
                log=label + ".log", binary_unchanged=sha(binary) == BINARY_SHA,
                image_unchanged=sha(image) == IMAGE_SHA,
                source_unchanged=compiled_inputs(root) == inputs,
                expected_names=names,
                inventory_matches=sorted(result["executed_tests"]) == sorted(names),
                summaries=re.findall(r"^test result: (?:ok|FAILED)\..*$", raw, re.M),
                callback_errors=re.findall(r"^(?:Python Exception|Traceback \(most recent call last\):).*$", raw, re.M),
            )
            record[label] = result
            save()
            if not all(result[key] for key in ["binary_unchanged", "image_unchanged", "source_unchanged", "inventory_matches"]):
                raise ValueError("diagnostic inputs or executed inventory changed")
            if result["timed_out"] or result["callback_errors"]:
                raise ValueError("diagnostic timed out or observer raised an exception")
            return result

        selected = run("selected", [str(binary), FILTER, "--test-threads", "1"], TESTS)
        group_names = [name for name in inventory if name.startswith("lisp::primitives::tests::")]
        if args.replay_group or not selected["exit_code"]:
            run("original-group", [str(binary), "lisp::primitives::tests::", "--test-threads", "1"], group_names)
        else:
            record["original_group_not_rerun"] = "The first retained-artifact diagnosis reproduced this whole group; selected controls reproduce again, so this observer follow-up needs no repeated unrelated tests."
        # If the isolated controls reproduce, avoid unrelated debugger work.
        # Otherwise retain the entire original group and its preceding state.
        selector, names = (FILTER, TESTS) if selected["exit_code"] else ("lisp::primitives::tests::", group_names)
        command = ["gdb", "--batch", "--return-child-result", "-ex", "set startup-with-shell off"]
        for name in ["LINES", "COLUMNS"]:
            if name not in environment:
                command += ["-ex", "unset environment " + name]
        command += ["-x", str(root / "tools/diagnostics/exact-census-watch.py"),
                    "--args", str(binary), selector, "--test-threads", "1"]
        run("debugger", command, names)
        raw = (output / "debugger.log").read_text(errors="replace")
        events = [json.loads(match) for match in re.findall(r'CENSUS_TRACE (\{[^\n]*\})', raw)]
        (output / "trace-events.json").write_text(json.dumps(events, indent=2) + "\n")
        for name in TESTS:
            short = name.rsplit("::", 1)[1]
            for event in ["explicit collection", "roots", "host stack", "swept vectors"]:
                observed = {item["collection"] for item in events if item["test"] == short and item["event"] == event}
                if not {1, 2, 3, 4} <= observed:
                    raise ValueError("observer did not cover the first four collections: " + short + "/" + event)
        record["trace_events"] = len(events)
        record["status"] = "diagnosis recorded; original failures remain failures"
        return next((record[name]["exit_code"] for name in ["selected", "original-group", "debugger"]
                     if name in record and record[name]["exit_code"]), 0)
    except BaseException as error:
        record.update(status="diagnostic error", error=repr(error))
        raise
    finally:
        save()


if __name__ == "__main__":
    raise SystemExit(main())
