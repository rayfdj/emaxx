#!/usr/bin/env python3
"""Capture GNU inputs before a gate and verify that they stayed unchanged.

This does not run GNU, alter fixtures, or change any validation verdict. The
record identifies the configured oracle; individual test-use provenance remains
the responsibility of the gate's command and execution records.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def capture(source: Path, output: Path) -> dict:
    source = source.resolve()
    output.mkdir(parents=True, exist_ok=False)
    record = {
        "format_version": 1,
        "status": "capturing",
        "source": str(source),
        "files": [],
        "qualification": "Configured GNU inputs captured before validation; no test result is implied.",
    }
    try:
        record["source_commit"] = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=source, text=True,
        ).strip()
        record["source_tracked_changes"] = subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=no"], cwd=source, text=True,
        ).splitlines()
        for name in ("emacs", "emacs.pdmp", "config.h", "Makefile"):
            original = source / "src" / name
            retained = output / name
            before = sha256(original)
            shutil.copy2(original, retained)
            if sha256(original) != before or sha256(retained) != before:
                raise ValueError(f"oracle input changed during capture: {name}")
            record["files"].append({
                "original": str(original), "artifact": name,
                "sha256": before, "bytes": retained.stat().st_size,
            })
        record["status"] = "captured"
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        record.update(status="failed", error=str(error))
        raise
    finally:
        with (output / "manifest.json").open("x") as stream:
            json.dump(record, stream, indent=2)
            stream.write("\n")
    return record


def verify(output: Path) -> dict:
    manifest = output / "manifest.json"
    captured = json.loads(manifest.read_text())
    record = {
        "status": "verifying",
        "capture_manifest_sha256": sha256(manifest),
        "qualification": "Checks original and retained bytes after validation; does not change gate outcomes.",
    }
    try:
        if captured["status"] != "captured" or len(captured["files"]) != 4:
            raise ValueError("oracle capture is incomplete")
        if [item["artifact"] for item in captured["files"]] != ["emacs", "emacs.pdmp", "config.h", "Makefile"]:
            raise ValueError("oracle capture inventory differs")
        for item in captured["files"]:
            original = Path(item["original"])
            retained = output / item["artifact"]
            for path in (original, retained):
                if path.stat().st_size != item["bytes"] or sha256(path) != item["sha256"]:
                    raise ValueError(f"oracle input no longer matches capture: {path}")
        record.update(status="unchanged", files_verified=4)
    except (OSError, ValueError, KeyError) as error:
        record.update(status="failed", error=str(error))
        raise
    finally:
        with (output / "verification.json").open("x") as stream:
            json.dump(record, stream, indent=2)
            stream.write("\n")
    return record


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capturing = commands.add_parser("capture")
    capturing.add_argument("--oracle-source", required=True, type=Path)
    capturing.add_argument("--output", required=True, type=Path)
    verifying = commands.add_parser("verify")
    verifying.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "capture":
        capture(args.oracle_source, args.output)
    else:
        verify(args.output)


if __name__ == "__main__":
    main()
