#!/usr/bin/env python3
"""Retain a finished Rust gate's executable and remaining fixture images.

This records evidence without rerunning tests or changing their outcome summary.
Images are the files present after the run, not a per-test image-use transcript.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def retain(summary_path: Path, fixture_directory: Path) -> dict:
    summary = json.loads(summary_path.read_text())
    destination = summary_path.parent / "retained-inputs"
    destination.mkdir(exist_ok=False)
    record = {
        "gate_status": summary["status"],
        "gate_summary_sha256": sha256(summary_path),
        "qualification": "Post-run files; no inference that every test used every image.",
        "status": "retaining",
        "files": [],
    }

    def copy(source: Path, target: Path, expected: str | None = None) -> None:
        before = sha256(source)
        if expected is not None and before != expected:
            raise ValueError("executable no longer matches the tested SHA-256")
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        if sha256(target) != before or sha256(source) != before:
            raise ValueError("retained artifact differs from its source")
        record["files"].append({
            "original": str(source), "artifact": str(target.relative_to(destination)),
            "sha256": before, "bytes": target.stat().st_size,
        })

    try:
        binary = summary.get("test_binary")
        if binary is not None:
            copy(Path(binary["path"]), destination / "libtest", binary["sha256"])
        elif summary["status"] == "passed":
            raise ValueError("a passing gate must identify its tested executable")
        else:
            record["binary_unavailable"] = "gate did not record a built test executable"
        for source in sorted(fixture_directory.glob("*.pdmp")):
            copy(source, destination / "fixture-images" / source.name)
        record["status"] = "retained"
    except (OSError, ValueError, KeyError) as error:
        record.update(status="failed", error=str(error))
        raise
    finally:
        (destination / "manifest.json").write_text(json.dumps(record, indent=2) + "\n")
    return record


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--fixture-directory", type=Path, required=True)
    args = parser.parse_args()
    retain(args.summary, args.fixture_directory)


if __name__ == "__main__":
    main()
