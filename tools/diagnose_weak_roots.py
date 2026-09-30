#!/usr/bin/env python3
"""Trace weak-key ownership in an isolated, explicitly instrumented checkout.

This preserves the selected Rust test's assertions and original gate timeout.
Its altered executable cannot certify ordinary correctness or performance.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def compiled_inputs(checkout):
    files = list((checkout / "src").rglob("*.rs"))
    files.extend(path for path in (checkout / "tests").rglob("*") if path.is_file())
    files.extend(checkout / name for name in ("Cargo.toml", "Cargo.lock", "build.rs"))
    files.extend(path for path in (checkout / ".cargo").glob("*") if path.is_file())
    return {str(path.relative_to(checkout)): sha256(path) for path in sorted(set(files))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--filter", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--oracle-source", type=Path)
    parser.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args()
    if not args.filter.strip():
        parser.error("--filter must select a nonempty original test inventory")

    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    checkout = root / "target" / f"weak-root-trace-{time.time_ns()}" / "emaxx"
    oracle = (args.oracle_source or root.parent / "emacs").resolve()
    if not (oracle / "src/emacs").is_file():
        parser.error("--oracle-source must contain the built GNU control")
    patch = root / "tools/diagnostics/weak-root-trace.patch"
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    record = {
        "status": "preparing", "base_commit": head, "checkout": str(checkout),
        "oracle_source": str(oracle), "filter": args.filter,
        "instrumentation_sha256": sha256(patch),
        "qualification": (
            "Diagnostic-only changes record weak-key graph predecessors and conservative "
            "root locations; they do not alter root decisions, assertions, selection or "
            "timeouts. Instrumentation changes compiler spills and allocations, so a "
            "passing diagnostic never clears a failure of the ordinary executable."
        ),
    }

    def save():
        (output / "summary.json").write_text(json.dumps(record, indent=2) + "\n")

    save()
    try:
        subprocess.run(["git", "worktree", "add", "--detach", str(checkout), head],
                       cwd=root, check=True)
        (checkout.parent / "emacs").symlink_to(oracle, target_is_directory=True)
        before = compiled_inputs(checkout)
        (output / "source-before.json").write_text(json.dumps(before, indent=2) + "\n")
        subprocess.run(["git", "apply", "--check", str(patch)], cwd=checkout, check=True)
        subprocess.run(["git", "apply", str(patch)], cwd=checkout, check=True)
        (output / "instrumentation.patch").write_bytes(
            subprocess.check_output(["git", "diff", "--binary"], cwd=checkout)
        )
        instrumented = compiled_inputs(checkout)
        (output / "source-instrumented.json").write_text(
            json.dumps(instrumented, indent=2) + "\n"
        )
        changed = sorted(name for name in before if before[name] != instrumented[name])
        if changed != ["src/lisp/alloc.rs", "src/lisp/eval.rs"]:
            raise RuntimeError(f"unexpected instrumentation inputs: {changed}")
        if args.prepare_only:
            record["status"] = "prepared only; no tests executed"
            return 0
        command = [sys.executable, "tools/diagnose_rust_gate.py", "--filter", args.filter,
                   "--plain", "--output", str(output / "replay")]
        environment = {**os.environ, "CARGO_TARGET_DIR": str(root / "target")}
        record.update(status="running diagnostic", command=command,
                      environment_override={"CARGO_TARGET_DIR": str(root / "target")})
        save()
        with (output / "driver.log").open("xb") as log:
            result = subprocess.run(command, cwd=checkout, env=environment,
                                    stdout=log, stderr=subprocess.STDOUT)
        unchanged = compiled_inputs(checkout) == instrumented
        record.update(status="diagnostic finished", exit_code=result.returncode,
                      instrumented_source_unchanged=unchanged)
        return result.returncode or (0 if unchanged else 2)
    except BaseException as error:
        record.update(status="diagnostic error", error=repr(error))
        raise
    finally:
        save()


if __name__ == "__main__":
    raise SystemExit(main())
