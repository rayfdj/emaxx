#!/usr/bin/env python3
"""Compare the Rust interval index with the configured, unmodified GNU C source.

This is a structural/behavioral differential control, not an editor benchmark.
Each output directory is fresh and retains every input, output, process result,
source hash, and compiler command, including failed comparisons.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import shlex
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def case(seed):
    rng = random.Random(seed)
    live = set()
    rows = []
    for step in range(6000):
        op = rng.randrange(10)
        if (op < 2 and len(live) < 256) or not live:
            index = rng.choice(sorted(set(range(256)) - live))
            beg = rng.randrange(256)
            end = beg + rng.randrange(64)
            rows.append(f"I {index} {beg} {end} {rng.randrange(2)} {rng.randrange(2)}")
            live.add(index)
        elif op == 2:
            index = rng.choice(sorted(live))
            rows.append(f"R {index}")
            live.remove(index)
        elif op == 3:
            index = rng.choice(sorted(live))
            beg = rng.randrange(256)
            rows.append(f"M {index} {beg} {beg + rng.randrange(64)}")
        elif op < 6:
            rows.append(f"+ {rng.randrange(256)} {rng.randrange(16)} {rng.randrange(2)}")
        elif op < 8:
            rows.append(f"- {rng.randrange(256)} {rng.randrange(32)}")
        else:
            beg = rng.randrange(320)
            end = beg + rng.randrange(128)
            narrow = rng.choice([True, False])
            nbegin = rng.randrange(beg, end + 1)
            nend = rng.randrange(nbegin, end + 1)
            rows.append(f"Q {beg} {end} {rng.randrange(4)} {nbegin if narrow else -1} {nend}")
        if step % 127 == 0:
            rows.append("S")
    for beg, end in [(-1, 100000), (0, 0), (1, 1), (1, 2), (100, 100), (255, 256), (512, 1024)]:
        for order in range(4):
            rows.append(f"Q {beg} {end} {order} -1 -1")
    rows.append("S")
    rows.extend(f"R {index}" for index in sorted(live))
    rows.append("S")
    return rows


def validate_model(rows, output):
    """Independent linear endpoint model; never uses tree links or offsets."""
    live = {}
    observed = iter(output.decode().splitlines())
    for step, row in enumerate(rows, 1):
        op, *args = row.split()
        args = list(map(int, args))
        if op == "I":
            index, beg, end, front, rear = args
            if index in live:
                raise ValueError("duplicate insertion")
            live[index] = [beg, end, front, rear]
        elif op == "R":
            del live[args[0]]
        elif op == "M":
            index, beg, end = args
            live[index][:2] = [beg, max(beg, end)]
        elif op == "+":
            pos, length, before = args
            for node in live.values():
                beg, end, front, rear = node
                if not before and beg == end == pos:
                    if rear:
                        end += length
                        if front:
                            beg += length
                else:
                    if beg > pos or (beg == pos and (front or before)):
                        beg += length
                    if end > pos or (end == pos and (rear or before)):
                        end += length
                node[:2] = [beg, end]
        elif op == "-":
            pos, length = args
            for node in live.values():
                node[:2] = [v if v <= pos else max(pos, v - length) for v in node[:2]]
        elif op in {"S", "Q"}:
            line = next(observed, "")
            fields = line.split()
            if not fields or fields[0] != f"{op}{step}":
                raise ValueError(f"missing or substituted operation {step}: {line}")
            if op == "S":
                if len(fields) < 2 or int(fields[1]) != len(live):
                    raise ValueError(f"wrong live count at {step}")
                fields = fields[2:]
            else:
                fields = fields[1:]
            nodes = [tuple(map(int, field.split(":"))) for field in fields]
            if len({node[0] for node in nodes}) != len(nodes):
                raise ValueError(f"duplicate query member at {step}")
            for index, beg, end in nodes:
                if index not in live or live[index][:2] != [beg, end]:
                    raise ValueError(f"wrong endpoint at {step}: {index}:{beg}:{end}")
            if op == "S" and {node[0] for node in nodes} != set(live):
                raise ValueError(f"incomplete snapshot at {step}")
            if op == "Q" and args[3] == -1:
                beg, end = args[:2]
                wanted = {index for index, (a, b, _, _) in live.items()
                          if (beg < b and a < end) or (a == b and beg == a)}
                if {node[0] for node in nodes} != wanted:
                    raise ValueError(f"wrong query membership at {step}")
    if next(observed, None) is not None:
        raise ValueError("extra output beyond complete operation inventory")


def run(command, directory, name, record, content=None, timeout=300):
    timed_out = False
    try:
        result = subprocess.run(command, input=content, capture_output=True, timeout=timeout, cwd=ROOT)
        output, errors, code = result.stdout, result.stderr, result.returncode
    except subprocess.TimeoutExpired as error:
        # subprocess.run kills and waits for the failed child. Preserve its
        # partial output and failure in the same inventory as other outcomes.
        timed_out = True
        output, errors, code = error.stdout or b"", error.stderr or b"", None
    stdout = directory / f"{name}.out"
    stderr = directory / f"{name}.err"
    stdout.write_bytes(output)
    stderr.write_bytes(errors)
    record.append({"name": name, "command": command, "exit_code": code,
                   "timed_out": timed_out, "timeout_seconds": timeout,
                   "stdout_sha256": digest(stdout), "stderr_sha256": digest(stderr)})
    if timed_out or code:
        raise RuntimeError(f"{name}: {'timeout' if timed_out else f'exit {code}'}; see {stderr}")
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gnu-src", type=Path, required=True, help="configured GNU checkout")
    parser.add_argument("--output", type=Path, required=True, help="new evidence directory")
    args = parser.parse_args()
    directory = args.output.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    gnu = args.gnu_src.resolve()
    files = [ROOT / "src/overlay/itree.rs", ROOT / "tests/fixtures/overlay-itree-driver.rs",
             ROOT / "tests/fixtures/overlay-itree-driver.c", Path(__file__).resolve(),
             gnu / "src/itree.c", gnu / "src/itree.h", gnu / "src/config.h", gnu / "lib/libgnu.a"]
    report = {"status": "running", "sources": {}, "processes": [], "cases": [],
              "python": sys.version, "host": platform.platform()}
    try:
        report["sources"] = {str(p): digest(p) for p in files}
        cc = shlex.split(os.environ.get("CC", "cc"))
        rustc = shlex.split(os.environ.get("RUSTC", "rustc"))
        for name, command in [("cc-version", cc + ["--version"]),
                              ("rustc-version", rustc + ["-vV"]),
                              ("gnu-revision", ["git", "-C", str(gnu), "rev-parse", "HEAD"]),
                              ("gnu-source-diff", ["git", "-C", str(gnu), "diff", "HEAD", "--", "src/itree.c", "src/itree.h"])]:
            report[name] = run(command, directory, name, report["processes"]).decode()
        run(cc + ["-std=gnu2x", "-O1", "-g", "-I", str(gnu / "src"),
            "-I", str(gnu / "lib"), str(ROOT / "tests/fixtures/overlay-itree-driver.c"),
            str(gnu / "lib/libgnu.a"), "-lm", "-o", str(directory / "gnu-driver")], directory, "compile-gnu", report["processes"])
        run(rustc + ["--edition=2024", "-D", "warnings", "-C", "opt-level=1",
            "-C", "debug-assertions=yes", "-C", "overflow-checks=yes", str(ROOT / "tests/fixtures/overlay-itree-driver.rs"),
            "-o", str(directory / "rust-driver")], directory, "compile-rust", report["processes"])
        report["executables"] = {name: digest(directory / f"{name}-driver") for name in ["gnu", "rust"]}
        for seed in range(32):
            rows = case(seed)
            content = ("\n".join(rows) + "\n").encode()
            input_path = directory / f"seed-{seed}.txt"
            input_path.write_bytes(content)
            outputs = [run([str(directory / f"{name}-driver")], directory, f"seed-{seed}-{name}", report["processes"], content)
                       for name in ["gnu", "rust"]]
            for output in outputs:
                validate_model(rows, output)
            if outputs[0] != outputs[1]:
                raise ValueError(f"seed {seed}: C/Rust outputs differ; all raw outputs retained")
            report["cases"].append({"seed": seed, "operations": len(rows), "input_sha256": digest(input_path)})
        if any(digest(Path(path)) != expected for path, expected in report["sources"].items()):
            raise ValueError("source changed during comparison")
        if any(digest(directory / f"{name}-driver") != expected for name, expected in report["executables"].items()):
            raise ValueError("executable changed during comparison")
        report["status"] = "passed"
    except Exception as error:
        report.update(status="failed", error=str(error))
    finally:
        (directory / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"status": report["status"], "cases": len(report["cases"]),
                      "operations": sum(case["operations"] for case in report["cases"]),
                      "error": report.get("error")}))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
