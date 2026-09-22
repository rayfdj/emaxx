#!/usr/bin/env python3
"""Measure the locked core suite through normal editor processes.

Never builds an editor, substitutes a host-language workload, repairs a child
report, or accepts a partial inventory. Preparation and profiling are separate
commands/runs. Raw process output and unsuccessful observations are retained.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import signal
import statistics
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "compat/core_runtime_perf.json"
WORKLOAD = ROOT / "compat/core_runtime_perf.el"
NATIVE_SOURCE = "test/src/comp-resources/comp-test-funcs.el"
REQUIRED_CASES = frozenset((
    "interpreted-lexical", "interpreted-dynamic", "interpreted-calls", "bytecode-calls",
    "native-execution", "interpreted-to-native", "bytecode-to-native",
    "native-to-interpreted", "native-to-bytecode", "cons-allocation", "list-traversal",
    "mapcar", "explicit-gc", "upstream-sort", "upstream-undo", "upstream-bindat"))


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def parse_json(text):
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result
    return json.loads(text, object_pairs_hook=unique_object)


def sha256(path):
    with path.open("rb") as source:
        digest = hashlib.sha256()
        for block in iter(lambda: source.read(1048576), b""):
            digest.update(block)
        return digest.hexdigest()


def validate_contract(contract):
    cases = contract.get("cases", [])
    names = [case.get("id") for case in cases]
    if set(names) != REQUIRED_CASES or len(names) != len(REQUIRED_CASES):
        raise ValueError("core contract has an incomplete or duplicate workload inventory")
    if any(type(case.get("n")) is not int or case["n"] <= 0 for case in cases):
        raise ValueError("core workloads require positive integer sizes")
    for key in ("warmup", "samples_per_process", "rounds", "timeout_seconds", "bootstrap_resamples"):
        if type(contract.get(key)) is not int or contract[key] <= 0:
            raise ValueError(f"invalid core measurement parameter: {key}")
    if not 1 <= contract.get("parity_ratio_limit", 0) <= 1.03:
        raise ValueError("the pre-optimization tolerance ceiling is 3 percent")


def git(directory, *args):
    return subprocess.check_output(["git", "-C", str(directory), *args], text=True).strip()


def lisp(value):
    if value is None:
        return "nil"
    # These arguments are data passed directly to exec, never shell text.
    return json.dumps(str(value), ensure_ascii=False)


def environment(directory, source):
    result = {key: value for key, value in os.environ.items()
              if not key.startswith("EMAXX_") and key not in
              ("EMACSLOADPATH", "EMACSDATA", "EMACSPATH", "EMACSDOC")}
    home = directory / "home"
    home.mkdir(parents=True, exist_ok=True)
    result.update(HOME=str(home), LANG="C", LC_ALL="C", TZ="UTC",
                  EMACS_TEST_DIRECTORY=str(source / "test"))
    return result


def command(binary, source):
    return [str(binary), "-Q", "--batch", "--eval",
            '(princ (format "CORE-PERF-READY %.9f\\n" (float-time)))',
            "--eval", "(setq native-comp-jit-compilation nil load-no-native t)",
            "-L", str(source / "test")]


def run_process(argv, directory, env, timeout):
    """Capture per-child rusage, with output files that cannot fill a pipe."""
    directory.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    epoch = time.time()
    load_before = os.getloadavg()
    outcome = {}
    with (directory / "stdout.txt").open("wb") as stdout, (directory / "stderr.txt").open("wb") as stderr:
        process = subprocess.Popen(argv, cwd=ROOT, env=env, stdout=stdout,
                                   stderr=stderr, start_new_session=True)

        def reap():
            _, status, usage = os.wait4(process.pid, 0)
            process.returncode = os.waitstatus_to_exitcode(status)
            outcome.update(exit=process.returncode,
                           total_seconds=time.monotonic() - started,
                           user_seconds=usage.ru_utime, system_seconds=usage.ru_stime,
                           peak_rss_bytes=int(usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024)))

        waiter = threading.Thread(target=reap)
        waiter.start()
        waiter.join(timeout)
        timed_out = waiter.is_alive()
        if timed_out:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            waiter.join()
    stdout_text = (directory / "stdout.txt").read_text(errors="replace")
    ready = [line for line in stdout_text.splitlines() if line.startswith("CORE-PERF-READY ")]
    startup = float(ready[0].split()[1]) - epoch if len(ready) == 1 else None
    record = dict(command=argv, environment={key: env[key] for key in
                  ("HOME", "LANG", "LC_ALL", "TZ", "EMACS_TEST_DIRECTORY")},
                  timeout=timed_out, startup_seconds=startup,
                  load_average_before=load_before, **outcome)
    # Actual load at completion is also retained; it is not a reason to discard
    # an unfavorable sample selectively.
    record["load_average_after"] = os.getloadavg()
    write_json(directory / "process.json", record)
    return record


def identities(source, binaries, native_dir):
    paths = [Path(__file__).resolve(), CONTRACT, WORKLOAD]
    for binary in binaries.values():
        paths.extend([binary, binary.with_suffix(".pdmp")])
    if native_dir:
        paths.extend(native_dir.glob("*/comp-test-funcs.eln"))
    # Includes bytecode as well as source, because normal require/load may use it.
    paths.extend(path for base in (source / "lisp", source / "test", source / "native-lisp")
                 for path in base.rglob("*")
                 if path.is_file() and path.suffix in (".el", ".elc", ".eln"))
    return {str(path): sha256(path) for path in sorted(set(paths))}


def provenance(args, binaries, native_dir):
    lock = json.loads((ROOT / ("compat/oracle.lock.json" if sys.platform == "darwin"
                              else "compat/oracle.lock.linux.json")).read_text())
    oracle_hash = sha256(args.oracle)
    source_commit = git(args.source, "rev-parse", "HEAD")
    frozen = (oracle_hash == lock["emacs_binary_sha256"] and
              source_commit == lock["emacs_repo_commit"])
    if not frozen and not args.diagnostic:
        raise ValueError("GNU identity differs from the platform lock; only an explicit --diagnostic run is allowed")
    return dict(frozen_oracle_identity=frozen,
                qualification="diagnostic" if args.diagnostic else "candidate certification",
                source_commit=git(ROOT, "rev-parse", "HEAD"),
                source_status=git(ROOT, "status", "--short"),
                source_diff_sha256=hashlib.sha256(git(ROOT, "diff", "--binary").encode()).hexdigest(),
                gnu_source_commit=source_commit, gnu_source_status=git(args.source, "status", "--short"),
                oracle_lock=lock, host=platform.uname()._asdict(),
                input_sha256=identities(args.source, binaries, native_dir))


def retain_sources(output, evidence):
    write_json(output / "provenance.json", evidence)
    directory = output / "measurement-source"
    directory.mkdir()
    for source in (Path(__file__).resolve(), WORKLOAD, CONTRACT):
        (directory / source.name).write_bytes(source.read_bytes())
    (directory / "worktree.patch").write_text(git(ROOT, "diff", "--binary") + "\n")


def check_calibration(directory, contract, evidence):
    if directory is None:
        return {"valid": False, "reason": "no GNU self-comparison supplied"}
    try:
        previous_contract = parse_json((directory / "contract.json").read_text())
        previous = parse_json((directory / "provenance.json").read_text())
        final_inputs = parse_json((directory / "final-inputs.json").read_text())
        if ({key: value for key, value in previous_contract.items() if key != "status"} !=
                {key: value for key, value in contract.items() if key != "status"}):
            raise ValueError("calibration used different measurement parameters")
        if not final_inputs.get("unchanged") or previous["host"] != evidence["host"]:
            raise ValueError("calibration inputs changed, or it used another host")
        for path, digest in previous["input_sha256"].items():
            if path == str(CONTRACT) or "/emaxx/comp-test-funcs.eln" in path:
                continue
            if evidence["input_sha256"].get(path) != digest:
                raise ValueError(f"calibration input changed: {path}")
        records = parse_json((directory / "runs.json").read_text())
        cases = {case["id"]: case for case in contract["cases"]}
        for record in records:
            location = directory / f"round-{record['round']:02}" / record["case_id"] / record["runner"]
            process = parse_json((location / "process.json").read_text())
            report = parse_json((location / "report.json").read_text())
            observations = validate_report(report, process, cases[record["case_id"]], contract,
                (location / "stdout.txt").read_text(), contract["warmup"], contract["samples_per_process"])
            if record["process"] != process or record["observations"] != observations:
                raise ValueError("calibration aggregate disagrees with raw observations")
        result = summarize(records, contract["cases"], contract, ["oracle-a", "oracle-b"], True, False)
        if not result["complete_inventory"] or not result["criterion_satisfied"]:
            raise ValueError("GNU self-comparison does not resolve the declared tolerance")
        return {"valid": True, "directory": str(directory), "summary": result}
    except (OSError, ValueError, KeyError, TypeError) as error:
        return {"valid": False, "directory": str(directory), "reason": str(error)}


def validate_report(report, process, case, contract, raw_stdout, warmup, samples):
    if process.get("exit") != 0 or process.get("timeout"):
        raise ValueError("unsuccessful process")
    for key in ("startup_seconds", "total_seconds"):
        value = process.get(key)
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"missing or invalid process timing: {key}")
    if type(process.get("peak_rss_bytes")) is not int or process["peak_rss_bytes"] <= 0:
        raise ValueError("missing OS memory measurement")
    if report.get("format_version") != 1 or report.get("status") != "completed":
        raise ValueError("invalid report version or status")
    for key, expected in (("case_id", case["id"]), ("n", case["n"]),
                          ("warmup", warmup), ("samples", samples),
                          ("gc_cons_threshold", contract["gc_cons_threshold"]),
                          ("gc_cons_percentage", contract["gc_cons_percentage"])):
        if report.get(key) != expected:
            raise ValueError(f"wrong {key}: {report.get(key)!r}; expected {expected!r}")
    modes = report.get("modes")
    if not isinstance(modes, dict) or not modes:
        raise ValueError("missing execution-mode evidence")
    if "modes" in case and modes != case["modes"]:
        raise ValueError(f"wrong execution mode: {modes!r}; expected {case['modes']!r}")
    observations = report.get("observations")
    if not isinstance(observations, list) or len(observations) != warmup + samples:
        raise ValueError("incomplete observation inventory")
    logged = [parse_json(line.removeprefix("CORE-PERF-SAMPLE "))
              for line in raw_stdout.splitlines() if line.startswith("CORE-PERF-SAMPLE ")]
    if logged != observations:
        raise ValueError("raw process samples disagree with report")
    for index, row in enumerate(observations):
        if row.get("index") != index or row.get("warmup") is not (index < warmup):
            raise ValueError("wrong sample index or warmup flag")
        if (type(row.get("seconds")) not in (int, float) or
                not math.isfinite(row["seconds"]) or row["seconds"] <= 0):
            raise ValueError("invalid duration")
        if type(row.get("gc_count")) is not int or row["gc_count"] < 0:
            raise ValueError("invalid GC count")
        if (type(row.get("gc_seconds")) not in (int, float) or
                not math.isfinite(row["gc_seconds"]) or row["gc_seconds"] < 0):
            raise ValueError("invalid GC time")
        if not isinstance(row.get("result"), str) or not row["result"]:
            raise ValueError("missing checked result")
        if "result" in case and row["result"] != case["result"]:
            raise ValueError("checked result differs from locked result")
        for key in ("allocation_before", "allocation_after"):
            counter = row.get(key, {})
            if counter.get("status") == "available":
                counts = counter.get("counts")
                if (not isinstance(counts, list) or len(counts) != 7 or
                        any(type(value) is not int or value < 0 for value in counts)):
                    raise ValueError("invalid allocation counters")
            elif counter.get("status") != "unavailable" or not counter.get("error"):
                raise ValueError("missing allocation evidence")
    if sum(row["seconds"] for row in observations) + process["startup_seconds"] > process["total_seconds"] + 0.005:
        raise ValueError("reported body and startup time exceed total process time")
    return observations


def interval(values, confidence, resamples, rng):
    medians = sorted(statistics.median(rng.choices(values, k=len(values))) for _ in range(resamples))
    tail = (1 - confidence) / 2
    return [medians[int(tail * (resamples - 1))], medians[int((1 - tail) * (resamples - 1))]]


def summarize(records, cases, contract, runners, calibration, eligible):
    expected = {(round_number, runner, case["id"])
                for round_number in range(contract["rounds"])
                for runner in runners for case in cases}
    actual = [(row["round"], row["runner"], row["case_id"]) for row in records]
    if set(actual) != expected or len(actual) != len(expected):
        raise ValueError("incomplete or duplicate run inventory")
    rng = random.Random(contract["bootstrap_seed"])
    rows = []
    for case in cases:
        observations = [row for row in records if row["case_id"] == case["id"]]
        row = {"case_id": case["id"], "runners": {}}
        for runner in runners:
            selected = [item for item in observations if item["runner"] == runner]
            valid = [item for item in selected if item.get("validation") == "completed"]
            values = [sample["seconds"] for item in valid for sample in item["observations"] if not sample["warmup"]]
            row["runners"][runner] = dict(completed=len(valid), total=len(selected), samples=values,
                                           median=statistics.median(values) if values else None)
        if len(runners) == 2 and all(item.get("validation") == "completed" for item in observations):
            paired = []
            for round_number in range(contract["rounds"]):
                pair = {item["runner"]: statistics.median(sample["seconds"] for sample in item["observations"]
                        if not sample["warmup"]) for item in observations if item["round"] == round_number}
                paired.append(pair[runners[1]] / pair[runners[0]])
            row.update(paired_ratios=paired, median_ratio=statistics.median(paired),
                       ratio_interval=interval(paired, contract["confidence"], contract["bootstrap_resamples"], rng))
            long_enough = all(sample >= contract["minimum_body_seconds"]
                              for runner in runners for sample in row["runners"][runner]["samples"])
            counters = all(sample[key]["status"] == "available" for item in observations
                           for sample in item["observations"] for key in ("allocation_before", "allocation_after"))
            comparable = all(item["modes"] == observations[0]["modes"] for item in observations)
            row.update(long_enough=long_enough, allocation_counters_available=counters, same_modes=comparable)
            limit = contract["parity_ratio_limit"]
            if calibration:
                row["criterion_satisfied"] = (long_enough and comparable and
                                               row["ratio_interval"][0] >= 1 / limit and
                                               row["ratio_interval"][1] <= limit)
            else:
                row["criterion_satisfied"] = (eligible and long_enough and counters and comparable and
                                               row["ratio_interval"][1] <= limit)
        else:
            row["criterion_satisfied"] = False
        rows.append(row)
    medians = [row["median_ratio"] for row in rows if "median_ratio" in row]
    return dict(cases=rows,
                complete_inventory=all(record.get("validation") == "completed" for record in records),
                geometric_mean_ratio=math.exp(statistics.mean(map(math.log, medians))) if len(medians) == len(cases) else None,
                criterion_satisfied=all(row["criterion_satisfied"] for row in rows))


def prepare(args, binaries):
    evidence = provenance(args, binaries, None)
    retain_sources(args.output, evidence)
    # Both compilers use the same unchanged bytes and source path, inside the
    # artifact directory, so any generated bytecode stays out of the GNU tree.
    fixture = args.output / "source" / "comp-test-funcs.el"
    fixture.parent.mkdir()
    fixture.write_bytes((args.source / NATIVE_SOURCE).read_bytes())
    failed = False
    for runner, binary in binaries.items():
        destination = args.output / runner
        destination.mkdir()
        target = destination / "comp-test-funcs.eln"
        argv = command(binary, args.source) + ["--eval",
                f"(progn (require 'comp) (native-compile {lisp(fixture)} {lisp(target)}))"]
        process = run_process(argv, destination / "compile", environment(destination, args.source), 600)
        valid = process["exit"] == 0 and not process["timeout"] and target.is_file()
        write_json(destination / "artifact.json", dict(completed=valid,
                   sha256=sha256(target) if target.is_file() else None, source_sha256=sha256(args.source / NATIVE_SOURCE)))
        failed |= not valid
        print(f"native preparation {runner}: {'completed' if valid else 'failed'}", flush=True)
    return 2 if failed else 0


def measure(args, binaries):
    contract = parse_json(CONTRACT.read_text())
    validate_contract(contract)
    if args.action == "run" and contract["status"] != "locked_before_optimization":
        raise ValueError("authoritative comparison requires the pre-optimization locked contract")
    cases = contract["cases"]
    if args.case:
        if args.action != "pilot":
            raise ValueError("partial selection is only allowed for diagnostic pilot runs")
        cases = [case for case in cases if case["id"] in args.case]
        if len(cases) != len(set(args.case)):
            raise ValueError("unknown or repeated pilot case")
    calibration = args.action == "calibrate"
    if calibration:
        binaries = {"oracle-a": args.oracle, "oracle-b": args.oracle}
    elif args.action == "pilot":
        binaries = {runner: binary for runner, binary in binaries.items()
                    if args.pilot_runner == "both" or runner == args.pilot_runner}
    evidence = provenance(args, binaries, args.native_dir)
    retain_sources(args.output, evidence)
    write_json(args.output / "contract.json", contract)
    calibration_check = check_calibration(args.calibration, contract, evidence) if args.action == "run" else None
    if calibration_check is not None:
        write_json(args.output / "calibration-validation.json", calibration_check)
        if not calibration_check["valid"] and not args.diagnostic:
            raise ValueError(calibration_check["reason"])
    records = []
    rounds = 1 if args.action == "pilot" else contract["rounds"]
    warmup = 0 if args.action == "pilot" else contract["warmup"]
    samples = 1 if args.action == "pilot" else contract["samples_per_process"]
    runners = list(binaries)
    for round_number in range(rounds):
        # Rotate case order, and alternate which editor runs first within pairs.
        ordered_cases = cases[round_number % len(cases):] + cases[:round_number % len(cases)]
        for case_index, case in enumerate(ordered_cases):
            order = runners if (round_number + case_index) % 2 == 0 else list(reversed(runners))
            for runner in order:
                directory = args.output / f"round-{round_number:02}" / case["id"] / runner
                native_runner = "oracle" if runner.startswith("oracle") else runner
                native = args.native_dir / native_runner / "comp-test-funcs.eln"
                report_path = directory / "report.json"
                argv = command(binaries[runner], args.source) + ["-l", str(WORKLOAD), "--eval",
                        f"(core-perf-run {lisp(case['id'])} {case['n']} {warmup} {samples} "
                        f"{lisp(args.source)} {lisp(native)} {lisp(report_path)})"]
                process = run_process(argv, directory, environment(args.output / "environment" / runner, args.source),
                                      contract["timeout_seconds"])
                record = dict(round=round_number, runner=runner, case_id=case["id"], process=process)
                try:
                    report = parse_json(report_path.read_text())
                    record["observations"] = validate_report(report, process, case, contract,
                        (directory / "stdout.txt").read_text(), warmup, samples)
                    record.update(validation="completed", modes=report["modes"])
                except (ValueError, OSError, KeyError, TypeError) as error:
                    record.update(validation="failed", error=str(error))
                write_json(directory / "validation.json", record)
                records.append(record)
                write_json(args.output / "runs.json", records)
                duration = (statistics.median(row["seconds"] for row in record["observations"] if not row["warmup"])
                            if record.get("validation") == "completed" else record["error"])
                print(round_number, runner, case["id"], record["validation"], duration, flush=True)
    after = identities(args.source, binaries, args.native_dir)
    unchanged = evidence["input_sha256"] == after
    write_json(args.output / "final-inputs.json", dict(unchanged=unchanged, input_sha256=after))
    eligible = (unchanged and not args.diagnostic and evidence["frozen_oracle_identity"] and
                contract["status"] == "locked_before_optimization" and
                calibration_check is not None and calibration_check["valid"])
    summary_contract = dict(contract, rounds=rounds)
    summary = summarize(records, cases, summary_contract, runners, calibration, eligible)
    summary.update(action=args.action, certification_eligible=eligible, inputs_unchanged=unchanged)
    write_json(args.output / "summary.json", summary)
    return 0 if unchanged and summary["complete_inventory"] else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "pilot", "calibrate", "run"))
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--oracle", type=Path, required=True)
    parser.add_argument("--emaxx", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--native-dir", type=Path)
    parser.add_argument("--calibration", type=Path)
    parser.add_argument("--diagnostic", action="store_true")
    parser.add_argument("--case", action="append")
    parser.add_argument("--pilot-runner", choices=("oracle", "emaxx", "both"), default="oracle")
    args = parser.parse_args()
    for key in ("source", "oracle", "emaxx", "output", "native_dir", "calibration"):
        value = getattr(args, key)
        if value is not None:
            setattr(args, key, value.resolve())
    if args.action != "prepare" and args.native_dir is None:
        parser.error("--native-dir is required after preparation")
    args.output.mkdir(parents=True, exist_ok=False)
    binaries = {"oracle": args.oracle, "emaxx": args.emaxx}
    try:
        return prepare(args, binaries) if args.action == "prepare" else measure(args, binaries)
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        write_json(args.output / "failure.json", dict(error=str(error)))
        print(error, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
