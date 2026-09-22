"""Adversarial controls for accepting runtime performance evidence."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

import core_runtime_perf as perf


class EvidenceControls(unittest.TestCase):
    def setUp(self):
        self.case = {"id": "sample", "n": 20, "result": "190", "modes": {"body": "bytecode"}}
        self.contract = {"gc_cons_threshold": 800000, "gc_cons_percentage": 1.0,
                         "rounds": 1, "bootstrap_seed": 7, "confidence": 0.95,
                         "bootstrap_resamples": 20, "minimum_body_seconds": 0.1,
                         "parity_ratio_limit": 1.03}
        self.process = {"exit": 0, "timeout": False, "startup_seconds": 0.1,
                        "total_seconds": 0.8, "peak_rss_bytes": 1000000}
        counter = {"status": "available", "counts": [0] * 7}
        self.report = {"format_version": 1, "status": "completed", "case_id": "sample",
                       "n": 20, "warmup": 1, "samples": 2,
                       "gc_cons_threshold": 800000, "gc_cons_percentage": 1.0,
                       "modes": {"body": "bytecode"},
                       "observations": [{"index": index, "warmup": index == 0,
                                         "seconds": 0.2, "gc_count": 1, "gc_seconds": 0.01,
                                         "result": "190", "allocation_before": counter,
                                         "allocation_after": counter} for index in range(3)]}

    def validate(self, report=None, process=None, raw=None):
        report = self.report if report is None else report
        if raw is None:
            raw = "\n".join("CORE-PERF-SAMPLE " + json.dumps(row) for row in report["observations"])
        return perf.validate_report(report, self.process if process is None else process,
                                    self.case, self.contract, raw, 1, 2)

    def test_completed_evidence_is_accepted(self):
        self.assertEqual(len(self.validate()), 3)

    def test_incorrect_results_modes_samples_and_counters_are_rejected(self):
        controls = [
            lambda report: report.update(status="failed"),
            lambda report: report.update(case_id="substitute"),
            lambda report: report.update(n=10),
            lambda report: report.update(gc_cons_threshold=100000000),
            lambda report: report.update(modes={"body": "interpreted"}),
            lambda report: report["observations"].pop(),
            lambda report: report["observations"].append(report["observations"][0]),
            lambda report: report["observations"][1].update(index=0),
            lambda report: report["observations"][1].update(warmup=True),
            lambda report: report["observations"][1].update(result="canned-wrong-answer"),
            lambda report: report["observations"][1].update(seconds=0),
            lambda report: report["observations"][1].update(seconds=True),
            lambda report: report["observations"][1].update(seconds=float("nan")),
            lambda report: report["observations"][1].update(gc_count=-1),
            lambda report: report["observations"][1].update(gc_seconds=float("inf")),
            lambda report: report["observations"][1].update(allocation_after={"status": "available", "counts": []}),
            lambda report: report["observations"][1].update(allocation_after={"status": "unavailable"}),
        ]
        for index, corrupt in enumerate(controls):
            with self.subTest(control=index):
                report = copy.deepcopy(self.report)
                corrupt(report)
                with self.assertRaises(ValueError):
                    self.validate(report=report)

    def test_unsuccessful_process_cannot_supply_completed_report(self):
        for process in ({"exit": 7, "timeout": False}, {"exit": 0, "timeout": True},
                        {"exit": None, "timeout": False}):
            with self.subTest(process=process), self.assertRaises(ValueError):
                self.validate(process=process)

    def test_missing_process_metrics_and_impossible_totals_are_rejected(self):
        for key, value in (("startup_seconds", None), ("total_seconds", 0.5),
                           ("peak_rss_bytes", 0)):
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.validate(process=dict(self.process, **{key: value}))

    def test_duplicate_json_keys_and_incomplete_contract_are_rejected(self):
        with self.assertRaises(ValueError):
            perf.parse_json('{"status":"failed", "status":"completed"}')
        contract = json.loads(perf.CONTRACT.read_text())
        perf.validate_contract(contract)
        contract["cases"].pop()
        with self.assertRaises(ValueError):
            perf.validate_contract(contract)
        contract["cases"] = []
        with self.assertRaises(ValueError):
            perf.validate_contract(contract)

    def test_raw_evidence_must_agree_and_is_preserved_on_rejection(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            original = json.dumps(self.report).encode()
            path.write_bytes(original)
            with self.assertRaises(ValueError):
                self.validate(report=json.loads(path.read_bytes()), raw="CORE-PERF-SAMPLE {}\n")
            self.assertEqual(path.read_bytes(), original)

    def test_complete_inventory_and_metric_availability_are_required(self):
        records = [dict(round=0, runner=runner, case_id="sample", validation="completed",
                        modes=self.report["modes"], observations=copy.deepcopy(self.report["observations"]))
                   for runner in ("oracle", "emaxx")]
        runners = ["oracle", "emaxx"]
        def summary(rows, eligible=True):
            return perf.summarize(rows, [self.case], self.contract, runners, False, eligible)
        self.assertTrue(summary(records)["criterion_satisfied"])
        self.assertFalse(summary(records, eligible=False)["criterion_satisfied"])
        for incomplete in (records[:1], records + records[:1]):
            with self.assertRaises(ValueError):
                summary(incomplete)
        records[1]["observations"][1]["allocation_after"] = {
            "status": "unavailable", "error": "real counter missing"}
        self.assertFalse(summary(records)["criterion_satisfied"])
        records[1]["validation"] = "failed"
        failed = summary(records)
        self.assertFalse(failed["complete_inventory"])
        self.assertNotIn("median_ratio", failed["cases"][0])

    def calibration_fixture(self, directory):
        contract = dict(self.contract, cases=[self.case], warmup=1, samples_per_process=2)
        evidence = {"host": {"system": "control-host"},
                    "input_sha256": {"oracle": "original-binary", "workload": "original-source"}}
        perf.write_json(directory / "contract.json", contract)
        perf.write_json(directory / "provenance.json", evidence)
        perf.write_json(directory / "final-inputs.json", {"unchanged": True})
        records = []
        for runner in ("oracle-a", "oracle-b"):
            location = directory / "round-00" / "sample" / runner
            location.mkdir(parents=True)
            perf.write_json(location / "process.json", self.process)
            perf.write_json(location / "report.json", self.report)
            (location / "stdout.txt").write_text("\n".join(
                "CORE-PERF-SAMPLE " + json.dumps(row) for row in self.report["observations"]))
            records.append(dict(round=0, runner=runner, case_id="sample", validation="completed",
                                process=self.process, modes=self.report["modes"],
                                observations=copy.deepcopy(self.report["observations"])))
        perf.write_json(directory / "runs.json", records)
        return contract, evidence

    def test_calibration_is_rechecked_against_raw_evidence_and_current_inputs(self):
        def alter_json(path, update):
            value = json.loads(path.read_text())
            update(value)
            path.write_text(json.dumps(value))

        controls = [
            lambda root: alter_json(root / "contract.json", lambda item: item.update(parity_ratio_limit=1.10)),
            lambda root: alter_json(root / "provenance.json", lambda item: item.update(host={})),
            lambda root: alter_json(root / "provenance.json", lambda item: item["input_sha256"].update(oracle="changed")),
            lambda root: alter_json(root / "final-inputs.json", lambda item: item.update(unchanged=False)),
            lambda root: alter_json(root / "runs.json", lambda item: item.pop()),
            lambda root: alter_json(root / "runs.json", lambda item: item[0]["observations"][1].update(seconds=0.19)),
            lambda root: alter_json(root / "round-00/sample/oracle-b/process.json", lambda item: item.update(exit=1)),
            lambda root: (root / "round-00/sample/oracle-b/report.json").unlink(),
            lambda root: (root / "round-00/sample/oracle-b/stdout.txt").write_text(""),
        ]
        for index, corrupt in enumerate(controls):
            with self.subTest(control=index), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                contract, evidence = self.calibration_fixture(root)
                self.assertTrue(perf.check_calibration(root, contract, evidence)["valid"])
                corrupt(root)
                self.assertFalse(perf.check_calibration(root, contract, evidence)["valid"])

    def test_missing_or_unresolved_calibration_cannot_certify_parity(self):
        self.assertFalse(perf.check_calibration(None, self.contract, {})["valid"])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            contract, evidence = self.calibration_fixture(root)
            report = copy.deepcopy(self.report)
            for row in report["observations"]:
                row["seconds"] *= 1.1
            location = root / "round-00/sample/oracle-b"
            perf.write_json(location / "report.json", report)
            (location / "stdout.txt").write_text("\n".join(
                "CORE-PERF-SAMPLE " + json.dumps(row) for row in report["observations"]))
            records = json.loads((root / "runs.json").read_text())
            records[1]["observations"] = report["observations"]
            perf.write_json(root / "runs.json", records)
            result = perf.check_calibration(root, contract, evidence)
            self.assertFalse(result["valid"])
            self.assertIn("does not resolve", result["reason"])


if __name__ == "__main__":
    unittest.main()
