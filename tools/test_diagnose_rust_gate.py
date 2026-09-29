"""The diagnostic must preserve process boundaries and reject incomplete runs."""

import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import diagnose_rust_gate as diagnostic


FAKE_LIBTEST = '''#!/usr/bin/env python3
import os, pathlib, sys
names = ["prelude", "target"]
selected = [name for name in names if len(sys.argv) == 2 or name in sys.argv]
if "--list" in sys.argv:
    if "--ignored" not in sys.argv:
        for name in selected:
            print(name + ": test")
    raise SystemExit(0)
name = selected[0]
root = pathlib.Path(os.environ["EMAXX_FIXTURE_IMAGE_DIR"])
with (root / "order.txt").open("a") as stream:
    stream.write(name + "\\n")
if name == "prelude":
    (root / "loadup-fixture.pdmp").write_bytes(b"original fixture image")
failed = name == os.environ.get("FAKE_FAILURE")
failed |= name == "target" and not (root / "loadup-fixture.pdmp").exists()
print("test " + name + " ... " + ("FAILED" if failed else "ok"))
print("test result: " + ("FAILED" if failed else "ok") + ". " +
      str(int(not failed)) + " passed; " + str(int(failed)) +
      " failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.01s")
raise SystemExit(101 if failed else 0)
'''


class DiagnosticTests(unittest.TestCase):
    def run_replay(self, prelude=True, failure=None):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        binary = root / "fake-libtest"
        binary.write_text(FAKE_LIBTEST)
        binary.chmod(0o700)
        images = root / "images"
        images.mkdir()
        output = root / "output"
        environment = {**os.environ, "EMAXX_FIXTURE_IMAGE_DIR": str(images)}
        if failure:
            environment["FAKE_FAILURE"] = failure
        arguments = ["diagnose_rust_gate.py", "--plain", "--filter", "target",
                     "--output", str(output)]
        if prelude:
            arguments += ["--prelude-filter", "prelude"]
        gate = diagnostic.serial_grouped_gate.gate
        with patch.object(sys, "argv", arguments), \
                patch.object(gate, "PROJECT_ROOT", root), \
                patch.object(gate, "gate_environment", return_value=environment), \
                patch.object(gate, "discover_test_binary", return_value=binary), \
                patch.object(gate, "git_state", return_value={"head": "unchanged", "dirty": False}):
            try:
                result = diagnostic.main()
            except gate.GateError:
                result = "failed"
        return result, json.loads((output / "summary.json").read_text()), images, output

    def test_prelude_runs_first_and_preserves_image_and_binary(self):
        result, summary, images, output = self.run_replay()
        self.assertEqual(result, 0)
        self.assertEqual((images / "order.txt").read_text(), "prelude\ntarget\n")
        self.assertEqual(summary["prelude"]["result"]["passed"], 1)
        self.assertEqual(summary["result"]["passed"], 1)
        self.assertEqual(summary["executed_tests"], ["target"])
        self.assertEqual((output / "fixture-images/loadup-fixture.pdmp").read_bytes(),
                         b"original fixture image")
        self.assertEqual(len(summary["fixture_images"]), 1)
        self.assertTrue((output / "libtest").is_file())

    def test_failed_prelude_stops_before_target(self):
        result, summary, images, output = self.run_replay(failure="prelude")
        self.assertEqual(result, "failed")
        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["prelude"]["exit_code"], 101)
        self.assertEqual((images / "order.txt").read_text(), "prelude\n")
        self.assertFalse((output / "replay.log").exists())
        self.assertEqual(len(summary["fixture_images"]), 1)

    def test_target_failure_keeps_successful_prelude_separate(self):
        result, summary, _, _ = self.run_replay(failure="target")
        self.assertEqual(result, "failed")
        self.assertEqual(summary["prelude"]["status"], "passed")
        self.assertEqual(summary["exit_code"], 101)

    def test_no_prelude_does_not_invent_startup_history(self):
        result, summary, images, _ = self.run_replay(prelude=False)
        self.assertEqual(result, "failed")
        self.assertNotIn("prelude", summary)
        self.assertEqual((images / "order.txt").read_text(), "target\n")
        self.assertEqual(summary["fixture_images"], [])

    def test_successful_process_with_missing_execution_is_rejected(self):
        record = dict(exit_code=0, timed_out=False, executed_tests=[])
        text = "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
        with self.assertRaises(diagnostic.serial_grouped_gate.gate.GateError):
            diagnostic.validate_execution(record, text, ["target"], ["target"], [])

    def test_timed_out_process_cannot_pass(self):
        record = dict(exit_code=0, timed_out=True, executed_tests=["target"])
        with self.assertRaises(diagnostic.serial_grouped_gate.gate.GateError):
            diagnostic.validate_execution(record, "", ["target"], ["target"], [])


if __name__ == "__main__":
    unittest.main()
