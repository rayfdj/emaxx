#!/usr/bin/env python3
"""Evidence controls; these fake files do not certify any Rust execution."""

import json
from pathlib import Path
import tempfile
import unittest

import retain_rust_gate_artifacts as artifacts


class RetainedInputsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.binary = self.root / "original-libtest"
        self.binary.write_bytes(b"test executable bytes")
        self.images = self.root / "fixture-images"
        self.images.mkdir()
        (self.images / "loadup.pdmp").write_bytes(b"test image bytes")
        self.summary = self.root / "summary.json"
        self.write_summary("passed")

    def write_summary(self, status, binary=True):
        record = {"status": status, "failed": 1 if status == "failed" else 0}
        if binary:
            record["test_binary"] = {
                "path": str(self.binary), "sha256": artifacts.sha256(self.binary),
            }
        self.summary.write_text(json.dumps(record))

    def test_retains_exact_binary_and_image_without_changing_gate_outcome(self):
        original = self.summary.read_bytes()
        record = artifacts.retain(self.summary, self.images)
        self.assertEqual(record["status"], "retained")
        self.assertEqual(len(record["files"]), 2)
        for file in record["files"]:
            copied = self.root / "retained-inputs" / file["artifact"]
            self.assertEqual(copied.read_bytes(), Path(file["original"]).read_bytes())
            self.assertEqual(artifacts.sha256(copied), file["sha256"])
        self.assertEqual(self.summary.read_bytes(), original)

    def test_failed_gate_stays_failed_and_still_keeps_its_inputs(self):
        self.write_summary("failed")
        original = self.summary.read_bytes()
        record = artifacts.retain(self.summary, self.images)
        self.assertEqual(record["gate_status"], "failed")
        self.assertEqual(len(record["files"]), 2)
        self.assertEqual(self.summary.read_bytes(), original)

    def test_changed_executable_is_rejected_with_failed_retention_receipt(self):
        self.binary.write_bytes(b"different build")
        with self.assertRaisesRegex(ValueError, "tested SHA-256"):
            artifacts.retain(self.summary, self.images)
        record = json.loads((self.root / "retained-inputs/manifest.json").read_text())
        self.assertEqual(record["status"], "failed")
        self.assertEqual(record["files"], [])

    def test_missing_executable_is_not_silently_accepted(self):
        self.binary.unlink()
        with self.assertRaises(FileNotFoundError):
            artifacts.retain(self.summary, self.images)

    def test_no_images_does_not_invent_an_image_input(self):
        (self.images / "loadup.pdmp").unlink()
        record = artifacts.retain(self.summary, self.images)
        self.assertEqual([file["artifact"] for file in record["files"]], ["libtest"])

    def test_failure_before_binary_build_is_reported_explicitly(self):
        self.write_summary("failed", binary=False)
        record = artifacts.retain(self.summary, self.images)
        self.assertIn("binary_unavailable", record)
        self.assertEqual(record["gate_status"], "failed")

    def test_pass_without_binary_identity_is_rejected(self):
        self.write_summary("passed", binary=False)
        with self.assertRaisesRegex(ValueError, "passing gate"):
            artifacts.retain(self.summary, self.images)

    def test_existing_artifacts_are_not_overwritten(self):
        artifacts.retain(self.summary, self.images)
        manifest = self.root / "retained-inputs/manifest.json"
        original = manifest.read_bytes()
        with self.assertRaises(FileExistsError):
            artifacts.retain(self.summary, self.images)
        self.assertEqual(manifest.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
