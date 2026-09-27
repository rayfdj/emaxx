"""Negative controls for the generated interval-tree comparison."""
import unittest
from pathlib import Path
import sys
import tempfile

from check_overlay_itree import run, validate_model


class IntervalEvidenceControls(unittest.TestCase):
    rows = ["I 0 2 5 1 0", "I 1 4 4 0 1", "S", "+ 4 3 0",
            "Q 1 10 0 -1 -1", "- 2 4", "S", "R 0", "R 1", "S"]
    good = b"S3 2 0:2:5 1:4:4\nQ5 0:2:8 1:4:7\nS7 2 0:2:4 1:2:3\nS10 0\n"

    def test_complete_geometry_and_membership_are_accepted(self):
        validate_model(self.rows, self.good)

    def test_corrupted_and_incomplete_reports_are_rejected(self):
        for output in [self.good.replace(b"0:2:8", b"0:2:7"),
                       self.good.replace(b"Q5 0:2:8 1:4:7", b"Q5 0:2:8"),
                       self.good.replace(b"Q5 0:2:8", b"Q5 0:2:8 0:2:8"),
                       self.good.replace(b"S3 2", b"S3 1"),
                       self.good.replace(b"S10 0\n", b""),
                       self.good + b"S11 0\n", b""]:
            with self.subTest(output=output), self.assertRaises(ValueError):
                validate_model(self.rows, output)

    def test_failed_process_is_rejected_and_its_output_is_preserved(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            record = []
            with self.assertRaises(RuntimeError):
                run([sys.executable, "-c", "print('partial'); raise SystemExit(7)"],
                    directory, "failed", record)
            self.assertEqual(record[0]["exit_code"], 7)
            self.assertFalse(record[0]["timed_out"])
            self.assertEqual((directory / "failed.out").read_bytes(), b"partial\n")

    def test_timed_out_process_cannot_pass_with_partial_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            record = []
            with self.assertRaises(RuntimeError):
                run([sys.executable, "-c", "import time; print('partial', flush=True); time.sleep(10)"],
                    directory, "timed-out", record, timeout=1)
            self.assertTrue(record[0]["timed_out"])
            self.assertIsNone(record[0]["exit_code"])
            self.assertEqual((directory / "timed-out.out").read_bytes(), b"partial\n")


if __name__ == "__main__":
    unittest.main()
