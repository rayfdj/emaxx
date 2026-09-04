#!/usr/bin/env python3
"""Offline unit tests for tramp_compat_gate.py."""

import argparse
import contextlib
import io
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import tramp_compat_gate as gate


class TrampCompatGateTests(unittest.TestCase):
    def test_remote_root_cannot_enable_ssh_implicitly(self):
        args = argparse.Namespace(
            timeout=10,
            live_ssh=False,
            remote_root="/ssh:user@example.test:/tmp/",
        )
        with self.assertRaisesRegex(ValueError, "only with --live-ssh"):
            gate.validate_args(args)

    def test_live_ssh_requires_explicit_tramp_ssh_root(self):
        args = argparse.Namespace(timeout=10, live_ssh=True, remote_root=None)
        with self.assertRaisesRegex(ValueError, "requires an explicit"):
            gate.validate_args(args)
        args.remote_root = "/tmp/not-remote"
        with self.assertRaisesRegex(ValueError, "requires an explicit"):
            gate.validate_args(args)
        args.remote_root = "/ssh:user@example.test:/tmp/"
        gate.validate_args(args)

    def test_mock_journey_exercises_required_integration_surfaces(self):
        program = gate.journey_lisp("/mock::/tmp/journey/", True)
        for form in (
            "find-file-noselect",
            "save-buffer",
            "revert-buffer",
            "directory-files",
            "file-name-completion",
            "copy-file",
            "rename-file",
            "delete-file",
            "file-attributes",
            "make-nearby-temp-file",
            "process-file",
            "start-file-process",
            "tramp-get-connection-process",
            "vc-registered",
            "project-current",
            "dired-noselect",
            "compilation-start",
            "find-file-name-handler",
            "tramp-cleanup-connection",
        ):
            self.assertIn(form, program)
        self.assertIn('"mock"', program)
        self.assertIn("prin1-to-string", program)
        self.assertIn("print-escape-newlines t", program)

    def test_live_journey_does_not_install_mock_transport(self):
        program = gate.journey_lisp("/ssh:user@example.test:/tmp/journey/", False)
        self.assertNotIn("tramp-login-program", program)
        self.assertIn("/ssh:user@example.test:/tmp/journey/", program)

    def test_protocol_rejects_unstructured_stdout(self):
        records, error = gate.parse_protocol(
            'TRAMP_JOURNEY\tvisit.contents\t"saved\\n"\nnoise\n'
        )
        self.assertEqual(records, {"visit.contents": ['"saved\\n"']})
        self.assertIn("unexpected stdout", error)

    def test_record_mismatches_preserve_exact_values(self):
        self.assertEqual(
            gate.record_mismatches(
                {"process.async": ['"one"']},
                {"process.async": ['"two"']},
            ),
            [
                {
                    "key": "process.async",
                    "gnu": ['"one"'],
                    "emaxx": ['"two"'],
                }
            ],
        )

    def test_semantic_assertions_reject_equal_but_false_records(self):
        records = dict(gate.EXACT_RECORDS)
        records.update({key: ["t"] for key in gate.TRUE_RECORDS})
        records.update(
            {
                "directory.entries": ['("visited.txt")'],
                "temp.remote": ['"/mock:host:"'],
                "visit.remote": ['"/mock:host:"'],
            }
        )
        self.assertEqual(gate.semantic_failures(records), [])
        records["connection.reused"] = ["nil"]
        self.assertIn("connection.reused expected [t]", gate.semantic_failures(records)[0])

    def test_diagnostics_allow_only_blank_progress_and_completion(self):
        self.assertIsNone(gate.diagnostic_error("\n\nCompilation finished\n"))
        self.assertIn(
            "warning: broken",
            gate.diagnostic_error("\nCompilation finished\nwarning: broken\n"),
        )

    def test_timeout_output_is_decoded_for_protocol_reporting(self):
        self.assertEqual(gate.output_text(b"partial\xff"), "partial\ufffd")
        self.assertEqual(gate.output_text(None), "")

    def test_oracle_protocol_failure_prevents_subject_start(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "emacs"
            (source / "lisp").mkdir(parents=True)
            oracle = root / "oracle"
            oracle.write_text("#!/bin/sh\nprintf 'noise\\n'\n", encoding="utf-8")
            oracle.chmod(0o755)
            subject_marker = root / "subject-ran"
            subject = root / "subject"
            subject.write_text(
                "#!/bin/sh\ntouch %s\n" % subject_marker,
                encoding="utf-8",
            )
            subject.chmod(0o755)
            args = argparse.Namespace(
                timeout=10,
                live_ssh=False,
                remote_root=None,
                gnu=oracle,
                emaxx=subject,
                emacs_source=source,
                report=root / "report.json",
            )
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(gate.run_gate(args), 1)
            self.assertFalse(subject_marker.exists())


if __name__ == "__main__":
    unittest.main()
