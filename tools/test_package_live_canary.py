#!/usr/bin/env python3
"""Offline unit tests for package_live_canary.py."""

import contextlib
import io
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import package_live_canary as canary


class PackageLiveCanaryTests(unittest.TestCase):
    def test_live_access_requires_explicit_flag(self):
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            status = canary.main([])
        self.assertEqual(status, 2)
        self.assertIn("requires the explicit --live flag", stderr.getvalue())

    def test_protocol_rejects_unstructured_stdout(self):
        records, error = canary.parse_protocol(
            "CANARY\tarchive.gnu.url\thttps://example.test/\nnoise\n"
        )
        self.assertEqual(
            records, {"archive.gnu.url": ["https://example.test/"]}
        )
        self.assertIn("unexpected stdout", error)

    def test_runtime_assumptions_are_recorded_but_not_compared(self):
        records = {
            "runtime.emacs-version": ["30.2"],
            "archive.gnu.contents-sha256": ["abc"],
        }
        self.assertEqual(
            canary.comparable_records(records),
            {"archive.gnu.contents-sha256": ["abc"]},
        )

    def test_record_mismatch_preserves_both_exact_values(self):
        mismatches = canary.record_mismatches(
            {"archive.gnu.target": ["compat@1"]},
            {"archive.gnu.target": ["compat@2"]},
        )
        self.assertEqual(
            mismatches,
            [
                {
                    "key": "archive.gnu.target",
                    "gnu": ["compat@1"],
                    "emaxx": ["compat@2"],
                }
            ],
        )

    def test_default_targets_cover_each_public_archive_once(self):
        canary.validate_targets(canary.TARGETS)
        self.assertEqual(
            {target.archive for target in canary.TARGETS}, set(canary.ARCHIVES)
        )

    def test_generated_refresh_uses_package_el_without_disabling_signatures(self):
        program = canary.refresh_lisp(Path("/tmp/canary"), canary.TARGETS)
        self.assertIn("(package-refresh-contents)", program)
        self.assertIn(
            '("gnu" "https://elpa.gnu.org/packages/" compat compat "31.0.0.2")',
            program,
        )
        self.assertNotIn("'compat", program)
        self.assertIn("package-check-signature was disabled", program)
        self.assertNotIn("(setq package-check-signature", program)

    def test_generated_install_attributes_load_errors_to_the_feature(self):
        program = canary.install_lisp(Path("/tmp/canary"), canary.TARGETS)
        self.assertIn("loading installed feature %s failed", program)
        self.assertIn('"compiled." full-name ".files"', program)

    def test_installed_package_names_preserve_hyphenated_names(self):
        self.assertEqual(
            canary.installed_names(
                ["compat-31.0.0.2", "rainbow-delimiters-2.1.5", "ht-20230703.558"]
            ),
            ["compat", "rainbow-delimiters", "ht"],
        )

    def test_removal_checks_activation_and_versioned_directories(self):
        program = canary.restart_remove_lisp(
            Path("/tmp/canary"),
            canary.TARGETS,
            ["compat-31.0.0.2", "rainbow-delimiters-2.1.5"],
        )
        self.assertIn("'(compat rainbow-delimiters)", program)
        self.assertIn('"compat-31.0.0.2"', program)
        self.assertIn("package directory remained after removal", program)

    def test_missing_compiled_file_is_a_behavior_mismatch(self):
        mismatches = canary.record_mismatches(
            {"compiled.dash-1.files": ["dash.elc"]},
            {"compiled.dash-1.files": [""]},
        )
        self.assertEqual(mismatches[0]["key"], "compiled.dash-1.files")

    def test_gnu_success_and_emaxx_failure_is_behavior_failure(self):
        good = canary.PhaseResult("gnu", "install", 0, 1.0, "", "", {}, None)
        bad = canary.PhaseResult(
            "emaxx", "install", 70, 1.0, "", "tls failed", {}, "tls failed"
        )
        self.assertEqual(
            canary.phase_failure_classification(good, bad)[0], "emaxx_behavior"
        )


if __name__ == "__main__":
    unittest.main()
