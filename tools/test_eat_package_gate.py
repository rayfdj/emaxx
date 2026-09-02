#!/usr/bin/env python3
"""Regression and anti-cheat tests for the pinned Eat package gate."""

import hashlib
from pathlib import Path
import re
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parent))

from eat_package_gate import (
    ARCHIVE_CONTENTS,
    ARTIFACTS,
    EXPECTED_COMPILED,
    EXPECTED_INSTALLED,
    EXPECTED_PROCESS_RECORDS,
    EXPECTED_TRANSACTION,
    Artifact,
    install_lisp,
    restart_lisp,
    verify_artifact,
)


class EatPackageGateTests(unittest.TestCase):
    def test_manifest_is_exact_unique_and_https(self) -> None:
        filenames = [artifact.filename for artifact in ARTIFACTS]
        self.assertEqual(len(filenames), len(set(filenames)))
        self.assertEqual(
            filenames,
            [
                "compat-31.0.0.2.tar",
                "eat-0.9.4.tar",
                "eat-v0.9.4-source.tar.gz",
            ],
        )
        for artifact in ARTIFACTS:
            self.assertTrue(artifact.url.startswith("https://"))
            self.assertEqual(len(artifact.sha256), 64)
            int(artifact.sha256, 16)
        self.assertEqual(sum(item.package_archive for item in ARTIFACTS), 2)
        self.assertIn(" (compat . [", ARCHIVE_CONTENTS)
        self.assertIn(" (eat . [", ARCHIVE_CONTENTS)

    def test_expected_install_and_process_inventories_are_fixed(self) -> None:
        self.assertEqual(EXPECTED_INSTALLED, ("eat-0.9.4",))
        self.assertEqual(EXPECTED_TRANSACTION, ("eat-0.9.4",))
        self.assertEqual(len(EXPECTED_COMPILED), 2)
        self.assertEqual(tuple(sorted(EXPECTED_COMPILED)), EXPECTED_COMPILED)
        self.assertEqual(len(EXPECTED_COMPILED), len(set(EXPECTED_COMPILED)))
        self.assertEqual(
            set(EXPECTED_PROCESS_RECORDS), {"deterministic", "signal", "shell"}
        )
        self.assertIn("200 t t t", EXPECTED_PROCESS_RECORDS["deterministic"])
        self.assertEqual(EXPECTED_PROCESS_RECORDS["signal"], "(signal 2 t t t t)")

    def test_cached_artifact_is_rehashed_before_use(self) -> None:
        payload = b"pinned Eat package payload\n"
        artifact = Artifact(
            "fixture.tar",
            "https://example.invalid/fixture.tar",
            hashlib.sha256(payload).hexdigest(),
            True,
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / artifact.filename
            path.write_bytes(payload)
            verify_artifact(path, artifact)
            path.write_bytes(payload + b"tampered")
            with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
                verify_artifact(path, artifact)

    def test_package_phases_use_real_package_el_without_editor_branches(self) -> None:
        root = Path("/tmp/eat-gate-root")
        archive = Path("/tmp/eat-gate-archive")
        tests = Path("/tmp/eat-tests.el")
        install = install_lisp(root, archive)
        restart = restart_lisp(root, archive, tests)
        self.assertIn("(package-refresh-contents)", install)
        self.assertIn("(package-install desc)", install)
        self.assertIn('(eat-gate-emit "emacs-version" emacs-version)', install)
        self.assertIn("(require 'eat)", restart)
        self.assertIn("(ert-run-tests-batch \"^eat-test-\")", restart)
        self.assertIn("file-in-directory-p origin package-user-dir", restart)
        for generated in (install, restart):
            self.assertNotIn("system-type", generated)
            self.assertNotIn("executable-find", generated)
            self.assertNotRegex(generated, re.compile(r"\b(if|cond)\b.*emaxx", re.I))

    def test_shared_process_gate_has_no_editor_or_fixture_dispatch(self) -> None:
        source = (Path(__file__).parent / "eat_process_gate.el").read_text(
            encoding="utf-8"
        )
        for required in (
            "(eat-make",
            "(eat-term-input-event",
            "(set-process-window-size",
            '"/bin/sh" nil "-i"',
            '"exit 3\\n"',
            "ROW:%03d",
        ):
            self.assertIn(required, source)
        for forbidden in (
            "system-type",
            "executable-find",
            "call-process",
            "start-process",
            "expected-output",
        ):
            self.assertNotIn(forbidden, source)
        self.assertNotRegex(source, re.compile(r"\b(if|cond)\b.*emaxx", re.I))


if __name__ == "__main__":
    unittest.main()
