#!/usr/bin/env python3
"""Regression and anti-cheat tests for the pinned Flycheck package gate."""

import hashlib
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parent))

from flycheck_package_gate import (
    ARCHIVE_CONTENTS,
    ARTIFACTS,
    EXPECTED_RUNTIME_COMPILED,
    EXPECTED_RUNTIME_INSTALLED,
    EXPECTED_SPEC_COMPILED,
    EXPECTED_SPEC_INSTALLED,
    EXPECTED_UPSTREAM_SPEC_COUNT,
    FLYCHECK_TTY_SCENARIOS,
    PACKAGE_ARTIFACTS,
    SOURCE_ARTIFACT,
    UPSTREAM_SPEC_FILES,
    Artifact,
    install_lisp,
    restart_lisp,
    upstream_lisp,
    verify_artifact,
)
import ttydiff


class FlycheckPackageGateTests(unittest.TestCase):
    def test_manifest_is_exact_unique_https_and_hash_pinned(self) -> None:
        filenames = [artifact.filename for artifact in ARTIFACTS]
        self.assertEqual(len(filenames), len(set(filenames)))
        self.assertEqual(len(ARTIFACTS), 4)
        self.assertEqual(
            {artifact.filename for artifact in PACKAGE_ARTIFACTS},
            {"buttercup-1.40.tar", "flycheck-39.0.tar", "seq-2.24.tar"},
        )
        self.assertEqual(SOURCE_ARTIFACT.filename, "flycheck-v39.0-source.tar.gz")
        for artifact in ARTIFACTS:
            self.assertTrue(artifact.url.startswith("https://"))
            self.assertEqual(len(artifact.sha256), 64)
            int(artifact.sha256, 16)
        for package in ("buttercup", "flycheck", "seq"):
            self.assertIn(" (%s . [" % package, ARCHIVE_CONTENTS)
        self.assertNotIn(SOURCE_ARTIFACT.filename, ARCHIVE_CONTENTS)

    def test_expected_closures_and_compiled_inventories_are_fixed(self) -> None:
        self.assertEqual(EXPECTED_RUNTIME_INSTALLED, ("flycheck-39.0",))
        self.assertEqual(EXPECTED_RUNTIME_COMPILED, ("flycheck-39.0/flycheck.elc",))
        self.assertEqual(
            EXPECTED_SPEC_INSTALLED,
            ("buttercup-1.40", "flycheck-39.0"),
        )
        self.assertEqual(
            EXPECTED_SPEC_COMPILED,
            (
                "buttercup-1.40/buttercup-compat.elc",
                "buttercup-1.40/buttercup.elc",
                "flycheck-39.0/flycheck.elc",
            ),
        )
        self.assertEqual(tuple(sorted(EXPECTED_SPEC_COMPILED)), EXPECTED_SPEC_COMPILED)
        self.assertEqual(len(EXPECTED_SPEC_COMPILED), len(set(EXPECTED_SPEC_COMPILED)))

    def test_cached_artifact_is_rehashed_before_use(self) -> None:
        payload = b"pinned package payload\n"
        artifact = Artifact(
            "fixture.tar",
            "https://stable.melpa.org/packages/fixture.tar",
            hashlib.sha256(payload).hexdigest(),
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / artifact.filename
            path.write_bytes(payload)
            verify_artifact(path, artifact)
            path.write_bytes(payload + b"tampered")
            with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
                verify_artifact(path, artifact)

    def test_package_and_spec_phases_have_no_editor_branch(self) -> None:
        root = Path("/tmp/flycheck-gate-root")
        archive = Path("/tmp/flycheck-gate-archive")
        source = Path("/tmp/flycheck-gate-source")
        install = install_lisp(root, archive)
        restart = restart_lisp(root, archive)
        upstream = upstream_lisp(root, archive, source)
        self.assertIn("(package-refresh-contents)", install)
        self.assertIn("(package-install desc)", install)
        self.assertIn("(require 'flycheck)", restart)
        self.assertIn("(require 'buttercup)", upstream)
        self.assertIn("(buttercup-run t)", upstream)
        for filename in UPSTREAM_SPEC_FILES:
            self.assertIn(filename, upstream)
        for generated in (install, restart, upstream):
            self.assertNotIn("emaxx", generated.lower())
            self.assertNotIn("system-type", generated)
            self.assertNotIn("expected-output", generated)

    def test_upstream_subset_and_tty_families_are_fixed(self) -> None:
        self.assertEqual(EXPECTED_UPSTREAM_SPEC_COUNT, 40)
        self.assertEqual(
            UPSTREAM_SPEC_FILES,
            (
                "test-error-filters.el",
                "test-error-parsers.el",
                "test-mode-line.el",
            ),
        )
        self.assertEqual(
            FLYCHECK_TTY_SCENARIOS,
            (
                "flycheck-diagnostics-navigation",
                "flycheck-clean-idle-teardown",
                "flycheck-malformed-missing-tool",
                "flycheck-cancellation",
            ),
        )
        scenarios = {entry[0]: entry for entry in ttydiff.SCENARIOS}
        self.assertEqual(ttydiff.FLYCHECK_SCENARIO_NAMES, FLYCHECK_TTY_SCENARIOS)
        for name in FLYCHECK_TTY_SCENARIOS:
            self.assertIn(name, scenarios)
            self.assertTrue(scenarios[name][4]["flycheck_package_root"])
            self.assertGreaterEqual(len(scenarios[name][2]), 5)

        diagnostic_keys = b"".join(
            action.keys
            for action in scenarios["flycheck-diagnostics-navigation"][2]
        )
        for keys in (b"\x03!c", b"\x03!l", b"\x03!n", b"\x03!p"):
            self.assertIn(keys, diagnostic_keys)
        self.assertIn(
            b"flycheck--idle-trigger-timer",
            b"".join(
                action.keys
                for action in scenarios["flycheck-clean-idle-teardown"][2]
            ),
        )
        self.assertIn(
            b"ttydiff-missing",
            b"".join(
                action.keys
                for action in scenarios["flycheck-malformed-missing-tool"][2]
            ),
        )
        self.assertIn(
            b"flycheck-stop",
            b"".join(
                action.keys for action in scenarios["flycheck-cancellation"][2]
            ),
        )

    def test_fixture_checker_outputs_only_source_derived_diagnostics(self) -> None:
        checker = Path(__file__).with_name("flycheck_fixture_checker.py")
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "sample.txt"
            source.write_text("plain\nWARN here\nERROR now\n", encoding="utf-8")
            content = subprocess.run(
                [sys.executable, str(checker), "content", str(source)],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(content.returncode, 1)
            self.assertEqual(
                content.stdout,
                "2:1: warning W200: deterministic warning\n"
                "3:1: error E300: deterministic error\n",
            )
            clean = subprocess.run(
                [sys.executable, str(checker), "clean", str(source)],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual((clean.returncode, clean.stdout, clean.stderr), (0, "", ""))
            malformed = subprocess.run(
                [sys.executable, str(checker), "malformed", str(source)],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(malformed.returncode, 1)
            self.assertEqual(
                malformed.stdout,
                "this output deliberately has no location or severity\n",
            )


if __name__ == "__main__":
    unittest.main()
