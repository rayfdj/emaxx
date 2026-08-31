#!/usr/bin/env python3
"""Regression and anti-cheat tests for the pinned lsp-mode package gate."""

import hashlib
from pathlib import Path
import re
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parent))

from lsp_mode_package_gate import (
    ARCHIVE_CONTENTS,
    ARTIFACTS,
    EXPECTED_COMPILED,
    EXPECTED_INSTALLED,
    LSP_MODE_TTY_SCENARIOS,
    Artifact,
    install_lisp,
    restart_lisp,
    verify_artifact,
)


class LspModePackageGateTests(unittest.TestCase):
    def test_manifest_is_exact_unique_and_https(self) -> None:
        filenames = [artifact.filename for artifact in ARTIFACTS]
        self.assertEqual(len(filenames), len(set(filenames)))
        self.assertEqual(len(ARTIFACTS), 8)
        self.assertIn("lsp-mode-10.0.0.tar", filenames)
        for artifact in ARTIFACTS:
            self.assertTrue(
                artifact.url.startswith(
                    ("https://stable.melpa.org/", "https://elpa.gnu.org/")
                )
            )
            self.assertEqual(len(artifact.sha256), 64)
            int(artifact.sha256, 16)
            package = re.sub(r"-[0-9].*\Z", "", artifact.filename)
            self.assertIn(" (%s . [" % package, ARCHIVE_CONTENTS)

    def test_expected_external_closure_and_compiled_inventory_are_fixed(self) -> None:
        self.assertEqual(
            EXPECTED_INSTALLED,
            (
                "dash-2.20.0",
                "f-0.21.0",
                "ht-2.3",
                "lsp-mode-10.0.0",
                "lv-0.15.0",
                "markdown-mode-2.8",
                "s-1.13.1",
                "spinner-1.7.4",
            ),
        )
        self.assertEqual(len(EXPECTED_COMPILED), 159)
        self.assertEqual(len(EXPECTED_COMPILED), len(set(EXPECTED_COMPILED)))
        self.assertEqual(tuple(sorted(EXPECTED_COMPILED)), EXPECTED_COMPILED)
        self.assertTrue(all(name.endswith(".elc") for name in EXPECTED_COMPILED))
        self.assertEqual(
            {name.split("/", 1)[0] for name in EXPECTED_COMPILED},
            set(EXPECTED_INSTALLED),
        )

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

    def test_both_phases_use_real_package_el_without_editor_branches(self) -> None:
        root = Path("/tmp/lsp-mode-gate-root")
        archive = Path("/tmp/lsp-mode-gate-archive")
        install = install_lisp(root, archive)
        restart = restart_lisp(root, archive)
        self.assertIn("(package-refresh-contents)", install)
        self.assertIn("(package-install desc)", install)
        self.assertIn("(package-initialize)", restart)
        self.assertIn("(require 'lsp-mode)", restart)
        self.assertIn("file-in-directory-p path package-user-dir", restart)
        for generated in (install, restart):
            self.assertNotIn("emaxx", generated.lower())
            self.assertNotIn("system-type", generated)
            self.assertNotIn("executable-find", generated)
            self.assertNotIn("expected-output", generated)

    def test_tty_gate_owns_all_required_command_families(self) -> None:
        self.assertEqual(
            LSP_MODE_TTY_SCENARIOS,
            (
                "lsp-mode-connect-diagnostics-completion-hover",
                "lsp-mode-xref-rename-edits",
                "lsp-mode-reconnect-shutdown",
                "lsp-mode-ui-buffers",
            ),
        )


if __name__ == "__main__":
    unittest.main()
