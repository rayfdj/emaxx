#!/usr/bin/env python3
"""Regression and anti-cheat tests for the completion-stack package gate."""

import hashlib
from pathlib import Path
import re
import sys
import tarfile
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parent))

from completion_stack_package_gate import (
    ARCHIVE_CONTENTS,
    ARTIFACTS,
    COMPLETION_STACK_TTY_SCENARIOS,
    EXPECTED_AUTOLOADS,
    EXPECTED_COMPILED,
    EXPECTED_INSTALLED,
    EXPECTED_ORIGINS,
    EXPECTED_TRANSACTION,
    Artifact,
    install_lisp,
    restart_lisp,
    source_test_inventory,
    verify_artifact,
)
from ttydiff import Action, SCENARIOS


class CompletionStackPackageGateTests(unittest.TestCase):
    def test_manifest_is_exact_unique_hash_pinned_and_https(self) -> None:
        filenames = [artifact.filename for artifact in ARTIFACTS]
        self.assertEqual(len(filenames), len(set(filenames)))
        self.assertEqual(
            filenames,
            [
                "compat-31.0.0.2.tar",
                "consult-3.7.tar",
                "corfu-2.14.tar",
                "corfu-terminal-0.7.tar",
                "popon-0.13.tar",
                "vertico-2.13.tar",
                "consult-3.7-source.tar.gz",
                "corfu-2.14-source.tar.gz",
                "corfu-terminal-0.7-source.tar.gz",
                "popon-0.13-source.tar.gz",
                "vertico-2.13-source.tar.gz",
            ],
        )
        self.assertEqual(sum(item.package_archive for item in ARTIFACTS), 6)
        self.assertEqual(sum(not item.package_archive for item in ARTIFACTS), 5)
        for artifact in ARTIFACTS:
            self.assertTrue(artifact.url.startswith("https://"))
            self.assertEqual(len(artifact.sha256), 64)
            int(artifact.sha256, 16)
        for package in (
            "compat",
            "consult",
            "corfu",
            "corfu-terminal",
            "popon",
            "vertico",
        ):
            self.assertIn(" (%s . [" % package, ARCHIVE_CONTENTS)

    def test_expected_transaction_and_payload_are_fixed(self) -> None:
        expected = (
            "compat-31.0.0.2",
            "consult-3.7",
            "corfu-2.14",
            "corfu-terminal-0.7",
            "popon-0.13",
            "vertico-2.13",
        )
        self.assertEqual(EXPECTED_TRANSACTION, expected)
        self.assertEqual(EXPECTED_INSTALLED, expected)
        self.assertEqual(len(EXPECTED_COMPILED), 41)
        self.assertEqual(tuple(sorted(EXPECTED_COMPILED)), EXPECTED_COMPILED)
        self.assertEqual(len(EXPECTED_COMPILED), len(set(EXPECTED_COMPILED)))
        self.assertEqual(
            {name.split("/", 1)[0] for name in EXPECTED_COMPILED}, set(expected)
        )
        self.assertEqual(len(EXPECTED_AUTOLOADS), 6)
        self.assertEqual(
            set(EXPECTED_ORIGINS),
            {
                "origin.compat",
                "origin.consult",
                "origin.corfu",
                "origin.corfu-terminal",
                "origin.popon",
                "origin.vertico",
            },
        )

    def test_cached_artifact_is_rehashed_before_use(self) -> None:
        payload = b"pinned completion package payload\n"
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

    def test_source_inventory_fails_closed_when_upstream_ships_tests(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            payload = root / "package-test.el"
            payload.write_text("(ert-deftest package-test () t)\n", encoding="utf-8")
            archive = root / "source.tar.gz"
            with tarfile.open(archive, "w:gz") as output:
                output.add(payload, arcname="package-1.0/test/package-test.el")
            self.assertEqual(
                source_test_inventory([archive]),
                ("source.tar.gz:package-1.0/test/package-test.el",),
            )

    def test_package_phases_use_package_el_without_editor_dispatch(self) -> None:
        root = Path("/tmp/completion-stack-gate-root")
        archive = Path("/tmp/completion-stack-gate-archive")
        install = install_lisp(root, archive)
        restart = restart_lisp(root, archive)
        self.assertIn("(package-refresh-contents)", install)
        self.assertIn("(package-compute-transaction descs requirements)", install)
        self.assertIn("(package-install desc)", install)
        self.assertIn("(package-initialize)", restart)
        self.assertIn(
            "(dolist (feature '(compat vertico consult corfu popon corfu-terminal))",
            restart,
        )
        self.assertIn("(require feature)", restart)
        self.assertIn("file-in-directory-p library package-user-dir", restart)
        for generated in (install, restart):
            self.assertNotIn("system-type", generated)
            self.assertNotIn("executable-find", generated)
            self.assertNotIn("expected-output", generated)
            self.assertNotRegex(generated, re.compile(r"\b(if|cond)\b.*emaxx", re.I))

    def test_tty_gate_owns_the_four_required_interactive_journeys(self) -> None:
        self.assertEqual(
            COMPLETION_STACK_TTY_SCENARIOS,
            (
                "stack-vertico",
                "stack-consult-line",
                "stack-consult-grep",
                "stack-corfu",
            ),
        )
        source = (Path(__file__).parent / "ttydiff.py").read_text(encoding="utf-8")
        for required in (
            "open-vertico-completing-read",
            "preview-next-consult-line",
            "run-asynchronous-grep",
            "preview-next-corfu-candidate",
            "completion_stack_package_root",
        ):
            self.assertIn(required, source)
        self.assertNotRegex(
            source,
            re.compile(r"\b(if|cond)\b.*emaxx.*(?:vertico|consult|corfu)", re.I),
        )

        expected_actions = {
            "stack-vertico": (
                "load-installed-completion-stack",
                "open-vertico-completing-read",
                "vertico-next-candidate",
                "vertico-filter-candidates",
                "vertico-accept-candidate",
                "verify-vertico-result-and-cleanup",
            ),
            "stack-consult-line": (
                "load-installed-completion-stack",
                "open-consult-line",
                "filter-consult-lines",
                "preview-next-consult-line",
                "accept-consult-line",
                "verify-consult-line-result",
            ),
            "stack-consult-grep": (
                "load-installed-completion-stack",
                "open-consult-grep",
                "run-asynchronous-grep",
                "preview-next-grep-result",
                "accept-grep-result",
                "verify-grep-result",
            ),
            "stack-corfu": (
                "load-installed-completion-stack",
                "configure-corfu-capf",
                "insert-completion-prefix",
                "open-corfu-terminal-popup",
                "preview-next-corfu-candidate",
                "insert-corfu-candidate",
                "verify-corfu-result-and-cleanup",
            ),
        }
        scenarios = {entry[0]: entry for entry in SCENARIOS}
        for name, action_names in expected_actions.items():
            actions = scenarios[name][2]
            self.assertEqual(tuple(action.name for action in actions), action_names)
            self.assertTrue(all(isinstance(action, Action) for action in actions))
            self.assertTrue(all(action.checkpoint for action in actions))


if __name__ == "__main__":
    unittest.main()
