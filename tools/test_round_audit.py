#!/usr/bin/env python3
"""Negative controls for the round audit: planted violations must be
reported, ordinary changes must not."""

from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parent))

from round_audit import audit


def git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        env={
            "GIT_AUTHOR_NAME": "audit",
            "GIT_AUTHOR_EMAIL": "audit@example.invalid",
            "GIT_COMMITTER_NAME": "audit",
            "GIT_COMMITTER_EMAIL": "audit@example.invalid",
            "PATH": "/usr/bin:/bin",
        },
    )


class RoundAuditTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self.tmp.name)
        git(self.repo, "init", "-q")
        (self.repo / "compat").mkdir()
        (self.repo / "compat" / "oracle_tests_all.txt").write_text(
            "test/src/fns-tests.el fns-tests-sort-stability-check\n"
        )
        (self.repo / "src" / "lisp").mkdir(parents=True)
        (self.repo / "src" / "lisp" / "eval.rs").write_text("fn eval() {}\n")
        (self.repo / "src" / "lisp" / "tests.rs").write_text(
            "#[test]\nfn keeps() {\n    assert_eq!(1, 1);\n}\n"
        )
        git(self.repo, "add", "-A")
        git(self.repo, "commit", "-q", "-m", "base")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def write(self, path: str, text: str) -> None:
        (self.repo / path).write_text(text)

    def hard(self) -> list[str]:
        return audit(self.repo, "HEAD").hard

    def test_an_ordinary_change_is_clean(self) -> None:
        self.write("src/lisp/eval.rs", "fn eval() {\n    let x = 1;\n}\n")
        self.assertEqual(self.hard(), [])

    def test_a_test_name_in_runtime_code_is_hard(self) -> None:
        self.write(
            "src/lisp/eval.rs",
            'fn eval(name: &str) -> bool {\n    name == "fns-tests-sort-stability-check"\n}\n',
        )
        self.assertTrue(any(entry.startswith("H2") for entry in self.hard()))

    def test_a_test_file_in_runtime_code_is_hard(self) -> None:
        self.write("src/lisp/eval.rs", 'const F: &str = "fns-tests.el";\n')
        self.assertTrue(any(entry.startswith("H3") for entry in self.hard()))

    def test_an_oracle_executable_in_runtime_code_is_hard(self) -> None:
        self.write(
            "src/lisp/eval.rs",
            'fn eval() { std::process::Command::new("emacs"); }\n',
        )
        self.assertTrue(any(entry.startswith("H1") for entry in self.hard()))

    def test_an_ignored_test_is_hard(self) -> None:
        self.write(
            "src/lisp/tests.rs",
            "#[test]\n#[ignore]\nfn keeps() {\n    assert_eq!(1, 1);\n}\n",
        )
        self.assertTrue(any(entry.startswith("H4") for entry in self.hard()))

    def test_a_lint_suppression_is_hard(self) -> None:
        self.write("src/lisp/eval.rs", "#[allow(dead_code)]\nfn eval() {}\n")
        self.assertTrue(any(entry.startswith("H5") for entry in self.hard()))

    def test_a_removed_assertion_is_hard(self) -> None:
        self.write("src/lisp/tests.rs", "#[test]\nfn keeps() {\n}\n")
        self.assertTrue(any(entry.startswith("H6") for entry in self.hard()))

    def test_a_removed_test_is_hard(self) -> None:
        self.write("src/lisp/tests.rs", "\n")
        self.assertTrue(any(entry.startswith("H6") for entry in self.hard()))

    def test_an_untracked_runtime_file_is_read(self) -> None:
        self.write("src/lisp/new.rs", 'const F: &str = "fns-tests.el";\n')
        self.assertTrue(any(entry.startswith("H3") for entry in self.hard()))

    def test_a_deleted_test_file_is_hard(self) -> None:
        (self.repo / "src" / "lisp" / "tests.rs").unlink()
        self.assertTrue(any(entry.startswith("H6") for entry in self.hard()))

    def test_an_expect_or_cfg_attr_suppression_is_hard(self) -> None:
        for text in (
            "#[expect(dead_code)]\nfn eval() {}\n",
            "#[cfg_attr(test, allow(dead_code))]\nfn eval() {}\n",
            "#![expect(unused)]\nfn eval() {}\n",
        ):
            self.write("src/lisp/eval.rs", text)
            self.assertTrue(any(entry.startswith("H5") for entry in self.hard()), text)

    def test_a_cargo_lint_allowance_is_hard(self) -> None:
        self.write("Cargo.toml", "[lints.rust]\nwarnings = 'allow'\n")
        self.assertTrue(any(entry.startswith("H5") for entry in self.hard()))

    def test_a_compiled_out_test_is_hard(self) -> None:
        self.write(
            "src/lisp/tests.rs",
            "#[cfg(any())]\n#[test]\nfn keeps() {\n    assert_eq!(1, 1);\n}\n",
        )
        self.assertTrue(any(entry.startswith("H4") for entry in self.hard()))

    def test_a_trivially_true_assertion_is_hard(self) -> None:
        self.write("src/lisp/tests.rs", "#[test]\nfn keeps() {\n    assert!(true);\n}\n")
        self.assertTrue(any(entry.startswith("H7") for entry in self.hard()))

    def test_a_removed_comparison_is_hard(self) -> None:
        (self.repo / "tests").mkdir()
        self.write("tests/cli.rs", "#[test]\nfn c() {\n    assert_same(&corpus, &[\"x\"]);\n}\n")
        git(self.repo, "add", "-A")
        git(self.repo, "commit", "-q", "-m", "comparison")
        self.write("tests/cli.rs", "#[test]\nfn c() {\n}\n")
        self.assertTrue(any(entry.startswith("H6") for entry in self.hard()))

    def test_a_narrowed_comparison_is_listed(self) -> None:
        self.write(
            "src/lisp/tests.rs",
            "#[test]\nfn keeps() {\n    assert_eq!(1, 1 + 0 * 2);\n}\n",
        )
        review = audit(self.repo, "HEAD").review
        self.assertIn("R8 assertion removed or changed", review)

    def test_harness_code_in_the_library_is_listed(self) -> None:
        self.write("src/compat.rs", 'fn run() { std::process::Command::new("emacs"); }\n')
        review = audit(self.repo, "HEAD").review
        self.assertIn(
            "src/compat.rs",
            review["R6 test, comparison or reporting infrastructure changed"],
        )

    def test_exec_and_embedded_files_are_listed(self) -> None:
        self.write(
            "src/lisp/eval.rs",
            'fn eval() { unsafe { libc::execvp(p, a) }; let t = include_str!("x"); }\n',
        )
        review = audit(self.repo, "HEAD").review
        self.assertIn("R1 process spawning", review)
        self.assertIn("R7 embedded file contents", review)

    def test_a_change_to_the_audit_itself_is_listed(self) -> None:
        (self.repo / "tools").mkdir()
        self.write("tools/round_audit.py", "print('weakened')\n")
        review = audit(self.repo, "HEAD").review
        self.assertIn(
            "tools/round_audit.py",
            review["R6 test, comparison or reporting infrastructure changed"],
        )

    def test_review_findings_are_listed(self) -> None:
        self.write(
            "src/lisp/eval.rs",
            "fn eval() {\n    let _ = std::fs::remove_file(\"x\"); // fallback\n}\n",
        )
        review = audit(self.repo, "HEAD").review
        self.assertIn("R2 dropped result or error", review)
        self.assertIn("R3 fallback/stub wording", review)


if __name__ == "__main__":
    unittest.main()
