#!/usr/bin/env python3
"""Mechanical audit of one round's changes, run before every commit.

It reads `git diff BASE` (the working tree against BASE, HEAD by default)
and reports, for the lines the round adds or removes:

HARD findings (exit status 1), which the round must remove or justify in
its record before it is committed:
  H1  runtime code that spawns or names a GNU Emacs executable;
  H2  runtime code that names an ERT test from the frozen inventory;
  H3  runtime code that names a test file (`*-tests.el');
  H4  a test marked #[ignore] or #[should_panic], or compiled out
      (#[cfg(any())], #[cfg(not(test))] in a test file);
  H5  a lint suppression (#[allow(...)], #[expect(...)], either inside
      cfg_attr, their crate-level forms, or an `allow' or `-A' in
      Cargo.toml's or .cargo's lint and flag settings);
  H6  more #[test] functions or assertions removed than added, a deleted
      file's lines included (assertions: assert!, assert_eq!, assert_ne!,
      the comparison helpers such as assert_same, and should_*);
  H7  a trivially true assertion (assert!(true), assert_eq!(x, x)).

REVIEW findings (listed, not fatal), each to be read by the round's
adversarial review:
  R1  runtime process spawning (Command, exec*, posix_spawn);
  R2  runtime lines that drop a Result or error (`let _ =', `.ok();',
      `unwrap_or_default()', `catch_unwind', `.unwrap_or(Value::Nil)');
  R3  runtime lines with fallback/stub/fabricate/canned/hack wording;
  R4  new process-wide or interpreter-wide state (statics, thread
      locals, caches, registries, side maps);
  R5  runtime lines that mention the oracle;
  R6  changed test-selection, comparison or reporting infrastructure
      (gate scripts, compat manifests and expectations, workflows, cargo
      configuration and the build script, the compatibility and
      performance harnesses, src/compat.rs and src/perf.rs among them) or
      the audit itself (the anti-cheat checks, this script, its controls,
      its rules);
  R7  runtime code that embeds a file's contents (include_str!,
      include_bytes!);
  R8  every assertion line removed, which a changed (narrowed) assertion
      balances in H6's counts.

A name the scan does not see whole (a test name or file name split, or
built at run time) escapes H1-H3: those are the review's to find.

Runtime code is everything under src/ except binaries, test modules and
the anti-cheat checks themselves.  The audit is a tripwire for the
review, not a substitute for it: a clean report certifies nothing.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

PATHS = [
    "src",
    "tests",
    "tools",
    "compat",
    ".github",
    "build.rs",
    "Cargo.toml",
    ".cargo",
    "docs/round-audit.md",
]

TEST_PATH = re.compile(
    r"(^tests/|/tests?\.rs$|/tests/|_tests?\.rs$|/compat_runtime_tests/|/test_support)"
)
HARNESS_PATH = re.compile(
    r"(^src/bin/|^tools/|^compat/|^\.github/|^src/compat\.rs$|^src/perf\.rs$)"
)
INFRA_PATH = re.compile(
    r"(^tools/(serial_)?grouped_gate\.py$|^compat/|^\.github/workflows/|^\.cargo/"
    r"|^Cargo\.toml$|^build\.rs$|^src/compat\.rs$|^src/perf\.rs$"
    r"|^src/bin/compat-harness\.rs$|^src/bin/perf-harness\.rs$"
    r"|^src/anti_cheat\.rs$|^tools/(test_)?round_audit\.py$|^docs/round-audit\.md$)"
)
CARGO_CONFIG_PATH = re.compile(r"(^Cargo\.toml$|^\.cargo/)")

ORACLE_SPAWN = re.compile(
    r"""(Command::new\(\s*"(emacs|/usr/(local/)?bin/emacs)"|emaxx-work/emacs/src/emacs|/usr/(local/)?bin/emacs\b|EMAXX_ORACLE|ORACLE_EMACS)"""
)
TEST_FILE = re.compile(r"[A-Za-z0-9_+-]+-tests?\.el\b")
IGNORE = re.compile(r"#\[\s*(ignore|should_panic)\b|#\[\s*cfg\s*\(\s*any\s*\(\s*\)\s*\)")
CFG_NOT_TEST = re.compile(r"#\[\s*cfg\s*\(\s*not\s*\(\s*test\s*\)\s*\)")
ALLOW = re.compile(r"#!?\[\s*(allow|expect)\s*\(|cfg_attr\s*\(.*\b(allow|expect)\s*\(")
CARGO_ALLOW = re.compile(r"(=\s*[\"']allow[\"']|level\s*=\s*[\"']allow|[\"']-A\b|\s-A\s|--cap-lints)")
TEST_FN = re.compile(r"#\[\s*test\s*\]")
ASSERT = re.compile(
    r"\b(assert(_eq|_ne)?!|debug_assert(_eq|_ne)?!|assert_matches!|assert_[a-z_]+\(|\(should\b)"
)
TRIVIAL_ASSERT = re.compile(r"\bassert!\s*\(\s*true\s*[,)]|\bassert_eq!\s*\(\s*([^,()]+?)\s*,\s*\1\s*[,)]")
SPAWN = re.compile(
    r"(std::process::Command|Command::new\(|process::Command|libc::exec\w*\(|\bexecv\w*\(|posix_spawn\w*\()"
)
EMBED = re.compile(r"\binclude_(str|bytes)!\s*\(")
SWALLOW = re.compile(
    r"(\blet\s+_\s*=|\.ok\(\)\s*;|unwrap_or_default\(\)|catch_unwind|\.unwrap_or\(Value::Nil\))"
)
WORDING = re.compile(
    r"\b(fallback|fall back|stub|fabricat\w*|canned|hack\w*|pretend\w*|special[- ]case\w*|workaround)\b",
    re.IGNORECASE,
)
STATE = re.compile(
    r"(\bthread_local!|\bstatic\s+mut\b|\bstatic\s+[A-Z_]+\s*:|OnceLock<|OnceCell<|LazyLock<|\b\w*(cache|registry|mirror|shadow)\w*\s*:\s*(Rc<|RefCell<|HashMap<|BTreeMap<|Vec<|Cell<))",
    re.IGNORECASE,
)
ORACLE_WORD = re.compile(r"\boracle\b", re.IGNORECASE)


@dataclass
class Report:
    hard: list[str] = field(default_factory=list)
    review: dict[str, list[str]] = field(default_factory=dict)

    def add_review(self, rule: str, entry: str) -> None:
        self.review.setdefault(rule, []).append(entry)


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def is_runtime(path: str) -> bool:
    return (
        path.startswith("src/")
        and not TEST_PATH.search(path)
        and not HARNESS_PATH.search(path)
        and path != "src/anti_cheat.rs"
    )


def is_test(path: str) -> bool:
    return bool(TEST_PATH.search(path))


def load_test_names(repo: Path) -> set[str]:
    names: set[str] = set()
    inventory = repo / "compat" / "oracle_tests_all.txt"
    if inventory.exists():
        for line in inventory.read_text(errors="replace").splitlines():
            for token in re.split(r"[\s:,]+", line.strip()):
                # Long, distinctive names only: short ones collide with
                # ordinary identifiers.
                if len(token) >= 16 and "-" in token and not token.endswith(".el"):
                    names.add(token)
    return names


def diff_hunks(repo: Path, base: str) -> list[tuple[str, str, int, str]]:
    """(path, sign, line number in the new file or 0, text) for each
    added or removed line."""
    text = git(repo, "diff", "--unified=0", "--no-color", base, "--", *PATHS)
    out: list[tuple[str, str, int, str]] = []
    path = ""
    old_path = ""
    new_line = 0
    for line in text.splitlines():
        if line.startswith("--- "):
            old_path = line[6:] if line.startswith("--- a/") else ""
        elif line.startswith("+++ "):
            # A deleted file's removed lines count under its old name.
            path = line[6:] if line.startswith("+++ b/") else old_path
        elif line.startswith("@@"):
            match = re.search(r"\+(\d+)", line)
            new_line = int(match.group(1)) if match else 0
        elif line.startswith("+") and not line.startswith("+++"):
            out.append((path, "+", new_line, line[1:]))
            new_line += 1
        elif line.startswith("-") and not line.startswith("---"):
            out.append((path, "-", 0, line[1:]))
    # Untracked files count as wholly added.
    for untracked in git(repo, "ls-files", "--others", "--exclude-standard", "--", *PATHS).split():
        file = repo / untracked
        if file.is_file():
            for number, content in enumerate(file.read_text(errors="replace").splitlines(), 1):
                out.append((untracked, "+", number, content))
    return out


def audit(repo: Path, base: str) -> Report:
    report = Report()
    names = load_test_names(repo)
    lines = diff_hunks(repo, base)
    added_tests = removed_tests = added_asserts = removed_asserts = 0
    infra: set[str] = set()
    for path, sign, number, text in lines:
        where = f"{path}:{number}" if number else path
        stripped = text.strip()
        if INFRA_PATH.search(path):
            infra.add(path)
        if path.endswith(".rs") and (is_test(path) or path.startswith("src/")):
            if TEST_FN.search(text):
                if sign == "+":
                    added_tests += 1
                else:
                    removed_tests += 1
            # Assertions anywhere in the code, inline test modules and the
            # runtime's own checks included.
            if ASSERT.search(text):
                if sign == "+":
                    added_asserts += 1
                else:
                    removed_asserts += 1
                    # A changed assertion (a narrowed comparison) balances
                    # the counts; each removed one is read in review.
                    report.add_review("R8 assertion removed or changed", f"{where}: {stripped}")
        if sign != "+":
            continue
        if path.endswith(".rs") and ALLOW.search(text):
            report.hard.append(f"H5 lint suppression at {where}: {stripped}")
        if CARGO_CONFIG_PATH.search(path) and CARGO_ALLOW.search(text):
            report.hard.append(f"H5 lint suppression in cargo configuration at {where}: {stripped}")
        if path.endswith(".rs") and (
            IGNORE.search(text) or (is_test(path) and CFG_NOT_TEST.search(text))
        ):
            report.hard.append(f"H4 ignored, should_panic or compiled-out test at {where}: {stripped}")
        if path.endswith(".rs") and TRIVIAL_ASSERT.search(text):
            report.hard.append(f"H7 trivially true assertion at {where}: {stripped}")
        if not is_runtime(path):
            continue
        if ORACLE_SPAWN.search(text):
            report.hard.append(f"H1 GNU Emacs executable in runtime code at {where}: {stripped}")
        if TEST_FILE.search(text):
            report.hard.append(f"H3 test file named in runtime code at {where}: {stripped}")
        if names:
            for token in re.findall(r"[A-Za-z0-9][A-Za-z0-9_+/-]{15,}", text):
                if token in names:
                    report.hard.append(f"H2 ERT test `{token}' named in runtime code at {where}")
        if SPAWN.search(text):
            report.add_review("R1 process spawning", f"{where}: {stripped}")
        if SWALLOW.search(text):
            report.add_review("R2 dropped result or error", f"{where}: {stripped}")
        if WORDING.search(text):
            report.add_review("R3 fallback/stub wording", f"{where}: {stripped}")
        if STATE.search(text):
            report.add_review("R4 new process or interpreter state", f"{where}: {stripped}")
        if ORACLE_WORD.search(text):
            report.add_review("R5 oracle mentioned", f"{where}: {stripped}")
        if EMBED.search(text):
            report.add_review("R7 embedded file contents", f"{where}: {stripped}")
    if removed_tests > added_tests:
        report.hard.append(
            f"H6 {removed_tests} #[test] functions removed against {added_tests} added"
        )
    if removed_asserts > added_asserts:
        report.hard.append(
            f"H6 {removed_asserts} test assertions removed against {added_asserts} added"
        )
    for path in sorted(infra):
        report.add_review("R6 test, comparison or reporting infrastructure changed", path)
    report.review.setdefault("summary", []).append(
        f"{len(lines)} changed lines; tests +{added_tests}/-{removed_tests}; "
        f"assertions +{added_asserts}/-{removed_asserts}"
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repo", default=".", type=Path)
    parser.add_argument("--base", default="HEAD", help="revision the round starts from")
    args = parser.parse_args()
    report = audit(args.repo.resolve(), args.base)
    print(f"# Round audit against {args.base}\n")
    print(f"HARD findings: {len(report.hard)}")
    for entry in report.hard:
        print(f"  - {entry}")
    for rule, entries in report.review.items():
        print(f"\n{rule}: {len(entries)}")
        for entry in entries[:40]:
            print(f"  - {entry}")
        if len(entries) > 40:
            print(f"  ... {len(entries) - 40} more")
    return 1 if report.hard else 0


if __name__ == "__main__":
    sys.exit(main())
