# Ordinary frozen-run success

Tracking: [correctness #69](https://github.com/rayfdj/emaxx/issues/69),
[runtime performance #70](https://github.com/rayfdj/emaxx/issues/70), and
[compiler latency #71](https://github.com/rayfdj/emaxx/issues/71).
The issues retain the original run identity, all 363 files sorted by slowdown,
profiling findings, and acceptance criteria.

## Success and parity

`compat-harness run` and `frozen` share their setup and execution checks.
Outcome parity is still reported, but a file prints `PASS` only when both
editors completed successfully with no unexpected ERT outcomes. A matching
load error or unexpected failure fails the command. So does an unexpected
pass. Legitimate upstream expected failures and skips remain valid.

The shared Lisp reporter asks `ert-test-result-expected-p` for each result,
including compound and predicate expectations. Rust retains that decision
when filtering reports and compares each test's declared expectation between
editors. It validates original reports before any filtering or summary
reconstruction, including runner/file/selector identity, missing expectation
evidence, inconsistent summaries, and incomplete/duplicate results. It also
rejects unsuccessful child exits. Per-file
`execution_issues` and aggregate `unsuccessful_files` record execution health
separately from matching/mismatching outcome counts. Frozen coverage checks
both selected and completed names against the manifest, including extra
selected tests that never produced a result.

## Resume evidence and message comparison

Each run writes `contract.json` before executing files. Resume requires that
contract to match the current binaries, helper, source/generated Lisp inputs,
dump images, effective environment, selector, file inventory, manifest, and
prepared external tools/module inputs.
Values of inherited environment variables are hashed, not published.

Each completed file has an `execution.json` receipt binding hashes of both
editors' separate raw reports, process evidence, and logs to the run contract.
Resume verifies those bytes and report identities, retains them in the new
artifact, and recomputes comparisons, execution health, timings, and exact
manifest coverage through the same path as fresh execution. It never reads
cached scores from `comparison.json`. A missing or invalid per-file receipt
causes an explicit replay of that file; other valid files remain reusable.
Older runs without the contract cannot certify a resume and remain available
for inspection. Hashes are integrity checks, not signed proof against someone
rewriting the entire artifact directory.

Before either child starts, the harness removes old report/marker files, so a
failed launch cannot reuse a prior pass. Reports changed after their producing
process returned are rejected when recording execution evidence.

Message normalization replaces only recorded checkout and private temporary
directory roots at path boundaries. Child names and arbitrary literals such
as `emacs-test-ABC123` retain their bytes. The former generic temporary-root
and random-name substitutions have been removed. A newly visible difference
must be investigated; it cannot be dismissed by its filename pattern.

## Fixture layout

Each editor receives its own short, canonical, private temporary directory.
On Unix this is below `/tmp`, with mode 0700 and a random name, leaving room
for upstream's nested Unix socket names even on macOS. TMPDIR, TMP, and TEMP
agree. Each child starts in its disposable GNU `test/` directory.

Both editors load the original pinned build's compiled Lisp. The disposable
checkout's `lisp/` directory links to those same libraries, so tests locating
libraries relative to their own filename resolve the real loaded files.
Writable test resources remain in the disposable `test/` tree. The harness
detaches the library link before reset/restoration and verifies original
source and generated-library fingerprints after execution. No expected test
paths, Lisp assertions, or editor results are rewritten.

Removing the old partial `source-directory` override alone did not solve
xref: the test derives its expected library paths from `load-file-name`.
The ordinary replay exposed that incomplete diagnosis before the library
layout was corrected.

## Initial Mac validation, 2026-09-14

The following unchanged upstream files passed through `compat-harness run
--file PATH`, sharing the normal frozen implementation:

| File | Passed | Expected failures | Skipped | Unexpected |
| --- | ---: | ---: | ---: | ---: |
| elisp-mode-tests | 58 | 1 | 4 | 0 |
| server-tests | 7 | 0 | 0 | 0 |
| wdired-tests | 7 | 0 | 0 | 0 |
| process-tests | 35 | 0 | 2 | 0 |
| uniquify-tests | 7 | 0 | 0 | 0 |

Both editors have the same results. These cover 20 of the original 26 shared
unexpected failures on this Mac. Evidence is retained under
`target/compat-review-20260914/ordinary-*.log`; the Elisp result after the
library correction is `ordinary-elisp-mode-final.log`. The other four
completed results were retained while correcting xref, rather than rerunning
the whole suite. This is incremental validation, not a final frozen score.

The initial 16 compatibility-report tests passed, as did all 41 harness checks using
the retained results and the affected checkout test after its update. The
real Unix socket test required execution outside the sandbox, which denied
`bind` in the initial invocation. A five-case GNU/Emaxx reporter control also
verified expected failure, unexpected pass, compound and predicate
expectations, and skip. Rustfmt and Clippy with `-D warnings` passed.

The follow-up audit repair passed 17 report tests, eight new adversarial
harness tests, and the affected manifest-name and checkout checks. Its
ordinary uniquify replay again passed all seven tests on both editors, with
raw receipts and unchanged-input verification enabled:
`target/compat/run-1789338447989900000-77677/`. A separate real-process control
deliberately failed only Emaxx; GNU passed, Emaxx retained its unexpected
failure, and the Rust scorer reported zero matching / one mismatching result.
These controls and logs are in `target/compat-audit-20260914/`. They verify
the targeted execution/scoring paths, not every possible runtime path. The
remaining completed upstream runs were not repeated.

## Ordinary prerequisites and inventory extension

The normal `run`, `frozen`, and discovery commands now prepare the same real
external prerequisites for both editors. Eglot uses Rust 1.75.0 with
rust-analyzer and rust-src, matching the pinned upstream fixture's
`rustAnalyzer/Indexing` protocol, and clangd with UTF-16 offsets. Install with:

```sh
rustup toolchain install 1.75.0 --profile minimal --component rust-analyzer --component rust-src
```

Clangd must be installed on PATH; `EMAXX_COMPAT_CLANGD` can select its actual
executable and `EMAXX_COMPAT_RUST_BIN` can select the 1.75 toolchain directory.
Missing prerequisites stop preflight rather than silently removing tests.
Tool executables, versions, sysroot contents, generated launchers, and module
build inputs are recorded and checked after execution. They also bind resume.

Module preparation runs the pinned source's configured GNU Makefile against
unchanged C source and headers. Each editor gets a separate short installation
containing its own unchanged executable and dump, the same compiled module,
and its original relative library layout. This preserves GNU's actual
`invocation-directory` lookup and portable-image native paths. Long artifact
paths caused the upstream help test to wrap its filename before abbreviation;
short installation paths remove that environmental failure without changing
Lisp variables, expectations, or report messages.

Ordinary Mac validation now includes Eglot (45 passes, seven upstream skips,
zero unexpected results on each editor) and modules (38 passes each):

- Eglot: `target/compat/run-1789340787561035000-79536/`.
- Modules: `target/compat/run-1789341049773817000-80892/`.

Together with the earlier file results, this provides incremental ordinary-run
evidence for all 26 original shared unexpected failures. It is not a final
frozen certificate. Earlier module attempts exposed missing native paths and
the help wrapping issue; their failures are retained. An invocation started
while the harness was rebuilding was stopped before tests and excluded
(`run-1789340337784221000-78881`).

The Darwin manifest deliberately adds the 38 names actually discovered by GNU,
replacing only its historical module load error. All previous 7,883 names
remain: 519 files, 7,921 outcomes, zero load errors. Frozen execution also
visits all files with zero recorded selected tests, so new selections cannot
hide in previously omitted files. Linux's inventory remains separately pinned
until actual Linux discovery is reviewed.

## Still open

The non-ASCII regexp defect, GNU's SHR timeout, and features-string diagnostics
remain open in #69. Runtime and native compilation measurements/fixes remain
tracked in #70 and #71.

Linux Rust checks for commit `fdca75e` passed, but those checks are not an
ordinary Linux frozen-success certificate. The new `frozen-run.yml` workflow
first proposes an explicit Linux oracle pin update: the existing native-ABI
CI builds pristine GNU revision `636f166c`, whereas the old frozen Linux pin
names an Ubuntu repack revision. The proposed lock and actual ordinary
module/Eglot results must be reviewed before committing the Linux inventory.
Final frozen mode validates the committed lock; it does not repin it.
The workflow caches build outputs, never compatibility results.

Issue #69 remains open until complete ordinary frozen artifacts on both
platforms meet its acceptance criteria. Completed valid file evidence is
retained during iteration; final integration will receive its own full runs.
