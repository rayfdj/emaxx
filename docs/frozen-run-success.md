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

## Regexp correction and SHR diagnosis

The general bracket translator now keeps ASCII/non-ASCII and byte-class
membership separate from Unicode case folding. Ordinary letters and ranges
in the same bracket still fold; negation and quantifiers apply to the entire
combined atom. It also preserves the correct membership of characters with
syntax properties and raw byte8 characters. This removes the false ASCII
matches through long-s/Kelvin case equivalence without recognizing test names.

All 632 diagnostic rows match the actual GNU oracle, covering both folding
settings, mixed/negated/repeated brackets, bytes and syntax properties. The
new regression and 16 adjacent controls pass. Ordinary regex-emacs (34), search
(1), and diff-mode (7) results match with zero unexpected outcomes:

- `target/compat/run-1789341707802267000-85433/`
- `target/compat/run-1789341778094170000-87935/`
- `target/compat/run-1789341794149079000-88864/`

Both diff-mode font-lock tests now naturally declare `:passed`, as GNU does;
no expectation was rewritten. Rustfmt and strict Clippy pass.

SHR's missing callback has a parser cause: the upstream test supplies an
unfinished image tag. GNU links Homebrew libxml2 2.15.3 and returns an empty
body; Emaxx links macOS libxml2 2.9.13 and retains the image. Both invoke the
same documented parser options. A separate full build of unchanged GNU
source with Apple's libxml2 2.9.13 now passes SHR through the ordinary runner.
The build recipe is `tools/build_macos_oracle.py`; it preserves the committed
GNU configure options and exposes the actual SDK xml2-config flags through
pkg-config, without editing GNU sources or updating pins automatically.

The explicitly reviewed new Mac oracle hash is
`591acf7b5da3582dca93367b6a69935dbff9c69a9310f05a1927fa230b428f8d`.
The old `7d8944fe` build and its pin/configuration remain retained. The new
checkout is pristine revision `636f166c`; otool confirms system libxml2.
Regenerating the native ABI from the completed build changes only the host
configuration from Darwin 25.5.0 to 25.6.0. Subroutine order, ABI version and
configure options are unchanged. Emaxx and its image were rebuilt against
that actual ABI and GNU library tree.

Ordinary validation on the new pair: SHR 4/4, XML 1/1 and modules 38/38 pass
in each editor with zero unexpected outcomes. The receipts and contracts
validate in `run-1789344114951647000-22759`,
`run-1789344146348364000-22964`, and
`run-1789344164000459000-23126`. These replace the failing SHR setup with a
working dependency combination, not a matching failure or changed assertion.

## Still open

The runtime performance increment fixes two general problems. Vector
`length` now reads the slot count without cloning the entire vector, as
GNU's `Flength` reads its size. Overlay ownership now follows GNU's GC
behavior: attached overlays are buffer roots, reachable detached overlays
retain their properties, and unreachable detached overlays are swept after
the shared mark/weak-table fixed point. Killing a buffer or deleting all its
overlays detaches those objects rather than destroying still-reachable
properties. Detached objects no longer accumulate in buffer edit scans.

GNU probes confirmed both lifetime bugs before the fix: an attached overlay
incorrectly disappeared from an Emaxx weak-key table, while an unreachable
deleted overlay incorrectly retained its payload. A compiled helper removes
temporary stack references in the latter probe; a direct top-level GNU
probe can conservatively retain a temporary, so it is not treated as an
exact collection-time assertion. Buffer teardown and bulk deletion now
preserve properties as GNU does. Dump restoration and interpreter cloning
also preserve independent detached ownership.

The first IndexMap experiment regressed buffer timing by about 14% and was
removed, including its direct dependency. The replacement was measured
against the saved pre-change executable/image with unchanged upstream tests
in fresh independent processes. These are single paired diagnostic timings,
not a statistical benchmark or frozen certificate:

| Case | Before | After |
| --- | ---: | ---: |
| `test-overlay-randomly` | 3.544 s | 0.817 s |
| `replace-buffer-contents-bug31837` | 2.292 s | 0.466 s |
| `mule-cmds-tests--ucs-names-missing-names` | 7.309 s | 3.905 s |
| `mule-cmds-tests--ucs-names-old-name-override` | 3.645 s | 3.740 s |

The Unicode override case did not improve; it remains in the slow-case work.
All diagnostic outcomes were preserved. Subsequent ordinary-run artifacts
independently validate 406 buffer passes, 22 editfns passes plus one upstream
expected failure, and 16 Mule passes in each editor, with no unexpected
outcomes. Raw receipt and contract hashes were checked. Evidence is retained
in `target/compat-audit-20260914/ordinary-ownership-evidence.json` and its
referenced run directories. Forty-six focused overlay controls, fourteen
native-GC controls, module GC/reentry, and the dump round trip pass; formatting
and strict Clippy pass.

The inventory audit also now compares the complete freshly discovered file
set with the frozen manifest before execution. Added and missing files fail
preflight, including zero-selection files; discovery runs once rather than
once per manifest row. Final aggregate totals are printed only after the
input/provenance checks and durable summary succeed. The inventory controls
exercise added, missing and newly selected cases. These changes strengthen
coverage and reporting without modifying upstream selections or outcomes.

Linux ordinary run `34789196776` has been inspected at the raw-report level.
Module results are independently 37 passed, one upstream skip and zero
unexpected outcomes in both editors. Its GNU discovery explicitly extends
the Linux manifest by 38 names, preserving all 7,883 existing names. The
reviewed Linux lock now records the actual pristine `636f166c` build and
matches the existing native ABI configuration.

The same CI run is **not** an Eglot success. GNU has 44 passes, seven skips
and one unexpected `jsonrpc-error` in
`eglot-test-rust-completion-exit-function`; Emaxx has 45 passes and seven
skips. GNU sent the completion request at 23:38:01.348 and timed out at
23:38:11.351. Both language-server connections finished indexing by
23:38:04.165; a completion payload appears in the retained process output,
so further diagnosis must distinguish server readiness from message delivery.
Neither the upstream deadline nor the expected result has been changed.

Adversarial review found the CI wrapper lost the harness's failure status
through `tee`: GitHub's implicit shell did not enable `pipefail`. Both
compatibility workflows now select explicit Bash, which runs with `-e -o
pipefail`. The actual workflow branch was exercised with independent child
exit codes 0, 1 and 2; all failures reach the workflow exit. The original
green badge remains historical evidence of this defect, not evidence of
Eglot success. Existing raw files correctly recorded the failure throughout.
An explicit single-file workflow input permits continuing with Eglot while
retaining the valid module result. Dispatch controls when validation runs;
publishing a workflow correction does not silently restart all affected files.

Final cross-platform frozen validation and feature/skip diagnostics remain
open in #69. Runtime and native compilation measurements/fixes remain
tracked in #70 and #71.

Linux's single-file Eglot continuation at `ea61026` (CI run `34790872047`,
ordinary artifact `run-1789343728748794028-6642`) has 45 passes, seven skips
and zero unexpected outcomes in each editor; all 52 outcomes match. The
raw contract and six receipt hashes were verified. The original GNU timeout
remains recorded as intermittent; a successful continuation does not prove
its root cause is fixed or substitute for final frozen validation.

Linux Rust checks for commit `fdca75e` passed, but those checks are not an
ordinary Linux frozen-success certificate. The new `frozen-run.yml` workflow
first proposes an explicit Linux oracle pin update: the existing native-ABI
CI builds pristine GNU revision `636f166c`, whereas the old frozen Linux pin
names an Ubuntu repack revision. The proposed lock and actual ordinary
module/Eglot results were reviewed as described above before committing the Linux inventory.
Final frozen mode validates the committed lock; it does not repin it.
The workflow caches build outputs, never compatibility results.

Issue #69 remains open until complete ordinary frozen artifacts on both
platforms meet its acceptance criteria. Completed valid file evidence is
retained during iteration; final integration will receive its own full runs.
