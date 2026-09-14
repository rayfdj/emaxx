# Final ordinary frozen results, 2026-09-14

Implementation: `a5084e846d90b7e56ef995628fcbcd359738b9d0`, PR [#72](https://github.com/rayfdj/emaxx/pull/72).
This report records the two completed independent frozen runs. Earlier partial
artifacts remain separate. The documentation commit adds no runtime changes.

| Platform | Files | Matching outcomes | Passes per editor | Declared expected failures | Skips | Unexpected | Command exit |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Linux | 519 | 7,928 / 7,928 | 7,670 | 47 | 211 | 0 | 0 |
| macOS | 519 | 7,915 / 7,921 | 7,623 | 46 | 252 | 0 | 1 |

**All 26 originally shared unexpected failures actually pass in both editors
on both platforms.** None of those 26 is skipped. The original cases comprise
six Eglot, nine Elisp/xref, seven server, two uniquify, one wdired, and one process
test. The normal runner supplies their real prerequisites, source layout,
working directories, private temporary roots, and child-terminal environment.

Mac is **not an all-green strict comparison**. Its only six mismatches are the
seccomp skip diagnostics whose actual build-feature strings differ. Missing
capabilities and faithful feature reporting remain in [#73](https://github.com/rayfdj/emaxx/issues/73),
separately scoped by the user. They are neither excluded nor normalized away.
Both editors correctly skip Linux seccomp behavior on Darwin. All six seccomp
tests actually pass on Linux. Missing optional prerequisites in other upstream
skips remain visible; the counts do not claim every optional tool is installed.

Both editors pass all 177 compiler tests on each platform. Mac modules pass
38/38; Linux modules pass 37 with the same one platform skip. Eglot matches
all 52 outcomes on each host (45 passes, seven skips); all six originally
failing Eglot cases, including the five Rust cases, actually pass.
Mac Tramp matches all 59 outcomes (52 passes, seven skips): GNU's test phase
is 117.304 seconds and Emaxx's 127.999 seconds under the unchanged 180-second
limit. This elapsed-time observation is not an isolated optimization claim.

## Execution and adversarial review

The [Linux run](https://github.com/rayfdj/emaxx/actions/runs/34833812558)
and Mac artifact `target/compat/frozen-1789384393596667000-52035` execute the
normal `compat-harness frozen` command from clean source at the revision above.
Both inventories retain all earlier selected names, with 38 module cases
explicitly added; Linux additionally includes seven actual Mercurial names.

For each platform, all 3,114 raw receipt hashes, exact manifest/selected/result
names, actual result totals, and 1,038 successful editor exits verify. There
are no timeouts or unsuccessful executions. An independent raw-row comparison
checks statuses, ERT expectedness and expectation declarations without trusting
the saved comparison verdicts. All Linux nonpass messages match verbatim. Mac
has six actual feature-message differences and one Tramp ACL skip differing
only by the recorded owned temporary-directory roots; the existing bounded
path normalization accounts for that one difference. All other nonpass messages
match verbatim. Every failed ERT result is explicitly declared `:failed` upstream.

Reviewed runner paths launch each editor separately, clear its owned outputs,
and retain its own raw report and process evidence. Module installation copies
and verifies each editor's own executable and image. Missing reports become
explicit load errors. Resume verifies original bytes and the run contract,
then recomputes comparisons. The new runtime fixes use object kinds, identities,
numeric ranges and GNU argument behavior; no runtime test-name branches or
result forwarding were added. The earlier real SQLite success stub was replaced
with actual extension loading.

This is a bounded audit, not a proof of universal editor equivalence. System
shared-library bytes are not comprehensively covered by provenance. Emaxx's
pre-existing piped batch-restart stdout flush differs from GNU; the child-process
controls do not claim complete restart-output parity. The random repair covers
value/argument contracts, not identity of the underlying PRNG algorithm.

## Rust and affected-area validation

All five new GNU contract controls pass on Mac and Linux, along with all
94 native-runtime controls. They cover shared/circular vector symbols, private
obarray identity, actual native relocation loading, native numeric predicates
and explicit-large-integer sorting, and random fixnum/argument/reseeding behavior.
Unchanged Pascal, Ruby and sort files pass through both final frozen runs;
Ruby retains its two declared expected failures. Earlier primitive, module,
reader, regexp, SQLite, CLI and integration evidence is retained in the
[chronological audit](frozen-run-success.md).

Rustfmt and all-target/all-feature Clippy with `-D warnings` pass on both
platforms. Previously completed full library groups were retained, with focused
controls and affected integration stages continued after fixes. No full suite
was repeated merely for fixture or documentation corrections. One manual image
invocation omitted the test-directory environment; only that preparation step
was continued. The first final Mac launch hit a sandbox denial in clang before
tests; the unchanged command continued with authorized normal host access.
Original failed logs and cached host-tool warning text remain retained.

## Measured improvements and remaining costs

Independently executed alternating Mac pairs, with input/output hashes checked,
measure individual changes: compact internal dump tags reduce median peak RSS
17.0%; removing a non-GNU `looking-at` assignment reduces Semantic body time
3.20%; avoiding repeated searches for already marked native conses reduces sort
6.95%; batch process completion reduces the eight-case compiler selection 4.22%
and the two Simple shell-command cases 9.56%. These are not additive combined
gains or Linux speedup claims. The compact-tag pilot does not establish a startup
speedup. The earlier vector-workload regression and two rejected compiler pilots
remain recorded.

The tables below sort actual final cases by descending Emaxx test-body time.
They describe current workloads, not controlled before/after performance.
Full per-case CSVs, sorted by Emaxx time and by extra time over GNU, are retained
under `target/compat-audit-20260914/{mac,linux}-frozen-a5084e8-reviewed-`
with suffixes `emaxx_seconds.csv` and `gap_seconds.csv`. The original 363-file
slowdown ranking remains in [#70](https://github.com/rayfdj/emaxx/issues/70).

### macOS: slowest 15 cases

| Case | Emaxx seconds | GNU seconds |
| --- | ---: | ---: |
| `simple-tests-shell-command-39067` | 11.948 | 11.049 |
| `tramp-test39-make-lock-file-name` | 11.016 | 10.406 |
| `tramp-test35-remote-path` | 10.659 | 11.050 |
| `tramp-test10-write-region` | 8.903 | 8.517 |
| `tramp-test12-rename-file` | 7.133 | 5.620 |
| `tramp-test11-copy-file` | 6.919 | 5.631 |
| `fns-tests-sort` | 6.661 | 0.588 |
| `tramp-test39-detect-external-change` | 6.121 | 6.068 |
| `em-pred-test/combine-predicate-and-modifier` | 5.188 | 5.502 |
| `tramp-test18-file-attributes` | 5.044 | 4.417 |
| `echo-server-with-dns` | 5.009 | 5.006 |
| `simple-tests-shell-command-dont-erase-buffer` | 4.831 | 4.413 |
| `tramp-test21-file-links` | 4.634 | 3.760 |
| `benchmark-tests` | 4.462 | 3.525 |
| `ert-test-run-tests-batch-expensive` | 4.434 | 17.780 |

### Linux: slowest 15 cases

| Case | Emaxx seconds | GNU seconds |
| --- | ---: | ---: |
| `semantic-utest-ia-doublens.cpp` | 19.602 | 0.648 |
| `semantic-utest-C` | 18.888 | 0.580 |
| `semantic-fmt-utest` | 18.360 | 0.634 |
| `simple-tests-shell-command-39067` | 13.653 | 1.993 |
| `ert-test-run-tests-batch-expensive` | 10.020 | 34.685 |
| `mule-cmds-tests--ucs-names-missing-names` | 9.973 | 0.944 |
| `undo-test4` | 9.775 | 0.796 |
| `mule-cmds-tests--ucs-names-old-name-override` | 9.734 | 0.913 |
| `fns-tests-sort` | 8.438 | 0.835 |
| `bindat-test--sint` | 7.393 | 0.342 |
| `track-changes-tests--random` | 7.245 | 0.386 |
| `eglot-test-rust-completion-exit-function` | 7.052 | 6.759 |
| `tramp-test10-write-region` | 5.618 | 1.135 |
| `simple-tests-shell-command-dont-erase-buffer` | 5.478 | 0.789 |
| `em-pred-test/combine-predicate-and-modifier` | 5.055 | 0.809 |

Tramp and shell-command timings include real subprocess and I/O waits; several
large absolute cases are similarly slow in GNU. Recorded profiles identify
remaining native value conversion/GC in sort, symbol lookup/regexp/GC in Semantic,
and repeated evaluator/value work in Unicode-name and undo tests. Simple's child
startup/loading/teardown motivated the measured batch fix. Track-changes and
package profiles expose native value conversion and file-name-handler/truename
work; the track-changes runs used different actual random seeds, preventing a
paired speedup claim. Mac compiler attribution identifies substantial system
loader waits; Linux instead shows repeated child startup/loading CPU. Sampling
may miss short-lived linker children, so per-invocation resource records are
retained. These findings remain performance debt in #12/#49 and compiler work
in #42; they do not justify attributing every current slow case to one cause.

## Artifact identities

- mac: contract `e5c6ee43e9dbad036f8145d377116c4740618eb8687e478b532ce19d4430d9f7`; summary `13300a019c0e887303333e5caacf84adc5157a23f2aa27b2d5eaf7162c891713`.
- linux: contract `8d2205736f708d5e2112a01647d07326d3306938457ea5d05f372ff05d311780`; summary `732302349d9bbfe0ffad998bc51b71191959051c849f4035d56a4b18040c3693`.

Local verification records include `{mac,linux}-frozen-a5084e8-reviewed.json`,
`original-26-{mac,linux}-frozen-a5084e8-reviewed.json`,
`linux-focused-a5084e8-reviewed.json`, and
`final-a5084e8-adversarial-review.json` under `target/compat-audit-20260914/`.
The original baseline and every failed or incomplete attempt remain separate.
