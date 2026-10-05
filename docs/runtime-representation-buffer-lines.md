# Profile-led buffer line lookup — 5 October 2026

The [full goal](runtime-representation-goal.md) is active and incomplete. Applied
runtime is **source292**, the LF-only correction. Main remains `21d20f0e`; PR79
stays draft. Both optional lookup changes, source294 and source295, are rejected.


## Applied LF-only correction

The [application review](handover/2026-09-30-shared-reader-draft/source292-application-review.json)
and [byte/mode verification](handover/2026-09-30-shared-reader-draft/source292-applied-source-verification.json)
apply exactly the separately validated source292 inputs. Production changes only
Ropey's feature configuration: GNU line operations count LF, so disable the extra
CR/Unicode separators and retain SIMD. The original backward scan is unchanged.
All original tests remain; the dedicated 338-row line-motion fixture and original
broader probe are retained. The broader comparison falls from 155 differing rows
to 60 unchanged display-column failures. Every line/motion field matches GNU.

The [second closed diagnostic](handover/2026-09-30-shared-reader-draft/source295-single-line-performance-manifest.json)
adds 288 verified actual answers/modes to the preceding 288, with no short bodies.
It compares original290, corrected292 and shortcut295 through all 16 unchanged
workloads. Across the two cohorts, six samples per original/corrected editor/case
give corrected292 a raw body geometric ratio **1.001869** versus original290 and
paired GNU ratio geometric mean **2.97884×**. Bytecode calls remain 3.194% slower
and explicit GC 3.780% slower; undo is 3.026% faster and sorting 3.475% faster.
All per-case changes and raw samples remain in the review. This necessary
correctness repair claims no performance improvement or statistical acceptance.

The [source295 decision](handover/2026-09-30-shared-reader-draft/source295-application-decision.json)
rejects its optional shortcut: raw body ratio 0.997711 versus corrected292, with
undo 2.036% slower and bytecode calls 4.842% slower. The small diagnostic provides
no useful demonstrated advantage to justify retaining the extra branch. No
source295 runtime code is applied. Source294's rejection remains unchanged.

All line pipelines are closed. Next review the profile-identified native fixed
argument copy against GNU `eval.c:funcall_subr`, preserving the existing native
arity, optional-padding, root, handler and unwind contracts. Exact-source Linux
and complete macOS validation are still required for the applied LF correction.

## Closed comparison and rejected indexed lookup

The [closed selected archive](handover/2026-09-30-shared-reader-draft/source294-buffer-line-selected-manifest.json)
verifies fresh source292 and294 builds, 532 inputs, zero-warning strict checks,
**290 passes in each gate/release profile**, two existing ignores, and **47 ordinary
actual GNU comparisons** per variant. Both required suspended-root contracts pass.
All original test assertions and fixture bytes remain. All 338 dedicated LF motion
rows and 198 generated reader/printer rows match. The unchanged broader probe still
fails 60 display-column rows; every line/motion field now matches, and every column
value equals the original source290 value. No display-width repair is claimed.

The [closed full16 diagnostic](handover/2026-09-30-shared-reader-draft/source294-buffer-line-performance-manifest.json)
verifies **288 process answers/modes**, all three interleaved repetitions for each
of original290, LF-only baseline292 and indexed294. Every sample and unfavorable
result remains, with startup/total/RSS/GC and unavailable-allocation reports.
There are no short bodies or concurrent builds/tests/profiles. Paired GNU ratio
geometric means are **2.99694× / 2.97907× / 3.04121×**. Indexed294 has a raw body
geometric ratio **1.02107** versus corrected292, and undo is **37.691% slower**.
The LF-only baseline has raw aggregate **0.995904** versus original290, which
establishes no broad statistically significant gain.

The [application decision](handover/2026-09-30-shared-reader-draft/source294-application-decision.json)
**rejects indexed294**. Do not apply it or claim a speedup. The current refinement
is based on corrected292's original scan, not the slower indexed implementation.
These diagnostics do not satisfy calibrated nine-round acceptance or the unchanged
upper95% ratio≤1.03 requirement on every case.

## Source295: constant-time proof that a scan is unnecessary

The [portable refinement and launch](handover/2026-09-30-shared-reader-draft/source294-review-and295-single-line-draft-manifest.json)
retains 532 exact byte/mode replays from `7f687d58` and main. After the original
position clamp, `line_start_at` checks whether the authoritative LF line count is
one. If so, no LF exists anywhere in the buffer and the original scan must return
the accessible beginning. Other buffers follow every original scan instruction.
This uses already maintained metadata, with no new cache, allocation, unsafe code,
callback or GC change. GNU's newline-free-region skip supplies the reference.

No test, fixture, expected byte or selector is added or changed relative to292.
The [closed selected archive](handover/2026-09-30-shared-reader-draft/source295-single-line-selected-manifest.json)
verifies zero-warning strict checks, fresh gate/release builds, 290 passes/two
existing ignores in each profile and all 47 ordinary GNU comparisons. Its broader
probe retains the same 60 width failures. The performance decision above rejects
this optional optimization. The frozen checkout remains
`target/runtime-goal/recovered-2026-10-05/buffer-single-line/emaxx`; every validation,
comparison and timing coordinator is closed. Preserve their final receipts under
`target/runtime-goal/resume-2026-09-28`; do not restart write-once producers.

## Preserved setup and cache failures

The [stale-cache rejection and fresh-build launch](handover/2026-09-30-shared-reader-draft/source293-stale-cache-and294-launch-manifest.json)
remain essential. Source293 reused source292's exact gate/release binaries;
those tests do not validate the indexed change. Its coordinator and benchmark
queue were withdrawn before timing. Source294 has identical source bytes but
explicitly cleans both profiles, requires `fresh=false` compiler records and
verifies binaries differ from292. Both profiles and the ordinary build pass
those checks. The available previous286–292 build logs were also rechecked;
their recorded test artifacts were freshly compiled and ordinary logs compile
Emaxx. No earlier timing was silently substituted.

A prior comparison child had completed all46 source292 ordinary controls before
its parent was withdrawn. The later coordinator stopped on immutable receipt
collision; all92 process receipts were independently reverified and reused,
without repeating those comparisons or changing their verdicts. Another preflight
stopped because sparse checkout omitted the benchmark driver/support files;
restoring exact base-commit tools/compat files, with all532 runtime inputs unchanged,
resolved it before any timing. Every stopped attempt and recovery remains in the
archives. The following paragraphs retain the original motivation and probes.

The source290 undo profile identifies `Buffer::line_start_at` as its largest
non-GC self-sample. Its scope includes pre-body collection, so collector samples
are not body-cost evidence. The method repeatedly scans backward through rope
chunks and converts character/byte positions. GNU `search.c:find_newline` searches
LF and can skip known newline-free regions in its gap buffer. Emaxx already has
Ropey's maintained line metadata, but its default configuration counts additional
Unicode separators and carriage returns as line endings.

The [portable drafts and original failures](handover/2026-09-30-shared-reader-draft/source292-293-buffer-line-drafts-manifest.json)
retain **532 inputs**, exact byte/mode replays from `7f687d58` and main, GNU
reference hashes, raw probes and all executed helpers. Source292 changes only
the Ropey feature configuration, retaining SIMD and the existing chunk scan.
It provides a separate correctness baseline. Source293 also replaces the backward
scan with `char_to_line` and `line_to_char`, keeping both position clamps and the
accessible beginning bound. No extra cache, mirror, unsafe block, allocation,
callback or collection change is introduced. The existing rope is the storage
constraint; its own index supplies the lookup instead of a second region cache.

The original broad probe compares **338 rows** with actual GNU, covering LF,
CR/CRLF, VT/FF/NEL/LS/PS, multibyte characters, long chunk boundaries, narrowing,
insertion/deletion, point preservation and forward/backward motion shortages.
Source290 differs in 155 rows. Line-number differences come from the rope's
separator configuration. **Sixty rows also expose existing display-column
differences**: `buffers.rs:column_after` counts each visible non-tab character
as one. The entire original program, expected output and actual failures remain.
That separate width defect is outside this line-index change and remains open.

A dedicated 338-row LF motion/count fixture does not compute display columns.
It does not replace or reclassify the broader failure. Source292 passes strict
checks with zero warnings and **290 selected tests in each gate/release profile**,
with the two pre-existing external TTY ignores. Its own ordinary executable and
image are built. Source293 validation is running at publication; ordinary
comparisons and the three-way unchanged full16 performance comparison are pending.
No speed or full-correctness claim is made for either draft.

The first wrapper stopped after the successful gate run because it assumed
one-line verdicts and no ignores. The corrected parser checks every name and
final verdict, preserves output, and rejects missing/failed verdicts or extra
ignores in four negative controls. Completed tests were not rerun. A later image
step exited 127 because the sparse checkout omitted `tools/build-image.sh`.
Restoring that exact base-commit script resolved it; both original failed
orchestrations and the successful continuation remain recorded.

Local frozen inputs are in `target/runtime-goal/recovered-2026-10-05/` under
`buffer-line-index/emaxx` (292) and `buffer-line-lookup/emaxx` (293). Helpers and
receipts are under `target/runtime-goal/resume-2026-09-28`. Inspect
`source293-validation-state.json`, `buffer-selection-v3-state.json` and
`buffer-performance-sequence-v3-state.json`, then their final results. Earlier
coordinators stopped on the recorded setup errors and must not be restarted.
The v3 coordinator finishes the queued validation, all ordinary comparisons,
unchanged broad-probe diagnosis and raw audits before starting timing. Keep
builds, tests and profiles out of the timing interval; preserve all samples.

Source290's [closed Linux Rust failure](handover/2026-09-30-shared-reader-draft/source290-complete-linux-rust-failure-manifest.json)
verifies **2,288 passes / one GNU reference failure**, with 756 library names and
both Cargo stages unexecuted. The first empty-record sample is −7 rather than 2;
all later values match. This actual GNU output equals the earlier source272
reference failure. The unchanged pinned GNU inputs verify; the cause remains
unresolved. The assertion stops before Emaxx. All new numeric, native-argument
and bytecode-call controls pass. The [closed Linux frozen archive](handover/2026-09-30-shared-reader-draft/source290-complete-linux-frozen-manifest.json)
verifies 519 files/7,928 equal outcomes/1,038 successful processes and all177 compiler
controls, including pinned oracle bytes. Full source290 macOS remains unstarted. No selected result certifies that failed run.

Remaining architecture, physical accounting/counters, ownership, final audits,
final-source platform validation and calibrated every-case GNU parity remain
required. The unchanged 3% criterion and locked workloads are not relaxed.
