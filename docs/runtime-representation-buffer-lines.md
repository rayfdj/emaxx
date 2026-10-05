# Profile-led buffer line lookup — 5 October 2026

The [full goal](runtime-representation-goal.md) remains active and incomplete.
Applied runtime is still source290, `b870a848`; the two drafts here are unapplied.
Main remains `21d20f0e` and PR79 stays draft.

## Fresh-build correction — 5 October 2026

The [stale-cache evidence and source294 launch](handover/2026-09-30-shared-reader-draft/source293-stale-cache-and294-launch-manifest.json)
supersede source293's validation status below. Cargo reused source292's exact
binaries in gate and release (`fresh=true`); those runs do **not** validate the
indexed lookup. Both v3 coordinators were terminated before any timing. Preserve
their outputs and withdrawal receipts; do not resume them.

**Source294 has exactly the same 532 source inputs as source293**, in
`target/runtime-goal/recovered-2026-10-05/buffer-line-lookup-fresh/emaxx`. It explicitly
cleans the package in gate and release and requires freshly compiled artifacts
and different test binaries from source292. Its build/validation is running;
no candidate speed or validation pass is claimed. Current coordinator receipts
are `buffer-selection-fresh-state.json` and `buffer-performance-sequence-fresh-state.json`,
followed by their final results, under `target/runtime-goal/resume-2026-09-28`.
Source292's separately verified fresh build and 290 selected passes per profile
remain valid. Applied source290 and all goal requirements are unchanged.

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
and bytecode-call controls pass. Linux frozen37269857506 remains in progress;
full source290 macOS is unstarted. No selected result certifies that failed run.

Remaining architecture, physical accounting/counters, ownership, final audits,
final-source platform validation and calibrated every-case GNU parity remain
required. The unchanged 3% criterion and locked workloads are not relaxed.
