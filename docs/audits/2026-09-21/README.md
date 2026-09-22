**Evidence for the 2026-09-21 remote-main de-cheating audit**

Read [the report](../../adversarial-decheating-audit-2026-09-21.md) for findings and qualifications. This directory retains diagnostic evidence; it is not a frozen compatibility certificate.

- `environment.json`: remote revision, GNU source revision and executable hash, compiler, host.
- `current/`, `baseline/`: provenance, exact macOS diagnostic patches, build and image logs. Current is `45eb153`; baseline is `4068bb4`, after the GC-rate correction.
- `probes/`: unchanged inputs given to each editor and raw result JSON. `parallel-values.rs` exercises safe public Rust allocation on two OS threads. The two long-named Lisp files are extracted from the ignored contracts. The unwind/GC probe matched GNU and is not reported as a defect.
- `measurements/`: three rotating-order rounds of the same checked Lisp workload. `evidence.json` includes input hashes, commands and raw output; `summary.json` reports medians and GC counts.
- `corpus/`: two alternating rounds of four unchanged upstream ERT selectors, with actual per-test reports and process output. Every selected test actually passed in all subjects. These are selected tests, not complete files or a full corpus pass.

Reproduce an individual Lisp comparison with the same command on each binary:

```sh
LANG=C LC_ALL=C /path/to/editor -Q --batch -l /absolute/path/to/probes/argument-growth.el
```

The timing drivers preserve the original working-artifact layout. To rerun locally, place `run-measurements.py`, `run-corpus.py`, and `probes/` under `target/audit-2026-09-21/`, with each compiled editor and its image at `current/bin/emaxx{,.pdmp}` and `baseline/bin/emaxx{,.pdmp}`. GNU is the sibling `../emacs/src/emacs`. Run the drivers sequentially, with no builds or other benchmarks running:

```sh
python3 target/audit-2026-09-21/run-measurements.py
python3 target/audit-2026-09-21/run-corpus.py
```

Build each revision from a separate source export with its recorded patch on macOS and `EMAXX_GNU_SOURCE_DIRECTORY` pointing to the same GNU tree. If reusing Cargo's target directory across source exports, clean **the emaxx package's release outputs** between revisions: Cargo initially treated the older export as fresh in this audit. That reuse was detected before measurement; the baseline was rebuilt after cleaning, and the distinct rebuilt binaries/images are hashed in the timing evidence.

The image-build invocation supplied both `EMAXX_DUMP_SOURCE_DIRECTORY=/path/to/emacs` and `EMACS_TEST_DIRECTORY=/path/to/emacs/test`; setting only the first failed the runtime provenance check. Both Rust diagnostic builds use exactly the same workaround. The timed editor processes do not inherit `EMAXX_*` variables. The corpus runner sets the shared reporting variables separately for each editor and never supplies oracle results to the Rust editor.

The local GNU executable differs from the tracked oracle binary hash despite having the pinned source revision. Library/shared-library bytes are not comprehensively fingerprinted. These limitations are explicit in the report; the data must not be promoted to an ordinary frozen-run certificate.
