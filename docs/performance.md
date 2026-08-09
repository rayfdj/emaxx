# Performance Guide

`perf-harness` is the performance scoreboard for `emaxx`.

It is intentionally separate from `cargo test` and from the authoritative correctness runner in `compat-harness`.

## Purpose

The performance harness exists to:

- collect real Emacs baselines now
- compare `emaxx` to the pinned Emacs oracle where we have a faithful paired workload
- keep those results visible without turning performance into a blocking gate too early

In v1, this is a manual/local scoreboard, not a CI gate.

## Commands

List scenarios:

```bash
cargo run --bin perf-harness -- list
```

Run all oracle scenarios:

```bash
cargo run --bin perf-harness -- run --runner oracle --all
```

Run all paired scenarios plus oracle-only baselines:

```bash
cargo run --bin perf-harness -- run --runner both --all
```

Run one paired scenario:

```bash
cargo run --bin perf-harness -- run --runner both --scenario noverlay/perf-marker-suite
```

## Scenario Tiers

Each scenario is classified into one of three tiers:

- `comparable`
  These count toward the headline “faster / parity / slower” scoreboard because both Emacs and `emaxx` have a meaningful paired workload.
- `provisional`
  These are partially informative but not mature enough to count toward the headline comparison.
- `oracle_only`
  These only measure real Emacs for now. They are tracked so the coverage gap stays visible.

Only `comparable` cases count toward the top-line performance claim.

## Comparison Classes

For `comparable` cases, the harness classifies medians as:

- `faster`
  `emaxx_median <= 0.95 * emacs_median`
- `parity`
  `0.95 * emacs_median < emaxx_median <= 1.05 * emacs_median`
- `slower`
  `emaxx_median > 1.05 * emacs_median`
- `unsupported`
  `emaxx` has no valid comparable result for that case
- `failed`
  the oracle or `emaxx` run failed or timed out

The command does not fail just because `emaxx` is slower. It only fails on harness/config/process problems.

Every comparable case also records `emaxx_over_oracle` and
`exceeds_two_x`.  The latter is inclusive (`ratio >= 2.0`) and is counted as
`over_two_x` in both the scenario and run summaries.  This makes the active
investigation threshold machine-readable instead of relying on a manual
calculation from rounded display values.

## Compatibility-Frontier Timing Policy

Compatibility runs should record both GNU Emacs and `emaxx` wall time and,
where practical, split each result into startup/image construction, library or
test-file loading, and selected test-body execution.

The active “investigate and fix `emaxx` when it is at least 2× slower” rule
applies to comparable **post-bootstrap** work: test bodies, library work after
the initial image is available, and other steady-state phases.  It excludes
the fixed startup difference caused by GNU Emacs beginning from a dumped image
while `emaxx` currently reconstructs its bootstrap state.  Continue reporting
that fixed cost, but track its remedy separately in
[`preloaded-startup-image-issue.md`](preloaded-startup-image-issue.md).

If a measurement cannot separate startup-image construction honestly, report
the full timing and mark it as unsegmented; do not use that number alone to
trigger the 2× compatibility-frontier gate.

`compat-harness` now makes this separation directly.  It writes a marker after
the selected test file has loaded and gives setup/load and selected test
execution independent deadlines (180 seconds per phase by default).  Timing
artifacts retain total, setup, and body durations, while `emaxx_over_gnu_milli`
and the 2x flag use only body duration from two completed runs.  A killed run
is censored evidence, so it has no performance ratio and always makes the
correctness comparison incomplete—even if both runners timed out identically.

## Execution Model

`perf-harness` reuses the pinned oracle from the compatibility harness:

- tracked lock: [`compat/oracle.lock.json`](../compat/oracle.lock.json)
- local config: `compat/oracle.local.json`

It also reuses upstream-like environment shaping where helpful:

- `LANG=C`
- the same Emacs env vars cleared by `compat-harness`

Unlike correctness runs, performance runs use a per-scenario temp `HOME` under `target/perf/...` so file-backed benchmarks can write inputs safely.

`emaxx` runs in `release` mode for perf:

```bash
cargo build --release --bin emaxx
```

## Artifacts

Artifacts are written under `target/perf/`.

Each run writes:

- `summary.json`
- per-scenario `oracle.json`
- per-scenario `emaxx.json` when applicable
- per-scenario `comparison.json` when applicable
- raw `oracle.log`
- raw `emaxx.log`

## Current Scope

The initial scenario catalog lives in [`compat/perf_scenarios.json`](../compat/perf_scenarios.json).

It currently includes:

- a shared, source-loaded interpreter suite covering list traversal, cons
  allocation/drop, and lexical function dispatch; every timed invocation
  verifies a semantic checksum
- noverlay marker microbenchmarks
- noverlay insert/delete microbenchmarks
- provisional real-world noverlay suites
- oracle-only redisplay and next-overlay-change suites
- the coding decoder benchmark

## Current Source-Interpreter Baseline

The pre-compact-value checkpoint run of `interpreter/source-eval-suite` on the
pinned GNU Emacs 30.2 oracle and a fat-LTO release `emaxx` is recorded in
`target/perf/run-1786203529/interpreter/source-eval-suite.perf/comparison.json`:

| Case | GNU Emacs | `emaxx` | Ratio |
|---|---:|---:|---:|
| list walk | 0.014421 s | 0.133063 s | 9.23× |
| cons allocation | 0.001323 s | 0.021152 s | 15.99× |
| interpreted function calls | 0.001321 s | 0.023775 s | 18.00× |

Both runners load the same benchmark definitions from `.el`, and setup is
outside the timed calls.  These are post-bootstrap source-interpreter ratios,
so all three trigger the 2× investigation rule.

An allocation-counting release probe found that one list-walk invocation made
about 5.26 million allocation calls and requested about 226 MB while traversing
prebuilt Lisp data.  It performed about 1.57 million symbol clones, 3.42
million cons-value clones, and 709,000 list-to-vector conversions.  The other
two cases made about 805,000 and 1.08 million allocation calls per invocation.

The representation explains the scale:

- `emaxx::lisp::types::Value` is 40 bytes on the measured 64-bit target.
- GNU's `Lisp_Object` is one tagged machine word (8 bytes on the same class of
  target).
- At the pre-migration baseline, an `emaxx` cons owned two separate
  `Rc<RefCell<Value>>` allocations; GNU represents a cons as one object
  containing two tagged words.
- At that baseline, source evaluation flattened each list form into a fresh
  owned `Vec<Value>` before dispatch, multiplying the large-value and
  owned-symbol costs.

This is a runtime representation/execution-model gap, not an inherent C versus
Rust result and not a bytecode-VM regression.  The optimized VM does not run
source-defined closures.

Small isolated experiments are not performance claims:

- borrowing the common non-alias variable name moved cases by roughly
  0.5–3%, too close to machine variation to retain as a standalone win;
- disabling all backtrace recording improved the three cases by about 9–10%,
  but is not a valid implementation because it breaks debugger and backtrace
  semantics;
- an early cache for repeated flattened source forms improved the synthetic
  cases by about 10–12%, but lacked a complete mutation-invalidation contract;
  that prototype was not retained.  The later typed, mutation-stamped design
  described below closes that correctness gap and produces a repeatable win.

The thematic work is therefore a compact Lisp value/object representation,
including shared/interned symbol identities, one-object cons cells, and source
dispatch that does not repeatedly materialize owned syntax vectors.  Treat
lazy backtrace materialization as a related follow-up, not as the main fix.

### Post-Compact-Value Result

The retained compact shared-value slice is recorded in
`target/perf/run-1786218758/interpreter/source-eval-suite.perf/comparison.json`:

| Case | GNU Emacs | `emaxx` | Ratio | Emaxx improvement |
|---|---:|---:|---:|---:|
| list walk | 0.013797 s | 0.114756 s | 8.317x | 13.76% |
| cons allocation | 0.001308 s | 0.018681 s | 14.282x | 11.68% |
| interpreted function calls | 0.001322 s | 0.021070 s | 15.938x | 11.38% |

The improvement column compares Emaxx with the pre-migration Emaxx medians
above, not with GNU.  The independent repeat in `run-1786218701` reports
0.115651, 0.018858, and 0.021062 seconds respectively, confirming that the
gain is well outside the roughly one-percent machine-noise concern.

The change reduces `Value` from 40 bytes to 16 bytes on the measured target
and makes text, symbol names, big integers, lambdas, and buffer descriptors
cheap to clone while retaining uninterned-symbol and mutable-object identity
contracts.  It does not yet remove source evaluation's repeated list
flattening and owned dispatch materialization, so the remaining 8-16x gap is
still thematic work rather than an inherent Rust-versus-C cost.

Electric's focused compatibility run stayed exact at 874/874 and measured
0.809 seconds for GNU versus 12.375 seconds for Emaxx; its ordered-prefix run
measured 0.745 versus 11.784 seconds.  The pre-migration focused Emaxx result
was 13.475 seconds.  These unsegmented end-to-end measurements include the
startup/loading cost and therefore are supporting evidence, not direct input
to the post-bootstrap 2x gate.

### Post-Compact Source-Dispatch Result

The retained source-form snapshot cache and evaluated-argument buffer pool are
recorded in
`target/perf/run-1786226330/interpreter/source-eval-suite.perf/comparison.json`:

| Case | GNU Emacs | `emaxx` | Ratio | Emaxx improvement |
|---|---:|---:|---:|---:|
| list walk | 0.014064 s | 0.105080 s | 7.472x | 8.93% |
| cons allocation | 0.001324 s | 0.017177 s | 12.974x | 8.95% |
| interpreted function calls | 0.001348 s | 0.019696 s | 14.612x | 6.91% |

The improvement column compares Emaxx with the exact immediately preceding
checkpoint run in `run-1786224259`, not with GNU.  Independent runs
`1786225791` and `1786225856` reported Emaxx medians of 0.105937/0.017317/
0.020124 and 0.105445/0.017201/0.019726 seconds, so the retained gain is well
outside the roughly one-percent machine-noise concern.

This is a derived snapshot cache, not a second syntax authority.  A single
typed cons-mutation epoch stamps source-form, macro-expansion, lambda-body, and
plain-quote derivations; all mutable cons-field borrows advance it.  Weak
source witnesses prevent allocator-address reuse from aliasing unrelated
forms.  Focused tests mutate cars and cdrs through ordinary source calls,
macros, old and new lambda closures, and reader-resolved quote templates, and
also cover recovery after evaluation errors.  The argument-vector pool uses
RAII so every early return clears its buffer, and it rejects oversized storage
instead of retaining it indefinitely.

Electric remains semantically exact at 874/874 in
`target/compat/run-1786226417171206000-50677`: GNU took 0.825 seconds and Emaxx
12.591 seconds.  That unsegmented result is effectively unchanged at machine
variation scale and still includes the dumped-image/startup asymmetry.  The
cache is therefore retained for its repeatable comparable post-bootstrap gain
and mutation-safe architecture, not as a claim that it closes Electric's
end-to-end gap.  Body-heavy Bindat, Edebug, ERC, international-text, and
package workloads still exceed the 2x threshold after any plausible fixed
startup subtraction; profile their shared evaluator/value-traffic path next.

The cumulative replay through C# Mode in
`target/compat/run-1786232582936062000-54801` matched 350/351 files.  The sole
mismatch is the pre-existing `test/lisp/net/tramp-tests.el` load boundary:
GNU loaded and completed discovery in 98.886 seconds, while Emaxx discovered
no tests before the 1,800.079-second cap.  Every file after TRAMP matched.
Routine development replays should now use focused/current-batch scopes and a
short, explicitly recorded cap for that known incomplete boundary.  Reserve
the 1,800-second TRAMP replay for changes to TRAMP/loading and coherent
publication checkpoints; never convert the timeout into a pass or skip.

### Post-Shallow-Environment Result

The retained lexical-environment work makes each frame a one-pointer,
copy-on-write snapshot.  Closure capture and invocation therefore share
ordered binding vectors until a frame is actually mutated.  This does not add
a competing lexical-cell model: stable frame identities and the existing
overlay remain authoritative, and the live-environment path is used only for
an exact frame-for-frame match.  A bounded weak-witness index avoids
registering the same captured environment repeatedly.  The source-form cache
also reuses native dispatch, literal classification, and `if` tail-alias
analysis under its existing cons-mutation stamp and weak source witness.

The final comparable artifact is
`target/perf/run-1786242006/interpreter/source-eval-suite.perf/comparison.json`:

| Case | GNU Emacs | `emaxx` | Ratio | Change from prior checkpoint |
|---|---:|---:|---:|---:|
| list walk | 0.015205 s | 0.101683 s | 6.687x | 3.23% faster |
| cons allocation | 0.001374 s | 0.016675 s | 12.136x | 2.92% faster |
| interpreted function calls | 0.001339 s | 0.018420 s | 13.756x | 6.48% faster |

The change column compares the Emaxx medians with the retained
post-source-dispatch artifact `run-1786226330`.  A closer immediately preceding
run after the environment representation but before cached dispatch analysis,
`run-1786238742`, measured 0.108944/0.017662/0.019318 seconds; the final values
are respectively 6.66%, 5.59%, and 4.65% faster.  The ratios remain well over
the comparable post-bootstrap 2x threshold, so this checkpoint is a thematic
improvement, not the end of the interpreter work.

The real-suite effect is larger where closure environment traffic dominates.
On the same machine and test source, Bindat fell from Emaxx 20.461 seconds at
base commit `29d4323` (`run-1786241225903316000-1785`) to 4.951 seconds
(`run-1786241990186148000-6088`), a 75.8% reduction, while all 29 tests match
GNU.  GNU measured 0.474 seconds in the final run; the unsegmented ratio still
includes both runtimes' loading/startup paths and is not the post-bootstrap
2x score.  Electric remains exact at 874/874 and essentially unchanged:
same-machine base was GNU 0.818/Emaxx 13.856 seconds, while final was GNU
0.798/Emaxx 13.865 seconds in
`run-1786241865230074000-5935`.  That control rules out trading Bindat speed
for an Electric regression and shows that Electric's remaining cost is a
different evaluator/loading profile.

Two tempting micro-changes were discarded: pointer equality for shared text
and an FNV variable-alias map produced mixed results around one-percent
machine noise.  A post-hoc selective closure-sync prototype was also removed
after becoming pathologically slow.  Only the shallow-frame representation,
exact live-frame path, idempotent registration, and mutation-stamped dispatch
analysis were retained, with focused identity/invalidation tests and the full
release suite green.

### Phase-Aware Compatibility Timing

The first real phase-split run, Bindat in
`target/compat/run-1786245510121736000-13871`, reports:

| Runner | Setup/load | Selected tests |
|---|---:|---:|
| GNU Emacs | 0.290 s | 0.218 s |
| `emaxx` | 2.062 s | 3.180 s |

All 29 results match.  The body ratio is 14.570x, so removing loading from the
equation does not explain away Bindat's remaining gap; it confirms a thematic
source-evaluator/runtime problem.

The canonical TRAMP probe in
`target/compat/run-1786245633469361000-14051` provides an equally important
correction.  GNU setup/body took 1.441/6.527 seconds.  Emaxx completed setup in
2.753 seconds and then exceeded 180.032 seconds in
`tramp-test18-file-attributes`.  Therefore the current canonical TRAMP blocker
is in selected test execution, not test-file loading.  Its Emaxx/GNU body
ratio is intentionally absent because a timeout is a lower bound, not a
completed sample.

Before a representation change is retained it must pass all of these gates:

- the paired source-interpreter suite with semantic checks and interleaved
  release A/B measurements;
- mutation tests proving cached or lowered forms observe `setcar` and `setcdr`;
- the native Rust suite and strict formatting/lint gates;
- Electric 874/874 and the current ordered compatibility prefix;
- a representative compatibility-workload improvement clearly outside
  machine noise, with no source-loading regression.

## Pre-Migration Safety Boundary

The commit immediately before the compact-value migration is tagged
`pre-compact-lisp-value-2026-08-08`.  The tag is the stable rollback and
`git bisect` boundary; ignored files under `target/` are evidence, not the
source of truth.

The checkpoint protects the migration with independent layers:

- Rust identity, mutation, cycle, dynamic-binding, bytecode, reader, marker,
  buffer/window, and cleanup regressions;
- strict rustfmt, Clippy, generated-manifest, all-target compilation, CLI, and
  ERT-runner publication gates;
- exact GNU/Emaxx compatibility evidence, including Electric 874/874 and the
  current ordered-prefix policy;
- the paired source-interpreter workload with semantic checks and explicit
  per-case Emaxx/GNU ratios.

Add focused tests for each newly exposed object-table or invalidation contract.
Do not postpone identity and mutation coverage until the final representation
flip, and do not accept a benchmark improvement that weakens Lisp semantics.

## How To Read Results

Use perf results as a second scoreboard:

- correctness still comes from `compat-harness`
- performance comes from `perf-harness`

A “faster” result is meaningful only for `comparable` cases.

A green perf run does not mean compatibility is correct, and a slower result does not fail the command in v1.
