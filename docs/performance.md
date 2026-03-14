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

## Execution Model

`perf-harness` reuses the pinned oracle from the compatibility harness:

- tracked lock: [`compat/oracle.lock.json`](/Users/alpha/CodexProjects/emaxx/compat/oracle.lock.json)
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

The initial scenario catalog lives in [`compat/perf_scenarios.json`](/Users/alpha/CodexProjects/emaxx/compat/perf_scenarios.json).

It currently includes:

- noverlay marker microbenchmarks
- noverlay insert/delete microbenchmarks
- provisional real-world noverlay suites
- oracle-only redisplay and next-overlay-change suites
- the coding decoder benchmark

## How To Read Results

Use perf results as a second scoreboard:

- correctness still comes from `compat-harness`
- performance comes from `perf-harness`

A “faster” result is meaningful only for `comparable` cases.

A green perf run does not mean compatibility is correct, and a slower result does not fail the command in v1.
