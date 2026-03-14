# Testing Guide

`emaxx` has three separate testing layers. They are intentionally different, and they should not be treated as interchangeable.

There is also a separate performance scoreboard in [`docs/performance.md`](/Users/alpha/CodexProjects/emaxx/docs/performance.md). It does not replace any of the correctness layers below.

## At A Glance

| Command | Purpose | Uses real Emacs oracle? | Scope | Strictness |
|---|---|---:|---|---|
| `cargo test --lib` | Fast Rust-level regression tests | No | Rust modules only | Strict |
| `cargo test` | Local smoke coverage | No | Unit tests + 3 upstream `.el` files | Permissive smoke |
| `cargo run --bin compat-harness -- run ...` | Compatibility scoreboard | Yes | Recursive `test/src` / `test/lisp` coverage | Strict oracle compare |

Performance is tracked separately with:

| Command | Purpose | Uses real Emacs oracle? | Scope | Strictness |
|---|---|---:|---|---|
| `cargo run --bin perf-harness -- run ...` | Performance scoreboard | Yes | Scenario manifest in `compat/perf_scenarios.json` | Non-blocking for slower results |

## 1. Rust Unit Tests

Run:

```bash
cargo test --lib
```

What this covers:

- Reader behavior
- Lisp evaluator behavior
- Buffer primitives
- Overlay adjustment logic
- Compatibility harness selector/env/comparison helpers

What this does not cover:

- Running upstream Emacs test files broadly
- Comparing `emaxx` behavior against a real Emacs binary

Use this when:

- You want the fastest feedback loop
- You are working on one subsystem and want targeted regressions

## 2. Smoke Integration Tests

Run:

```bash
cargo test
```

Or run one smoke test directly:

```bash
cargo test ert_editfns_tests -- --nocapture
cargo test ert_buffer_tests -- --nocapture
cargo test ert_cmds_tests -- --nocapture
```

These tests live in [tests/ert_runner.rs](../tests/ert_runner.rs).

They execute a small fixed subset of upstream Emacs test files through the Rust interpreter:

- `test/src/editfns-tests.el`
- `test/src/buffer-tests.el`
- `test/src/cmds-tests.el`

Important limitations:

- This layer is intentionally smoke-only.
- It is permissive by design.
- It only asserts that some tests pass.
- It does not compare results to real Emacs.
- It ignores top-level load errors while collecting tests.

That means `cargo test` can be green while compatibility is still badly broken.

Use this when:

- You want lightweight upstream-flavored signal in normal local development
- You want CI-friendly smoke checks without running the full oracle compare

Do not use this when:

- You want to know whether `emaxx` actually matches Emacs
- You need full or even broad upstream coverage

## 3. Authoritative Compatibility Harness

This is the real compatibility runner.

It drives:

- one pinned real Emacs binary as the oracle
- the `emaxx` batch runner

Both sides are invoked with near-matching batch-style commands and upstream-like environment setup.

### Pin The Oracle

First pin the Emacs binary and source tree you want to compare against:

```bash
cargo run --bin compat-harness -- oracle pin --emacs /path/to/emacs --repo ../emacs
```

This writes:

- tracked lock file: [compat/oracle.lock.json](../compat/oracle.lock.json)
- local machine config: `compat/oracle.local.json`

If the pinned binary, commit, system type, or native compilation capability changes, the harness refuses authoritative runs until you repin.

### Inspect Available Selectors

```bash
cargo run --bin compat-harness -- selectors
```

Named selectors mirror upstream make/ERT usage for the pinned oracle:

- `default`
- `expensive`
- `all`
- `check`
- `check-maybe`
- `check-expensive`
- `check-all`

You can also pass a literal ERT selector expression directly with `--selector`.

### List Coverage

```bash
cargo run --bin compat-harness -- list --scope all --selector default
```

You can narrow the run:

```bash
cargo run --bin compat-harness -- list --scope src --selector default --file test/src/buffer-tests.el
cargo run --bin compat-harness -- list --scope all --selector default --name overlay
```

### Run Compatibility Comparisons

Run everything in both upstream trees:

```bash
cargo run --bin compat-harness -- run --scope all --selector default
```

Run one file:

```bash
cargo run --bin compat-harness -- run --scope src --selector default --file test/src/buffer-tests.el
```

Run a different selector:

```bash
cargo run --bin compat-harness -- run --scope src --selector check-all --file test/src/comp-tests.el
```

### What The Harness Compares

For each file, the harness compares:

- file load status
- discovered test set
- selected test set
- per-test pass/fail/skip status
- failure or skip condition type

It fails the run on mismatches.

### Coverage Scope

The harness discovers upstream tests recursively from:

- `test/src`
- `test/lisp`

This is the broadest coverage path in the repo, and it is the only path that should be treated as the compatibility scoreboard.

### Artifacts

Artifacts are written under `target/compat/`.

Each run includes per-file data such as:

- raw oracle log
- raw `emaxx` log
- oracle JSON report
- `emaxx` JSON report
- comparison report

Use this when:

- You want truth, not just smoke signal
- You are measuring progress toward README-level Emacs compatibility
- You want to inspect exact mismatches against real Emacs

## Upstream-Like Invocation Model

The compatibility harness uses upstream-like environment and batch flags where possible.

Examples include:

- `LANG=C`
- `HOME=/nonexistent`
- `EMACS_TEST_DIRECTORY=...`
- unsetting `EMACSDATA`, `EMACSDOC`, `EMACSLOADPATH`, `EMACSPATH`, `GREP_OPTIONS`, and `XDG_CONFIG_HOME`
- batch flags like `--no-init-file`, `--no-site-file`, `--no-site-lisp`, `--batch`, `-L`, `-l`, `--eval`

The supported pass-through environment knobs are:

- `EMACS_TEST_TIMEOUT`
- `EMACS_TEST_VERBOSE`
- `EMACS_TEST_JUNIT_REPORT`
- `TEST_BACKTRACE_LINE_LENGTH`

## Recommended Workflow

For normal development:

1. `cargo test --lib`
2. `cargo test`
3. `cargo run --bin compat-harness -- run --scope src --selector default --file <target>`

Then widen out:

4. `cargo run --bin compat-harness -- run --scope all --selector default`

## Which Result Should You Trust?

If the three layers disagree, trust them in this order:

1. `compat-harness`
2. `cargo test`
3. `cargo test --lib`

That order is not about code quality; it is about how directly the layer measures compatibility against real Emacs.
