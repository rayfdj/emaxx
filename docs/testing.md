# Testing Guide

`emaxx` has three separate testing layers. They are intentionally different, and they should not be treated as interchangeable.

There is also a separate performance scoreboard in [`docs/performance.md`](performance.md). It does not replace any of the correctness layers below.

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

The repository defaults the Rust test harness to two workers.  Many of these
tests run complete GNU Lisp subsystems or child processes inside the debug
interpreter; higher test-level concurrency merely starves independent
interpreters and makes process deadlines host-load dependent.  Asynchronous
behavior inside each interpreter remains fully exercised.

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

### Experimental resource-grouped gate

The full library suite contains two very different costs: lightweight Rust
tests and tests that reconstruct or consume the complete GNU Lisp image.  The
experimental grouped runner reuses a validated image only inside isolated,
single-threaded worker processes and runs at most two measured-safe workers at
once:

```bash
python3 tools/grouped_gate.py --scope full
```

Its current resource schedule is deliberately fixed:

| Phase | Concurrency | Image policy |
|---|---:|---|
| `eval_01` + `eval_02` | 2 processes | one cached image per serial process |
| `eval_03` + `eval_04` | 2 processes | one cached image per serial process |
| `eval_05` | exclusive | cached; Eshell/process timing cluster isolated |
| primitives | exclusive | cached; oracle/network/process tests isolated |
| compat-runtime + TTY unit tests | 2 processes | one cached image per serial process |
| batch reconstruction | one process, 2 test threads | fresh images; construction is the subject |
| lightweight remainder | one process, 2 test threads | no image template |

The runner builds the test binary first, discovers that exact binary's test
inventory, and proves that every selected name occurs in exactly one group.
Each group must report the expected outcome count and complementary
`filtered out` count.  New `eval_*` modules are rejected until explicitly
classified.  There are no retries, accepted failures, or additional ignores.
The runner discovers ignored names separately and rejects any name outside the
four reviewed, pre-existing opt-outs; removing an opt-out remains allowed.
For `--scope full`, binary and integration-test targets are discovered from
Cargo metadata rather than maintained as a list, and every target must emit
one successful test result.  Adding a new test binary therefore cannot
silently leave it outside the gate.
Per-group logs, timings, test-binary hash, inventory hash, Git state, and a
machine-readable summary are written under `target/grouped-gate/`.
The runner pins the gate locale, stack size, and two-thread ceiling instead of
inheriting a caller's potentially unsafe test-thread override.

Use `--scope eval --repetitions 3` to stress only the five eval groups, or
`--scope lib` to omit the binary and integration stages.  Until repeated
grouped runs and a complete conventional gate agree on the same tree, treat
this as an experimental accelerator rather than replacing the authoritative
gate.

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

## 3. Native Compiler Artifact Identity

Native-comp development also has an oracle-backed integration test that walks
unchanged GNU test inputs from small to large and requires the complete `.eln`
files produced by GNU Emacs and Emaxx to be byte-for-byte identical:

```bash
cargo test --release --test native_comp_identity -- --ignored --nocapture --test-threads=1
```

The test invokes each editor through its ordinary `-f batch-native-compile`
entry point.  It does not load helper Elisp, modify the upstream tests, or use
the compatibility reporter.  It is ignored by the default `cargo test` run
because it requires the sibling native-comp GNU build and runs two native
compiler processes for every fixture.

The fixtures are deliberately ordered by size.  Each exact rung records the
semantic ground already covered, so the first later mismatch identifies the
new compiler surface introduced by that file:

| Upstream GNU source | Bytes | Semantics exercised | Current identity status |
|---|---:|---|---|
| `test/src/comp-resources/comp-test-45603.el` | 923 | Lexical closures, captured lambda arguments, aliases, conditional function selection | Exact `.eln` |
| `test/src/comp-resources/comp-test-funcs-dyn2.el` | 1,073 | Dynamic binding and the unchanged `no-byte-compile` policy | Neither editor emits an artifact |
| `test/src/comp-resources/comp-test-pure.el` | 1,244 | Direct calls, recursion, arithmetic, pure/impure relocation classification | Exact `.eln` |
| `test/src/comp-resources/comp-test-funcs-dyn.el` | 1,494 | Dynamic binding, fixed/optional/rest arguments, `cl-loop`, `cl-defun` | Exact `.eln` |
| `test/lisp/emacs-lisp/comp-tests.el` | 3,364 | ERT and CL expansion, nested cleanup closures, filesystem control flow, shared constants | Exact `.eln` |
| `test/lisp/emacs-lisp/comp-cstr-tests.el` | 7,141 | Constraint type conversion, unions, intersections, negations, integer ranges, member sets, conservative normalization | Exact `.eln` |
| `test/src/comp-resources/comp-test-funcs.el` | 18,832 | Broad opcode lowering, variables, aggregates, argument ABIs, branches and jump tables, mutation, handlers/unwind, buffers, interactive forms, records, cyclic constants, non-ASCII names, and dead/no-return control flow | Exact `.eln` |
| `test/src/comp-tests.el` | 49,628 | Full upstream native-compiler ERT definitions, resource orchestration, options, diagnostics, asynchronous compilation, loading, runtime assertions, positioned definition names, and positioned interpreted-closure argument lists | Exact `.eln` (`995b8230bb390928510d256567da4c1639d5ab396c4ffa8139c8ca76d3ad6f39`) |

“Exact” means equality of the complete artifact bytes for the same copied
source path, platform, GNU source tree, and toolchain—not merely equal code
sections, matching behavior, or matching extracted strings.

## 4. Authoritative Compatibility Harness

This is the real compatibility runner.

It drives:

- one pinned real Emacs binary as the oracle
- the `emaxx` batch runner

Both sides are invoked with near-matching batch-style commands and upstream-like environment setup.

### Pin The Oracle

First pin the Emacs binary and source tree you want to compare against:

Before building or repinning it, follow the platform capability matrix and
finished-binary probe in [the oracle build contract](oracle-build-contract.md).
The source revision alone does not identify an oracle: optional libraries can
change executed tests into skips and change the native primitive inventory.

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

Every execution has a 120-second per-runner timeout unless
`--timeout-seconds` or `EMACS_TEST_TIMEOUT` supplies another positive value.
The harness prints the newly-created artifact directory; it never reuses an
existing run directory.

### Compare Two Emaxx Revisions Safely

Use one current harness to build and run both source checkouts.  Do not copy a
harness or Emaxx executable between target directories:

```bash
cargo run --release --bin compat-harness -- run --scope all --selector check-all --through-file test/lisp/example-tests.el --subject-root /path/to/baseline --timeout-seconds 120
cargo run --release --bin compat-harness -- run --scope all --selector check-all --through-file test/lisp/example-tests.el --timeout-seconds 120
cargo run --release --bin compat-harness -- compare-subjects --baseline /first/printed/artifact --candidate /second/printed/artifact
```

The comparison fails closed unless both artifacts used the same harness,
oracle executable and helper, selector, file list, name filter, Cargo profile,
and timeout.  It also fails for pass-to-fail, pass-to-skip, missing, or added
subject results.  Each source checkout builds in its own owned
`target/compat-subject` cache; source and executable hashes are checked again
before a valid summary is written.  A copied harness, a shared cache, a
concurrent run against the same subject, or source changes during a run are
rejected.

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
- aggregate summary with harness, subject, oracle, source, binary, profile,
  timeout, Git, and SHA-256 provenance

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

The built-in minibuffer and completion contract combines 113 exact upstream
GNU Emacs 30.2 outcome comparisons with six strict interactive TTY journeys
covering defaults/history, require-match recovery, metadata and annotations,
the `*Completions*` window, completion preview/CAPF, recursive reads, and
keyboard macros.  See
[`docs/minibuffer-completion-compatibility.md`](minibuffer-completion-compatibility.md)
for the commands, permanent scenario inventory, anti-cheat boundary, and
scope.

The deterministic network contract combines the complete upstream JSON-RPC,
GnuTLS, network-stream, and JSON suites with a real one-shot localhost HTTP
retrieval and the existing Eglot/lsp-mode application journeys.  See
[`docs/network-compatibility.md`](network-compatibility.md) for the exact
commands, outcome counts, rejected evidence, and public-network boundary.

The built-in Eglot contract has its own permanent upstream outcome inventory,
deterministic JSON-RPC server, and strict interactive TTY journeys.  See
[`docs/eglot-compatibility.md`](eglot-compatibility.md) for the exact oracle,
commands, preserved failures/skips, and scope boundary.

The third-party Magit contract installs a hash-pinned release and its complete
external dependency closure through real `package.el`, restarts both editors,
checks the installed origins and compiled inventory, and then runs strict TTY
journeys against deterministic Git repositories.  See
[`docs/magit-compatibility.md`](magit-compatibility.md) for the exact command,
scenario inventory, determinism boundary, and anti-cheat evidence.

The third-party Eat contract installs the hash-pinned 0.9.4 release through
real `package.el`, restarts both editors into separate clean package trees,
runs all 57 unedited upstream tests from installed bytecode, and compares
structured records from real PTY children covering input, resize, cursor and
color state, alternate screen, scrollback, EOF, signals, exit, and cleanup.
See [`docs/eat-compatibility.md`](eat-compatibility.md) for the artifact hashes,
exact command, process inventory, and scope boundary.

The third-party Vertico/Consult/Corfu contract installs hash-pinned releases
and their exact dependency closure through `package.el`, verifies 41 compiled
files and installed-bytecode origins after clean restarts, and exercises strict
Vertico selection, Consult line/real-grep preview, and Corfu Terminal CAPF
journeys.  See
[`docs/completion-stack-compatibility.md`](completion-stack-compatibility.md)
for the artifact/source hashes, Emacs 30.2 terminal boundary, exact command,
and scope.

The third-party lsp-mode contract likewise installs a hash-pinned release and
dependency closure through `package.el`, verifies the exact compiled payload
after a clean restart, and exercises real stdio JSON-RPC workspaces through
strict diagnostics, completion, hover, xref, rename, lifecycle, and UI-buffer
TTY journeys.  See
[`docs/lsp-mode-compatibility.md`](lsp-mode-compatibility.md) for the pinned
inventory, commands, deterministic inputs, and scope boundary.

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
