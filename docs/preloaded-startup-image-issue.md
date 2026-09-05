# Persist a GNU-Equivalent Preloaded Startup Image

Published as [GitHub issue #11](https://github.com/rayfdj/emaxx/issues/11).

This issue records historical measurements and design candidates, not the
current implementation plan. The active native-comp goal now follows the
[GNU portable-dump contract ledger](pdump-c-parity-ledger.md). Current
reconstruction executes unchanged GNU `loadup.el`; new preload manifests,
authored Elisp, or Rust copies of Lisp startup policy are not authorized.
Use the new ledger's fresh measurements rather than the older timings below.

## Problem

GNU Emacs starts from a dumped executable whose core Lisp runtime state is
already constructed.  `emaxx` currently reconstructs much of that state on
every batch invocation by loading source libraries.  This fixed startup-image
cost dominates small compatibility files and obscures the performance of the
test and library work under active development.

This is a startup-image architecture project, not an Electric correctness
failure and not evidence that the bytecode instruction loop itself is
uniformly slow.

## Observed Phase Timings

Same machine, release `emaxx`, GNU Emacs 30.2 oracle, Electric compatibility
file with identical 874/874 behavior:

| Phase | GNU Emacs | `emaxx` |
|---|---:|---:|
| Empty batch startup/preload | about 0.19 s | about 1.34 s |
| Add ERT | effectively preloaded | about 0.03 s incremental |
| Load `electric-tests.el` and dependencies | about 0.30–0.36 s incremental | about 1.66 s incremental |
| Execute selected test bodies | about 0.24 s | about 3.05 s |
| Full file | about 0.76 s | about 6.08 s |

Earlier files with small test bodies showed larger full-process ratios because
the fixed interpreted-loading tax dominated.

## Root Cause and Prototype Findings

- GNU's dumped image already contains bootstrap libraries and their runtime
  state.
- `emaxx` replays a hand-built preload sequence from `.el` on each process.
- Compatibility checkouts historically lacked `.elc` artifacts, and the load
  resolver preferred `.el` when both forms existed.
- Enabling `.elc` after bootstrap helps ordinary library loading.  Enabling it
  for bootstrap itself exposes serialized-runtime contracts that source
  interpretation does not exercise.
- GNU `#[...]` syntax represents byte-code functions and interpreted lexical
  closures; the latter requires real reader/runtime support.
- Compiled libraries directly consume dumped functions and values from
  Custom, `subr`, oclosure, cl-generic, macroexp, CL structure registries, and
  built-in class objects.
- File targets and provided features are different identifiers and need one
  explicit target-to-feature manifest if loadup replay remains in the design.

## Direction

Prefer a real, versioned startup image over expanding a fragile per-process
preload list.  Candidate designs are:

1. Build the startup state once and serialize a versioned `emaxx` image.
2. Generate or cache it from a GNU-like loadup manifest, invalidated by the
   `emaxx` version, GNU oracle version, relevant Lisp hashes, and runtime
   representation schema.
3. If compiled preload remains the image builder, complete serialized runtime
   contracts systematically and keep load order in one target-to-feature
   manifest.

GNU C/runtime responsibilities may be implemented idiomatically in Rust.  GNU
Elisp responsibilities must remain loaded and evaluated as Elisp; do not copy
missing Elisp owners into Rust.

## Acceptance Criteria

- A clean checkout can reproducibly build or obtain the startup image.
- Normal batch invocations do not reinterpret the bootstrap dependency tree.
- GNU-compatible `.elc` preference and source fallback are explicit and
  tested.
- The image invalidates safely when the runtime schema, `emaxx`, GNU version,
  or owning Lisp changes.
- Existing compatibility outcomes remain exact across the complete green
  prefix and representative source/compiled suites.
- Timing reports include full wall time and separated phases.
- Preload and dependency facts have one owner rather than duplicated lists.
- No Elisp-owned implementation moves into Rust.

## Performance-Policy Boundary

Continue recording full GNU-versus-`emaxx` time so the startup-image issue
remains measurable.  For active compatibility-frontier work, apply the 2×
blocking rule only to comparable post-bootstrap work.  Exclude the fixed
difference caused by GNU's dumped image until this issue is scheduled.  If a
run cannot separate the phases honestly, report the full time but do not use
it alone to trigger the 2× gate.
