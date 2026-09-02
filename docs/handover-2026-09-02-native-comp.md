# Handover — native compilation checkpoint (2026-09-02, Asia/Jakarta)

**THIS IS THE CURRENT HANDOVER.** It supersedes
`docs/handover-2026-08-28.md` for current work. The older handovers remain
useful history, but their statement that Emaxx models an Emacs build without
native compilation is no longer the active design.

The active branch is `native-comp`. It started from `main`/`origin/main` at
`5a20e24`. The checkpoint commit containing this document is intentionally an
incomplete but working native-runtime milestone, not a claim that native
compilation is finished.

## Non-negotiable boundaries and completion standard

These are Ray's explicit instructions. They survive every session and take
precedence over older plans and handovers.

1. **C becomes Rust; Elisp stays Elisp.** Implement in Rust only behavior
   owned by GNU Emacs C: principally `src/comp.c`, plus the necessary C runtime
   semantics in `alloc.c`, `eval.c`, `data.c`, `lread.c`, `bytecode.c`, and
   `lisp.h`. Use the pinned GNU source as the reference implementation, while
   writing idiomatic, safe where possible, high-performance Rust.
2. **Do not reimplement anything from GNU Elisp in Rust.** In particular,
   `lisp/emacs-lisp/comp.el`, `pcase.el`, `macroexp.el`, `bytecomp.el`, and
   `loadup.el` remain their unchanged upstream files. If a failure appears in
   one of them, repair the Rust implementation of the C/runtime contract that
   the file exercises. Do not patch around the failure in Rust by recognizing
   a compiler-specific function, form, filename, or test.
3. **Do not write new Elisp for native compilation, including diagnostics.**
   No helper `.el`, generated `.el`, `--eval` probe, wrapper, shim, manifest
   generator, compatibility runner, or alternate entry point. Ordinary Emaxx
   must be able to load and execute the same existing `.el` that ordinary GNU
   Emacs loads and execute the same functionality.
4. **Do not call GNU Emacs from Emaxx.** GNU may be run separately as a test
   oracle or profiler. The Emaxx executable and native compiler must never
   delegate work to the GNU executable, link to GNU Emacs runtime code, copy a
   GNU-produced answer at runtime, or select behavior based on the oracle.
5. **Names are an audit signal.** A new `emaxx-*` compiler entry point or
   compatibility mechanism is presumptively a boundary violation. The public
   entry points are GNU's normal ones, such as loading `comp.el` and invoking
   `batch-native-compile`.
6. **Byte-for-byte `.eln` identity is required.** For identical Elisp input,
   GNU source revision, platform, build configuration, compiler options, and
   libgccjit toolchain, GNU Emacs and Emaxx must emit byte-identical `.eln`
   files. Compare the whole files with `cmp`, not merely function results or
   selected machine-code sections. `.elc` is a separate byte-compilation
   product and is not an intermediate required to produce `.eln`; `.elc`
   differences do not excuse `.eln` differences.
7. **Performance is part of correctness.** Native compilation and native
   execution must be at least competitive with GNU's C implementation. Avoid
   whole-heap walks, repeated graph conversion, string-named hot dispatch,
   quadratic list construction, and hidden process startup. Measure GNU and
   Emaxx on the same input and toolchain after correctness is established.
8. **Run an adversarial de-cheating audit before every commit and every push.**
   Look specifically for test-conditioned behavior, copied oracle outputs,
   source/test-name special cases, weakened comparisons, swallowed errors,
   custom runners, invented Elisp, and movement of Elisp-owned policy into
   Rust. Fix findings before committing or pushing.
9. **Formatting and lint must finish cleanly.** The completed work must have
   zero `rustfmt` differences and zero Clippy/rustc warnings. Do not suppress
   legitimate warnings merely to make the count zero; remove dead scaffolding
   or make the intended code path real.
10. **This machine is heavily loaded.** Run expensive builds and tests
    serially. Focused tests are appropriate during implementation. Per Ray's
    later instruction, do not repeatedly run the full multi-hour gate while
    native compilation is incomplete; run it once native compilation itself
    is implemented and supported by ample exact-output proof.
11. Commit as `Ray <26018378+rayfdj@users.noreply.github.com>`. No AI
    attribution or generated/co-authored trailers.

## What the checkpoint implements

The new Rust native-comp subsystem is under `src/lisp/native_comp/`:

- `backend.rs` ports the libgccjit context and emission work owned by
  `comp.c`. It consumes the compiler IR that unchanged `comp.el` constructs;
  it must not decide Elisp compiler policy itself.
- `gccjit.rs` is the dynamic libgccjit API binding.
- `loader.rs` ports `.eln` loading, relocation setup, native-function
  registration, and nested `.eln` loads.
- `runtime.rs` supplies GNU-compatible tagged words, native call trampolines,
  nonlocal exits, unwind/handler state, C primitive link-table entries, and a
  shared Rust/native cons heap.
- `abi.rs` defines the parts of GNU's C ABI that generated code observes.
- `generated_native_subrs.rs` is the exact 1,445-entry C subr registration
  order consumed by `.eln` relocation tables.
- `lisp.rs` translates the Lisp records and IR handed to the C-owned primitive
  boundary. This file is not permission to port `comp.el`; every operation in
  it must remain anchored to what `comp.c` reads or writes.
- `state.rs` owns per-interpreter compiler and loader state.

`src/lisp/primitives/dispatch/comp.rs` exposes the actual GNU `comp.c`
primitives, including `comp--init-ctxt`, `comp--compile-ctxt-to-file0`,
registration, release, trampoline installation, and subr signature queries.
The exact names are GNU names because they are the C/Elisp boundary already
defined by GNU, not a new compatibility surface.

The batch bootstrap now evaluates unchanged GNU `loadup.el` instead of
maintaining a hand-written Rust list of libraries and copied top-level Elisp
forms. It stops only at loadup's final `(eval top-level t)` process-control
handoff. General evaluator, bytecode, reader, symbol-position, keymap, numeric,
printing, and loading fixes elsewhere in the diff repair C-runtime behavior
that upstream Elisp and upstream `.eln` code exercise.

The pre-commit boundary audit caught and removed a Rust shortcut that parsed
`-no-comp-spawn` and set the Elisp-owned `comp-no-spawn` variable itself. GNU's
`startup.el` owns that option transition; any future spawned-compiler support
must route the ordinary command line through unchanged `startup.el`, not
recreate the transition in Rust or silently accept the option.

The native cons bridge has two important performance/correctness mechanisms:

- Native calls have separate touched-cons frames, including nested calls, so a
  nested return does not repeatedly rescan every cons seen by the outer
  top-level load.
- Rust-side mutations enqueue only the mirrored cons cells that became dirty.
  Passing an unchanged mirrored cons is O(1), and a primitive mutation reached
  indirectly through a closure, record, or global is published back to the
  native two-word mirror before generated code resumes.

This is general shared-heap behavior. There is no `pcase`, `comp.el`, source
filename, or test-specific branch in the native heap implementation.

## Proven results so far

### Native ABI and loading

- The live native subr ABI contains 1,445 entries in pinned GNU registration
  order.
- Nested `.eln` loads join the already-active registry and runtime instead of
  constructing an isolated temporary native state.
- Focused native-runtime tests pass, including direct machine writes, indirect
  interpreter writes through captured objects, queued mutation publication,
  handler/unwind behavior, tagging, and MANY calls. The last focused run was
  12 passed, 0 failed, with 1 deliberately ignored stress test.

### Exact small `.eln` artifacts

There is genuine whole-file identity for the small fixed and dynamic upstream
fixtures. The surviving temporary artifacts are:

```text
fixed GNU:   /private/tmp/emaxx-native-compare.cS74bn/gnu-comp-test-funcs.eln
fixed Emaxx: /private/tmp/emaxx-reader-opt-main.eln
SHA-256:     1690f3f5102c25ba0acfa71fdadeede5729e57571b426e200d23d13ad9889ee0

dynamic GNU:   /private/tmp/emaxx-native-compare.cS74bn/gnu-comp-test-funcs-dyn.eln
dynamic Emaxx: /private/tmp/emaxx-reader-opt-dyn.eln
SHA-256:       342db98af34d7a96fbf99e70f8daae8e17b7bc96151c116bd190f52366ada52f
```

Both pairs passed `cmp -s` on 2026-09-02. Do not confuse them with the older
`emaxx-comp-test-funcs*.eln` files in the same temporary directory; those are
stale pre-fix artifacts and differ.

### Performance progress, not completion

On this loaded machine, with an optimized binary:

- Plain Emaxx batch startup fell from 32.85 seconds to 7.18 seconds.
- Direct loading of upstream `pcase.eln` fell from 36.11 seconds to 6.46
  seconds, essentially the Emaxx startup floor.
- GNU loads the same `pcase.eln` in about 0.18 seconds. Emaxx therefore does
  **not** yet satisfy the final performance requirement; the large remaining
  difference is mostly whole-process startup, not `pcase.eln` incremental load.
- Before the heap fixes, the unchanged `comp.el` command was still in macro
  expansion after 527 seconds. It now reaches the single deterministic
  failure below in roughly 13–19 seconds.

The upstream non-expensive compatibility replay previously reached 177/177,
but that is not proof that the full frontend is correct. The self-bootstrap
below exercises a substantially deeper path and currently fails.

## Current blocker: `listp 3` while unchanged `comp.el` is loaded

Reproduce with the ordinary user-facing path—no helper Elisp and no `--eval`:

```sh
target/release/emaxx -Q --batch \
  -l /Users/nbmhqa186/native/emacs/lisp/emacs-lisp/comp.el \
  -f batch-native-compile \
  /Users/nbmhqa186/native/emacs/lisp/emacs-lisp/comp.el
```

`/Users/nbmhqa186/native/emacs` is a sibling symlink to the unchanged checkout
at `/Users/nbmhqa186/projects/emacs`.

The run stops while eager macro-expansion defines
`comp--limplify-lap-inst`:

```text
Eager macro-expansion failure: (wrong-type-argument listp 3)
comp--body-eff(
  ((cl-destructuring-bind (_TAG label-num . label-sp) ...))
  "TAG"
  nil)
```

The arguments shown for `comp--body-eff` are sensible: `body` is a one-element
proper list, `op-name` is `"TAG"`, and `sp-delta` is `nil`. The error occurs
deeper while the unchanged GNU `pcase`/macroexp machinery evaluates that body.

### Important correction to the initial diagnosis

Do **not** continue from the claim that `apply` received a list whose final
`nil` had changed to integer `3`. That was a false lead caused by reading a
propagated primitive error as its origin.

Temporary Rust-only probes established:

- The relevant direct `apply` boundary received two arguments with types
  `lambda` and `cons`; its final list was proper at entry.
- The outer native `funcall -> apply` boundary also did not receive an improper
  list.
- `apply` and the enclosing `funcall` calls merely propagated the error raised
  by the function body.
- The existing native `wrong-type-argument` helper probe produced no hit for
  this error, and LLDB did not stop in `runtime_wrong_type_argument` when the
  failure occurred. The evidence therefore points to a Rust-side evaluator,
  bytecode, or primitive list accessor returning the error, not generated code
  directly calling the native wrong-type helper.

All temporary `apply`, `funcall`, and `Value::{car,cdr,to_vec}` diagnostic
probes were removed before this checkpoint. Do not restore them permanently.

### Best next diagnostic step

Temporarily instrument the Rust list accessors and record
`std::panic::Location::caller()` when they are about to return
`wrong-type-argument listp` for integer `3`. A debug build completed this
instrumentation in about 40 seconds on the loaded machine, versus about two
minutes for each optimized relink. Run the ordinary command above, identify
the exact Rust caller, then remove the probe before committing. No diagnostic
instrumentation should remain in a checkpoint.

Candidate paths that can construct `listp` without passing through ordinary
primitive-dispatch logging include `src/lisp/bytecode/vm.rs` and evaluator code
that calls `Value::car`, `Value::cdr`, or `Value::to_vec` directly. Treat this
as a routing hint, not permission to special-case the failing GNU function.
Once the caller is known, compare its behavior to the owning GNU C code and add
a general Rust regression at that boundary.

## Full `comp.el` reference artifact

The correct GNU stage-2 artifact is:

```text
/private/tmp/gnu-comp-stage2.eln
size:    881808 bytes
SHA-256: e2c8a5638b136b204bd22d22d99531209a31d22f223a208d7691d3fa7fb45507
```

`/private/tmp/emaxx-comp-stage1.eln` is a stale, incorrect earlier artifact:

```text
size:    865312 bytes
SHA-256: 72f41d2d780b5222d5ec65045824fa675dc8f1b584d5aa57d39bdd0806705957
```

It is not evidence of current parity. A previous comparison was also invalid
because the two frontend runs did not load the same upstream native dependencies
(notably `cl-extra.eln`). Control the GNU checkout, load path, native load path,
toolchain, options, and input identically before comparing artifacts.

## Required final proof

Native compilation is complete only after all of the following are true:

1. Ordinary Emaxx loads unchanged GNU `loadup.el`, `comp.el`, and their normal
   dependencies without a compiler-specific runner or injected Elisp.
2. The unchanged upstream native-comp tests pass, including the expensive
   bootstrap. A test count alone is insufficient; confirm that every expected
   test was discovered and that compilation actually occurred.
3. GNU and Emaxx compile the same representative corpus—small fixed functions,
   dynamic functions, closures, handlers/unwind, constants and cyclic/shared
   objects, and `comp.el` itself—with identical inputs and toolchain. Every
   complete `.eln` pair passes `cmp -s` and has the same SHA-256.
4. Both editors can load and execute the artifacts through their normal
   machinery with equivalent behavior. No alternate Emaxx-only loader or
   compatibility entry point is allowed.
5. Differential checks of intermediate compiler data may be used to localize a
   mismatch, but they must use existing GNU Elisp and ordinary entry points;
   they do not replace whole-file `.eln` identity.
6. Native compile time and native execution are measured against GNU C on the
   same machine under comparable load. Emaxx must be at least competitive,
   with profiles demonstrating that no conversion or dispatch pathology is
   hidden by small fixtures.
7. Run the adversarial de-cheating audit, fix every finding, then run the final
   serial gate. `cargo fmt --check` and strict Clippy/rustc warning checks must
   be clean. Only then make the final completion claim.

## Checkpoint verification and operational notes

- `cargo fmt --all -- --check` passes.
- `cargo clippy --all-targets --all-features -- -D warnings` passes with zero
  Clippy or rustc warnings.
- `cargo test --lib lisp::native_comp::runtime::tests` passes: 12 passed, 0
  failed, and 1 intentionally ignored stress test.
- The focused native ABI-order, keymap range-event, and libgccjit smoke tests
  each pass.
- Both small fixed/dynamic GNU-versus-Emaxx artifact pairs listed above still
  pass whole-file `cmp -s`, and each pair has the recorded matching SHA-256.
- The full gate was deliberately not run at this incomplete checkpoint, per
  Ray's instruction. The ordinary unchanged-`comp.el` self-compile still has
  the documented deterministic blocker.
- Heavy work must remain serial on this machine.
- Do not run the full gate at every diagnostic edit. Use the focused native
  runtime and directly relevant evaluator tests until the compiler is complete.
- The native-runtime focused test filter is:

  ```sh
  cargo test --lib lisp::native_comp::runtime::tests
  ```

- Then reproduce the unchanged `comp.el` command above. When it emits an
  artifact, compare it directly with the GNU reference using `cmp -s` and
  `shasum -a 256`.
- Temporary files under `/private/tmp` can be reaped. Their hashes and sizes
  above are the durable evidence; regenerate with ordinary GNU/Emaxx commands
  if the files disappear.

## Intentionally excluded dirty measurement work

The author's working tree also contained uncommitted changes to:

```text
compat/emacs_compat_runner.el
src/compat.rs
docs/honesty-audit-2026-08-18.md
```

They adjust the existing general compatibility reporter so GNU's
`comp-tests.el` run has a writable temporary `.eln` cache before the test file
loads. The runner itself predates this branch; it was not created as a native
compiler. These three changes are measurement-harness work, not native-comp
implementation, and are deliberately not part of this checkpoint commit.
They were preserved rather than discarded because the working tree may contain
someone else's work.
