# Process-loader integration, 2026-09-10

This integrates native-comp `e8f370a` (loader implementation `b9c0c21`)
with main `08a4004`. Validation is in progress; this is not a new frozen
7,883-test result or a claim that native images work.

## Review and corrections

- Retained main's thread continuations, scoped stack roots and shared native
  heap ownership. The buffer conflict contained equivalent image declarations;
  the existing main declaration and undo implementation are retained.
- Removed the duplicated `stack_roots` reset-inventory entry. The field audit
  supplements behavioral validation; a listed field is not proof of restoration.
- `init_after_pdump_load` takes both Lisp environment lists from the existing
  process-entry snapshot. Reading the C environment again after libgccjit would
  undo main's protection against compiler-owned environment changes. The real
  libgccjit compile/execute smoke test now checks this restored-session path too.
- GNU `keyboard.c:safe_run_hooks` dynamically binds `inhibit-quit` and establishes
  `internal_condition_case_n` before each callback. The integration now does both,
  preserves nonlocal exits, unwinds the binding, and reads the global hook value
  when the local list reaches `t`. Error removal uses function identity, as GNU
  does. Interactive startup runs this hook after terminal/display setup.
- GNU `lread.c:init_lread` reconstructs the process load path from the configured
  build/installation paths; keeping a customized saved `load-path` instead would
  not reproduce GNU's initialization.
- Further review found saved invocation/shell paths and scratch-buffer directory
  were not replaced. GNU `init_cmdargs`, `init_callproc` and `init_buffer` do
  replace them. The integration now selects scratch, resets its and the first
  minibuffer's directory, and reapplies the existing process path initializers;
  other saved buffers retain their own directories. The positive probe now
  changes cwd between building and loading, and requires invocation variables
  to name the relocated executable. This prevents children silently launching
  the original executable without the new sibling image.
- Failing safe-hook removal calls the existing `set`/`set-default` primitives,
  preserving variable watcher notifications and assignment errors as GNU does.

## Native-image limit and test honesty

The first Darwin library run passed 44 of 46 focused controls. One failure was
a new test incorrectly expecting an unmatched `throw` to escape a safe hook:
GNU signals `no-catch` without an active catch. That test now registers the
enclosing catch. The other failure was real: ordinary Darwin loadup contains
native compiled functions, which this writer still refuses (D14/D15).

The incoming batch/CLI image tests did not account for that existing limit.
Their names and assertions now distinguish a supported image round trip from
the precise native-image refusal. A native refusal requires actual native
functions in the ordinary startup and an empty incomplete output file. These
boundary checks cannot close startup compatibility. No native-loading setting,
original ERT test, selector, timeout or frozen manifest is changed.

`tools/dumped_startup_gate.py` is the positive acceptance gate. Each editor
builds its own image from ordinary startup before loading any tests. An
unchanged copied executable discovers that image beside itself, including in
the original tests' child processes. The GNU build's Lisp/data layout and
unchanged native libraries accompany the executable. A saved marker, real
`pdumper-stats`, processed command line and fresh environment prove restoration.
Both editors must then pass all three original tests, with their original
timeouts. Missing images, skipped/missing tests, nonzero exits and matching
failures all fail this gate. Fixture/image hashes are checked after execution;
commands, timings and raw output are retained.

The first relocated GNU run lacked native libraries; the second lacked the
uninstalled Lisp/data layout. Those were validation setup errors, not runtime
results. With that layout restored, GNU's image probe passed; the first test
run passed the hook-order and async-shell cases but timed out in `erc--find-mode`.
It overlapped a local Rust build and is retained as a failed attempt, not
discarded or counted as closure. Emaxx still refused the ordinary Darwin image.

## Remaining validation

- Linux ordinary image startup and the three original timeout cases.
- Focused hook/process controls, thread/GC/native preservation, full Rust gate,
  rustfmt and zero-warning Clippy on the integrated implementation.
- A fresh frozen corpus run only after ordinary startup is established. Linux
  diagnostic results against the ABI oracle do not repin the frozen Linux
  oracle or establish the Darwin score. D14/D15 still block native images.

Local diagnostic receipts are under
`/private/tmp/emaxx-loader-sept10.IqOklp/`; CI uploads its raw receipts.

## First Linux diagnostics and cache audit

Run [34441410465](https://github.com/rayfdj/emaxx/actions/runs/34441410465)
compiled candidate `871742e` from source. GNU passed all three original tests
from its image. Emaxx refused an ordinary image containing native compiled
functions. The actual Linux build has 137 preloaded native libraries and 12
other native libraries; the earlier description of Linux as a generally
supported non-native image path was too broad. Native-image support is still
needed for this ordinary startup too. No timeout was changed.

Run [34441758870](https://github.com/rayfdj/emaxx/actions/runs/34441758870)
checked out `f929d34`, but its build finished in 0.15 seconds without compiling
Emaxx and produced the same subject SHA256 as the earlier candidate:
`3b36a20338240d8a4667327549778d2c8096d6c779a61de960ce6112f06b1286`.
The restored target cache was newer than the checkout. This run is not evidence
for `f929d34`. The workflow now cleans Emaxx's own package artifacts after cache
restore, asserts the executable is absent, rebuilds it, and records the source
commit and binary hash. Dependency artifacts remain reusable.
Run [34443360422](https://github.com/rayfdj/emaxx/actions/runs/34443360422)
stopped at the executable-absence assertion: Cargo's default package clean
does not clean the custom `gate` profile. The workflow now explicitly cleans
that profile too. No runtime test result was produced by this attempt.

The positive startup result remains a mandatory failing result if no real
image is available. It is recorded separately so runtime, warning and full
Rust checks can finish even when D14/D15 blocks this acceptance test. A green
Rust gate alone cannot close the startup cases or establish a new corpus score.

On Darwin the final focused selection passed 46 of 47 checks; the sole failure
was an unquoted lambda in the new bare-interpreter watcher fixture. Correcting
that test alone passed its rerun. No previously passed group was repeated for
the fixture correction. Strict Clippy then identified a redundant closure in
the hook wrapper; it was replaced by the equivalent block without suppressing
the lint. The final strict Clippy run and CLI image-boundary control pass.

## Completed Darwin preservation and oracle controls

- `native_thread_continuations`: one integration test, six real GNU/Emaxx
  native/thread/GC programs, all matching (74.65 seconds).
- `native_comp_identity`: one integration test, nine unchanged fixtures,
  eight complete `.eln` byte comparisons and the no-artifact policy case,
  all passing (227.31 seconds).
- `cargo fmt --all -- --check` and Clippy with `--all-targets --all-features
  -- -D warnings` pass. There are no lint suppressions for the new code.

The quieter relocated-GNU retry still timed out in `erc--find-mode`.
An isolated comparison then reused the same image and executable bytes but
linked the original native-library installation instead of copying those
libraries. All three original tests passed: the two ERC cases took 1.09 and
0.38 seconds, and async-shell took 0.28 seconds. This demonstrates a library
relocation/setup timing effect; it does not identify its OS-level cause.

The gate now shares the unchanged built native libraries, as it already does
the standard Lisp/data installation. Hashes and the complete native-library
inventory are checked after execution. A fresh run with that layout passes
the GNU image probe and all three original tests. Emaxx still fails while
building its native image (exit 255); the gate correctly exits 1. The raw
attempts remain under `dumped-startup-darwin-final`,
`gnu-shared-native-libraries`, and `dumped-startup-darwin-shared` in the
scratch root. The six gate-acceptance negative controls pass.

`supported_image_starts_in_a_fresh_process_with_new_process_values` adds a
positive process-boundary control without claiming ordinary loadup. It writes
a supported image from the built-in interpreter state, copies the unchanged
test executable, and launches that copy from a new directory. The child must
discover its sibling image through the production loader, validate its real
fingerprint, retain the parent's saved marker, and expose the child's new
invocation name, directory and environment. Its exact child test must execute;
an empty test selection is rejected. It passes in 0.73 seconds. The subsequent
24 audit checks plus that control all pass, and strict Clippy passes after
removing an unnecessary mutable borrow in the new test. These are additional
focused controls, not a replacement for the original startup acceptance test.
