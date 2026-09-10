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
