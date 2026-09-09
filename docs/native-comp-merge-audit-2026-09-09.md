# Native-comp merge audit, 2026-09-09

Merge target: main `4311aa6` plus native-comp `7a87362` (fetched for this
request). Native-comp includes image writer checkpoints D09–D11b and its
merge of main `85f0c28`; main adds the real thread continuations and the
OpenPGP ASCII correction. This is not a completed startup image loader.

## Review before validation

- Keep both buffer additions: saved undo payloads remain traced while
  threads are suspended, and the image writer receives BufferImage.
- Keep main's portable PTY argument (`&raw mut size` with mutable storage).
  The branch's Linux-only shared reference does not match Darwin libc.
- Preserve both audit histories, the OpenPGP correction and the continuation
  implementation. The native graph dump control uses the new loader API
  without replacing the shared native heap or execution ownership code.
- Account for scoped execution roots in the dump inventory. GNU recreates
  specpdl and bytecode stacks after loading; these are live call frames,
  not image objects. The existing refusal of other live Lisp threads stays
  before file creation. The root audit remains enabled.
- Replace the incoming GCC_EXEC_PREFIX restoration guard with a copied
  startup environment. GNU callproc.c:set_initial_environment copies the
  process environment at startup; subsequent Lisp instances use that same
  initial host snapshot and get independent mutable Lisp lists. Capture
  also precedes low-level libgccjit context acquisition. No Rust set_var or
  remove_var call is added to compilation. The real libgccjit smoke test
  checks the fresh interpreter's environment after compilation as well as
  executing generated code. Rust documents why global environment writes
  require stronger guarantees than ownership of one Lisp thread:
  https://doc.rust-lang.org/std/env/fn.set_var.html
- Reprobe the changed dynamic-obarray fixture against GNU. Fresh runs with
  native-comp-enable-subr-trampolines nil and t return respectively
  ("erc-lo2-mode" t) and (void-function byte-code). The incoming binding
  isolates the closure contract and is supported by the oracle.
- Image controls retain real bytes, cycle/sharing checks, malformed-image
  refusals and GNU-supported object refusals. The new loader is still
  cfg(test); runtime startup restoration remains D12/D13. Round trips are
  not claimed as fresh-process restoration or GNU/Emaxx pdmp byte identity.
- No original GNU assertion, oracle lock, corpus inventory, ciphertext,
  native artifact comparison or failure result is normalized or weakened.
  The Linux workflow retains its real GPG success requirement and adds
  the image/compiler integration controls. Its public GNU 30.2 diagnostic
  oracle remains separate from the historical frozen Linux binary.

Validation receipts are recorded below when completed. The scratch root
is `/private/tmp/emaxx-native-merge-sept9-round2`.

## First preflight and the native image limit

All 40 dump/root/compiler preflight checks passed, including all 23
anti-cheating checks. The separate complete-startup-image check failed on
Darwin: the ordinary startup loaded native functions, and the image writer
correctly reported its still-unimplemented native-function kind. The raw
failure is retained in `complete-image.log`; it is not a successful dump.
GNU supports these functions through `dump_subr`, `RELOC_NATIVE_SUBR` and
`dump_native_comp_unit`. D14/D15 remain missing here.

The incoming test's unconditional completed-image claim was broader than
its source-loaded Linux fixture proved. It is now explicitly a context
and capability-boundary control. It inspects actual bound native functions
before dumping: a native image must expose the precise native-function
refusal and empty output while restoring the context; otherwise it must
complete and pass every existing round-trip/root assertion. The normal
startup and native search path are untouched. There are no new ignored
checks or accepted generic errors. The unconditional supported-graph image
round trips remain separate tests. A passing Rust boundary control does
not close native image restoration or any startup compatibility mismatch.

A fresh GNU check independently confirms the gap: its ordinary Darwin
startup reports 3,930 bound native functions, then dump-emacs-portable
completes successfully (hot 14,001,620 bytes, discardable 207,064, cold
8,836,272; exit zero). The exact program, output and image are retained in
`gnu-native-dump/`. Emaxx's native-image refusal is therefore a real missing
D14/D15 capability, not shared GNU behavior or a newly closed mismatch.

## Validation scheduling

The corrected Darwin boundary test passes. Its first full-gate launch
stopped at a loopback-permission preflight before running any full-gate
tests; the socket/PTY checks use approved execution permissions. The full
serial Rust gate is assigned to Linux, whose ordinary non-AOT startup can
use the existing test-image cache. Darwin's real AOT startup takes about
48 seconds per fresh interpreter and correctly refuses template cloning
with live native ownership. Its targeted native artifacts, thread, server
and warning checks continue against that unchanged startup.

`tools/serial_grouped_gate.py` retains the official gate's whole inventory,
selectors, template policy and strict outcome checks, changing only the
schedule to one group and one libtest worker, progress verbosity, and the
matching recorded provenance. All 12 existing gate-harness tests pass.
No resource-only change is represented as reduced coverage.

## Completed focused receipts

- Linux Actions run
  [34339137375](https://github.com/rayfdj/emaxx/actions/runs/34339137375),
  candidate `24f60adbff674b9f6b20ec4d782f2be3382e4b3c`, passed all 65 focused
  Rust controls with zero failures and zero ignores. This includes the
  original unconditional complete-startup-image round trip, before the
  Darwin capability correction. Its non-AOT image therefore really completed.
  All four original OpenPGP tests passed in both GNU and Emaxx, at both
  short and historical long fixture paths. Formatting and strict Clippy
  passed. The runner had GCC 13 and libgccjit 14, exercising the compiler
  environment mismatch that motivated the startup snapshot.
- Darwin: all 40 initial dump/root/compiler checks passed, followed by the
  corrected native-image boundary test (one passed). Formatting and
  `cargo clippy --locked --all-targets --all-features -- -D warnings` passed
  with zero warnings.
- Darwin native artifact identity passed in 717.96 seconds: nine unchanged
  GNU source fixtures, eight whole `.eln` byte comparisons (including
  `comp.el` and the upstream compiler test suite) and the unchanged
  no-byte-compile policy in both editors. This is one Rust integration
  test containing nine comparisons, not nine Rust test functions.
- Darwin native-thread integration passed in 78.33 seconds: all six real
  GNU/Emaxx programs agreed, covering early signals, native suspension,
  nil signal roots, signal-data identity, condition broadcast and caller
  boundaries (one Rust integration test, zero ignored).
- Darwin upstream compiler replay
  (`target/gate/compat-harness run --file test/src/comp-tests.el --selector all
  --timeout-seconds 3600`): 178 matching outcomes, zero mismatches. Both
  editors passed 177 and failed `comp-tests-bootstrap` because the isolated
  fixture checkout lacks `lisp/emacs-lisp/comp.elc`. This is the same shared
  setup failure recorded before the merge, not a passing bootstrap test.
  Artifacts: `target/compat/run-1788951458397139000-91250`. The harness reports
  test times of 92.644 seconds for GNU and 1,406.730 for Emaxx; matching
  behavior does not establish performance parity.
- Darwin native-cache suite: all three tests passed in both editors, zero
  mismatches (`target/compat/run-1788953064009262000-94920`).
- The `test/src/thread-tests.el` replay discovered 33 tests: both editors
  passed 32 and failed `threads-join-error` with the same `ert-test-failed`,
  nil return and "did not signal an error" assertion. The harness exits one
  because its failure-message comparison retains the different thread
  addresses (`0xab7808910` versus `0x460c`). That failed receipt is preserved
  in `target/compat/run-1788953090044997000-95090`; it is not reported as a
  successful harness run. No runtime behavior, assertion or normalization
  was changed. The prior 36-test total additionally loaded
  `test/lisp/thread-tests.el` (three more tests); that file and the server
  suite continue in a separate final-stage runner, without repeating the
  completed native checks. `validation-macos.json` retains its failed stage;
  the continuation uses `validation-macos-remaining.json` and asserts the
  same tracked-source fingerprint.

- The additional Lisp thread file passed two tests and skipped the
  without-threads-build test in each editor, zero mismatches
  (`target/compat/run-1788953218350447000-95293`). Across the two files, the
  actual outcome inventory remains 34 passed, one shared failure and one
  shared skip, as before the merge.
- The first server harness replay reported seven matching outcomes, but
  **all seven were failures**: both editors refused the harness's long
  socket paths with `Service name too long`. The raw receipt remains in
  `target/compat/run-1788953242261325000-95462`; the harness's `PASS` line
  does not establish a successful server workflow.
- The unchanged server file was then run directly in both editors with
  fresh HOME/TMPDIR under `/private/tmp/ex-nm-sept9/{gnu,emaxx}`, `TERM=xterm`
  and the real GNU emacsclient. **All seven tests passed in each editor**,
  with no skips or assertion changes (`server-direct-{gnu,emaxx}.log`).
  Shortening a private socket path is an environment correction; it does
  not change the editor or the original test.
- The real terminal-client driver passed in both editors under
  `LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8`. Both produced `abXéc`, saved
  `abXéc\n`, preserved the primary frame after client-frame deletion,
  observed client exit zero and preserved the primary after disconnection.
  Screens and raw terminal logs: `/private/tmp/ex-nm-sept9/pty`.
  The commands, exits and elapsed times are in `validation-server-pty.json`.

## First full Linux serial gate

Actions run
[34341065537](https://github.com/rayfdj/emaxx/actions/runs/34341065537)
on `66d650b` passed the 65 focused controls again, then passed all five
evaluator groups serially: eval_01 351, eval_02 284, eval_03 320, eval_04
251 and eval_05 351 (1,557 tests, zero failures or ignores). The primitive
group completed with 441 passes and three failures, and the gate stopped.
The remaining library groups, binary targets and integration targets have
not yet run in this full-gate attempt. The run failed; it was not timed out.

Two failures arise before Emaxx is evaluated: GNU on this Linux build
retained a weak key at an immediate GC boundary where the Darwin oracle
released it. They are `dead_thread_results_are_rooted_only_through_reachable_thread_objects`
and `suspended_bytecode_observes_live_constant_vector_mutation`. The third,
`program_search_follows_openp_over_exec_path`, differs at its asynchronous
shell-output assertion. These failures and complete per-test logs are
retained as failures in the run artifacts. The diagnostics and fixture
corrections follow below; the completed evaluator groups are retained.

Diagnostic branch checkpoint `cfdd1e3` changes only the workflow and checks
out unchanged source `66d650b` for fresh GNU/Emaxx GC and process probes.
The equivalent Darwin probes retain the original positive release
expectations when GC runs after the setup file returns, and both editors
agree on shell output before and after draining. No additional runtime or
test change has been made at this diagnostic checkpoint.

The Linux diagnostic run
[34348556781](https://github.com/rayfdj/emaxx/actions/runs/34348556781)
confirms both revised GC observers return the original required values in
both editors: `(1 0)` and `(1 t 0)`. The original Linux GNU fixtures still
return `(1 1)` and `(1 t 1)`; Emaxx returns the release values. The isolated
Rust replay reproduces those two GNU-side failures and passes the original
argv0 test. Fresh PTY and pipe probes agree on the resolved shell path and
on fully delivered output.

An additional direct probe on Darwin shows **both** editors can report
process exit with an empty buffer, then deliver `ready` during
`accept-process-output`. GNU process.c:Fprocess_status updates raw status
without draining output; `status_notify` owns that drain. The argv0 fixture
now drains before comparing its unchanged expected value. Both GC fixtures
perform their release observation after the setup file returns, avoiding
conservative roots in active loader frames. Retention, live constant-vector
mutation and eventual release assertions remain intact; no expected value
is weakened and no runtime source changes for these corrections.

Formatting and strict all-target/all-feature Clippy pass on Darwin after
these isolated test edits. The continuation validates the prior inventory,
ignore set, complete group result logs and the original gate's zero-exit
markers before reusing the five evaluator groups. It rejects runtime or
evaluator source changes. It reruns the entire primitive group, followed
by all previously unrun library groups and binary/integration targets.

All three corrected Rust fixtures pass on Darwin: three passed, zero
failed or ignored, 64.43 seconds (`fixture-fixes.log` / `fixture-fixes.json`).
The final continuation checks out source
`8bf354bfad4cdaec3e7a49162dd3bbeec26cd23e`; workflow checkpoint `06f163a`
only controls that diagnostic execution. Runtime source remains identical
to `66d650b`. The standard workflow's overall time allowance is increased
to 180 minutes for the complete serial workload; its tests and per-group
failure checks are unchanged.

The initial full-gate candidate was
`66d650bb21e1bb38710eb90cde12c000a450eb2d`. Runtime source is unchanged from
`24f60ad`; its differences are the explicit native-image test boundary,
serial gate scheduling, workflow and documentation. The subsequent
`8bf354b` adds only the three isolated fixture corrections, the workflow's
time allowance and documentation. The final merge commit
will retain the actual main/native-comp parents, rather than adopting the
diagnostic branch's intermediate commit topology.

## Completed library gate and CLI integration follow-up

Linux continuation [34349868260](https://github.com/rayfdj/emaxx/actions/runs/34349868260)
on source `8bf354b` validated and retained the 1,557 evaluator passes, then
passed primitives 444, compat_runtime 84, tty 56 (two existing ignores),
batch 46 and lightweight 410. Thus all 2,597 active library tests passed;
the inventory remains 2,599. All three binary targets also passed (39 + 2
+ 1 tests). The full gate still **failed**: CLI integration passed 14 and
failed its merged-stdio/backtrace comparison before the other four
integration targets could run. Original logs and the failure remain in
that run's artifacts. Its full gate used runner-default Rust **1.98.0**;
the explicitly selected build/fmt/Clippy toolchain is **1.97.1**. These are
separate compiler identities, not a claim that every check used 1.97.1.

The CLI mismatch contains identical stdio ordering, error and interpreted
application frames. Emaxx additionally prints `eval-buffer` and `load`
frames beneath them. GNU comp.el:comp--call-optim-form-call removes funcall
trampolines for primitive calls in native-compiled preload functions;
therefore native and non-native startup/mule callers have different
backtraces. This fixture predates the present native-comp merge (unchanged
since `85f0c28`). A match on the Darwin preload does not establish a match
against Linux's differently compiled preload.

The fixture now defines an explicit interpreted loader caller and sets
`load-source-file-function` to nil in **both** processes for its ASCII
source. GNU lread.c's direct reader then owns loading. It still compares
exit status and **every byte of the complete output, including all loader
and startup frames**. It does not filter output, accept either frame list,
skip a platform, or change the runtime's normal loader/native compilation.
The original unconfigured Linux trace mismatch remains documented; this
stdio check no longer asserts identical implicit preloading configurations.
The direct Darwin probe matches byte for byte. A fresh Linux probe and the
four integration targets not reached previously run at source `8bf354b`
in workflow checkpoint `d61e2ca`; a focused Rust check covers the revised
fixture itself separately. No completed library or binary tests need
repetition for this private integration-test edit.

The revised CLI Rust test passed on Darwin (13.84 seconds) and Linux
(31.24 seconds, run [34355687638](https://github.com/rayfdj/emaxx/actions/runs/34355687638),
source `a612e355`). Both formatting and strict Clippy passed. Linux's
separate probe confirms `command-line-1` and `load-with-code-conversion`
are native subrs in GNU and bytecode in Emaxx, and the complete controlled
output matches. All 15 CLI cases now have passing receipts, with the other
14 retained from the preceding run.

The remaining-integration attempt
[34355419764](https://github.com/rayfdj/emaxx/actions/runs/34355419764)
passed all three `ert_runner` cases, then **failed** the native artifact
identity test at its first fixture, `comp-test-45603.el`: both ELFs were
17,032 bytes, first difference at byte 584. This is not declared harmless
or accepted as a native-comp pass. The default Rust toolchain in this
attempt was 1.98.0. Native-thread and package-lifecycle targets had not run
when Cargo stopped. The original attempt's log is retained; its temporary
ELFs were not included by that workflow's artifact upload. The follow-up
keeps complete fresh generated ELF artifacts and repeats each editor's
compile to distinguish determinism/configuration from compiler behavior.

The fresh ELF diagnostics reproduce the mismatch on Rust 1.97.1 too.
Each editor's repeat is byte-identical to its own first file. Across
editors, the only 28 differing bytes are the 20-byte ELF build ID at
584–603 and the eight ASCII bytes of `comp-abi-hash` at 12329–12336;
the complete disassembly matches. The two hashes are GNU `c10985d9`
and Emaxx `1564b906`. Applying GNU's hash_native_abi formula to the
unchanged committed subroutine signatures and the CI configure-option
string reproduces `c10985d9`; using the committed reference configure
string produces `1564b906`. The discrepancy therefore follows exactly
from CI's expanded option string, including redundant feature switches,
not a changed generated instruction or nondeterminism. These remain
failing whole-file comparisons against that differently configured oracle.

The standard Linux workflow now reads the exact configure options from
the existing generated native ABI reference. It keys the GNU build cache
by that manifest, then mechanically regenerates and compares the complete
subroutine table **and configuration metadata** from the actual built
oracle before testing. The product ABI target and generated tables are
unchanged. No ABI hash is overridden and no byte is excluded from native
artifact comparison. A fresh reference build and the original complete
native identity test validate this workflow correction.
