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
