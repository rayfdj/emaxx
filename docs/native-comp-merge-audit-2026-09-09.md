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
