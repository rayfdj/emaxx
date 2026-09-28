# Hash semantics and completed macOS validation — 28 September 2026

The [complete runtime goal](runtime-representation-goal.md) remains open.
The [portable receipts](handover/2026-09-28-validation-and-hash-semantics/manifest.json)
preserve the completed validation, new failed baselines, semantic repairs and
unfinished native-layout and VM drafts. No full architecture or performance
completion is claimed.

## Completed validation of source 109

The unmodified complete macOS gate passes after 2,786.28 seconds, with unchanged
source and executable: 2,890 library tests pass, the two existing terminal
ignores remain, and all binary and integration stages pass (60 and 39 tests).
The native artifact identity integration test runs all nine unchanged fixtures.
The source is `64e18d84f64be95c58aa4287ce012b0ab38cbbb5`; its immutable library
executable has SHA-256
`6b58da1ff2209c08e0e9deb016f3b4f00b6cf852ef9a33eed954ff0068d09df6`.
These results cover the source-109 checkpoint, before the hash changes below.

The [full Linux run](https://github.com/rayfdj/emaxx/actions/runs/36422855934)
on `f3842fb56d5b65f9b8df9b13abfab92cdcc25768` still fails. Its first five groups
pass 1,606 tests; primitives passes 568 and fails one. Both source-109 test
repairs now pass. The remaining failure is the original
`suspended_bytecode_retains_operand_and_unwind_roots`: the first dead weak key
remains, producing `((1 t 1) (1 t 0))` instead of `((1 t 0) (1 t 0))`.
Live-root assertions are unchanged. Later four library groups and all Cargo
stages are unexecuted. The strict result parser also rejects the nested child
failure's extra libtest summary; its checks are unchanged.

The [569-test primitives diagnostic](https://github.com/rayfdj/emaxx/actions/runs/36422864349)
passes, but uses a different executable with `CARGO_PROFILE_GATE_DEBUG=1` and
the GC verifier enabled. It therefore does not clear the default-profile
failure. Commit `b588d5a8d1ea4ff9e85dc3af30e5bd8f202666be` adds `rust-replay` to
the existing Linux workflow. It uses the full gate's ordinary build settings,
executes without a debugger, saves the exact executable and retains inventory,
timeout and result checks. Thirteen gate-validator tests, Python parsing,
CLI help, workflow shell syntax and diff checks pass. The full gate itself
is unchanged. The [isolated original test](https://github.com/rayfdj/emaxx/actions/runs/36427648384)
and [complete original primitives group](https://github.com/rayfdj/emaxx/actions/runs/36427656115)
are running on that tooling commit.

The explicit 37-scenario terminal tail passes in 597.79 seconds, with unchanged
source and inputs. This includes the failed scenario 187 and the remaining
36 scenarios from source 108's incomplete full run. It does not retroactively
pass that run. A new complete 223-scenario attempt is still running with the
original inventory, timings and comparisons.

## Hash semantic repairs

Code commit `b00d25449f9784b81718d4e377e62ce320f64923` integrates the repairs from
`013dd37d754659dc72427fe26afbe283681bea7a`:

- GNU `fns.c:copy_hash_table` copies stored hash codes and indices. Emaxx was
  rehashing all keys, so a key mutated after insertion became spuriously
  reachable in its copy. The copy now retains the original codes and slot
  state. Same-input controls cover strings, vectors and lists, shared keys,
  mutation back to the original value, counts and capacity.
- GNU captures a custom test's function objects at construction. Emaxx was
  rereading the symbol property on every operation. The existing table slots
  now retain the selected functions through copying and GC. Replacing or
  removing the property affects subsequent construction, while a captured
  function symbol continues to resolve its current function definition.
- GNU reads the first two cons cells of the custom-test property. The former
  conversion to a Rust vector both allocated unnecessarily and rejected
  GNU-valid properties with an arbitrary tail. Construction now reads those
  two cells directly.

All three ordinary controls fail on unchanged source-108 production. On source
111, the same inputs match GNU exactly using a freshly built ordinary editor
and image. All six processes exit zero with empty stderr and unchanged source,
executable, image and inputs. GNU is the pristine source-matched candidate with
the committed native ABI; its executable still differs from the frozen Darwin
pin. No pin or expected output changed.

Source 111's debug and release selections each pass 62 tests and fail one,
with zero ignored. The failure is the unchanged, unpublished native-layout
probe described below; these selections must never be reported as full passes.
The 62 passes include the new semantic regressions and existing hash, purecopy,
GC and dump controls. Formatting, all-target/all-feature compiler checks,
Clippy with warnings denied and diff checks pass. The committed semantic
change files match the tested snapshot byte for byte. The native-layout test
remains a separate unfinished draft, as in the earlier portable continuation;
no existing published assertion or selector was removed.

## Unfinished architecture and GC work

The GNU C layout probes establish a 72-byte `PVEC_HASH_TABLE` object: header
tag 14, zero inline Lisp slots, eight remaining payload words, entry-array
pointer at offset 24, count at 48, capacity at 56 and flags at 61. Emaxx still
exposes a host record with tag 34 and per-interpreter hash state. Source 110's
native regression fails on that header before attempting raw entry reads or
stores; source 111 retains the same failure. Its exact test patch and receipts
are packaged. `hash_arrays_draft.rs` is an unintegrated, uncompiled component;
descriptor ownership, roots, copying, Value dispatch, native callbacks,
collection, images and primitive wiring are still required. It is not a
working migration and must not replace the validated source as-is.

The separately packaged source-113 patch removes five remaining error-payload
unbox/rebox sites in the VM and evaluator and roots a boxed thrown value across
comparison. It is a candidate for reducing inactive stack storage, following
the earlier demonstrated evaluator retention repair. Validation is still
running when this draft is packaged; it is neither a proven explanation nor
a validated fix for the Linux failure. Its original tests are unchanged.

Continue the Linux diagnosis and the real hash allocation migration. Preserve
all original failures, the pending complete terminal result, final-source
macOS/Linux and frozen requirements, public ownership and accounting work,
the adversarial audit, and the unchanged locked performance criterion. The
earlier measured 3–4× GNU body costs remain unfavorable evidence.
