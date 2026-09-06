# Handover — native compilation checkpoint (2026-09-02, Asia/Jakarta)

**THIS IS THE CURRENT HANDOVER.** It supersedes
`docs/handover-2026-08-28.md` for current work. The older handovers remain
useful history, but their statement that Emaxx models an Emacs build without
native compilation is no longer the active design.

## Resume here — pushed 2e0b5cf and merged main (2026-09-06)

Repository `/Users/nbmhqa186/native/emaxx`, branch `native-comp`.
**Latest implementation checkpoint: `7bfce9b` — match GNU's
`Fgarbage_collect` symbol mode; handover evidence is pushed in `2e0b5cf`.**
It retains the ordinary GC threshold correction from
`bc8402d`, the `origin/main` merge, and the `pipe`/`fcntl(FD_CLOEXEC)` fallback
required because macOS has no `libc::pipe2`; it additionally temporarily
binds `symbols-with-pos-enabled` to nil around public GC exactly as GNU
`alloc.c:Fgarbage_collect` does. The focused census regression, anti-cheat
(18/18), formatting, all-target checking, and strict all-feature Clippy pass.
The merged-tree identity and normal native gates also pass, as recorded below.

The implementation checkpoint is pushed; `origin/main` was fetched and merged
before the documentation push, and the worktree is clean. Continue from the
remaining L08 source obligation.

The next open native contract remains L08, GNU `alloc.c` live-byte accounting.
This checkpoint closes only the public GNU C layout-size portion; the census
classes, allocator accounting, free-list columns, and broader GC parity remain
open. Continue from the ledger's exact C source obligation; do not infer that
the passing focused gate closes L08.

Checkpoint-specific validation for `042074a`/`57b0a71`: the nine-rung
unchanged-source identity ladder passed, including byte-identical `comp.el`,
and the normal upstream native selector passed 177/177 with zero mismatches.
The Emaxx execution phase took 1,395.899 seconds versus GNU's 89.751 seconds;
the performance gap remains open. The full `check-all` selector additionally
includes the separately tagged bootstrap case and is not the normal 177-case
gate.

Merged-tree validation for `8883607`: the nine-rung identity test passed all
fixtures, including byte-identical `comp.el` (881,800 bytes), in 244.19
seconds on the preceding merged tree; the rerun on `8883607` completed in
303.20 seconds. After `origin/main` advanced, the
same normal upstream native selector passed 177/177 with zero mismatches;
Emaxx took 1,302.477 seconds versus GNU's 82.642 seconds. After
`origin/main` advanced, the identity test became non-ignored, so the correct
command is `cargo test --release -j1 --test native_comp_identity --
--nocapture --test-threads=1` without `--ignored`.

Post-correction validation for `2e0b5cf` (subject `7bfce9b`): the normal
upstream native selector passed 177/177 with zero mismatches. Emaxx took
1,266.137 seconds in the test phase versus GNU's 158.694 seconds; setup took
68.597 versus 4.035 seconds. The harness also recorded the remaining
performance regression, not a semantic mismatch.

**L03 bounded placement correction (2026-09-06):** GNU `eval.c:eval_sub`
performs `maybe_quit`, then `maybe_gc`, then increments the evaluation depth.
Rust now invokes the active native GC trampoline in that same position before
form dispatch; outside a native activation the helper is the corresponding
fast no-op. A Rust-only control allocates past the configured native threshold
inside an active native call and evaluates a cons form; the old placement
would perform no collection, while the new path observes the collection at
the eval boundary. The focused placement probe, 14 native-GC controls,
formatting, all-target checking, and strict Clippy pass. The nine-rung
unchanged-source identity ladder passes, including byte-identical `comp.el`,
and the fresh isolated native execution gate passes 177/177 with zero
unexpected results in 1,245.17 seconds. This accepts the placement half of
L03; complete live-object census/accounting parity remains open under L08.

**L06 target-resolution audit (2026-09-06):** GNU `eval.c:funcall_general`
resolves symbol indirection before selecting `funcall_subr`,
`funcall_lambda`, or the invalid-function/error paths. The native word path
now has focused coverage for an aliased builtin, an interpreted lambda, and
an invalid function, with the exact GNU error value and clean evaluator
unwind; all eight native-Ffuncall tests pass. Autoload loading and the wider
callable-class matrix remain open, so L06 stays partial.

**L07 arity audit (2026-09-06):** GNU `eval.c:funcall_subr` now has focused
coverage for the direct fixed-optional, MANY, and UNEVALLED branches: native
Ffuncall preserves nil padding, forwards the complete MANY vector, and rejects
special forms with the resolved subr object. The full 0..8 arity matrix and
remaining subroutine edge cases remain open.

**L08 bounded layout correction (2026-09-06):** GNU `alloc.c` reports the C
allocator layouts, not the host implementation's struct sizes. The configured
64-bit GNU build reports cons=16, symbol=48, string=32, vector=16,
vector-slot=8, float=8, interval=56, and buffer=992 bytes. Rust now uses those
constants both for `LiveObjectCensus::total_bytes_of_live_objects` and for the
public `garbage-collect` rows; a focused regression pins every SIZE column.
The live census model and GNU free-list/accounting parity remain incomplete.

**L08 conditional collection correction (2026-09-06):** GNU
`Fgarbage_collect_maybe` calls GC iff `since_gc > gc_threshold / factor` for a
nonnegative factor, returning `Qt` only when it collects. Rust now mirrors
that boundary for both the active native heap and ordinary interpreter state,
including GNU's dynamic threshold/percentage retuning and wrong-type error
for negative factors. The active native control, ordinary dynamically-bound
threshold control, existing conditional-GC contract, anti-cheat, check, and
Clippy gates pass; full native/artifact reruns for this final correction are
still pending.

**L08 public GC mode correction (2026-09-06):** GNU
`Fgarbage_collect` dynamically binds `symbols-with-pos-enabled` to nil for
the mark/sweep and restores the caller's value before taking the `gcstat`
snapshot. Rust now follows that scope around the public `garbage-collect`
primitive, with focused restoration coverage. This is separate from the
remaining allocator free-list and cumulative-counter gaps.

### Historical September 6 continuation — EQ/live-flag and reader work

The live-flag dependency described below is now implemented, not just
planned. The interpreter owns a stable `Box<Cell<bool>>`; native relocation
and ordinary/native readers use it. NativeRuntime's snapshot and per-call
lookup are removed. Existing forwarding paths handle stores, binding and
restoration, buffer selection and detachment. No backend/codegen, GNU source
or Elisp changed. Five existing Rust fixtures now set the real flag cell
instead of faking it with a lexical Env; their expectations are unchanged.

Evidence in `/private/tmp/emaxx-native-eq-words.aEIw2H`:

- `live-flag-red.log`: both directions of inner-native binding fail with
  the old snapshot (2 failed, 0 passed). The earlier
  `live-flag-red-build-error.log` executed no tests.
- `forwarding-focused.log`: **128 passed, zero failed, one preexisting
  manual timing probe ignored**, 87.06s after a 2m09s serial release build.
  This includes all 18 adversarial checks, new relocation/alias/local/copy
  tests, native binding/store controls, detached/lexical-shadow controls,
  earlier GC controls and existing positioned-symbol integration tests.
- `forwarding-check.log`: all-target check clean. `final-clippy.log`:
  strict all-target/all-feature Clippy clean. The initial Clippy log found
  12 test-only `unwrap()` calls; they now have explanatory `expect()`
  messages, not lint suppressions. Formatting and diff whitespace checks
  are clean.
- Final rerun/identity pipeline **75946 is terminal, exit 0**. The same focused
  selection passed again in `final-focused.log`: **128 passed, 0 failed,
  1 manual timing probe ignored**, 86.97s. `identity.log` then passed all nine
  fixtures in 213.11s: eight entire byte-identical `.eln` files through the
  881,800-byte `comp.el`, and one correct no-artifact result. No candidate
  timing exists yet.
- `gnu-exec-negative-control.log`: a fresh run of the existing GNU-executable
  fence rejects GNU with exit **71**. Native execution inputs are prepared
  under `execution/source`; the test file and complete resource directory
  compare unchanged with GNU. Subject/oracle homes and temporary directories
  are separate. The fenced candidate run (`execution/subject.log`, session
  **73568**, exit **255**) failed before reaching any tests: its child
  rejected GNU comp.el's serialized context with `invalid-read-syntax` while
  compiling the first helper. The queued GNU suite did not start.
- The saved pushed `ae12db4` binary fails at the same stage with the same
  input in separate fresh directories (`execution/before.log`, session
  **46824**, exit **255**). Thus this failure predates the current correction;
  do not infer which earlier commit introduced it without more evidence.
- GNU successfully byte-compiled an unchanged copy of the candidate's
  rejected context (`execution/gnu-reader.log`, exit 0; only the expected
  missing lexical-binding header warning). `execution/gnu-reader/context.el`
  is 750,498 bytes, SHA-256
  `adb349f9bfe16b3d36b797e43bb427b00378d66888910f03389514ebac81a74d`;
  GNU produced a 750,740-byte `context.elc`. This proves GNU accepts these
  bytes, not full frontend/execution parity.
- Candidate release editor SHA-256:
  `374895def0afc4218c58e460cc802dea6964edbc3d9df6da8f86e62a5f34d24c`.

**Reader interruption resolved (2026-09-06):** the preserved GNU-generated
context is byte-identical on both sides and the ordinary `load_file_strict`
path materializes it correctly. The actual failure was in
`primitives/loading.rs:eval_buffer_forms`: its default `read` path parsed and
evaluated forms without the reader-dependent materialization step. GNU creates
`#s` records, hash tables, and circular `#[...]` objects before evaluation;
Emaxx therefore reached native compilation with `ReaderForm` markers and
reported `invalid-read-syntax`. The path now calls
`materialize_read_object_literals` after interning each form. The focused
loading suite passes **21/21**, and the exact previously failing generated
context now reaches native compilation and exits **0**. The temporary absolute
path diagnostic was removed.

Next: restore genuine native execution in the full fenced suite and complete
the EQ/live-flag acceptance gates; then repeated
direct post-startup before/after/GNU measurements, final audit and checkpoint
push. The recorded `ae12db4` before binary is still the baseline. After pushing,
fetch/integrate main, then continue the finite dump prerequisites.

**Checkpoint acceptance completed (2026-09-06):** the fresh fenced native
execution run passed **177/177**, zero unexpected, exit 0, using unchanged GNU
`comp-tests.el` and a fresh isolated HOME/TMPDIR under `LANG=C`. The nine-rung
unchanged-source identity test passed all fixtures, including byte-identical
artifacts for the full `comp.el`; the focused native runtime suite passed
70/70 with one separate timing probe ignored. The final adversarial audit
passed 18/18, loading tests passed 21/21, formatting was clean, all-target/all-
feature checking was clean, and strict all-target/all-feature Clippy was clean.
The reader-materialization correction and the EQ/live-flag work are ready for
this checkpoint commit. No new timing claim is made by this bounded fix.

The C review recorded an independent V05 bug: `set` and `set-default` could
return/notify watchers with a normalized bool instead of GNU's original
NEWVAL. The bounded correction below fixes and tests that contract. Full
symbol storage/redirect ownership and native object representations remain
open; this is not full forwarding parity or dump readiness.

**Post-merge V05 correction (2026-09-06):** the bounded follow-up now
preserves GNU's original NEWVAL for `set`/`set-default` return values and
watcher callbacks, and canonicalizes the GNU `set-default` operation to
`set` before callbacks. Focused
watcher coverage, the 18-case adversarial audit, formatting, all-target
checking, and strict Clippy are clean. The post-correction fresh native
execution gate passed 177/177 with zero unexpected results, and the nine-rung
identity ladder passed all fixtures, including byte-identical `comp.el`. This
correction is ready for its checkpoint commit. The identity ladder completed
in 249.06 seconds; the fresh native suite completed in 1,365.02 seconds.

**L05 debugger-on-exit correction (2026-09-06):** GNU `eval.c:Ffuncall`
checks `backtrace_debug_on_exit` after the callee returns and calls
`call_debugger (list2 (Qexit, val))` before dropping that activation frame.
The native Ffuncall path now retains the returned native word, decodes the
same Lisp value, invokes an evaluator-level `call_debugger`, and re-encodes
the debugger's return value. The helper follows GNU `eval.c:call_debugger` by
clearing `debug-on-next-call` and dynamically binding
`debugger-may-continue`, `inhibit-redisplay`, `inhibit-debugger`, and
`inhibit-changing-match-data` around the callback. Focused L05 and surrounding
native-Ffuncall tests pass 8/8, the adversarial audit passes 18/18, all-target
checking and strict Clippy are clean, all nine unchanged-source identity
fixtures pass including byte-identical `comp.el`, and the fresh isolated
native execution gate passes 177/177 with zero unexpected results in
1,253.37 seconds. This closes the bounded L05 contract; redisplay-specific
top-level unwinding remains outside the headless native runtime scope.

The full active goal and seven milestone states below remain unchanged:
GNU-faithful runtime foundations, a real portable startup image as soon as
its actual correctness prerequisites permit, then remaining native-comp
correctness/performance. No dumper exists and no whole milestone is complete.
Read the full objective attachment when resuming; keep every boundary and
de-cheating requirement, not just this checkpoint's focused tests. The
attachment is
`/Users/nbmhqa186/.codex/attachments/cc60fc9a-8d38-462a-8cb1-10ea579d9827/pasted-text-1.txt`.
The repository summary below and the non-negotiable rules later in this
document preserve the working contract if that local attachment is absent.

### Current goal and milestone states

The immediate major outcome is a **separate ordinary Emaxx process restoring
a real persistent image, with native functions, without replaying preload**.
Reach it once concrete correctness prerequisites permit; do not wait for
every runtime optimization. Then finish native-comp correctness/performance.

| Milestone, in dependency order | State and next obligation |
|---|---|
| 1. GNU dump contracts and baseline | Partial. The existing finite inventory is D01–D20 (PRE/BUILD/ENABLE/LATER), not just D01–D15 as older summaries said. Ordinary startup is corrected and direct post-startup measurements exist. Complete the relevant contract verification. |
| 2. Shared object and symbol foundations | Partial. Correct authoritative identity, storage and shared mutation (R02c/R03); plain value cells, aliases, buffer-local and forwarded fields (V02–V05); affected closure/symbol roots and lifetimes. The current EQ/live-flag finding belongs here. |
| 3. Function restoration and GC readiness | Partial. GC graph/weak-entry/sweep ordering is corrected. Callable identity versus process addresses, complete roots, weak/finalizer/pure-object behavior and accounting remain obligations. |
| 4. Portable writer and loader | Not implemented. Follow GNU traversal, sections, relocations, roots, fingerprints and rejection behavior; use unchanged GNU loadup and Rust round-trip tests. |
| 5. Native restoration and ordinary dumped startup | Not implemented. Reopen libraries and reconnect functions in GNU order; separate restored persistent state from fresh-process initialization. Do not replay native top levels that GNU does not replay. |
| 6. Validate restored startup and accelerate tests | Not reached. Genuine native suite, entire-artifact ladder, sharing/GC/startup/rejection checks, broader integration gate and representative startup/test measurements. Template cloning is not dumping. |
| 7. Remaining native correctness/performance | Open. Finish deferred runtime contracts and achieve fairly measured GNU-comparable hot-path performance; neither selected artifact identity nor 177 passing tests closes the goal. |

Use the [finite dump prerequisite inventory](pdump-c-parity-ledger.md)
alongside the [native contract ledger](native-comp-c-parity-ledger.md).
Only concrete identity, ownership, root, relocation or startup correctness
dependencies may delay the image milestone. Argument-copy elimination and
other performance-only work do not automatically belong before dumping.

### What the last pushed commit changed and proved

What changed:

- GNU `data.c:Ftype_of` does not implement old-struct compatibility. Removed
  Rust's partial policy and its per-query mode-variable lookup. GNU's
  unchanged `cl-lib.el` advice remains the owner and works without it.
- GNU `Fcl_type_of` inspects object tags, not public fixnum-limit variables.
  Removed those two lookups from every query, reused the existing tagged
  integer limits, and preserved the allocated bignum subtype.
- Removed the two now-unused helpers. Added two Rust-only ordinary/native
  subr controls and made the adversarial inventory require them. No Elisp,
  GNU source, ABI, cache, GC threshold or ownership model changed.

Verified evidence, `/private/tmp/emaxx-native-type-tags.E73e1Z`:

- Both new controls fail on the previous code (`red-controls.log`). The
  earlier short exact selector matched zero tests, not a pass.
- Final focused selection: **95 passed, zero failed, one preexisting manual
  timing probe ignored**, including all 18 adversarial checks and unchanged
  old-struct advice integration. Formatting/check/strict Clippy are clean.
- Ordinary loading of unchanged GNU `cl-lib-tests.el` matches GNU: **45
  passes, the same one expected failure, zero unexpected failures/skips**.
  Emaxx ran with GNU executable launches forbidden; the negative control
  rejects GNU with exit 71. No authored Elisp or alternate runner was used.
- All nine artifact fixtures pass: **eight entire byte-identical `.eln`
  files through `comp.el`, one correctly absent artifact**. The 177-case
  native execution suite was not rerun for this bounded correction.
- Two post-startup rounds: corrected CPU **48.97 / 49.78s**, previous checkpoint
  **52.06 / 52.26s**, GNU **8.37 / 8.33s**, including compiler children.
  Paired reductions are **5.94% / 4.75%**; baseline variation is 0.39%.
  The evidence supports roughly 5–6% less CPU for this input, not a precise
  universal speedup. Rust is still about **5.9x GNU by CPU** (5.8x elapsed).
  Preload reconstruction is outside every timing window. All six measured
  artifacts are whole-byte identical, 881,800 bytes, SHA-256
  `f2752387ccbf72e1f21def74a0e438e8890d06e86b36ab30229c39ec79821c83`.
- Corrected editor SHA-256:
  `d8f0d55a78719caa23ca07dd667641a440c8a212798f12afbca506f2d2517a3c`.
  Saved baseline `before-emaxx` in the same directory has SHA-256
  `15d1486fa62578847331fde3ea934726089f51d8266368d6ae0a51c7095adc10`.

Next area: **native object conversion/synchronization, R02c/R03**, starting
from the corrected editor's completed post-startup profile, not a fresh
guess or another cache. `after.sample`, `profile.log`, and
`after-attribution.txt` are in the type-tags directory. The worker is
`Thread_24937884`, 40,691 samples; the main thread is blocked in join.
All compiler/sampler jobs are terminal (profile session `10717`, PID `76435`;
timing session `7914`; identity session `97404`). Do not restart them.

The after-profile confirms that old-struct probing disappears and sampled
`cl_type_value` work drops from 1,893 to 66 samples. Remaining nearest-owner
regions are native object bridge **8,436**, bindings/variables **5,066**,
other GC **1,246**, other work **25,943**; these regions do not overlap.
Specific paths do overlap: `reconcile_mirror` 3,074, `native_eq` 2,459,
`decode_program` 1,891. Read their actual callers before selecting the next
bounded unit. `NativeHeap::decode_inner` is now the leading active leaf.

The first R02c caller audit is complete: direct generated `funcall`, `apply`,
and `mapcar` preserve native words already. Remaining round trips are at the
Rust-owned typed boundaries for public arguments, direct byte-code calls,
hash-table key/value extraction, and relocations. Do not add a side cache;
select one boundary only after its runtime-owned word transport, GC roots, and
mutation behavior are specified.

The external analyzer at
`/private/tmp/emaxx-native-poststartup.mXNHW4/attribute_sample.py` accepts a
capture path and worker-thread name. It accounts for every worker sample
and validates the leaf totals against the sampler's own summary (491
collapsed leaves checked for the after-profile). The after-profile spans
the 58.55-second instrumented operation; it is not another unprofiled timing
sample. Its artifact also compares whole-byte identical with GNU.

The selected bounded unit and its unfinished state follow. No optimization
after `ae12db4` has been accepted or benchmarked.

### Discovery history — EQ identity and the live positioned-symbol flag

This records the September 5 starting point. The September 6 continuation
above supersedes its implementation/test status and pending-file list.

In plain language: GNU can answer “are these the same object?” by comparing
object words. Rust sometimes reconstructs an unrelated list and reads its
contents first. It also treats two separately allocated, equal-valued big
integers as the same object on this path, although GNU `eq` does not.
These are concrete C deviations, not a proposed new compiler algorithm.

GNU owners already inspected:

- `data.c:Feq` and `lisp.h:EQ`, `PSEUDOVECTORP`, `SYMBOL_WITH_POS_P`,
  `XSYMBOL_WITH_POS_SYM`, `maybe_remove_pos_from_symbol`: unwrap only an
  actual positioned symbol when the live C flag enables it, then compare
  object words. Do not traverse unrelated cons/vector contents.
- `alloc.c:build_symbol_with_pos`: stores bare symbol and position
  separately; EQ needs only the bare-symbol field.
- `fns.c:Fmemq`/`Fassq` use EQ; `Feql` separately owns numeric bignum/float
  comparison. Do not change EQL into identity comparison.

Pending files, all belonging to this unfinished unit:

- `src/lisp/native_comp/runtime.rs`: candidate `native_remove_symbol_position`
  and word-based EQ, plus three Rust controls. Positioned-symbol fields
  still use the existing typed/native bridge; this does not finish R02c/R03.
- `src/lisp/primitives/values.rs`: ordinary bignum EQ uses allocation identity.
- `src/lisp/types.rs`: `SharedBigInt::ptr_eq` supports that identity check.
- `src/anti_cheat.rs`: requires the three new regression controls.
- `docs/native-comp-c-parity-ledger.md`: P13/R03 findings and unfinished status.

Evidence is in `/private/tmp/emaxx-native-eq-words.aEIw2H`:

- `red.log`: two controls ran against old production code and **both failed**
  as intended: unwanted cons materialization, and ordinary/native EQ both
  returning `t` for distinct bignums. This is failing-control evidence,
  not two passing tests. `red-build-error.log` is an earlier compile failure,
  corrected before that run; no tests executed in that first attempt.
- `check.log`: the candidate passed all-target compiler checking, zero
  warnings (9.09s). Formatting was applied. **No green test run, strict
  Clippy acceptance, new artifact run or candidate timing exists yet.**
- `native_eq_unwraps_only_positioned_symbols_and_preserves_identity` was
  added after the red run and has not run. It covers both direct/native-subr
  routes, enabled/disabled handling, interned/uninterned symbols and identity.
- `before-emaxx` is the saved pushed `ae12db4` binary. It and the current
  `target/release/emaxx` still have SHA-256
  `d8f0d55a78719caa23ca07dd667641a440c8a212798f12afbca506f2d2517a3c`.
  The release unit-test binary is from the red run, not the candidate.

**The source-faithfulness audit found a dependency before acceptance:**
`NativeRuntime::symbols_with_positions_enabled` is a separate `Box<bool>`
copied from the Lisp variable only at `NativeRuntime::invoke` entry. Native
`runtime_specbind`/ordinary writes do not update that box. The native loader
also points generated code's relocation at this snapshot. A binding changed
inside the native call can therefore leave both the helper and generated
code reading stale state. The old EQ fallback's second Lisp lookup masked
one direction of this problem; removing it can expose a regression.

GNU instead initializes the actual forwarded C boolean to false in
`data.c:syms_of_data`; `do_symval_forwarding` reads it and
`store_symval_forwarding` writes it immediately (nil versus non-nil).
`comp.c` connects `F_SYMBOLS_WITH_POS_ENABLED_RELOC_SYM` directly to
`&symbols_with_pos_enabled`. Rust needs that same live state contract,
not an entry-time snapshot or another refresh cache.

### Original dependency checklist (steps 2–5 now implemented and focused-tested)

Use the current continuation above for the next action. Keep the remaining
acceptance requirements here; do not redo completed source review or red runs.

1. Read this resume section, the objective and the current diff. Preserve
   the unfinished edits. Confirm HEAD/origin and actual process state.
   The recorded red-build/check jobs finished; do not restart old jobs or
   mistake the pushed release executable for a build of the candidate.
2. Finish the GNU forwarding review before changing the flag owner:
   `data.c:set_internal` (including detachment on makunbound),
   `eval.c:specbind`/unbinding, aliases and buffer-local forwarding. Compare
   `src/lisp/eval/variables.rs`'s existing normalization, update, read,
   refresh and detached-forwarding machinery, and `eval.rs` field ownership.
3. Add Rust-only failing controls for changing the flag **inside** an active
   native call in both directions and restoring it. Use existing
   `runtime_specbind` and `runtime_unbind_n`; the latter takes a tagged Lisp
   integer count, not an untagged machine integer. Check ordinary stores
   and the pointer read by loaded code as well, not just the EQ helper.
4. Implement one stable, live forwarded boolean owner and connect the
   loader/runtime to it. A boxed `Cell<bool>` owned by the interpreter is
   a proposed Rust representation, **not yet implemented or accepted**.
   Verify lifetime and FFI access, normalization, binding/unbinding,
   aliases, buffer switching and detachment against GNU. Do not retain a
   second runtime snapshot, add per-subr refreshes, or alter generated
   machine code to compensate. Keep raw initialization false; later GNU
   Elisp changes remain separate.
5. Re-review the entire EQ candidate against C, including bignum identity
   and native argument lifetimes. The existing cheap “neither operand is
   vector-like” guard was removed by the draft: check preserving it rather
   than adding unnecessary helper work. EQL conversion overhead remains
   open; do not claim it was fixed by EQ.
6. Only after faithfulness review: focused red/green tests, adversarial
   de-cheating checks, clean formatting/check/strict Clippy, relevant whole
   artifact and execution tests. Expand coverage for ownership/relocation
   transitions where needed. Do not label historical 177-case results fresh.
7. Then measure serial, order-balanced before/after/GNU post-startup work.
   Use the saved `ae12db4` baseline and the existing external observer
   `/private/tmp/emaxx-native-poststartup.mXNHW4/native_phase.py`; validate
   its entry/return/exit evidence and whole artifacts. Exclude preload
   directly, account for compiler children and loaded-host variation.
   Observer logs are diagnostics, not a new editor interface or Elisp runner.
8. Update both ledgers and this handover with accepted evidence. At a
   natural verified checkpoint, state the exact scope, commit and push
   under the standing authorization, then fetch/integrate main. Resume the
   finite dump prerequisites and real writer/loader, not an unbounded
   optimization campaign.

All boundaries below still apply: **C-to-Rust only; unchanged GNU Elisp;
no authored Elisp even for tests; no GNU runtime delegation; no approved
semantic deviations; entire `.eln` identity; adversarial audits; zero
warnings.** Artifact identity does not prove arbitrary execution, GC or
dump correctness. No portable image exists, and the overall goal is active.

The sections below preserve historical checkpoints and standing rules.
For conflicting uses of “current”, “pending” or “last pushed” in that history,
the resume section above and the actual Git state take precedence.

## Previous GC checkpoint — pushed a94d410 (2026-09-05)

Repository: `/Users/nbmhqa186/native/emaxx`, branch `native-comp`.
Last pushed implementation commit:
**`a94d4105842eb8cb1d5292f0fb975653a8546f64` — Finish shared Lisp graph
marking before native GC sweep**. It is on `origin/native-comp`. After
pushing, `origin` was fetched; current main
`84f342a132eb041286d98cf7afd6551996db1529` was already an ancestor, so no
empty merge was made. The worktree was clean before this requested handover
update. No implementation change is pending after that checkpoint.

GNU reference: clean 30.2 source at
`/Users/nbmhqa186/projects/emacs` (sibling `../emacs`), revision
`636f166cfc86aa90d63f592fd99f3fdd9ef95ebd`. Do not modify it.

### Active goal, not yet complete

The goal is **GNU-faithful runtime foundations, real portable dumping,
then completion of native-comp correctness and performance**. The user's
full objective is
`/Users/nbmhqa186/.codex/attachments/cc60fc9a-8d38-462a-8cb1-10ea579d9827/pasted-text-1.txt`;
read it when resuming. Preserve its scope, not just the currently passing
tests. The goal remains active; no major milestone has been declared done.

| Milestone | Actual state at this checkpoint |
|---|---|
| 1. GNU dump contracts and baseline | Finite D01-D15 inventory mapped; independent ordinary startup corrected; post-startup timing now measured directly. Contract verification remains partial. |
| 2. Shared object and symbol foundations | Partial. Shared vector/cons work and the GC graph correction are retained. Native/typed ownership and symbol value/alias/local/forwarded storage remain open. |
| 3. Function restoration and GC readiness | Partial. Mark/weak-entry/sweep ordering is corrected, not complete root, finalizer, pure-object, accounting or callable-restoration parity. |
| 4. Portable writer and loader | Not implemented. No persistent image exists. |
| 5. Native restoration and ordinary dumped startup | Not implemented. Ordinary startup still reconstructs preload state through unchanged GNU loadup. |
| 6. Validate restored startup and accelerate tests | Not reached. In-process template cloning is not a dumped image or a substitute for isolation proof. |
| 7. Remaining native correctness/performance | Open. Native artifacts pass the ladder, but identified runtime contracts and substantial post-startup performance work remain. |

The immediate major goal remains a fresh ordinary process restoring a real
image, including native functions, without replaying preload. Only concrete
ownership/root/relocation/startup correctness prerequisites may block that
milestone; not every optimization in the native ledger belongs before it.

### What the pushed GC correction proves

GNU `alloc.c:process_mark_stack` traverses cons/vector references before
`garbage_collect` finishes weak-table marking, removes dead weak entries,
and sweeps storage. Rust previously swept native cons storage before its
separate typed walk discovered a rooted vector's cons element. The old-code
control fails; the retained correction keeps those walks connected before
sweeping, uses current cons fields in car-first order, and removes dead
weak entries before native storage is freed.

- **89 focused tests pass**, zero failures; one preexisting manual timing
  probe is ignored. These include seven new Rust GC controls and all 18
  adversarial checks. A separate fresh pre-push adversarial run passes 18/18.
- Formatting, all-target compiler check and strict all-target/all-feature
  Clippy pass with zero warnings and no new suppressions.
- **All nine unchanged-source artifact fixtures pass**: eight complete
  byte-identical `.eln` files through `comp.el`, and one artifact correctly
  absent because of GNU's no-byte-compile policy. The current `comp.el`
  artifact is 881,800 bytes. The canonical 177-case execution suite was
  **not rerun for this bounded GC checkpoint**; do not present older suite
  results as fresh evidence for it.
- The ordinary GC subr is exercised through the native ABI by Rust controls.
  Artifact compilation executes the unchanged GNU frontend. These checks
  do not prove full GC, arbitrary-input native correctness or dump safety.
- No Elisp, GNU source, fixture selection or whole-file comparison changed.
  Emaxx measurement runs succeeded with GNU executable launches forbidden;
  the fence's GNU negative control fails with the expected exit 71.

Open GC work includes reference-count-derived external handle roots,
native collection skipped without an active native stack boundary,
remaining duplicate representations, full root/type census, pure objects,
finalizers and exact public accounting. Do not call GC or dumping finished.

### Correct performance baseline

The earlier approximately 8x comparison included preload reconstruction and
is **not** a compiler-speed result. The accepted observation starts at
unchanged `batch-native-compile` entry and stops at its actual return, after
startup/library loading and around the full frontend/backend operation.

| Editor | Two post-startup elapsed times | Two CPU times, including waited-for children |
|---|---|---|
| Pushed GC implementation | 54.18s / 52.44s | 53.64s / 52.22s |
| Saved a92e620 baseline | 55.92s / 52.32s | 55.15s / 52.11s |
| GNU | 8.59s / 8.72s | 8.43s / 8.46s |

Paired CPU changes are -2.73% and +0.20%; the baseline itself varies 5.82%.
There is no consistent measured GC regression or precise speedup. The
remaining post-startup gap for this input is about **6.2x by elapsed time**
(6.3x by CPU), under the same external observer. All six artifacts are
whole-byte identical. No startup subtraction or helper Elisp was used.

Evidence:

- `/private/tmp/emaxx-native-gc-graph.tkKNPk`: failing old-code control,
  focused implementation checks, current all-nine artifact run, and saved
  `before-emaxx` from a92e620.
- `/private/tmp/emaxx-native-poststartup.mXNHW4`: validated external
  `native_phase.py`, clock control, six final timing logs/artifacts,
  `checkpoint-{check,clippy,focused}.log`, `prepush-adversarial.log`, and
  the fresh fenced GNU negative control.
- Editor SHA-256:
  `15d1486fa62578847331fde3ea934726089f51d8266368d6ae0a51c7095adc10`.
  More hashes, rejected preliminary measurements and limitations are in
  [the dump ledger](pdump-c-parity-ledger.md).

### Next work, explicitly requested by Ray

**Find and fix the biggest contributor to post-startup Rust native-comp
slowness, starting from deviations from GNU C behavior.** Do not assume it
is GC, native-object conversion or symbol lookup from older profiles.

1. Inspect the completed fresh post-startup profile below, from ordinary
   unchanged `comp.el` compilation on a94d410. Rank actual costs; distinguish
   total inclusive stacks from exclusive leaf work. Obtain a matching GNU profile
   where needed to explain the discrepancy.
2. Locate/read the dominant path's GNU C owner and Rust counterpart. Record
   the exact extra work or wrong ownership/dispatch behavior. No speculative
   cache, mixer, threshold or Rust-only replacement algorithm is authorized.
3. Add a focused Rust failing control where applicable; implement GNU's
   behavior idiomatically in Rust. C-faithfulness review precedes heavy
   verification/timing. Follow with focused correctness, adversarial audit,
   relevant entire artifacts/execution and repeated post-startup comparisons.
4. Preserve the finite dump prerequisite list and full goal. Checkpoint/push
   naturally, then fetch/integrate main before starting another checkpoint.

A first external profile **completed successfully**: the ordinary editor
and sampler both exited 0. Exec session `63869` and editor PID `74200`
are terminal; do not poll or restart them. No background job remains.
Log:
`/private/tmp/emaxx-native-poststartup.mXNHW4/profile-current.log`, with
sample file `profile-current.sample` beside it (189,081,316 bytes).
The sampler started only after the validated compiler-entry boundary and
sampled 60 seconds of the 63.85-second instrumented operation, not its final
few seconds. The compiler worker (`Thread_24894036`) has 43,902 samples;
the main thread waits in `pthread_join`. The profiled
artifact still compares whole-byte identical with GNU. This run is for
attribution, not an additional unprofiled timing sample.

The exclusive leaf summary begins at line 327557 of the sample file.
Initial observed leaf counts include SipHasher write 2,283, native
`decode_inner` 1,996, `Value` destruction 1,713, `global_binding_value` 1,305,
`Value::clone` 1,192, and `reconcile_mirror` 818. These are leads, not proof
that any one subsystem is the biggest contributor: inspect their callers
and avoid double-counting inclusive stacks. The 43,902 `__ulock_wait`
samples belong to a blocked thread and must not be ranked as active CPU
work. A shared/merged hash symbol's `regex_automata` suffix is not proof
that regexp matching owns that cost. No next optimization is implemented
or accepted yet; attribution and the GNU C comparison come first.

The external `native_profile.py`
and `native_phase.py` are diagnostics, not editor interfaces or a new compat
runner. No profiler hook or helper Elisp has been added to production.

Keep the boundaries below. In particular: C-to-Rust only, GNU Elisp unchanged,
no authored Elisp even for probes, no GNU runtime delegation, no approved
semantic exceptions, whole `.eln` identity, adversarial audits and zero
warnings. Run expensive work serially on this loaded machine; do not reject
faithful code from a single noisy timing. All checkpoint pushes for this
goal are authorized. The retired harness stash `7cc4cb2` and backup at
`/private/tmp/emaxx-retired-harness-edits.ay9bJI/retired-harness-edits.patch`
are archived work, not pending edits to restore.

The remaining sections retain detailed rules and historical checkpoint
evidence. Where an older section says "current", "worktree" or "pending",
use this dated resume section and the actual Git/process state first.

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
12. **After every checkpoint commit and push, merge main before starting the
    next checkpoint** (Ray, 2026-09-05). Fetch `origin`, merge the latest
    `origin/main` into `native-comp`, and audit/verify the semantic
    interactions as well as textual conflicts. Preserve unrelated work.
    If main is already an ancestor, record that it is up to date; do not
    manufacture an empty merge. Do not begin the next checkpoint on a stale
    main base.
13. **Standing checkpoint-push authorization** (Ray, 2026-09-05). Ray
    explicitly authorizes all pushes related to checkpoint commits of the
    GNU-faithful runtime, portable-dump, and native-comp goal to
    `https://github.com/rayfdj/emaxx.git`, branch `native-comp`
    (`origin/native-comp`). Do not request separate user approval for each
    such push. Continue stating the exact scope, verifying the checkpoint,
    auditing before commit/push, and integrating current main afterward.
    This does not authorize force-pushes, branch deletion, unrelated work,
    or publication to another destination. Required tool permission checks
    still apply; this record is not permission to bypass a rejection.
14. **GNU faithfulness is the first gate, before expensive testing or timing**
    (Ray, 2026-09-05). Read the corresponding pinned GNU C before changing
    Rust. Review the proposed and actual diff against that C: control flow,
    authoritative object state, ownership, roots, mutation, errors and
    boundaries. Record unexplained extra work or behavior as a failed
    source review, not something a fast benchmark or passing tests can
    excuse. The order is source-faithfulness review, focused correctness
    and adversarial de-cheating checks, then expensive artifact/execution
    verification and comparable timings. Stop at the first failed stage.
    Re-review any subsequent implementation change before benchmarking it.
    Source review is necessary but is not a claim of automatic proof of
    semantic equivalence; all later acceptance checks still apply.
15. **Loaded-host noise is not a demonstrated performance regression**
    (Ray, 2026-09-05). Do not reject a correct, GNU-faithful correction
    merely because one run is slower on this loaded machine. Use repeated,
    order-balanced controls and report the baseline's own variation;
    CPU time as well as elapsed time can vary. Distinguish an inconclusive
    result from a demonstrated regression. This does not approve an actual
    regression or waive the final GNU performance target, and timings
    never override the C-faithfulness requirement.
16. **Exclude preload reconstruction from compiler/runtime performance**
    (Ray, 2026-09-05). Until portable dumping exists, rebuilding Emaxx's
    preload state must be outside every compiler/runtime comparison and
    before/after performance acceptance window. Measure equivalent work
    after preload/startup finishes in each process. Record image building
    and startup separately; cold command duration is not a compiler-speed
    result. Do not subtract an unrelated startup run to manufacture a
    phase measurement. The earlier approximately 8x whole-process GNU
    comparison does not establish the post-startup slowdown. Require
    direct post-startup evidence for that ratio. Use unchanged GNU entry
    points and external observation or existing instrumentation, not new
    Elisp, a private runner, or a backend-only interval mislabeled as the
    complete frontend/backend compilation.

## Current work after pushed a92e620 (2026-09-05)

The startup/loading checkpoint below is committed and pushed as `a92e620`.
Freshly fetched main at `84f342a` is included. The next uncommitted D03/D06
unit fixes native GC graph marking: a rooted vector's native cons element
was reclaimed before the separate Lisp reachability pass could see it.
The old-code control fails. C-first review has removed an added blanket
cons synchronization pass, corrected the raw cons work stack to car-first
order, and moved dead weak-entry removal before native storage sweeping.
The current worktree follows GNU's mark/fixed-point/weak-entry-removal/sweep
sequence for this bounded correction. It passes **89 focused checks**, zero
failures, with only the existing timing probe ignored. All 18 adversarial
checks are included; formatting, compiler and strict Clippy checks are clean.
Seven new Rust controls cover retained identity, cycles, current native
writes, car-first marking, repeated weak-table marking and actual removal,
and the ordinary GC subr called through the native ABI.

The unit has now cleared the bounded correctness and performance review.
Fresh precommit formatting, check and strict Clippy gates pass without
warnings; the combined focused/adversarial run again passes 89 tests,
zero failures, one separate timing probe ignored (13.49s). The earlier, rejected
candidate passed nine artifact fixtures but increased CPU by 1.75% and
3.83% in two prematurely run timing pairs. Those results do not verify the
current revision. The corrected editor has now been rebuilt and passes
all nine artifact fixtures: eight entire identical .eln files through
comp.el and one correctly absent artifact. Two new serial before/after
pairs completed after C review, focused/adversarial checks and artifact
verification. However, those measurements still include preload rebuilding:
they establish neither the post-startup cost of this correction nor its
performance acceptance. They are retained as cold-process diagnostics only.
The recorded roughly 8x GNU comparison likewise includes preload and must
not be quoted as a compiler/runtime-speed comparison. Direct post-startup
measurement now completes two pairs: current 54.18/52.44s, baseline
55.92/52.32s, GNU 8.59/8.72s inside unchanged `batch-native-compile`.
The correction's CPU differences reverse sign (-2.73%, +0.20%) while the
baseline itself varies 5.82%; no consistent regression or precise speedup
is established. All outputs are entire-byte identical. The remaining
post-startup gap is approximately 6.2x GNU by elapsed time for this input,
not the earlier startup-inclusive 8x. See the dump ledger for the external
observer's boundary/unit controls, logs and limitations.
Neither a passing test nor a faster run approves a departure from GNU. See
[the dump ledger](pdump-c-parity-ledger.md) for the source review, evidence,
and remaining root/representation/inactive-GC/accounting/finalizer gaps.
No dumper exists yet.

Ray's next priority (2026-09-05): once this bounded GC fix is accepted,
checkpoint and push it, integrate current main, then investigate the
largest contributor to Rust's post-startup native-comp slowness. Use a
fresh profile of the ordinary compilation route to rank costs, compare
the dominant Rust work with its GNU C owner, and fix an evidenced
behavior/representation deviation first. Do not assume GC is dominant,
add a speculative cache, or count preload reconstruction as compiler cost.
The portable-image goal and finite dump prerequisites remain in scope.

## Startup/loading checkpoint a92e620 (2026-09-05)

This intermediate checkpoint removes the production GNU-executable path
query, handwritten startup Elisp/policy, private source/bytecode preference,
filename alias and duplicate load search. It retains one rooted load-path
list, runs GNU's installed top-level form in both CLI modes, shares GNU's
C-owned openp search and hands source loading to unchanged GNU Elisp.
Related C-boundary corrections cover startup defaults, directory access,
minibuffer message clearing, keymap parent identity, character validation,
eval-buffer history/eager-owner selection and unibyte header matching.

Verification covers **91 distinct targeted tests**: 87/87 in
`d02-checkpoint-focused.log`, then 24/24 in
`d02-checkpoint-adjacent-green.log` (20 repeated audit/header checks plus
four adjacent regressions). Zero failed or ignored. An initial adjacent
failure exposed two live native interpreter fixtures sharing one dlopen
handle; releasing the compiler before constructing the fresh loader fixes
that test without changing its Lisp, compiled result assertion, native
capability or production relocation handling. This does not establish safe
coexisting native interpreters or template cloning.

All **nine ordinary-editor artifact fixtures pass** in
`d02-checkpoint-identity.log` (238.29s): eight entire .eln files are
byte-identical to GNU, including unchanged comp.el (881,800 bytes), and one
correctly emits none. The separate small before/current/GNU comparison also
passes with GNU execution forbidden during both Emaxx runs. Editor SHA-256:
`b5b16b0898407cf1ef2429d7555405e20aa3e3b1b037cd03a649089a3a17c10c`.
Exact evidence and limitations are in the [dump ledger](pdump-c-parity-ledger.md).
Final formatting, all-target check and strict all-target/all-feature Clippy
pass with zero warnings (`d02-checkpoint-*-final.log`).

The 177-case native execution suite and full runtime gate were **not rerun**
for this intermediate checkpoint. No persistent dumper exists. Remaining
loader entry/unwind/reader behavior, explicit .elc validation, replacement
regexp canonical tables, preload bookkeeping and native-unit publication/
finalization remain open. No semantic exception is approved. Do not claim
full loader parity, restored startup or measured hot-path performance parity.

### Historical implementation trail before this checkpoint

Latest uncommitted unit: the loader now uses a shared GNU openp search instead
of private source/bytecode preference, source-size and filename-alias rules.
All six old-code controls fail before the correction; the final focused run
passes 45/45, including 18 adversarial checks, zero ignored. The rebuilt
ordinary editor and retained baseline both compile unchanged comp-test-45603.el
with GNU execution forbidden; their complete 34,536-byte artifacts match the
separate GNU oracle. Formatting, all-target check and strict Clippy are clean.
Evidence is under `/private/tmp/emaxx-pdump-contracts.Yyf1mY`, with exact
hashes and limitations in the portable-dump ledger. No commit or push has
been made for this startup/search worktree; the full native/identity/runtime
gates have not been rerun for it.

The following uncommitted Fload handoff is now implemented and under
verification: direct reading retains the chosen descriptor; source/native/
module branches close it at their respective GNU boundaries; ordinary source
calls unchanged Vload_source_file_function. Three old-code callback controls
fail and now pass; the expanded surrounding run passes 48/48 with zero
ignored, including the 18 adversarial gates. Descriptor-lifetime, detached
C-field and recursion controls subsequently pass in the combined 53/53 run,
zero ignored, 64.62s (`d02-handoff-green3.log`). This also covers the corrected
preexisting locate-file/provenance expectation; its history assertions and
Elisp fixture remain unchanged. Strict Clippy and all-target check are clean.
The ordinary editor is rebuilt (SHA-256
`00fa75ad352e8e6cc8d94a6b8badbb885b3a037d39e8a405e8ffbc95907cf2a3`).
Both pre-handoff and updated Emaxx produce a whole-identical 34,536-byte
artifact against GNU on unchanged comp-test-45603.el, under the GNU-launch
fence and isolated homes. See `d02-handoff-artifact.n0tlSg/` and the dump
ledger for complete hashes and timing limitations. This unit is uncommitted.

The subsequent Feval_buffer correction honors the supplied history filename,
checks its type, accepts nil history independently of an outer load, and
freezes GNU's eager-macroexpansion decision at entry. Four old-code controls
fail; five focused checks and the wider 60-check integration pass, zero
ignored (`d02-eval-buffer-integration1.log`, 83.20s, including 18 audits).

The precommit review also corrected the new header probe's omitted unibyte
pattern conversion and wrong Latin-1 input decoding. Two Rust controls fail
before and pass after the repair, including all 65,536 byte pairs against
GNU's initial ASCII canonical-table rule. No Elisp was authored. GNU can
replace its standard canonical table, so this is not proof of arbitrary
case-table parity; that shared regexp-engine gap remains open.

Do not call this full loader parity: explicit bytecode-header validation,
several Feval_buffer/readevalloop state/reader contracts, preload bookkeeping
and native candidate publication remain open. Fix those C owners rather
than copying mule.el or authoring helper Elisp. The expanded checkpoint
integration/artifact run is pending, and no commit has yet been made for
this goal. The dump ledger records exact evidence and limitations;
no portable dump exists.

### Active portable-dump goal, starting from pushed `b432d86`

The new goal prioritizes actual GNU dump prerequisites, then a usable
restored image, then the remaining native hot-path work. Current source
mapping, fresh startup timings, and the next bounded implementation are in
[the portable-dump ledger](pdump-c-parity-ledger.md). Main was fetched again:
`84f342a` is already included. No empty merge is needed.

The three former compatibility-harness edits have been archived and removed
at Ray's request. The working tree was clean at the start of this goal.
Stash `7cc4cb2` is an archive, not pending work; older instructions below to
preserve/restore those edits are historical and must not be acted on.

The first audit found a real startup boundary violation: ordinary Emaxx calls
`compat::emaxx_upstream_load_path`, which executes GNU to obtain its path.
The unchanged editor fails with exit 2 when a read-only diagnostic sandbox
forbids only GNU executable launches. The previous batch-source audit stopped
at an inline `#[cfg(test)]` attribute and never inspected this code. The
pending audit fix scans through to the actual test module; do not whitelist
the oracle call or replace it with a copied Lisp directory manifest. GNU
`lread.c:init_lread` owns initial paths; unchanged `startup.el` owns
subdirectory expansion and the session lifecycle. The first implementation
unit must preserve that boundary. No portable dump has been implemented yet.

The worktree now corrects the initial dump-build path and `Vload_path`
ownership: one rooted Lisp list instead of a host vector reconstructed on
reads. Both original-list and splice controls failed before the repair.
Seven focused checks now pass, including a direct call to unchanged GNU
`normal-top-level-add-to-load-path`, dynamic/buffer-local restoration,
detachment, root replacement, and real source lookup. The existing copier
check concerns test-fixture isolation only, not portable dumping. The
surrounding evaluator-field suite passed 15/15 and unchanged loadup/seq 1/1.
Strict Clippy and formatting are clean. The rebuilt ordinary editor produces
the same complete 34,536-byte `comp-test-45603.el` artifact as baseline and
GNU; detailed commands, hashes and timing limitations are in the dump ledger.

The next worktree unit now removes the remaining production oracle query
and routes both batch and interactive CLI startup through `Vtop_level`, as
`keyboard.c:top_level_2` does. GNU startup owns subdirectory expansion and
argument processing; the former handwritten startup helpers are removed,
including the TTY startup forms. See the dump ledger's pending-integration
section for the changed phase boundaries and source references.

Do not push this as a completed startup repair. The integrated selection
first passed 20/21 with a TTY missing-`subr-x` failure; after using GNU startup
it passed 23/24 with a real `internal-char-font` character-range failure.
The worktree corrects that GNU C contract, the undo/GC build-versus-session
defaults, and the POSIX directory-search check; verification is in progress.
The old 17/18 audit result and small-artifact comparison above predate this
integration. Current evidence now includes 18/18 adversarial passes and a
rebuilt ordinary editor that starts and native-compiles with GNU launches
forbidden. The fresh complete small artifact is byte-identical (34,536 bytes,
SHA-256 c1b134b0c6af1b8e216a556721bf6b5ad0e63827a9a4efdcfddffd5b86f71eb6).
Detailed hashes, commands, timings and limitations are in the dump ledger.

The earlier 25/26 startup selection reaches the normal abbrev file, but
Rust's loader fails to expand its ~/ path. With an isolated HOME the revised
startup test now passes, including the exact message confirmed in a separate
GNU terminal session. Scrolling/recentring also pass their unchanged GNU
expectations after explicitly emptying the test sample buffer: the fixture
now finishes startup, including initial-scratch-message insertion.

The remaining prompt-display regression was a missing C call, not a Lisp
policy issue: read_minibuf calls clear_message(true, true) before installing
the keymap. Rust now implements its callback/GC guards, C-slot reads,
independent echo-buffer flags, dynamic bindings, safe signal handling and
activation unwind. The raw clear-message-function slot starts at nil, as
in syms_of_xdisp; unchanged minibuffer.el installs the function later.
Eight new C-contract tests, the original TTY regression and all 18
adversarial audits pass (27/27, zero ignored, 12.06s). Check and strict
Clippy are warning-free. See `d02-clear-slot-*` in the evidence directory.
The nearby integration selection first passed 8/9 (zero ignored): Enter
inserted a newline instead of exiting in the initial-post-command-hook test.
Passive Rust diagnostics found that the completion map's parent was never
installed: constructors return public cons roots, but the parent getter and
setter only accepted private records. Both sides of that mismatch also exist
in HEAD b432d86. The bounded worktree fix resolves the original root through
its existing owner and returns the installed parent, as GNU does. Both new
Rust parent-identity tests, the original minibuffer regression and walker
test, the clearing/TTY checks and all 18 audits now pass: 31/31, zero ignored,
33.12s (`d03-keymap-focused.log`). No diagnostic callbacks remain. Check and
strict Clippy are clean. The broader selection passes 43/44: its only
failure is a stale assertion that last is bytecode. A separate GNU terminal
session evaluates the existing expression and confirms the actual subr
result and all other fields. Only the Rust expectation/comment is corrected.
The final focused run passes 32/32, zero ignored, including that check and
all 18 audits (41.12s). Public parent-tail sharing and the remaining
getter/setter branches are explicitly open in D03, not declared complete
by this fix.

Before the parent fix, the refreshed ordinary editor passed the whole-file
34,536-byte comparison with GNU execution forbidden
for Emaxx (`d02-clear-artifact/`, SHA-256
cc82282cdfca745de0ea28a7d5765006509b13fda63f562e084fb263553961b7).
After the parent correction, the fresh before/after/GNU comparison also
passes: all three complete files are 34,536 bytes, SHA-256
56e12f1da5e840980c0be88db84346b04a5dda7606e62c72d26c2303bcf1458e
(`d03-keymap-artifact/`). Both Emaxx runs forbid GNU execution; GNU runs
last with isolated outputs. Current editor SHA-256 is
01763c9d5b50530233fc249f165ad63755c34a5903ff9ab7129aa50bee7d0083.
Single cold-process timings are not a hot-path performance claim; the full
identity ladder and 177 native execution cases have not been rerun here.
This is not a completed or pushed startup checkpoint.

Do not fix the ~/ failure by suppressing GNU abbrev loading. The source
review also found a non-GNU repeated-directory filename rewrite and private
VM/source-size suffix selection in the old loader. The next bounded task is
to consolidate the actual openp search and remove those deviations, using
unchanged GNU fixtures and isolated HOME, not personal files. Detailed C
owners and remaining scope are in the dump ledger. CLI sorting, error recovery,
and real dump-build entry remain open too. The artifact ladder was updated
to follow the now-correct user cache location with isolated editor outputs,
but its full rerun is pending. No GNU source, new Elisp, warning suppression,
or replacement compatibility runner was added.

### Verified original constants-vector ownership (base `c236c21`)

The type-test checkpoint below was committed and pushed as `c236c21`.
Both pre-commit and pre-push adversarial audits passed 17/17. The immediate
post-push fetch confirmed `origin/main` is still `84f342a`, already an ancestor;
no empty merge was made. The three unrelated user edits remain unstaged.

Work continues with L15, a C-owned bytecode constants-vector ownership check
on the native-comp frontend path. GNU `alloc.c:Fmake_byte_code` stores the
original constants vector; `bytecode.c:exec_byte_code` reads it through
`vectorp`. Rust copied its slots into `ByteCodeObject` and again into
`CachedProgram`. Both direct Rust controls failed on the unchanged implementation:
the VM returns stale `11` instead of `29` after mutation between calls, and
stale `11` instead of `77` after mutation during a frame (`red.log`; 0 passed,
2 failed; optimized build 4m07s). Evidence directory:
`/private/tmp/emaxx-bytecode-constants.aiZp6U`. Saved baseline editor:
`emaxx-c236c21`, SHA-256
`1b60e41944ab675911592e45d69ea127fddbc83704e9799a92cd31d73f1bb345`.

The correction shares the original `Rc<VectorValue>` through both
structures and reads a slot at each instruction, ending the borrow before
Lisp can run. The outer closure fields are also read directly instead of
copying their slot array. It removes the VM's duplicate reader-graph traversal/materializer;
GNU `lread.c:bytecode_from_rev_list` constructs those objects before execution.
The existing Rust reader boundary remains unchanged. Existing decoder-fixture
unit tests now call that existing reader boundary before execution; their
Elisp/bytecode fixtures and expected results are unchanged. No custom CLI,
Elisp or cache is added. A third Rust contract checks original vector identity
and absence of a retained old constant after replacement. The adversarial
gate executes a mutation negative control through ordinary C-owned entry
points and requires all three regression tests. Initial focused optimized verification passed
109 tests, zero failures, with one separate timing probe ignored
(`contracts.log`, 30.47s; optimized build 3m14s). This includes every bytecode
test, native-runtime contract and anti-cheating gate, plus the existing
byte-compiler and named-let regressions. The initial all-target check caught an audit import
of a test-only module; the audit now runs its own ordinary-API control so it
works in standalone and unit-test builds. This changes no VM behavior and
adds no warning suppression. Final focused verification covers that audit
wiring and the direct outer-slot read: 109 passed, zero failed, one separate
timing probe ignored (`final-contracts.log`, 44.72s). Formatting, all-target
check and strict all-target/all-feature Clippy finish without warnings
(`warnings-final.log`). The rebuilt ordinary editor has SHA-256
`e3547c198c6b65bb551101bdf8e511963c5f1c485f23205c260547a2d1419db4`.
All nine ordinary-editor artifact fixtures pass: eight whole `.eln` files
are byte-identical and the no-byte-compile file correctly produces none
(`identity.log`, 629.37s). A three-second sample during the slow final
`comp.el` rung shows active GCC code generation; this sampled correctness
run is not a performance measurement. The ordinary editor also passes all
177 unchanged GNU native tests, zero unexpected, with both native helper
artifacts freshly compiled and loaded (`emaxx-native.stderr`; ERT
2394.882872s, real/user/system 2489.00/2236.56/90.85s). This uses GNU's
native-enabled default Makefile selector. Its slow function-compilation
cases use unchanged `comp.el`'s normal child-editor route, not a separate
runner.

Paired full unchanged `comp.el` timings (seconds; fresh homes, no profiler,
one identical source path within each pair, GNU runs last):

| Pair/order | Before wall/user/system | After wall/user/system | GNU wall/user/system |
|---|---|---|---|
| 1: before, after, GNU | 125.25 / 115.53 / 6.62 | 122.45 / 112.95 / 6.75 | 19.27 / 18.26 / 0.49 |
| 2: after, before, GNU | 157.43 / 143.26 / 8.87 | 141.64 / 129.79 / 7.87 | 18.81 / 18.10 / 0.44 |

All three complete 881,800-byte artifacts match within each pair. SHA-256:
`341e99373c8cab5ad30e33aa366c917bde6ba6c6d7f7cb6c129623b8acb02b4c`
(pair 1) and
`89e4886cbdb286396ef4ca202fdc92965e8de8520e51f115cadfd5f50dc5833d`
(pair 2). User CPU falls 2.23% and 9.40% respectively. Both pairs support
retaining this GNU behavior correction without an observed regression;
the loaded-host spread does not establish a precise stable speedup. Mean
user CPU is 129.395s before, 121.37s after, and 18.18s GNU, so current Emaxx
is still about 6.68x GNU on this workload including startup. Native-comp
performance parity is not finished.

The source audit is recorded in the evidence directory as `source-audit.md`.
Every production change maps to the C-owned vector/read/execution contract;
there is no authored Elisp, GNU edit, oracle delegation, new cache, or
semantic exception. Final pre-commit format/check/strict-Clippy checks are
clean and all 17 adversarial tests pass (`warnings-pre-commit.log`,
`pre-commit-audit.log`; audit 3.85s). Repeat the audit before pushing.
L15 does not close broader opcode-cache, make-byte-code
validation, or GC/lifetime gaps. The read-only follow-up observations in
`next-source-audit.md` are not implemented changes or additional verified
contracts. R02c remains the leading performance priority. After this
checkpoint is committed and pushed, fetch/integrate main before starting it.

### Verified GNU type-test corrections (base `36dd465`)

The main integration below was committed and pushed as `36dd465`. The
post-push fetch confirmed `origin/main` (`84f342a`) was already included.
The three unrelated user edits were restored, unstaged; the scoped stash
backup remains available. Do not reapply it or stage those files wholesale.

Ray asked work to continue, not to stop at the checkpoint handoff. The next
native-comp audit started with R02c's repeated native-handle encoding and a
fresh five-second sample of the unchanged editor compiling unchanged GNU
`comp.el`. Evidence directory: `/private/tmp/emaxx-r02c.YelOLe`. Saved baseline:
`emaxx-36dd465`, SHA-256
`d01db433db5c3eb35a60380a7fe2f74bc7c3d4abfca3aeb813d29fd469273b8c`.
The sample again finds encoding/decoding and Value ownership traffic, but
also bytecode classification/setup and string copying. This different-phase,
loaded-host sample is not a before/after performance claim. No R02c cache or
object-storage draft has been implemented.

Two bounded C-owned deviations are now tracked as L13/L14 in the ledger:

- L13: `byte-code-function-p` falsely accepted an interpreted lambda by the
  exact names of its parameters, or a record by its type-name spelling. GNU
  `data.c:Fbyte_code_function_p` checks the closure kind and string code-slot
  tag instead. The native call classification and internal bytecode shape
  inspection also copied string/vector payloads unnecessarily. Both new Rust
  contracts failed on the old code (`l13-red.log`). The correction passes
  102 focused Rust tests, zero failures, one separate timing probe ignored
  (`l13-green.log`, 83.50s), including existing byte-compiler regressions.
- L14: string type predicates copied payloads through `string_like` and could
  accept an ordinary vector shaped like a propertized-string literal. The
  character predicate used Unicode's limit instead of GNU's `MAX_CHAR`, and
  sequence classification omitted char-tables. Three new contracts failed
  before the correction (`l14-red.log`). The shared `string_like` vector
  fallback is removed too: GNU `lisp.h:CHECK_STRING` rejects ordinary vectors,
  and the existing reader already constructs real StringObjects for valid
  propertized-string literals. Ordinary/native `string-bytes` error tests
  preserve the original offending vector's identity.
  P35 adds GNU's direct tagged-word `stringp` to the native ABI table, with a
  no-active-runtime contract. The existing adversarial gate now executes
  negative controls for the historical false positives, not only an inventory
  of source/test names. No new Elisp, GNU changes or native ABI layout changes.

The final frozen implementation passes 116 focused optimized Rust tests,
zero failures, with one separate timing probe ignored (`final-contracts.log`,
64.63s). This includes all bytecode and native-runtime contracts, all 17
anti-cheating gates, existing compiler regressions, and existing string,
reader, documentation, and sequence regressions. Formatting, all-target check,
and all-feature/all-target Clippy with `-D warnings` pass without warnings.
The final ordinary editor SHA-256 is
`1b60e41944ab675911592e45d69ea127fddbc83704e9799a92cd31d73f1bb345`.
All nine unchanged-source artifact fixtures pass (`identity.log`, 281.74s):
eight entire `.eln` files are byte-identical to GNU, including the 881,800-byte
`comp.el` artifact, and the no-byte-compile fixture emits nothing in either
editor. The ordinary CLI also passes all 177 GNU native execution tests,
zero unexpected results, exit 0, using the existing GNU Makefile's
native-enabled default selector (`emaxx-native.stderr`). Both helper
libraries were freshly compiled and loaded; no image cloning was enabled.
ERT took 1563.00s; total wall/user/system times were 1622.78/1504.66/40.48s.
The host load varied substantially during the serial run; this unpaired
duration is not performance evidence against an older checkpoint.
Two serial full-compilation pairs use the unchanged GNU `comp.el`, identical
source paths within each pair, fresh homes, ordinary CLI entry points, no
profiler, and reversed before/after order. User CPU before/after was
106.74/110.82s, then 120.18/113.79s; GNU used 15.00s and 17.34s. All four
Emaxx artifacts are byte-identical to their GNU reference (881,800 bytes).
Logs and retained artifacts are under `performance/pair-{1,2}` in the evidence
directory. Means are 113.46/112.305/16.17s: current Emaxx remains about 6.95x
GNU. The opposite-signed differences (+3.8%, -5.3%) and host variation establish
neither a repeatable regression nor a speedup. Retain these as corrections to
GNU-defined type behavior, not a claim that native-comp performance is solved.
Do not declare general string representation, all closure dispatch, or R02c
fixed by these bounded type-test corrections.

Final pre-commit checks repeat cleanly: 17/17 adversarial gates
(`pre-commit-audit.log`, 3.39s), formatting, all-target check and strict
all-feature/all-target Clippy. The manual diff audit found no new Elisp,
GNU runtime delegation, oracle-output reuse, test-selected production
behavior, error swallowing, comparison normalization or added warning
suppression. The three unrelated user files remain unchanged and unstaged.
Repeat the audit before pushing, then fetch and integrate main before the
next checkpoint; do not stop simply because this checkpoint was pushed.

### Verified main integration checkpoint

`9097866` (the verified checkpoint below) is committed and pushed. As Ray
requested, the next action was fetching and merging main, not starting another
performance experiment. `origin/main` advanced to `84f342a`: Eshell bytecode
string ownership, Darwin exec-failure behavior, and the runtime/Editfns audit
changes. This checkpoint integrates those changes; fresh merge verification
passes 177 native tests, all nine artifact fixtures and 112 final Rust tests,
with clean warning/audit checks and no measured material timing regression.

Text conflicts are resolved. Semantic resolutions preserve one GNU Ffuncall
depth entry, prevent `makunbound` from reconnecting detached direct C fields,
and retain the actual C-side object roots through GC and image copying. The
obsolete vector cache and dumped-local fallback stay removed. GNU C owners
and exact scope are recorded in the parity ledger's post-push integration
section. No GNU source or Elisp has been changed.

Initial optimized contracts: 78 passed, zero failed, one separate native
timing probe ignored. This includes all 49 native-runtime correctness tests,
17 anti-cheating gates, four new direct Rust merge contracts, and the incoming
bytecode string-ownership contract. Formatting, all-target check and
all-feature/all-target Clippy with `-D warnings` pass. Logs are under
`/private/tmp/emaxx-main-84f342a.3jARTW`.

The existing incoming-regression replay selected 26 runnable tests (one
additional requested case is Linux-only). 24 passed, including both earlier
closure/callback regressions and Ffuncall depth. Two failed in the **GNU oracle
assertion before Emaxx ran**, because the ambient UTF-8 locale changes quote
characters: `commandp_follows_fcommandp_order_and_property_error` and
`quoting_style_reaches_message_error_text_and_display_table`. Their unchanged
replay with the normal suite's `LANG=C LC_ALL=C` passes all 26 tests (271.65s;
`main-regressions-c-locale.log`). This is not a
reason to modify GNU, alter the test programs, or normalize compared output.
The artifact replay passed four fixtures, then found a real mismatch on
unchanged `test/lisp/emacs-lisp/comp-tests.el`: both files are 86,384 bytes,
first difference at byte 768. The serialized constants show a lost shared
`" *temp file*"` string, shifting data and code addresses. The saved `9097866`
editor still produces a byte-identical artifact on exactly the same retained
source under the same C locale. Logs: `identity.log`, `identity-before.stderr`.
Artifacts are retained in
`/var/folders/js/swz7g_zx0qj34jhbbc1hr_6w0000gn/T/native-comp-identity-76040-1788583729315424000`
and `target/native-lisp/30.2-adba4e3f/comp-tests-a153e8cb-d5ff1e9e.eln`.

L12 now tracks the correction: GNU `bytecode.c:exec_byte_code` pushes the
original argument word; main `17da04f` instead allocated a new string via
`stored_value` on every bytecode argument pass. Remove that copying and fix
the actual C producer, `print.c:Ferror_message_string`: its general result
is an already-mutable multibyte buffer string, and `(error STRING)` returns
the original string unchanged. The Rust bytecode contract now also checks
identity, caller-visible properties, and the error-string fast return; the
existing GNU Eshell fixture and its expectations remain unchanged. This
correction passes 112 selected optimized Rust tests, zero failures, including
the unchanged Eshell regression, every bytecode test, all native-runtime
correctness tests, all anti-cheating checks and error-message rendering tests
(`string-ownership-contracts.log`, 44.29s). One separate native timing probe
is ignored. Format, all-target check and strict Clippy are clean. The corrected
ordinary editor now passes all nine artifact fixtures: eight entire `.eln`
files byte-identical, including 881,800 bytes for GNU `comp.el`, and one
correctly absent artifact (`identity-string-fixed.log`, 216.48s). No promotion
cache, new Elisp or GNU change was used. Final editor SHA-256:
`d01db433db5c3eb35a60380a7fe2f74bc7c3d4abfca3aeb813d29fd469273b8c`.
The complete merged-tree native execution gate passes all 177 tests, zero
unexpected results, exit 0, through the ordinary CLI and GNU's existing
Makefile selector (`emaxx-native.stderr`). Both helper libraries were freshly
compiled and loaded. ERT took 1076.91s; total wall/user/system times were
1114.12/1024.41/26.13s. GNU's normal per-compile subprocess policy was retained,
and no image cloning was enabled. This is fresh merged-tree evidence, not the
checkpoint's older execution result below.

Paired full `comp.el` user CPU was before/merged 62.81/63.02s, then
69.94/66.42s with order reversed. GNU used 8.43s and 9.47s. All before/merged
artifacts are byte-identical to GNU. The samples show no material merge
regression, but their variation does not establish a speedup. Means are
66.38/64.72/8.95s: Emaxx remains about 7.2x GNU, including startup. See the
ledger for full evidence; logs are in the `performance` subdirectory above.
Final format/check/strict Clippy are clean, and the pre-commit adversarial
audit passes 17/17 (`pre-commit-audit.log`). Repeat it before pushing. No
unverified V02 cache draft, new Elisp, GNU change, output normalization or
native-state cloning shortcut is retained. Broader runtime/GC/forwarding
representation gaps remain open; this is not a universal correctness claim.

Unrelated user work is preserved in scoped stash
`bdfd8f6bcdc91da2bd80f99a50430afe960c57bc` (compat reporter, `src/compat.rs`,
and the honesty-audit note), excluded from the merge commit. The post-merge
step restores it while retaining the backup; check the worktree before
reapplying it on a later continuation. Do not include it in a native checkpoint
or restore older rejected V02 drafts.

### Verified checkpoint `9097866`

Main was merged and pushed as `38b2ee8` on `native-comp`. This checkpoint adds
the V06 original-symbol error repair and the L09-L11 lexical-symbol ownership,
closure-printing, and symbol-name corrections described below. Current gates:
177/177 GNU native tests, all nine artifact fixtures, 69 Rust contracts/audits,
and clean formatting/check/Clippy. Paired compilation CPU is unchanged versus
`38b2ee8` and remains about 7.1x GNU. This does not complete native-comp
performance work or prove full-runtime equivalence.

The `data.c:Fsymbol_value` correction preserves the caller's original symbol
(including alias, uninterned, and positioned identity) in `void-variable`.
The checkpoint also repairs an outside-native-call GC test that previously
did not collect anything. All proposed V02 symbol-storage
drafts were rejected and removed; see `docs/native-comp-c-parity-ledger.md`
for their performance regressions, synchronization, and ownership findings.
Do not restore them or claim the existing epoch cache is GNU-equivalent.

Before L09, the Rust runtime tests passed 48/48 (one separate benchmark ignored),
all 16 anti-cheating gates passed, and formatting/check/Clippy were warning-free.
The permanent identity ladder now includes unchanged GNU `comp.el`: all nine
fixtures pass (eight entire `.eln` files identical and one correctly absent
artifact).  The `comp.el` artifact is 881,800 bytes in that run.

**Earlier execution failure, before L09-L11 (now repaired below).** Running GNU's existing
`test/Makefile.in` command with its native-enabled normal selector selected
177 tests.  GNU passed 177/177.  Emaxx compiled and loaded both real native
helper files, passed 52 cases, then failed `comp-tests-fw-prop-1`.  Its failure
reporter aborted before printing the condition, leaving 124 cases unrun.
Logs: `/private/tmp/emaxx-v06-execution.tyIV38/emaxx.stderr` and `gnu.stderr`.
A focused verbose replay using the untouched merge binary at
`/private/tmp/emaxx-v02-baseline-target/release/emaxx` also failed the same
test (0/1, exit 2). See `baseline-fw-prop.stderr` beside the full-run logs.
Temporary Rust tracing exposed the original condition: `void-variable
--cl-var--`. GNU `comp.el` prepends `comp-tests-fw-prop-1-f` when re-signaling
it; that function name is not the missing variable. The reporter separately
fails with `wrong-type-argument number-or-marker-p nil`. This failure predates today's V06 correction;
do not carry the older 177/177 claim below forward to this merged tree.
Publication and performance measurements were paused for diagnosis.

The cause is now traced: native `assq` compared two different symbol objects
for the same uninterned accumulator immediately before its binding was lost.
Lexical frames and parsed function parameters had copied symbol names into
Strings; closure projection recreated a different uninterned symbol. L09 in
the ledger tracks the Rust correction: retain original `SymbolName` handles
through these C-owned binding boundaries. Native EQ/assq and GNU `cconv.el`
remain unchanged. The ordinary release editor now passes the formerly failing
GNU test (1/1, `emaxx-fw-prop-fixed.stderr`, 47.77s wall / 42.85s user CPU).
All 49 native runtime contracts pass (one separate timing benchmark ignored),
including a negative control that rejects name-reconstructed symbols. Format,
all-target check, and Clippy also passed. At that point, the GC-root companion,
complete 177-test replay, artifact comparisons, and performance measurements
were still pending; later results follow. All temporary
Rust traces have been removed from both source and the release editor.

The GC-root contract subsequently passed. A serial focused Rust run selected
106 closure/lexical/uninterned/audit cases: 104 passed, 2 failed. Both reported
merge callback regressions (file-lock lifecycle and custom-hash captured
counter) passed. Both remaining failures also reproduce on untouched `38b2ee8`
in an optimized build (`baseline-closure-regressions.log`, 0/2, 21.10s).

- L10: the printer re-trimmed closure environments in Rust after GNU Elisp had
  consumed `:closure-dont-trim-context`. This was an Elisp/C boundary violation.
  GNU `print.c:PVEC_CLOSURE` prints all stored slots; the Rust trim and free-
  variable scan have been deleted. GNU's unchanged `cconv-safe-for-space`
  passes, and all four focused Rust print tests now pass. One existing test
  now uses the full GNU image to obtain GNU cconv filtering; its Elisp and
  expected output are unchanged.
- L11: the dynamic-obarray callback actually worked, but `symbol-name`
  returned `erc-lo2-mode` with an internal obarray suffix. Construction had
  stored the lookup key as the Lisp name. The draft separates them, retains
  Fintern's supplied string on a miss (with GNU shorthand/purecopy selection),
  and keeps existing-symbol hits unchanged. The getter and native EQ are not
  patched. Full obarray storage/lifetime parity is not claimed.

The optimized expanded replay selected 119 tests: 117 passed, including both
baseline failures above. Two newly added Rust assertions failed because the
test forgot GNU's initial `purify-flag=t` and expected the wrong host error
variant; both assertions were corrected without changing the implementation
or GNU defaults. The final serial contract run passes 69 tests: all 49 native
runtime contracts, all 16 anti-cheating gates, the lexical/name GC-root test,
the weak-key reachability test, and both new intern contracts. One separate
native hot-path timing probe remains ignored. This final run also covers
GNU's explicitly multibyte shorthand-name construction and the traced
SYMBOL_NAME child. `cargo check --all-targets`, all-feature/all-target Clippy
with `-D warnings`, and formatting are clean. Logs: `focused-final.log` and
`final-contracts.log` in the diagnostic directory above.

The final release editor now passes all 177 native-enabled GNU tests, zero
unexpected results, exit 0. Both helper `.eln` files were freshly compiled
and loaded. Log: `/private/tmp/emaxx-native-final.3mbz17/emaxx.stderr`.
The run took 1128.20s wall / 1034.87s user / 26.77s system CPU, with 1090.59s
inside ERT. Its 80 return-type cases each call `native-compile`; unchanged GNU
`comp.el:comp--final` launches an editor subprocess for each non-batch compile.
Those cases took approximately 11-12s each. No spawning-policy override or
image-cloning shortcut was used. This restores the 177/177 result on the
merged tree; the earlier failure above is diagnostic history, not current
execution status. The final artifact ladder also passes all nine unchanged
GNU fixtures (229.33s): eight entire `.eln` files byte-identical, including
881,800 bytes for `comp.el`, and one correctly absent artifact. Log:
`/private/tmp/emaxx-native-final.3mbz17/identity.log`. The unchanged GNU
`cconv-safe-for-space` test also passes through the ordinary editor (1/1;
`cconv.stderr` beside those logs).

Paired full `comp.el` compilation is performance-neutral versus pushed merge
`38b2ee8`: baseline/current user CPU was 62.10/62.93s, then 63.00/62.15s with
execution order reversed. Means are 62.55/62.54s. GNU used 8.44s and 9.07s
(mean 8.76s): Emaxx remains about 7.1x slower on this workload, including
startup. Do not claim a speedup or performance parity. Every measured baseline
and current artifact is whole-file identical to the corresponding GNU output.
Logs/artifacts: `/private/tmp/emaxx-v06-measure.zr27c1`; hashes and audit details
are in the ledger. This is a correctness/C-boundary checkpoint, not acceptance
of the existing V02 epoch cache or a claim of full-runtime equivalence.
Diagnostic logs include
`emaxx-fw-prop-{trace,environment,identity}.stderr` in the directory above.
A Rust-unit reproduction was removed after it could not
serve as the editor subprocess that unchanged `comp.el:comp--final` launches;
no override to GNU's spawning policy or invocation variables was retained.

Preserve the unrelated pre-existing edits in `compat/emacs_compat_runner.el`,
`src/compat.rs`, and `docs/honesty-audit-2026-08-18.md`; they are excluded from
this unit.  No new Elisp or GNU source changes were made.  The test command
and selector were read directly from GNU's Makefile, not replaced with a
new helper or compatibility runner.

## Darwin exact-artifact progress (2026-09-03; historical pre-merge evidence)

This section records the pre-merge execution point, not the current failing
checkpoint above.  The ordinary, unchanged GNU `batch-native-compile` entry
point now produces byte-identical complete `.eln` files for all eight rungs in
the current smallest-to-largest ladder, through the 49,628-byte full
native-compiler test file.  The ordered results and the semantics each rung
covers are recorded in `docs/testing.md` and enforced by the ignored oracle
integration test `tests/native_comp_identity.rs`.

The newly cleared rung is unchanged upstream `test/src/comp-tests.el`
(49,628 bytes).  GNU and Emaxx both produced a 1,021,168-byte artifact from
the same copied absolute source path.  Both SHA-256 digests were
`995b8230bb390928510d256567da4c1639d5ab396c4ffa8139c8ca76d3ad6f39`,
and `cmp` found no differing byte.  This covers the full upstream
native-compiler ERT definitions, resource orchestration, compiler options,
diagnostics, asynchronous compilation, loading, runtime assertions, and the
positioned-symbol contracts described below.

The full fixture initially exposed two C-boundary mismatches.  GNU
`eval.c:Fdefvar` and `Fdefconst` use `CHECK_SYMBOL`/`XSYMBOL`, so a source
symbol with position is accepted while `symbols-with-pos-enabled` is active,
the definition is installed on the underlying bare symbol, and the original
object is returned.  The Rust special forms now do the same.  GNU
`eval.c:Fmake_interpreted_closure` stores its `ARGS` list verbatim in closure
slot zero; the Rust closure now retains that exact Lisp object, including
source-position symbols, while also keeping a compact parsed parameter vector
for fast invocation.  `eval.c:Ffunc_arity`/`lambda_arity` now use the same
`SYMBOLP`, `XSYMBOL`, and position-aware `EQ` rules in Rust for function names,
macro wrappers, and closure argument markers.  No Elisp was changed or added.

The identity harness now preserves each fixture's upstream relative path in
its temporary tree.  This is necessary because unchanged
`test/src/comp-tests.el` locates `comp-resources` through `ert-resource-file`;
flattening the source into the temporary root changed the input environment
and made GNU itself fail before compilation.

The failure was not in `comp.el`.  Emaxx's Rust cons-mutation watcher map used
to clear every watcher at one million field keys.  Live native mirrors were
invalidated during that reset and later marked current without being
re-registered.  A subsequent Rust `setcar` could therefore leave GNU-generated
machine code reading the old fixnum directly from its two-word `Lisp_Cons`
view.  The fix in `src/lisp/types.rs` compacts only dead weak watcher entries,
preserves all live subscriptions, rebuilds the Bloom filter, and grows the
next compaction threshold amortized.  No Elisp was changed or added.

Focused verification at this checkpoint:

- The unchanged upstream `test/src/comp-tests.el` normal selector ran through
  Emaxx with native compilation enabled: 177 passed, zero failed, zero
  unexpected.  The run compiled and loaded both upstream helper `.eln` files
  and compiled fresh artifacts in the individual runtime-compilation tests.
- `cargo test --release -j1 --test native_comp_identity -- --ignored
  --nocapture --test-threads=1` passed the complete eight-rung comparison.
  Seven fixtures emitted byte-identical `.eln` files and the upstream
  `no-byte-compile` fixture correctly emitted nothing in either editor.
- `cargo test -j1 --lib lisp::native_comp::runtime::tests --
  --test-threads=1`: 21 passed, zero failed, one intentionally ignored
  hot-path benchmark.
- The automatic watcher-compaction regression and GNU 61-bit `make_int`
  boundary regression pass.
- `cargo fmt --all -- --check`, `cargo check -j1 --all-targets`, and
  `cargo clippy -j1 --all-targets --all-features -- -D warnings` pass with no
  suppression added for the work.  The loader's eight loose arguments were
  grouped into a typed `LoaderState` instead of allowing the Clippy warning.
- Temporary GC, assertion, subroutine-profile, and mirror-invariant tracing
  has been removed.

The first 177-case run stopped at `comp-tests-doc`: Emaxx's ordinary `load`
path crossed directly into the native loader without the surrounding
`lread.c:Fload` bookkeeping.  The Rust path now follows GNU exactly: resolve
the source-facing `.elc` name through `comp-eln-to-el-h`, dynamically bind the
load context and `current-load-list`, call the `comp.c`-owned native loader,
commit `load-history`, unwind, and only then invoke GNU Elisp's unchanged
`do-after-load-evaluation`.  This is C-to-Rust work; no Elisp was added or
modified.  The isolated upstream `comp-tests-doc` test then passed and the
full normal suite reached 177/177.

The one test outside the normal 177 is `comp-tests-bootstrap`, tagged by GNU
as `:expensive-test`.  On this Darwin reference, GNU itself fails that test's
raw `cmp`: its two output paths have different basenames, and GNU `comp.c`
faithfully places each basename in Mach-O `LC_ID_DYLIB` via `-install_name`.
Emaxx has the same behavior.  Do not change the backend to hide that genuine
GNU result.

Correctness is now green at both required levels: upstream behavior is
177/177 and generated artifacts are byte-identical across the full ladder.
The next phase is performance.  Re-measure on an otherwise quiet host before
optimizing; older loaded-host observations put an already-built GNU editor at
about 6.3 seconds and release Emaxx between roughly 71 and 86 seconds for the
full fixture.  Preserve exact output and rerun the focused correctness gates
after every hot-path change.

## Linux support and boundary fixes (2026-09-02, second session)

This section supersedes the "Current blocker" and "Best next diagnostic
step" sections below; they remain as history.

### Platform support

GNU Emacs supports Linux and macOS, so Emaxx's native compiler now does too.
The supported native targets are `aarch64-apple-darwin` and
`x86_64-unknown-linux-gnu`; any other target fails at compile time with a
`compile_error!` in `abi.rs` naming what is missing (measured layout
constants and a generated subroutine table).

- `abi.rs` carries per-target layout constants measured from the pinned
  reference build's own headers (`sizeof`/`offsetof` against its configured
  `lisp.h` and `thread.h`), never derived by hand.  The Linux x86-64
  values are `SYS_JMP_BUF_SIZE` 200, handler `val`/`next`/`jmp` offsets
  24/32/64, `HANDLER_SIZE` 304, `THREAD_STATE_SIZE` 520, and
  `m_handlerlist` at 96.  The previous unmeasured Linux guess of
  `HANDLER_SIZE` 312 was wrong; `HAVE_X_WINDOWS` adds
  `x_error_handler_depth` to `struct handler`, so the constants belong to
  the same X11/GTK configuration as the generated table.
- `NativeThreadState` is `align(8)`, matching GNU's `GCALIGNED_STRUCT`; the
  earlier `align(16)` padded the Linux size to 528.
- `runtime.rs` has a System V x86-64 `native_call_trampoline` beside the
  Darwin arm64 one.
- One generated table per target:
  `generated_native_subrs_aarch64_apple_darwin.rs` (1,445 entries, moved
  unchanged) and `generated_native_subrs_x86_64_unknown_linux_gnu.rs`
  (1,467 entries).  `mod.rs` selects by `#[path]` under `cfg`.  The Linux
  table was verified against the live oracle: `(mapcar #'subr-name
  comp-subr-list)` and every `(subr-arity ...)` are identical, and the
  ABI hash it produces (`30.2-319e459f`) is the oracle's
  `comp-native-version-dir`.
- `tools/generate_native_subrs.rs` no longer carries a hand-maintained,
  NS-specific source list.  It preprocesses `emacs.c` with the reference
  build's own compiler configuration, walks the argument-less startup calls
  in `main` in order, follows each into the built source that defines it,
  and reads that function's active `defsubr` calls.  Build it with
  `rustc --edition 2024 -O tools/generate_native_subrs.rs`; run it as
  `TOOL GNU-SRC GNU-BINARY OUTPUT` against a configured, built tree.  The
  Darwin table was not regenerated in this session (no Darwin build was
  available); regenerate it on the Mac and confirm it is byte-identical to
  the moved file before trusting the tool on Darwin.

### Linux oracle used here

The Linux pin in `compat/oracle.lock.linux.json` (`6ee5c136`) is not in the
public `emacs-mirror` history, so this session built the oracle from the
Darwin pin `636f166c` (which is on `emacs-30`) on Ubuntu 24.04 with
`CC=gcc-14`, libgccjit 14.2, and
`--with-native-compilation=aot --with-x-toolkit=gtk3 --with-cairo
--with-harfbuzz --with-tree-sitter --with-xml2 --with-gnutls --with-modules
--with-sqlite3 --with-rsvg --with-webp --without-compress-install`.
The generated table's configuration strings record exactly that build.  If
the real Linux oracle is configured differently, regenerate the table from
it; the eln directory name and the subroutine order both depend on it.

### Root cause of the `listp 3` blocker

The Mac blocker was a stale cons mirror.  `pcase` builds placeholder cells
`(pcase--placeholder . N)` inside natively compiled `pcase.eln` and later
patches them in place with `setcar`/`setcdr`.  Rust had already mirrored
those cells, and the mirror was not refreshed before Rust read them, so the
expansion still contained the placeholders (`3` was a placeholder index).
`(macroexpand '(pcase x ((pred stringp) 1) (_ 2)))` reproduces it in one
line and now expands correctly; `(pcase 3 ((pred integerp) 'int) (_ 'other))`
evaluates to `int`.

The general fix is in `runtime.rs`:

- Every mirror records the two words on which its Rust cell and native cell
  last agreed (`mirror_words`, shared with frame entries as `AgreedWords`).
- `reconcile_mirror` brings one cell back into agreement per field: a native
  word that changed since agreement is decoded into the Rust field; a Rust
  field that changed since agreement is encoded into the native word.  It
  runs whenever an existing mirror crosses the boundary in either direction
  and for every queued interpreter write.
- A nested native call's tracked cells stay registered with the enclosing
  frame when it returns, and results of nested calls stay tracked, because
  the enclosing generated frame can keep writing to them.
- At every primitive entry from generated code, all active frames' tracked
  cells are scanned (two loads and two compares each) and only changed cells
  are reconciled.  The scan of the innermost frame alone was insufficient: a
  `byte-compile-lapcode` frame patched jump-table tag cells that had been
  mirrored by an outer frame, and the `maphash` callback, which GNU also runs
  as bytecode, read the stale ids.

The remaining structural gap is documented under "What is not done".

### Boundary audit findings fixed

- `comp-el-to-eln-filename` searched a temporary `eln-cache` when BASE-DIR
  was nil; comp.c searches `native-comp-eln-load-path` for the first
  writable directory (creating one), expands a relative BASE-DIR against
  `invocation-directory`, and appends `preloaded/` when
  `comp-file-preloaded-p` is set or the file is named in `LISP_PRELOADED`.
  Ported.
- `comp-el-to-eln-rel-filename` normalized the path hash from `load-path`;
  comp.c replaces a match of `\`[[:ascii:]]+/VERSION/lisp/` or of the dump
  load directory (`source-directory` + `lisp/`) with `//` using
  `string-match`/`replace-match`.  Ported.
- `native-elisp-load` did not rename a file loaded earlier in the session
  before reopening it (comp.c does, so `dlopen` returns a fresh handle),
  and `load` of an `.eln` bypassed it.  Ported, and `load` now routes
  through it as lread.c does.
- `load_comp_unit`: a unit whose `*saved_cu` is already set is reused
  without touching its static relocations; a recursive load of a unit does
  not rewrite its ephemeral data; every load registers the unit in
  `comp-loaded-comp-units-h`.  Ported.
- `assq`/`rassq` fell back to structural equality for keys that were not
  scalars; fns.c uses `EQ`.  This merged distinct `(args-out-of-range)`
  condition constants in `.elc` output.  Fixed with `values_eq_in_env`.
- `string-search`, `make-char`, and `map-charset-chars` rejected an explicit
  nil optional argument; generated code always passes every argument, so a
  nil START-POS from `warnings.el` failed with `integerp nil`.  Fixed to
  GNU's `NILP` semantics; `string-search` now signals
  `(args-out-of-range HAYSTACK START-POS)` like fns.c.
- `type-of` answered `native-comp-function` for a native function; data.c
  answers `subr` there and only `cl-type-of` distinguishes native functions.

### Verified on Linux

- `cargo check --all-targets`, `cargo fmt --all -- --check`, and
  `cargo clippy --all-targets --all-features -- -D warnings` are clean with
  rustc 1.98 (the lockfile requires 1.95 or newer; the container had 1.94).
  Sixteen pre-existing `chunks_exact(2)` uses were changed to `as_chunks`
  because the newer Clippy flags them.
- `cargo test --lib lisp::native_comp`: 16 passed, 1 ignored, including the
  x86-64 trampoline round trip and the libgccjit smoke test.  Two tests
  that pinned the Mac's libgccjit 15.2.0 now assert the loaded library's
  own version.
- `.elc` identity: `batch-byte-compile` of `test/src/comp-resources/comp-test-funcs.el`
  is byte-identical to GNU's output when the byte compiler runs as bytecode
  (`load-no-native`), and a `cond` switch fixture is byte-identical with GNU's
  own native `bytecomp.eln` executing inside Emaxx.

### What is not done

- No `.eln` produced on Linux has been compared with GNU yet.  With the
  system native units loaded, `batch-native-compile` of `comp-test-funcs.el`
  did not finish within 15 minutes, and the unchanged `comp.el` self-compile
  did not finish within an hour.  The cost is the per-primitive scan of all
  tracked cells: correctness required scanning every active frame, and the
  top-level load frames of the preloaded units track tens of thousands of
  cells.  Plain batch startup on this container is 22 seconds against
  GNU's 0.2 seconds, and the fixture byte-compile adds about 4 seconds under
  bytecode but exceeds 15 minutes under native `bytecomp.eln`.
- The dual representation is the structural cause: GNU has one heap, Emaxx
  keeps a Rust value and a two-word native mirror, and every write by
  generated code is invisible until Rust compares memory.  Options, in
  order of preference: page-granular dirty tracking of the mirror arena
  (`mprotect` plus a fault handler marks dirty pages; scan only those);
  tracking mirror identity inside `ConsCell` so Rust reads verify against
  memory in O(1); or dropping frame tracking for cells no Rust reference
  can reach (`Rc::strong_count` equal to the registry's own references).
  This is the next design decision and it is Ray's.
- Byte-compile warning positions differ in one case: GNU reports the free
  variable `c` in `comp-test-silly-frame2` at 711:10, Emaxx at 712:10
  (the second occurrence).  The artifacts are unaffected.
- `normal-top-level` does run (the backtrace shows it), but nothing here
  verified that `native-comp-eln-load-path` gets the user `eln-cache`
  entry the way `startup.el` arranges it; the comparisons pinned
  `native-compile-target-directory` on both editors instead.
- The three uncommitted measurement-harness files on the Mac (compat
  runner eln cache) are still needed to run `comp-tests.el` through the
  harness.

### Historical comparison qualification

The Linux artifact comparisons configured the upstream
`native-compile-target-directory` variable through a command-line Elisp form.
That did not modify either source tree, but it is not admissible as final proof
under Ray's stricter rule against diagnostic/injected Elisp. Do not repeat or
cite those runs as final native-comp compatibility proof. Re-run the final
GNU-versus-Emaxx corpus through ordinary entry points with no helper `.el` and
no injected Elisp, and compare every complete artifact with `cmp`.

### Verification after applying this patch on Darwin/arm64

- `cargo fmt --all -- --check`, `cargo check --all-targets`, and
  `cargo clippy --all-targets --all-features -- -D warnings` pass.
- `cargo test --lib lisp::native_comp` passes: 16 passed, 0 failed, and 1
  deliberately ignored stress probe.
- The unchanged ordinary `comp.el` self-compile command below ran for two
  minutes without the former deterministic `wrong-type-argument listp 3`
  failure, which previously occurred in 13–19 seconds. The run was then
  interrupted because the known all-active-frame mirror scan makes continued
  compilation prohibitively expensive on the loaded machine. This confirms
  progress past the old blocker, not completed compilation or `.eln` identity.

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

## Current blocker: `listp 3` while unchanged `comp.el` is loaded (superseded, see the Linux section above)

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
