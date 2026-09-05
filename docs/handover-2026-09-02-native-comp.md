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
12. **After every checkpoint commit and push, merge main before starting the
    next checkpoint** (Ray, 2026-09-05). Fetch `origin`, merge the latest
    `origin/main` into `native-comp`, and audit/verify the semantic
    interactions as well as textual conflicts. Preserve unrelated work.
    If main is already an ancestor, record that it is up to date; do not
    manufacture an empty merge. Do not begin the next checkpoint on a stale
    main base.

## Current continuation checkpoint (2026-09-05)

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
