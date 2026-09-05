# Portable-dump contracts and the native-comp critical path

## Objective and current state

Build a GNU-faithful persistent startup image as soon as its actual
correctness prerequisites are satisfied, then finish the remaining
[native-comp contracts](native-comp-c-parity-ledger.md). C-owned behavior
becomes Rust; unchanged GNU Elisp remains the owner of loadup, startup,
compiler policy, and any temporary Elisp it generates itself. No authored
Elisp, replacement runner, GNU-runtime delegation, or semantic exception
is authorized.

Audit baseline: `native-comp` at `b432d86`; fetched `origin/main` at
`84f342a` is already an ancestor. The GNU source is the clean 30.2 checkout
at `636f166cfc86aa90d63f592fd99f3fdd9ef95ebd`. The retired harness edits
have been removed at Ray's request; archived stash `7cc4cb2` is not pending
work and must not be restored as part of this goal.

Evidence directory: `/private/tmp/emaxx-pdump-contracts.Yyf1mY`.

- Baseline editor SHA-256:
  `e3547c198c6b65bb551101bdf8e511963c5f1c485f23205c260547a2d1419db4`.
- Separate GNU oracle SHA-256:
  `7d8944fe2b2bdbd2856cfd4f47dbd5c80db90089ac20be641c10a348bf217e82`.
- No image writer/loader exists. `dump-emacs-portable` currently reports
  the explicit unavailable condition; `pdumper-stats` returns nil.
- `batch.rs:preload_batch_compat_libraries` runs unchanged `loadup.el`
  and catches that unavailable condition. This reconstructs state on every
  startup; it is not a restored image.
- `deep_clone_image` is an in-process test mechanism, not a portable dump.
  `NativeCompilerState::clone` refuses live compiler/native state. Do not
  remove that safeguard or silently disable native libraries.

## Admission rule

A prerequisite must identify the exact GNU contract and explain what would
be incorrect in a real image without it. An open performance item is not,
by itself, a dumping blocker. Do not require zero encoding lookups, every
dispatch optimization, or a universal runtime rewrite before implementing
the dumper. Conversely, a snapshot of duplicate authoritative state, an
incomplete root set, or stale process pointers is not an acceptable shortcut.

Classification:

- **PRE**: needed for independent, faithful image construction or ownership.
- **BUILD**: implement as part of the real writer/loader, not in a preparatory
  substitute serializer.
- **ENABLE**: must pass before ordinary restored startup is accepted.
- **LATER**: still required by the overall goal, but not presently a proven
  dump dependency.

`mapped/open` means the C owner and Rust gap were inspected; it does not
mean the complete contract is verified. Add concrete tests and evidence as
each item is implemented. Do not treat this inventory as an additional
count of tests, independent bugs, or a completion percentage.

## Finite contract inventory

| ID / phase | GNU owner and invariant | Rust counterpart / present gap | Required evidence |
|---|---|---|---|
| D01 PRE | `lread.c:load_path_default`, `init_lread`; `emacs.c:decode_env_path`: initialize paths from build/runtime state, not another editor. Subdirectory expansion belongs to GNU `startup.el`. | Worktree removes production oracle-path queries and `effective_batch_load_path`; `startup.rs` implements C path construction over the rooted Lisp list. Shared openp search fixes the ~/ failure and removes invented filename/suffix rules. Focused checks and a fresh whole-native-artifact comparison pass with GNU launches forbidden. General loader/path/error contracts remain open. | Ordinary startup and image construction succeed with GNU executable launches forbidden; exact initial paths/env handling; unchanged loadup/startup supply their own Lisp transformations. |
| D02 PRE/ENABLE | `emacs.c:main`, `keyboard.c:top_level_2`, `loadup.el`, `startup.el`: distinguish uninitialized build, initialized process, and Lisp command-line execution. | Worktree sends both ordinary CLI modes through the stored `Vtop_level` form. Duplicated Rust startup policy is removed. Focused checks pass for internal-char-font, process-mode defaults, scratch/messages startup, and the missing minibuffer clear_message operation. CLI sorting/consumption, error recovery, and real dump-build entry remain open contracts. | Ordinary CLI and dump-build routes use GNU's owning lifecycle; no new form injection or manual copy of Lisp startup decisions. |
| D03 PRE | `lisp.h` tagged object identity; `alloc.c` ownership; `pdumper.c:dump_object`: repeated references denote the same object. | R02c/R03: Rust/native bridge, cons mirrors and other object storage need per-type ownership proof. Existing shared vector corrections remain intact. | Rust identity, cycle, mutation and lifetime controls across native/bytecode/interpreted access; no second object authority in the dump. |
| D04 PRE | `pdumper.c:dump_symbol`, `dump_blv`, `dump_fwd`; `data.c:find_symbol_value`: preserve plain values, alias targets, localized value/default cells and forwarding destinations. | V02-V06 and L11: external binding tables/cache and symbol/name storage are not a license to snapshot independent copies. | One authoritative value/redirect, correct unbound state, alias/dynamic/local mutation, obarray/name identity, roots. Address remapping must preserve those relationships. |
| D05 PRE/ENABLE | `pdumper.c:dump_subr`; `eval.c:funcall_subr`: function identity/metadata survive, executable addresses are rebound for the new process. | `Value::BuiltinFunc` stores a name; `NativeFunction`/`LoadedUnit` store callable metadata and library-owned addresses. Decide the C-faithful persistent owner before encoding it. | Builtin identity/arity and native-unit ownership survive; targets are current-process Rust/native entry points. Argument-copy optimization is separately deferrable. |
| D06 PRE/ENABLE | `alloc.c:garbage_collect`, `pdumper.c:dump_roots` and `dump_metadata_for_pdumper`: correct roots, weak reachability, finalization and post-restore collection. | `Interpreter::weak_hash_reachability`, `live_object_census`, `NativeRuntime::begin_garbage_collection`; L03/L08/S04 are incomplete audits. Do not equate every host allocation or RC survivor with a GNU live object. | Complete affected root/type census; weak/finalizer controls; collection before and after restoration; no hidden roots from caches or native mirrors. |
| D07 BUILD | `pdumper.c:Fdump_emacs_portable`: enforce batch/main-thread/thread restrictions; call unchanged `load--fixup-all-elns`; collect repeatedly while finalizers run; dynamically bind command-line processing state. | `dispatch/buffer_meta.rs` implements only argument checks and the unavailable error. | Exact ordering/errors and unwind behavior; call GNU's existing Lisp fixup owner rather than implement it in Rust. |
| D08 BUILD | `pdumper.c:dump_header`, `dump_write`, `dump_do_fixups`, relocation emitters: hot, discardable and cold sections, object-start metadata and references to dump/executable locations. | No writer or persisted object layout exists. | Rust round trips at different addresses, sharing/cycles, section/alignment checks, no printed-value replay. The native `.eln` ABI must remain unchanged. |
| D09 BUILD | `pdumper.c:dump_symbol`, `dump_string`, `dump_vectorlike_generic`, `dump_obarray`, numeric and interval writers: save supported type-specific state. | Current `Value`, interpreter tables and native objects are not covered by a disk traversal. | Cover each type reached from real loadup, including closure constants/environments and mutable/propertized strings; do not silently omit types. |
| D10 BUILD | `pdumper.c:hash_table_freeze`, `dump_hash_table`, `thaw_hash_tables`: compact contents, retain weakness/test metadata, reconstruct lookup state. GNU rejects user-defined hash tests. | Rust hash records/internal lookup tables have no dump freeze/thaw path. | Key/value identity and order-sensitive behavior, weakness, thawed lookup/mutation, GNU rejection of unsupported tests. Do not serialize stale pointer hashes. |
| D11 BUILD | `pdumper.c:dump_buffer`, marker/overlay/finalizer writers, `dump_vectorlike`: type-specific persistence, nilled process/frame/window/terminal pseudovectors, special main-thread treatment and unsupported-type errors. | Interpreter/buffer/record state and OS resources currently survive only within a live process. | Preserve exactly what GNU retains; clear/recreate exactly what GNU clears/recreates; fail on unsupported objects rather than retain process handles. |
| D12 BUILD/ENABLE | `pdumper.c:pdumper_load`: validate size/magic/incomplete marker/fingerprint before using the image; reject loading over an initialized universe or loading twice. | No reader; existing `pdumper-fingerprint` is not evidence of image validation. | Missing/truncated/incomplete/incompatible-image controls, matching build acceptance, rejection before state publication. |
| D13 BUILD/ENABLE | `pdumper.c:pdumper_load`, `dump_do_all_dump_reloc_for_phase`: early relocations, executable fixups, initialization hooks, native-unit late relocations, native-subr very-late relocations, late hooks. | No ordered restore lifecycle. | Dependency/order controls, correct address rebasing and GC metadata before allocation/native calls. |
| D14 BUILD/ENABLE | `comp.c:load_comp_unit(loading_dump=true)`: retain dumped data vectors, link runtime/data imports, do not recreate serialized objects or execute `top_level_run`. | `native_comp/loader.rs:load`/`first_load` explicitly implement the ordinary non-dump path. | Mutated saved data is retained, top-level side effects do not rerun, unit sharing and anonymous functions survive, normal loading is unchanged. |
| D15 BUILD/ENABLE | `pdumper.c:dump_native_comp_unit`, `dump_subr`, `RELOC_NATIVE_COMP_UNIT`, `RELOC_NATIVE_SUBR`: clear library handles/targets in the image; reopen correct unit path, resolve native C-name, fix anonymous-lambda impure relocations. | Native registry/heap contain process-local pointers and library state. | Real native functions execute in a separate restored process, correct missing-library/symbol failures, GC guard and library lifetime checks. |
| D16 ENABLE | `emacs.c:main`, `init_environment`, `init_lread`, `after-pdump-load-hook`: run the current process's initialization around restored state; unchanged `startup.el` owns user/locale/path/customization policy. | Reconstructed startup mixes build and session work; manual helper paths must not become the restored implementation. | Fresh argv/environment/directories, batch/interactive GC defaults, unchanged startup hooks, no build-time user state leaking into a later process. |
| D17 ENABLE | `pdumper.c:Fpdumper_stats`, fingerprint/load-time reporting: only report a real restored image. | `pdumper-stats` currently correctly returns nil for reconstruction. | Accurate image filename/load time and actual restored status; do not advertise a dump during partial implementation. |
| D18 ENABLE | GNU process-level image/native-library lifetime; no equivalent to arbitrary cloning of loaded interpreters. | `test_support.rs` optional template and shared-library globals cannot be assumed independent. | Ordinary subprocess tests use the production loader; independently prove any in-process fixture isolation. Keep the live-native cloning refusal until genuine safety is established. |
| D19 ENABLE | Entire GNU native compilation/execution and shared-runtime behavior remain intact after startup changes. | Existing 177 cases, nine artifact fixtures, runtime integration tests and warning/audit gates are the starting evidence, not restored-image evidence. | Run native/artifact gates through real restored startup, then the broad integration gate once at this cross-cutting checkpoint; measure representative test improvement. |
| D20 LATER | Remaining `eval.c` dispatch/debugger branches, R02c lookup traffic and other non-blocking native/GC performance contracts. | The original native ledger remains active and unfinished. | Finish after usable dumped startup unless a concrete new dependency promotes an individual contract. Do not silently close deferred work. |

## Pending D01/D02 integration (after the load-path ownership checks)

The ordinary CLI now uses GNU's stored top-level form in both batch and TTY
mode. The C-owned call to `tty-set-up-initial-frame-faces` remains a typed
function call. Rust no longer reconstructs Lisp forms for startup Custom,
user directories, scratch/messages buffers, palettes, terminal libraries,
file visitation, or startup messages. GNU startup owns these operations.
`-L` argument order is preserved for its splice logic in `startup.el:command-line-1`.

The first integrated batch selection passed 20/21, zero ignored; TTY failed
to find `subr-x` because its legacy manual initialization never ran GNU's
subdirectory expansion (`d02-startup-focused.log`). After connecting the
interactive path to the same owner, 23/24 passed, zero ignored (build 2m35s,
tests 114.26s; `d02-startup-focused2.log`). All five new path tests passed.
TTY reached `startup--setup-quote-display` and failed on character 4194303.

`font.c:Finternal_char_font` uses `CHECK_CHARACTER` for nil POSITION, not
Rust's Unicode scalar range. With a position it checks position first, then
`CHECK_FIXNAT` for an explicit character. The worktree corrects these checks
and adds Rust-only boundary/error-order tests. Linux console glyph-table and
graphical font selection are not implemented by this bounded correction.

Further C review corrects the build/session split for `undo-outer-limit`:
`emacs.c` clears it for --batch before `syms_of_undo`, so the uninitialized
builder subsequently reinstalls 24000000. A restored batch process does not
rerun that initializer and retains nil; an interactive process retains the
dumped default. Session GC percentages remain 1.0 / 0.1. Initial frame faces
now follow session path initialization and interactive terminal setup.

The path warning check calls the internal `fileio.c:file_accessible_directory_p`,
not the public Lisp primitive. The POSIX implementation tests DIR/./ with
one `faccessat(F_OK, AT_EACCESS)`, accepting searchable but unreadable
directories and retaining the actual error. The public local-file branch
shares that operation. Locale-specific diagnostics, all filename-handler
error details, Windows relocation, and custom installation configurations
are not established by the current pinned POSIX tests.

The next selection passed 25/26, zero ignored (build 2m18s, tests 134.83s;
`d02-startup-focused3.log`). The new process-mode and directory-search checks
passed. The interactive case now reaches GNU's normal abbrev-file loading
and fails on `~/.emacs.d/abbrev_defs`: the file predicates resolve the name,
but `resolve_load_target_in_env` does not apply `openp`'s Fexpand_file_name.
Do not read or copy the user's personal definitions to diagnose this. Use
unchanged GNU fixtures under a fresh temporary home for the regression.

With a fresh HOME/TMPDIR, GNU interactive startup completes; the old test's
empty-message assertion fails because it now observes the end of startup,
not the old manual midpoint (`d02-tty-isolated.log`). A separate GNU -nw -Q
session, using only ordinary key commands, confirms its 66-character startup
message in *Messages*. The updated Rust test checks the exact message text
as well as the existing mode/size probe and passes 1/1, zero ignored, in
11.62s (`d02-tty-final.log`). See
`gnu-tty-message-observation.md` in the evidence directory. Quick startup
does not suppress GNU's normal abbrev-file loading, so tests need a fresh
home, not an assumption that -Q isolates all user state.

The focused font contract passes 1/1; explicit no-test-tree discovery passes
1/1; all 18 adversarial checks now pass, zero ignored (`d02-font-contract.log`,
`d02-no-test-discovery.log`, `d02-audit.log`). These source/contract audits
are not a claim of complete startup semantics.

The ordinary release editor rebuilt in 1m47s (`d02-editor-build.log`), SHA-256
`32c9c257423d754287e49e11c31a37114a19bb5b00cd3ae623cbeff1d01e51ec`.
The sandbox fence rejects GNU --version with exit 71 / Operation not permitted
(`d02-gnu-fence-control.log`). Under that same fence, fresh ordinary Emaxx
-Q --batch exits 0: 12.07s wall, 11.26s user, 0.15s system
(`d02-independent-startup.stderr`). This closes the observed executable
delegation failure, not the entire D01/D02 inventory.

Fresh small-artifact evidence is in `d02-artifact/`: unchanged
`comp-test-45603.el` at a common source path, clean environments, separate
homes/temp directories, Emaxx first under the GNU-exec fence, GNU last.
Both exit 0 with the same source warning. Each complete file is 34,536 bytes;
cmp succeeds, SHA-256
`c1b134b0c6af1b8e216a556721bf6b5ad0e63827a9a4efdcfddffd5b86f71eb6`.
Emaxx wall/user/system: 11.97/11.53/0.22s; GNU: 0.46/0.27/0.08s. These
include startup and are not a repeated hot-path performance comparison.

The actual Emaxx output is now under its temporary HOME/.emacs.d/eln-cache,
as unchanged startup.el specifies. The identity integration test follows
that location, gives each editor/fixture a fresh home and temporary directory,
clears inherited compiler knobs, runs Emaxx before GNU, and fails on cache
traversal errors. It does not change or normalize compared bytes. The updated
full ladder has not yet run. Latest all-target check and strict Clippy are
clean (`d02-check-latest.log`, `d02-clippy-latest.log`; Clippy 15.36s).

These remain uncommitted implementation changes. The ~/ loader gap and the
other D02 contracts are not waived by the successful small artifact or audit.
No portable dump is implemented yet.

### Focused interactive integration follow-up

The next 31-test selection passed 27 and failed four, zero ignored
(`d02-interactive-regressions.log`). One failure was the source-manifest audit's
rustfmt tool shim: the fresh HOME lost its toolchain installation and tried
to look it up over the network. Prefixing PATH with the installed toolchain
fixes that environment error; all 18 audits pass, zero ignored, in 1.68s
(`d02-audit-isolated-toolchain.log`). No source expectation was relaxed.

The scrolling and recentering failures were different test preconditions:
GNU command-line-1 executes the existing --eval samples before inserting
initial-scratch-message, but the in-process fixture now finishes startup.
An explicit Rust call to erase-buffer establishes the empty sample buffer.
The unchanged programs and original oracle expectations now pass 2/2, zero
ignored, in 23.63s (`d02-scroll-fixtures.log`). Production scrolling and
recentring behavior is unchanged. All four mode-line cases had already
passed with the same explicit sample-buffer setup.

The remaining minibuffer prompt failure exposed a real missing C call:
`minibuf.c:read_minibuf` invokes `xdisp.c:clear_message(true, true)` after
inserting initial input and before installing the keymap. The old Rust
activation never cleared the startup echo message. The new bounded operation
implements independent current/last clearing, FUNCTIONP and GC guards,
the inhibit-quit/inhibit-redisplay bindings, redisplay inhibition, the
dont-clear-message result, signal handling and nonlocal exit propagation.
GNU's callback ordering and activation unwind are part of this contract.
Focused Rust bytecode/object controls are being verified; this paragraph
does not claim their pending result.

`add_to_log` must not call `message`: that would invoke echo callbacks while
handling their errors. The existing message-log insertion sink is factored
for reuse, without claiming completion of its preexisting duplicate-line,
marker restoration, or full message3 lifecycle gaps. Those independent
display contracts are not new portable-dump prerequisites.

The clearing audit additionally requires C-slot reads (not lexical lookup),
the original slot after makunbound, and xdisp.c's initial nil callback before
unchanged minibuffer.el installs its function. The first new nonlocal-exit
test omitted an enclosing catch; GNU correctly turns that into a no-catch
signal. Its corrected Rust setup registers an enclosing catch. The callback
state test uses ordinary current-local-map/active-minibuffer-window calls,
not a synthetic Lisp variable for Rust's internal keymap storage.

Verification history: the initial clearing selection passed 5/6, including
the original TTY prompt regression, and exposed the missing catch in the
new test (`d02-clear-focused.log`). The next selection passed 8/9, including
callback activation ordering and nonlocal-entry unwind, but the new raw
C-default check found clear-message-function unbound
(`d02-clear-final-tests.log`). Registering its name alone was insufficient:
the existing constructor also requires builtin_var_value's nil initializer.
The worktree now supplies both pieces, matching xdisp.c:syms_of_xdisp, and
rechecks the C-slot/lexical/detachment contract. The earlier selection did
not run the adversarial tests because its filter used the wrong module
name; those zero selected audits are not counted as passes. The next run
uses the actual anti_cheat:: namespace (18 listed tests).

The final clearing selection passes 27/27, zero ignored, in 12.06s:
eight new C-contract tests, the original TTY prompt regression, and all
18 adversarial audits (`d02-clear-slot-tests.log`). All-target check and
strict Clippy pass without warnings (`d02-clear-slot-check.log`,
`d02-clear-slot-clippy.log`); release test compilation took 2m17s.
No code was changed during this final build.

The nearby integration selection passes 8/9, zero ignored
(`d02-clear-integration.log`, 72.93s). The remaining failure is
`native_minibuffer_runs_initial_post_command_hook_before_input`: its single
Enter event inserts a newline rather than exiting, the hook runs twice,
and input exhaustion reports quit. Passive Rust observations establish
the echo sequence `Prompt: ` then `Prompt: \n`
(`d02-entry-echo.log`). This is not yet attributed to an old defect versus
the current changes, and must not be labeled preexisting without a control.
Temporary test-only keymap observations are being used to locate the wrong
dispatch; original input and result expectations remain unchanged.

The ordinary editor was rebuilt with the clearing correction in 1m35s
(`d02-clear-editor-build.log`), SHA-256
`d40e06431f0860fed5359dafd8fc14f8aadc15220a636f655689e200459c9e8d`.
A fresh unchanged `comp-test-45603.el` comparison in `d02-clear-artifact/`
runs Emaxx first with GNU execution forbidden, and GNU last with a separate
HOME/TMPDIR. Both exit 0; whole-file cmp passes at 34,536 bytes, SHA-256
`cc82282cdfca745de0ea28a7d5765006509b13fda63f562e084fb263553961b7`.
Wall/user/system seconds are 11.37/10.41/0.20 for Emaxx and 0.43/0.26/0.07
for GNU. This is current bounded artifact/independence evidence, not a
hot-path benchmark or proof of the full identity ladder.

### D03 bounded constructor/parent identity correction

The passive keymap trace (`d02-entry-map.log`, 0/1 test) finds the base
minibuffer map's Enter binding intact, but both the completion map and the
active local map have no inherited binding. Global lookup consequently
selects newline. Reading `keymap.c:Fmake_sparse_keymap`, `get_keymap`,
`Fkeymap_parent` and `Fset_keymap_parent` identifies a concrete Rust routing
defect: `make_runtime_keymap` returns its public cons root, but both parent
primitives only match `Value::Record`. The unchanged GNU definition of
the completion map therefore cannot attach its parent.

The bounded correction resolves the original public root through the
existing owner map, without another cache, replacement map or Lisp policy.
The setter also returns its parent, as GNU does. New Rust-only tests cover
sparse/full constructor identity, inherited mutation, parent replacement
and detachment. All 31 focused checks pass, zero ignored, in 33.12s
(`d03-keymap-focused.log`): both new tests, the existing direct/inherited
walker test, the original minibuffer regression, all eight message-clearing
tests, the TTY prompt regression and all 18 adversarial audits. Enter now
exits and the original hook count is one. Temporary passive tracing was
removed before this run; the existing fixture and expectations are unchanged.
All-target check and strict Clippy are warning-free (`d03-keymap-check3.log`,
`d03-keymap-clippy2.log`); release test build took 2m16s. Surrounding keymap
and startup integration passed 43/44, zero ignored, in 230.60s
(`d03-keymap-integration.log`), including all nine previously selected
startup/minibuffer cases and the native direct keymap-mutation control.
The only failure expected `last` to be a byte-code-function and explicitly
documented the retired no-native-comp model. The actual value is subr.
The separate pinned GNU terminal oracle evaluated the existing expression
and confirmed every field of the Emaxx result, including subr
(`d03-keymap-gnu-tty/observation.md`, session 44475). The Rust expectation
is corrected without altering the existing Elisp expression or relaxing
equality. The final focused run passes 32/32, zero ignored, in 41.12s
(`d03-final-tests.log`), including that corrected expectation and the same
31 contracts/audits. Final formatting, all-target check and strict Clippy
are clean (`d03-final-format.log`, `d03-final-check.log`,
`d03-final-clippy.log`). Release test/editor builds take 2m09s / 1m31s.
The refreshed ordinary editor has SHA-256
`01763c9d5b50530233fc249f165ad63755c34a5903ff9ab7129aa50bee7d0083`;
its fresh before/after/oracle comparison passes. In `d03-keymap-artifact/`,
the pre-parent-fix binary (SHA-256 d40e06431f0860fed5359dafd8fc14f8aadc15220a636f655689e200459c9e8d)
and the final editor run first, both with GNU execution forbidden, on the
same unchanged source pathname and separate empty homes/temp directories.
GNU runs last. All three complete 34,536-byte artifacts compare equal,
SHA-256 `56e12f1da5e840980c0be88db84346b04a5dda7606e62c72d26c2303bcf1458e`.
Wall/user/system seconds: before 10.78/9.65/0.29; after 10.65/9.72/0.24;
GNU 0.46/0.25/0.08. These single cold-process samples include startup;
the saved baseline executable is at a different pathname. They are not
a repeated hot-path benchmark, a precise speedup, or a completed performance
parity claim. The whole identity ladder and canonical native suite have not
been rerun on this uncommitted startup checkpoint.

This does not complete the whole keymap contract: the current public view
nests its parent instead of using GNU's shared cdr tail, and plain
unregistered keymaps, symbolic/autoload resolution, type errors, cycle
rejection and pure-cons mutation need further C-owned corrections. These
are explicit D03 ownership/behavior gaps, not approved exceptions or a
claim that the entire getter/setter now matches GNU.

### Shared loader search correction (in progress)

The ~/ failure is not justification for adding a tilde-only fallback.
`lread.c:openp` expands the requested filename through Fexpand_file_name for
the relevant load-path entry, uses the current buffer's directory for nil
or relative bases, tries the caller-supplied suffixes in order, and stops
repeating the path search for a complete filename. Fload supplies its
suffix/no-suffix/must-suffix and load-prefer-newer policy. Existing Rust
`locate_file_search` already has part of the shared search, but the ordinary
loader bypasses it with two host-path walks.

The same source review found `eval/runtime.rs:repeated_directory_load_alias`,
which strips a repeated directory-name prefix from a missing filename.
GNU openp has no such filename rewrite. The resolver also probes process
cwd before load-path and selects .el/.elc through a private VM/source-size
preference instead of Fload's suffix list. These are existing, unapproved
deviations to remove while consolidating the actual C-owned load search;
they must not become image-building behavior. Focused controls should use
unchanged GNU source files, Lisp process-environment HOME (not personal
files), explicit default-directory/load-path, and normal handler callbacks.
Retain explicit unresolved scope for encoding, error errno, native swapping,
compressed files and descriptor lifetime until their C paths are verified.

The six new Rust controls all fail against the old implementation, zero
ignored (`d02-load-red.log`). They exercise the invented filename rewrite,
suffix policy, Lisp HOME expansion, nil-path/default-directory behavior,
predicate t, and directory rejection. The initial test build had five
ordinary Rust string-borrow errors; after correction it built successfully.
Those compiler errors are not counted as behavioral controls.

The worktree now routes interpreter loading, the batch action resolver,
locate-file-internal and executable search through the shared POSIX openp
search in `primitives/loading/search.rs`. It follows GNU's live cons-tail
iteration, suffix prevalidation, name expansion before suffix concatenation,
nil/t/function/nonnegative-fixnum predicate split, directory rejection,
and newest-within-first-path-entry selection with earlier-suffix tie breaking.
Local descriptors stay owned while candidate selection proceeds.
Fget_load_suffixes reads its C slots and calls the C concat primitive; the
ordinary resolver uses these suffixes rather than a private bytecode mode.
The duplicated batch resolver, repeated-directory filename alias, source-size
preference and EMAXX_BYTECODE_VM resolver override are removed. Source-only
test preconditions use the ordinary load-suffixes variable. No Elisp source,
expression, wrapper or GNU file is added or changed.

The public load route now supplies nosuffix/mustsuffix and GNU's retained
substitute-in-file-name operation; its first handler probe no longer consults
default-directory for a raw relative name. locate-file-internal accepts GNU's
2..4 arity and reports the actual found path instead of a provenance rewrite.
Broader provenance remapping elsewhere is not closed by this correction.
The initial all-target check passes (`d02-load-check3.log`). Subsequent
execution and artifact results are below; the full Fload contract remains open.

The focused optimized run now passes **45/45**, zero ignored, in 64.46s
(`d02-load-green.log`): all eleven new search controls, the shared batch
resolver, load-path ownership, public/internal locate-file cases, source/
bytecode selection, missing-program errno, unchanged-loadup startup and the
two minibuffer integration cases, including all 18 adversarial checks.
Strict all-target/all-feature Clippy is clean (`d02-load-clippy2.log`).
Release test build: 2m18s. These results do not close the later Fload branches.

The ordinary release build completes in 3m01s (`d02-load-editor-build.log`;
this command built the existing auxiliary binaries too, so future editor-only
builds should specify --bin emaxx). Editor SHA-256:
`d81bebfe6f90975130a4f44e36f0188d111acb9e14cbc4a6cc61ed15781a3e78`.
Fresh whole-artifact evidence is in `d02-load-artifact/`. The retained
pre-search editor (SHA-256
`01763c9d5b50530233fc249f165ad63755c34a5903ff9ab7129aa50bee7d0083`)
and changed ordinary editor run first, serially under the GNU-execution
fence. GNU runs last. All use the same unchanged comp-test-45603.el pathname
and separate initially empty HOME/TMPDIR trees with cleared environments.
All three exit 0 with the same source warning. Both complete-file cmp checks
pass: 34,536 bytes, SHA-256
`ebfde280ff7c365856be3fa6566473e9bb7bd8e74fcf365ae28a229d4a1b7363`.
Wall/user/system seconds: before 11.75/10.29/0.22; after 11.44/10.41/0.26;
GNU 0.45/0.26/0.07. These single cold-process observations include startup;
the retained baseline binary is at a different pathname. They establish
neither a precise speedup nor hot-path performance parity. The full identity
ladder, canonical native suite and broad gate have not been rerun for this
uncommitted search unit. No persistent dump exists yet.

Final all-target check (`d02-load-final-check.log`), formatting
(`d02-load-format.log`), strict Clippy and git diff --check are clean.

Explicit remaining scope includes preserving the selected open descriptor
into the actual file reader, the found-file handler branch and user-init-file
update, full filename encoding/error behavior, native substitution lifetime,
compressed loading and the remaining build/startup lifecycle. These are
unapproved gaps to correct, not permission to replace GNU behavior.

The follow-up C read (`lread.c:Fload`, lines 1488–1738) makes the descriptor
handoff precise: direct bytecode/early-source reading uses the selected fd;
GNU closes it before calling the unchanged Vload_source_file_function for
ordinary source, and before module/native loading. The Rust path currently
closes during resolution and reopens in load_file_strict; it also bypasses
that source-loader callback. Correct these owning branches, including their
dynamic load context, rather than imposing a blanket keep-open policy or
copying load-with-code-conversion/jka-compr Elisp into Rust. They remain
separate unverified contracts after the search correction.

### Fload source-owner and descriptor handoff (in progress)

The subsequent worktree implements the actual `lread.c:Fload` handoff in
`primitives/loading/file.rs`. Ordinary public load, interpreter loads and
require now use that owner. Direct reading receives the selected `File`,
not a filename to reopen; ordinary source closes it before calling the
unchanged `Vload_source_file_function`. The callback owns its return value,
decoding, source buffer, history, after-load hooks and completion messages.
Those Lisp behaviors are not copied into Rust. Native/module branches close
the selected file before their respective loaders.

The outer C context now includes raw/found handler calls with GNU's distinct
argument lists, `Vuser_init_file`'s raw assignment, the rooted static
`Vloads_in_progress` stack and its greater-than-three prior-occurrences
check, lexical/warning bindings and their unwind order. The old duplicate
batch search/reopen and interpreter load wrapper are removed. Require honors
a nil load callback result. These statements describe the implementation,
not completed verification of every branch.

Three focused callback controls fail on the previous implementation
(`d02-handoff-red.log`, 0/3): it ignores the callback and instead attempts
to evaluate source, and does not propagate the callback's throw. They now
pass alongside unchanged GNU startup (`d02-handoff-green1.log`, 4/4,
zero ignored, 10.32s). The surrounding serial optimized selection also passes
48/48, zero ignored, 62.84s (`d02-handoff-integration1.log`), including all
18 adversarial gates. The first strict check was clean after removing a
dead old error helper. Additional descriptor, detached-field and recursive
stack tests are being verified; they are not included in the 48-test result.

The next seven-contract-plus-audit run initially passed 24/25, zero ignored
(`d02-handoff-contracts2.log`). The descriptor-close probe incorrectly
expected file-attributes to return nil for a closed /dev/fd entry; both
GNU's `file_attribute_errno` and Rust signal on EBADF. The probe now uses
file-exists-p with an explicit open-descriptor positive control. No
production descriptor behavior was changed to satisfy that mistaken test.
The direct-reader inode-replacement, detached source-callback field and
recursion/unwind controls already passed in that run.

A separately rerun preexisting provenance test fails its locate-file
expectation (`d02-handoff-provenance1.log`, 0/1): it expects a nonexistent
build-tree path instead of the runtime file actually found. GNU
`Flocate_file_internal` returns openp's found name, independently of old
load-history provenance. Only that expected value/comment is corrected;
the existing Elisp fixture and the separate history assertions are unchanged.

The corrected combined optimized run passes **53/53**, zero ignored, in
64.62s (`d02-handoff-green3.log`). This includes both descriptor contracts,
all five source-callback/recursion controls, the corrected preexisting
provenance test, all previous search/startup/minibuffer controls and all
18 adversarial gates. Strict all-target/all-feature Clippy is clean
(`d02-handoff-clippy4.log`, 15.13s); all-target check is clean
(`d02-handoff-final-check.log`, 8.40s). The release test build took 2m18s.
These focused results do not close the explicitly listed remaining C gaps.

The ordinary editor build completes in 1m36s (`d02-handoff-editor-build.log`),
SHA-256 `00fa75ad352e8e6cc8d94a6b8badbb885b3a037d39e8a405e8ffbc95907cf2a3`.
Fresh evidence is in `d02-handoff-artifact.n0tlSg/` under the same evidence
root. The retained pre-handoff editor is the earlier search binary
`d81bebfe6f90975130a4f44e36f0188d111acb9e14cbc4a6cc61ed15781a3e78`.
Both Emaxx runs finish before GNU runs, with GNU launches forbidden by the
execution fence, separate initially empty HOME/TMPDIR trees, cleared
environments and the same unchanged comp-test-45603.el source pathname.
All three exit 0 with the same expected undefined-function warning.

Both whole-file cmp comparisons pass: **34,536 bytes**, SHA-256
`0efe5f2ae45611af5159f5b2e8c3545f0d9303c63d7236a1ad76c17de66eed38`.
Relative artifact name:
`.emacs.d/eln-cache/30.2-adba4e3f/comp-test-45603-d2d90c95-7ed4d447.eln`.
Wall/user/system seconds: before 11.02/9.81/0.30; after 10.72/9.72/0.27;
GNU 0.47/0.25/0.08. These are single cold-process observations, including
startup, with the baseline executable at a different pathname: no precise
speedup or hot-path parity claim. Whole-artifact identity for this fixture
does not prove the open source-buffer/header contracts or dump restoration.
Formatting, git diff --check, Clippy and all-target check are clean; GNU
remains clean at 636f166cfc86aa90d63f592fd99f3fdd9ef95ebd. No new Elisp is
authored. No commit/push, full native suite, full identity ladder or broad
gate has occurred for this incomplete loader unit.

Two additional existing source-buffer integrations pass, zero ignored, in
20.32s (`d02-handoff-buffer-integration.log`): eval-buffer symbol interning
and faceup's directory context across load/eval-buffer/eval-defun. These
fixtures do not assert Feval_buffer's explicit FILENAME argument or early
macroexpander availability. The next bounded correction is those C-owned
contracts, with Rust-level controls and unchanged GNU source: preserve the
requested history filename and decide eager expansion once at read/eval-loop
entry. Keep GNU mule.el unchanged and do not infer full Feval_buffer parity
from the two passing integration cases.

### Feval_buffer filename and eager-owner selection (in progress)

GNU `lread.c:Feval_buffer` uses its third argument unless nil, then falls
back to the buffer's filename. `readevalloop` checks a non-nil source name
with CHECK_STRING and chooses eager expansion once at entry: disabled when
the owner is undefined or the source name ends in .elc. It initializes
current-load-list from that source name. Rust `eval_buffer_impl` instead
ignores the third argument, while both buffer-reading branches always invoke
the eager owner. This breaks the C/Elisp source-loader handoff, whose unchanged
GNU caller explicitly supplies the intended history name.

Four new Rust controls are being checked against the old implementation:
explicit filename on an empty buffer without visiting it, invalid filename
type, unchanged GNU comp-test-45603.el before the eager owner exists, and
the same unchanged source with a .elc history name and a deliberately failing
ordinary C callback standing in for the eager owner. These are C-boundary
tests, not a replacement macroexpander or new Lisp fixture. Remaining
Feval_buffer flags, loop ownership and history sharing contracts are not
silently considered closed by this bounded unit.

All four old-code controls fail (`d02-eval-buffer-red.log`, 0/4, zero
ignored). The worktree now selects the explicit/fallback filename, checks
its type, freezes the eager-owner decision before binding current-load-list,
and passes that decision through both existing buffer-reader branches.
The history helper accepts a Lisp filename, including nil; a nameless
buffer now has its own dynamically scoped history entry instead of inheriting
an enclosing load's list. This does not repair the helper's existing
snapshot-based history representation or partial-buffer history merging.

The four regressions and added nil-filename isolation/replacement control
pass **5/5**, zero ignored, 0.41s (`d02-eval-buffer-green1.log`). All-target
check is clean (`d02-eval-buffer-check1.log`, 7.85s); strict all-target/
all-feature Clippy is clean (`d02-eval-buffer-clippy1.log`, 11.86s).
Release test build: 2m13s. The wider integration/audit selection subsequently
passed **60/60**, zero ignored, 83.20s
(`d02-eval-buffer-integration1.log`). It includes all 18 adversarial checks,
the preceding loader checks and the existing source-buffer integrations.
The previous whole-artifact proof predates this buffer change; a fresh
checkpoint comparison is required.

The C review identified work that remains after these bounded corrections.
Do not describe a verified intermediate commit as a complete loader:

- `safe_to_load_version`/`Fload`: validate explicit .elc headers, source-newer
  warning behavior and old-version reader/doc-string handling. The new
  header probe's unibyte conversion is addressed below. GNU's canonical
  table starts ASCII-only, but `set-standard-case-table` can replace it.
  Arbitrary replacement canonical tables remain a shared regexp-engine
  gap, not an approved exception or an exact-regexp claim.
- `Feval_buffer`/`readevalloop`: filename and eager-owner selection are now
  corrected, but several flags, entry/unwind bindings, point handling,
  custom-reader behavior and reader-object materialization remain incomplete.
  The real GNU source callback reaches this path. Fix the C reader/evaluator
  owners, not `mule.el:load-with-code-conversion`.
- Direct-branch preloaded-file-list/purecopy bookkeeping, complete module
  context and native candidate publication/lifetime still require review.
- The batch reconstruction entry still calls a legacy direct loadup reader
  to stop at the unavailable dumper. It is not a real dump build or restore
  path; the ordinary C lifecycle remains an obligation.

No new Elisp or GNU-source edit is made. No persistent image exists, and
the broader native suite/identity ladder are not implied by these controls.

### Bytecode-header unibyte matching checkpoint audit

GNU `search.c:fast_c_string_match_internal` first invokes
`fns.c:string_make_unibyte` and matches the byte input with
`casetab.c:Vascii_canon_table`. `init_casetab_once` initializes that table
with ASCII mappings; `set_case_table(standard=true)` can replace it.
The initial Rust helper omitted the pattern
conversion and decoded input bytes as Unicode Latin-1. That is a new
implementation defect, not an accepted semantic exception.

Two Rust controls fail against that helper (`d02-header-red.log`, 0/2,
zero ignored): the complete 256-by-256 quoted-byte matrix first fails on
byte 0x80 matching itself, and a multibyte pattern whose low byte is A fails
to match ASCII a. The matrix also checks ASCII-only case folding and that
Lisp match data is unchanged. No Lisp expression or fixture is authored.

The correction reuses the existing `string-make-unibyte` C primitive and
the existing byte8 string representation. In that representation only
ASCII characters have case pairs; non-ASCII bytes are not Unicode letters.
This verifies the initial canonical-table behavior, not support for arbitrary
replacement tables. That existing shared regexp-engine limitation remains
open; do not turn a passing default-table control into a universal claim.
No new cache, regexp grammar, private filename rule or GNU-runtime call is
introduced. Both controls pass (`d02-header-green1.log`, 2/2, zero ignored,
0.02s); all-target check and strict Clippy are warning-free. The expanded
checkpoint integration and refreshed artifacts are pending; the older
artifact result must not be attributed to this fix.

The expanded selection passes **87/87**, zero ignored, 152.03s
(`d02-checkpoint-focused.log`), including all 18 adversarial checks, the
header controls, load-path ownership, source handoff, startup, minibuffer,
keymap parent, character-range and mode-line checks. An additional four-test
selection passes 3/4 (`d02-checkpoint-adjacent.log`, 47.00s). Its failure is
not an incorrect GV result: constructing a second interpreter while its
compiler interpreter remains alive encounters an unknown saved native unit
word during debug-early loading, before the generated file is loaded.

`comp.c:load_comp_unit` explicitly retains a previously loaded shared
library's saved unit and static relocation pointers. The Rust loader follows
that rule; a second interpreter cannot own that first interpreter's word.
The pending fixture correction releases the compiler before constructing
its fresh loader. It changes no test Lisp, expected result, production
relocation handling, native capability or compilation route. Sequential
fixture verification is required; this is not general evidence of safe
coexisting interpreters, template cloning or dump restoration. GNU's
`unload_comp_unit` clearing/finalization contract remains part of D05/D06/D15.

That sequential fixture correction now passes with the original compiled
result assertion intact. The rerun passes **24/24**, zero ignored, 60.14s
(`d02-checkpoint-adjacent-green.log`): all four adjacent cases, all 18
adversarial checks and both header tests. Combined with the preceding
87-test selection, this covers 91 distinct targeted tests, not a new count
of GNU native tests. The 177-case native suite has not been rerun here.

The ordinary editor rebuild completed in 3m18s; SHA-256:
`b5b16b0898407cf1ef2429d7555405e20aa3e3b1b037cd03a649089a3a17c10c`.
The refreshed `d02-checkpoint-artifact.lshgcO/` comparison uses one unchanged
comp-test-45603.el path, separate fresh homes/temp directories, an empty
inherited environment except toolchain PATH and LANG/LC_ALL=C, and GNU last.
Both retained pre-buffer/header Emaxx and current Emaxx run under the
GNU-execution-denying sandbox. The fresh negative control rejects GNU
`--version` with exit 71 (`d02-checkpoint-gnu-denied.log`).

All three complete **34,536-byte** artifacts are identical (`cmp` exit 0),
SHA-256 `8f7becef32ce92c4f84b53d6aa0aaab80221924e500cd475b6ff29a5e82346ca`.
The three compilers exit 0 with the same unchanged-source warning. Cold
wall/user/system seconds are before 12.34/11.08/0.31, current
11.32/10.05/0.33, GNU 0.55/0.26/0.08. This single pair includes startup and
does not establish a repeatable speedup or post-startup performance parity.
The full ordinary-editor nine-fixture ladder also passes
(`d02-checkpoint-identity.log`, 238.29s): eight complete .eln files are
byte-identical, including 881,800 bytes for unchanged comp.el, and the
no-byte-compile fixture correctly emits none. The existing integration test
was explicitly run with `--ignored`; its result is one integration test
passed, zero ignored, exercising all nine fixtures. Per-fixture source
coverage and artifact sizes are printed in that log and recorded in
`tests/native_comp_identity.rs`. This is not execution of the 177 GNU cases
and is not restored-image evidence.

Final quality gates pass without warnings: `cargo fmt --all -- --check`,
`cargo check --all-targets -j1` (11.34s), and
`cargo clippy -j1 --all-targets --all-features -- -D warnings` (15.00s).
Logs are `d02-checkpoint-{fmt,check,clippy}-final.log`. The final diff review
retains no new Elisp, production oracle call, private source/bytecode switch,
fixture-specific result, native disablement, comparison normalization or
warning suppression. GNU sources and the three retired harness files are
unchanged. Open contracts above remain obligations, not approved exceptions.

## D01 negative control and the audit blind spot (historical controls)

GNU `load_path_default` returns `PATH_DUMPLOADSEARCH` in the dump-build
phase. `init_lread` ignores `EMACSLOADPATH` during that phase. For an
initialized process it applies the documented installed/uninstalled path
rules and environment expansion. GNU `startup.el:normal-top-level` later loads
`subdirs.el`; its list expansion is Lisp-owned. Copying the oracle's final
expanded path, reading that list into a Rust replacement, or falling back to
recursive directory discovery would not implement this lifecycle.

The baseline Emaxx path is:

`batch.rs` -> `compat::emaxx_upstream_load_path` ->
`upstream_repo_load_path` -> GNU executable with an existing generated
`--eval` program. It occurs before ordinary batch work, during reconstruction
setup and again during live-path setup. This is not allowed runtime behavior,
even though the compiled native artifact itself is emitted by Rust/libgccjit.

A read-only negative control ran the unchanged baseline editor under macOS
`sandbox-exec`, allowing ordinary operations but denying `process-exec` for
both the canonical GNU executable and its sibling-symlink spelling. With
fresh HOME/TMPDIR and ordinary `-Q --batch`, Emaxx exited 2:

> oracle load-path probe failed and the silent manual-walk fallback is disabled while an oracle binary exists (finding 130): run /Users/nbmhqa186/projects/emacs/src/emacs --batch to inspect load-path: Operation not permitted (os error 1)

No GNU source/executable was moved or changed; no Elisp was authored for the
control. This must become a successful startup before D01 is closed.

The existing batch-source audit used `split("#[cfg(test)]")`. An inline
attribute on the bootstrap permit precedes the production startup functions,
so the audit never reached them. The pending audit correction recognizes the
actual trailing test-module boundary and has a Rust-only regression control
for the inline-attribute case. The expanded gate must expose the current
oracle dependency; do not whitelist that helper to make the gate green.
Prior green audits do not prove this startup path was independent.

The expanded optimized audit run completed: 18 tests selected, 17 passed,
one failed, none ignored (`audit-expanded.log`; build 3m28s, tests 3.06s).
The inline-attribute regression passes, and the production audit now fails
specifically on `emaxx_upstream_load_path`. This is the intended red control,
not a verified checkpoint and not a reason to weaken the gate. Formatting
and `git diff --check` are clean for the audit correction.

D01 is split into the initial dump-build path and the initialized-session
path. A focused Rust test now checks the initial GNU C contract: exactly the
configured source `lisp` root, before Lisp-owned subdirectory expansion.
The baseline control fails with the oracle's 25 expanded directories versus
the one configured root (`d01-initial-path-red.log`; build 4m39s, test 0.33s,
0 passed/1 failed). The first production correction now returns the
configured root directly instead of querying GNU in
`installation_lisp_load_path`. Its focused green test passed (1/1, zero
ignored; `d01-initial-path-green.log`, optimized build 4m30s). The unchanged
loadup/seq check also passed (1/1, zero ignored, 19.29s;
`d01-loadup-seq.log`). This does not yet remove the separate
`effective_batch_load_path` oracle calls or close D01/D02. Missing-path and
general installed/environment initialization are not verified by this
single valid-build-root control.

The next connected C contract is `Vload_path` object ownership:
`lread.c:syms_of_lread` declares a `DEFVAR_LISP`, and
`data.c:do_symval_forwarding` returns the stored Lisp object. Rust currently
keeps a `Vec<PathBuf>` and synthesizes a fresh Lisp list in each fallback
read. GNU `startup.el:normal-top-level-add-to-load-path` uses `setcdr` to
splice into the existing list; a reconstructed list cannot preserve that
mutation. Both Rust-only identity/splicing controls fail against the old
storage (0/2 passed, zero ignored; `d01-list-identity-red.log`, optimized
build 4m23s). This is a concrete D01/D03/V05 dependency,
not authorization to reproduce the Lisp expansion algorithm in Rust.

The pending correction replaces the stored host vector with the rooted
Lisp value, includes it in existing forwarded-field mutation/buffer switching,
and makes the loader read the C-side slot after `makunbound`. Initialization
allocates its directory strings/list once, not once per variable read. Host
path projections are temporary, never stored as another object graph. The
existing fixture copier preserves references to the same copied list; it is
not a persistent dumper. Five focused tests cover identity, splicing,
dynamic/buffer-local restoration, detached roots/replacement, and fixture
copy sharing. All five passed (zero ignored; `d01-list-identity-green.log`,
optimized build 2m40s, tests 0.01s). The surrounding evaluator-field suite
passed 15/15 (`d01-forwarded-core.log`), and unchanged loadup/seq passed
1/1 in 11.97s (`d01-list-loadup-seq.log`). Those single test timings are
not a controlled performance comparison. All-target `cargo check` passes
without warnings (`d01-list-check.log`, 12.89s).

The final focused selection adds a call to the unchanged GNU
`normal-top-level-add-to-load-path` function and a real GNU source-file
lookup after detaching/rebinding the Lisp symbol. No test-authored Lisp is
introduced. The weak-root control also requires both table entries to be
present, preventing empty traversal from passing vacuously. Strict Clippy
passes with zero warnings (`d01-list-clippy-final.log`, 17.98s); the initial
run caught undescribed `unwrap` panics in the new tests, which now have
explicit failure explanations rather than lint suppressions. The seven-test
optimized selection passes 7/7, zero ignored (`d01-list-final.log`, build
2m38s, tests 12.09s), including GNU's actual unchanged startup function.
The C-boundary source audit now
requires these contracts to remain present; the whole production audit
still must fail on the unresolved ordinary-startup oracle dependency.

The rerun confirms exactly that result: 18 selected, 17 passed, one failed,
zero ignored (`d01-list-audit.log`, 1.84s); the failure names only
`emaxx_upstream_load_path`. No whitelist exception or new Elisp was added,
and the GNU checkout remains clean. This is still not a pushable checkpoint.

An ordinary-editor small-artifact comparison passed under
`artifact-small/`: unchanged `comp-test-45603.el`, one common source path,
separate fresh homes/temp directories, clean environment, Emaxx baseline
first and GNU last. The baseline exited 0 and produced its actual `.eln`
(12.02s wall, 10.86s user, 0.32s system, including startup). That test-owned
cache file was moved to the evidence directory before compiling the candidate,
so it could not be reused. The candidate release build completed in 1m37s;
editor SHA-256 is
`ada6d80484442a5871e8b07447e0726f122f4d74d73bc5bc5dab19c0fdb53f50`.
Candidate and GNU both exited 0 with the same existing source warning.
The complete 34,536-byte files from baseline, candidate, and GNU are identical
(`cmp` exit 0), SHA-256
`508f908fd2a27a7b4f37d00270c8b263e7a45e2036b94502bb3b9087aea0aa38`.
This fixture exercises lexical closures, mapcar capture, aliases, and
conditional function selection; it is not proof for the entire artifact
ladder or the 177 native execution cases.

Candidate wall/user/system seconds are 11.98/10.93/0.30; GNU is
0.48/0.26/0.08. The baseline/candidate difference is small and opposite in
wall and CPU sign. This one pair establishes neither a material regression
nor a speedup. It includes startup and the still-unfixed oracle query, so it
does not establish post-startup native-comp performance or clean independence.

This bounded correction does not close general symbol-owned forwarding
storage (V02-V05), path decoding, file-name-handler/error semantics in
`openp`, or the D02 lifecycle. Existing loader suffix/fallback handling also
still needs its own C audit; it must not be treated as verified because a
valid string-directory case passes.

There is a second connected ownership issue: `main.rs` invokes unchanged
GNU `normal-top-level` only for `-no-comp-spawn`. The ordinary batch route
contains older manually authored startup forms. Do not repair D01 by adding
more such forms or by copying `subdirs.el` policy into Rust. Read and preserve
GNU's build/session sequencing before wiring the replacement.

The next C-owned entry contract is `keyboard.c:top_level_2`, which calls
`Feval(Vtop_level, Qt)` with the batch early-error handler, and
`top_level_1`, which checks for a non-nil stored form. Rust's existing
`run_batch_through_normal_top_level` instead constructs a fixed call to
`normal-top-level`; it does not use the value GNU startup installed in
`top-level`. Follow the stored-form contract and GNU's initialization state
when integrating ordinary CLI startup. Do not add another Rust-owned Lisp
startup sequence or special-case the identity test.

## Fresh startup baseline (diagnostic, not acceptance)

The unchanged `b432d86` binary was run three times per editor, serially,
with `env -i`, matching inherited PATH, `LANG=C`, `LC_ALL=C`, fresh separate
HOME/TMPDIR, and ordinary `-Q --batch`. No EMAXX knobs, reporter, profiler,
or custom Lisp expression were used. Logs: `startup/{1,2,3}/{emaxx,gnu}`
under the evidence directory. Both editors exited successfully in these
unrestricted runs.

| Sample | Emaxx wall / user / system seconds | GNU wall / user / system seconds |
|---|---|---|
| 1 | 18.55 / 17.50 / 0.54 | 0.28 / 0.21 / 0.04 |
| 2 | 19.70 / 18.83 / 0.51 | 0.35 / 0.25 / 0.06 |
| 3 | 18.81 / 17.99 / 0.49 | 0.28 / 0.21 / 0.04 |

Mean startup user CPU is 18.107s versus 0.223s. This includes the baseline
Emaxx oracle-probe defect and is explicitly not clean independence evidence.
Do not subtract these numbers from old full-compilation timings and call the
result a measured compiler phase. Current post-startup profiling/phase
measurements remain to be collected. Host load and cache state vary; these
three sequential samples are not a guaranteed future speedup.

## Next bounded work and acceptance

1. Finish the D01/D02 source audit and expose the missed startup dependency
   with the strengthened gate. This precedes writing a dump of that state.
2. Implement GNU's C-owned initial-path/build-state behavior; let unchanged
   GNU startup own expansion and command processing. No oracle or new Lisp.
3. Prove the denied-oracle control now succeeds, run focused startup and
   native artifact tests, and refresh startup/post-startup measurements.
4. Advance the dump-relevant D03-D06 ownership units only with explicit
   contracts, then implement D07-D15 rather than a separate snapshot system.

No runtime ownership rewrite, new image format, compatibility runner,
approved semantic exception, or claim of dumped-image support is introduced
by this inventory. The goal remains active until both the image milestone
and the broader native-comp completion conditions are met.
