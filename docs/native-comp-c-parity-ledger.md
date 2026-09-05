# Native-comp GNU C parity ledger

This is the finite working list for the current native-comp C-behavior audit.
It covers every direct C-owned primitive fast path currently exposed by the
native runtime, plus the cross-cutting evaluator/runtime contracts encountered
on the native-comp hot path.  Finding another C-owned path requires adding it
here before changing it.

Status meanings:

- `registered`: the anti-cheating gate names the GNU owner and a Rust contract
  test, but the item has not yet completed the focused C source audit.
- `partial`: some GNU branches are proven, but open branches are listed.
- `verified`: focused contract test, anti-cheating gate, and unchanged GNU
  source `.eln` identity evidence all pass.
- `open`: confirmed GNU/Rust difference; no implementation is accepted yet.
- `in progress`: implementation exists but the complete item gate has not run.

A semantic deviation from GNU C is forbidden by default.  It may be retained
only after Ray explicitly approves a visible exception with a defensible
justification, a regression test, and performance evidence.  The approved
exception inventory is currently empty.

No item authorizes new Elisp, changes to GNU Elisp, a compatibility runner, or
calling GNU C from Emaxx.  GNU C is the behavior oracle; C-owned behavior is
implemented in Rust and GNU Elisp remains GNU Elisp.

## Direct primitive boundaries (35)

| ID | Lisp primitive | GNU C owner | Status |
|---|---|---|---|
| P01 | `<` | `data.c:Flss` | registered |
| P02 | `<=` | `data.c:Fleq` | registered |
| P03 | `=` | `data.c:Feqlsign` | registered |
| P04 | `>` | `data.c:Fgtr` | registered |
| P05 | `>=` | `data.c:Fgeq` | registered |
| P06 | `atom` | `data.c:Fatom` | registered |
| P07 | `bare-symbol-p` | `data.c:Fbare_symbol_p` | registered |
| P08 | `car` | `data.c:Fcar` | registered |
| P09 | `car-safe` | `data.c:Fcar_safe` | registered |
| P10 | `cdr` | `data.c:Fcdr` | registered |
| P11 | `cdr-safe` | `data.c:Fcdr_safe` | registered |
| P12 | `consp` | `data.c:Fconsp` | registered |
| P13 | `eq` | `data.c:Feq` | registered |
| P14 | `listp` | `data.c:Flistp` | registered |
| P15 | `nlistp` | `data.c:Fnlistp` | registered |
| P16 | `null` | `data.c:Fnull` | registered |
| P17 | `symbol-value` | `data.c:Fsymbol_value` | partial; see V01-V06 |
| P18 | `symbolp` | `data.c:Fsymbolp` | registered |
| P19 | `type-of` | `data.c:Ftype_of` | registered |
| P20 | `assq` | `fns.c:Fassq` | registered |
| P21 | `eql` | `fns.c:Feql` | registered |
| P22 | `get` | `fns.c:Fget` | registered |
| P23 | `identity` | `fns.c:Fidentity` | registered |
| P24 | `length` | `fns.c:Flength` | registered |
| P25 | `mapcar` | `fns.c:Fmapcar` | registered |
| P26 | `maphash` | `fns.c:Fmaphash` | registered |
| P27 | `memq` | `fns.c:Fmemq` | registered |
| P28 | `nreverse` | `fns.c:Fnreverse` | registered |
| P29 | `plist-member` | `fns.c:Fplist_member` | registered |
| P30 | `cons` | `alloc.c:Fcons` | registered |
| P31 | `list` | `alloc.c:Flist` | registered |
| P32 | `make-closure` | `alloc.c:Fmake_closure` | verified |
| P33 | `apply` | `eval.c:Fapply` | registered |
| P34 | `funcall` | `eval.c:Ffuncall` and `funcall_subr` | partial; see L04-L07 |
| P35 | `stringp` | `data.c:Fstringp`; `lisp.h:STRINGP` | verified; direct tagged-word test, see L14 |

## Cross-cutting hot-path contracts (15)

| ID | GNU C contract | Status | Exact remaining work |
|---|---|---|---|
| L01 | `eval.c:eval_sub` depth limit | verified | Extra platform stack probe removed; GNU post-increment/floor behavior tested; smallest unchanged `.eln` is identical. |
| L02 | `eval.c:eval_sub` `maybe_quit` placement | verified | Focused quit-order test passes; smallest unchanged `.eln` is identical. |
| L03 | `eval.c:eval_sub` `maybe_gc` placement | open | Map GNU allocation counter and live-byte threshold to all Rust-owned and native-mirrored Lisp objects before implementation. |
| L04 | `eval.c:Ffuncall` depth limit | verified | Extra platform stack probe removed; focused test and smallest unchanged `.eln` identity pass. |
| L05 | `eval.c:Ffuncall` debugger-on-exit | open | Direct native return path does not yet perform GNU's `backtrace_debug_on_exit`/`call_debugger` branch. |
| L06 | `eval.c:funcall_general` target resolution | partial | Builtin, native, and byte-code direct targets are covered; verify indirection, autoload, interpreted closure, and invalid-function fallback behavior. |
| L07 | `eval.c:funcall_subr` arity and nil padding | partial | Fixed optional padding, wrong arity, and direct builtin errors are tested; audit every 0..8/MANY/UNEVALLED branch. |
| L08 | `alloc.c` GC live-byte accounting | partial | Threshold/default behavior is implemented; prove the census includes exactly the same live Lisp object classes and bytes as GNU at equivalent state. |
| L09 | `eval.c:Flet`, `FletX`, `funcall_lambda`, `Fmake_interpreted_closure`: retain original lexical symbols | verified | Original handles survive binding, parameter, environment, and debugger projections; symbol/name roots and native assq identity have explicit contracts. Full GNU execution, nine artifact fixtures, and paired no-regression measurements pass. Dynamic/global/local-special storage remains separate work. |
| L10 | `print.c:print_object` `PVEC_CLOSURE`: print the stored slots | verified | Rust's extra free-variable scan/environment trimming is removed. GNU `cconv.el` remains the owner. Four Rust print tests and unchanged GNU `cconv-safe-for-space` pass, with full native execution/artifact/performance gates. |
| L11 | `lread.c:Fintern`, `intern_driver`; `alloc.c:init_symbol`; `data.c:Fsymbol_name`: retain the Lisp name separately from private lookup identity | partial | Newly allocated symbols retain their Lisp name separately from the lookup key; ordinary-table Fintern name identity, existing hits, shorthand/purecopy, and type-check order pass focused contracts and full native gates. Full obarray storage/lifetime and early compact-abbreviation-table parity remain open. |
| L12 | `bytecode.c:exec_byte_code` argument words; `print.c:Ferror_message_string` result ownership | verified | Bytecode preserves original argument objects; diagnostics are mutable at their C-owned producer, with the original-string fast return preserved. Caller identity/mutation, unchanged Eshell, 177 native cases, nine artifact fixtures and paired timing checks pass. Other string-representation and error-rendering gaps are not closed by this bounded ownership correction. |
| L13 | `data.c:Fbyte_code_function_p`; allocation-free bytecode slot-type inspection | verified | Removed the parameter-name-based interpreted-lambda false positive and type-name spoofing; classify closures by their code-slot tag. Internal shape checks no longer copy string/vector payloads just to inspect their types. Both contracts failed before the repair; final 116 Rust tests, 177 GNU native tests and nine artifact fixtures pass, with paired measurements recorded below. This does not close all VM storage or closure-dispatch gaps. |
| L14 | `data.c:Fstringp`, `Farrayp`, `Fsequencep`, `Fchar_or_string_p`; `doc.c:Fdocumentation_stringp`; `lisp.h:CHECK_STRING` | verified | Replaced copying predicate calls with GNU tag checks, included char-tables in sequence classification and corrected `character.h:MAX_CHAR`. Native stringp keeps its argument as a tagged word. Three contracts failed on old code. The shared vector-as-string fallback is removed too, with ordinary/native `Fstring_bytes` argument-identity checks. Final Rust/native/artifact gates pass; timing differences reverse sign, with no demonstrated speedup. Remaining integer/record/string representations are separate audits; no new cache is introduced. |
| L15 | `alloc.c:Fmake_byte_code`; `bytecode.c:exec_byte_code` original constants-vector ownership; `lread.c:bytecode_from_rev_list` reader ownership | verified | Two Rust controls fail on the old copied constants (stale values between and during calls). The correction retains the original vector, reads its current slots and removes duplicate VM-side reader construction. Original-vector identity/lifetime and executable adversarial controls pass. Final 109 focused tests, warning gates, all nine artifact fixtures and 177/177 unchanged GNU native execution tests pass. Both timing pairs use less CPU with identical artifacts; see evidence below. No invalidation cache or Elisp change is introduced. Broader opcode caching and make-byte-code validation remain separate work. |

L15 evidence (2026-09-05, baseline `c236c21`):
`/private/tmp/emaxx-bytecode-constants.aiZp6U`. The two mutation controls
fail before the repair (`red.log`); final focused optimized verification
passes 109 tests, zero failures, one separate timing probe ignored (44.72s).
The third contract proves that no constant snapshot retains a removed value;
it is not a claim of universal GC timing equivalence. The existing decoder
fixtures and assertions are unchanged, with the existing reader boundary
now explicitly completed before their VM execution. The ordinary production
reader is independently exercised by the nine unchanged-source artifact
fixtures (eight complete `.eln` files identical, one correctly absent).
Ordinary native execution passes 177/177 with fresh native helpers and GNU's
native-enabled default selector (ERT 2394.882872s, zero unexpected).

Full unchanged `comp.el` timing pairs, all serial and unprofiled:

| Pair/order | Before wall/user/system | After wall/user/system | GNU wall/user/system |
|---|---|---|---|
| 1: before, after, GNU | 125.25 / 115.53 / 6.62 | 122.45 / 112.95 / 6.75 | 19.27 / 18.26 / 0.49 |
| 2: after, before, GNU | 157.43 / 143.26 / 8.87 | 141.64 / 129.79 / 7.87 | 18.81 / 18.10 / 0.44 |

Each pair uses one identical source path and fresh per-editor homes. GNU
runs last, preventing reuse of its oracle output by either Emaxx run. All
three 881,800-byte files match within each pair, SHA-256
`341e99373c8cab5ad30e33aa366c917bde6ba6c6d7f7cb6c129623b8acb02b4c` (pair 1),
`89e4886cbdb286396ef4ca202fdc92965e8de8520e51f115cadfd5f50dc5833d` (pair 2).
User CPU is 2.23% and 9.40% lower after the correction. Neither pair shows
a regression, but the host variability prevents claiming a precise stable
speedup. Means are 129.395 / 121.37 / 18.18s before / after / GNU; current
Emaxx is still about 6.68x GNU including startup. R02c, V02-V05, the full
live-object census and broader VM contracts remain open. Final formatting,
all-target check and strict all-target/all-feature Clippy are clean; the
pre-commit adversarial gate passes 17/17. Repeat it before pushing.

The L11 audit also requires the symbol's stored name to be a traced child,
as in `alloc.c:mark_objects` (including string intervals). The internal lookup
key must not be counted as an additional Lisp string. The legacy standard-
obarray membership/identity split and compact early-image abbreviation-table
fallback remain outside this bounded name-field correction; they are not
declared GNU-equivalent by this checkpoint.

L13/L14/P35 evidence (2026-09-05, baseline `36dd465`):
`/private/tmp/emaxx-r02c.YelOLe`. The final optimized Rust selection passes
116 tests, zero failures, with one separate timing probe ignored (64.63s).
The existing anti-cheating gate now executes negative controls for interpreted
lambda parameter-name spoofing, vector-as-string spoofing and GNU's character
range; it is not merely a source-name inventory. All 17 audit tests pass.
Formatting, all-target check and strict all-feature/all-target Clippy are clean.
The ordinary editor passes 177/177 unchanged GNU native tests and all nine
identity fixtures (eight complete `.eln` files equal, one correctly absent).
The suite uses GNU's native-enabled default Makefile selector, not its
native-disabled selection. No GNU source, Elisp, ABI layout or generated
manifest changes are part of this correction.

Full unchanged `comp.el` timing pairs (seconds; all jobs serial, no profiler):

| Pair/order | Before wall/user/system | After wall/user/system | GNU wall/user/system |
|---|---|---|---|
| 1: before, after, GNU | 114.93 / 106.74 / 5.45 | 120.08 / 110.82 / 6.44 | 15.63 / 15.00 / 0.33 |
| 2: after, before, GNU | 130.84 / 120.18 / 7.16 | 123.45 / 113.79 / 6.72 | 18.37 / 17.34 / 0.43 |

Each pair uses one identical source path and fresh per-editor homes. GNU runs
last, so neither Emaxx run can consume that pair's oracle artifact. All three
881,800-byte artifacts match within each pair: SHA-256
`c76ea387beac23f13d4aa11e72f31ebfb31459bfa397f5719f2672683f902cc7` (pair 1),
`4f305d60e4104eb41cc77f332669dda53d21e18bf4a4e0f1b92e9b54d363b1f6` (pair 2).
User CPU means are 113.46 / 112.305 / 16.17s (before / after / GNU).
The +3.8% and -5.3% paired differences do not establish a speedup or a
repeatable regression on this loaded host; current Emaxx is still about
6.95x GNU. These are GNU behavior corrections with bounded evidence, not
completion of R02c or universal runtime/GC parity.

L10 was exposed by the focused closure regression run (104 passed, 2 failed).
The closure-print failure reproduces on untouched pushed merge `38b2ee8`;
GNU's unchanged `cconv-safe-for-space` test passes. GNU `print.c` prints slots
0 through PVSIZE without recomputing capture policy. Rust instead re-trimmed
the environment when printing, after GNU Elisp had already consumed the
`:closure-dont-trim-context` marker. Removing that Rust Elisp-policy duplicate
is mandatory, not a performance exception. An existing early-image print test
that expects cconv filtering must load the existing full GNU image; keep its
Elisp and expected result unchanged. The other baseline failure exposes a
private-obarray suffix through `symbol-name`; it is tracked separately and is
not evidence of failed callback mutation.

L09 (2026-09-05) is a correctness dependency of the native-comp execution
checkpoint, not a new test runner or a claim about total compatibility tests.
The normal GNU `comp-tests-fw-prop-1` fails even on pushed merge `38b2ee8`.
Rust-only tracing showed its hash-table callback retained `checker` but lost
the generated accumulator `--cl-var--` (internal identity 1584 in that run).
Native `assq` compared two different symbol pointers with that same internal
name immediately before the unbound-variable error. Log:
`/private/tmp/emaxx-v06-execution.tyIV38/emaxx-fw-prop-identity.stderr`.

GNU `eval.c:Flet`/`FletX` put the original VAR into each lexical binding cons;
`funcall_lambda` retains the original parameter symbol; `Fmake_interpreted_closure`
stores ENV unchanged. Emaxx's string keys and parsed parameter strings broke
that object ownership before unchanged `cconv.el` ran its filter. The draft
stores `SymbolName` handles in lexical frames and parsed parameter vectors,
clones those handles into public binding conses, and preserves them through
closure reconstruction, handler bindings, eval-alist input, and debugger
snapshots. It removes per-key String copies and avoids manufacturing Lisp
symbol objects during projection. It does not change native EQ/assq, binding
order, frame-cell sharing, dynamic/global storage, or GNU Elisp filtering.
The retained symbols must also be traced as roots: GNU `alloc.c` marks both
the car and cdr of each binding cons. The old value-only typed-frame walk
omitted binding names; update frame, parameter, and debugger-root walks with
the representation change and test reachability before any public projection.
The new Rust contract also checks that two distinct uninterned symbols with
the same visible name remain distinct and that sibling projections reuse the
same binding conses without allocating more uninterned symbols. Acceptance
and paired performance evidence are recorded below; do not claim this fixes all symbol
storage (locally-special metadata and global tables remain separate work).

Initial L09 evidence: all 49 native-runtime correctness tests pass, with one
separate timing benchmark ignored. Format, all-target check, and all-feature
Clippy pass without warnings. The ordinary release editor passes the formerly
failing unchanged GNU test `comp-tests-fw-prop-1` (1/1, exit 0, 47.77s wall /
42.85s user CPU; `emaxx-fw-prop-fixed.stderr` in the diagnostic directory).
This is not yet the complete 177-test gate or a performance measurement.

## Vector storage contracts (4)

These units were added after the profile tied 547 samples to Emaxx's tagged
cons-vector registration while `alloc.c:Fmake_closure` was hot.

| ID | GNU C contract | Status | Exact remaining work |
|---|---|---|---|
| S01 | `alloc.c:allocate_vectorlike`, `allocate_clear_vector`, `Fvector` | verified | One identity-bearing contiguous Rust vector allocation and GNU's shared zero-length vector behavior are covered by focused tests, anti-cheating gates, and unchanged `.eln` identity. |
| S02 | `data.c:Faref` and `lisp.h:AREF` | verified | Vector slots are read directly with GNU bounds/type behavior; the reconstructed slot cache is gone. |
| S03 | `data.c:Faset` and `lisp.h:ASET` | verified | The same vector allocation is mutated in place with GNU identity, return value, bounds, and type behavior. |
| S04 | `alloc.c:sweep_vectors` and `vector_cells_consed` accounting | partial | The tagged-cons representation and weak registration ledger are gone; prove GNU's allocation rounding and `vector_cells_consed` counter exactly. |
| S05 | `alloc.c:Flist` versus vector construction and sequence consumers | in progress | Integration removed the remaining symbol-tag conversions. Optimized reader/types/list-vector regressions passed 70/70; unchanged upstream sequence/JSON replays and native identity remain pending. |

## Integration boundary findings (2026-09-05)

| ID | GNU C contract | Status | Exact remaining work |
|---|---|---|---|
| I01 | Configured `syms_of_*`/`defsubr` ABI inventory | verified on Darwin | Mandatory full-table regeneration and the combined header-layout extension passed on the pinned Darwin build, including calling conventions and configuration strings. Linux's differently configured inventory is not verified by this Darwin result. |
| I02 | `lread.c:maybe_swap_for_eln` load-mode bookkeeping and selection | in progress | Suppression bookkeeping, missing-source warning/error branches, default-directory-relative paths, readability, timestamps, and main's detached C slots passed three focused Rust contracts. Audit reentrant file-handler snapshot ordering and run unchanged GNU native tests before closing. |
| I03 | `lisp.h`/`thread.h` native layout constants | verified on Darwin | The independently compiled C probe measured Darwin `struct handler` as 288 bytes, contradicting the incoming 304-byte constant. The corrected production constant and all eight sizeof/offsetof facts pass independent regeneration. After the separate I08 repair, all nine unchanged artifact fixtures pass on the integrated 36dd465 tree. Linux has not been measured here. |
| I04 | `comp.c` direct forwarded-slot readers | baseline and focused follow-up verified | Native output controls, ABI validation, loaded-unit/deferred tables and purify state read retained C slots; Elisp-owned compiler options keep ordinary lookup. Output-path detachment and loader regressions pass. The integrated pre-I09 baseline passes all nine artifact fixtures and 177/177 default native cases. The attached-slot lexical correction passes its separate 86-test validation under I09. |
| I05 | `lread.c:defvar_lisp` static roots and `alloc.c:mark_roots` | focused verification passed | Startup records initialized source-declared C slots separately from ordinary value cells. Repeated makunbound retains the first C value; plain-variable fallback suppression retains names only. Detached DEFVAR_LISP roots are marked, and clearing the direct quit slot releases its saved root. Positive/negative weak-GC, image-clone and release contracts pass; optimized check and strict Clippy pass. Nine artifacts and 177 native cases pass on the pre-I09 integration baseline; buffer/keyboard/no-protect roots still require their owning-storage audit. |
| I06 | `data.c:store_symval_forwarding` Boolean declaration coverage | focused verification passed | Replaced the incomplete Boolean-only list with 908 typed C/Objective-C declarations, including all 184 Boolean names. Four adversarial parser tests, mandatory full regeneration, independent Boolean scan, existing GNU-oracle assignment contract and initialized-versus-inactive runtime tests pass. Runtime coercion requires C startup registration, not merely an all-platform declaration. Optimized check, strict crate Clippy and standalone-tool Clippy pass. |
| I07 | `comp.c:loadsearch_regexps` compile-time path ownership | verified on integrated Darwin tree | Build-time Lisp-directory configuration replaces mutable source-directory lookup. Regexps are cached only after successful initialization, independently rooted and graph-copied with image aliases. Both configured-header path constants and all eight ABI facts pass independent regeneration. Source-directory rebinding, hashing, cache identity, weak GC and image-copy tests pass. Optimized check, strict Clippy and all nine native artifact fixtures pass. |
| I08 | `bytecode.c:exec_byte_code` argument identity; C-owned string allocation | verified on integrated Darwin tree | The initial 109-test batch exposed one Eshell failure. Correcting the error-message-string allocation owner and preserving its multibyte buffer result resolved it. Both branches' allocation/alias assertions and unchanged Eshell pass in the combined 123/123 batch, zero ignored. All nine native artifact fixtures now pass, including the formerly failing 86,384-byte shared-string fixture and all 881,800 bytes of comp.el; optimized check and strict Clippy pass. This does not claim every other string producer has been audited. |
| I09 | `lread.c:Vload_path` and other attached C slots versus `eval.c` lexical environments | focused verification passed | Attached C-slot reads consult actual symbol storage, not the caller's lexical environment. Direct evaluator fields and detached slots retain their existing paths. The three-slot identity/dynamic-binding/detachment contract and native runtime, loader/output, C-root and mandatory audit regressions pass: 86/86, zero failed/ignored. Rustfmt, optimized check and zero-warning Clippy pass after correcting test-module placement. The 177/177 native run predates this change and is baseline evidence only; final full-tree gates remain required. |

## Native Lisp-word representation contracts (6)

These units split the profiled native-word bridge into exact GNU object-access
contracts.  They do not permit changing the `.eln` ABI or exposing a second
Lisp object model.

| ID | GNU C contract | Status | Exact remaining work |
|---|---|---|---|
| R01a | `lisp.h:XUNTAG`, `XPNTR`, and generic typed object access | verified | Live native words now reach their stable pointed-to object directly; swept diagnostic words retain a safe checked path. Focused lifetime tests, anti-cheating gates, exact unchanged `comp.el` output, and a 107.95s to 70.33s user-CPU improvement pass. |
| R01b | `lisp.h:XSYMBOL`; `data.c:find_symbol_value` initial symbol access | verified | The stable pointed-to object owns its value, tag, identity, and cache metadata, so live reads and updates dereference it directly without a reverse map or slot-vector lookup. Three focused contracts, 16/16 anti-cheating gates, warning gates, and whole-file unchanged `comp.el` identity (`da155503…e454b4`) pass. Low-load structural runs of 62.62s and 61.37s user CPU showed no regression against reverse-map controls of 61.80s and 64.73s. The earlier pointer-to-slot implementation was rejected after repeatable low-load regressions. This unit does not verify P17: Emaxx's value/redirect cell and epoch cache remain a separate documented deviation. |
| R02a | `lisp.h:XSYMBOL` and `XSETSYMBOL` object identity | verified | Symbol and builtin handles use stable object identity, never symbol-name bytes; focused identity/symbol tests, anti-cheating gates, and exact unchanged `comp.el` output pass. |
| R02b | `lisp.h:make_lisp_ptr` pointer tagging | verified | The private bridge cache indexes one pre-mixed identity word with exact equality and GC reuse. The rejected clustering mix was removed; lookup fell from 2,847 to 9 leaves in equal five-second samples, with no measured CPU regression. |
| R02c | Native words remain native words across C-owned calls | open | Eliminate repeated encode-cache lookup by carrying the assigned native word in the object representation itself. A separate 64-entry Rust cache was rejected after two exact-output runs regressed to 88.87s and 94.87s user CPU. |
| R03 | GNU's single object storage across generated code and primitives | partial | Conses expose their two ABI words directly and vectors share one slot array; remove the remaining mirror/reconciliation work only where both mutation directions and GC visibility are proven. |

## Symbol-value contracts (6)

These units follow `data.c:find_symbol_value` in source order.  Passing the
combined `native_symbol_value_and_type_of_follow_data_c` test is supporting
evidence only; each branch remains open until its storage, mutation, GC, error,
and performance behavior is separately proven against GNU.

| ID | GNU C contract | Status | Exact remaining work |
|---|---|---|---|
| V01 | `CHECK_SYMBOL`; `lisp.h:XSYMBOL` | verified | R01b proves that a live bare-symbol word directly addresses its complete stable Rust object without a reverse map or slot-vector lookup. Positioned-symbol handling remains on the general `CHECK_SYMBOL` path as in GNU. |
| V02 | `SYMBOL_PLAINVAL`; `SYMBOL_VAL` | open | Replace the external `OrderedBindings` lookup plus process-wide epoch cache with a symbol-owned value cell that returns its native word directly, is updated by the exact GNU mutation transitions, and is traced as part of the symbol object. |
| V03 | `SYMBOL_VARALIAS`; `SYMBOL_ALIAS`; `goto start` | open | Store the alias redirect in the symbol object and follow the target object directly, including cycle/error behavior established by GNU's alias-creation path. |
| V04 | `SYMBOL_LOCALIZED`; `SYMBOL_BLV`; `swap_in_symval_forwarding` | open | Represent the buffer-local-value object and its selected/default cells directly; remove name-map probing from the native branch without changing dynamic/default/local precedence. |
| V05 | `SYMBOL_FORWARDED`; `SYMBOL_FWD`; `do_symval_forwarding` | open | Represent each supported forwarding kind and read its C-owned Rust field directly, preserving normalization and buffer-local forwarding behavior. |
| V06 | `Fsymbol_value`; `Qunbound`; `xsignal1 (Qvoid_variable, symbol)` | partial | Original-symbol error payload and CHECK_SYMBOL ordering pass focused contracts in ordinary, native-direct, and native-subr dispatch, 16 anti-cheating gates, and all nine unchanged-source artifact fixtures. A single stored Qunbound word remains dependent on V02. |

Rejected V02 draft: replacing the process-wide epoch with an optional word in
`NativeHandle` was still a shadow cache, not `struct Lisp_Symbol::val.value`.
It left `OrderedBindings` authoritative, conflated an unpopulated cache with
GNU's distinct plain-unbound, alias, and localized states, and routed writes
through a second identity hash lookup.  Although an unmatched pair of
eight-second samples showed fewer `invoke_native_symbol_value` leaf samples,
five paired full compiles were performance-neutral under machine load and
could not justify behavior that differs from GNU.  None of that draft is
retained.  V02 requires one authoritative per-runtime symbol object and one
plain value word, including the exact `Qunbound` word.

Follow-up V02 drafts rejected (2026-09-05): boxing the global binding and
linking a raw pointer from the native handle avoided the epoch, but encoding
its Rust `Value` on every read regressed three paired full `comp.el` compiles.
Baseline/draft process CPU seconds were 61.46/62.78, 61.49/65.41, and
61.89/63.99.  Its nine-fixture artifact comparison passed; that did not make
the performance regression acceptable.  Adding an optional encoded word to
the boxed cell introduced a second synchronized value again, plus three
runtime-selection paths that discarded encoding errors.  The strengthened
test's GC assertion was invalid: `begin_garbage_collection` outside a native
call returns without collection.  The cross-owner raw-pointer lifetime and
native-heap ownership were also not proven.  That final variant had only a
focused test/check, not the previous variant's artifact proof; its lone
loaded-host timing is not evidence of a gain.  All eight Rust files changed
by these drafts have been restored to the pushed merge checkpoint.  V02
remains open; no new cache or storage implementation is accepted.

V06 bounded error correction (2026-09-05): `data.c:Fsymbol_value` passes its original SYMBOL
to `xsignal1` after `find_symbol_value` returns `Qunbound`.  Rust's
`eval/bindings.rs:symbol_value_cell` constructed `Void` using the resolved
alias target; the ordinary primitive also lost the original positioned-symbol
object, and reconstructing an uninterned symbol from that string created a
different symbol object.  The new Rust contract failed before the fix.  Both
public `Fsymbol_value` boundaries now replace only that `Void` error with the
original Lisp object.  Other errors propagate unchanged; CHECK_SYMBOL still
runs first.  `LispError`'s host formatter preserves the diagnostic prefix for
the object-carrying condition.  GNU's error text is from `data.c:syms_of_data`.

The two new Rust tests cover an alias chain extended after its first link,
plain and uninterned symbol pointer identity, positioned-record identity,
and type-error ordering with positioning disabled.  Each runs through the
ordinary primitive, direct native helper, and native subr-import dispatch.
The existing symbol-value/GC test now establishes a live native-call boundary
and proves an unreachable cons was swept; its previous outside-call collection
was a no-op.  The native C audit gate requires the two new contracts to remain
present.  Runtime tests: 48 passed, zero failed, one benchmark ignored;
anti-cheating: 16 passed.  Formatting, all-target check, and all-feature Clippy
passed without warnings.  The nine-fixture whole-artifact gate passed in
368.21 seconds: eight complete `.eln` files identical, including 881,800 bytes
for `comp.el`, and neither compiler emitted an artifact for the unchanged
no-byte-compile fixture. Before L09-L11, the execution checkpoint failed: the ordinary
GNU Makefile test command selected 177 cases; GNU passed all 177, but Emaxx
passed 52, failed `comp-tests-fw-prop-1`, then aborted while printing the
failure, leaving 124 cases unrun.  The two helper `.eln` files were genuinely
compiled and loaded.  Logs are in
`/private/tmp/emaxx-v06-execution.tyIV38/{emaxx,gnu}.stderr`.  An isolated
verbose replay on the untouched `38b2ee8` binary also failed this test and
aborted its reporter (0/1, exit 2, 79.04 seconds; `baseline-fw-prop.stderr`
in the same directory).  Its reported condition argument is
`comp-tests-fw-prop-1-f`. Temporary Rust tracing subsequently exposed the
original `void-variable --cl-var--` (a generated lexical variable); GNU
`comp.el` prepends the function name when re-signaling it. The reporter
separately fails with `wrong-type-argument number-or-marker-p nil`.
The failing test predates this V06 change. Publication and timing were paused
for diagnosis. After the L09-L11 corrections, the ordinary release editor
passes all 177 native-enabled GNU tests, zero unexpected results, exit 0:
`/private/tmp/emaxx-native-final.3mbz17/emaxx.stderr`. Both helper libraries were
freshly compiled/loaded. ERT took 1090.59s; total wall/user/system times were
1128.20/1034.87/26.77 seconds. The 80 return-type tests each retain GNU's normal
compiler subprocess, taking about 11-12 seconds apiece. Final Rust contracts
pass 69/69 (including 49 native contracts and 16 anti-cheating gates), with one
separate timing probe ignored. This is new post-merge evidence, not the older
pre-merge 177/177 claim. The final nine-fixture artifact ladder also passes
(229.33s), including all 881,800 bytes of `comp.el`; log `identity.log` beside
the execution log. The ordinary editor also passes unchanged GNU
`cconv-safe-for-space` (1/1, exit 0, `cconv.stderr` in the same directory).

Final paired measurement, unchanged GNU `comp.el`, fresh processes and homes,
ordinary `-Q --batch -f batch-native-compile`, same source path within each
pair. Baseline is untouched pushed merge `38b2ee8`. Times include startup.

| Pair / order | Baseline user CPU | Current user CPU | GNU user CPU |
|---|---:|---:|---:|
| 1: baseline, current, GNU | 62.10s | 62.93s | 8.44s |
| 2: current, baseline, GNU | 63.00s | 62.15s | 9.07s |
| Mean | 62.55s | 62.54s | 8.76s |

No material regression or speedup is established by these samples. Emaxx is
still approximately 7.1x GNU on this workload; performance parity is not
achieved. In both pairs, baseline and current entire artifacts compare equal
to GNU with `cmp`. Logs/artifacts: `/private/tmp/emaxx-v06-measure.zr27c1`.
Pair-one SHA-256: `45a1720fb510102a768eba529da7edd923cac9f456fa7d5b0c46d555baf591f3`;
pair two: `ca4858f93845598f532f68995f59660579ecf1d2806b2376b814f837638e00e0`.
Source paths differ between pairs, so hashes are not universal golden files.

Final adversarial review: no GNU source changes, authored Elisp, new runner,
GNU runtime delegation, copied oracle output, weakened artifact comparison,
test-name production branch, suppressed Rust warnings, or new cache is
retained. The printer's Rust copy of Elisp capture policy was removed. The
existing early-image print fixture now loads the GNU owner required by its
unchanged expectation. The GC test genuinely collects during a native call;
the assq negative control rejects reconstructed symbols with matching names.
The unrelated dirty compatibility reporter/honesty files are excluded from
this checkpoint. Unfixed representation items above remain open, not approved
semantic exceptions.

No successful plain-value fast path, value-cell storage, cache, or mutation
transition was changed.  This is correctness work, not a claimed compiler
speedup.  V02 storage, V03-V05 redirect representation, V06's single stored
Qunbound word, and other primitives' error payloads remain open.  In
particular, the existing epoch cache is still a documented V02 deviation; this
checkpoint neither replaces it with a new cache nor calls it GNU-equivalent.

## Post-push main integration checkpoint (2026-09-05)

After pushing `9097866`, this checkpoint merges `origin/main` at `84f342a`
(three commits after the previous main base), before starting another
performance unit. Fresh merged-tree verification is recorded below; the
earlier checkpoint's results were not reused as proof of the merge.

Concrete overlaps identified before accepting the merge:

- L04: both branches implement `eval.c:Ffuncall` depth entry. Keep the native
  branch's shared entry/exit boundary; do not add main's second counter around
  the private dispatch helper or its `direct_form_call` marker. GNU direct
  form application does not enter Ffuncall a second time.
- V05: main's `makunbound` detachment must also disconnect the native branch's
  six direct evaluator fields. `data.c:set_internal` changes the symbol's
  redirect to plain/unbound without modifying the C slot. Later Lisp stores
  are uncoerced and cannot reconnect it. `eval.c:process_quit_flag` and the
  depth-limit floor still write the C slot, not the detached Lisp binding.
  Preserve this distinction through buffer changes and image copying.
- L08: `lread.c:defvar_lisp` static-protects the C slot independently of its
  symbol. Mark those direct fields, main's detached slot values, queued thread
  events, and the new coding-system Lisp children. Copy their actual object
  graph when copying an otherwise cloneable image, never reconstruct a
  detached C field from the now-independent Lisp binding. This does not make
  images with live native state cloneable.
- S01-S03 and the C/Elisp boundary: do not resurrect the removed vector-slot
  cache or the dumped auto-buffer-local value fallback during text resolution.

Initial contracts pass 78 tests, including all 49 native-runtime correctness
tests, 17 anti-cheating checks and four new direct Rust merge contracts. One
separate timing benchmark is ignored. The existing incoming-regression replay
passes 26/26 with `LANG=C LC_ALL=C`, including main's Ffuncall test and both
earlier closure/callback regressions. An initial ambient UTF-8 replay passed
24 and failed two GNU-oracle quote expectations before Emaxx was called; no
test source or expectation was changed. One additional selected test is
Linux-only and is not counted as a pass on macOS. Format, all-target check and
all-feature/all-target Clippy with `-D warnings` pass. Logs are under
`/private/tmp/emaxx-main-84f342a.3jARTW`.

Subsequent native execution, whole-artifact comparisons and timing evidence
follow. No new Elisp, cache, runtime GNU delegation, or performance exception
is authorized by this merge.

The first artifact replay exposed L12: four fixtures passed, then unchanged
`test/lisp/emacs-lisp/comp-tests.el` differed at byte 768 (both 86,384 bytes).
The serialized constants lose GNU's shared `" *temp file*"` string references,
which shifts the data layout and machine-code addresses. The saved `9097866`
editor, compiling exactly that retained source under the same C locale,
still emits a whole-file-identical artifact. Main `17da04f` added
`Interpreter::stored_value` to every supplied bytecode argument; that helper
allocates a fresh mutable string for each compact string argument. GNU
`bytecode.c:exec_byte_code` instead executes `PUSH (*args)` unchanged. Its
allocation behavior is not an approved performance exception. Correct the
diagnostic producer without copying arguments or adding a promotion cache.
`print.c:Ferror_message_string` returns the original string for `(error STRING)`;
otherwise its `Fbuffer_string` result is an ordinary mutable Lisp string.
The existing Eshell fixture uses a wrong-type diagnostic and exercises that
producer through unchanged Elisp. Publication was paused for this correction.

The L12 correction passes 112 optimized Rust tests, zero failures, including
all bytecode tests, all native-runtime correctness tests, all 17 anti-cheating
checks, the unchanged Eshell fixture and error-message rendering. One separate
timing probe is ignored. The enhanced direct-bytecode contract requires the
original compact-string identity across repeated calls, an already-mutable
multibyte diagnostic from its C producer, mutation visible through the
caller's reference, and Ferror_message_string's no-copy fast return with
properties preserved. Format/check/strict Clippy are clean. The corrected
ordinary editor passes all nine artifact fixtures (`identity-string-fixed.log`,
216.48s): eight entire `.eln` files byte-identical, including 881,800 bytes for
GNU `comp.el`, plus the correct no-artifact result. The merged-tree native
execution run passes 177/177, zero unexpected results, exit 0, with freshly
compiled and loaded helpers (`emaxx-native.stderr`). ERT took 1076.91s;
total wall/user/system times were 1114.12/1024.41/26.13s, with GNU's normal
compiler subprocesses and no image cloning. Editor SHA-256:
`d01db433db5c3eb35a60380a7fe2f74bc7c3d4abfca3aeb813d29fd469273b8c`.

Paired regression measurements use unchanged GNU `comp.el`, the same source
path within each pair, fresh homes/processes, the ordinary
`-Q --batch -f batch-native-compile` entry and no profiler/image template.
Times include startup. Before is the saved, hash-verified `9097866` editor.

| Pair / order | Before user CPU | Merged user CPU | GNU user CPU |
|---|---:|---:|---:|
| 1: before, merged, GNU | 62.81s | 63.02s | 8.43s |
| 2: merged, before, GNU | 69.94s | 66.42s | 9.47s |
| Mean | 66.38s | 64.72s | 8.95s |

These samples show no material merge regression, but their variation does
not establish a speedup. Emaxx remains about 7.2x GNU including startup;
performance parity is not achieved. All four before/merged artifacts compare
byte-for-byte equal to their corresponding GNU artifacts. Logs and retained
artifacts: `/private/tmp/emaxx-main-84f342a.3jARTW/performance`.
Pair-one SHA-256: `19feaaabd00d11a0ddc75471f82b0924dadc7e1ae75edb272c43334bd93dd302`;
pair two: `3a68ec39956a95d2741666b4875261ef112abd2b681187788c8165a7f6d04835`.
Different source paths make these per-pair evidence, not universal golden hashes.

Final pre-commit review: all 17 adversarial gates pass, zero ignored; format,
all-target check and all-feature/all-target Clippy with `-D warnings` are
clean. No GNU source, Elisp, test selector, artifact comparison, native
backend cache or runtime delegation was added/changed by the resolutions.
The incoming argument-copy workaround is removed, not exempted. The existing
Eshell fixture remains unchanged and its Rust contract now checks identity as
well as mutation. V02-V05 representation, the complete L08 census and broader
string ownership remain open; no global runtime-equivalence claim is made.
Unrelated user compatibility-reporter edits are excluded. Repeat the audit
before pushing, then fetch/merge main before another checkpoint.

## Performance priority overlay

This ordering comes from the latest `comp.el` sample.  Sample counts locate
work; they are not timing claims.  These rows point into the acceptance units
above or identify a representation gap that must first be split into exact GNU
C contracts and added to the inventory.

| Priority | Measured Rust cost | Samples | Next bounded audit |
|---|---|---:|---|
| 1 | Native-handle encoding | 477 / 5s | Complete R02c: retain an already assigned native word where GNU simply keeps its `Lisp_Object`. |
| 2 | Remaining SipHash routing | 143 / 5s | Attribute callers and map Lisp hash behavior to GNU's exact `fns.c` functions before changing them. |
| 3 | Rust `Value` clone/drop traffic | 111 drop, 94 clone / 5s | Attribute the traffic to exact object classes before adding a representation unit. |
| 4 | Native-word decoding | 84 / 5s | R01a-R01b are verified; attribute the remaining cons-mirror branch to R03. |
| 5 | Global symbol binding lookup | 72 / 5s | Complete V02 first, then V03-V06 in `find_symbol_value` source order. |

The same profile recorded only 12 leaf samples in the general vector helper,
so S01-S03 removed vector registration from the leading costs.  `funcall` is
the dominant native caller surrounding R01 (931 inclusive samples), so P34 and
L05-L07 remain the next call-boundary audit after the representation units.

## Per-item acceptance gate

1. Read and cite the exact GNU C function.
2. Record input, result, error, mutation, allocation, GC, and call-order behavior.
3. List concrete Rust differences before editing.
4. Add or strengthen a focused Rust contract test; do not add Elisp.
5. Make one narrowly scoped Rust change.
6. Run the focused test and the anti-cheating gate with nonzero test counts.
7. Compile the smallest relevant unchanged GNU `.el` fixture with both editors
   and require complete byte-for-byte `.eln` identity.
8. Measure the relevant hot path.  Do not infer gains from load-contaminated or
   cache-warmed wall time.
9. Run formatting and warning gates before a commit checkpoint.
10. Update this ledger before starting the next item.

Checkpoint sequencing (Ray, 2026-09-05): after every commit and push, fetch
and merge the latest `origin/main` into `native-comp` before beginning the
next checkpoint. Audit semantic merge interactions, verify relevant gates,
and preserve unrelated edits. An already-up-to-date main needs no empty merge.

The nine-rung native artifact identity test and the full native-comp gate are
checkpoint tests, not per-line iteration tests.
