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

## Direct primitive boundaries (34)

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
| P17 | `symbol-value` | `data.c:Fsymbol_value` | open; direct tagged-object access is R01b, but the value/redirect cell still lives outside the symbol object |
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

## Cross-cutting hot-path contracts (8)

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

## Vector storage contracts (4)

These units were added after the profile tied 547 samples to Emaxx's tagged
cons-vector registration while `alloc.c:Fmake_closure` was hot.

| ID | GNU C contract | Status | Exact remaining work |
|---|---|---|---|
| S01 | `alloc.c:allocate_vectorlike`, `allocate_clear_vector`, `Fvector` | verified | One identity-bearing contiguous Rust vector allocation and GNU's shared zero-length vector behavior are covered by focused tests, anti-cheating gates, and unchanged `.eln` identity. |
| S02 | `data.c:Faref` and `lisp.h:AREF` | verified | Vector slots are read directly with GNU bounds/type behavior; the reconstructed slot cache is gone. |
| S03 | `data.c:Faset` and `lisp.h:ASET` | verified | The same vector allocation is mutated in place with GNU identity, return value, bounds, and type behavior. |
| S04 | `alloc.c:sweep_vectors` and `vector_cells_consed` accounting | partial | The tagged-cons representation and weak registration ledger are gone; prove GNU's allocation rounding and `vector_cells_consed` counter exactly. |

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
| 5 | Global symbol binding lookup | 72 / 5s | Audit P17's external value/redirect cell and epoch cache against `data.c:find_symbol_value`. |

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

The eight-rung native artifact identity test and the full native-comp gate are
checkpoint tests, not per-line iteration tests.
