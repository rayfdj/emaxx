# Native-comp GNU C parity ledger

The active goal now prioritizes a faithful persistent startup image before
finishing performance-only items. See the linked
[portable-dump contract/prerequisite ledger](pdump-c-parity-ledger.md).
Latest implementation checkpoint: `52298d7`, the bounded L08 GNU
`Fgarbage_collect` layout-size correction after the L03/L05/L06/L07 audits.
Focused testing, 18 anti-cheat gates, formatting, all-target checking, and
strict all-feature Clippy are green; the full native execution and artifact
identity gates are still pending for this checkpoint. See
[the handover's resume section](handover-2026-09-02-native-comp.md) for exact
current evidence, full-goal progress, and the exact continuation point.
This unit comes from the completed post-startup profile and GNU C audit,
not a speculative optimization. The chronology below is historical, not a claim that all its
"worktree" changes remain uncommitted.
The startup/loading checkpoint based on b432d86 now has 91 distinct targeted
tests passing, including all 18 adversarial checks, and all nine
ordinary-editor artifact fixtures passing (eight entire .eln files identical
through comp.el, one correctly absent). The GNU-execution-denied small
before/current/oracle comparison also passes. Evidence:
`/private/tmp/emaxx-pdump-contracts.Yyf1mY/d02-checkpoint-*`.
This is an intermediate correction, not a dumper or full loader completion;
the 177-case execution suite has not been rerun for it. The chronology below
retains the earlier unit results rather than presenting them as newer proof.

Its D01 negative control exposed an ordinary-startup GNU-executable
dependency that the old batch-source audit missed. The worktree removes it:
ordinary startup and a whole-identical small native artifact now succeed
with GNU execution forbidden. That is not proof of full startup parity;
the linked ledger records remaining loader and lifecycle gaps. This ledger's
unfinished contracts remain obligations, not automatic dumping prerequisites.

The D03 object audit also found parent primitives rejecting the public cons
objects returned by the runtime's own keymap constructors. The bounded
`keymap.c:Fkeymap_parent` / `Fset_keymap_parent` routing correction uses the
existing owner identity and restores inherited command lookup. Its two new
Rust controls and original Enter/minibuffer regression pass with all 18
adversarial checks (31 focused checks total). The linked dump ledger records
the broader 43/44 integration result, the separately verified stale
no-native-comp expectation, and open public-tail/error/autoload contracts.
This is not a declaration of complete GNU keymap representation parity.

The subsequent D01/D02 load-search correction removes the private bytecode
preference, source-size rule, missing-filename alias, duplicate batch resolver,
and locate-file provenance rewrite. Interpreter loads, locate-file-internal
and executable lookup share the C-owned openp search. Six old-code controls
fail; the new focused run passes 45/45 including all 18 adversarial checks,
zero ignored. Unchanged GNU loadup and minibuffer integration pass. Full
Fload/source-handler/descriptor contracts remain open. A fresh complete
34,536-byte artifact matches baseline and GNU, with both Emaxx runs under
the GNU-execution fence; see the linked dump ledger for hashes, timing
limitations and gates not rerun. No portable dump exists yet.

The next uncommitted Fload unit now calls GNU's unchanged Elisp source owner
and preserves the selected descriptor on the direct-reader branch. Three
callback controls are red on the previous code and green now; the expanded
integration selection passes 48/48, zero ignored, including 18 adversarial
gates. The final combined run passes 53/53, zero ignored, including descriptor,
detached C-field, recursion and preexisting provenance controls. This is
partial: bytecode-header/ASCII-regexp contracts, Feval_buffer history/reader
contracts and other loader lifecycle branches remain explicitly open in
the dump ledger. A fresh whole 34,536-byte artifact matches baseline and GNU
with both Emaxx runs under the execution fence (`d02-handoff-artifact.n0tlSg/`;
hashes and cold-run timing limitations are in the dump ledger). Do not infer
full native or restored-image correctness.

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

The acceptance order is mandatory: review the actual Rust diff against
the pinned GNU C first, then focused correctness and adversarial checks,
then expensive artifact/execution verification and comparable timings.
An unexplained departure fails the first stage; do not benchmark it to
decide whether to keep it. Re-review implementation changes before further
timings. A source review is not itself proof of semantic equivalence, and
neither tests nor speed can approve an unapproved semantic exception.

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

### P19 follow-up: type classification and Lisp-owned advice

The completed a94d410 post-startup `comp.el` capture now has caller
attribution, not just a leaf ranking. A streaming tree analysis accounts for
all 43,902 compiler-worker samples, excludes the blocked main thread, and
checks its collapsed leaf counts against the sampler's own summary. Nearest
owner regions are non-overlapping: bindings/variables 9,194 samples, native
object bridge 8,353, other GC 1,336, other work 25,019. This is a sampled
wall-time attribution, not a precise CPU breakdown or speedup prediction.
Evidence: `/private/tmp/emaxx-native-poststartup.mXNHW4/attribute_sample.py`
and `attribution.txt`; the underlying capture is unchanged.

The callers expose a bounded correction before a symbol-storage rewrite:
`legacy_struct_vector_type` owns 2,051 samples and `cl_type_value` 1,893
(distinct paths, about 9% combined). Not all of the latter's work is extra.
GNU `data.c:Ftype_of` (185-198) checks SYMBOLP, INTEGERP and SUBRP, otherwise
calls `Fcl_type_of` (200-306), which dispatches on the object tag/subtype.
Neither consults `cl-old-struct-compat-mode` or the public fixnum-limit
variables. The fixnum range is a C representation constant in `lisp.h`;
`data.c:syms_of_data` merely publishes read-only Lisp values for it.

Rust `dispatch/misc_keymaps.rs:type-of` instead consults the mode variable
on every slow type query and implements a partial old-struct policy itself.
GNU's unchanged `cl-lib.el:cl--old-struct-type-of` and
`cl-old-struct-compat-mode` own this policy through ordinary advice. Remove
the Rust policy; do not port the remaining advice into Rust. Separately,
`buffers.rs:cl_type_value` reads both limit variable cells before inspecting
even a non-numeric object. Replace that dependency with the existing tagged
integer representation constants, preserving actual bignum object tags.

This is a C/Lisp boundary and type-classification correction, not completion
of all P19 pseudovector branches, V02 storage, or R03 ownership. It creates
no new dump prerequisite. Both new Rust controls fail on the old code:
the original type-of subr returns a structure name without installed advice;
cl-type-of signals integerp on nil solely because an internal control
poisoned a limit variable cell. The latter is deliberately a C-side negative
control, not a claim that GNU lets Lisp assign those read-only constants.
Evidence: `/private/tmp/emaxx-native-type-tags.E73e1Z/red-controls.log`
(two selected, two failed, zero ignored). The initial build command's short
exact selector matched zero tests; `red-build.log` is not correctness proof.

The candidate removes the Rust old-struct policy and both now-unused helpers,
uses the existing fixnum representation constants, and returns bignum for
an actual allocated BigInteger. New Rust controls cover ordinary and native
subr entry points, boundary integers, a record, a vector, and scalar tags.
The first focused run passes 95 tests, zero failures; one preexisting manual
timing probe is ignored (`focused.log`, 61.62s). Both old-struct advice
integration checks pass unchanged, as do the two new ordinary/native controls
and all 18 adversarial checks. That build exposed the two unused helpers;
after removing them, the final warning-free rebuild repeats 95 passes,
zero failures and one ignored timing probe (`focused-final.log`, 61.51s).
Formatting, all-target check (11.05s) and strict all-target/all-feature
Clippy (15.62s) pass with zero warnings.

The final release editor builds without warnings (2m55s), SHA-256
`d8f0d55a78719caa23ca07dd667641a440c8a212798f12afbca506f2d2517a3c`.
Ordinary `-Q --batch` loading the unchanged GNU `cl-lib-tests.el`, followed
by its normal ERT batch entry point, completes all 46 cases in both editors:
45 pass, the same `cl-lib-nth-value-test-multiple-values` expected failure,
zero unexpected failures/skips. Both old-struct tests pass. Both emit the
same GNU cl-typep optimization notices; those are not Rust compiler warnings.
Emaxx ran with GNU executable launches forbidden; the fresh GNU negative
control exits 71. Logs: `cl-subject.log`, `cl-gnu.log`,
`gnu-exec-negative.log` in the type-tags evidence directory. This is ordinary
unchanged-source execution, not the canonical 177 native cases. The complete
unchanged-source artifact ladder also passes (`identity.log`, 215.66s):
eight entire byte-identical `.eln` files through `comp.el` (881,800 bytes),
plus the correctly absent no-byte-compile artifact; one explicitly selected
integration test passes, zero failures/ignored. Its whole-gate duration is
not a post-startup performance measurement.

Two fresh serial post-startup rounds use the unchanged validated external
`native_phase.py` observer (SHA-256
`bbec55ac08176ac28075362dcfc61c4b0991b81b832abf57f9010c7bf37966aa`).
Both editors enter unchanged `batch-native-compile` before the clock starts;
the actual return ends it. Preload reconstruction is outside the window.
Order is current/before/GNU, then before/current/GNU; every process gets a
fresh home/temp directory and the same unchanged source path/environment.

| Editor | Post-startup wall seconds | CPU seconds, including waited-for children |
|---|---|---|
| Corrected type queries | 49.2066 / 50.0012 | 48.9661 / 49.7822 |
| Saved pushed a94d410 code | 52.2847 / 52.4752 | 52.0579 / 52.2624 |
| GNU | 8.5534 / 8.5028 | 8.3658 / 8.3347 |

Paired CPU changes are -5.94% and -4.75%; baseline variation is about 0.39%
and current variation 1.67%. This supports a roughly 5–6% reduction for this
input, not a universal or exact speedup. Current mean CPU is still 5.91x
GNU (elapsed about 5.82x). All six whole 881,800-byte artifacts are identical,
SHA-256 `f2752387ccbf72e1f21def74a0e438e8890d06e86b36ab30229c39ec79821c83`.
Emaxx timing runs use the same GNU-execution-denied fence as the execution
control. Evidence: six `{current,before,gnu}{1,2}.log` files and
`timing-results.txt` in the type-tags directory. No thresholds, cache mixers,
ABI, GC ownership or Elisp changed. This accepts the bounded C/Lisp boundary
and type-query correction, not all P19 branches or general performance parity.

The after-profile completes successfully (`profile.log`, `after.sample`),
with another whole-identical artifact. The worker has 40,691 samples;
`legacy_struct_vector_type` is absent and `cl_type_value` owns only 66
samples, versus 2,051/1,893 respectively before. Nearest-owner regions:
native object bridge 8,436, bindings/variables 5,066, other GC 1,246,
other work 25,943. The tree parser accounts for the complete worker count
and validates all 491 reported collapsed leaf totals against the sampler's
own summary (`after-attribution.txt`). Sampling perturbs elapsed time;
58.55s instrumented wall / 49.02s CPU is not a third unprofiled timing.
The bridge is now the largest identified owner region. R02c/R03 caller
attribution and GNU C comparison come next, without a new cache or an
unbounded dump prerequisite.

### P13/R03 follow-up: EQ must not traverse unrelated objects (verified)

After the preceding checkpoint, the completed post-startup profile attributes 2,459
worker samples to `native_eq` and descendants, including cons decoding and
binding lookup under `memq`/`assq`. This overlaps the object-bridge region;
do not add those counts. Source review finds a concrete extra operation:
when position handling is enabled and either operand is vector-like, Rust
`native_eq` decodes both operands and invokes `values_eq_in_env`. An
unrelated native cons therefore gets a typed mirror and its fields decoded
just to answer an identity comparison.

GNU `data.c:Feq` calls `lisp.h:EQ` (1354): when the C boolean permits it,
unwrap only PVEC_SYMBOL_WITH_POS objects via XSYMBOL_WITH_POS_SYM, then
BASE_EQ the two object words. `PSEUDOVECTORP` (1093) checks the tag and
subtype; it does not read unrelated cons fields. `alloc.c:build_symbol_with_pos`
(4004) stores the bare symbol and position separately; equality only needs
the symbol. `fns.c:Fmemq` (1914) and `Fassq` use the same EQ contract.

The general Rust equality fallback also numerically compares two separately
allocated BigInteger objects. GNU EQ compares their identities; numeric
bignum comparison belongs to `fns.c:Feql` (2759). This makes the old native
result wrongly depend on whether position handling is enabled. Ordinary
`values_eq_in_env` shares that identity bug. Existing hash-table EQ matching
uses the same ordinary predicate and must continue to respect its result.

The committed bounded correction keeps native words opaque unless the
object really is a positioned symbol, then compares the resulting word
identities. Positioned-symbol field encoding remains part of the existing
bridge until R02c/R03 owns those fields directly; no new cache or alternate
symbol identity scheme is authorized. Correct ordinary bignum EQ without
changing EQL's numeric comparison. This does not close all native object
ownership or create another dump prerequisite.

The two Rust-only negative controls now pass against the corrected
implementation: EQ does not materialize unrelated cons fields, and distinct
bignums remain identity-unequal. The positioned-symbol identity matrix also
passes. The focused P13/R03 controls pass; the full checkpoint acceptance is
recorded above. No new performance claim is made by this bounded correction.

Follow-up C review found a connected V05 dependency in the old
implementation: NativeRuntime kept a `Box<bool>` snapshot of
`symbols-with-pos-enabled`, updated only at native call entry. Binding or
setting the variable inside that call did not update this snapshot, which
also backed the generated-code relocation.
GNU `data.c:syms_of_data` initializes the forwarded C boolean to false;
`do_symval_forwarding`/`store_symval_forwarding` read/write that live cell,
and `comp.c` connects the relocation directly to its address. Removing
Rust's EQ fallback can expose a stale-state error that its second Lisp
lookup partly masked. Do not accept or benchmark this candidate until
the live owner, binding/unbinding and detachment contracts are corrected
and tested against C. The committed correction uses the stable interpreter
owner directly; no replacement snapshot/refresh cache was added.

Evidence directory: `/private/tmp/emaxx-native-eq-words.aEIw2H`. Exact resume
steps and the pushed-versus-uncommitted split are in the
[current handover](handover-2026-09-02-native-comp.md#resume-here--pushed-ae12db4-and-unfinished-eq-work-2026-09-06).
No new performance conclusion is claimed by this correction.

2026-09-06 source-review follow-up: both inner-native binding controls failed
on the old snapshot implementation (`live-flag-red.log`, 2 failed, 0 passed;
an earlier test compile error executed nothing). The committed correction
moves the live bool to a stable interpreter-owned `Box<Cell<bool>>`, removes
NativeRuntime's snapshot and its entry-time lookup, and connects the loader
and all ordinary/native flag readers to that owner. Existing forwarding
store/normalization/detachment and buffer-selection paths update it; no new
refresh cache, generated-code change or Lisp policy is added. GNU
`data.c:set_internal`, `set_default_internal`, `swap_in_symval_forwarding`
and `eval.c:specbind`/`do_one_unbind` were read before making this change.
The non-vector-like EQ guard is retained. The bool is not a Lisp object/root;
ordinary cloning before any native library is loaded must copy it to an
independent allocation, while live-native cloning remains prohibited.

Added Rust controls cover both directions of binding and setting inside a
native call, the relocated address, alias binding/unbinding, selected versus
default/local cells, moves/clones, and C readers ignoring lexical shadows
and detached Lisp cells. Existing Rust fixtures that faked a C variable by
putting it in a lexical Env now set its actual value cell; expectations are
unchanged. All-target checks are clean; focused/strict/artifact acceptance
is still pending. No candidate timing has run.

The first corrected focused run now passes **128 tests, zero failures, one
preexisting manual timing probe ignored** (`forwarding-focused.log`, 87.06s),
including all 18 adversarial checks. All-target check and final strict
Clippy are clean; the initial Clippy findings were 12 new test-only unwraps,
replaced with explanatory expects. The final focused rerun also passed
128/0/1 in 86.97s; the unchanged identity ladder then passed all nine
fixtures in 213.11s (eight entire identical `.eln` files through `comp.el`,
one correctly absent). Pipeline 75946 is terminal, exit 0.

The ordinary fenced native suite failed before reaching any tests: the
child rejects comp.el's generated compiler-context file with
`invalid-read-syntax`. The saved `ae12db4` binary fails at the same stage
on the same source in independent fresh directories; this predates the
current correction. GNU accepts and byte-compiles an unchanged copy of
the rejected context. The reader/materialization investigation is in
progress; see the handover for exact files, hashes and diagnostic handles.
The fresh post-correction native execution gate now passes 177/177 with zero
unexpected results, and the nine-rung identity ladder remains byte-identical.
This accepts the reader-materialization correction for native execution; no
new performance claim is made by this bounded fix, and the broader native
object/performance and portable-dump goals remain open.

V05 correction (2026-09-06): GNU `Fset`/`Fset_default` return their original
NEWVAL and notify watchers with that original object before bool storage
normalizes it; GNU canonicalizes the `set-default` watcher operation to
`set` before invoking callbacks.
`dispatch/misc.rs` now preserves those contracts while retaining normalized
storage. Focused watcher coverage, the 18-case adversarial audit, formatting,
all-target checking, and strict Clippy are clean. The post-correction native
execution gate passed 177/177 with zero unexpected results, and the nine-rung
identity ladder passed all fixtures, including byte-identical `comp.el`.
This remains separate from full forwarded-variable parity.

## Cross-cutting hot-path contracts (15)

| ID | GNU C contract | Status | Exact remaining work |
|---|---|---|---|
| L01 | `eval.c:eval_sub` depth limit | verified | Extra platform stack probe removed; GNU post-increment/floor behavior tested; smallest unchanged `.eln` is identical. |
| L02 | `eval.c:eval_sub` `maybe_quit` placement | verified | Focused quit-order test passes; smallest unchanged `.eln` is identical. |
| L03 | `eval.c:eval_sub` `maybe_gc` placement | partial | Rust now calls the active native GC trampoline after `maybe_quit` and before depth/form dispatch; placement probe, anti-cheating, identity, and 177-case native gates pass. Full live-object census/accounting parity remains open under L08. |
| L04 | `eval.c:Ffuncall` depth limit | verified | Extra platform stack probe removed; focused test and smallest unchanged `.eln` identity pass. |
| L05 | `eval.c:Ffuncall` debugger-on-exit | verified | Native return path now performs GNU's `backtrace_debug_on_exit`/`call_debugger` branch, including the debugger-control dynamic bindings; focused, anti-cheating, unchanged-source identity, and 177-case native execution gates pass. Headless redisplay top-level unwinding remains outside scope. |
| L06 | `eval.c:funcall_general` target resolution | partial | Builtin, native, byte-code, symbol indirection, interpreted-lambda fallback, and invalid-function behavior are covered; autoload loading and the wider callable-class matrix remain open. |
| L07 | `eval.c:funcall_subr` arity and nil padding | partial | Fixed optional padding, MANY forwarding, UNEVALLED rejection, wrong arity, and direct builtin errors are tested; audit the remaining 0..8 subroutine edge cases. |
| L08 | `alloc.c` GC live-byte accounting | partial | GNU C SIZE constants now drive both live-byte totals and public `garbage-collect` rows, with exact row-size coverage; prove the census classes, allocator accounting, and free-list columns match GNU at equivalent state. |
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

R02c caller audit (2026-09-06): generated `funcall`, `apply`, and `mapcar`
already keep their arguments as native words through the direct C-shaped path.
The remaining Rust-owned round trips are explicit: public `Value` arguments at
the native entry boundary, byte-code direct calls (decode arguments, execute
typed Rust bytecode, encode the result), hash-table key/value extraction, and
relocation materialization. GNU's corresponding C paths retain `Lisp_Object`
words. No cache or object-field patch is accepted from this audit; the next
bounded implementation must first provide a runtime-owned word transport for
one of those typed boundaries, with GC and mutation coverage.

R03/D03/D06 follow-up after `a92e620`: a failing Rust control demonstrates
native GC reclaiming a cons still held by a rooted vector. The candidate
connects typed/native marking and weak-table reachability before native
sweeping. C-first review removed the added blanket synchronization pass,
corrected raw cons marking to car-first order, and moved weak-entry removal
before storage sweeping. The revised code passes 89 focused tests (one
separate timing probe ignored), all 18 included adversarial checks and
warning gates. The rebuilt current editor also passes all nine unchanged
artifact fixtures in 243.10s. Direct post-startup checks now complete too:
CPU differences reverse sign (-2.73%, +0.20%), compared with 5.82% baseline
variation; no consistent regression or precise speedup is established.
Full measured outputs remain byte-identical. This bounded correction is
accepted; refreshed precommit warning gates and all 89 focused/adversarial
checks pass (one separate timing probe ignored). Two earlier before/current pairs
completed, but include preload reconstruction and are retained only as
cold-process diagnostics, not compiler/runtime acceptance evidence. The
roughly 8x GNU whole-process comparison also does not establish a native
compiler slowdown. Exclude rebuilding preload state from subsequent
compiler/runtime timing windows; keep startup costs separate, and do not
subtract an unrelated startup run. The rejected first candidate's nine
artifact passes and two premature timing pairs (CPU +1.75% and +3.83%) do not verify this
revision. Source review and correctness/de-cheating checks precede timing.
Complete ownership/root/GC parity is not claimed. See
[the current dump-ledger unit](pdump-c-parity-ledger.md).

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

V05/D01 worktree correction (2026-09-05): `load-path` now retains the actual
rooted Lisp object, including C-side lifetime after `makunbound`, instead of
reconstructing a list from a stored host path vector. Seven focused checks
and an unchanged small whole-ELN comparison pass; see the
[portable-dump ledger](pdump-c-parity-ledger.md) for red controls and limits.
The full source audit still exposes the separate startup GNU-executable
dependency. This is not a verified V05 checkpoint and does not close the
remaining symbol-owned forwarding representation or general loader contracts.

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
