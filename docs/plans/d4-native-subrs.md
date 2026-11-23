# A primitive called from native-compiled Lisp (D4): inventory and staged plan

Written on 23 November 2026 before round 374, from a reading of the tree at
round 371. Paths beginning `/home/user/` are those of the container it was
written in: `R/` is this repository and `G/` GNU Emacs 30.2's `src/`. Line
numbers are of that tree, and the expected counts are estimates made by
reading; HANDOVER.md records what each stage measured. Stages S0 and S1 are
round 374.

Read-only work: nothing was built, no cargo ran, and this is the only file written. Besides reading, I ran the prebuilt Emaxx (`target/prof-r371`) under callgrind on four byte-compiled loops (eight short runs, the profile streamed through a pipe, nothing saved) to attribute the three outliers, and re-read two profiles already in the scratchpad (`d17/ns-*.out`, `d4/bc.out`).

Paths: `R/` = `/home/user/emaxx-draft/`, `G/` = `/home/user/emacs-mirror/emacs/src/`, `rt` = `R/src/lisp/native_comp/runtime.rs`, `abi` = `R/src/lisp/native_comp/abi.rs`, `gen` = `R/src/lisp/native_comp/generated_native_subrs_x86_64_unknown_linux_gnu.rs`, `tool` = `R/tools/generate_native_subrs.rs`. Line numbers are of HEAD f30f05ee. Counts are instructions in the plain release build; "per call" excludes the loop (51 in Emaxx, 50 in GNU). Numbers marked "est." are my estimates from reading, to be replaced by the probe's.

## 0. The short version

- GNU's freloc entry for a subr is the address of the subr's C function (`G/comp.c:864-865`, `G/lisp.h:3471-3477`). Emaxx's is a generated shell that passes an index to one shared function, which finds the subr, its cached Rust entry, the interpreter and the handler state, and calls a body written for a by-name dispatcher.
- Of the 64 instructions `bufferp` costs, 40 are the shell and `invoke_subr`, and about 14 of the body's 24 are the slice, `need_args` and `Result` conventions. Only about 10 are the test GNU makes in 12.
- Three things cannot be removed from a general entry in this codebase: the thread-local read that finds the interpreter (3), the handler-list comparison (5) and the `Result` test (3). With the argument slice, the call and the frame that puts the floor at 20 to 23 instructions plus the body. A subr whose body needs none of the three (a type predicate) can cost what GNU's does.
- Proposal: the subr's `function` word names an `extern "C"` function of the subr's own arity in one of four shapes: (a) word entry, the predicate itself; (b) context entry, the interpreter read without handler sync or `Result`; (c) fixed entry, the general path inlined in the wrapper with every static fact folded; (d) MANY entry, the same over the caller's own vector. The generator chooses the shape from name lists kept inside the span the anti-cheat inventory already scans.
- Eleven stages (S0 to S10), each behaviour-neutral. Expected counts are in section 3.1.

## 1. Inventory

### 1.1 GNU

| Piece | Where | Note |
|---|---|---|
| DEFUN: a static `Lisp_Subr` whose `.function.aN` is the C function of N parameters | `G/lisp.h:3471-3477`, union at `2195-2211` | static data; nothing is learned at run time |
| freloc table: `subr->function.a0` for every subr in `Vcomp_subr_list` order | `G/comp.c:864-865` (trampolines replace an entry at `4591`) | native code calls through the table, no arity check |
| `Fbufferp`, `Fvectorp`, `Fkeywordp`, `Frecordp`, `Fsymbol_with_pos_p` | `G/data.c:478, 393, 380, 402, 359` | 8 to 14 instructions each |
| interpreter state | C globals (`current_thread`, `current_buffer`, `Vinternal_interpreter_environment`) | one load each |
| handler pop in native code: `current_thread->m_handlerlist = ...->next` emitted inline | `G/comp.c:2371-2408` | no call into the runtime |
| a signal: `xsignal` to `signal_or_quit` to `unwind_to_catch` to `longjmp`, from inside the subr | `G/eval.c:1839, 1323` | the subr never returns an error |

### 1.2 The Emaxx path for `bufferp` (64 per call against 12)

| # | Piece | Where | Instr. | What it is for |
|---|---|---|---|---|
| 1 | `native_subr_1333(arg_0)`: stores the word, passes index and slice | `gen:17798-17800`, emitted at `tool:179-190` | 8 | the freloc entry; exists because the entry is not the subr |
| 2a | `native_subrs().get(index)` and index to address | `rt:2675-2677` | 2 + 2 | the shell passed a number, not the subr |
| 2b | `full_arity_entry(len)`: load the cached word, test, compare `len` with `max_args_word` | `abi:126-140` | 3 + 3 | the Rust entry is learned at the first call (`rt:2758`, `abi:145-154`, from `abi:224-238`, from `abi:255-262`, from `R/src/lisp/primitives/dispatch.rs:191-193, 299-360`) |
| 2c | `ACTIVE_CALL.get()`, null test | `rt:2776-2779` (record at `669-675`, thread-local at `709`) | 3 | finds the interpreter, the environment and the runtime |
| 2d | `handlers_in_sync()`: `runtime.top_handler == runtime.thread.handler()` | `rt:2782`, `2252-2254` | 5 | see 1.3 |
| 2e | interpreter and environment pointers; `live_values` (free) | `rt:2786-2789`, `3182-3185` | 2 | the body's explicit parameters |
| 2f | indirect call | `rt:2787` | 1 | |
| 2g | `Result` test, `value.word()`; the error arm calls `remember_active_helper_error` | `rt:2790-2792`, `2818-2821` | 4 | see 1.3 |
| 2h | frame, return | | 3 | |
| 3 | the lifted arm: prologue, `need_args`, `args[0].kind()`, t or nil, `Ok` in two registers | arm `R/src/lisp/primitives/dispatch/predicates.rs:496-503`; lifted by `R/src/lisp/mod.rs:225-242`; `need_args` at `R/src/lisp/primitives/interactive.rs:2649-2654` | 24 | the arm's body, verbatim, as a function |

The existing profile confirms the split: `invoke_subr` 32.0, `native_subr_1333` 8.0, `predicates::LIFTED_PRIMITIVES::lifted` 24.0; GNU `Fbufferp` 12.0 (`d17/ns-e-*.out`, `ns-g-*.out`).

### 1.3 What exists only because of how Emaxx is built

1. **The interpreter is a parameter, not a global.** Every primitive takes `&mut Interpreter` and `&mut Env` (`DirectPrimitive`, `dispatch.rs:82-83`). A C-ABI callee has only its Lisp arguments, so it recovers both from `ACTIVE_CALL`, a pointer to a record in the frame that started the native call (`rt:838-870`; a nested call installs its own, `rt:1955-1967`): `interpreter`, `environment` (a `Cell`, rewritten by nested calls), `runtime`. The load is one `fs:`-relative instruction on Linux, the same cost as GNU's read of `current_thread`. The null test (2) guards a call with no active native call.
2. **The handler stack has a Rust half.** Native code pops a handler by writing `m_handlerlist` itself. Emaxx keeps, per handler, a `HandlerEntry` in `runtime.handlers` (`rt:1318`) and a registration in the interpreter (catch tag or condition-case, `rt:5373-5377`). After an inline pop these are stale until `sync_handlers` (`rt:2241-2247`, `2257-2293`) pops them. That must happen before anything that can signal, throw, allocate (a stale entry is a root GNU does not have, comment at `rt:2931-2937`) or run Lisp. The runtime cannot see the pop, so every general entry compares the two list heads. GNU has no counterpart: `handlerlist` is the only handler stack.
3. **An error is a value until the ABI boundary.** A body returns `Err(LispError)` (a boxed error; the `Result` is two words in registers, `R/src/lisp/types.rs:3675-3679`). The C-ABI function turns it into GNU's control flow in `remember_helper_error` (`rt:5293-5311`): `sync_handlers`, `signal_or_quit` (handler-bind, debugger), then `prepare_nonlocal_exit` (`rt:2345-2438`) searches the native handlers of this call, unbinds to the chosen one and returns its jmp buffer, or with no match stores the error in the call's frame (`remember_error`, `rt:2295-2304`) and returns the call's escape buffer; `jump_nonlocal` (`rt:5282-5291`) longjmps. So Emaxx also leaves by longjmp, but from the wrapper, after the body returned. What a wrapper must do: test the `Result`; on `Err` call the cold `remember_active_helper_error` and hold nothing with a destructor or a `RefCell` borrow across it (the jump skips Rust frames); return 0 only if no escape buffer is installed.
4. **`need_args` runs again** because the lifted function is the arm's body unchanged, and the arm was written for `call(interp, name, args, env)`, which anyone may enter with any count. Every caller of the pointer (`call_with_facts`, `dispatch.rs:431-451`; the VM; `invoke_direct_subr`) has already checked the count against the descriptor. It costs 2 or 3 instructions and keeps the bounds checks of `args[i]` away. GNU's function has exactly its parameters.
5. **Facts are learned lazily.** `NativeSubr` carries `kind`, `facts`, `direct` and `full_arity_entry` (`abi:36-49`), each empty until first use. GNU's subr is complete at link time.

### 1.4 Entry shapes in the generated table today (Linux: 1,455 subrs)

| Shape | Count | Emitted at | Path |
|---|---|---|---|
| `function` names a `direct_native_*` in the runtime | 36 (32 fixed, 4 MANY) | `tool:200-240` | the function itself; slow cases call `invoke_subr` through `slow_unary_subr` or `subr_index` (`rt:3187-3203`) |
| `cons` | 1 | `tool:147-153` | `invoke_cons` (`rt:5218-5220`) |
| `<` `>` `<=` `>=` `=` | 5 | `tool:154-171` | `invoke_numeric_comparison` (`rt:5192-5213`): two fixnums at once, else `invoke_subr_many` |
| fixed arity, general | 1,334 | `tool:179-190` | `invoke_subr(index, &[..])` |
| MANY, general | 58 | `tool:173-175` | `invoke_subr_many` (`rt:5222-5254`), then `invoke_subr` |
| UNEVALLED | 21 | `tool:176-178` | `invoke_subr(index, &[args])`; native code does not call these |

Arity of the fixed subrs: 0: 153, 1: 598, 2: 328, 3: 162, 4: 69, 5: 33, 6: 13, 7: 10, 8: 1. The Darwin table (1,445 subrs) has the same 36 targets and shapes.

### 1.5 `DirectSubr` kinds

`DirectSubr` (`rt:2476-2524`) names 46 subrs by `of_name` (`2527-2577`); a subr's kind is a byte of its descriptor, learned at its first `invoke_subr` (`2596-2612`, `2698-2705`). `invoke_subr` tries `full_arity_entry` first; that word is only ever written for kind `Other` (`rt:2758`), so a subr with a kind always goes on to `invoke_kind_subr` (`2710-2725`): `invoke_context_free_kind` (`2627-2672`), then `invoke_variable_subr` (`2843-2890`), then `invoke_word_level_subr` (`2959-3153`), which takes the active call, syncs the handlers (`entered_subr`, `2939-2954`) and runs a cascade of tests on the kind before the subr's own entry (`3158-3174`).

Of the 46, 33 have a direct target or a wrapper of their own and reach the cascade only from a slow path. The 13 that still arrive through a general wrapper on every call are `+`, `-`, `min`, `max`, `apply`, `mapcar`, `maphash`, `member`, `boundp`, `default-boundp`, `default-value`, `local-variable-p`, `set-default`. That is why `+` costs 119 per call (`=`, with its own wrapper, 31) and `mapcar` (3.4% of calls) passes about 60 instructions of dispatch before `invoke_native_mapcar`.

### 1.6 `keywordp`, `assoc`, `delq`

Per-call self cost of the bodies, measured this session (byte-compiled loop, argument `'foo` and `(1 2 3)` as in the probes, 100,000 against 300,000 iterations):

| subr | lifted arm | callees | body in all | GNU, whole subr |
|---|---|---|---|---|
| `vectorp` | 16 | | 16 | 8 |
| `keywordp` | 34 | `SymbolName::is_keyword` 60 | 94 | 14 |
| `assoc` | 17 | `direct_assoc_family` 51, `direct_assq_family` 104 | 172 | 117 |
| `delq` | 7 | `delete_from_list` 118 | 125 | 101 |

- **`keywordp` (+122)**: 40 are the general path. The other 80 are the body. `is_keyword` (`R/src/lisp/types.rs:714-722`) asks `is_private_obarray_symbol(self.as_str())`, which is `str::contains` with a `&str` pattern (`types.rs:2570-2572`): the general substring searcher over the symbol's text on every call, then the name string through its `RefCell` for the first byte. The arm (`predicates.rs:125-141`) also reads `symbols-with-pos-enabled` and builds the positioned-symbol `Option` before the cheap test (34 against 16 for `vectorp`).
- **`assoc` (+103)**: about 45 are the general path with three arguments. The walk itself is at parity (`direct_assq_family` 104; GNU's 117 covers `Fassoc` and `Fassq`). The rest is two layers in front of it: the arm (`lists.rs:729-733`: range check, `args.get(2)`, a three-word array) 17, and `direct_assoc_family` (`lists.rs:2133-2172`) 51 for what GNU does in about 13: it saves the registers of its own loop, decodes the key's whole `kind` for `eq_comparable_value` (`2122-2127`), builds a two-word array and calls on.
- **`delq` (+69)**: about 44 are the general path. `delete_from_list` (`collections.rs:207-244`) is 118 against `Fdelq`'s 101: `values_eq_in_env` per element, an `Option<Value>` for `prev`, the `Result` of `set_cdr`.

### 1.7 Frequency

`d4/bc.out` (byte-compiling `subr-x.el`): 74,621 calls of general or semi-general wrappers over 237 subrs. `cons` 30.9% (already under GNU), `nth` 13.7% (a general call when the index is not a constant: wrapper, `invoke_subr`, `direct_nth` at `lists.rs:1918-1926`), `=` 5.6%, `+` 5.6%, `purecopy` 3.7%, `mapcar` 3.4%, `internal-set-lisp-face-attribute` 3.1%, `>=` 2.6%, `<` 2.4%, `symbol-with-pos-p` 2.3%, `delq` 2.0%, `max` 1.5%, `vectorp` 1.5%, `keywordp` 1.4%, `recordp` 1.3%, `string-match` 1.0%, `apply` 0.9%, `bare-symbol` 0.8%, `eval`, `put`, `assoc`, `make-hash-table`, `point` 0.7%. At the costs above the excess is 2 to 2.5M of the run's 353M instructions.

## 2. Target design

### 2.1 Principle

As DEFUN does, a subr's `function` word is the address of an `extern "C"` function with the subr's own parameters whose body is the primitive. One body serves both ABIs: the dispatch arm and the C-ABI function call the same Rust function, so they cannot drift. Everything a wrapper can know statically (which subr, its arity, its entry) is a constant in the wrapper, and nothing is looked up at run time that GNU has in a static initializer.

### 2.2 Four shapes and the budget of each

| Shape | For | The entry is | Budget | Why not less |
|---|---|---|---|---|
| (a) word entry | a subr whose answer is a function of its argument words and the objects they name | the predicate: `extern "C" fn(word) -> word`, no active call, no sync, no `Result` | GNU's own count, 8 to 14 | nothing of Emaxx's is left; where Emaxx's predicate has an extra case (`recordp`'s reader form) it costs that case |
| (b) context entry | a subr that reads interpreter state and, on the taken path, neither signals, allocates nor runs Lisp | thread-local load 1, null test 2, interpreter pointer 1, the read; a second path (a signal, a rare case) passes on to the general entry | 4 + the read | the interpreter is not a global. The read itself is dearer than GNU's while the buffer is behind a `RefCell` (D13's work, not D4's) |
| (c) fixed entry | every other fixed-arity subr | in the wrapper: thread-local 3, handler comparison 5, interpreter and environment 2, argument array N + 2, direct call 1, `Result` test 3, frame and return 3 | about 20 + the lifted body (23 while the entry is read from the subr's word) | the three items of section 0. The body still pays its prologue, `need_args` and `Ok` unless link-time optimization inlines it into the wrapper, where all three fold and the cost becomes about 8 + the test itself |
| (d) MANY entry | MANY subrs | as (c) over the caller's own vector: no array, a count test against the minimum (2), a null test only when the minimum is 0 | about 22 + body | as (c) |

`bufferp` under each: (a) about 10, (c) 23 + 24 = 47 today's body, about 16 if inlined.

The handler comparison could leave (c) and (d) only by moving `sync_handlers` to the consumers of handler state (throw and signal search, handler push, collection, thread switch). That is a change to the interpreter's handler model with its own risks. I list it as an open question (4.2), not a stage.

### 2.3 Where the body lives

- **(a) and (b): generated code names a runtime function by path.** This is the existing mechanism (`tool:200-240`; `rt:3219-3778`). New entries are `direct_native_*` functions in `rt`, placed in the band between `fn slow_unary_subr` and `fn native_subr_address`. Each calls a `pub(crate)` function that the dispatch arm also calls. For a predicate the shared function is the arm's present expression moved into a function, not a new reading of GNU's macro: Emaxx's `recordp` also accepts `record_literal_items`, `atom` excludes vector-like conses, and so on.
- **An entry with a second path** (a signal, a rare case) needs its subr. Today's entries find it through a `static OnceLock<usize>` and a name comparison (`rt:3196-3217`). New ones should not add statics: the generator emits `native_subr_NNNN(a0) { super::runtime::direct_native_x(NNNN, a0) }` and the runtime function is `#[inline(always)]`, so the wrapper is the entry and the index is a constant.
- **(c) and (d): the wrapper calls one `#[inline(always)]` helper** with its index and its arguments as an array, `call_fixed_subr::<N>(INDEX, [a0, ..])`. With `INDEX` and `N` constant the bounds test, the address computation and the arity comparison fold. The body pointer comes first from the subr's existing `full_arity_entry` word (S6: no new state, the same conditions as `invoke_subr`'s first branch), later from a compile-time constant (S8).
- **Static entries (S8).** `facts().ordinary_direct()` is a pure function of the name and of constant tables, so it can be a `const fn subr_entry(name) -> Option<DirectPrimitive>`: the hand list of `dispatch.rs:87-169` as a table, the file-name-handler operations (`R/src/lisp/primitives/system.rs:2447`, whose constructor is already `const`) as a table, and the modules' `LIFTED_PRIMITIVES` in module order. The wrapper holds `const ENTRY` and calls it directly, as GNU's freloc call reaches `Fname`; `NativeSubr::new` can take the same constant, and the lazy `direct` and `full_arity_entry` words go. This is the DEFUN-faithful end state and removes state instead of adding it.
- **Rejected: a table filled at startup.** It is a registry GNU does not have (rule 5 of `R/docs/round-audit.md`; R4 of the round audit), and the lazy words already are one.
- **Rejected: the lifted macro emitting typed entries.** `macro_rules` cannot make a function name from a string literal, the arity is not in the arm, and any new function carrying an arm's body would need the lint allowances the present `lifted` functions carry (`R/src/lisp/mod.rs:232, 251, 271, 290, 310, 329`), which may not be added. The plan does not touch those macro arms at all.

### 2.4 What the generator must learn

1. Selection functions for the new shapes, all placed between `fn direct_native_target` and `fn read_min_arg_constants`, because `R/src/anti_cheat.rs:782-792` scans exactly that span for primitive names. The `cons` and comparison selections in `main` (`tool:147-171`) should move there too, before S10 removes `DirectSubr::of_name`, which is what keeps those names in the inventory today.
2. Two emission forms beside the present ones: the indexed inline entry (2.3) and the `call_fixed_subr` / `call_many_subr` wrappers. An assertion of the expected arity for each selected name, as `tool:148` and `tool:163-166` have.
3. **A re-emit mode**, `generate_native_subrs --reemit EXISTING OUTPUT`: read the names, arities and the three ABI constants from an existing generated file and emit with the current rules. There is no Darwin oracle here, and earlier rounds edited the Darwin table by hand (a8830051 changed 15 lines in each file). S6 changes about 1,300 wrappers. Re-emitting the Linux file must reproduce it byte for byte after `rustfmt`, which ties the mode to CI's full regeneration (`R/.github/workflows/frozen-run.yml:142-151`).
4. Output stays deterministic and `rustfmt`-stable, since CI compares bytes.

### 2.5 Shape (a) from GNU's source

Subrs of the pinned table whose C body reads only its arguments and the objects they name (bodies read in `G/`; a script listed the DEFUNs that call only `lisp.h` predicates, and I read the multi-line ones by hand):

- **Tag and header only**: `null`, `consp`, `atom`, `listp`, `nlistp`, `bare-symbol-p`, `symbol-with-pos-p`, `stringp`, `vectorp`, `recordp`, `char-table-p`, `vector-or-char-table-p`, `bool-vector-p`, `arrayp`, `sequencep`, `bufferp`, `markerp`, `user-ptrp`, `subrp`, `closurep`, `module-function-p`, `integerp`, `integer-or-marker-p`, `numberp`, `number-or-marker-p`, `floatp`, `threadp`, `mutexp`, `condition-variable-p`, `hash-table-p`, `overlayp`, `windowp`, `processp`, `obarrayp`, `window-configuration-p`, `characterp`, `char-or-string-p`, `identity`, `car-safe`, `cdr-safe`, `remove-pos-from-symbol`, `max-char`.
- **One more word of the object**: `multibyte-string-p`, `native-comp-function-p`, `byte-code-function-p`, `interpreted-function-p`, `natnump`, `category-table-p`, `syntax-table-p`, `case-table-p`, `buffer-live-p`, `window-live-p`, `framep`, `frame-live-p`.
- **Word-level with a signalling path**: `car`, `cdr`, `bare-symbol`, `symbol-with-pos-pos`.
- **Word-level but reading the C variable `symbols_with_pos_enabled`** (through SYMBOLP, EQ or CHECK_SYMBOL): `eq`, `symbolp`, `keywordp`, `function-equal`, `fboundp`, `symbol-function`, `symbol-plist`, `symbol-name`, `special-variable-p`. Only a vectorlike argument can make the flag matter, so the entry answers every other tag from the word and reads the flag for that one, as `direct_native_symbolp` does (`rt:3238-3249`).

In Emaxx today:

- **Already entries (16)**: `null`, `consp`, `atom`, `listp`, `nlistp`, `bare-symbol-p`, `stringp`, `identity`, `car-safe`, `cdr-safe`, `car`, `cdr`, `symbolp`, `eq`, `fboundp`, `symbol-function`.
- **Arm is a function of the value alone (33, S1 and S2)**: `symbol-with-pos-p`, `vectorp`, `recordp`, `bufferp`, `markerp`, `subrp`, `closurep`, `byte-code-function-p`, `interpreted-function-p`, `floatp`, `integerp`, `numberp`, `natnump`, `integer-or-marker-p`, `number-or-marker-p`, `char-or-string-p`, `arrayp`, `sequencep`, `char-table-p`, `vector-or-char-table-p`, `bool-vector-p`, `multibyte-string-p`, `native-comp-function-p`, `characterp`, `hash-table-p`, `overlayp`, `windowp`, `framep`, `buffer-live-p`, `syntax-table-p`, `category-table-p`, `case-table-p`, `max-char`. (A script checked that these arms name neither `interp` nor `env`; `integerp` and `numberp` delegate to `direct_*` functions that ignore both. Each is to be re-read when it is moved.)
- **Word-level in fact, the arm passes an unused interpreter or needs a second path (S3)**: `bare-symbol`, `symbol-with-pos-pos`, `remove-pos-from-symbol`, `keywordp`.
- **Not word-level in Emaxx yet**: `user-ptrp`, `module-function-p`, `threadp`, `mutexp`, `condition-variable-p`, `processp`, `obarrayp`, `window-configuration-p`, `window-live-p`, `frame-live-p` resolve a host record through the interpreter. They are shape (b) now and become (a) when those objects are pseudovectors (the D3 line of work).
- **Left on the general entry**: `symbol-plist`, `symbol-name`, `special-variable-p`, `function-equal` (rare; S6 serves them).

### 2.6 Shape (b) from GNU's source

No argument, cannot signal: `point`, `point-min`, `point-max`, `gap-position`, `gap-size`, `following-char`, `preceding-char`, `bolp`, `eolp`, `bobp`, `eobp`, `current-buffer`, `mark-marker`, `current-case-table`, `standard-case-table`, `category-table`, `current-local-map`, `current-global-map`, `selected-window`, `selected-frame`, `minibuffer-depth`, `recursion-depth`, `byteorder`. With an argument and a common path that neither signals nor runs Lisp: `purecopy` (`purify-flag` nil returns the argument, `G/alloc.c:5981-5994`), `char-after` and `char-before` of nil or a fixnum, the host-record predicates of 2.5.

First set for S4, by frequency and because Emaxx already has infallible readers for them: `purecopy`, `point`, `point-min`, `point-max`, `following-char`, `preceding-char`, `bolp`, `eolp`, `bobp`, `eobp`, `current-buffer`, `selected-window`, `selected-frame`.

## 3. Stages

Each stage is behaviour-neutral, is validated by the usual recipe (plain build, clippy, gate, integration, round audit against its parent, one reviewer), and is measured with `tools/perf/cg-table.py` on the probes of S0. Line estimates are hand-written lines; generated lines are counted apart. Every stage that changes the generator regenerates both tables (Darwin through `--reemit`) and extends the anti-cheat inventory where it adds a name.

### 3.1 Expected counts (per iteration, plain release; loop 51; all est. until measured)

| Probe | Now | After | Expected | GNU |
|---|---|---|---|---|
| `native-subr` (`bufferp` and `<`) | 126.1 | S1 | 73 | 73.0 |
| `vectorp` | 109 | S1 | 62 | 58 |
| `symbol-with-pos-p` | 109 | S1 | 61 | 58 |
| `recordp` | 104 | S1 | 64 | 58 |
| `bare-symbol` | 110 | S3 | 61 | 60 |
| `keywordp` | 186 | S3 | 68 | 64 |
| `nth`, variable index (new) | measure in S0 (about 140) | S3 | 72 | measure |
| `point` | 110 | S4 | 69 | 57 |
| `+` | 170 | S5 | 84 | 151 |
| `max` | 164 | S5 | 84 | 185 |
| `symbol-name` (new, a small general body) | measure (about 105) | S6, then S8 | 88, then 70 to 85 | measure (about 58) |
| `delq` | 220 | S6, then S9 | 203, then 185 | 151 |
| `assoc` | 270 | S6, then S9 | 253, then 171 | 167 |
| a general MANY subr (new, for example `logand` or `concat`) | measure | S7 | 27 fewer | measure |
| guards: `cons` 93, `=` 82, `>=` 83, `nth` constant index 58 | | every stage | unchanged | 405, 140, 81, 57 |

### S0. Probes, the agreement harness, the re-emit mode (no runtime change)

- **Files**: `R/tools/perf/probes/` (the fifteen `ns-*.el` of the scratchpad's `d4/probes` under committed names, and three new: `nth` with a variable index, `symbol-name`, one general MANY subr); `rt` tests; `tool`; a test beside `abi`'s (`abi:535-572`).
- **What moves**: nothing at run time.
  1. `assert_string_type_predicates` (`rt:7149-7197`) already compares `primitives::call` with the link-table target for five predicates on one value. Generalize it to `assert_entry_agrees(name, args)` (equal words on `Ok`, equal condition data on `Err`) and add `sample_objects`: one live value of every `Kind` (`types.rs:1797-1846`) and the boundary cases inside a kind: nil, t, fixnums (0, -1, both extremes, `#x3FFFFF`, `#x400000`), bignums of both signs, a float and NaN, unibyte, multibyte and empty strings, an interned symbol, a keyword, an uninterned symbol named `:k`, a private-obarray symbol, a positioned symbol and a positioned keyword, a cons, a dotted pair, empty and non-empty vectors, a record, a bool-vector, a plain char-table and a syntax, category and case table, a hash table, a live and a killed buffer, a marker in a buffer and one in none, an overlay, a live and a deleted window, a frame, a terminal, a builtin subr and a special form, a byte-code and an interpreted closure, a finalizer, a process, thread, mutex and condition variable, an obarray, a window configuration, and a reader form if one can be built.
  2. `word_level_entries_agree_with_their_primitives`: for every name of a list in the test, every sample (every pair for two-argument subrs), with `symbols-with-pos-enabled` nil and then t. It also asserts that each predicate answers t for some sample and nil for some sample, and that the test's list equals the names the generator selects (read from `tool`'s text, as the anti-cheat does). In S0 the list is the 16 existing word entries.
  3. `--reemit` (2.4), and a text test that the two generated tables give each shared name the same target and wrapper shape and that each fixed wrapper has as many parameters as its `Fixed(n)`.
- **Risks**: none at run time. The re-emit must be a fixed point on the Linux file.
- **Expected**: all counts unchanged; baselines of the three new probes recorded. About 350 lines.

### S1. Word entries I: `data.c` type predicates

- **Names (17)**: `symbol-with-pos-p`, `vectorp`, `recordp`, `bufferp`, `markerp`, `subrp`, `closurep`, `byte-code-function-p`, `interpreted-function-p`, `floatp`, `integerp`, `numberp`, `natnump`, `integer-or-marker-p`, `number-or-marker-p`, `char-or-string-p`, `arrayp`.
- **Files**: `R/src/lisp/primitives/dispatch/predicates.rs` (each arm's expression becomes a `pub(crate) fn name(value: &Value) -> bool`, the arm calls it); `rt` (one `direct_native_*` per name, three lines each or one local `macro_rules`); `tool` (17 lines in `direct_native_target`); both generated tables (17 table lines each, 17 wrappers gone); `R/src/anti_cheat.rs` (17 `NativeCFastPathContract` entries, owner `data.c:Fname`, contract test the S0 test); the S0 test's list.
- **Risks**:
  - *Calling the body without the general path's checks.* Sound only if the body uses neither interpreter nor environment, cannot fail and cannot allocate. The compiler enforces the first (the shared function takes only `&Value`), its `bool` return the second; the third is by reading. Then no handler sync is needed: nothing in the body can observe the handler state or collect, and the next general call syncs.
  - *A different predicate.* Avoided by moving the arm's expression, not rewriting it; the agreement test covers every kind.
  - *The argument word.* The header of a vectorlike argument is read, as the arm does through `kind()`; native code passes live words. `kind()`'s unknown-tag arm is `unreachable_unchecked` in release (`types.rs:1784-1790`) on both paths alike.
  - *GC*: the body makes no call, so there is no collection point and nothing to root.
  - *Threads*: no state is read.
- **Tests**: the S0 test over the 17 names; the existing `native_string_type_predicates_*` tests keep passing through the new targets.
- **Expected**: see 3.1. About 400 lines.

### S2. Word entries II: the rest of the pure predicates

- **Names (16)**: `sequencep`, `char-table-p`, `vector-or-char-table-p`, `bool-vector-p`, `multibyte-string-p`, `native-comp-function-p`, `characterp` (two parameters, the second ignored, as `Fcharacterp`), `hash-table-p`, `overlayp`, `windowp`, `framep`, `buffer-live-p`, `syntax-table-p`, `category-table-p`, `case-table-p`, `max-char`.
- **Files**: the arms' modules (`collections.rs`, `strings.rs`, `buffer_meta.rs`, `misc_keymaps.rs`, `overlays.rs`, `display.rs`, `frames.rs`, `buffer_edit.rs`, `predicates.rs`), then as S1. Owners are in `data.c`, `fns.c`, `buffer.c`, `window.c`, `frame.c`, `syntax.c`, `category.c`, `casetab.c`, `character.c`.
- **Risks**: as S1. `buffer-live-p`, `framep` and the three table predicates read one more word of the object; confirm for each that Emaxx reads it from the object and not from interpreter state. Any that does not is left for S4.
- **Expected**: none of the listed probes moves (only `framep`, 0.2%, is in the profile's top 45). The stage completes the class and may be deferred. About 350 lines.

### S3. Word entries with a second path: `bare-symbol`, `symbol-with-pos-pos`, `remove-pos-from-symbol`, `keywordp`, `nth`

- **What moves**:
  - `remove-pos-from-symbol`: pure, as S1.
  - `bare-symbol`, `symbol-with-pos-pos`: the word path for a bare or positioned symbol; anything else passes to `invoke_other_subr_slow(subr, &[word])`, whose arm signals. Emitted as indexed inline entries (2.3), so no static and no name search.
  - `keywordp`, in two commits. First the body: `is_keyword` tests the private-obarray marker with the byte scan `is_visible_symbol_name` already uses (`types.rs:2577-2584`) in place of the `&str` substring search, and the arm tests the bare symbol before it reads the flag. Then the entry: any tag but vectorlike is answered from the word; a vectorlike one takes the active call, reads `symbols_with_positions_enabled` and tests the positioned symbol's symbol.
  - `nth` (13.7% of calls): `fns.c:Fnth` is `Fcar (Fnthcdr (n, list))`. The entry takes a fixnum N up to `Fnthcdr`'s small-count bound, steps N conses, and returns the car of a cons or nil for nil; a non-fixnum or larger N, a non-list tail and a non-list result pass to the general entry, which signals with GNU's datum.
- **Files**: `types.rs`, `predicates.rs`, `lists.rs`, `rt`, `tool` (a second selection function for indexed entries, inside the scanned span), tables, `anti_cheat.rs` (5 entries), tests.
- **Risks**:
  - *`symbols-with-pos-enabled`*: `bare-symbol`, `symbol-with-pos-pos` and `remove-pos-from-symbol` ignore it by GNU's definition; `keywordp` must not. The S0 test runs under both settings with a positioned keyword in the sample.
  - *A signal inside a word-level body*: there is none. The second path is the general entry, which syncs the handlers and converts the error. The word path must be exactly the non-signalling cases.
  - *`nth`'s error data*: `CHECK_LIST_END (tail, list)` reports the list and `Fcar` reports the tail. The entry constructs neither, so they cannot diverge.
  - *`is_keyword` has ten other callers' worth of semantics behind it*: the scan must find exactly what the substring search found (one marker character).
- **Tests**: the S0 test for the four symbol subrs; for `nth` a table of N in {-1, 0, 1, 2, 127, 128, a bignum, a float, nil} against nil, a proper list, a dotted list and a non-list; an `is_keyword` test over interned, uninterned, private-obarray and positioned names.
- **Expected**: see 3.1. About 450 lines.

### S4. Context entries

- **Names (13)**: `purecopy`, `point`, `point-min`, `point-max`, `following-char`, `preceding-char`, `bolp`, `eolp`, `bobp`, `eobp`, `current-buffer`, `selected-window`, `selected-frame`.
- **What moves**: each reader becomes a `pub(crate) fn(&Interpreter) -> Value` that the present arm or `direct_*` function wraps in `Ok` (`buffer_edit.rs:3838-3862, 3892-3941`, `display.rs:8432-8440`, `buffer_meta.rs:231`, `display.rs:7539`). The entry is `with_active(|active| reader(&*active.interpreter).word())`, as `direct_native_functionp` is (`rt:3262-3272`). `purecopy` is an indexed entry: `purify-flag` nil returns the word, anything else passes to the general entry (`R/src/lisp/primitives/purecopy.rs:274-288`).
- **Risks**:
  - *No handler sync*: right only if the reader cannot signal, allocate or run Lisp. A fixnum result does not allocate, but `Value::Integer` makes a bignum past the fixnum range: use a reader whose result is a fixnum by construction or keep the range test and pass the rare case on.
  - *Borrows*: the readers take `interp.buffer.borrow()`, as today's bodies do under the same active call. Take a shared reference to the interpreter, not `&mut`.
  - *Threads*: the active call is read on each call and nothing is kept.
- **Tests**: `context_entries_agree_with_their_primitives`: each entry against `primitives::call` at point-min, mid-buffer and point-max, in a narrowed, an empty and a multibyte buffer, after `set-buffer`, and with a stale handler (a native handler popped but not yet synced) to show the answer does not depend on the sync.
- **Expected**: `point` 69. About 350 lines.

### S5. The thirteen kinds still on general wrappers get entries of their own

- **What moves**: `+` and `-` (and `min`, `max`) through `invoke_fixnum_arithmetic(index, op, nargs, args)`, the twin of `invoke_numeric_comparison`: two fixnums whose result is a fixnum at once, else `invoke_subr_many`. `mapcar`, `maphash`, `apply`, `member`, `boundp`, `default-boundp`, `default-value`, `local-variable-p`, `set-default` as `direct_native_*` calling the functions `invoke_word_level_subr` and `invoke_variable_subr` call today (`invoke_native_mapcar` `rt:4314`, `native_memq` `rt:5142`, `native_variable_subr` `rt:2900`), in the same order, with the same sync. The generator's selections for these, for `cons` and for the comparisons move into the scanned span.
- **Risks**:
  - *Order relative to the sync*: today `+` on two fixnums syncs first (`entered_subr`) and the new entry would not. Nothing can observe the difference (no allocation, signal or Lisp between), and the comparisons and the plain-variable path already work this way (`rt:2835-2841`). A failed sync (an internal error) would be reported at the next general call instead; state this in the round's record.
  - *Each entry must keep its kind's present guards* (for example `member` only for a fixnum or bare-symbol element, `local-variable-p` only with a nil or absent buffer).
- **Tests**: the existing contract tests named in `anti_cheat.rs:393-666` already cover these subrs through the link table; add overflow to a bignum, a float operand, one and three arguments for the arithmetic entry.
- **Expected**: `+` and `max` 84; `mapcar`, `member`, `boundp` about 45 fewer per call. About 400 lines.

### S6. The fixed-arity entry in the wrapper

- **What moves**: `rt` gains `#[inline(always)] pub(crate) fn call_fixed_subr<const N: usize>(index: usize, arguments: [NativeWord; N]) -> NativeWord`: the subr at a constant index, its `full_arity_entry` word (a new accessor without the arity comparison, with a `debug_assert` that N is the subr's maximum), then exactly `invoke_direct_subr`'s body (`rt:2771-2793`); a zero word goes to `invoke_subr(index, &arguments)` and stale handlers to `invoke_other_subr_slow`, both out of line. The generator emits `super::runtime::call_fixed_subr(INDEX, [arg_0, ..])` for every general fixed wrapper.
- **Files**: `rt` (about 50), `abi` (about 12), `tool` (about 15), tests (about 100); generated: about 1,330 wrappers in each table.
- **Risks**:
  - *Neutrality*: the fast conditions are a subset of `invoke_subr`'s first branch, and every other case calls `invoke_subr` as today. The dropped tests are true by construction (index in range, N equal to the maximum), and the S0 text test checks the second on both tables.
  - *GC of words held only in registers*: unchanged. The array is in the wrapper's frame, inside the range the collector scans, and collections under an active call enter through the register-spilling trampoline (`rt:640-655`, `1027`, `5672`).
  - *Subrs with no Rust entry* (special shapes, file-name-handler operations) pay three instructions more than today before the same path. S8 removes that.
  - *The error path*: the helper returns `remember_active_helper_error(error)` and holds nothing droppable across it.
  - *Code size*: about 70 bytes more per wrapper, under 100 KB.
- **Tests**: `fixed_entries_answer_as_invoke_subr`: about thirty subrs across arities 0 to 8 through the link table against `primitives::call`, with a wrong-type error, a throw caught by a native catch, a call made after an inline handler pop, the first call of a subr (empty word) and a collection inside the body.
- **Expected**: 17 fewer on every general fixed call (`delq` 203, `assoc` 253, `symbol-name` about 88).

### S7. The MANY entry in the wrapper

- **What moves**: `call_many_subr(INDEX, MIN, nargs, args)`, inline: a count of at least MIN (which also rejects a negative one), a non-null vector when MIN is 0 and the count is not, the subr's resolved `direct` word, then as S6 with the caller's vector as the slice; every other case goes to `invoke_subr_many`.
- **Risks**: `invoke_subr_many`'s treatment of a negative count for `list` (`rt:5227-5235`) stays on the out-of-line path; `list` has its own entry. The vector is the native caller's, live and scanned.
- **Tests**: counts at the minimum, above it, zero with a null vector, and negative, through the link table against `primitives::call`.
- **Expected**: about 27 fewer on general MANY calls. About 150 lines; 58 wrappers in each table.

### S8. Static entries (gated by a prototype)

- **Gate**: build S6 with five hand-chosen wrappers given a constant entry and measure. Go on only if the probes show the direct call and inlining worth it (I expect 3 fewer where the body stays out of line and 15 to 25 fewer where it inlines).
- **What moves**: `const fn subr_entry(name)` in `dispatch.rs` (2.3); `direct_primitive` and `file_name_handler_operation` read tables; the wrapper holds `const ENTRY: Option<DirectPrimitive> = subr_entry("name")` and passes it to the helper, where a `None` folds to the general call.
- **Risks**:
  - *A different answer from the run-time facts*: the facts pick the first module whose `handles` matches, including guarded arms; the constant picks the first table that has the name. A test over all 1,455 subrs asserts the two agree, compared with `std::ptr::fn_addr_eq`.
  - *Inlining is the optimizer's choice*: the expected counts are not guaranteed. Measure plain and PGO.
  - *Compile time*: about 1,400 constant evaluations over about 1,500 table entries each. Measure; compare lengths before bytes.
  - *Arguments in registers once a body is inlined*: as S6's note; callee-saved registers are spilled before a scan and no caller-saved register holds a value across a call.
- **Expected**: `symbol-name` 70 to 85. About 300 lines.

### S9. `delq` and `assoc`

- `assoc`: an indexed entry. A nil TESTFN and a fixnum or bare-symbol key is `Fassq`: sync, then `native_assq` (`rt:4913`), as `direct_native_assq` does. Everything else takes the general entry. Expected 171.
- `delq`: trim `delete_from_list` (one flag read for the walk when the element is not vectorlike, `prev` as a word). Expected about 185. A word-level `Fdelq` would reach about 160 but needs a second implementation of `Fsetcdr`'s checks; I would not do it unless the profile asks.
- **Tests**: the S0 harness over keys of every kind and alists with non-cons elements, dotted and circular tails; `delq` on shared structure with identity of the retained cells checked.
- About 200 lines.

### S10. Retire the cascade and the lazy words (cleanup, no count change)

After S5 no wrapper reaches `invoke_kind_subr`. Slow paths call `invoke_other_subr_slow` with their subr; `DirectSubr`, `invoke_kind_subr`, `invoke_word_level_subr` and the `kind` byte go; with S8, `direct` and `full_arity_entry` go too. Mind the anchors and the assertion balance (4.1). Mostly deletions.

## 4. Traps and open questions

### 4.1 Audit and anti-cheat tooling the change touches

1. **The exact fast-path inventory** (`R/src/anti_cheat.rs:682-1036`, run in the gate and at the start of every compat-harness run). It collects every string literal that names an available GNU C primitive from three spans of `rt` and one of `tool`, and requires the set to equal `EXACT_NATIVE_C_FAST_PATHS` (`391-667`). So:
   - every name added to the generator's selection needs a contract entry: an `owner` of the form `file.c:Fname` that exists in `../emacs/src/file.c`, and a `contract_test` whose `fn` is in `rt` itself (the check greps `rt`'s text, so the tests cannot move to a submodule without changing the check);
   - the spans are found by text: `fn invoke_context_free_subr`, `pub(crate) fn invoke_subr`, `fn slow_unary_subr`, `fn native_subr_address`, `enum DirectFuncallTarget` in `rt`, and `fn direct_native_target`, `fn read_min_arg_constants` in `tool`. Do not rename them, do not add a function whose name begins `pub(crate) fn invoke_subr` above line 2674, and keep new helpers that contain no name literals in the band between `fn slow_unary_subr` and `fn native_subr_address`;
   - S10 removes `of_name`, the only place some names appear (`cons`, the comparisons, the thirteen of 1.5). They must already be in the generator's scanned span (S5) or the inventory and the scan disagree.
2. **The native-dispatch scan** (`anti_cheat.rs:360-381`) matches `call(<one argument>, "<name>"` in runtime files and requires the name to be a GNU C primitive. Helper names such as `call_fixed_subr` do not match; do not add a helper called plain `call` that takes a name.
3. **CI compares bytes**: `frozen-run.yml:142-151` regenerates the Linux table from the oracle build, formats it and `cmp`s it with the committed file. The committed table is the generator's output, never a hand edit. Nothing regenerates the Darwin table in CI; hence `--reemit` and the S0 text test. Adding a re-emit check to the workflow is an R6 item.
4. **Round audit, hard findings** (`R/tools/round_audit.py`):
   - H5: any added line with `#[allow(` or `#[expect(`, in any `.rs` under `src`, `tools` or `tests`. A moved or re-indented line counts as added. So leave the six `#[allow(...)]` lines of `dispatch_table!` and the one at `rt:1326` where they are, and write the new code to pass clippy as it is: no `F`-prefixed function names (`non_snake_case`), keep `NativeWord` in `extern "C"` signatures, pass arguments to Rust-ABI helpers as an array (`too_many_arguments` ignores `extern "C"` functions but not the helper), compare function pointers with `std::ptr::fn_addr_eq`.
   - H6: assertions and `#[test]` functions removed may not exceed those added, `debug_assert` included. S10 deletes `direct_subr_kinds_follow_the_abi_table_names` and others; pair it with the tests of the stage or do S10 together with S8.
   - H2 and H3: `rt` is runtime code for the audit, its test module included. No ERT test name and no `*-tests.el` file name in added lines, comments included.
5. **Round audit, review findings** to expect and explain in the round's record: R6 for every change to `anti_cheat.rs` or a workflow; R4 for any new `static`, `OnceLock` or thread-local (the plan adds none; the indexed entries exist to avoid them); R3 for the words "fallback", "stub", "special case", "workaround", "hack" in added runtime lines, comments included (the existing comments say "takes the subr's own path"); R2 for `let _ =` and `.ok();`; R5 for the word "oracle".
6. **Manifests**: the GNU primitive, arity and DEFSYM manifests are untouched. The subr table's order and arities do not change in any stage; only targets and wrapper bodies do.

### 4.2 Open questions

1. **Handler sync at the consumers.** Would save 5 on every general call. It needs an inventory of every reader of the interpreter's catch and condition-case registrations and of the handler roots. Worth a design of its own only if generic calls still matter after S8.
2. **`Result` in bodies that cannot fail.** A typed infallible entry would save the test and the `Ok`, but the arms cannot say so without a macro change that H5 blocks. Shapes (a) and (b) are the hand-picked answer; S8's inlining the mechanical one.
3. **How far S2 and S4 should go.** The lists of 2.5 and 2.6 are complete for GNU; the profile justifies only a handful. I would do S1, S3, S4's first set, S5 and S6, measure, and decide S2 and the rest of S4 then.
4. **Host-record predicates** (`threadp`, `processp`, `mutexp`, `obarrayp`, ...): word-level in GNU, interpreter lookups here. They follow the representation work, not D4.
5. **`point` stays at about 69 against 57** until the current buffer's fields are read as GNU reads them (D13, A3.6).
6. **Trampolines.** GNU replaces a freloc entry when a primitive is redefined (`G/comp.c:4591`). A direct entry bypasses the function cell exactly as GNU's direct call does; confirm that Emaxx's trampoline installation writes the same link-table slot the new entries occupy (it does for the 36 existing ones, by construction of `runtime_link_table`, `rt:5749-5771`).
7. **Darwin.** No stage is measured there. Thread-local access on macOS goes through a resolver call, so shapes (b) to (d) cost more than on Linux; (a) does not.
8. **Counts against time.** The probes count instructions. S6 trades one shared indirect call site for one per wrapper, which should predict better; check time on a quiet host for `nth` and `+`, the two that dominate.
9. **What I did not verify.** The estimates in 3.1 (no build was made); that each reader of S4 is free of allocation in every state; that the arms of S2 read the object and not the interpreter for the "one more word" predicates; the `nth` baseline; GNU's counts for the new probes.

## Critical files for implementation

- `/home/user/emaxx-draft/src/lisp/native_comp/runtime.rs`
- `/home/user/emaxx-draft/tools/generate_native_subrs.rs`
- `/home/user/emaxx-draft/src/lisp/native_comp/abi.rs`
- `/home/user/emaxx-draft/src/anti_cheat.rs`
- `/home/user/emaxx-draft/src/lisp/primitives/dispatch/predicates.rs`
