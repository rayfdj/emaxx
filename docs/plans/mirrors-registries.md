# Mirrors, registries and the image (A2.3, A3.3, A3.4, A3.6, A3.8, A4.1, A4.2, A4.3, B19): inventory and staged plan

Written on 24 November 2026 before round 385, from a reading of the tree at
round 381 and callgrind runs on its release build. Paths beginning
`/home/user/` and `/tmp/` are those of the container it was written in; line
numbers are of that tree, and the expected counts are estimates. HANDOVER.md
records what each stage measured. Stages R0, R1 and R2 are round 385.

Read-only planning, 24 November 2026. Source read: `/home/user/emaxx-draft` at
`e26e60b6` (round 381); line numbers are that tree's. GNU source:
`/home/user/emacs-mirror/emacs/src` (30.2). Nothing was built and no cargo
command ran. Measurements use the prebuilt editors: GNU
`/home/user/emacs-mirror/emacs/src/emacs` and Emaxx
`/home/user/emaxx/target/prof-r381/release/emaxx` (round 381's release
build), under callgrind, one run at a time. Probes, outputs and the helper
are in the scratch directory
`the scratch directory mirrors/`
(`probes/`, `batch1.txt`, `batch2.txt`, `bin/cgd.sh`, `cg-*-startup.out`).
Every count marked "expected" is an estimate until a stage measures it.

## 0. Scope and the short version

**What the existing plans already cover (not re-planned here):**

| Plan | Covers (of this group's items) |
|---|---|
| `docs/plans/d3-windows.md` (done) | the window registry, window ids, `WindowRef`; windows are window.h's struct |
| `docs/plans/d14-symbols.md` (S0-S2 done) | the symbol host text (`SymbolText`, 16 bytes a cell), `AddressNames` (S8), the `mapatoms` name filter, the 48-byte cell and a dump without key texts (S7), comparisons with literals, `PropertyKey for &str` |
| `docs/plans/d13-buffers.md` (S0-S2 done) | `inactive_buffers`, `buffer_list`, `ALL_BUFFERS`, buffer ids, `buffer_locals`/`LoadedBindings` by id, the case/syntax/category tables by id, `RefCell<Buffer>`, buffer text, killed-buffer state, the buffer census and B19's buffer counters (S10), per-buffer variables (S11), the two `BUFFER_SLOT` variables |
| `/home/user/emaxx-work/m2/thread-plan.md` | `LIVE_STATES` and `registered_gc_view`, `HOST_SYMBOLS`/`ProcessTable`, `NATIVE_EXECUTION_LOCK` (A2.3's "synchronization layer"), thread-local roots, the allocator frontier statics, the image template |
| `docs/plans/d4-native-subrs.md`, `d9-bytecode-frames.md`, `m2/d17-plan.md` | the call paths; the byte-code verifier and its entry caches (`CACHE`, `VERIFIED`, `CLOSURES`, `CODE_CHANGES`, `VECTOR_SWEEPS`), kept by d17 as a named deviation (Rust soundness of unverified byte code) |

**What is left, and what this plan stages (R0-R18):**

1. **Forwarded C variables kept twice.** Ten variables the evaluator reads
   (`quit-flag`, `inhibit-quit`, `load-path`, `max-lisp-eval-depth` ...) live
   in the symbol cell *and* in an evaluator field kept in step on every
   store; the fourteen KBOARD variables live in the cell and in a hash map on
   the terminal. GNU keeps one copy: the C variable, which the symbol
   forwards to. `(setq inhibit-quit nil)` costs 932.6 instructions a loop
   iteration against GNU's 248.0 (`(setq print-length nil)`: 120.6);
   `(setq last-command nil)` 1,003.6 against 257.0. (R1, R2.)
2. **Host pseudovector records.** Thirteen object kinds (process, thread,
   mutex, condition variable, private obarray, window configuration, font,
   module function, user pointer, three tree-sitter kinds, SQLite) are a
   generic `RecordState { id, owner, type_tag, slots: Vec }` found by id
   through a registry, with eleven side tables keyed by that id, a sweep
   purge pass and a census by formula. GNU has one struct per kind. Lisp
   sees it in `memory-use-counts`: `make-mutex` conses 6 vector cells (GNU
   9), `make-thread` 6 (GNU 64), `obarray-make` 6 (GNU 2),
   `current-window-configuration` 60 (GNU 121). A lock and unlock of a mutex
   costs 1,726.6 with fifty other mutexes alive (GNU 709.0): the side tables
   are scanned linearly. (R6-R10.)
3. **`nil` and `t` are two things each**: the ABI words 0 and 48, and an
   allocated cell elsewhere that a per-editor table maps them to.
   `(get nil 'foo)` costs 521.6 against GNU's 388.0, where `get` of any other
   symbol costs 220.6 against 338.0. (R13.)
4. **Identity caches, dead machinery and replacement Lisp**: a snapshot of
   the obarray for `mapatoms` (Lisp-visible: GNU's `mapatoms` sees a symbol
   its callback interns, Emaxx's does not), a copy of each case-folding
   table's ASCII part revalidated by a global char-table store count, a
   charset attribute cache with its own epoch, a Rust registry of ERT tests
   fed by intercepting every `put` (`put` 564.6 against 399.0), a native
   timer model only tests feed, reader placeholder objects, a dead function
   binding generation, a dead cons-mutation snapshot. (R3, R4, R5, R11, R12.)
5. **The charset and coding-system tables** are host vectors and maps where
   GNU keeps Lisp hash tables of attribute vectors, plus synthetic "books"
   that charge `hash_table_allocated_bytes` for the Lisp tables that do not
   exist. `encode-char` 733.6 against 493.0, `char-charset` 634.6 against
   535.0, `coding-system-get` 806.6 against 708.0. (R11, R12.)
6. **Accounting.** The cons block's 8-byte weak-reference serial (A3.3) is
   read by no runtime code, only by tests and assertion messages, and costs
   a third of each cons block (1,344 conses a block against GNU's 2,030).
   `garbage-collect` reports every free count as 0 (hard-coded); live bytes
   use GNU's symbol and string sizes while the cells are 64 and 48 bytes;
   `intervals-consed` counts make_interval call sites of a structure that
   allocates no intervals. (R14, R15, R18.)
7. **The image** (A4.1). Today 14,176 of 18,001 symbols, every cons, string
   and vector are mapped in place, so the milestone's note is partly
   stale. What is still rebuilt at each load: 3,825 symbol records with
   their fields installed one by one (all forwarded, built-in, uninterned
   or private symbols), 5,901 native subrs, 485 hash tables, 24 host
   records, 134 floats, the buffers, markers and overlays, and 52 root
   groups converted from Lisp lists back into host tables. Marks of mapped
   objects are written into the mapped pages (GNU: a separate dump bitset),
   and a first collection dirties 2.3 MB of them (private dirty 19,080 →
   21,360 kB; GNU 17,744 → 17,744). `Loader::load` is 63.6M instructions; GNU's
   whole non-native pdumper_load is about 29M. (R16, R17.)

**Headline counts** (uncovered by any plan; §1 lists each with file:line):
22 registries and id- or name-keyed side tables, 6 mirrors, 5 identity
caches (one a necessary deviation), 2 agreement checks, 9 conversion
layers and mutation notifications, 3 replacement-Lisp mechanisms, and 12
groups of comments judged as migration adapters (7 removed by this plan,
2 shared with d13/d14, 1 left to d13, 2 not adapters). Covered elsewhere:
11 registries, 2 mirrors, 4 identity caches.

## 1. Inventory

### 1.1 Method

`grep` of runtime code (`src/` less binaries, `tests.rs`, `tests/`, the
compat runtime tests) for maps and sets in struct fields and statics (103
declarations, scratch `maps.txt`), for `_id`/`record_id`/`find_*` lookups,
for "cache", "epoch", "generation", "mirror", "shadow", "registry",
"adapter", "bridge", "legacy", "compat", "until", "for now", and a read of
`InterpreterState` (eval.rs:6686-7263), the allocator, the loader and the
census. Costs are callgrind instructions a loop iteration of a
byte-compiled loop at 100,000 and 300,000 iterations (the difference over
200,000), `cg-table.py`'s method; the empty loop costs 116.0 in GNU and
71.6 in Emaxx (`base.el`). Lisp-visible differences were probed in both
editors with the same file.

### 1.2 Registries and side tables keyed by an object's id, address or name

| # | Where | What it keys | GNU keeps instead | Lisp-visible? (probe) | Cost on an ordinary path | Plan |
|---|---|---|---|---|---|---|
| G1 | `RecordRegistry` eval/record_registry.rs:21-104 (BTreeMap id → record, 32-slot recent cache); `records` eval.rs:6983; `next_record_id` 7027; `record_owner` 6985, `next_record_owner` eval.rs:1389 | the 13 host record kinds (`RecordKind` eval.rs:1275) by a u64 id | nothing: a Lisp_Object is the struct's pointer (`XPROCESS (p)->name`) | indirectly, through G4-G11 | `find_record` 42 sites, `record_value` 68, `record_ref` 14 | none |
| G2 | `RecordIdsByType` eval.rs:4027, field 6989 | record ids by type name (obarray census) | nothing | no | small | none |
| G3 | `FREED_RECORDS` alloc/vectors.rs (thread-plan P4), `purge_freed_records_in_live_states` eval.rs:1425 | ids the sweep freed, purged from G1 and the side tables after every collection | nothing (a dead pseudovector is just freed) | no | every collection: a pass over each live state's registry and tables | none |
| G4 | `process_states: Vec<ProcessState>` eval.rs:7235 (`ProcessState.record_id` 3570), `find_process_state` threads.rs:441 (151 uses) | process fields by record id, linear | struct Lisp_Process, 360 bytes (gdb) | counts: GNU's process object is 45 words | `process-name`: 392.6 (GNU 371.0); with 21 processes alive, of the last made: **525.6** (GNU 371.0) | none |
| G5 | `thread_states` eval.rs:7219 (`ThreadState` eval.rs:2043), `find_thread_state` threads.rs:414 | thread fields by record id, linear; also scanned for every thread record the mark phase traces (eval.rs:5044) | struct thread_state, 520 bytes | `make-thread` conses 6 vector cells (GNU 64; counts2.el) | `thread-name` 381.6 (GNU 366.0) | none |
| G6 | `mutex_states` 7224 (`MutexState` 2061), `find_mutex_state_mut` threads.rs:426 | by record id, linear | struct Lisp_Mutex, 80 bytes | `make-mutex` 6 cells (GNU 9) | lock + unlock 726.6 (GNU 709.0); **1,726.6 with 50 other mutexes** (GNU 709.0) | none |
| G7 | `condition_variables` 7225 (`ConditionVariableState` 2069, holds `mutex_id: u64`) | by record id | struct Lisp_CondVar, 72 bytes | `make-condition-variable` 12 cells (GNU 17 with its mutex) | rare | none |
| G8 | `sqlite_handles: Vec<(u64, SqliteHandleState)>` 7017 | by record id | struct Lisp_Sqlite | census counts 0 for the kind (`gnu_vector_slots`, eval.rs:1314-1318) | rare | none |
| G9 | `treesit_queries`, `treesit_parsers`, `treesit_nodes` 7019-7023 | by record id | struct Lisp_TS_Parser/Node/Query | census 0 | rare | none |
| G10 | `ModuleState.functions: HashMap<u64, ModuleFunction>`, `.pointers: HashMap<u64, UserPointer>` modules/mod.rs:41-42 | module functions and user pointers by record id | struct Lisp_Module_Function (72), Lisp_User_Ptr (24) | counts | each module call: a map lookup | none |
| G11 | `fd_thread_locks: HashMap<i32, u64>` eval.rs:6716 | descriptor → thread record id | `fd_callback_info[fd].thread`, a thread pointer | no | process I/O waits | none |
| G12 | `standard_obarray_id: u64` eval.rs:6826 and private obarrays as `RecordKind::Obarray` records | the obarray objects by id | struct Lisp_Obarray, 24 bytes | `obarray-make` 6 cells (GNU 2) | private intern 3,662 (GNU 2,421; d14's probe, part of it this lookup) | d14 covers the obarray id in the host text only |
| G13 | `TerminalState.keyboard: HashMap<SymbolName, Value>` eval/terminal.rs:31 | the KBOARD variables by symbol | struct kboard's KVAR fields (200 bytes) | no | see M2 | none |
| G14 | `builtin_doc_offsets: HashMap<String, i64>` eval.rs:7188 | DOC offsets by subr name text | `Lisp_Subr.doc` | no | `documentation` of a subr | none |
| G15-G17 | `charset_id_index` 6893, `charset_alias_index` 6895, `coding_system_hash` 6957 (+ `charset_ids`, `charset_aliases`, `charset_plists`, `charset_defs`, `charset_supplementary`, `charset_unified`, `iso_charsets` Vecs/sets 6889-6942) | charsets and coding systems by symbol | `Vcharset_hash_table` and `Vcoding_system_hash_table`, Lisp hash tables of attribute vectors; `charset_table`, a C array | `define-charset`/`define-coding-system` counts; probe pending (R11/R12 S0) | `char-charset` 634.6 (GNU 535.0), `encode-char` 733.6 (493.0), `coding-system-get` 806.6 (708.0), `coding-system-p` 413.6 (431.0) | none |
| G18-G19 | `CompositionIndex.by_hash` eval.rs:3888, `GstringCache.by_hash` 3903 | compositions and glyph-strings by `equal` hash | `composition_hash_table`, `gstring_hash_table`: Lisp hash tables | not measured | composition paths only | none |
| G20 | `IdentityHasher` types.rs:37 ("bridge indexing") | the hasher of G10-G19's maps | nothing | — | — | goes with them |
| G21 | `RecordIdsByType`'s sibling `record_kind_code` image codes pdumper/context.rs:156 | records by kind code in the image | GNU dumps each PVEC by its own dumper | — | — | goes with G1 |
| G22 | `Kind::Record` vs `Kind::LispRecord` (types.rs:1943-1948, decode 2240-2248, `record_has_inline_slots` generic_records.rs:119) | a PVEC_RECORD's header decides which | one PVEC_RECORD | no | every decode of a record: one more header test | none |

**Covered elsewhere:** buffers (d13: 9), `LIVE_STATES` and `HOST_SYMBOLS`
(thread plan: 2). **GNU's own, kept:** `BLOCKS` alloc.rs:129-148 (alloc.c's
mem tree), `HEAP_ROOTS` alloc.rs:735 (SAFE_ALLOCA_LISP regions),
`lface_id_to_name` (xfaces.c's), `frame_states` (Vframe_list), `terminals`
(terminal_list), `composition_states` (composition_table, an append-only C
array in GNU too), `fontset_table` (a Lisp vector), `STRING_CHAR_BYTE_CACHE`
(fns.c's), `SEARCHBUFS` (search.c's).

### 1.3 Mirrors and shadow copies of state GNU keeps once

| # | Where | Duplicates | GNU keeps | Lisp-visible? | Cost | Plan |
|---|---|---|---|---|---|---|
| M1 | `FORWARDED_EVAL_CELLS` eval/variables.rs:11-22: `quit_flag`, `inhibit_quit`, `throw_on_input`, `overriding_plist_environment`, `load_path`, `max_lisp_eval_depth`, `debug_on_next_call`, the symbols-with-pos flag, `undo_inhibit_record_point`, the orderly-shutdown flag (fields eval.rs:6701-6726, `load_path` 7211); kept in step by `update_forwarded_eval_cell` variables.rs:1589 (a scan of the ten keys), `refresh_forwarded_eval_cells` 1678, the store slow path `set_global_binding_resolved` 1837-1878 (`STORE_EFFECT` bit, alloc/symbols.rs:222-264) | the symbol's value cell, which holds the same value | the C variable (`Vquit_flag` ...); the symbol is SYMBOL_FORWARDED to it (Lisp_Objfwd/Intfwd/Boolfwd) | no (probed `max-lisp-eval-depth` after `makunbound` + `setq`: both `(excessive-lisp-nesting 11)`) | **setq inhibit-quit 932.6 (GNU 248.0)**, setq print-length 120.6; **let inhibit-quit 1,312.6 (GNU 630.0)**, let print-length 249.6 | none (d13 S4 *adds* `case-fold-search` and `parse-sexp-lookup-properties` to it: trap T9) |
| M2 | KBOARD variables: `KBOARD_FORWARD` alloc/symbols.rs:220 + G13's map; `set_terminal_keyboard_value`/`terminal_keyboard_value` eval/terminal.rs:1300-1330; `is_keyboard_variable` types/symbols.rs:1435 and terminal.rs:1272; the 14 names types/symbols.rs:1449 | the cell's value and the map's | struct kboard's field, the symbol SYMBOL_FORWARDED with a Lisp_Kboard_Objfwd | no | **setq last-command 1,003.6 (GNU 257.0)**; `last-command`, `real-last-command`, `prefix-arg` and their kin are set each command | none |
| M3 | `ert_tests: Vec<ErtTestDefinition>` eval.rs:7089, filled by `ert_set_test` eval/ert.rs:29-62, which `put` calls for every `ert--test` property (dispatch/buffer_edit.rs:1675-1684); dumped (dump_roots.rs:813, root `ErtTests`); read by `discovered_tests` for the embedder's report (batch.rs:205) | the `ert--test` plist entry (name, body, tags, expected result, file copied out) | the plist alone (ert.el owns the registry) | the plist is GNU's; the copy outlives `ert-make-test-unbound`, which is `cl-remprop` (ert.el:159-162) and never reaches `put` (retention, §1.9) | **put 564.6 (GNU 399.0)**: `bare_symbol_name` (dispatch/misc.rs:237) allocates a Rust String of every property name to compare it with "ert--test" | none |
| M4 | nil and t: the words 0 and 48 (types.rs:1973-1975) and the cells `Symbols.builtins` types/symbols.rs:970, reached through `builtin_boolean` (types/symbols.rs:1274; 59 runtime sites), `note_builtin_boolean` 1249, `BUILTIN_NIL`/`BUILTIN_T` state bits alloc/symbols.rs:207-208, `Value::Symbol`'s test types.rs:2015-2019, `mark_names`' special case types/symbols.rs:1466-1480 | one symbol, two identities | `lispsym[0]` and `lispsym[1]`: the words are offsets from lispsym, so word 0 *is* nil's cell | not by these reads (D128, in milestone M3, is the evaluator's handling of them) | **get nil 521.6 (GNU 388.0)** where get of another symbol is 220.6 (338.0); symbol-plist nil 384.6 (354.0); symbol-name nil 103.6 like any symbol | none (the progress map, docs/runtime-representation-progress.md:1750, names it a migration adapter) |
| M5 | GC trigger state per native runtime: `NativeGcState` native_comp/runtime.rs:6045 inside each `NativeRuntime.heap` (one per editor's `NativeCompilerState`, native_comp/state.rs:15, and the loader's registered runtime), with `ACTIVE_GC`/`ACTIVE_GC_TRIGGER` TLS copies runtime.rs:712-716, the process total `LISP_ALLOCATED_BYTES` 950 | consing_until_gc and gc_threshold, selected by context (`with_current_runtime` 969, the interpreter fallback 1050) | one `consing_until_gc`, one `gc_threshold` (alloc.c) | no: under equal settings 2M conses collect 40 times in both, 200k strings 16 times in both (lisp-visible.el) | maybe_gc: a TLS load and compare (cheap) | thread plan touches `ACTIVE_NATIVE_HEAP` only |
| M6 | `HostTableBooks` alloc/vectors/hash_tables.rs:583-620 in `charset_table_books` eval.rs:6897 and `coding_table_books` 6960 | the size, count and `hash_table_allocated_bytes` share of two Lisp hash tables that do not exist | the tables themselves | the allocated-bytes total | charset/alias definitions | none |

**Covered elsewhere:** the buffer's liveness and base copies (d13, done),
the symbol host text beside the Lisp name (d14).

### 1.4 Identity caches

| # | Where | Keyed by | GNU | Lisp-visible? | Cost | Plan |
|---|---|---|---|---|---|---|
| I1 | `known_symbols_cache` eval.rs:6815, `obarray_epoch` 6817, `known_symbols_shared` eval/bindings.rs:1043-1070; users completion.rs:99, 732, dispatch/misc.rs:1661 (`mapatoms`) | (symbol count, removal epoch) | map_obarray walks the buckets live (lread.c) | **yes**: a callback that interns 200 symbols during `mapatoms` sees them in GNU (`seen-new=t`), not in Emaxx (`nil`) (lisp-visible.el) | a Vec of every symbol rebuilt after any removal | d14 covers only the name filter inside it |
| I2 | `CANON_CACHE` primitives/search.rs:96, `canon_for` 329-357 (an Rc copy of a canonicalization table's ASCII part) | table address and the global char-table store count | RE_TRANSLATE reads the char-table (CHAR_TABLE_REF_ASCII for ASCII) | no | `canon_for` 36 a case-folding `looking-at` (d13 §3's measurement) | d13 names it "a char-table item", unplanned |
| I3 | `charset_table: RefCell<HashMap<SymbolName, Rc<CharsetInfo>>>` eval.rs:6934 with `charset_epoch` 6933 | charset name, emptied on any charset change | `charset_table[id]`, filled at definition (CHARSET_FROM_ID) | no | part of G15's costs | none |
| I4 | `handler_search_memo` eval.rs:7262 (`HandlerSearch` 7275) | error box and condition addresses, handler generation, epoch | signal_or_quit searches once and longjmps | no | a signal through 20 frames 11,322.4 (GNU 7,248.3) | none: **necessary deviation** (§2.6) |
| I5 | `RecordRegistry.recent` record_registry.rs:21-24 | id modulo 32 | none | no | every `find_record` | goes with G1 |

**Covered elsewhere:** `AddressNames` (d14 S8); the verifier's `CACHE`,
`VERIFIED`, `CLOSURES` (d17, kept with its constraint). **GNU's own:**
`STRING_CHAR_BYTE_CACHE`, `SEARCHBUFS` (whose entries now carry the
syntax table's identity as GNU's, search.rs:22-35).

### 1.5 Agreement and consistency checks

| # | Where | Checks | GNU | Plan |
|---|---|---|---|---|
| A1 | `ConsMutationSnapshot` types.rs:72-175 ("temporary derived-cache validation for keymap and syntax views") | words of weakly held conses still equal a snapshot | none | none. **Dead in runtime code**: its only users are tests (types.rs:5005-5088, native_comp/runtime.rs tests); the module-wide `#![allow(dead_code)]` hides it |
| A2 | `ImageCells` alloc.rs:247-330 (`note_marked` on every first mark of an image cell, `unchanged`, `set_live`) | the image cells marked this epoch equal those the last reading sweep left, to skip reading image blocks | sweeps never visit the dump; marks are in a dump bitset | none |

The checked build's heap verifier (`VERIFY` alloc.rs:2114) is a debugging
aid outside ordinary paths and stays.

### 1.6 Conversion layers and mutation notifications

| # | Where | Converts or notifies | GNU | Lisp-visible? | Cost | Plan |
|---|---|---|---|---|---|---|
| C1 | nil/t word ↔ cell (M4) | 59 sites | none | no | M4 | none |
| C2 | `ReaderForm` placeholders: `Kind::ReaderForm` (42 sites, 19 files), `literal_object` reader.rs:956, `materialize_read_object_literals` eval/macros.rs:181 (22 sites), `intern_symbols_in_value` eval/variables.rs:1115 | a reader without a context builds placeholder objects that a second pass makes into the real ones and interns | read0 builds and interns as it reads | no: every production reader has a context (print.rs:1049, 1330, loading.rs:1113, mod.rs:918); only the embedder's `--eval` (batch.rs:216-237) and tests read without one | none on ordinary paths | none |
| C3 | `Kind::Record`/`Kind::LispRecord` (G22) | — | — | — | — | — |
| C4 | `CHAR_TABLE_STORES` alloc/vectors/char_tables.rs:11-32, raised at char_tables.rs:86, 168, 229, 698 and vectors.rs:932 | every char-table store, for I2 and `translate_stamp` (search.rs:614-620) | `clear_regexp_cache` on `modify-syntax-entry` only (syntax.c:1278) | no | 3 instructions a char-table store | none |
| C5 | `charset_epoch` (I3) | any charset change | none | no | — | none |
| C6 | `obarray_epoch`, `note_obarray_removal` bindings.rs:1076 (I1) | any removal | none | via I1 | — | none |
| C7 | the `put` interception (M3) | a property store notifies the ERT mirror | none | no | M3 | none |
| C8 | `function_binding_generation` eval.rs:7070, bumped at bindings.rs:1106 | **written, never read**: what it invalidated is gone | none | no | one add a function-cell store | none |
| C9 | the 52 root groups: host tables written as Lisp lists (eval/dump_roots.rs) and rebuilt by `install_root_group` at every load (2.19M instructions at startup) | charsets, coding systems, compositions, fontsets, ERT tests, the native timers, the detached variables, the buffer alist ... | staticpro'd Lisp objects are mapped; C arrays like charset_table are dumped as data (pdumper.c dump_charset_table) | no | each startup | partly d13 (buffer alist) |

`string_text()` (184 runtime sites, 44 files) copies a Lisp string into a
Rust `String`, and the raw-byte sentinel adapters (reader.rs:1712,
coding.rs:313, print/output.rs:338) convert raw bytes to U+E080..E0FF.
These are transient copies, not a second mutable payload; buffer text is
d13's (S5); the hot string cases are d13 §3's "strings side of A3.6"
(`StringText::new` 64 in `string-match`). Not staged here; listed for A3.6.

**Covered elsewhere:** `CODE_CHANGES` and `VECTOR_SWEEPS` (d17), the
buffers' edit serials (d13).

### 1.7 Replacement Lisp

| # | Where | Replaces | Reached by | Plan |
|---|---|---|---|---|
| L1 | the ERT registry (M3) and the Rust runner `run_ert_tests_with_selector` eval/ert.rs:109 (cfg(test)) | ert.el's test registry | production: the embedder report; tests | none |
| L2 | native timers: `pending_timers: Vec<ScheduledTimer>` eval.rs:7239 (`ScheduledTimer` 3855), `run_pending_native_timers` threads.rs:4802, dumped as root `TimerList` (dump_roots.rs:842, 1373) | timer.el's `timer-list` | **only tests**: the one producer, `schedule_timer_after` threads.rs:3751, is cfg(test); production timers are timer.el's | none |
| L3 | the embedder's batch actions `run_batch_actions` batch.rs:180-250 (`--eval` read without context, an intern walk, materialize) | startup.el's command-line-1 for `-l`, `--eval`, `-f` | the library entry `run_batch`/`run_batch_with_actions` (tests, the compat helper); the `emaxx` binary uses startup.el (`run_batch_through_normal_top_level`, batch.rs:162) | none |

### 1.8 Comments that say adapter, bridge, legacy, compat, until, for now

Judged one by one (`until` has 400 hits; a sample of 60 are descriptions
of GNU's own timing, "until the next collection" and the like, and are
not adapters):

| Comment | Judgement | Removed by |
|---|---|---|
| types/symbols.rs:1433 "the existing terminal forwarding adapter ... until keyboard bindings use an allocated Lisp_Fwd redirect" | adapter (M2) | R2 |
| alloc/symbols.rs:15-17, 199-201 ("remaining host name and localized/forwarded adapters"; `C_SLOT` "a migration adapter") | adapter: host name (d14 S7), forwarded (R1, R2), localized per-buffer (d13 S11) | R1/R2, d14, d13 |
| eval/symbol_cells.rs:7, 456, 471-473, 751 (localized value and enumeration adapters) | adapter | d13 S11, R1 |
| eval/bindings.rs:400 "the remaining native slot adapter" | adapter (C slots) | R1 |
| alloc/vectors/generic_records.rs:119-121, eval.rs:5036 "the remaining host pseudovector adapter" | adapter (G1) | R6-R10 |
| pdumper/context.rs:3030 "the remaining dedicated-slot adapter" | adapter | R1, R17 |
| pdumper/image.rs:272 "legacy mutable-string wire type" | image-format leftover | the first stage that changes the format (R4a) |
| native_comp/runtime.rs:17781 "the remaining host adapter" (a test's overlapping record ids) | adapter (G1) | R10 |
| eval.rs:7240 "source-loaded callbacks stand in for GNU's byte-compiled Lisp": `deferred_defsubst_unbindings` 7244, `defer_unloaded_defsubst` bindings.rs:1326-1362 (macros.rs:429; begun at interactive.rs:2033) | adapter with a Lisp-visible effect: while a timer runs, `defalias NAME nil` of a defsubst during `unload-feature` is deferred until the timer returns | R5 |
| reader.rs:1712, coding.rs:313, print/output.rs:338 (raw-byte sentinels, host-text consumers) | text-representation adapters | d13 S5 (buffers); strings unplanned (§1.6) |
| completion.rs:80-163 "legacy vector obarray", bytecode.rs:602 `ArgSpec::Legacy`, system.rs:496 `legacy_unsigned_id` | **not adapters**: GNU's own legacy forms | — |
| tty.rs:10 "deliberately minimal for now", processes.rs:3233, eval/ert.rs:179 | not runtime-representation adapters | — |

### 1.9 The collector: what is traced twice, what is retained only because a registry or mirror holds it

No object is marked twice: every mark is the epoch word or bit
(`MarkBit` types.rs:213, the block bitmaps), so a second visit returns at
the test. What is *walked* twice or for nothing:

- `buffer_locals` is iterated twice in `mark_static_roots_into`
  (eval.rs:5689-5693 the symbols, 5718-5722 the values). The symbols are
  interned and already marked through the obarray. d13 S11 removes the map.
- Every thread record traced scans `thread_states` (eval.rs:5044:
  `find_thread_state`, linear): quadratic in the number of threads. R6.
- nil and t are marked through `mark_names`' special path
  (types/symbols.rs:1466-1480) for each symbol table (the host one and the
  active editor's). R13.
- After every sweep, `purge_freed_records_in_live_states` (eval.rs:1425)
  walks each live state's registry and side tables (G3). R10.
- An image cell first marked raises `ImageCells.marked` (A2); the sweep
  compares counts to decide whether to read image blocks. R16.

Retained only by a mirror or registry:

- **M3**: the body, tags and file of a test that `ert-make-test-unbound`
  (or a later `put` of nil) removed stay in `ert_tests`, rooted
  (eval.rs:5912) and dumped. GNU frees them with the plist entry. R5.
- **L2**: nothing in production (the list is always empty there), but the
  roots, the dump slot and the copy code exist for it. R4.
- The side tables G4-G7 hold Lisp values by id. Processes and live threads
  are GNU roots too (Vprocess_alist, all_threads), so no extra retention;
  mutexes' and condition variables' names are purged with their records
  (eval.rs:5274-5282). No retention beyond GNU's, but one more pass.
- `known_symbols_cache` (I1) holds symbol words it does not root; it is
  never used across a removal (the epoch), so it neither retains nor
  dangles, but it keeps a Vec of every symbol name alive between walks.
- `detached_forwarded_variables` (eval.rs:6765) roots the C side of a
  `makunbound` DEFVAR, which GNU's staticpro of the C variable also
  retains. Not extra.

### 1.10 The dumper and the loader

**GNU after load** (pdumper.c, pdumper.h): the file is mapped private
(`dump_mmap_contiguous`); `dump_do_all_dump_reloc_for_phase` adjusts each
pointer word in place (597,762 relocations, 21.9M instructions self, at
this host's startup); emacs relocations copy the dumped contents of the
builtin symbols into `lispsym` and set the staticpro'd C variables;
`thaw_hash_tables` (1.2M) rebuilds each hash table's index in place;
RELOC_NATIVE_SUBR and RELOC_NATIVE_COMP_UNIT fix native code
(dlopen/dlsym). A dumped object is recognised by `pdumper_object_p`, an
address-range test (pdumper.h:162). Its mark is a bit in
`dump_private.mark_bits`, one per DUMP_ALIGNMENT word of the hot region
(`pdumper_set_marked_impl`, pdumper.c:5274), swapped and cleared at each
collection (`pdumper_clear_marks_impl` 5287); cold objects (string data,
bool vectors) count as always marked (alloc.c:4262-4271). The sweeps
iterate the heap's own blocks only, so a dumped object is never swept,
freed or counted in `total_bytes_of_live_objects`; dead dumped objects
stay. GNU's own startup collects once (`garbage_collect` 52.6M, of which
marking dumped char-tables is most); `pdumper_marked_p_impl` and
`pdumper_set_marked_impl` are 8.3M of it.

**Emaxx after load** (`Loader::load` pdumper/load.rs:819-1366,
`install_image` eval/image_install.rs:47-115):

| Object | Today | Startup cost (callgrind, inclusive) |
|---|---|---|
| conses | mapped: the image's cons blocks are heap blocks where they lie (`adopt_cons_blocks` 1454), fields relocated in place (`relocate_conses` 1836) | 13.2M |
| strings | mapped string blocks and data (`adopt_string_blocks` 1495); `check_character_count` validates each (7,356 calls) | 1.4M validation |
| vectors, closures, Lisp records, char tables, bool vectors | mapped in vector blocks (`adopt_vectors` 1580, `relocate_vectors` 1636) | 10.0M |
| symbols | **14,176 mapped** (`adopt_symbol_blocks` 1697, filed into the obarray one by one: `file_image` 2.4M); **3,825 rebuilt** from records (`symbol_of_record` 2077: `intern_image`, `make_symbol`, `make_private`) because `mapped_symbol_state` (context.rs:134) and `image_state` (alloc/symbols.rs:707-723) exclude built-in, nil/t, C_SLOT, INT_FORWARD, KBOARD_FORWARD, STORE_EFFECT, BUFFER_SLOT, STORE_CONVERTS, trapped-write, uninterned and private symbols; each record's fields installed by `install_symbol` (image_install.rs:250) | 1.4M + 0.6M, and a SipHash set of 35,664 symbol hashes in `pdumper_load` 5.2M |
| native subrs | **5,901 rebuilt** (`NativeFunctionRef::blank`, `set_arity`/`set_fields`/`set_image_names`), their names read as C strings (`image_c_string` 2.8M) | 4.3M |
| native compilation units | 87 rebuilt (`NativeUnitRef::new`) and reopened by `load_dumped_code` (20.9M, the dlopen work GNU also does) | — |
| hash tables | **485 rebuilt**: `make_hash_table_value` then 9,809 `insert`s with `standard_hash_code` | 2.0M (GNU's thaw 1.2M) |
| host records | 24 rebuilt (`install_record` with `vec![Nil; n]` slots, then filled) | small |
| buffers, markers, overlays | rebuilt (`BufferRef::for_restore`, `load_buffer`, 4/12/4) | small; d13 |
| floats, bignums, the empty string, the zero vector | rebuilt in phase 1 (134 floats) | small |
| frames, terminals, windows | dead objects made anew (GNU nils them too) | — |
| root groups | 52 reinstalled from their Lisp values (`install_root_group`) | 2.2M |
| block registry | `register_image_blocks` inserts every mapped block into BTreeMaps | 2.0M |
| marks | in each object or block: cons and vector block bitmaps at the block's start, symbol and string cells' mark words, all inside the mapped pages | private dirty 19,080 → 21,360 kB at the first collection (GNU 17,744 → 17,744; rss.el) |

`Loader::load` totals 63.6M; with `pdumper_load`'s own 3.1M, the SipHash
set 5.2M and the install, the non-native load is about 73M against GNU's
about 29M (dump_do_dump_relocation's pointer work, pdumper_load's self
3.7M, thaw 1.2M). Emaxx's whole `-Q --batch` startup is still lower,
129.8M against 152.2M, because GNU collects once at startup.

### 1.11 Allocation accounting

**GNU's seven counters** (alloc.c), raised at: `make_interval`
(intervals_consed, alloc.c:1513), `allocate_string` (strings_consed,
1913), `allocate_string_data` (string_chars_consed += nbytes, 2597),
`make_float` (2769), `Fcons` (2894), `allocate_vectorlike`
(vector_cells_consed += len, the words after the header, 3642),
`Fmake_symbol` (3987) and `syms_of_alloc`'s `symbols_consed +=
ARRAYELTS (lispsym)` (8207); DEFVAR_INT, restored from the dump.

**Emaxx's** (alloc/counters.rs): the same seven words, raised at
`allocate_float` alloc.rs:600, `allocate_cons` 1807, `allocate_symbol`
alloc/symbols.rs:899, `allocate_string` alloc/strings.rs:457-458,
`allocate_vectorlike` alloc/vectors.rs:601, and `note_intervals`
counters.rs:54 at the points intervals.c would call make_interval
(intervals.rs:629, 752, 769, 1019, 2263, print.rs:498). The symbols forward
to the words (`forward_to_counter`); the loader stores the image's values
(pdumper/mod.rs:458-468).

**What the probes show** (counts.el, counts2.el; deltas of one call, GNU /
Emaxx, the order conses floats vector-cells symbols string-chars
intervals strings, the probe's own 7 conses included in both): equal for
cons, float, vector, record, strings (unibyte and multibyte), symbol,
propertized string, bool vector, hash table, marker, overlay, closure,
char table, finalizer, intern, format, keymap, puthash, make-byte-code,
match-data, vconcat. Different:

| Allocation | GNU | Emaxx | Cause | Plan |
|---|---|---|---|---|
| get-buffer-create | (9 0 128 0 12 0 2) | (7 0 45 0 12 0 2) | buffer layout | d13 S10 |
| insert into a temp buffer | (19 0 133 ...) | (8 0 50 ...) | buffer | d13 |
| bignum | 2 cells | 4 | `LispBignum(BigInt)`: num-bigint's Vec (3 words) and sign | §2.6, R15 |
| make-mutex | 9 | 6 | host record | R6 |
| make-condition-variable | 17 | 12 | host record | R6 |
| make-thread | 64 | 6 | host record | R6 |
| obarray-make | 2 | 6 | host record | R8 |
| current-window-configuration | (15 0 121 ...) | (11 0 60 ...) | host record and saved windows | R8 |

**Not counted, or counted by model:** `intervals-consed` counts make_interval
*events* of a gap sequence that allocates no interval objects
(counters.rs:15-20, intervals.rs:50-80): the count equals GNU's on the
probes but is not an allocation count (B19). Host storage behind Lisp
objects is not in any counter, as GNU's xmalloc'd storage is not:
`RecordState.slots` and the side-table states (G4-G11), `RefCell<Buffer>`
(d13), `FrameState`, `TerminalState` and the KBOARD map, the charset and
coding host tables, BigInt limbs (GNU's mpz limbs are not counted either).

**The census** (`garbage-collect`'s report, dispatch/misc_keymaps.rs:1653-1720):
every free count is the literal 0 (GNU: `(conses 16 1936 11311)` after a
collection that freed 100,000 conses; Emaxx `(conses 16 1761 0)`; free.el),
though the sweeps compute `FREE_CONSES` (alloc.rs:351) and the others.
Sizes printed are GNU's: symbols 48 (the cell is 64, alloc/symbols.rs:813),
strings 32 (the cell is 48: `SharedStringState` 32 plus `borrows` 8, the mark
word 4 and four flag bytes, alloc/strings.rs:28-39). Host record kinds are
counted by `gnu_vector_slots` (eval.rs:1292-1320, used at
alloc/vectors.rs:1439 and 1590): GNU's size, not the object's, and 0 for
fonts, tree-sitter kinds and SQLite. `total_bytes_of_live_objects`
(eval.rs:5098) uses GNU's 48 and 32; its doc comment (5094-5096) says
"symbol and string headers at this allocator's actual cell sizes", which
the code does not do.

**The cons block** (alloc.rs:60-72, 1346-1430): 32,760 usable bytes hold
1,344 conses: each 16-byte cons, an 8-byte serial slot (`serials`), and
three bits (`allocated`, `marked`, `serial_given`). GNU's block holds 2,030
(one mark bit a cons, CONS_BLOCK_SIZE alloc.c:2784). The serial exists for
`WeakConsRef` (alloc.rs:1671-1700), whose users are `ConsMutationSnapshot`
(A1, dead in runtime code), `allocated_serial` (alloc.rs:1968, cfg(test)),
the checked build's verifier messages (alloc.rs:2137-2149, 2225) and 39
test sites (eval_01.rs 6, types.rs 15, native_comp/runtime.rs tests 18). No
runtime path reads it. The `allocated` bit is read by `mem_find_cons`
(conservative scanning) and checked native-word decoding; GNU marks a free
cons by its car (`dead_object ()`), not by a bit.

**Collection counts under equal settings** match on the probes (40 and 16
collections). The locked suite's cons-allocation difference (GNU 0,
Emaxx 3 collections a sample) is HANDOVER's: GNU's heap keeps the previous
sample's two million conses through a stale stack word, so its threshold
(gc-cons-percentage 1.0) is past the sample; from equal heaps the two
collect alike.

## 2. Target design (for what no other plan covers)

### 2.1 Forwarded variables: one copy, GNU's redirect

A symbol whose value C owns is SYMBOL_FORWARDED with a forward descriptor,
as lisp.h's `union Lisp_Fwd`: `Objfwd` (a `Cell<Value>` field of the
interpreter state: Vquit_flag, Vinhibit_quit, Vthrow_on_input,
Voverriding_plist_environment, Vload_path), `Intfwd` (an i64 field:
max_lisp_eval_depth; alloc.c's counters already are), `Boolfwd` (a bool
field: debug_on_next_call, symbols_with_pos_enabled, undo_inhibit_record_point,
attempt_orderly_shutdown), `Kboard_Objfwd` (an offset into the current
kboard), `Buffer_Objfwd` (d13 S11). The symbol's payload holds the
descriptor (the payload union already holds `counter: &'static AtomicI64`
for Intfwd, alloc/symbols.rs:279-286). Reads are `do_symval_forwarding`,
stores `store_symval_forwarding` (with the store's own C side effect, such
as the call-depth gate Emaxx keeps, done by the store of that field and
nowhere else). C code reads the field. `set_buffer_internal_2`'s loop over
the new buffer's local bindings that have a forward (buffer.c) replaces
`refresh_forwarded_eval_cells_after_switch` for exactly those bindings.
`makunbound` makes the symbol plain and leaves the field (data.c
set_internal), so `detached_forwarded_variables` is needed only for the
DEFVARs that still have no field of their own.

The KBOARD's fourteen Lisp variables are a `Kboard` struct of
`Cell<Value>` fields in keyboard.h's order, owned by the terminal object
(GNU's `t->kboard`), and `current_kboard` is the selected frame's
terminal's.

**Deviation kept:** the descriptor points into the boxed `InterpreterState`
(its address is stable; eval.rs registers it by that address) rather than a
C global, because Emaxx's editor state is one object, not a set of
globals. A test editor's copy re-points its forwards (trap T6). Cost: a
read or store through the descriptor is a load of the payload and one of
the field, what GNU's `*XOBJFWD (valcontents)->objvar` does; today a
forwarded variable without the mirror already stores at 120.6 a loop
iteration (`setq print-length`) against GNU's 248.0, and R1 measures the
ten.

### 2.2 nil and t at lispsym

Builtin symbols live in a static array `LISPSYM` (globals.h's lispsym):
first nil, t and unbound, at offsets 0, 48 and 96 once d14 S7 makes the
cell 48 bytes. Every symbol's word is its cell's offset from `LISPSYM`
(lisp.h's `make_lisp_symbol`/`XSYMBOL`), so word 0 is nil's cell, as
GNU's. A heap symbol's word is its address minus `LISPSYM`'s, a link-time
constant: one subtraction to make a word and one addition to read a cell,
which GNU pays too. Then `builtin_boolean` is the constant cell,
`Value::Symbol` has no test, the image refers to nil and t by offset
(GNU's emacs relocation) and the `NilCells`/`TCells` root slots go.
Later (R17) every DEFSYM can be a `LISPSYM` entry, dumped by index.

**Native code is unaffected**: comp.c emits nil as the immediate word and
every other symbol through a relocation (comp.c:1790-1802, `emit_NILP`
1806), never XSYMBOL, so GNU's `.eln` files keep working; the loader writes
the new words into `d_reloc`. Emaxx's own backend (native_comp/backend.rs)
must emit the same.

### 2.3 Host records: one pseudovector per kind, its fields inline

As d3 did for windows: each kind becomes its own vectorlike tag with its
GNU struct's layout (sizes from gdb: thread_state 520, Lisp_Mutex 80,
Lisp_CondVar 72, Lisp_Process 360, Lisp_Obarray 24, save_window_data 112,
Lisp_Module_Function 72, Lisp_User_Ptr 24, struct font 216, and treesit.h's
and sqlite's), its Lisp words traced as mark_vectorlike traces them, the
side-table state as its fields, the holders keeping the object. The id,
`RecordState`, `RecordKind`, `RecordRegistry`, `FREED_RECORDS`, the purge,
`RecordIdsByType`, `gnu_vector_slots` and `Kind::Record` go.
`thread_states` becomes all_threads (the threads linked through their
objects). The census counts each object's actual words, which are then
GNU's.

**Deviations kept**, each owned by the object and freed by
`cleanup_vector` (alloc.c's `cleanup_vector` frees a font's driver data and
a module function's finalizer the same way): a process's
`std::process::Child`, pty and TLS handles; a thread's corosensei stack and
continuation; SQLite and tree-sitter library handles. Rust owns OS
resources through these types and cannot keep them as raw C fields without
re-implementing their drop. Each is one pointer word in the C part of the
struct, so the struct keeps GNU's size; where a Rust type is larger than
GNU's C fields, the excess goes behind that pointer, not into the object.
Cost: one indirection on the paths that use the OS handle (process I/O,
thread switches), none on the Lisp-field accessors the probes measure;
measured by R6b and R7b.

### 2.4 Charsets and coding systems as GNU keeps them

`Vcharset_hash_table` and `Vcoding_system_hash_table` are Lisp hash tables
(an alias is another key to the same attribute vector, as
`Fdefine_charset_alias` does); a charset's attributes are a Lisp vector in
CHARSET_ATTR_* order (its plist is CHARSET_ATTR_PLIST of it) and a coding
system's spec is GNU's `[attrs aliases eol-type]`; `charset_table` is a
Vec of plain `Charset` structs indexed by id (the C array), filled by
`define-charset-internal`; the ordered lists are Lisp lists with
`charset_ordered_list_tick`; names that GNU keeps as symbols (Vsjis_coding_system,
Vbig5_coding_system, coding categories' ids, a terminal's coding) are
symbols or ids, not Strings. `HostTableBooks` goes: the tables count
themselves. Nothing here needs a deviation.

### 2.5 The image: mapped objects as pdumper's

The mapped region is one address range (`ImageBytes`); a mark of an object
in it sets a bit in a side bitset of the region's words
(`pdumper_set_marked`), cleared at each collection; mapped blocks leave the
sweeps' lists (they stay in `BLOCKS.all` for `mem_find`) and are never
freed. Objects the loader still builds keep their image-region blocks
until R17 maps them: forwarded and built-in symbols (their forward
descriptors become emacs relocations, as GNU's `dump_field_emacs_ptr` of a
Lisp_Fwd), hash tables (mapped and thawed in place), native subrs (mapped
Lisp_Subr objects with RELOC_NATIVE_SUBR fixups only), floats.

**Deviation kept:** objects rebuilt because they own host storage
(buffers until d13 ends, host records until R6-R10 end) are swept in their
image-region blocks as now.

### 2.6 Accounting

- The cons block drops the serial: two bitmaps (mark and allocated), about 2,016
  conses a block. **Deviation kept:** the allocated bit (1 bit a cons),
  because Emaxx's free cell's first word is the free-list link where GNU's
  car is `dead_object ()`; adopting GNU's marker is a later option that
  reaches 2,030. Cost: 14 conses a block (0.7% more blocks), and no new work
  on an ordinary path (Fcons already sets the allocated bit).
- `garbage-collect` reports the sweeps' free counts.
- The census prints each kind's actual object size where Emaxx's layout is
  GNU's, and a separate, non-Lisp physical record (the perf harness's)
  states every remaining difference: the symbol cell's 16 bytes of host
  text until d14 S7; the string cell's 16 bytes (`borrows`, which Rust's
  aliasing rules need while a borrow of the bytes is live; the mark epoch
  word; four flags), bignum's 2 extra words (num-bigint's `Vec`), the
  allocated bitmap, image bytes.
- `intervals-consed` becomes an allocation count only when intervals are
  objects (R18, a decision).
- **Necessary deviation kept, I4:** the handler-search memo, because an
  error propagates as a Rust `Result` frame by frame (a longjmp over Rust
  frames with destructors is unsound), so the search GNU makes once is
  asked again at each frame; the memo makes the repeats O(1). Measured
  (`handler-deep.el`, a `signal` caught by a `condition-case` 20
  byte-compiled frames up): 11,322.4 instructions against GNU's 7,248.3,
  and 2,542.4 against 1,328.3 at depth 0, so about 440 a frame unwound
  against GNU's 296; the call's own excess is about 42 a level (lex-rec),
  and the rest is the frame-by-frame return of the error, of which the
  memo is the cheap part. Kept; the per-frame unwinding is d9/d17's
  call-path subject.

## 3. Stages

Ordered by value: what ordinary paths pay or Lisp sees first, then what
only retention, tracing or the image pays. Each stage builds and passes on
its own, is behaviour-neutral unless it names the change with GNU's
answer, and is under about 600 hand-written lines (mechanical conversions
grouped in their own commits). Every stage runs the round's usual gate
(the grouped gate, cli_parity, clippy `-D warnings`, rustfmt, the round
audit) and the call guards of `cg-table.py` (lex-rec, dyn-rec, mapc-bc,
interp-ignore, windows) with the stage's own probes.

**Common tests** each stage adds where it touches objects:
survival (the object reachable only through the changed structure
survives `(garbage-collect)` and `EMAXX_GC_STRESS`), reclamation (an
unreachable one is reclaimed, witnessed by a weak-key hash table entry
vanishing after `garbage-collect`, which GNU answers the same), and the
image (a dump with a variable holding it, then a load, answers as GNU's:
GNU refuses to dump a mutex, condition variable, user pointer, module
function, SQLite or tree-sitter object, window configuration
(pdumper.c:3111-3125: "unsupported object type"), nils processes and
terminals (3088-3090), and keeps only the main thread (3104)).

### R0. Guard tests and probes (tests only, about 450 lines)

- **Probes** into `tools/perf/probes/` (not built or tested): `put`,
  `getnil`, `setq-iq`, `let-iq`, `setq-lastcmd`, `mutex`, `mutex50`,
  `procname`, `procname20`, `charcharset`, `encodechar`, `codingget`,
  `handler-deep`, `handler-shallow` (from the scratch `probes/`), and the counts in the
  `cg-table.py` docstring.
- **Oracle tests** (GNU 30.2's answers recorded, as d14's S0):
  - T1 the ten evaluator variables and the fourteen KBOARD variables:
    `setq`, `let`, a buffer-local binding in another buffer and a switch,
    `kill-local-variable`, `makunbound` then `setq` then the C effect
    (nesting depth, a `load` search, `last-command` after a command), a
    variable watcher, a thread's binding, a dump and load.
  - T2 nil and t: `get`, `put`, `symbol-plist`, `symbol-function`,
    `fboundp`, `boundp`, `intern "nil"`, `unintern` of nil then a new nil
    symbol, printing, keymap keys, a dump of a plist on nil.
  - T3 the host kinds: `memory-use-counts` deltas of each constructor
    (GNU: mutex 9, condition variable 17, thread 64, obarray 2, window
    configuration 121, pipe process), `type-of`, printing, `eq` after a
    round trip through a hash table, the image refusals above.
  - T4 `mapatoms` and `all-completions` while the callback interns.
  - T5 `garbage-collect`'s shape: nine entries, each count non-negative,
    the free columns present.
  - T6 an ERT test defined, made unbound, collected: the weak table
    witness answers as GNU's.
- **Effect:** none. **Closes:** nothing.

### R1. The evaluator's ten C variables as forwarded fields (about 450 lines)

- **Files:** alloc/symbols.rs (payload: `Objfwd`/`Boolfwd` beside the
  existing `Intfwd`; state bits); eval/variables.rs (`FORWARDED_EVAL_CELLS`
  :11, `is_forwarded_eval_cell` :27, `update_forwarded_eval_cell` :1589,
  `forwarded_eval_cell_value` :1642, `refresh_forwarded_eval_cells*`
  :1678-1710, `set_global_binding_resolved` :1837, the
  `STORE_EFFECT_VARIABLES` list alloc/symbols.rs:251); eval/bindings.rs
  (specbind/unbind of a forwarded symbol); eval.rs (the fields keep their
  names; the call-depth gate refreshes from the stores of its three);
  dump_roots.rs and image_install.rs (the `QuitFlag`, `InhibitQuit`,
  `ThrowOnInput` root slots stay: they are GNU's staticpro'd C variables);
  pdumper context and load (the forward is a kind code, re-pointed at load).
- **Moves:** the value of each of the ten lives in its field only. A read
  of the symbol goes through the forward; a store writes the field. The
  `STORE_EFFECT` bit remains only for `text-conversion-style` (d13's).
- **Risks:** a binding in a buffer of one of these (GNU makes a BLV with
  the forward; Emaxx's `LocalCells` has its own copy): keep the
  swap-in-on-switch for those bindings only (T1 covers it); threads (a
  thread switch saves and restores specbind'd values: the forwarded store
  path must be the one used); the image template's copier (trap T6).
- **Tests:** T1; a variable watcher on `inhibit-quit`; survival of the
  values in the fields across a collection (`load-path` held only there).
- **Expected:** setq-iq 932.6 → about 125, let-iq 1,312.6 → about 260;
  guards ±3. **Lisp-visible:** none expected (T1 is the check).
- **Closes:** M1 with its store notification (`update_forwarded_eval_cell`),
  the adapter at eval/bindings.rs:400 for these ten. Part of A2.3, A3.4, A3.8.

### R2. The KBOARD variables in a kboard (about 400 lines)

- **Files:** eval/terminal.rs (`TerminalState.keyboard` :31 → a `Kboard`
  of 14 `Cell<Value>`; `terminal_keyboard_value`/`set_terminal_keyboard_value`
  :1300-1330; `is_keyboard_variable` :1272), types/symbols.rs (:1433-1464),
  alloc/symbols.rs (`KBOARD_FORWARD` :220 becomes the `Kboard_Objfwd`
  redirect), eval/variables.rs (:827, :1556, :1793, :2610), the terminal's
  `visit_lisp_values` (types.rs:1170-1186).
- **Moves:** the values from the map to the fields; `current_kboard` is
  the selected frame's terminal's.
- **Risks:** a second terminal (tty frames) must keep its own kboard, as
  GNU's; a `let` of a KBOARD variable (GNU's SPECPDL_LET on a
  Kboard_Objfwd) must restore into the kboard current at unbind, as
  specbind does.
- **Tests:** T1's KBOARD part; two terminals with different
  `last-command` (the existing multi-frame terminal tests); a dump.
- **Expected:** setq-lastcmd 1,003.6 → about 130 (about 870 fewer for
  each store of a KBOARD variable through its symbol). **Lisp-visible:**
  none.
- **Closes:** M2, G13, the adapter at types/symbols.rs:1433. A3.8 (that
  adapter), A2.3, A3.4.

### R3. The canonicalization tables read in place; no char-table store count (about 150 lines)

- **Files:** primitives/search.rs (`Canon`, `canon_for` :329, `CANON_CACHE`
  :96, `translate_stamp` :614), the regex backend's users of the stamp
  (regex_emacs), alloc/vectors/char_tables.rs (`CHAR_TABLE_STORES` :19 and
  its five raise sites), alloc/vectors.rs:932.
- **Moves:** RE_TRANSLATE reads the canon table: ASCII through its
  `ascii` sub-char-table (chartab.c's char_table_ascii /
  CHAR_TABLE_REF_ASCII), the rest by `char_table_ref`. The test
  `native_char_table_stores_invalidate_warm_regexp_syntax_caches`
  (native_comp/runtime.rs:15579) keeps its assertion (a search after a
  table store sees the store), now true by construction.
- **Risks:** whatever else reads the stamp (grep at the stage); a table
  replaced by `set-case-table` mid-search (the matcher holds the table, as
  GNU's).
- **Expected:** a failing case-folding `looking-at` about −30 (d13's
  probe), every char-table `aset` −3. **Lisp-visible:** none.
- **Closes:** I2, C4. Part of A3.4.

### R4. Dead and test-only machinery out of the runtime (two commits)

- **R4a (about 350 lines):** `function_binding_generation` (eval.rs:7070,
  bindings.rs:1106, dump_roots.rs:395); `ConsMutationSnapshot`
  (types.rs:72-175) moved into the test module that uses it; the native
  timers (`pending_timers`, `ScheduledTimer`, `run_pending_native_timers`,
  the `TimerList` root slot, `schedule_timer_after`): their tests schedule
  through timer.el's `run-at-time`; the orphan doc comments in
  `InterpreterState` (eval.rs:6775 "Variable aliases keyed by alias
  name", 6818-6823 tombstones, 6849 "Per-frame old selected window", 6990
  "Record mark bits ...", 7041 "Next generated symbol ID", 7071
  "Immutable lambda code ..."), and the
  image's "legacy mutable-string wire type" (image.rs:272). The image
  format changes (one root slot fewer).
- **R4b (about 500 lines, mostly deletions):** every reader gets a
  context: the embedder's `--eval` (batch.rs:216-237) reads as
  `read-from-string` does and evaluates as startup.el's command-line-1;
  `intern_symbols_in_value` goes; then `ReaderForm`, `VectorTag::ReaderForm`,
  `Kind::ReaderForm` (42 sites), `materialize_read_object_literals`
  (22 sites) and the reader's placeholder path. Tests that used a
  context-free `Reader::new` use the interpreter's reader.
- **Risks:** R4a's timer tests must keep their assertions (H6); the
  anti-cheat thread visitor scan looks for the text `Kind::ReaderForm(form) =>`
  in eval.rs (anti_cheat.rs:1731) and must be re-anchored in R4b (trap T2).
- **Expected:** no count changes. **Lisp-visible:** none.
- **Closes:** A1, C2, C8, L2, L3's reader half. A3.8 (ReaderForm), A2.3.

### R5. `put` without the ERT mirror; the deferred defsubst adapter (about 300 lines)

- **Files:** dispatch/buffer_edit.rs:1669-1690 (`put` is plist_put of the
  bare symbol; no name String), eval/ert.rs (`ert_set_test` goes; the
  cfg(test) runner reads the plist), eval.rs (`ert_tests`), batch.rs:205 and
  src/compat.rs (`discovered_tests` walks the obarray for `ert--test`
  records when the report is made, in the order the report has now),
  dump_roots.rs (`ErtTests` root slot), eval/bindings.rs:1322-1362,
  eval/macros.rs:429, primitives/interactive.rs:2033.
- **Moves:** the test list comes from GNU's own state at report time.
  The defsubst deferral goes if its probe agrees with GNU: inside a timer
  callback, `unload-feature` of a feature with a defsubst, then
  `(fboundp 'that-defsubst)` in the same callback (GNU: nil). Find the
  upstream test that motivated it (`git log -S deferred_defsubst`) and run
  it; if it then fails, keep the adapter and record why.
- **Risks:** the report is test-selection and reporting infrastructure
  (round audit R6, the frozen run's inventory): the discovered list must
  stay identical on all 519 frozen files (run the inventory comparison).
- **Tests:** T6; the report of a file whose tests are redefined.
- **Expected:** put 564.6 → about 355. **Lisp-visible:** none for the
  ERT part; the defsubst probe's answer becomes GNU's (name it in the
  round's record).
- **Closes:** M3, C7, L1, the adapter at eval.rs:7240. A2.3, A3.8, A4.2.

### R6. Threads, mutexes and condition variables as thread.h's objects (two commits)

- **R6a (mechanical, about 600 lines):** `ThreadRef`, `MutexRef`,
  `CondVarRef` handles over the records (d3's round-369 pattern); the 28
  `find_thread_state`, 5 mutex and 3 condition-variable uses and the
  record-id parameters of threads.rs through them. No behaviour change.
- **R6b (about 550 lines):** tags for PVEC_THREAD, PVEC_MUTEX, PVEC_CONDVAR
  with thread.h's layouts (520, 80, 72 bytes, asserted by offsets from
  gdb); `ThreadState` (eval.rs:2043), `MutexState` (2061),
  `ConditionVariableState` (2069) become their fields; the continuation
  and stack behind one pointer; all_threads instead of `thread_states`;
  `fd_thread_locks` holds thread objects; the mark phase traces the
  object's Lisp words (eval.rs:5040-5050's lookup goes).
- **Risks:** finding 84 and the suspended native activations (the thread
  plan's stage 6 and stage 5 touch the same objects: land after them or
  coordinate); a thread's object outliving its OS-level stack; the
  anti-cheat gate parses `struct ThreadState {` and
  `if record.kind == RecordKind::Thread` (anti_cheat.rs:1721-1735): update
  it to the object's visitor in the same commit (trap T2).
- **Tests:** T3's thread part; `native_thread_continuations`; the upstream
  thread-tests selection; survival of a joined thread's result; a mutex
  and condition variable reclaimed when unreachable; a dump refused as
  GNU's.
- **Expected:** mutex 726.6 → about 700, mutex50 1,726.6 → about 700,
  thread-name 381.6 → about 365. **Lisp-visible:** vector-cells-consed of
  `make-mutex` 6 → 9, `make-condition-variable` 12 → 17, `make-thread`
  6 → 64 (GNU's, counts2.el), and the census's vector-slots of each.
- **Closes:** G5, G6, G7, G11. Part of A2.3, A3.6, A4.3, B19.

### R7. Processes as struct Lisp_Process (two commits)

- **R7a (mechanical, about 600 lines):** a `ProcessRef` handle; the 151
  `find_process_state` uses (threads.rs) and processes.rs through it.
- **R7b (about 550 lines):** PVEC_PROCESS with process.h's Lisp fields in
  GNU's order and its C fields (360 bytes); `ProcessState` (eval.rs:3569)
  into the object; the OS handles behind one pointer; Vprocess_alist holds
  the objects; `ProcessState.buffer_id` becomes the buffer object (d13
  S7's holder list names it: whichever lands second converts it).
- **Risks:** the reaper thread (threads.rs:395-410) must not touch the
  object; sentinels during a sweep; `delete-process` while a filter runs.
- **Tests:** process tests of the frozen inventory's process files; a
  process reclaimed after deletion and loss of its last reference; a dump
  nils it as GNU's.
- **Expected:** procname 392.6 → about 375, procname20 525.6 → about 375,
  process-buffer 399.6 → about 375. **Lisp-visible:** a process's
  vector-cells-consed and census become GNU's (probe in R7b).
- **Closes:** G4. Part of A2.3, A3.6, A4.3.

### R8. Obarrays and window configurations (about 550 lines)

- PVEC_OBARRAY (struct Lisp_Obarray, 24 bytes) for private obarrays and
  for Vobarray, replacing `RecordKind::Obarray` and `standard_obarray_id`
  (eval.rs:6826); a private symbol's pending text (d14) names the
  obarray object's address until d14 S7 removes the text.
- PVEC_WINDOW_CONFIGURATION: struct save_window_data (112 bytes) and its
  saved-window vector, replacing the record and `WindowConfigurationSnapshot`.
- **Expected:** priv-intern (d14's probe, 3,662) lower by the record
  lookup; `obarray-make` 6 → 2 cells and `current-window-configuration`
  60 → 121 (GNU's). **Lisp-visible:** those counts.
- **Tests:** d14's private-obarray fixture; `set-window-configuration`
  after the windows it saved were deleted; reclamation of an obarray and
  of its symbols.
- **Closes:** G12, the obarray part of G2. Part of A3.6, A4.3, B19.

### R9. Fonts, module functions, user pointers, tree-sitter, SQLite (two commits, about 550 lines each)

- PVEC_FONT (font.h's three sizes), PVEC_MODULE_FUNCTION and PVEC_USER_PTR
  (lisp.h), the tree-sitter kinds (treesit.h), PVEC_SQLITE. `ModuleState.functions`
  and `.pointers` (modules/mod.rs:41-42) go; a user pointer's finalizer
  runs from `cleanup_vector` as alloc.c's does.
- **Risks:** module assertion mode's retired handles
  (modules/mod.rs:46-52) must still tell an expired handle from a new
  object; a module's finalizer that calls back into Emacs during a sweep
  (GNU forbids it; keep Emaxx's existing deferral).
- **Tests:** `emacs-module-tests` selections; a user pointer's finalizer
  run once after reclamation; SQLite and tree-sitter files of the frozen
  inventory.
- **Lisp-visible:** these kinds' counts and census (0 today for fonts,
  tree-sitter and SQLite).
- **Closes:** G8, G9, G10. Part of A3.6 (native handles), A4.3.

### R10. The record registry removed (about 450 lines, mostly deletions)

- `RecordRegistry` (record_registry.rs) and its test (balanced by R6-R9's
  added tests: H6), `RecordState`/`RecordKind` (eval.rs:1275-1460), the
  ids and owner, `RecordIdsByType`, `FREED_RECORDS`, `take_freed_records`,
  `purge_freed_records_in_live_states` (eval.rs:1425), `Kind::Record`,
  `record_has_inline_slots`, `gnu_vector_slots` (vectors.rs:1439, 1590),
  the image's record codes (context.rs:156-172, load.rs:986-1010), the
  overlapping-id test control (native_comp/runtime.rs:17781).
- **Expected:** a record's decode −2; each collection without the purge
  pass. **Lisp-visible:** none beyond R6-R9's.
- **Closes:** G1, G2, G3, G21, G22, I5, the host pseudovector adapter. A2.3
  (registries), A3.8, A4.2, A4.3's census by formula.

### R11. Charsets as charset.c keeps them (two commits, about 550 lines each)

- **R11a:** `charset_table` as a Vec of `Charset` structs indexed by id,
  filled by `define-charset-internal`; the `CharsetInfo` cache and
  `charset_epoch` (eval.rs:6933-6940) go; `charset_supplementary` and
  `charset_unified` become flags of the struct.
- **R11b:** Vcharset_hash_table a Lisp hash table of attribute vectors
  (aliases as keys), the plists and definitions in those vectors; the
  index maps, `charset_ids`, `charset_aliases`, `charset_plists`,
  `charset_defs` and `charset_table_books` go; the root groups
  `Charsets`, `CharsetAliases`, `CharsetList` ... become the staticpro'd
  objects they are in GNU.
- **R11 S0 probes:** `(eq (charset-plist 'ascii) (charset-plist 'ascii))`,
  `charset-plist` after `set-charset-plist`, vector-cells-consed of a
  `define-charset`.
- **Expected:** charcharset 634.6 → about 520, encodechar 733.6 → about
  490; startup's `install_root_group` (2.2M) smaller. **Lisp-visible:** the
  probes' answers become GNU's if they differ today (record them in S0).
- **Closes:** G15, G16, I3, C5, half of M6. A2.3, A3.4, A4.3.

### R12. Coding systems as coding.c keeps them (two commits, about 550 lines each)

- Vcoding_system_hash_table a Lisp hash table of GNU's spec vectors,
  `coding_attrs` as CODING_ID_ATTRS vectors, the category representatives
  as ids, `sjis_coding_system` and `big5_coding_system` (eval.rs:6919) and
  the terminal's codings as symbols; `coding_system_hash` (6957) and
  `coding_table_books` (6960) go.
- **Expected:** codingget 806.6 → about 700. **Lisp-visible:** probe in its
  S0 (`coding-system-plist` identity, `define-coding-system-internal`'s
  counts).
- **Closes:** G17, the other half of M6. A2.3, A3.4, A4.3.

### R13. nil and t at lispsym (after d14 S7; about 500 hand lines and a mechanical commit)

- **Files:** types.rs (`Value::Nil/T/Unbound` :1973, `Value::Symbol` :2015,
  `symbol_of_cell` :2024, `kind` :2186-2203, `symbol_name_by_tag` :2265),
  alloc/symbols.rs (`BUILTIN_NIL/T`, `builtin_boolean`, `mark_builtin_boolean`
  :311-320, `SymbolRef` from and to words), types/symbols.rs (`builtins`
  :970, `note_builtin_boolean` :1249, `builtin_boolean` :1274,
  `mark_names` :1466), alloc.rs (`mem_find` of a symbol-tagged word adds
  the base), the 59 `builtin_boolean` sites (mechanical commit),
  pdumper (`NilCells`/`TCells`, `load_builtin_cells`), native_comp/loader.rs
  (relocation words, :1326), the native backend's constants.
- **Moves:** `LISPSYM` static with nil, t and unbound; symbol words become
  offsets from it.
- **Risks:** the conservative scan must add the base before `mem_find`;
  several editors in one test process share nil's and t's plists and
  function cells (T2 records it; the thread plan makes one runtime a
  process); every `from_word`/`word` of a symbol in the native runtime
  (runtime.rs:6403-6412 already treats 0 and 48 specially).
- **Expected:** getnil 521.6 → about 225, plistnil 384.6 → about 300; each
  symbol dereference +1 (measure lex-rec, dyn-rec, get, interp-ignore; GNU
  pays the same add). **Lisp-visible:** none (D128 stays M3's).
- **Closes:** M4, C1, the two nil/t migration adapters of the progress map
  (docs/runtime-representation-progress.md:1750). A3.6, A2.3.

### R14. Cons blocks without the weak-reference serial (about 300 lines and the test conversions)

- **Files:** alloc.rs (`ConsMetadata.serials`/`serial_given` :1384-1396,
  `SERIAL` :361, `ConsMark::serial`/`given_serial` :1535-1560,
  `WeakConsRef` :1671-1700, `downgrade` :1622, `allocated_serial` :1968,
  `CELLS_PER_BLOCK` :71, the verifier's messages :2137-2149, :2225),
  types.rs (`WeakConsSlot` :1530, `ConsSlot::downgrade` :1516), the 39
  test sites.
- **Moves:** tests witness reclamation through GNU-observable means (a
  weak-key or weak-value hash table entry, a finalizer, the census) and
  survival through a strong reference; hidden-address checks that need
  "still this allocation" allocate the witness alone in a new block
  (`abandon_allocation_frontiers` is test-reachable) so a reused address
  cannot be another cons.
- **Risks:** H6 (keep every assertion; convert, do not delete); the image's
  cons blocks change layout (the fingerprint is the executable's SHA-256,
  so old images are refused; pdumper/tests.rs round trips rebuild theirs);
  the native runtime's checked decoding keeps the allocated bit.
- **Expected:** about 2,016 conses a block (from 1,344); `conses` and
  `conses-nogc` −1 to −3 a cons (fewer block refills); 2,016,296 live
  conses (the suite's sample) take 16 MB less. **Lisp-visible:** none.
- **Closes:** A3.3 (the serial; the allocated bit named and accounted in
  R15), A1's last users.

### R15. Honest census and live bytes (about 250 lines)

- `garbage-collect`'s free columns from the sweeps' counts
  (dispatch/misc_keymaps.rs:1653-1720; `FREE_CONSES`, `FREE_SYMBOLS`,
  `FREE_FLOATS`, the string and vector free counts, intervals).
- The physical record: the perf harness prints, per kind, the census
  count × GNU's size, × the actual cell size, the block metadata (cons
  allocated bitmap, float bitmaps, vector block marks), image bytes and
  host storage it can measure. Not a Lisp variable.
- `total_bytes_of_live_objects`' doc comment (eval.rs:5094-5096) says what
  the code does; the decision on which sizes `garbage-collect` prints for
  symbols and strings (open question Q2) is recorded.
- **Lisp-visible:** free counts as GNU reports them (free.el: GNU
  `(conses 16 1936 11311)`, Emaxx `(conses 16 1761 0)` today).
- **Tests:** T5; the report's free columns equal the sweeps' own counts
  (a library test reading `FREE_CONSES` and its siblings); after a
  collection that frees 100,000 conses the free count is positive in both
  editors (GNU releases a block only once it is wholly free and more than
  a block's worth is free, alloc.c's sweep_conses).
- **Closes:** A4.3 (free counts, live-byte formula), A3.3's accounting half.

### R16. Mapped image objects marked in a dump bitset; never swept (about 450 lines)

- **Files:** alloc.rs (`ImageCells` :247-330, `IMAGE_STRINGS/SYMBOLS/VECTORS`,
  the sweeps' image-block branches, `BLOCKS.mapped` :141), the mark paths
  (`MarkBit::mark` types.rs:217, the cons, float and vector bitmap marks),
  pdumper/load.rs (the region's range and bitset).
- **Moves:** `pdumper_object_p` is a range test on the mapped region; a
  mapped object's mark is a bit in the side bitset (one per 8-byte word),
  cleared at each collection; mapped blocks are not on the sweep lists.
- **Risks:** an object rebuilt in an image-region block that is *not*
  mapped keeps its in-object mark (two cases until R17); `mem_find` must
  still find mapped cells; the bitset is new process state (round audit
  R4: name it pdumper's `dump_private.mark_bits`).
- **Tests:** an object in the image made unreachable stays (GNU never
  frees the dump) and the census does not count it; an image object made
  to point at a fresh heap object keeps it alive across collections (the
  heap object is reached through the mapped object's fields);
  `EMAXX_GC_STRESS` over a loaded image.
- **Expected:** first-collection private dirty 21,360 → about 19,100 kB;
  explicit-gc about −1.6% (HANDOVER's image-block checks).
  **Lisp-visible:** none.
- **Closes:** A2, A4.1's marking, A4.2 (image sweeps).

### R17. What the loader still rebuilds (two commits, about 550 lines each)

- **R17a, symbols:** after R1, R2 and R13 a forward is a relocatable word
  and nil/t are `LISPSYM` entries, so `mapped_symbol_state`
  (context.rs:134) and `image_state` (alloc/symbols.rs:707) admit
  forwarded and built-in symbols; the 3,825 symbol records shrink to the
  uninterned and private ones; `install_symbol`'s per-field install and
  the SipHash set go for mapped symbols; the obarray's buckets are dumped
  and mapped (no `file_image` per symbol).
- **R17b, hash tables, native subrs, floats:** hash tables mapped and
  thawed in place (`hash_table_thaw`), native subrs mapped as Lisp_Subr
  objects with RELOC_NATIVE_SUBR fixups, floats mapped in a float block,
  `check_character_count` at load dropped for strings the dumper wrote.
- **Expected** (from the startup profile): `Loader::load` 63.6M → about
  40M, the SipHash set's 5.2M gone; GNU's non-native load is about 29M.
  **Lisp-visible:** none.
- **Closes:** A4.1 ("image symbols and other records are still rebuilt"),
  C9 for the converted groups.

### R18. Intervals as GNU's INTERVAL objects (decision; several rounds)

- intervals.c's balanced tree of 56-byte `struct interval` in interval
  blocks, marked by `mark_interval_tree` and swept by `sweep_intervals`,
  in place of the gap sequence (intervals.rs:50-80); `intervals-consed`
  raised by allocation; the census counts objects.
- **Decision first:** no Rust constraint forces the sequence (it is a
  performance choice), so A2.3 does not admit it as a deviation; B19 asks
  for real counters. If the owner keeps the sequence, record it as an
  explicit deviation with its measured advantage and keep `intervals-consed`
  documented as a count of make_interval events.
- **Closes:** B19's seventh counter, A3.6's text properties.

### Stage summary

| Stage | Hand lines | Expected | Lisp-visible change (GNU probe) | Items |
|---|---|---|---|---|
| R0 | 450 (tests) | — | — | — |
| R1 | 450 | setq-iq 932.6 → ~125, let-iq 1,312.6 → ~260 | none | A2.3, A3.4, A3.8 |
| R2 | 400 | setq-lastcmd 1,003.6 → ~130 | none | A2.3, A3.4, A3.8 |
| R3 | 150 | case-fold looking-at −30 | none | A3.4 |
| R4a/b | 350 + 500 | — | none | A2.3, A3.8 |
| R5 | 300 | put 564.6 → ~355 | defsubst deferral (probe) | A2.3, A3.8, A4.2 |
| R6a/b | 600 mech + 550 | mutex50 1,726.6 → ~700 | counts: mutex 9, condvar 17, thread 64 | A2.3, A3.6, A4.3, B19 |
| R7a/b | 600 mech + 550 | procname20 525.6 → ~375 | process counts | A2.3, A3.6, A4.3 |
| R8 | 550 | priv-intern lower | obarray 2, winconf 121 | A3.6, A4.3, B19 |
| R9 | 2 × 550 | — | counts of fonts, modules, tree-sitter, SQLite | A3.6, A4.3 |
| R10 | 450 | record decode −2 | none | A2.3, A3.8, A4.2, A4.3 |
| R11a/b | 2 × 550 | encodechar 733.6 → ~490 | probes in S0 | A2.3, A3.4, A4.3 |
| R12 | 2 × 550 | codingget 806.6 → ~700 | probes in S0 | A2.3, A3.4, A4.3 |
| R13 | 500 + mech | getnil 521.6 → ~225 | none | A2.3, A3.6 |
| R14 | 300 + tests | conses −1..−3, 16 MB on 2M conses | none | A3.3 |
| R15 | 250 | — | free counts | A3.3, A4.3 |
| R16 | 450 | dirty −2.3 MB, explicit-gc −1.6% | none | A4.1, A4.2 |
| R17a/b | 2 × 550 | Loader::load 63.6M → ~40M | none | A4.1 |
| R18 | decision | — | — | B19, A3.6 |

**When the items close** (with the other plans): A2.3 after R1-R13 with
d14's S8 and the thread plan's stage 6; A3.4 after R1-R3, R11, R12 and
d13; A3.6 after R6-R9, R13, d14 S7 and d13 (strings' 16 bytes stay a
named deviation, §2.6); A3.8 after R1, R2, R4, R5, R10, d14 S7 and d13
S11 (the last adapters); A4.1 after R16 and R17 (and d13 for buffers);
A4.2 after R5, R10, R16; A3.3 after R14 and R15; A4.3 after R6-R12 and
R15 with d13 S10; B19 after R6, R8 and R18's decision with d13 S10, in
the final run.

## 4. Traps and open questions

**T1. The round audit (tools/round_audit.py).** H6 counts `#[test]`
functions and assertions removed against added: R4, R10 and R14 delete
tests of removed machinery (`record_registry.rs`'s test, the weak-cons
tests, the native timer tests); each must convert, not drop, its
assertions, and the stage must add at least as many. H5 forbids any new
`#[allow]`, `#[expect]` or `cfg_attr(..., allow)`: code a stage leaves used
only by tests is gated `#[cfg(test)]`; the module-wide `#![allow(dead_code)]`
of types.rs and buffer.rs already hides dead runtime code (A1 was found
that way), so each stage checks its removals with a scratch build without
that allowance (not committed). R4 (new state) fires for R16's bitset and
R2's kboard: name their GNU counterparts in the record. R6 (reporting
infrastructure) fires for R5 (src/compat.rs, batch.rs's report) and for
any change to the anti-cheat file.

**T2. The anti-cheat gates (src/anti_cheat.rs).**
`interpreter_value_fields_are_gc_roots_or_documented` (1608) scans
`InterpreterState`'s fields whose types mention `Value`, `SymbolCells`,
`LocalCells` or `BufferLocalBindings` and requires each in the root-marking
functions or its documented list; it also parses `struct ThreadState {`
and the text from `if record.kind == RecordKind::Thread` to
`Kind::ReaderForm(form) =>` in eval.rs (1721-1735) and panics if they are
missing: R4b and R6b must move that check to the new visitor in the same
commit. `interpreter_roots_are_dumped_or_documented` (1753) requires every
`self.field` the marking reads to be dumped or in `ROOTS_RESET_AFTER_LOAD`/
`TRANSIENT_ROOTS`; `interpreter_fields_are_carried_or_documented` (1823)
requires every field to be dumped or in `FIELDS_NOT_CARRIED`: each removed
field leaves those lists in the same commit (a stale entry is not caught),
each new field enters them. The census formulas are tests in
primitives/tests.rs (`garbage_collect_reports_the_live_census` 20416,
`native_pseudovector_census_matches_gnu_word_layout` 26068 and their
neighbours): R6-R10 change counts toward GNU's and must state each changed
expectation old and new, as round 378 did.

**T3. The dump format and fingerprint.** The fingerprint is the
executable's SHA-256 (pdumper/image.rs:32-70), so any build refuses an old
image; no `.pdmp` is committed. Formats change in R4a (a root slot), R5
(ErtTests), R10 (record codes), R13 (nil/t), R14 (cons blocks), R16-R17;
pdumper/tests.rs builds its images in the test. The image template of the
test support (thread plan P9) is rebuilt per process.

**T4. Native code and modules that hold handles.** GNU's `.eln` files
are loaded as they are: nil is the immediate 0, every other constant comes
from `d_reloc` (comp.c:1790-1802), so only the words the loader writes
change (R13); `symbols_with_pos_enabled` is read through a relocated
pointer (comp.c:1570), so R1's Boolfwd must keep that address stable.
Native subrs and units already have GNU's layouts (native_functions.rs:12,
native_units.rs:13). Modules see only `emacs_value` boxes
(modules/mod.rs:44); R9 moves user pointers and module functions into
objects without changing the ABI, but a user pointer's finalizer now runs
from the sweep's `cleanup_vector`, which must not call into the runtime.

**T5. No lint suppression** (goal §8, round audit H5): see T1. R6's and
R7's Rust-owned handles must not need `#[allow(clippy::vec_box)]` beyond
the existing ones.

**T6. Several editors in one process.** Tests build many interpreters
(the thread plan's subject). R1's forwards point into one editor's state:
the test copier (eval.rs:4053-4120, cfg(test)) must re-point them. R13's
`LISPSYM` makes nil's and t's plists and function cells process-wide, as
GNU's are; T2 shows whether any test relied on per-editor nil plists.

**T7. Process-heavy and thread-heavy validation.** R6 and R7 change the
objects the reaper thread, the sentinels and the suspended native stacks
touch; run the process, thread and network files of the frozen
inventory and `native_thread_continuations`, not only the library tests.

**T8. The ERT report** (R5) is what the frozen comparison reads: the
discovered-test lists of all 519 files must be identical before and after.

**T9. d13's S4 extends the evaluator mirror** (it adds `case-fold-search`
and `parse-sexp-lookup-properties` to `FORWARDED_EVAL_CELLS`). Land R1
first and make S4 use the forwarded fields, or the mirror grows.

**T10. R11/R12 touch coding.rs (4,714 lines) and the charset loader:**
the coding systems are defined at startup by mule-conf.el and the image;
every change runs the coding, charset and file-I/O files of the frozen
inventory.

**Open questions.**
- **Q1 (R18):** GNU's interval objects, or the sequence as a documented
  deviation. The owner's decision; B19 depends on it.
- **Q2 (R15):** which sizes `garbage-collect` prints while a cell is
  larger than GNU's struct: GNU's (Lisp sees GNU's numbers, the physical
  record states the rest) or the actual (Lisp sees the difference). Today
  it prints GNU's; the plan keeps that and adds the physical record.
- **Q3 (§2.6):** whether to replace the cons `allocated` bitmap by GNU's
  `dead_object ()` car (2,030 conses a block; the free list then links
  through the cdr).
- **Q4:** the string cell's 16 extra bytes: `borrows` is Rust's aliasing
  guard while a borrow of the bytes lives; the mark word and flags could
  share one word (40 bytes). No stage here; A3.6 lists it.
- **Q5 (R5):** the defsubst deferral's origin and whether its upstream test
  still needs it with the native-compiled image.
- **Q6:** frames (`FrameValue`'s `RefCell<FrameState>`, types.rs:1083) and
  terminals keep Rust structs inside their objects with no registry or
  mirror (FrameState holds objects, not ids); their layouts differ from
  GNU's, which the census shows. Not in this group's items; recorded for
  A3.6's review.

## Appendix: probes and raw numbers

Scratch: `the scratch directory mirrors/`.

- `bin/cgd.sh PROBE N1 N2`: callgrind of both editors at two counts, the
  difference an iteration.
- `batch1.txt`, `batch2.txt` (100,000:300,000, GNU / Emaxx, an
  iteration of the byte-compiled loop, its 116.0 / 71.6 included): base
  116.0/71.6; put 399.0/564.6; get 338.0/220.6; getnil 388.0/521.6;
  plistnil 354.0/384.6; setq-iq 248.0/932.6; setq-pl 248.0/120.6; let-iq
  630.0/1,312.6; let-pl 630.0/249.6; setq-lastcmd 257.0/1,003.6; mutex
  709.0/726.6; mutex50 709.0/1,726.6; threadname 366.0/381.6; procbuf
  371.0/399.6; procname 371.0/392.6; procname20 (the last of 21)
  371.0/525.6; charsetp 428.0/407.6; charcharset 535.0/634.6; codingp
  431.0/413.6; codingget 708.0/806.6; encodechar 493.0/733.6; symname-nil
  and symname-sym 176.0/103.6; let-cfs 1,291.0/974.6. handler-deep 7,248.3/11,322.4 and
  handler-shallow 1,328.3/2,542.4 (20,000:60,000).
- `probes/counts.el`, `counts2.el`: `memory-use-counts` deltas (§1.11).
- `probes/free.el`: `garbage-collect` after freeing 100,000 conses.
- `probes/rss.el`: VmRSS and Private_Dirty around two collections.
- `probes/lisp-visible.el`: `mapatoms` while interning, collections
  under equal settings, `makunbound` of `max-lisp-eval-depth`.
- `cg-emaxx-startup.out`, `cg-gnu-startup.out`: `-Q --batch -l empty.el`,
  129,778,726 and 152,161,966 instructions.
