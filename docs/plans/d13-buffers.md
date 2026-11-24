# Buffers as buffer.h's struct buffer (D13, B19, A3.6): inventory and staged plan

Written on 23 November 2026 before round 381, from a reading of the tree at
round 378 and callgrind and `memory-use-counts' runs on round 377's release
build. Paths beginning `/home/user/` and `/tmp/` are those of the container it
was written in; line numbers are of that tree, and the expected counts are
estimates. HANDOVER.md records what each stage measured. Stages S0, S1 and S2
are round 381.

Read-only work. Nothing was built and no source was modified. Measurements come from callgrind and `memory-use-counts` on the prebuilt editors: GNU `/home/user/emacs-mirror/emacs/src/emacs` and Emaxx `target/prof-r377`. The draft tree is r378, and its buffer code is unchanged since r377. The layout is from `gdb ptype /o struct buffer` on the GNU binary. Probes, outputs and annotated assembly are in `the scratch directory bufplan/`: `counts.el`, `counts2.el`, `*-20000.el`/`*-60000.el`, `gdb-buffer.txt`, `lan.ann`, `sm.ann` and `fdiff.py`.

Path prefixes: `S/` = `/home/user/emaxx-draft/src/`, `R/` = `S/lisp/`, `G/` = `/home/user/emacs-mirror/emacs/src/`. Counts are grep counts excluding definitions. "≈" means classified by reading.

## 1. Inventory

### 1.1 What a buffer is today
| Type | Where | What |
|---|---|---|
| `BufferRef` = `VectorlikeRef<BufferValue>`, PVEC tag 13 | R/types.rs:1198, R/alloc/vectors.rs:1289 (`LISP_SLOTS` 0) | The Lisp object: a real pseudovector, `Value::Buffer`, eq by address |
| `BufferValue { id: u64, markers, base, state: RefCell<Buffer>, live: Cell<bool> }` | R/types.rs:1012-1025 | 42 payload words (measured: 47 cells for get-buffer-create = 42 + the 5-word mark marker) |
| `Buffer` (≈45 fields, 290 methods) | S/buffer.rs:110-252, impl 1306-4859 | The rest of the buffer behind the RefCell. The file starts with `#![allow(dead_code)]` |
| `BufferText` behind `Rc<RefCell<…>>` | S/buffer.rs:121, 305-424 | Shared by an indirect family. Holds GNU's modiffs, unchanged counts, redisplay and window_count, intervals (Rust `Intervals`), and Emaxx caches: raw_markers, raw_anchor, PositionCache, checkpoints, char_cache, edit serials and log, extended_chars |
| `GapText { data: Rc<GapData>, newlines }` | S/buffer/gap.rs:22-52 | Bytes plus gap_start/gap_end/gap_char/chars. UTF-8 with raw bytes as U+E080..E0FF, not GNU's internal encoding. Copy-on-write: a scan shares the bytes by cloning the Rc |
| `MarkerValue` | R/types/marker.rs:9-16 | Already lisp.h's marker: buffer pointer and weak `next` chain. The chain head is `BufferValue.markers` of the chain owner (the base: marker.rs:164) |

### 1.2 Where each part of GNU's buffer lives now
| GNU part | Emaxx |
|---|---|
| text, gap, Z/GPT and their bytes | `Buffer.text` → `BufferText.text: GapText` → `Rc<GapData>` |
| PT, BEGV, ZV | `Buffer.pt/begv/zv`, chars only. The bytes are recomputed on each use (`match_view`, S/buffer.rs:2204) |
| markers | `BufferValue.markers` on the chain owner |
| intervals | `BufferText.intervals: Option<Intervals>`, a Rust tree. `intervals-consed` is never raised |
| local_var_alist and local_flags | `Interpreter.buffer_locals: HashMap<u64, LocalCells>` (R/eval.rs:7024, R/eval/local_cells.rs). The loaded binding cache is `LoadedBindings{symbol, buffer: u64, shape}` (local_cells.rs:345-425) |
| 19 always-local slots | 14 in `LocalCells` FIXED_SLOTS (local_cells.rs:50; names at R/eval/variables.rs:97). 5 on `Buffer`: file, file_truename, undo_list, mark_active, `multibyte: bool` |
| 44 flagged per-buffer slots (tab-width …) | Ordinary bindings in `LocalCells`, or the symbol's default |
| syntax/category tables | `buffer_syntax_tables`, `buffer_category_tables`: `BufferCharTables` keyed by id with a last-hit cache (R/eval/coding.rs:1477, eval.rs:7030/7034) |
| four case tables | `buffer_case_tables`: `BufferCaseTables`, a Vec keyed by id (coding.rs:1523, eval.rs:6958) |
| overlays | `Buffer.overlays: OverlayTree` |
| undo list | `Buffer.undo_list`. Indirect sharing is copied on each switch (R/eval/runtime.rs:700-719) |
| other C slots (name, last_name, keymap, mark, last_selected_window, text_conversion_style, modtime …) | `Buffer` fields |
| base_buffer | Three copies: `BufferValue.base` (while sharing), `Buffer.base_buffer` (kept after kill) and `Interpreter.indirect_buffers: Vec<(u64,u64)>` (eval.rs:7041, scanned linearly by `buffer_base_id`, R/eval/buffers.rs:194) |
| liveness (name non-nil) | Three copies: `BufferValue.live`, `Buffer.killed`, and membership of `inactive_buffers` |
| ts_parser_list, display_error_modiff, indirections, region caches | absent (tree-sitter parsers hold `buffer_id`, R/eval.rs:1510-1512) |

### 1.3 Naming a buffer by id
| Pattern | Sites (files) |
|---|---|
| `current_buffer_id()` / `self.current_buffer_id` | 335 (57) / 86 (14) |
| `set_current_buffer_id(` | 145 (33) |
| `get_buffer_by_id(` / `_mut(` (linear scan of `inactive_buffers`, buffers.rs:624-647) | 135 (30) / 63 (17) |
| `buffer_object(` (buffers.rs:612) / `buffer_value(` (eval.rs:5281) | 44 (21) / 67 (24) |
| `has_buffer_id(` (runtime.rs:69) / `live_buffer_id(` (573) / `resolve_buffer_id(` (580) | 78 / 83 / 19 |
| `Kind::Buffer(` / `Value::Buffer(` | 119 (38) / 51 (25) |
| `.id` of a BufferRef | ≈55 (22) |
| Current buffer through the RefCell: `.buffer.borrow()` / `.buffer.borrow_mut()` | 773 (60) / 222 (36). The hot readers among them: point 132, point_max 95, point_min 67, is_multibyte 60, size_total 30, char_at 22 |
| `fn` signatures with a buffer-id parameter | ≈177 (variables.rs 44, runtime.rs 25, eval.rs 16, buffers.rs 13, window.rs 10, loading.rs 9, display.rs 9, coding.rs 9, tty.rs 7, …) |
| Test sites using ids | ≈70 (tests.rs, eval_01/05, pdumper/tests.rs, vectors/tests.rs). Hand-built ids: `Value::buffer(52, …)` (native_comp/runtime.rs:13513, 18163; loader.rs:262; tests.rs:3886; types.rs:4571) |

### 1.4 Registries and side tables keyed by id
| Table | Holds |
|---|---|
| `buffer: BufferRef` + `current_buffer_id` (eval.rs:6805, 6810) | The current buffer, swapped in and out of the next entry by `set_current_buffer_id` (runtime.rs:657-697: `position`, `swap_remove`, `push`) |
| `inactive_buffers: Vec<(u64, BufferRef)>` (6859) | The registry: every live non-current buffer and print.c's hidden one |
| `buffer_list: Vec<(u64, Value)>` (6862) | Vbuffer_alist as (id, name). Dumped and restored as a Lisp alist (R/eval/dump_roots.rs:537-543, 1022-1041) |
| `next_buffer_id` (6864), `alloc_buffer_id` (runtime.rs:59), `reserve_buffer_id` (eval.rs:5380) | The id counter |
| `ALL_BUFFERS: Mutex<HashSet<usize>>` (R/alloc/vectors.rs:372-388; insert 1330, remove 1478, sweep 1636-1675) | A process-wide set of every buffer header, live or dead, for marker unchaining. GNU 30 has none: `sweep_buffers` walks Vbuffer_alist (G/alloc.c:7852) |
| `buffer_locals`, `loaded_bindings`, the three table maps | As in §1.2 |
| `FrameState.buffer_list`, `buried_buffer_list: Vec<u64>` (eval.rs:3955-3956) | GNU's `f->buffer_list` is a Lisp list |

### 1.5 Holders of a buffer id (≈39 fields; script count)
- **eval.rs:** TreeSitterParserState.buffer_id and list_buffer_id (1510, 1512); `SpecialBindingScope::BufferLocal(u64)` (1592, GNU's specbind `where`); ThreadState.buffer_id (2042); CombinedAfterChangeState (2075); LabeledRestriction (2087); SavedExcursion (2097); SavedRestriction (2107); ProcessState.buffer_id (3568, beside the `buffer: Value` slot); WindowConfigurationSnapshot.current_buffer_id (3734); MinibufferRuntimeState.active_buffer_id (4029).
- **InterpreterState:** prin1_to_string_buffer, print_unreadable_callback_buffer (6728), code_conversion_reused_workbuf (GNU's Vcode_conversion_reused_workbuf is a buffer object), buffer_before_last_command_or_undo (7117).
- **Elsewhere:** `UnwindAction::CurrentBuffer(u64)` (R/eval/specpdl.rs:79), unmarked (102), pushed by bytecode/vm.rs:3145 and native_comp/runtime.rs:5811; ObjectKey::Buffer (pdumper/context.rs:43); ActiveMinibuffer (completion.rs:1402, 1409); SearchObject/Destination/FindStart/WindowRenderInfo; 9 tty.rs job structs (125, 2241, 3075, 3125, 4879, 5604, 6854, 10390).
- **Already objects:** `last_thing_searched`, marker.buffer, overlay buffer, window `contents`, `process.buffer`.

### 1.6 Collector
- **Trace:** `Kind::Buffer` → `buffer.borrow().visit_lisp_values` (eval.rs:4953-4957 → S/buffer.rs:2523-2557). It visits name, last_name, text_conversion_style, mark_active, the mark marker, pt/begv/zv markers, base, interval plists, undo_list, keymap, last_selected_window, file, file_truename, killed_locals and overlays.
- **Roots:** `self.buffer`, `inactive_buffers` and the buffer_list names (eval.rs:5943-5948). The locals' symbols and values (5667-5671, 5698-5701), the case, syntax and category tables (5735-5745) and last_thing_searched (5714-5717) are rooted on the interpreter, not traced from the buffer.
- **Sweep:** ALL_BUFFERS pass (vectors.rs:1667-1674); `cleanup_vector` drops the RefCell<Buffer> (1476-1480). Census: `buffers` = `buffer_list.len()` (eval.rs:6024), size `GNU_BUFFER_SIZE = 992` (eval.rs:4696). vector-slots counts the real 43 words.

### 1.7 Dumper and loader
- **Writer:** `dump_buffer` (pdumper/context.rs:2012-2222) writes a custom 32-word record (BUFFER_ID … BUFFER_VARIABLE_PART, 2860-2915). It contains the id, then locals and the marker chain. A killed buffer is a record with BUFFER_FLAG_DEAD. Objects are keyed by id (context.rs:86).
- **Loader:** load.rs:1010-1034 allocates `BufferRef::for_restore(id, …)`. A dead buffer only reserves its id. `load_buffer` (2237-2410) calls `install_buffer` (eval.rs:5387-5404), which **replaces any process buffer with the same id**, the current one included (`self.buffer = buffer`). The `buffer_locals`, table and indirect side tables are reinstalled by id.

### 1.8 Killed buffers
`kill_buffer_id` (runtime.rs:2677-2741):
- Frames' lists, undo nil, out of buffer_list, windows replaced.
- Markers unchained (all of them for a base: GNU's too).
- `leave_marker_chain`, overlays deleted.
- The 14 FIXED slots plus mark-active are copied into `Buffer.killed_locals` (S/buffer.rs:152, which carries `#[allow(clippy::type_complexity)]`). The side tables are cleared.
- `live.set(false)`, then `release_killed_storage` (S/buffer.rs:4395): a new empty BufferText keeping the modiffs; size and bounds go to `killed_size`/`killed_bounds`.
- `remove_buffer_id` takes it out of the registry. It stays `self.buffer` if it is current.

## 2. GNU's layout and counts

### 2.1 struct buffer (gdb, `gdb-buffer.txt`)
- **Size:** sizeof 992 = header plus VECSIZE 123 words (G/buffer.h:305-709). BUFFER_LISP_SIZE = 76 (buffer.h:1085), rest 47.
- **Lisp fields:** 76 words at offsets 8-608, `name_` … `cursor_in_non_selected_windows_`. `undo_list_` is the last word (984). It is marked apart: alloc.c:6959 and compacted at 6648.
- **own_text (616-759):** `struct buffer_text`, 144 bytes. beg, gpt, z, gpt_byte, z_byte, gap_size, modiff, chars_modiff, save_modiff, overlay_modiff, compact, beg/end_unchanged, unchanged_modified, overlay_unchanged_modified, intervals, markers, then the inhibit_shrinking and redisplay bits.
- **C fields:** text 760, pt…zv_byte 768-808, base_buffer 816, indirections and window_count 824/828, local_flags[50] 832, modtime 888, modtime_size 904, auto_save_modified 912, display_error_modiff 920, auto_save_failure_time 928, last_window_start 936, newline/width_run/bidi caches 944-960, four bits 968, overlays 976.
- **Local flags:** 19 always-local (`-1`), 14 C-only (`0`), and 44 slots with local-flag indices 1.. (G/buffer.c:4645 init_buffer_once).
- **case-fold-search** is not a field. It is DEFVAR_LISP made buffer-local (buffer.c:5971-5975): a C global that set_buffer_internal_2 (buffer.c:2311) loads (data.c:1542). parse_sexp_lookup_properties is a DEFVAR_BOOL, likewise (syntax.c:3749).
- **Allocation:** `allocate_buffer` (alloc.c:3705) is one ALLOCATE_PSEUDOVECTOR. `mark_buffer` (alloc.c:6950) marks the Lisp words, intervals, the undo list (dead buffers only), overlays and base_buffer. `sweep_buffers` unchains dead markers of the live buffers only, because Fkill_buffer unchained the rest.

### 2.2 GNU's field → Emaxx today
| GNU | Emaxx |
|---|---|
| name_, last_name_, filename_, file_truename_, mark_, mark_active_, keymap_, last_selected_window_, text_conversion_style_, undo_list_ | `Buffer` fields (BufferMark is a position until installed) |
| directory_, backed_up_, save_length_, auto_save_file_name_, read_only_, major_mode_, local_minor_modes_, mode_name_, file_format_, auto_save_file_format_, point_before_scroll_, invisibility_spec_, display_count_, display_time_ | `LocalCells` fixed slots; `killed_locals` once killed |
| enable_multibyte_characters_ | `Buffer.multibyte: bool` |
| local_var_alist_ | `buffer_locals[id]` |
| syntax_table_, category_table_, the 4 case tables | interpreter side tables (none = standard) |
| pt_marker_, begv_marker_, zv_marker_ | `Buffer.position_markers` |
| 44 flagged per-buffer slots, width_table_, ts_parser_list_ | LocalCells bindings or absent |
| own_text, text | `Rc<RefCell<BufferText>>` + `Rc<GapData>` |
| pt_byte, begv_byte, zv_byte | absent (recomputed) |
| base_buffer | three copies (§1.2) |
| local_flags | LocalCells presence |
| inhibit_shrinking | absent (COW share instead) |
| inhibit_buffer_hooks | `Buffer.inhibit_hooks` |
| Emaxx-only | `id`, `live`, `killed*`, `positions_away`, `text_conversion_style_is_local`, `point_before_last_command_or_undo` (keyboard.c static), `undo_inhibit_record_point` (C global), the text caches |

### 2.3 What GNU allocates and counts (measured)
`memory-use-counts` deltas are baseline-corrected. The order is conses, floats, vector-cells, symbols, string-chars, intervals, strings.
| Operation | GNU | Emaxx |
|---|---|---|
| get-buffer-create (new name) | (2 0 128 0 8 0 2) | (0 0 47 0 8 0 2) |
| generate-new-buffer, 1st / 2nd same name | (2 0 128 0 5 0 1) / (2 0 128 0 16 0 2) | (0 0 47 0 5 0 1) / (0 0 47 0 19 0 3) |
| with-temp-buffer (empty / insert "abc") | (9 0 140 0 7 0 1) / (12 0 140 …) | (1 0 59 0 7 0 1) / (1 0 59 …) |
| make-indirect-buffer, 1st / 2nd on one base | (2 0 158 …) / (2 0 143 …) | (0 0 82 …) / (0 0 67 …) |
| get-buffer-create + kill-buffer | (3 0 133 0 3 0 1) | (1 0 52 0 3 0 1) |
| kill-buffer, not current / current | (1 0 5) / (7 0 5) | (1 0 5) / (1 0 5) |
| other-buffer | 6 conses | 0 |
| setq-local of a new variable | 3 conses | 0 |
| insert "abc" in a named buffer | (18 0 12) | (14 0 10) |
| census per buffer: vectors / vector-slots / `buffers` | +2 / +130 / (buffers 992 n) | +2 / +49 / (buffers 992 n) |

- **The brief's numbers:** "133 and 3 against 52 and 1" are create plus kill. Creation alone is 128 cells (VECSIZE 123 + the 5-cell mark marker) and 2 conses (the Vbuffer_alist entry) against 47 and 0.
- **make-indirect-buffer:** GNU's 143 = 123 + 4 markers. Emaxx's 67 = 42 + **5** markers: `replace_state` installs a second mark marker (R/primitives/dispatch/buffer_meta.rs:385-387 → R/types.rs:1188-1193).
- **generate-new-buffer, second call:** one more string (3 chars) than GNU, whose "<N>" is an AUTO_STRING.
- **with-temp-buffer:** of GNU's 9 conses, 2 are the alist and 6 are `Fget_buffer_window`'s window list (window.c), outside this item.

## 3. Where looking-at and string-match spend the excess (measured)

Instructions per iteration, 40,000-iteration difference, byte-compiled loop. Emaxx's per-function costs are self costs. Lines inside `looking_at_named` and `string_match_1` were split with `--dump-instr` and objdump (`lan.ann`, `sm.ann`); the r377 binary has no line info.

| Probe | GNU | Emaxx | Excess |
|---|---|---|---|
| looking-at "x" failing at "abc" | 1147.0 | 1385.6 | +238.6 |
| looking-at "a" matching | 1370.0 | 1555.6 | +185.6 |
| string-match "x" "abc" failing | 1148.0 | 1374.6 | +226.6 |
| string-match "a" "abc" matching | 1595.0 | 1577.7 | −17.3 |

**looking-at, failing, by function**
| Part | GNU | Emaxx |
|---|---|---|
| VM and call | exec_byte_code 304 + funcall_subr 64 + Flooking_at 6 | run_verified 313 + direct_looking_at 6 |
| Freeze and unwind | unbind_to 112 + record_unwind_protect_ptr 28 + unwind_re_match 4 + unfreeze 3 | release_cell 33 + drop LispMatchEnv 20 |
| Pattern cache | compile_pattern 90 + Fstring_equal 42 + internal_equal 37 + Fequal 11 + memcmp 10 | compile_pattern 168 + memcmp 10 |
| Matcher | re_match_2 35 + re_match_2_internal 238 + memset 20 | re_match 77 + re_match_2_internal 156 + PooledState::new 43 + Reader::load 18 |
| **Buffer state and text** | looking_at_1 104 + RE_SETUP_SYNTAX_TABLE_FOR_OBJECT 24 + re_set_registers 11 | looking_at_named 303 + BufferSnapshot::take 86 + offset 6 + local_variable_value_key 68 + setup_syntax 42 + canon_for 36 |

**Inside looking_at_named's 303 (from the assembly)**
- About 105 build the gap halves and the `Object::Buffer` from the snapshot, with bounds checks.
- About 51 read `case-fold-search`: a thread-local symbol key, the call and the test (the call itself is the 34 above).
- About 22 are the case tables' last-hit lookup in `BufferCaseTables` by id (copying the 4-table entry) and about 10 the syntax table's in `BufferCharTables`.
- About 8 drop the snapshot's Rc.
- About 25 read `inhibit-changing-match-data` through a thread-local symbol key. That is not buffer state.
- The rest is GNU's looking_at_1 work plus Registers moves.
- `setup_syntax` reads `parse-sexp-lookup-properties` the same way: 34 + 42, against GNU's 24.

So the excess is all on the buffer side: about 400 against GNU's about 140. Emaxx is cheaper in the VM (−55) and in freezing (−91).

**What a buffer with GNU's fields removes (estimates)**
| Change | looking-at | string-match |
|---|---|---|
| Fields read directly instead of the snapshot (§4 S5) | −140..160 | — |
| case-fold-search and parse-sexp-lookup-properties as forwarded C words (S4) | ≈ −95 | ≈ −45 (only case-fold is read for a string) |
| case and syntax tables as fields (S3) | ≈ −25 | ≈ −25 |
| `set_buffer_search_regs` (109 + 10, matching only) against GNU's BYTE_TO_CHAR loop and buf_bytepos_to_charpos 42 | ≈ −40 | — |

- Expected after S3-S5: looking-at failing ≈ 1,120 and matching ≈ 1,230 (GNU 1,147 / 1,370); string-match failing ≈ 1,300 (GNU 1,148).
- **Not removed by this item:**
  - `canon_for` (36): a thread-local Rc cache of the canonicalization's ASCII part, where GNU reads the char-table's `ascii` sub-table. This belongs to a char-table item.
  - string-match's remaining ≈150: StringText::new 64 and string_match_1's own 275 against GNU's 87 (argument decoding, the thread-local symbol keys). This is the strings side of A3.6.

**Other buffer probes** (whole loop iteration, GNU / Emaxx):
- goto-char 2: 232.0 / 133.6
- insert "x", no undo: 3412.4 / 2083.5
- point: 179.1 / 109.6
- **char-after 2: 260.0 / 443.6.** Emaxx: op_in_place 156, public_buffer_char_code_at 53, text_char_at 44, direct_char_after 33, extended_char_at 19. GNU: Fchar_after 34 + buf_charpos_to_bytepos 21.
- **with-current-buffer b: 515.0 / 697.7.** Emaxx: set_current_buffer_id 318 and resolve_buffer_id 81 (registry scans). GNU: set_buffer_internal_2 98 and record/fetch_buffer_markers 42.
- buffer-name of another buffer: 355.0 / 405.6 (decode_buffer 26 + lifted 50, against Fbuffer_name 14)
- tab-width read from Lisp: 226.0 / 171.6
- get-buffer-create + kill-buffer: 51,022 / 17,924

## 4. Stages

**Target.** `BufferValue` becomes buffer.h's struct buffer after the header (precedent: R/alloc/vectors/windows.rs):
- **Lisp fields:** 76 `Cell<Value>` words in GNU's order (`LISP_SLOTS = 76`, traced as mark_vectorlike traces BUFFER_LISP_SIZE).
- **own_text:** a `BufferText` of Cells.
  - `beg` is the shared byte allocation, the copy-on-write counterpart of `inhibit_shrinking`; gpt, z, gpt_byte, z_byte and gap_size are fields; the modiffs, `intervals`, `markers` and the two bits complete it.
  - Emaxx's text caches go in one boxed structure in the base's `newline_cache` word, outside the Lisp heap as GNU's xmalloc'd region caches are.
- **The rest of GNU's C fields:** `text`, pt…zv_byte, base_buffer, indirections, window_count, local_flags, modtime …, overlays, undo_list_. Size 984; header plus payload 992.
- **No id:** the address is the identity. Vbuffer_alist is a Lisp alist, the current buffer is a `BufferRef`, and today's `Buffer` methods become `&self` methods on `BufferRef`.

Each stage builds, passes and changes no behaviour unless the stage says so. Hand-written lines are kept under about 600; mechanical conversions are committed one file family at a time: `primitives/dispatch/*`, then `primitives/*`, then `eval/*`, then `tty*` and `native_comp`.

### S0 — Tests that must exist first (tests only, about 350 lines)
- **T1 killed buffer:** `buffer-name` nil, `buffer-file-name` kept, `buffer-local-value` of default-directory, major-mode and mode-name after kill, `buffer-base-buffer` of a killed indirect buffer, `buffer-live-p`, markers detached. Extends eval_02.rs:8456 and tests.rs:650.
- **T2 indirect family:**
  - Edits move every member's point and markers (tests.rs:10977, 35729 exist).
  - The undo list is shared across switches.
  - kill-buffer of the base kills the indirect buffers (tests.rs:453 exists).
  - window_count over the family.
- **T3 save-current-buffer:** a buffer killed in the body, with `(garbage-collect)` in the body, comes back as GNU's set_buffer_if_live has it.
- **T4 threads:** a thread whose current buffer another thread kills, and a collection while it is parked (extends tests.rs:522).
- **T5 images:**
  - A variable holding a killed buffer and one holding an indirect buffer.
  - Per-buffer case and syntax tables, and a buffer-local `case-fold-search`.
  - **The id collision:** the image's *scratch* (id 0) against the process's *scratch* and the initial window's contents (eval.rs:5387-5404). This is the counterpart of D119.
- **T6 case tables:** after `set-standard-case-table`, old buffers keep their tables and new ones take the new standard (GNU).
- **T7 forwarded variables:** `let` of case-fold-search and parse-sexp-lookup-properties in buffer A, then a search in B; setq-local; kill-local-variable; kill-all-local-variables; a variable watcher; a thread switch.
- **T8 matcher callout:** a `syntax-propertize-function` that puts properties during `looking-at` and `re-search-forward`.
- **T9 buffer-swap-text:** with markers, overlays, intervals and point.
- **T10 counts:**
  - A Rust test that pins memory-use-counts for get-buffer-create, with-temp-buffer, make-indirect-buffer and kill-buffer as a formula of `size_of::<BufferValue>()` (like tests.rs:25203).
  - The GNU-valued oracle versions arrive with S9 and S10.
- **Perf probes:** add `char-after`, `set-buffer`, `insert`, `goto-char` and `buffer-name` probes to tools/perf/probes (R6 review), beside looking-at and string-match.

### S1 — Handle funnel (about 450 hand-written lines and about 700 mechanical)
- **Add (R/types.rs near 1151):** reader methods on `BufferRef` with the final object's names, `pt()`, `begv()`, `zv()`, `z()`, `multibyte()`, `modiff()`, `chars_modiff()`, `name()`, `char_at()`, over `self.borrow()` for now; `Interpreter::current_buffer() -> BufferRef`; `decode_live_buffer(&Value) -> Option<BufferRef>`.
- **Convert:** about 430 hot read sites (§1.3), one family per commit. Then `live_buffer_id` and `resolve_buffer_id` (runtime.rs:573-597) test `is_live()` instead of scanning `inactive_buffers`.
- **Effect:** with-current-buffer about −60 (resolve_buffer_id 81 → ≈20). Everything else is neutral.
- **Risk:** "live" and "registered" must agree. They differ after an image install replaces a same-id buffer (§1.7). T5 must pass before this change.

### S2 — One liveness and one base (about 300 lines)
- **Liveness:** delete `Buffer.killed`. `BufferValue.live` is the only flag, and becomes `NILP (name_)` in S6.
- **Base:** `BufferValue.base`, `Buffer.base_buffer` and `indirect_buffers` become one `base_buffer` field with GNU's lifetime (kept after the kill), plus `indirections`. `buffer_base_id`, `root_buffer_id` and `related_buffer_ids` (buffers.rs:194-231) read the field. The family comes from walking Vbuffer_alist, as insdel.c does.
- **Delete:** the `indirect_buffers` row of FIELDS_NOT_CARRIED (dump_roots.rs:391), in the same commit.
- **Effect:** set_current_buffer_id's two `buffer_base_id` scans go when indirect buffers exist.
- **Risks:** `chain_owner` (marker.rs:164) must mean "shares the text", not "has a base". A killed indirect buffer has its own empty text.

### S3 — Case, syntax and category tables as BVAR fields (about 400 lines)
- **Move:** six `Cell<Value>` fields on BufferValue, outside the RefCell. Delete `BufferCharTables` ×2 and `BufferCaseTables`, and the holder installs of set_standard_case_tables (coding.rs:1308-1324).
- **Creation and kill:** fill them with the standard tables, as reset_buffer_local_variables does (G/buffer.c:1119).
- **Collector and dumper:** trace them in the `Kind::Buffer` arm (eval.rs:4953). In the same commit, delete the root marks (eval.rs:5735-5745) and the FIELDS_NOT_CARRIED rows (dump_roots.rs:129-142). The dumper reads the fields; write nil for the standard table to keep the image format, or accept a format change (the fingerprint forces a rebuild).
- **Effect:** looking-at ≈ −25, string-match ≈ −25.
- **Risks:**
  - A table now reachable only through a buffer is a use-after-free if the arm misses it. T9 extends to "every Lisp word traced".
  - Killed buffers.
  - The `syntax-table`/`set-syntax-table` paths and `with-syntax-table` (eval_02.rs:2784).

### S4 — case-fold-search and parse-sexp-lookup-properties as forwarded C words (about 250 lines)
- **Move:** add both to the forwarded evaluator cells (variables.rs:26-45, `refresh_forwarded_eval_cells_after_switch`). `case_fold_search_active` (variables.rs:2297) and `setup_syntax` (search.rs:623) read the word, as search.c reads Vcase_fold_search.
- **Effect:** looking-at ≈ −95, string-match ≈ −45. with-current-buffer gains a few instructions only in buffers that bind them.
- **Risks:**
  - `let` in one buffer with an unbind in another (`SpecialBindingScope::BufferLocal`), set-default, makunbound, watchers, thread switches (thread_context.rs).
  - The image: list the words in dump_roots.rs as refilled after load, like undo-inhibit-record-point (dump_roots.rs:264), or `interpreter_fields_are_carried_or_documented` fails.

### S5 — The text as buffer_text (three commits of about 550, 450 and 350 lines)
- **5a own_text inline:**
  - `Rc<RefCell<BufferText>>` (S/buffer.rs:121) becomes `own_text` in the object plus a `text` pointer to its own or the base's.
  - GapData's gap fields move into own_text's gpt, gpt_byte, z, z_byte and gap_size; the bytes stay an `Rc` allocation for sharing.
  - The caches are boxed (§4 target).
  - Mechanical inside buffer.rs: 147 `text.borrow()` and 69 `text.borrow_mut()`.
- **5b positions with their bytes:** pt_byte, begv_byte and zv_byte as fields, kept where GNU keeps them: SET_PT_BOTH, insdel's adjustments, narrowing, record/fetch_buffer_markers (buffer.c:756-797). An ASCII fast path when z == z_byte.
- **5c readers:** `BufferSnapshot::take` (search.rs:1044) and `match_view` read the fields directly. The matcher shares the bytes through the Rc only when it can run Lisp (parse-sexp-lookup-properties set). Otherwise it uses a new `text_halves_unguarded` with `bytes_unguarded`'s contract (R/alloc/strings.rs:130-142): no edit, no Lisp and no collection while the slices are in use. char-after and `char_at` read through the fields.
- **Effect:** looking-at −140..160 more; matching about −40 more (set_buffer_search_regs); char-after 443.6 → ≈300; goto-char up to +10 in non-ASCII text.
- **Risks:**
  - The indirect `text` pointer points into the base object. The base outlives its indirect buffers because kill-buffer kills them first (files_process.rs:3444) and mark_buffer marks base_buffer.
  - buffer-swap-text: GNU refuses indirect buffers; swap the own_text contents and fix the markers (marker.rs:220).
  - The image copier's clone (eval.rs:4286), the loader's text fixup, the edit log and serials.
  - A Lisp callout that edits the text mid-match must copy, not write in place.
  - These stored bytes are UTF-8 plus the marker form, not GNU's. `position-bytes` keeps its conversion (§5.12).

### S6 — The C slots and Lisp fields as fields; the RefCell<Buffer> dissolves (five commits)
- **6a scalars:** modtime, modtime_size, auto_save_modified, auto_save_failure_time, last_window_start, window_count and the four bits become Cells (about 250 lines).
- **6b Lisp fields:**
  - The 10 Lisp fields now on `Buffer`, the pt/begv/zv markers and `mark_` become `Cell<Value>` words in GNU's order.
  - The trace becomes the generic walk over the Lisp words plus intervals, overlays, base and undo (alloc.c:6950). It replaces Buffer::visit_lisp_values (buffer.rs:2523). About 400 lines.
- **6c always-local slots:** the 14 FIXED slots leave LocalCells for fields, and enable_multibyte_characters becomes a Lisp field. `killed_locals` is deleted together with its `#[allow]` (buffer.rs:152): a killed buffer keeps its fields, as GNU's does after reset_buffer_local_variables (b, 1). About 450 lines.
- **6d local_var_alist:**
  - LocalCells moves from `buffer_locals` (eval.rs:7024) into the object; its FIELDS_NOT_CARRIED row (dump_roots.rs:129) and root loops (eval.rs:5667, 5698) move into the buffer's trace in the same commit.
  - `LoadedBindings` keys by the buffer's address. The shape must advance when a buffer is reclaimed, because addresses are reused (local_cells.rs:83-89 only reshapes on a non-empty drop).
  - The anti-cheat's `holds_objects` names `BufferLocalBindings` (anti_cheat.rs:1668).
  - About 400 lines.
- **6e the rest:**
  - Overlays and intervals sit behind GNU's pointer words.
  - The 773 `.buffer.borrow()` and 222 `.borrow_mut()` sites become direct calls, mechanically, one family per commit.
- **Effect:** about −5 per buffer access (the RefCell flag); buffer-name 405.6 → ≈360.
- **Risks:**
  - Methods that held `&mut Buffer` over nested work. Non-Copy members need a RefCell or a documented unguarded contract.
  - Every Lisp word must be traced (T9).
  - Code moved out of buffer.rs loses its `#![allow(dead_code)]`. Delete the dead code; adding an allow is H5.

### S7 — Holders keep the object (about 400 lines)
- **Move:** the §1.5 fields and `UnwindAction::CurrentBuffer` hold a `BufferRef`, or a `Value` where GNU holds a Lisp_Object.
- **Mark them where GNU marks them:** mark_specpdl (specpdl.rs:98-108), mark_threads (thread_context.rs:118-143), processes, frames and window configurations.
- **Native:** `runtime_record_unwind_current_buffer` (native_comp/runtime.rs:5806) passes the object; the native ABI is otherwise unchanged.
- **Behaviour change:** a killed buffer that only the specpdl or a parked thread holds now survives until the unwind, as in GNU. Census tests that count reclaimed buffers must expect that.

### S8 — Ids out of signatures (about 1,800-2,500 mechanical lines, one family per commit)
- **Convert to `BufferRef`:** about 177 functions and the sites of §1.3. `set_current_buffer_id` becomes `set_buffer_internal(BufferRef)`.
- **Trap:** `get_buffer_by_id(_mut)` returns None for a killed buffer, so `if let Some(mut b) = …` is silently a no-op on dead buffers today. Each of the 198 sites needs an explicit `is_live()` test where that branch mattered. Review every one.
- **Effect:** with-current-buffer ≈ 560.

### S9 — Registry and ids removed (about 500 lines)
- **Vbuffer_alist:** a Lisp alist (`buffer_alist: Value`, GNU's two conses per buffer). It is marked and written through RootSlot::BufferAlist.
- **Delete:** `inactive_buffers`, `buffer_list`, `BufferValue.id`, `next_buffer_id`, `alloc_buffer_id`, `reserve_buffer_id`, `ObjectKey::Buffer(id)` and BUFFER_ID (context.rs:43, 86, 2860). Objects are keyed by address.
- **ALL_BUFFERS:** becomes FOR_EACH_LIVE_BUFFER over each live state's alist (several interpreter states share a process). Every killed buffer's chain must be empty (runtime.rs:2706 unchains it).
- **Frames:** buffer_list and buried_buffer_list become Lisp lists. That is frame.c, and record_buffer then conses.
- **Effect:** get-buffer-create 0 → 2 conses (GNU 2); with-temp-buffer 1 → 3 (GNU 9, the rest window.c); with-current-buffer ≈ 520 (GNU 515), independent of the number of buffers.
- **Audit:** vectors.rs:1479 and 1665 lose two assertions; add at least as many (H6).

### S10 — GNU's size and census (about 300 lines)
- **Layout:** all 76 Lisp words present, `size_of::<BufferValue>() == 984`, and const offset asserts from `gdb-buffer.txt` (name_ 8, own_text 616, text 760, pt 768, base_buffer 816, local_flags 832, overlays 976, undo_list_ 984).
- **Header and census:** the header gets BUFFER_LISP_SIZE 76 and rest 47; `GNU_BUFFER_SIZE` (eval.rs:4696) is computed from the size.
- **Effect:**
  - get-buffer-create 47 → 128 vector cells (GNU 128); with-temp-buffer 59 → 140 (GNU 140).
  - make-indirect-buffer 67 → 148 (GNU 143 once the duplicate mark is fixed); census vector-slots +49 → +130 per buffer (GNU 130).
  - The B19 oracle tests now pass.
- **Precondition:** the 44 flagged per-buffer words (S11). Without S11 they would be nil words that do not hold the variables' values. Either do S11 first, or name them reserved and document them so. Decide before S10.

### S11 — Per-buffer variables as BVAR fields with local_flags (decision; large)
- **Move:** the 44 flagged variables leave LocalCells for fields read through Lisp_Buffer_Objfwd, with buffer_defaults, buffer_local_flags and buffer_permanent_local_flags.
- **Scope:** kill-all-local-variables, `buffer-local-variables` order, specbind's LET_DEFAULT, and the 44 call sites in variables.rs.
- **Effect:** no D13 or B19 gain (GNU conses nothing here). Faithfulness only.

## 5. Traps and open questions
1. **Duplicates (A3.6):** liveness three times, base three times, and three registries (inactive_buffers, buffer_list, ALL_BUFFERS). S2 and S9 remove them. Name one authority per fact in each commit.
2. **Image ids replace process buffers.** load.rs:1018-1031 then install_buffer (eval.rs:5387-5404) replaces the process's same-id buffer, the current one included; the replaced object stays `live` but unregistered. This is the D119 pattern; T5 tests it before S1. S9 removes the mechanism.
3. **A killed buffer reads as None** through every id lookup. S8's conversion must keep each "dead means skip" branch explicit.
4. **The anti-cheat misses BufferRef holders.**
   - `interpreter_value_fields_are_gc_roots_or_documented` (S/anti_cheat.rs:1608) checks only fields whose types name Value, SymbolCells, LocalCells or BufferLocalBindings (1668), so a new `BufferRef` holder that nobody marks passes. Add `BufferRef` to that list in S7 (R6 review).
   - `interpreter_roots_are_dumped_or_documented` hard-codes the exception `"buffer" | "inactive_buffers"` (1805-1806); renaming or deleting those fields edits it (R6).
   - `interpreter_fields_are_carried_or_documented` (1823): every new InterpreterState field must be read by dump_roots.rs or image_install.rs, or be listed.
5. **dump_roots.rs.** Its test (1457-1497) fails if a FIELDS_NOT_CARRIED name is no longer a field, so each deletion removes its row in the same commit. Rows: buffer_locals 129, buffer_syntax_tables 133, buffer_category_tables 137, buffer_case_tables 141, print_unreadable_callback_buffer 179, next_buffer_id 336, loaded_bindings 338, indirect_buffers 391, buffer_before_last_command_or_undo 422, code_conversion_reused_workbuf 463, buffer 485.
6. **round_audit.py:**
   - **H5:** any added `#[allow]`, `#[expect]` or `cfg_attr(…allow…)` line is HARD, including one moved with its code. The cases here are `#![allow(dead_code)]` (S/buffer.rs:1), `#[allow(clippy::type_complexity)]` (buffer.rs:152) and `cfg_attr(not(test), allow(dead_code))` (eval.rs:5186, 5386, 5482). Leave those lines untouched or delete them; never re-add one.
   - **H6:** removed tests or assertions must not outnumber added ones. Runtime `assert!` and `debug_assert!` count (vectors.rs:1479, 1665), as do the ≈70 id-using test sites.
   - **R4:** the boxed text-cache struct (name with "cache" + RefCell/Vec) and any registry are listed for review.
   - **R2:** `let _ = self.remove_buffer_id(id)` (runtime.rs:2738) and new `.unwrap_or(Value::Nil)` are listed.
   - **R6:** anti_cheat.rs and tools/perf probes are listed.
7. **Native fast paths.** The contracts for bufferp, current-buffer, point and eobp (anti_cheat.rs:678, 823) must keep their tests passing. The tag allow-list (native_comp/runtime.rs:6491) keeps Buffer at 13.
8. **Threads.** Emaxx does not restore a thread's buffer if it was killed (thread_context.rs:137-143). Check GNU's thread.c post_acquire_global_lock before S7 and decide.
9. **inhibit_shrinking.** GNU's freeze costs 140 instructions through the specpdl. The Rc copy-on-write share is the safe Rust counterpart. Keep the bit in own_text for compact_buffer and the dump; do not reproduce the specpdl unwind. Confirm this is acceptable as "structure included".
10. **B19 beyond the struct:**
    - local_var_alist as a Lisp alist (GNU: 3 conses per new setq-local);
    - frame buffer lists as Lisp lists;
    - other-buffer's window list (6 conses, window.c);
    - insert in a temp buffer (GNU 12 conses, Emaxx 1; source not found);
    - the duplicate mark marker of make-indirect-buffer;
    - generate-new-buffer-name's extra string.
    Each is its own small fix. The marker and string fixes can go before S10.
11. **Hand-built ids** in tests: `Value::buffer(52, …)` and others (§1.3). Rewrite them in S9 with balanced assertions.
12. **Encoding.** Text and strings are stored as UTF-8 with raw bytes as U+E080..E0FF and a sidecar for characters beyond Unicode (buffer.rs:415-421). z_byte and gpt_byte count those bytes, not GNU's. A GNU-encoded text is a separate item; the fields here must document which bytes they count.
13. **Out of scope:** canon_for (a char-table ASCII fast path); string_match_1/StringText (strings); intervals as allocated objects (`intervals-consed`); the regexp matcher itself (already level with GNU: 294 against 293).
14. **The image copier** (eval.rs:4251-4290, 6112-6120) relies on `Buffer::clone` being a text snapshot. With inline own_text the copy must be written out.
15. **Several interpreter states in one process.** ALL_BUFFERS is process-wide today. The sweep must reach every live state's alist.

### Critical files
- /home/user/emaxx-draft/src/buffer.rs, src/buffer/gap.rs
- /home/user/emaxx-draft/src/lisp/types.rs, src/lisp/types/marker.rs
- /home/user/emaxx-draft/src/lisp/alloc/vectors.rs (and a new vectors/buffers.rs)
- /home/user/emaxx-draft/src/lisp/eval.rs, eval/runtime.rs, eval/buffers.rs, eval/local_cells.rs, eval/variables.rs, eval/coding.rs, eval/dump_roots.rs
- /home/user/emaxx-draft/src/lisp/primitives/search.rs, primitives/pdumper/{context,load}.rs
