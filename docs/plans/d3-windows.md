# Windows as GNU's struct window (D3, A3.5): inventory and staged plan

Written on 23 November 2026 before round 369, from a reading of the tree at
round 368. Paths beginning `/home/user/` are those of the container it was
written in: `R/` is `src/lisp/` of this repository and `G/` GNU Emacs 30.2's
`src/`. Line numbers are of that tree. Stages 1a, 1b, 2a, 2b, 2c and 2d are
rounds 369, 371, 372 and 373; HANDOVER.md records what each did and found.


Read-only work: nothing was built, run or modified. Besides grep/sed, the only tool used was `gdb -batch` on the existing GNU oracle objects (`/home/user/emacs/src/window.o`) to read the real layout.

Path prefixes: `R/` = `/home/user/emaxx-draft/src/lisp/`, `G/` = `/home/user/emacs-mirror/emacs/src/`. Counts are call sites excluding the definition; "≈" marks counts I classified by reading rather than by an exact grep.

## 1. Inventory

### 1.1 Creation of window records
| Site | What |
|---|---|
| `R/eval.rs:9063-9078`, `9098-9113` | initial root/selected window and minibuffer window |
| `R/eval/terminal.rs:1137-1161` | make_frame's `make_window` closure, called twice (root, mini) |
| `R/primitives/dispatch/display.rs:3244-3252` | `make_parent_window`: clones the whole slot Vec |
| `R/primitives/dispatch/display.rs:3617-3625` | split-window's new leaf |
| `R/eval/runtime.rs:3182-3202` → `3245-3274` | `create_pseudovector` (frame slot, sequence number) → `create_record_with_kind` (allocation, id, registry, type index) |
| `R/primitives/window.rs:468-532` | `window_record_slots`, make_window's initial slots (4 callers) |
| `R/primitives/pdumper/load.rs:996-1005` → `R/eval.rs:5142-5164` | image load: `install_record` of a nilled window under the image's id |
| `R/eval.rs:4490-4515` | test-only `ImageGraphCopier` copy |
| tests | `R/primitives/tests.rs:25174`, `R/eval/runtime.rs:4135` |

### 1.2 Slot constants and readers/writers
`WINDOW_*_SLOT` 0..54 and `WINDOW_SLOT_COUNT = 55` are at `R/primitives/window.rs:27-116`; `WINDOW_MARKER_SLOTS` at `247-248`; `WindowKind` at `416-463`. The constants are used 505 times in 18 files: display.rs 239, eval/runtime.rs 116, window.rs 37, tty.rs 27, tty/redisplay_flags.rs 20, display/frame_size.rs 20, display/mode_line.rs 10, eval/terminal.rs 8, six smaller files, and tests.

| Helper family (definition) | Call sites |
|---|---|
| display.rs `window_slot_value` (1504) / `set_window_slot_value` (1512) | 34 / 38 |
| display.rs `window_link` (1532) / `set_window_link` (2986) | 43 / 35 |
| display.rs `window_geometry` (1539), `window_integer_slot` (4208) | 47, 13 |
| display.rs `window_kind_of_id` (1440), `window_is_minibuffer`, `window_is_deleted`, `window_combination`, `set_window_combination` (3000) | 6, 8, 2, 10, 6 |
| display.rs `window_parameter_value`/`_entry`/`set_` (1449-1502) | 6 |
| display.rs `window_link_value` (1389), `window_child_value` (1421), `valid_window_or_selected` (1371) | 3, 2, 3 |
| window.rs `window_flag` (204) / `set_window_flag` (211) | 14 / 30 |
| window.rs `window_count_slot` (222) / `set_` (229) | 7 / 7 |
| window.rs `window_marker` (251), `window_marker_position` (263), `set_window_marker` (292), restricted variants (306, 324) | 19, 17, 15, 7 |
| window.rs `window_char_geometry` (126) / `set_window_char_slot` (149), margins (164, 181) | 14 / 23, 2 |
| window.rs `window_buffer_id` (574), `window_slot_buffer_id` (564), `WindowKind::of` / `of_slot` | 56, 11, 8 / 6 |
| runtime.rs `set_window_slot` (2633), `wset_buffer` (966), `window_record_buffer_id` (1253), `adjust_window_count` (945), `wset_redisplay` (1044), `wset_update_mode_line` (1053), use-time fns (850-881) | 16, 10, 15, 8, 13, 4, 4 |
| terminal.rs `window_frame_id` (1003) | 26 |
| Raw `.slots[...]` / `.slots.get(..)` on a window record outside those bodies | ≈60: runtime.rs ≈25, display.rs ≈12, terminal.rs 5, completion.rs 4, eval/buffers.rs 4, buffer_edit.rs 2, and one each at buffer_meta.rs:640, mode_line.rs:1986, indent_motion.rs:73, tty.rs:2234, eval.rs:9086 |

That is about 605 helper call sites behind about 45 helper bodies, plus about 60 raw sites.

### 1.3 Value ↔ id ↔ object
| Direction | Sites |
|---|---|
| value → record: `window_record` (window.rs:548) | 12 |
| value → id: `window_record_id_from_value` 18, `live_window_id_or_selected` (display.rs:2814) 57, `valid_window_id` (2944) 12, `window_id_or_selected` (1362) 8 | 95 |
| ad hoc `Kind::Record(w)` used as a window | ≈28: completion.rs:1556, 1663, 1796, 1813, 2079; frames.rs:355, 1060; display.rs:1398, 1431, 1534, 3250, 3622, 4238, 4635, 7973; runtime.rs:1770, 1978, 2072, 2588, 3195, 3751; terminal.rs:1138; eval.rs:9079, 9114; textprop.rs:649 |
| id → value: `record_value(window_id)` (eval.rs:5180) | 85: display.rs 48, runtime.rs 13, tty.rs 4, window.rs 4, 16 elsewhere |
| id → `&RecordState`: `find_record` ≈42, `find_record_mut` ≈22 (runtime.rs:3280, 3289) | ≈64 |
| id → handle: `record_ref` | ≈12: runtime.rs:793, 805, 821, 2429; display.rs 5; eval.rs:5583; roots.rs:144 |
| handle → value: `Value::Record(w)` | ≈20: display.rs 11, eval.rs:4198, 4948, runtime.rs:800, 816, roots.rs:146, frames.rs:354 |
| id as currency | 158 `…: u64` parameters (display.rs 40, tty.rs 34, window.rs 25, move_it.rs 12, runtime.rs 11, mode_line.rs 10, buffer_edit.rs 9, redisplay_flags.rs 7); `window_id` appears ≈900 times; `selected_window_id()` 101 |

### 1.4 `RecordKind::Window` (21 uses and 2 definitions)
- **Definitions:** eval.rs:1278, 1303 (`gnu_vector_slots => 66`).
- **Creation:** eval.rs:9064, 9099; terminal.rs:1140; display.rs:3249, 3618; runtime.rs:3188, 3194.
- **Predicates:** window.rs:550; frames.rs:302; completion.rs:26; runtime.rs:2656.
- **Enumeration:** terminal.rs:1251.
- **type-of:** primitives/buffers.rs:875.
- **equal / value<:** primitives/values.rs:334, 899.
- **Printer:** print/object.rs:1910.
- **Dumper:** pdumper/context.rs:159, 183, 1752.
- **Tests:** tests.rs:25174; runtime.rs:4135.

### 1.5 Special-casing by subsystem
| Subsystem | Places |
|---|---|
| Collector | generic record mark and trace: eval.rs:4752, 4845, 4980-4986; a frame traces its four windows: 4936-4949; id-held roots: 5581-5594, 5899-5901, roots.rs:132-152; alloc.rs:2176; vectors.rs:1298, 1435-1445, 1507-1521 (freed-record note), 1579-1590, 1964; purge: eval.rs:1428-1451, 5192-5229 |
| Dumper / loader | context.rs:93 (keyed by record id), 154-195 (code 9), 1153 → 1735-1755 (nilled), 1884-1902; load.rs:959-1009, 1081-1113, 1195 → runtime.rs:2554-2571; dump_roots.rs:62, 81, 301-322, 349-354, 448-453. Windows are never mapped in place: vectors.rs:775-784 admits tags 1, 12, 31-34 only |
| Printer | print/object.rs:1830 → 1865 → 1910-1929; sequence number from the side table at 1913 |
| eq / equal / hash | `eq` is the word (types.rs:2160, 3361); `equal` values.rs:312-352; `value<` 857-908; sxhash.rs:112 → 207-236 (address) |
| type-of, predicates | buffers.rs:863-880; misc_keymaps.rs:1465-1470; display.rs:8042-8059, 8103; frames.rs:300; completion.rs:23 |
| window-configuration | runtime.rs:1690-1728, 1757-1897 (save), 1899-1923 (the record), 1944-2050, 2056-2548 (restore), 2573-2622, 3886-3917; eval.rs:3737-3752; display.rs:7646-7693 |
| Native runtime | checked-decode tag allow-list: native_comp/runtime.rs:6230-6250; tests at 15660-15815, 15878 |

### 1.6 Side tables keyed by window id
| Table | Holds |
|---|---|
| `records` (eval.rs:6887; `R/eval/record_registry.rs`) | id → RecordRef for all host records; weak, purged after sweep |
| `record_ids_by_type_index["window"]` (eval.rs:6893) | the only "all windows" enumeration; 6 users: runtime.rs:993, 1010, 2556; display.rs:1899, 1964; buffer_meta.rs:636 |
| `window_sequence_numbers` and counter (eval.rs:7072-7073) | GNU `w->sequence_number`; never purged |
| `window_cursor_visibility` (eval.rs:6750; runtime.rs:835-844) | GNU `w->cursor_off_p`; never purged |
| tty `views: HashMap<u64, WindowView>` (`/home/user/emaxx-draft/src/tty.rs:67-99, 385`) | cursor, last cursor row, last-display anchors |
| Scalars by id | `selected_window_id` (eval.rs:6744), `minibuffer_selected_window_id` (6747), `old_selected_window_id` (6752), `minibuffer_runtime.active_window_id` (4048), `WindowConfigurationSnapshot.{selected,root}_window_id` (3743-3744), window-configuration slot 1 (runtime.rs:1913), completion.rs:1399, `MotionWindow.id` (indent_motion.rs:27), `WindowRenderInfo.window_id`, `WindowIds` (display.rs:1882) |
| Already by object | `FrameState.{root,selected,minibuffer,old_selected}_window: Option<RecordRef>` (eval.rs:3916-3919), `Buffer.last_selected_window`, link slots, saved-window vectors |

## 2. GNU's layout

`struct window` is at `G/window.h:100-399`, PVEC_WINDOW = 11 (`G/lisp.h:1031`; `VectorTag` has 10 and 12, so 11 is free). The offsets below are from gdb on the oracle build, whose configure line equals `R/native_comp/generated_native_subrs_x86_64_unknown_linux_gnu.rs:11`. sizeof is 544: 25 traced Lisp words, 42 rest words, 68 words with the header.

**Lisp fields (traced, offsets 8-200)**
| GNU field | Emaxx today |
|---|---|
| frame | slot 30 |
| next / prev | slots 15 / 14 (root↔mini link synthesized, not stored) |
| parent | slot 13 |
| normal_lines / normal_cols | slots 22 / 21 |
| new_total / new_normal / new_pixel | slots 9 / 10 / 8 |
| contents | split: slot 0 (buffer), slot 16 (first child), slot 3 (kind code) |
| old_buffer | slot 28 |
| start / pointm / old_pointm | slots 1 / 29 / 2 |
| temslot | absent (an `order` Vec, runtime.rs:1762) |
| vertical_scroll_bar, horizontal_scroll_bar and their `_type`s | absent (constant t in configurations) |
| display_table / dedicated / combination_limit | slots 11 / 7 / 23 |
| window_parameters / cursor_type | slots 4 / 12 |
| mode_line_help_echo (last Lisp member) | absent |

**C fields (offsets 208-543)**
| GNU field | Emaxx today |
|---|---|
| current_matrix, desired_matrix | absent |
| prev_buffers, next_buffers (Lisp, marked by `mark_window`, `G/alloc.c:7086-7089`) | slots 5, 6 |
| use_time | slot 24 |
| sequence_number | side table |
| change_stamp | slot 31 |
| pixel_left / pixel_top | slots 19 / 20 |
| left_col / top_line | slots 41 / 42 |
| pixel_width / pixel_height | slots 17 / 18 |
| old_pixel_* and old_body_pixel_* | slots 32-35 |
| total_cols / total_lines | slots 43 / 44 |
| hscroll / min_hscroll | slots 25 / 26 |
| last_modified / last_overlay_modified / last_point | slots 52 / 53 / 51 |
| base_line_number, base_line_pos | slot 36 (a 6-element list, mode_line.rs:1858-1889) |
| column_number_displayed | slot 50 |
| cursor, phys_cursor, output_cursor, last_cursor_vpos | tty `views` |
| left_margin_cols / right_margin_cols | slots 45 / 46 |
| window_end_pos | slot 37 |
| bits mini, horizontal | slot 3 |
| bits update_mode_line, last_had_star, start_at_line_beg, force_start, optional_new_start | slots 48, 49, 40, 38, 39 |
| bit cursor_off_p | side table |
| bits window_end_valid, redisplay, suspend_auto_hscroll | slots 54, 47, 27 |
| hscroll_whole, ephemeral_last_point, last_mark, scale factors, phys_cursor_*, fringe widths, scroll_bar_*, the three `*_line_height`s, window_end_vpos, the other bits, vscroll, window_end_bytepos | absent |

Emaxx-only state: `id`, `owner`, `type_tag` (eval.rs:1380-1389) and the Deleted kind code.

## 3. Staged plan

**Target.** A new `R/alloc/vectors/windows.rs` holds `#[repr(C)] WindowValue` with GNU's 25 Lisp fields first, in GNU order, as `Cell<Value>`; `impl Vectorlike { TAG = Window (11); LISP_SLOTS = 25 }`; `WindowRef = VectorlikeRef<WindowValue>`; `Kind::Window` / `Value::Window`. The patterns are `NativeFunctionState` (native_functions.rs:9-70, with its const offset asserts) and `FrameValue` (types.rs:1027-1054, vectors.rs:1271-1274).

### Stage 1: the object, with ids and side tables kept (two commits)

**1a — funnel; behaviour-neutral, about 500 lines.**
- In window.rs add a newtype `WindowRef(RecordRef)` with `slot(i)`, `set_slot(i, v)`, `id()`, `kind()`, `value()`.
- Add `Interpreter::{window_ref(id), window_value(id), make_window(..)}`.
- Convert through them: the 5 creation sites, the ≈60 raw slot sites, the ≈45 helper bodies, the ≈160 id → object sites (§1.3), the ≈28 ad hoc `Kind::Record` matches, the ≈20 `Value::Record(w)`, and FrameState's four fields.
- Three commits by file group: display.rs and display/*; eval/*; the rest including tty.
- No expected test output changes.

**1b — switch the representation; about 700 lines.**
- **Allocator:** `VectorTag::Window`, `from_word` (vectors.rs:90-139), census (1399-1449, 1546-1594), `cleanup_vector` (1456; it must note the freed id as 1507-1521 does), `value_of` (1940).
- **Types:** the `Kind` arms at types.rs:2061-2133, 2180-2210, 3241, 3358, 3454.
- **Collector:** eval.rs:4729, 4824, 4854 (through one `WindowValue::visit_lisp_values`, as `TerminalRef` does at types.rs:1120), 4948, alloc.rs:2159.
- **Registry:** a second, weak `windows` registry (make `RecordRegistry` generic), purged in `purge_freed_records`; list the new field in dump_roots.rs:349-354.
- **Copier:** eval.rs:4164, 6201.
- **Dumper / loader:** `DumpType::Window` (code 12 is free, image.rs:268-318) written by `dump_nilled_pseudovec` as Frame is (context.rs:91, 657, 1167); the loader calls a new `install_dead_window` (pattern eval.rs:5371, load.rs:1058-1065).
- **Printer, type-of, cl-type-of, equal, value<, purecopy, core.rs:495:** one arm each.
- **Native runtime:** add the tag to native_comp/runtime.rs:6230-6250.
- **Slots:** all 55 stay `Cell<Value>` words in the object. 21 are GNU-named fields; 34 sit in a transitional array, reached by `slot(i)` through a const offset table.
- **Accessors:** the eleven arms (display.rs:7789, 8009, 8030-8072) decode the argument once and read named fields. Cache `selected_window: WindowRef` in `set_selected_window_id` (runtime.rs:791) so a nil argument is direct too.

**Risks of 1b.**
- An untraced cell or a missed purge is a use-after-free. The window's three markers are held only by the window.
- The image format changes. The fingerprint already forces a rebuild (`tools/build-image.sh:33-35`).
- The native ABI is unchanged: no subr name or arity changes and the generated tables are untouched.

**Tests for stage 1.**
- Groups: `lisp::primitives::tests::` (35 window-named), `eval_01` (16), `eval_03` (37), `eval_05` (5), `tty::` (13), `compat_runtime_tests`.
- `/home/user/emaxx-draft/tests/cli_parity.rs`: 2950, 3149, 4291, 4321, 4352, 10785.
- native_comp/runtime.rs: 15641, 15781, 15844.
- Gates: `tools/grouped_gate.py`, `tools/dumped_startup_gate.py`, `tools/terminal_frame_gate.py`, `tools/round_audit.py`.
- Instructions: `tools/perf/cg-table.py` with probes `windows:100000:300000` and `native-window`.
- New tests after alloc/vectors/tests.rs:131 and 195: header tag 11 and lisp size 25; every cell traced; an unreachable split tree reclaimed and gone from the registry; dump and load of a variable holding the selected window.
- Tests to rewrite: tests.rs:25163-25197, 30834, 30921; runtime.rs:4127-4143; compat_01.rs:1429; native_comp/runtime.rs:15660-15815, 15878.

### Stage 2: GNU field semantics, one helper family per commit (100-300 lines each)
- **2a** `contents`, `mini` and `horizontal` replace slots 0, 16 and 3; deleted means contents nil. Writers: `wset_buffer` 10, `set_window_combination` 6, kind writes 11.
- **2b** `sequence_number`, `use_time` and `cursor_off_p` move into the object; drop the two HashMaps.
- **2c** geometry ints: `window_geometry` 47, char geometry 14 / 23, margins.
- **2d** redisplay bits and counters: flags 14 / 30, counts 7 / 7, base line, end pos, hscroll.
- **2e** delete the `WINDOW_*_SLOT` constants and the shim; assert GNU's offsets and size; census by `nbytes`.

Risk: "absent" slots that read as nil today need GNU's sentinels (column_number_displayed −1, the total_cols fallback at window.rs:126-146).

### Stage 3: holders keep the object (about 250 lines)
The scalar ids of §1.6, the snapshot, and configuration slot 1 become `WindowRef`. Roots at eval.rs:5581-5594, 5899 and roots.rs:144 become direct marks.

### Stage 4: flip the currency (about 1,500-2,000 mechanical lines, one file per commit)
The 158 `u64` parameters, `WindowIds`, `WindowRenderInfo`, `MotionWindow` and the tty `views` key become `WindowRef`.

### Stage 5: remove ids (about 300 lines, mostly deletions)
Frame-tree walks replace the 7 enumerations. Delete `id`, `owner`, the registry, the freed note, `RecordKind::Window` and codec 9. `WindowConfiguration` (PVEC 17) stays a record; it is a separate item.

Lisp threads: `R/eval/thread_context.rs:28-58` exchanges the specpdl and handlers only. The one window datum there is a snapshot on the specpdl, covered in 1a and stage 3.

## 4. Open questions and traps

1. **Image ids replace live windows (D119).** load.rs:999-1005 installs a dumped window under the dumping process's id, and eval.rs:5138-5162 replaces whatever the registry held. The loading process's own initial windows got the same ids from the same boot. The frame keeps its object but `selected_window_value()` (runtime.rs:783) then answers the nilled one, whose frame slot is nil; that fits the panic at display.rs:7810 / 7929. This is from reading, not from a run. 1b removes the mechanism; keep D119's probe as its test.
2. **A nil kind slot means leaf** (window.rs:439-447), so a zeroed window looks valid. `install_dead_window` and any blank constructor must set Deleted explicitly until 2a.
3. **Deleted windows.** The buffer is squirreled in combination_limit (runtime.rs:2602-2613, restored at 2206-2216). `clear_deleted_window` (display.rs:3020-3030) and `retire_frame` (terminal.rs:1249-1271) clear links that GNU leaves dangling.
4. **Minibuffer window.** make_frame's root↔mini `next`/`prev` are not stored. They are synthesized on read, for the selected frame only (display.rs:1402-1415). `save_window_save` (runtime.rs:1757-1897, whose comment assumes the link) and `unshow_window_tree` (2579) read the raw slot and never reach the mini window. Keep the fallback in 1b; storing the links changes 43 `window_link` walkers and is a separate decision.
5. **Configurations.**
   - Windows are held as objects in each saved vector (runtime.rs:1781) and slot 5, but the selected window is an integer id in slot 1 (1913), re-read at 1959 and passed to `record_value` at 2506. That panics if the id is gone (eval.rs:5180-5185).
   - Restore identifies windows by `.id` (2069-2075).
   - Slots are copied by value (1793-1826) and markers as fresh copies (1863-1880).
   - `make_parent_window` shares the marker objects and then nils them (display.rs:3244-3273), as GNU's memcpy does.
6. **Borrows.** `find_record` ties `&RecordState` to `&self` (runtime.rs:3280), so it cannot cross a Lisp call. The exposure is `find_record_mut`'s `&mut` (3289-3295) while a `RecordRef` deref is live: `valid_window_or_selected` returns one (display.rs:1371; used at 1396, 1428, 8032). `save_window_save` also holds the borrow across allocations (1765-1891). `Cell` fields end both, and the 13+ `slots.resize` calls go away.
7. **Identity.** `eq` and hashing never use `id`; only Rust-side comparisons, the registry and the dumper's object key (context.rs:93) do. Ids are per interpreter (`owner`), so the purge must run in every live state, as eval.rs:1428-1451 does.
8. **Footprint.** The oracle has 68 words with the header (544 bytes); eval.rs:1301-1303 and tests.rs:25163-25196 pin 66. Decide which number the census reports before 2e.
9. **Hand-built windows** with an integer in the buffer slot (window.rs:564-570, 579; runtime.rs:1788, 2078): find the tests that build them before `contents` is typed.
10. **Anti-cheat manifest.** A new Interpreter field must appear in dump_roots.rs or the gate fails.
11. **Out of scope.** GNU's `mark_window` prunes killed buffers from prev/next_buffers; Emaxx has no counterpart.

### Critical Files for Implementation
- /home/user/emaxx-draft/src/lisp/alloc/vectors.rs
- /home/user/emaxx-draft/src/lisp/primitives/window.rs
- /home/user/emaxx-draft/src/lisp/primitives/dispatch/display.rs
- /home/user/emaxx-draft/src/lisp/eval/runtime.rs
- /home/user/emaxx-draft/src/lisp/eval.rs