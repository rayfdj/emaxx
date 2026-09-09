//! pdumper.c:dump_roots for the interpreter's remaining root groups (D11b).
//!
//! GNU's static roots are the staticpro'd `Lisp_Object' slots; most of the
//! state Emaxx keeps in typed tables (coding systems, charsets, timers,
//! fontsets, ...) is a Lisp object in GNU (a vector in a hash table, an
//! alist, a list of vectors).  Each group here is written in that shape --
//! a synthesized Lisp value under its own root slot -- so the image writer
//! needs no record kind per table, and the loader reads the value back
//! and reinstalls the table.  A group GNU resets after a load
//! (`PDUMPER_RESET_LV', the `*_for_pdumper' hooks, `init_buffer',
//! `init_process_emacs') is not written; `ROOTS_RESET_AFTER_LOAD' names
//! each with the C line, and the anti-cheat gate requires every root the
//! mark phase visits to be either written here or listed there.

use super::*;
use crate::lisp::primitives::pdumper::image::RootSlot;

/// The root groups the mark phase visits that the image does not carry,
/// each with the GNU reason: the state is re-created after a load, or is
/// the running thread's transient state, which pdumper never dumps.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const ROOTS_RESET_AFTER_LOAD: &[(&str, &str)] = &[
    (
        "frame_states",
        "frame.c:init_frame_once_for_pdumper resets Vframe_list and selected_frame; frames are nilled in the image, their windows and face hash tables with them (window.c:init_window_once_for_pdumper)",
    ),
    (
        "terminals",
        "the terminal pseudovectors are nilled (pdumper.c:dump_vectorlike PVEC_TERMINAL); init_tty makes the initial terminal anew, its parameters, codings and keyboard with it",
    ),
    (
        "pending_funcalls",
        "keyboard.c:syms_of_keyboard_for_pdumper resets pending_funcalls to nil",
    ),
    (
        "selected_window_id",
        "window.c:init_window_once_for_pdumper resets selected_window",
    ),
    (
        "kbd_macro_definition",
        "a KVAR of the kboard, which is not in the image: keyboard.c:syms_of_keyboard_for_pdumper allocates initial_kboard anew",
    ),
    (
        "kbd_macro_executions",
        "kboard state, as kbd_macro_definition",
    ),
    (
        "process_states",
        "process.c:init_process_emacs sets Vprocess_alist to nil after a load; process pseudovectors are nilled",
    ),
    (
        "thread_states",
        "thread.c: only the main thread exists at dump time (Fdump_emacs_portable refuses otherwise) and it is runtime magic",
    ),
    (
        "file_notify_watches",
        "inotify.c: watch_list is reset when the descriptor is first opened in the new process",
    ),
    (
        "pending_file_notifications",
        "queued events of the old process's descriptors, as file_notify_watches",
    ),
    (
        "active_special_restores",
        "eval.c:init_eval_once_for_pdumper allocates a fresh specpdl; the dump runs inside its own specbind",
    ),
    (
        "active_handlers",
        "the main thread's handlerlist is per-thread runtime state, not dumped",
    ),
    (
        "active_catch_tags",
        "catch tags live on the handlerlist, not dumped",
    ),
    (
        "backtrace_frames",
        "the specpdl's backtrace entries, not dumped",
    ),
    (
        "batch_error_backtrace",
        "a rendering of the specpdl kept for the batch error report, not dumped",
    ),
    (
        "doomed_finalizers",
        "empty after Fdump_emacs_portable's collection loop; the writer refuses otherwise",
    ),
    (
        "plain_quote_templates",
        "a cache over quoted constants, rebuilt on demand (the constants themselves are reached through the code)",
    ),
    (
        "char_tables",
        "the registry of char-table objects: each is written when a root or object reaches it, as GNU's heap objects are",
    ),
    (
        "globals",
        "the symbol cells: written per symbol from the obarray (dump_symbol)",
    ),
    (
        "symbol_properties",
        "written per symbol (dump_symbol's plist)",
    ),
    (
        "variable_watchers",
        "written per symbol (dump_symbol's watchers)",
    ),
    (
        "functions",
        "written per symbol (dump_symbol's function cell)",
    ),
    (
        "buffer_locals",
        "written per buffer (dump_buffer's local_var_alist)",
    ),
    (
        "buffer_local_hooks",
        "written per buffer with the local bindings",
    ),
    (
        "standard_obarray_id",
        "the obarray root (RootSlot::Obarray)",
    ),
    (
        "stack_roots",
        "the running Rust frames' registered Lisp roots (GNU's stack scan of the main thread): per-thread runtime state, empty once the dump returns",
    ),
];

/// The interpreter's remaining fields, none of them a Lisp root, each
/// with why the image does not carry it: GNU re-creates the state after
/// a load, or it is the running process's transient state, or a cache
/// or index rebuilt from carried state.  The anti-cheat gate
/// `interpreter_fields_are_carried_or_documented' requires every field
/// of `Interpreter' to be written, installed, or listed here.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const FIELDS_NOT_CARRIED: &[(&str, &str)] = &[
    (
        "state",
        "the shell's owned editor payload (InterpreterState), whose fields this inventory covers",
    ),
    (
        "continuations",
        "the suspended Lisp thread stacks: only the main thread exists at dump time (Fdump_emacs_portable refuses otherwise), so there are none",
    ),
    (
        "new_thread_continuations",
        "never-started thread stacks awaiting the driving shell: none at dump time, as continuations",
    ),
    (
        "image_template_token",
        "the test harness's template ownership, not process state",
    ),
    (
        "max_lisp_eval_depth",
        "the forwarded C slot of max-lisp-eval-depth, refilled from the symbol's value on install",
    ),
    (
        "debug_on_next_call",
        "the forwarded C slot of debug-on-next-call, refilled likewise",
    ),
    (
        "symbols_with_positions_enabled",
        "the forwarded C slot of symbols-with-pos-enabled, refilled likewise",
    ),
    (
        "local_special_names",
        "eval.c's specpdl-scoped local specials of the running evaluation",
    ),
    (
        "dlet_active_names",
        "dynamic bindings of the running evaluation",
    ),
    ("special_scan_floor", "an evaluation-scoped scan position"),
    ("lisp_eval_depth", "eval.c:init_eval resets lisp_eval_depth"),
    (
        "garbage_collection_inhibited",
        "the running collection's inhibit count",
    ),
    (
        "kbd_macro_committed_len",
        "kboard state (keyboard.c:syms_of_keyboard_for_pdumper allocates initial_kboard anew)",
    ),
    (
        "command_loop_recursion_depth",
        "minibuf.c:init_minibuf_once_for_pdumper resets the command loop state",
    ),
    (
        "minibuffer_runtime",
        "minibuf.c:init_minibuf_once_for_pdumper resets minibuffer state",
    ),
    ("minibuffer_activation_count", "minibuffer state, as above"),
    (
        "external_debugging_output_target",
        "the running process's debug output",
    ),
    ("native_compiler", "the native units are D14/D15"),
    (
        "default_file_modes",
        "emacs.c:init_callproc_1 takes the umask of the new process",
    ),
    (
        "symbol_properties_index",
        "an index over symbol_properties, rebuilt by set_symbol_plist",
    ),
    (
        "interned_symbols",
        "filled by install_image in the obarray's order",
    ),
    ("interned_symbol_names", "the set over interned_symbols"),
    ("keymap_public_cons_owners", "a cache over keymap records"),
    ("keymap_public_cons_ids", "a cache over keymap records"),
    (
        "minibuffer_selected_window_id",
        "window.c:init_window_once_for_pdumper resets minibuf_selected_window",
    ),
    (
        "window_cursor_visibility",
        "window state re-created with the initial frame",
    ),
    (
        "old_selected_window_id",
        "window.c:init_window_once_for_pdumper resets old_selected_window",
    ),
    (
        "window_select_count",
        "window state re-created with the initial frame",
    ),
    (
        "selected_frame_id",
        "frame.c:init_frame_once_for_pdumper resets selected_frame",
    ),
    ("old_selected_frame_id", "frame state, as selected_frame_id"),
    (
        "killed_buffer_file_names",
        "Emaxx's memory of killed buffers' file names; a killed buffer's filename is nil in GNU",
    ),
    ("next_buffer_id", "carried in the remembered scalars"),
    ("next_overlay_id", "carried in the remembered scalars"),
    ("next_marker_id", "carried in the remembered scalars"),
    ("markers", "installed per marker record"),
    ("markers_by_buffer", "the index install_marker keeps"),
    (
        "buffer_mark_marker_ids",
        "the relation install_marker keeps",
    ),
    ("char_table_mutation_generation", "a cache generation"),
    ("regexp_syntax_class_cache", "a cache"),
    ("syntax_segment_cache", "a cache"),
    ("equal_hash_tables", "thawed from the hash list"),
    (
        "custom_hash_tables",
        "user-defined tests are refused by the writer",
    ),
    (
        "hash_tables_under_test",
        "the running comparison's critical section",
    ),
    ("immutable_hash_tables", "restored by the thaw"),
    (
        "safe_terminal_coding",
        "terminal state, re-created by init_tty",
    ),
    (
        "input_interrupt_mode",
        "keyboard.c's interrupt mode is set at terminal init",
    ),
    ("buffer_case_tables", "written per buffer record"),
    ("next_char_table_id", "carried in the remembered scalars"),
    ("records", "installed per record"),
    ("record_ids_by_type_index", "the index install_record keeps"),
    ("gc_live_record_ids", "the last collection's census"),
    ("gc_record_high_water", "the last collection's census"),
    ("gc_has_record_census", "the last collection's census"),
    ("vm_stack_pool", "released bytecode stacks"),
    ("backtrace_args_pool", "released argument vectors"),
    (
        "sqlite_handles",
        "sqlite objects are refused by the writer (OS handles)",
    ),
    (
        "treesit_queries",
        "tree-sitter objects are refused by the writer",
    ),
    (
        "treesit_parsers",
        "tree-sitter objects are refused by the writer",
    ),
    (
        "treesit_nodes",
        "tree-sitter objects are refused by the writer",
    ),
    (
        "treesit_languages",
        "tree-sitter languages are loaded libraries of the process",
    ),
    ("next_record_id", "carried in the remembered scalars"),
    ("next_finalizer_id", "carried in the remembered scalars"),
    ("finalizer_functions", "installed from the finalizer chain"),
    ("finalizers_run", "carried in the remembered scalars"),
    (
        "image_reconstruction_handoff",
        "the startup reconstruction's flag",
    ),
    ("buffer_syntax_tables", "written per buffer record"),
    (
        "next_special_binding_id",
        "carried in the remembered scalars",
    ),
    ("indirect_buffers", "written as each buffer's base"),
    ("change_hooks_running", "a running change hook's guard"),
    (
        "network_connect_counter",
        "the running process's connections",
    ),
    ("definition_generation", "a cache generation"),
    ("not_macro_names", "a cache"),
    ("lambda_source_bodies", "a cache"),
    ("current_load_file", "nil at top level, where the dump runs"),
    (
        "load_source_provenance_remap",
        "a process option of the harness",
    ),
    ("ert_test_source_file", "the running ERT session"),
    ("current_ert_test_name", "the running ERT session"),
    ("test_results", "the running ERT session"),
    ("last_selected_tests", "the running ERT session"),
    ("pending_termination", "the running process's exit"),
    (
        "last_match_data",
        "search.c's search_regs are the running process's",
    ),
    ("last_match_data_buffer_id", "as last_match_data"),
    ("profiler_memory_running", "the running profiler"),
    ("profiler_memory_log_pending", "the running profiler"),
    ("profiler_cpu_running", "the running profiler"),
    ("profiler_cpu_log_pending", "the running profiler"),
    ("message_capture_stack", "the running ERT session"),
    (
        "batch_standard_output_last_char",
        "the running process's stdout",
    ),
    ("batch_stdout_need_newline", "the running process's stdout"),
    (
        "print_number_index",
        "print.c's print_number_index is per print",
    ),
    ("current_activation_id", "the running evaluation"),
    ("next_activation_id", "the running evaluation"),
    (
        "closure_capture_cache",
        "weak owners, re-registered as closures load",
    ),
    (
        "captured_lexical_frames",
        "weak owners, re-registered as closures load",
    ),
    ("captured_env_registrations", "a registration counter"),
    (
        "closure_eval_contexts",
        "weak owners of running evaluations",
    ),
    (
        "closure_eval_context_registrations",
        "a registration counter",
    ),
    ("lossage_size", "carried in the remembered scalars"),
    ("interactive_call_depth", "the running command"),
    ("next_lisp_face_id", "carried in the remembered scalars"),
    (
        "tty_suppress_bold_inverse_default_colors",
        "tty state set at terminal init",
    ),
    ("builtin_doc_offsets", "carried in the remembered scalars"),
    ("require_nesting", "the running load"),
    ("lambda_capture_overrides", "the running evaluation"),
    ("mutex_states", "mutex objects are refused by the writer"),
    (
        "condition_variables",
        "condition variable objects are refused by the writer",
    ),
    ("combined_after_change", "the running change group"),
    ("timer_callback_depth", "the running timer"),
    ("file_notify_kqueue", "an OS descriptor of the process"),
    ("file_notify_inotify", "an OS descriptor of the process"),
    (
        "main_thread_id",
        "the main thread is the running process's (runtime magic)",
    ),
    ("active_thread_id", "as main_thread_id"),
    ("handler_dispatch_depth", "the running signal dispatch"),
    ("dispatched_signal", "the running signal dispatch"),
    (
        "suspend_condition_case_count",
        "the running signal dispatch",
    ),
    (
        "window_margins",
        "window state re-created with the initial frame",
    ),
    ("face_change_count", "carried in the remembered scalars"),
    ("special_variables", "carried in the remembered scalars"),
    ("pdumper_loaded", "set by the load itself"),
    ("buffer", "written through the buffer alist"),
];

/// The transient groups that must be empty at dump time: they belong to
/// an evaluation in progress, which Fdump_emacs_portable is not inside
/// of beyond its own frame.
pub(crate) const TRANSIENT_ROOTS: &[&str] =
    &["pending_thread_events", "deferred_defsubst_unbindings"];

fn opt_string(value: Option<&str>) -> Value {
    value.map_or(Value::Nil, Value::string)
}

fn opt_symbol(value: Option<&str>) -> Value {
    value.map_or(Value::Nil, Value::symbol)
}

fn symbol_list<'a>(names: impl IntoIterator<Item = &'a String>) -> Value {
    Value::list(names.into_iter().map(|name| Value::symbol(name)))
}

fn bool_value(flag: bool) -> Value {
    if flag { Value::T } else { Value::Nil }
}

fn pair(car: Value, cdr: Value) -> Value {
    Value::cons(car, cdr)
}

impl Interpreter {
    /// The static roots beyond `dump_root_values''s single slots, the
    /// obarray and the finalizer heads: each group as the Lisp value GNU
    /// keeps for it.
    pub(crate) fn dump_root_groups(&self) -> Vec<(RootSlot, Value)> {
        let mut groups = Vec::new();
        // buffer.c: Vbuffer_alist, an alist of (name . buffer).  The
        // current buffer is not a root: init_buffer selects *scratch*
        // after a load (emacs.c:main).
        groups.push((
            RootSlot::BufferAlist,
            Value::list(self.buffer_list.iter().filter_map(|(id, name)| {
                self.buffer_value(*id)
                    .map(|buffer| pair(Value::string(name), buffer))
            })),
        ));
        // keyboard.c: this_command_keys, raw_keybuf and recent_keys are
        // staticpro'd vectors (syms_of_keyboard); the kboard's
        // internal_last_event_frame is reset (syms_of_keyboard_for_pdumper).
        groups.push((
            RootSlot::ThisCommandKeys,
            Value::vector(self.keyboard_input.command_keys.iter().cloned()),
        ));
        groups.push((
            RootSlot::RawKeybuf,
            Value::vector(self.keyboard_input.raw_keys.iter().cloned()),
        ));
        groups.push((
            RootSlot::RecentKeys,
            Value::vector(self.keyboard_input.recent_keys.iter().cloned()),
        ));
        // lread.c:defvar_lisp static-protects each C slot: the value a
        // forwarded variable's C slot holds after the symbol let go of it.
        let mut detached = self.detached_forwarded_variables.iter().collect::<Vec<_>>();
        detached.sort_by(|a, b| a.0.cmp(b.0));
        groups.push((
            RootSlot::DetachedForwardedVariables,
            Value::list(
                detached
                    .into_iter()
                    .map(|(name, value)| pair(Value::symbol(name), value.clone())),
            ),
        ));
        // charset.c: charset_table (dump_charset_table) and
        // Vcharset_hash_table hold each charset's attributes; the ordered
        // list, `charset-list', Viso_2022_charset_list, the aliases,
        // iso_charset_table (remembered scalar) and the non-preferred head
        // are their own roots.
        groups.push((
            RootSlot::Charsets,
            Value::list(self.charset_ids.iter().map(|(name, id)| {
                let plist = self
                    .charset_plists
                    .iter()
                    .rev()
                    .find(|(registered, _)| registered == name)
                    .map(|(_, plist)| plist.clone())
                    .unwrap_or(Value::Nil);
                Value::vector([
                    Value::symbol(name),
                    Value::Integer(*id),
                    plist,
                    bool_value(self.charset_supplementary.contains(name)),
                    bool_value(self.charset_unified.contains(name)),
                ])
            })),
        ));
        groups.push((
            RootSlot::CharsetOrderedList,
            symbol_list(&self.charset_priority),
        ));
        groups.push((RootSlot::CharsetList, symbol_list(&self.charset_names)));
        groups.push((
            RootSlot::Iso2022CharsetList,
            symbol_list(&self.iso_2022_charset_list),
        ));
        groups.push((
            RootSlot::CharsetAliases,
            Value::list(
                self.charset_aliases
                    .iter()
                    .map(|(alias, target)| pair(Value::symbol(alias), Value::symbol(target))),
            ),
        ));
        groups.push((
            RootSlot::IsoCharsetTable,
            Value::list(
                self.iso_charsets
                    .iter()
                    .map(|(dimension, chars, final_char, name)| {
                        Value::vector([
                            Value::Integer(*dimension),
                            Value::Integer(*chars),
                            Value::Integer(i64::from(*final_char)),
                            Value::symbol(name),
                        ])
                    }),
            ),
        ));
        // charset.c's Vcharset_non_preferred_head is not staticpro'd: a
        // loaded session starts with it nil, whatever loadup left there.
        groups.push((RootSlot::CharsetNonPreferredHead, Value::Nil));
        groups.push((
            RootSlot::SjisCodingSystem,
            Value::symbol(&self.sjis_coding_system),
        ));
        groups.push((
            RootSlot::Big5CodingSystem,
            Value::symbol(&self.big5_coding_system),
        ));
        // coding.c: Vcoding_system_hash_table's attribute vectors,
        // Vcoding_system_alist's aliases, coding_priorities and the
        // coding_categories representatives.
        groups.push((
            RootSlot::CodingSystems,
            Value::list(self.coding_systems.iter().map(|coding| {
                Value::vector([
                    Value::symbol(&coding.name),
                    Value::symbol(&coding.base),
                    Value::symbol(&coding.kind),
                    coding.eol_type.map_or(Value::Nil, Value::Integer),
                    coding.plist.clone(),
                    Value::Integer(coding.category as i64),
                    coding.charset_list.clone(),
                    Value::Integer(i64::from(coding.default_char)),
                    Value::vector(coding.type_args.iter().cloned()),
                ])
            })),
        ));
        groups.push((
            RootSlot::CodingAliases,
            Value::list(
                self.coding_aliases
                    .iter()
                    .map(|(alias, target)| pair(Value::symbol(alias), Value::symbol(target))),
            ),
        ));
        groups.push((RootSlot::CodingPriority, symbol_list(&self.coding_priority)));
        groups.push((
            RootSlot::CodingCategoryRepresentatives,
            Value::vector(
                self.coding_category_representatives
                    .iter()
                    .map(|name| opt_symbol(name.as_deref())),
            ),
        ));
        groups.push((
            RootSlot::CodingCategoryPriorities,
            Value::vector(
                self.coding_category_priorities
                    .iter()
                    .map(|category| Value::Integer(*category as i64)),
            ),
        ));
        // ccl.c: Vccl_program_table, a vector of [name program ...] or nil.
        groups.push((
            RootSlot::CclProgramTable,
            Value::vector(self.ccl_programs.iter().map(|entry| match entry {
                Some((name, program)) => pair(Value::symbol(name), program.clone()),
                None => Value::Nil,
            })),
        ));
        // buffer_defaults' BVARs (remembered data in GNU): the standard
        // syntax, category and case tables; casetab.c's ASCII tables.
        groups.push((
            RootSlot::StandardSyntaxTable,
            Value::CharTable(self.standard_syntax_table_id),
        ));
        groups.push((
            RootSlot::StandardCategoryTable,
            self.standard_category_table_id
                .map_or(Value::Nil, Value::CharTable),
        ));
        groups.push((
            RootSlot::StandardCaseTable,
            self.standard_case_table_id
                .map_or(Value::Nil, Value::CharTable),
        ));
        groups.push((
            RootSlot::AsciiCaseTables,
            Value::vector(
                self.ascii_case_table_ids
                    .iter()
                    .map(|id| Value::CharTable(*id)),
            ),
        ));
        groups.push((
            RootSlot::SyntaxWordChars,
            Value::vector(
                self.syntax_word_chars
                    .iter()
                    .map(|code| Value::Integer(i64::from(*code))),
            ),
        ));
        // fontset.c: Vfontset_table; each fontset's mappings.
        groups.push((
            RootSlot::Fontsets,
            Value::list(self.fontset_states.iter().map(|fontset| {
                Value::vector([
                    Value::string(&fontset.name),
                    Value::list(fontset.mappings.iter().map(|mapping| {
                        let target = match &mapping.target {
                            FontsetTargetState::Character(code) => {
                                Value::list([Value::symbol("character"), Value::Integer(*code)])
                            }
                            FontsetTargetState::Range(from, to) => Value::list([
                                Value::symbol("range"),
                                Value::Integer(*from),
                                Value::Integer(*to),
                            ]),
                            FontsetTargetState::Script(script) => {
                                Value::list([Value::symbol("script"), Value::string(script)])
                            }
                            FontsetTargetState::Fallback => {
                                Value::list([Value::symbol("fallback")])
                            }
                        };
                        let patterns =
                            Value::list(mapping.patterns.iter().map(|pattern| match pattern {
                                Some(pattern) => pair(
                                    opt_string(pattern.family.as_deref()),
                                    opt_string(pattern.registry.as_deref()),
                                ),
                                None => Value::Nil,
                            }));
                        Value::vector([target, patterns])
                    })),
                ])
            })),
        ));
        // xfaces.c: Vface_new_frame_defaults holds the global lface
        // vectors; a frame's own vectors (the `frames' map) go with the
        // nilled frame.
        groups.push((
            RootSlot::LispFaces,
            Value::list(self.lisp_face_states.iter().map(|face| {
                Value::vector([
                    Value::string(&face.name),
                    face.id.map_or(Value::Nil, Value::Integer),
                    face.global.clone().unwrap_or(Value::Nil),
                ])
            })),
        ));
        groups.push((
            RootSlot::FontSelectionOrder,
            Value::vector(
                self.font_selection_order
                    .iter()
                    .map(|name| Value::symbol(name)),
            ),
        ));
        groups.push((
            RootSlot::AlternativeFontFamilyAlist,
            self.alternative_font_family_alist.clone(),
        ));
        groups.push((
            RootSlot::AlternativeFontRegistryAlist,
            self.alternative_font_registry_alist.clone(),
        ));
        // fringe.c: the bitmaps `define-fringe-bitmap' made and their faces.
        groups.push((
            RootSlot::FringeBitmaps,
            Value::list(self.fringe_bitmap_states.iter().map(|bitmap| {
                Value::vector([
                    Value::symbol(&bitmap.name),
                    Value::Integer(bitmap.id),
                    bool_value(bitmap.standard),
                    bitmap.definition.clone().unwrap_or(Value::Nil),
                    bitmap.face.clone(),
                ])
            })),
        ));
        // composite.c: composition_hash_table's entries.
        groups.push((
            RootSlot::Compositions,
            Value::list(self.composition_states.iter().map(|composition| {
                Value::vector([
                    composition.components.clone(),
                    bool_value(composition.relative),
                    Value::Integer(composition.width),
                ])
            })),
        ));
        // ert.el keeps a test in its symbol's `ert--test' property; the
        // registry mirrors it.
        groups.push((
            RootSlot::ErtTests,
            Value::list(self.ert_tests.iter().map(|test| {
                Value::vector([
                    Value::symbol(&test.name),
                    test.body.clone(),
                    opt_string(test.source_file.as_deref()),
                    symbol_list(&test.tags),
                    Value::symbol(&test.expected_result),
                ])
            })),
        ));
        // editfns.c: labeled_restrictions, an alist of (buffer (label beg end)...).
        groups.push((
            RootSlot::LabeledRestrictions,
            Value::list(self.labeled_restrictions.iter().filter_map(|restriction| {
                self.buffer_value(restriction.buffer_id).map(|buffer| {
                    Value::vector([
                        buffer,
                        restriction.label.clone().unwrap_or(Value::Nil),
                        Value::Marker(restriction.beg_marker_id),
                        Value::Marker(restriction.end_marker_id),
                    ])
                })
            })),
        ));
        // keyboard.c: Vtimer_list's timer vectors; a due time as the
        // seconds still to wait at dump time.
        let now = std::time::Instant::now();
        groups.push((
            RootSlot::TimerList,
            Value::list(self.pending_timers.iter().map(|timer| {
                Value::vector([
                    timer.function.clone(),
                    Value::list(timer.args.iter().cloned()),
                    timer.due.map_or(Value::Nil, |due| {
                        Value::float(due.saturating_duration_since(now).as_secs_f64())
                    }),
                    timer.repeat.map_or(Value::Nil, Value::float),
                    opt_symbol(timer.original_name.as_deref()),
                ])
            })),
        ));
        // thread.c: last_thread_error.
        groups.push((
            RootSlot::LastThreadError,
            self.last_thread_error.clone().unwrap_or(Value::Nil),
        ));
        // The captured lexical cells that were assigned to: GNU's storage
        // is the (SYMBOL . VALUE) cons of the closure's environment;
        // Emaxx keeps the current value per frame identity and name.
        let mut frames = self.lexical_cell_updates.iter().collect::<Vec<_>>();
        frames.sort_by_key(|(identity, _)| **identity);
        groups.push((
            RootSlot::LexicalCellUpdates,
            Value::list(frames.into_iter().map(|(identity, updates)| {
                let mut updates = updates.iter().collect::<Vec<_>>();
                updates.sort_by(|a, b| a.0.cmp(b.0));
                Value::vector([
                    Value::Integer(*identity),
                    Value::list(
                        updates
                            .into_iter()
                            .map(|(name, value)| pair(Value::symbol(name), value.clone())),
                    ),
                ])
            })),
        ));
        // PDUMPER_REMEMBER_SCALAR: the native state beside the Lisp
        // objects that a loaded session must start from -- the id
        // allocators above every id in the image, the declared-special
        // list, the DOC offsets Snarf-documentation installed (GNU keeps
        // them in the subr objects), lossage_size, the face change
        // counter and the finalizer count.  Everything else the
        // interpreter holds is in `FIELDS_NOT_CARRIED' with its reason.
        let mut doc_offsets = self.builtin_doc_offsets.iter().collect::<Vec<_>>();
        doc_offsets.sort_by(|a, b| a.0.cmp(b.0));
        groups.push((
            RootSlot::RememberedScalars,
            Value::list([
                pair(
                    Value::symbol("special-variables"),
                    symbol_list(&self.special_variables),
                ),
                pair(
                    Value::symbol("next-buffer-id"),
                    Value::Integer(self.next_buffer_id as i64),
                ),
                pair(
                    Value::symbol("next-overlay-id"),
                    Value::Integer(self.next_overlay_id as i64),
                ),
                pair(
                    Value::symbol("next-marker-id"),
                    Value::Integer(self.next_marker_id as i64),
                ),
                pair(
                    Value::symbol("next-char-table-id"),
                    Value::Integer(self.next_char_table_id as i64),
                ),
                pair(
                    Value::symbol("next-record-id"),
                    Value::Integer(self.next_record_id as i64),
                ),
                pair(
                    Value::symbol("next-finalizer-id"),
                    Value::Integer(self.next_finalizer_id as i64),
                ),
                pair(
                    Value::symbol("next-lisp-face-id"),
                    Value::Integer(self.next_lisp_face_id),
                ),
                pair(
                    Value::symbol("next-special-binding-id"),
                    Value::Integer(self.next_special_binding_id as i64),
                ),
                pair(
                    Value::symbol("lossage-size"),
                    Value::Integer(self.lossage_size),
                ),
                pair(
                    Value::symbol("builtin-doc-offsets"),
                    Value::list(
                        doc_offsets.into_iter().map(|(name, offset)| {
                            pair(Value::symbol(name), Value::Integer(*offset))
                        }),
                    ),
                ),
                pair(
                    Value::symbol("face-change-count"),
                    Value::Integer(self.face_change_count as i64),
                ),
                pair(
                    Value::symbol("finalizers-run"),
                    Value::Integer(self.finalizers_run as i64),
                ),
            ]),
        ));
        groups
    }

    /// The transient groups that must be empty when the image is written.
    pub(crate) fn dump_transient_roots_check(&self) -> Result<(), String> {
        let counts = [
            ("pending_thread_events", self.pending_thread_events.len()),
            (
                "deferred_defsubst_unbindings",
                self.deferred_defsubst_unbindings.len(),
            ),
        ];
        debug_assert_eq!(counts.len(), TRANSIENT_ROOTS.len());
        for (name, count) in counts {
            if count != 0 {
                return Err(format!(
                    "cannot dump with {count} entries of {name} pending"
                ));
            }
        }
        Ok(())
    }
}

/// pdumper_load's side: the values back into the tables.  Exercised by
/// the round-trip controls until the process-level restore (D12/D13).
#[cfg_attr(not(test), allow(dead_code))]
mod install {
    use super::*;

    fn expect_list(value: &Value, what: &str) -> Result<Vec<Value>, String> {
        value
            .to_vec()
            .map_err(|_| format!("{what}: not a list: {value:?}"))
    }

    fn expect_vector(value: &Value, what: &str) -> Result<Vec<Value>, String> {
        match value {
            Value::Vector(vector) => Ok(vector.slots().clone()),
            other => Err(format!("{what}: not a vector: {other:?}")),
        }
    }

    fn expect_string(value: &Value, what: &str) -> Result<String, String> {
        crate::lisp::primitives::string_like(value)
            .map(|string| string.text)
            .ok_or_else(|| format!("{what}: not a string: {value:?}"))
    }

    fn expect_symbol(value: &Value, what: &str) -> Result<String, String> {
        value
            .as_symbol()
            .map(str::to_owned)
            .map_err(|_| format!("{what}: not a symbol: {value:?}"))
    }

    fn expect_int(value: &Value, what: &str) -> Result<i64, String> {
        value
            .as_integer()
            .map_err(|_| format!("{what}: not an integer: {value:?}"))
    }

    fn expect_id(value: &Value, what: &str) -> Result<u64, String> {
        u64::try_from(expect_int(value, what)?).map_err(|_| format!("{what}: negative id"))
    }

    fn opt_of<T>(
        value: &Value,
        parse: impl FnOnce(&Value) -> Result<T, String>,
    ) -> Result<Option<T>, String> {
        if value.is_nil() {
            Ok(None)
        } else {
            parse(value).map(Some)
        }
    }

    fn expect_char_table(value: &Value, what: &str) -> Result<u64, String> {
        match value {
            Value::CharTable(id) => Ok(*id),
            other => Err(format!("{what}: not a char-table: {other:?}")),
        }
    }

    fn expect_marker(value: &Value, what: &str) -> Result<u64, String> {
        match value {
            Value::Marker(id) => Ok(*id),
            other => Err(format!("{what}: not a marker: {other:?}")),
        }
    }

    fn expect_buffer(value: &Value, what: &str) -> Result<u64, String> {
        match value {
            Value::Buffer(buffer) => Ok(buffer.id),
            other => Err(format!("{what}: not a buffer: {other:?}")),
        }
    }

    fn expect_pair(value: &Value, what: &str) -> Result<(Value, Value), String> {
        match (value.car(), value.cdr()) {
            (Ok(car), Ok(cdr)) => Ok((car, cdr)),
            _ => Err(format!("{what}: not a pair: {value:?}")),
        }
    }

    impl Interpreter {
        /// Reinstall one root group from the value the image holds for it; a
        /// slot that is not a group is left to the caller.
        pub(crate) fn install_root_group(
            &mut self,
            slot: RootSlot,
            value: &Value,
        ) -> Result<(), String> {
            match slot {
                RootSlot::BufferAlist => {
                    let mut list = Vec::new();
                    for entry in expect_list(value, "buffer alist")? {
                        let (name, buffer) = expect_pair(&entry, "buffer alist entry")?;
                        list.push((
                            expect_buffer(&buffer, "buffer alist entry")?,
                            expect_string(&name, "buffer name")?,
                        ));
                    }
                    let current = self.current_buffer_id;
                    self.inactive_buffers.retain(|(id, _)| {
                        *id == current || list.iter().any(|(listed, _)| listed == id)
                    });
                    self.buffer_list = list;
                }
                RootSlot::ThisCommandKeys => {
                    self.keyboard_input.command_keys = expect_vector(value, "this_command_keys")?;
                }
                RootSlot::RawKeybuf => {
                    self.keyboard_input.raw_keys = expect_vector(value, "raw_keybuf")?;
                }
                RootSlot::RecentKeys => {
                    self.keyboard_input.recent_keys = expect_vector(value, "recent_keys")?;
                }
                RootSlot::DetachedForwardedVariables => {
                    self.detached_forwarded_variables.clear();
                    for entry in expect_list(value, "detached forwarded variables")? {
                        let (name, value) = expect_pair(&entry, "detached forwarded variable")?;
                        self.detached_forwarded_variables
                            .insert(expect_symbol(&name, "variable name")?, value);
                    }
                }
                RootSlot::Charsets => {
                    self.charset_ids.clear();
                    self.charset_plists.clear();
                    self.charset_supplementary.clear();
                    self.charset_unified.clear();
                    for entry in expect_list(value, "charsets")? {
                        let fields = expect_vector(&entry, "charset")?;
                        if fields.len() != 5 {
                            return Err("charset: five fields expected".into());
                        }
                        let name = expect_symbol(&fields[0], "charset name")?;
                        self.charset_ids
                            .push((name.clone(), expect_int(&fields[1], "charset id")?));
                        self.charset_plists.push((name.clone(), fields[2].clone()));
                        if fields[3].is_truthy() {
                            self.charset_supplementary.insert(name.clone());
                        }
                        if fields[4].is_truthy() {
                            self.charset_unified.insert(name);
                        }
                    }
                }
                RootSlot::CharsetOrderedList => {
                    self.charset_priority = symbol_names(value, "charset ordered list")?;
                }
                RootSlot::CharsetList => {
                    self.charset_names = symbol_names(value, "charset list")?;
                }
                RootSlot::Iso2022CharsetList => {
                    self.iso_2022_charset_list = symbol_names(value, "iso-2022 charset list")?;
                }
                RootSlot::CharsetAliases => {
                    self.charset_aliases = symbol_pairs(value, "charset aliases")?;
                }
                RootSlot::IsoCharsetTable => {
                    self.iso_charsets.clear();
                    for entry in expect_list(value, "iso charset table")? {
                        let fields = expect_vector(&entry, "iso charset entry")?;
                        if fields.len() != 4 {
                            return Err("iso charset entry: four fields expected".into());
                        }
                        self.iso_charsets.push((
                            expect_int(&fields[0], "dimension")?,
                            expect_int(&fields[1], "chars")?,
                            u32::try_from(expect_int(&fields[2], "final byte")?)
                                .map_err(|_| "final byte out of range".to_owned())?,
                            expect_symbol(&fields[3], "charset name")?,
                        ));
                    }
                }
                RootSlot::CharsetNonPreferredHead => {
                    self.charset_non_preferred_head =
                        opt_of(value, |v| expect_symbol(v, "non-preferred head"))?;
                }
                RootSlot::SjisCodingSystem => {
                    self.sjis_coding_system = expect_symbol(value, "sjis coding system")?;
                }
                RootSlot::Big5CodingSystem => {
                    self.big5_coding_system = expect_symbol(value, "big5 coding system")?;
                }
                RootSlot::CodingSystems => {
                    self.coding_systems.clear();
                    for entry in expect_list(value, "coding systems")? {
                        let fields = expect_vector(&entry, "coding system")?;
                        if fields.len() != 9 {
                            return Err("coding system: nine fields expected".into());
                        }
                        self.coding_systems.push(CodingSystemState {
                            name: expect_symbol(&fields[0], "coding system name")?,
                            base: expect_symbol(&fields[1], "coding system base")?,
                            kind: expect_symbol(&fields[2], "coding system kind")?,
                            eol_type: opt_of(&fields[3], |v| expect_int(v, "eol type"))?,
                            plist: fields[4].clone(),
                            category: usize::try_from(expect_int(&fields[5], "category")?)
                                .map_err(|_| "category out of range".to_owned())?,
                            charset_list: fields[6].clone(),
                            default_char: u32::try_from(expect_int(&fields[7], "default char")?)
                                .map_err(|_| "default char out of range".to_owned())?,
                            type_args: expect_vector(&fields[8], "type args")?,
                        });
                    }
                }
                RootSlot::CodingAliases => {
                    self.coding_aliases = symbol_pairs(value, "coding aliases")?;
                }
                RootSlot::CodingPriority => {
                    self.coding_priority = symbol_names(value, "coding priority")?;
                }
                RootSlot::CodingCategoryRepresentatives => {
                    let mut representatives = Vec::new();
                    for entry in expect_vector(value, "coding category representatives")? {
                        representatives
                            .push(opt_of(&entry, |v| expect_symbol(v, "representative"))?);
                    }
                    self.coding_category_representatives = representatives;
                }
                RootSlot::CodingCategoryPriorities => {
                    let mut priorities = Vec::new();
                    for entry in expect_vector(value, "coding category priorities")? {
                        priorities.push(
                            usize::try_from(expect_int(&entry, "category")?)
                                .map_err(|_| "category out of range".to_owned())?,
                        );
                    }
                    self.coding_category_priorities = priorities;
                }
                RootSlot::CclProgramTable => {
                    let mut programs = Vec::new();
                    for entry in expect_vector(value, "ccl program table")? {
                        programs.push(if entry.is_nil() {
                            None
                        } else {
                            let (name, program) = expect_pair(&entry, "ccl program entry")?;
                            Some((expect_symbol(&name, "ccl program name")?, program))
                        });
                    }
                    self.ccl_programs = programs;
                }
                RootSlot::StandardSyntaxTable => {
                    self.standard_syntax_table_id =
                        expect_char_table(value, "standard syntax table")?;
                }
                RootSlot::StandardCategoryTable => {
                    self.standard_category_table_id =
                        opt_of(value, |v| expect_char_table(v, "standard category table"))?;
                }
                RootSlot::StandardCaseTable => {
                    self.standard_case_table_id =
                        opt_of(value, |v| expect_char_table(v, "standard case table"))?;
                }
                RootSlot::AsciiCaseTables => {
                    let mut ids = Vec::new();
                    for entry in expect_vector(value, "ascii case tables")? {
                        ids.push(expect_char_table(&entry, "ascii case table")?);
                    }
                    self.ascii_case_table_ids = ids;
                }
                RootSlot::SyntaxWordChars => {
                    let mut codes = Vec::new();
                    for entry in expect_vector(value, "syntax word chars")? {
                        codes.push(
                            u32::try_from(expect_int(&entry, "word char")?)
                                .map_err(|_| "word char out of range".to_owned())?,
                        );
                    }
                    self.syntax_word_chars = codes;
                }
                RootSlot::Fontsets => {
                    let mut fontsets = Vec::new();
                    for entry in expect_list(value, "fontsets")? {
                        let fields = expect_vector(&entry, "fontset")?;
                        if fields.len() != 2 {
                            return Err("fontset: two fields expected".into());
                        }
                        let mut mappings = Vec::new();
                        for mapping in expect_list(&fields[1], "fontset mappings")? {
                            let mapping = expect_vector(&mapping, "fontset mapping")?;
                            if mapping.len() != 2 {
                                return Err("fontset mapping: two fields expected".into());
                            }
                            let target = expect_list(&mapping[0], "fontset target")?;
                            let kind = target
                                .first()
                                .map(|kind| expect_symbol(kind, "fontset target kind"))
                                .transpose()?
                                .unwrap_or_default();
                            let target = match (kind.as_str(), target.len()) {
                                ("character", 2) => FontsetTargetState::Character(expect_int(
                                    &target[1],
                                    "character",
                                )?),
                                ("range", 3) => FontsetTargetState::Range(
                                    expect_int(&target[1], "range start")?,
                                    expect_int(&target[2], "range end")?,
                                ),
                                ("script", 2) => {
                                    FontsetTargetState::Script(expect_string(&target[1], "script")?)
                                }
                                ("fallback", 1) => FontsetTargetState::Fallback,
                                _ => {
                                    return Err(format!("fontset target: unknown form {target:?}"));
                                }
                            };
                            let mut patterns = Vec::new();
                            for pattern in expect_list(&mapping[1], "fontset patterns")? {
                                patterns.push(if pattern.is_nil() {
                                    None
                                } else {
                                    let (family, registry) = expect_pair(&pattern, "font pattern")?;
                                    Some(FontPatternState {
                                        family: opt_of(&family, |v| expect_string(v, "family"))?,
                                        registry: opt_of(&registry, |v| {
                                            expect_string(v, "registry")
                                        })?,
                                    })
                                });
                            }
                            mappings.push(FontsetMappingState { target, patterns });
                        }
                        fontsets.push(FontsetState {
                            name: expect_string(&fields[0], "fontset name")?,
                            mappings,
                        });
                    }
                    self.fontset_states = fontsets;
                }
                RootSlot::LispFaces => {
                    let mut faces = Vec::new();
                    for entry in expect_list(value, "lisp faces")? {
                        let fields = expect_vector(&entry, "lisp face")?;
                        if fields.len() != 3 {
                            return Err("lisp face: three fields expected".into());
                        }
                        faces.push(LispFaceState {
                            name: expect_string(&fields[0], "face name")?,
                            id: opt_of(&fields[1], |v| expect_int(v, "face id"))?,
                            global: opt_of(&fields[2], |v| Ok(v.clone()))?,
                            frames: HashMap::new(),
                        });
                    }
                    self.lisp_face_states = faces;
                }
                RootSlot::FontSelectionOrder => {
                    let order = expect_vector(value, "font selection order")?;
                    if order.len() != 4 {
                        return Err("font selection order: four entries expected".into());
                    }
                    for (slot, entry) in self.font_selection_order.iter_mut().zip(&order) {
                        *slot = expect_symbol(entry, "font selection order entry")?;
                    }
                }
                RootSlot::AlternativeFontFamilyAlist => {
                    self.alternative_font_family_alist = value.clone();
                }
                RootSlot::AlternativeFontRegistryAlist => {
                    self.alternative_font_registry_alist = value.clone();
                }
                RootSlot::FringeBitmaps => {
                    let mut bitmaps = Vec::new();
                    for entry in expect_list(value, "fringe bitmaps")? {
                        let fields = expect_vector(&entry, "fringe bitmap")?;
                        if fields.len() != 5 {
                            return Err("fringe bitmap: five fields expected".into());
                        }
                        bitmaps.push(FringeBitmapState {
                            name: expect_symbol(&fields[0], "fringe bitmap name")?,
                            id: expect_int(&fields[1], "fringe bitmap id")?,
                            standard: fields[2].is_truthy(),
                            definition: opt_of(&fields[3], |v| Ok(v.clone()))?,
                            face: fields[4].clone(),
                        });
                    }
                    self.fringe_bitmap_states = bitmaps;
                }
                RootSlot::Compositions => {
                    let mut compositions = Vec::new();
                    for entry in expect_list(value, "compositions")? {
                        let fields = expect_vector(&entry, "composition")?;
                        if fields.len() != 3 {
                            return Err("composition: three fields expected".into());
                        }
                        compositions.push(CompositionState {
                            components: fields[0].clone(),
                            relative: fields[1].is_truthy(),
                            width: expect_int(&fields[2], "composition width")?,
                        });
                    }
                    self.composition_states = compositions;
                }
                RootSlot::ErtTests => {
                    let mut tests = Vec::new();
                    for entry in expect_list(value, "ert tests")? {
                        let fields = expect_vector(&entry, "ert test")?;
                        if fields.len() != 5 {
                            return Err("ert test: five fields expected".into());
                        }
                        tests.push(ErtTestDefinition {
                            name: expect_symbol(&fields[0], "test name")?,
                            body: fields[1].clone(),
                            source_file: opt_of(&fields[2], |v| expect_string(v, "source file"))?,
                            tags: symbol_names(&fields[3], "test tags")?,
                            expected_result: expect_symbol(&fields[4], "expected result")?,
                        });
                    }
                    self.ert_tests = tests;
                }
                RootSlot::LabeledRestrictions => {
                    let mut restrictions = Vec::new();
                    for entry in expect_list(value, "labeled restrictions")? {
                        let fields = expect_vector(&entry, "labeled restriction")?;
                        if fields.len() != 4 {
                            return Err("labeled restriction: four fields expected".into());
                        }
                        restrictions.push(LabeledRestriction {
                            buffer_id: expect_buffer(&fields[0], "restriction buffer")?,
                            label: opt_of(&fields[1], |v| Ok(v.clone()))?,
                            beg_marker_id: expect_marker(&fields[2], "restriction start")?,
                            end_marker_id: expect_marker(&fields[3], "restriction end")?,
                        });
                    }
                    self.labeled_restrictions = restrictions;
                }
                RootSlot::TimerList => {
                    let now = std::time::Instant::now();
                    let mut timers = Vec::new();
                    for entry in expect_list(value, "timer list")? {
                        let fields = expect_vector(&entry, "timer")?;
                        if fields.len() != 5 {
                            return Err("timer: five fields expected".into());
                        }
                        let due = opt_of(&fields[2], |v| {
                            v.as_float()
                                .map_err(|_| "timer due: not a float".to_owned())
                        })?
                        .map(|seconds| now + std::time::Duration::from_secs_f64(seconds.max(0.0)));
                        timers.push(ScheduledTimer {
                            function: fields[0].clone(),
                            args: expect_list(&fields[1], "timer args")?,
                            due,
                            repeat: opt_of(&fields[3], |v| {
                                v.as_float()
                                    .map_err(|_| "timer repeat: not a float".to_owned())
                            })?,
                            original_name: opt_of(&fields[4], |v| expect_symbol(v, "timer name"))?,
                        });
                    }
                    self.pending_timers = timers;
                }
                RootSlot::LastThreadError => {
                    self.last_thread_error = opt_of(value, |v| Ok(v.clone()))?;
                }
                RootSlot::RememberedScalars => {
                    for entry in expect_list(value, "remembered scalars")? {
                        let (name, scalar) = expect_pair(&entry, "remembered scalar")?;
                        let name = expect_symbol(&name, "remembered scalar name")?;
                        let int = |what: &str| expect_int(&scalar, what);
                        let id = |what: &str| expect_id(&scalar, what);
                        match name.as_str() {
                            "special-variables" => {
                                self.special_variables =
                                    symbol_names(&scalar, "special variables")?;
                            }
                            "next-buffer-id" => {
                                self.next_buffer_id = self.next_buffer_id.max(id(&name)?)
                            }
                            "next-overlay-id" => {
                                self.next_overlay_id = self.next_overlay_id.max(id(&name)?);
                            }
                            "next-marker-id" => {
                                self.next_marker_id = self.next_marker_id.max(id(&name)?)
                            }
                            "next-char-table-id" => {
                                self.next_char_table_id = self.next_char_table_id.max(id(&name)?);
                            }
                            "next-record-id" => {
                                self.next_record_id = self.next_record_id.max(id(&name)?)
                            }
                            "next-finalizer-id" => {
                                self.next_finalizer_id = self.next_finalizer_id.max(id(&name)?);
                            }
                            "next-lisp-face-id" => self.next_lisp_face_id = int(&name)?,
                            "next-special-binding-id" => {
                                self.next_special_binding_id = id(&name)?;
                            }
                            "lossage-size" => self.lossage_size = int(&name)?,
                            "builtin-doc-offsets" => {
                                self.builtin_doc_offsets.clear();
                                for offset in expect_list(&scalar, "doc offsets")? {
                                    let (subr, offset) = expect_pair(&offset, "doc offset")?;
                                    self.builtin_doc_offsets.insert(
                                        expect_symbol(&subr, "doc offset name")?,
                                        expect_int(&offset, "doc offset")?,
                                    );
                                }
                            }
                            "face-change-count" => self.face_change_count = id(&name)?,
                            "finalizers-run" => self.finalizers_run = id(&name)?,
                            other => {
                                return Err(format!("unknown remembered scalar {other}"));
                            }
                        }
                    }
                }
                RootSlot::LexicalCellUpdates => {
                    self.lexical_cell_updates.clear();
                    for entry in expect_list(value, "lexical cell updates")? {
                        let fields = expect_vector(&entry, "lexical cell updates entry")?;
                        if fields.len() != 2 {
                            return Err("lexical cell updates entry: two fields expected".into());
                        }
                        let identity = expect_int(&fields[0], "frame identity")?;
                        let mut updates = HashMap::new();
                        for update in expect_list(&fields[1], "lexical cell updates")? {
                            let (name, value) = expect_pair(&update, "lexical cell update")?;
                            updates.insert(expect_symbol(&name, "lexical variable")?, value);
                        }
                        self.lexical_cell_updates.insert(identity, updates);
                    }
                }
                _ => {}
            }
            Ok(())
        }
    }

    fn symbol_names(value: &Value, what: &str) -> Result<Vec<String>, String> {
        expect_list(value, what)?
            .iter()
            .map(|entry| expect_symbol(entry, what))
            .collect()
    }

    fn symbol_pairs(value: &Value, what: &str) -> Result<Vec<(String, String)>, String> {
        let mut pairs = Vec::new();
        for entry in expect_list(value, what)? {
            let (car, cdr) = expect_pair(&entry, what)?;
            pairs.push((expect_symbol(&car, what)?, expect_symbol(&cdr, what)?));
        }
        Ok(pairs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_reset_root_is_a_mark_phase_root() {
        // The documented list names real fields only.
        let source =
            std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lisp/eval.rs"))
                .expect("read eval.rs");
        let names = ROOTS_RESET_AFTER_LOAD
            .iter()
            .map(|(name, _)| *name)
            .chain(TRANSIENT_ROOTS.iter().copied());
        for name in names {
            assert!(
                source.contains(&format!("self.{name}")),
                "{name} is not an Interpreter field the mark phase visits"
            );
        }
        // Every field documented as not carried is a declared field of
        // the `Interpreter' shell or its `InterpreterState' payload.
        let declarations = ["pub struct Interpreter {", "pub struct InterpreterState {"]
            .iter()
            .map(|header| {
                let start = source.find(header).expect("struct definition");
                let end = start + source[start..].find("\n}\n").expect("end of struct");
                &source[start..end]
            })
            .collect::<Vec<_>>()
            .join("\n");
        for (name, _) in FIELDS_NOT_CARRIED {
            assert!(
                declarations.contains(&format!(" {name}: ")),
                "{name} is not a field of Interpreter or InterpreterState"
            );
        }
        // A field is documented once: either it is a re-created root or
        // it is state the image does not carry, never both.
        for (name, _) in FIELDS_NOT_CARRIED {
            assert!(
                !ROOTS_RESET_AFTER_LOAD
                    .iter()
                    .any(|(reset, _)| reset == name),
                "{name} is listed both as reset after load and as not carried"
            );
        }
    }
}
