use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::path::PathBuf;
use std::process::Child;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::primitives;
use super::reader::RECORD_LITERAL_SYMBOL;
use super::sqlite::SqliteHandleState;
use super::types::{Env, LispError, Value, shared_env};
use crate::compat::{BatchSummary, DiscoveredTest, TestOutcome, TestStatus};
use regex::Regex;

mod bindings;
mod bootstrap;
mod buffers;
mod classes;
mod coding;
mod control_forms;
mod core;
mod definitions;
mod loops;
mod macros;
mod preload;
mod resource_forms;
mod runtime;
mod rx;
mod threads;
mod variables;
use bootstrap::*;
pub(crate) use preload::*;
mod ert;

pub(crate) use rx::compile_rx_to_string;

#[derive(Clone, Debug)]
pub struct ErtTestDefinition {
    pub name: String,
    pub body: Value,
    pub source_file: Option<String>,
    pub tags: Vec<String>,
    pub expected_result: String,
}

impl ErtTestDefinition {
    fn discovered(&self) -> DiscoveredTest {
        DiscoveredTest {
            name: self.name.clone(),
            tags: self.tags.clone(),
            expected_result: self.expected_result.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MarkerState {
    pub id: u64,
    pub buffer_id: Option<u64>,
    pub position: Option<usize>,
    pub last_position: Option<usize>,
    pub insertion_type: bool,
}

#[derive(Clone, Debug)]
pub struct CharTableState {
    pub id: u64,
    pub subtype: Option<String>,
    pub default: Value,
    pub parent: Option<u64>,
    pub extra_slots: Vec<Value>,
    pub entries: Vec<CharTableEntry>,
    pub category_docs: Vec<(u32, String)>,
}

#[derive(Clone, Debug)]
pub struct CharTableEntry {
    pub start: u32,
    pub end: u32,
    pub value: Value,
}

#[derive(Clone, Debug)]
pub struct RecordState {
    pub id: u64,
    pub type_name: String,
    pub slots: Vec<Value>,
}

#[derive(Clone, Debug)]
pub struct CodingSystemState {
    pub name: String,
    pub base: String,
    pub kind: String,
    pub eol_type: Option<i64>,
    pub plist: Value,
}

#[derive(Clone, Debug, Default)]
struct UndoSequenceState {
    original_groups: Vec<Vec<crate::buffer::UndoEntry>>,
    undone_count: usize,
    redo_groups: Vec<Vec<crate::buffer::UndoEntry>>,
    had_error: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SpecialBindingScope {
    Global,
    BufferLocal(u64),
}

#[derive(Clone, Debug)]
struct SpecialBindingRestore {
    name: String,
    scope: SpecialBindingScope,
    binding_buffer_id: Option<u64>,
    previous: Option<Value>,
}

#[derive(Clone, Debug)]
struct BacktraceFrame {
    function: Value,
    args: Vec<Value>,
    locals: Vec<(String, Value)>,
    evald: bool,
    debug_on_exit: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum BufferDisposition {
    Default,
    Preserve,
    Silently,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ThreadBlocker {
    Mutex(u64),
    ConditionVariable(u64),
    Sleep,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ThreadStatus {
    Runnable,
    Blocked(ThreadBlocker),
    Finished,
}

#[derive(Clone, Debug)]
enum ThreadProgram {
    Main,
    Ignore,
    Call(Value),
    SetGlobal {
        name: String,
        value: Value,
    },
    Sleep {
        blocked: bool,
    },
    YieldThenSetGlobal {
        target: String,
        value: Value,
        phase: u8,
    },
    MutexContention {
        phase: u8,
    },
    MutexBlock {
        phase: u8,
    },
    SignalError {
        value: Value,
    },
    Noop,
    InfiniteYield,
    SignalMainThread,
    CondvarWaitTwice {
        phase: u8,
    },
    CaptureBufferLocal {
        target: String,
        source: String,
    },
    ThreadListMutexWait {
        phase: u8,
    },
}

#[derive(Clone, Debug)]
enum ThreadOutcome {
    Returned(Value),
    Signaled(Value),
}

#[derive(Clone, Debug)]
struct ThreadState {
    record_id: u64,
    name: Option<String>,
    buffer_id: u64,
    buffer_disposition: BufferDisposition,
    buffer_killed: bool,
    status: ThreadStatus,
    program: ThreadProgram,
    outcome: Option<ThreadOutcome>,
}

#[derive(Clone, Debug)]
struct MutexState {
    record_id: u64,
    _name: Option<String>,
    owner: Option<u64>,
    recursion_depth: usize,
}

#[derive(Clone, Debug)]
struct ConditionVariableState {
    record_id: u64,
    mutex_id: u64,
    name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ProcessStatus {
    Run,
    Exit,
}

impl ProcessStatus {
    fn symbol(&self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Exit => "exit",
        }
    }

    fn is_live(&self) -> bool {
        matches!(self, Self::Run)
    }
}

struct RunningProcess {
    child: Child,
}

impl std::fmt::Debug for RunningProcess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunningProcess").finish_non_exhaustive()
    }
}

struct ProcessState {
    record_id: u64,
    buffer_id: Option<u64>,
    mark_marker_id: u64,
    status: ProcessStatus,
    filter: Option<Value>,
    _query_on_exit_flag: bool,
    decoding: Value,
    encoding: Value,
    program: Option<String>,
    argv: Vec<String>,
    runtime: Option<RunningProcess>,
}

#[derive(Clone, Debug)]
struct WindowConfigurationSnapshot {
    current_buffer_id: u64,
    selected_window_id: u64,
    selected_window_slots: Vec<Value>,
    frame_width: i64,
    frame_height: i64,
}

#[derive(Clone, Debug)]
struct ClassState {
    name: String,
    record_id: u64,
    parents: Vec<String>,
    slot_specs: Vec<Value>,
    options: Vec<Value>,
    children: Vec<String>,
}

#[derive(Clone, Debug)]
struct GenericGeneralizerState {
    name: String,
    record_id: u64,
    priority: i64,
    tagcode_function: Value,
    specializers_function: Value,
}

#[derive(Clone, Debug)]
struct ScheduledTimer {
    function: Value,
    original_name: Option<String>,
    args: Vec<Value>,
}

/// The interpreter state: holds the global environment, the current buffer,
/// and ERT test results.
pub struct Interpreter {
    /// Global variable bindings (defvar, setq at top level).
    globals: Vec<(String, Value)>,
    /// Variable aliases keyed by alias name.
    variable_aliases: Vec<(String, String)>,
    /// Variables with dynamic binding semantics.
    special_variables: Vec<String>,
    /// Symbol properties keyed by symbol name.
    symbol_properties: Vec<(String, Vec<(String, Value)>)>,
    /// Symbols explicitly interned into the standard obarray.
    interned_symbols: Vec<String>,
    /// Variable watchers keyed by canonical variable name.
    variable_watchers: Vec<(String, Vec<Value>)>,
    /// The current buffer being operated on.
    pub buffer: crate::buffer::Buffer,
    /// The ID of the current buffer.
    current_buffer_id: u64,
    /// The currently selected window record.
    selected_window_id: u64,
    /// The selected frame width in character columns.
    frame_width: i64,
    /// The selected frame height in character rows.
    frame_height: i64,
    /// Terminal-local parameters for the single runtime terminal.
    terminal_parameters: Vec<(String, Value)>,
    /// Inactive buffers keyed by ID.
    inactive_buffers: Vec<(u64, crate::buffer::Buffer)>,
    /// Known buffers: (id, name) pairs.
    pub buffer_list: Vec<(u64, String)>,
    /// Next buffer ID for identity tracking.
    next_buffer_id: u64,
    /// Next overlay ID for identity tracking.
    next_overlay_id: u64,
    /// Next marker ID for identity tracking.
    next_marker_id: u64,
    /// All markers currently known to the interpreter.
    markers: Vec<MarkerState>,
    /// Char tables allocated by the interpreter.
    char_tables: Vec<CharTableState>,
    /// Charset aliases defined at runtime.
    charset_aliases: Vec<(String, String)>,
    /// Charset plist overrides keyed by canonical charset name.
    charset_plists: Vec<(String, Value)>,
    /// Current charset priority order.
    charset_priority: Vec<String>,
    /// ISO charset associations keyed by (dimension, chars, final).
    iso_charsets: Vec<(i64, i64, u32, String)>,
    /// Coding systems keyed by canonical name.
    coding_systems: Vec<CodingSystemState>,
    /// Coding-system aliases keyed by alias name.
    coding_aliases: Vec<(String, String)>,
    /// Current coding-system priority order.
    coding_priority: Vec<String>,
    /// Current terminal coding system.
    terminal_coding: Option<String>,
    /// Current keyboard coding system.
    keyboard_coding: Option<String>,
    /// Shared standard category table.
    standard_category_table_id: Option<u64>,
    /// Shared standard case table.
    standard_case_table_id: Option<u64>,
    /// Buffer-local case tables keyed by buffer id.
    buffer_case_tables: Vec<(u64, u64)>,
    /// Next char-table ID for identity tracking.
    next_char_table_id: u64,
    /// Allocated record objects.
    records: Vec<RecordState>,
    /// SQLite objects keyed by record ID.
    sqlite_handles: Vec<(u64, SqliteHandleState)>,
    /// Next record ID for identity tracking.
    next_record_id: u64,
    /// Next finalizer ID for identity tracking.
    next_finalizer_id: u64,
    /// Next generated symbol ID used by built-in macro expansion helpers.
    next_generated_symbol_id: u64,
    /// Buffer-local hook lists keyed by (buffer id, hook name).
    buffer_local_hooks: Vec<(u64, String, Vec<Value>)>,
    /// Buffer-local variable values keyed by (buffer id, variable name).
    buffer_locals: Vec<(u64, String, Value)>,
    /// Buffer-local syntax tables keyed by buffer id.
    buffer_syntax_tables: Vec<(u64, u64)>,
    /// Variables that automatically become buffer-local when set.
    auto_buffer_locals: Vec<String>,
    /// Active dynamic special bindings in stack order.
    active_special_restores: Vec<SpecialBindingRestore>,
    /// Active labeled restrictions keyed by (buffer id, label, start, end).
    labeled_restrictions: Vec<(u64, String, usize, usize)>,
    /// Indirect buffer mapping: (buffer id, base buffer id).
    indirect_buffers: Vec<(u64, u64)>,
    /// Prevent recursive before/after-change hook re-entry.
    change_hooks_running: usize,
    /// User-defined macros: name → (params, body).
    macros: Vec<(String, Vec<String>, Vec<Value>)>,
    /// User-defined functions in the function namespace.
    functions: Vec<(String, Value)>,
    /// Features currently available in this interpreter.
    provided_features: Vec<String>,
    /// Forms waiting for a feature to be provided.
    after_load_forms: Vec<(String, Vec<Value>, Env)>,
    /// File currently being loaded, if any.
    current_load_file: Option<String>,
    /// Collected ERT test definitions.
    pub ert_tests: Vec<ErtTestDefinition>,
    /// Results from the most recent ERT run.
    pub test_results: Vec<TestOutcome>,
    /// Selected test names from the most recent ERT run.
    pub last_selected_tests: Vec<String>,
    /// The latest regexp match data in buffer coordinates.
    pub last_match_data: Option<Vec<Option<(usize, usize)>>>,
    /// Source buffer for buffer-origin match data; string searches leave this unset.
    pub last_match_data_buffer_id: Option<u64>,
    pub profiler_memory_running: bool,
    pub profiler_memory_log_pending: bool,
    pub profiler_cpu_running: bool,
    pub profiler_cpu_log_pending: bool,
    pub message_capture_stack: Vec<String>,
    pub lossage_size: i64,
    face_inheritance: Vec<(String, Option<String>)>,
    syntax_word_chars: Vec<u32>,
    standard_syntax_table_id: u64,
    undo_sequence: Option<UndoSequenceState>,
    load_path: Vec<PathBuf>,
    loading_features: Vec<String>,
    lambda_capture_overrides: Vec<bool>,
    thread_states: Vec<ThreadState>,
    mutex_states: Vec<MutexState>,
    condition_variables: Vec<ConditionVariableState>,
    process_states: Vec<ProcessState>,
    class_states: Vec<ClassState>,
    class_parent_overrides: Vec<(u64, Vec<String>)>,
    generalizer_states: Vec<GenericGeneralizerState>,
    pending_timers: Vec<ScheduledTimer>,
    main_thread_id: u64,
    active_thread_id: u64,
    last_thread_error: Option<Value>,
    backtrace_frames: Vec<BacktraceFrame>,
    active_handlers: Vec<(String, Value)>,
    handler_dispatch_depth: usize,
    suspend_condition_case_count: usize,
    condition_case_depth: usize,
}

impl Default for Interpreter {
    fn default() -> Self {
        Self::new()
    }
}

impl Interpreter {
    pub fn new() -> Self {
        let main_thread_id = 1u64;
        let standard_syntax_table_id = 1u64;
        let mut interp = Interpreter {
            globals: vec![
                ("main-thread".into(), Value::Record(main_thread_id)),
                ("cl--proclaims-deferred".into(), Value::Nil),
                (
                    "command-line-args".into(),
                    primitives::command_line_args_value(),
                ),
                ("cpp-font-lock-keywords".into(), Value::Nil),
                ("current-load-list".into(), Value::Nil),
                ("case-replace".into(), Value::T),
                ("defining-kbd-macro".into(), Value::Nil),
                ("executing-kbd-macro".into(), Value::Nil),
                ("exec-path".into(), current_exec_path()),
                ("last-kbd-macro".into(), Value::Nil),
                ("file-name-handler-alist".into(), Value::Nil),
                ("inhibit-file-name-handlers".into(), Value::Nil),
                ("inhibit-file-name-operation".into(), Value::Nil),
                ("inhibit-read-only".into(), Value::Nil),
                ("null-device".into(), Value::String("/dev/null".into())),
                ("process-connection-type".into(), Value::T),
                ("selection-converter-alist".into(), Value::Nil),
                ("system-uses-terminfo".into(), Value::T),
                (
                    "vc-directory-exclusion-list".into(),
                    preloaded_vc_directory_exclusion_list(),
                ),
                (
                    "standard-output".into(),
                    Value::Symbol("external-debugging-output".into()),
                ),
                ("emaxx-external-debugging-output-target".into(), Value::Nil),
            ],
            variable_aliases: Vec::new(),
            special_variables: vec![
                "case-fold-search".into(),
                "command-line-args".into(),
                "command-line-args-left".into(),
                "command-switch-alist".into(),
                "cl--proclaims-deferred".into(),
                "current-load-list".into(),
                "display-hourglass".into(),
                "exec-path".into(),
                "file-name-handler-alist".into(),
                "gc-cons-threshold".into(),
                "inhibit-read-only".into(),
                "inhibit-file-name-handlers".into(),
                "inhibit-file-name-operation".into(),
                "initial-window-system".into(),
                "last-coding-system-used".into(),
                "line-spacing".into(),
                "left-margin".into(),
                "last-command".into(),
                "load-force-doc-strings".into(),
                "null-device".into(),
                "overwrite-mode".into(),
                "process-connection-type".into(),
                "process-environment".into(),
                "selection-converter-alist".into(),
                "window-system".into(),
                "scroll-preserve-screen-position".into(),
                "scroll-up-aggressively".into(),
                "standard-output".into(),
                "vertical-scroll-bar".into(),
                "vc-directory-exclusion-list".into(),
            ],
            symbol_properties: Vec::new(),
            interned_symbols: Vec::new(),
            variable_watchers: Vec::new(),
            buffer: crate::buffer::Buffer::new("*test*"),
            current_buffer_id: 0,
            selected_window_id: 0,
            frame_width: 80,
            frame_height: 24,
            terminal_parameters: Vec::new(),
            inactive_buffers: vec![(1, crate::buffer::Buffer::new("*Messages*"))],
            buffer_list: vec![(0, "*test*".to_string()), (1, "*Messages*".to_string())],
            next_buffer_id: 2,
            next_overlay_id: 1,
            next_marker_id: 1,
            markers: Vec::new(),
            char_tables: vec![CharTableState {
                id: standard_syntax_table_id,
                subtype: Some("syntax-table".into()),
                default: Value::Nil,
                parent: None,
                extra_slots: Vec::new(),
                entries: standard_syntax_table_entries(),
                category_docs: Vec::new(),
            }],
            charset_aliases: Vec::new(),
            charset_plists: Vec::new(),
            charset_priority: vec!["unicode".into(), "ascii".into(), "eight-bit".into()],
            iso_charsets: vec![(1, 94, 'B' as u32, "ascii".into())],
            coding_systems: builtin_coding_systems(),
            coding_aliases: builtin_coding_aliases(),
            coding_priority: builtin_coding_priority(),
            terminal_coding: None,
            keyboard_coding: None,
            standard_category_table_id: None,
            standard_case_table_id: None,
            buffer_case_tables: Vec::new(),
            next_char_table_id: 2,
            records: vec![RecordState {
                id: main_thread_id,
                type_name: "thread".into(),
                slots: Vec::new(),
            }],
            sqlite_handles: Vec::new(),
            next_record_id: 2,
            next_finalizer_id: 1,
            next_generated_symbol_id: 1,
            buffer_local_hooks: Vec::new(),
            buffer_locals: Vec::new(),
            buffer_syntax_tables: Vec::new(),
            auto_buffer_locals: vec![
                "case-fold-search".into(),
                "cursor-in-non-selected-windows".into(),
                "left-margin".into(),
                "line-spacing".into(),
                "overwrite-mode".into(),
                "scroll-up-aggressively".into(),
                "vertical-scroll-bar".into(),
            ],
            active_special_restores: Vec::new(),
            labeled_restrictions: Vec::new(),
            indirect_buffers: Vec::new(),
            change_hooks_running: 0,
            macros: Vec::new(),
            functions: Vec::new(),
            provided_features: vec![
                "emaxx".into(),
                "ert".into(),
                "kqueue".into(),
                "lcms2".into(),
                "threads".into(),
            ],
            after_load_forms: Vec::new(),
            current_load_file: None,
            ert_tests: Vec::new(),
            test_results: Vec::new(),
            last_selected_tests: Vec::new(),
            last_match_data: None,
            last_match_data_buffer_id: None,
            profiler_memory_running: false,
            profiler_memory_log_pending: false,
            profiler_cpu_running: false,
            profiler_cpu_log_pending: false,
            message_capture_stack: Vec::new(),
            lossage_size: 300,
            face_inheritance: Vec::new(),
            syntax_word_chars: Vec::new(),
            standard_syntax_table_id,
            undo_sequence: None,
            load_path: Vec::new(),
            loading_features: Vec::new(),
            lambda_capture_overrides: Vec::new(),
            thread_states: vec![ThreadState {
                record_id: main_thread_id,
                name: None,
                buffer_id: 0,
                buffer_disposition: BufferDisposition::Default,
                buffer_killed: false,
                status: ThreadStatus::Runnable,
                program: ThreadProgram::Main,
                outcome: None,
            }],
            mutex_states: Vec::new(),
            condition_variables: Vec::new(),
            process_states: Vec::new(),
            class_states: Vec::new(),
            class_parent_overrides: Vec::new(),
            generalizer_states: Vec::new(),
            pending_timers: Vec::new(),
            main_thread_id,
            active_thread_id: main_thread_id,
            last_thread_error: None,
            backtrace_frames: Vec::new(),
            active_handlers: Vec::new(),
            handler_dispatch_depth: 0,
            suspend_condition_case_count: 0,
            condition_case_depth: 0,
        };
        let esc_map = primitives::make_runtime_full_keymap(&mut interp, Some("esc-map"));
        interp.set_global_binding("esc-map", esc_map.clone());
        let ctl_x_4_map = primitives::make_runtime_keymap(&mut interp, Some("ctl-x-4-map"));
        interp.set_global_binding("ctl-x-4-map", ctl_x_4_map.clone());
        let ctl_x_5_map = primitives::make_runtime_keymap(&mut interp, Some("ctl-x-5-map"));
        interp.set_global_binding("ctl-x-5-map", ctl_x_5_map.clone());
        let tab_prefix_map = primitives::make_runtime_keymap(&mut interp, Some("tab-prefix-map"));
        interp.set_global_binding("tab-prefix-map", tab_prefix_map.clone());
        let ctl_x_map = primitives::make_runtime_full_keymap(&mut interp, Some("ctl-x-map"));
        interp.set_global_binding("ctl-x-map", ctl_x_map.clone());
        let _ = primitives::keymap_define_binding(&mut interp, &ctl_x_map, "4", ctl_x_4_map);
        let _ = primitives::keymap_define_binding(&mut interp, &ctl_x_map, "5", ctl_x_5_map);
        let _ = primitives::keymap_define_binding(&mut interp, &ctl_x_map, "t", tab_prefix_map);
        let _ = primitives::keymap_define_binding_with_placement(
            &mut interp,
            &ctl_x_map,
            "C-f",
            Some(vec!["C-f".into()]),
            Value::Symbol("find-file".into()),
            true,
        );
        let global_map = primitives::make_runtime_full_keymap(&mut interp, Some("global-map"));
        interp.set_global_binding("global-map", global_map);
        let buffer_menu_mode_map =
            primitives::make_runtime_keymap(&mut interp, Some("Buffer-menu-mode-map"));
        interp.set_global_binding("Buffer-menu-mode-map", buffer_menu_mode_map.clone());
        let global_map = interp
            .lookup_var("global-map", &Vec::new())
            .unwrap_or(Value::Nil);
        let _ = primitives::keymap_define_binding_with_placement(
            &mut interp,
            &buffer_menu_mode_map,
            "SPC",
            Some(vec!["SPC".into()]),
            Value::Symbol("Buffer-menu-select".into()),
            true,
        );
        let esc_map = interp
            .lookup_var("esc-map", &Vec::new())
            .unwrap_or(Value::Nil);
        let _ = primitives::keymap_define_binding(
            &mut interp,
            &esc_map,
            "x",
            Value::Symbol("execute-extended-command".into()),
        );
        let ctl_x_map = interp
            .lookup_var("ctl-x-map", &Vec::new())
            .unwrap_or(Value::Nil);
        let _ = primitives::keymap_define_binding(&mut interp, &global_map, "\u{1b}", esc_map);
        let _ = primitives::keymap_define_binding(&mut interp, &global_map, "\u{18}", ctl_x_map);
        let menu_bar_edit_menu = primitives::make_runtime_keymap(&mut interp, Some("Edit"));
        interp.set_global_binding("menu-bar-edit-menu", menu_bar_edit_menu);
        let input_decode_map =
            primitives::make_runtime_keymap(&mut interp, Some("input-decode-map"));
        interp.set_global_binding("input-decode-map", input_decode_map);
        let minibuffer_local_map =
            primitives::make_runtime_keymap(&mut interp, Some("minibuffer-local-map"));
        interp.set_global_binding("minibuffer-local-map", minibuffer_local_map);
        let minibuffer_local_completion_map =
            primitives::make_runtime_keymap(&mut interp, Some("minibuffer-local-completion-map"));
        interp.set_global_binding(
            "minibuffer-local-completion-map",
            minibuffer_local_completion_map,
        );
        let query_replace_map =
            primitives::make_runtime_keymap(&mut interp, Some("query-replace-map"));
        interp.set_global_binding("query-replace-map", query_replace_map);
        interp.set_global_binding("mouse-wheel-buttons", Value::Nil);
        interp.set_global_binding("minor-mode-map-alist", Value::Nil);
        interp.set_global_binding("font-lock-mode", Value::Nil);
        interp.mark_auto_buffer_local("font-lock-mode");
        interp.set_global_binding("font-lock-fontified", Value::Nil);
        interp.mark_auto_buffer_local("font-lock-fontified");
        interp.set_global_binding("header-line-indent-mode", Value::Nil);
        interp.mark_auto_buffer_local("header-line-indent-mode");
        interp.set_global_binding("major-mode", Value::Symbol("fundamental-mode".into()));
        interp.mark_auto_buffer_local("major-mode");
        interp.set_global_binding("mode-name", Value::String("Fundamental".into()));
        interp.mark_auto_buffer_local("mode-name");
        interp.set_global_binding(
            "mode-line-buffer-identification",
            Value::list([Value::String("%12b".into())]),
        );
        let glyphless_char_display =
            interp.make_char_table(Some("glyphless-char-display".into()), Value::Nil);
        interp.set_global_binding("glyphless-char-display", glyphless_char_display);
        interp.set_global_binding("buffer-read-only", Value::Nil);
        interp.mark_auto_buffer_local("buffer-read-only");
        interp.set_global_binding("read-only-mode", Value::Nil);
        interp.mark_auto_buffer_local("read-only-mode");
        interp.put_symbol_property("default-directory", "permanent-local", Value::T);
        interp.set_global_binding("mark-ring", Value::Nil);
        interp.mark_auto_buffer_local("mark-ring");
        interp.put_symbol_property("mark-ring", "permanent-local", Value::T);
        interp.set_global_binding("mark-ring-max", Value::Integer(16));
        interp.put_symbol_property(
            "mark-ring-max",
            "standard-value",
            Value::list([quoted_literal(&Value::Integer(16))]),
        );
        interp.put_symbol_property(
            "mark-ring-max",
            "custom-type",
            Value::Symbol("natnum".into()),
        );
        interp.set_global_binding("global-mark-ring", Value::Nil);
        interp.set_global_binding("global-mark-ring-max", Value::Integer(16));
        interp.put_symbol_property(
            "global-mark-ring-max",
            "standard-value",
            Value::list([quoted_literal(&Value::Integer(16))]),
        );
        interp.put_symbol_property(
            "global-mark-ring-max",
            "custom-type",
            Value::Symbol("natnum".into()),
        );
        interp.set_global_binding("current-prefix-arg", Value::Nil);
        interp.set_global_binding("this-command", Value::Nil);
        interp.set_global_binding("last-command", Value::Nil);
        interp.set_global_binding("tab-bar-new-tab-choice", Value::T);
        interp.set_global_binding("max-lisp-eval-depth", Value::Integer(1600));
        interp.put_symbol_property(
            "tab-bar-new-tab-choice",
            "custom-type",
            tab_bar_new_tab_choice_custom_type(),
        );
        interp.set_global_binding("search-upper-case", Value::Symbol("not-yanks".into()));
        interp.set_global_binding("search-spaces-regexp", Value::Nil);
        interp.set_global_binding("search-whitespace-regexp", Value::String("[ \t]+".into()));
        if let Some(temp_dir) = interp.lookup_var("temporary-file-directory", &Vec::new()) {
            interp.put_symbol_property(
                "temporary-file-directory",
                "standard-value",
                Value::list([quoted_literal(&temp_dir)]),
            );
        }
        interp.put_symbol_property(
            "window-parameter",
            "emaxx-gv-setter",
            Value::Symbol("set-window-parameter".into()),
        );
        let selected_window = interp.create_record(
            "window",
            vec![
                Value::Integer(interp.current_buffer_id as i64),
                Value::Integer(interp.buffer.point_min() as i64),
            ],
        );
        let Value::Record(selected_window_id) = selected_window else {
            unreachable!("window records use Value::Record");
        };
        interp.selected_window_id = selected_window_id;
        let (minibuffer_buffer_id, _) = interp.create_buffer(" *Minibuf-0*");
        let minibuffer_window = interp.create_record(
            "window",
            vec![
                Value::Integer(minibuffer_buffer_id as i64),
                Value::Integer(1),
                Value::Nil,
                Value::Symbol(primitives::MINIBUFFER_WINDOW_KIND.into()),
            ],
        );
        interp.set_global_binding("emaxx-minibuffer-window", minibuffer_window);
        interp.set_global_binding("emaxx-minibuffer-selected-window", Value::Nil);
        interp
    }

    pub fn set_load_path(&mut self, load_path: Vec<PathBuf>) {
        self.load_path = load_path;
    }

    pub(crate) fn push_lambda_capture_override(&mut self, capture: bool) {
        self.lambda_capture_overrides.push(capture);
    }

    pub(crate) fn pop_lambda_capture_override(&mut self) {
        self.lambda_capture_overrides.pop();
    }

    pub(crate) fn lambda_capture_override(&self) -> Option<bool> {
        self.lambda_capture_overrides.last().copied()
    }
}

fn symbol_name(value: &Value) -> Option<String> {
    match value {
        Value::Symbol(name) => Some(name.clone()),
        _ => None,
    }
}

fn keyword_symbol_name(value: &Value) -> Option<String> {
    symbol_name(value)
}

fn quoted_symbol_name(value: &Value) -> Option<String> {
    match unquote(value) {
        Value::Symbol(name) => Some(name),
        _ => None,
    }
}

fn function_name_from_binding_form(value: &Value) -> Result<String, LispError> {
    match value {
        Value::Cons(_, _) => {
            let items = value.to_vec()?;
            if items.len() == 2
                && matches!(items.first(), Some(Value::Symbol(name)) if name == "setf")
            {
                let target = function_name_from_binding_form(&items[1])?;
                return Ok(format!("(setf {target})"));
            }
            if items.len() == 2
                && matches!(items.first(), Some(Value::Symbol(name)) if name == "function" || name == "function-quote" || name == "quote")
            {
                return function_name_from_binding_form(&items[1]);
            }
            let other = unquote(value);
            Err(LispError::TypeError("symbol".into(), other.type_name()))
        }
        _ => match unquote(value) {
            Value::Symbol(name) => Ok(name),
            other => Err(LispError::TypeError("symbol".into(), other.type_name())),
        },
    }
}

fn assignment_target_name(value: &Value) -> Result<String, LispError> {
    match value {
        Value::Symbol(name) => Ok(name.clone()),
        Value::Nil => Ok("nil".into()),
        Value::T => Ok("t".into()),
        other => Err(LispError::TypeError("symbol".into(), other.type_name())),
    }
}

fn unquote(value: &Value) -> Value {
    match value {
        Value::Cons(_, _) => {
            if let Ok(items) = value.to_vec()
                && items.len() == 2
                && matches!(items.first(), Some(Value::Symbol(name)) if name == "quote")
            {
                return items[1].clone();
            }
            value.clone()
        }
        _ => value.clone(),
    }
}

fn quoted_literal(value: &Value) -> Value {
    Value::list([Value::Symbol("quote".into()), value.clone()])
}

fn decoded_time_accessor_index(name: &str) -> Option<usize> {
    match name {
        "decoded-time-second" => Some(0),
        "decoded-time-minute" => Some(1),
        "decoded-time-hour" => Some(2),
        "decoded-time-day" => Some(3),
        "decoded-time-month" => Some(4),
        "decoded-time-year" => Some(5),
        "decoded-time-weekday" => Some(6),
        "decoded-time-dst" => Some(7),
        "decoded-time-zone" => Some(8),
        _ => None,
    }
}

fn decoded_time_accessor_value(index: usize, target: &Value) -> Result<Value, LispError> {
    let mut cell = target.clone();
    for _ in 0..index {
        cell = cell.cdr()?;
    }
    cell.car()
}

fn set_decoded_time_accessor_value(
    index: usize,
    target: &mut Value,
    value: Value,
) -> Result<(), LispError> {
    for _ in 0..index {
        *target = target.cdr()?;
    }
    target.set_car(value)
}

fn forms_to_progn(forms: &[Value]) -> Value {
    match forms {
        [] => Value::Nil,
        [single] => single.clone(),
        _ => {
            Value::list(std::iter::once(Value::Symbol("progn".into())).chain(forms.iter().cloned()))
        }
    }
}

fn normalize_if_let_spec(spec: &Value) -> Result<Value, LispError> {
    let items = spec.to_vec()?;
    let old_single_binding_syntax = !items.is_empty()
        && items.len() <= 2
        && !matches!(items[0], Value::Nil | Value::Cons(_, _));
    Ok(if old_single_binding_syntax {
        Value::list([spec.clone()])
    } else {
        spec.clone()
    })
}

fn named_let_tail_call(name: &str, forms: &[Value]) -> Option<(Vec<Value>, Vec<Value>)> {
    let (tail, prefix) = forms.split_last()?;
    let items = tail.to_vec().ok()?;
    match items.split_first() {
        Some((Value::Symbol(symbol), args)) if symbol == name => {
            Some((prefix.to_vec(), args.to_vec()))
        }
        _ => None,
    }
}

fn named_let_contains_call(name: &str, value: &Value) -> bool {
    if let Ok(items) = value.to_vec() {
        if matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == name) {
            return true;
        }
        return items.iter().any(|item| named_let_contains_call(name, item));
    }
    false
}

fn named_let_branch_safe_for_loop(name: &str, forms: &[Value]) -> bool {
    named_let_tail_call(name, forms).is_some()
        || forms
            .iter()
            .all(|form| !named_let_contains_call(name, form))
}

pub(crate) fn error_condition_value(error: &LispError) -> Value {
    match error {
        LispError::TypeError(expected, got) => Value::list([
            Value::Symbol("wrong-type-argument".into()),
            Value::Symbol(expected.clone()),
            match got.as_str() {
                "nil" => Value::Nil,
                _ => Value::String(got.clone()),
            },
        ]),
        LispError::Void(symbol) => Value::list([
            Value::Symbol("void-variable".into()),
            Value::Symbol(symbol.clone()),
        ]),
        LispError::WrongNumberOfArgs(name, count) => Value::list([
            Value::Symbol("wrong-number-of-arguments".into()),
            Value::Symbol(name.clone()),
            Value::Integer(*count as i64),
        ]),
        LispError::EndOfInput => Value::list([Value::Symbol("end-of-file".into()), Value::Nil]),
        LispError::TestSkipped(message) => Value::list([
            Value::Symbol("ert-test-skipped".into()),
            Value::String(message.clone()),
        ]),
        LispError::ErtTestFailed(message) => Value::list([
            Value::Symbol("ert-test-failed".into()),
            Value::String(message.clone()),
        ]),
        LispError::ReadError(message) | LispError::Signal(message) => Value::list([
            Value::Symbol("error".into()),
            Value::String(message.clone()),
        ]),
        LispError::Throw(tag, value) => {
            Value::list([Value::Symbol("no-catch".into()), tag.clone(), value.clone()])
        }
        LispError::SignalValue(value) => value.clone(),
    }
}

fn buffer_undo_head_to_entry(value: &Value) -> crate::buffer::UndoEntry {
    match value {
        Value::Nil => crate::buffer::UndoEntry::Boundary,
        Value::Cons(_, _) => match value.cons_values() {
            Some((Value::Integer(pos), Value::Integer(len))) if pos >= 0 && len >= 0 => {
                crate::buffer::UndoEntry::Insert {
                    pos: pos as usize,
                    len: len as usize,
                }
            }
            Some((Value::String(text), Value::Integer(pos))) if pos >= 0 => {
                crate::buffer::UndoEntry::Delete {
                    pos: pos as usize,
                    text,
                    props: Vec::new(),
                    markers: Vec::new(),
                }
            }
            _ => crate::buffer::UndoEntry::Opaque(value.clone()),
        },
        _ => crate::buffer::UndoEntry::Opaque(value.clone()),
    }
}

fn combined_undo_display(entries: &[crate::buffer::UndoEntry]) -> Value {
    Value::list([
        Value::Symbol("apply".into()),
        Value::Integer(2),
        Value::Integer(1),
        Value::Integer(1),
        Value::Symbol("undo--wrap-and-run-primitive-undo".into()),
        Value::Integer(1),
        Value::Integer(1),
        Value::list(entries.iter().map(undo_entry_display)),
    ])
}

fn undo_entry_display(entry: &crate::buffer::UndoEntry) -> Value {
    match entry {
        crate::buffer::UndoEntry::Insert { pos, len } => {
            Value::cons(Value::Integer(*pos as i64), Value::Integer(*len as i64))
        }
        crate::buffer::UndoEntry::Delete { pos, text, .. } => {
            Value::cons(Value::String(text.clone()), Value::Integer(*pos as i64))
        }
        crate::buffer::UndoEntry::Combined { display, .. }
        | crate::buffer::UndoEntry::Opaque(display) => display.clone(),
        crate::buffer::UndoEntry::Boundary => Value::Nil,
    }
}

fn latest_generated_undo_group(
    entries: &[crate::buffer::UndoEntry],
) -> Vec<crate::buffer::UndoEntry> {
    entries
        .iter()
        .filter(|entry| !matches!(entry, crate::buffer::UndoEntry::Boundary))
        .cloned()
        .collect()
}

fn render_undo_value(value: &Value) -> String {
    match value {
        Value::Nil => "nil".into(),
        Value::T => "t".into(),
        Value::Integer(n) => n.to_string(),
        Value::BigInteger(n) => n.to_string(),
        Value::Float(n) => {
            if n.fract() == 0.0 {
                format!("{n:.1}")
            } else {
                n.to_string()
            }
        }
        Value::String(s) => format!("\"{}\"", s),
        Value::StringObject(state) => format!("\"{}\"", state.borrow().text),
        Value::Symbol(s) => s.clone(),
        Value::Cons(_, _) => {
            let mut rendered = String::from("(");
            let mut current = value.clone();
            let mut first = true;
            loop {
                match current {
                    Value::Cons(car, cdr) => {
                        if !first {
                            rendered.push(' ');
                        }
                        rendered.push_str(&render_undo_value(&car.borrow()));
                        first = false;
                        current = cdr.borrow().clone();
                    }
                    Value::Nil => break,
                    other => {
                        rendered.push_str(" . ");
                        rendered.push_str(&render_undo_value(&other));
                        break;
                    }
                }
            }
            rendered.push(')');
            rendered
        }
        Value::BuiltinFunc(name) => format!("#<builtin {name}>"),
        Value::Lambda(params, _, _) => format!("#<lambda ({})>", params.join(" ")),
        Value::Buffer(_, name) => format!("#<buffer {name}>"),
        Value::Marker(id) => format!("#<marker id:{id}>"),
        Value::Overlay(id) => format!("#<overlay id:{id}>"),
        Value::CharTable(id) => format!("#<char-table id:{id}>"),
        Value::Record(id) => format!("#<record id:{id}>"),
        Value::Finalizer(id) => format!("#<finalizer id:{id}>"),
        Value::Unbound => "#<unbound>".into(),
    }
}

fn function_executable_body(body: &[Value]) -> &[Value] {
    let mut start = 0usize;
    if body.len() > 1
        && matches!(
            body.first(),
            Some(Value::String(_) | Value::StringObject(_))
        )
    {
        start = 1;
    }
    while start < body.len()
        && (is_function_declare_form(&body[start]) || is_function_interactive_form(&body[start]))
    {
        start += 1;
    }
    &body[start..]
}

fn is_function_declare_form(form: &Value) -> bool {
    form.to_vec().ok().is_some_and(
        |items| matches!(items.first(), Some(Value::Symbol(name)) if name == "declare"),
    )
}

fn function_declare_gv_setter(form: &Value) -> Option<String> {
    let items = form.to_vec().ok()?;
    if !matches!(items.first(), Some(Value::Symbol(name)) if name == "declare") {
        return None;
    }
    items[1..].iter().find_map(|declaration| {
        let declaration_items = declaration.to_vec().ok()?;
        match declaration_items.as_slice() {
            [Value::Symbol(kind), Value::Symbol(setter)] if kind == "gv-setter" => {
                Some(setter.clone())
            }
            _ => None,
        }
    })
}

fn is_function_interactive_form(form: &Value) -> bool {
    form.to_vec().ok().is_some_and(
        |items| matches!(items.first(), Some(Value::Symbol(name)) if name == "interactive"),
    )
}

fn is_vector_literal(value: &Value) -> bool {
    value.to_vec().ok().is_some_and(
        |items| matches!(items.first(), Some(Value::Symbol(name)) if name == "vector-literal"),
    )
}

fn is_bool_vector_literal(value: &Value) -> bool {
    value.to_vec().ok().is_some_and(
        |items| matches!(items.first(), Some(Value::Symbol(name)) if name == "bool-vector-literal"),
    )
}

fn is_record_literal_slot_form(value: &Value) -> bool {
    match value {
        Value::Nil
        | Value::T
        | Value::Integer(_)
        | Value::BigInteger(_)
        | Value::Float(_)
        | Value::String(_)
        | Value::StringObject(_)
        | Value::Buffer(_, _)
        | Value::Marker(_)
        | Value::Overlay(_)
        | Value::CharTable(_)
        | Value::Record(_)
        | Value::Finalizer(_)
        | Value::BuiltinFunc(_)
        | Value::Lambda(_, _, _)
        | Value::Unbound => true,
        Value::Cons(_, _) => {
            let Ok(items) = value.to_vec() else {
                return false;
            };
            matches!(items.as_slice(), [Value::Symbol(symbol), _] if symbol == "quote")
                || is_vector_literal(value)
                || is_bool_vector_literal(value)
                || is_record_literal_reader_form(value)
        }
        Value::Symbol(_) => false,
    }
}

fn is_record_literal_reader_form(value: &Value) -> bool {
    let Ok(items) = value.to_vec() else {
        return false;
    };
    matches!(items.first(), Some(Value::Symbol(name)) if name == RECORD_LITERAL_SYMBOL)
        && items[1..].iter().all(is_record_literal_slot_form)
}

fn is_quote_form(value: &Value) -> bool {
    value.to_vec().ok().is_some_and(
        |items| matches!(items.as_slice(), [Value::Symbol(symbol), _] if symbol == "quote"),
    )
}

fn is_backquote_atomic_cons_tail(value: &Value) -> bool {
    is_quote_form(value)
        || is_vector_literal(value)
        || is_bool_vector_literal(value)
        || is_record_literal_reader_form(value)
}

fn is_lambda_form(value: &Value) -> bool {
    value
        .to_vec()
        .ok()
        .is_some_and(|items| matches!(items.first(), Some(Value::Symbol(name)) if name == "lambda"))
}

fn wrong_type_argument(predicate: &str, value: Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("wrong-type-argument".into()),
        Value::Symbol(predicate.into()),
        value,
    ]))
}

fn load_file_missing_error(target: &str) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("file-missing".into()),
        Value::String("Cannot open load file".into()),
        Value::String("No such file or directory".into()),
        Value::String(target.into()),
    ]))
}

fn invalid_function(value: Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("invalid-function".into()),
        value,
    ]))
}

fn backquote_unquote_form(value: &Value) -> Option<(&'static str, Value)> {
    let items = value.to_vec().ok()?;
    match items.as_slice() {
        [Value::Symbol(symbol), value] if symbol == "comma" => Some(("comma", value.clone())),
        [Value::Symbol(symbol), value] if symbol == "comma-at" => Some(("comma-at", value.clone())),
        _ => None,
    }
}

fn nested_backquote_body(value: &Value) -> Option<Value> {
    let items = value.to_vec().ok()?;
    match items.as_slice() {
        [Value::Symbol(symbol), body] if symbol == "backquote" => Some(body.clone()),
        _ => None,
    }
}

fn defface_spec_literal(spec_form: &Value) -> Option<Value> {
    match spec_form {
        Value::Cons(_, _) => {
            let items = spec_form.to_vec().ok()?;
            match items.as_slice() {
                [Value::Symbol(symbol), value] if symbol == "quote" => Some(value.clone()),
                _ if items
                    .iter()
                    .all(|item| matches!(item, Value::Cons(_, _) | Value::Nil)) =>
                {
                    Some(spec_form.clone())
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn defface_runtime_attributes(spec: &Value) -> Option<Vec<(String, Value)>> {
    let clauses = spec.to_vec().ok()?;
    clauses
        .iter()
        .find_map(|clause| defface_clause_attributes(clause, true))
}

fn defface_clause_attributes(
    clause: &Value,
    require_default_clause: bool,
) -> Option<Vec<(String, Value)>> {
    let parts = clause.to_vec().ok()?;
    if parts.len() < 2 {
        return None;
    }
    if require_default_clause && !defface_matches_default_display(&parts[0]) {
        return None;
    }

    let attribute_source = if parts.len() == 2
        && matches!(&parts[1], Value::Cons(_, _))
        && parts[1].to_vec().ok().is_some_and(|items| {
            items
                .first()
                .and_then(|item| item.as_symbol().ok())
                .is_some_and(|symbol| symbol.starts_with(':'))
        }) {
        parts[1].to_vec().ok()?
    } else {
        parts[1..].to_vec()
    };

    let mut attributes = Vec::new();
    let mut index = 0;
    while index + 1 < attribute_source.len() {
        let attribute = attribute_source[index].as_symbol().ok()?;
        if attribute.starts_with(':') {
            attributes.push((attribute.to_string(), attribute_source[index + 1].clone()));
        }
        index += 2;
    }
    if attributes.is_empty() {
        None
    } else {
        Some(attributes)
    }
}

fn defface_matches_default_display(display: &Value) -> bool {
    matches!(display, Value::T)
        || matches!(display, Value::Symbol(symbol) if symbol == "t" || symbol == "default")
}

fn cons_list_with_tail(items: Vec<Value>, tail: Value) -> Value {
    let mut out = tail;
    for item in items.into_iter().rev() {
        out = Value::cons(item, out);
    }
    out
}

fn validate_lambda_list(spec: &Value, items: &[Value]) -> Result<(), LispError> {
    let mut seen_optional = false;
    let mut seen_rest = false;
    let mut needs_rest_arg = false;
    let mut rest_arg_seen = false;

    for item in items {
        let Value::Symbol(symbol) = item else {
            return Err(invalid_function(spec.clone()));
        };
        match symbol.as_str() {
            "&optional" => {
                if seen_optional || seen_rest {
                    return Err(invalid_function(spec.clone()));
                }
                seen_optional = true;
            }
            "&rest" => {
                if seen_rest {
                    return Err(invalid_function(spec.clone()));
                }
                seen_rest = true;
                needs_rest_arg = true;
            }
            _ => {
                if needs_rest_arg {
                    needs_rest_arg = false;
                    rest_arg_seen = true;
                } else if rest_arg_seen {
                    return Err(invalid_function(spec.clone()));
                }
            }
        }
    }

    if needs_rest_arg {
        return Err(invalid_function(spec.clone()));
    }

    Ok(())
}

struct LoweredClDefun {
    params: Vec<Value>,
    destructuring_bindings: Vec<(Value, String)>,
    keyword_rest_param: Option<String>,
    keyword_bindings: Vec<ClKeyBinding>,
}

struct ClKeyBinding {
    variable_name: String,
    keyword_name: String,
    default_value: Value,
    supplied_name: Option<String>,
}

fn lower_cl_defun_lambda_list(name: &str, spec: &Value) -> Result<LoweredClDefun, LispError> {
    let items = match spec {
        Value::Nil => Vec::new(),
        Value::Cons(_, _) => spec.to_vec()?,
        _ => return Err(invalid_function(spec.clone())),
    };

    let mut lowered = Vec::with_capacity(items.len());
    let mut destructuring_bindings = Vec::new();
    let mut keyword_bindings = Vec::new();
    let mut keyword_rest_param = None;
    let mut in_key_section = false;
    let mut expecting_rest_name = false;

    for (index, item) in items.into_iter().enumerate() {
        match item {
            Value::Symbol(symbol) => match symbol.as_str() {
                "&optional" => {
                    if in_key_section {
                        return Err(LispError::Signal(
                            "Unsupported cl-defun lambda list keyword: &optional".into(),
                        ));
                    }
                    lowered.push(Value::Symbol(symbol));
                }
                "&rest" => {
                    if in_key_section {
                        return Err(LispError::Signal(
                            "Unsupported cl-defun lambda list keyword: &rest".into(),
                        ));
                    }
                    lowered.push(Value::Symbol(symbol));
                    expecting_rest_name = true;
                }
                "&body" => {
                    if in_key_section {
                        return Err(LispError::Signal(
                            "Unsupported cl-defun lambda list keyword: &body".into(),
                        ));
                    }
                    lowered.push(Value::Symbol("&rest".into()));
                    expecting_rest_name = true;
                }
                "&key" => {
                    if expecting_rest_name {
                        return Err(invalid_function(spec.clone()));
                    }
                    in_key_section = true;
                    if keyword_rest_param.is_none() {
                        let temp_name = format!("emaxx--cl-defun-{name}-keys");
                        lowered.push(Value::Symbol("&rest".into()));
                        lowered.push(Value::Symbol(temp_name.clone()));
                        keyword_rest_param = Some(temp_name);
                    }
                }
                "&allow-other-keys" if in_key_section => {}
                "&aux" | "&whole" | "&environment" => {
                    return Err(LispError::Signal(format!(
                        "Unsupported cl-defun lambda list keyword: {symbol}"
                    )));
                }
                _ if in_key_section => {
                    keyword_bindings.push(ClKeyBinding {
                        variable_name: symbol.clone(),
                        keyword_name: format!(":{symbol}"),
                        default_value: Value::Nil,
                        supplied_name: None,
                    });
                }
                _ => {
                    if expecting_rest_name {
                        keyword_rest_param = Some(symbol.clone());
                        expecting_rest_name = false;
                    }
                    lowered.push(Value::Symbol(symbol));
                }
            },
            Value::Cons(_, _) if in_key_section => {
                keyword_bindings.push(parse_cl_defun_key_binding(item)?);
            }
            Value::Cons(_, _) => {
                let temp_name = format!("emaxx--cl-defun-{name}-arg-{index}");
                lowered.push(Value::Symbol(temp_name.clone()));
                if expecting_rest_name {
                    keyword_rest_param = Some(temp_name.clone());
                    expecting_rest_name = false;
                }
                destructuring_bindings.push((item, temp_name));
            }
            _ => return Err(invalid_function(spec.clone())),
        }
    }

    if expecting_rest_name {
        return Err(invalid_function(spec.clone()));
    }

    Ok(LoweredClDefun {
        params: lowered,
        destructuring_bindings,
        keyword_rest_param,
        keyword_bindings,
    })
}

fn parse_cl_defun_key_binding(spec: Value) -> Result<ClKeyBinding, LispError> {
    let items = spec.to_vec()?;
    if items.is_empty() {
        return Err(LispError::Signal(
            "Unsupported cl-defun &key binding".into(),
        ));
    }

    let (keyword_name, variable_name, default_value, supplied_name) = match items.as_slice() {
        [Value::Symbol(variable_name)] => (
            format!(":{variable_name}"),
            variable_name.clone(),
            Value::Nil,
            None,
        ),
        [Value::Symbol(variable_name), default_value] => (
            format!(":{variable_name}"),
            variable_name.clone(),
            default_value.clone(),
            None,
        ),
        [
            Value::Symbol(variable_name),
            default_value,
            Value::Symbol(supplied_name),
        ] => (
            format!(":{variable_name}"),
            variable_name.clone(),
            default_value.clone(),
            Some(supplied_name.clone()),
        ),
        [pattern @ Value::Cons(_, _)] => {
            let pair = pattern.to_vec()?;
            let [Value::Symbol(keyword_name), Value::Symbol(variable_name)] = pair.as_slice()
            else {
                return Err(LispError::Signal(
                    "Unsupported cl-defun &key binding".into(),
                ));
            };
            (
                normalize_cl_defun_keyword(keyword_name),
                variable_name.clone(),
                Value::Nil,
                None,
            )
        }
        [pattern @ Value::Cons(_, _), default_value] => {
            let pair = pattern.to_vec()?;
            let [Value::Symbol(keyword_name), Value::Symbol(variable_name)] = pair.as_slice()
            else {
                return Err(LispError::Signal(
                    "Unsupported cl-defun &key binding".into(),
                ));
            };
            (
                normalize_cl_defun_keyword(keyword_name),
                variable_name.clone(),
                default_value.clone(),
                None,
            )
        }
        [
            pattern @ Value::Cons(_, _),
            default_value,
            Value::Symbol(supplied_name),
        ] => {
            let pair = pattern.to_vec()?;
            let [Value::Symbol(keyword_name), Value::Symbol(variable_name)] = pair.as_slice()
            else {
                return Err(LispError::Signal(
                    "Unsupported cl-defun &key binding".into(),
                ));
            };
            (
                normalize_cl_defun_keyword(keyword_name),
                variable_name.clone(),
                default_value.clone(),
                Some(supplied_name.clone()),
            )
        }
        _ => {
            return Err(LispError::Signal(
                "Unsupported cl-defun &key binding".into(),
            ));
        }
    };

    Ok(ClKeyBinding {
        variable_name,
        keyword_name,
        default_value,
        supplied_name,
    })
}

fn normalize_cl_defun_keyword(name: &str) -> String {
    if name.starts_with(':') {
        name.to_string()
    } else {
        format!(":{name}")
    }
}

fn is_lambda_list_keyword(symbol: &str) -> bool {
    matches!(
        symbol,
        "&optional" | "&rest" | "&body" | "&key" | "&allow-other-keys" | "&aux"
    )
}

fn lower_cl_defmethod_lambda_list(spec: &Value) -> Result<Value, LispError> {
    let items = spec.to_vec()?;
    let mut lowered = Vec::with_capacity(items.len());
    let mut skipping_context = false;

    for item in items {
        match item {
            Value::Symbol(symbol) if symbol == "&context" => {
                skipping_context = true;
            }
            Value::Symbol(symbol) => {
                if skipping_context {
                    if is_lambda_list_keyword(&symbol) {
                        skipping_context = false;
                    } else {
                        continue;
                    }
                }
                lowered.push(Value::Symbol(symbol));
            }
            Value::Cons(_, _) => {
                if skipping_context {
                    continue;
                }
                let parts = item.to_vec()?;
                if let Some(Value::Symbol(variable_name)) = parts.first() {
                    lowered.push(Value::Symbol(variable_name.clone()));
                } else {
                    lowered.push(item);
                }
            }
            other => {
                if !skipping_context {
                    lowered.push(other);
                }
            }
        }
    }

    Ok(Value::list(lowered))
}

fn first_cl_defmethod_specializer(spec: &Value) -> Result<Option<(String, String)>, LispError> {
    for item in spec.to_vec()? {
        let Value::Cons(_, _) = item else {
            continue;
        };
        let parts = item.to_vec()?;
        let Some(Value::Symbol(variable)) = parts.first() else {
            continue;
        };
        let Some(Value::Symbol(class_name)) = parts.get(1) else {
            continue;
        };
        return Ok(Some((variable.clone(), class_name.clone())));
    }
    Ok(None)
}

fn substitute_symbol_macros(
    form: &Value,
    expansions: &HashMap<String, Value>,
) -> Result<Value, LispError> {
    match form {
        Value::Symbol(symbol) => Ok(expansions
            .get(symbol)
            .cloned()
            .unwrap_or_else(|| form.clone())),
        Value::Cons(_, _) => substitute_symbol_macros_in_list(form, expansions),
        _ => Ok(form.clone()),
    }
}

fn substitute_symbol_macros_in_list(
    form: &Value,
    expansions: &HashMap<String, Value>,
) -> Result<Value, LispError> {
    let items = form.to_vec()?;
    let Some(Value::Symbol(head)) = items.first() else {
        return items
            .iter()
            .map(|item| substitute_symbol_macros(item, expansions))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::list);
    };
    match head.as_str() {
        "quote" | "function" => Ok(form.clone()),
        "lambda" => substitute_symbol_macros_in_lambda(&items, expansions),
        "let" => substitute_symbol_macros_in_let(&items, expansions, false),
        "let*" => substitute_symbol_macros_in_let(&items, expansions, true),
        "setq" => substitute_symbol_macros_in_setq(&items, expansions),
        _ => items
            .iter()
            .map(|item| substitute_symbol_macros(item, expansions))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::list),
    }
}

fn substitute_symbol_macros_in_lambda(
    items: &[Value],
    expansions: &HashMap<String, Value>,
) -> Result<Value, LispError> {
    let Some(params) = items.get(1) else {
        return Ok(Value::list(items.iter().cloned()));
    };
    let scoped = symbol_macro_expansions_without_bindings(expansions, params)?;
    let mut rewritten = Vec::with_capacity(items.len());
    rewritten.extend(items[..2].iter().cloned());
    for form in &items[2..] {
        rewritten.push(substitute_symbol_macros(form, &scoped)?);
    }
    Ok(Value::list(rewritten))
}

fn substitute_symbol_macros_in_let(
    items: &[Value],
    expansions: &HashMap<String, Value>,
    sequential: bool,
) -> Result<Value, LispError> {
    let Some(bindings_value) = items.get(1) else {
        return Ok(Value::list(items.iter().cloned()));
    };
    let bindings = bindings_value.to_vec()?;
    let mut scoped = expansions.clone();
    let mut rewritten_bindings = Vec::with_capacity(bindings.len());
    for binding in &bindings {
        match binding {
            Value::Symbol(symbol) => {
                scoped.remove(symbol);
                rewritten_bindings.push(Value::Symbol(symbol.clone()));
            }
            Value::Cons(_, _) => {
                let parts = binding.to_vec()?;
                let Some(Value::Symbol(symbol)) = parts.first() else {
                    rewritten_bindings.push(substitute_symbol_macros(binding, &scoped)?);
                    continue;
                };
                let init_scope = if sequential { &scoped } else { expansions };
                let mut rewritten = Vec::with_capacity(parts.len());
                rewritten.push(Value::Symbol(symbol.clone()));
                for form in &parts[1..] {
                    rewritten.push(substitute_symbol_macros(form, init_scope)?);
                }
                scoped.remove(symbol);
                rewritten_bindings.push(Value::list(rewritten));
            }
            other => rewritten_bindings.push(substitute_symbol_macros(other, &scoped)?),
        }
    }
    let body_scope = if sequential {
        scoped
    } else {
        let mut body_scope = expansions.clone();
        for binding in &bindings {
            match binding {
                Value::Symbol(symbol) => {
                    body_scope.remove(symbol);
                }
                Value::Cons(_, _) => {
                    if let Ok(parts) = binding.to_vec()
                        && let Some(Value::Symbol(symbol)) = parts.first()
                    {
                        body_scope.remove(symbol);
                    }
                }
                _ => {}
            }
        }
        body_scope
    };
    let mut rewritten = Vec::with_capacity(items.len());
    rewritten.push(items[0].clone());
    rewritten.push(Value::list(rewritten_bindings));
    for form in &items[2..] {
        rewritten.push(substitute_symbol_macros(form, &body_scope)?);
    }
    Ok(Value::list(rewritten))
}

fn substitute_symbol_macros_in_setq(
    items: &[Value],
    expansions: &HashMap<String, Value>,
) -> Result<Value, LispError> {
    let mut rewritten = Vec::new();
    let mut index = 1;
    while index + 1 < items.len() {
        if let Some(symbol) = items[index].as_symbol().ok()
            && let Some(expansion) = expansions.get(symbol)
        {
            rewritten.push(Value::list([
                Value::Symbol("setf".into()),
                expansion.clone(),
                substitute_symbol_macros(&items[index + 1], expansions)?,
            ]));
        } else {
            rewritten.push(Value::list([
                Value::Symbol("setq".into()),
                items[index].clone(),
                substitute_symbol_macros(&items[index + 1], expansions)?,
            ]));
        }
        index += 2;
    }
    Ok(match rewritten.len() {
        0 => Value::Nil,
        1 => rewritten.pop().unwrap_or(Value::Nil),
        _ => {
            let mut progn = Vec::with_capacity(rewritten.len() + 1);
            progn.push(Value::Symbol("progn".into()));
            progn.extend(rewritten);
            Value::list(progn)
        }
    })
}

fn symbol_macro_expansions_without_bindings(
    expansions: &HashMap<String, Value>,
    params: &Value,
) -> Result<HashMap<String, Value>, LispError> {
    let mut scoped = expansions.clone();
    for item in params.to_vec()? {
        if let Ok(symbol) = item.as_symbol()
            && !is_lambda_list_keyword(symbol)
        {
            scoped.remove(symbol);
        }
    }
    Ok(scoped)
}

fn lower_define_inline_form(value: &Value) -> Value {
    let Ok(items) = value.to_vec() else {
        return value.clone();
    };
    let Some(Value::Symbol(head)) = items.first() else {
        return value.clone();
    };
    match head.as_str() {
        "inline-quote" => items
            .get(1)
            .map(lower_inline_quote_form)
            .unwrap_or(Value::Nil),
        "inline-letevals" => lower_inline_progn(&items[2..]),
        "inline-const-val" => items
            .get(1)
            .map(lower_define_inline_form)
            .unwrap_or(Value::Nil),
        "inline-const-p" => Value::T,
        "inline-error" => {
            let mut lowered = vec![Value::Symbol("error".into())];
            lowered.extend(items[1..].iter().map(lower_define_inline_form));
            Value::list(lowered)
        }
        _ => Value::list(
            items
                .into_iter()
                .map(|item| lower_define_inline_form(&item)),
        ),
    }
}

fn lower_inline_quote_form(value: &Value) -> Value {
    let Ok(items) = value.to_vec() else {
        return value.clone();
    };
    let Some(Value::Symbol(head)) = items.first() else {
        return value.clone();
    };
    match head.as_str() {
        "comma" => items
            .get(1)
            .map(lower_define_inline_form)
            .unwrap_or(Value::Nil),
        "quote" if items.len() == 2 => match items[1].to_vec() {
            Ok(quoted) if matches!(quoted.first(), Some(Value::Symbol(name)) if name == "comma") => {
                quoted
                    .get(1)
                    .map(lower_define_inline_form)
                    .unwrap_or(Value::Nil)
            }
            _ => Value::list([Value::Symbol("quote".into()), items[1].clone()]),
        },
        "function" | "function-quote" if items.len() == 2 => match items[1].to_vec() {
            Ok(quoted) if matches!(quoted.first(), Some(Value::Symbol(name)) if name == "comma") => {
                quoted
                    .get(1)
                    .map(lower_define_inline_form)
                    .unwrap_or(Value::Nil)
            }
            _ => Value::list([Value::Symbol(head.clone()), items[1].clone()]),
        },
        _ => Value::list(items.into_iter().map(|item| lower_inline_quote_form(&item))),
    }
}

fn lower_inline_progn(forms: &[Value]) -> Value {
    match forms {
        [] => Value::Nil,
        [single] => lower_define_inline_form(single),
        many => Value::list(
            std::iter::once(Value::Symbol("progn".into()))
                .chain(many.iter().map(lower_define_inline_form)),
        ),
    }
}

fn setcdr_tail_aliases(
    interp: &Interpreter,
    value: &Value,
    tail: &Value,
    env: &Env,
) -> Vec<String> {
    let mut aliases = Vec::new();
    collect_setcdr_tail_aliases(interp, value, tail, env, &mut aliases);
    aliases
}

fn collect_setcdr_tail_aliases(
    interp: &Interpreter,
    value: &Value,
    tail: &Value,
    env: &Env,
    aliases: &mut Vec<String>,
) {
    let Ok(items) = value.to_vec() else {
        return;
    };
    if matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "setcdr")
        && let Some(Value::Symbol(name)) = items.get(1)
        && interp.lookup_var(name, env).as_ref() == Some(tail)
        && !aliases.iter().any(|alias| alias == name)
    {
        aliases.push(name.clone());
    }
    for item in &items {
        collect_setcdr_tail_aliases(interp, item, tail, env, aliases);
    }
}

fn tail_aliases_became_improper(interp: &Interpreter, aliases: &[String], env: &Env) -> bool {
    aliases.iter().any(|name| {
        interp
            .lookup_var(name, env)
            .is_some_and(|value| value.to_vec().is_err())
    })
}

fn snapshot_tail_alias_values(
    interp: &Interpreter,
    aliases: &[String],
    env: &Env,
) -> Vec<(String, Value)> {
    aliases
        .iter()
        .filter_map(|name| {
            interp
                .lookup_var(name, env)
                .map(|value| (name.clone(), deep_copy_value(&value)))
        })
        .collect()
}

fn restore_tail_alias_values(interp: &mut Interpreter, aliases: &[(String, Value)], env: &mut Env) {
    for (name, value) in aliases {
        interp.set_variable(name, value.clone(), env);
    }
}

fn deep_copy_value(value: &Value) -> Value {
    match value {
        Value::Cons(car, cdr) => Value::cons(
            deep_copy_value(&car.borrow()),
            deep_copy_value(&cdr.borrow()),
        ),
        _ => value.clone(),
    }
}

fn parse_cl_defstruct_constructor_params(items: Vec<Value>) -> (Vec<String>, Vec<(String, Value)>) {
    let mut params = Vec::new();
    let mut aux_bindings = Vec::new();
    let mut in_aux = false;
    for item in items {
        if matches!(&item, Value::Symbol(name) if name == "&aux") {
            in_aux = true;
            continue;
        }
        if in_aux {
            match item {
                Value::Symbol(name) => aux_bindings.push((name, Value::Nil)),
                Value::Cons(_, _) => {
                    if let Ok(parts) = item.to_vec()
                        && let Some(name) = parts.first().and_then(|value| value.as_symbol().ok())
                    {
                        aux_bindings.push((
                            name.to_string(),
                            parts.get(1).cloned().unwrap_or(Value::Nil),
                        ));
                    }
                }
                _ => {}
            }
        } else if let Ok(name) = item.as_symbol() {
            params.push(name.to_string());
        }
    }
    (params, aux_bindings)
}

fn pcase_pattern_bindings(
    interp: &mut Interpreter,
    env: &mut Env,
    pattern: &Value,
    value: &Value,
    bindings: &mut Vec<(String, Value)>,
) -> Result<bool, LispError> {
    pcase_pattern_bindings_with_mode(interp, env, pattern, value, bindings, false)
}

fn pcase_pattern_bindings_lenient_list(
    interp: &mut Interpreter,
    env: &mut Env,
    pattern: &Value,
    value: &Value,
    bindings: &mut Vec<(String, Value)>,
) -> Result<bool, LispError> {
    pcase_pattern_bindings_with_mode(interp, env, pattern, value, bindings, true)
}

fn pcase_pattern_bindings_with_mode(
    interp: &mut Interpreter,
    env: &mut Env,
    pattern: &Value,
    value: &Value,
    bindings: &mut Vec<(String, Value)>,
    lenient_list_match: bool,
) -> Result<bool, LispError> {
    pcase_pattern_bindings_inner(
        interp,
        env,
        pattern,
        value,
        bindings,
        lenient_list_match,
        false,
    )
}

fn pcase_pattern_bindings_inner(
    interp: &mut Interpreter,
    env: &mut Env,
    pattern: &Value,
    value: &Value,
    bindings: &mut Vec<(String, Value)>,
    lenient_list_match: bool,
    backquoted: bool,
) -> Result<bool, LispError> {
    if !backquoted && matches!(pattern, Value::Symbol(name) if name == "_") {
        return Ok(true);
    }
    if let Value::Symbol(name) = pattern
        && name != "nil"
        && name != "t"
    {
        if backquoted {
            return Ok(pattern == value);
        }
        bindings.push((name.clone(), value.clone()));
        return Ok(true);
    }
    if let Ok(parts) = pattern.to_vec() {
        if matches!(parts.first(), Some(Value::Symbol(name)) if name == "backquote") {
            return pcase_pattern_bindings_inner(
                interp,
                env,
                parts.get(1).unwrap_or(&Value::Nil),
                value,
                bindings,
                lenient_list_match,
                true,
            );
        }
        if backquoted {
            if matches!(parts.first(), Some(Value::Symbol(name)) if name == "comma" || name == "comma-at")
                && let Some(Value::Symbol(name)) = parts.get(1)
            {
                bindings.push((name.clone(), value.clone()));
                return Ok(true);
            }
        } else {
            if matches!(parts.first(), Some(Value::Symbol(name)) if name == "or") {
                let original = bindings.clone();
                for candidate in &parts[1..] {
                    let mut trial = original.clone();
                    if pcase_pattern_bindings_inner(
                        interp,
                        env,
                        candidate,
                        value,
                        &mut trial,
                        lenient_list_match,
                        backquoted,
                    )? {
                        *bindings = trial;
                        return Ok(true);
                    }
                }
                *bindings = original;
                return Ok(false);
            }
            if matches!(parts.first(), Some(Value::Symbol(name)) if name == "and") {
                let start = bindings.len();
                for candidate in &parts[1..] {
                    if !pcase_pattern_bindings_inner(
                        interp,
                        env,
                        candidate,
                        value,
                        bindings,
                        lenient_list_match,
                        backquoted,
                    )? {
                        bindings.truncate(start);
                        return Ok(false);
                    }
                }
                return Ok(true);
            }
            if matches!(parts.first(), Some(Value::Symbol(name)) if name == "let")
                && parts.len() >= 3
            {
                env.push(bindings.clone());
                let evaluated = interp.eval(&parts[2], env);
                env.pop();
                return pcase_pattern_bindings_inner(
                    interp,
                    env,
                    &parts[1],
                    &evaluated?,
                    bindings,
                    lenient_list_match,
                    backquoted,
                );
            }
            if matches!(parts.first(), Some(Value::Symbol(name)) if name == "guard")
                && parts.len() >= 2
            {
                env.push(bindings.clone());
                let guard = interp.eval(&parts[1], env);
                env.pop();
                return Ok(guard?.is_truthy());
            }
            if matches!(parts.first(), Some(Value::Symbol(name)) if name == "pred")
                && parts.len() >= 2
            {
                let (negated, predicate_form) = if let Ok(predicate_parts) = parts[1].to_vec() {
                    if matches!(predicate_parts.first(), Some(Value::Symbol(name)) if name == "not")
                        && predicate_parts.len() >= 2
                    {
                        (true, predicate_parts[1].clone())
                    } else {
                        (false, parts[1].clone())
                    }
                } else {
                    (false, parts[1].clone())
                };
                let predicate = pcase_predicate_function(interp, env, &predicate_form)?;
                let matches = crate::lisp::primitives::call_function_value(
                    interp,
                    &predicate,
                    std::slice::from_ref(value),
                    env,
                )?
                .is_truthy();
                return Ok(if negated { !matches } else { matches });
            }
            if matches!(parts.first(), Some(Value::Symbol(name)) if name == "cl-struct")
                && parts.len() >= 2
            {
                let Some(type_name) = parts.get(1).and_then(|value| value.as_symbol().ok()) else {
                    return Ok(false);
                };
                let Value::Record(record_id) = value else {
                    return Ok(false);
                };
                let Some(record) = interp.find_record(*record_id) else {
                    return Ok(false);
                };
                if record.type_name != type_name {
                    return Ok(false);
                }
                let slots = record.slots.clone();
                let slot_names = interp
                    .get_symbol_property(type_name, "emaxx-struct-slots")
                    .and_then(|value| value.to_vec().ok())
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|value| value.as_symbol().ok().map(str::to_string))
                    .collect::<Vec<_>>();
                let start = bindings.len();
                for slot_pattern in &parts[2..] {
                    let (slot_name, nested_pattern) = match slot_pattern {
                        Value::Symbol(name) => (name.clone(), slot_pattern.clone()),
                        Value::Cons(_, _) => {
                            let Ok(slot_parts) = slot_pattern.to_vec() else {
                                bindings.truncate(start);
                                return Ok(false);
                            };
                            let Some(slot_name) =
                                slot_parts.first().and_then(|value| value.as_symbol().ok())
                            else {
                                bindings.truncate(start);
                                return Ok(false);
                            };
                            (
                                slot_name.to_string(),
                                slot_parts
                                    .get(1)
                                    .cloned()
                                    .unwrap_or_else(|| slot_pattern.clone()),
                            )
                        }
                        _ => {
                            bindings.truncate(start);
                            return Ok(false);
                        }
                    };
                    let Some(slot_index) = slot_names.iter().position(|name| name == &slot_name)
                    else {
                        bindings.truncate(start);
                        return Ok(false);
                    };
                    let slot_value = slots.get(slot_index).cloned().unwrap_or(Value::Nil);
                    if !pcase_pattern_bindings_inner(
                        interp,
                        env,
                        &nested_pattern,
                        &slot_value,
                        bindings,
                        lenient_list_match,
                        backquoted,
                    )? {
                        bindings.truncate(start);
                        return Ok(false);
                    }
                }
                return Ok(true);
            }
            if matches!(parts.first(), Some(Value::Symbol(name)) if name == "seq") {
                let values = value.to_vec().unwrap_or_default();
                let start = bindings.len();
                let mut value_index = 0usize;
                let mut pattern_index = 1usize;
                while pattern_index < parts.len() {
                    if matches!(&parts[pattern_index], Value::Symbol(name) if name == "&rest") {
                        let Some(rest_pattern) = parts.get(pattern_index + 1) else {
                            bindings.truncate(start);
                            return Ok(false);
                        };
                        let rest = Value::list(values[value_index..].iter().cloned());
                        if !pcase_pattern_bindings_inner(
                            interp,
                            env,
                            rest_pattern,
                            &rest,
                            bindings,
                            lenient_list_match,
                            backquoted,
                        )? {
                            bindings.truncate(start);
                            return Ok(false);
                        }
                        return Ok(true);
                    }
                    let item = values.get(value_index).cloned().unwrap_or(Value::Nil);
                    if !pcase_pattern_bindings_inner(
                        interp,
                        env,
                        &parts[pattern_index],
                        &item,
                        bindings,
                        lenient_list_match,
                        backquoted,
                    )? {
                        bindings.truncate(start);
                        return Ok(false);
                    }
                    value_index += 1;
                    pattern_index += 1;
                }
                return Ok(true);
            }
            if matches!(parts.first(), Some(Value::Symbol(name)) if name == "quote") {
                return Ok(parts.get(1).is_some_and(|quoted| quoted == value));
            }
            if matches!(parts.first(), Some(Value::Symbol(name)) if name == "comma" || name == "comma-at")
                && let Some(Value::Symbol(name)) = parts.get(1)
            {
                bindings.push((name.clone(), value.clone()));
                return Ok(true);
            }
        }
    }

    match (pattern, value) {
        (Value::Cons(pattern_car, pattern_cdr), Value::Cons(value_car, value_cdr)) => {
            let start = bindings.len();
            let pattern_car = pattern_car.borrow().clone();
            let pattern_cdr = pattern_cdr.borrow().clone();
            let value_car = value_car.borrow().clone();
            let value_cdr = value_cdr.borrow().clone();
            if !pcase_pattern_bindings_inner(
                interp,
                env,
                &pattern_car,
                &value_car,
                bindings,
                lenient_list_match,
                backquoted,
            )? {
                bindings.truncate(start);
                return Ok(false);
            }
            if !pcase_pattern_bindings_inner(
                interp,
                env,
                &pattern_cdr,
                &value_cdr,
                bindings,
                lenient_list_match,
                backquoted,
            )? {
                bindings.truncate(start);
                return Ok(false);
            }
            Ok(true)
        }
        (Value::Cons(pattern_car, pattern_cdr), Value::Nil) if lenient_list_match => {
            let start = bindings.len();
            let pattern_car = pattern_car.borrow().clone();
            let pattern_cdr = pattern_cdr.borrow().clone();
            if !pcase_pattern_bindings_inner(
                interp,
                env,
                &pattern_car,
                &Value::Nil,
                bindings,
                lenient_list_match,
                backquoted,
            )? {
                bindings.truncate(start);
                return Ok(false);
            }
            if !pcase_pattern_bindings_inner(
                interp,
                env,
                &pattern_cdr,
                &Value::Nil,
                bindings,
                lenient_list_match,
                backquoted,
            )? {
                bindings.truncate(start);
                return Ok(false);
            }
            Ok(true)
        }
        (Value::Nil, Value::Cons(_, _)) if lenient_list_match => Ok(true),
        (Value::Nil, Value::Nil) => Ok(true),
        _ => Ok(pattern == value),
    }
}

fn pcase_predicate_function(
    interp: &mut Interpreter,
    env: &mut Env,
    predicate_form: &Value,
) -> Result<Value, LispError> {
    match interp.eval(predicate_form, env) {
        Ok(value) => Ok(value),
        Err(LispError::Void(_)) if matches!(predicate_form, Value::Symbol(_)) => {
            Ok(predicate_form.clone())
        }
        Err(error) => Err(error),
    }
}

fn is_compat_preloaded_feature(feature: &str) -> bool {
    matches!(
        feature,
        "cl-extra"
            | "cl-generic"
            | "cl-lib"
            | "cus-load"
            | "edmacro"
            | "ert-x"
            | "map"
            | "python"
            | "seq"
            | "subr-x"
            | "thread"
    )
}

fn build_signal_value(condition: Value, data: Value) -> Value {
    if let Ok(items) = data.to_vec() {
        Value::cons(condition, Value::list(items))
    } else {
        Value::list([condition, data])
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
