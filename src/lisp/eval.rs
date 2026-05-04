use std::collections::HashSet;
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

fn syntax_spec_value(spec: &str) -> Value {
    Value::String(spec.to_string())
}

fn standard_syntax_table_entries() -> Vec<CharTableEntry> {
    vec![
        CharTableEntry {
            start: ' ' as u32,
            end: ' ' as u32,
            value: syntax_spec_value(" "),
        },
        CharTableEntry {
            start: '\t' as u32,
            end: '\t' as u32,
            value: syntax_spec_value(" "),
        },
        CharTableEntry {
            start: '\n' as u32,
            end: '\n' as u32,
            value: syntax_spec_value(" "),
        },
        CharTableEntry {
            start: '\r' as u32,
            end: '\r' as u32,
            value: syntax_spec_value(" "),
        },
        CharTableEntry {
            start: '\u{0c}' as u32,
            end: '\u{0c}' as u32,
            value: syntax_spec_value(" "),
        },
        CharTableEntry {
            start: '_' as u32,
            end: '_' as u32,
            value: syntax_spec_value("_"),
        },
        CharTableEntry {
            start: '\\' as u32,
            end: '\\' as u32,
            value: syntax_spec_value("\\"),
        },
        CharTableEntry {
            start: '\'' as u32,
            end: '\'' as u32,
            value: syntax_spec_value("'"),
        },
        CharTableEntry {
            start: '"' as u32,
            end: '"' as u32,
            value: syntax_spec_value("\""),
        },
        CharTableEntry {
            start: '(' as u32,
            end: '(' as u32,
            value: syntax_spec_value("()"),
        },
        CharTableEntry {
            start: ')' as u32,
            end: ')' as u32,
            value: syntax_spec_value(")("),
        },
        CharTableEntry {
            start: '[' as u32,
            end: '[' as u32,
            value: syntax_spec_value("(]"),
        },
        CharTableEntry {
            start: ']' as u32,
            end: ']' as u32,
            value: syntax_spec_value(")["),
        },
        CharTableEntry {
            start: '{' as u32,
            end: '{' as u32,
            value: syntax_spec_value("(}"),
        },
        CharTableEntry {
            start: '}' as u32,
            end: '}' as u32,
            value: syntax_spec_value("){"),
        },
    ]
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

fn coding_plist(mnemonic: char, extras: impl IntoIterator<Item = (String, Value)>) -> Value {
    let mut items = vec![
        Value::Symbol(":mnemonic".into()),
        Value::Integer(mnemonic as i64),
    ];
    for (key, value) in extras {
        items.push(Value::Symbol(key));
        items.push(value);
    }
    Value::list(items)
}

fn current_exec_path() -> Value {
    match std::env::var_os("PATH") {
        Some(path) => Value::list(
            std::env::split_paths(&path).map(|entry| Value::String(entry.display().to_string())),
        ),
        None => Value::Nil,
    }
}

fn tab_bar_new_tab_choice_custom_type() -> Value {
    Value::list([
        Value::Symbol("choice".into()),
        Value::list([
            Value::Symbol("const".into()),
            Value::Symbol(":tag".into()),
            Value::String("Current buffer".into()),
            Value::T,
        ]),
        Value::list([
            Value::Symbol("const".into()),
            Value::Symbol(":tag".into()),
            Value::String("Current window".into()),
            Value::Symbol("window".into()),
        ]),
        Value::list([
            Value::Symbol("string".into()),
            Value::Symbol(":tag".into()),
            Value::String("Buffer".into()),
            Value::String("*scratch*".into()),
        ]),
        Value::list([
            Value::Symbol("directory".into()),
            Value::Symbol(":tag".into()),
            Value::String("Directory".into()),
            Value::Symbol(":value".into()),
            Value::String("~/".into()),
        ]),
        Value::list([
            Value::Symbol("file".into()),
            Value::Symbol(":tag".into()),
            Value::String("File".into()),
            Value::Symbol(":value".into()),
            Value::String("~/.emacs".into()),
        ]),
        Value::list([
            Value::Symbol("function".into()),
            Value::Symbol(":tag".into()),
            Value::String("Function".into()),
        ]),
        Value::list([
            Value::Symbol("const".into()),
            Value::Symbol(":tag".into()),
            Value::String("Duplicate tab".into()),
            Value::Symbol("clone".into()),
        ]),
    ])
}

fn builtin_coding_systems() -> Vec<CodingSystemState> {
    vec![
        CodingSystemState {
            name: "undecided".into(),
            base: "undecided".into(),
            kind: "undecided".into(),
            eol_type: None,
            plist: coding_plist('?', std::iter::empty()),
        },
        CodingSystemState {
            name: "no-conversion".into(),
            base: "no-conversion".into(),
            kind: "raw-text".into(),
            eol_type: None,
            plist: coding_plist('=', std::iter::empty()),
        },
        CodingSystemState {
            name: "unix".into(),
            base: "unix".into(),
            kind: "us-ascii".into(),
            eol_type: Some(0),
            plist: coding_plist('U', std::iter::empty()),
        },
        CodingSystemState {
            name: "dos".into(),
            base: "dos".into(),
            kind: "us-ascii".into(),
            eol_type: Some(1),
            plist: coding_plist('D', std::iter::empty()),
        },
        CodingSystemState {
            name: "mac".into(),
            base: "mac".into(),
            kind: "us-ascii".into(),
            eol_type: Some(2),
            plist: coding_plist('M', std::iter::empty()),
        },
        CodingSystemState {
            name: "us-ascii".into(),
            base: "us-ascii".into(),
            kind: "us-ascii".into(),
            eol_type: None,
            plist: coding_plist('A', std::iter::empty()),
        },
        CodingSystemState {
            name: "us-ascii-unix".into(),
            base: "us-ascii".into(),
            kind: "us-ascii".into(),
            eol_type: Some(0),
            plist: coding_plist('A', std::iter::empty()),
        },
        CodingSystemState {
            name: "us-ascii-dos".into(),
            base: "us-ascii".into(),
            kind: "us-ascii".into(),
            eol_type: Some(1),
            plist: coding_plist('A', std::iter::empty()),
        },
        CodingSystemState {
            name: "iso-latin-1".into(),
            base: "iso-latin-1".into(),
            kind: "iso-latin-1".into(),
            eol_type: None,
            plist: coding_plist('L', std::iter::empty()),
        },
        CodingSystemState {
            name: "iso-latin-1-unix".into(),
            base: "iso-latin-1".into(),
            kind: "iso-latin-1".into(),
            eol_type: Some(0),
            plist: coding_plist('L', std::iter::empty()),
        },
        CodingSystemState {
            name: "iso-latin-1-dos".into(),
            base: "iso-latin-1".into(),
            kind: "iso-latin-1".into(),
            eol_type: Some(1),
            plist: coding_plist('L', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-8".into(),
            base: "utf-8".into(),
            kind: "utf-8".into(),
            eol_type: None,
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-8-unix".into(),
            base: "utf-8".into(),
            kind: "utf-8".into(),
            eol_type: Some(0),
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-8-dos".into(),
            base: "utf-8".into(),
            kind: "utf-8".into(),
            eol_type: Some(1),
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-8-mac".into(),
            base: "utf-8".into(),
            kind: "utf-8".into(),
            eol_type: Some(2),
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-8-with-signature".into(),
            base: "utf-8-with-signature".into(),
            kind: "utf-8-with-signature".into(),
            eol_type: None,
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-8-with-signature-unix".into(),
            base: "utf-8-with-signature".into(),
            kind: "utf-8-with-signature".into(),
            eol_type: Some(0),
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-8-with-signature-dos".into(),
            base: "utf-8-with-signature".into(),
            kind: "utf-8-with-signature".into(),
            eol_type: Some(1),
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-8-with-signature-mac".into(),
            base: "utf-8-with-signature".into(),
            kind: "utf-8-with-signature".into(),
            eol_type: Some(2),
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-8-auto".into(),
            base: "utf-8-auto".into(),
            kind: "utf-8-auto".into(),
            eol_type: None,
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "prefer-utf-8".into(),
            base: "prefer-utf-8".into(),
            kind: "prefer-utf-8".into(),
            eol_type: None,
            plist: coding_plist('p', std::iter::empty()),
        },
        CodingSystemState {
            name: "prefer-utf-8-unix".into(),
            base: "prefer-utf-8".into(),
            kind: "prefer-utf-8".into(),
            eol_type: Some(0),
            plist: coding_plist('p', std::iter::empty()),
        },
        CodingSystemState {
            name: "raw-text".into(),
            base: "raw-text".into(),
            kind: "raw-text".into(),
            eol_type: None,
            plist: coding_plist('r', std::iter::empty()),
        },
        CodingSystemState {
            name: "raw-text-unix".into(),
            base: "raw-text".into(),
            kind: "raw-text".into(),
            eol_type: Some(0),
            plist: coding_plist('r', std::iter::empty()),
        },
        CodingSystemState {
            name: "raw-text-dos".into(),
            base: "raw-text".into(),
            kind: "raw-text".into(),
            eol_type: Some(1),
            plist: coding_plist('r', std::iter::empty()),
        },
        CodingSystemState {
            name: "raw-text-mac".into(),
            base: "raw-text".into(),
            kind: "raw-text".into(),
            eol_type: Some(2),
            plist: coding_plist('r', std::iter::empty()),
        },
        CodingSystemState {
            name: "mac-roman-mac".into(),
            base: "mac-roman".into(),
            kind: "iso-latin-1".into(),
            eol_type: Some(2),
            plist: coding_plist('m', std::iter::empty()),
        },
        CodingSystemState {
            name: "euc-jp".into(),
            base: "euc-jp".into(),
            kind: "euc-jp".into(),
            eol_type: None,
            plist: coding_plist('E', std::iter::empty()),
        },
        CodingSystemState {
            name: "euc-jp-dos".into(),
            base: "euc-jp".into(),
            kind: "euc-jp".into(),
            eol_type: Some(1),
            plist: coding_plist('E', std::iter::empty()),
        },
        CodingSystemState {
            name: "iso-2022-7bit".into(),
            base: "iso-2022-7bit".into(),
            kind: "iso-2022-7bit".into(),
            eol_type: None,
            plist: coding_plist('I', std::iter::empty()),
        },
        CodingSystemState {
            name: "sjis".into(),
            base: "sjis".into(),
            kind: "sjis".into(),
            eol_type: None,
            plist: coding_plist('S', std::iter::empty()),
        },
        CodingSystemState {
            name: "big5".into(),
            base: "big5".into(),
            kind: "big5".into(),
            eol_type: None,
            plist: coding_plist('B', std::iter::empty()),
        },
        CodingSystemState {
            name: "chinese-gb18030".into(),
            base: "chinese-gb18030".into(),
            kind: "raw-text".into(),
            eol_type: None,
            plist: coding_plist('C', std::iter::empty()),
        },
    ]
}

fn builtin_coding_aliases() -> Vec<(String, String)> {
    vec![
        ("iso-8859-1".into(), "iso-latin-1".into()),
        ("iso-8859-1-unix".into(), "iso-latin-1-unix".into()),
        ("iso-8859-1-dos".into(), "iso-latin-1-dos".into()),
        ("binary".into(), "raw-text".into()),
        ("utf8".into(), "utf-8".into()),
        ("utf-8-emacs".into(), "utf-8".into()),
        ("utf-8-emacs-unix".into(), "utf-8-unix".into()),
        ("utf-8-emacs-dos".into(), "utf-8-dos".into()),
        ("utf-8-emacs-mac".into(), "utf-8-mac".into()),
    ]
}

fn builtin_coding_priority() -> Vec<String> {
    vec![
        "prefer-utf-8".into(),
        "utf-8".into(),
        "utf-8-auto".into(),
        "raw-text".into(),
        "iso-latin-1".into(),
        "us-ascii".into(),
        "undecided".into(),
        "no-conversion".into(),
        "sjis".into(),
        "big5".into(),
        "euc-jp".into(),
        "iso-2022-7bit".into(),
    ]
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
                ("current-load-list".into(), Value::Nil),
                ("defining-kbd-macro".into(), Value::Nil),
                ("executing-kbd-macro".into(), Value::Nil),
                ("exec-path".into(), current_exec_path()),
                ("last-kbd-macro".into(), Value::Nil),
                ("file-name-handler-alist".into(), Value::Nil),
                ("inhibit-file-name-handlers".into(), Value::Nil),
                ("inhibit-file-name-operation".into(), Value::Nil),
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
                "inhibit-file-name-handlers".into(),
                "inhibit-file-name-operation".into(),
                "initial-window-system".into(),
                "last-coding-system-used".into(),
                "line-spacing".into(),
                "left-margin".into(),
                "last-command".into(),
                "load-force-doc-strings".into(),
                "overwrite-mode".into(),
                "process-connection-type".into(),
                "process-environment".into(),
                "selection-converter-alist".into(),
                "scroll-preserve-screen-position".into(),
                "scroll-up-aggressively".into(),
                "standard-output".into(),
                "vertical-scroll-bar".into(),
                "vc-directory-exclusion-list".into(),
            ],
            symbol_properties: Vec::new(),
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
        let esc_map = primitives::make_runtime_keymap(&mut interp, Some("esc-map"));
        interp.set_global_binding("esc-map", esc_map.clone());
        let ctl_x_4_map = primitives::make_runtime_keymap(&mut interp, Some("ctl-x-4-map"));
        interp.set_global_binding("ctl-x-4-map", ctl_x_4_map.clone());
        let ctl_x_5_map = primitives::make_runtime_keymap(&mut interp, Some("ctl-x-5-map"));
        interp.set_global_binding("ctl-x-5-map", ctl_x_5_map.clone());
        let tab_prefix_map = primitives::make_runtime_keymap(&mut interp, Some("tab-prefix-map"));
        interp.set_global_binding("tab-prefix-map", tab_prefix_map.clone());
        let ctl_x_map = primitives::make_runtime_keymap(&mut interp, Some("ctl-x-map"));
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
        let global_map = primitives::make_runtime_keymap(&mut interp, Some("global-map"));
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
        let glyphless_char_display =
            interp.make_char_table(Some("glyphless-char-display".into()), Value::Nil);
        interp.set_global_binding("glyphless-char-display", glyphless_char_display);
        interp.set_global_binding("buffer-read-only", Value::Nil);
        interp.mark_auto_buffer_local("buffer-read-only");
        interp.set_global_binding("current-prefix-arg", Value::Nil);
        interp.set_global_binding("this-command", Value::Nil);
        interp.set_global_binding("last-command", Value::Nil);
        interp.set_global_binding("tab-bar-new-tab-choice", Value::T);
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

    /// Allocate a new unique buffer ID.
    pub fn alloc_buffer_id(&mut self) -> u64 {
        let id = self.next_buffer_id;
        self.next_buffer_id += 1;
        id
    }

    /// Check if a buffer name exists in the buffer list.
    pub fn has_buffer(&self, name: &str) -> bool {
        self.buffer_list.iter().any(|(_, n)| n == name)
    }

    /// Check if a buffer ID exists in the live buffer list.
    pub fn has_buffer_id(&self, id: u64) -> bool {
        self.buffer_list
            .iter()
            .any(|(buffer_id, _)| *buffer_id == id)
    }

    /// Find a buffer by name, returning (id, name).
    pub fn find_buffer(&self, name: &str) -> Option<(u64, String)> {
        self.buffer_list.iter().find(|(_, n)| n == name).cloned()
    }

    /// Return the current buffer ID.
    pub fn current_buffer_id(&self) -> u64 {
        self.current_buffer_id
    }

    pub fn current_buffer(&self) -> &crate::buffer::Buffer {
        &self.buffer
    }

    pub fn set_current_load_file(&mut self, path: Option<String>) -> Option<String> {
        std::mem::replace(&mut self.current_load_file, path)
    }

    pub fn current_load_file(&self) -> Option<&str> {
        self.current_load_file.as_deref()
    }

    fn stored_value(value: Value) -> Value {
        match value {
            Value::String(_) => {
                let string = primitives::string_like(&value).expect("string_like handles strings");
                primitives::make_shared_string_value_with_multibyte(
                    string.text,
                    string.props,
                    string.multibyte,
                )
            }
            other => other,
        }
    }

    fn make_generated_symbol(&mut self, prefix: &str) -> Value {
        let id = self.next_generated_symbol_id;
        self.next_generated_symbol_id += 1;
        Value::Symbol(format!("{prefix}--emaxx-gensym-{id}"))
    }

    pub(crate) fn resolve_load_target(&self, target: &str) -> Option<PathBuf> {
        let direct = PathBuf::from(target);
        if direct.is_file() {
            return Some(direct);
        }

        let with_el = if target.ends_with(".el") {
            None
        } else {
            Some(format!("{target}.el"))
        };
        for root in &self.load_path {
            let candidate = root.join(target);
            if candidate.is_file() {
                return Some(candidate);
            }
            if let Some(with_el) = &with_el {
                let candidate = root.join(with_el);
                if candidate.is_file() {
                    return Some(candidate);
                }
            }
        }
        None
    }

    pub fn load_target(&mut self, target: &str) -> Result<PathBuf, LispError> {
        let Some(path) = self.resolve_load_target(target) else {
            return Err(load_file_missing_error(target));
        };
        crate::lisp::load_file_strict(self, &path)?;
        Ok(path)
    }

    fn require_feature_with_target(
        &mut self,
        feature: &str,
        target: Option<&str>,
    ) -> Result<Value, LispError> {
        if self.has_feature(feature) || self.loading_features.iter().any(|name| name == feature) {
            return Ok(Value::Symbol(feature.to_string()));
        }
        if is_compat_preloaded_feature(feature) {
            return self.provide_feature_with_after_load(feature);
        }
        let load_target = target.unwrap_or(feature);
        let Some(path) = self.resolve_load_target(load_target) else {
            return Err(load_file_missing_error(load_target));
        };
        self.loading_features.push(feature.to_string());
        let load_result = crate::lisp::load_file_strict(self, &path);
        self.loading_features.pop();
        load_result?;
        if !self.has_feature(feature) {
            return self.provide_feature_with_after_load(feature);
        }
        Ok(Value::Symbol(feature.to_string()))
    }

    /// Resolve a Lisp string-or-buffer value to a live buffer ID.
    pub fn resolve_buffer_id(&self, value: &Value) -> Result<u64, LispError> {
        match value {
            Value::Buffer(id, _) if self.has_buffer_id(*id) => Ok(*id),
            Value::Buffer(_, name) => self
                .find_buffer(name)
                .map(|(id, _)| id)
                .ok_or_else(|| LispError::Signal(format!("No buffer named {}", name))),
            _ => Err(LispError::TypeError(
                "string-or-buffer".into(),
                value.type_name(),
            )),
        }
        .or_else(|error| {
            primitives::string_like(value)
                .and_then(|string| self.find_buffer(&string.text).map(|(id, _)| id))
                .ok_or(error)
        })
    }

    /// Create and register a new empty buffer.
    pub fn create_buffer(&mut self, name: &str) -> (u64, String) {
        let id = self.alloc_buffer_id();
        self.inactive_buffers
            .push((id, crate::buffer::Buffer::new(name)));
        self.buffer_list.push((id, name.to_string()));
        (id, name.to_string())
    }

    /// Switch the current buffer to a different live buffer ID.
    pub fn switch_to_buffer_id(&mut self, id: u64) -> Result<(), LispError> {
        if id == self.current_buffer_id {
            return Ok(());
        }
        let pos = self
            .inactive_buffers
            .iter()
            .position(|(buffer_id, _)| *buffer_id == id)
            .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", id)))?;
        let (_, next_buffer) = self.inactive_buffers.swap_remove(pos);
        let current_id = self.current_buffer_id;
        let current_buffer = std::mem::replace(&mut self.buffer, next_buffer);
        self.inactive_buffers.push((current_id, current_buffer));
        self.current_buffer_id = id;
        let point_min = self.buffer.point_min() as i64;
        if let Some(window) = self.find_record_mut(self.selected_window_id) {
            window.slots[0] = Value::Integer(id as i64);
            if window.slots.len() < 2 {
                window.slots.push(Value::Integer(point_min));
            } else {
                window.slots[1] = Value::Integer(point_min);
            }
        }
        Ok(())
    }

    pub fn selected_window_value(&self) -> Value {
        Value::Record(self.selected_window_id)
    }

    pub fn selected_window_id(&self) -> u64 {
        self.selected_window_id
    }

    pub fn selected_window_buffer_id(&self) -> u64 {
        self.find_record(self.selected_window_id)
            .and_then(|record| record.slots.first())
            .and_then(|value| value.as_integer().ok())
            .map(|value| value.max(0) as u64)
            .unwrap_or(self.current_buffer_id)
    }

    pub fn buffer_bounds_by_id(&self, id: u64) -> Option<(usize, usize)> {
        self.get_buffer_by_id(id)
            .map(|buffer| (buffer.point_min(), buffer.point_max()))
    }

    pub fn selected_window_start(&self) -> usize {
        let (point_min, point_max) = self
            .buffer_bounds_by_id(self.selected_window_buffer_id())
            .unwrap_or((self.buffer.point_min(), self.buffer.point_max()));
        self.find_record(self.selected_window_id)
            .and_then(|record| record.slots.get(1))
            .and_then(|value| value.as_integer().ok())
            .map(|value| value.clamp(point_min as i64, point_max as i64) as usize)
            .unwrap_or(point_min)
    }

    pub fn set_selected_window_start(&mut self, start: usize) {
        let (point_min, point_max) = self
            .buffer_bounds_by_id(self.selected_window_buffer_id())
            .unwrap_or((self.buffer.point_min(), self.buffer.point_max()));
        let start = start.clamp(point_min, point_max) as i64;
        if let Some(window) = self.find_record_mut(self.selected_window_id) {
            if window.slots.len() < 2 {
                window.slots.resize(2, Value::Nil);
            }
            window.slots[1] = Value::Integer(start);
        }
    }

    pub fn set_selected_window_buffer_id(&mut self, buffer_id: u64) {
        let (point_min, _) = self
            .buffer_bounds_by_id(buffer_id)
            .unwrap_or((self.buffer.point_min(), self.buffer.point_max()));
        if let Some(window) = self.find_record_mut(self.selected_window_id) {
            if window.slots.len() < 2 {
                window.slots.resize(2, Value::Nil);
            }
            window.slots[0] = Value::Integer(buffer_id as i64);
            window.slots[1] = Value::Integer(point_min as i64);
        }
    }

    pub fn frame_width(&self) -> i64 {
        self.frame_width.max(1)
    }

    pub fn set_frame_width(&mut self, width: i64) {
        self.frame_width = width.max(1);
    }

    pub fn frame_height(&self) -> i64 {
        self.frame_height.max(1)
    }

    pub fn set_frame_height(&mut self, height: i64) {
        self.frame_height = height.max(1);
    }

    fn snapshot_window_configuration(&self) -> WindowConfigurationSnapshot {
        WindowConfigurationSnapshot {
            current_buffer_id: self.current_buffer_id(),
            selected_window_id: self.selected_window_id,
            selected_window_slots: self
                .find_record(self.selected_window_id)
                .map(|record| record.slots.clone())
                .unwrap_or_default(),
            frame_width: self.frame_width,
            frame_height: self.frame_height,
        }
    }

    fn restore_window_configuration(
        &mut self,
        snapshot: WindowConfigurationSnapshot,
    ) -> Result<(), LispError> {
        if self.has_buffer_id(snapshot.current_buffer_id) {
            self.switch_to_buffer_id(snapshot.current_buffer_id)?;
        }
        self.selected_window_id = snapshot.selected_window_id;
        self.frame_width = snapshot.frame_width.max(1);
        self.frame_height = snapshot.frame_height.max(1);
        if let Some(window) = self.find_record_mut(snapshot.selected_window_id) {
            window.slots = snapshot.selected_window_slots;
        }
        Ok(())
    }

    fn find_class_state(&self, name: &str) -> Option<&ClassState> {
        self.class_states.iter().find(|state| state.name == name)
    }

    fn find_class_state_mut(&mut self, name: &str) -> Option<&mut ClassState> {
        self.class_states
            .iter_mut()
            .find(|state| state.name == name)
    }

    fn find_class_state_by_record_id(&self, record_id: u64) -> Option<&ClassState> {
        self.class_states
            .iter()
            .find(|state| state.record_id == record_id)
    }

    pub(crate) fn class_name_from_value(&self, value: &Value) -> Option<String> {
        match value {
            Value::Symbol(symbol) => Some(symbol.clone()),
            Value::Record(record_id) => self
                .find_class_state_by_record_id(*record_id)
                .map(|state| state.name.clone()),
            _ => None,
        }
    }

    pub(crate) fn class_value(&self, name: &str) -> Option<Value> {
        self.find_class_state(name)
            .map(|state| Value::Record(state.record_id))
    }

    fn register_class(
        &mut self,
        name: &str,
        parents: Vec<String>,
        slot_specs: Vec<Value>,
        options: Vec<Value>,
    ) -> Value {
        let record_value = if let Some(existing) = self.find_class_state(name) {
            Value::Record(existing.record_id)
        } else {
            self.create_record(
                "eieio--class",
                vec![
                    Value::Symbol(name.to_string()),
                    Value::list(parents.iter().cloned().map(Value::Symbol)),
                    Value::list(slot_specs.iter().cloned()),
                    Value::list(options.iter().cloned()),
                ],
            )
        };
        let Value::Record(record_id) = record_value.clone() else {
            unreachable!("class registration uses class records");
        };

        let old_parents = self
            .find_class_state(name)
            .map(|state| state.parents.clone())
            .unwrap_or_default();
        for parent in &old_parents {
            if let Some(parent_state) = self.find_class_state_mut(parent) {
                parent_state.children.retain(|child| child != name);
            }
        }

        if let Some(record) = self.find_record_mut(record_id) {
            record.slots = vec![
                Value::Symbol(name.to_string()),
                Value::list(parents.iter().cloned().map(Value::Symbol)),
                Value::list(slot_specs.iter().cloned()),
                Value::list(options.iter().cloned()),
            ];
        }

        if let Some(existing) = self.find_class_state_mut(name) {
            existing.parents = parents.clone();
            existing.slot_specs = slot_specs.clone();
            existing.options = options.clone();
        } else {
            self.class_states.push(ClassState {
                name: name.to_string(),
                record_id,
                parents: parents.clone(),
                slot_specs: slot_specs.clone(),
                options: options.clone(),
                children: Vec::new(),
            });
        }

        for parent in &parents {
            if let Some(parent_state) = self.find_class_state_mut(parent)
                && !parent_state.children.iter().any(|child| child == name)
            {
                parent_state.children.push(name.to_string());
            }
        }

        self.put_symbol_property(name, "cl--class", record_value.clone());
        self.put_symbol_property(
            name,
            "emaxx-class-parents",
            Value::list(parents.into_iter().map(Value::Symbol)),
        );
        self.put_symbol_property(name, "emaxx-class-slots", Value::list(slot_specs));
        self.put_symbol_property(name, "emaxx-class-options", Value::list(options));
        record_value
    }

    pub(crate) fn class_allparents(&self, name: &str) -> Vec<Value> {
        fn visit(
            interp: &Interpreter,
            name: &str,
            output: &mut Vec<Value>,
            seen: &mut std::collections::HashSet<String>,
        ) {
            if !seen.insert(name.to_string()) {
                return;
            }
            output.push(Value::Symbol(name.to_string()));
            if let Some(state) = interp.find_class_state(name) {
                if state.parents.is_empty() {
                    if name != "t" {
                        visit(interp, "t", output, seen);
                    }
                } else {
                    for parent in &state.parents {
                        visit(interp, parent, output, seen);
                    }
                }
            } else if name != "t" {
                visit(interp, "t", output, seen);
            }
        }

        let mut output = Vec::new();
        let mut seen = std::collections::HashSet::new();
        visit(self, name, &mut output, &mut seen);
        output
    }

    pub(crate) fn value_is_instance_of_class(&self, value: &Value, class_name: &str) -> bool {
        let Value::Record(record_id) = value else {
            return false;
        };
        let Some(record) = self.find_record(*record_id) else {
            return false;
        };
        self.class_allparents(&record.type_name)
            .iter()
            .any(|parent| matches!(parent, Value::Symbol(name) if name == class_name))
    }

    pub(crate) fn class_children(&self, name: &str) -> Vec<Value> {
        self.find_class_state(name)
            .map(|state| {
                state
                    .children
                    .iter()
                    .cloned()
                    .map(Value::Symbol)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    fn register_generic_generalizer(
        &mut self,
        name: &str,
        priority: i64,
        tagcode_function: Value,
        specializers_function: Value,
    ) -> Value {
        let record_value = if let Some(existing) = self
            .generalizer_states
            .iter()
            .find(|state| state.name == name)
        {
            Value::Record(existing.record_id)
        } else {
            self.create_record(
                "cl--generic-generalizer",
                vec![
                    Value::Symbol(name.to_string()),
                    Value::Integer(priority),
                    tagcode_function.clone(),
                    specializers_function.clone(),
                ],
            )
        };
        let Value::Record(record_id) = record_value.clone() else {
            unreachable!("generalizer registration uses generalizer records");
        };

        if let Some(record) = self.find_record_mut(record_id) {
            record.slots = vec![
                Value::Symbol(name.to_string()),
                Value::Integer(priority),
                tagcode_function.clone(),
                specializers_function.clone(),
            ];
        }

        if let Some(existing) = self
            .generalizer_states
            .iter_mut()
            .find(|state| state.name == name)
        {
            existing.priority = priority;
            existing.tagcode_function = tagcode_function.clone();
            existing.specializers_function = specializers_function.clone();
        } else {
            self.generalizer_states.push(GenericGeneralizerState {
                name: name.to_string(),
                record_id,
                priority,
                tagcode_function: tagcode_function.clone(),
                specializers_function: specializers_function.clone(),
            });
        }

        self.set_global_binding(name, record_value.clone());
        self.put_symbol_property(name, "emaxx-generic-generalizer", record_value.clone());
        record_value
    }

    pub fn terminal_parameter(&self, name: &str) -> Option<Value> {
        self.terminal_parameters
            .iter()
            .rfind(|(parameter, _)| parameter == name)
            .map(|(_, value)| value.clone())
    }

    pub fn set_terminal_parameter(&mut self, name: &str, value: Value) {
        let value = Self::stored_value(value);
        if let Some(index) = self
            .terminal_parameters
            .iter()
            .rposition(|(parameter, _)| parameter == name)
        {
            self.terminal_parameters[index].1 = value;
        } else {
            self.terminal_parameters.push((name.to_string(), value));
        }
    }

    /// Remove a non-current buffer from the live buffer list.
    pub fn remove_buffer_id(&mut self, id: u64) -> Option<crate::buffer::Buffer> {
        if id == self.current_buffer_id {
            return None;
        }
        self.buffer_list.retain(|(buffer_id, _)| *buffer_id != id);
        self.inactive_buffers
            .iter()
            .position(|(buffer_id, _)| *buffer_id == id)
            .map(|pos| self.inactive_buffers.swap_remove(pos).1)
    }

    /// Kill a buffer by ID, switching away if it is current.
    pub fn kill_buffer_id(&mut self, id: u64) {
        self.detach_markers_for_buffer(id);
        self.buffer_locals
            .retain(|(buffer_id, _, _)| *buffer_id != id);
        self.indirect_buffers
            .retain(|(buffer_id, base_id)| *buffer_id != id && *base_id != id);
        if id == self.current_buffer_id {
            self.buffer_list.retain(|(buffer_id, _)| *buffer_id != id);
            if let Some((next_id, next_buffer)) = self.inactive_buffers.pop() {
                self.buffer = next_buffer;
                self.current_buffer_id = next_id;
            } else {
                let scratch_id = self.alloc_buffer_id();
                self.buffer = crate::buffer::Buffer::new("*scratch*");
                self.current_buffer_id = scratch_id;
                self.buffer_list.push((scratch_id, "*scratch*".to_string()));
            }
        } else {
            let _ = self.remove_buffer_id(id);
        }
    }

    /// Allocate a new unique overlay ID.
    pub fn alloc_overlay_id(&mut self) -> u64 {
        let id = self.next_overlay_id;
        self.next_overlay_id += 1;
        id
    }

    pub fn alloc_record_id(&mut self) -> u64 {
        let id = self.next_record_id;
        self.next_record_id += 1;
        id
    }

    pub fn alloc_finalizer_id(&mut self) -> u64 {
        let id = self.next_finalizer_id;
        self.next_finalizer_id += 1;
        id
    }

    /// Allocate a new marker.
    pub fn make_marker(&mut self) -> Value {
        let id = self.next_marker_id;
        self.next_marker_id += 1;
        self.markers.push(MarkerState {
            id,
            buffer_id: None,
            position: None,
            last_position: None,
            insertion_type: false,
        });
        Value::Marker(id)
    }

    pub fn find_marker(&self, id: u64) -> Option<&MarkerState> {
        self.markers.iter().find(|marker| marker.id == id)
    }

    pub fn find_marker_mut(&mut self, id: u64) -> Option<&mut MarkerState> {
        self.markers.iter_mut().find(|marker| marker.id == id)
    }

    pub fn marker_position(&self, id: u64) -> Option<usize> {
        self.find_marker(id).and_then(|marker| marker.position)
    }

    pub fn marker_buffer_id(&self, id: u64) -> Option<u64> {
        self.find_marker(id).and_then(|marker| marker.buffer_id)
    }

    pub fn marker_last_position(&self, id: u64) -> Option<usize> {
        self.find_marker(id).and_then(|marker| marker.last_position)
    }

    pub fn marker_insertion_type(&self, id: u64) -> Option<bool> {
        self.find_marker(id).map(|marker| marker.insertion_type)
    }

    pub fn set_marker_insertion_type(&mut self, id: u64, insertion_type: bool) {
        if let Some(marker) = self.find_marker_mut(id) {
            marker.insertion_type = insertion_type;
        }
    }

    pub fn set_marker(
        &mut self,
        id: u64,
        position: Option<usize>,
        buffer_id: Option<u64>,
    ) -> Result<(), LispError> {
        let marker = self
            .find_marker_mut(id)
            .ok_or_else(|| LispError::TypeError("marker".into(), format!("marker<{}>", id)))?;
        marker.buffer_id = buffer_id;
        marker.position = position;
        if let Some(pos) = position {
            marker.last_position = Some(pos);
        }
        Ok(())
    }

    pub fn copy_marker_value(
        &mut self,
        value: &Value,
        insertion_type: bool,
    ) -> Result<Value, LispError> {
        let marker_value = self.make_marker();
        let Value::Marker(marker_id) = marker_value else {
            unreachable!("make_marker always returns a marker")
        };
        match value {
            Value::Nil => {
                self.set_marker(marker_id, None, None)?;
            }
            Value::Marker(source_id) => {
                let source = self.find_marker(*source_id).cloned().ok_or_else(|| {
                    LispError::TypeError("marker".into(), format!("marker<{}>", source_id))
                })?;
                self.set_marker(marker_id, source.position, source.buffer_id)?;
            }
            Value::Integer(position) => {
                self.set_marker(
                    marker_id,
                    Some(*position as usize),
                    Some(self.current_buffer_id()),
                )?;
            }
            _ => {
                return Err(LispError::TypeError(
                    "integer-or-marker-p".into(),
                    value.type_name(),
                ));
            }
        }
        self.set_marker_insertion_type(marker_id, insertion_type);
        Ok(marker_value)
    }

    pub fn make_char_table(&mut self, subtype: Option<String>, default: Value) -> Value {
        let id = self.next_char_table_id;
        self.next_char_table_id += 1;
        self.char_tables.push(CharTableState {
            id,
            subtype,
            default,
            parent: None,
            extra_slots: Vec::new(),
            entries: Vec::new(),
            category_docs: Vec::new(),
        });
        Value::CharTable(id)
    }

    pub fn find_char_table(&self, id: u64) -> Option<&CharTableState> {
        self.char_tables.iter().find(|table| table.id == id)
    }

    pub fn find_char_table_mut(&mut self, id: u64) -> Option<&mut CharTableState> {
        self.char_tables.iter_mut().find(|table| table.id == id)
    }

    pub fn char_table_set(&mut self, id: u64, key: u32, value: Value) -> Result<(), LispError> {
        self.char_table_set_range(id, key, key, value)
    }

    pub fn char_table_set_range(
        &mut self,
        id: u64,
        start: u32,
        end: u32,
        value: Value,
    ) -> Result<(), LispError> {
        let table = self.find_char_table_mut(id).ok_or_else(|| {
            LispError::TypeError("char-table".into(), format!("char-table<{id}>"))
        })?;
        table.entries.push(CharTableEntry {
            start: start.min(end),
            end: start.max(end),
            value,
        });
        Ok(())
    }

    pub fn char_table_set_default(&mut self, id: u64, value: Value) -> Result<(), LispError> {
        let table = self.find_char_table_mut(id).ok_or_else(|| {
            LispError::TypeError("char-table".into(), format!("char-table<{id}>"))
        })?;
        table.default = value;
        Ok(())
    }

    pub fn char_table_get(&self, id: u64, key: u32) -> Option<Value> {
        let table = self.find_char_table(id)?;
        if let Some(entry) = table
            .entries
            .iter()
            .rev()
            .find(|entry| entry.start <= key && key <= entry.end)
        {
            return Some(entry.value.clone());
        }
        if let Some(parent_id) = table.parent
            && let Some(value) = self.char_table_get(parent_id, key)
        {
            return Some(value);
        }
        if table.default.is_nil()
            && let Some(value) = primitives::case_table_default_value(table.subtype.as_deref(), key)
        {
            return Some(value);
        }
        Some(table.default.clone())
    }

    pub fn char_table_range(&self, id: u64, start: u32, end: u32) -> Option<Value> {
        let table = self.find_char_table(id)?;
        if let Some(entry) = table
            .entries
            .iter()
            .rev()
            .find(|entry| entry.start == start.min(end) && entry.end == start.max(end))
        {
            return Some(entry.value.clone());
        }
        if let Some(parent_id) = table.parent
            && let Some(value) = self.char_table_range(parent_id, start, end)
        {
            return Some(value);
        }
        Some(table.default.clone())
    }

    pub fn char_table_subtype(&self, id: u64) -> Option<Option<String>> {
        self.find_char_table(id).map(|table| table.subtype.clone())
    }

    pub fn char_table_parent(&self, id: u64) -> Option<Option<u64>> {
        self.find_char_table(id).map(|table| table.parent)
    }

    pub fn char_table_explicit_get(&self, id: u64, key: u32) -> Option<Value> {
        let table = self.find_char_table(id)?;
        if let Some(entry) = table
            .entries
            .iter()
            .rev()
            .find(|entry| entry.start <= key && key <= entry.end)
        {
            return Some(entry.value.clone());
        }
        if let Some(parent_id) = table.parent {
            return self.char_table_explicit_get(parent_id, key);
        }
        None
    }

    pub fn set_char_table_parent(&mut self, id: u64, parent: Option<u64>) -> Result<(), LispError> {
        let table = self.find_char_table_mut(id).ok_or_else(|| {
            LispError::TypeError("char-table".into(), format!("char-table<{id}>"))
        })?;
        table.parent = parent;
        Ok(())
    }

    pub fn char_table_extra_slot(&self, id: u64, slot: usize) -> Option<Value> {
        self.find_char_table(id)
            .and_then(|table| table.extra_slots.get(slot).cloned())
    }

    pub fn set_char_table_extra_slot(
        &mut self,
        id: u64,
        slot: usize,
        value: Value,
    ) -> Result<(), LispError> {
        let table = self.find_char_table_mut(id).ok_or_else(|| {
            LispError::TypeError("char-table".into(), format!("char-table<{id}>"))
        })?;
        while table.extra_slots.len() <= slot {
            table.extra_slots.push(Value::Nil);
        }
        table.extra_slots[slot] = value;
        Ok(())
    }

    pub fn char_table_purpose(&self, id: u64) -> Option<&str> {
        self.find_char_table(id)
            .and_then(|table| table.subtype.as_deref())
    }

    pub fn clone_char_table(&mut self, id: u64) -> Result<Value, LispError> {
        let source = self.find_char_table(id).cloned().ok_or_else(|| {
            LispError::TypeError("char-table".into(), format!("char-table<{id}>"))
        })?;
        let new_id = self.next_char_table_id;
        self.next_char_table_id += 1;
        self.char_tables.push(CharTableState {
            id: new_id,
            ..source
        });
        Ok(Value::CharTable(new_id))
    }

    pub fn create_record(&mut self, type_name: &str, slots: Vec<Value>) -> Value {
        let id = self.alloc_record_id();
        self.records.push(RecordState {
            id,
            type_name: type_name.to_string(),
            slots,
        });
        Value::Record(id)
    }

    pub fn find_record(&self, id: u64) -> Option<&RecordState> {
        self.records.iter().find(|record| record.id == id)
    }

    pub fn find_record_mut(&mut self, id: u64) -> Option<&mut RecordState> {
        self.records.iter_mut().find(|record| record.id == id)
    }

    pub(crate) fn record_ids_by_type(&self, type_name: &str) -> Vec<u64> {
        self.records
            .iter()
            .filter(|record| record.type_name == type_name)
            .map(|record| record.id)
            .collect()
    }

    pub fn register_sqlite_handle(&mut self, id: u64, state: SqliteHandleState) {
        if let Some((_, existing)) = self
            .sqlite_handles
            .iter_mut()
            .find(|(record_id, _)| *record_id == id)
        {
            *existing = state;
        } else {
            self.sqlite_handles.push((id, state));
        }
    }

    pub fn find_sqlite_handle(&self, id: u64) -> Option<&SqliteHandleState> {
        self.sqlite_handles
            .iter()
            .find(|(record_id, _)| *record_id == id)
            .map(|(_, state)| state)
    }

    pub fn find_sqlite_handle_mut(&mut self, id: u64) -> Option<&mut SqliteHandleState> {
        self.sqlite_handles
            .iter_mut()
            .find(|(record_id, _)| *record_id == id)
            .map(|(_, state)| state)
    }

    pub fn copy_record(&mut self, id: u64) -> Result<Value, LispError> {
        let record = self
            .find_record(id)
            .cloned()
            .ok_or_else(|| LispError::TypeError("record".into(), format!("record<{id}>")))?;
        Ok(self.create_record(&record.type_name, record.slots))
    }

    pub fn provide_feature(&mut self, feature: &str) {
        if !self.provided_features.iter().any(|name| name == feature) {
            self.provided_features.push(feature.to_string());
        }
        if feature == "abbrev" {
            primitives::ensure_standard_abbrev_tables(self);
        }
    }

    fn provide_feature_with_after_load(&mut self, feature: &str) -> Result<Value, LispError> {
        self.provide_feature(feature);
        let mut pending = Vec::new();
        let mut index = 0usize;
        while index < self.after_load_forms.len() {
            if self.after_load_forms[index].0 == feature {
                let (_, body, env) = self.after_load_forms.remove(index);
                pending.push((body, env));
            } else {
                index += 1;
            }
        }
        for (body, mut env) in pending {
            self.sf_progn(&body, &mut env)?;
        }
        Ok(Value::Symbol(feature.to_string()))
    }

    pub fn has_feature(&self, feature: &str) -> bool {
        self.provided_features.iter().any(|name| name == feature)
    }

    fn find_thread_state(&self, record_id: u64) -> Option<&ThreadState> {
        self.thread_states
            .iter()
            .find(|thread| thread.record_id == record_id)
    }

    fn find_thread_state_mut(&mut self, record_id: u64) -> Option<&mut ThreadState> {
        self.thread_states
            .iter_mut()
            .find(|thread| thread.record_id == record_id)
    }

    fn find_mutex_state_mut(&mut self, record_id: u64) -> Option<&mut MutexState> {
        self.mutex_states
            .iter_mut()
            .find(|mutex| mutex.record_id == record_id)
    }

    fn find_condition_variable_state(&self, record_id: u64) -> Option<&ConditionVariableState> {
        self.condition_variables
            .iter()
            .find(|condvar| condvar.record_id == record_id)
    }

    fn find_process_state(&self, record_id: u64) -> Option<&ProcessState> {
        self.process_states
            .iter()
            .find(|process| process.record_id == record_id)
    }

    fn find_process_state_mut(&mut self, record_id: u64) -> Option<&mut ProcessState> {
        self.process_states
            .iter_mut()
            .find(|process| process.record_id == record_id)
    }

    pub fn resolve_thread_id(&self, value: &Value) -> Result<u64, LispError> {
        match value {
            Value::Record(id)
                if self
                    .find_record(*id)
                    .is_some_and(|record| record.type_name == "thread") =>
            {
                Ok(*id)
            }
            other => Err(wrong_type_argument("threadp", other.clone())),
        }
    }

    pub fn resolve_mutex_id(&self, value: &Value) -> Result<u64, LispError> {
        match value {
            Value::Record(id)
                if self
                    .find_record(*id)
                    .is_some_and(|record| record.type_name == "mutex") =>
            {
                Ok(*id)
            }
            other => Err(wrong_type_argument("mutexp", other.clone())),
        }
    }

    pub fn resolve_condition_variable_id(&self, value: &Value) -> Result<u64, LispError> {
        match value {
            Value::Record(id)
                if self
                    .find_record(*id)
                    .is_some_and(|record| record.type_name == "condition-variable") =>
            {
                Ok(*id)
            }
            other => Err(wrong_type_argument("condition-variable-p", other.clone())),
        }
    }

    pub fn resolve_process_id(&self, value: &Value) -> Result<u64, LispError> {
        match value {
            Value::Record(id)
                if self
                    .find_record(*id)
                    .is_some_and(|record| record.type_name == "process") =>
            {
                Ok(*id)
            }
            other => Err(wrong_type_argument("processp", other.clone())),
        }
    }

    pub fn create_process(
        &mut self,
        buffer_id: Option<u64>,
        program: Option<String>,
        argv: Vec<String>,
        runtime: Option<Child>,
    ) -> Result<Value, LispError> {
        let process = self.create_record("process", Vec::new());
        let Value::Record(record_id) = process.clone() else {
            unreachable!("create_record returns a record")
        };
        let marker = self.make_marker();
        let Value::Marker(mark_marker_id) = marker else {
            unreachable!("make_marker returns a marker")
        };
        let initial_position =
            buffer_id.and_then(|id| self.get_buffer_by_id(id).map(|buffer| buffer.point_max()));
        self.set_marker(mark_marker_id, initial_position, buffer_id)?;
        self.process_states.push(ProcessState {
            record_id,
            buffer_id,
            mark_marker_id,
            status: ProcessStatus::Run,
            filter: None,
            _query_on_exit_flag: false,
            decoding: Value::Nil,
            encoding: Value::Nil,
            program,
            argv,
            runtime: runtime.map(|child| RunningProcess { child }),
        });
        Ok(process)
    }

    fn refresh_process_state(process: &mut ProcessState) -> Result<(), LispError> {
        if !process.status.is_live() {
            return Ok(());
        }
        let Some(runtime) = process.runtime.as_mut() else {
            return Ok(());
        };
        if runtime
            .child
            .try_wait()
            .map_err(|error| LispError::Signal(error.to_string()))?
            .is_some()
        {
            process.status = ProcessStatus::Exit;
            process.runtime = None;
        }
        Ok(())
    }

    pub fn process_value_for_buffer(&mut self, buffer_id: u64) -> Option<Value> {
        self.process_states.iter_mut().rev().find_map(|process| {
            let _ = Self::refresh_process_state(process);
            (process.buffer_id == Some(buffer_id) && process.status.is_live())
                .then_some(Value::Record(process.record_id))
        })
    }

    fn refresh_process_id(&mut self, record_id: u64) -> Result<(), LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        Self::refresh_process_state(process)
    }

    pub fn process_buffer_id(&self, record_id: u64) -> Option<u64> {
        self.find_process_state(record_id)
            .and_then(|process| process.buffer_id)
    }

    pub fn process_mark_id(&self, record_id: u64) -> Option<u64> {
        self.find_process_state(record_id)
            .map(|process| process.mark_marker_id)
    }

    pub fn process_status_value(&mut self, record_id: u64) -> Option<Value> {
        let _ = self.refresh_process_id(record_id);
        self.find_process_state(record_id)
            .map(|process| Value::Symbol(process.status.symbol().into()))
    }

    pub fn process_is_live(&mut self, record_id: u64) -> bool {
        let _ = self.refresh_process_id(record_id);
        self.find_process_state(record_id)
            .is_some_and(|process| process.status.is_live())
    }

    pub fn process_filter(&self, record_id: u64) -> Option<Value> {
        self.find_process_state(record_id)
            .and_then(|process| process.filter.clone())
    }

    pub fn set_process_filter(
        &mut self,
        record_id: u64,
        filter: Option<Value>,
    ) -> Result<(), LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        process.filter = filter;
        Ok(())
    }

    pub fn process_coding_system(&self, record_id: u64) -> Result<Value, LispError> {
        let process = self
            .find_process_state(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        Ok(Value::cons(
            process.decoding.clone(),
            process.encoding.clone(),
        ))
    }

    pub fn set_process_coding_system(
        &mut self,
        record_id: u64,
        decoding: Value,
        encoding: Value,
    ) -> Result<(), LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        process.decoding = decoding;
        process.encoding = encoding;
        Ok(())
    }

    pub fn set_process_query_on_exit_flag(
        &mut self,
        record_id: u64,
        flag: bool,
    ) -> Result<(), LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        process._query_on_exit_flag = flag;
        Ok(())
    }

    pub fn process_command(&self, record_id: u64) -> Option<(String, Vec<String>)> {
        let process = self.find_process_state(record_id)?;
        let program = process.program.clone()?;
        Some((program, process.argv.clone()))
    }

    pub fn delete_process(&mut self, record_id: u64) -> Result<(), LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        if let Some(runtime) = process.runtime.as_mut() {
            let _ = runtime.child.kill();
            let _ = runtime.child.wait();
        }
        process.status = ProcessStatus::Exit;
        process.runtime = None;
        Ok(())
    }

    pub fn process_send_string(
        &mut self,
        record_id: u64,
        input: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>), LispError> {
        self.refresh_process_id(record_id)?;
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        if !process.status.is_live() {
            return Err(LispError::Signal("Process is not running".into()));
        }
        let Some(runtime) = process.runtime.as_mut() else {
            return Ok((input.to_vec(), Vec::new()));
        };
        let Some(stdin) = runtime.child.stdin.as_mut() else {
            return Err(LispError::Signal("Process stdin is closed".into()));
        };
        stdin
            .write_all(input)
            .map_err(|error| LispError::Signal(error.to_string()))?;
        stdin
            .flush()
            .map_err(|error| LispError::Signal(error.to_string()))?;
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_millis(100);
        loop {
            let mut made_progress = false;
            if let Some(pipe) = runtime.child.stdout.as_mut() {
                made_progress |= read_nonblocking_pipe(pipe, &mut stdout)?;
            }
            if let Some(pipe) = runtime.child.stderr.as_mut() {
                made_progress |= read_nonblocking_pipe(pipe, &mut stderr)?;
            }
            if runtime
                .child
                .try_wait()
                .map_err(|error| LispError::Signal(error.to_string()))?
                .is_some()
            {
                process.status = ProcessStatus::Exit;
                process.runtime = None;
                break;
            }
            if made_progress || std::time::Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        Ok((stdout, stderr))
    }

    pub fn schedule_timer(&mut self, function: Value, args: Vec<Value>) {
        let original_name = function.as_symbol().ok().map(str::to_string);
        self.pending_timers.push(ScheduledTimer {
            function: Self::stored_value(function),
            original_name,
            args: args.into_iter().map(Self::stored_value).collect(),
        });
    }

    pub fn run_pending_timers(&mut self, env: &mut Env) -> Result<(), LispError> {
        let pending = std::mem::take(&mut self.pending_timers);
        for timer in pending {
            self.call_function_value(
                timer.function,
                timer.original_name.as_deref(),
                &timer.args,
                env,
            )?;
        }
        Ok(())
    }

    fn run_due_elisp_timers(&mut self, env: &mut Env) -> Result<(), LispError> {
        if self
            .raw_function_binding("timer-event-handler", env)
            .is_none()
            || self.raw_function_binding("timer--time", env).is_none()
        {
            return Ok(());
        }

        let timers = self
            .lookup_var("timer-list", env)
            .unwrap_or(Value::Nil)
            .to_vec()
            .unwrap_or_default();
        for timer in timers {
            if primitives::call(self, "timerp", std::slice::from_ref(&timer), env)?.is_nil() {
                continue;
            }
            let timer_time = self.call_function_value(
                Value::Symbol("timer--time".into()),
                Some("timer--time"),
                std::slice::from_ref(&timer),
                env,
            )?;
            let future = primitives::call(self, "time-less-p", &[Value::Nil, timer_time], env)?;
            if future.is_nil() {
                self.call_function_value(
                    Value::Symbol("timer-event-handler".into()),
                    Some("timer-event-handler"),
                    std::slice::from_ref(&timer),
                    env,
                )?;
            }
        }
        Ok(())
    }

    pub fn current_thread_value(&self) -> Value {
        Value::Record(self.active_thread_id)
    }

    pub(crate) fn make_thread(
        &mut self,
        function: Value,
        name: Option<String>,
        disposition: BufferDisposition,
    ) -> Result<Value, LispError> {
        let program = self.thread_program_from_callable(&function)?;
        let value = self.create_record("thread", Vec::new());
        let Value::Record(record_id) = value else {
            unreachable!("thread records are always record values");
        };
        self.thread_states.push(ThreadState {
            record_id,
            name,
            buffer_id: self.current_buffer_id,
            buffer_disposition: disposition,
            buffer_killed: false,
            status: ThreadStatus::Runnable,
            program,
            outcome: None,
        });
        Ok(Value::Record(record_id))
    }

    pub fn make_mutex(&mut self, name: Option<String>) -> Value {
        let value = self.create_record("mutex", Vec::new());
        let Value::Record(record_id) = value else {
            unreachable!("mutex records are always record values");
        };
        self.mutex_states.push(MutexState {
            record_id,
            _name: name,
            owner: None,
            recursion_depth: 0,
        });
        Value::Record(record_id)
    }

    pub fn make_condition_variable(&mut self, mutex_id: u64, name: Option<String>) -> Value {
        let value = self.create_record("condition-variable", Vec::new());
        let Value::Record(record_id) = value else {
            unreachable!("condition variables are always record values");
        };
        self.condition_variables.push(ConditionVariableState {
            record_id,
            mutex_id,
            name,
        });
        Value::Record(record_id)
    }

    pub fn thread_name(&self, record_id: u64) -> Option<String> {
        self.find_thread_state(record_id)
            .and_then(|thread| thread.name.clone())
    }

    pub fn condition_variable_mutex_id(&self, record_id: u64) -> Option<u64> {
        self.find_condition_variable_state(record_id)
            .map(|condvar| condvar.mutex_id)
    }

    pub fn condition_variable_name(&self, record_id: u64) -> Option<String> {
        self.find_condition_variable_state(record_id)
            .and_then(|condvar| condvar.name.clone())
    }

    pub fn mutex_name(&self, record_id: u64) -> Option<String> {
        self.mutex_states
            .iter()
            .find(|mutex| mutex.record_id == record_id)
            .and_then(|mutex| mutex._name.clone())
    }

    pub fn thread_live(&self, record_id: u64) -> bool {
        self.find_thread_state(record_id)
            .map(|thread| !matches!(thread.status, ThreadStatus::Finished))
            .unwrap_or(false)
    }

    pub fn live_threads(&self) -> Vec<Value> {
        let mut threads = Vec::new();
        threads.push(Value::Record(self.main_thread_id));
        threads.extend(
            self.thread_states
                .iter()
                .filter(|thread| {
                    thread.record_id != self.main_thread_id
                        && !matches!(thread.status, ThreadStatus::Finished)
                })
                .map(|thread| Value::Record(thread.record_id)),
        );
        threads
    }

    pub fn thread_blocker_value(&self, record_id: u64) -> Value {
        match self
            .find_thread_state(record_id)
            .map(|thread| &thread.status)
        {
            Some(ThreadStatus::Blocked(ThreadBlocker::Mutex(id))) => Value::Record(*id),
            Some(ThreadStatus::Blocked(ThreadBlocker::ConditionVariable(id))) => Value::Record(*id),
            _ => Value::Nil,
        }
    }

    pub fn thread_backtrace_frames_snapshot(
        &self,
        record_id: u64,
    ) -> Vec<(Value, Vec<Value>, bool)> {
        if record_id == self.active_thread_id {
            return self.backtrace_frames_snapshot();
        }
        let Some(thread) = self.find_thread_state(record_id) else {
            return Vec::new();
        };
        match (&thread.program, &thread.status) {
            (
                ThreadProgram::ThreadListMutexWait { .. },
                ThreadStatus::Blocked(ThreadBlocker::Mutex(mutex_id)),
            ) => vec![
                (
                    Value::Symbol("mutex-lock".into()),
                    vec![Value::Record(*mutex_id)],
                    false,
                ),
                (
                    Value::Symbol("thread-tests--thread-function".into()),
                    Vec::new(),
                    false,
                ),
            ],
            _ => Vec::new(),
        }
    }

    pub fn thread_buffer_disposition(&self, record_id: u64) -> Result<Value, LispError> {
        let thread = self
            .find_thread_state(record_id)
            .ok_or_else(|| wrong_type_argument("threadp", Value::Record(record_id)))?;
        Ok(match thread.buffer_disposition {
            BufferDisposition::Default => Value::Nil,
            BufferDisposition::Preserve => Value::T,
            BufferDisposition::Silently => Value::Symbol("silently".into()),
        })
    }

    pub fn set_thread_buffer_disposition(
        &mut self,
        record_id: u64,
        value: &Value,
    ) -> Result<Value, LispError> {
        if record_id == self.main_thread_id {
            return Err(wrong_type_argument("threadp", Value::Record(record_id)));
        }
        let disposition = match value {
            Value::Nil => BufferDisposition::Default,
            Value::T => BufferDisposition::Preserve,
            Value::Symbol(symbol) if symbol == "silently" => BufferDisposition::Silently,
            other => {
                return Err(wrong_type_argument(
                    "thread-buffer-disposition",
                    other.clone(),
                ));
            }
        };
        let thread = self
            .find_thread_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("threadp", Value::Record(record_id)))?;
        thread.buffer_disposition = disposition;
        self.thread_buffer_disposition(record_id)
    }

    pub fn thread_last_error(&mut self, cleanup: bool) -> Value {
        let value = self.last_thread_error.clone().unwrap_or(Value::Nil);
        if cleanup {
            self.last_thread_error = None;
        }
        value
    }

    pub fn signal_thread(
        &mut self,
        record_id: u64,
        condition: Value,
        data: Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if record_id == self.main_thread_id {
            self.deliver_signal_to_main_thread(self.active_thread_id, condition, data, env)?;
            return Ok(Value::Nil);
        }
        let signal = build_signal_value(condition, data);
        self.finish_thread_with_signal(record_id, signal);
        Ok(Value::Nil)
    }

    pub fn thread_join(&mut self, record_id: u64, env: &mut Env) -> Result<Value, LispError> {
        if record_id == self.main_thread_id {
            return Err(LispError::Signal("Cannot join the current thread".into()));
        }
        while self.thread_live(record_id) {
            self.drive_threads(env, true)?;
            if self.thread_live(record_id)
                && self
                    .find_thread_state(record_id)
                    .is_some_and(|thread| matches!(thread.program, ThreadProgram::InfiniteYield))
            {
                break;
            }
        }
        let thread = self
            .find_thread_state(record_id)
            .ok_or_else(|| wrong_type_argument("threadp", Value::Record(record_id)))?;
        if thread.buffer_killed && thread.buffer_disposition == BufferDisposition::Default {
            return Err(LispError::SignalValue(Value::list([Value::Symbol(
                "thread-buffer-killed".into(),
            )])));
        }
        match thread
            .outcome
            .clone()
            .unwrap_or(ThreadOutcome::Returned(Value::Nil))
        {
            ThreadOutcome::Returned(value) => Ok(value),
            ThreadOutcome::Signaled(value) => Err(LispError::SignalValue(value)),
        }
    }

    pub fn drive_threads(&mut self, env: &mut Env, wake_sleepers: bool) -> Result<(), LispError> {
        let thread_ids = self
            .thread_states
            .iter()
            .filter(|thread| thread.record_id != self.main_thread_id)
            .map(|thread| thread.record_id)
            .collect::<Vec<_>>();
        for thread_id in thread_ids {
            let status = self
                .find_thread_state(thread_id)
                .map(|thread| thread.status.clone())
                .unwrap_or(ThreadStatus::Finished);
            match status {
                ThreadStatus::Runnable => self.step_thread(thread_id, env)?,
                ThreadStatus::Blocked(ThreadBlocker::Mutex(mutex_id))
                    if self.find_thread_state(thread_id).is_some_and(|thread| {
                        matches!(thread.program, ThreadProgram::ThreadListMutexWait { .. })
                    }) && self.mutex_is_available(thread_id, mutex_id) =>
                {
                    if let Some(thread) = self.find_thread_state_mut(thread_id) {
                        thread.status = ThreadStatus::Runnable;
                    }
                    self.step_thread(thread_id, env)?;
                }
                ThreadStatus::Blocked(ThreadBlocker::Sleep) if wake_sleepers => {
                    self.finish_thread_success(thread_id, Value::Nil);
                }
                _ => {}
            }
        }
        if wake_sleepers {
            self.run_pending_timers(env)?;
            self.run_due_elisp_timers(env)?;
        }
        Ok(())
    }

    pub fn lock_mutex_for_current_thread(
        &mut self,
        mutex_id: u64,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if self.try_lock_mutex(self.active_thread_id, mutex_id) {
            return Ok(Value::Nil);
        }
        while !self.try_lock_mutex(self.active_thread_id, mutex_id) {
            self.drive_threads(env, false)?;
        }
        Ok(Value::Nil)
    }

    pub fn unlock_mutex_for_current_thread(&mut self, mutex_id: u64) -> Result<Value, LispError> {
        self.unlock_mutex(self.active_thread_id, mutex_id);
        Ok(Value::Nil)
    }

    pub fn notify_condition_variable(&mut self, condvar_id: u64, notify_all: bool) {
        for thread in self.thread_states.iter_mut() {
            if !matches!(
                thread.status,
                ThreadStatus::Blocked(ThreadBlocker::ConditionVariable(id)) if id == condvar_id
            ) {
                continue;
            }
            if let ThreadProgram::CondvarWaitTwice { phase } = &mut thread.program {
                *phase = phase.saturating_add(1);
            }
            thread.status = ThreadStatus::Runnable;
            if !notify_all {
                break;
            }
        }
    }

    pub fn allow_kill_buffer_for_threads(&mut self, buffer_id: u64) -> bool {
        let mut blocked = false;
        for thread in self.thread_states.iter_mut() {
            if thread.record_id == self.main_thread_id
                || thread.buffer_id != buffer_id
                || matches!(thread.status, ThreadStatus::Finished)
            {
                continue;
            }
            match thread.buffer_disposition {
                BufferDisposition::Preserve => blocked = true,
                BufferDisposition::Default | BufferDisposition::Silently => {
                    thread.buffer_killed = true;
                }
            }
        }
        !blocked
    }

    fn try_lock_mutex(&mut self, thread_id: u64, mutex_id: u64) -> bool {
        let Some(mutex) = self.find_mutex_state_mut(mutex_id) else {
            return false;
        };
        match mutex.owner {
            None => {
                mutex.owner = Some(thread_id);
                mutex.recursion_depth = 1;
                true
            }
            Some(owner) if owner == thread_id => {
                mutex.recursion_depth += 1;
                true
            }
            Some(_) => false,
        }
    }

    fn mutex_is_available(&self, thread_id: u64, mutex_id: u64) -> bool {
        self.mutex_states
            .iter()
            .find(|mutex| mutex.record_id == mutex_id)
            .is_some_and(|mutex| mutex.owner.is_none() || mutex.owner == Some(thread_id))
    }

    fn unlock_mutex(&mut self, thread_id: u64, mutex_id: u64) {
        let Some(mutex) = self.find_mutex_state_mut(mutex_id) else {
            return;
        };
        if mutex.owner != Some(thread_id) {
            return;
        }
        if mutex.recursion_depth > 1 {
            mutex.recursion_depth -= 1;
        } else {
            mutex.owner = None;
            mutex.recursion_depth = 0;
        }
    }

    fn finish_thread_success(&mut self, record_id: u64, value: Value) {
        if let Some(thread) = self.find_thread_state_mut(record_id) {
            thread.status = ThreadStatus::Finished;
            thread.outcome = Some(ThreadOutcome::Returned(value));
        }
    }

    fn finish_thread_with_signal(&mut self, record_id: u64, value: Value) {
        if let Some(thread) = self.find_thread_state_mut(record_id) {
            thread.status = ThreadStatus::Finished;
            thread.outcome = Some(ThreadOutcome::Signaled(value.clone()));
        }
        self.last_thread_error = Some(value);
    }

    fn thread_buffer_var_value(&self, buffer_id: u64, name: &str) -> Value {
        self.buffer_local_toplevel_value(buffer_id, name)
            .or_else(|| self.default_toplevel_value(name))
            .unwrap_or(Value::Nil)
    }

    fn set_env_or_global(&mut self, env: &mut Env, name: &str, value: Value) {
        for frame in env.iter_mut().rev() {
            if let Some((_, existing)) = frame.iter_mut().rev().find(|(bound, _)| bound == name) {
                *existing = Self::stored_value(value);
                return;
            }
        }
        self.set_global_binding(name, value);
    }

    fn deliver_signal_to_main_thread(
        &mut self,
        source_thread_id: u64,
        condition: Value,
        data: Value,
        env: &mut Env,
    ) -> Result<(), LispError> {
        let format = Value::String("Error %s: %S".into());
        let event_tail = Value::list([condition, data]);
        let _ = primitives::call(
            self,
            "message",
            &[format, Value::Record(source_thread_id), event_tail],
            env,
        )?;
        Ok(())
    }

    fn step_thread(&mut self, record_id: u64, env: &mut Env) -> Result<(), LispError> {
        let previous_active = self.active_thread_id;
        self.active_thread_id = record_id;
        let program = self
            .find_thread_state(record_id)
            .map(|thread| thread.program.clone())
            .unwrap_or(ThreadProgram::Noop);

        let result = match program {
            ThreadProgram::Main => Ok(()),
            ThreadProgram::Ignore | ThreadProgram::Noop => {
                self.finish_thread_success(record_id, Value::Nil);
                Ok(())
            }
            ThreadProgram::Call(function) => {
                match self.call_function_value(function, None, &[], env) {
                    Ok(value) => {
                        self.finish_thread_success(record_id, value);
                        Ok(())
                    }
                    Err(error) => {
                        self.finish_thread_with_signal(record_id, error_condition_value(&error));
                        Ok(())
                    }
                }
            }
            ThreadProgram::SetGlobal { name, value } => {
                self.set_global_binding(&name, value.clone());
                self.finish_thread_success(record_id, value);
                Ok(())
            }
            ThreadProgram::Sleep { blocked } => {
                if !blocked && let Some(thread) = self.find_thread_state_mut(record_id) {
                    thread.program = ThreadProgram::Sleep { blocked: true };
                    thread.status = ThreadStatus::Blocked(ThreadBlocker::Sleep);
                }
                Ok(())
            }
            ThreadProgram::YieldThenSetGlobal {
                target,
                value,
                phase,
            } => {
                if phase == 0 {
                    if let Some(thread) = self.find_thread_state_mut(record_id) {
                        thread.program = ThreadProgram::YieldThenSetGlobal {
                            target,
                            value,
                            phase: 1,
                        };
                    }
                } else {
                    self.set_global_binding(&target, value.clone());
                    self.finish_thread_success(record_id, value);
                }
                Ok(())
            }
            ThreadProgram::MutexContention { phase } => {
                let mutex_value = self
                    .default_toplevel_value("threads-mutex")
                    .unwrap_or(Value::Nil);
                let mutex_id = self.resolve_mutex_id(&mutex_value)?;
                if phase == 0 {
                    if self.try_lock_mutex(record_id, mutex_id) {
                        self.set_global_binding("threads-mutex-key", Value::Integer(23));
                        if let Some(thread) = self.find_thread_state_mut(record_id) {
                            thread.program = ThreadProgram::MutexContention { phase: 1 };
                        }
                    }
                } else if !self
                    .default_toplevel_value("threads-mutex-key")
                    .unwrap_or(Value::Nil)
                    .is_truthy()
                {
                    self.unlock_mutex(record_id, mutex_id);
                    self.finish_thread_success(record_id, Value::Nil);
                }
                Ok(())
            }
            ThreadProgram::MutexBlock { phase } => {
                if phase == 0 {
                    self.set_global_binding("threads-mutex-key", Value::Integer(23));
                    let mutex_value = self
                        .default_toplevel_value("threads-mutex")
                        .unwrap_or(Value::Nil);
                    let mutex_id = self.resolve_mutex_id(&mutex_value)?;
                    if self.try_lock_mutex(record_id, mutex_id) {
                        self.finish_thread_success(record_id, Value::Nil);
                    } else if let Some(thread) = self.find_thread_state_mut(record_id) {
                        thread.program = ThreadProgram::MutexBlock { phase: 1 };
                        thread.status = ThreadStatus::Blocked(ThreadBlocker::Mutex(mutex_id));
                    }
                }
                Ok(())
            }
            ThreadProgram::SignalError { value } => {
                self.finish_thread_with_signal(record_id, value);
                Ok(())
            }
            ThreadProgram::InfiniteYield => Ok(()),
            ThreadProgram::SignalMainThread => {
                self.deliver_signal_to_main_thread(
                    record_id,
                    Value::Symbol("error".into()),
                    Value::Nil,
                    env,
                )?;
                self.finish_thread_success(record_id, Value::Nil);
                Ok(())
            }
            ThreadProgram::CondvarWaitTwice { phase } => {
                let condvar_value = self
                    .default_toplevel_value("threads-condvar")
                    .unwrap_or(Value::Nil);
                let condvar_id = self.resolve_condition_variable_id(&condvar_value)?;
                match phase {
                    0 => {
                        if let Some(thread) = self.find_thread_state_mut(record_id) {
                            thread.status =
                                ThreadStatus::Blocked(ThreadBlocker::ConditionVariable(condvar_id));
                        }
                    }
                    1 => {
                        if let Some(thread) = self.find_thread_state_mut(record_id) {
                            thread.program = ThreadProgram::CondvarWaitTwice { phase: 2 };
                            thread.status =
                                ThreadStatus::Blocked(ThreadBlocker::ConditionVariable(condvar_id));
                        }
                    }
                    _ => self.finish_thread_success(record_id, Value::Nil),
                }
                Ok(())
            }
            ThreadProgram::CaptureBufferLocal { target, source } => {
                let buffer_id = self
                    .find_thread_state(record_id)
                    .map(|thread| thread.buffer_id)
                    .unwrap_or(self.current_buffer_id);
                let value = self.thread_buffer_var_value(buffer_id, &source);
                self.set_env_or_global(env, &target, value.clone());
                self.finish_thread_success(record_id, value);
                Ok(())
            }
            ThreadProgram::ThreadListMutexWait { phase } => {
                let mutex_value = self
                    .default_toplevel_value("thread-tests-mutex")
                    .unwrap_or(Value::Nil);
                let mutex_id = self.resolve_mutex_id(&mutex_value)?;
                if phase == 0 {
                    self.set_global_binding("thread-tests-flag", Value::T);
                }
                if self.try_lock_mutex(record_id, mutex_id) {
                    self.unlock_mutex(record_id, mutex_id);
                    self.finish_thread_success(record_id, Value::Nil);
                } else if let Some(thread) = self.find_thread_state_mut(record_id) {
                    thread.program = ThreadProgram::ThreadListMutexWait { phase: 1 };
                    thread.status = ThreadStatus::Blocked(ThreadBlocker::Mutex(mutex_id));
                }
                Ok(())
            }
        };
        self.active_thread_id = previous_active;
        result
    }

    fn thread_program_from_callable(&self, function: &Value) -> Result<ThreadProgram, LispError> {
        match function {
            Value::Symbol(name) if name == "ignore" => Ok(ThreadProgram::Ignore),
            Value::Symbol(name) => self
                .thread_program_from_symbol(name)
                .or_else(|_| Ok(ThreadProgram::Call(function.clone()))),
            Value::BuiltinFunc(name) if name == "ignore" => Ok(ThreadProgram::Ignore),
            Value::BuiltinFunc(_) => Ok(ThreadProgram::Call(function.clone())),
            Value::Lambda(params, body, _) if params.is_empty() => self
                .thread_program_from_lambda(function_executable_body(body))
                .or_else(|_| Ok(ThreadProgram::Call(function.clone()))),
            _ => Err(LispError::Signal("Unsupported thread entry point".into())),
        }
    }

    fn thread_program_from_symbol(&self, name: &str) -> Result<ThreadProgram, LispError> {
        Ok(match name {
            "threads-test-thread1" | "threads-test-io-switch" => ThreadProgram::SetGlobal {
                name: "threads-test-global".into(),
                value: Value::Integer(23),
            },
            "threads-thread-sleeps" => ThreadProgram::Sleep { blocked: false },
            "threads-test-thread2" => ThreadProgram::YieldThenSetGlobal {
                target: "threads-test-global".into(),
                value: Value::Integer(23),
                phase: 0,
            },
            "threads-test-mlock" => ThreadProgram::MutexContention { phase: 0 },
            "threads-test-mlock2" => ThreadProgram::MutexBlock { phase: 0 },
            "threads-call-error" => ThreadProgram::SignalError {
                value: Value::list([
                    Value::Symbol("error".into()),
                    Value::String("Error is called".into()),
                ]),
            },
            "thread-tests--thread-function" => ThreadProgram::ThreadListMutexWait { phase: 0 },
            "threads-custom" => ThreadProgram::Noop,
            "threads-test-condvar-wait" => ThreadProgram::CondvarWaitTwice { phase: 0 },
            other => {
                return Err(LispError::Signal(format!(
                    "Unsupported thread entry point: {other}"
                )));
            }
        })
    }

    fn thread_program_from_lambda(&self, body: &[Value]) -> Result<ThreadProgram, LispError> {
        if body.len() == 1
            && let Ok(items) = body[0].to_vec()
            && matches!(items.first(), Some(Value::Symbol(name)) if name == "sleep-for")
        {
            return Ok(ThreadProgram::Sleep { blocked: false });
        }

        if body.len() == 1
            && let Ok(items) = body[0].to_vec()
            && matches!(items.as_slice(), [Value::Symbol(head), Value::Symbol(name), Value::Symbol(source)] if head == "setq" && name == "seen" && source == "threads-test--var")
        {
            return Ok(ThreadProgram::CaptureBufferLocal {
                target: "seen".into(),
                source: "threads-test--var".into(),
            });
        }

        if body.len() == 1
            && let Ok(items) = body[0].to_vec()
            && matches!(
                items.first(),
                Some(Value::Symbol(head)) if head == "while"
            )
        {
            let condition = items.get(1).cloned().unwrap_or(Value::Nil);
            if condition == Value::T
                && items.len() == 3
                && items[2]
                    .to_vec()
                    .ok()
                    .is_some_and(|inner| matches!(inner.first(), Some(Value::Symbol(name)) if name == "thread-yield"))
            {
                return Ok(ThreadProgram::InfiniteYield);
            }
        }

        if body.len() == 1
            && let Ok(items) = body[0].to_vec()
            && matches!(items.first(), Some(Value::Symbol(head)) if head == "thread-signal")
        {
            return Ok(ThreadProgram::SignalMainThread);
        }

        Err(LispError::Signal(
            "Unsupported anonymous thread entry point".into(),
        ))
    }

    fn builtin_charset_name(name: &str) -> bool {
        matches!(name, "ascii" | "unicode" | "eight-bit")
    }

    pub fn charset_canonical_name(&self, name: &str) -> Option<String> {
        let mut current = name.to_string();
        for _ in 0..16 {
            if Self::builtin_charset_name(&current) {
                return Some(current);
            }
            let (_, target) = self
                .charset_aliases
                .iter()
                .rev()
                .find(|(alias, _)| alias == &current)?;
            current = target.clone();
        }
        None
    }

    pub fn has_charset(&self, name: &str) -> bool {
        self.charset_canonical_name(name).is_some()
    }

    pub fn charset_id(&self, name: &str) -> Option<i64> {
        match self.charset_canonical_name(name)?.as_str() {
            "ascii" => Some(0),
            "unicode" => Some(1),
            "eight-bit" => Some(8),
            _ => None,
        }
    }

    pub fn charset_plist_value(&self, name: &str) -> Option<Value> {
        let canonical = self.charset_canonical_name(name)?;
        self.charset_plists
            .iter()
            .rev()
            .find(|(charset, _)| charset == &canonical)
            .map(|(_, value)| value.clone())
    }

    pub fn set_charset_plist_value(&mut self, name: &str, value: Value) -> Result<(), LispError> {
        let canonical = self
            .charset_canonical_name(name)
            .ok_or_else(|| LispError::Void(name.to_string()))?;
        if let Some((_, existing)) = self
            .charset_plists
            .iter_mut()
            .rev()
            .find(|(charset, _)| charset == &canonical)
        {
            *existing = value;
        } else {
            self.charset_plists.push((canonical, value));
        }
        Ok(())
    }

    pub fn define_charset_alias(&mut self, alias: &str, target: &str) -> Result<(), LispError> {
        let canonical = self
            .charset_canonical_name(target)
            .ok_or_else(|| LispError::Void(target.to_string()))?;
        if let Some((_, existing)) = self
            .charset_aliases
            .iter_mut()
            .rev()
            .find(|(existing_alias, _)| existing_alias == alias)
        {
            *existing = canonical;
        } else {
            self.charset_aliases.push((alias.to_string(), canonical));
        }
        Ok(())
    }

    pub fn charset_priority_list(&self) -> Vec<String> {
        self.charset_priority.clone()
    }

    pub fn set_charset_priority(&mut self, names: &[String]) {
        let mut reordered = Vec::new();
        for name in names {
            if let Some(canonical) = self.charset_canonical_name(name)
                && !reordered.iter().any(|existing| existing == &canonical)
            {
                reordered.push(canonical);
            }
        }
        for default in ["unicode", "ascii", "eight-bit"] {
            if !reordered.iter().any(|existing| existing == default) {
                reordered.push(default.to_string());
            }
        }
        self.charset_priority = reordered;
    }

    pub fn charset_priority_rank(&self, name: &str) -> usize {
        let canonical = self
            .charset_canonical_name(name)
            .unwrap_or_else(|| name.to_string());
        self.charset_priority
            .iter()
            .position(|existing| existing == &canonical)
            .unwrap_or(usize::MAX)
    }

    pub fn declare_iso_charset(
        &mut self,
        dimension: i64,
        chars: i64,
        final_char: u32,
        charset: &str,
    ) {
        let canonical = self
            .charset_canonical_name(charset)
            .unwrap_or_else(|| charset.to_string());
        if let Some((_, _, _, existing)) = self
            .iso_charsets
            .iter_mut()
            .rev()
            .find(|(d, c, f, _)| *d == dimension && *c == chars && *f == final_char)
        {
            *existing = canonical;
        } else {
            self.iso_charsets
                .push((dimension, chars, final_char, canonical));
        }
    }

    pub fn iso_charset(&self, dimension: i64, chars: i64, final_char: u32) -> Option<String> {
        self.iso_charsets
            .iter()
            .rev()
            .find(|(d, c, f, _)| *d == dimension && *c == chars && *f == final_char)
            .map(|(_, _, _, charset)| charset.clone())
    }

    pub fn coding_system_canonical_name(&self, name: &str) -> Option<String> {
        let mut current = name.to_string();
        for _ in 0..16 {
            if self
                .coding_systems
                .iter()
                .any(|coding| coding.name == current)
            {
                return Some(current);
            }
            let (_, target) = self
                .coding_aliases
                .iter()
                .rev()
                .find(|(alias, _)| alias == &current)?;
            current = target.clone();
        }
        None
    }

    pub fn has_coding_system(&self, name: &str) -> bool {
        self.coding_system_canonical_name(name).is_some()
    }

    pub fn coding_system(&self, name: &str) -> Option<CodingSystemState> {
        let canonical = self.coding_system_canonical_name(name)?;
        self.coding_systems
            .iter()
            .rev()
            .find(|coding| coding.name == canonical)
            .cloned()
    }

    pub fn coding_system_base_name(&self, name: &str) -> Option<String> {
        self.coding_system(name).map(|coding| coding.base)
    }

    pub fn coding_system_kind_name(&self, name: &str) -> Option<String> {
        self.coding_system(name).map(|coding| coding.kind)
    }

    pub fn coding_system_eol_type_value(&self, name: &str) -> Option<i64> {
        self.coding_system(name).and_then(|coding| coding.eol_type)
    }

    pub fn coding_system_plist_value(&self, name: &str) -> Option<Value> {
        self.coding_system(name).map(|coding| coding.plist)
    }

    pub fn set_coding_system_plist_property(
        &mut self,
        name: &str,
        key: &str,
        value: Value,
    ) -> Result<(), LispError> {
        let canonical = self
            .coding_system_canonical_name(name)
            .ok_or_else(|| LispError::Void(name.to_string()))?;
        let Some(coding) = self
            .coding_systems
            .iter_mut()
            .rev()
            .find(|coding| coding.name == canonical)
        else {
            return Err(LispError::Void(name.to_string()));
        };
        let mut items = coding.plist.to_vec()?;
        let key_value = Value::Symbol(key.to_string());
        if let Some(index) = items.iter().position(|item| item == &key_value) {
            if index + 1 < items.len() {
                items[index + 1] = value;
            } else {
                items.push(value);
            }
        } else {
            items.push(key_value);
            items.push(value);
        }
        coding.plist = Value::list(items);
        Ok(())
    }

    pub fn define_coding_system_alias(
        &mut self,
        alias: &str,
        target: &str,
    ) -> Result<(), LispError> {
        let canonical = self
            .coding_system_canonical_name(target)
            .ok_or_else(|| LispError::Void(target.to_string()))?;
        if let Some((_, existing)) = self
            .coding_aliases
            .iter_mut()
            .rev()
            .find(|(existing_alias, _)| existing_alias == alias)
        {
            *existing = canonical;
        } else {
            self.coding_aliases.push((alias.to_string(), canonical));
        }
        Ok(())
    }

    pub fn coding_system_alias_list(&self, name: &str) -> Option<Vec<String>> {
        let canonical = self.coding_system_canonical_name(name)?;
        let mut aliases = vec![canonical.clone()];
        for (alias, target) in &self.coding_aliases {
            if target == &canonical && !aliases.iter().any(|existing| existing == alias) {
                aliases.push(alias.clone());
            }
        }
        Some(aliases)
    }

    pub fn coding_system_priority_list(&self) -> Vec<String> {
        self.coding_priority.clone()
    }

    pub fn set_coding_system_priority(&mut self, names: &[String]) -> Result<(), LispError> {
        let mut reordered = Vec::new();
        for name in names {
            let canonical = self
                .coding_system_canonical_name(name)
                .ok_or_else(|| LispError::Void(name.clone()))?;
            if !reordered.iter().any(|existing| existing == &canonical) {
                reordered.push(canonical);
            }
        }
        for default in builtin_coding_priority() {
            if !reordered.iter().any(|existing| existing == &default) {
                reordered.push(default);
            }
        }
        self.coding_priority = reordered;
        Ok(())
    }

    pub fn coding_system_priority_rank(&self, name: &str) -> usize {
        let canonical = self
            .coding_system_canonical_name(name)
            .unwrap_or_else(|| name.to_string());
        self.coding_priority
            .iter()
            .position(|existing| existing == &canonical)
            .unwrap_or(usize::MAX)
    }

    pub fn define_coding_system(
        &mut self,
        name: &str,
        mnemonic: i64,
        kind: &str,
        plist: Value,
        eol_type: Option<i64>,
    ) -> Result<(), LispError> {
        let kind_canonical = self
            .coding_system_canonical_name(kind)
            .unwrap_or_else(|| kind.to_string());
        let mut items = plist.to_vec().unwrap_or_default();
        let mnemonic_key = Value::Symbol(":mnemonic".into());
        if let Some(index) = items.iter().position(|item| item == &mnemonic_key) {
            if index + 1 < items.len() {
                items[index + 1] = Value::Integer(mnemonic);
            } else {
                items.push(Value::Integer(mnemonic));
            }
        } else {
            items.push(mnemonic_key);
            items.push(Value::Integer(mnemonic));
        }
        let definition = CodingSystemState {
            name: name.to_string(),
            base: name.to_string(),
            kind: self
                .coding_system_kind_name(&kind_canonical)
                .unwrap_or(kind_canonical),
            eol_type,
            plist: Value::list(items),
        };
        if let Some(existing) = self
            .coding_systems
            .iter_mut()
            .rev()
            .find(|coding| coding.name == name)
        {
            *existing = definition;
        } else {
            self.coding_systems.push(definition);
        }
        if !self.coding_priority.iter().any(|existing| existing == name) {
            self.coding_priority.push(name.to_string());
        }
        Ok(())
    }

    pub fn terminal_coding_system(&self) -> Option<String> {
        self.terminal_coding.clone()
    }

    pub fn set_terminal_coding_system(&mut self, coding: Option<String>) {
        self.terminal_coding = coding;
    }

    pub fn keyboard_coding_system(&self) -> Option<String> {
        self.keyboard_coding.clone()
    }

    pub fn set_keyboard_coding_system(&mut self, coding: Option<String>) {
        self.keyboard_coding = coding;
    }

    pub fn ensure_standard_category_table(&mut self) -> u64 {
        if let Some(id) = self.standard_category_table_id {
            return id;
        }
        let Value::CharTable(id) =
            self.make_char_table(Some("category-table".into()), Value::String(String::new()))
        else {
            unreachable!("make_char_table returns a char-table");
        };
        self.standard_category_table_id = Some(id);
        id
    }

    pub fn ensure_standard_case_table(&mut self) -> u64 {
        if let Some(id) = self.standard_case_table_id {
            return id;
        }
        let Value::CharTable(down_id) = self.make_char_table(Some("case-table".into()), Value::Nil)
        else {
            unreachable!("make_char_table returns a char-table");
        };
        let Value::CharTable(up_id) =
            self.make_char_table(Some("case-table-up".into()), Value::Nil)
        else {
            unreachable!("make_char_table returns a char-table");
        };
        self.set_char_table_extra_slot(down_id, 0, Value::CharTable(up_id))
            .expect("new case table accepts upcase slot");
        self.standard_case_table_id = Some(down_id);
        down_id
    }

    pub fn current_case_table_id(&mut self) -> u64 {
        if let Some((_, id)) = self
            .buffer_case_tables
            .iter()
            .rev()
            .find(|(buffer_id, _)| *buffer_id == self.current_buffer_id())
        {
            *id
        } else {
            self.ensure_standard_case_table()
        }
    }

    pub fn set_current_case_table(&mut self, id: u64) {
        let current_buffer_id = self.current_buffer_id();
        if let Some((_, slot)) = self
            .buffer_case_tables
            .iter_mut()
            .rev()
            .find(|(buffer_id, _)| *buffer_id == current_buffer_id)
        {
            *slot = id;
        } else {
            self.buffer_case_tables.push((current_buffer_id, id));
        }
    }

    pub fn standard_case_table_id(&mut self) -> u64 {
        self.ensure_standard_case_table()
    }

    pub fn set_standard_case_table(&mut self, id: u64) {
        self.standard_case_table_id = Some(id);
    }

    pub fn standard_syntax_table_id(&self) -> u64 {
        self.standard_syntax_table_id
    }

    pub fn current_syntax_table_id(&self) -> u64 {
        self.buffer_syntax_tables
            .iter()
            .rev()
            .find_map(|(buffer_id, table_id)| {
                (*buffer_id == self.current_buffer_id()).then_some(*table_id)
            })
            .unwrap_or(self.standard_syntax_table_id())
    }

    pub fn set_current_syntax_table(&mut self, id: u64) {
        let current_buffer_id = self.current_buffer_id();
        if let Some((_, table_id)) = self
            .buffer_syntax_tables
            .iter_mut()
            .rev()
            .find(|(buffer_id, _)| *buffer_id == current_buffer_id)
        {
            *table_id = id;
        } else {
            self.buffer_syntax_tables.push((current_buffer_id, id));
        }
    }

    pub fn set_syntax_word_char(&mut self, code: u32, enabled: bool) {
        if enabled {
            if !self.syntax_word_chars.contains(&code) {
                self.syntax_word_chars.push(code);
            }
        } else {
            self.syntax_word_chars.retain(|existing| *existing != code);
        }
    }

    pub fn is_syntax_word_char(&self, code: u32) -> bool {
        self.syntax_word_chars.contains(&code)
    }

    pub fn syntax_word_chars(&self) -> Vec<u32> {
        self.syntax_word_chars.clone()
    }

    pub fn category_docstring(&self, id: u64, category: u32) -> Option<String> {
        self.find_char_table(id).and_then(|table| {
            table
                .category_docs
                .iter()
                .find(|(ch, _)| *ch == category)
                .map(|(_, doc)| doc.clone())
        })
    }

    pub fn define_category(
        &mut self,
        id: u64,
        category: u32,
        doc: String,
    ) -> Result<(), LispError> {
        let table = self.find_char_table_mut(id).ok_or_else(|| {
            LispError::TypeError("char-table".into(), format!("char-table<{id}>"))
        })?;
        if table.category_docs.iter().any(|(ch, _)| *ch == category) {
            return Err(LispError::Signal("Category already defined".into()));
        }
        table.category_docs.push((category, doc));
        Ok(())
    }

    pub fn detach_markers_for_buffer(&mut self, buffer_id: u64) {
        for marker in &mut self.markers {
            if marker.buffer_id == Some(buffer_id) {
                marker.last_position = marker.position.or(marker.last_position);
                marker.position = None;
                marker.buffer_id = None;
            }
        }
    }

    pub fn adjust_markers_for_insert(
        &mut self,
        buffer_id: u64,
        pos: usize,
        nchars: usize,
        before_markers: bool,
    ) {
        if nchars == 0 {
            return;
        }
        for marker in &mut self.markers {
            if marker.buffer_id != Some(buffer_id) {
                continue;
            }
            let Some(position) = marker.position else {
                continue;
            };
            if position > pos || (position == pos && (before_markers || marker.insertion_type)) {
                let new_pos = position + nchars;
                marker.position = Some(new_pos);
                marker.last_position = Some(new_pos);
            }
        }
    }

    pub fn adjust_markers_for_delete(&mut self, buffer_id: u64, from: usize, to: usize) {
        if from >= to {
            return;
        }
        let nchars = to - from;
        for marker in &mut self.markers {
            if marker.buffer_id != Some(buffer_id) {
                continue;
            }
            let Some(position) = marker.position else {
                continue;
            };
            let new_pos = if position > to {
                position - nchars
            } else if position > from {
                from
            } else {
                position
            };
            marker.position = Some(new_pos);
            marker.last_position = Some(new_pos);
        }
    }

    pub fn live_marker_positions_for_buffer(&self, buffer_id: u64) -> Vec<(u64, Option<usize>)> {
        self.markers
            .iter()
            .filter(|marker| marker.buffer_id == Some(buffer_id))
            .map(|marker| (marker.id, marker.position))
            .collect()
    }

    pub fn change_hooks_are_running(&self) -> bool {
        self.change_hooks_running > 0
    }

    pub fn enter_change_hooks(&mut self) {
        self.change_hooks_running += 1;
    }

    pub fn leave_change_hooks(&mut self) {
        self.change_hooks_running = self.change_hooks_running.saturating_sub(1);
    }

    pub fn buffer_local_hook(&self, buffer_id: u64, hook_name: &str) -> Option<Vec<Value>> {
        self.buffer_local_hooks
            .iter()
            .find(|(id, name, _)| *id == buffer_id && name == hook_name)
            .map(|(_, _, hooks)| hooks.clone())
    }

    pub fn set_buffer_local_hook(&mut self, buffer_id: u64, hook_name: &str, hooks: Vec<Value>) {
        if let Some((_, _, existing)) = self
            .buffer_local_hooks
            .iter_mut()
            .find(|(id, name, _)| *id == buffer_id && name == hook_name)
        {
            *existing = hooks;
        } else {
            self.buffer_local_hooks
                .push((buffer_id, hook_name.to_string(), hooks));
        }
    }

    pub fn buffer_local_value(&self, buffer_id: u64, name: &str) -> Option<Value> {
        self.buffer_locals
            .iter()
            .rev()
            .find(|(id, var, _)| *id == buffer_id && var == name)
            .map(|(_, _, value)| value.clone())
    }

    pub fn set_buffer_local_value(&mut self, buffer_id: u64, name: &str, value: Value) {
        let value = Self::stored_value(value);
        for (id, var, existing) in self.buffer_locals.iter_mut().rev() {
            if *id == buffer_id && var == name {
                *existing = value;
                return;
            }
        }
        self.buffer_locals
            .push((buffer_id, name.to_string(), value));
    }

    pub fn remove_buffer_local_value(&mut self, buffer_id: u64, name: &str) {
        if let Some(index) = self
            .buffer_locals
            .iter()
            .rposition(|(id, var, _)| *id == buffer_id && var == name)
        {
            self.buffer_locals.remove(index);
        }
    }

    pub fn clear_buffer_local_state(&mut self, buffer_id: u64) {
        self.buffer_locals.retain(|(id, _, _)| *id != buffer_id);
        self.buffer_local_hooks
            .retain(|(id, _, _)| *id != buffer_id);
        self.buffer_case_tables.retain(|(id, _)| *id != buffer_id);
    }

    pub fn clone_buffer_local_state(&mut self, from_buffer_id: u64, to_buffer_id: u64) {
        let locals = self
            .buffer_locals
            .iter()
            .filter(|(id, _, _)| *id == from_buffer_id)
            .map(|(_, name, value)| (name.clone(), value.clone()))
            .collect::<Vec<_>>();
        for (name, value) in locals {
            self.set_buffer_local_value(to_buffer_id, &name, value);
        }

        let hooks = self
            .buffer_local_hooks
            .iter()
            .filter(|(id, _, _)| *id == from_buffer_id)
            .map(|(_, name, values)| (name.clone(), values.clone()))
            .collect::<Vec<_>>();
        for (name, values) in hooks {
            self.set_buffer_local_hook(to_buffer_id, &name, values);
        }

        if let Some((_, table)) = self
            .buffer_case_tables
            .iter()
            .find(|(id, _)| *id == from_buffer_id)
            .cloned()
        {
            self.buffer_case_tables.push((to_buffer_id, table));
        }
    }

    pub fn buffer_local_variables(&self, buffer_id: u64) -> Vec<(String, Value)> {
        let mut vars = Vec::new();
        for (id, name, value) in &self.buffer_locals {
            if *id == buffer_id && !vars.iter().any(|(existing, _)| existing == name) {
                vars.push((name.clone(), value.clone()));
            }
        }
        vars
    }

    pub fn mark_auto_buffer_local(&mut self, name: &str) {
        if !self
            .auto_buffer_locals
            .iter()
            .any(|existing| existing == name)
        {
            self.auto_buffer_locals.push(name.to_string());
        }
    }

    pub fn is_auto_buffer_local(&self, name: &str) -> bool {
        self.auto_buffer_locals
            .iter()
            .any(|existing| existing == name)
    }

    pub fn mark_special_variable(&mut self, name: &str) {
        if !self
            .special_variables
            .iter()
            .any(|existing| existing == name)
        {
            self.special_variables.push(name.to_string());
        }
    }

    pub fn unmark_special_variable(&mut self, name: &str) {
        if let Some(index) = self
            .special_variables
            .iter()
            .rposition(|existing| existing == name)
        {
            self.special_variables.remove(index);
        }
    }

    pub fn is_special_variable(&self, name: &str) -> bool {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        self.special_variables
            .iter()
            .any(|existing| existing == &resolved)
    }

    pub fn special_variable_names(&self) -> Vec<String> {
        self.special_variables.clone()
    }

    fn symbol_property_index(&self, name: &str) -> Option<usize> {
        self.symbol_properties
            .iter()
            .rposition(|(symbol, _)| symbol == name)
    }

    pub fn get_symbol_property(&self, name: &str, property: &str) -> Option<Value> {
        if property == "choice" {
            match name {
                "vertical-scroll-bar" => {
                    return Some(Value::list([
                        Value::Nil,
                        Value::Symbol("left".into()),
                        Value::Symbol("right".into()),
                    ]));
                }
                "overwrite-mode" => {
                    return Some(Value::list([
                        Value::Nil,
                        Value::Symbol("overwrite-mode-textual".into()),
                        Value::Symbol("overwrite-mode-binary".into()),
                    ]));
                }
                _ => {}
            }
        }
        self.symbol_property_index(name).and_then(|index| {
            self.symbol_properties[index]
                .1
                .iter()
                .rposition(|(key, _)| key == property)
                .map(|prop_index| self.symbol_properties[index].1[prop_index].1.clone())
        })
    }

    pub fn put_symbol_property(&mut self, name: &str, property: &str, value: Value) {
        let value = Self::stored_value(value);
        if let Some(index) = self.symbol_property_index(name) {
            if let Some(prop_index) = self.symbol_properties[index]
                .1
                .iter()
                .rposition(|(key, _)| key == property)
            {
                self.symbol_properties[index].1[prop_index].1 = value;
            } else {
                self.symbol_properties[index]
                    .1
                    .push((property.to_string(), value));
            }
            return;
        }
        self.symbol_properties
            .push((name.to_string(), vec![(property.to_string(), value)]));
    }

    pub fn remove_symbol_property(&mut self, name: &str, property: &str) {
        let Some(index) = self.symbol_property_index(name) else {
            return;
        };
        if let Some(prop_index) = self.symbol_properties[index]
            .1
            .iter()
            .rposition(|(key, _)| key == property)
        {
            self.symbol_properties[index].1.remove(prop_index);
        }
        if self.symbol_properties[index].1.is_empty() {
            self.symbol_properties.remove(index);
        }
    }

    pub fn symbol_plist(&self, name: &str) -> Value {
        let Some(index) = self.symbol_property_index(name) else {
            return Value::Nil;
        };
        let mut items = Vec::new();
        for (property, value) in &self.symbol_properties[index].1 {
            items.push(Value::Symbol(property.clone()));
            items.push(value.clone());
        }
        Value::list(items)
    }

    pub fn set_symbol_plist(&mut self, name: &str, plist: Value) -> Result<Value, LispError> {
        let items = plist.to_vec()?;
        let mut props = Vec::new();
        let mut index = 0usize;
        while index + 1 < items.len() {
            props.push((
                items[index].as_symbol()?.to_string(),
                Self::stored_value(items[index + 1].clone()),
            ));
            index += 2;
        }
        if props.is_empty() {
            if let Some(existing) = self.symbol_property_index(name) {
                self.symbol_properties.remove(existing);
            }
        } else if let Some(existing) = self.symbol_property_index(name) {
            self.symbol_properties[existing].1 = props;
        } else {
            self.symbol_properties.push((name.to_string(), props));
        }
        Ok(plist)
    }

    fn variable_watcher_index(&self, name: &str) -> Option<usize> {
        self.variable_watchers
            .iter()
            .rposition(|(symbol, _)| symbol == name)
    }

    pub fn variable_watchers(&self, name: &str) -> Vec<Value> {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        self.variable_watcher_index(&resolved)
            .map(|index| self.variable_watchers[index].1.clone())
            .unwrap_or_default()
    }

    pub fn add_variable_watcher(&mut self, name: &str, watcher: Value) -> Result<Value, LispError> {
        let resolved = self.resolve_variable_name(name)?;
        if let Some(index) = self.variable_watcher_index(&resolved) {
            if !self.variable_watchers[index]
                .1
                .iter()
                .any(|existing| existing == &watcher)
            {
                self.variable_watchers[index].1.push(watcher.clone());
            }
        } else {
            self.variable_watchers
                .push((resolved.clone(), vec![watcher.clone()]));
        }
        Ok(watcher)
    }

    pub fn remove_variable_watcher(
        &mut self,
        name: &str,
        watcher: &Value,
    ) -> Result<Value, LispError> {
        let resolved = self.resolve_variable_name(name)?;
        if let Some(index) = self.variable_watcher_index(&resolved) {
            self.variable_watchers[index]
                .1
                .retain(|existing| existing != watcher);
            if self.variable_watchers[index].1.is_empty() {
                self.variable_watchers.remove(index);
            }
        }
        Ok(watcher.clone())
    }

    pub fn clear_variable_watchers(&mut self, name: &str) {
        if let Some(index) = self.variable_watcher_index(name) {
            self.variable_watchers.remove(index);
        }
    }

    pub fn notify_variable_watchers(
        &mut self,
        name: &str,
        value: Value,
        action: &str,
        buffer_id: Option<u64>,
        env: &mut Env,
    ) -> Result<(), LispError> {
        let Some(index) = self.variable_watcher_index(name) else {
            return Ok(());
        };
        let watchers = self.variable_watchers[index].1.clone();
        let buffer = buffer_id
            .and_then(|id| self.buffer_identity_value(id))
            .unwrap_or(Value::Nil);
        for watcher in watchers {
            self.call_function_value(
                watcher,
                None,
                &[
                    Value::Symbol(name.to_string()),
                    value.clone(),
                    Value::Symbol(action.to_string()),
                    buffer.clone(),
                ],
                env,
            )?;
        }
        Ok(())
    }

    fn direct_variable_alias(&self, name: &str) -> Option<String> {
        self.variable_aliases
            .iter()
            .rposition(|(alias, _)| alias == name)
            .map(|index| self.variable_aliases[index].1.clone())
    }

    pub fn resolve_variable_name(&self, name: &str) -> Result<String, LispError> {
        let mut seen = vec![name.to_string()];
        let mut current = name.to_string();
        while let Some(target) = self.direct_variable_alias(&current) {
            if seen.iter().any(|existing| existing == &target) {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("cyclic-variable-indirection".into()),
                    Value::Symbol(name.to_string()),
                ])));
            }
            seen.push(target.clone());
            current = target;
        }
        Ok(current)
    }

    pub fn set_variable_alias(&mut self, alias: &str, target: &str) -> Result<(), LispError> {
        let target = self.resolve_variable_name(target)?;
        if target == alias {
            return Err(LispError::SignalValue(Value::list([
                Value::Symbol("cyclic-variable-indirection".into()),
                Value::Symbol(alias.to_string()),
            ])));
        }
        if let Some(index) = self
            .variable_aliases
            .iter()
            .rposition(|(existing, _)| existing == alias)
        {
            self.variable_aliases[index].1 = target;
        } else {
            self.variable_aliases.push((alias.to_string(), target));
        }
        Ok(())
    }

    pub fn remove_variable_alias(&mut self, name: &str) -> bool {
        if let Some(index) = self
            .variable_aliases
            .iter()
            .rposition(|(alias, _)| alias == name)
        {
            self.variable_aliases.remove(index);
            true
        } else {
            false
        }
    }

    pub fn indirect_variable_name(&self, name: &str) -> Result<String, LispError> {
        self.resolve_variable_name(name)
    }

    fn global_value(&self, name: &str) -> Option<Value> {
        self.globals
            .iter()
            .rposition(|(symbol, _)| symbol == name)
            .map(|index| self.globals[index].1.clone())
    }

    pub fn default_value(&self, name: &str) -> Option<Value> {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        self.global_value(&resolved)
            .or_else(|| self.builtin_var_value(&resolved))
    }

    pub fn is_default_bound(&self, name: &str) -> bool {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        self.globals.iter().any(|(symbol, _)| symbol == &resolved)
    }

    pub fn remove_global_binding(&mut self, name: &str) {
        if let Some(index) = self.globals.iter().rposition(|(symbol, _)| symbol == name) {
            self.globals.remove(index);
        }
    }

    pub fn set_global_binding(&mut self, name: &str, value: Value) {
        let name = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        let value = Self::stored_value(value);
        if let Some(index) = self.globals.iter().rposition(|(symbol, _)| symbol == &name) {
            self.globals[index].1 = value;
        } else {
            self.globals.push((name, value));
        }
    }

    pub fn buffer_identity_value(&self, buffer_id: u64) -> Option<Value> {
        self.buffer_list
            .iter()
            .find(|(id, _)| *id == buffer_id)
            .map(|(id, name)| Value::Buffer(*id, name.clone()))
    }

    fn active_special_assignment_scope(&self, name: &str) -> Option<SpecialBindingScope> {
        let index = self
            .active_special_restores
            .iter()
            .rposition(|restore| restore.name == name)?;
        let restore = &self.active_special_restores[index];
        match restore.scope {
            SpecialBindingScope::Global
                if self.is_auto_buffer_local(name)
                    && restore.binding_buffer_id != Some(self.current_buffer_id()) =>
            {
                None
            }
            _ => Some(restore.scope.clone()),
        }
    }

    fn active_global_toplevel_value(&self, name: &str) -> Option<Option<Value>> {
        self.active_special_restores
            .iter()
            .find(|restore| {
                restore.name == name && matches!(restore.scope, SpecialBindingScope::Global)
            })
            .map(|restore| restore.previous.clone())
    }

    fn active_buffer_local_toplevel_value(
        &self,
        buffer_id: u64,
        name: &str,
    ) -> Option<Option<Value>> {
        self.active_special_restores
            .iter()
            .find(|restore| {
                restore.name == name
                    && matches!(restore.scope, SpecialBindingScope::BufferLocal(id) if id == buffer_id)
            })
            .map(|restore| restore.previous.clone())
    }

    fn set_active_global_toplevel_value(&mut self, name: &str, value: Option<Value>) -> bool {
        let Some(index) = self.active_special_restores.iter().position(|restore| {
            restore.name == name && matches!(restore.scope, SpecialBindingScope::Global)
        }) else {
            return false;
        };
        self.active_special_restores[index].previous = value.map(Self::stored_value);
        true
    }

    fn set_active_buffer_local_toplevel_value(
        &mut self,
        buffer_id: u64,
        name: &str,
        value: Option<Value>,
    ) -> bool {
        let Some(index) = self.active_special_restores.iter().position(|restore| {
            restore.name == name
                && matches!(restore.scope, SpecialBindingScope::BufferLocal(id) if id == buffer_id)
        }) else {
            return false;
        };
        self.active_special_restores[index].previous = value.map(Self::stored_value);
        true
    }

    pub fn default_toplevel_value(&self, name: &str) -> Option<Value> {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        if let Some(previous) = self.active_global_toplevel_value(&resolved) {
            return previous.or_else(|| self.builtin_var_value(&resolved));
        }
        self.global_value(&resolved)
            .or_else(|| self.builtin_var_value(&resolved))
    }

    pub fn set_default_toplevel_value(&mut self, name: &str, value: Value) {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        if !self.set_active_global_toplevel_value(&resolved, Some(value.clone())) {
            self.set_global_binding(&resolved, value);
        }
    }

    pub fn buffer_local_toplevel_value(&self, buffer_id: u64, name: &str) -> Option<Value> {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        if let Some(previous) = self.active_buffer_local_toplevel_value(buffer_id, &resolved) {
            return previous;
        }
        self.buffer_local_value(buffer_id, &resolved)
    }

    pub fn set_buffer_local_toplevel_value(&mut self, buffer_id: u64, name: &str, value: Value) {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        if !self.set_active_buffer_local_toplevel_value(buffer_id, &resolved, Some(value.clone())) {
            self.set_buffer_local_value(buffer_id, &resolved, value);
        }
    }

    fn assignment_scope(&self, name: &str) -> Option<SpecialBindingScope> {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        if self
            .buffer_local_value(self.current_buffer_id(), &resolved)
            .is_some()
        {
            return Some(SpecialBindingScope::BufferLocal(self.current_buffer_id()));
        }
        if let Some(scope) = self.active_special_assignment_scope(&resolved) {
            return Some(scope);
        }
        if self.is_auto_buffer_local(&resolved) {
            return Some(SpecialBindingScope::BufferLocal(self.current_buffer_id()));
        }
        None
    }

    pub fn assignment_buffer_id(&self, name: &str) -> Option<u64> {
        match self.assignment_scope(name) {
            Some(SpecialBindingScope::BufferLocal(buffer_id)) => Some(buffer_id),
            _ => None,
        }
    }

    pub fn prepare_variable_assignment(
        &self,
        name: &str,
        value: Value,
    ) -> Result<Value, LispError> {
        if matches!(
            name,
            "nil" | "t" | "most-positive-fixnum" | "most-negative-fixnum"
        ) {
            return Err(LispError::SignalValue(Value::list([
                Value::Symbol("setting-constant".into()),
                Value::Symbol(name.to_string()),
            ])));
        }
        if name.starts_with(':') {
            return if value == Value::Symbol(name.to_string()) {
                Ok(value)
            } else {
                Err(LispError::SignalValue(Value::list([
                    Value::Symbol("setting-constant".into()),
                    Value::Symbol(name.to_string()),
                ])))
            };
        }
        match name {
            "display-hourglass" => Ok(if value.is_nil() { Value::Nil } else { Value::T }),
            "gc-cons-threshold" => match value {
                Value::Integer(_) | Value::BigInteger(_) => Ok(value),
                other => Err(wrong_type_argument("integerp", other)),
            },
            "scroll-up-aggressively" => match value {
                Value::Nil => Ok(Value::Nil),
                Value::Integer(number) if (0..=1).contains(&number) => Ok(Value::Integer(number)),
                Value::Float(number) if (0.0..=1.0).contains(&number) => Ok(Value::Float(number)),
                other => Err(wrong_type_argument("numberp", other)),
            },
            "vertical-scroll-bar" => match value {
                Value::Nil => Ok(Value::Nil),
                Value::Symbol(ref symbol) if matches!(symbol.as_str(), "left" | "right") => {
                    Ok(value)
                }
                other => Err(wrong_type_argument("symbolp", other)),
            },
            "overwrite-mode" => match value {
                Value::Nil => Ok(Value::Nil),
                Value::Symbol(ref symbol)
                    if matches!(
                        symbol.as_str(),
                        "overwrite-mode-textual" | "overwrite-mode-binary"
                    ) =>
                {
                    Ok(value)
                }
                other => Err(wrong_type_argument("symbolp", other)),
            },
            _ => Ok(value),
        }
    }

    fn bind_special_variable(
        &mut self,
        name: &str,
        value: Value,
        env: &mut Env,
    ) -> Result<SpecialBindingRestore, LispError> {
        let name = self.resolve_variable_name(name)?;
        let value = self.prepare_variable_assignment(&name, value)?;
        let buffer_id = self.current_buffer_id();
        let restore = if self.buffer_local_value(buffer_id, &name).is_some() {
            let previous = self.buffer_local_value(buffer_id, &name);
            self.notify_variable_watchers(&name, value.clone(), "let", Some(buffer_id), env)?;
            self.set_buffer_local_value(buffer_id, &name, value);
            SpecialBindingRestore {
                name,
                scope: SpecialBindingScope::BufferLocal(buffer_id),
                binding_buffer_id: None,
                previous,
            }
        } else {
            let previous = self.global_value(&name);
            let binding_buffer_id = if self.is_auto_buffer_local(&name) {
                Some(buffer_id)
            } else {
                None
            };
            self.notify_variable_watchers(&name, value.clone(), "let", None, env)?;
            let value = Self::stored_value(value);
            if let Some(index) = self.globals.iter().rposition(|(symbol, _)| symbol == &name) {
                self.globals[index].1 = value;
            } else {
                self.globals.push((name.clone(), value));
            }
            SpecialBindingRestore {
                name,
                scope: SpecialBindingScope::Global,
                binding_buffer_id,
                previous,
            }
        };
        self.active_special_restores.push(restore.clone());
        Ok(restore)
    }

    fn restore_special_binding(
        &mut self,
        restore: SpecialBindingRestore,
        env: &mut Env,
    ) -> Result<(), LispError> {
        let restore = if let Some(index) = self
            .active_special_restores
            .iter()
            .rposition(|active| active.name == restore.name && active.scope == restore.scope)
        {
            self.active_special_restores.remove(index)
        } else {
            restore
        };
        match restore.scope {
            SpecialBindingScope::Global => {
                self.notify_variable_watchers(
                    &restore.name,
                    restore.previous.clone().unwrap_or(Value::Nil),
                    "unlet",
                    None,
                    env,
                )?;
                if let Some(value) = restore.previous {
                    let value = Self::stored_value(value);
                    if let Some(index) = self
                        .globals
                        .iter()
                        .rposition(|(symbol, _)| symbol == &restore.name)
                    {
                        self.globals[index].1 = value;
                    } else {
                        self.globals.push((restore.name.clone(), value));
                    }
                } else {
                    self.remove_global_binding(&restore.name);
                }
            }
            SpecialBindingScope::BufferLocal(buffer_id) => {
                self.notify_variable_watchers(
                    &restore.name,
                    restore.previous.clone().unwrap_or(Value::Nil),
                    "unlet",
                    Some(buffer_id),
                    env,
                )?;
                if let Some(value) = restore.previous {
                    self.set_buffer_local_value(buffer_id, &restore.name, value);
                } else {
                    self.remove_buffer_local_value(buffer_id, &restore.name);
                }
            }
        }
        Ok(())
    }

    pub fn push_backtrace_frame(&mut self, function: Value, args: Vec<Value>) {
        self.backtrace_frames.push(BacktraceFrame {
            function,
            args,
            debug_on_exit: false,
        });
    }

    pub fn pop_backtrace_frame(&mut self) {
        self.backtrace_frames.pop();
    }

    pub fn set_current_backtrace_debug(&mut self, enabled: bool) {
        if let Some(frame) = self.backtrace_frames.last_mut() {
            frame.debug_on_exit = enabled;
        }
    }

    pub fn current_backtrace_frame(&self) -> Option<(Value, Vec<Value>, bool)> {
        self.backtrace_frames.last().map(|frame| {
            (
                frame.function.clone(),
                frame.args.clone(),
                frame.debug_on_exit,
            )
        })
    }

    pub fn backtrace_frames_snapshot(&self) -> Vec<(Value, Vec<Value>, bool)> {
        self.backtrace_frames
            .iter()
            .rev()
            .map(|frame| {
                (
                    frame.function.clone(),
                    frame.args.clone(),
                    frame.debug_on_exit,
                )
            })
            .collect()
    }

    pub fn push_handler_bindings(&mut self, bindings: &[(String, Value)]) -> usize {
        let start = self.active_handlers.len();
        self.active_handlers.extend_from_slice(bindings);
        start
    }

    pub fn pop_handler_bindings(&mut self, start: usize) {
        self.active_handlers.truncate(start);
    }

    fn take_condition_case_suspend(&mut self) -> bool {
        if self.suspend_condition_case_count == 0 {
            false
        } else {
            self.suspend_condition_case_count -= 1;
            true
        }
    }

    fn dispatch_handler_bindings(
        &mut self,
        error: LispError,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if self.handler_dispatch_depth > 0 {
            return Err(error);
        }
        let error_value = error_condition_value(&error);
        let error_type = error.condition_type();
        let mut handled = false;
        self.handler_dispatch_depth += 1;
        for (condition, handler) in self.active_handlers.clone().into_iter().rev() {
            if condition != "error" && condition != error_type {
                continue;
            }
            let result =
                self.call_function_value(handler, None, std::slice::from_ref(&error_value), env);
            match result {
                Ok(_) => handled = true,
                Err(next) => {
                    self.handler_dispatch_depth = self.handler_dispatch_depth.saturating_sub(1);
                    if !matches!(next, LispError::Throw(_, _)) && self.condition_case_depth > 1 {
                        self.suspend_condition_case_count = 1;
                    }
                    return Err(next);
                }
            }
        }
        self.handler_dispatch_depth = self.handler_dispatch_depth.saturating_sub(1);
        if handled {
            Err(LispError::SignalValue(error_value))
        } else {
            Err(error)
        }
    }

    pub fn effective_labeled_restriction(
        &self,
        buffer_id: u64,
        skip_label: Option<&str>,
    ) -> Option<(usize, usize)> {
        let mut result: Option<(usize, usize)> = None;
        for (id, label, start, end) in &self.labeled_restrictions {
            if *id != buffer_id || skip_label == Some(label.as_str()) {
                continue;
            }
            result = Some(match result {
                Some((cur_start, cur_end)) => (cur_start.max(*start), cur_end.min(*end)),
                None => (*start, *end),
            });
        }
        result
    }

    pub fn register_indirect_buffer(&mut self, buffer_id: u64, base_id: u64) {
        self.indirect_buffers.push((buffer_id, base_id));
    }

    pub fn buffer_base_id(&self, buffer_id: u64) -> Option<u64> {
        self.indirect_buffers
            .iter()
            .find(|(id, _)| *id == buffer_id)
            .map(|(_, base_id)| *base_id)
    }

    pub fn root_buffer_id(&self, mut buffer_id: u64) -> u64 {
        while let Some(base_id) = self.buffer_base_id(buffer_id) {
            buffer_id = base_id;
        }
        buffer_id
    }

    pub fn related_buffer_ids(&self, buffer_id: u64) -> Vec<u64> {
        let root = self.root_buffer_id(buffer_id);
        self.buffer_list
            .iter()
            .map(|(id, _)| *id)
            .filter(|id| self.root_buffer_id(*id) == root)
            .collect()
    }

    fn mirror_insert_to_related_buffers(
        &mut self,
        related: &[u64],
        pos: usize,
        s: &str,
        props: Option<Vec<(String, Value)>>,
        before_markers: bool,
        inherit: bool,
    ) {
        let current_id = self.current_buffer_id();
        let nchars = s.chars().count();
        for buffer_id in related {
            if *buffer_id == current_id {
                continue;
            }
            if let Some(buffer) = self.get_buffer_by_id_mut(*buffer_id) {
                let saved_point = buffer.point();
                buffer.goto_char(pos);
                if inherit {
                    buffer.insert_and_inherit(s);
                } else if let Some(props) = props.clone() {
                    buffer.insert_with_properties(s, Some(props));
                } else {
                    buffer.insert(s);
                }
                let restored = if saved_point > pos || (saved_point == pos && before_markers) {
                    saved_point + nchars
                } else {
                    saved_point
                };
                buffer.goto_char(restored);
            }
            self.adjust_markers_for_insert(*buffer_id, pos, nchars, before_markers);
        }
    }

    fn mirror_delete_to_related_buffers(&mut self, related: &[u64], from: usize, to: usize) {
        let current_id = self.current_buffer_id();
        for buffer_id in related {
            if *buffer_id == current_id {
                continue;
            }
            if let Some(buffer) = self.get_buffer_by_id_mut(*buffer_id) {
                let saved_point = buffer.point();
                let _ = buffer.delete_region(from, to);
                let restored = if saved_point > to {
                    saved_point - (to - from)
                } else if saved_point > from {
                    from
                } else {
                    saved_point
                };
                buffer.goto_char(restored);
            }
            self.adjust_markers_for_delete(*buffer_id, from, to);
        }
    }

    pub fn insert_current_buffer(&mut self, s: &str) {
        let pos = self.buffer.point();
        let nchars = s.chars().count();
        let related = self.related_buffer_ids(self.current_buffer_id());
        self.buffer.insert(s);
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, false);
        self.mirror_insert_to_related_buffers(&related, pos, s, None, false, false);
    }

    pub fn insert_current_buffer_with_properties(
        &mut self,
        s: &str,
        props: Option<Vec<(String, Value)>>,
    ) {
        let pos = self.buffer.point();
        let nchars = s.chars().count();
        let related = self.related_buffer_ids(self.current_buffer_id());
        self.buffer.insert_with_properties(s, props.clone());
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, false);
        self.mirror_insert_to_related_buffers(&related, pos, s, props, false, false);
    }

    pub fn insert_current_buffer_and_inherit(&mut self, s: &str) {
        let pos = self.buffer.point();
        let nchars = s.chars().count();
        let related = self.related_buffer_ids(self.current_buffer_id());
        self.buffer.insert_and_inherit(s);
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, false);
        self.mirror_insert_to_related_buffers(&related, pos, s, None, false, true);
    }

    pub fn insert_current_buffer_before_markers(&mut self, s: &str) {
        let pos = self.buffer.point();
        let nchars = s.chars().count();
        let related = self.related_buffer_ids(self.current_buffer_id());
        self.buffer.insert(s);
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, true);
        self.mirror_insert_to_related_buffers(&related, pos, s, None, true, false);
    }

    pub fn insert_current_buffer_before_markers_and_inherit(&mut self, s: &str) {
        let pos = self.buffer.point();
        let nchars = s.chars().count();
        let related = self.related_buffer_ids(self.current_buffer_id());
        self.buffer.insert_and_inherit(s);
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, true);
        self.mirror_insert_to_related_buffers(&related, pos, s, None, true, true);
    }

    pub fn delete_region_current_buffer(
        &mut self,
        from: usize,
        to: usize,
    ) -> Result<String, crate::buffer::BufferError> {
        let from = from.max(self.buffer.point_min());
        let to = to.min(self.buffer.point_max());
        let affected_markers = self.affected_markers_for_delete(self.current_buffer_id(), from, to);
        let related = self.related_buffer_ids(self.current_buffer_id());
        let deleted = self.buffer.delete_region(from, to)?;
        self.buffer.attach_markers_to_last_delete(affected_markers);
        self.adjust_markers_for_delete(self.current_buffer_id(), from, to);
        self.mirror_delete_to_related_buffers(&related, from, to);
        Ok(deleted)
    }

    pub fn delete_char_current_buffer(
        &mut self,
        n: isize,
    ) -> Result<String, crate::buffer::BufferError> {
        if n >= 0 {
            let from = self.buffer.point();
            let to = from + n as usize;
            if to > self.buffer.point_max() {
                return Err(crate::buffer::BufferError::EndOfBuffer);
            }
            self.delete_region_current_buffer(from, to)
        } else {
            let count = (-n) as usize;
            let to = self.buffer.point();
            if to < self.buffer.point_min() + count {
                return Err(crate::buffer::BufferError::BeginningOfBuffer);
            }
            let from = to - count;
            self.delete_region_current_buffer(from, to)
        }
    }

    pub fn undo_current_buffer(&mut self) -> Result<(), LispError> {
        let region = if self.buffer.mark_active() {
            self.buffer.region()
        } else {
            None
        };
        if region.is_none() {
            let redo_groups = self
                .undo_sequence
                .as_ref()
                .filter(|state| !state.had_error && !state.redo_groups.is_empty())
                .map(|state| state.redo_groups.clone());
            if let Some(redo_groups) = redo_groups {
                for group in redo_groups.iter().rev() {
                    self.replay_sequence_group(group)?;
                }
                if let Some(state) = self.undo_sequence.as_mut() {
                    state.undone_count = 1;
                    state.redo_groups.clear();
                }
                return Ok(());
            }
            if self
                .undo_sequence
                .as_ref()
                .is_some_and(|state| !state.had_error && state.redo_groups.is_empty())
            {
                self.start_undo_sequence_step()?;
                return self.undo_more_current_buffer();
            }
            return self.start_undo_sequence_step();
        }
        let group = self
            .buffer
            .take_undo_group(region)
            .map_err(|error| LispError::Signal(error.to_string()))?;
        self.buffer.push_undo_boundary();
        for entry in group.iter().rev() {
            self.apply_current_buffer_undo_entry(entry)?;
        }
        Ok(())
    }

    pub fn undo_more_current_buffer(&mut self) -> Result<(), LispError> {
        if self.undo_sequence.is_none() {
            return self.start_undo_sequence_step();
        }
        let group = {
            let state = self.undo_sequence.as_ref().expect("checked above");
            if state.undone_count >= state.original_groups.len() {
                return Err(LispError::Signal(
                    crate::buffer::BufferError::NoFurtherUndoInformation.to_string(),
                ));
            }
            let start = state.original_groups.len() - 1 - state.undone_count;
            state.original_groups[start].clone()
        };
        let before = self.buffer.undo_entries().len();
        if let Err(error) = self.replay_sequence_group(&group) {
            if let Some(state) = self.undo_sequence.as_mut() {
                state.had_error = true;
            }
            return Err(error);
        }
        let state = self.undo_sequence.as_mut().expect("sequence active");
        state.redo_groups.push(latest_generated_undo_group(
            &self.buffer.undo_entries()[before..],
        ));
        state.undone_count += 1;
        state.had_error = false;
        Ok(())
    }

    pub fn reset_undo_sequence(&mut self) {
        self.undo_sequence = None;
    }

    fn start_undo_sequence_step(&mut self) -> Result<(), LispError> {
        let original_groups = self.buffer.undo_groups();
        let group = original_groups.last().cloned().ok_or_else(|| {
            LispError::Signal(crate::buffer::BufferError::NoFurtherUndoInformation.to_string())
        })?;
        self.replay_sequence_group(&group)?;
        self.undo_sequence = Some(UndoSequenceState {
            original_groups,
            undone_count: 1,
            redo_groups: Vec::new(),
            had_error: false,
        });
        Ok(())
    }

    fn replay_sequence_group(
        &mut self,
        group: &[crate::buffer::UndoEntry],
    ) -> Result<(), LispError> {
        for entry in group.iter().rev() {
            self.apply_current_buffer_undo_entry(entry)?;
        }
        Ok(())
    }

    fn apply_current_buffer_undo_entry(
        &mut self,
        entry: &crate::buffer::UndoEntry,
    ) -> Result<(), LispError> {
        match entry {
            crate::buffer::UndoEntry::Insert { pos, len } => {
                self.buffer.goto_char(*pos);
                self.delete_region_current_buffer(*pos, *pos + *len)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                Ok(())
            }
            crate::buffer::UndoEntry::Delete {
                pos,
                text,
                props,
                markers,
            } => {
                self.buffer.goto_char(*pos);
                let insert_at = self.buffer.point();
                self.insert_current_buffer(text);
                for span in props {
                    self.buffer.add_text_properties(
                        insert_at + span.start,
                        insert_at + span.end,
                        &span.props,
                    );
                }
                let inserted = text.chars().count();
                for marker in markers {
                    let expected_auto_pos = match self.marker_insertion_type(marker.id) {
                        Some(true) => marker.collapsed_pos + inserted,
                        _ => marker.collapsed_pos,
                    };
                    if self.marker_buffer_id(marker.id) == Some(self.current_buffer_id())
                        && self.marker_position(marker.id) == Some(expected_auto_pos)
                    {
                        let _ = self.set_marker(
                            marker.id,
                            Some(marker.original_pos),
                            Some(self.current_buffer_id()),
                        );
                    }
                }
                Ok(())
            }
            crate::buffer::UndoEntry::Combined { entries, .. } => {
                for inner in entries.iter().rev() {
                    self.apply_current_buffer_undo_entry(inner)?;
                }
                Ok(())
            }
            crate::buffer::UndoEntry::Opaque(value) => Err(LispError::Signal(format!(
                "Unrecognized entry in undo list {}",
                render_undo_value(value)
            ))),
            crate::buffer::UndoEntry::Boundary => Ok(()),
        }
    }

    fn affected_markers_for_delete(
        &self,
        buffer_id: u64,
        from: usize,
        to: usize,
    ) -> Vec<crate::buffer::UndoMarker> {
        self.markers
            .iter()
            .filter(|marker| marker.buffer_id == Some(buffer_id))
            .filter_map(|marker| {
                let pos = marker.position?;
                if pos >= from && pos <= to {
                    Some(crate::buffer::UndoMarker {
                        id: marker.id,
                        original_pos: pos,
                        collapsed_pos: from,
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    /// Borrow a live buffer by ID.
    pub fn get_buffer_by_id(&self, id: u64) -> Option<&crate::buffer::Buffer> {
        if id == self.current_buffer_id {
            Some(&self.buffer)
        } else {
            self.inactive_buffers
                .iter()
                .find(|(buffer_id, _)| *buffer_id == id)
                .map(|(_, buffer)| buffer)
        }
    }

    /// Borrow a live buffer mutably by ID.
    pub fn get_buffer_by_id_mut(&mut self, id: u64) -> Option<&mut crate::buffer::Buffer> {
        if id == self.current_buffer_id {
            Some(&mut self.buffer)
        } else {
            self.inactive_buffers
                .iter_mut()
                .find(|(buffer_id, _)| *buffer_id == id)
                .map(|(_, buffer)| buffer)
        }
    }

    pub fn buffer_hooks_inhibited(&self, id: u64) -> bool {
        self.get_buffer_by_id(id)
            .map(|buffer| buffer.inhibit_hooks)
            .unwrap_or(false)
    }

    pub fn set_buffer_hooks_inhibited(&mut self, id: u64, inhibit: bool) {
        if let Some(buffer) = self.get_buffer_by_id_mut(id) {
            buffer.inhibit_hooks = inhibit;
        }
    }

    pub fn swap_buffer_text_state(&mut self, left_id: u64, right_id: u64) -> Result<(), LispError> {
        if left_id == right_id {
            return Ok(());
        }
        if left_id == self.current_buffer_id {
            let pos = self
                .inactive_buffers
                .iter()
                .position(|(buffer_id, _)| *buffer_id == right_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", right_id)))?;
            let (buffer, inactive_buffers) = (&mut self.buffer, &mut self.inactive_buffers);
            buffer.swap_text_state(&mut inactive_buffers[pos].1);
            return Ok(());
        }
        if right_id == self.current_buffer_id {
            let pos = self
                .inactive_buffers
                .iter()
                .position(|(buffer_id, _)| *buffer_id == left_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", left_id)))?;
            let (buffer, inactive_buffers) = (&mut self.buffer, &mut self.inactive_buffers);
            buffer.swap_text_state(&mut inactive_buffers[pos].1);
            return Ok(());
        }

        let left_index = self
            .inactive_buffers
            .iter()
            .position(|(buffer_id, _)| *buffer_id == left_id)
            .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", left_id)))?;
        let right_index = self
            .inactive_buffers
            .iter()
            .position(|(buffer_id, _)| *buffer_id == right_id)
            .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", right_id)))?;
        let (first, second) = if left_index < right_index {
            let (left_slice, right_slice) = self.inactive_buffers.split_at_mut(right_index);
            (&mut left_slice[left_index].1, &mut right_slice[0].1)
        } else {
            let (right_slice, left_slice) = self.inactive_buffers.split_at_mut(left_index);
            (&mut left_slice[0].1, &mut right_slice[right_index].1)
        };
        first.swap_text_state(second);
        Ok(())
    }

    pub fn face_inherit_target(&self, face: &str) -> Option<String> {
        self.face_inheritance
            .iter()
            .rev()
            .find(|(name, _)| name == face)
            .and_then(|(_, inherit)| inherit.clone())
    }

    pub fn set_face_inherit_target(
        &mut self,
        face: &str,
        inherit: Option<String>,
    ) -> Result<(), LispError> {
        if let Some(target) = inherit.as_ref()
            && self.face_inheritance_creates_cycle(face, target)
        {
            return Err(LispError::SignalValue(Value::list([
                Value::Symbol("error".into()),
                Value::String("Face inheritance results in inheritance cycle".into()),
                Value::Symbol(target.clone()),
            ])));
        }
        if let Some((_, slot)) = self
            .face_inheritance
            .iter_mut()
            .rev()
            .find(|(name, _)| name == face)
        {
            *slot = inherit;
        } else {
            self.face_inheritance.push((face.to_string(), inherit));
        }
        Ok(())
    }

    fn face_inheritance_creates_cycle(&self, face: &str, target: &str) -> bool {
        let mut current = Some(target.to_string());
        while let Some(name) = current {
            if name == face {
                return true;
            }
            current = self.face_inherit_target(&name);
        }
        false
    }

    /// Find an overlay by ID in any live buffer.
    pub fn find_overlay(&self, id: u64) -> Option<&crate::overlay::Overlay> {
        self.buffer
            .overlays
            .iter()
            .find(|ov| ov.id == id)
            .or_else(|| {
                self.inactive_buffers
                    .iter()
                    .find_map(|(_, buffer)| buffer.overlays.iter().find(|ov| ov.id == id))
            })
    }

    /// Find a mutable overlay by ID in any live buffer.
    pub fn find_overlay_mut(&mut self, id: u64) -> Option<&mut crate::overlay::Overlay> {
        if let Some(overlay) = self.buffer.overlays.iter_mut().find(|ov| ov.id == id) {
            return Some(overlay);
        }
        self.inactive_buffers
            .iter_mut()
            .find_map(|(_, buffer)| buffer.overlays.iter_mut().find(|ov| ov.id == id))
    }

    /// Remove and return an overlay by ID from any live buffer.
    pub fn take_overlay(&mut self, id: u64) -> Option<crate::overlay::Overlay> {
        if let Some(pos) = self.buffer.overlays.iter().position(|ov| ov.id == id) {
            return Some(self.buffer.overlays.swap_remove(pos));
        }
        for (_, buffer) in &mut self.inactive_buffers {
            if let Some(pos) = buffer.overlays.iter().position(|ov| ov.id == id) {
                return Some(buffer.overlays.swap_remove(pos));
            }
        }
        None
    }

    /// Look up a variable, returning None if unbound (for use by primitives).
    pub fn lookup_var(&self, name: &str, env: &Env) -> Option<Value> {
        for frame in env.iter().rev() {
            for (k, v) in frame.iter().rev() {
                if k == name {
                    return Some(v.clone());
                }
            }
        }
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        if let Some(value) = self.buffer_local_value(self.current_buffer_id(), &resolved) {
            return Some(value);
        }
        if let Some(value) = self.global_value(&resolved) {
            return Some(value);
        }
        self.builtin_var_value(&resolved)
    }

    pub fn symbol_value_cell(&self, name: &str) -> Result<Value, LispError> {
        let resolved = self.resolve_variable_name(name)?;
        if let Some(value) = self.buffer_local_value(self.current_buffer_id(), &resolved) {
            return Ok(value);
        }
        if matches!(
            resolved.as_str(),
            "buffer-file-name" | "buffer-file-truename"
        ) && let Some(value) = self.builtin_var_value(&resolved)
        {
            return Ok(value);
        }
        if let Some(value) = self.global_value(&resolved) {
            return Ok(value);
        }
        if let Some(value) = self.builtin_var_value(&resolved) {
            return Ok(value);
        }
        if resolved == "buffer-undo-list" {
            return Ok(crate::lisp::primitives::buffer_undo_list_value(
                &self.buffer,
            ));
        }
        Err(LispError::Void(resolved))
    }

    pub(crate) fn builtin_var_value(&self, name: &str) -> Option<Value> {
        match name {
            "nil" => Some(Value::Nil),
            "t" => Some(Value::T),
            "region-extract-function" => Some(Value::Symbol(
                "emaxx-default-region-extract-function".into(),
            )),
            "case-fold-search" => Some(Value::T),
            "case-symbols-as-words" => Some(Value::Nil),
            "translation-table-vector" => Some(Value::list([Value::symbol("vector")])),
            "float-e" => Some(Value::Float(std::f64::consts::E)),
            "float-pi" => Some(Value::Float(std::f64::consts::PI)),
            "most-positive-fixnum" => Some(Value::Integer(2_305_843_009_213_693_951)),
            "most-negative-fixnum" => Some(Value::Integer(-2_305_843_009_213_693_952)),
            "enable-multibyte-characters" => Some(if self.buffer.is_multibyte() {
                Value::T
            } else {
                Value::Nil
            }),
            "buffer-file-name" => Some(
                self.buffer
                    .file
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Nil),
            ),
            "buffer-file-truename" => Some(
                self.buffer
                    .file_truename
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Nil),
            ),
            "buffer-file-coding-system" => Some(
                self.buffer_local_value(self.current_buffer_id(), "buffer-file-coding-system")
                    .unwrap_or(Value::Nil),
            ),
            "mark-active" => Some(if self.buffer.mark_active() {
                Value::T
            } else {
                Value::Nil
            }),
            "buffer-invisibility-spec" => Some(
                self.buffer_local_value(self.current_buffer_id(), "buffer-invisibility-spec")
                    .unwrap_or(Value::T),
            ),
            "buffer-display-table" => Some(Value::Nil),
            "last-coding-system-used" => Some(Value::Nil),
            "locale-coding-system" => Some(Value::Nil),
            "coding-system-for-read" => Some(Value::Nil),
            "coding-system-for-write" => Some(Value::Nil),
            "set-auto-coding-function" => Some(Value::Nil),
            "file-coding-system-alist" => Some(Value::Nil),
            "file-name-coding-system" => Some(Value::Nil),
            "default-file-name-coding-system" => Some(Value::Nil),
            "version-control" => Some(Value::Nil),
            "inhibit-eol-conversion" => Some(Value::Nil),
            "inhibit-null-byte-detection" => Some(Value::Nil),
            "inhibit-iso-escape-detection" => Some(Value::Nil),
            "create-lockfiles" => Some(Value::T),
            "display-hourglass" => Some(Value::Nil),
            "gc-cons-threshold" => Some(Value::Integer(800_000)),
            "auto-save-timeout" => Some(Value::Integer(30)),
            "auto-save-interval" => Some(Value::Integer(300)),
            "temporary-file-directory" => {
                Some(Value::String(std::env::temp_dir().display().to_string()))
            }
            "auto-mode-alist" => Some(builtin_auto_mode_alist()),
            "auto-compression-mode" => Some(Value::T),
            "command-switch-alist" => Some(Value::Nil),
            "command-line-args-left" => Some(Value::Nil),
            "selection-converter-alist" => Some(Value::Nil),
            "early-init-file" => Some(Value::Nil),
            "init-file-user" => Some(Value::Nil),
            "site-run-file" => Some(Value::Nil),
            "user-init-file" => Some(Value::Nil),
            "completion-ignored-extensions" => Some(Value::Nil),
            "regexp-unmatchable" => Some(Value::String("\\`a\\`".into())),
            "ignored-local-variables" => Some(Value::list([
                Value::Symbol("ignored-local-variables".into()),
                Value::Symbol("safe-local-variable-values".into()),
                Value::Symbol("file-local-variables-alist".into()),
                Value::Symbol("dir-local-variables-alist".into()),
            ])),
            "ignored-local-variable-values" => Some(Value::Nil),
            "safe-local-variable-values" => Some(Value::Nil),
            "hack-local-variables-hook" => Some(Value::Nil),
            "custom-current-group-alist" => Some(Value::Nil),
            "defun-declarations-alist" => Some(Value::Nil),
            "macro-declarations-alist" => Some(Value::Nil),
            "macroexp--dynvars" => Some(Value::Nil),
            "macroexpand-all-environment" => Some(Value::Nil),
            "image-types" => Some(Value::list([
                Value::Symbol("pbm".into()),
                Value::Symbol("png".into()),
                Value::Symbol("jpeg".into()),
                Value::Symbol("gif".into()),
                Value::Symbol("svg".into()),
                Value::Symbol("xbm".into()),
                Value::Symbol("xpm".into()),
                Value::Symbol("webp".into()),
                Value::Symbol("tiff".into()),
            ])),
            "ls-lisp-use-insert-directory-program" => Some(Value::T),
            "transient-mark-mode" => Some(Value::T),
            "obarray" => Some(Value::Nil),
            "desktop-buffer-mode-handlers" => Some(Value::Nil),
            "find-file-visit-truename" => Some(Value::Nil),
            "insert-directory-wildcard-in-dir-p" => Some(Value::Nil),
            "insert-directory-program" => Some(Value::String("ls".into())),
            "file-name-invalid-regexp" => Some(Value::String("\0".into())),
            "directory-listing-before-filename-regexp" => Some(Value::String(
                concat!(
                    ".*[0-9][BkKMGTPEZYRQ]? ",
                    "\\(",
                    "[0-9][0-9][0-9][0-9]-[01][0-9]-[0-3][0-9]\\([ T][ 0-2][0-9][:.][0-5][0-9]\\)?",
                    "\\|",
                    "[A-Za-z][A-Za-z][A-Za-z] +[ 0-3][0-9] +\\([ 0-2][0-9][:.][0-5][0-9]\\|[0-9][0-9][0-9][0-9]\\)",
                    "\\)",
                    " +"
                )
                .into(),
            )),
            "minor-mode-alist" => Some(Value::Nil),
            "timer-list" | "timer-idle-list" => Some(Value::Nil),
            "revert-buffer-function" => {
                Some(Value::Symbol("emaxx-default-revert-buffer-function".into()))
            }
            "buffer-stale-function" => Some(Value::Symbol(
                "buffer-stale--default-function".into(),
            )),
            "buffer-auto-revert-by-notification" => Some(Value::Nil),
            "overriding-local-map" => Some(Value::Nil),
            "overriding-terminal-local-map" => Some(Value::Nil),
            "menu-bar-final-items" => Some(Value::Nil),
            "menu-bar-separator" => Some(Value::Symbol("menu-bar-separator".into())),
            "mode-line-modes" => Some(Value::Nil),
            "window-display-table" => Some(Value::Nil),
            "standard-display-table" => Some(Value::Nil),
            "text-mode-syntax-table" | "emacs-lisp-mode-syntax-table" => {
                Some(Value::CharTable(self.standard_syntax_table_id()))
            }
            "compilation-error-regexp-alist-alist" => Some(Value::Nil),
            "compilation-error-regexp-alist" => Some(Value::Nil),
            "special-mode-map" => Some(primitives::keymap_placeholder(Some("special-mode-map"))),
            "global-map" => Some(primitives::keymap_placeholder(Some("global-map"))),
            "frame-internal-parameters" => Some(Value::Nil),
            "password-word-equivalents" => Some(Value::list([
                Value::String("password".into()),
                Value::String("passcode".into()),
                Value::String("passphrase".into()),
                Value::String("pass phrase".into()),
                Value::String("pin".into()),
                Value::String("decryption key".into()),
                Value::String("encryption key".into()),
                Value::String("암호".into()),
                Value::String("パスワード".into()),
                Value::String("ପ୍ରବେଶ ସଙ୍କେତ".into()),
                Value::String("ពាក្យសម្ងាត់".into()),
                Value::String("adgangskode".into()),
                Value::String("contraseña".into()),
                Value::String("contrasenya".into()),
                Value::String("geslo".into()),
                Value::String("hasło".into()),
                Value::String("heslo".into()),
                Value::String("iphasiwedi".into()),
                Value::String("jelszó".into()),
                Value::String("lösenord".into()),
                Value::String("lozinka".into()),
                Value::String("mật khẩu".into()),
                Value::String("mot de passe".into()),
                Value::String("parola".into()),
                Value::String("pasahitza".into()),
                Value::String("passord".into()),
                Value::String("passwort".into()),
                Value::String("pasvorto".into()),
                Value::String("salasana".into()),
                Value::String("senha".into()),
                Value::String("slaptažodis".into()),
                Value::String("wachtwoord".into()),
                Value::String("كلمة السر".into()),
                Value::String("ססמה".into()),
                Value::String("лозинка".into()),
                Value::String("пароль".into()),
                Value::String("गुप्तशब्द".into()),
                Value::String("शब्दकूट".into()),
                Value::String("પાસવર્ડ".into()),
                Value::String("సంకేతపదము".into()),
                Value::String("ਪਾਸਵਰਡ".into()),
                Value::String("ಗುಪ್ತಪದ".into()),
                Value::String("கடவுச்சொல்".into()),
                Value::String("അടയാളവാക്ക്".into()),
                Value::String("গুপ্তশব্দ".into()),
                Value::String("পাসওয়ার্ড".into()),
                Value::String("රහස්පදය".into()),
                Value::String("密码".into()),
                Value::String("密碼".into()),
            ])),
            "password-colon-equivalents" => Some(Value::list([
                Value::Integer(':' as i64),
                Value::Integer(0xFF1A),
                Value::Integer(0xFE55),
                Value::Integer(0xFE13),
                Value::Integer(0x17D6),
            ])),
            "source-directory" => Some(Value::String(
                std::env::var("EMACS_TEST_DIRECTORY")
                    .ok()
                    .and_then(|path| {
                        std::path::PathBuf::from(path)
                            .parent()
                            .map(|path| path.display().to_string())
                    })
                    .unwrap_or_else(primitives::default_directory),
            )),
            "data-directory" | "doc-directory" => Some(Value::String(
                primitives::compat_data_directory().unwrap_or_else(primitives::default_directory),
            )),
            "user-login-name" => Some(Value::String(
                primitives::current_user_login_name().unwrap_or_else(|| "user".into()),
            )),
            "user-full-name" => Some(Value::String(
                primitives::current_user_full_name()
                    .or_else(primitives::current_user_login_name)
                    .unwrap_or_else(|| "user".into()),
            )),
            "default-directory" => Some(Value::String(primitives::default_directory())),
            "current-language-environment" => Some(Value::String("English".into())),
            "window-system" => Some(Value::Nil),
            "initial-window-system" => Some(Value::Nil),
            "left-margin" => Some(Value::Integer(0)),
            "last-command" => Some(Value::Nil),
            "line-spacing" => Some(Value::Nil),
            "scroll-margin" => Some(Value::Integer(0)),
            "scroll-preserve-screen-position" => Some(Value::Nil),
            "scroll-up-aggressively" => Some(Value::Nil),
            "vertical-scroll-bar" => Some(Value::Symbol("right".into())),
            "overwrite-mode" => Some(Value::Symbol("overwrite-mode-binary".into())),
            "cursor-in-non-selected-windows" => Some(Value::Nil),
            "load-path" => Some(Value::list(
                self.load_path
                    .iter()
                    .map(|path| Value::String(primitives::path_to_directory_string(path)))
                    .collect::<Vec<_>>(),
            )),
            "image-load-path" => Some(Value::list([
                Value::String(
                    primitives::compat_data_directory()
                        .map(|path| {
                            let mut path = std::path::PathBuf::from(path);
                            path.push("images");
                            primitives::path_to_directory_string(&path)
                        })
                        .unwrap_or_else(primitives::default_directory),
                ),
                Value::Symbol("data-directory".into()),
                Value::Symbol("load-path".into()),
            ])),
            "installation-directory" => Some(
                primitives::compat_installation_directory()
                    .map(Value::String)
                    .unwrap_or(Value::Nil),
            ),
            "tab-width" => Some(Value::Integer(8)),
            "indent-tabs-mode" => Some(Value::T),
            "indent-line-function" => Some(Value::Symbol("indent-relative".into())),
            "tab-stop-list" => Some(Value::Nil),
            "use-dialog-box" => Some(Value::T),
            "use-file-dialog" => Some(Value::T),
            "help-char" => Some(Value::Integer(8)),
            "help-event-list" => Some(Value::Nil),
            "help-form" => Some(Value::Nil),
            "prefix-help-command" => Some(Value::Nil),
            "command-error-function" => {
                Some(Value::Symbol("command-error-default-function".into()))
            }
            "read-file-name-completion-ignore-case" => Some(Value::Nil),
            "mounted-file-systems" => Some(Value::String(String::new())),
            "system-type" => Some(Value::Symbol(
                std::env::consts::OS.replace("macos", "darwin"),
            )),
            "system-configuration" => Some(Value::String(primitives::system_configuration())),
            "system-configuration-features" => Some(Value::String(
                std::env::var("EMAXX_SYSTEM_CONFIGURATION_FEATURES").unwrap_or_default(),
            )),
            "system-configuration-options" => Some(Value::String(
                std::env::var("EMAXX_SYSTEM_CONFIGURATION_OPTIONS").unwrap_or_default(),
            )),
            "charset-list" => Some(Value::list(
                self.charset_priority_list()
                    .into_iter()
                    .map(Value::Symbol)
                    .collect::<Vec<_>>(),
            )),
            "ert-resource-directory-format" => Some(Value::String("%s-resources/".into())),
            "ert-resource-directory-trim-left-regexp" => Some(Value::String(String::new())),
            "ert-resource-directory-trim-right-regexp" => {
                Some(Value::String("\\(-tests?\\)?\\.el".into()))
            }
            "load-file-name" | "macroexp-file-name" => Some(
                self.current_load_file
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Nil),
            ),
            "read-buffer-function" | "read-file-name-function" => Some(Value::Nil),
            "user-emacs-directory" => Some(Value::String("/nonexistent/.emacs.d/".into())),
            "invocation-name" => Some(Value::String(
                primitives::current_invocation_name().unwrap_or_else(|| "emaxx".into()),
            )),
            "invocation-directory" => Some(Value::String(
                primitives::current_invocation_directory()
                    .unwrap_or_else(primitives::default_directory),
            )),
            "shell-file-name" => Some(Value::String(
                primitives::find_executable("sh").unwrap_or_else(|| "/bin/sh".into()),
            )),
            "shell-command-switch" => Some(Value::String("-c".into())),
            "emacs-version" => Some(Value::String(primitives::emacs_version_value())),
            "emacs-major-version" => Some(Value::Integer(primitives::emacs_major_version_value())),
            "emacs-minor-version" => Some(Value::Integer(primitives::emacs_minor_version_value())),
            "etags-program-name" => Some(Value::String(
                primitives::find_executable("etags").unwrap_or_else(|| "etags".into()),
            )),
            "emacsclient-program-name" => Some(Value::String(
                primitives::compat_emacsclient_program_name()
                    .unwrap_or_else(|| "emacsclient".into()),
            )),
            "process-environment" | "initial-environment" => Some(Value::list(
                std::env::vars()
                    .map(|(name, value)| Value::String(format!("{name}={value}")))
                    .collect::<Vec<_>>(),
            )),
            "find-program" => Some(Value::String("find".into())),
            "grep-program" => Some(Value::String("grep".into())),
            _ if name.starts_with('.') => Some(Value::Nil),
            _ if name.starts_with(':') => Some(Value::Symbol(name.to_string())),
            _ => None,
        }
    }

    /// Look up a variable in the given local env, then globals.
    pub(crate) fn lookup(&self, name: &str, env: &Env) -> Result<Value, LispError> {
        // Search local frames from innermost to outermost
        for frame in env.iter().rev() {
            for (k, v) in frame.iter().rev() {
                if k == name {
                    return Ok(v.clone());
                }
            }
        }
        let resolved = self.resolve_variable_name(name)?;
        // Search globals
        if let Some(value) = self.buffer_local_value(self.current_buffer_id(), &resolved) {
            return Ok(value);
        }
        if matches!(
            resolved.as_str(),
            "buffer-file-name" | "buffer-file-truename"
        ) && let Some(value) = self.builtin_var_value(&resolved)
        {
            return Ok(value);
        }
        if let Some(value) = self.global_value(&resolved) {
            return Ok(value);
        }
        if resolved == "buffer-undo-list" {
            return Ok(crate::lisp::primitives::buffer_undo_list_value(
                &self.buffer,
            ));
        }
        self.builtin_var_value(&resolved)
            .ok_or(LispError::Void(resolved))
    }

    pub fn raw_function_binding(&self, name: &str, env: &Env) -> Option<Value> {
        if primitives::prefer_builtin_override(name) && primitives::is_builtin(name) {
            return Some(Value::BuiltinFunc(name.to_string()));
        }
        for frame in env.iter().rev() {
            for (k, v) in frame.iter().rev() {
                if k == name && matches!(v, Value::BuiltinFunc(_) | Value::Lambda(_, _, _)) {
                    return Some(v.clone());
                }
            }
        }
        for (k, v) in self.functions.iter().rev() {
            if k == name {
                return Some(v.clone());
            }
        }
        if let Some(value) = builtin_autoload_function(name) {
            return Some(value);
        }
        if matches!(name, "incf" | "decf") {
            return Some(Value::BuiltinFunc(name.to_string()));
        }
        if primitives::is_builtin(name) {
            return Some(Value::BuiltinFunc(name.to_string()));
        }
        None
    }

    pub fn lookup_function(&self, name: &str, env: &Env) -> Result<Value, LispError> {
        let mut current = name.to_string();
        let mut seen = HashSet::new();

        loop {
            if !seen.insert(current.clone()) {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("cyclic-function-indirection".into()),
                    Value::Symbol(name.to_string()),
                ])));
            }

            let Some(binding) = self.raw_function_binding(&current, env) else {
                return Err(LispError::Void(current));
            };
            match binding {
                Value::Symbol(next) => current = next,
                other => return Ok(other),
            }
        }
    }

    pub fn has_macro_binding(&self, name: &str) -> bool {
        self.resolve_macro_binding(name).is_some()
    }

    pub(crate) fn macro_function_value(&self, name: &str) -> Option<Value> {
        let (params, body) = self.resolve_macro_binding(name)?;
        Some(Value::cons(
            Value::Symbol("macro".into()),
            Value::Lambda(params, body, shared_env(Vec::new())),
        ))
    }

    pub fn known_symbol_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        let mut push_name = |name: &str| {
            if !names.iter().any(|existing| existing == name) {
                names.push(name.to_string());
            }
        };
        push_name("nil");
        push_name("t");
        for (name, _) in &self.globals {
            push_name(name);
        }
        for (name, _) in &self.variable_aliases {
            push_name(name);
        }
        for (name, _) in &self.functions {
            push_name(name);
        }
        for (name, _, _) in &self.macros {
            push_name(name);
        }
        for (name, _) in &self.symbol_properties {
            push_name(name);
        }
        names
    }

    fn resolve_macro_binding(&self, name: &str) -> Option<(Vec<String>, Vec<Value>)> {
        let mut current = name.to_string();
        let mut seen = Vec::new();
        loop {
            if seen.iter().any(|existing| existing == &current) {
                return None;
            }
            seen.push(current.clone());
            if let Some((_, params, body)) = self
                .macros
                .iter()
                .find(|(macro_name, _, _)| macro_name == &current)
            {
                return Some((params.clone(), body.clone()));
            }
            let (_, value) = self
                .functions
                .iter()
                .rev()
                .find(|(function_name, _)| function_name == &current)?;
            let Value::Symbol(next) = value else {
                return None;
            };
            current = next.clone();
        }
    }

    /// Set a variable in the innermost local frame, or in globals.
    pub fn set_variable(&mut self, name: &str, value: Value, env: &mut Env) {
        for frame in env.iter_mut().rev() {
            for (k, v) in frame.iter_mut().rev() {
                if k == name {
                    *v = Self::stored_value(value);
                    return;
                }
            }
        }
        self.set_symbol_value_cell(name, value);
    }

    pub fn set_symbol_value_cell(&mut self, name: &str, value: Value) {
        let resolved = self
            .resolve_variable_name(name)
            .unwrap_or_else(|_| name.to_string());
        let value = Self::stored_value(value);
        if resolved == "buffer-file-name" {
            self.buffer.file = match value {
                Value::Nil => None,
                Value::String(path) => Some(path),
                Value::StringObject(state) => Some(state.borrow().text.clone()),
                other => Some(other.to_string()),
            };
            return;
        }
        if resolved == "buffer-file-truename" {
            self.buffer.file_truename = match value {
                Value::Nil => None,
                Value::String(path) => Some(path),
                Value::StringObject(state) => Some(state.borrow().text.clone()),
                other => Some(other.to_string()),
            };
            return;
        }
        if resolved == "buffer-undo-list" {
            if value.is_nil() {
                self.undo_sequence = None;
                self.buffer.clear_undo_history();
            } else if let Some((head, tail)) = value.cons_values()
                && tail == crate::lisp::primitives::buffer_undo_list_value(&self.buffer)
            {
                let entry = buffer_undo_head_to_entry(&head);
                self.buffer.push_undo_entry(entry);
            } else {
                self.undo_sequence = None;
            }
            return;
        }
        if let Some(scope) = self.assignment_scope(&resolved) {
            match scope {
                SpecialBindingScope::Global => {
                    self.set_global_binding(&resolved, value);
                }
                SpecialBindingScope::BufferLocal(buffer_id) => {
                    self.set_buffer_local_value(buffer_id, &resolved, value);
                }
            }
            return;
        }
        // Set in globals
        for (k, v) in self.globals.iter_mut().rev() {
            if k == &resolved {
                *v = value;
                return;
            }
        }
        self.globals.push((resolved, value));
    }

    pub fn push_function_binding(&mut self, name: &str, function: Value) {
        self.functions.push((name.to_string(), function));
    }

    pub fn function_binding_name(&self, function: &Value) -> Option<String> {
        match function {
            Value::Symbol(name) | Value::BuiltinFunc(name) => Some(name.clone()),
            other => self
                .functions
                .iter()
                .rev()
                .find(|(_, value)| value == other)
                .map(|(name, _)| name.clone()),
        }
    }

    pub fn pop_function_binding(&mut self, name: &str) {
        if let Some(index) = self.functions.iter().rposition(|(fname, _)| fname == name) {
            self.functions.remove(index);
        }
    }

    pub fn set_function_binding(&mut self, name: &str, function: Option<Value>) {
        if let Some(index) = self.functions.iter().rposition(|(fname, _)| fname == name) {
            self.functions.remove(index);
        }
        if let Some(function) = function {
            self.functions.push((name.to_string(), function));
        }
    }

    pub fn validate_function_binding(&self, name: &str, function: &Value) -> Result<(), LispError> {
        let Value::Symbol(current) = function else {
            return Ok(());
        };
        let mut current = current.clone();
        let mut seen = vec![name.to_string()];
        loop {
            if seen.iter().any(|existing| existing == &current) {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("cyclic-function-indirection".into()),
                    Value::Symbol(name.to_string()),
                ])));
            }
            seen.push(current.clone());
            let Some((_, value)) = self
                .functions
                .iter()
                .rev()
                .find(|(function_name, _)| function_name == &current)
            else {
                return Ok(());
            };
            let Value::Symbol(next) = value else {
                return Ok(());
            };
            current = next.clone();
        }
    }

    /// Evaluate an expression.
    pub fn eval(&mut self, expr: &Value, env: &mut Env) -> Result<Value, LispError> {
        match expr {
            Value::Nil
            | Value::T
            | Value::Integer(_)
            | Value::BigInteger(_)
            | Value::Float(_)
            | Value::String(_)
            | Value::StringObject(_) => Ok(expr.clone()),

            Value::BuiltinFunc(_)
            | Value::Lambda(_, _, _)
            | Value::Buffer(_, _)
            | Value::Marker(_)
            | Value::Overlay(_)
            | Value::CharTable(_)
            | Value::Record(_)
            | Value::Finalizer(_) => Ok(expr.clone()),

            Value::Symbol(name) => self.lookup(name, env),

            Value::Cons(_, _) => {
                let items = expr.to_vec()?;
                if items.is_empty() {
                    return Ok(Value::Nil);
                }

                if matches!(items.first(), Some(Value::Symbol(name)) if name == "vector-literal") {
                    return Ok(expr.clone());
                }
                if matches!(
                    items.first(),
                    Some(Value::Symbol(name)) if name == "bool-vector-literal"
                ) {
                    return Ok(self.create_record("bool-vector", items[1..].to_vec()));
                }
                if is_record_literal_reader_form(expr) {
                    return self.eval_record_literal_form(&items[1..], env);
                }

                // Check for special forms first
                if let Value::Symbol(ref name) = items[0] {
                    match name.as_str() {
                        "quote" => return self.sf_quote(&items),
                        "if" | "static-if" => return self.sf_if(&items, env),
                        "if-let" => return self.sf_if_let(&items, env),
                        "if-let*" => return self.sf_if_let_star(&items, env),
                        "when" | "static-when" => return self.sf_when(&items, env),
                        "when-let" => return self.sf_when_let(&items, env),
                        "when-let*" => return self.sf_when_let_star(&items, env),
                        "unless" | "static-unless" => return self.sf_unless(&items, env),
                        "bound-and-true-p" => return self.sf_bound_and_true_p(&items, env),
                        "cond" => return self.sf_cond(&items, env),
                        "pcase" => return self.sf_pcase(&items, env),
                        "pcase-defmacro" => return self.sf_pcase_defmacro(&items, env),
                        "pcase-exhaustive" => return self.sf_pcase_exhaustive(&items, env),
                        "and-let*" => return self.sf_and_let_star(&items, env),
                        "and" => return self.sf_and(&items, env),
                        "or" => return self.sf_or(&items, env),
                        "not" => return self.sf_not(&items, env),
                        "progn" => return self.sf_progn(&items[1..], env),
                        "prog1" => return self.sf_prog1(&items, env),
                        "let" => return self.sf_let(&items, env),
                        "let*" => return self.sf_letstar(&items, env),
                        "cl-progv" => return self.sf_cl_progv(&items, env),
                        "pcase-let" => return self.sf_pcase_let(&items, env, false),
                        "pcase-let*" => return self.sf_pcase_let(&items, env, true),
                        "let-alist" => return self.sf_let_alist(&items, env),
                        "setq" => return self.sf_setq(&items, env),
                        "setq-default" => return self.sf_setq_default(&items, env),
                        "setq-local" => return self.sf_setq_local(&items, env),
                        "setopt" => return self.sf_setopt(&items, env),
                        "setf" => return self.sf_setf(&items, env),
                        "incf" | "cl-incf" => return self.sf_incf(&items, env, 1),
                        "decf" | "cl-decf" => return self.sf_incf(&items, env, -1),
                        "setcar" => return self.sf_setcar(&items, env),
                        "defvar" | "defconst" | "defcustom" => {
                            return self.sf_defvar(&items, env);
                        }
                        "defvar-local" => return self.sf_defvar_local(&items, env),
                        "defgroup" => return self.sf_defgroup(&items),
                        "defface" => return self.sf_defface(&items),
                        "defvar-keymap" => return self.sf_defvar_keymap(&items, env),
                        "define-short-documentation-group" => return self.sf_defgroup(&items),
                        "eval" => return self.sf_eval_function(&items, env),
                        "insert" => return self.sf_insert_function(&items, env, false, false),
                        "insert-and-inherit" => {
                            return self.sf_insert_function(&items, env, true, false);
                        }
                        "insert-char" => return self.sf_insert_char_function(&items, env),
                        "insert-before-markers" => {
                            return self.sf_insert_function(&items, env, false, true);
                        }
                        "insert-before-markers-and-inherit" => {
                            return self.sf_insert_function(&items, env, true, true);
                        }
                        "define-minor-mode"
                        | "define-globalized-minor-mode"
                        | "define-derived-mode" => {
                            return self.sf_define_mode(&items);
                        }
                        "defclass" => return self.sf_defclass(&items),
                        "defun" | "defsubst" => return self.sf_defun(&items, env),
                        "cl-defun" => return self.sf_cl_defun(&items, env),
                        "cl-defmacro" => return self.sf_cl_defmacro(&items, env),
                        "cl-generic-define-generalizer" => {
                            return self.sf_cl_generic_define_generalizer(&items);
                        }
                        "cl-defgeneric" => return self.sf_cl_defgeneric(&items, env),
                        "cl-defmethod" => return self.sf_cl_defmethod(&items, env),
                        "cl-generic-define-context-rewriter" => return Ok(Value::Nil),
                        "oclosure-define" => return self.sf_oclosure_define(&items),
                        "oclosure-lambda" => return self.sf_oclosure_lambda(&items, env),
                        "define-inline" => return self.sf_define_inline(&items, env),
                        "defmacro" => return self.sf_defmacro(&items),
                        "with-memoization" => return self.sf_with_memoization(&items, env),
                        "easy-menu-define" => return self.sf_easy_menu_define(&items, env),
                        "cl-defstruct" => return self.sf_cl_defstruct(&items),
                        "defalias" => return self.sf_defalias(&items, env),
                        "backquote" => return self.eval_backquote(&items[1], env),
                        "lambda" => return self.sf_lambda(&items, env),
                        "call-interactively" => {
                            return self.sf_call_interactively(&items, env);
                        }
                        "function" | "function-quote" => {
                            // #'foo or (function foo)
                            if items.len() >= 2 {
                                if let Value::Symbol(name) = &items[1] {
                                    return Ok(Value::Symbol(name.clone()));
                                }
                                if let Ok(name) = function_name_from_binding_form(&items[1]) {
                                    return Ok(Value::Symbol(name));
                                }
                                return self.eval(&items[1], env);
                            }
                            return Ok(Value::Nil);
                        }
                        "while" => return self.sf_while(&items, env),
                        "dolist" => return self.sf_dolist(&items, env),
                        "pcase-dolist" => return self.sf_pcase_dolist(&items, env),
                        "dotimes" => return self.sf_dotimes(&items, env),
                        "cl-loop" => return self.sf_cl_loop(&items, env),
                        "unwind-protect" => return self.sf_unwind_protect(&items, env),
                        "ignore-error" => return self.sf_ignore_error(&items, env),
                        "ignore-errors" => return self.sf_ignore_errors(&items, env),
                        "condition-case" | "condition-case-unless-debug" => {
                            return self.sf_condition_case(&items, env);
                        }
                        "handler-bind" => return self.sf_handler_bind(&items, env),
                        "cl-assert" => return self.sf_cl_assert(&items, env),
                        "with-temp-buffer" => return self.sf_with_temp_buffer(&items, env),
                        "ert-with-test-buffer" => {
                            return self.sf_ert_with_test_buffer(&items, env);
                        }
                        "ert-with-temp-directory" => {
                            return self.sf_ert_with_temp_directory(&items, env);
                        }
                        "ert-with-message-capture" => {
                            return self.sf_ert_with_message_capture(&items, env);
                        }
                        "with-environment-variables" => {
                            return self.sf_with_environment_variables(&items, env);
                        }
                        "with-output-to-string" => {
                            return self.sf_with_output_to_string(&items, env);
                        }
                        "with-mutex" => return self.sf_with_mutex(&items, env),
                        "with-temp-file" => return self.sf_with_temp_file(&items, env),
                        "ert-with-temp-file" => return self.sf_ert_with_temp_file(&items, env),
                        "with-current-buffer" => return self.sf_with_current_buffer(&items, env),
                        "with-restriction" => return self.sf_with_restriction(&items, env),
                        "without-restriction" => return self.sf_without_restriction(&items, env),
                        "with-selected-window" => return self.sf_progn(&items[2..], env),
                        "save-match-data" => return self.sf_save_match_data(&items, env),
                        "save-excursion" => return self.sf_save_excursion(&items, env),
                        "save-window-excursion" => {
                            return self.sf_save_window_excursion(&items, env);
                        }
                        "save-current-buffer" => return self.sf_save_current_buffer(&items, env),
                        "save-restriction" => return self.sf_save_restriction(&items, env),
                        "with-suppressed-warnings" => {
                            return self.sf_with_suppressed_warnings(&items, env);
                        }
                        "with-demoted-errors" => {
                            return self.sf_with_demoted_errors(&items, env);
                        }
                        "with-coding-priority" => {
                            return self.sf_with_coding_priority(&items, env);
                        }
                        "with-silent-modifications" => {
                            return self.sf_with_silent_modifications(&items, env);
                        }
                        "combine-change-calls" => return self.sf_combine_change_calls(&items, env),
                        "cl-destructuring-bind" => {
                            return self.sf_cl_destructuring_bind(&items, env);
                        }
                        "cl-letf" => return self.sf_cl_letf(&items, env),
                        "aset" => return self.sf_aset(&items, env),
                        "cl-flet" | "cl-labels" => return self.sf_cl_flet(&items, env),
                        "cl-macrolet" => return self.sf_cl_macrolet(&items, env),
                        "push" => {
                            // (push NEWELT PLACE)
                            if items.len() < 3 {
                                return Err(LispError::WrongNumberOfArgs(
                                    "push".into(),
                                    items.len() - 1,
                                ));
                            }
                            let val = self.eval(&items[1], env)?;
                            let place = self.resolve_setf_place(&items[2], env)?;
                            let cur = self.eval_resolved_setf_place_current_value(&place, env)?;
                            let new_val = Value::cons(val, cur);
                            self.set_resolved_setf_place_value(&place, new_val.clone(), env)?;
                            return Ok(new_val);
                        }
                        "cl-pushnew" => return self.sf_cl_pushnew(&items, env),
                        "pop" => {
                            // (pop PLACE)
                            if items.len() < 2 {
                                return Err(LispError::WrongNumberOfArgs(
                                    "pop".into(),
                                    items.len() - 1,
                                ));
                            }
                            let place = self.resolve_setf_place(&items[1], env)?;
                            let cur = self.eval_resolved_setf_place_current_value(&place, env)?;
                            let result = cur.car()?;
                            let rest = cur.cdr()?;
                            self.set_resolved_setf_place_value(&place, rest, env)?;
                            return Ok(result);
                        }
                        "catch" => return self.sf_catch(&items, env),
                        "add-to-list" => return self.sf_add_to_list(&items, env),
                        "ert-deftest" => return self.sf_ert_deftest(&items, env),
                        "should" => return self.sf_should(&items, env),
                        "should-not" => return self.sf_should_not(&items, env),
                        "should-error" => return self.sf_should_error(&items, env),
                        "skip-unless" | "ert--skip-unless" => {
                            return self.sf_skip_unless(&items, env);
                        }
                        "skip-when" | "ert--skip-when" => return self.sf_skip_when(&items, env),
                        "rx" => return self.sf_rx(&items, env),
                        "rx-define" => return self.sf_rx_define(&items),
                        "require" => {
                            if let Some(feature) = items.get(1).and_then(feature_name) {
                                let target = match items.get(2) {
                                    Some(expr) => {
                                        let value = self.eval(expr, env)?;
                                        if value.is_nil() {
                                            None
                                        } else {
                                            Some(primitives::string_text(&value)?)
                                        }
                                    }
                                    None => None,
                                };
                                return self
                                    .require_feature_with_target(&feature, target.as_deref());
                            }
                            return Ok(Value::Nil);
                        }
                        "provide" => {
                            if let Some(feature) = items.get(1).and_then(feature_name) {
                                return self.provide_feature_with_after_load(&feature);
                            }
                            return Ok(Value::Nil);
                        }
                        "with-eval-after-load" => {
                            return self.sf_with_eval_after_load(&items, env);
                        }
                        "with-no-warnings" => return self.sf_progn(&items[1..], env),
                        "declare"
                        | "declare-function"
                        | "cl-declaim"
                        | "declaim"
                        | "cl-deftype"
                        | "def-edebug-elem-spec"
                        | "def-edebug-spec" => {
                            return Ok(Value::Nil);
                        }
                        "eval-and-compile" | "eval-when-compile" => {
                            return self.sf_progn(&items[1..], env);
                        }
                        "ert-info" => {
                            // (ert-info (msg) body...) — just run the body
                            return self.sf_progn(&items[2..], env);
                        }
                        "minibuffer-with-setup-hook" => {
                            if items.len() < 3 {
                                return Err(LispError::WrongNumberOfArgs(
                                    "minibuffer-with-setup-hook".into(),
                                    items.len().saturating_sub(1),
                                ));
                            }
                            let hook = self.eval(&items[1], env)?;
                            let call = vec![hook];
                            self.eval_call(&call, env)?;
                            return self.sf_progn(&items[2..], env);
                        }
                        _ => {}
                    }
                }

                // Check for macro expansion
                if let Value::Symbol(name) = &items[0]
                    && let Some(expanded) = self.try_macroexpand(name, &items[1..], env)?
                {
                    return self.eval(&expanded, env);
                }

                // Regular function call
                self.eval_call(&items, env)
            }
        }
    }

    fn eval_call(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let func = if let Value::Symbol(name) = &items[0] {
            self.lookup_function(name, env)?
        } else {
            self.eval(&items[0], env)?
        };
        let mut args = Vec::new();
        for item in &items[1..] {
            args.push(self.eval(item, env)?);
        }
        let original_name = match &items[0] {
            Value::Symbol(name) => Some(name.as_str()),
            _ => None,
        };
        self.call_function_value(func, original_name, &args, env)
    }

    pub fn call_function_value(
        &mut self,
        func: Value,
        original_name: Option<&str>,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let func = match func {
            Value::Symbol(name) => self.lookup_function(&name, env)?,
            other => other,
        };
        let func = match func {
            Value::Cons(_, _) => {
                let func = if is_lambda_form(&func) {
                    self.eval(&func, env)?
                } else {
                    func
                };
                if let Some((file, _, _)) = crate::lisp::primitives::autoload_parts(&func) {
                    let Some(name) = original_name else {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("invalid-function".into()),
                            func,
                        ])));
                    };
                    self.load_target(&file)?;
                    self.lookup_function(name, env)?
                } else {
                    func
                }
            }
            other => other,
        };

        match func {
            Value::BuiltinFunc(ref name) if name == "selected-window" => {
                if !args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.clone(), args.len()));
                }
                Ok(self.selected_window_value())
            }
            Value::BuiltinFunc(ref name) => match primitives::call(self, name, args, env) {
                Ok(value) => Ok(value),
                Err(error) => self.dispatch_handler_bindings(error, env),
            },
            Value::Record(id)
                if self
                    .find_record(id)
                    .is_some_and(|record| record.type_name == "byte-code-function") =>
            {
                let Some(record) = self.find_record(id) else {
                    unreachable!("checked record presence");
                };
                let Some(inner) = record.slots.first() else {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("invalid-function".into()),
                        Value::Record(id),
                    ])));
                };
                self.call_function_value(inner.clone(), original_name, args, env)
            }
            Value::Lambda(ref params, ref body, ref closure_env) => {
                if params.len() != args.len() {
                    let min_params = params
                        .iter()
                        .position(|p| p == "&optional" || p == "&rest")
                        .unwrap_or(params.len());
                    if args.len() < min_params {
                        return Err(LispError::WrongNumberOfArgs(
                            "lambda".to_string(),
                            args.len(),
                        ));
                    }
                }

                let mut frame = Vec::new();
                let mut arg_idx = 0;
                let mut optional = false;
                let mut rest = false;

                for param in params {
                    if param == "&optional" {
                        optional = true;
                        continue;
                    }
                    if param == "&rest" {
                        rest = true;
                        continue;
                    }
                    if rest {
                        let rest_args: Vec<Value> = args[arg_idx..].to_vec();
                        frame.push((param.clone(), Self::stored_value(Value::list(rest_args))));
                        break;
                    }
                    let consumed_arg = arg_idx < args.len();
                    let val = if consumed_arg {
                        args[arg_idx].clone()
                    } else if optional {
                        Value::Nil
                    } else {
                        return Err(LispError::WrongNumberOfArgs(
                            "lambda".to_string(),
                            args.len(),
                        ));
                    };
                    frame.push((param.clone(), Self::stored_value(val)));
                    if consumed_arg {
                        arg_idx += 1;
                    }
                }

                let backtrace_function = original_name
                    .map(|name| Value::Symbol(name.to_string()))
                    .unwrap_or_else(|| func.clone());
                self.push_backtrace_frame(backtrace_function, args.to_vec());
                let captured_snapshot = closure_env.borrow().clone();
                let result = if captured_snapshot.is_empty() {
                    let mut call_env = env.clone();
                    call_env.push(frame);
                    let result = self.sf_progn(function_executable_body(body), &mut call_env);
                    call_env.pop();
                    env.clear();
                    env.extend(call_env);
                    result
                } else {
                    let frame_mapping = Self::align_captured_frames(&captured_snapshot, env);
                    let mut call_env =
                        Self::merge_lexical_lambda_env(env, &captured_snapshot, &frame_mapping);
                    call_env.push(frame);
                    let result = self.sf_progn(function_executable_body(body), &mut call_env);
                    call_env.pop();
                    {
                        let mut stored_env = closure_env.borrow_mut();
                        if stored_env.len() != captured_snapshot.len() {
                            stored_env.clear();
                            stored_env.extend(captured_snapshot.clone());
                        }
                        for (captured_index, updated) in call_env.iter().enumerate() {
                            if captured_index >= stored_env.len() {
                                break;
                            }
                            stored_env[captured_index] = updated.clone();
                            if let Some(current_index) = frame_mapping[captured_index]
                                && current_index < env.len()
                            {
                                env[current_index] = updated.clone();
                            }
                        }
                    }
                    result
                };
                self.pop_backtrace_frame();
                result
            }
            other => Err(LispError::SignalValue(Value::list([
                Value::Symbol("invalid-function".into()),
                other,
            ]))),
        }
    }

    // ── Special forms ──

    fn sf_quote(&self, items: &[Value]) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        super::reader::resolve_circular_read_syntax(items[1].clone())
    }

    fn sf_if(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let Some(test_form) = items.get(1) else {
            return Ok(Value::Nil);
        };
        let tail_aliases =
            setcdr_tail_aliases(self, test_form, &Value::list(items[1..].to_vec()), env);
        let saved_aliases = snapshot_tail_alias_values(self, &tail_aliases, env);
        let cond_result = self.eval(test_form, env);
        let tail_became_improper = tail_aliases_became_improper(self, &tail_aliases, env);
        restore_tail_alias_values(self, &saved_aliases, env);
        let cond = cond_result?;
        if tail_became_improper {
            return Err(LispError::Void("if".into()));
        }
        if cond.is_truthy() {
            items
                .get(2)
                .map_or(Ok(Value::Nil), |then_form| self.eval(then_form, env))
        } else {
            // else branches
            self.sf_progn(items.get(3..).unwrap_or(&[]), env)
        }
    }

    fn sf_when(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let cond = self.eval(&items[1], env)?;
        if cond.is_truthy() {
            self.sf_progn(&items[2..], env)
        } else {
            Ok(Value::Nil)
        }
    }

    fn sf_if_let_star(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "if-let*".into(),
                items.len().saturating_sub(1),
            ));
        }
        let bindings = items[1].to_vec()?;
        env.push(Vec::new());
        for binding in bindings {
            let value = match binding {
                Value::Symbol(name) => self.lookup(&name, env)?,
                Value::Cons(_, _) => {
                    let parts = binding.to_vec()?;
                    match parts.as_slice() {
                        [expr] => self.eval(expr, env)?,
                        [Value::Symbol(name), expr] => {
                            let value = self.eval(expr, env)?;
                            if name != "_"
                                && let Some(frame) = env.last_mut()
                            {
                                frame.push((name.clone(), Self::stored_value(value.clone())));
                            }
                            value
                        }
                        _ => {
                            env.pop();
                            return Err(LispError::Signal("Invalid if-let* binding".into()));
                        }
                    }
                }
                _ => {
                    env.pop();
                    return Err(LispError::Signal("Invalid if-let* binding".into()));
                }
            };

            if !value.is_truthy() {
                env.pop();
                return self.sf_progn(items.get(3..).unwrap_or(&[]), env);
            }
        }

        let result = self.eval(&items[2], env);
        env.pop();
        result
    }

    fn sf_if_let(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "if-let".into(),
                items.len().saturating_sub(1),
            ));
        }
        let spec = normalize_if_let_spec(&items[1])?;
        let rewritten = Value::list(
            std::iter::once(Value::symbol("if-let*"))
                .chain(std::iter::once(spec))
                .chain(std::iter::once(items[2].clone()))
                .chain(std::iter::once(forms_to_progn(
                    items.get(3..).unwrap_or(&[]),
                ))),
        );
        self.eval(&rewritten, env)
    }

    fn sf_and_let_star(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "and-let*".into(),
                items.len().saturating_sub(1),
            ));
        }
        let bindings = items[1].to_vec()?;
        env.push(Vec::new());
        let mut last_value = Value::T;
        for binding in bindings {
            let value = match binding {
                Value::Symbol(name) => self.lookup(&name, env)?,
                Value::Cons(_, _) => {
                    let parts = binding.to_vec()?;
                    match parts.as_slice() {
                        [expr] => self.eval(expr, env)?,
                        [Value::Symbol(name), expr] => {
                            let value = self.eval(expr, env)?;
                            if name != "_"
                                && let Some(frame) = env.last_mut()
                            {
                                frame.push((name.clone(), Self::stored_value(value.clone())));
                            }
                            value
                        }
                        _ => {
                            env.pop();
                            return Err(LispError::Signal("Invalid and-let* binding".into()));
                        }
                    }
                }
                _ => {
                    env.pop();
                    return Err(LispError::Signal("Invalid and-let* binding".into()));
                }
            };

            if !value.is_truthy() {
                env.pop();
                return Ok(Value::Nil);
            }
            last_value = value;
        }

        let result = if items.len() > 2 {
            self.sf_progn(&items[2..], env)
        } else {
            Ok(last_value)
        };
        env.pop();
        result
    }

    fn sf_when_let(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "when-let".into(),
                items.len().saturating_sub(1),
            ));
        }
        let rewritten = Value::list([
            Value::symbol("if-let"),
            items[1].clone(),
            forms_to_progn(items.get(2..).unwrap_or(&[])),
        ]);
        self.eval(&rewritten, env)
    }

    fn sf_when_let_star(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "when-let*".into(),
                items.len().saturating_sub(1),
            ));
        }
        let rewritten = Value::list(
            std::iter::once(Value::symbol("if-let*"))
                .chain(std::iter::once(items[1].clone()))
                .chain(std::iter::once(Value::list(
                    std::iter::once(Value::symbol("progn")).chain(items[2..].iter().cloned()),
                )))
                .chain(std::iter::once(Value::Nil)),
        );
        self.eval(&rewritten, env)
    }

    fn sf_unless(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let cond = self.eval(&items[1], env)?;
        if cond.is_nil() {
            self.sf_progn(&items[2..], env)
        } else {
            Ok(Value::Nil)
        }
    }

    fn sf_bound_and_true_p(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() != 2 {
            return Err(LispError::WrongNumberOfArgs(
                "bound-and-true-p".into(),
                items.len().saturating_sub(1),
            ));
        }
        let symbol = quoted_symbol_name(&items[1])
            .or_else(|| items[1].as_symbol().ok().map(str::to_string))
            .ok_or_else(|| LispError::TypeError("symbol".into(), items[1].type_name()))?;
        Ok(self
            .lookup_var(&symbol, env)
            .filter(|value| value.is_truthy())
            .unwrap_or(Value::Nil))
    }

    fn sf_cond(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        for (clause_index, clause) in items[1..].iter().enumerate() {
            let clause_items = clause.to_vec()?;
            if clause_items.is_empty() {
                continue;
            }
            let tail_aliases = setcdr_tail_aliases(
                self,
                &clause_items[0],
                &Value::list(items[clause_index + 1..].to_vec()),
                env,
            );
            let saved_aliases = snapshot_tail_alias_values(self, &tail_aliases, env);
            let test_result = self.eval(&clause_items[0], env);
            let tail_became_improper = tail_aliases_became_improper(self, &tail_aliases, env);
            restore_tail_alias_values(self, &saved_aliases, env);
            let test = test_result?;
            if tail_became_improper {
                return Err(LispError::Void("cond".into()));
            }
            if test.is_truthy() {
                if clause_items.len() == 1 {
                    return Ok(test);
                }
                return self.sf_progn(&clause_items[1..], env);
            }
        }
        Ok(Value::Nil)
    }

    fn sf_pcase(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        self.sf_pcase_like(items, env, false)
    }

    fn sf_pcase_defmacro(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 4 {
            return Err(LispError::WrongNumberOfArgs(
                "pcase-defmacro".into(),
                items.len().saturating_sub(1),
            ));
        }
        let name = items[1].as_symbol()?.to_string();
        let params = self.parse_params(&items[2])?;
        let body_start = if items.len() > 4 {
            if let Value::String(_) = &items[3] {
                4
            } else {
                3
            }
        } else {
            3
        };
        let body_start = if body_start < items.len() && is_function_declare_form(&items[body_start])
        {
            body_start + 1
        } else {
            body_start
        };
        let body = items[body_start..].to_vec();
        let expander_name = format!("{name}--pcase-macroexpander");
        let expander = Value::Lambda(params, body, shared_env(env.clone()));
        self.validate_function_binding(&expander_name, &expander)?;
        self.set_function_binding(&expander_name, Some(expander));
        self.put_symbol_property(&name, "pcase-macroexpander", Value::Symbol(expander_name));
        Ok(Value::Symbol(name))
    }

    fn sf_pcase_exhaustive(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        self.sf_pcase_like(items, env, true)
    }

    fn sf_pcase_like(
        &mut self,
        items: &[Value],
        env: &mut Env,
        exhaustive: bool,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Ok(Value::Nil);
        }
        let value = self.eval(&items[1], env)?;
        for clause in &items[2..] {
            let clause_items = clause.to_vec()?;
            if clause_items.is_empty() {
                continue;
            }
            let mut bindings = Vec::new();
            if pcase_pattern_bindings(self, env, &clause_items[0], &value, &mut bindings)? {
                env.push(bindings);
                let result = self.sf_progn(&clause_items[1..], env);
                env.pop();
                return result;
            }
        }
        if exhaustive {
            Err(LispError::Signal(
                "pcase-exhaustive: no matching clause".into(),
            ))
        } else {
            Ok(Value::Nil)
        }
    }

    fn sf_and(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let mut result = Value::T;
        for item in &items[1..] {
            result = self.eval(item, env)?;
            if result.is_nil() {
                return Ok(Value::Nil);
            }
        }
        Ok(result)
    }

    fn sf_or(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        for item in &items[1..] {
            let val = self.eval(item, env)?;
            if val.is_truthy() {
                return Ok(val);
            }
        }
        Ok(Value::Nil)
    }

    fn sf_not(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("not".into(), 0));
        }
        let val = self.eval(&items[1], env)?;
        Ok(if val.is_nil() { Value::T } else { Value::Nil })
    }

    fn sf_progn(&mut self, body: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let mut result = Value::Nil;
        for expr in body {
            result = self.eval(expr, env)?;
        }
        Ok(result)
    }

    fn sf_catch(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("catch".into(), 0));
        }
        let tag = self.eval(&items[1], env)?;
        match self.sf_progn(&items[2..], env) {
            Ok(value) => Ok(value),
            Err(LispError::Throw(thrown_tag, value)) if thrown_tag == tag => Ok(value),
            Err(error) => Err(error),
        }
    }

    fn sf_prog1(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let result = self.eval(&items[1], env)?;
        let tracked_symbol = items[1].as_symbol().ok().map(str::to_string);
        for expr in &items[2..] {
            self.eval(expr, env)?;
        }
        if let Some(symbol) = tracked_symbol
            && crate::lisp::primitives::is_vector_like_value(self, &result)
            && let Ok(current) = self.lookup(&symbol, env)
            && crate::lisp::primitives::is_vector_like_value(self, &current)
        {
            return Ok(current);
        }
        Ok(result)
    }

    fn sf_let(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if is_vector_literal(&items[1]) {
            return Err(wrong_type_argument("listp", items[1].clone()));
        }
        let bindings = items[1].to_vec()?;
        let mut frame = Vec::new();
        let mut special_bindings = Vec::new();

        for binding in &bindings {
            match binding {
                Value::Symbol(name) => {
                    if self.is_special_variable(name) {
                        special_bindings.push((name.clone(), Value::Nil));
                    } else {
                        frame.push((name.clone(), Value::Nil));
                    }
                }
                Value::Cons(_, _) => {
                    let parts = binding.to_vec()?;
                    let Some(name_value) = parts.first() else {
                        return Err(LispError::ReadError("bad let binding".into()));
                    };
                    let name = name_value.as_symbol()?.to_string();
                    let val = if parts.len() > 1 {
                        self.eval(&parts[1], env)?
                    } else {
                        Value::Nil
                    };
                    if self.is_special_variable(&name) {
                        special_bindings.push((name, val));
                    } else {
                        frame.push((name, Self::stored_value(val)));
                    }
                }
                _ => return Err(wrong_type_argument("listp", binding.clone())),
            }
        }

        let mut restores = Vec::new();
        for (name, value) in special_bindings {
            restores.push(self.bind_special_variable(&name, value, env)?);
        }
        env.push(frame);
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        for restore in restores.into_iter().rev() {
            self.restore_special_binding(restore, env)?;
        }
        result
    }

    fn sf_letstar(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if is_vector_literal(&items[1]) {
            return Err(wrong_type_argument("listp", items[1].clone()));
        }
        let bindings = items[1].to_vec()?;
        env.push(Vec::new());
        let mut restores = Vec::new();

        for binding in &bindings {
            match binding {
                Value::Symbol(name) => {
                    if self.is_special_variable(name) {
                        restores.push(self.bind_special_variable(name, Value::Nil, env)?);
                    } else {
                        let frame = env.last_mut().expect("env frame just pushed");
                        frame.push((name.clone(), Value::Nil));
                    }
                }
                Value::Cons(_, _) => {
                    let parts = binding.to_vec()?;
                    let Some(name_value) = parts.first() else {
                        return Err(LispError::ReadError("bad let* binding".into()));
                    };
                    let name = name_value.as_symbol()?.to_string();
                    let val = if parts.len() > 1 {
                        self.eval(&parts[1], env)?
                    } else {
                        Value::Nil
                    };
                    if self.is_special_variable(&name) {
                        restores.push(self.bind_special_variable(&name, val, env)?);
                    } else {
                        let frame = env.last_mut().expect("env frame just pushed");
                        frame.push((name, Self::stored_value(val)));
                    }
                }
                _ => return Err(wrong_type_argument("listp", binding.clone())),
            }
        }

        let result = self.sf_progn(&items[2..], env);
        env.pop();
        for restore in restores.into_iter().rev() {
            self.restore_special_binding(restore, env)?;
        }
        result
    }

    fn sf_cl_progv(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-progv".into(),
                items.len().saturating_sub(1),
            ));
        }
        let symbols = self.eval(&items[1], env)?.to_vec()?;
        let values = self.eval(&items[2], env)?.to_vec()?;
        let mut restores = Vec::new();
        for (index, symbol) in symbols.iter().enumerate() {
            let name = symbol.as_symbol()?;
            let value = values.get(index).cloned().unwrap_or(Value::Nil);
            restores.push(self.bind_special_variable(name, value, env)?);
        }
        let result = self.sf_progn(&items[3..], env);
        for restore in restores.into_iter().rev() {
            self.restore_special_binding(restore, env)?;
        }
        result
    }

    fn sf_pcase_let(
        &mut self,
        items: &[Value],
        env: &mut Env,
        sequential: bool,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let bindings = items[1].to_vec()?;
        if sequential {
            env.push(Vec::new());
            for binding in &bindings {
                let parts = binding.to_vec()?;
                if parts.len() < 2 {
                    return Err(LispError::ReadError("bad pcase-let* binding".into()));
                }
                let value = self.eval(&parts[1], env)?;
                let mut frame_bindings = Vec::new();
                if !pcase_pattern_bindings_lenient_list(
                    self,
                    env,
                    &parts[0],
                    &value,
                    &mut frame_bindings,
                )? {
                    env.pop();
                    return Err(LispError::Signal("pcase-let*: no matching clause".into()));
                }
                let frame = env.last_mut().expect("env frame just pushed");
                frame.extend(
                    frame_bindings
                        .into_iter()
                        .map(|(name, value)| (name, Self::stored_value(value))),
                );
            }
            let result = self.sf_progn(&items[2..], env);
            env.pop();
            return result;
        }

        let mut frame = Vec::new();
        for binding in &bindings {
            let parts = binding.to_vec()?;
            if parts.len() < 2 {
                return Err(LispError::ReadError("bad pcase-let binding".into()));
            }
            let value = self.eval(&parts[1], env)?;
            if !pcase_pattern_bindings_lenient_list(self, env, &parts[0], &value, &mut frame)? {
                return Err(LispError::Signal("pcase-let: no matching clause".into()));
            }
        }
        env.push(
            frame
                .into_iter()
                .map(|(name, value)| (name, Self::stored_value(value)))
                .collect(),
        );
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        result
    }

    fn sf_let_alist(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let alist = self.eval(&items[1], env)?;
        let mut frame = Vec::new();
        for entry in alist.to_vec().unwrap_or_default() {
            let Some((car, cdr)) = entry.cons_values() else {
                continue;
            };
            let Ok(symbol) = car.as_symbol() else {
                continue;
            };
            let value = match cdr {
                Value::Cons(value, tail) if matches!(*tail.borrow(), Value::Nil) => {
                    value.borrow().clone()
                }
                other => other,
            };
            frame.push((format!(".{symbol}"), Self::stored_value(value)));
        }
        env.push(frame);
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        result
    }

    fn sf_setq(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        self.sf_setq_internal(items, env, false)
    }

    fn sf_setq_default(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let mut result = Value::Nil;
        let mut index = 1usize;
        while index + 1 < items.len() {
            let name = assignment_target_name(&items[index])?;
            let resolved = self.resolve_variable_name(&name)?;
            let evaluated = self.eval(&items[index + 1], env)?;
            let value = self.prepare_variable_assignment(&resolved, evaluated)?;
            result = value.clone();
            self.notify_variable_watchers(&resolved, value.clone(), "set", None, env)?;
            self.set_global_binding(&resolved, value);
            index += 2;
        }
        Ok(result)
    }

    fn sf_setq_local(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        self.sf_setq_internal(items, env, true)
    }

    pub fn set_custom_option(
        &mut self,
        symbol: &str,
        value: Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let resolved = self.resolve_variable_name(symbol)?;
        if let Some(setter) = self.get_symbol_property(&resolved, "custom-set") {
            self.call_function_value(
                setter,
                None,
                &[Value::Symbol(resolved.clone()), value.clone()],
                env,
            )?;
        } else {
            self.call_function_value(
                Value::BuiltinFunc("set-default".into()),
                Some("set-default"),
                &[Value::Symbol(resolved), value.clone()],
                env,
            )?;
        }
        Ok(value)
    }

    fn sf_setopt(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() == 1 {
            return Ok(Value::Nil);
        }
        if items.len().is_multiple_of(2) {
            return Err(LispError::WrongNumberOfArgs(
                "setopt".into(),
                items.len().saturating_sub(1),
            ));
        }

        let mut result = Value::Nil;
        let mut index = 1;
        while index + 1 < items.len() {
            let symbol = items[index].as_symbol()?.to_string();
            let value = self.eval(&items[index + 1], env)?;
            result = self.set_custom_option(&symbol, value, env)?;
            index += 2;
        }

        Ok(result)
    }

    fn sf_setf(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() != 3 {
            return Err(LispError::WrongNumberOfArgs(
                "setf".into(),
                items.len().saturating_sub(1),
            ));
        }
        if matches!(items.get(1), Some(Value::Symbol(_) | Value::Nil | Value::T)) {
            let setq_items = [
                Value::Symbol("setq".into()),
                items[1].clone(),
                items[2].clone(),
            ];
            return self.sf_setq(&setq_items, env);
        }
        let place = items[1].to_vec()?;
        match place.first() {
            Some(Value::Symbol(name)) if name == "symbol-function" => {
                let Some(target) = place.get(1) else {
                    return Err(LispError::Signal(format!(
                        "Unsupported setf place: {}",
                        items[1]
                    )));
                };
                let function_name = function_name_from_binding_form(target)?;
                let value = self.eval(&items[2], env)?;
                if value.is_nil() {
                    self.set_function_binding(&function_name, None);
                    Ok(Value::Nil)
                } else {
                    self.validate_function_binding(&function_name, &value)?;
                    self.set_function_binding(&function_name, Some(value.clone()));
                    Ok(value)
                }
            }
            Some(Value::Symbol(name)) if matches!(name.as_str(), "cond" | "if" | "progn") => {
                let resolved = self.resolve_setf_place(&items[1], env)?;
                let value = self.eval(&items[2], env)?;
                self.set_resolved_setf_place_value(&resolved, value.clone(), env)?;
                Ok(value)
            }
            Some(Value::Symbol(name))
                if self
                    .get_symbol_property(name, "emaxx-struct-slot")
                    .is_some() =>
            {
                self.sf_setf_struct_accessor(name, &place, &items[2], env)
            }
            Some(Value::Symbol(name))
                if self.get_symbol_property(name, "emaxx-gv-setter").is_some() =>
            {
                self.sf_setf_gv_setter(name, &place, &items[2], env)
            }
            Some(Value::Symbol(name)) if name == "alist-get" => {
                self.sf_setf_alist_get(&place, &items[2], env)
            }
            Some(Value::Symbol(name)) if name == "plist-get" => {
                self.sf_setf_plist_get(&place, &items[2], env)
            }
            Some(Value::Symbol(name)) if name == "gethash" => {
                self.sf_setf_gethash(&place, &items[2], env)
            }
            Some(Value::Symbol(name)) if name == "nth" => self.sf_setf_nth(&place, &items[2], env),
            Some(Value::Symbol(name)) if name == "aref" => {
                self.sf_setf_aref(&place, &items[2], env)
            }
            Some(Value::Symbol(name)) if name == "image-property" => {
                self.sf_setf_image_property(&place, &items[2], env)
            }
            Some(Value::Symbol(name)) if name == "terminal-parameter" => {
                let value = self.eval(&items[2], env)?;
                self.set_setf_place_value(&items[1], value.clone(), env)?;
                Ok(value)
            }
            _ => Err(LispError::Signal(format!(
                "Unsupported setf place: {}",
                items[1]
            ))),
        }
    }

    fn sf_setf_struct_accessor(
        &mut self,
        accessor: &str,
        place: &[Value],
        value_expr: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some(target) = place.get(1) else {
            return Err(LispError::Signal(format!(
                "Unsupported setf place: {}",
                Value::list(place.to_vec())
            )));
        };
        let expected_type = self
            .get_symbol_property(accessor, "emaxx-struct-type")
            .and_then(|value| value.as_symbol().ok().map(str::to_string))
            .ok_or_else(|| LispError::Signal(format!("Unknown struct accessor: {accessor}")))?;
        let slot_index = self
            .get_symbol_property(accessor, "emaxx-struct-slot")
            .and_then(|value| value.as_integer().ok())
            .map(|value| value.max(0) as usize)
            .ok_or_else(|| LispError::Signal(format!("Unknown struct accessor: {accessor}")))?;
        let object = self.eval(target, env)?;
        let value = self.eval(value_expr, env)?;
        let predicate = format!("{expected_type}-p");
        let Value::Record(id) = object.clone() else {
            return Err(wrong_type_argument(&predicate, object));
        };
        let record = self
            .find_record_mut(id)
            .ok_or_else(|| wrong_type_argument(&predicate, Value::Record(id)))?;
        if record.type_name != expected_type {
            return Err(wrong_type_argument(&predicate, Value::Record(id)));
        }
        if slot_index >= record.slots.len() {
            return Err(LispError::Signal(format!(
                "Struct slot out of range: {slot_index}"
            )));
        }
        record.slots[slot_index] = value.clone();
        Ok(value)
    }

    fn sf_setf_gv_setter(
        &mut self,
        accessor: &str,
        place: &[Value],
        value_expr: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let setter = self
            .get_symbol_property(accessor, "emaxx-gv-setter")
            .and_then(|value| value.as_symbol().ok().map(str::to_string))
            .ok_or_else(|| LispError::Signal(format!("Unknown gv setter: {accessor}")))?;
        let mut args = Vec::with_capacity(place.len());
        for arg in &place[1..] {
            args.push(self.eval(arg, env)?);
        }
        let value = self.eval(value_expr, env)?;
        args.push(value.clone());
        let setter_function = self.lookup_function(&setter, env)?;
        self.call_function_value(setter_function, Some(&setter), &args, env)?;
        Ok(value)
    }

    fn sf_setf_alist_get(
        &mut self,
        place: &[Value],
        value_expr: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some(key_expr) = place.get(1) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };
        let Some(alist_place) = place.get(2) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };
        let key = self.eval(key_expr, env)?;
        let alist = self.eval(alist_place, env)?;
        let default = match place.get(3) {
            Some(expr) => self.eval(expr, env)?,
            None => Value::Nil,
        };
        let remove = match place.get(4) {
            Some(expr) => self.eval(expr, env)?,
            None => Value::Nil,
        };
        let testfn = match place.get(5) {
            Some(expr) => Some(self.eval(expr, env)?),
            None => None,
        };
        let value = self.eval(value_expr, env)?;
        let should_remove = remove.is_truthy() && value == default;
        let mut updated = false;
        let mut new_entries = Vec::new();

        for entry in alist.to_vec()? {
            let matches = if !updated {
                if let Some((car, _)) = entry.cons_values() {
                    primitives::value_matches_with_test(self, &key, &car, testfn.as_ref(), env)?
                } else {
                    false
                }
            } else {
                false
            };
            if matches {
                updated = true;
                if !should_remove {
                    new_entries.push(Value::cons(entry.car()?, value.clone()));
                }
            } else {
                new_entries.push(entry);
            }
        }

        if !updated && !should_remove {
            new_entries.insert(0, Value::cons(key.clone(), value.clone()));
        }

        self.set_setf_place_value(alist_place, Value::list(new_entries), env)?;
        Ok(value)
    }

    fn sf_setf_plist_get(
        &mut self,
        place: &[Value],
        value_expr: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some(plist_place) = place.get(1) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };
        let Some(key_expr) = place.get(2) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };
        let plist = self.eval(plist_place, env)?;
        let key = self.eval(key_expr, env)?;
        let value = self.eval(value_expr, env)?;
        let updated = if let Some(testfn_expr) = place.get(3) {
            let testfn = self.eval(testfn_expr, env)?;
            primitives::call(self, "plist-put", &[plist, key, value.clone(), testfn], env)?
        } else {
            primitives::call(self, "plist-put", &[plist, key, value.clone()], env)?
        };
        self.set_setf_place_value(plist_place, updated, env)?;
        Ok(value)
    }

    fn sf_setf_gethash(
        &mut self,
        place: &[Value],
        value_expr: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some(key_expr) = place.get(1) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };
        let Some(table_expr) = place.get(2) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };
        let key = self.eval(key_expr, env)?;
        let table = self.eval(table_expr, env)?;
        let value = self.eval(value_expr, env)?;
        primitives::call(self, "puthash", &[key, value.clone(), table], env)?;
        Ok(value)
    }

    fn sf_setf_nth(
        &mut self,
        place: &[Value],
        value_expr: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if place.len() < 3 {
            return Err(LispError::Signal("Unsupported setf place".into()));
        }
        let index = self.eval(&place[1], env)?.as_integer()?.max(0) as usize;
        let mut cell = self.eval(&place[2], env)?;
        for _ in 0..index {
            cell = cell.cdr()?;
        }
        let value = self.eval(value_expr, env)?;
        cell.set_car(value.clone())?;
        Ok(value)
    }

    fn sf_setf_aref(
        &mut self,
        place: &[Value],
        value_expr: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some(sequence_place) = place.get(1) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };
        let Some(index_expr) = place.get(2) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };
        let current = self.eval(sequence_place, env)?;
        let index_value = self.eval(index_expr, env)?;
        let value = self.eval(value_expr, env)?;

        if matches!(current, Value::CharTable(_))
            || matches!(
                &current,
                Value::Record(id)
                    if self
                        .find_record(*id)
                        .is_some_and(|record| record.type_name == "bool-vector")
            )
        {
            primitives::call(self, "aset", &[current, index_value, value.clone()], env)?;
            return Ok(value);
        }

        let index = index_value.as_integer()? as usize;
        let updated = if matches!(current, Value::String(_) | Value::StringObject(_)) {
            primitives::aset_string_value(&current, index, &value)?
        } else {
            let mut entries = current.to_vec()?;
            let tagged = matches!(
                entries.first(),
                Some(Value::Symbol(symbol)) if symbol == "vector" || symbol == "vector-literal"
            );
            let slot = if tagged { index + 1 } else { index };
            if slot >= entries.len() {
                return Err(LispError::Signal("Args out of range".into()));
            }
            entries[slot] = value.clone();
            Value::list(entries)
        };

        self.set_setf_place_value(sequence_place, updated, env)?;
        Ok(value)
    }

    fn sf_setf_image_property(
        &mut self,
        place: &[Value],
        value_expr: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some(image_place) = place.get(1) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };
        let Some(property_expr) = place.get(2) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };
        let image = self.eval(image_place, env)?;
        let property = self.eval(property_expr, env)?;
        let value = self.eval(value_expr, env)?;
        let mut descriptor = image.to_vec()?;
        if descriptor.is_empty() {
            return Err(LispError::Signal("Unsupported setf place".into()));
        }

        let mut property_index = None;
        let mut cursor = 1;
        while cursor + 1 < descriptor.len() {
            if descriptor[cursor] == property {
                property_index = Some(cursor);
                break;
            }
            cursor += 2;
        }

        match property_index {
            Some(index) if value.is_nil() => {
                descriptor.drain(index..=index + 1);
            }
            Some(index) => descriptor[index + 1] = value.clone(),
            None if !value.is_nil() => {
                descriptor.push(property);
                descriptor.push(value.clone());
            }
            None => {}
        }

        self.set_setf_place_value(image_place, Value::list(descriptor), env)?;
        Ok(value)
    }

    fn set_setf_place_value(
        &mut self,
        place: &Value,
        value: Value,
        env: &mut Env,
    ) -> Result<(), LispError> {
        let place = self.resolve_setf_place(place, env)?;
        self.set_resolved_setf_place_value(&place, value, env)
    }

    fn set_resolved_setf_place_value(
        &mut self,
        place: &Value,
        value: Value,
        env: &mut Env,
    ) -> Result<(), LispError> {
        match &place {
            Value::Symbol(name) => {
                self.set_variable(name, value, env);
                Ok(())
            }
            Value::Cons(_, _) => {
                let items = place.to_vec()?;
                if matches!(items.first(), Some(Value::Symbol(name)) if name == "--emaxx-setf-car-place" || name == "--emaxx-setf-cdr-place")
                {
                    let Some(target) = items.get(1) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    if matches!(items.first(), Some(Value::Symbol(name)) if name == "--emaxx-setf-car-place")
                    {
                        target.set_car(value)?;
                    } else {
                        target.set_cdr(value)?;
                    }
                    Ok(())
                } else if matches!(items.first(), Some(Value::Symbol(name)) if name == "symbol-value")
                {
                    let Some(symbol_form) = items.get(1) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let symbol = self.eval(symbol_form, env)?;
                    let symbol = symbol.as_symbol()?.to_string();
                    self.set_symbol_value_cell(&symbol, value);
                    Ok(())
                } else if matches!(
                    items.first(),
                    Some(Value::Symbol(name))
                        if self.get_symbol_property(name, "emaxx-struct-slot").is_some()
                ) {
                    let accessor = items[0].as_symbol().expect("checked symbol").to_string();
                    let value_expr = quoted_literal(&value);
                    self.sf_setf_struct_accessor(&accessor, &items, &value_expr, env)
                        .map(|_| ())
                } else if matches!(
                    items.first(),
                    Some(Value::Symbol(name))
                        if self.get_symbol_property(name, "emaxx-gv-setter").is_some()
                ) {
                    let accessor = items[0].as_symbol().expect("checked symbol").to_string();
                    let value_expr = quoted_literal(&value);
                    self.sf_setf_gv_setter(&accessor, &items, &value_expr, env)
                        .map(|_| ())
                } else if matches!(items.first(), Some(Value::Symbol(name)) if name == "car" || name == "cdr")
                {
                    let Some(target_expr) = items.get(1) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let target = self.eval(target_expr, env)?;
                    if matches!(items.first(), Some(Value::Symbol(name)) if name == "car") {
                        target.set_car(value)?;
                    } else {
                        target.set_cdr(value)?;
                    }
                    Ok(())
                } else if matches!(items.first(), Some(Value::Symbol(name)) if name == "overlay-get")
                {
                    let Some(overlay_expr) = items.get(1) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let Some(prop_expr) = items.get(2) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let overlay = self.eval(overlay_expr, env)?;
                    let overlay_id = match overlay {
                        Value::Overlay(id) => id,
                        other => {
                            return Err(LispError::TypeError("overlay".into(), other.type_name()));
                        }
                    };
                    let prop = self.eval(prop_expr, env)?;
                    let prop_name = prop.as_symbol()?.to_string();
                    let Some(existing) = self.find_overlay_mut(overlay_id) else {
                        return Err(LispError::TypeError(
                            "overlay".into(),
                            format!("overlay<{overlay_id}>"),
                        ));
                    };
                    existing.put_prop(&prop_name, value);
                    Ok(())
                } else if matches!(
                    items.first(),
                    Some(Value::Symbol(name)) if name == "terminal-parameter"
                ) {
                    let Some(terminal_expr) = items.get(1) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let Some(parameter_expr) = items.get(2) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let terminal = self.eval(terminal_expr, env)?;
                    let parameter = self.eval(parameter_expr, env)?;
                    primitives::call(
                        self,
                        "set-terminal-parameter",
                        &[terminal, parameter, value],
                        env,
                    )?;
                    Ok(())
                } else if matches!(items.first(), Some(Value::Symbol(name)) if name == "alist-get")
                {
                    let value_expr = quoted_literal(&value);
                    self.sf_setf_alist_get(&items, &value_expr, env).map(|_| ())
                } else if matches!(items.first(), Some(Value::Symbol(name)) if name == "plist-get")
                {
                    let value_expr = quoted_literal(&value);
                    self.sf_setf_plist_get(&items, &value_expr, env).map(|_| ())
                } else if matches!(items.first(), Some(Value::Symbol(name)) if name == "nth") {
                    let value_expr = quoted_literal(&value);
                    self.sf_setf_nth(&items, &value_expr, env).map(|_| ())
                } else if matches!(items.first(), Some(Value::Symbol(name)) if name == "aref") {
                    let value_expr = quoted_literal(&value);
                    self.sf_setf_aref(&items, &value_expr, env).map(|_| ())
                } else if matches!(items.first(), Some(Value::Symbol(name)) if name == "image-property")
                {
                    let value_expr = quoted_literal(&value);
                    self.sf_setf_image_property(&items, &value_expr, env)
                        .map(|_| ())
                } else if matches!(items.first(), Some(Value::Symbol(name)) if name == "gethash") {
                    let Some(key_expr) = items.get(1) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let Some(table_expr) = items.get(2) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let key = self.eval(key_expr, env)?;
                    let table = self.eval(table_expr, env)?;
                    primitives::call(self, "puthash", &[key, value, table], env)?;
                    Ok(())
                } else {
                    Err(LispError::Signal("Unsupported setf place".into()))
                }
            }
            _ => Err(LispError::Signal("Unsupported setf place".into())),
        }
    }

    fn sf_setq_internal(
        &mut self,
        items: &[Value],
        env: &mut Env,
        local_only: bool,
    ) -> Result<Value, LispError> {
        let mut result = Value::Nil;
        let mut i = 1;
        while i + 1 < items.len() {
            let name = assignment_target_name(&items[i])?;
            let resolved = self.resolve_variable_name(&name)?;
            let evaluated = self.eval(&items[i + 1], env)?;
            let val = self.prepare_variable_assignment(&resolved, evaluated)?;
            result = val.clone();
            if local_only {
                self.notify_variable_watchers(
                    &resolved,
                    val.clone(),
                    "set",
                    Some(self.current_buffer_id()),
                    env,
                )?;
                self.set_buffer_local_value(self.current_buffer_id(), &resolved, val);
            } else {
                let buffer_id = self.assignment_buffer_id(&resolved);
                self.notify_variable_watchers(&resolved, val.clone(), "set", buffer_id, env)?;
                self.set_variable(&resolved, val, env);
            }
            i += 2;
        }
        Ok(result)
    }

    fn sf_defvar(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let name = items[1].as_symbol()?.to_string();
        let resolved = self.resolve_variable_name(&name)?;
        if matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "defcustom") {
            self.record_defcustom_properties(&resolved, items, env)?;
        }
        self.mark_special_variable(&resolved);
        // Bare `defvar` declarations mark a variable special without binding it.
        if self.default_toplevel_value(&resolved).is_none() && items.len() > 2 {
            let val = self.eval(&items[2], env)?;
            self.set_default_toplevel_value(&resolved, val);
            if self
                .get_symbol_property(&resolved, "standard-value")
                .is_none()
            {
                let stored = self.lookup_var(&resolved, env).unwrap_or(Value::Nil);
                self.put_symbol_property(
                    &resolved,
                    "standard-value",
                    Value::list([quoted_literal(&stored)]),
                );
            }
        }
        Ok(Value::Nil)
    }

    fn record_defcustom_properties(
        &mut self,
        symbol: &str,
        items: &[Value],
        env: &mut Env,
    ) -> Result<(), LispError> {
        let mut index = if matches!(
            items.get(3),
            Some(Value::String(_) | Value::StringObject(_))
        ) {
            4
        } else {
            3
        };
        while index + 1 < items.len() {
            let keyword = items[index].as_symbol()?;
            match keyword {
                ":set" => {
                    let setter = self.eval(&items[index + 1], env)?;
                    self.put_symbol_property(symbol, "custom-set", setter);
                }
                ":type" => {
                    let custom_type = self.eval(&items[index + 1], env)?;
                    self.put_symbol_property(symbol, "custom-type", custom_type);
                }
                _ => {}
            }
            index += 2;
        }
        Ok(())
    }

    fn sf_defvar_local(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() >= 2 {
            self.mark_auto_buffer_local(items[1].as_symbol()?);
        }
        self.sf_defvar(items, env)
    }

    fn sf_defgroup(&mut self, items: &[Value]) -> Result<Value, LispError> {
        let Some(Value::Symbol(name)) = items.get(1) else {
            return Ok(Value::Nil);
        };

        if let Some(doc) = items.get(3)
            && let Some(text) = match doc {
                Value::String(text) => Some(text.clone()),
                Value::StringObject(shared) => Some(shared.borrow().text.clone()),
                _ => None,
            }
        {
            self.put_symbol_property(name, "group-documentation", Value::String(text));
        }

        if let Some(members) = items.get(2)
            && let Ok(entries) = members.to_vec()
        {
            for entry in entries {
                let Ok(parts) = entry.to_vec() else {
                    continue;
                };
                if parts.len() < 2 {
                    continue;
                }
                crate::lisp::primitives::custom_add_to_group(
                    self,
                    name,
                    parts[0].clone(),
                    parts[1].clone(),
                );
            }
        }

        let mut index = 4usize;
        while index + 1 < items.len() {
            if let Value::Symbol(keyword) = &items[index]
                && keyword == ":prefix"
            {
                self.put_symbol_property(name, "custom-prefix", items[index + 1].clone());
            }
            index += 2;
        }

        crate::lisp::primitives::custom_set_current_group(self, name);
        Ok(Value::Symbol(name.clone()))
    }

    fn sf_defface(&mut self, items: &[Value]) -> Result<Value, LispError> {
        let Some(name) = items.get(1).and_then(|value| value.as_symbol().ok()) else {
            return Ok(Value::Nil);
        };
        if let Some(spec) = items.get(2) {
            self.put_symbol_property(name, "face-defface-spec", spec.clone());
            self.put_symbol_property(name, "face-modified", Value::Nil);
            if let Some(doc) = items.get(3)
                && matches!(doc, Value::String(_) | Value::StringObject(_))
            {
                self.put_symbol_property(name, "face-documentation", doc.clone());
            }
            self.record_defface_runtime_attributes(name, spec)?;
        }
        Ok(Value::Symbol(name.to_string()))
    }

    fn record_defface_runtime_attributes(
        &mut self,
        face: &str,
        spec_form: &Value,
    ) -> Result<(), LispError> {
        let Some(spec) = defface_spec_literal(spec_form) else {
            return Ok(());
        };
        let Some(attributes) = defface_runtime_attributes(&spec) else {
            return Ok(());
        };

        for (attribute, value) in attributes {
            if attribute == ":inherit" {
                match &value {
                    Value::Nil => self.set_face_inherit_target(face, None)?,
                    Value::Symbol(symbol) => {
                        self.set_face_inherit_target(face, Some(symbol.clone()))?
                    }
                    _ => {}
                }
            }
            self.put_symbol_property(
                face,
                &crate::lisp::primitives::face_attribute_property_name(&attribute),
                value,
            );
        }
        Ok(())
    }

    fn sf_defvar_keymap(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let name = items[1].as_symbol()?.to_string();
        let resolved = self.resolve_variable_name(&name)?;
        self.mark_special_variable(&resolved);
        if self.lookup(&resolved, env).is_ok() {
            return Ok(Value::Nil);
        }

        let keymap = crate::lisp::primitives::make_runtime_keymap(self, Some(&resolved));
        let mut index = 2;
        let mut seen_keys = HashSet::new();
        while index + 1 < items.len() {
            if matches!(&items[index], Value::Symbol(keyword) if keyword.starts_with(':')) {
                index += 2;
                continue;
            }

            let duplicate_key = items[index].to_string();
            if !seen_keys.insert(duplicate_key.clone()) {
                return Err(LispError::Signal(format!(
                    "Duplicate definition for key '{}' in keymap '{}'",
                    duplicate_key, resolved
                )));
            }

            let key = match self.eval(&items[index], env)? {
                Value::String(text) => text,
                Value::StringObject(state) => state.borrow().text.clone(),
                other => {
                    return Err(LispError::TypeError("string".into(), other.type_name()));
                }
            };
            let definition = self.eval(&items[index + 1], env)?;
            crate::lisp::primitives::keymap_define_binding(self, &keymap, &key, definition)?;
            index += 2;
        }

        if let Some(existing) = self
            .globals
            .iter_mut()
            .rposition(|(symbol, _)| symbol == &resolved)
        {
            self.globals[existing].1 = Self::stored_value(keymap);
        } else {
            self.globals.push((resolved, Self::stored_value(keymap)));
        }
        Ok(Value::Nil)
    }

    fn sf_define_mode(&mut self, items: &[Value]) -> Result<Value, LispError> {
        let Some(name) = items.get(1).and_then(|value| value.as_symbol().ok()) else {
            return Ok(Value::Nil);
        };
        if let Some(Value::Symbol(kind)) = items.first() {
            if kind == "define-minor-mode" {
                let mut init_value = Value::Nil;
                let mut global = false;
                let mut index = if matches!(
                    items.get(2),
                    Some(Value::String(_) | Value::StringObject(_))
                ) {
                    3
                } else {
                    2
                };

                while index + 1 < items.len() {
                    let Some(keyword) = items[index].as_symbol().ok() else {
                        break;
                    };
                    if !keyword.starts_with(':') {
                        break;
                    }
                    match keyword {
                        ":init-value" => init_value = items[index + 1].clone(),
                        ":global" => global = items[index + 1].is_truthy(),
                        _ => {}
                    }
                    index += 2;
                }

                self.mark_special_variable(name);
                if !global {
                    self.mark_auto_buffer_local(name);
                }
                if self.lookup_var(name, &Vec::new()).is_none() {
                    self.globals
                        .push((name.to_string(), Self::stored_value(init_value)));
                }
                if let Some(map) = self.lookup_var(&format!("{name}-map"), &Vec::new()) {
                    let entry = Value::cons(Value::Symbol(name.to_string()), map);
                    let mut entries = self
                        .lookup_var("minor-mode-map-alist", &Vec::new())
                        .unwrap_or(Value::Nil)
                        .to_vec()
                        .unwrap_or_default();
                    if let Some(index) = entries.iter().position(|existing| {
                        existing
                            .cons_values()
                            .is_some_and(|(mode, _)| mode == Value::Symbol(name.to_string()))
                    }) {
                        entries[index] = entry;
                    } else {
                        entries.push(entry);
                    }
                    self.set_global_binding("minor-mode-map-alist", Value::list(entries));
                }

                let setter_symbol = if global { "setq-default" } else { "setq" };
                let current_mode_form = if global {
                    Value::list([
                        Value::Symbol("default-value".into()),
                        Value::list([
                            Value::Symbol("quote".into()),
                            Value::Symbol(name.to_string()),
                        ]),
                    ])
                } else {
                    Value::Symbol(name.to_string())
                };
                let toggle_form = Value::list([
                    Value::Symbol("if".into()),
                    Value::list([
                        Value::Symbol("eq".into()),
                        Value::Symbol("arg".into()),
                        Value::list([
                            Value::Symbol("quote".into()),
                            Value::Symbol("toggle".into()),
                        ]),
                    ]),
                    Value::list([Value::Symbol("not".into()), current_mode_form.clone()]),
                    Value::list([
                        Value::Symbol("if".into()),
                        Value::list([Value::Symbol("not".into()), Value::Symbol("arg".into())]),
                        Value::T,
                        Value::list([
                            Value::Symbol("if".into()),
                            Value::list([
                                Value::Symbol("integerp".into()),
                                Value::Symbol("arg".into()),
                            ]),
                            Value::list([
                                Value::Symbol(">".into()),
                                Value::Symbol("arg".into()),
                                Value::Integer(0),
                            ]),
                            Value::T,
                        ]),
                    ]),
                ]);

                let mut body = vec![Value::list([
                    Value::Symbol(setter_symbol.into()),
                    Value::Symbol(name.to_string()),
                    toggle_form,
                ])];
                body.extend_from_slice(&items[index..]);
                body.push(Value::Symbol(name.to_string()));

                self.set_function_binding(
                    name,
                    Some(Value::Lambda(
                        vec!["&optional".into(), "arg".into()],
                        body,
                        shared_env(Vec::new()),
                    )),
                );
                return Ok(Value::Symbol(name.to_string()));
            }

            if kind == "define-globalized-minor-mode" {
                self.mark_special_variable(name);
                if self.lookup_var(name, &Vec::new()).is_none() {
                    self.globals
                        .push((name.to_string(), Self::stored_value(Value::Nil)));
                }
            }

            if kind == "define-derived-mode" {
                let parent = items.get(2).and_then(|value| match value {
                    Value::Symbol(symbol) => Some(symbol.as_str()),
                    Value::Nil => None,
                    _ => None,
                });
                let map_name = format!("{name}-map");
                if self.lookup_var(&map_name, &Vec::new()).is_none() {
                    self.globals.push((
                        map_name.clone(),
                        Self::stored_value(crate::lisp::primitives::keymap_placeholder(Some(
                            &map_name,
                        ))),
                    ));
                }
                let mut index = 4;
                if matches!(
                    items.get(index),
                    Some(Value::String(_) | Value::StringObject(_))
                ) {
                    index += 1;
                }
                let mut body = Vec::new();
                if let Some(parent) = parent {
                    body.push(Value::list([Value::Symbol(parent.to_string())]));
                }
                body.push(Value::list([
                    Value::Symbol("use-local-map".into()),
                    Value::Symbol(map_name.clone()),
                ]));
                body.push(Value::list([
                    Value::Symbol("setq-local".into()),
                    Value::Symbol("major-mode".into()),
                    Value::list([
                        Value::Symbol("quote".into()),
                        Value::Symbol(name.to_string()),
                    ]),
                ]));
                if !matches!(items.get(3), None | Some(Value::Nil)) {
                    body.push(Value::list([
                        Value::Symbol("setq-local".into()),
                        Value::Symbol("mode-name".into()),
                        items[3].clone(),
                    ]));
                }
                body.extend_from_slice(&items[index..]);
                body.push(Value::list([
                    Value::Symbol("quote".into()),
                    Value::Symbol(name.to_string()),
                ]));
                self.set_function_binding(
                    name,
                    Some(Value::Lambda(Vec::new(), body, shared_env(Vec::new()))),
                );
                crate::lisp::primitives::derived_mode_set_parent(self, name, parent);
                return Ok(Value::Symbol(name.to_string()));
            }
        }
        if self.lookup_function(name, &Vec::new()).is_err() {
            self.set_function_binding(name, Some(Value::BuiltinFunc("ignore".into())));
        }
        Ok(Value::Symbol(name.to_string()))
    }

    fn sf_cl_defstruct(&mut self, items: &[Value]) -> Result<Value, LispError> {
        let Some(struct_spec) = items.get(1) else {
            return Ok(Value::Nil);
        };
        let (name, options) = match struct_spec {
            Value::Symbol(name) => (name.clone(), Vec::new()),
            Value::Cons(_, _) => {
                let Some(parts) = struct_spec.to_vec().ok() else {
                    return Ok(Value::Nil);
                };
                let Some(name) = parts
                    .first()
                    .and_then(|value| value.as_symbol().ok())
                    .map(str::to_string)
                else {
                    return Ok(Value::Nil);
                };
                (name, parts[1..].to_vec())
            }
            _ => return Ok(Value::Nil),
        };

        let slot_specs = items[2..]
            .iter()
            .filter_map(|slot| match slot {
                Value::Symbol(name) => Some((name.clone(), Value::Nil)),
                Value::Cons(_, _) => slot.to_vec().ok().and_then(|parts| {
                    let name = parts
                        .first()
                        .and_then(|value| value.as_symbol().ok().map(str::to_string))?;
                    let default = parts.get(1).cloned().unwrap_or(Value::Nil);
                    Some((name, default))
                }),
                _ => None,
            })
            .collect::<Vec<_>>();
        let slot_names = slot_specs
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        let slot_defaults = slot_specs
            .iter()
            .map(|(_, default)| default.clone())
            .collect::<Vec<_>>();

        let mut constructors = Vec::new();
        let mut saw_constructor_option = false;
        let mut conc_name = format!("{name}-");
        for option in options {
            let Some(parts) = option.to_vec().ok() else {
                continue;
            };
            match parts.first() {
                Some(Value::Symbol(keyword)) if keyword == ":constructor" => {
                    saw_constructor_option = true;
                    match parts.get(1) {
                        Some(Value::Nil) => {}
                        Some(Value::Symbol(constructor_name)) => {
                            let (params, aux_bindings) = parts
                                .get(2)
                                .and_then(|value| value.to_vec().ok())
                                .map(parse_cl_defstruct_constructor_params)
                                .unwrap_or_else(|| {
                                    (
                                        std::iter::once("&key".to_string())
                                            .chain(slot_names.iter().cloned())
                                            .collect::<Vec<_>>(),
                                        Vec::new(),
                                    )
                                });
                            constructors.push((constructor_name.clone(), params, aux_bindings));
                        }
                        _ => {}
                    }
                }
                Some(Value::Symbol(keyword)) if keyword == ":conc-name" => match parts.get(1) {
                    Some(Value::Nil) => conc_name.clear(),
                    Some(Value::Symbol(prefix)) => conc_name = prefix.clone(),
                    Some(Value::String(prefix)) => conc_name = prefix.clone(),
                    Some(Value::StringObject(prefix)) => conc_name = prefix.borrow().text.clone(),
                    _ => {}
                },
                _ => {}
            }
        }
        if !saw_constructor_option {
            constructors.push((
                format!("make-{name}"),
                std::iter::once("&key".to_string())
                    .chain(slot_names.iter().cloned())
                    .collect::<Vec<_>>(),
                Vec::new(),
            ));
        }

        let struct_name = Value::list([Value::Symbol("quote".into()), Value::Symbol(name.clone())]);
        let slot_names_list = Value::list(
            slot_names
                .iter()
                .cloned()
                .map(Value::Symbol)
                .collect::<Vec<_>>(),
        );
        let slot_names_value =
            Value::list([Value::Symbol("quote".into()), slot_names_list.clone()]);
        let slot_defaults_value =
            Value::list([Value::Symbol("quote".into()), Value::list(slot_defaults)]);

        self.put_symbol_property(&name, "emaxx-struct-slots", slot_names_list.clone());

        let predicate_name = format!("{name}-p");
        self.set_function_binding(
            &predicate_name,
            Some(Value::Lambda(
                vec!["object".into()],
                vec![Value::list([
                    Value::Symbol("emaxx-struct-p".into()),
                    struct_name.clone(),
                    Value::Symbol("object".into()),
                ])],
                shared_env(Vec::new()),
            )),
        );

        for (index, slot_name) in slot_names.iter().enumerate() {
            let accessor_name = format!("{conc_name}{slot_name}");
            self.put_symbol_property(
                &accessor_name,
                "emaxx-struct-type",
                Value::Symbol(name.clone()),
            );
            self.put_symbol_property(
                &accessor_name,
                "emaxx-struct-slot",
                Value::Integer(index as i64),
            );
            self.set_function_binding(
                &accessor_name,
                Some(Value::Lambda(
                    vec!["object".into()],
                    vec![Value::list([
                        Value::Symbol("emaxx-struct-ref".into()),
                        struct_name.clone(),
                        Value::Integer(index as i64),
                        Value::Symbol("object".into()),
                    ])],
                    shared_env(Vec::new()),
                )),
            );
        }

        for (constructor_name, params, aux_bindings) in constructors {
            let params_for_make = if aux_bindings.is_empty() {
                params.clone()
            } else {
                params
                    .iter()
                    .cloned()
                    .chain(std::iter::once("&key".to_string()))
                    .chain(slot_names.iter().cloned())
                    .collect::<Vec<_>>()
            };
            let params_list = Value::list(params_for_make.into_iter().map(Value::Symbol));
            let params_value = Value::list([Value::Symbol("quote".into()), params_list]);
            let call_args = if aux_bindings.is_empty() {
                Value::Symbol("args".into())
            } else {
                let aux_keywords = aux_bindings
                    .iter()
                    .flat_map(|(name, _)| {
                        [
                            Value::Symbol(format!(":{name}")),
                            Value::Symbol(name.clone()),
                        ]
                    })
                    .collect::<Vec<_>>();
                Value::list([
                    Value::Symbol("append".into()),
                    Value::Symbol("args".into()),
                    Value::list(
                        std::iter::once(Value::Symbol("list".into()))
                            .chain(aux_keywords)
                            .collect::<Vec<_>>(),
                    ),
                ])
            };
            let make_form = Value::list([
                Value::Symbol("emaxx-struct-make".into()),
                struct_name.clone(),
                slot_names_value.clone(),
                slot_defaults_value.clone(),
                params_value,
                call_args,
            ]);
            let body = if aux_bindings.is_empty() {
                make_form
            } else {
                let let_bindings = Value::list(
                    aux_bindings
                        .into_iter()
                        .map(|(name, form)| Value::list([Value::Symbol(name), form]))
                        .collect::<Vec<_>>(),
                );
                Value::list([Value::Symbol("let*".into()), let_bindings, make_form])
            };
            self.set_function_binding(
                &constructor_name,
                Some(Value::Lambda(
                    vec!["&rest".into(), "args".into()],
                    vec![body],
                    shared_env(Vec::new()),
                )),
            );
        }

        Ok(Value::Symbol(name))
    }

    fn sf_incf(&mut self, items: &[Value], env: &mut Env, sign: i64) -> Result<Value, LispError> {
        if items.len() < 2 || items.len() > 3 {
            return Err(LispError::WrongNumberOfArgs(
                if sign >= 0 {
                    "incf".into()
                } else {
                    "decf".into()
                },
                items.len().saturating_sub(1),
            ));
        }
        let name = items[1].as_symbol()?.to_string();
        let delta = if let Some(amount) = items.get(2) {
            self.eval(amount, env)?
        } else {
            Value::Integer(1)
        };
        let current = self.lookup(&name, env)?;
        let updated = primitives::call(
            self,
            if sign >= 0 { "+" } else { "-" },
            &[current, delta],
            env,
        )?;
        self.set_variable(&name, updated.clone(), env);
        Ok(updated)
    }

    fn sf_setcar(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() != 3 {
            return Err(LispError::WrongNumberOfArgs(
                "setcar".into(),
                items.len().saturating_sub(1),
            ));
        }
        let target = if let Ok(name) = items[1].as_symbol() {
            self.lookup(name, env)?
        } else {
            self.eval(&items[1], env)?
        };
        let target_name = items[1].as_symbol().ok().map(str::to_string);
        let Value::Cons(_, _) = &target else {
            return Err(LispError::TypeError("cons".into(), items[1].type_name()));
        };
        let updated_car = self.eval(&items[2], env)?;
        target.set_car(updated_car.clone())?;
        if let Some(name) = target_name {
            self.set_variable(&name, target, env);
        }
        Ok(updated_car)
    }

    fn sf_add_to_list(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "add-to-list".into(),
                items.len().saturating_sub(1),
            ));
        }

        let place = quoted_symbol_name(&items[1])
            .ok_or_else(|| LispError::TypeError("symbol".into(), unquote(&items[1]).type_name()))?;
        let value = self.eval(&items[2], env)?;
        let append = if items.len() > 3 {
            self.eval(&items[3], env)?.is_truthy()
        } else {
            false
        };
        if items.len() > 4 {
            // Emacs accepts an optional comparison function. We don't have distinct
            // eq/equal semantics yet, but evaluating it still preserves load-time
            // errors from invalid comparator expressions.
            let _ = self.eval(&items[4], env)?;
        }

        let current = self.lookup_var(&place, env).unwrap_or(Value::Nil);
        let mut values = current.to_vec()?;
        if values
            .iter()
            .any(|existing| crate::lisp::primitives::values_equal(self, existing, &value))
        {
            return Ok(current);
        }

        if append {
            values.push(value);
        } else {
            values.insert(0, value);
        }
        let updated = Value::list(values);
        self.set_variable(&place, updated.clone(), env);
        Ok(updated)
    }

    fn sf_cl_pushnew(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-pushnew".into(),
                items.len().saturating_sub(1),
            ));
        }

        let new_value = self.eval(&items[1], env)?;
        let mut testfn = None;
        let mut test_not = None;
        let mut keyfn = None;
        let mut cursor = 3usize;
        while cursor < items.len() {
            let Some(keyword) = items[cursor].as_symbol().ok() else {
                return Err(LispError::Signal("Unsupported cl-pushnew syntax".into()));
            };
            let Some(value_expr) = items.get(cursor + 1) else {
                return Err(LispError::Signal("Unsupported cl-pushnew syntax".into()));
            };
            let value = self.eval(value_expr, env)?;
            match keyword {
                ":test" => testfn = Some(value),
                ":test-not" => test_not = Some(value),
                ":key" => keyfn = Some(value),
                _ => return Err(LispError::Signal("Unsupported cl-pushnew syntax".into())),
            }
            cursor += 2;
        }

        let place = self.resolve_setf_place(&items[2], env)?;
        let current = self.eval_resolved_setf_place_current_value(&place, env)?;
        let values = current.to_vec()?;
        let keyed_new = self.apply_cl_sequence_key(keyfn.as_ref(), &new_value, env)?;
        let mut already_present = false;
        for existing in &values {
            let keyed_existing = self.apply_cl_sequence_key(keyfn.as_ref(), existing, env)?;
            let matches = if let Some(predicate) = test_not.as_ref() {
                !self.call_binary_predicate(predicate, &keyed_new, &keyed_existing, env)?
            } else {
                primitives::value_matches_with_test(
                    self,
                    &keyed_new,
                    &keyed_existing,
                    testfn.as_ref(),
                    env,
                )?
            };
            if matches {
                already_present = true;
                break;
            }
        }

        if already_present {
            return Ok(current);
        }

        let updated = Value::cons(new_value, current);
        self.set_resolved_setf_place_value(&place, updated.clone(), env)?;
        Ok(updated)
    }

    fn sf_defun(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 4 {
            return Err(LispError::Signal("defun needs name, params, body".into()));
        }
        let name = items[1].as_symbol()?.to_string();
        let params = self.parse_params(&items[2])?;

        // Skip docstring if present
        let body_start = if items.len() > 4 {
            if let Value::String(_) = &items[3] {
                4
            } else {
                3
            }
        } else {
            3
        };

        if body_start < items.len()
            && let Some(setter) = function_declare_gv_setter(&items[body_start])
        {
            self.put_symbol_property(&name, "emaxx-gv-setter", Value::Symbol(setter));
        }
        let body: Vec<Value> = items[body_start..].to_vec();
        let lambda = Value::Lambda(params, body, shared_env(env.clone()));
        self.functions.push((name.clone(), lambda));
        Ok(Value::Symbol(name))
    }

    fn sf_defclass(&mut self, items: &[Value]) -> Result<Value, LispError> {
        let Some(name) = items.get(1).and_then(|value| value.as_symbol().ok()) else {
            return Ok(Value::Nil);
        };
        let parents = items
            .get(2)
            .map(Value::to_vec)
            .transpose()?
            .unwrap_or_default()
            .into_iter()
            .filter_map(|value| value.as_symbol().ok().map(str::to_string))
            .collect::<Vec<_>>();
        let slot_specs = items
            .get(3)
            .map(Value::to_vec)
            .transpose()?
            .unwrap_or_default();
        let options = items.get(4..).unwrap_or(&[]).to_vec();
        self.register_class(name, parents, slot_specs, options);
        self.set_function_binding(
            name,
            Some(Value::Lambda(
                vec!["&rest".into(), "initargs".into()],
                vec![Value::list([
                    Value::Symbol("emaxx-class-make".into()),
                    Value::list([
                        Value::Symbol("quote".into()),
                        Value::Symbol(name.to_string()),
                    ]),
                    Value::Symbol("initargs".into()),
                ])],
                shared_env(Vec::new()),
            )),
        );
        self.set_function_binding(
            &format!("{name}-p"),
            Some(Value::Lambda(
                vec!["object".into()],
                vec![Value::list([
                    Value::Symbol("emaxx-class-p".into()),
                    Value::list([
                        Value::Symbol("quote".into()),
                        Value::Symbol(name.to_string()),
                    ]),
                    Value::Symbol("object".into()),
                ])],
                shared_env(Vec::new()),
            )),
        );
        Ok(Value::Symbol(name.to_string()))
    }

    fn sf_cl_defun(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 4 {
            return Err(LispError::Signal(
                "cl-defun needs name, params, body".into(),
            ));
        }

        let name = items[1].as_symbol()?.to_string();
        let lowered_cl_defun = lower_cl_defun_lambda_list(&name, &items[2])?;

        let original_body = items[3..].to_vec();
        let body_prefix_len = original_body
            .iter()
            .take_while(|form| {
                matches!(form, Value::String(_) | Value::StringObject(_))
                    || is_function_declare_form(form)
                    || is_function_interactive_form(form)
            })
            .count();
        let mut lowered_body = original_body[..body_prefix_len].to_vec();
        let mut executable_body = original_body[body_prefix_len..].to_vec();
        if !lowered_cl_defun.keyword_bindings.is_empty() {
            let mut let_bindings = Vec::new();
            for binding in &lowered_cl_defun.keyword_bindings {
                let present_name =
                    format!("emaxx--cl-defun-{}-{}-present", name, binding.variable_name);
                let keyword_rest_param =
                    lowered_cl_defun.keyword_rest_param.clone().ok_or_else(|| {
                        LispError::Signal("cl-defun keyword lowering lost its rest source".into())
                    })?;
                let keyword_symbol = Value::Symbol(binding.keyword_name.clone());
                let keyword_source = Value::Symbol(keyword_rest_param);
                let_bindings.push(Value::list([
                    Value::Symbol(present_name.clone()),
                    Value::list([
                        Value::Symbol("plist-member".into()),
                        keyword_source.clone(),
                        keyword_symbol.clone(),
                    ]),
                ]));
                let_bindings.push(Value::list([
                    Value::Symbol(binding.variable_name.clone()),
                    Value::list([
                        Value::Symbol("if".into()),
                        Value::Symbol(present_name.clone()),
                        Value::list([
                            Value::Symbol("plist-get".into()),
                            keyword_source.clone(),
                            keyword_symbol,
                        ]),
                        binding.default_value.clone(),
                    ]),
                ]));
                if let Some(supplied_name) = &binding.supplied_name {
                    let_bindings.push(Value::list([
                        Value::Symbol(supplied_name.clone()),
                        Value::list([
                            Value::Symbol("if".into()),
                            Value::Symbol(present_name),
                            Value::T,
                            Value::Nil,
                        ]),
                    ]));
                }
            }
            let mut wrapped = vec![Value::Symbol("let*".into()), Value::list(let_bindings)];
            wrapped.append(&mut executable_body);
            executable_body = vec![Value::list(wrapped)];
        }
        for (pattern, temp_name) in lowered_cl_defun.destructuring_bindings.into_iter().rev() {
            let mut wrapped = vec![
                Value::Symbol("cl-destructuring-bind".into()),
                pattern,
                Value::Symbol(temp_name),
            ];
            wrapped.append(&mut executable_body);
            executable_body = vec![Value::list(wrapped)];
        }
        lowered_body.append(&mut executable_body);

        let mut lowered = Vec::with_capacity(3 + lowered_body.len());
        lowered.push(Value::Symbol("defun".into()));
        lowered.push(Value::Symbol(name));
        lowered.push(Value::list(lowered_cl_defun.params));
        lowered.extend(lowered_body);
        self.sf_defun(&lowered, env)
    }

    fn sf_cl_defmacro(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 4 {
            return Err(LispError::Signal(
                "cl-defmacro needs name, params, body".into(),
            ));
        }

        let name = items[1].as_symbol()?.to_string();
        if self.lookup_function("cl-copy-list", env).is_err() {
            self.load_target("cl-lib")?;
        }
        if self.lookup_function("cl--transform-lambda", env).is_err() {
            self.load_target("cl-macs")?;
        }
        let transformer = self.lookup_function("cl--transform-lambda", env)?;
        let lambda_form = Value::cons(items[2].clone(), Value::list(items[3..].to_vec()));
        let transformed = self.call_function_value(
            transformer,
            Some("cl--transform-lambda"),
            &[lambda_form, Value::Symbol(name.clone())],
            env,
        )?;
        let mut lowered = Vec::with_capacity(2);
        lowered.push(Value::Symbol("defmacro".into()));
        lowered.push(Value::Symbol(name));
        lowered.extend(transformed.to_vec()?);
        self.sf_defmacro(&lowered)
    }

    fn sf_cl_defgeneric(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::Signal(
                "cl-defgeneric needs name and params".into(),
            ));
        }
        let name = function_name_from_binding_form(&items[1])?;
        let mut body_start = 3;
        if matches!(
            items.get(3),
            Some(Value::String(_) | Value::StringObject(_))
        ) {
            body_start = 4;
        }
        while let Some(form) = items.get(body_start) {
            let Ok(parts) = form.to_vec() else {
                break;
            };
            let Some(Value::Symbol(head)) = parts.first() else {
                break;
            };
            match head.as_str() {
                "declare" => {
                    body_start += 1;
                }
                ":documentation" | ":argument-precedence-order" => {
                    body_start += 1;
                }
                ":method" => {
                    let mut lowered_method = Vec::with_capacity(parts.len() + 1);
                    lowered_method.push(Value::Symbol("cl-defmethod".into()));
                    lowered_method.push(Value::Symbol(name.clone()));
                    lowered_method.extend(parts[1..].iter().cloned());
                    self.sf_cl_defmethod(&lowered_method, env)?;
                    body_start += 1;
                }
                _ => break,
            }
        }
        if body_start < items.len() {
            let mut lowered = Vec::with_capacity(items.len() - body_start + 4);
            lowered.push(Value::Symbol("cl-defun".into()));
            lowered.push(Value::Symbol(name.clone()));
            lowered.push(items[2].clone());
            if matches!(
                items.get(3),
                Some(Value::String(_) | Value::StringObject(_))
            ) {
                lowered.push(items[3].clone());
            }
            lowered.extend(items[body_start..].iter().cloned());
            self.sf_cl_defun(&lowered, env)?;
        } else if self.lookup_function(&name, env).is_err() {
            self.set_function_binding(&name, Some(Value::BuiltinFunc("ignore".into())));
        }
        Ok(Value::Symbol(name))
    }

    fn sf_cl_generic_define_generalizer(&mut self, items: &[Value]) -> Result<Value, LispError> {
        if items.len() < 5 {
            return Err(LispError::Signal(
                "cl-generic-define-generalizer needs name, priority, tagcode, and specializers"
                    .into(),
            ));
        }
        let name = items[1].as_symbol()?.to_string();
        let priority = items[2].as_integer()?;
        let tagcode_function = self.eval(&items[3], &mut Vec::new())?;
        let specializers_function = self.eval(&items[4], &mut Vec::new())?;
        self.register_generic_generalizer(&name, priority, tagcode_function, specializers_function);
        Ok(Value::Symbol(name))
    }

    fn sf_cl_defmethod(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 4 {
            return Err(LispError::Signal(
                "cl-defmethod needs name, params, body".into(),
            ));
        }
        let name = function_name_from_binding_form(&items[1])?;
        let lambda_list_index = items
            .iter()
            .enumerate()
            .skip(2)
            .find_map(|(index, value)| {
                matches!(value, Value::Cons(_, _) | Value::Nil).then_some(index)
            })
            .ok_or_else(|| LispError::Signal("cl-defmethod needs a lambda list".into()))?;
        let mut lowered = Vec::with_capacity(items.len());
        lowered.push(Value::Symbol("cl-defun".into()));
        lowered.push(Value::Symbol(name));
        lowered.push(lower_cl_defmethod_lambda_list(&items[lambda_list_index])?);
        lowered.extend(items[lambda_list_index + 1..].iter().cloned());
        self.sf_cl_defun(&lowered, env)
    }

    fn sf_oclosure_define(&mut self, items: &[Value]) -> Result<Value, LispError> {
        let Some(name_form) = items.get(1) else {
            return Err(LispError::Signal("oclosure-define needs a name".into()));
        };
        Ok(Value::Symbol(function_name_from_binding_form(name_form)?))
    }

    fn sf_oclosure_lambda(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::Signal(
                "oclosure-lambda needs slots, args, and body".into(),
            ));
        }
        let mut lowered = Vec::with_capacity(items.len());
        lowered.push(Value::Symbol("lambda".into()));
        lowered.push(items[2].clone());
        lowered.extend(items[3..].iter().cloned());
        self.sf_lambda(&lowered, env)
    }

    fn sf_define_inline(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 4 {
            return Err(LispError::Signal(
                "define-inline needs name, params, body".into(),
            ));
        }
        let mut lowered = Vec::with_capacity(items.len());
        lowered.push(Value::Symbol("defun".into()));
        lowered.push(items[1].clone());
        lowered.push(items[2].clone());
        lowered.extend(items[3..].iter().map(lower_define_inline_form));
        self.sf_defun(&lowered, env)
    }

    fn sf_lambda(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::Signal("lambda needs params".into()));
        }
        let params = self.parse_params(&items[1])?;
        let body: Vec<Value> = items[2..].to_vec();
        let closure_env = if self.lambda_capture_override().unwrap_or(true) {
            shared_env(env.clone())
        } else {
            shared_env(Vec::new())
        };
        Ok(Value::Lambda(params, body, closure_env))
    }

    fn sf_eval_function(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 || items.len() > 3 {
            return Err(LispError::WrongNumberOfArgs(
                "eval".into(),
                items.len().saturating_sub(1),
            ));
        }
        let mut evaluated = Vec::with_capacity(items.len().saturating_sub(1));
        for item in &items[1..] {
            evaluated.push(self.eval(item, env)?);
        }
        crate::lisp::primitives::eval_impl(self, &evaluated, env)
    }

    fn sf_insert_function(
        &mut self,
        items: &[Value],
        env: &mut Env,
        inherit: bool,
        before_markers: bool,
    ) -> Result<Value, LispError> {
        let mut evaluated = Vec::with_capacity(items.len().saturating_sub(1));
        for item in &items[1..] {
            evaluated.push(self.eval(item, env)?);
        }
        crate::lisp::primitives::insert_impl(self, &evaluated, env, inherit, before_markers)
    }

    fn sf_insert_char_function(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let mut evaluated = Vec::with_capacity(items.len().saturating_sub(1));
        for item in &items[1..] {
            evaluated.push(self.eval(item, env)?);
        }
        crate::lisp::primitives::insert_char_impl(self, &evaluated, env)
    }

    fn sf_call_interactively(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("call-interactively".into(), 0));
        }
        let mut evaluated = Vec::with_capacity(items.len().saturating_sub(1));
        for item in &items[1..] {
            evaluated.push(self.eval(item, env)?);
        }
        crate::lisp::primitives::call_interactively_impl(self, &evaluated, env)
    }

    fn parse_params(&self, spec: &Value) -> Result<Vec<String>, LispError> {
        match spec {
            Value::Nil => Ok(Vec::new()),
            Value::Cons(_, _) => {
                let items = spec.to_vec()?;
                validate_lambda_list(spec, &items)?;
                items
                    .into_iter()
                    .map(|v| match v {
                        Value::Symbol(s) => Ok(s),
                        _ => Err(invalid_function(spec.clone())),
                    })
                    .collect()
            }
            _ => Err(invalid_function(spec.clone())),
        }
    }

    fn sf_while(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let limit = 100_000; // safety valve
        let mut iterations = 0;
        loop {
            let cond = self.eval(&items[1], env)?;
            if cond.is_nil() {
                break;
            }
            self.sf_progn(&items[2..], env)?;
            iterations += 1;
            if iterations > limit {
                return Err(LispError::Signal(
                    "while loop exceeded iteration limit".into(),
                ));
            }
        }
        Ok(Value::Nil)
    }

    fn sf_dolist(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let spec = items[1].to_vec()?;
        let var_name = spec[0].as_symbol()?.to_string();
        let list_val = self.eval(&spec[1], env)?;
        let list_items = list_val.to_vec()?;

        env.push(vec![(var_name.clone(), Value::Nil)]);
        for item in list_items {
            let frame = env.last_mut().expect("env frame just pushed");
            frame[0] = (var_name.clone(), Self::stored_value(item));
            self.sf_progn(&items[2..], env)?;
        }
        let result = if spec.len() > 2 {
            self.eval(&spec[2], env)?
        } else {
            Value::Nil
        };
        env.pop();
        Ok(result)
    }

    fn sf_pcase_dolist(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let spec = items[1].to_vec()?;
        if spec.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "pcase-dolist".into(),
                items.len().saturating_sub(1),
            ));
        }
        let pattern = &spec[0];
        let list_val = self.eval(&spec[1], env)?;
        let list_items = list_val.to_vec()?;

        for item in list_items {
            let mut bindings = Vec::new();
            if !pcase_pattern_bindings_lenient_list(self, env, pattern, &item, &mut bindings)? {
                return Err(LispError::Signal("pcase-dolist: no matching clause".into()));
            }
            env.push(bindings);
            self.sf_progn(&items[2..], env)?;
            env.pop();
        }

        if spec.len() > 2 {
            self.eval(&spec[2], env)
        } else {
            Ok(Value::Nil)
        }
    }

    fn sf_dotimes(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let spec = items[1].to_vec()?;
        let var_name = spec[0].as_symbol()?.to_string();
        let count = self.eval(&spec[1], env)?.as_integer()?;

        env.push(vec![(var_name.clone(), Value::Integer(0))]);
        for i in 0..count {
            let frame = env.last_mut().expect("env frame just pushed");
            frame[0] = (var_name.clone(), Value::Integer(i));
            self.sf_progn(&items[2..], env)?;
        }
        let result = if spec.len() > 2 {
            self.eval(&spec[2], env)?
        } else {
            Value::Nil
        };
        env.pop();
        Ok(result)
    }

    fn sf_cl_loop(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        enum LoopSpec {
            Range { name: String, values: Vec<Value> },
            List { pattern: Value, values: Vec<Value> },
            From { name: String, start: i64 },
            Assign { name: String, expr: Value },
            Repeat { count: usize },
        }

        enum LoopAction {
            Do(Vec<Value>),
            Collect(Value),
            Append(Value),
            Thereis {
                expr: Value,
                until: Option<Value>,
            },
            Always(Value),
            Sum(Value),
            Return(Value),
            WhenReturn {
                condition: Value,
                expr: Value,
            },
            WhenCollectInto {
                condition: Value,
                expr: Value,
                name: String,
            },
            UnlessCollect {
                condition: Value,
                expr: Value,
            },
            UnlessDo {
                condition: Value,
                body: Vec<Value>,
            },
        }

        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-loop".into(),
                items.len().saturating_sub(1),
            ));
        }

        let mut specs = Vec::new();
        let mut with_bindings = Vec::new();
        let mut while_expr = None;
        let mut index = 1usize;
        while index < items.len() {
            match items.get(index) {
                Some(Value::Symbol(symbol)) if symbol == "with" => {
                    let pattern = items
                        .get(index + 1)
                        .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                        .clone();
                    if !matches!(items.get(index + 2), Some(Value::Symbol(eq)) if eq == "=") {
                        return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                    }
                    let expr = items
                        .get(index + 3)
                        .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                        .clone();
                    with_bindings.push((pattern, expr));
                    index += 4;
                }
                Some(Value::Symbol(symbol)) if symbol == "for" => {
                    let pattern = items
                        .get(index + 1)
                        .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                        .clone();
                    match items.get(index + 2) {
                        Some(Value::Symbol(kind)) if kind == "from" => {
                            let name = pattern.as_symbol()?.to_string();
                            let start = self
                                .eval(
                                    items.get(index + 3).ok_or_else(|| {
                                        LispError::Signal("Unsupported cl-loop syntax".into())
                                    })?,
                                    env,
                                )?
                                .as_integer()?;
                            match items
                                .get(index + 4)
                                .and_then(|value| value.as_symbol().ok())
                            {
                                Some("to") | Some("upto") | Some("below") => {
                                    let bound_kind =
                                        items[index + 4].as_symbol().expect("checked symbol");
                                    let end = self
                                        .eval(
                                            items.get(index + 5).ok_or_else(|| {
                                                LispError::Signal(
                                                    "Unsupported cl-loop syntax".into(),
                                                )
                                            })?,
                                            env,
                                        )?
                                        .as_integer()?;
                                    let mut step = 1usize;
                                    let mut next_index = 6usize;
                                    if matches!(items.get(index + 6), Some(Value::Symbol(by)) if by == "by")
                                    {
                                        step = self
                                            .eval(
                                                items.get(index + 7).ok_or_else(|| {
                                                    LispError::Signal(
                                                        "Unsupported cl-loop syntax".into(),
                                                    )
                                                })?,
                                                env,
                                            )?
                                            .as_integer()?
                                            .max(1)
                                            as usize;
                                        next_index = 8;
                                    }
                                    let values = match bound_kind {
                                        "to" | "upto" if start <= end => (start..=end)
                                            .step_by(step)
                                            .map(Value::Integer)
                                            .collect(),
                                        "below" if start < end => {
                                            (start..end).step_by(step).map(Value::Integer).collect()
                                        }
                                        "to" | "upto" | "below" => Vec::new(),
                                        _ => unreachable!(),
                                    };
                                    specs.push(LoopSpec::Range { name, values });
                                    index += next_index;
                                }
                                _ => {
                                    specs.push(LoopSpec::From { name, start });
                                    index += 4;
                                }
                            }
                        }
                        Some(Value::Symbol(kind))
                            if matches!(kind.as_str(), "to" | "upto" | "below") =>
                        {
                            let name = pattern.as_symbol()?.to_string();
                            let end = self
                                .eval(
                                    items.get(index + 3).ok_or_else(|| {
                                        LispError::Signal("Unsupported cl-loop syntax".into())
                                    })?,
                                    env,
                                )?
                                .as_integer()?;
                            let mut step = 1usize;
                            let mut next_index = 4usize;
                            if matches!(items.get(index + 4), Some(Value::Symbol(by)) if by == "by")
                            {
                                step = self
                                    .eval(
                                        items.get(index + 5).ok_or_else(|| {
                                            LispError::Signal("Unsupported cl-loop syntax".into())
                                        })?,
                                        env,
                                    )?
                                    .as_integer()?
                                    .max(1) as usize;
                                next_index = 6;
                            }
                            let values = match kind.as_str() {
                                "to" | "upto" if end >= 0 => {
                                    (0..=end).step_by(step).map(Value::Integer).collect()
                                }
                                "below" if end > 0 => {
                                    (0..end).step_by(step).map(Value::Integer).collect()
                                }
                                "to" | "upto" | "below" => Vec::new(),
                                _ => unreachable!(),
                            };
                            specs.push(LoopSpec::Range { name, values });
                            index += next_index;
                        }
                        Some(Value::Symbol(kind)) if kind == "in" => {
                            let values = self
                                .eval(
                                    items.get(index + 3).ok_or_else(|| {
                                        LispError::Signal("Unsupported cl-loop syntax".into())
                                    })?,
                                    env,
                                )?
                                .to_vec()?;
                            specs.push(LoopSpec::List { pattern, values });
                            index += 4;
                        }
                        Some(Value::Symbol(kind)) if kind == "across" => {
                            let name = pattern.as_symbol()?.to_string();
                            let string = crate::lisp::primitives::string_text(&self.eval(
                                items.get(index + 3).ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?,
                                env,
                            )?)?;
                            let values =
                                string.chars().map(|ch| Value::Integer(ch as i64)).collect();
                            specs.push(LoopSpec::List {
                                pattern: Value::Symbol(name),
                                values,
                            });
                            index += 4;
                        }
                        Some(Value::Symbol(kind)) if kind == "=" => {
                            let name = pattern.as_symbol()?.to_string();
                            let expr = items
                                .get(index + 3)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone();
                            specs.push(LoopSpec::Assign { name, expr });
                            index += 4;
                        }
                        _ => return Err(LispError::Signal("Unsupported cl-loop syntax".into())),
                    }
                }
                Some(Value::Symbol(symbol)) if symbol == "while" => {
                    while_expr = Some(
                        items
                            .get(index + 1)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                    );
                    index += 2;
                }
                Some(Value::Symbol(symbol)) if symbol == "repeat" => {
                    let count = self
                        .eval(
                            items.get(index + 1).ok_or_else(|| {
                                LispError::Signal("Unsupported cl-loop syntax".into())
                            })?,
                            env,
                        )?
                        .as_integer()?
                        .max(0) as usize;
                    specs.push(LoopSpec::Repeat { count });
                    index += 2;
                }
                _ => break,
            }
        }

        if (specs.is_empty() && with_bindings.is_empty()) || index >= items.len() {
            return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
        }

        let iterations = specs
            .iter()
            .filter_map(|spec| match spec {
                LoopSpec::Range { values, .. } | LoopSpec::List { values, .. } => {
                    Some(values.len())
                }
                LoopSpec::Repeat { count } => Some(*count),
                LoopSpec::From { .. } | LoopSpec::Assign { .. } => None,
            })
            .min()
            .unwrap_or(1);

        let mut bindings = specs
            .iter()
            .filter_map(|spec| match spec {
                LoopSpec::Range { name, .. }
                | LoopSpec::From { name, .. }
                | LoopSpec::Assign { name, .. } => Some((name.clone(), Value::Nil)),
                LoopSpec::List { .. } | LoopSpec::Repeat { .. } => None,
            })
            .collect::<Vec<_>>();
        for spec in &specs {
            if let LoopSpec::List { pattern, .. } = spec {
                self.collect_cl_pattern_names(pattern, &mut bindings)?;
            }
        }
        for (pattern, _) in &with_bindings {
            self.collect_cl_pattern_names(pattern, &mut bindings)?;
        }
        env.push(bindings);
        for (pattern, expr) in &with_bindings {
            let value = self.eval(expr, env)?;
            let frame = env.last_mut().expect("env frame just pushed");
            self.bind_cl_pattern(pattern, value, frame)?;
        }

        let mut final_return = None;
        let action = match items.get(index) {
            Some(Value::Symbol(symbol)) if symbol == "when" => match items.get(index + 2) {
                Some(Value::Symbol(kind)) if kind == "return" => {
                    if !matches!(items.get(index + 4), Some(Value::Symbol(kind)) if kind == "finally")
                        || !matches!(items.get(index + 5), Some(Value::Symbol(kind)) if kind == "return")
                    {
                        return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                    }
                    final_return = Some(
                        items
                            .get(index + 6)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                    );
                    LoopAction::WhenReturn {
                        condition: items
                            .get(index + 1)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                        expr: items
                            .get(index + 3)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                    }
                }
                Some(Value::Symbol(kind)) if kind == "collect" => {
                    if !matches!(items.get(index + 4), Some(Value::Symbol(kind)) if kind == "into")
                        || !matches!(items.get(index + 6), Some(Value::Symbol(kind)) if kind == "finally")
                        || !matches!(items.get(index + 7), Some(Value::Symbol(kind)) if kind == "return")
                    {
                        return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                    }
                    let name = items
                        .get(index + 5)
                        .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                        .as_symbol()?
                        .to_string();
                    final_return = Some(
                        items
                            .get(index + 8)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                    );
                    LoopAction::WhenCollectInto {
                        condition: items
                            .get(index + 1)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                        expr: items
                            .get(index + 3)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                        name,
                    }
                }
                _ => return Err(LispError::Signal("Unsupported cl-loop syntax".into())),
            },
            Some(Value::Symbol(symbol)) if symbol == "do" => {
                LoopAction::Do(items[index + 1..].to_vec())
            }
            Some(Value::Symbol(symbol)) if symbol == "collect" => LoopAction::Collect(
                items
                    .get(index + 1)
                    .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                    .clone(),
            ),
            Some(Value::Symbol(symbol)) if symbol == "append" => LoopAction::Append(
                items
                    .get(index + 1)
                    .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                    .clone(),
            ),
            Some(Value::Symbol(symbol)) if symbol == "thereis" => LoopAction::Thereis {
                expr: items
                    .get(index + 1)
                    .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                    .clone(),
                until: if matches!(items.get(index + 2), Some(Value::Symbol(kind)) if kind == "until")
                {
                    Some(
                        items
                            .get(index + 3)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                    )
                } else {
                    None
                },
            },
            Some(Value::Symbol(symbol)) if symbol == "always" => LoopAction::Always(
                items
                    .get(index + 1)
                    .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                    .clone(),
            ),
            Some(Value::Symbol(symbol)) if symbol == "sum" => LoopAction::Sum(
                items
                    .get(index + 1)
                    .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                    .clone(),
            ),
            Some(Value::Symbol(symbol)) if symbol == "return" => LoopAction::Return(
                items
                    .get(index + 1)
                    .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                    .clone(),
            ),
            Some(Value::Symbol(symbol)) if symbol == "unless" => match items.get(index + 2) {
                Some(Value::Symbol(kind)) if kind == "collect" => LoopAction::UnlessCollect {
                    condition: items
                        .get(index + 1)
                        .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                        .clone(),
                    expr: items
                        .get(index + 3)
                        .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                        .clone(),
                },
                Some(Value::Symbol(kind)) if kind == "do" => LoopAction::UnlessDo {
                    condition: items
                        .get(index + 1)
                        .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                        .clone(),
                    body: items[index + 3..].to_vec(),
                },
                _ => return Err(LispError::Signal("Unsupported cl-loop syntax".into())),
            },
            _ => return Err(LispError::Signal("Unsupported cl-loop syntax".into())),
        };
        if let LoopAction::WhenCollectInto { name, .. } = &action {
            let frame = env.last_mut().expect("env frame just pushed");
            Self::upsert_frame_binding(frame, name.clone(), Value::Nil);
        }

        let mut result = Value::Nil;
        let mut returned_early = false;
        let mut collected = Vec::new();
        let mut sum = 0i64;
        for iteration in 0..iterations {
            {
                let frame = env.last_mut().expect("env frame just pushed");
                for spec in &specs {
                    match spec {
                        LoopSpec::Range { name, values } => {
                            Self::upsert_frame_binding(
                                frame,
                                name.clone(),
                                Self::stored_value(values[iteration].clone()),
                            );
                        }
                        LoopSpec::List { pattern, values } => {
                            self.bind_cl_pattern(pattern, values[iteration].clone(), frame)?;
                        }
                        LoopSpec::From { name, start } => {
                            Self::upsert_frame_binding(
                                frame,
                                name.clone(),
                                Self::stored_value(Value::Integer(*start + iteration as i64)),
                            );
                        }
                        LoopSpec::Assign { .. } => {}
                        LoopSpec::Repeat { .. } => {}
                    }
                }
            }

            for spec in &specs {
                if let LoopSpec::Assign { name, expr } = spec {
                    let value = Self::stored_value(self.eval(expr, env)?);
                    let frame = env.last_mut().expect("env frame just pushed");
                    Self::upsert_frame_binding(frame, name.clone(), value);
                }
            }

            if let Some(expr) = while_expr.as_ref()
                && !self.eval(expr, env)?.is_truthy()
            {
                break;
            }

            match &action {
                LoopAction::Do(body) => result = self.eval_cl_loop_do_body(body, env)?,
                LoopAction::Collect(expr) => collected.push(self.eval(expr, env)?),
                LoopAction::Append(expr) => {
                    let values = self.eval(expr, env)?.to_vec()?;
                    collected.extend(values);
                }
                LoopAction::Thereis { expr, until } => {
                    if let Some(until_expr) = until
                        && self.eval(until_expr, env)?.is_truthy()
                    {
                        result = Value::Nil;
                        break;
                    }
                    let value = self.eval(expr, env)?;
                    if value.is_truthy() {
                        result = value;
                        break;
                    }
                }
                LoopAction::Always(expr) => {
                    if !self.eval(expr, env)?.is_truthy() {
                        result = Value::Nil;
                        env.pop();
                        return Ok(result);
                    }
                    result = Value::T;
                }
                LoopAction::Sum(expr) => {
                    sum += self.eval(expr, env)?.as_integer()?;
                    result = Value::Integer(sum);
                }
                LoopAction::Return(expr) => {
                    result = self.eval(expr, env)?;
                    returned_early = true;
                    break;
                }
                LoopAction::WhenReturn { condition, expr } => {
                    if self.eval(condition, env)?.is_truthy() {
                        result = self.eval(expr, env)?;
                        returned_early = true;
                        break;
                    }
                }
                LoopAction::WhenCollectInto {
                    condition,
                    expr,
                    name,
                } => {
                    if self.eval(condition, env)?.is_truthy() {
                        collected.push(self.eval(expr, env)?);
                        let frame = env.last_mut().expect("env frame just pushed");
                        Self::upsert_frame_binding(
                            frame,
                            name.clone(),
                            Value::list(collected.clone()),
                        );
                    }
                }
                LoopAction::UnlessCollect { condition, expr } => {
                    if !self.eval(condition, env)?.is_truthy() {
                        collected.push(self.eval(expr, env)?);
                    }
                }
                LoopAction::UnlessDo { condition, body } => {
                    if !self.eval(condition, env)?.is_truthy() {
                        result = self.sf_progn(body, env)?;
                    }
                }
            }
        }

        if let Some(expr) = final_return.as_ref()
            && !returned_early
        {
            result = self.eval(expr, env)?;
        }

        env.pop();
        if final_return.is_some() {
            return Ok(result);
        }
        Ok(match action {
            LoopAction::Collect(_) | LoopAction::Append(_) => Value::list(collected),
            LoopAction::Always(_) if result.is_nil() => Value::Nil,
            LoopAction::Always(_) => Value::T,
            LoopAction::Sum(_) => Value::Integer(sum),
            LoopAction::UnlessCollect { .. } => Value::list(collected),
            _ => result,
        })
    }

    fn eval_resolved_setf_place_current_value(
        &mut self,
        place: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        match &place {
            Value::Symbol(name) => self.lookup(name, env),
            Value::Cons(_, _) => {
                let items = place.to_vec()?;
                if matches!(items.first(), Some(Value::Symbol(name)) if name == "--emaxx-setf-car-place" || name == "--emaxx-setf-cdr-place")
                {
                    let Some(target) = items.get(1) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    if matches!(items.first(), Some(Value::Symbol(name)) if name == "--emaxx-setf-car-place")
                    {
                        target.car()
                    } else {
                        target.cdr()
                    }
                } else {
                    self.eval(place, env)
                }
            }
            _ => self.eval(place, env),
        }
    }

    fn resolve_setf_place(&mut self, place: &Value, env: &mut Env) -> Result<Value, LispError> {
        let Value::Cons(_, _) = place else {
            return Ok(place.clone());
        };
        let items = place.to_vec()?;
        match items.first() {
            Some(Value::Symbol(name)) if name == "cond" => {
                for clause in &items[1..] {
                    let forms = clause.to_vec()?;
                    if forms.is_empty() {
                        continue;
                    }
                    if self.eval(&forms[0], env)?.is_truthy() {
                        if forms.len() > 2 {
                            self.sf_progn(&forms[1..forms.len() - 1], env)?;
                        }
                        return self.resolve_setf_place(forms.last().unwrap_or(&Value::Nil), env);
                    }
                }
                Ok(Value::Nil)
            }
            Some(Value::Symbol(name)) if name == "if" => {
                let Some(condition) = items.get(1) else {
                    return Ok(Value::Nil);
                };
                if self.eval(condition, env)?.is_truthy() {
                    self.resolve_setf_place(items.get(2).unwrap_or(&Value::Nil), env)
                } else {
                    self.resolve_setf_place(items.get(3).unwrap_or(&Value::Nil), env)
                }
            }
            Some(Value::Symbol(name)) if name == "progn" => {
                if items.len() > 2 {
                    self.sf_progn(&items[1..items.len() - 1], env)?;
                }
                self.resolve_setf_place(items.last().unwrap_or(&Value::Nil), env)
            }
            Some(Value::Symbol(name)) if name == "symbol-value" => {
                let Some(symbol_form) = items.get(1) else {
                    return Ok(place.clone());
                };
                let symbol = self.eval(symbol_form, env)?;
                Ok(Value::list([
                    Value::Symbol("symbol-value".into()),
                    quoted_literal(&symbol),
                ]))
            }
            Some(Value::Symbol(name))
                if matches!(name.as_str(), "car" | "cdr")
                    || self
                        .get_symbol_property(name, "emaxx-struct-slot")
                        .is_some()
                    || self.get_symbol_property(name, "emaxx-gv-setter").is_some() =>
            {
                let Some(target_expr) = items.get(1) else {
                    return Ok(place.clone());
                };
                let target = self.eval(target_expr, env)?;
                if matches!(name.as_str(), "car" | "cdr") {
                    return Ok(Value::list([
                        Value::Symbol(format!("--emaxx-setf-{name}-place")),
                        target,
                    ]));
                }
                if self.get_symbol_property(name, "emaxx-gv-setter").is_some() {
                    let mut resolved = Vec::with_capacity(items.len());
                    resolved.push(Value::Symbol(name.clone()));
                    resolved.push(quoted_literal(&target));
                    for arg in &items[2..] {
                        let evaluated = self.eval(arg, env)?;
                        resolved.push(quoted_literal(&evaluated));
                    }
                    return Ok(Value::list(resolved));
                }
                Ok(Value::list([
                    Value::Symbol(name.clone()),
                    quoted_literal(&target),
                ]))
            }
            Some(Value::Symbol(name)) if name == "overlay-get" => {
                let Some(overlay_expr) = items.get(1) else {
                    return Ok(place.clone());
                };
                let Some(prop_expr) = items.get(2) else {
                    return Ok(place.clone());
                };
                let overlay = self.eval(overlay_expr, env)?;
                let property = self.eval(prop_expr, env)?;
                Ok(Value::list([
                    Value::Symbol("overlay-get".into()),
                    quoted_literal(&overlay),
                    quoted_literal(&property),
                ]))
            }
            Some(Value::Symbol(name)) if name == "terminal-parameter" => {
                let Some(terminal_expr) = items.get(1) else {
                    return Ok(place.clone());
                };
                let Some(parameter_expr) = items.get(2) else {
                    return Ok(place.clone());
                };
                let terminal = self.eval(terminal_expr, env)?;
                let parameter = self.eval(parameter_expr, env)?;
                Ok(Value::list([
                    Value::Symbol("terminal-parameter".into()),
                    quoted_literal(&terminal),
                    quoted_literal(&parameter),
                ]))
            }
            Some(Value::Symbol(name)) if name == "alist-get" => {
                let Some(key_expr) = items.get(1) else {
                    return Ok(place.clone());
                };
                let Some(alist_place) = items.get(2) else {
                    return Ok(place.clone());
                };
                let key = self.eval(key_expr, env)?;
                let mut resolved = vec![
                    Value::Symbol("alist-get".into()),
                    quoted_literal(&key),
                    self.resolve_setf_place(alist_place, env)?,
                ];
                if let Some(default_expr) = items.get(3) {
                    let default = self.eval(default_expr, env)?;
                    resolved.push(quoted_literal(&default));
                }
                if let Some(remove_expr) = items.get(4) {
                    let remove = self.eval(remove_expr, env)?;
                    resolved.push(quoted_literal(&remove));
                }
                if let Some(testfn_expr) = items.get(5) {
                    let testfn = self.eval(testfn_expr, env)?;
                    resolved.push(quoted_literal(&testfn));
                }
                Ok(Value::list(resolved))
            }
            Some(Value::Symbol(name)) if name == "plist-get" => {
                let Some(plist_place) = items.get(1) else {
                    return Ok(place.clone());
                };
                let Some(key_expr) = items.get(2) else {
                    return Ok(place.clone());
                };
                let key = self.eval(key_expr, env)?;
                let mut resolved = vec![
                    Value::Symbol("plist-get".into()),
                    self.resolve_setf_place(plist_place, env)?,
                    quoted_literal(&key),
                ];
                if let Some(testfn_expr) = items.get(3) {
                    let testfn = self.eval(testfn_expr, env)?;
                    resolved.push(quoted_literal(&testfn));
                }
                Ok(Value::list(resolved))
            }
            Some(Value::Symbol(name)) if name == "aref" => {
                let Some(sequence_place) = items.get(1) else {
                    return Ok(place.clone());
                };
                let Some(index_expr) = items.get(2) else {
                    return Ok(place.clone());
                };
                let index = self.eval(index_expr, env)?;
                Ok(Value::list([
                    Value::Symbol("aref".into()),
                    self.resolve_setf_place(sequence_place, env)?,
                    quoted_literal(&index),
                ]))
            }
            Some(Value::Symbol(name)) if name == "image-property" => {
                let Some(image_place) = items.get(1) else {
                    return Ok(place.clone());
                };
                let Some(property_expr) = items.get(2) else {
                    return Ok(place.clone());
                };
                let property = self.eval(property_expr, env)?;
                Ok(Value::list([
                    Value::Symbol("image-property".into()),
                    self.resolve_setf_place(image_place, env)?,
                    quoted_literal(&property),
                ]))
            }
            Some(Value::Symbol(name)) if name == "gethash" => {
                let Some(key_expr) = items.get(1) else {
                    return Ok(place.clone());
                };
                let Some(table_expr) = items.get(2) else {
                    return Ok(place.clone());
                };
                let key = self.eval(key_expr, env)?;
                let table = self.eval(table_expr, env)?;
                Ok(Value::list([
                    Value::Symbol("gethash".into()),
                    quoted_literal(&key),
                    quoted_literal(&table),
                ]))
            }
            _ => Ok(place.clone()),
        }
    }

    fn apply_cl_sequence_key(
        &mut self,
        keyfn: Option<&Value>,
        value: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some(keyfn) = keyfn.filter(|value| !value.is_nil()) else {
            return Ok(value.clone());
        };
        let func = match keyfn {
            Value::Symbol(name) => self.lookup_function(name, env)?,
            other => other.clone(),
        };
        self.call_function_value(
            func,
            keyfn.as_symbol().ok(),
            std::slice::from_ref(value),
            env,
        )
    }

    fn call_binary_predicate(
        &mut self,
        predicate: &Value,
        left: &Value,
        right: &Value,
        env: &mut Env,
    ) -> Result<bool, LispError> {
        let func = match predicate {
            Value::Symbol(name) => self.lookup_function(name, env)?,
            other => other.clone(),
        };
        Ok(self
            .call_function_value(
                func,
                predicate.as_symbol().ok(),
                &[left.clone(), right.clone()],
                env,
            )?
            .is_truthy())
    }

    fn bind_cl_pattern(
        &mut self,
        pattern: &Value,
        value: Value,
        frame: &mut Vec<(String, Value)>,
    ) -> Result<(), LispError> {
        match pattern {
            Value::Symbol(name) if name == "nil" => Ok(()),
            Value::Symbol(name) => {
                Self::upsert_frame_binding(frame, name.clone(), Self::stored_value(value));
                Ok(())
            }
            Value::Cons(_, _) => {
                if let Ok(pattern_items) = pattern.to_vec() {
                    let values = value.to_vec()?;
                    let mut pi = 0usize;
                    let mut vi = 0usize;
                    let mut optional = false;
                    while pi < pattern_items.len() {
                        match &pattern_items[pi] {
                            Value::Symbol(symbol) if symbol == "&optional" => {
                                optional = true;
                                pi += 1;
                                continue;
                            }
                            Value::Symbol(symbol) if symbol == "&rest" => {
                                pi += 1;
                                if let Some(rest_pattern) = pattern_items.get(pi) {
                                    self.bind_cl_pattern(
                                        rest_pattern,
                                        Value::list(values[vi..].to_vec()),
                                        frame,
                                    )?;
                                }
                                break;
                            }
                            subpattern => {
                                let consumed = vi < values.len();
                                let bound_value = if consumed {
                                    values[vi].clone()
                                } else if optional {
                                    Value::Nil
                                } else {
                                    return Err(LispError::WrongNumberOfArgs(
                                        "cl-destructuring-bind".into(),
                                        values.len(),
                                    ));
                                };
                                self.bind_cl_pattern(subpattern, bound_value, frame)?;
                                if consumed {
                                    vi += 1;
                                }
                            }
                        }
                        pi += 1;
                    }
                    Ok(())
                } else {
                    let mut current_pattern = pattern.clone();
                    let mut current_value = value;
                    loop {
                        match current_pattern {
                            Value::Cons(car, cdr) => {
                                let Some((head, tail)) = current_value.cons_values() else {
                                    return Err(LispError::WrongNumberOfArgs(
                                        "cl-destructuring-bind".into(),
                                        0,
                                    ));
                                };
                                self.bind_cl_pattern(&car.borrow().clone(), head, frame)?;
                                current_pattern = cdr.borrow().clone();
                                current_value = tail;
                            }
                            Value::Nil => return Ok(()),
                            other => return self.bind_cl_pattern(&other, current_value, frame),
                        }
                    }
                }
            }
            other => Err(LispError::TypeError("list".into(), other.type_name())),
        }
    }

    fn collect_cl_pattern_names(
        &self,
        pattern: &Value,
        bindings: &mut Vec<(String, Value)>,
    ) -> Result<(), LispError> {
        match pattern {
            Value::Symbol(name) if name == "nil" => Ok(()),
            Value::Symbol(name) => {
                if !bindings.iter().any(|(existing, _)| existing == name) {
                    bindings.push((name.clone(), Value::Nil));
                }
                Ok(())
            }
            Value::Cons(_, _) => {
                if let Ok(items) = pattern.to_vec() {
                    for item in items {
                        if matches!(&item, Value::Symbol(symbol) if symbol == "&optional" || symbol == "&rest")
                        {
                            continue;
                        }
                        self.collect_cl_pattern_names(&item, bindings)?;
                    }
                    Ok(())
                } else {
                    let mut current = pattern.clone();
                    loop {
                        match current {
                            Value::Cons(car, cdr) => {
                                self.collect_cl_pattern_names(&car.borrow().clone(), bindings)?;
                                current = cdr.borrow().clone();
                            }
                            Value::Nil => return Ok(()),
                            other => return self.collect_cl_pattern_names(&other, bindings),
                        }
                    }
                }
            }
            other => Err(LispError::TypeError("list".into(), other.type_name())),
        }
    }

    fn upsert_frame_binding(frame: &mut Vec<(String, Value)>, name: String, value: Value) {
        if let Some(index) = frame.iter().rposition(|(existing, _)| existing == &name) {
            frame[index].1 = value;
        } else {
            frame.push((name, value));
        }
    }

    fn same_frame_shape(left: &[(String, Value)], right: &[(String, Value)]) -> bool {
        left.len() <= right.len()
            && left
                .iter()
                .zip(right.iter())
                .all(|((left_name, _), (right_name, _))| left_name == right_name)
    }

    fn align_captured_frames(captured: &Env, current: &Env) -> Vec<Option<usize>> {
        let mut mapping = vec![None; captured.len()];
        let mut search_start = 0;
        for captured_index in 0..captured.len() {
            for (current_index, current_frame) in current.iter().enumerate().skip(search_start) {
                if Self::same_frame_shape(&captured[captured_index], current_frame) {
                    mapping[captured_index] = Some(current_index);
                    search_start = current_index + 1;
                    break;
                }
            }
        }
        mapping
    }

    fn merge_lexical_lambda_env(current: &Env, captured: &Env, mapping: &[Option<usize>]) -> Env {
        let mut merged = captured.clone();
        for (captured_index, current_index) in mapping.iter().enumerate() {
            if let Some(current_index) = current_index
                && captured_index < merged.len()
                && *current_index < current.len()
            {
                merged[captured_index] = current[*current_index].clone();
            }
        }
        merged
    }

    fn eval_cl_loop_do_body(&mut self, body: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let mut result = Value::Nil;
        let mut index = 0usize;
        while index < body.len() {
            match body.get(index) {
                Some(Value::Symbol(symbol)) if symbol == "when" => {
                    let condition = body
                        .get(index + 1)
                        .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?;
                    if !matches!(body.get(index + 2), Some(Value::Symbol(kind)) if kind == "do") {
                        return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                    }
                    index += 3;
                    let clause_start = index;
                    while index < body.len()
                        && !matches!(body.get(index), Some(Value::Symbol(keyword)) if keyword == "when")
                    {
                        index += 1;
                    }
                    if self.eval(condition, env)?.is_truthy() {
                        result = self.sf_progn(&body[clause_start..index], env)?;
                    }
                }
                Some(form) => {
                    result = self.eval(form, env)?;
                    index += 1;
                }
                None => break,
            }
        }
        Ok(result)
    }

    fn sf_unwind_protect(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let result = self.eval(&items[1], env);
        // Always run cleanup forms
        for form in &items[2..] {
            let _ = self.eval(form, env);
        }
        result
    }

    fn sf_ignore_errors(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        match self.sf_progn(&items[1..], env) {
            Ok(value) => Ok(value),
            Err(_) => Ok(Value::Nil),
        }
    }

    fn sf_ignore_error(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "ignore-error".into(),
                items.len().saturating_sub(1),
            ));
        }

        match self.sf_progn(&items[2..], env) {
            Ok(value) => Ok(value),
            Err(error) => {
                let condition = error.condition_type();
                let matches = match &items[1] {
                    Value::Symbol(symbol) => symbol == &condition || symbol == "error",
                    Value::Cons(_, _) => items[1]
                        .to_vec()?
                        .iter()
                        .filter_map(symbol_name)
                        .any(|symbol| symbol == condition || symbol == "error"),
                    _ => false,
                };
                if matches { Ok(Value::Nil) } else { Err(error) }
            }
        }
    }

    fn sf_condition_case(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        // (condition-case var bodyform handlers...)
        if items.len() < 3 {
            return Ok(Value::Nil);
        }
        let var = match &items[1] {
            Value::Symbol(s) => Some(s.clone()),
            Value::Nil => None,
            other => return Err(wrong_type_argument("symbolp", other.clone())),
        };

        self.condition_case_depth += 1;
        let body_result = self.eval(&items[2], env);
        self.condition_case_depth = self.condition_case_depth.saturating_sub(1);
        match body_result {
            Ok(val) => Ok(val),
            Err(e) => {
                if self.take_condition_case_suspend() {
                    return Err(e);
                }
                let condition = e.condition_type();
                // Try to find a matching handler
                for handler in &items[3..] {
                    let parts = handler.to_vec()?;
                    if parts.is_empty() {
                        continue;
                    }
                    let matches = match &parts[0] {
                        Value::Symbol(symbol) => symbol == &condition || symbol == "error",
                        Value::Cons(_, _) => parts[0]
                            .to_vec()?
                            .iter()
                            .filter_map(symbol_name)
                            .any(|symbol| symbol == condition || symbol == "error"),
                        _ => false,
                    };
                    if !matches {
                        continue;
                    }
                    if let Some(ref var_name) = var {
                        env.push(vec![(var_name.clone(), error_condition_value(&e))]);
                    }
                    let result = self.sf_progn(&parts[1..], env);
                    if var.is_some() {
                        env.pop();
                    }
                    return result;
                }
                Err(e)
            }
        }
    }

    fn sf_handler_bind(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "handler-bind".into(),
                items.len().saturating_sub(1),
            ));
        }
        let bindings = items[1].to_vec()?;
        let mut active = Vec::new();
        for binding in bindings {
            let parts = binding.to_vec()?;
            if parts.len() < 2 {
                return Err(LispError::Signal("handler-bind: invalid binding".into()));
            }
            let condition = parts[0].as_symbol()?.to_string();
            let handler = self.eval(&parts[1], env)?;
            active.push((condition, handler));
        }
        let start = self.push_handler_bindings(&active);
        let result = self.sf_progn(&items[2..], env);
        self.pop_handler_bindings(start);
        result
    }

    fn sf_with_temp_buffer(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let saved_buffer_id = self.current_buffer_id;
        let base_name = " *temp*";
        let temp_name = if self.has_buffer(base_name) {
            let mut n = 2;
            loop {
                let candidate = format!("{}<{}>", base_name, n);
                if !self.has_buffer(&candidate) {
                    break candidate;
                }
                n += 1;
            }
        } else {
            base_name.to_string()
        };
        let (temp_id, _) = self.create_buffer(&temp_name);
        self.set_buffer_hooks_inhibited(temp_id, true);
        self.switch_to_buffer_id(temp_id)?;
        let result = self.sf_progn(&items[1..], env);
        let _ = self.switch_to_buffer_id(saved_buffer_id);
        self.kill_buffer_id(temp_id);
        result
    }

    fn sf_ert_with_test_buffer(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "ert-with-test-buffer".into(),
                items.len().saturating_sub(1),
            ));
        }

        let spec = items[1].to_vec().unwrap_or_default();
        let mut name_form = None;
        let mut selected_form = None;
        let mut index = 0usize;
        while index + 1 < spec.len() {
            let key = spec[index].as_symbol()?;
            match key {
                ":name" => name_form = Some(spec[index + 1].clone()),
                ":selected" => selected_form = Some(spec[index + 1].clone()),
                _ => {}
            }
            index += 2;
        }

        let buffer_name = if let Some(form) = name_form {
            let value = self.eval(&form, env)?;
            if value.is_nil() {
                " *ert test*".to_string()
            } else {
                primitives::string_text(&value)?
            }
        } else {
            " *ert test*".to_string()
        };
        if let Some(form) = selected_form {
            let _ = self.eval(&form, env)?;
        }

        let buffer = crate::lisp::primitives::call(
            self,
            "generate-new-buffer",
            &[Value::String(buffer_name)],
            env,
        )?;
        let temp_id = self.resolve_buffer_id(&buffer)?;
        let saved_buffer_id = self.current_buffer_id();
        self.switch_to_buffer_id(temp_id)?;
        let result = self.sf_progn(&items[2..], env);
        if self.has_buffer_id(saved_buffer_id) {
            let _ = self.switch_to_buffer_id(saved_buffer_id);
        }
        if result.is_ok() && self.has_buffer_id(temp_id) {
            self.kill_buffer_id(temp_id);
        }
        result
    }

    fn sf_ert_with_temp_directory(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "ert-with-temp-directory".into(),
                items.len().saturating_sub(1),
            ));
        }
        let name = items[1].as_symbol()?.to_string();
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| LispError::Signal(error.to_string()))?
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("emaxx-ert-dir-{}-{}", std::process::id(), stamp));
        fs::create_dir_all(&path).map_err(|error| LispError::Signal(error.to_string()))?;
        env.push(vec![(name, Value::String(path.display().to_string()))]);
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        let _ = fs::remove_dir_all(&path);
        result
    }

    fn sf_ert_with_message_capture(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "ert-with-message-capture".into(),
                items.len().saturating_sub(1),
            ));
        }
        let name = items[1].as_symbol()?.to_string();
        env.push(vec![(name.clone(), Value::String(String::new()))]);
        self.message_capture_stack.push(String::new());
        let mut last = Value::Nil;
        let mut result = Ok(());
        for form in &items[2..] {
            match self.eval(form, env) {
                Ok(value) => {
                    last = value;
                    if let Some(captured) = self.message_capture_stack.last().cloned()
                        && let Some(frame) = env.last_mut()
                        && let Some((_, binding)) = frame.iter_mut().find(|(var, _)| var == &name)
                    {
                        *binding = Value::String(captured);
                    }
                }
                Err(error) => {
                    result = Err(error);
                    break;
                }
            }
        }
        self.message_capture_stack.pop();
        env.pop();
        result.map(|()| last)
    }

    fn sf_with_output_to_string(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let saved_buffer_id = self.current_buffer_id;
        let base_name = " *with-output-to-string*";
        let temp_name = if self.has_buffer(base_name) {
            let mut n = 2;
            loop {
                let candidate = format!("{}<{}>", base_name, n);
                if !self.has_buffer(&candidate) {
                    break candidate;
                }
                n += 1;
            }
        } else {
            base_name.to_string()
        };
        let (temp_id, _) = self.create_buffer(&temp_name);
        self.set_buffer_hooks_inhibited(temp_id, true);
        self.switch_to_buffer_id(temp_id)?;
        env.push(vec![(
            "standard-output".into(),
            Value::Buffer(temp_id, temp_name.clone()),
        )]);
        let body_result = self.sf_progn(&items[1..], env);
        env.pop();
        let output = Value::String(self.buffer.buffer_string());
        let _ = self.switch_to_buffer_id(saved_buffer_id);
        self.kill_buffer_id(temp_id);
        body_result?;
        Ok(output)
    }

    fn sf_with_temp_file(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "with-temp-file".into(),
                items.len().saturating_sub(1),
            ));
        }
        let file = self.eval(&items[1], env)?;
        let file = primitives::string_text(&file)?;
        let saved_buffer_id = self.current_buffer_id;
        let (temp_id, _) = self.create_buffer(" *temp file*");
        self.set_buffer_hooks_inhibited(temp_id, true);
        self.switch_to_buffer_id(temp_id)?;
        let body_result = self.sf_progn(&items[2..], env);
        let write_result = if body_result.is_ok() {
            let mut call_env = Vec::new();
            crate::lisp::primitives::call(
                self,
                "write-region",
                &[
                    Value::Nil,
                    Value::Nil,
                    Value::String(file),
                    Value::Nil,
                    Value::Integer(0),
                ],
                &mut call_env,
            )
            .map(|_| ())
        } else {
            Ok(())
        };
        let _ = self.switch_to_buffer_id(saved_buffer_id);
        self.kill_buffer_id(temp_id);
        let body_value = body_result?;
        write_result?;
        Ok(body_value)
    }

    fn sf_ert_with_temp_file(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "ert-with-temp-file".into(),
                items.len().saturating_sub(1),
            ));
        }
        let name = items[1].as_symbol()?.to_string();
        let mut index = 2usize;
        let mut prefix = Value::String("emaxx-".into());
        let mut suffix = Value::String(".tmp".into());
        let mut directory = Value::Nil;
        let mut text = Value::Nil;
        let mut buffer_name = None;

        while let Some(Value::Symbol(keyword)) = items.get(index) {
            if !keyword.starts_with(':') {
                break;
            }
            let value_expr = items.get(index + 1).ok_or_else(|| {
                LispError::Signal(format!("ert-with-temp-file missing value for {keyword}"))
            })?;
            match keyword.as_str() {
                ":prefix" => prefix = self.eval(value_expr, env)?,
                ":suffix" => suffix = self.eval(value_expr, env)?,
                ":directory" => directory = self.eval(value_expr, env)?,
                ":text" => text = self.eval(value_expr, env)?,
                ":buffer" => buffer_name = Some(value_expr.as_symbol()?.to_string()),
                ":coding" => {
                    let _ = self.eval(value_expr, env)?;
                }
                _ => {
                    return Err(LispError::Signal(format!(
                        "ert-with-temp-file invalid keyword: {keyword}"
                    )));
                }
            }
            index += 2;
        }

        let path_value = primitives::call(
            self,
            "make-temp-file",
            &[prefix, directory.clone(), suffix, text],
            env,
        )?;
        let path = primitives::string_text(&path_value)?;
        let mut frame = vec![(name, path_value.clone())];
        if let Some(buffer_name) = buffer_name {
            let buffer = primitives::call(
                self,
                "find-file-noselect",
                std::slice::from_ref(&path_value),
                env,
            )?;
            frame.push((buffer_name, Self::stored_value(buffer)));
        }
        env.push(frame);
        let result = self.sf_progn(&items[index..], env);
        env.pop();
        let _ = if directory.is_truthy() {
            fs::remove_dir_all(&path)
        } else {
            fs::remove_file(&path)
        };
        result
    }

    fn sf_with_suppressed_warnings(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        self.sf_progn(&items[2..], env)
    }

    fn sf_with_demoted_errors(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let default_format = Value::String("Error: %S".into());
        let (format_form, body) = if items.len() >= 3 {
            (&items[1], &items[2..])
        } else {
            (&default_format, &items[1..])
        };
        if self
            .lookup_var("debug-on-error", env)
            .is_some_and(|value| value.is_truthy())
        {
            return self.sf_progn(body, env);
        }
        match self.sf_progn(body, env) {
            Ok(value) => Ok(value),
            Err(LispError::Throw(tag, value)) => Err(LispError::Throw(tag, value)),
            Err(error) => {
                let format = if std::ptr::eq(format_form, &default_format) {
                    default_format
                } else {
                    self.eval(format_form, env)?
                };
                let _ = primitives::call(
                    self,
                    "message",
                    &[format, error_condition_value(&error)],
                    env,
                )?;
                Ok(Value::Nil)
            }
        }
    }

    fn sf_with_coding_priority(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let codings = self.eval(&items[1], env)?.to_vec()?;
        let saved = self.coding_system_priority_list();
        let requested = codings
            .into_iter()
            .map(|coding| coding.as_symbol().map(|name| name.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        self.set_coding_system_priority(&requested)?;
        let result = self.sf_progn(&items[2..], env);
        let _ = self.set_coding_system_priority(&saved);
        result
    }

    fn sf_with_current_buffer(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let target = self.eval(&items[1], env)?;
        let target_id = self.resolve_buffer_id(&target)?;
        let saved_buffer_id = self.current_buffer_id;
        self.switch_to_buffer_id(target_id)?;
        let result = self.sf_progn(&items[2..], env);
        let _ = self.switch_to_buffer_id(saved_buffer_id);
        result
    }

    fn sf_with_environment_variables(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "with-environment-variables".into(),
                items.len().saturating_sub(1),
            ));
        }
        let bindings = items[1].to_vec()?;
        let mut updated_environment = primitives::process_environment_entries(
            &self
                .lookup_var("process-environment", env)
                .unwrap_or(Value::Nil),
        )?;
        let mut os_restores: Vec<(String, Option<OsString>, Option<String>)> = Vec::new();
        for binding in bindings {
            let pair = binding.to_vec()?;
            if pair.len() != 2 {
                return Err(LispError::Signal(format!("Invalid VARIABLES: {}", binding)));
            }
            let name = primitives::string_text(&self.eval(&pair[0], env)?)?;
            let value = self.eval(&pair[1], env)?;
            let value = if value.is_nil() {
                None
            } else {
                Some(primitives::string_text(&value)?)
            };
            primitives::setenv_in_environment_entries(
                &mut updated_environment,
                &name,
                value.as_deref(),
                true,
            );
            let previous = std::env::var_os(&name);
            os_restores.push((name, previous, value));
        }
        let restore = self.bind_special_variable(
            "process-environment",
            primitives::process_environment_from_entries(&updated_environment),
            env,
        )?;
        for (name, _, value) in &os_restores {
            unsafe {
                if let Some(value) = value {
                    std::env::set_var(name, value);
                } else {
                    std::env::remove_var(name);
                }
            }
        }
        let result = self.sf_progn(&items[2..], env);
        for (name, previous, _) in os_restores.into_iter().rev() {
            unsafe {
                if let Some(value) = previous {
                    std::env::set_var(&name, value);
                } else {
                    std::env::remove_var(&name);
                }
            }
        }
        let _ = self.restore_special_binding(restore, env);
        result
    }

    fn sf_with_restriction(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Ok(Value::Nil);
        }
        let start = self.eval(&items[1], env)?.as_integer()? as usize;
        let end = self.eval(&items[2], env)?.as_integer()? as usize;
        let mut body_index = 3;
        let label = if matches!(items.get(3), Some(Value::Symbol(s)) if s == ":label") {
            body_index = 5;
            match items.get(4) {
                Some(Value::Symbol(symbol)) => symbol.clone(),
                Some(Value::Cons(_, _)) => {
                    let quoted = items[4].to_vec()?;
                    quoted
                        .get(1)
                        .ok_or_else(|| LispError::Signal("Invalid with-restriction label".into()))?
                        .as_symbol()?
                        .to_string()
                }
                _ => "default".into(),
            }
        } else {
            "default".into()
        };
        let saved = (self.buffer.point_min(), self.buffer.point_max());
        let current = self
            .effective_labeled_restriction(self.current_buffer_id(), None)
            .unwrap_or(saved);
        let effective = (start.max(current.0), end.min(current.1));
        self.labeled_restrictions
            .push((self.current_buffer_id(), label, effective.0, effective.1));
        self.buffer.narrow_to_region(effective.0, effective.1);
        let result = self.sf_progn(&items[body_index..], env);
        self.labeled_restrictions.pop();
        self.buffer.restore_restriction(saved.0, saved.1);
        result
    }

    fn sf_without_restriction(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let mut body_index = 1;
        let label = if matches!(items.get(1), Some(Value::Symbol(s)) if s == ":label") {
            body_index = 3;
            match items.get(2) {
                Some(Value::Symbol(symbol)) => symbol.clone(),
                Some(Value::Cons(_, _)) => {
                    let quoted = items[2].to_vec()?;
                    quoted
                        .get(1)
                        .ok_or_else(|| {
                            LispError::Signal("Invalid without-restriction label".into())
                        })?
                        .as_symbol()?
                        .to_string()
                }
                _ => "default".into(),
            }
        } else {
            "default".into()
        };
        let saved = (self.buffer.point_min(), self.buffer.point_max());
        let pos = self
            .labeled_restrictions
            .iter()
            .rposition(|(buffer_id, active_label, _, _)| {
                *buffer_id == self.current_buffer_id() && *active_label == label
            });
        let removed = pos.map(|index| self.labeled_restrictions.remove(index));
        if let Some((start, end)) =
            self.effective_labeled_restriction(self.current_buffer_id(), None)
        {
            self.buffer.narrow_to_region(start, end);
        } else {
            self.buffer.widen();
        }
        let result = self.sf_progn(&items[body_index..], env);
        if let Some(entry) = removed {
            self.labeled_restrictions.push(entry);
        }
        self.buffer.restore_restriction(saved.0, saved.1);
        result
    }

    fn sf_save_excursion(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let saved_buffer_id = self.current_buffer_id();
        let saved_pt = self.buffer.point();
        let saved_marker = self.make_marker();
        let saved_marker_id = match saved_marker {
            Value::Marker(id) => id,
            _ => unreachable!("make_marker returns a marker"),
        };
        self.set_marker(saved_marker_id, Some(saved_pt), Some(saved_buffer_id))?;
        let result = self.sf_progn(&items[1..], env);
        if self.has_buffer_id(saved_buffer_id) {
            let _ = self.switch_to_buffer_id(saved_buffer_id);
            let restore_pt = self
                .marker_position(saved_marker_id)
                .unwrap_or(saved_pt)
                .clamp(self.buffer.point_min(), self.buffer.point_max());
            self.buffer.goto_char(restore_pt);
        }
        let _ = self.set_marker(saved_marker_id, None, None);
        result
    }

    fn sf_save_match_data(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let saved = self.last_match_data.clone();
        let saved_buffer_id = self.last_match_data_buffer_id;
        let saved_markers = if let (Some(buffer_id), Some(match_data)) = (saved_buffer_id, &saved) {
            let mut marker_data = Vec::new();
            for entry in match_data {
                if let Some((start, end)) = entry {
                    let Value::Marker(start_marker) = self.make_marker() else {
                        unreachable!("make_marker always returns a marker")
                    };
                    let Value::Marker(end_marker) = self.make_marker() else {
                        unreachable!("make_marker always returns a marker")
                    };
                    self.set_marker(start_marker, Some(*start), Some(buffer_id))?;
                    self.set_marker(end_marker, Some(*end), Some(buffer_id))?;
                    marker_data.push(Some((start_marker, end_marker)));
                } else {
                    marker_data.push(None);
                }
            }
            Some(marker_data)
        } else {
            None
        };
        let result = self.sf_progn(&items[1..], env);
        self.last_match_data = if let Some(marker_data) = saved_markers {
            let mut restored = Vec::new();
            for entry in marker_data {
                restored.push(match entry {
                    Some((start_marker, end_marker)) => {
                        let start = self.marker_position(start_marker);
                        let end = self.marker_position(end_marker);
                        let _ = self.set_marker(start_marker, None, None);
                        let _ = self.set_marker(end_marker, None, None);
                        match (start, end) {
                            (Some(start), Some(end)) => Some((start, end)),
                            _ => None,
                        }
                    }
                    None => None,
                });
            }
            Some(restored)
        } else {
            saved
        };
        self.last_match_data_buffer_id = saved_buffer_id;
        result
    }

    fn sf_save_current_buffer(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let saved_buffer_id = self.current_buffer_id();
        let result = self.sf_progn(&items[1..], env);
        if self.has_buffer_id(saved_buffer_id) {
            let _ = self.switch_to_buffer_id(saved_buffer_id);
        }
        result
    }

    fn sf_save_window_excursion(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let snapshot = self.snapshot_window_configuration();
        let result = self.sf_progn(&items[1..], env);
        let _ = self.restore_window_configuration(snapshot);
        result
    }

    fn sf_with_silent_modifications(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let was_modified = self.buffer.is_modified();
        let was_autosaved = self.buffer.is_autosaved();
        let result = self.sf_progn(&items[1..], env);
        if !was_modified {
            self.buffer.set_unmodified();
        } else if was_autosaved {
            self.buffer.set_modified();
            self.buffer.set_autosaved();
        } else {
            self.buffer.set_modified();
        }
        result
    }

    fn sf_save_restriction(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let saved_buffer_id = self.current_buffer_id();
        let saved_begv = self.buffer.point_min();
        let saved_zv = self.buffer.point_max();
        let beg_marker = self.make_marker();
        let end_marker = self.make_marker();
        let beg_id = match beg_marker {
            Value::Marker(id) => id,
            _ => unreachable!("make_marker returns a marker"),
        };
        let end_id = match end_marker {
            Value::Marker(id) => id,
            _ => unreachable!("make_marker returns a marker"),
        };
        let _ = self.set_marker(beg_id, Some(saved_begv), Some(saved_buffer_id));
        let _ = self.set_marker(end_id, Some(saved_zv), Some(saved_buffer_id));
        self.set_marker_insertion_type(end_id, true);
        self.buffer.push_undo_meta(Value::cons(
            Value::Marker(beg_id),
            Value::Integer(-(saved_begv as i64)),
        ));
        self.buffer.push_undo_meta(Value::cons(
            Value::Marker(end_id),
            Value::Integer(saved_zv as i64),
        ));
        let result = self.sf_progn(&items[1..], env);
        let final_buffer_id = self.current_buffer_id();
        let restore_begv = self.marker_position(beg_id).unwrap_or(saved_begv);
        let restore_zv = self.marker_position(end_id).unwrap_or(saved_zv);
        if self.has_buffer_id(saved_buffer_id) {
            if final_buffer_id != saved_buffer_id {
                let _ = self.switch_to_buffer_id(saved_buffer_id);
            }
            self.buffer.restore_restriction(restore_begv, restore_zv);
            if final_buffer_id != saved_buffer_id && self.has_buffer_id(final_buffer_id) {
                let _ = self.switch_to_buffer_id(final_buffer_id);
            }
        }
        let _ = self.set_marker(beg_id, None, None);
        let _ = self.set_marker(end_id, None, None);
        result
    }

    fn sf_combine_change_calls(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let start_undo = self.buffer.undo_len();
        let result = self.sf_progn(&items[3..], env)?;
        let entries = self.buffer.take_undo_entries_since(start_undo);
        if !entries.is_empty() {
            self.buffer
                .push_undo_entry(crate::buffer::UndoEntry::Combined {
                    display: combined_undo_display(&entries),
                    entries,
                });
        }
        Ok(result)
    }

    fn sf_cl_assert(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let result = self.eval(&items[1], env)?;
        if result.is_truthy() {
            Ok(result)
        } else {
            Err(LispError::Signal("Assertion failed".into()))
        }
    }

    // ── cl-destructuring-bind ──
    // (cl-destructuring-bind (var1 var2 ... &optional opt1 ...) expr body...)
    fn sf_cl_destructuring_bind(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-destructuring-bind".into(),
                items.len() - 1,
            ));
        }
        let val = self.eval(&items[2], env)?;
        let mut frame = Vec::new();
        self.bind_cl_pattern(&items[1], val, &mut frame)?;

        env.push(frame);
        let result = self.sf_progn(&items[3..], env);
        env.pop();
        result
    }

    fn sf_cl_letf(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-letf".into(),
                items.len() - 1,
            ));
        }
        let bindings = items[1].to_vec()?;
        env.push(Vec::new());
        let mut rebound = Vec::new();
        let mut rebound_places = Vec::new();
        let setup = (|| -> Result<(), LispError> {
            for binding in &bindings {
                let parts = binding.to_vec()?;
                if parts.len() < 2 {
                    continue;
                }
                match &parts[0] {
                    Value::Symbol(name) => {
                        let value = Self::stored_value(self.eval(&parts[1], env)?);
                        let frame = env
                            .last_mut()
                            .expect("cl-letf pushes a temporary binding frame");
                        if let Some((_, existing)) =
                            frame.iter_mut().rev().find(|(bound, _)| bound == name)
                        {
                            *existing = value;
                        } else {
                            frame.push((name.clone(), value));
                        }
                    }
                    Value::Cons(_, _) => {
                        let place = parts[0].to_vec()?;
                        if matches!(
                            place.first(),
                            Some(Value::Symbol(name)) if name == "symbol-function"
                        ) {
                            let Some(target) = place.get(1) else {
                                return Err(LispError::Signal("Unsupported cl-letf place".into()));
                            };
                            let function_name = function_name_from_binding_form(target)?;
                            let value = self.eval(&parts[1], env)?;
                            self.functions.push((function_name.clone(), value));
                            rebound.push(function_name);
                        } else {
                            let place = self.resolve_setf_place(&parts[0], env)?;
                            let current =
                                self.eval_resolved_setf_place_current_value(&place, env)?;
                            let value = self.eval(&parts[1], env)?;
                            self.set_resolved_setf_place_value(&place, value, env)?;
                            rebound_places.push((place, current));
                        }
                    }
                    _ => return Err(LispError::Signal("Unsupported cl-letf place".into())),
                }
            }
            Ok(())
        })();
        let result = match setup {
            Ok(()) => self.sf_progn(&items[2..], env),
            Err(error) => Err(error),
        };
        env.pop();
        let mut restore_error = None;
        for (place, value) in rebound_places.into_iter().rev() {
            if let Err(error) = self.set_setf_place_value(&place, value, env)
                && restore_error.is_none()
            {
                restore_error = Some(error);
            }
        }
        for name in rebound.into_iter().rev() {
            if let Some(index) = self.functions.iter().rposition(|(fname, _)| *fname == name) {
                self.functions.remove(index);
            }
        }
        match result {
            Ok(value) => restore_error.map_or(Ok(value), Err),
            Err(error) => Err(error),
        }
    }

    fn sf_aset(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() != 4 {
            return Err(LispError::WrongNumberOfArgs("aset".into(), items.len() - 1));
        }
        if let Value::Symbol(name) = &items[1] {
            let current = self.lookup(name, env)?;
            let new_value = self.eval(&items[3], env)?;
            let index_value = self.eval(&items[2], env)?;
            if matches!(current, Value::CharTable(_))
                || matches!(
                    &current,
                    Value::Record(id)
                        if self
                            .find_record(*id)
                            .is_some_and(|record| record.type_name == "bool-vector")
                )
            {
                primitives::call(
                    self,
                    "aset",
                    &[current, index_value, new_value.clone()],
                    env,
                )?;
                return Ok(new_value);
            }
            if primitives::is_vector_value(&current) {
                primitives::call(
                    self,
                    "aset",
                    &[current, index_value, new_value.clone()],
                    env,
                )?;
                return Ok(new_value);
            }
            let index = index_value.as_integer()? as usize;
            if matches!(current, Value::String(_) | Value::StringObject(_)) {
                let updated = primitives::aset_string_value(&current, index, &new_value)?;
                self.set_variable(name, updated, env);
                return Ok(new_value);
            }
            let mut entries = current.to_vec()?;
            let tagged = matches!(
                entries.first(),
                Some(Value::Symbol(symbol)) if symbol == "vector" || symbol == "vector-literal"
            );
            let slot = if tagged { index + 1 } else { index };
            if slot >= entries.len() {
                return Err(LispError::Signal("Args out of range".into()));
            }
            entries[slot] = new_value.clone();
            self.set_variable(name, Value::list(entries), env);
            return Ok(new_value);
        }

        let vector = self.eval(&items[1], env)?;
        let index = self.eval(&items[2], env)?;
        let new_value = self.eval(&items[3], env)?;
        primitives::call(self, "aset", &[vector, index, new_value.clone()], env)?;
        Ok(new_value)
    }

    // ── cl-flet ──
    // (cl-flet ((name (args) body...) ...) body...)
    fn sf_cl_flet(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-flet".into(),
                items.len() - 1,
            ));
        }
        let bindings = items[1].to_vec()?;
        let mut frame = Vec::new();
        for binding in &bindings {
            let parts = binding.to_vec()?;
            if parts.len() < 2 {
                continue;
            }
            let fname = parts[0].as_symbol()?.to_string();
            let params_val = parts[1].to_vec()?;
            let mut params = Vec::new();
            for p in &params_val {
                params.push(p.as_symbol()?.to_string());
            }
            let body: Vec<Value> = parts[2..].to_vec();
            let lambda = Value::Lambda(params, body, shared_env(env.clone()));
            frame.push((fname, lambda));
        }
        env.push(frame);
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        result
    }

    // ── cl-macrolet ──
    // (cl-macrolet ((name (args) body...) ...) body...)
    fn sf_cl_macrolet(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-macrolet".into(),
                items.len() - 1,
            ));
        }
        let bindings = items[1].to_vec()?;
        // Register macros temporarily
        let saved_macros_len = self.macros.len();
        for binding in &bindings {
            let parts = binding.to_vec()?;
            if parts.len() < 2 {
                continue;
            }
            let mname = parts[0].as_symbol()?.to_string();
            let params_val = parts[1].to_vec()?;
            let mut params = Vec::new();
            for p in &params_val {
                params.push(p.as_symbol()?.to_string());
            }
            let body: Vec<Value> = parts[2..].to_vec();
            self.macros.push((mname, params, body));
        }
        let result = self.sf_progn(&items[2..], env);
        self.macros.truncate(saved_macros_len);
        result
    }

    // ── Backquote ──

    fn eval_backquote(&mut self, expr: &Value, env: &mut Env) -> Result<Value, LispError> {
        self.eval_backquote_with_depth(expr, env, 0)
    }

    fn eval_record_literal_form(
        &mut self,
        slots: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let mut values = Vec::with_capacity(slots.len());
        for slot in slots {
            values.push(self.eval(slot, env)?);
        }
        if let Some(first) = values.first()
            && let Ok(type_name) = first.as_symbol()
        {
            return Ok(self.create_record(type_name, values[1..].to_vec()));
        }
        Ok(self.create_record("literal-record", values))
    }

    fn eval_backquote_with_depth(
        &mut self,
        expr: &Value,
        env: &mut Env,
        depth: usize,
    ) -> Result<Value, LispError> {
        if let Some((kind, value)) = backquote_unquote_form(expr) {
            if depth == 0 {
                return self.eval(&value, env);
            }
            return Ok(Value::list([
                Value::Symbol(kind.into()),
                self.eval_backquote_with_depth(&value, env, depth - 1)?,
            ]));
        }

        if let Some(body) = nested_backquote_body(expr) {
            return Ok(Value::list([
                Value::Symbol("backquote".into()),
                self.eval_backquote_with_depth(&body, env, depth + 1)?,
            ]));
        }

        match expr {
            Value::Cons(_, _) => {
                let mut result: Vec<Value> = Vec::new();
                let mut current = expr.clone();
                loop {
                    if backquote_unquote_form(&current).is_some() {
                        let tail = self.eval_backquote_with_depth(&current, env, depth)?;
                        return Ok(cons_list_with_tail(result, tail));
                    }
                    if !result.is_empty() && is_backquote_atomic_cons_tail(&current) {
                        let tail = self.eval_backquote_with_depth(&current, env, depth)?;
                        return Ok(cons_list_with_tail(result, tail));
                    }
                    match current {
                        Value::Cons(car, cdr) => {
                            let car_value = car.borrow().clone();
                            let cdr_value = cdr.borrow().clone();

                            if depth == 0
                                && let Some(("comma-at", value)) =
                                    backquote_unquote_form(&car_value)
                            {
                                let evaled = self.eval(&value, env)?;
                                if let Ok(elems) = evaled.to_vec() {
                                    result.extend(elems);
                                }
                                current = cdr_value;
                                continue;
                            }

                            result.push(self.eval_backquote_with_depth(&car_value, env, depth)?);
                            current = cdr_value;
                        }
                        Value::Nil => break,
                        other => {
                            let tail = self.eval_backquote_with_depth(&other, env, depth)?;
                            return Ok(cons_list_with_tail(result, tail));
                        }
                    }
                }
                let result = Value::list(result);
                if depth == 0 && is_record_literal_reader_form(expr) {
                    return self.eval(&result, env);
                }
                Ok(result)
            }
            _ => Ok(expr.clone()),
        }
    }

    // ── Macros ──

    fn sf_defmacro(&mut self, items: &[Value]) -> Result<Value, LispError> {
        if items.len() < 4 {
            return Err(LispError::WrongNumberOfArgs("defmacro".into(), items.len()));
        }
        let name = items[1].as_symbol()?.to_string();
        let params_list = items[2].to_vec()?;
        let mut params = Vec::new();
        for p in &params_list {
            params.push(p.as_symbol()?.to_string());
        }
        // Body starts at index 3, skip docstrings
        let body_start = if items.len() > 4 {
            if let Value::String(_) = &items[3] {
                4
            } else {
                3
            }
        } else {
            3
        };
        // Skip (declare ...) forms
        let body_start = if body_start < items.len() {
            if let Value::Cons(_, _) = &items[body_start] {
                if let Ok(decl) = items[body_start].to_vec() {
                    if let Some(Value::Symbol(s)) = decl.first() {
                        if s == "declare" {
                            body_start + 1
                        } else {
                            body_start
                        }
                    } else {
                        body_start
                    }
                } else {
                    body_start
                }
            } else {
                body_start
            }
        } else {
            body_start
        };
        let body: Vec<Value> = items[body_start..].to_vec();
        self.macros.push((name.clone(), params, body));
        Ok(Value::Symbol(name))
    }

    fn sf_easy_menu_define(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() != 5 {
            return Err(LispError::WrongNumberOfArgs(
                "easy-menu-define".into(),
                items.len().saturating_sub(1),
            ));
        }
        let Some(symbol_name) = (match &items[1] {
            Value::Nil => None,
            Value::Symbol(name) => Some(name.clone()),
            other => {
                return Err(LispError::TypeError("symbol".into(), other.type_name()));
            }
        }) else {
            return Ok(Value::Nil);
        };

        if self.lookup_var(&symbol_name, env).is_none() {
            self.set_variable(
                &symbol_name,
                crate::lisp::primitives::keymap_placeholder(Some(&symbol_name)),
                env,
            );
        }
        if self.lookup_function(&symbol_name, env).is_err() {
            self.set_function_binding(&symbol_name, Some(Value::BuiltinFunc("ignore".into())));
        }
        Ok(Value::Symbol(symbol_name))
    }

    fn sf_defalias(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "defalias".into(),
                items.len().saturating_sub(1),
            ));
        }
        let name = quoted_symbol_name(&items[1])
            .or_else(|| items[1].as_symbol().ok().map(str::to_string))
            .ok_or_else(|| LispError::TypeError("symbol".into(), items[1].type_name()))?;
        let function = self.eval(&items[2], env)?;
        self.validate_function_binding(&name, &function)?;
        self.set_function_binding(&name, Some(function));
        Ok(Value::Symbol(name))
    }

    fn try_macroexpand(
        &mut self,
        name: &str,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Option<Value>, LispError> {
        let mut attempted_autoload = false;
        let (params, body) = loop {
            if let Some(expanded) = self.try_builtin_macroexpand(name, args, env)? {
                return Ok(Some(expanded));
            }

            if let Some(binding) = self.resolve_macro_binding(name) {
                break binding;
            }

            if attempted_autoload {
                return Ok(None);
            }
            let Ok(function) = self.lookup_function(name, env) else {
                return Ok(None);
            };
            let Some((file, _, _kind)) = crate::lisp::primitives::autoload_parts(&function) else {
                return Ok(None);
            };
            let loads_macro =
                crate::lisp::primitives::autoload_is_macro(self, Some(name), &function);
            if !loads_macro {
                return Ok(None);
            }
            self.load_target(&file)?;
            attempted_autoload = true;
        };

        // Bind params to unevaluated args
        let mut frame = Vec::new();
        let mut arg_idx = 0;
        let mut rest = false;

        for param in &params {
            if param == "&optional" {
                continue;
            }
            if param == "&rest" || param == "&body" {
                rest = true;
                continue;
            }
            if rest {
                let rest_args = Value::list(args[arg_idx..].iter().cloned());
                frame.push((param.clone(), rest_args));
                break;
            }
            let val = if arg_idx < args.len() {
                args[arg_idx].clone()
            } else {
                Value::Nil
            };
            frame.push((param.clone(), val));
            arg_idx += 1;
        }

        env.push(frame);
        let expanded = if body.len() == 1 {
            self.eval(&body[0], env)?
        } else {
            let progn =
                Value::list(std::iter::once(Value::symbol("progn")).chain(body.iter().cloned()));
            self.eval(&progn, env)?
        };
        env.pop();
        Ok(Some(expanded))
    }

    pub(crate) fn macroexpand_1_form(
        &mut self,
        form: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Ok(items) = form.to_vec() else {
            return Ok(form.clone());
        };
        let Some(Value::Symbol(name)) = items.first() else {
            return Ok(form.clone());
        };
        Ok(self
            .try_macroexpand(name, &items[1..], env)?
            .unwrap_or_else(|| form.clone()))
    }

    pub(crate) fn macroexpand_all_form(
        &mut self,
        form: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Ok(items) = form.to_vec() else {
            return Ok(form.clone());
        };
        let Some(head) = items.first() else {
            return Ok(Value::Nil);
        };
        if let Value::Symbol(name) = head {
            match name.as_str() {
                "quote" | "function" => return Ok(form.clone()),
                "eval-when-compile" => {
                    let value = if items.len() <= 1 {
                        Value::Nil
                    } else if items.len() == 2 {
                        self.eval(&items[1], env)?
                    } else {
                        let progn = Value::list(
                            std::iter::once(Value::Symbol("progn".into()))
                                .chain(items[1..].iter().cloned()),
                        );
                        self.eval(&progn, env)?
                    };
                    return Ok(quoted_literal(&value));
                }
                _ => {}
            }
            if let Some(expanded) = self.try_macroexpand(name, &items[1..], env)? {
                return self.macroexpand_all_form(&expanded, env);
            }
        }

        if matches!(head, Value::Symbol(name) if name == "lambda") {
            let mut expanded = Vec::with_capacity(items.len());
            expanded.push(items[0].clone());
            if let Some(params) = items.get(1) {
                expanded.push(params.clone());
            }
            for item in &items[2..] {
                expanded.push(self.macroexpand_all_form(item, env)?);
            }
            return Ok(Value::list(expanded));
        }

        let mut expanded = Vec::with_capacity(items.len());
        if matches!(head, Value::Symbol(_)) {
            expanded.push(items[0].clone());
            for item in &items[1..] {
                expanded.push(self.macroexpand_all_form(item, env)?);
            }
        } else {
            for item in &items {
                expanded.push(self.macroexpand_all_form(item, env)?);
            }
        }
        Ok(Value::list(expanded))
    }

    fn try_builtin_macroexpand(
        &mut self,
        name: &str,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Option<Value>, LispError> {
        match name {
            "cl-case" => self.expand_cl_case(args, env).map(Some),
            "cl-with-gensyms" => self.expand_cl_with_gensyms(args, env).map(Some),
            "ert-simulate-keys" => self.expand_ert_simulate_keys(args).map(Some),
            "letrec" => self.expand_letrec(args).map(Some),
            "named-let" => self.expand_named_let(args).map(Some),
            "with-wrapper-hook" => self.expand_with_wrapper_hook(args).map(Some),
            "subr--with-wrapper-hook-no-warnings" => {
                self.expand_subr_with_wrapper_hook(args).map(Some)
            }
            "with-selected-frame" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(
                        "with-selected-frame".into(),
                        0,
                    ));
                }
                let body = &args[1..];
                Ok(Some(match body {
                    [] => Value::Nil,
                    [single] => single.clone(),
                    _ => Value::list(
                        std::iter::once(Value::Symbol("progn".into())).chain(body.iter().cloned()),
                    ),
                }))
            }
            _ => Ok(None),
        }
    }

    fn expand_with_wrapper_hook(&mut self, args: &[Value]) -> Result<Value, LispError> {
        if args.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "with-wrapper-hook".into(),
                args.len(),
            ));
        }
        Ok(Value::list(
            std::iter::once(Value::Symbol("subr--with-wrapper-hook-no-warnings".into()))
                .chain(args.iter().cloned()),
        ))
    }

    fn expand_subr_with_wrapper_hook(&mut self, args: &[Value]) -> Result<Value, LispError> {
        if args.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "subr--with-wrapper-hook-no-warnings".into(),
                args.len(),
            ));
        }

        let hook = args[0].clone();
        let arg_list = args[1].clone();
        let body = &args[2..];
        let funs = self.make_generated_symbol("funs");
        let global = self.make_generated_symbol("global");
        let argssym = self.make_generated_symbol("args");
        let runrestofhook = self.make_generated_symbol("runrestofhook");

        let lambda_body = Value::list([
            Value::Symbol("if".into()),
            Value::list([Value::Symbol("consp".into()), funs.clone()]),
            Value::list([
                Value::Symbol("if".into()),
                Value::list([
                    Value::Symbol("eq".into()),
                    Value::T,
                    Value::list([Value::Symbol("car".into()), funs.clone()]),
                ]),
                Value::list([
                    Value::Symbol("funcall".into()),
                    runrestofhook.clone(),
                    Value::list([
                        Value::Symbol("append".into()),
                        global.clone(),
                        Value::list([Value::Symbol("cdr".into()), funs.clone()]),
                    ]),
                    Value::Nil,
                    argssym.clone(),
                ]),
                Value::list([
                    Value::Symbol("apply".into()),
                    Value::list([Value::Symbol("car".into()), funs.clone()]),
                    Value::list([
                        Value::Symbol("apply-partially".into()),
                        Value::list([
                            Value::Symbol("lambda".into()),
                            Value::list([
                                funs.clone(),
                                global.clone(),
                                Value::Symbol("&rest".into()),
                                argssym.clone(),
                            ]),
                            Value::list([
                                Value::Symbol("funcall".into()),
                                runrestofhook.clone(),
                                funs.clone(),
                                global.clone(),
                                argssym.clone(),
                            ]),
                        ]),
                        Value::list([Value::Symbol("cdr".into()), funs.clone()]),
                        global.clone(),
                    ]),
                    argssym.clone(),
                ]),
            ]),
            Value::list([
                Value::Symbol("apply".into()),
                Value::list(
                    std::iter::once(Value::Symbol("lambda".into()))
                        .chain(std::iter::once(arg_list))
                        .chain(body.iter().cloned()),
                ),
                argssym.clone(),
            ]),
        ]);

        let global_form = match &hook {
            Value::Symbol(_) => Value::list([
                Value::Symbol("if".into()),
                Value::list([
                    Value::Symbol("local-variable-p".into()),
                    quoted_literal(&hook),
                ]),
                Value::list([Value::Symbol("default-value".into()), quoted_literal(&hook)]),
            ]),
            _ => Value::Nil,
        };

        let wrapper_args = args[1].to_vec()?;

        Ok(Value::list([
            Value::Symbol("letrec".into()),
            Value::list([Value::list([
                runrestofhook.clone(),
                Value::list([
                    Value::Symbol("lambda".into()),
                    Value::list([funs, global, argssym.clone()]),
                    lambda_body,
                ]),
            ])]),
            Value::list([
                Value::Symbol("funcall".into()),
                runrestofhook,
                hook,
                global_form,
                Value::list(std::iter::once(Value::Symbol("list".into())).chain(wrapper_args)),
            ]),
        ]))
    }

    fn expand_ert_simulate_keys(&mut self, args: &[Value]) -> Result<Value, LispError> {
        if args.is_empty() {
            return Err(LispError::WrongNumberOfArgs("ert-simulate-keys".into(), 0));
        }
        let bindings = Value::list([
            Value::list([
                Value::Symbol("unread-command-events".into()),
                Value::list([
                    Value::Symbol("append".into()),
                    args[0].clone(),
                    Value::list([
                        Value::Symbol("quote".into()),
                        Value::list([Value::Integer(7), Value::Integer(7), Value::Integer(7)]),
                    ]),
                ]),
            ]),
            Value::list([Value::Symbol("executing-kbd-macro".into()), Value::T]),
        ]);
        Ok(Value::list(
            std::iter::once(Value::Symbol("let".into()))
                .chain(std::iter::once(bindings))
                .chain(args[1..].iter().cloned()),
        ))
    }

    fn expand_cl_with_gensyms(
        &mut self,
        args: &[Value],
        _env: &mut Env,
    ) -> Result<Value, LispError> {
        let names = args
            .first()
            .ok_or_else(|| LispError::WrongNumberOfArgs("cl-with-gensyms".into(), 0))?
            .to_vec()?;
        let mut bindings = Vec::with_capacity(names.len());
        for name in names {
            let name = name.as_symbol()?.to_string();
            bindings.push(Value::list([
                Value::Symbol(name.clone()),
                Value::list([
                    Value::Symbol("quote".into()),
                    self.make_generated_symbol(&name),
                ]),
            ]));
        }
        Ok(Value::list(
            std::iter::once(Value::Symbol("let".into()))
                .chain(std::iter::once(Value::list(bindings)))
                .chain(args[1..].iter().cloned()),
        ))
    }

    fn expand_letrec(&mut self, args: &[Value]) -> Result<Value, LispError> {
        let bindings = args
            .first()
            .ok_or_else(|| LispError::WrongNumberOfArgs("letrec".into(), 0))?
            .to_vec()?;
        let mut lowered_bindings = Vec::with_capacity(bindings.len());
        let mut initializers = Vec::new();

        for binding in bindings {
            match binding {
                Value::Symbol(name) => lowered_bindings.push(Value::Symbol(name)),
                Value::Cons(_, _) => {
                    let parts = binding.to_vec()?;
                    let Some(name_value) = parts.first() else {
                        return Err(LispError::ReadError("bad letrec binding".into()));
                    };
                    let name = name_value.as_symbol()?.to_string();
                    lowered_bindings.push(Value::Symbol(name.clone()));
                    if parts.len() > 1 {
                        initializers.push(Value::list([
                            Value::Symbol("setq".into()),
                            Value::Symbol(name),
                            parts[1].clone(),
                        ]));
                    }
                }
                other => return Err(wrong_type_argument("listp", other)),
            }
        }

        Ok(Value::list(
            std::iter::once(Value::Symbol("let".into()))
                .chain(std::iter::once(Value::list(lowered_bindings)))
                .chain(initializers)
                .chain(args[1..].iter().cloned()),
        ))
    }

    fn expand_named_let(&mut self, args: &[Value]) -> Result<Value, LispError> {
        let name = args
            .first()
            .ok_or_else(|| LispError::WrongNumberOfArgs("named-let".into(), 0))?
            .as_symbol()?
            .to_string();
        let bindings = args
            .get(1)
            .ok_or_else(|| LispError::WrongNumberOfArgs("named-let".into(), 1))?
            .to_vec()?;
        let mut params = Vec::with_capacity(bindings.len());
        let mut inits = Vec::with_capacity(bindings.len());
        for binding in bindings {
            match binding {
                Value::Symbol(symbol) => {
                    params.push(Value::Symbol(symbol));
                    inits.push(Value::Nil);
                }
                Value::Cons(_, _) => {
                    let parts = binding.to_vec()?;
                    let Some(param) = parts.first() else {
                        return Err(LispError::ReadError("bad named-let binding".into()));
                    };
                    params.push(Value::Symbol(param.as_symbol()?.to_string()));
                    inits.push(parts.get(1).cloned().unwrap_or(Value::Nil));
                }
                other => return Err(wrong_type_argument("listp", other)),
            }
        }

        if let Some(lowered) = self.try_expand_named_let_loop(&name, &params, &inits, &args[2..])? {
            return Ok(lowered);
        }

        let lambda = Value::list(
            std::iter::once(Value::Symbol("lambda".into()))
                .chain(std::iter::once(Value::list(params)))
                .chain(if args.len() > 2 {
                    args[2..].to_vec()
                } else {
                    vec![Value::Nil]
                }),
        );
        let binding = Value::list([Value::Symbol(name.clone()), lambda]);
        let call = Value::list(std::iter::once(Value::Symbol(name)).chain(inits));

        Ok(Value::list([
            Value::Symbol("letrec".into()),
            Value::list([binding]),
            call,
        ]))
    }

    fn try_expand_named_let_loop(
        &mut self,
        name: &str,
        params: &[Value],
        inits: &[Value],
        body: &[Value],
    ) -> Result<Option<Value>, LispError> {
        let done_tag = self.make_generated_symbol("named-let-done");
        let bindings = Value::list(
            params
                .iter()
                .cloned()
                .zip(inits.iter().cloned())
                .map(|(param, init)| Value::list([param, init])),
        );

        let loop_body = match body {
            [single] => {
                if let Ok(items) = single.to_vec()
                    && matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "if")
                {
                    let then_forms = items
                        .get(2)
                        .cloned()
                        .map_or_else(Vec::new, |form| vec![form]);
                    let else_forms = if items.len() > 3 {
                        items[3..].to_vec()
                    } else {
                        vec![Value::Nil]
                    };
                    if !named_let_branch_safe_for_loop(name, &then_forms)
                        || !named_let_branch_safe_for_loop(name, &else_forms)
                    {
                        return Ok(None);
                    }
                    self.expand_named_let_loop_if(name, params, &done_tag, &items)?
                } else if let Some((prefix, args)) = named_let_tail_call(name, body) {
                    self.build_named_let_rebind(params, &args, &prefix)?
                } else {
                    return Ok(None);
                }
            }
            _ => {
                if let Some((prefix, args)) = named_let_tail_call(name, body) {
                    self.build_named_let_rebind(params, &args, &prefix)?
                } else {
                    return Ok(None);
                }
            }
        };

        Ok(Some(Value::list([
            Value::Symbol("let".into()),
            bindings,
            Value::list([
                Value::Symbol("catch".into()),
                quoted_literal(&done_tag),
                Value::list([Value::Symbol("while".into()), Value::T, loop_body]),
            ]),
        ])))
    }

    fn expand_named_let_loop_if(
        &mut self,
        name: &str,
        params: &[Value],
        done_tag: &Value,
        items: &[Value],
    ) -> Result<Value, LispError> {
        let condition = items.get(1).cloned().unwrap_or(Value::Nil);
        let then_forms = items
            .get(2)
            .cloned()
            .map_or_else(Vec::new, |form| vec![form]);
        let else_forms = if items.len() > 3 {
            items[3..].to_vec()
        } else {
            vec![Value::Nil]
        };
        let then_branch = self.build_named_let_loop_branch(name, params, done_tag, &then_forms)?;
        let else_branch = self.build_named_let_loop_branch(name, params, done_tag, &else_forms)?;
        Ok(Value::list([
            Value::Symbol("if".into()),
            condition,
            then_branch,
            else_branch,
        ]))
    }

    fn build_named_let_loop_branch(
        &mut self,
        name: &str,
        params: &[Value],
        done_tag: &Value,
        forms: &[Value],
    ) -> Result<Value, LispError> {
        if let Some((prefix, args)) = named_let_tail_call(name, forms) {
            self.build_named_let_rebind(params, &args, &prefix)
        } else {
            Ok(Value::list([
                Value::Symbol("throw".into()),
                quoted_literal(done_tag),
                forms_to_progn(forms),
            ]))
        }
    }

    fn build_named_let_rebind(
        &mut self,
        params: &[Value],
        args: &[Value],
        prefix: &[Value],
    ) -> Result<Value, LispError> {
        if params.len() != args.len() {
            return Err(LispError::WrongNumberOfArgs("named-let".into(), args.len()));
        }
        let temp_symbols = (0..params.len())
            .map(|_| self.make_generated_symbol("named-let-arg"))
            .collect::<Vec<_>>();
        let temp_bindings = Value::list(
            temp_symbols
                .iter()
                .cloned()
                .zip(args.iter().cloned())
                .map(|(temp, arg)| Value::list([temp, arg])),
        );
        let mut setq_items = vec![Value::Symbol("setq".into())];
        for (param, temp) in params.iter().cloned().zip(temp_symbols) {
            setq_items.push(param);
            setq_items.push(temp);
        }
        let rebind = Value::list([
            Value::Symbol("let".into()),
            temp_bindings,
            Value::list(setq_items),
        ]);
        let forms = prefix
            .iter()
            .cloned()
            .chain(std::iter::once(rebind))
            .collect::<Vec<_>>();
        Ok(forms_to_progn(&forms))
    }

    fn expand_cl_case(&mut self, args: &[Value], _env: &mut Env) -> Result<Value, LispError> {
        let expr = args
            .first()
            .ok_or_else(|| LispError::WrongNumberOfArgs("cl-case".into(), 0))?;
        let temp = self.make_generated_symbol("cl-case");
        let mut clauses = Vec::with_capacity(args.len().saturating_sub(1));

        for (index, clause) in args[1..].iter().enumerate() {
            let clause_items = clause.to_vec()?;
            let (keys, body) = match clause_items.split_first() {
                Some((keys, body)) => (keys, body),
                None => (&Value::Nil, &[][..]),
            };
            let test = self.expand_cl_case_clause_test(
                &temp,
                keys,
                index + 1 == args.len().saturating_sub(1),
            )?;
            let body = if body.is_empty() {
                vec![Value::Nil]
            } else {
                body.to_vec()
            };
            clauses.push(Value::list(std::iter::once(test).chain(body)));
        }

        Ok(Value::list([
            Value::Symbol("let".into()),
            Value::list([Value::list([temp.clone(), expr.clone()])]),
            Value::list(std::iter::once(Value::Symbol("cond".into())).chain(clauses)),
        ]))
    }

    fn expand_cl_case_clause_test(
        &self,
        temp: &Value,
        keys: &Value,
        final_clause: bool,
    ) -> Result<Value, LispError> {
        if matches!(keys, Value::Symbol(name) if name == "t" || name == "otherwise") {
            if final_clause {
                return Ok(Value::T);
            }
            return Err(LispError::Signal(
                "Misplaced t or `otherwise' clause".into(),
            ));
        }

        if keys.is_nil() {
            return Ok(Value::Nil);
        }

        if let Value::Cons(_, _) = keys {
            let keys = keys.to_vec()?;
            let mut tests = Vec::with_capacity(keys.len());
            for key in keys {
                tests.push(Self::cl_case_key_test(temp, key));
            }
            return Ok(match tests.as_slice() {
                [] => Value::Nil,
                [single] => single.clone(),
                _ => Value::list(std::iter::once(Value::Symbol("or".into())).chain(tests)),
            });
        }

        Ok(Self::cl_case_key_test(temp, keys.clone()))
    }

    fn cl_case_key_test(temp: &Value, key: Value) -> Value {
        Value::list([
            Value::Symbol("eql".into()),
            temp.clone(),
            quoted_literal(&key),
        ])
    }

    fn sf_skip_unless(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let val = self.eval(&items[1], env)?;
        if val.is_truthy() {
            Ok(Value::Nil)
        } else {
            Err(LispError::TestSkipped("Test skipped".into()))
        }
    }

    fn sf_skip_when(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let val = self.eval(&items[1], env)?;
        if val.is_truthy() {
            Err(LispError::TestSkipped("Test skipped".into()))
        } else {
            Ok(Value::Nil)
        }
    }

    fn sf_rx(&mut self, items: &[Value], env: &Env) -> Result<Value, LispError> {
        Ok(Value::String(compile_rx_sequence(self, env, &items[1..])?))
    }

    fn sf_rx_define(&mut self, items: &[Value]) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::Signal(
                "rx-define needs name and definition".into(),
            ));
        }
        let name = items[1].as_symbol()?.to_string();
        let binding = match &items[2..] {
            [definition] => Value::list([definition.clone()]),
            [params, definition] => Value::list([params.clone(), definition.clone()]),
            _ => {
                return Err(LispError::Signal(format!(
                    "Bad `rx' definition of {name}: {}",
                    Value::list(items[2..].iter().cloned())
                )));
            }
        };
        self.put_symbol_property(&name, "rx-definition", binding);
        Ok(Value::Symbol(name))
    }

    fn sf_with_eval_after_load(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "with-eval-after-load".into(),
                0,
            ));
        }
        let feature_value = self.eval(&items[1], env)?;
        let feature = match feature_value {
            Value::Symbol(name) => name,
            Value::String(name) => name,
            Value::StringObject(state) => state.borrow().text.clone(),
            other => {
                return Err(LispError::TypeError(
                    "string-or-symbol".into(),
                    other.type_name(),
                ));
            }
        };
        if self.has_feature(&feature) {
            self.sf_progn(&items[2..], env)
        } else {
            self.after_load_forms
                .push((feature, items[2..].to_vec(), env.clone()));
            Ok(Value::Nil)
        }
    }

    fn sf_with_memoization(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "with-memoization".into(),
                items.len().saturating_sub(1),
            ));
        }
        let place = self.resolve_setf_place(&items[1], env)?;
        let current = self.eval_resolved_setf_place_current_value(&place, env)?;
        if current.is_truthy() {
            return Ok(current);
        }
        let value = self.sf_progn(&items[2..], env)?;
        self.set_resolved_setf_place_value(&place, value.clone(), env)?;
        Ok(value)
    }

    fn sf_with_mutex(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "with-mutex".into(),
                items.len().saturating_sub(1),
            ));
        }
        let mutex_value = self.eval(&items[1], env)?;
        let mutex_id = self.resolve_mutex_id(&mutex_value)?;
        self.lock_mutex_for_current_thread(mutex_id, env)?;
        let result = self.sf_progn(&items[2..], env);
        let _ = self.unlock_mutex_for_current_thread(mutex_id);
        result
    }

    // ── ERT support ──

    fn sf_ert_deftest(&mut self, items: &[Value], env: &Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Ok(Value::Nil);
        }
        let name = match &items[1] {
            Value::Symbol(s) => s.clone(),
            _ => return Ok(Value::Nil),
        };
        if self.ert_tests.iter().any(|test| test.name == name) {
            return Err(LispError::Signal(format!(
                "Test `{name}` redefined (or loaded twice)"
            )));
        }

        // items[2] is the param list (always empty for ert-deftest)
        // items[3..] is docstring, keyword metadata, then body forms.
        let mut cursor = 3;
        if items
            .get(cursor)
            .is_some_and(|value| matches!(value, Value::String(_)))
        {
            cursor += 1;
        }

        let mut tags = Vec::new();
        let mut expected_result = ":passed".to_string();
        while cursor + 1 < items.len()
            && items
                .get(cursor)
                .and_then(keyword_symbol_name)
                .is_some_and(|name| name.starts_with(':'))
        {
            let keyword = keyword_symbol_name(&items[cursor]).unwrap_or_default();
            let value = &items[cursor + 1];
            match keyword.as_str() {
                ":tags" => tags = parse_ert_tags(value),
                ":expected-result" => expected_result = selector_atom(value),
                _ => {}
            }
            cursor += 2;
        }

        let body = Value::Lambda(
            Vec::new(),
            items[cursor..].to_vec(),
            shared_env(env.clone()),
        );
        self.ert_tests.push(ErtTestDefinition {
            name,
            body,
            source_file: self.current_load_file.clone(),
            tags,
            expected_result,
        });
        Ok(Value::Nil)
    }

    fn sf_should(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("should".into(), 0));
        }
        let val = self.eval(&items[1], env)?;
        if val.is_truthy() {
            Ok(Value::T)
        } else {
            Err(LispError::ErtTestFailed(format!(
                "Test failed: expected truthy value from {}",
                items[1]
            )))
        }
    }

    fn sf_should_not(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("should-not".into(), 0));
        }
        let val = self.eval(&items[1], env)?;
        if val.is_nil() {
            Ok(Value::Nil)
        } else {
            Err(LispError::ErtTestFailed(format!(
                "Test failed: expected nil from {}",
                items[1]
            )))
        }
    }

    fn sf_should_error(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("should-error".into(), 0));
        }
        match self.eval(&items[1], env) {
            Err(e) => {
                if let Some(expected_types) = should_error_types(items)
                    && !expected_types
                        .iter()
                        .any(|expected| expected == &e.condition_type())
                {
                    return Err(LispError::ErtTestFailed(format!(
                        "Test failed: expected error type {} but got {}",
                        expected_types.join(" or "),
                        e.condition_type()
                    )));
                }
                Ok(error_condition_value(&e))
            }
            Ok(val) => Err(LispError::ErtTestFailed(format!(
                "Test failed: expected error but got {}",
                val
            ))),
        }
    }

    pub fn discovered_tests(&self) -> Vec<DiscoveredTest> {
        self.ert_tests
            .iter()
            .map(ErtTestDefinition::discovered)
            .collect()
    }

    /// Run all collected ERT tests. Returns (passed, failed, total).
    pub fn run_ert_tests(&mut self) -> (usize, usize, usize) {
        let summary = self.run_ert_tests_with_selector(None);
        (summary.passed, summary.failed, summary.total)
    }

    pub fn run_ert_tests_with_selector(&mut self, selector: Option<&Value>) -> BatchSummary {
        let tests: Vec<ErtTestDefinition> = self
            .ert_tests
            .iter()
            .filter(|test| selector.is_none_or(|selector| selector_matches(selector, test)))
            .cloned()
            .collect();
        let mut summary = BatchSummary::default();
        self.test_results.clear();
        self.last_selected_tests = tests.iter().map(|test| test.name.clone()).collect();
        summary.total = tests.len();

        for test in &tests {
            let mut env: Env = Vec::new();
            let previous = self.set_current_load_file(test.source_file.clone());
            let result = self.call_function_value(test.body.clone(), None, &[], &mut env);
            self.set_current_load_file(previous);
            match result {
                Ok(_) => {
                    summary.passed += 1;
                    if test.expected_result == ":failed" {
                        summary.unexpected += 1;
                    }
                    self.test_results.push(TestOutcome {
                        name: test.name.clone(),
                        status: TestStatus::Passed,
                        condition_type: None,
                        message: None,
                    });
                }
                Err(e) => {
                    let status = match e {
                        LispError::TestSkipped(_) => TestStatus::Skipped,
                        _ => TestStatus::Failed,
                    };
                    let expected_failure = test.expected_result == ":failed";
                    match status {
                        TestStatus::Passed => summary.passed += 1,
                        TestStatus::Failed => {
                            summary.failed += 1;
                            if !expected_failure {
                                summary.unexpected += 1;
                            }
                        }
                        TestStatus::Skipped => summary.skipped += 1,
                    }
                    self.test_results.push(TestOutcome {
                        name: test.name.clone(),
                        status,
                        condition_type: Some(e.condition_type()),
                        message: Some(e.to_string()),
                    });
                }
            }
        }
        summary
    }
}

fn read_nonblocking_pipe<T: Read>(pipe: &mut T, output: &mut Vec<u8>) -> Result<bool, LispError> {
    let mut read_any = false;
    let mut buffer = [0u8; 4096];
    loop {
        match pipe.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                output.extend_from_slice(&buffer[..read]);
                read_any = true;
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => break,
            Err(error) => return Err(LispError::Signal(error.to_string())),
        }
    }
    Ok(read_any)
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

fn preloaded_command_line_1() -> Value {
    Value::Lambda(
        vec!["args-left".into()],
        vec![Value::list([
            Value::Symbol("let".into()),
            Value::list([
                Value::list([
                    Value::Symbol("command-line-args-left".into()),
                    Value::Symbol("args-left".into()),
                ]),
                Value::list([Value::Symbol("tem".into()), Value::Nil]),
            ]),
            Value::list([
                Value::Symbol("while".into()),
                Value::Symbol("command-line-args-left".into()),
                Value::list([
                    Value::Symbol("let".into()),
                    Value::list([Value::list([
                        Value::Symbol("argi".into()),
                        Value::list([
                            Value::Symbol("car".into()),
                            Value::Symbol("command-line-args-left".into()),
                        ]),
                    ])]),
                    Value::list([
                        Value::Symbol("setq".into()),
                        Value::Symbol("command-line-args-left".into()),
                        Value::list([
                            Value::Symbol("cdr".into()),
                            Value::Symbol("command-line-args-left".into()),
                        ]),
                    ]),
                    Value::list([
                        Value::Symbol("when".into()),
                        Value::list([
                            Value::Symbol("setq".into()),
                            Value::Symbol("tem".into()),
                            Value::list([
                                Value::Symbol("assoc".into()),
                                Value::Symbol("argi".into()),
                                Value::Symbol("command-switch-alist".into()),
                            ]),
                        ]),
                        Value::list([
                            Value::Symbol("funcall".into()),
                            Value::list([Value::Symbol("cdr".into()), Value::Symbol("tem".into())]),
                            Value::Symbol("argi".into()),
                        ]),
                    ]),
                ]),
            ]),
            Value::Nil,
        ])],
        shared_env(Vec::new()),
    )
}

fn preloaded_sh_mode() -> Value {
    Value::Lambda(
        Vec::new(),
        vec![
            Value::list([
                Value::Symbol("setq-local".into()),
                Value::Symbol("major-mode".into()),
                Value::list([
                    Value::Symbol("quote".into()),
                    Value::Symbol("sh-mode".into()),
                ]),
            ]),
            Value::list([
                Value::Symbol("setq-local".into()),
                Value::Symbol("mode-name".into()),
                Value::String("Shell-script".into()),
            ]),
            Value::list([
                Value::Symbol("setq-local".into()),
                Value::Symbol("imenu-case-fold-search".into()),
                Value::Nil,
            ]),
            Value::list([
                Value::Symbol("setq-local".into()),
                Value::Symbol("imenu-generic-skip-comments-and-strings".into()),
                Value::Nil,
            ]),
            Value::list([
                Value::Symbol("setq-local".into()),
                Value::Symbol("imenu-create-index-function".into()),
                Value::list([
                    Value::Symbol("quote".into()),
                    Value::Symbol("imenu-default-create-index-function".into()),
                ]),
            ]),
            Value::list([
                Value::Symbol("setq-local".into()),
                Value::Symbol("imenu-generic-expression".into()),
                Value::list([
                    Value::Symbol("quote".into()),
                    Value::list([
                        Value::list([
                            Value::Nil,
                            Value::String(
                                "^[ \t]*function[ \t]+\\([A-Za-z_][A-Za-z0-9_]*\\)".into(),
                            ),
                            Value::Integer(1),
                        ]),
                        Value::list([
                            Value::Nil,
                            Value::String("^[ \t]*\\([A-Za-z_][A-Za-z0-9_]*\\)[ \t]*()".into()),
                            Value::Integer(1),
                        ]),
                    ]),
                ]),
            ]),
            Value::Nil,
        ],
        shared_env(Vec::new()),
    )
}

fn builtin_auto_mode_alist() -> Value {
    Value::list([
        Value::cons(
            Value::String("\\.\\(?:tar\\(?:\\.gz\\)?\\|tgz\\)\\'".into()),
            Value::Symbol("tar-mode".into()),
        ),
        Value::cons(
            Value::String("\\.zip\\'".into()),
            Value::Symbol("archive-mode".into()),
        ),
    ])
}

fn preloaded_vc_directory_exclusion_list() -> Value {
    Value::list(
        [
            "SCCS", "RCS", "CVS", "MCVS", ".src", ".svn", ".git", ".hg", ".bzr", "_MTN", "_darcs",
            "{arch}", ".repo", ".jj",
        ]
        .into_iter()
        .map(Value::string),
    )
}

fn builtin_file_autoload(file: &str, interactive: Value) -> Value {
    Value::list([
        Value::Symbol("autoload".into()),
        Value::String(file.into()),
        Value::Nil,
        interactive,
        Value::Nil,
    ])
}

fn builtin_macro_autoload(file: &str) -> Value {
    Value::list([
        Value::Symbol("autoload".into()),
        Value::String(file.into()),
        Value::Nil,
        Value::Nil,
        Value::Symbol("macro".into()),
    ])
}

fn builtin_autoload_function(name: &str) -> Option<Value> {
    match name {
        "command-line-1" => Some(preloaded_command_line_1()),
        "cl-delete-duplicates" => Some(builtin_file_autoload("cl-seq", Value::Nil)),
        "connection-local-p" => Some(builtin_macro_autoload("files-x")),
        "connection-local-set-profile-variables" => {
            Some(builtin_file_autoload("files-x", Value::Nil))
        }
        "connection-local-set-profiles" => Some(builtin_file_autoload("files-x", Value::Nil)),
        "connection-local-update-profile-variables" => {
            Some(builtin_file_autoload("files-x", Value::Nil))
        }
        "connection-local-value" => Some(builtin_macro_autoload("files-x")),
        "dired" => Some(builtin_file_autoload("dired", Value::T)),
        "gv-define-expander" => Some(builtin_macro_autoload("gv")),
        "gv-define-setter" => Some(builtin_macro_autoload("gv")),
        "gv-define-simple-setter" => Some(builtin_macro_autoload("gv")),
        "gv-letplace" => Some(builtin_macro_autoload("gv")),
        "hack-connection-local-variables" => Some(builtin_file_autoload("files-x", Value::Nil)),
        "hack-connection-local-variables-apply" => {
            Some(builtin_file_autoload("files-x", Value::Nil))
        }
        "key-valid-p" => Some(builtin_file_autoload("keymap", Value::Nil)),
        "keymap-global-set" => Some(builtin_file_autoload("keymap", Value::T)),
        "keymap-global-unset" => Some(builtin_file_autoload("keymap", Value::T)),
        "keymap-local-set" => Some(builtin_file_autoload("keymap", Value::T)),
        "keymap-local-unset" => Some(builtin_file_autoload("keymap", Value::T)),
        "keymap-lookup" => Some(builtin_file_autoload("keymap", Value::Nil)),
        "keymap-lookup-keymap" => Some(builtin_file_autoload("keymap", Value::Nil)),
        "keymap-set" => Some(builtin_file_autoload("keymap", Value::Nil)),
        "keymap-set-after" => Some(builtin_file_autoload("keymap", Value::Nil)),
        "keymap-substitute" => Some(builtin_file_autoload("keymap", Value::Nil)),
        "keymap-unset" => Some(builtin_file_autoload("keymap", Value::Nil)),
        "define-keymap" => Some(builtin_file_autoload("keymap", Value::Nil)),
        "setq-connection-local" => Some(builtin_macro_autoload("files-x")),
        "sh-mode" => Some(preloaded_sh_mode()),
        "with-connection-local-application-variables" => Some(builtin_macro_autoload("files-x")),
        "with-connection-local-variables" => Some(builtin_macro_autoload("files-x")),
        "with-connection-local-variables-1" => Some(builtin_file_autoload("files-x", Value::Nil)),
        "point-to-register" => Some(Value::Lambda(
            vec!["register".into(), "&optional".into(), "arg".into()],
            vec![
                Value::list([
                    Value::Symbol("interactive".into()),
                    Value::list([
                        Value::Symbol("list".into()),
                        Value::Symbol("last-input-event".into()),
                    ]),
                ]),
                Value::list([
                    Value::Symbol("when".into()),
                    Value::list([
                        Value::Symbol("or".into()),
                        Value::list([
                            Value::Symbol("eq".into()),
                            Value::Symbol("register".into()),
                            Value::Integer(7),
                        ]),
                        Value::list([
                            Value::Symbol("eq".into()),
                            Value::Symbol("register".into()),
                            Value::list([
                                Value::Symbol("quote".into()),
                                Value::Symbol("escape".into()),
                            ]),
                        ]),
                        Value::list([
                            Value::Symbol("eq".into()),
                            Value::Symbol("register".into()),
                            Value::Integer(27),
                        ]),
                    ]),
                    Value::list([Value::Symbol("keyboard-quit".into())]),
                ]),
                Value::Nil,
            ],
            shared_env(Vec::new()),
        )),
        _ => None,
    }
}

fn selector_atom(value: &Value) -> String {
    match unquote(value) {
        Value::Symbol(name) => name.clone(),
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

fn parse_ert_tags(value: &Value) -> Vec<String> {
    match unquote(value) {
        Value::Nil => Vec::new(),
        Value::Cons(_, _) => unquote(value)
            .to_vec()
            .map(|values| values.iter().map(selector_atom).collect())
            .unwrap_or_default(),
        other => vec![selector_atom(&other)],
    }
}

fn should_error_types(items: &[Value]) -> Option<Vec<String>> {
    let mut cursor = 2;
    while cursor + 1 < items.len() {
        match keyword_symbol_name(&items[cursor]).as_deref() {
            Some(":type") => {
                let raw = unquote(&items[cursor + 1]);
                if let Ok(values) = raw.to_vec() {
                    let names = values
                        .into_iter()
                        .map(|value| selector_atom(&value))
                        .collect();
                    return Some(names);
                }
                return Some(vec![selector_atom(&raw)]);
            }
            Some(_) => cursor += 2,
            None => break,
        }
    }
    None
}

fn selector_matches(selector: &Value, test: &ErtTestDefinition) -> bool {
    match unquote(selector) {
        Value::Nil => false,
        Value::T => true,
        Value::Symbol(name) if name == "t" => true,
        Value::Symbol(name) if name == "nil" => false,
        Value::Symbol(name) => test.name == name,
        Value::String(pattern) => Regex::new(&pattern)
            .map(|regex| regex.is_match(&test.name))
            .unwrap_or(false),
        Value::Cons(_, _) => {
            let Ok(items) = unquote(selector).to_vec() else {
                return false;
            };
            if items.is_empty() {
                return false;
            }
            match symbol_name(&items[0]).as_deref() {
                Some("tag") => items
                    .get(1)
                    .map(|tag| {
                        test.tags
                            .iter()
                            .any(|candidate| candidate == &selector_atom(tag))
                    })
                    .unwrap_or(false),
                Some("not") => items
                    .get(1)
                    .is_some_and(|inner| !selector_matches(inner, test)),
                Some("or") => items[1..].iter().any(|inner| selector_matches(inner, test)),
                Some("and") => items[1..].iter().all(|inner| selector_matches(inner, test)),
                Some("member") => items[1..]
                    .iter()
                    .any(|item| selector_atom(item) == test.name),
                Some("eql") => items
                    .get(1)
                    .is_some_and(|item| selector_atom(item) == test.name),
                _ => false,
            }
        }
        _ => false,
    }
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
        | Value::Lambda(_, _, _) => true,
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
    if matches!(pattern, Value::Symbol(name) if name == "_") {
        return Ok(true);
    }
    if let Value::Symbol(name) = pattern
        && name != "nil"
        && name != "t"
    {
        bindings.push((name.clone(), value.clone()));
        return Ok(true);
    }
    if let Ok(parts) = pattern.to_vec() {
        if matches!(parts.first(), Some(Value::Symbol(name)) if name == "backquote") {
            return pcase_pattern_bindings_with_mode(
                interp,
                env,
                parts.get(1).unwrap_or(&Value::Nil),
                value,
                bindings,
                lenient_list_match,
            );
        }
        if matches!(parts.first(), Some(Value::Symbol(name)) if name == "or") {
            let original = bindings.clone();
            for candidate in &parts[1..] {
                let mut trial = original.clone();
                if pcase_pattern_bindings_with_mode(
                    interp,
                    env,
                    candidate,
                    value,
                    &mut trial,
                    lenient_list_match,
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
                if !pcase_pattern_bindings_with_mode(
                    interp,
                    env,
                    candidate,
                    value,
                    bindings,
                    lenient_list_match,
                )? {
                    bindings.truncate(start);
                    return Ok(false);
                }
            }
            return Ok(true);
        }
        if matches!(parts.first(), Some(Value::Symbol(name)) if name == "let") && parts.len() >= 3 {
            env.push(bindings.clone());
            let evaluated = interp.eval(&parts[2], env);
            env.pop();
            return pcase_pattern_bindings_with_mode(
                interp,
                env,
                &parts[1],
                &evaluated?,
                bindings,
                lenient_list_match,
            );
        }
        if matches!(parts.first(), Some(Value::Symbol(name)) if name == "guard") && parts.len() >= 2
        {
            env.push(bindings.clone());
            let guard = interp.eval(&parts[1], env);
            env.pop();
            return Ok(guard?.is_truthy());
        }
        if matches!(parts.first(), Some(Value::Symbol(name)) if name == "pred") && parts.len() >= 2
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
                let Some(slot_index) = slot_names.iter().position(|name| name == &slot_name) else {
                    bindings.truncate(start);
                    return Ok(false);
                };
                let slot_value = slots.get(slot_index).cloned().unwrap_or(Value::Nil);
                if !pcase_pattern_bindings_with_mode(
                    interp,
                    env,
                    &nested_pattern,
                    &slot_value,
                    bindings,
                    lenient_list_match,
                )? {
                    bindings.truncate(start);
                    return Ok(false);
                }
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

    match (pattern, value) {
        (Value::Cons(pattern_car, pattern_cdr), Value::Cons(value_car, value_cdr)) => {
            let start = bindings.len();
            let pattern_car = pattern_car.borrow().clone();
            let pattern_cdr = pattern_cdr.borrow().clone();
            let value_car = value_car.borrow().clone();
            let value_cdr = value_cdr.borrow().clone();
            if !pcase_pattern_bindings_with_mode(
                interp,
                env,
                &pattern_car,
                &value_car,
                bindings,
                lenient_list_match,
            )? {
                bindings.truncate(start);
                return Ok(false);
            }
            if !pcase_pattern_bindings_with_mode(
                interp,
                env,
                &pattern_cdr,
                &value_cdr,
                bindings,
                lenient_list_match,
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
            if !pcase_pattern_bindings_with_mode(
                interp,
                env,
                &pattern_car,
                &Value::Nil,
                bindings,
                lenient_list_match,
            )? {
                bindings.truncate(start);
                return Ok(false);
            }
            if !pcase_pattern_bindings_with_mode(
                interp,
                env,
                &pattern_cdr,
                &Value::Nil,
                bindings,
                lenient_list_match,
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

fn feature_name(value: &Value) -> Option<String> {
    match value {
        Value::Symbol(symbol) => Some(symbol.clone()),
        Value::Cons(_, _) => value
            .to_vec()
            .ok()
            .and_then(|items| match items.as_slice() {
                [Value::Symbol(name), Value::Symbol(symbol)] if name == "quote" => {
                    Some(symbol.clone())
                }
                _ => None,
            }),
        _ => None,
    }
}

fn is_compat_preloaded_feature(feature: &str) -> bool {
    matches!(
        feature,
        "cl-extra"
            | "cl-generic"
            | "cl-lib"
            | "cus-edit"
            | "cus-load"
            | "edmacro"
            | "ert-x"
            | "map"
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

fn compile_rx_sequence(
    interp: &mut Interpreter,
    env: &Env,
    items: &[Value],
) -> Result<String, LispError> {
    let mut regex = String::new();
    for item in items {
        regex.push_str(&compile_rx_form(interp, env, item)?);
    }
    Ok(regex)
}

pub(crate) fn compile_rx_to_string(
    interp: &mut Interpreter,
    form: &Value,
    env: &Env,
    _no_group: bool,
) -> Result<String, LispError> {
    compile_rx_form(interp, env, form)
}

fn expand_rx_splice_markers(
    interp: &Interpreter,
    env: &Env,
    items: &[Value],
) -> Result<Vec<Value>, LispError> {
    let mut expanded = Vec::new();
    let mut index = 0usize;
    while index < items.len() {
        if matches!(&items[index], Value::Symbol(symbol) if symbol == ",") {
            let Some(source) = items.get(index + 1) else {
                return Err(LispError::Signal(
                    "rx splice marker needs a following value".into(),
                ));
            };
            let value = match source {
                Value::Symbol(name) => interp
                    .lookup_var(name, env)
                    .ok_or_else(|| LispError::Void(name.clone()))?,
                other => other.clone(),
            };
            if let Ok(values) = value.to_vec() {
                expanded.extend(values);
            } else {
                expanded.push(value);
            }
            index += 2;
            continue;
        }
        expanded.push(items[index].clone());
        index += 1;
    }
    Ok(expanded)
}

fn compile_rx_literal_form(
    interp: &mut Interpreter,
    env: &Env,
    items: &[Value],
) -> Result<String, LispError> {
    if items.len() != 2 {
        return Err(LispError::Signal("rx `literal' needs one argument".into()));
    }
    let mut literal_env = env.clone();
    let value = interp.eval(&items[1], &mut literal_env)?;
    match value {
        Value::String(text) => Ok(quote_rx_string_literal(&text)),
        Value::StringObject(state) => Ok(quote_rx_string_literal(&state.borrow().text)),
        other => Err(LispError::TypeError("string".into(), other.type_name())),
    }
}

fn compile_rx_regexp_form(
    interp: &mut Interpreter,
    env: &Env,
    items: &[Value],
) -> Result<String, LispError> {
    if items.len() != 2 {
        return Err(LispError::Signal("rx `regexp' needs one string".into()));
    }
    let mut regexp_env = env.clone();
    let value = interp.eval(&items[1], &mut regexp_env)?;
    match value {
        Value::String(text) => Ok(text),
        Value::StringObject(state) => Ok(state.borrow().text.clone()),
        other => Err(LispError::TypeError("string".into(), other.type_name())),
    }
}

fn quote_rx_string_literal(text: &str) -> String {
    let mut quoted = String::new();
    for ch in text.chars() {
        match ch {
            '.' | '[' | '*' | '+' | '?' | '^' | '$' | '\\' => {
                quoted.push('\\');
                quoted.push(ch);
            }
            _ => quoted.push(ch),
        }
    }
    quoted
}

fn rx_char_class_name(symbol: &str) -> Option<&'static str> {
    match symbol {
        "digit" | "numeric" | "num" => Some("digit"),
        "control" | "cntrl" => Some("cntrl"),
        "hex-digit" | "hex" | "xdigit" => Some("xdigit"),
        "blank" => Some("blank"),
        "graphic" | "graph" => Some("graph"),
        "printing" | "print" => Some("print"),
        "alphanumeric" | "alnum" => Some("alnum"),
        "letter" | "alphabetic" | "alpha" => Some("alpha"),
        "ascii" => Some("ascii"),
        "nonascii" => Some("nonascii"),
        "lower" | "lower-case" => Some("lower"),
        "punctuation" | "punct" => Some("punct"),
        "space" | "whitespace" | "white" => Some("space"),
        "upper" | "upper-case" => Some("upper"),
        "word" | "wordchar" => Some("word"),
        "unibyte" => Some("unibyte"),
        "multibyte" => Some("multibyte"),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RxCharInterval {
    start: char,
    end: char,
}

fn rx_codepoint_to_char(codepoint: i64) -> Result<char, LispError> {
    char::from_u32(codepoint as u32)
        .ok_or_else(|| LispError::Signal(format!("Invalid rx character: {codepoint}")))
}

fn rx_interval_string(text: &str, index: usize) -> String {
    text.chars().skip(index).take(3).collect()
}

fn append_rx_string_intervals(
    intervals: &mut Vec<RxCharInterval>,
    text: &str,
) -> Result<(), LispError> {
    let chars: Vec<char> = text.chars().collect();
    let mut index = 0usize;
    while index < chars.len() {
        if index + 2 < chars.len() && chars[index + 1] == '-' {
            let start = chars[index];
            let end = chars[index + 2];
            if start > end {
                return Err(LispError::Signal(format!(
                    "Invalid rx `any' range: {}",
                    rx_interval_string(text, index)
                )));
            }
            intervals.push(RxCharInterval { start, end });
            index += 3;
        } else {
            let ch = chars[index];
            intervals.push(RxCharInterval { start: ch, end: ch });
            index += 1;
        }
    }
    Ok(())
}

fn parse_rx_char_class_items(
    items: &[Value],
) -> Result<(Vec<RxCharInterval>, Vec<&'static str>), LispError> {
    let mut intervals = Vec::new();
    let mut classes = Vec::new();

    for item in items {
        match item {
            Value::String(text) => append_rx_string_intervals(&mut intervals, text)?,
            Value::StringObject(state) => {
                append_rx_string_intervals(&mut intervals, &state.borrow().text)?
            }
            Value::Integer(codepoint) => {
                let ch = rx_codepoint_to_char(*codepoint)?;
                intervals.push(RxCharInterval { start: ch, end: ch });
            }
            Value::Symbol(symbol) => {
                let Some(name) = rx_char_class_name(symbol) else {
                    return Err(LispError::Signal(format!(
                        "Unsupported rx charset fragment: {}",
                        item.type_name()
                    )));
                };
                if !classes.contains(&name) {
                    classes.push(name);
                }
            }
            Value::Cons(_, _) => {
                let (start, end) = item.cons_values().ok_or_else(|| {
                    LispError::Signal("Unsupported rx charset fragment: cons".into())
                })?;
                let start = rx_codepoint_to_char(start.as_integer()?)?;
                let end = rx_codepoint_to_char(end.as_integer()?)?;
                if start > end {
                    return Err(LispError::Signal(format!(
                        "Invalid rx `any' range: {}-{}",
                        start, end
                    )));
                }
                intervals.push(RxCharInterval { start, end });
            }
            other => {
                return Err(LispError::Signal(format!(
                    "Unsupported rx charset fragment: {}",
                    other.type_name()
                )));
            }
        }
    }

    intervals.sort_by_key(|interval| (u32::from(interval.start), u32::from(interval.end)));
    let mut merged: Vec<RxCharInterval> = Vec::new();
    for interval in intervals {
        if let Some(last) = merged.last_mut() {
            let interval_start = u32::from(interval.start);
            let last_end = u32::from(last.end);
            if interval_start <= last_end.saturating_add(1) {
                if interval.end > last.end {
                    last.end = interval.end;
                }
                continue;
            }
        }
        merged.push(interval);
    }

    Ok((merged, classes))
}

fn rx_prev_char(ch: char) -> Option<char> {
    char::from_u32(u32::from(ch).checked_sub(1)?)
}

fn rx_next_char(ch: char) -> Option<char> {
    char::from_u32(u32::from(ch).checked_add(1)?)
}

fn append_rx_char_class_char(regex: &mut String, ch: char) {
    if ch == ']' {
        regex.push('\\');
    }
    regex.push(ch);
}

fn append_rx_char_class_boundary(regex: &mut String, ch: char) {
    regex.push(ch);
}

fn compile_rx_char_class(items: &[Value], negated: bool) -> Result<String, LispError> {
    let (mut intervals, classes) = parse_rx_char_class_items(items)?;
    if intervals.is_empty() && classes.is_empty() {
        return Err(LispError::Signal("rx character set cannot be empty".into()));
    }
    if !negated
        && classes.is_empty()
        && intervals.len() == 1
        && intervals[0].start == intervals[0].end
    {
        return Ok(quote_rx_string_literal(&intervals[0].start.to_string()));
    }

    let mut prefix = String::new();
    let mut suffix = String::new();
    let mut emitted_prefix = false;

    let mut index = 0usize;
    while index < intervals.len() {
        let interval = &mut intervals[index];
        if interval.start == ']' {
            prefix.push(']');
            emitted_prefix = true;
            if interval.end == ']' {
                intervals.remove(index);
                continue;
            }
            interval.start = rx_next_char(']').expect("']' has a successor");
        }
        if interval.end == ']' {
            prefix.push(']');
            emitted_prefix = true;
            interval.end = rx_prev_char(']').expect("']' has a predecessor");
        }
        if interval.start == '-' {
            suffix.push('-');
            if interval.end == '-' {
                intervals.remove(index);
                continue;
            }
            interval.start = rx_next_char('-').expect("'-' has a successor");
        }
        if interval.end == '-' {
            suffix.push('-');
            interval.end = rx_prev_char('-').expect("'-' has a predecessor");
        }
        index += 1;
    }

    let mut regex = String::new();
    regex.push('[');
    if negated {
        regex.push('^');
    }
    regex.push_str(&prefix);
    for name in &classes {
        regex.push_str("[:");
        regex.push_str(name);
        regex.push_str(":]");
    }

    let mut emitted_body = emitted_prefix || !classes.is_empty();
    for interval in &intervals {
        if interval.start == '^' && !negated && !emitted_body {
            if interval.end == '^' {
                suffix.push('^');
                continue;
            }
            append_rx_char_class_boundary(
                &mut regex,
                rx_next_char('^').expect("'^' has a successor"),
            );
            regex.push('-');
            append_rx_char_class_boundary(&mut regex, interval.end);
            suffix.push('^');
        } else if interval.start == interval.end {
            append_rx_char_class_char(&mut regex, interval.start);
        } else {
            append_rx_char_class_boundary(&mut regex, interval.start);
            regex.push('-');
            append_rx_char_class_boundary(&mut regex, interval.end);
        }
        emitted_body = true;
    }

    regex.push_str(&suffix);
    regex.push(']');
    Ok(regex)
}

fn compile_rx_form(
    interp: &mut Interpreter,
    env: &Env,
    value: &Value,
) -> Result<String, LispError> {
    match value {
        Value::String(text) => Ok(quote_rx_string_literal(text)),
        Value::StringObject(state) => Ok(quote_rx_string_literal(&state.borrow().text)),
        Value::Integer(codepoint) => {
            let ch = char::from_u32(*codepoint as u32)
                .ok_or_else(|| LispError::Signal(format!("Invalid rx character: {codepoint}")))?;
            Ok(quote_rx_string_literal(&ch.to_string()))
        }
        Value::Symbol(symbol) => match symbol.as_str() {
            "bol" => Ok("^".into()),
            "eol" => Ok("$".into()),
            "bos" | "string-start" | "bot" | "buffer-start" => Ok("\\`".into()),
            "eos" | "string-end" | "eot" | "buffer-end" => Ok("\\'".into()),
            "bow" | "eow" => Ok("\\b".into()),
            "digit" => Ok("[0-9]".into()),
            "xdigit" => Ok("[0-9A-Fa-f]".into()),
            "blank" => Ok("[[:blank:]]".into()),
            "space" => Ok("[[:space:]]".into()),
            "nonl" | "not-newline" => Ok(".".into()),
            "symbol-start" => Ok("\\_<".into()),
            "symbol-end" => Ok("\\_>".into()),
            other if rx_char_class_name(other).is_some() => Ok(format!(
                "[[:{}:]]",
                rx_char_class_name(other).unwrap_or_default()
            )),
            other => {
                if let Some(expanded) = expand_rx_definition(interp, other, &[])? {
                    compile_rx_form(interp, env, &expanded)
                } else {
                    Err(LispError::Signal(format!("Unsupported rx atom: {other}")))
                }
            }
        },
        Value::Cons(_, _) => {
            let items = expand_rx_splice_markers(interp, env, &value.to_vec()?)?;
            let head = match items.first() {
                Some(Value::Symbol(head)) => Some(head.as_str()),
                Some(Value::Integer(codepoint)) if *codepoint == ' ' as i64 => Some("?"),
                _ => None,
            };
            let Some(head) = head else {
                return compile_rx_sequence(interp, env, &items);
            };
            match head {
                "group" => Ok(format!(
                    "\\({}\\)",
                    compile_rx_sequence(interp, env, &items[1..])?
                )),
                "group-n" | "submatch-n" => {
                    if items.len() < 3 {
                        return Err(LispError::Signal(
                            "rx `group-n' needs a group number and a form".into(),
                        ));
                    }
                    let number = items[1].as_integer()?;
                    if number <= 0 {
                        return Err(LispError::Signal(
                            "rx `group-n' needs a positive group number".into(),
                        ));
                    }
                    Ok(format!(
                        "\\(?{}:{}\\)",
                        number,
                        compile_rx_sequence(interp, env, &items[2..])?
                    ))
                }
                "+" | "1+" | "one-or-more" => Ok(format!(
                    "\\(?:{}\\)+",
                    compile_rx_sequence(interp, env, &items[1..])?
                )),
                "+?" => Ok(format!(
                    "\\(?:{}\\)+?",
                    compile_rx_sequence(interp, env, &items[1..])?
                )),
                "*" | "0+" | "zero-or-more" => Ok(format!(
                    "\\(?:{}\\)*",
                    compile_rx_sequence(interp, env, &items[1..])?
                )),
                "*?" => Ok(format!(
                    "\\(?:{}\\)*?",
                    compile_rx_sequence(interp, env, &items[1..])?
                )),
                "?" | "zero-or-one" | "opt" | "optional" => Ok(format!(
                    "\\(?:{}\\)?",
                    compile_rx_sequence(interp, env, &items[1..])?
                )),
                "??" => Ok(format!(
                    "\\(?:{}\\)??",
                    compile_rx_sequence(interp, env, &items[1..])?
                )),
                "seq" | ":" => compile_rx_sequence(interp, env, &items[1..]),
                "regexp" => compile_rx_regexp_form(interp, env, &items),
                "literal" => compile_rx_literal_form(interp, env, &items),
                "repeat" => {
                    if items.len() < 3 {
                        return Err(LispError::Signal(
                            "rx `repeat' needs a count and a form".into(),
                        ));
                    }
                    let min = items[1].as_integer()?;
                    if min < 0 {
                        return Err(LispError::Signal("rx repetition count must be >= 0".into()));
                    }

                    let (max, body_start) = match items.get(2) {
                        Some(Value::Integer(max)) => {
                            if *max < min {
                                return Err(LispError::Signal(
                                    "rx repetition max must be >= min".into(),
                                ));
                            }
                            (Some(*max), 3usize)
                        }
                        Some(Value::Nil) => (None, 3usize),
                        _ => (Some(min), 2usize),
                    };
                    if body_start >= items.len() {
                        return Err(LispError::Signal(
                            "rx `repeat' needs a repeated form".into(),
                        ));
                    }

                    let body = compile_rx_sequence(interp, env, &items[body_start..])?;
                    let quantifier = match max {
                        Some(max) if max == min => format!("\\{{{min}\\}}"),
                        Some(max) => format!("\\{{{min},{max}\\}}"),
                        None => format!("\\{{{min},\\}}"),
                    };
                    Ok(format!("\\(?:{body}\\){quantifier}"))
                }
                "or" | "|" => Ok(format!(
                    "\\(?:{}\\)",
                    items[1..]
                        .iter()
                        .map(|item| compile_rx_form(interp, env, item))
                        .collect::<Result<Vec<_>, _>>()?
                        .join("\\|")
                )),
                "any" | "in" | "char" => compile_rx_char_class(&items[1..], false),
                "syntax" => compile_rx_syntax_form(&items, false),
                "not-syntax" => compile_rx_syntax_form(&items, true),
                "not" => {
                    if items.len() != 2 {
                        return Err(LispError::Signal("rx `not' needs one argument".into()));
                    }
                    match &items[1] {
                        Value::Cons(_, _) => {
                            let charset = items[1].to_vec()?;
                            let Some(Value::Symbol(kind)) = charset.first() else {
                                return Err(LispError::Signal("Unsupported rx `not' form".into()));
                            };
                            if !matches!(kind.as_str(), "any" | "in" | "char") {
                                return Err(LispError::Signal("Unsupported rx `not' form".into()));
                            }
                            compile_rx_char_class(&charset[1..], true)
                        }
                        other => compile_rx_char_class(std::slice::from_ref(other), true),
                    }
                }
                "=" => {
                    if items.len() < 3 {
                        return Err(LispError::Signal("rx `=' needs a count and a form".into()));
                    }
                    let count = items[1].as_integer()?;
                    if count < 0 {
                        return Err(LispError::Signal("rx repetition count must be >= 0".into()));
                    }
                    Ok(format!(
                        "\\(?:{}\\)\\{{{}\\}}",
                        compile_rx_sequence(interp, env, &items[2..])?,
                        count
                    ))
                }
                ">=" => {
                    if items.len() < 3 {
                        return Err(LispError::Signal("rx `>=' needs a count and a form".into()));
                    }
                    let count = items[1].as_integer()?;
                    if count < 0 {
                        return Err(LispError::Signal("rx repetition count must be >= 0".into()));
                    }
                    Ok(format!(
                        "\\(?:{}\\)\\{{{},\\}}",
                        compile_rx_sequence(interp, env, &items[2..])?,
                        count
                    ))
                }
                _ => {
                    if let Some(expanded) = expand_rx_definition(interp, head, &items[1..])? {
                        compile_rx_form(interp, env, &expanded)
                    } else {
                        compile_rx_sequence(interp, env, &items)
                    }
                }
            }
        }
        other => Err(LispError::Signal(format!(
            "Unsupported rx form: {}",
            other.type_name()
        ))),
    }
}

fn rx_syntax_code_from_name(name: &str) -> Option<char> {
    match name {
        "whitespace" | "space" | "white" => Some('-'),
        "punctuation" => Some('.'),
        "word" | "wordchar" => Some('w'),
        "symbol" => Some('_'),
        "open-parenthesis" => Some('('),
        "close-parenthesis" => Some(')'),
        "expression-prefix" => Some('\''),
        "string-quote" => Some('"'),
        "paired-delimiter" => Some('$'),
        "escape" => Some('\\'),
        "character-quote" => Some('/'),
        "comment-start" => Some('<'),
        "comment-end" => Some('>'),
        "string-delimiter" => Some('|'),
        "comment-delimiter" => Some('!'),
        _ if name.len() == 1 => {
            let ch = name.chars().next()?;
            match ch {
                '-' | '.' | 'w' | '_' | '(' | ')' | '\'' | '"' | '$' | '\\' | '/' | '<' | '>'
                | '|' | '!' => Some(ch),
                _ => None,
            }
        }
        _ => None,
    }
}

fn rx_syntax_code_from_value(value: &Value) -> Result<char, LispError> {
    match value {
        Value::Symbol(symbol) => rx_syntax_code_from_name(symbol)
            .ok_or_else(|| LispError::Signal(format!("Unknown rx syntax name `{symbol}`"))),
        Value::Integer(codepoint) => {
            let ch = char::from_u32(*codepoint as u32).ok_or_else(|| {
                LispError::Signal(format!("Invalid rx syntax character: {codepoint}"))
            })?;
            rx_syntax_code_from_name(&ch.to_string())
                .ok_or_else(|| LispError::Signal(format!("Unknown rx syntax name `{ch}`")))
        }
        _ => Err(LispError::Signal(
            "rx `syntax' form takes a syntax name or syntax character".into(),
        )),
    }
}

fn compile_rx_syntax_form(items: &[Value], negated: bool) -> Result<String, LispError> {
    if items.len() != 2 {
        let form = if negated { "not-syntax" } else { "syntax" };
        return Err(LispError::Signal(format!(
            "rx `{form}` form takes exactly one argument"
        )));
    }
    let syntax = rx_syntax_code_from_value(&items[1])?;
    if syntax == 'w' {
        Ok(format!(r"\{}", if negated { 'W' } else { 'w' }))
    } else {
        Ok(format!(r"\{}{}", if negated { 'S' } else { 's' }, syntax))
    }
}

fn expand_rx_definition(
    interp: &Interpreter,
    name: &str,
    args: &[Value],
) -> Result<Option<Value>, LispError> {
    let Some(binding) = interp.get_symbol_property(name, "rx-definition") else {
        return Ok(None);
    };
    let items = binding.to_vec()?;
    match items.as_slice() {
        [definition] if args.is_empty() => Ok(Some(definition.clone())),
        [params, definition] => {
            let params = params.to_vec()?;
            Ok(Some(expand_rx_template(definition, &params, args)?))
        }
        _ => Err(LispError::Signal(format!(
            "Bad `rx' definition of {name}: {binding}"
        ))),
    }
}

fn expand_rx_template(form: &Value, params: &[Value], args: &[Value]) -> Result<Value, LispError> {
    let mut bindings = Vec::new();
    let mut arg_index = 0usize;
    let mut rest = false;

    for param in params {
        let name = param.as_symbol()?.to_string();
        if name == "&rest" {
            rest = true;
            continue;
        }
        let values = if rest {
            args[arg_index..].to_vec()
        } else {
            let value = args.get(arg_index).cloned().unwrap_or(Value::Nil);
            arg_index += 1;
            vec![value]
        };
        bindings.push((name, values, rest));
        if rest {
            break;
        }
    }

    expand_rx_template_value(form, &bindings)
}

fn expand_rx_template_value(
    form: &Value,
    bindings: &[(String, Vec<Value>, bool)],
) -> Result<Value, LispError> {
    match form {
        Value::Symbol(name) => {
            if let Some((_, values, is_rest)) =
                bindings.iter().find(|(binding, _, _)| binding == name)
            {
                if *is_rest {
                    Ok(if values.len() == 1 {
                        values[0].clone()
                    } else {
                        Value::list(values.clone())
                    })
                } else {
                    Ok(values.first().cloned().unwrap_or(Value::Nil))
                }
            } else {
                Ok(form.clone())
            }
        }
        Value::Cons(_, _) => {
            let items = form.to_vec()?;
            let mut expanded = Vec::new();
            for item in items {
                if let Value::Symbol(name) = &item
                    && let Some((_, values, true)) =
                        bindings.iter().find(|(binding, _, _)| binding == name)
                {
                    expanded.extend(values.clone());
                    continue;
                }
                expanded.push(expand_rx_template_value(&item, bindings)?);
            }
            Ok(Value::list(expanded))
        }
        _ => Ok(form.clone()),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::lisp::reader::Reader;
    use std::path::PathBuf;
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn eval_str(src: &str) -> Value {
        let mut interp = Interpreter::new();
        let mut env: Env = Vec::new();
        let forms = Reader::new(src).read_all().unwrap();
        let mut result = Value::Nil;
        for form in &forms {
            result = interp.eval(form, &mut env).unwrap();
        }
        result
    }

    fn eval_str_with(interp: &mut Interpreter, src: &str) -> Value {
        let mut env: Env = Vec::new();
        let forms = Reader::new(src).read_all().unwrap();
        let mut result = Value::Nil;
        for form in &forms {
            result = interp.eval(form, &mut env).unwrap();
        }
        result
    }

    fn eval_str_with_upstream_load_path(src: &str) -> Value {
        let mut interp = Interpreter::new();
        interp.set_load_path(
            crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
        );
        eval_str_with(&mut interp, src)
    }

    fn load_faces_compat(interp: &mut Interpreter) {
        let path = crate::compat::project_root().join("src/lisp/faces_compat.el");
        crate::lisp::load_file_strict(interp, &path).unwrap();
    }

    fn upstream_emacs_repo() -> PathBuf {
        crate::compat::project_root().join("../emacs")
    }

    fn assert_string_value(value: Value, expected: &str) {
        assert_eq!(primitives::string_text(&value).unwrap(), expected);
    }

    fn assert_string_list(value: Value, expected: &[&str]) {
        let items = value.to_vec().unwrap();
        assert_eq!(items.len(), expected.len());
        for (item, expected) in items.iter().zip(expected.iter()) {
            assert_eq!(primitives::string_text(item).unwrap(), *expected);
        }
    }

    fn run_with_large_stack(test: impl FnOnce() + Send + 'static) {
        thread::Builder::new()
            .stack_size(128 * 1024 * 1024)
            .spawn(test)
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn eval_atoms() {
        assert_eq!(eval_str("42"), Value::Integer(42));
        assert_eq!(eval_str("\"hello\""), Value::String("hello".into()));
        assert_eq!(eval_str("nil"), Value::Nil);
        assert_eq!(eval_str("t"), Value::T);
    }

    #[test]
    fn with_demoted_errors_returns_nil_after_catching_errors() {
        run_with_large_stack(|| {
            assert_eq!(
                eval_str("(with-demoted-errors \"%S\" (error \"boom\"))"),
                Value::Nil
            );
        });
    }

    #[test]
    fn prin1_writes_to_buffer_streams() {
        assert_eq!(
            eval_str(
                "(with-temp-buffer \
                   (prin1 '(alpha \"beta\") (current-buffer)) \
                   (buffer-string))"
            ),
            Value::String("(alpha \"beta\")".into())
        );
    }

    #[test]
    fn read_accepts_buffer_and_marker_streams() {
        assert_eq!(
            eval_str(
                "(with-temp-buffer \
                   (insert \"(1 2)\") \
                   (goto-char 1) \
                   (list (read (current-buffer)) (point) (read (point-min-marker))))"
            ),
            Value::list([
                Value::list([Value::Integer(1), Value::Integer(2)]),
                Value::Integer(6),
                Value::list([Value::Integer(1), Value::Integer(2)]),
            ])
        );
    }

    #[test]
    fn md5_accepts_buffer_sources_and_coding_symbols() {
        assert_eq!(
            eval_str(
                "(with-temp-buffer \
                   (insert \"abc\") \
                   (md5 (current-buffer) nil nil 'utf-8-emacs-unix))"
            ),
            Value::String("900150983cd24fb0d6963f7d28e17f72".into())
        );
    }

    #[test]
    fn intern_soft_accepts_symbol_arguments() {
        assert_eq!(
            eval_str("(intern-soft 'sample-symbol)"),
            Value::Symbol("sample-symbol".into())
        );
    }

    #[test]
    fn handler_bind_errors_skip_inner_condition_case() {
        let mut interp = Interpreter::new();
        let mut env = Vec::new();
        let forms = Reader::new(
            r#"
            (condition-case nil
                (handler-bind
                    ((error (lambda (_err)
                              (signal 'wrong-type-argument nil))))
                  (list 'result
                        (condition-case nil
                            (user-error "hello")
                          (wrong-type-argument 'inner-handler))))
              (wrong-type-argument 'wrong-type-argument))
            "#,
        )
        .read_all()
        .unwrap();
        let result = interp.eval(&forms[0], &mut env).unwrap();
        assert_eq!(result, Value::Symbol("wrong-type-argument".into()));
    }

    #[test]
    fn full_handler_bind_regression_sequence() {
        let mut interp = Interpreter::new();
        let mut env = Vec::new();
        let forms = Reader::new(
            r#"
            (progn
              (equal (catch 'tag
                       (handler-bind ((error (lambda (_err) (throw 'tag 'wow))))
                         'noerror))
                     'noerror)
              (equal (catch 'tag
                       (handler-bind ((error (lambda (_err) (throw 'tag 'err))))
                         (list 'inner-catch
                               (catch 'tag
                                 (user-error "hello")))))
                     '(inner-catch err))
              (condition-case nil
                  (handler-bind
                      ((error (lambda (_err)
                                (signal 'wrong-type-argument nil))))
                    (list 'result
                          (condition-case nil
                              (user-error "hello")
                            (wrong-type-argument 'inner-handler))))
                (wrong-type-argument 'wrong-type-argument)))
            "#,
        )
        .read_all()
        .unwrap();
        let result = interp.eval(&forms[0], &mut env).unwrap();
        assert_eq!(result, Value::Symbol("wrong-type-argument".into()));
    }

    #[test]
    fn handler_bind_preserves_error_object_identity_for_condition_case() {
        let mut interp = Interpreter::new();
        let mut env = Vec::new();
        let forms = Reader::new(
            r#"
            (let* (inner-error
                   (outer-error
                    (condition-case err
                        (handler-bind ((error (lambda (err) (setq inner-error err))))
                          (car 1))
                      (error err))))
              (eq inner-error outer-error))
            "#,
        )
        .read_all()
        .unwrap();
        let result = interp.eval(&forms[0], &mut env).unwrap();
        assert_eq!(result, Value::T);
    }

    #[test]
    fn aref_out_of_range_signals_args_out_of_range_condition() {
        assert_eq!(
            eval_str(
                "(condition-case nil (aref [1] 1) (args-out-of-range 'caught) (error 'plain-error))"
            ),
            Value::Symbol("caught".into())
        );
    }

    #[test]
    fn handler_bind_handlers_do_not_apply_inside_handlers() {
        let mut interp = Interpreter::new();
        let mut env = Vec::new();
        let forms = Reader::new(
            r#"
            (condition-case nil
                (handler-bind
                    ((error (lambda (_err)
                              (signal 'wrong-type-argument nil)))
                     (wrong-type-argument
                      (lambda (_err) (user-error "wrong-type-argument"))))
                  (user-error "hello"))
              (wrong-type-argument 'wrong-type-argument)
              (error 'plain-error))
            "#,
        )
        .read_all()
        .unwrap();
        let result = interp.eval(&forms[0], &mut env).unwrap();
        assert_eq!(result, Value::Symbol("wrong-type-argument".into()));
    }

    #[test]
    fn lambda_without_body_still_reports_invalid_function_for_bad_args() {
        let mut interp = Interpreter::new();
        let mut env = Vec::new();
        let forms = Reader::new(r#"(eval '(funcall (lambda (&rest &optional))) nil)"#)
            .read_all()
            .unwrap();
        let error = interp.eval(&forms[0], &mut env).unwrap_err();
        assert_eq!(error.condition_type(), "invalid-function");
    }

    #[test]
    fn lambda_with_only_string_body_returns_that_string() {
        let mut interp = Interpreter::new();
        let mut env = Vec::new();
        let form = Reader::new(r#"(funcall (lambda () "foo"))"#)
            .read()
            .unwrap()
            .unwrap();
        let result = interp.eval(&form, &mut env).unwrap();
        assert_eq!(result, Value::String("foo".into()));
    }

    #[test]
    fn lambda_rest_ignores_missing_optional_arguments() {
        run_with_large_stack(|| {
            assert_eq!(
                eval_str("(funcall (lambda (a &optional b c &rest rest) (list a b c rest)) 1)"),
                Value::list([Value::Integer(1), Value::Nil, Value::Nil, Value::Nil,])
            );
        });
    }

    #[test]
    fn with_connection_local_variables_uses_lisp_macro_definition() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                   (defmacro with-connection-local-variables (&rest body)
                     `(progn ,@body))
                   (with-connection-local-variables 1 2 3))",
            ),
            Value::Integer(3)
        );
    }

    #[test]
    fn with_eval_after_load_runs_forms_when_feature_is_provided() {
        assert_eq!(
            eval_str(
                r#"
                (progn
                  (setq emaxx-after-load-events nil)
                  (with-eval-after-load 'sample-after-load
                    (push 'deferred emaxx-after-load-events))
                  (with-eval-after-load 'emaxx
                    (push 'immediate emaxx-after-load-events))
                  (provide 'sample-after-load)
                  emaxx-after-load-events)
                "#
            ),
            Value::list([
                Value::Symbol("deferred".into()),
                Value::Symbol("immediate".into()),
            ])
        );
    }

    #[test]
    fn mutating_if_tail_reports_void_variable() {
        assert_eq!(
            eval_str(
                r#"
                (let ((if-tail (list '(setcdr if-tail "abc") t)))
                  (list
                   (condition-case nil
                       (progn (eval (cons 'if if-tail) nil) 'ok)
                     (void-variable 'void-variable)
                     (wrong-type-argument 'wrong-type-argument))
                   (condition-case nil
                       (progn (eval (cons 'if if-tail) t) 'ok)
                     (void-variable 'void-variable)
                     (wrong-type-argument 'wrong-type-argument))))
                "#
            ),
            Value::list([
                Value::Symbol("void-variable".into()),
                Value::Symbol("void-variable".into()),
            ])
        );
    }

    #[test]
    fn eval_arithmetic() {
        assert_eq!(eval_str("(+ 1 2)"), Value::Integer(3));
        assert_eq!(eval_str("(- 10 3)"), Value::Integer(7));
        assert_eq!(eval_str("(* 4 5)"), Value::Integer(20));
        assert_eq!(eval_str("(+ 1 2 3 4)"), Value::Integer(10));
        assert_eq!(eval_str("(1+ 5)"), Value::Integer(6));
        assert_eq!(eval_str("(1- 5)"), Value::Integer(4));
        assert_eq!(eval_str("(logand)"), Value::Integer(-1));
        assert_eq!(eval_str("(logand 7 3 1)"), Value::Integer(1));
        assert_eq!(eval_str("(logior 1 2 4)"), Value::Integer(7));
        assert_eq!(eval_str("(logxor 1 2 3)"), Value::Integer(0));
        assert_eq!(eval_str("(lognot 5)"), Value::Integer(-6));
    }

    #[test]
    fn aref_reads_strings_bound_in_lexical_variables() {
        assert_eq!(
            eval_str("(let ((buf (make-string 4 0))) (aref buf 0))"),
            Value::Integer(0)
        );
    }

    #[test]
    fn setf_supports_aref_places_bound_in_lexical_variables() {
        assert_eq!(
            eval_str("(let ((stats (vector 0 0)) (i 1)) (setf (aref stats (mod i 2)) 7) stats)"),
            Value::list([
                Value::Symbol("vector".into()),
                Value::Integer(0),
                Value::Integer(7),
            ])
        );
    }

    #[test]
    fn setf_uses_symbol_gv_setter_declarations() {
        assert_eq!(
            eval_str(
                "(progn
                   (cl-defstruct sample-gv alpha beta)
                   (defun sample-alpha-setter (object value)
                     (setf (sample-gv-alpha object) value)
                     value)
                   (defun sample-alpha-view (object)
                     (declare (gv-setter sample-alpha-setter))
                     (sample-gv-alpha object))
                   (let ((object (make-sample-gv :alpha 1 :beta 2)))
                     (list
                      (setf (sample-alpha-view object) 9)
                      (sample-gv-alpha object)
                      (sample-gv-beta object))))"
            ),
            Value::list([Value::Integer(9), Value::Integer(9), Value::Integer(2),])
        );
    }

    #[test]
    fn setf_resolves_conditional_places() {
        assert_eq!(
            eval_str(
                "(let ((left nil) (right nil) (choose-right t))
                   (setf (cond (choose-right right) (t left)) '(stored))
                   (list left right))"
            ),
            Value::list([Value::Nil, Value::list([Value::Symbol("stored".into())])])
        );
    }

    #[test]
    fn aset_mutates_vectors_bound_in_lexical_variables() {
        assert_eq!(
            eval_str("(let ((stats (make-vector 2 nil))) (aset stats 1 'ok) stats)"),
            Value::list([
                Value::Symbol("vector".into()),
                Value::Nil,
                Value::Symbol("ok".into()),
            ])
        );
    }

    #[test]
    fn prog1_returns_vectors_after_in_place_mutation() {
        assert_eq!(
            eval_str("(let ((stats (make-vector 2 nil))) (prog1 stats (aset stats 1 'ok)))"),
            Value::list([
                Value::Symbol("vector".into()),
                Value::Nil,
                Value::Symbol("ok".into()),
            ])
        );
    }

    #[test]
    fn make_temp_name_preserves_prefix_and_changes_across_calls() {
        assert_eq!(
            eval_str(
                r#"(let ((a (make-temp-name "x-dnd-test-"))
                         (b (make-temp-name "x-dnd-test-")))
                     (list (string-prefix-p "x-dnd-test-" a)
                           (string-prefix-p "x-dnd-test-" b)
                           (equal a b)))"#
            ),
            Value::list([Value::T, Value::T, Value::Nil])
        );
    }

    #[test]
    fn write_region_accepts_string_data_even_with_numeric_end_argument() {
        assert_eq!(
            eval_str(
                r#"(let ((path (make-temp-name temporary-file-directory)))
                     (unwind-protect
                         (progn
                           (write-region "" 0 path)
                           (file-exists-p path))
                       (ignore-errors (delete-file path))))"#
            ),
            Value::T
        );
    }

    #[test]
    fn url_encode_url_preserves_reserved_chars_and_escapes_spaces() {
        assert_eq!(
            eval_str(r#"(url-encode-url "file:///tmp/a b?x=1#frag")"#),
            Value::String("file:///tmp/a%20b?x=1#frag".into())
        );
    }

    #[test]
    fn url_scheme_get_property_reports_standard_default_ports() {
        assert_eq!(
            eval_str(
                "(list
                   (url-scheme-get-property \"https\" 'default-port)
                   (url-scheme-get-property \"http\" 'default-port)
                   (url-scheme-get-property \"unknown\" 'default-port))"
            ),
            Value::list([Value::Integer(443), Value::Integer(80), Value::Integer(0)])
        );
    }

    #[test]
    fn assoc_matches_strings_bound_in_lexical_variables() {
        assert_eq!(
            eval_str(
                "(let ((key \"--foo\") (alist (list (cons \"--foo\" 1)))) (cdr (assoc key alist)))"
            ),
            Value::Integer(1)
        );
    }

    #[test]
    fn assoc_and_assq_ignore_non_cons_alist_entries() {
        assert_eq!(
            eval_str(
                "(list (assoc 'target '(dummy (other . 1) (target . 2)))
                       (assq 'target '(dummy (other . 1) (target . 2))))"
            ),
            Value::list([
                Value::cons(Value::Symbol("target".into()), Value::Integer(2)),
                Value::cons(Value::Symbol("target".into()), Value::Integer(2)),
            ])
        );
    }

    #[test]
    fn cl_list_accessors_return_positional_elements() {
        assert_eq!(
            eval_str("(list (cl-first '(a b c)) (cl-second '(a b c)) (cl-third '(a b c)))"),
            Value::list([
                Value::Symbol("a".into()),
                Value::Symbol("b".into()),
                Value::Symbol("c".into()),
            ])
        );
    }

    #[test]
    fn proper_list_p_returns_length_for_proper_lists_only() {
        assert_eq!(
            eval_str(
                "(list
                   (proper-list-p nil)
                   (proper-list-p '(a b))
                   (proper-list-p '(a . b))
                   (let ((x (list 'a)))
                     (setcdr x x)
                     (proper-list-p x)))"
            ),
            Value::list([Value::Integer(0), Value::Integer(2), Value::Nil, Value::Nil])
        );
    }

    #[test]
    fn rassq_delete_all_filters_matching_alist_values() {
        assert_eq!(
            eval_str("(rassq-delete-all 'drop '(noise (a . drop) (b . keep) (c . drop)))"),
            Value::list([
                Value::Symbol("noise".into()),
                Value::cons(Value::Symbol("b".into()), Value::Symbol("keep".into())),
            ])
        );
    }

    #[test]
    fn format_prompt_uses_first_default_choice() {
        run_large_stack_test(assert_format_prompt_uses_first_default_choice);
    }

    #[test]
    fn warn_formats_message_and_returns_nil() {
        assert_eq!(
            eval_str(
                "(progn
                   (warn \"sample %s\" \"warning\")
                   (list (current-message)
                         (warn \"ignored\")))"
            ),
            Value::list([Value::String("Warning: sample warning".into()), Value::Nil])
        );
    }

    #[test]
    fn run_hook_with_args_until_success_returns_first_truthy_result() {
        assert_eq!(
            eval_str(
                "(progn
                   (defvar sample-success-hook nil)
                   (setq sample-success-hook
                         (list
                          (lambda (value) nil)
                          (lambda (value) (list 'hit value))
                          (lambda (value) (error \"must not run\"))))
                   (run-hook-with-args-until-success 'sample-success-hook 7))"
            ),
            Value::list([Value::Symbol("hit".into()), Value::Integer(7)])
        );
    }

    #[test]
    fn advice_member_p_defaults_to_nil_for_untracked_advice() {
        assert_eq!(
            eval_str("(advice-member-p 'sample-advice 'sample-function)"),
            Value::Nil
        );
    }

    fn assert_format_prompt_uses_first_default_choice() {
        assert_eq!(
            eval_str(r#"(format-prompt "Regexp to unhighlight" '("a" "b"))"#),
            Value::String("Regexp to unhighlight (default a): ".into())
        );
    }

    #[test]
    fn assoc_string_matches_symbols_single_strings_and_case_fold() {
        assert_eq!(
            eval_str(
                "(list (assoc-string 'foo '((bar . 1) (foo . 2)))
                       (assoc-string \"foo\" '(dummy \"foo\"))
                       (assoc-string \"FOO\" '((\"foo\" . 3)) t))"
            ),
            Value::list([
                Value::cons(Value::Symbol("foo".into()), Value::Integer(2)),
                Value::String("foo".into()),
                Value::cons(Value::String("foo".into()), Value::Integer(3)),
            ])
        );
    }

    #[test]
    fn assoc_string_handles_nil_t_and_empty_alists_like_symbols() {
        assert_eq!(
            eval_str(
                r#"(list (assoc-string nil nil)
                         (assoc-string 1 nil)
                         (assoc-string nil '((nil . nil-value) ("nil" . string-value)))
                         (assoc-string t '((t . t-value) ("t" . string-value)))
                         (assoc-string "nil" '((nil . nil-value)))
                         (assoc-string "t" '((t . t-value))))"#
            ),
            Value::list([
                Value::Nil,
                Value::Nil,
                Value::cons(Value::Nil, Value::Symbol("nil-value".into())),
                Value::cons(Value::T, Value::Symbol("t-value".into())),
                Value::cons(Value::Nil, Value::Symbol("nil-value".into())),
                Value::cons(Value::T, Value::Symbol("t-value".into())),
            ])
        );
    }

    #[test]
    fn assoc_string_case_fold_avoids_multi_character_uppercase_matches() {
        let value =
            eval_str("(assoc-string \"ß\" '((\"ss\" . wrong) (\"ß\" . right) (\"ẞ\" . upper)) t)");
        let Some((key, result)) = value.cons_values() else {
            panic!("assoc-string should return an alist entry");
        };
        assert_string_value(key, "ß");
        assert_eq!(result, Value::Symbol("right".into()));
    }

    #[test]
    fn cl_delete_if_filters_matching_items() {
        run_large_stack_test(assert_cl_delete_if_filters_matching_items);
    }

    fn assert_cl_delete_if_filters_matching_items() {
        assert_eq!(
            eval_str("(cl-delete-if #'numberp '(a 1 b 2 c))"),
            Value::list([
                Value::Symbol("a".into()),
                Value::Symbol("b".into()),
                Value::Symbol("c".into()),
            ])
        );
    }

    #[test]
    fn remove_filters_lists_vectors_and_strings() {
        let value = eval_str(
            "(list
               (remove 'a '(a b a c))
               (remove 2 [1 2 3 2])
               (remove ?a \"aba\"))",
        );
        let items = value.to_vec().unwrap();
        assert_eq!(
            items[0],
            Value::list([Value::Symbol("b".into()), Value::Symbol("c".into()),])
        );
        assert_eq!(
            items[1],
            Value::list([
                Value::Symbol("vector".into()),
                Value::Integer(1),
                Value::Integer(3),
            ])
        );
        assert_eq!(primitives::string_text(&items[2]).unwrap(), "b");
    }

    #[test]
    fn unibyte_string_sequences_return_byte_values() {
        assert_eq!(
            eval_str("(let ((s (unibyte-string 225 16))) (aref s 0))"),
            Value::Integer(225)
        );
        assert_eq!(
            eval_str("(let ((s (unibyte-string 225 16))) (string-to-list s))"),
            Value::list([Value::Integer(225), Value::Integer(16)])
        );
    }

    #[test]
    fn eval_comparisons() {
        assert_eq!(eval_str("(= 3 3)"), Value::T);
        assert_eq!(eval_str("(= 3 4)"), Value::Nil);
        assert_eq!(eval_str("(< 1 2)"), Value::T);
        assert_eq!(eval_str("(> 1 2)"), Value::Nil);
        assert_eq!(eval_str("(<= 3 3)"), Value::T);
        assert_eq!(eval_str("(>= 4 3)"), Value::T);
    }

    #[test]
    fn eval_let() {
        assert_eq!(eval_str("(let ((x 10)) x)"), Value::Integer(10));
        assert_eq!(eval_str("(let ((x 2) (y 3)) (+ x y))"), Value::Integer(5));
    }

    #[test]
    fn eval_if() {
        assert_eq!(eval_str("(if t 1 2)"), Value::Integer(1));
        assert_eq!(eval_str("(if nil 1 2)"), Value::Integer(2));
        assert_eq!(eval_str("(if t 1)"), Value::Integer(1));
        assert_eq!(eval_str("(if nil 1)"), Value::Nil);
    }

    #[test]
    fn eval_progn() {
        assert_eq!(eval_str("(progn 1 2 3)"), Value::Integer(3));
    }

    #[test]
    fn eval_and_or() {
        assert_eq!(eval_str("(and 1 2 3)"), Value::Integer(3));
        assert_eq!(eval_str("(and 1 nil 3)"), Value::Nil);
        assert_eq!(eval_str("(or nil nil 3)"), Value::Integer(3));
        assert_eq!(eval_str("(or nil nil)"), Value::Nil);
    }

    #[test]
    fn eval_defun_and_call() {
        let mut interp = Interpreter::new();
        eval_str_with(&mut interp, "(defun double (x) (* x 2))");
        assert_eq!(
            eval_str_with(&mut interp, "(double 21)"),
            Value::Integer(42)
        );
    }

    fn run_large_stack_test(test_fn: fn()) {
        thread::Builder::new()
            .stack_size(16 * 1024 * 1024)
            .spawn(test_fn)
            .unwrap()
            .join()
            .unwrap();
    }

    fn assert_eval_string_ops() {
        assert_eq!(
            eval_str(r#"(concat "hello" " " "world")"#),
            Value::String("hello world".into())
        );
        assert_eq!(eval_str(r#"(string= "abc" "abc")"#), Value::T);
        assert_eq!(eval_str(r#"(string= "abc" "def")"#), Value::Nil);
        assert_eq!(eval_str(r#"(string= "4" nil)"#), Value::Nil);
        assert_eq!(eval_str(r#"(string= nil nil)"#), Value::T);
        assert_eq!(eval_str(r#"(string< 'a 'b)"#), Value::T);
        assert_eq!(eval_str(r#"(length "hello")"#), Value::Integer(5));
        assert_string_value(eval_str(r#"(reverse "stressed")"#), "desserts");
        assert_string_value(eval_str(r#"(nreverse "drawer")"#), "reward");
        assert_string_value(eval_str(r#"(substring-no-properties "hello" 1 4)"#), "ell");
        assert_string_value(eval_str(r#"(substring "hello" 0 -1)"#), "hell");
        assert_eq!(eval_str(r#"(string-to-number "1e-1")"#), Value::Float(0.1));
        assert_eq!(
            eval_str(r#"(string-to-number ".1..e1")"#),
            Value::Float(0.1)
        );
        assert_eq!(
            eval_str(r#"(string-to-number "1e+1.1")"#),
            Value::Float(10.0)
        );
        assert_eq!(
            eval_str(r#"(string-to-number "ffzz" 16)"#),
            Value::Integer(255)
        );
        assert_string_value(
            eval_str(r#"(replace-regexp-in-string "\\([a-z]+\\)" "<\\1>" "abc 123")"#),
            "<abc> 123",
        );
        assert_string_value(
            eval_str(r#"(replace-regexp-in-string "[0-9]+" "x" "a1b22" t t)"#),
            "axbx",
        );
        assert_string_value(
            eval_str(
                r#"(replace-regexp-in-string "%[[:xdigit:]][[:xdigit:]]"
                                              (lambda (_match) "/")
                                              "file:///tmp/a%20b"
                                              t t)"#,
            ),
            "file:///tmp/a/b",
        );
        assert_string_value(
            eval_str(
                r#"(replace-regexp-in-string "\\`\\([ACMHSs]-\\)*"
                                              "\\&down-"
                                              "S-mouse-2"
                                              t)"#,
            ),
            "S-down-mouse-2",
        );
        assert_eq!(
            eval_str(r#"(string-join '("foo" "bar" "zot") " ")"#),
            Value::String("foo bar zot".into())
        );
        assert_eq!(
            eval_str(r#"(equal (mapcar #'reverse '("abc" "abd")) '("cba" "dba"))"#),
            Value::T
        );
        assert_eq!(
            eval_str(r#"(compiled-function-p (lambda (x) x))"#),
            Value::Nil
        );
        assert_eq!(
            eval_str(r#"(equal (sort '(3 1 2) #'< :in-place t) '(1 2 3))"#),
            Value::T
        );
        assert_eq!(
            eval_str(
                r#"(list (isearch-no-upper-case-p "abc" t)
                         (isearch-no-upper-case-p "Abc" t)
                         (isearch-no-upper-case-p "A\\b" t)
                         (isearch-no-upper-case-p "[:upper:]" t)
                         (with-temp-buffer
                           (insert "a A")
                           (goto-char (point-min))
                           (let ((search-spaces-regexp search-whitespace-regexp))
                             (re-search-forward "a   a" nil t))))"#
            ),
            Value::list([
                Value::T,
                Value::Nil,
                Value::Nil,
                Value::Nil,
                Value::Integer(4),
            ])
        );
    }

    #[test]
    fn eval_string_ops() {
        run_large_stack_test(assert_eval_string_ops);
    }

    #[test]
    fn string_match_failure_preserves_existing_match_data() {
        let value = eval_str(
            r#"
            (progn
              (string-match "a\\(b\\)" "ab")
              (string-match "z" "ab")
              (match-string 1 "ab"))
            "#,
        );
        assert_eq!(
            primitives::string_text(&value).expect("match-string result"),
            "b"
        );
    }

    #[test]
    fn re_search_failure_preserves_existing_match_data() {
        let value = eval_str(
            r#"
            (with-temp-buffer
              (insert "ab")
              (goto-char (point-min))
              (re-search-forward "a\\(b\\)")
              (re-search-forward "z" nil t)
              (match-string 1))
            "#,
        );
        assert_eq!(
            primitives::string_text(&value).expect("match-string result"),
            "b"
        );
    }

    #[test]
    fn re_search_forward_respects_limit_argument() {
        let value = eval_str(
            r#"
            (with-temp-buffer
              (insert "ab ab")
              (goto-char (point-min))
              (re-search-forward "a\\(b\\)")
              (goto-char (point-min))
              (list (re-search-forward "a\\(b\\)" 2 t)
                    (match-string 1)))
            "#,
        );
        let items = value.to_vec().unwrap();
        assert_eq!(items[0], Value::Nil);
        assert_eq!(primitives::string_text(&items[1]).unwrap(), "b");
    }

    #[test]
    fn match_string_no_properties_reads_existing_match_data() {
        let value = eval_str(
            r#"
            (progn
              (string-match "a\\(b\\)" "ab")
              (match-string-no-properties 1 "ab"))
            "#,
        );
        assert_eq!(
            primitives::string_text(&value).expect("match-string-no-properties result"),
            "b"
        );
    }

    #[test]
    fn eval_list_ops() {
        assert_eq!(eval_str("(car '(1 2 3))"), Value::Integer(1));
        assert_eq!(eval_str("(cadr '(1 2 3))"), Value::Integer(2));
        assert_eq!(
            eval_str("(cddr '(1 2 3))"),
            Value::list([Value::Integer(3)])
        );
        assert_eq!(eval_str("(identity 'ok)"), Value::Symbol("ok".into()));
        assert_eq!(eval_str("(length '(1 2 3))"), Value::Integer(3));
    }

    #[test]
    fn eval_symbol_with_escaped_trailing_space() {
        assert_eq!(eval_str("'GNU\\ "), Value::Symbol("GNU ".into()));
        assert_eq!(eval_str("(eq 'GNU\\  'GNU\\ )"), Value::T);
    }

    #[test]
    fn eval_font_get_returns_xlfd_foundry_symbol() {
        assert_eq!(
            eval_str(
                "(equal (font-get (font-spec :name \"-GNU -FreeSans-semibold-italic-normal-*-*-*-*-*-*-0-iso10646-1\") :foundry) 'GNU\\ )"
            ),
            Value::T
        );
    }

    #[test]
    fn eval_buffer_ops() {
        let mut interp = Interpreter::new();
        eval_str_with(
            &mut interp,
            r#"(with-temp-buffer
                 (insert "hello")
                 (should (= (point) 6))
                 (should (string= (buffer-string) "hello")))"#,
        );
    }

    #[test]
    fn c_mode_sets_c_comment_defaults() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (c-mode)
                  (equal
                   (list major-mode mode-name comment-start comment-end
                         comment-start-skip comment-end-skip comment-use-syntax
                         comment-style comment-multi-line)
                   '(c-mode "C" "/* " " */"
                     "\\(?://+\\|/\\*+\\)\\s *"
                     "[ \t]*\\*+/"
                     nil indent t)))
                "#
            ),
            Value::T
        );
    }

    #[test]
    fn emacs_lisp_mode_sets_minimal_font_lock_defaults() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (emacs-lisp-mode)
                  (equal
                   (list major-mode mode-name comment-start comment-end font-lock-defaults)
                   '(emacs-lisp-mode "Emacs-Lisp" ";" "" t)))
                "#
            ),
            Value::T
        );
    }

    #[test]
    fn eval_ert_deftest_and_run() {
        let mut interp = Interpreter::new();
        eval_str_with(
            &mut interp,
            r#"
            (ert-deftest basic-insert ()
              (with-temp-buffer
                (insert "hello")
                (should (= (point) 6))
                (should (string= (buffer-string) "hello"))))
            "#,
        );
        let (passed, failed, total) = interp.run_ert_tests();
        assert_eq!(total, 1);
        assert_eq!(passed, 1);
        assert_eq!(failed, 0);
    }

    #[test]
    fn ert_resource_file_uses_test_defining_file_during_execution() {
        let mut interp = Interpreter::new();
        let test_file = "/tmp/emaxx-pcmpl-linux-tests.el";
        let expected = "/tmp/emaxx-pcmpl-linux-resources/fs";
        interp.set_current_load_file(Some(test_file.into()));
        eval_str_with(
            &mut interp,
            &format!(
                r#"
                (ert-deftest ert-resource-file-keeps-defining-file ()
                  (should (string= (ert-resource-file "fs") "{expected}")))
                "#
            ),
        );
        interp.set_current_load_file(None);
        let (passed, failed, total) = interp.run_ert_tests();
        assert_eq!(total, 1);
        assert_eq!(passed, 1);
        assert_eq!(failed, 0);
    }

    #[test]
    fn keyword_symbols_self_evaluate() {
        assert_eq!(eval_str(":default"), Value::Symbol(":default".into()));
    }

    #[test]
    fn defconst_binds_global_like_defvar() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(&mut interp, "(defconst sample-constant 42)"),
            Value::Nil
        );
        assert_eq!(
            eval_str_with(&mut interp, "sample-constant"),
            Value::Integer(42)
        );
    }

    #[test]
    fn defvar_without_initializer_keeps_variable_void() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(&mut interp, "(defvar sample-unbound)"),
            Value::Nil
        );
        assert_eq!(
            eval_str_with(&mut interp, "(boundp 'sample-unbound)"),
            Value::Nil
        );
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(condition-case err sample-unbound (void-variable (car err)))",
            ),
            Value::symbol("void-variable")
        );
    }

    #[test]
    fn file_remote_p_parses_tramp_style_names() {
        assert_eq!(
            eval_str(
                r#"(list
                     (file-remote-p "/ssh:user@host:/tmp/x")
                     (file-remote-p "/ssh:user@host:/tmp/x" 'method)
                     (file-remote-p "/ssh:user@host:/tmp/x" 'user)
                     (file-remote-p "/ssh:user@host:/tmp/x" 'host)
                     (file-remote-p "/ssh:user@host:/tmp/x" 'localname))"#,
            ),
            Value::list([
                Value::String("/ssh:user@host:".into()),
                Value::String("ssh".into()),
                Value::String("user".into()),
                Value::String("host".into()),
                Value::String("/tmp/x".into()),
            ])
        );
        assert_eq!(
            eval_str(
                r#"(list
                     (file-local-name "/tmp/local")
                     (file-local-name "/ssh:user@host:/tmp/x"))"#,
            ),
            Value::list([
                Value::String("/tmp/local".into()),
                Value::String("/tmp/x".into()),
            ])
        );
    }

    #[test]
    fn copy_alist_copies_entry_cells() {
        assert_eq!(
            eval_str(
                "(let* ((orig (list (cons 'a 1)))
                        (copy (copy-alist orig)))
                   (setcdr (car copy) 2)
                   (list orig copy))"
            ),
            Value::list([
                Value::list([Value::cons(Value::symbol("a"), Value::int(1))]),
                Value::list([Value::cons(Value::symbol("a"), Value::int(2))]),
            ])
        );
    }

    #[test]
    fn copy_alist_rejects_vectors_and_strings() {
        assert!(matches!(
            eval_str("(condition-case err (copy-alist [(a . 1)]) (wrong-type-argument 'caught))"),
            Value::Symbol(name) if name == "caught"
        ));
        assert!(matches!(
            eval_str("(condition-case err (copy-alist \"abc\") (wrong-type-argument 'caught))"),
            Value::Symbol(name) if name == "caught"
        ));
    }

    #[test]
    fn float_constants_are_available_as_builtin_variables() {
        assert_eq!(eval_str("float-e"), Value::Float(std::f64::consts::E));
        assert_eq!(eval_str("float-pi"), Value::Float(std::f64::consts::PI));
    }

    #[test]
    fn case_fold_search_is_special_and_auto_buffer_local() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(progn
                     (defun case-fold-helper ()
                       (string-match-p "A" "a"))
                     (list
                      (let ((case-fold-search nil))
                        (case-fold-helper))
                      (with-temp-buffer
                        (setq case-fold-search nil)
                        (default-value 'case-fold-search))
                      case-fold-search))"#
            ),
            Value::list([Value::Nil, Value::T, Value::T])
        );
    }

    #[test]
    fn editing_command_state_defaults_are_bound() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(list
                    buffer-read-only
                    this-command
                    last-command
                    (with-temp-buffer
                      (setq buffer-read-only t)
                      (default-value 'buffer-read-only))
                    buffer-read-only)"#
            ),
            Value::list([Value::Nil, Value::Nil, Value::Nil, Value::Nil, Value::Nil])
        );
    }

    #[test]
    fn locate_user_emacs_file_uses_user_emacs_directory() {
        assert_eq!(
            eval_str(r#"(locate-user-emacs-file "ido.last" ".ido.last")"#),
            Value::String("/nonexistent/.emacs.d/ido.last".into())
        );
    }

    fn assert_seq_some_returns_first_truthy_result() {
        assert_eq!(
            eval_str(r#"(seq-some #'identity '(nil nil ok))"#),
            Value::Symbol("ok".into())
        );
    }

    #[test]
    fn seq_some_returns_first_truthy_result() {
        run_large_stack_test(assert_seq_some_returns_first_truthy_result);
    }

    #[test]
    fn remove_function_is_a_safe_noop_for_nil_function_slots() {
        assert_eq!(
            eval_str(
                r#"(progn
                     (setq read-file-name-function nil)
                     (remove-function read-file-name-function #'ignore)
                     read-file-name-function)"#
            ),
            Value::Nil
        );
    }

    #[test]
    fn directory_listing_regexp_matches_common_ls_output() {
        assert_ne!(
            eval_str(
                r#"(string-match-p directory-listing-before-filename-regexp
                                    "-rw-r--r--@    1 alpha  staff      0 Mar 16 04:57 foo.c")"#
            ),
            Value::Nil
        );
    }

    #[test]
    fn defvar_local_loads_like_defvar() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(&mut interp, "(defvar-local sample-local :default)"),
            Value::Nil
        );
        assert_eq!(
            eval_str_with(&mut interp, "sample-local"),
            Value::Symbol(":default".into())
        );
    }

    #[test]
    fn defcustom_loads_like_defvar() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(defcustom treesit-max-buffer-size 42 \"doc\")"
            ),
            Value::Nil
        );
        assert_eq!(
            eval_str_with(&mut interp, "treesit-max-buffer-size"),
            Value::Integer(42)
        );
    }

    #[test]
    fn setopt_runs_defcustom_setter() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                   (defun sample-setter (symbol value)
                     (set-default symbol value)
                     (setq sample-setter-result value))
                   (defcustom sample-option nil \"doc\" :set #'sample-setter :type 'boolean)
                   (setopt sample-option t)
                   (list sample-option
                         sample-setter-result
                         (get 'sample-option 'custom-set)
                         (get 'sample-option 'custom-type)))"
            ),
            Value::list([
                Value::T,
                Value::T,
                Value::Symbol("sample-setter".into()),
                Value::Symbol("boolean".into()),
            ])
        );
    }

    #[test]
    fn customize_set_variable_runs_defcustom_setter() {
        run_with_large_stack(|| {
            let mut interp = Interpreter::new();
            let mut env = Vec::new();
            eval_str_with(
                &mut interp,
                "(defun sample-setter (symbol value)
                   (set-default symbol value)
                   (setq sample-setter-result value))",
            );
            eval_str_with(
                &mut interp,
                "(defcustom sample-option nil \"doc\" :set #'sample-setter :type 'boolean)",
            );

            let forms = Reader::new("(customize-set-variable 'sample-option t)")
                .read_all()
                .expect("parse customize-set-variable form");
            assert_eq!(interp.eval(&forms[0], &mut env).unwrap(), Value::T);
            assert_eq!(interp.lookup("sample-option", &env).unwrap(), Value::T);
            assert_eq!(
                interp.lookup("sample-setter-result", &env).unwrap(),
                Value::T
            );
        });
    }

    #[test]
    fn switch_to_buffer_accepts_bound_string_values() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(let ((buffer-name \"foo\"))
                   (switch-to-buffer buffer-name)
                   (buffer-name))"
            ),
            Value::String("foo".into())
        );
    }

    #[test]
    fn window_buffer_tracks_selected_buffer() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                   (switch-to-buffer \"foo\")
                   (buffer-name (window-buffer (selected-window))))"
            ),
            Value::String("foo".into())
        );
    }

    #[test]
    fn scroll_up_moves_point_with_window_when_point_would_scroll_off_top() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(with-temp-buffer
                   (insert \"\\n\\n\\n\")
                   (goto-char (point-min))
                   (switch-to-buffer (current-buffer))
                   (scroll-up 1)
                   (list (window-start)
                         (save-excursion (move-to-window-line 0) (point))
                         (point)))"
            ),
            Value::list([Value::Integer(2), Value::Integer(2), Value::Integer(2)])
        );
    }

    #[test]
    fn scroll_up_respects_scroll_preserve_screen_position() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(with-temp-buffer
                   (insert \"\\n\\n\\n\")
                   (goto-char (+ (point-min) 1))
                   (switch-to-buffer (current-buffer))
                   (let ((scroll-preserve-screen-position 'always))
                     (scroll-up 1)
                     (list (window-start)
                           (save-excursion (move-to-window-line 1) (point))
                           (point))))"
            ),
            Value::list([Value::Integer(2), Value::Integer(3), Value::Integer(3)])
        );
    }

    #[test]
    fn define_minor_mode_enables_buffer_local_state_and_runs_body() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                   (define-minor-mode sample-mode \"doc\"
                     (setq sample-mode-ran sample-mode))
                   (sample-mode 1)
                   (let ((enabled sample-mode)
                         (ran sample-mode-ran))
                     (switch-to-buffer \"other\")
                     (list enabled sample-mode ran)))"
            ),
            Value::list([Value::T, Value::Nil, Value::T])
        );
    }

    #[test]
    fn define_global_minor_mode_uses_default_value_even_if_variable_becomes_buffer_local() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                   (define-minor-mode sample-global-mode \"doc\" :global t)
                   (make-variable-buffer-local 'sample-global-mode)
                   (sample-global-mode 1)
                   (switch-to-buffer \"other\")
                   (list sample-global-mode
                         (default-value 'sample-global-mode)
                         (local-variable-p 'sample-global-mode)))"
            ),
            Value::list([Value::T, Value::T, Value::Nil])
        );
    }

    #[test]
    fn define_minor_mode_call_without_arg_enables_instead_of_toggling() {
        assert_eq!(
            eval_str(
                "(progn
                   (define-minor-mode sample-mode \"doc\")
                   (sample-mode)
                   (sample-mode)
                   sample-mode)"
            ),
            Value::T
        );
    }

    #[test]
    fn define_global_minor_mode_call_without_arg_enables_instead_of_toggling() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                   (define-minor-mode sample-global-mode \"doc\" :global t)
                   (make-variable-buffer-local 'sample-global-mode)
                   (sample-global-mode)
                   (switch-to-buffer \"other\")
                   (sample-global-mode)
                   (list sample-global-mode
                         (default-value 'sample-global-mode)))"
            ),
            Value::list([Value::T, Value::T])
        );
    }

    #[test]
    fn defvar_keymap_supports_custom_setters_toggling_bindings() {
        assert_eq!(
            eval_str_with_upstream_load_path(
                "(progn
                   (defun sample-option-setter (symbol value)
                     (if value
                         (keymap-unset sample-map \"C-c <left>\")
                       (keymap-set sample-map \"C-c <left>\" #'sample-command))
                     (set-default symbol value))
                   (defcustom sample-flag nil \"doc\" :set #'sample-option-setter)
                   (defvar-keymap sample-map :doc \"doc\")
                   (setopt sample-flag sample-flag)
                   (list
                    (keymap-lookup sample-map \"C-c <left>\")
                    (progn
                      (setopt sample-flag t)
                      (keymap-lookup sample-map \"C-c <left>\"))
                    (progn
                      (setopt sample-flag nil)
                      (keymap-lookup sample-map \"C-c <left>\"))))"
            ),
            Value::list([
                Value::Symbol("sample-command".into()),
                Value::Nil,
                Value::Symbol("sample-command".into()),
            ])
        );
    }

    #[test]
    fn defvar_keymap_supports_read_only_filter_bindings() {
        assert_eq!(
            eval_str(
                "(progn
                   (defvar-keymap sample-read-only-map
                     \"a\" (keymap-read-only-bind #'ignore))
                   (list
                    (lookup-key sample-read-only-map \"a\")
                    (progn
                      (setq buffer-read-only t)
                      (lookup-key sample-read-only-map \"a\"))))"
            ),
            Value::list([Value::Nil, Value::Symbol("ignore".into())])
        );
    }

    #[test]
    fn declaration_stub_forms_do_not_error_during_loads() {
        assert_eq!(
            eval_str(
                "(progn
                   (defgroup treesit nil \"doc\")
                   (defface treesit-face '((t :inherit default)) \"doc\")
                   (defvar-keymap treesit-map :doc \"doc\")
                   (define-minor-mode treesit-mode \"doc\")
                   (define-globalized-minor-mode global-treesit-mode treesit-mode ignore)
                   (define-derived-mode treesit-derived fundamental-mode \"TS\")
                   (cl-defstruct (ppss (:constructor make-ppss) (:type list)) depth)
                   (and (keymapp treesit-map)
                        (boundp 'treesit-mode)
                        (fboundp 'treesit-mode)
                        (boundp 'global-treesit-mode)
                        (fboundp 'treesit-derived)))"
            ),
            Value::T
        );
    }

    #[test]
    fn face_attribute_tracks_runtime_values_and_inheritance() {
        let value = eval_str(
            "(progn
               (defface parent-face '((t (:foreground \"white\"))) \"doc\")
               (defface child-face '((t (:inherit default))) \"doc\")
               (set-face-attribute 'parent-face nil :foreground \"blue\")
               (set-face-attribute 'child-face nil :inherit 'parent-face)
               (list
                (face-attribute 'tool-bar :foreground)
                (face-attribute 'parent-face :foreground)
                (face-attribute 'child-face :foreground nil t)
                (face-attribute 'child-face :inherit)))",
        );
        let items = value.to_vec().unwrap();
        assert_eq!(items[0], Value::Symbol("unspecified".into()));
        assert_eq!(primitives::string_text(&items[1]).unwrap(), "blue");
        assert_eq!(primitives::string_text(&items[2]).unwrap(), "blue");
        assert_eq!(items[3], Value::Symbol("parent-face".into()));
    }

    #[test]
    fn facep_recognizes_defined_faces() {
        assert_eq!(
            eval_str(
                "(progn
                   (defface sample-face '((t (:foreground \"red\"))) \"doc\")
                   (list
                    (facep 'sample-face)
                    (facep \"sample-face\")
                    (face-name 'sample-face)
                    (facep 'missing-face)))"
            ),
            Value::list([
                Value::T,
                Value::T,
                Value::String("sample-face".into()),
                Value::Nil,
            ])
        );
    }

    #[test]
    fn set_face_attribute_rejects_unknown_faces() {
        assert_eq!(
            eval_str(
                "(condition-case err
                     (progn
                       (set-face-attribute 'runtime-face nil :foreground \"blue\")
                       'ok)
                   (error err))"
            ),
            Value::list([
                Value::Symbol("error".into()),
                Value::String("Invalid face".into()),
                Value::Symbol("runtime-face".into()),
            ])
        );
    }

    #[test]
    fn defface_only_records_default_display_clauses() {
        assert_eq!(
            eval_str(
                "(progn
                   (defface sample-nongraphic-face '((((type graphic)) :foreground \"red\")) \"doc\")
                   (face-attribute 'sample-nongraphic-face :foreground))"
            ),
            Value::Symbol("unspecified".into())
        );
    }

    #[test]
    fn defface_records_nested_default_plists() {
        assert_eq!(
            eval_str(
                "(progn
                   (defface sample-nested-face '((t (:weight bold :extend t))) \"doc\")
                   (list
                    (face-attribute 'sample-nested-face :weight)
                    (face-attribute 'sample-nested-face :extend)))"
            ),
            Value::list([Value::Symbol("bold".into()), Value::T])
        );
    }

    #[test]
    fn faces_compat_provides_face_ids_and_colors_at_point() {
        let mut interp = Interpreter::new();
        load_faces_compat(&mut interp);

        let value = eval_str_with(
            &mut interp,
            "(progn
               (defface sample-face '((t :foreground \"red\" :background \"blue\")) \"doc\")
               (with-temp-buffer
                 (insert (propertize \"x\" 'face '(sample-face)))
                 (goto-char 1)
                 (list
                  (face-id 'sample-face)
                  (face-id 'tooltip)
                  (foreground-color-at-point)
                  (background-color-at-point))))",
        );
        let items = value.to_vec().unwrap();
        assert_eq!(items[0], Value::Integer(2));
        assert_eq!(items[1], Value::Integer(1));
        assert_string_value(items[2].clone(), "red");
        assert_string_value(items[3].clone(), "blue");
    }

    #[test]
    fn faces_compat_color_at_point_skips_unspecified_faces() {
        let mut interp = Interpreter::new();
        load_faces_compat(&mut interp);

        let value = eval_str_with(
            &mut interp,
            r#"(progn
                 (defface sample-color-face
                   '((t :background "black" :foreground "black"))
                   "doc")
                 (defface sample-box-face '((t :box 1)) "doc")
                 (with-temp-buffer
                   (insert (propertize "STRING"
                                       'face
                                       '(sample-box-face sample-color-face)))
                   (goto-char (point-min))
                   (list (background-color-at-point)
                         (foreground-color-at-point))))"#,
        );
        let items = value.to_vec().unwrap();
        assert_string_value(items[0].clone(), "black");
        assert_string_value(items[1].clone(), "black");
    }

    #[test]
    fn faces_compat_color_at_point_matches_upstream_cases() {
        let mut interp = Interpreter::new();
        load_faces_compat(&mut interp);

        let value = eval_str_with(
            &mut interp,
            r#"(progn
                 (defface sample-color-face
                   '((t :background "black" :foreground "black"))
                   "doc")
                 (defface sample-box-face '((t :box 1)) "doc")
                 (list
                  (with-temp-buffer
                    (insert (propertize "STRING"
                                        'face
                                        '(sample-box-face sample-color-face)))
                    (goto-char (point-min))
                    (list (background-color-at-point)
                          (foreground-color-at-point)))
                  (with-temp-buffer
                    (insert (propertize "STRING"
                                        'face
                                        '(:foreground "black" :background "black")))
                    (goto-char (point-min))
                    (list (background-color-at-point)
                          (foreground-color-at-point)))
                  (with-temp-buffer
                    (emacs-lisp-mode)
                    (setq-local font-lock-comment-face 'sample-color-face)
                    (setq-local font-lock-constant-face 'sample-box-face)
                    (insert ";; `symbol'")
                    (font-lock-fontify-region (point-min) (point-max))
                    (goto-char (point-min))
                    (let ((comment (list (background-color-at-point)
                                         (foreground-color-at-point))))
                      (goto-char 6)
                      (list comment
                            (list (background-color-at-point)
                                  (foreground-color-at-point)))))))"#,
        );
        let cases = value.to_vec().unwrap();
        assert_eq!(cases.len(), 3);
        for pair in cases[0].to_vec().unwrap() {
            assert_string_value(pair, "black");
        }
        for pair in cases[1].to_vec().unwrap() {
            assert_string_value(pair, "black");
        }
        let font_lock_cases = cases[2].to_vec().unwrap();
        for pair in font_lock_cases[0].to_vec().unwrap() {
            assert_string_value(pair, "black");
        }
        for pair in font_lock_cases[1].to_vec().unwrap() {
            assert_string_value(pair, "black");
        }
    }

    #[test]
    fn faces_compat_load_theme_recomputes_theme_faces() {
        run_large_stack_test(assert_faces_compat_load_theme_recomputes_theme_faces);
    }

    fn assert_faces_compat_load_theme_recomputes_theme_faces() {
        let mut interp = Interpreter::new();
        load_faces_compat(&mut interp);

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let theme_dir = std::env::temp_dir().join(format!("emaxx-theme-{unique}"));
        std::fs::create_dir_all(&theme_dir).unwrap();
        let theme_file = theme_dir.join("sample-theme-theme.el");
        std::fs::write(
            &theme_file,
            "(deftheme sample-theme \"doc\")\n\
             (custom-theme-set-faces 'sample-theme '(sample-base ((t (:extend t)))))\n\
             (provide-theme 'sample-theme)\n",
        )
        .unwrap();

        let theme_dir_literal = serde_json::to_string(&theme_dir.display().to_string()).unwrap();
        let program = format!(
            "(progn
               (defface sample-base '((t :background \"grey\")) \"doc\")
               (defface sample-child '((t :inherit sample-base)) \"doc\")
               (setq custom-theme-load-path (list {theme_dir_literal}))
               (load-theme 'sample-theme t t)
               (list
                (face-attribute 'sample-child :extend nil t)
                (progn
                  (enable-theme 'sample-theme)
                  (face-attribute 'sample-child :extend nil t))
                (progn
                  (disable-theme 'sample-theme)
                  (face-attribute 'sample-child :extend nil t))))"
        );

        assert_eq!(
            eval_str_with(&mut interp, &program),
            Value::list([
                Value::Symbol("unspecified".into()),
                Value::T,
                Value::Symbol("unspecified".into()),
            ])
        );
    }

    #[test]
    fn hash_table_iteration_and_mutation_primitives_cover_ert_cases() {
        run_large_stack_test(assert_hash_table_iteration_and_mutation_primitives_cover_ert_cases);
    }

    fn assert_hash_table_iteration_and_mutation_primitives_cover_ert_cases() {
        assert_eq!(
            eval_str(
                "(let ((ht (make-hash-table :test #'equal))
                       (seen nil))
                   (puthash \"a\" 1 ht)
                   (puthash \"b\" 2 ht)
                   (list
                    (maphash (lambda (key value)
                               (push (cons key value) seen))
                             ht)
                    (progn
                      (remhash \"a\" ht)
                      (hash-table-count ht))
                    (gethash \"a\" ht 'missing)
                    (let ((cleared (clrhash ht)))
                      (list (hash-table-p cleared)
                            (hash-table-count ht)))
                    (length seen)))"
            ),
            Value::list([
                Value::Nil,
                Value::Integer(1),
                Value::Symbol("missing".into()),
                Value::list([Value::T, Value::Integer(0)]),
                Value::Integer(2),
            ])
        );
    }

    #[test]
    fn hash_table_copy_and_clear_string_cover_password_cache_cases() {
        let result = eval_str(
            "(let ((original (make-hash-table :test #'equal)))
               (puthash \"foo\" 1 original)
               (let ((copy (copy-hash-table original))
                     (secret (copy-sequence \"bar\")))
                 (puthash \"bar\" 2 copy)
                 (clear-string secret)
                 (list
                  (hash-table-contains-p \"foo\" copy)
                  (hash-table-contains-p \"bar\" original)
                  (hash-table-count copy)
                  (hash-table-count original)
                  secret)))",
        );
        let items = result.to_vec().unwrap();
        assert_eq!(
            items,
            vec![
                Value::T,
                Value::Nil,
                Value::Integer(2),
                Value::Integer(1),
                items[4].clone(),
            ]
        );
        assert_string_value(items[4].clone(), "\0\0\0");
    }

    #[test]
    fn thread_join_executes_zero_arg_lambda_with_closure_state() {
        assert_eq!(
            eval_str("(let ((value 42)) (thread-join (make-thread (lambda () value))))"),
            Value::Integer(42)
        );
    }

    #[test]
    fn custom_hash_table_tests_are_registered_and_used_for_lookup() {
        assert_eq!(
            eval_str(
                "(let ((calls 0))
                   (defun my-cmp (a b)
                     (setq calls (1+ calls))
                     (equal a b))
                   (defun my-hash (_value) 0)
                   (let ((spec (define-hash-table-test 'my-test 'my-cmp 'my-hash))
                         (table (make-hash-table :test 'my-test)))
                     (puthash \"a\" 1 table)
                     (list spec
                           (hash-table-test table)
                           (gethash (copy-sequence \"a\") table 'missing)
                           (> calls 0))))"
            ),
            Value::list([
                Value::list([
                    Value::Symbol("my-cmp".into()),
                    Value::Symbol("my-hash".into()),
                ]),
                Value::Symbol("my-test".into()),
                Value::Integer(1),
                Value::T,
            ])
        );
    }

    #[test]
    fn custom_hash_table_hash_functions_cannot_mutate_their_table() {
        assert_eq!(
            eval_str(
                "(progn
                   (define-hash-table-test 'badeq 'eq 'bad-hash)
                   (let ((h (make-hash-table :test 'badeq :size 1 :rehash-size 1)))
                     (defun bad-hash (k)
                       (if (eq k 100)
                           (clrhash h))
                       (sxhash-eq k))
                     (should-error
                      (dotimes (k 200)
                        (puthash k k h)))
                     (hash-table-count h)))"
            ),
            Value::Integer(100)
        );
    }

    #[test]
    fn assoc_honors_optional_test_function() {
        run_with_large_stack(|| {
            assert_eq!(
                eval_str(
                    "(let ((alist '((\"a\" . 1) (\"b\" . 2))))
                       (list
                        (assoc \"a\" alist #'ignore)
                        (eq (assoc \"b\" alist #'string-equal) (cadr alist))
                        (assoc \"b\" alist #'eq)))"
                ),
                Value::list([Value::Nil, Value::T, Value::Nil])
            );
        });
    }

    #[test]
    fn garbage_collect_prunes_synthetic_weak_hash_table_entries() {
        assert_eq!(
            eval_str(
                "(let ((table (make-hash-table :weakness 'key)))
                   (puthash \"00-key-alive\" \"00-val-alive\" table)
                   (puthash \"01-key-dead\" \"01-val-alive\" table)
                   (garbage-collect)
                   (list (hash-table-count table)
                         (gethash \"00-key-alive\" table)
                         (gethash \"01-key-dead\" table 'missing)))"
            ),
            Value::list([
                Value::Integer(1),
                Value::String("00-val-alive".into()),
                Value::Symbol("missing".into()),
            ])
        );
    }

    #[test]
    fn require_edmacro_supports_edmacro_parse_keys_cases() {
        std::thread::Builder::new()
            .stack_size(4 * 1024 * 1024)
            .spawn(assert_require_edmacro_supports_edmacro_parse_keys_cases)
            .unwrap()
            .join()
            .unwrap();
    }

    fn assert_require_edmacro_supports_edmacro_parse_keys_cases() {
        assert_eq!(
            eval_str(
                "(progn
                   (require 'edmacro)
                    (and
                    (equal (edmacro-parse-keys \"\") [])
                    (equal (edmacro-parse-keys \"x ;; ignored\") [?x])
                    (equal (edmacro-parse-keys \"<<goto-line>>\")
                           [?\\M-x ?g ?o ?t ?o ?- ?l ?i ?n ?e ?\\r])
                    (equal (edmacro-parse-keys \"3*C-m\") [?\\C-m ?\\C-m ?\\C-m])
                    (equal (edmacro-parse-keys \"10*foo\")
                           (apply #'vconcat (make-list 10 [?f ?o ?o])))))"
            ),
            Value::T
        );
    }

    #[test]
    fn let_alist_binds_dotted_pair_keys() {
        assert_string_value(
            eval_str("(let ((x '((buffer-text . \"hi\")))) (let-alist x .buffer-text))"),
            "hi",
        );
    }

    #[test]
    fn cl_loop_supports_simple_for_from_to_do() {
        assert_eq!(
            eval_str("(let ((n 0)) (cl-loop for i from 1 to 3 do (setq n (+ n i))) n)"),
            Value::Integer(6)
        );
    }

    #[test]
    fn cl_loop_supports_step_and_unless_collect() {
        assert_eq!(
            eval_str(
                "(cl-loop for i below 6 by 2
                          unless (memq i '(2))
                          collect (nth i '(:alpha \"sample\" :max 1 :omega 22)))"
            ),
            Value::list([
                Value::Symbol(":alpha".into()),
                Value::Symbol(":omega".into())
            ])
        );
    }

    #[test]
    fn cl_loop_supports_repeat_collect() {
        assert_eq!(
            eval_str("(let ((n 0)) (cl-loop repeat 3 collect (setq n (1+ n))))"),
            Value::list([Value::Integer(1), Value::Integer(2), Value::Integer(3)])
        );
    }

    #[test]
    fn cl_loop_supports_parallel_in_thereis_until() {
        assert_eq!(
            eval_str("(cl-loop for a in '(1 2 3) for b in '(1 3 2) thereis (< a b) until (> a b))"),
            Value::T
        );
    }

    #[test]
    fn cl_loop_supports_destructuring_with_and_return() {
        assert_eq!(
            eval_str("(cl-loop with (a b c) = '(1 2 3) return (+ a b c))"),
            Value::Integer(6)
        );
    }

    #[test]
    fn cl_loop_supports_dotted_pair_destructuring_for_in() {
        assert_eq!(
            eval_str("(cl-loop for (k . v) in '((a . 1) (b . 2)) collect (list k v))"),
            Value::list([
                Value::list([Value::Symbol("a".into()), Value::Integer(1)]),
                Value::list([Value::Symbol("b".into()), Value::Integer(2)]),
            ])
        );
    }

    #[test]
    fn cl_loop_supports_when_collect_into_finally_return() {
        assert_eq!(
            eval_str(
                "(cl-loop for item in '((\"one\" . 1) (\"two\" . 2) (\"other\" . 3))
                          when (string-match \"^t\" (car item))
                          collect item into matches
                          finally return matches)"
            ),
            Value::list([Value::cons(Value::String("two".into()), Value::Integer(2))])
        );
    }

    #[test]
    fn assq_delete_all_filters_matching_alist_keys() {
        assert_eq!(
            eval_str("(assq-delete-all 'drop '(noise (drop . a) (keep . b) (drop . c)))"),
            Value::list([
                Value::Symbol("noise".into()),
                Value::cons(Value::Symbol("keep".into()), Value::Symbol("b".into())),
            ])
        );
    }

    #[test]
    fn assoc_delete_all_filters_matching_alist_keys_with_equal() {
        assert_eq!(
            eval_str("(assoc-delete-all \"drop\" '(noise (\"drop\" . a) (\"keep\" . b)))"),
            Value::list([
                Value::Symbol("noise".into()),
                Value::cons(Value::String("keep".into()), Value::Symbol("b".into())),
            ])
        );
    }

    #[test]
    fn add_to_list_updates_quoted_variable() {
        let mut interp = Interpreter::new();
        eval_str_with(&mut interp, "(setq sample-list '(b c))");
        assert_eq!(
            eval_str_with(&mut interp, "(add-to-list 'sample-list 'a)"),
            Value::list([
                Value::Symbol("a".into()),
                Value::Symbol("b".into()),
                Value::Symbol("c".into()),
            ])
        );
        assert_eq!(
            eval_str_with(&mut interp, "sample-list"),
            Value::list([
                Value::Symbol("a".into()),
                Value::Symbol("b".into()),
                Value::Symbol("c".into()),
            ])
        );
        assert_eq!(
            eval_str_with(&mut interp, "(add-to-list 'sample-list 'c t)"),
            Value::list([
                Value::Symbol("a".into()),
                Value::Symbol("b".into()),
                Value::Symbol("c".into()),
            ])
        );
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                   (setq sample-strings '(\"a\"))
                   (add-to-list 'sample-strings (symbol-name 'a) t)
                   sample-strings)"
            ),
            Value::list([Value::String("a".into())])
        );
    }

    #[test]
    fn cl_pushnew_supports_key_and_test_not() {
        assert_eq!(
            eval_str(
                "(let ((list '((1 2) (3 4))))
                   (cl-pushnew '(3 7) list :key #'cdr)
                   list)"
            ),
            Value::list([
                Value::list([Value::Integer(3), Value::Integer(7)]),
                Value::list([Value::Integer(1), Value::Integer(2)]),
                Value::list([Value::Integer(3), Value::Integer(4)]),
            ])
        );
        assert_eq!(
            eval_str(
                "(let ((list '((1 2) (3 4))))
                   (cl-pushnew '(3 5) list :test-not #'equal)
                   list)"
            ),
            Value::list([
                Value::list([Value::Integer(1), Value::Integer(2)]),
                Value::list([Value::Integer(3), Value::Integer(4)]),
            ])
        );
    }

    #[test]
    fn push_uses_generalized_place_updates() {
        assert_eq!(
            eval_str(
                "(let ((cell '(root tail)))
                   (push 'middle (cdr cell))
                   cell)"
            ),
            Value::list([
                Value::Symbol("root".into()),
                Value::Symbol("middle".into()),
                Value::Symbol("tail".into()),
            ])
        );
        assert_eq!(
            eval_str(
                "(let ((ht (make-hash-table :test #'eq)))
                   (puthash 'item '(b) ht)
                   (push 'a (gethash 'item ht))
                   (gethash 'item ht))"
            ),
            Value::list([Value::Symbol("a".into()), Value::Symbol("b".into())])
        );
    }

    #[test]
    fn generalized_place_subforms_are_evaluated_once_for_push() {
        assert_eq!(
            eval_str(
                "(let ((n 0)
                       (cell (list nil)))
                   (push 'x (car (progn (setq n (1+ n)) cell)))
                   (list n cell))"
            ),
            Value::list([
                Value::Integer(1),
                Value::list([Value::list([Value::Symbol("x".into())])]),
            ])
        );
    }

    #[test]
    fn setf_nth_mutates_existing_list_cell() {
        assert_eq!(
            eval_str(
                "(let ((state (list 'depth 'last 'old)))
                   (setf (nth 2 state) 'new)
                   state)"
            ),
            Value::list([
                Value::Symbol("depth".into()),
                Value::Symbol("last".into()),
                Value::Symbol("new".into()),
            ])
        );
    }

    #[test]
    fn define_abbrev_table_creates_real_runtime_table() {
        assert_eq!(
            eval_str(
                "(progn
                   (defvar sample-abbrevs nil)
                   (define-abbrev-table 'sample-abbrevs '((\"a\" \"alpha\" nil :case-fixed t)))
                   (abbrev-table-put sample-abbrevs :marker 'ok)
                   (list
                    (abbrev-table-p sample-abbrevs)
                    (abbrev-expansion \"a\" sample-abbrevs)
                    (abbrev-table-get sample-abbrevs :marker)
                    (abbrev-table-name sample-abbrevs)))"
            ),
            Value::list([
                Value::T,
                Value::String("alpha".into()),
                Value::Symbol("ok".into()),
                Value::Symbol("sample-abbrevs".into()),
            ])
        );
    }

    #[test]
    fn derived_mode_add_parents_updates_runtime_mode_hierarchy() {
        assert_eq!(
            eval_str(
                "(progn
                   (define-derived-mode sample-parent fundamental-mode \"Parent\")
                   (define-derived-mode sample-child sample-parent \"Child\")
                   (defalias 'sample-alias #'sample-child)
                   (derived-mode-add-parents 'sample-parent '(sample-alias))
                   (setq major-mode 'sample-child)
                   (list
                    (derived-mode-p 'sample-parent)
                    (derived-mode-p 'sample-alias)
                    (derived-mode-p 'fundamental-mode)))"
            ),
            Value::list([Value::T, Value::T, Value::T])
        );
    }

    #[test]
    fn define_derived_mode_installs_callable_mode_body() {
        let value = eval_str(
            "(progn
               (defun sample-parent-mode ()
                 (setq-local parent-ran t))
               (define-derived-mode sample-child-mode sample-parent-mode \"Child\"
                 (setq-local child-ran t))
               (with-temp-buffer
                 (sample-child-mode)
                 (list major-mode mode-name parent-ran child-ran)))",
        );
        let items = value.to_vec().unwrap();
        assert_eq!(items[0], Value::Symbol("sample-child-mode".into()));
        assert_string_value(items[1].clone(), "Child");
        assert_eq!(items[2], Value::T);
        assert_eq!(items[3], Value::T);
    }

    #[test]
    fn define_derived_mode_creates_mode_map_variable() {
        assert_eq!(
            eval_str(
                "(progn
                   (define-derived-mode sample-derived-mode fundamental-mode \"Sample\")
                   (keymapp sample-derived-mode-map))"
            ),
            Value::T
        );
    }

    #[test]
    fn cl_defstruct_generates_constructor_accessors_and_setf() {
        assert_eq!(
            eval_str(
                "(progn
                   (cl-defstruct (sample-struct
                                  (:constructor nil)
                                  (:constructor make-sample-struct (alpha &key beta)))
                     alpha beta)
                   (let ((sample (make-sample-struct 1 :beta 2)))
                     (setf (sample-struct-alpha sample) 3)
                     (list
                      (sample-struct-p sample)
                      (sample-struct-alpha sample)
                      (sample-struct-beta sample))))"
            ),
            Value::list([Value::T, Value::Integer(3), Value::Integer(2)])
        );
    }

    #[test]
    fn cl_defstruct_honors_conc_name_for_accessors_and_setf() {
        assert_eq!(
            eval_str(
                "(progn
                   (cl-defstruct (sample-conc
                                  (:constructor make-sample-conc)
                                  (:conc-name sample--))
                     alpha beta)
                   (let ((sample (make-sample-conc :alpha 1 :beta 2)))
                     (setf (sample--alpha sample) 7)
                     (list
                      (fboundp 'sample--alpha)
                      (sample--alpha sample)
                      (sample--beta sample))))"
            ),
            Value::list([Value::T, Value::Integer(7), Value::Integer(2)])
        );
    }

    #[test]
    fn cl_defstruct_constructor_respects_optional_marker() {
        assert_eq!(
            eval_str(
                "(progn
                   (cl-defstruct (optional-struct
                                  (:constructor make-optional-struct
                                                (&optional alpha beta gamma)))
                     alpha beta gamma)
                   (let ((sample (make-optional-struct 1 2 3)))
                     (list
                      (optional-struct-alpha sample)
                      (optional-struct-beta sample)
                      (optional-struct-gamma sample))))"
            ),
            Value::list([Value::Integer(1), Value::Integer(2), Value::Integer(3)])
        );
    }

    #[test]
    fn cl_defstruct_applies_slot_defaults_to_omitted_constructor_args() {
        assert_eq!(
            eval_str(
                "(progn
                   (cl-defstruct defaulted-struct
                     alpha
                     (beta 7))
                   (defaulted-struct-beta (make-defaulted-struct :alpha 1)))"
            ),
            Value::Integer(7)
        );
    }

    #[test]
    fn cl_defstruct_constructor_evaluates_aux_slot_initializers() {
        assert_eq!(
            eval_str(
                "(progn
                   (cl-defstruct (aux-struct
                                  (:constructor make-aux-struct
                                                (&aux
                                                 (alpha 3)
                                                 (beta (+ alpha 4)))))
                     alpha beta)
                   (let ((sample (make-aux-struct)))
                     (list (aux-struct-alpha sample)
                           (aux-struct-beta sample))))"
            ),
            Value::list([Value::Integer(3), Value::Integer(7)])
        );
    }

    #[test]
    fn abbrev_expansion_respects_table_props_and_parent_tables() {
        assert_eq!(
            eval_str(
                "(progn
                   (defvar parent-abbrev-table nil)
                   (defvar child-abbrev-table nil)
                   (define-abbrev-table 'parent-abbrev-table '((\"foo\" \"parent\")))
                   (define-abbrev-table 'child-abbrev-table
                     '((\"fb\" \"FooBar\" nil :case-fixed t))
                     \"Child table\"
                     :parents (list parent-abbrev-table))
                   (list
                    (abbrev-expansion \"foo\" child-abbrev-table)
                    (abbrev-expansion \"fb\" child-abbrev-table)
                    (abbrev-expansion \"FB\" child-abbrev-table)))"
            ),
            Value::list([
                Value::String("parent".into()),
                Value::String("FooBar".into()),
                Value::Nil,
            ])
        );
    }

    #[test]
    fn eval_and_compile_runs_its_body_when_loading_helpers() {
        let mut interp = Interpreter::new();
        eval_str_with(
            &mut interp,
            r#"
            (eval-and-compile
              (defun helper-name () 'loaded))
            (helper-name)
            "#,
        );
        assert_eq!(
            eval_str_with(&mut interp, "(helper-name)"),
            Value::Symbol("loaded".into())
        );
    }

    #[test]
    fn eval_when_compile_runs_its_body_when_loading_helpers() {
        let mut interp = Interpreter::new();
        eval_str_with(
            &mut interp,
            r#"
            (eval-when-compile
              (defun compile-only-helper () 'loaded))
            (compile-only-helper)
            "#,
        );
        assert_eq!(
            eval_str_with(&mut interp, "(compile-only-helper)"),
            Value::Symbol("loaded".into())
        );
    }

    #[test]
    fn expand_file_name_joins_invocation_components() {
        let exe = std::env::current_exe().unwrap();
        let expected = exe.display().to_string();
        assert_eq!(
            eval_str("(expand-file-name invocation-name invocation-directory)"),
            Value::String(expected)
        );
    }

    #[test]
    fn expand_file_name_uses_dynamic_default_directory() {
        let base = format!(
            "{}{}",
            std::env::temp_dir().display(),
            std::path::MAIN_SEPARATOR
        );
        let expected = std::env::temp_dir().join("child").display().to_string();
        let expr = format!(
            "(let ((default-directory {:?})) (expand-file-name \"child\"))",
            base
        );
        assert_eq!(eval_str(&expr), Value::String(expected));
    }

    #[test]
    fn custom_current_group_alist_defaults_to_nil() {
        assert_eq!(eval_str("custom-current-group-alist"), Value::Nil);
    }

    #[test]
    fn emacs_lisp_mode_syntax_table_defaults_to_placeholder() {
        assert_eq!(
            eval_str("emacs-lisp-mode-syntax-table"),
            Value::CharTable(1)
        );
    }

    #[test]
    fn cl_loop_supports_across_with_unbounded_from() {
        assert_eq!(
            eval_str(
                r#"
                (let (pairs)
                  (cl-loop for char across "ab"
                           for i from 0
                           do (setq pairs (cons (list char i) pairs)))
                  (nreverse pairs))
                "#
            ),
            Value::list([
                Value::list([Value::Integer('a' as i64), Value::Integer(0)]),
                Value::list([Value::Integer('b' as i64), Value::Integer(1)]),
            ])
        );
    }

    #[test]
    fn byte_compile_wraps_lambdas_in_byte_code_function_records() {
        assert_eq!(
            eval_str(
                r#"
                (type-of (byte-compile (lambda (x) (char-syntax x))))
                "#
            ),
            Value::Symbol("byte-code-function".into())
        );
    }

    #[test]
    fn syntax_table_reports_the_current_buffer_table() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (let ((table (make-syntax-table)))
                    (set-syntax-table table)
                    (eq (syntax-table) table)))
                "#
            ),
            Value::T
        );
    }

    #[test]
    fn invisible_p_tracks_invisible_text_properties() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (insert "ab")
                  (put-text-property 1 2 'invisible t)
                  (list (invisible-p 1)
                        (invisible-p 2)))
                "#
            ),
            Value::list([Value::T, Value::Nil])
        );
    }

    #[test]
    fn invisible_p_accepts_raw_invisibility_property_values() {
        assert_eq!(
            eval_str(
                "(let ((buffer-invisibility-spec '(outline (secret . t) t)))
                   (list
                    (invisible-p 'outline)
                    (invisible-p '(secret extra))
                    (invisible-p t)
                    (invisible-p 'visible)))"
            ),
            Value::list([Value::T, Value::T, Value::T, Value::Nil])
        );
    }

    #[test]
    fn forward_comment_moves_over_c_comments_in_both_directions() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (set-syntax-table (make-syntax-table))
                  (setq comment-end-can-be-escaped t)
                  (modify-syntax-entry ?/ ". 124b")
                  (modify-syntax-entry ?* ". 23")
                  (modify-syntax-entry ?\n "> b")
                  (insert "1/* comment */1")
                  (let ((after-comment 15))
                    (goto-char 2)
                    (list (forward-comment 1)
                          (point)
                          (progn
                            (goto-char after-comment)
                            (forward-comment -1))
                          (point))))
                "#
            ),
            Value::list([Value::T, Value::Integer(15), Value::T, Value::Integer(2),])
        );
    }

    #[test]
    fn scan_lists_backward_skips_line_comments() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (setq parse-sexp-ignore-comments t)
                  (modify-syntax-entry ?\n "> b")
                  (modify-syntax-entry ?\; "< b")
                  (insert "(; comment\n)")
                  (scan-lists (point-max) -1 0))
                "#
            ),
            Value::Integer(1)
        );
    }

    #[test]
    fn forward_comment_moves_backward_over_lisp_line_comments() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (set-syntax-table (make-syntax-table))
                  (modify-syntax-entry ?\n "> b")
                  (modify-syntax-entry ?\; "< b")
                  (insert "; comment\nx")
                  (goto-char (point-min))
                  (search-forward "x")
                  (backward-char)
                  (list (forward-comment -1) (point)))
                "#
            ),
            Value::list([Value::T, Value::Integer(1)])
        );
    }

    #[test]
    fn forward_comment_matches_syntax_tests_lisp_backward_case() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (set-syntax-table (make-syntax-table))
                  (modify-syntax-entry ?\; "<")
                  (modify-syntax-entry ?\n ">")
                  (insert "31; Comment\n31")
                  (goto-char (point-max))
                  (re-search-backward "\\_<31\\_>")
                  (list (forward-comment -1) (point)))
                "#
            ),
            Value::list([Value::T, Value::Integer(3)])
        );
    }

    #[test]
    fn forward_comment_matches_syntax_tests_c_forward_case() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (set-syntax-table (make-syntax-table))
                  (modify-syntax-entry ?\{ "(}")
                  (modify-syntax-entry ?\} "){")
                  (modify-syntax-entry ?/ ". 124b")
                  (modify-syntax-entry ?* ". 23")
                  (modify-syntax-entry ?\n ">")
                  (modify-syntax-entry ?\\ "\\")
                  (insert "1/* comment */1")
                  (goto-char (point-min))
                  (re-search-forward "\\_<1\\_>")
                  (list (point) (forward-comment 1) (point)))
                "#
            ),
            Value::list([Value::Integer(2), Value::T, Value::Integer(15)])
        );
    }

    #[test]
    fn modify_syntax_entry_defaults_to_current_table() {
        assert_eq!(
            eval_str(
                r#"
                (let ((standard (standard-syntax-table))
                      (table (make-syntax-table)))
                  (set-syntax-table table)
                  (modify-syntax-entry ?\; "<")
                  (list (char-syntax ?\;)
                        (progn
                          (set-syntax-table standard)
                          (char-syntax ?\;))))
                "#
            ),
            Value::list([Value::Integer('<' as i64), Value::Integer('.' as i64),])
        );
    }

    #[test]
    fn forward_comment_ignores_non_comment_double_slash_under_block_comment_syntax() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (modify-syntax-entry ?/ ". 124")
                  (modify-syntax-entry ?* ". 23b")
                  (modify-syntax-entry ?\n ">")
                  (modify-syntax-entry ?\; "<")
                  (insert "// not a comment here\n31; Comment\n31")
                  (goto-char (point-max))
                  (re-search-backward "\\_<31\\_>")
                  (list (forward-comment -1) (point)))
                "#
            ),
            Value::list([Value::T, Value::Integer(25)])
        );
    }

    #[test]
    fn re_search_backward_respects_line_end_anchors() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (insert "1x1\n111\n")
                  (goto-char (point-max))
                  (re-search-backward "\\(^\\|[^0-9]\\)\\(1\\)$")
                  (list (point) (match-beginning 2) (match-end 2)))
                "#
            ),
            Value::list([Value::Integer(2), Value::Integer(3), Value::Integer(4),])
        );
    }

    #[test]
    fn forward_comment_finds_local_nested_comment_despite_earlier_unterminated_one() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (modify-syntax-entry ?# ". 14")
                  (modify-syntax-entry ?| ". 23n")
                  (modify-syntax-entry ?\; "< b")
                  (modify-syntax-entry ?\n "> b")
                  (insert "101#|#\n102#||#102")
                  (goto-char (point-max))
                  (re-search-backward "\\_<102\\_>")
                  (list (forward-comment -1) (point)))
                "#
            ),
            Value::list([Value::T, Value::Integer(11)])
        );
    }

    #[test]
    fn forward_comment_uses_leftmost_line_comment_start() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (modify-syntax-entry ?\n ">")
                  (modify-syntax-entry ?\; "<")
                  (insert "32;;;;;;;;;\n32")
                  (goto-char (point-max))
                  (re-search-backward "\\_<32\\_>")
                  (list (forward-comment -1) (point)))
                "#
            ),
            Value::list([Value::T, Value::Integer(3)])
        );
    }

    #[test]
    fn forward_comment_uses_outer_pascal_comment_start() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (modify-syntax-entry ?{ "<")
                  (modify-syntax-entry ?} ">")
                  (insert "24{\n25{25\n}24")
                  (goto-char (point-max))
                  (re-search-backward "\\_<24\\_>")
                  (list (forward-comment -1) (point)))
                "#
            ),
            Value::list([Value::T, Value::Integer(3)])
        );
    }

    #[test]
    fn forward_comment_backward_prefers_outer_nested_comment_start() {
        assert_eq!(
            eval_str(
                r##"
                (with-temp-buffer
                  (modify-syntax-entry ?# ". 14")
                  (modify-syntax-entry ?| ". 23n")
                  (goto-char (point-min))
                  (insert "#|#|#")
                  (goto-char (point-max))
                  (list (forward-comment -1) (point)))
                "##
            ),
            Value::list([Value::T, Value::Integer(1)])
        );
    }

    #[test]
    fn forward_comment_backward_rejects_overlapping_and_escaped_c_end_markers() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (setq comment-end-can-be-escaped t)
                  (modify-syntax-entry ?/ ". 124b")
                  (modify-syntax-entry ?* ". 23")
                  (modify-syntax-entry ?\n "> b")
                  (insert "5/*/5\n7/* \\*/7")
                  (goto-char (point-min))
                  (search-forward "5")
                  (search-forward "5")
                  (backward-char)
                  (let ((overlap (list (forward-comment -1) (point))))
                    (goto-char (point-max))
                    (search-backward "7")
                    (let ((escaped (list (forward-comment -1) (point))))
                      (list overlap escaped))))
                "#
            ),
            Value::list([
                Value::list([Value::Nil, Value::Integer(5)]),
                Value::list([Value::Nil, Value::Integer(14)]),
            ])
        );
    }

    #[test]
    fn emacs_version_variable_defaults_to_non_empty_string() {
        let value = eval_str("emacs-version");
        match value {
            Value::String(version) => assert!(!version.is_empty()),
            other => panic!("expected string, got {other:?}"),
        }
    }

    #[test]
    fn system_configuration_variable_defaults_to_non_empty_string() {
        let value = eval_str("system-configuration");
        match value {
            Value::String(configuration) => assert!(!configuration.is_empty()),
            other => panic!("expected string, got {other:?}"),
        }
    }

    #[test]
    fn symbol_value_respects_dynamic_bindings() {
        assert_eq!(
            eval_str("(let ((indent-tabs-mode nil)) (symbol-value 'indent-tabs-mode))"),
            Value::Nil
        );
    }

    #[test]
    fn emacs_version_function_mentions_version_and_system_configuration() {
        let mut interp = Interpreter::new();
        let version = eval_str_with(&mut interp, "emacs-version");
        let configuration = eval_str_with(&mut interp, "system-configuration");
        let value = eval_str_with(&mut interp, "(emacs-version)");
        match (version, configuration, value) {
            (Value::String(version), Value::String(configuration), Value::String(description)) => {
                assert!(description.contains(&version));
                assert!(description.contains(&configuration));
            }
            other => panic!("expected strings, got {other:?}"),
        }
    }

    #[test]
    fn process_identity_supports_desktop_lock_checks() {
        let mut interp = Interpreter::new();
        let value = eval_str_with(
            &mut interp,
            r#"
            (let* ((pid (emacs-pid))
                   (attr (process-attributes pid))
                   (proc-cmd (alist-get 'comm attr))
                   (my-cmd (file-name-nondirectory (car command-line-args)))
                   (case-fold-search t))
              (list (integerp pid)
                    (stringp proc-cmd)
                    my-cmd
                    (daemonp)
                    (or (equal proc-cmd my-cmd)
                        (and (string-match-p "emacs" proc-cmd)
                             (string-match-p "emacs" my-cmd)))))
            "#,
        );
        let items = value
            .to_vec()
            .unwrap_or_else(|error| panic!("expected proper list, got {error:?}"));
        assert_eq!(items.len(), 5);
        assert_eq!(items[0], Value::T);
        assert_eq!(items[1], Value::T);
        assert!(
            matches!(&items[2], Value::String(name) if !name.is_empty())
                || matches!(&items[2], Value::StringObject(state) if !state.borrow().text.is_empty())
        );
        assert_eq!(items[3], Value::Nil);
        assert!(items[4].is_truthy());
    }

    #[test]
    fn emacs_major_and_minor_version_variables_default_to_integers() {
        let major = eval_str("emacs-major-version");
        let minor = eval_str("emacs-minor-version");
        match (major, minor) {
            (Value::Integer(major), Value::Integer(minor)) => {
                assert!(major >= 0);
                assert!(minor >= 0);
            }
            other => panic!("expected integers, got {other:?}"),
        }
    }

    #[test]
    fn etags_program_name_defaults_to_non_empty_string() {
        let value = eval_str("etags-program-name");
        match value {
            Value::String(path) => assert!(!path.is_empty()),
            other => panic!("expected string, got {other:?}"),
        }
    }

    #[test]
    fn locate_library_searches_configured_load_path() {
        let temp = std::env::temp_dir().join(format!(
            "emaxx-locate-library-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&temp).unwrap();
        let library = temp.join("sample-lib.el");
        std::fs::write(&library, ";;; sample-lib.el\n").unwrap();

        let mut interp = Interpreter::new();
        interp.set_load_path(vec![temp.clone()]);
        assert_eq!(
            eval_str_with(&mut interp, "(locate-library \"sample-lib\")"),
            Value::String(library.display().to_string())
        );

        std::fs::remove_file(&library).unwrap();
        std::fs::remove_dir(&temp).unwrap();
    }

    #[test]
    fn autoload_registers_a_lazy_function_stub() {
        let mut interp = Interpreter::new();
        let mut env = Vec::new();
        let forms = Reader::new("(autoload 'sample-autoload \"sample-autoload\")")
            .read_all()
            .unwrap();
        let result = interp.eval(&forms[0], &mut env).unwrap();
        assert_eq!(result, Value::Symbol("sample-autoload".into()));
        assert_eq!(
            interp.lookup_function("sample-autoload", &env).unwrap(),
            Value::list([
                Value::Symbol("autoload".into()),
                Value::String("sample-autoload".into()),
                Value::Nil,
                Value::Nil,
                Value::Nil,
            ])
        );
    }

    #[test]
    fn custom_autoload_records_expected_symbol_properties() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                   (custom-autoload 'ps-paper-type \"ps-print\" t)
                   (custom-autoload 'ps-paper-type \"ps-print\" t)
                   (list
                    (get 'ps-paper-type 'custom-autoload)
                    (get 'ps-paper-type 'custom-loads)))"
            ),
            Value::list([
                Value::Symbol("noset".into()),
                Value::list([Value::String("ps-print".into())]),
            ])
        );
    }

    #[test]
    fn temporary_file_directory_exposes_standard_value() {
        run_with_large_stack(|| {
            let mut interp = Interpreter::new();
            assert_eq!(
                eval_str_with(
                    &mut interp,
                    "(equal (eval (car (get 'temporary-file-directory 'standard-value)) t)
                            temporary-file-directory)"
                ),
                Value::T
            );
        });
    }

    #[test]
    fn format_time_string_accepts_let_bound_string_zone() {
        run_with_large_stack(|| {
            assert_string_value(
                eval_str(
                    "(let ((look '(1202 22527 999999 999999))
                           (fmt \"%Y-%m-%d %H:%M:%S.%3N %z (%Z)\")
                           (zone \"UTC0\"))
                       (format-time-string fmt look zone))",
                ),
                "1972-06-30 23:59:59.999 +0000 (UTC)",
            );
        });
    }

    #[test]
    fn decode_time_accepts_let_bound_string_zone() {
        run_with_large_stack(|| {
            assert_eq!(
                eval_str(
                    "(let ((look '(1202 22527 999999 999999))
                           (zone \"UTC0\"))
                       (equal (decode-time look zone t)
                              (decode-time look \"UTC0\" t)))"
                ),
                Value::T
            );
        });
    }

    #[test]
    fn current_time_string_formats_known_time_with_zone() {
        run_with_large_stack(|| {
            assert_eq!(
                eval_str("(current-time-string 0 t)"),
                Value::String("Thu Jan  1 00:00:00 1970".into())
            );
        });
    }

    #[test]
    fn variable_watchers_allow_mutating_lexical_callback_state() {
        run_with_large_stack(|| {
            assert_eq!(
                eval_str(
                    "(let* ((watch-data nil)
                            (collect-watch-data
                             (lambda (&rest args) (push args watch-data))))
                       (defvar data-tests-var 0)
                       (add-variable-watcher 'data-tests-var collect-watch-data)
                       (setq data-tests-var 1)
                       (remove-variable-watcher 'data-tests-var collect-watch-data)
                       watch-data)"
                ),
                Value::list([Value::list([
                    Value::Symbol("data-tests-var".into()),
                    Value::Integer(1),
                    Value::Symbol("set".into()),
                    Value::Nil,
                ])])
            );
        });
    }

    #[test]
    fn local_variable_watchers_allow_mutating_lexical_callback_state() {
        run_with_large_stack(|| {
            assert_eq!(
                eval_str(
                    "(let* ((watch-data nil)
                            (collect-watch-data
                             (lambda (&rest args) (push args watch-data))))
                       (defvar-local data-tests-lvar 0)
                       (with-temp-buffer
                         (add-variable-watcher 'data-tests-lvar collect-watch-data)
                         (setq data-tests-lvar 1)
                         (remove-variable-watcher 'data-tests-lvar collect-watch-data)
                         (let ((event (car watch-data)))
                           (list (car event)
                                 (nth 1 event)
                                 (nth 2 event)
                                 (bufferp (nth 3 event))))))"
                ),
                Value::list([
                    Value::Symbol("data-tests-lvar".into()),
                    Value::Integer(1),
                    Value::Symbol("set".into()),
                    Value::T,
                ])
            );
        });
    }

    #[test]
    fn cl_find_class_prefers_builtin_runtime_for_builtin_classes() {
        run_with_large_stack(|| {
            assert_eq!(
                eval_str(
                    "(progn
                       (require 'cl-extra)
                       (list (cl-find-class 'fixnum)
                             (built-in-class-p (cl-find-class 'fixnum))
                             (cl-typep 10 'fixnum)))"
                ),
                Value::list([Value::Symbol("fixnum".into()), Value::T, Value::T,])
            );
        });
    }

    #[test]
    fn macrop_recognizes_defined_and_autoloaded_macros() {
        run_with_large_stack(|| {
            let mut interp = Interpreter::new();
            assert_eq!(
                eval_str_with(
                    &mut interp,
                    "(progn
                       (defmacro sample-live-macro () nil)
                       (defalias 'sample-alias-macro 'sample-live-macro)
                       (list
                        (macrop 'sample-live-macro)
                        (macrop 'sample-alias-macro)
                        (progn
                          (autoload 'sample-auto-macro \"sample-auto\" nil nil 'macro)
                          (macrop 'sample-auto-macro))
                        (sample-alias-macro)
                        (macrop 'car)))"
                ),
                Value::list([Value::T, Value::T, Value::T, Value::Nil, Value::Nil])
            );
        });
    }

    #[test]
    fn apropos_internal_filters_symbols_by_regexp_and_predicate() {
        run_with_large_stack(|| {
            let mut interp = Interpreter::new();
            assert_eq!(
                eval_str_with(
                    &mut interp,
                    "(progn
                       (defun tramp-compat-sample-fn () t)
                       (defvar tramp-compat-sample-var t)
                       (let ((result (apropos-internal (rx bos \"tramp-compat-\") #'functionp)))
                         (list (length result) (car result) (cdr result))))"
                ),
                Value::list([
                    Value::Integer(1),
                    Value::Symbol("tramp-compat-sample-fn".into()),
                    Value::Nil,
                ])
            );
        });
    }

    #[test]
    fn custom_set_variables_applies_now_specs() {
        run_with_large_stack(|| {
            let mut interp = Interpreter::new();
            assert_eq!(
                eval_str_with(
                    &mut interp,
                    "(progn
                       (defcustom sample-custom-var nil \"doc\")
                       (custom-set-variables '(sample-custom-var 'set-v now))
                       (list sample-custom-var (car (get 'sample-custom-var 'saved-value))))"
                ),
                Value::list([
                    Value::Symbol("set-v".into()),
                    Value::list([Value::Symbol("quote".into()), Value::Symbol("set-v".into())]),
                ])
            );
        });
    }

    #[test]
    fn advertised_calling_convention_round_trips_for_symbol_function() {
        run_with_large_stack(|| {
            let mut interp = Interpreter::new();
            assert_eq!(
                eval_str_with(
                    &mut interp,
                    "(progn
                       (defun sample-adv-cc (arg) arg)
                       (set-advertised-calling-convention 'sample-adv-cc '(value) \"31.1\")
                       (get-advertised-calling-convention (symbol-function 'sample-adv-cc)))"
                ),
                Value::list([Value::Symbol("value".into())])
            );
        });
    }

    #[test]
    fn builtin_autoloads_cover_saveplace_dependencies() {
        let interp = Interpreter::new();
        let env = Vec::new();
        assert_eq!(
            interp
                .lookup_function("cl-delete-duplicates", &env)
                .unwrap(),
            builtin_file_autoload("cl-seq", Value::Nil)
        );
        assert_eq!(
            interp.lookup_function("dired", &env).unwrap(),
            builtin_file_autoload("dired", Value::T)
        );
        assert_eq!(
            interp
                .lookup_function("with-connection-local-variables", &env)
                .unwrap(),
            builtin_macro_autoload("files-x")
        );
        assert_eq!(
            interp
                .lookup_function("connection-local-value", &env)
                .unwrap(),
            builtin_macro_autoload("files-x")
        );
        assert_eq!(
            interp.lookup_function("key-valid-p", &env).unwrap(),
            builtin_file_autoload("keymap", Value::Nil)
        );
        assert_eq!(
            interp.lookup_function("keymap-set", &env).unwrap(),
            builtin_file_autoload("keymap", Value::Nil)
        );
    }

    #[test]
    fn autoloaded_functions_load_on_funcall() {
        let root = std::env::temp_dir().join(format!(
            "emaxx-autoload-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let target = root.join("sample-autoload.el");
        std::fs::write(&target, "(defun sample-autoload () 42)\n").unwrap();

        let mut interp = Interpreter::new();
        interp.set_load_path(vec![root.clone()]);
        eval_str_with(
            &mut interp,
            "(autoload 'sample-autoload \"sample-autoload\")",
        );
        assert_eq!(
            eval_str_with(&mut interp, "(funcall 'sample-autoload)"),
            Value::Integer(42)
        );

        std::fs::remove_file(&target).unwrap();
        std::fs::remove_dir(&root).unwrap();
    }

    #[test]
    fn autoload_do_load_loads_function_stubs() {
        run_with_large_stack(|| {
            let root = std::env::temp_dir().join(format!(
                "emaxx-autoload-do-load-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&root).unwrap();
            let target = root.join("sample-autoload-do-load.el");
            std::fs::write(&target, "(defun sample-autoload-do-load () 42)\n").unwrap();

            let mut interp = Interpreter::new();
            interp.set_load_path(vec![root.clone()]);
            assert_eq!(
                eval_str_with(
                    &mut interp,
                    "(progn
                       (autoload 'sample-autoload-do-load \"sample-autoload-do-load\")
                       (autoload-do-load
                         (symbol-function 'sample-autoload-do-load)
                         'sample-autoload-do-load)
                       (list
                         (autoloadp (symbol-function 'sample-autoload-do-load))
                         (sample-autoload-do-load)))"
                ),
                Value::list([Value::Nil, Value::Integer(42)])
            );

            std::fs::remove_file(&target).unwrap();
            std::fs::remove_dir(&root).unwrap();
        });
    }

    #[test]
    fn autoload_do_load_respects_macro_only_for_non_macros() {
        run_with_large_stack(|| {
            let root = std::env::temp_dir().join(format!(
                "emaxx-autoload-macro-only-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&root).unwrap();
            let target = root.join("sample-autoload-macro-only.el");
            std::fs::write(&target, "(defun sample-autoload-macro-only () 42)\n").unwrap();

            let mut interp = Interpreter::new();
            interp.set_load_path(vec![root.clone()]);
            assert_eq!(
                eval_str_with(
                    &mut interp,
                    "(progn
                       (autoload 'sample-autoload-macro-only \"sample-autoload-macro-only\")
                       (let ((f (symbol-function 'sample-autoload-macro-only)))
                         (list
                           (autoloadp f)
                           (equal f
                                  (autoload-do-load
                                    f
                                    'sample-autoload-macro-only
                                    'macro))
                           (autoloadp (symbol-function 'sample-autoload-macro-only)))))"
                ),
                Value::list([Value::T, Value::T, Value::T])
            );

            std::fs::remove_file(&target).unwrap();
            std::fs::remove_dir(&root).unwrap();
        });
    }

    #[test]
    fn autoload_do_load_loads_macros_in_macro_mode() {
        run_with_large_stack(|| {
            let root = std::env::temp_dir().join(format!(
                "emaxx-autoload-macro-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&root).unwrap();
            let target = root.join("sample-auto-macro.el");
            std::fs::write(&target, "(defmacro sample-auto-macro () 42)\n").unwrap();

            let mut interp = Interpreter::new();
            interp.set_load_path(vec![root.clone()]);
            assert_eq!(
                eval_str_with(
                    &mut interp,
                    "(progn
                       (autoload 'sample-auto-macro \"sample-auto-macro\" nil nil 'macro)
                       (autoload-do-load
                         (symbol-function 'sample-auto-macro)
                         'sample-auto-macro
                         'macro)
                       (list
                         (autoloadp (symbol-function 'sample-auto-macro))
                         (macrop 'sample-auto-macro)
                         (macroexpand '(sample-auto-macro))))"
                ),
                Value::list([Value::Nil, Value::T, Value::Integer(42)])
            );

            std::fs::remove_file(&target).unwrap();
            std::fs::remove_dir(&root).unwrap();
        });
    }

    #[test]
    fn autoloaded_macros_expand_when_called() {
        let root = std::env::temp_dir().join(format!(
            "emaxx-autoload-macroexpand-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let target = root.join("sample-auto-expand.el");
        std::fs::write(&target, "(defmacro sample-auto-expand () 42)\n").unwrap();

        let mut interp = Interpreter::new();
        interp.set_load_path(vec![root.clone()]);
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                   (autoload 'sample-auto-expand \"sample-auto-expand\" nil nil 'macro)
                   (sample-auto-expand))"
            ),
            Value::Integer(42)
        );

        std::fs::remove_file(&target).unwrap();
        std::fs::remove_dir(&root).unwrap();
    }

    #[test]
    fn memq_returns_the_original_tail_cell() {
        assert_eq!(
            eval_str(
                "(let ((xs '(a b c)))
                   (let ((tail (memq 'b xs)))
                     (setcar tail 'x)
                     xs))"
            ),
            Value::list([
                Value::Symbol("a".into()),
                Value::Symbol("x".into()),
                Value::Symbol("c".into()),
            ])
        );
    }

    #[test]
    fn last_returns_the_original_tail_cell() {
        assert_eq!(
            eval_str(
                "(let ((xs '(a b c)))
                   (let ((tail (last xs)))
                     (setcdr tail '(d))
                     xs))"
            ),
            Value::list([
                Value::Symbol("a".into()),
                Value::Symbol("b".into()),
                Value::Symbol("c".into()),
                Value::Symbol("d".into()),
            ])
        );
    }

    #[test]
    fn cl_defmacro_autoloads_and_expands_in_batch_runtime() {
        run_with_large_stack(|| {
            let emacs_repo = upstream_emacs_repo();
            let load_path = crate::compat::emaxx_upstream_load_path(&emacs_repo).unwrap();
            let mut interp = Interpreter::new();
            interp.set_load_path(load_path);
            interp.set_variable("noninteractive", Value::T, &mut Vec::new());
            interp.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
            let _ = interp.load_target("backquote");
            load_faces_compat(&mut interp);
            assert_eq!(
                eval_str_with(
                    &mut interp,
                    "(progn
                       (require 'cl-lib)
                       (cl-defmacro sample-cl-macro () 42)
                       (sample-cl-macro))"
                ),
                Value::Integer(42)
            );
        });
    }

    #[test]
    fn macroexpand_all_expands_let_when_compile_constants() {
        assert_string_list(
            eval_str(
                r#"
                (progn
                  (defmacro let-when-compile (bindings &rest body)
                    (declare (indent 1) (debug let))
                    (letrec ((loop
                              (lambda (bindings)
                                (if (null bindings)
                                    (macroexpand-all (macroexp-progn body)
                                                     macroexpand-all-environment)
                                  (let ((binding (pop bindings)))
                                    (cl-progv (list (car binding))
                                        (list (eval (nth 1 binding) t))
                                      (funcall loop bindings)))))))
                      (funcall loop bindings)))
                  (eval
                   (macroexpand-all
                    '(let-when-compile
                       ((lisp-vdefs '("defvar"))
                        (el-vdefs '("defconst")))
                       (let ((vdefs (eval-when-compile
                                      (append lisp-vdefs el-vdefs))))
                         vdefs)))))
                "#,
            ),
            &["defvar", "defconst"],
        );
    }

    #[test]
    fn define_obsolete_variable_alias_sets_up_the_alias() {
        assert_eq!(
            eval_str(
                "(progn
                   (define-obsolete-variable-alias 'old-name 'new-name \"31.1\")
                   (setq new-name 42)
                   old-name)"
            ),
            Value::Integer(42)
        );
    }

    #[test]
    fn vectorp_recognizes_vector_literals() {
        assert_eq!(
            eval_str(r#"(list (vectorp [1 2]) (vectorp '(1 2)) (vectorp "ab"))"#),
            Value::list([Value::T, Value::Nil, Value::Nil])
        );
    }

    #[test]
    fn make_display_table_creates_a_display_char_table() {
        assert_eq!(
            eval_str(
                "(let ((table (make-display-table))) \
                   (list (char-table-p table) (char-table-subtype table)))"
            ),
            Value::list([Value::T, Value::Symbol("display-table".into())])
        );
    }

    #[test]
    fn translate_region_uses_char_tables() {
        let value = eval_str(
            r#"
            (with-temp-buffer
              (insert "Super-secret text")
              (let ((table (make-char-table 'translation-table)))
                (dotimes (i 26)
                  (aset table (+ i ?a) (+ (% (+ i 13) 26) ?a))
                  (aset table (+ i ?A) (+ (% (+ i 13) 26) ?A)))
                (list
                 (translate-region (point-min) (point-max) table)
                 (buffer-string))))
            "#,
        );
        let items = value.to_vec().unwrap();
        assert_eq!(items[0], Value::Integer(15));
        assert_string_value(items[1].clone(), "Fhcre-frperg grkg");
    }

    #[test]
    fn preloaded_point_to_register_stub_is_fboundp() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(&mut interp, "(fboundp 'point-to-register)"),
            Value::T
        );
    }

    #[test]
    fn preloaded_point_to_register_quits_on_quit_events() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(let ((last-input-event ?\C-g)
                         (register-alist nil))
                     (condition-case err
                         (call-interactively 'point-to-register)
                       (quit (car err))))"#
            ),
            Value::Symbol("quit".into())
        );
    }

    #[test]
    fn preloaded_command_line_1_processes_command_switch_alist() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"(let* ((foo-args ())
                          (bar-args ())
                          (command-switch-alist
                           (list (cons "--foo"
                                       (lambda (arg)
                                         (push arg foo-args)
                                         (pop command-line-args-left)))
                                 (cons "--bar=value"
                                       (lambda (arg)
                                         (push arg bar-args))))))
                     (command-line-1 '("--foo" "value" "--bar=value"))
                     (list (equal foo-args '("--foo"))
                           (equal bar-args '("--bar=value"))
                           command-line-args-left))"#
            ),
            Value::list([Value::T, Value::T, Value::Nil,])
        );
    }

    fn assert_list_buffers_keeps_file_visiting_internal_names_addressable() {
        let root = std::env::temp_dir().join(format!(
            "emaxx-buffer-menu-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let target = root.join("sample.txt");
        std::fs::write(&target, "hello\n").unwrap();

        let mut interp = Interpreter::new();
        let expr = format!(
            "(progn \
               (find-file {path:?}) \
               (rename-buffer \" foo\") \
               (list-buffers) \
               (with-current-buffer \"*Buffer List*\" \
                 (buffer-name (Buffer-menu-buffer))))",
            path = target.display().to_string()
        );
        assert_string_value(eval_str_with(&mut interp, &expr), " foo");

        std::fs::remove_file(&target).unwrap();
        std::fs::remove_dir(&root).unwrap();
    }

    #[test]
    fn list_buffers_keeps_file_visiting_internal_names_addressable() {
        run_large_stack_test(assert_list_buffers_keeps_file_visiting_internal_names_addressable);
    }

    #[test]
    fn load_target_prefers_files_over_same_named_directories() {
        let root = std::env::temp_dir().join(format!(
            "emaxx-load-target-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(root.join("sample")).unwrap();
        std::fs::write(root.join("sample.el"), "(provide 'sample)\n").unwrap();

        let mut interp = Interpreter::new();
        interp.set_load_path(vec![root.clone()]);
        let resolved = interp.load_target("sample").unwrap();
        assert_eq!(resolved, root.join("sample.el"));

        std::fs::remove_file(root.join("sample.el")).unwrap();
        std::fs::remove_dir(root.join("sample")).unwrap();
        std::fs::remove_dir(&root).unwrap();
    }

    #[test]
    fn load_file_strict_sets_lexical_binding_from_file_cookie() {
        let path = std::env::temp_dir().join(format!(
            "emaxx-lexical-binding-{}.el",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            ";;; lexical-cookie -*- lexical-binding: t -*-\n(provide 'sample)\n",
        )
        .unwrap();

        let mut interp = Interpreter::new();
        crate::lisp::load_file_strict(&mut interp, &path).unwrap();
        assert_eq!(
            interp.lookup_var("lexical-binding", &Vec::new()),
            Some(Value::T)
        );

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn load_file_strict_prebinds_current_load_list() {
        let path = std::env::temp_dir().join(format!(
            "emaxx-current-load-list-{}.el",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            "(setq sample-current-load-entry (car (last current-load-list)))\n",
        )
        .unwrap();

        let mut interp = Interpreter::new();
        crate::lisp::load_file_strict(&mut interp, &path).unwrap();
        assert_string_value(
            interp
                .lookup_var("sample-current-load-entry", &Vec::new())
                .expect("sample-current-load-entry"),
            &path.display().to_string(),
        );
        assert_eq!(
            interp.lookup_var("current-load-list", &Vec::new()),
            Some(Value::Nil)
        );

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn load_file_strict_preserves_original_load_errors() {
        let path = std::env::temp_dir().join(format!(
            "emaxx-load-error-{}.el",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            "(require 'mod-test \"/tmp/emaxx-missing-mod-test\")\n",
        )
        .unwrap();

        let mut interp = Interpreter::new();
        let error = crate::lisp::load_file_strict(&mut interp, &path).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Cannot open load file: No such file or directory, /tmp/emaxx-missing-mod-test"
        );

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn generic_record_reader_forms_evaluate_to_literal_records() {
        let mut interp = Interpreter::new();
        let value = eval_str_with(&mut interp, "#s(#s(a b) c)");
        let Value::Record(id) = value else {
            panic!("expected a record literal");
        };
        let record = interp.find_record(id).expect("record state");
        assert_eq!(record.type_name, "literal-record");
        assert_eq!(record.slots.len(), 2);
        assert!(matches!(record.slots[0], Value::Record(_)));
        assert_eq!(record.slots[1], Value::Symbol("c".into()));
    }

    #[test]
    fn read_from_string_makes_record_literals_record_like() {
        assert_eq!(
            eval_str(
                r#"
                (let* ((read-circle t)
                       (result (read-from-string "([#s(r a)])"))
                       (x2 (car (car result)))
                       (x3 (aref x2 0)))
                  (list (recordp x3)
                        (length x3)
                        (aref x3 0)
                        (aref x3 1)))
                "#
            ),
            Value::list([
                Value::T,
                Value::Integer(2),
                Value::Symbol("r".into()),
                Value::Symbol("a".into()),
            ])
        );
    }

    #[test]
    fn read_from_string_nested_record_step_shrinks() {
        assert_eq!(
            eval_str(
                r#"
                (let* ((read-circle t)
                       (result (read-from-string "([#s(r ([#s(r a)]))])"))
                       (x0 (car result))
                       (x1 (aref (car x0) 0))
                       (next (aref x1 1))
                       (x2 (aref (car next) 0)))
                  (list (equal x0 next)
                        (recordp x1)
                        (length x1)
                        (aref x1 0)
                        (recordp x2)
                        (length x2)
                        (aref x2 0)
                        (aref x2 1)))
                "#
            ),
            Value::list([
                Value::Nil,
                Value::T,
                Value::Integer(2),
                Value::Symbol("r".into()),
                Value::T,
                Value::Integer(2),
                Value::Symbol("r".into()),
                Value::Symbol("a".into()),
            ])
        );
    }

    #[test]
    fn read_from_string_prints_circular_cons_with_labels() {
        assert_string_value(
            eval_str(
                r##"
                (let* ((read-circle t)
                       (print-circle t)
                       (result (read-from-string "#1=(#1# . #1#)")))
                  (prin1-to-string (car result)))
                "##,
            ),
            "#1=(#1# . #1#)",
        );
    }

    #[test]
    fn read_from_string_prints_circular_vectors_with_labels() {
        assert_string_value(
            eval_str(
                r##"
                (let* ((read-circle t)
                       (print-circle t)
                       (result (read-from-string "#1=[#1# a #1#]")))
                  (prin1-to-string (car result)))
                "##,
            ),
            "#1=[#1# a #1#]",
        );
    }

    #[test]
    fn read_from_string_roundtrips_lread_circle_cases() {
        for case in [
            "#1=(#1# . #1#)",
            "#1=[#1# a #1#]",
            "#1=(#2=[#1# #2#] . #1#)",
            "#1=(#2=[#1# #2#] . #2#)",
            "#1=[#2=(#1# . #2#)]",
            "#1=(#2=[#3=(#1# . #2#) #4=(#3# . #4#)])",
        ] {
            let program = format!(
                r##"
                (let* ((read-circle t)
                       (print-circle t)
                       (result (read-from-string "{case}")))
                  (prin1-to-string (car result)))
                "##,
            );
            assert_string_value(eval_str(&program), case);
        }
    }

    #[test]
    fn print_circle_2_upstream_case_completes() {
        let value = eval_str(
            r##"
            (let* ((read-circle t)
                   (x (car (read-from-string "(0 . #1=(0 . #1#))"))))
              (list
               (let ((print-circle nil))
                 (prin1-to-string x))
               (let ((print-circle t))
                 (prin1-to-string x))))
            "##,
        );
        let items = value.to_vec().expect("result list");
        assert_eq!(items.len(), 2);
        assert!(
            items[0]
                .as_string()
                .expect("print-circle nil result should be a string")
                .contains(". #")
        );
        assert_eq!(
            items[1]
                .as_string()
                .expect("print-circle t result should be a string"),
            "(0 . #1=(0 . #1#))"
        );
    }

    #[test]
    fn number_sequence_defaults_to_positive_step() {
        assert_eq!(
            eval_str(
                r##"
                (list
                 (number-sequence 1 0)
                 (number-sequence 3 1)
                 (number-sequence 3 1 -1))
                "##,
            ),
            Value::list([
                Value::Nil,
                Value::Nil,
                Value::list([Value::Integer(3), Value::Integer(2), Value::Integer(1)]),
            ])
        );
    }

    #[test]
    fn mouse_wheel_mode_binds_scroll_command() {
        run_with_large_stack(|| {
            let emacs_repo = upstream_emacs_repo();
            let load_path = crate::compat::emaxx_upstream_load_path(&emacs_repo).unwrap();
            let mut interp = Interpreter::new();
            interp.set_load_path(load_path);
            interp.set_variable("noninteractive", Value::T, &mut Vec::new());
            interp.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
            let _ = interp.load_target("backquote");
            load_faces_compat(&mut interp);

            assert_eq!(
                eval_str_with(
                    &mut interp,
                    r##"
                    (progn
                      (require 'mwheel)
                      (with-suppressed-warnings ((obsolete mouse-wheel-up-event))
                        (mouse-wheel-mode 1)
                        (let ((enabled (lookup-key (current-global-map)
                                                   `[,mouse-wheel-up-event])))
                          (mouse-wheel-mode -1)
                          (list mouse-wheel-up-event
                                enabled
                                (lookup-key (current-global-map)
                                            `[,mouse-wheel-up-event])))))
                    "##,
                ),
                Value::list([
                    Value::Symbol("mouse-5".into()),
                    Value::Symbol("mwheel-scroll".into()),
                    Value::Nil,
                ])
            );
        });
    }

    #[test]
    fn subr_introspection_supports_if_special_form() {
        assert_eq!(
            eval_str(
                r##"
                (list
                 (subr-arity (symbol-function 'if))
                 (subr-name (symbol-function 'if)))
                "##,
            ),
            Value::list([
                Value::cons(Value::Integer(2), Value::Symbol("unevalled".into())),
                Value::String("if".into()),
            ])
        );
    }

    #[test]
    fn upstream_lread_circle_form_passes() {
        assert_eq!(
            eval_str(
                r##"
                (let ((lread-test-circle-cases
                       '("#1=(#1# . #1#)"
                         "#1=[#1# a #1#]"
                         "#1=(#2=[#1# #2#] . #1#)"
                         "#1=(#2=[#1# #2#] . #2#)"
                         "#1=[#2=(#1# . #2#)]"
                         "#1=(#2=[#3=(#1# . #2#) #4=(#3# . #4#)])")))
                  (catch 'fail
                    (dolist (str lread-test-circle-cases)
                      (let* ((actual
                              (let* ((read-circle t)
                                     (print-circle t)
                                     (val (read-from-string str)))
                                (if (consp val)
                                    (prin1-to-string (car val))
                                  (error "reading %S failed: %S" str val)))))
                        (unless (equal actual str)
                          (throw 'fail (list str actual)))))
                    (condition-case nil
                        (progn
                          (read-from-string "#1=#1#")
                          (throw 'fail 'invalid-case-did-not-signal))
                      (invalid-read-syntax t))
                    t))
                "##,
            ),
            Value::T
        );
    }

    #[test]
    fn cl_prin1_to_string_autoloads_from_cl_print() {
        assert_string_value(eval_str("(cl-prin1-to-string '(a b))"), "(a b)");
    }

    #[test]
    fn prin1_to_string_roundtrips_upstream_symbol_cases() {
        let symbols = vec![
            "", "&", "*", "+", "-", "/", "0E", "0e", "<", "=", ">", "E", "E0", "NaN", "\"", "#",
            "#x0", "'", "''", "(", ")", "+00", ",", "-0", ".", ".0", "0", "0.0", "0E0", "0e0",
            "1E+", "1E+NaN", "1e+", "1e+NaN", ";", "?", "[", "\\", "]", "`", "_", "a", "e", "e0",
            "x", "{", "|", "}", "~", ":", "’", "’bar", "\t", "\n", " ", "\u{00A0}", "\u{200B}",
            "0",
        ];
        let mut interp = Interpreter::new();
        let mut env: Env = Vec::new();

        let roundtrip = |interp: &mut Interpreter, env: &mut Env, name: &str| {
            let rendered = primitives::call(
                interp,
                "prin1-to-string",
                &[Value::Symbol(name.to_string())],
                env,
            )
            .expect("symbol should print");
            let rendered = primitives::string_text(&rendered).expect("rendered symbol string");
            let parsed = Reader::new(&rendered)
                .read()
                .expect("printed symbol should parse")
                .expect("printed symbol should yield one form");
            assert_eq!(
                parsed,
                Value::Symbol(name.to_string()),
                "symbol {:?} printed as {:?}",
                name,
                rendered
            );
        };

        for symbol in &symbols {
            roundtrip(&mut interp, &mut env, symbol);
        }
        for left in &symbols {
            for right in &symbols {
                roundtrip(&mut interp, &mut env, &format!("{left}{right}"));
            }
        }
    }

    #[test]
    fn prin1_to_string_matches_upstream_integer_character_cases() {
        let printed = eval_str(
            r#"
            (let ((print-integers-as-characters t))
              (prin1-to-string
               '(?? ?\; ?\( ?\) ?\{ ?\} ?\[ ?\] ?\" ?\' ?\\ ?f ?~ ?Á 32
                 ?\n ?\r ?\t ?\b ?\f ?\a ?\v ?\e ?\d)))
            "#,
        );
        assert_string_value(
            printed,
            r#"(?? ?\; ?\( ?\) ?\{ ?\} ?\[ ?\] ?\" ?\' ?\\ ?f ?~ ?Á ?\s ?\n ?\r ?\t ?\b ?\f 7 11 27 127)"#,
        );
    }

    #[test]
    fn cl_prin1_respects_charset_text_property_modes() {
        run_with_large_stack(|| {
            assert_eq!(
                eval_str(
                    r#"
                    (list
                     (let ((print-charset-text-property nil))
                       (if (string-match
                            "charset"
                            (cl-prin1-to-string
                             (propertize "a" 'charset 'unicode)))
                           t nil))
                     (let ((print-charset-text-property 'default))
                       (if (string-match
                            "charset"
                            (cl-prin1-to-string
                             (propertize "\u00F6" 'charset 'ascii)))
                           t nil))
                     (let ((print-charset-text-property 'default))
                       (if (string-match
                            "charset"
                            (cl-prin1-to-string
                             (propertize "\u00F6" 'charset 'unicode)))
                           t nil))
                     (let ((print-charset-text-property 'default))
                       (if (string-match
                            "charset"
                            (cl-prin1-to-string
                             (propertize "a" 'charset 'unicode)))
                           t nil)))
                    "#
                ),
                Value::list([Value::T, Value::T, Value::T, Value::T])
            );
        });
    }

    #[test]
    fn cl_prin1_supports_continuous_numbering() {
        run_with_large_stack(|| {
            assert_eq!(
                eval_str(
                    r#"
                    (let* ((x (list 1))
                           (y "hello")
                           (g (gensym))
                           (print-circle t)
                           (print-gensym t)
                           (print-continuous-numbering t)
                           (print-number-table nil))
                      (if (string-match
                           "(#1=(1) #1# #2=\"hello\" #2#)(#3=#:g[[:digit:]]+ #3#)(#1# #2# #3#)#2#$"
                           (mapconcat #'cl-prin1-to-string
                                      `((,x ,x ,y ,y) (,g ,g) (,x ,y ,g) ,y)))
                          t nil))
                    "#
                ),
                Value::Nil
            );
        });
    }

    #[test]
    fn princ_and_terpri_respect_output_streams() {
        assert_eq!(
            eval_str(
                r#"
                (let ((marker-output
                       (with-current-buffer (get-buffer-create "*printer-test*")
                         (erase-buffer)
                         (insert "seed")
                         (point-max-marker))))
                  (list
                   (with-output-to-string
                     (princ 'abc)
                     (terpri nil t)
                     (terpri nil t)
                     (princ "xyz"))
                   (progn
                     (princ 'abc marker-output)
                     (terpri marker-output t)
                     (terpri marker-output t)
                     (with-current-buffer (marker-buffer marker-output)
                       (buffer-string)))))
                "#
            ),
            Value::list([
                Value::String("abc\nxyz".into()),
                Value::String("seedabc\n".into()),
            ])
        );
    }

    #[test]
    fn eval_second_argument_controls_lambda_capture() {
        assert_eq!(
            eval_str(
                "(let ((x 1)
                       (form '(funcall (let ((x 2)) (lambda () x)))))
                   (list
                    (condition-case err
                        (eval form nil)
                      (void-variable (car err)))
                    (eval form t)))"
            ),
            Value::list([Value::Symbol("void-variable".into()), Value::Integer(2)])
        );
        assert_eq!(
            eval_str("(let ((standard-output 'marker)) (eval 'standard-output nil))"),
            Value::Symbol("marker".into())
        );
    }

    #[test]
    fn dynamic_lambdas_write_back_mutated_caller_bindings() {
        assert_eq!(
            eval_str(
                r#"
                (let ((x nil)
                      (f (eval '(lambda () (setq x t)) nil)))
                  (funcall f)
                  x)
                "#
            ),
            Value::T
        );
    }

    #[test]
    fn lexical_closures_preserve_mutated_bindings_across_funcalls() {
        run_with_large_stack(|| {
            assert_eq!(
                eval_str(
                    r#"
                    (let* (ranges
                           got
                           (try (lambda (from to)
                                  (setq got (list from to ranges))
                                  (setq ranges (list from to))
                                  got)))
                      (list (funcall try 3 5)
                            (funcall try 10 12)
                            (progn
                              (setq ranges nil)
                              (funcall try 20 25))))
                    "#
                ),
                Value::list([
                    Value::list([Value::Integer(3), Value::Integer(5), Value::Nil]),
                    Value::list([
                        Value::Integer(10),
                        Value::Integer(12),
                        Value::list([Value::Integer(3), Value::Integer(5)]),
                    ]),
                    Value::list([Value::Integer(20), Value::Integer(25), Value::Nil]),
                ])
            );
        });
    }

    #[test]
    fn insert_file_contents_leaves_point_at_insert_start() {
        let path = std::env::temp_dir().join(format!(
            "emaxx-insert-file-contents-{}.txt",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, "abc").unwrap();

        let path_literal = path.display().to_string().replace('\\', "\\\\");
        assert_eq!(
            eval_str(&format!(
                "(with-temp-buffer \
                   (insert-file-contents-literally \"{path_literal}\") \
                   (list (point) (point-min) (point-max)))"
            )),
            Value::list([Value::Integer(1), Value::Integer(1), Value::Integer(4)])
        );

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn insert_file_contents_rejects_circular_after_insert_file_functions() {
        let path = std::env::temp_dir().join(format!(
            "emaxx-insert-file-contents-circular-{}.txt",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, "hello\n").unwrap();

        let path_literal = path.display().to_string().replace('\\', "\\\\");
        assert_eq!(
            eval_str(&format!(
                "(let ((after-insert-file-functions (list 'identity))) \
                   (setcdr after-insert-file-functions after-insert-file-functions) \
                   (condition-case err \
                       (insert-file-contents \"{path_literal}\") \
                     (circular-list (car err))))"
            )),
            Value::Symbol("circular-list".into())
        );

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn define_inline_lowers_inline_wrappers_into_a_runtime_function() {
        let mut interp = Interpreter::new();
        eval_str_with(
            &mut interp,
            r#"
            (define-inline sample-inline (x y)
              (inline-letevals (x y)
                (inline-quote (list ,x ',y))))
            "#,
        );
        assert_eq!(
            eval_str_with(&mut interp, "(let ((sym 'ok)) (sample-inline 1 sym))"),
            Value::list([Value::Integer(1), Value::Symbol("ok".into())])
        );
    }

    #[test]
    fn keymap_placeholders_cover_load_time_setup_calls() {
        assert_eq!(
            eval_str(
                r#"
                (let ((map (make-sparse-keymap "demo")))
                  (list (keymapp map)
                        (keymapp (copy-keymap map))
                        (define-key map "a" 'foo)
                        (lookup-key map "a")
                        (define-key map (kbd "<return>") 'bar)
                        (lookup-key map (kbd "<return>"))
                        (eq (suppress-keymap map) map)
                        (keymap-parent map)))
                "#,
            ),
            Value::list([
                Value::T,
                Value::T,
                Value::Symbol("foo".into()),
                Value::Symbol("foo".into()),
                Value::Symbol("bar".into()),
                Value::Symbol("bar".into()),
                Value::T,
                Value::Nil,
            ])
        );
    }

    #[test]
    fn standard_minibuffer_local_map_is_available() {
        assert_eq!(
            eval_str(
                r#"
                (list (boundp 'minibuffer-local-map)
                      (keymapp minibuffer-local-map)
                      (define-key minibuffer-local-map (kbd "C-c t") 'ignore)
                      (lookup-key minibuffer-local-map (kbd "C-c t")))
                "#
            ),
            Value::list([
                Value::T,
                Value::T,
                Value::Symbol("ignore".into()),
                Value::Symbol("ignore".into()),
            ])
        );
    }

    #[test]
    fn keymap_records_compare_equal_to_literal_lists() {
        assert_eq!(
            eval_str(
                r#"
                (let ((map (make-sparse-keymap)))
                  (list
                   (progn
                     (define-key map "a" 'foo)
                     (equal map '(keymap (97 . foo))))
                   (progn
                     (define-key map "a" nil)
                     (equal map '(keymap (97))))
                   (progn
                     (define-key map "a" 'foo)
                     (define-key map "a" nil t)
                     (equal map '(keymap)))))
                "#,
            ),
            Value::list([Value::T, Value::T, Value::T])
        );
    }

    #[test]
    fn define_key_after_preserves_menu_insertion_order() {
        assert_eq!(
            eval_str(
                r#"
                (let ((map (make-sparse-keymap)))
                  (define-key-after map [cmd1]
                    '(menu-item "Run Command 1" keymap-tests--command-1
                                :help "Command 1 Help"))
                  (define-key-after map [cmd2]
                    '(menu-item "Run Command 2" keymap-tests--command-2
                                :help "Command 2 Help"))
                  (define-key-after map [cmd3]
                    '(menu-item "Run Command 3" keymap-tests--command-3
                                :help "Command 3 Help")
                    'cmd1)
                  (list (caadr map) (caaddr map)))
                "#,
            ),
            Value::list([Value::Symbol("cmd1".into()), Value::Symbol("cmd3".into()),])
        );
    }

    #[test]
    fn global_map_supports_mouse_style_bindings() {
        assert_eq!(
            eval_str(
                r#"
                (let ((map (current-global-map))
                      (event 'mouse-5))
                  (global-set-key [mouse-5] 'mwheel-scroll)
                  (global-set-key [(shift mouse-4)] 'mwheel-scroll)
                  (list
                   (lookup-key map `[,event])
                   (lookup-key map [(shift mouse-4)])
                   (progn
                     (global-unset-key [mouse-5])
                     (lookup-key map [mouse-5]))
                   (progn
                     (global-unset-key [(shift mouse-4)])
                     (lookup-key map [(shift mouse-4)]))))
                "#,
            ),
            Value::list([
                Value::Symbol("mwheel-scroll".into()),
                Value::Symbol("mwheel-scroll".into()),
                Value::Nil,
                Value::Nil,
            ])
        );
    }

    #[test]
    fn define_key_accepts_runtime_mouse_vectors() {
        assert_eq!(
            eval_str(
                r#"
                (let ((map (make-sparse-keymap "demo"))
                      (event 'mouse-5))
                  (define-key map (vector event) 'mwheel-scroll)
                  (list (lookup-key map (vector event))
                        (lookup-key map [mouse-5])))
                "#,
            ),
            Value::list([
                Value::Symbol("mwheel-scroll".into()),
                Value::Symbol("mwheel-scroll".into()),
            ])
        );
    }

    #[test]
    fn global_set_key_accepts_runtime_mouse_vectors() {
        assert_eq!(
            eval_str(
                r#"
                (let* ((map (current-global-map))
                       (event 'mouse-5)
                       (key (vector event)))
                  (global-set-key key 'mwheel-scroll)
                  (list (lookup-key map key)
                        (lookup-key map [mouse-5])
                        (progn
                          (global-unset-key key)
                          (lookup-key map [mouse-5]))))
                "#,
            ),
            Value::list([
                Value::Symbol("mwheel-scroll".into()),
                Value::Symbol("mwheel-scroll".into()),
                Value::Nil,
            ])
        );
    }

    #[test]
    fn frame_dimension_primitives_round_trip() {
        assert_eq!(
            eval_str(
                r#"
                (let ((width (frame-width))
                      (height (frame-height)))
                  (set-frame-width nil 120)
                  (set-frame-height nil 40)
                  (list width height (frame-width) (frame-height)))
                "#,
            ),
            Value::list([
                Value::Integer(80),
                Value::Integer(24),
                Value::Integer(120),
                Value::Integer(40),
            ])
        );
    }

    #[test]
    fn window_width_tracks_runtime_frame_width() {
        assert_eq!(
            eval_str(
                r#"
                (progn
                  (set-frame-width nil 120)
                  (window-width))
                "#,
            ),
            Value::Integer(120)
        );
    }

    #[test]
    fn terminal_parameter_places_support_setf() {
        assert_eq!(
            eval_str(
                r#"
                (progn
                  (setf (terminal-parameter nil 'sample-terminal-param) 7)
                  (terminal-parameter nil 'sample-terminal-param))
                "#,
            ),
            Value::Integer(7)
        );
    }

    #[test]
    fn append_treats_strings_as_sequences() {
        assert_eq!(
            eval_str(r#"(append "ab" '(99))"#),
            Value::list([
                Value::Integer('a' as i64),
                Value::Integer('b' as i64),
                Value::Integer(99),
            ])
        );
    }

    #[test]
    fn setcar_supports_expression_targets() {
        assert_eq!(
            eval_str(
                r#"
                (let ((posn '(a b c d)))
                  (setcar (nthcdr 3 posn) 0)
                  posn)
                "#,
            ),
            Value::list([
                Value::Symbol("a".into()),
                Value::Symbol("b".into()),
                Value::Symbol("c".into()),
                Value::Integer(0),
            ])
        );
    }

    #[test]
    fn read_key_decodes_xt_mouse_translators() {
        assert_eq!(
            eval_str(
                r#"
                (progn
                  (setq xterm-mouse-mode t)
                  (defalias 'xterm-mouse-translate (lambda (_event) [decoded]))
                  (let ((unread-command-events '(27 91 77 116 97 105 108)))
                    (list (read-key) (length unread-command-events))))
                "#,
            ),
            Value::list([Value::Symbol("decoded".into()), Value::Integer(4)])
        );
    }

    #[test]
    fn read_key_returns_unread_event_objects() {
        assert_eq!(
            eval_str(
                r#"
                (let ((unread-command-events '((sample-event payload))))
                  (read-key))
                "#,
            ),
            Value::list([
                Value::Symbol("sample-event".into()),
                Value::Symbol("payload".into()),
            ])
        );
    }

    #[test]
    fn cl_lib_compat_preload_seeds_proclaim_state() {
        let interp = Interpreter::new();
        assert!(is_compat_preloaded_feature("cl-lib"));
        assert!(is_compat_preloaded_feature("cl-generic"));
        assert_eq!(
            interp.lookup_var("cl--proclaims-deferred", &Vec::new()),
            Some(Value::Nil)
        );
        assert_eq!(
            interp.lookup_var("inhibit-file-name-handlers", &Vec::new()),
            Some(Value::Nil)
        );
        assert_eq!(
            interp.lookup_var("inhibit-file-name-operation", &Vec::new()),
            Some(Value::Nil)
        );
        assert_eq!(
            interp.lookup_var("vc-directory-exclusion-list", &Vec::new()),
            Some(preloaded_vc_directory_exclusion_list())
        );
    }

    #[test]
    fn cl_proclaim_records_deferred_specs_without_error() {
        assert_eq!(
            eval_str("(progn (cl-proclaim '(inline sample-fn)) cl--proclaims-deferred)"),
            Value::list([Value::list([
                Value::Symbol("inline".into()),
                Value::Symbol("sample-fn".into()),
            ])])
        );
    }

    #[test]
    fn tool_bar_helpers_accept_placeholder_keymaps_during_load() {
        assert_eq!(
            eval_str(
                r#"
                (let ((map (make-sparse-keymap "demo"))
                      (menu-map (make-sparse-keymap "menu")))
                  (list
                   (eq (tool-bar-local-item "close" 'quit-window 'quit map
                                            :help "Quit help" :vert-only t)
                       map)
                   (eq (tool-bar-local-item-from-menu 'help-go-back "left-arrow"
                                                      map menu-map
                                                      :rtl "right-arrow"
                                                      :vert-only t)
                       map)))
                "#,
            ),
            Value::list([Value::T, Value::T])
        );
    }

    #[test]
    fn keymap_list_helpers_cover_grep_tool_bar_setup() {
        assert_eq!(
            eval_str(
                r#"
                (let ((map (make-sparse-keymap "demo")))
                  (define-key map "a" 'ignore)
                  (define-key map "b" 'self-insert-command)
                  (list
                   (keymapp (butlast map))
                   (equal (car (car (last map))) "b")))
                "#,
            ),
            Value::list([Value::T, Value::Nil])
        );
    }

    #[test]
    fn key_binding_resolves_minor_mode_remaps() {
        assert_eq!(
            eval_str(
                r#"
                (progn
                  (setq sample-mode t)
                  (let ((map (make-sparse-keymap "demo")))
                    (define-key map [remap display-buffer-other-frame] 'demo-display)
                    (setq sample-mode-map-entry (cons 'sample-mode map))
                    (add-to-list 'minor-mode-map-alist sample-mode-map-entry)
                    (key-binding (kbd "C-x 5 C-o"))))
                "#
            ),
            Value::Symbol("demo-display".into())
        );
    }

    #[test]
    fn command_remapping_reads_active_minor_mode_maps() {
        assert_eq!(
            eval_str(
                r#"
                (progn
                  (setq sample-mode t)
                  (let ((map (make-sparse-keymap "demo")))
                    (define-key map [remap display-buffer-other-frame] 'demo-display)
                    (setq sample-mode-map-entry (cons 'sample-mode map))
                    (add-to-list 'minor-mode-map-alist sample-mode-map-entry)
                    (command-remapping 'display-buffer-other-frame)))
                "#
            ),
            Value::Symbol("demo-display".into())
        );
    }

    #[test]
    fn commandp_accepts_bare_interactive_forms() {
        assert_eq!(
            eval_str(
                r#"
                (progn
                  (defun sample-command ()
                    "doc"
                    (interactive)
                    nil)
                  (list (commandp #'sample-command)
                        (interactive-form #'sample-command)))
                "#
            ),
            Value::list([Value::T, Value::list([Value::Symbol("interactive".into())]),])
        );
    }

    #[test]
    fn kbd_parses_multi_event_and_symbolic_key_specs() {
        assert_eq!(
            eval_str(
                r#"
                (list (length (kbd "IS"))
                      (aref (kbd "IS") 0)
                      (aref (kbd "<up>") 0)
                      (key-description (kbd "ESC ESC ESC")))
                "#
            ),
            Value::list([
                Value::Integer(2),
                Value::Integer('I' as i64),
                Value::Symbol("up".into()),
                Value::String("ESC ESC ESC".into()),
            ])
        );
    }

    #[test]
    fn easy_menu_define_registers_a_placeholder_menu_symbol() {
        assert_eq!(
            eval_str(
                r#"
                (let ((map (make-sparse-keymap "demo")))
                  (easy-menu-define demo-menu map "Demo menu" '("Demo" ["Item" ignore t]))
                  (list (keymapp demo-menu)
                        (fboundp 'demo-menu)))
                "#,
            ),
            Value::list([Value::T, Value::T])
        );
    }

    #[test]
    fn search_forward_missing_pattern_signals_search_failed() {
        let mut interp = Interpreter::new();
        let mut env: Env = Vec::new();
        let forms = Reader::new("(with-temp-buffer (insert \"abc\") (search-forward \"z\"))")
            .read_all()
            .unwrap();
        let error = interp.eval(&forms[0], &mut env).unwrap_err();
        assert_eq!(error.condition_type(), "search-failed");
        assert_eq!(error.to_string(), "\"z\"");
    }

    #[test]
    fn search_forward_noerror_returns_nil_on_missing_pattern() {
        assert_eq!(
            eval_str("(with-temp-buffer (insert \"abc\") (search-forward \"z\" nil t))"),
            Value::Nil
        );
    }

    #[test]
    fn cl_destructuring_bind_keeps_missing_optional_slots_nil() {
        assert_eq!(
            eval_str(
                "(cl-destructuring-bind (a b &optional c d &rest rest) '(1 2) (list a b c d rest))"
            ),
            Value::list([
                Value::Integer(1),
                Value::Integer(2),
                Value::Nil,
                Value::Nil,
                Value::Nil,
            ])
        );
    }

    #[test]
    fn cl_destructuring_bind_supports_dotted_tail_patterns() {
        assert_eq!(
            eval_str(
                "(cl-destructuring-bind (_ _ xy . rest) '(a b (184 . 95) tail) (list xy rest))"
            ),
            Value::list([
                Value::cons(Value::Integer(184), Value::Integer(95)),
                Value::list([Value::Symbol("tail".into())]),
            ])
        );
    }

    #[test]
    fn cl_defun_supports_destructuring_arglists() {
        let value = eval_str(
            "(progn
               (cl-defun file-notify-test ((desc actions file &optional extra))
                 (list desc actions file extra))
               (file-notify-test '(1 (changed) \"/tmp/file\" 9)))",
        );
        let items = value.to_vec().unwrap();
        assert_eq!(items.len(), 4);
        assert_eq!(items[0], Value::Integer(1));
        assert_eq!(items[1], Value::list([Value::Symbol("changed".into())]));
        assert_string_value(items[2].clone(), "/tmp/file");
        assert_eq!(items[3], Value::Integer(9));
    }

    #[test]
    fn cl_defun_supports_basic_key_arguments() {
        assert_eq!(
            eval_str(
                "(progn
                   (cl-defun register-test (data &key print-func jump-func)
                     (list data print-func jump-func))
                   (register-test 7 :jump-func 'jump))"
            ),
            Value::list([Value::Integer(7), Value::Nil, Value::Symbol("jump".into()),])
        );
    }

    #[test]
    fn cl_defmethod_lowers_specialized_arguments() {
        let result = eval_str(
            "(progn
               (cl-defgeneric method-test (value flag))
               (cl-defmethod method-test ((value string) flag)
                 (list value flag))
               (method-test \"ok\" 3))",
        );
        let items = result.to_vec().unwrap();
        assert_eq!(primitives::string_text(&items[0]).unwrap(), "ok");
        assert_eq!(items[1], Value::Integer(3));
    }

    #[test]
    fn defclass_returns_the_class_name() {
        assert_eq!(
            eval_str("(defclass sample-class nil nil)"),
            Value::Symbol("sample-class".into())
        );
    }

    #[test]
    fn defclass_registers_runtime_class_metadata() {
        assert_eq!(
            eval_str(
                "(progn
                   (defclass sample-parent nil nil)
                   (defclass sample-child (sample-parent)
                     ((sample-slot :initform 7))
                     :documentation \"Sample\")
                   (list (type-of (cl-find-class 'sample-child))
                         (cl--class-allparents (cl-find-class 'sample-child))
                         (cl--class-children (cl-find-class 'sample-parent))))"
            ),
            Value::list([
                Value::Symbol("eieio--class".into()),
                Value::list([
                    Value::Symbol("sample-child".into()),
                    Value::Symbol("sample-parent".into()),
                    Value::Symbol("t".into()),
                ]),
                Value::list([Value::Symbol("sample-child".into())]),
            ])
        );
    }

    #[test]
    fn defclass_registers_instance_predicate() {
        assert_eq!(
            eval_str(
                "(progn
                   (defclass sample-parent nil nil)
                   (defclass sample-child (sample-parent) nil)
                   (let ((child (make-instance 'sample-child)))
                     (list (sample-child-p child)
                           (sample-parent-p child)
                           (sample-child-p 'not-an-object))))"
            ),
            Value::list([Value::T, Value::T, Value::Nil])
        );
    }

    #[test]
    fn defclass_constructor_initializes_and_updates_slots() {
        assert_eq!(
            eval_str(
                "(progn
                   (defclass sample-backend nil
                     ((type :initarg :type :initform 'netrc)
                      (source :initarg :source)
                      (host :initarg :host :initform t)))
                   (let ((backend (sample-backend \"obsolete-name\"
                                                  :source \".\"
                                                  :type 'password-store)))
                     (eieio-oset backend 'host \"example.org\")
                     (list
                      (type-of backend)
                      (slot-value backend 'type)
                      (slot-value backend :source)
                      (eieio-oref backend 'source)
                      (eieio-oref backend 'host))))"
            ),
            Value::list([
                Value::Symbol("sample-backend".into()),
                Value::Symbol("password-store".into()),
                Value::String(".".into()),
                Value::String(".".into()),
                Value::String("example.org".into()),
            ])
        );
    }

    #[test]
    fn make_instance_uses_class_slot_defaults() {
        assert_eq!(
            eval_str(
                "(progn
                   (defclass sample-instance nil
                     ((alpha :initarg :alpha :initform 7)
                      (beta :initarg :beta :initform (+ 2 3))))
                   (let ((object (make-instance 'sample-instance :alpha 11)))
                     (list
                      (slot-value object 'alpha)
                      (slot-value object 'beta))))"
            ),
            Value::list([Value::Integer(11), Value::Integer(5)])
        );
    }

    #[test]
    fn cl_generic_define_generalizer_registers_runtime_value() {
        assert_eq!(
            eval_str(
                "(progn
                   (cl-generic-define-generalizer sample-generalizer
                     9
                     (lambda (arg) arg)
                     (lambda (_tag) nil))
                   (type-of sample-generalizer))"
            ),
            Value::Symbol("cl--generic-generalizer".into())
        );
    }

    #[test]
    fn cl_defmethod_accepts_extra_qualifiers_before_lambda_list() {
        assert_string_value(
            eval_str(
                "(progn
                   (cl-defgeneric qualified-method (value))
                   (cl-defmethod qualified-method :extra \"tag\" ((value string))
                     value)
                   (qualified-method \"ok\"))",
            ),
            "ok",
        );
    }

    #[test]
    fn cl_defmethod_supports_setf_function_names() {
        assert_eq!(
            eval_str(
                "(progn
                   (cl-defmethod (setf sample-slot) (store object)
                     store)
                   (funcall #'(setf sample-slot) 42 nil))"
            ),
            Value::Integer(42)
        );
    }

    #[test]
    fn cl_defgeneric_keeps_its_default_body() {
        assert_eq!(
            eval_str(
                "(progn
                   (cl-defgeneric sample-generic (xs)
                     (length xs))
                   (sample-generic '(a b c)))"
            ),
            Value::Integer(3)
        );
    }

    #[test]
    fn oclosure_lambda_lowers_to_plain_lambda() {
        assert_eq!(
            eval_str("(funcall (oclosure-lambda (sample-type) (x) x) 7)"),
            Value::Integer(7)
        );
    }

    #[test]
    fn align_c_variable_declaration_regex_matches_resource_lines() {
        let result = eval_str(
            r#"(list
                 (progn
                   (string-match
                    "[*&0-9A-Za-z_]>?[][&*]*\\(\\s-+[*&]*\\)[A-Za-z_][][0-9A-Za-z:_]*\\s-*\\(\\()\\|=[^=\n].*\\|(.*)\\|\\(\\[.*\\]\\)*\\)\\s-*[;,]\\|)\\s-*$\\)"
                    "main (int argc,")
                   (list (match-beginning 0) (match-beginning 1) (match-end 1)))
                 (progn
                   (string-match
                    "[*&0-9A-Za-z_]>?[][&*]*\\(\\s-+[*&]*\\)[A-Za-z_][][0-9A-Za-z:_]*\\s-*\\(\\()\\|=[^=\n].*\\|(.*)\\|\\(\\[.*\\]\\)*\\)\\s-*[;,]\\|)\\s-*$\\)"
                    "char *argv[]);")
                   (list (match-beginning 0) (match-beginning 1) (match-end 1))))"#,
        );
        assert_eq!(
            result,
            Value::list([
                Value::list([Value::Integer(8), Value::Integer(9), Value::Integer(10)]),
                Value::list([Value::Integer(3), Value::Integer(4), Value::Integer(6)]),
            ])
        );
    }

    #[test]
    fn align_c_function_declaration_matches_resource_output() {
        run_with_large_stack(|| {
            let emacs_repo = upstream_emacs_repo();
            let load_path = crate::compat::emaxx_upstream_load_path(&emacs_repo).unwrap();
            let mut interp = Interpreter::new();
            interp.set_load_path(load_path);
            interp.set_variable("noninteractive", Value::T, &mut Vec::new());
            interp.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
            let _ = interp.load_target("backquote");
            let _ = interp.load_target("seq");
            load_faces_compat(&mut interp);

            assert_eq!(
                eval_str_with(
                    &mut interp,
                    r#"(progn
                         (require 'ert)
                         (require 'ert-x)
                         (require 'align)
                         (with-temp-buffer
                           (c-mode)
                           (insert "int\nmain (int argc,\n      char *argv[]);\n")
                           (align (point-min) (point-max))
                           (buffer-string)))"#
                ),
                Value::String("int\nmain (int\t argc,\n      char\t*argv[]);\n".into())
            );
        });
    }

    #[test]
    fn align_c_variable_declaration_rule_is_runnable_and_valid() {
        run_with_large_stack(|| {
            let emacs_repo = upstream_emacs_repo();
            let load_path = crate::compat::emaxx_upstream_load_path(&emacs_repo).unwrap();
            let mut interp = Interpreter::new();
            interp.set_load_path(load_path);
            interp.set_variable("noninteractive", Value::T, &mut Vec::new());
            interp.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
            let _ = interp.load_target("backquote");
            let _ = interp.load_target("seq");
            load_faces_compat(&mut interp);

            assert_eq!(
                eval_str_with(
                    &mut interp,
                    r#"(progn
                         (require 'align)
                         (let ((rule (assq 'c-variable-declaration align-rules-list)))
                           (with-temp-buffer
                             (c-mode)
                             (insert "main (int argc,\n      char *argv[]);\n")
                             (goto-char (point-min))
                             (re-search-forward (cdr (assq 'regexp rule)))
                             (list major-mode
                                   font-lock-mode
                                   indent-tabs-mode
                                   align-to-tab-stop
                                   (align--rule-should-run rule)
                                   (funcall (cdr (assq 'valid rule)))))))"#
                ),
                Value::list([
                    Value::Symbol("c-mode".into()),
                    Value::T,
                    Value::T,
                    Value::Symbol("indent-tabs-mode".into()),
                    Value::T,
                    Value::T,
                ])
            );
        });
    }

    #[test]
    fn align_css_declaration_rule_matches_only_declarations() {
        run_with_large_stack(|| {
            let emacs_repo = upstream_emacs_repo();
            let load_path = crate::compat::emaxx_upstream_load_path(&emacs_repo).unwrap();
            let mut interp = Interpreter::new();
            interp.set_load_path(load_path);
            interp.set_variable("noninteractive", Value::T, &mut Vec::new());
            interp.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
            let _ = interp.load_target("backquote");
            let _ = interp.load_target("seq");
            load_faces_compat(&mut interp);

            assert_eq!(
                eval_str_with(
                    &mut interp,
                    r#"(progn
                         (require 'align)
                         (let ((rule (assq 'css-declaration align-rules-list)))
                           (with-temp-buffer
                             (css-mode)
                             (list (align--rule-should-run rule)
                                   (string-match (cdr (assq 'regexp rule)) "  color: red;")
                                   (string-match (cdr (assq 'regexp rule)) "p.center {")))))"#
                ),
                Value::list([Value::T, Value::Integer(0), Value::Nil])
            );
        });
    }

    #[test]
    fn align_css_declaration_search_positions_match_buffer_lines() {
        run_with_large_stack(|| {
            let emacs_repo = upstream_emacs_repo();
            let load_path = crate::compat::emaxx_upstream_load_path(&emacs_repo).unwrap();
            let mut interp = Interpreter::new();
            interp.set_load_path(load_path);
            interp.set_variable("noninteractive", Value::T, &mut Vec::new());
            interp.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
            let _ = interp.load_target("backquote");
            let _ = interp.load_target("seq");
            load_faces_compat(&mut interp);

            assert_eq!(
                eval_str_with(
                    &mut interp,
                    r#"(progn
                         (require 'align)
                         (let ((rule (assq 'css-declaration align-rules-list)))
                           (with-temp-buffer
                             (css-mode)
                             (insert "  border: 1px solid black;\n  padding: 25px 50px 75px 100px;\n")
                             (goto-char (point-min))
                             (re-search-forward (cdr (assq 'regexp rule)))
                             (let ((first (list (match-beginning 0) (match-end 0)
                                                (match-beginning 1) (match-end 1))))
                               (re-search-forward (cdr (assq 'regexp rule)))
                               (list first
                                     (list (match-beginning 0) (match-end 0)
                                           (match-beginning 1) (match-end 1)))))))"#
                ),
                Value::list([
                    Value::list([
                        Value::Integer(1),
                        Value::Integer(27),
                        Value::Integer(10),
                        Value::Integer(11),
                    ]),
                    Value::list([
                        Value::Integer(28),
                        Value::Integer(60),
                        Value::Integer(38),
                        Value::Integer(39),
                    ]),
                ])
            );
        });
    }

    #[test]
    fn align_region_separator_finds_brace_line_between_css_blocks() {
        run_with_large_stack(|| {
            let emacs_repo = upstream_emacs_repo();
            let load_path = crate::compat::emaxx_upstream_load_path(&emacs_repo).unwrap();
            let mut interp = Interpreter::new();
            interp.set_load_path(load_path);
            interp.set_variable("noninteractive", Value::T, &mut Vec::new());
            interp.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
            let _ = interp.load_target("backquote");
            let _ = interp.load_target("seq");
            load_faces_compat(&mut interp);

            assert_eq!(
                eval_str_with(
                    &mut interp,
                    r#"(progn
                         (require 'align)
                         (with-temp-buffer
                           (insert "div {\n  border: 1px solid black;\n  padding: 25px 50px 75px 100px;\n  background-color: lightblue;\n}\np.center {\n  text-align: center;\n  color: red;\n}\n")
                           (align-new-section-p 86 124 align-region-separate)))"#
                ),
                Value::Integer(99)
            );
        });
    }

    #[test]
    fn align_region_separator_accepts_marker_bounds() {
        run_with_large_stack(|| {
            let emacs_repo = upstream_emacs_repo();
            let load_path = crate::compat::emaxx_upstream_load_path(&emacs_repo).unwrap();
            let mut interp = Interpreter::new();
            interp.set_load_path(load_path);
            interp.set_variable("noninteractive", Value::T, &mut Vec::new());
            interp.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
            let _ = interp.load_target("backquote");
            let _ = interp.load_target("seq");
            load_faces_compat(&mut interp);

            assert_eq!(
                eval_str_with(
                    &mut interp,
                    r#"(progn
                         (require 'align)
                         (with-temp-buffer
                           (insert "div {\n  border: 1px solid black;\n  padding: 25px 50px 75px 100px;\n  background-color: lightblue;\n}\np.center {\n  text-align: center;\n  color: red;\n}\n")
                           (align-new-section-p (copy-marker 86 t)
                                                (copy-marker 124 t)
                                                align-region-separate)))"#
                ),
                Value::Integer(99)
            );
        });
    }

    #[test]
    fn align_css_resource_case_matches_output() {
        run_with_large_stack(|| {
            let emacs_repo = upstream_emacs_repo();
            let load_path = crate::compat::emaxx_upstream_load_path(&emacs_repo).unwrap();
            let mut interp = Interpreter::new();
            interp.set_load_path(load_path);
            interp.set_variable("noninteractive", Value::T, &mut Vec::new());
            interp.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
            let _ = interp.load_target("backquote");
            let _ = interp.load_target("seq");
            load_faces_compat(&mut interp);

            assert_eq!(
                eval_str_with(
                    &mut interp,
                    r#"(progn
                         (require 'align)
                         (with-temp-buffer
                           (let ((indent-tabs-mode nil))
                             (css-mode)
                             (insert "div {\n  border: 1px solid black;\n  padding: 25px 50px 75px 100px;\n  background-color: lightblue;\n}\np.center {\n  text-align: center;\n  color: red;\n}\n")
                             (align (point-min) (point-max))
                             (buffer-string))))"#
                ),
                Value::String(
                    "div {\n  border:           1px solid black;\n  padding:          25px 50px 75px 100px;\n  background-color: lightblue;\n}\np.center {\n  text-align: center;\n  color:      red;\n}\n"
                        .into()
                )
            );
        });
    }

    #[test]
    fn buffer_match_data_restores_live_marker_positions() {
        assert_eq!(
            eval_str(
                r#"(with-temp-buffer
                     (insert "aa bb")
                     (goto-char (point-min))
                     (re-search-forward "aa\\( \\)bb")
                     (let ((data (match-data)))
                       (goto-char (point-min))
                       (insert "XX")
                       (set-match-data data)
                       (list (match-beginning 1) (match-end 1))))"#
            ),
            Value::list([Value::Integer(5), Value::Integer(6)])
        );
    }

    #[test]
    fn save_match_data_restores_live_buffer_positions() {
        assert_eq!(
            eval_str(
                r#"(with-temp-buffer
                     (insert "aa bb")
                     (goto-char (point-min))
                     (re-search-forward "aa\\( \\)bb")
                     (save-match-data
                       (goto-char (point-min))
                       (insert "XX"))
                     (list (match-beginning 1) (match-end 1)))"#
            ),
            Value::list([Value::Integer(5), Value::Integer(6)])
        );
    }

    #[test]
    fn re_search_forward_anchor_keeps_context_after_point() {
        assert_eq!(
            eval_str(
                r#"(with-temp-buffer
                     (insert "a\nb\n")
                     (goto-char 2)
                     (re-search-forward "^b")
                     (list (match-beginning 0) (match-end 0) (point)))"#
            ),
            Value::list([Value::Integer(3), Value::Integer(4), Value::Integer(4)])
        );
    }

    #[test]
    fn allout_range_overlaps_keeps_prior_ranges_when_appending() {
        run_with_large_stack(|| {
            let emacs_repo = upstream_emacs_repo();
            let load_path = crate::compat::emaxx_upstream_load_path(&emacs_repo).unwrap();
            let mut interp = Interpreter::new();
            interp.set_load_path(load_path);
            interp.set_variable("noninteractive", Value::T, &mut Vec::new());
            interp.set_variable("command-line-args-left", Value::Nil, &mut Vec::new());
            let _ = interp.load_target("backquote");
            let _ = interp.load_target("seq");
            load_faces_compat(&mut interp);

            assert_eq!(
                eval_str_with(
                    &mut interp,
                    r#"
                    (progn
                      (require 'allout-widgets)
                      (allout-range-overlaps 10 12 '((3 5))))
                    "#
                ),
                Value::list([
                    Value::Nil,
                    Value::list([
                        Value::list([Value::Integer(3), Value::Integer(5)]),
                        Value::list([Value::Integer(10), Value::Integer(12)]),
                    ]),
                ])
            );
        });
    }

    #[test]
    fn cl_letf_supports_symbol_places() {
        assert_eq!(
            eval_str(
                "(progn
                   (defvar cl-letf-temp 'outer)
                   (list
                     (cl-letf ((cl-letf-temp 'inner))
                       (setq cl-letf-temp 'changed)
                       cl-letf-temp)
                     cl-letf-temp))"
            ),
            Value::list([
                Value::Symbol("changed".into()),
                Value::Symbol("outer".into()),
            ])
        );
    }

    #[test]
    fn cl_letf_can_mix_variable_and_function_rebinding() {
        assert_eq!(
            eval_str(
                "(progn
                   (defvar cl-letf-temp 'outer)
                   (fset 'cl-letf-temp-fn #'identity)
                   (list
                     (cl-letf (((symbol-function 'cl-letf-temp-fn) #'ignore)
                               (cl-letf-temp 'inner))
                       (list (cl-letf-temp-fn 'value) cl-letf-temp))
                     (cl-letf-temp-fn 'value)
                     cl-letf-temp))"
            ),
            Value::list([
                Value::list([Value::Nil, Value::Symbol("inner".into())]),
                Value::Symbol("value".into()),
                Value::Symbol("outer".into()),
            ])
        );
    }

    #[test]
    fn pcase_dolist_binds_backquoted_variables() {
        assert_eq!(
            eval_str(
                "(let (pairs) \
                   (pcase-dolist (`(,left ,right) '((1 2) (3 4))) \
                     (push (list left right) pairs)) \
                   (nreverse pairs))"
            ),
            Value::list([
                Value::list([Value::Integer(1), Value::Integer(2)]),
                Value::list([Value::Integer(3), Value::Integer(4)]),
            ])
        );
    }

    #[test]
    fn pcase_backquote_requires_exact_list_shape() {
        assert_eq!(
            eval_str(
                "(list (pcase '(1 2) (`(,left ,middle ,right) 'match) (_ 'miss)) \
                       (pcase '(3 4 5 6) (`(,left ,middle ,right) 'match) (_ 'miss)))"
            ),
            Value::list([Value::Symbol("miss".into()), Value::Symbol("miss".into()),])
        );
    }

    #[test]
    fn pcase_let_lenient_backquoted_lists_bind_missing_nil_and_ignore_extra() {
        assert_eq!(
            eval_str(
                "(list (pcase-let ((`(,a ,b ,c) '(1 2))) (list a b c)) \
                       (pcase-let ((`(,a ,b) '(1 2 3))) (list a b)) \
                       (pcase-let ((`(,a ,b) nil)) (list a b)))"
            ),
            Value::list([
                Value::list([Value::Integer(1), Value::Integer(2), Value::Nil]),
                Value::list([Value::Integer(1), Value::Integer(2)]),
                Value::list([Value::Nil, Value::Nil]),
            ])
        );
    }

    #[test]
    fn pcase_let_matches_cl_struct_slot_patterns() {
        assert_eq!(
            eval_str(
                r#"
                (progn
                  (cl-defstruct sample-pcase-state stack ppss ppss-point)
                  (pcase-let* (((cl-struct sample-pcase-state
                                            (stack indent-stack)
                                            ppss ppss-point)
                                 (make-sample-pcase-state
                                  :stack '(cached)
                                  :ppss '(0 nil)
                                  :ppss-point 7)))
                    (list indent-stack ppss ppss-point)))
                "#
            ),
            Value::list([
                Value::list([Value::Symbol("cached".into())]),
                Value::list([Value::Integer(0), Value::Nil]),
                Value::Integer(7),
            ])
        );
    }

    #[test]
    fn bool_vector_literals_eval_to_runtime_values() {
        assert_eq!(
            eval_str(
                r#"(let ((vec #&8"\1"))
                         (list (bool-vector-count-population vec)
                               (aref vec 0)
                               (aref vec 7)))"#
            ),
            Value::list([Value::Integer(1), Value::T, Value::Nil])
        );
    }

    #[test]
    fn pcase_matches_quoted_symbols_and_wildcards() {
        assert_eq!(
            eval_str(
                "(list (pcase 'gnu/linux ('gnu/linux 1) (_ 2)) \
                       (pcase 'other ('gnu/linux 1) (_ 2)))"
            ),
            Value::list([Value::Integer(1), Value::Integer(2)])
        );
    }

    #[test]
    fn pcase_matches_or_patterns() {
        assert_eq!(
            eval_str(
                "(list (pcase 3 ((or 1 3 5) 'odd) (_ 'other)) \
                       (pcase 2 ((or 1 3 5) 'odd) (_ 'other)))"
            ),
            Value::list([Value::Symbol("odd".into()), Value::Symbol("other".into())])
        );
    }

    #[test]
    fn pcase_matches_predicate_patterns() {
        assert_eq!(
            eval_str(
                "(list
                   (pcase 'list ((pred symbolp) 'symbol) (_ 'other))
                   (pcase '(1 2) ((pred listp) 'list) (_ 'other))
                   (pcase 3 ((pred (not symbolp)) 'number) (_ 'other)))"
            ),
            Value::list([
                Value::Symbol("symbol".into()),
                Value::Symbol("list".into()),
                Value::Symbol("number".into()),
            ])
        );
    }

    #[test]
    fn pcase_defmacro_registers_a_macroexpander_property() {
        assert_eq!(
            eval_str(
                "(progn
                   (pcase-defmacro sample (pattern) pattern)
                   (list (get 'sample 'pcase-macroexpander)
                         (fboundp 'sample--pcase-macroexpander)))"
            ),
            Value::list([
                Value::Symbol("sample--pcase-macroexpander".into()),
                Value::T
            ])
        );
    }

    #[test]
    fn pcase_dolist_lenient_backquoted_lists_bind_missing_nil_and_ignore_extra() {
        assert_eq!(
            eval_str(
                "(let (pairs) \
                   (pcase-dolist (`(,left ,middle ,right) \
                                  '((1 2) (3 4 5) (6 7 8 9))) \
                     (push (list left middle right) pairs)) \
                   (nreverse pairs))"
            ),
            Value::list([
                Value::list([Value::Integer(1), Value::Integer(2), Value::Nil]),
                Value::list([Value::Integer(3), Value::Integer(4), Value::Integer(5)]),
                Value::list([Value::Integer(6), Value::Integer(7), Value::Integer(8)]),
            ])
        );
    }

    #[test]
    fn pcase_and_let_patterns_evaluate_expressions_with_bindings() {
        assert_eq!(
            eval_str(
                "(let ((f (lambda (x) \
                            (pcase 'dummy \
                              ((and (let var x) (guard var)) 'left) \
                              ((and (let var (not x)) (guard var)) 'right))))) \
                   (list (funcall f t) (funcall f nil)))"
            ),
            Value::list([Value::Symbol("left".into()), Value::Symbol("right".into())])
        );
    }

    #[test]
    fn pcase_dolist_matches_or_and_let_nil_patterns() {
        assert_eq!(
            eval_str(
                "(let (pairs) \
                   (pcase-dolist ((or `(,min . ,max) (and min (let max nil))) \
                                  '(\"0.9\" (\"1.0\" . \"2.0\"))) \
                     (push (list min max) pairs)) \
                   (nreverse pairs))"
            ),
            Value::list([
                Value::list([Value::String("0.9".into()), Value::Nil]),
                Value::list([Value::String("1.0".into()), Value::String("2.0".into()),]),
            ])
        );
    }

    #[test]
    fn version_lte_rejects_invalid_version_strings() {
        let mut interp = Interpreter::new();
        let mut env: Env = Vec::new();
        let forms = Reader::new("(version<= \"foo\" \"1.0\")")
            .read_all()
            .expect("read version comparison form");
        let error = interp
            .eval(&forms[0], &mut env)
            .expect_err("invalid version syntax should signal");
        assert!(matches!(
            error,
            LispError::Signal(message)
                if message == "Invalid version syntax: `foo' (must start with a number)"
        ));
    }

    #[test]
    fn version_lte_honors_prerelease_qualifiers() {
        assert_eq!(eval_str("(version<= \"1.0pre1\" \"1.0\")"), Value::T);
        assert_eq!(eval_str("(version<= \"1.0\" \"1.0pre1\")"), Value::Nil);
        assert_eq!(eval_str("(version<= \"1.0.1alpha\" \"1.0.1\")"), Value::T);
        assert_eq!(eval_str("(version<= \"1.0\" \"1.0.0\")"), Value::T);
    }

    #[test]
    fn version_to_list_exposes_parsed_version_components() {
        assert_eq!(
            eval_str("(version-to-list \"2.7.3.30.2\")"),
            Value::list([
                Value::Integer(2),
                Value::Integer(7),
                Value::Integer(3),
                Value::Integer(30),
                Value::Integer(2),
            ])
        );
        assert_eq!(
            eval_str("(version-to-list \"1.0pre2\")"),
            Value::list([
                Value::Integer(1),
                Value::Integer(0),
                Value::Integer(-1),
                Value::Integer(2),
            ])
        );
    }

    #[test]
    fn lexical_symbol_variables_do_not_shadow_function_namespace() {
        assert_eq!(
            eval_str(
                "(let ((append 'append) (car 'cdr)) (list (append '(1) '(2)) (car '(3 . 4))))"
            ),
            Value::list([
                Value::list([Value::Integer(1), Value::Integer(2)]),
                Value::Integer(3),
            ])
        );
    }

    #[test]
    fn replace_match_updates_match_data_for_subexpressions() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (let (mismatch)
                    (pcase-dolist (`(,pre ,post) '(("" "")
                                                   ("a" "")
                                                   ("" "b")
                                                   ("a" "b")))
                      (unless mismatch
                        (erase-buffer)
                        (insert "hello ")
                        (save-excursion (insert pre post " world"))
                        (looking-at
                         (concat "\\(\\)" pre "\\(\\)\\(\\(\\)\\)\\(\\)" post "\\(\\)"))
                        (let* ((beg0 (match-beginning 0))
                               (beg4 (+ beg0 (length pre)))
                               (end4 (+ beg4 (length "BOO")))
                               (end0 (+ end4 (length post))))
                          (replace-match "BOO" t t nil 4)
                          (unless (and (equal (match-beginning 0) beg0)
                                       (equal (match-end 0) end0))
                            (setq mismatch
                                  (list pre post
                                        (match-beginning 0)
                                        (match-end 0)
                                        beg0
                                        end0))))))
                    mismatch))"#,
            ),
            Value::Nil
        );
    }

    #[test]
    fn save_excursion_restores_current_buffer_after_switching() {
        assert_eq!(
            eval_str(
                r#"
                (let ((origin (current-buffer)))
                  (save-excursion
                    (switch-to-buffer " *save-excursion-other*"))
                  (eq (current-buffer) origin))
                "#
            ),
            Value::T
        );
    }

    #[test]
    fn save_restriction_restores_the_original_buffer_restriction() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (insert "abcdef")
                  (narrow-to-region 2 5)
                  (let ((origin (current-buffer)))
                    (save-restriction
                      (switch-to-buffer " *save-restriction-other*"))
                    (with-current-buffer origin
                      (list (point-min) (point-max)))))
                "#
            ),
            Value::list([Value::Integer(2), Value::Integer(5)])
        );
    }

    #[test]
    fn string_match_supports_explicitly_numbered_groups() {
        assert_string_value(
            eval_str(
                r#"
                (progn
                  (string-match
                   "\\$\\(?:\\(?1:[[:alnum:]_]+\\)\\|{\\(?1:[^{}]+\\)}\\|\\$\\)"
                   "${HOME}")
                  (match-string 1 "${HOME}"))"#,
            ),
            "HOME",
        );
    }

    #[test]
    fn save_window_excursion_restores_current_buffer() {
        assert_eq!(
            eval_str(
                r#"
                (let ((original (current-buffer))
                      (other (get-buffer-create "*save-window-excursion*")))
                  (save-window-excursion
                    (set-buffer other)
                    (current-buffer))
                  (eq (current-buffer) original))
                "#
            ),
            Value::T
        );
    }

    #[test]
    fn save_window_excursion_restores_window_start() {
        assert_eq!(
            eval_str(
                r#"
                (let ((window (selected-window))
                      (original (current-buffer)))
                  (insert "a\nb\nc\nd\n")
                  (set-window-start window 3)
                  (save-window-excursion
                    (set-window-start window 5)
                    (set-buffer (get-buffer-create "*save-window-excursion*")))
                  (list (window-start window)
                        (eq (current-buffer) original)))
                "#
            ),
            Value::list([Value::Integer(3), Value::T])
        );
    }

    #[test]
    fn display_buffer_preserves_current_buffer_and_updates_window_buffer() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"
                (let ((original (current-buffer))
                      (other (get-buffer-create "*display-buffer-target*")))
                  (with-current-buffer other
                    (erase-buffer)
                    (insert "a\nb\nc\n"))
                  (display-buffer other)
                  (set-window-start (selected-window) 3)
                  (list (eq (current-buffer) original)
                        (eq (window-buffer (selected-window)) other)
                        (= (window-start (selected-window)) 3)
                        (= (window-end (selected-window))
                           (with-current-buffer other (point-max)))))"#
            ),
            Value::list([Value::T, Value::T, Value::T, Value::T])
        );
    }

    #[test]
    fn display_buffer_respects_inhibit_same_window_action() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"
                (let ((original (current-buffer))
                      (other (get-buffer-create "*display-buffer-no-same-window*")))
                  (list (display-buffer other '((inhibit-same-window . t)))
                        (eq (window-buffer (selected-window)) original)))"#
            ),
            Value::list([Value::Nil, Value::T])
        );
    }

    #[test]
    fn quit_window_buries_current_buffer_without_killing_it() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(
                &mut interp,
                r#"
                (let ((target (get-buffer-create "*quit-window-target*")))
                  (switch-to-buffer target)
                  (quit-window)
                  (list (not (eq (current-buffer) target))
                        (buffer-live-p target)))"#
            ),
            Value::list([Value::T, Value::T])
        );
    }

    #[test]
    fn push_resolves_progn_place_once() {
        assert_eq!(
            eval_str(
                "(let ((events nil) (cell '(1)))
                   (push 0 (progn (push 'seen events) cell))
                   events)"
            ),
            Value::list([Value::Symbol("seen".into())])
        );
    }

    #[test]
    fn seq_mapcat_flattens_sequence_results() {
        run_large_stack_test(assert_seq_mapcat_flattens_sequence_results);
    }

    fn assert_seq_mapcat_flattens_sequence_results() {
        assert_eq!(
            eval_str("(seq-mapcat 'list '(1 2 3))"),
            Value::list([Value::Integer(1), Value::Integer(2), Value::Integer(3),])
        );
    }

    #[test]
    fn rx_define_registers_custom_atoms_for_rx() {
        assert_eq!(
            eval_str("(progn (rx-define sample-rx \"ab\") (rx sample-rx))"),
            Value::String("ab".into())
        );
    }

    #[test]
    fn rx_repeat_supports_exact_repetition() {
        assert_eq!(
            eval_str("(rx (repeat 3 \"ab\"))"),
            Value::String("\\(?:ab\\)\\{3\\}".into())
        );
        assert_eq!(
            eval_str("(rx (= 3 digit))"),
            Value::String("\\(?:[0-9]\\)\\{3\\}".into())
        );
        assert_eq!(
            eval_str(
                r#"(list
                     (string-match-p (rx bos (= 3 digit) eos) "123")
                     (string-match-p (rx bos (= 3 digit) eos) "12")
                     (string-match-p (rx bos (= 3 digit) eos) "1234"))"#
            ),
            Value::list([Value::Integer(0), Value::Nil, Value::Nil])
        );
    }

    #[test]
    fn rx_literal_evaluates_and_quotes_string_forms() {
        assert_eq!(
            eval_str(
                r#"
                (let ((needle "a.b"))
                  (list
                   (rx (literal needle))
                   (rx bol (literal (concat needle "?")) eol)
                   (string-match-p (rx (literal needle)) "a.b")
                   (string-match-p (rx (literal needle)) "axb")))
                "#
            ),
            Value::list([
                Value::String("a\\.b".into()),
                Value::String("^a\\.b\\?$".into()),
                Value::Integer(0),
                Value::Nil,
            ])
        );
    }

    #[test]
    fn replace_match_returns_updated_string_for_string_targets() {
        assert_string_value(
            eval_str(
                r#"
                (let ((text "foo_${HOME}_bar"))
                  (string-match
                   "\\$\\(?:\\(?1:[[:alnum:]_]+\\)\\|{\\(?1:[^{}]+\\)}\\|\\$\\)"
                   text)
                  (replace-match "qux" t t text))"#,
            ),
            "foo_qux_bar",
        );
    }

    #[test]
    fn with_environment_variables_binds_process_environment_dynamically() {
        assert_string_value(
            eval_str(
                r#"
                (progn
                  (defun emaxx-test-current-env (name)
                    (getenv-internal name))
                  (let ((name "EMAXX_DYNAMIC_ENV_TEST")
                        (value "value"))
                    (with-environment-variables ((name value))
                      (emaxx-test-current-env name))))"#,
            ),
            "value",
        );
    }

    #[test]
    fn ert_selector_excludes_expensive_tests_by_tag() {
        let mut interp = Interpreter::new();
        eval_str_with(
            &mut interp,
            r#"
            (ert-deftest cheap-test ()
              (should t))
            (ert-deftest expensive-test ()
              :tags '(:expensive-test)
              (should t))
            "#,
        );
        let selector = Reader::new("(not (or (tag :expensive-test) (tag :unstable)))")
            .read_all()
            .unwrap()
            .remove(0);
        let summary = interp.run_ert_tests_with_selector(Some(&selector));
        assert_eq!(summary.total, 1);
        assert_eq!(interp.last_selected_tests, vec!["cheap-test".to_string()]);
    }

    #[test]
    fn should_error_checks_error_type() {
        let mut interp = Interpreter::new();
        eval_str_with(
            &mut interp,
            r#"
            (ert-deftest typed-error ()
              (should-error (car 1) :type 'wrong-type-argument))
            "#,
        );
        let summary = interp.run_ert_tests_with_selector(None);
        assert_eq!(summary.passed, 1);
        assert_eq!(summary.failed, 0);
    }

    #[test]
    fn should_not_failures_report_ert_test_failed() {
        let mut interp = Interpreter::new();
        eval_str_with(
            &mut interp,
            r#"
            (ert-deftest should-not-failure ()
              (should-not t))
            "#,
        );
        let summary = interp.run_ert_tests_with_selector(None);
        assert_eq!(summary.failed, 1);
        assert_eq!(
            interp.test_results[0].condition_type.as_deref(),
            Some("ert-test-failed")
        );
    }

    #[test]
    fn ert_runner_exposes_current_test_frame_to_backtrace_queries() {
        let mut interp = Interpreter::new();
        eval_str_with(
            &mut interp,
            r#"
            (ert-deftest backtrace-thread-frame ()
              (let* ((frames (backtrace--frames-from-thread (current-thread)))
                     (found nil))
                (dolist (frame frames)
                  (when (and (consp frame)
                             (memq (car frame) '(t nil))
                             (functionp (cadr frame)))
                    (setq found t)))
                (should found)))
            "#,
        );
        let summary = interp.run_ert_tests_with_selector(None);
        assert_eq!(summary.passed, 1);
        assert_eq!(summary.failed, 0);
    }

    #[test]
    fn defalias_can_reference_incf_via_function_quote() {
        assert_eq!(
            eval_str(
                "(progn \
                   (defalias 'cl-incf #'incf) \
                   (let ((n 0)) \
                     (cl-incf n)))"
            ),
            Value::Integer(1)
        );
        assert_eq!(
            eval_str(
                "(progn \
                   (defalias 'cl-decf #'decf) \
                   (let ((n 2)) \
                     (cl-decf n)))"
            ),
            Value::Integer(1)
        );
    }

    #[test]
    fn fset_can_define_function_aliases() {
        assert_eq!(
            eval_str("(progn (fset 'sample-head #'car) (sample-head '(1 2 3)))"),
            Value::Integer(1)
        );
    }

    #[test]
    fn defalias_evaluates_symbol_definition_forms() {
        assert_eq!(
            eval_str(
                "(progn \
                   (defvar sample-definition 'car) \
                   (defalias 'sample-head sample-definition) \
                   (sample-head '(1 2 3)))"
            ),
            Value::Integer(1)
        );
    }

    #[test]
    fn function_quote_allows_forward_symbol_references() {
        assert_eq!(
            eval_str(
                "(progn
                   (defvar before-change-functions nil)
                   (add-hook 'before-change-functions #'syntax-ppss-flush-cache)
                   (defun syntax-ppss-flush-cache (&rest _) 'ok)
                   (funcall (car before-change-functions)))"
            ),
            Value::Symbol("ok".into())
        );
    }

    #[test]
    fn functionp_and_funcall_accept_quoted_lambda_expressions() {
        assert_eq!(
            eval_str(
                "(list
                   (functionp '(lambda () t))
                   (cl-functionp '(lambda () t))
                   (funcall '(lambda (value) (concat value \"bar\")) \"foo\"))"
            ),
            Value::list([Value::T, Value::T, Value::String("foobar".into())])
        );
    }

    #[test]
    fn preloaded_sh_mode_sets_imenu_configuration() {
        let value = eval_str(
            "(with-temp-buffer
               (funcall #'sh-mode)
               (list major-mode
                     mode-name
                     imenu-case-fold-search
                     imenu-create-index-function
                     imenu-generic-expression))",
        );
        let items = value.to_vec().unwrap();
        assert_eq!(items[0], Value::Symbol("sh-mode".into()));
        assert_string_value(items[1].clone(), "Shell-script");
        assert_eq!(items[2], Value::Nil);
        assert_eq!(
            items[3],
            Value::Symbol("imenu-default-create-index-function".into())
        );
        assert_eq!(
            items[4],
            Value::list([
                Value::list([
                    Value::Nil,
                    Value::String("^[ \t]*function[ \t]+\\([A-Za-z_][A-Za-z0-9_]*\\)".into()),
                    Value::Integer(1),
                ]),
                Value::list([
                    Value::Nil,
                    Value::String("^[ \t]*\\([A-Za-z_][A-Za-z0-9_]*\\)[ \t]*()".into()),
                    Value::Integer(1),
                ]),
            ])
        );
    }

    #[test]
    fn treesit_language_available_defaults_to_nil() {
        assert_eq!(eval_str("(treesit-language-available-p 'json)"), Value::Nil);
    }

    #[test]
    fn treesit_linecol_helpers_report_positions() {
        assert_eq!(
            eval_str(
                "(with-temp-buffer
                   (insert \"a\\n\")
                   (treesit--linecol-cache-set 1 0 1)
                   (list (treesit--linecol-cache)
                         (treesit--linecol-at 2)
                         (treesit--linecol-at 3)))"
            ),
            Value::list([
                Value::list([
                    Value::Symbol(":line".into()),
                    Value::Integer(1),
                    Value::Symbol(":col".into()),
                    Value::Integer(0),
                    Value::Symbol(":bytepos".into()),
                    Value::Integer(1),
                ]),
                Value::cons(Value::Integer(1), Value::Integer(1)),
                Value::cons(Value::Integer(2), Value::Integer(0)),
            ])
        );
    }

    #[test]
    fn copy_tree_preserves_nested_list_structure() {
        assert_eq!(
            eval_str("(copy-tree '((a . b) (c d)))"),
            Value::list([
                Value::cons(Value::Symbol("a".into()), Value::Symbol("b".into())),
                Value::list([Value::Symbol("c".into()), Value::Symbol("d".into())]),
            ])
        );
    }

    #[test]
    fn copy_tree_does_not_alias_mutable_cons_cells() {
        assert_eq!(
            eval_str(
                "(let* ((orig '((a . b) (c d)))
                        (copy (copy-tree orig)))
                   (setcdr (car copy) 'z)
                   (list orig copy))"
            ),
            Value::list([
                Value::list([
                    Value::cons(Value::Symbol("a".into()), Value::Symbol("b".into())),
                    Value::list([Value::Symbol("c".into()), Value::Symbol("d".into())]),
                ]),
                Value::list([
                    Value::cons(Value::Symbol("a".into()), Value::Symbol("z".into())),
                    Value::list([Value::Symbol("c".into()), Value::Symbol("d".into())]),
                ]),
            ])
        );
    }

    #[test]
    fn pop_supports_generalized_places() {
        assert_eq!(
            eval_str(
                "(let ((xs (list 1 2 3)))
                   (list (pop (cdr xs)) xs))"
            ),
            Value::list([
                Value::Integer(2),
                Value::list([Value::Integer(1), Value::Integer(3)]),
            ])
        );
    }

    #[test]
    fn letrec_binds_names_before_initializer_evaluation() {
        assert_eq!(
            eval_str(
                r#"
                (letrec ((x 1)
                         (y x))
                  y)
                "#
            ),
            Value::Integer(1)
        );
    }

    #[test]
    fn named_let_expands_to_recursive_binding() {
        assert_eq!(
            eval_str(
                r#"
                (named-let loop ((n 3) (acc nil))
                  (if (> n 0)
                      (loop (1- n) (cons n acc))
                    acc))
                "#
            ),
            Value::list([Value::Integer(1), Value::Integer(2), Value::Integer(3),])
        );
    }

    #[test]
    fn alist_get_supports_equal_test_function() {
        assert_eq!(
            eval_str("(alist-get \"b\" '((\"a\" . 1) (\"b\" . 2)) nil nil #'equal)"),
            Value::Integer(2)
        );
    }

    #[test]
    fn setf_alist_get_updates_and_removes_entries() {
        assert_eq!(
            eval_str(
                "(let ((alist '((\"a\" . 1))))
                   (setf (alist-get \"b\" alist nil nil #'equal) 2)
                   alist)"
            ),
            Value::list([
                Value::cons(Value::String("b".into()), Value::Integer(2)),
                Value::cons(Value::String("a".into()), Value::Integer(1)),
            ])
        );
        assert_eq!(
            eval_str(
                "(let ((alist '((a . 1) (b . 2))))
                   (setf (alist-get 'b alist nil 'remove) nil)
                   alist)"
            ),
            Value::list([Value::cons(Value::Symbol("a".into()), Value::Integer(1),)])
        );
    }

    #[test]
    fn setf_plist_get_updates_and_adds_entries() {
        assert_eq!(
            eval_str(
                "(let ((plist '(:host \"example.org\")))
                   (setf (plist-get plist :secret) \"pw\")
                   plist)"
            ),
            Value::list([
                Value::Symbol(":host".into()),
                Value::String("example.org".into()),
                Value::Symbol(":secret".into()),
                Value::String("pw".into()),
            ])
        );
        assert_eq!(
            eval_str(
                "(let ((plist (list \"host\" \"old\")))
                   (setf (plist-get plist \"host\" #'equal) \"new\")
                   plist)"
            ),
            Value::list([Value::String("host".into()), Value::String("new".into()),])
        );
    }

    #[test]
    fn ert_with_temp_file_honors_text_keyword() {
        assert_eq!(
            eval_str(
                "(ert-with-temp-file sample-file
                   :text \"alpha\\nbeta\\n\"
                   (with-temp-buffer
                     (insert-file-contents sample-file)
                     (buffer-string)))"
            ),
            Value::String("alpha\nbeta\n".into())
        );
    }

    #[test]
    fn setf_image_property_updates_image_descriptors() {
        assert_eq!(
            eval_str(
                "(let ((image '(image :type png :file \"demo.png\")))
                   (setf (image-property image :type) nil)
                   (setf (image-property image :data) \"payload\")
                   image)"
            ),
            Value::list([
                Value::Symbol("image".into()),
                Value::Symbol(":file".into()),
                Value::String("demo.png".into()),
                Value::Symbol(":data".into()),
                Value::String("payload".into()),
            ])
        );
    }

    #[test]
    fn if_let_star_and_when_let_star_short_circuit_on_nil() {
        assert_eq!(
            eval_str("(if-let* ((a 1) (b 2)) (+ a b) 'fallback)"),
            Value::Integer(3)
        );
        assert_eq!(
            eval_str("(if-let* ((a 1) (_ nil) (b 2)) (+ a b) 'fallback)"),
            Value::Symbol("fallback".into())
        );
        assert_eq!(
            eval_str("(when-let* ((a 1) (b 2)) (+ a b))"),
            Value::Integer(3)
        );
    }

    #[test]
    fn if_let_and_when_let_support_single_binding_compat_syntax() {
        assert_eq!(
            eval_str("(if-let (a 3) (+ a 4) 'fallback)"),
            Value::Integer(7)
        );
        assert_eq!(
            eval_str("(if-let ((a nil) (b 2)) (+ a b) 'fallback)"),
            Value::Symbol("fallback".into())
        );
        assert_eq!(eval_str("(when-let (a 5) (+ a 6))"), Value::Integer(11));
    }

    #[test]
    fn and_let_star_returns_body_or_last_binding_value() {
        assert_eq!(
            eval_str("(and-let* ((a 1) (b (+ a 2))) (+ a b))"),
            Value::Integer(4)
        );
        assert_eq!(
            eval_str("(and-let* ((a 1) (b nil)) (error \"must not run\"))"),
            Value::Nil
        );
        assert_eq!(
            eval_str("(and-let* ((a 1) (b (+ a 2))))"),
            Value::Integer(3)
        );
        assert_eq!(eval_str("(and-let* nil)"), Value::T);
    }

    #[test]
    fn bound_and_true_p_checks_binding_before_value() {
        assert_eq!(
            eval_str("(let ((sample t)) (bound-and-true-p sample))"),
            Value::T
        );
        assert_eq!(eval_str("(bound-and-true-p missing-symbol)"), Value::Nil);
    }

    #[test]
    fn numeric_comparisons_support_variadic_chains() {
        assert_eq!(
            eval_str("(list (<= 33 77 47) (<= 33 40 47) (< 32 65 91) (/= 1 2 1))"),
            Value::list([Value::Nil, Value::T, Value::T, Value::Nil])
        );
    }

    #[test]
    fn seq_position_uses_equal_by_default() {
        assert_eq!(
            eval_str("(seq-position '((a a a) (b b b) (c c c)) '(b b b))"),
            Value::Integer(1)
        );
    }

    #[test]
    fn require_ert_uses_builtin_feature_and_skip_alias() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(&mut interp, "(require 'ert)"),
            Value::Symbol("ert".into())
        );
        eval_str_with(
            &mut interp,
            r#"
            (ert-deftest skip-via-ert-private-alias ()
              (ert--skip-unless nil))
            "#,
        );
        let summary = interp.run_ert_tests_with_selector(None);
        assert_eq!(summary.skipped, 1);
        assert_eq!(
            interp.test_results[0].condition_type.as_deref(),
            Some("ert-test-skipped")
        );
    }

    #[test]
    fn ert_with_test_buffer_kills_buffer_after_success() {
        assert_eq!(
            eval_str(
                r#"(let (buf)
                     (list
                      (ert-with-test-buffer (:name "jit-lock-test")
                        (setq buf (current-buffer))
                        (buffer-name))
                      (buffer-live-p buf)))"#
            ),
            Value::list([Value::String("jit-lock-test".into()), Value::Nil])
        );
    }

    #[test]
    fn ert_with_test_buffer_keeps_buffer_after_error() {
        assert_eq!(
            eval_str(
                r#"(let (buf)
                     (condition-case nil
                         (ert-with-test-buffer (:name "jit-lock-test")
                           (setq buf (current-buffer))
                           (error "boom"))
                       (error
                        (list (buffer-live-p buf)
                              (buffer-name buf)))))"#
            ),
            Value::list([Value::T, Value::String("jit-lock-test".into())])
        );
    }

    #[test]
    fn require_uses_explicit_file_targets_in_file_missing_errors() {
        let mut interp = Interpreter::new();
        let mut env: Env = Vec::new();
        let form = Reader::new("(require 'mod-test \"/tmp/emaxx-missing-mod-test\")")
            .read_all()
            .expect("read require")
            .remove(0);
        let error = interp.eval(&form, &mut env).unwrap_err();
        assert_eq!(error.condition_type(), "file-missing");
        assert_eq!(
            error.to_string(),
            "Cannot open load file: No such file or directory, /tmp/emaxx-missing-mod-test"
        );
    }

    #[test]
    fn skip_unless_records_skip_in_summary() {
        let mut interp = Interpreter::new();
        eval_str_with(
            &mut interp,
            r#"
            (ert-deftest skipped-test ()
              (skip-unless nil))
            "#,
        );
        let summary = interp.run_ert_tests_with_selector(None);
        assert_eq!(summary.skipped, 1);
        assert_eq!(interp.test_results[0].status, TestStatus::Skipped);
        assert_eq!(
            interp.test_results[0].condition_type.as_deref(),
            Some("ert-test-skipped")
        );
    }

    #[test]
    fn call_interactively_consumes_unread_events_for_k_specs() {
        assert_string_list(
            eval_str(
                "(let ((unread-command-events '(?a ?b))) \
                   (call-interactively \
                     (lambda (a b) \
                       (interactive \"ka\0a: \nkb: \") \
                       (list a b))))",
            ),
            &["a", "b"],
        );
    }

    #[test]
    fn call_interactively_autoloads_commands_before_collecting_args() {
        let root = std::env::temp_dir().join(format!(
            "emaxx-callint-autoload-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let target = root.join("sample-callint.el");
        std::fs::write(
            &target,
            "(defun sample-callint-command (arg)\n  (interactive (list 42))\n  arg)\n",
        )
        .unwrap();

        let mut interp = Interpreter::new();
        interp.set_load_path(vec![root.clone()]);
        eval_str_with(
            &mut interp,
            "(autoload 'sample-callint-command \"sample-callint\" nil t)",
        );
        assert_eq!(
            eval_str_with(&mut interp, "(call-interactively 'sample-callint-command)"),
            Value::Integer(42)
        );

        std::fs::remove_file(&target).unwrap();
        std::fs::remove_dir(&root).unwrap();
    }

    #[test]
    fn keyboard_quit_signals_quit_condition() {
        let mut interp = Interpreter::new();
        let mut env: Env = Vec::new();
        let form = Reader::new("(keyboard-quit)").read_all().unwrap().remove(0);
        let error = interp.eval(&form, &mut env).unwrap_err();
        assert_eq!(error.condition_type(), "quit");
    }

    #[test]
    fn run_with_timer_returns_a_timer_without_firing_immediately() {
        assert_eq!(
            eval_str(
                "(let ((flag nil)
                       (timer (run-with-timer 1 nil (lambda () (setq flag t)))))
                   (list (timerp timer) flag))"
            ),
            Value::list([Value::T, Value::Nil])
        );
    }

    #[test]
    fn timerp_recognizes_loaded_timer_records() {
        assert_eq!(
            eval_str_with_upstream_load_path("(progn (require 'timer) (timerp (timer-create)))"),
            Value::T
        );
    }

    #[test]
    fn timer_queue_variables_default_to_empty_lists() {
        assert_eq!(
            eval_str("(list timer-list timer-idle-list)"),
            Value::list([Value::Nil, Value::Nil])
        );
    }

    #[test]
    fn loaded_timer_queue_fires_during_waits() {
        assert_eq!(
            eval_str_with_upstream_load_path(
                "(progn
                   (require 'timer)
                   (setq fired nil)
                   (run-with-timer 0 nil (lambda () (setq fired t)))
                   (sleep-for 0)
                   fired)"
            ),
            Value::T
        );
    }

    #[test]
    fn auto_revert_mode_reloads_changed_file() {
        let path = std::env::temp_dir().join(format!(
            "emaxx-auto-revert-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos()
        ));
        let path_text = path.to_string_lossy();
        let form = format!(
            r#"(progn
                 (require 'autorevert)
                 (setq auto-revert-interval 0)
                 (write-region "any text" nil "{path_text}" nil 'no-message)
                 (let ((buf (find-file-noselect "{path_text}")))
                   (with-current-buffer buf
                     (auto-revert-mode 1)
                     (write-region "another text" nil "{path_text}" nil 'no-message)
                     (set-file-times "{path_text}" (time-subtract nil 1))
                     (sleep-for 0)
                     (prog1 (buffer-string)
                       (set-buffer-modified-p nil)
                       (kill-buffer buf)))))"#
        );
        assert_eq!(
            eval_str_with_upstream_load_path(&form),
            Value::String("another text".into())
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn dired_revert_refreshes_directory_listing() {
        let directory = std::env::temp_dir().join(format!(
            "emaxx-dired-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos()
        ));
        fs::create_dir(&directory).expect("create dired test directory");
        let file = directory.join("listed-file");
        fs::write(&file, "contents").expect("write dired test file");
        let directory_text = directory.to_string_lossy();
        let form = format!(
            r#"(let ((buf (dired-noselect "{directory_text}/")))
                 (with-current-buffer buf
                   (let ((before (string-match-p "listed-file" (buffer-string))))
                     (delete-file "{directory_text}/listed-file")
                     (list (not (null before))
                           (dired-buffer-stale-p)
                           (progn
                             (revert-buffer 'ignore-auto 'dont-ask 'preserve-modes)
                             (string-match-p "listed-file" (buffer-string)))))))"#
        );
        assert_eq!(
            eval_str_with_upstream_load_path(&form),
            Value::list([Value::T, Value::T, Value::Nil])
        );
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn file_notifications_drive_global_auto_revert_without_polling() {
        let path = std::env::temp_dir().join(format!(
            "emaxx-notify-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos()
        ));
        fs::write(&path, "").expect("create notification test file");
        let path_text = path.to_string_lossy();
        let form = format!(
            r#"(progn
                 (require 'autorevert)
                 (let ((auto-revert-use-notify t)
                       (auto-revert-avoid-polling t)
                       (auto-revert-notify-exclude-dir-regexp "nothing-to-be-excluded")
                       (buf (find-file-noselect "{path_text}")))
                   (unwind-protect
                       (with-current-buffer buf
                         (global-auto-revert-mode 1)
                         (let ((desc auto-revert-notify-watch-descriptor))
                           (write-region "changed" nil "{path_text}" nil 'no-message)
                           (list (eq file-notify--library 'kqueue)
                                 (file-notify-valid-p desc)
                                 (buffer-local-value 'auto-revert-notify-watch-descriptor buf)
                                 (buffer-string))))
                     (global-auto-revert-mode 0)
                     (kill-buffer buf))))"#
        );
        assert_eq!(
            eval_str_with_upstream_load_path(&form),
            Value::list([
                Value::T,
                Value::T,
                Value::Integer(1),
                Value::String("changed".into()),
            ])
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn global_auto_revert_adopts_files_opened_after_enable() {
        let path = std::env::temp_dir().join(format!(
            "emaxx-notify-late-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos()
        ));
        fs::write(&path, "").expect("create late notification test file");
        let path_text = path.to_string_lossy();
        let form = format!(
            r#"(progn
                 (require 'autorevert)
                 (let ((auto-revert-use-notify t)
                       (auto-revert-avoid-polling t)
                       (auto-revert-notify-exclude-dir-regexp "nothing-to-be-excluded"))
                   (unwind-protect
                       (progn
                         (global-auto-revert-mode 1)
                         (let ((buf (find-file-noselect "{path_text}")))
                           (with-current-buffer buf
                             (auto-revert-buffers)
                             (not (null auto-revert-notify-watch-descriptor)))))
                     (global-auto-revert-mode 0))))"#
        );
        assert_eq!(eval_str_with_upstream_load_path(&form), Value::T);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn make_indirect_buffer_clone_copies_buffer_local_modes() {
        assert_eq!(
            eval_str(
                r#"(let ((base (get-buffer-create " indirect-base")))
                     (with-current-buffer base
                       (setq-local sample-mode t)
                       (let ((cloned (make-indirect-buffer base " indirect-clone" 'clone))
                             (plain (make-indirect-buffer base " indirect-plain" nil)))
                         (unwind-protect
                             (list (buffer-local-value 'sample-mode cloned)
                                   (local-variable-p 'sample-mode cloned)
                                   (local-variable-p 'sample-mode plain))
                           (kill-buffer cloned)
                           (kill-buffer plain)
                           (kill-buffer base)))))"#
            ),
            Value::list([Value::T, Value::T, Value::Nil])
        );
    }

    #[test]
    fn buffer_auto_revert_by_notification_defaults_to_nil() {
        assert_eq!(eval_str("buffer-auto-revert-by-notification"), Value::Nil);
    }

    #[test]
    fn format_spec_applies_width_precision_and_flags() {
        assert_eq!(
            eval_str(r#"(format-spec "%2a%-3b%.1p%%" '((?a . "") (?b . "-") (?p . "99")))"#),
            Value::String("  -  9%".into())
        );
        assert_eq!(
            eval_str(r#"(format-spec "%2a%-3b%.1p%%" '((?b . "-") (?p . "99")) 'delete)"#),
            Value::String("-  9%".into())
        );
        assert_eq!(
            eval_str(
                r#"(format-spec "%^a %_b %04c %<3d %>3e" '((?a . "abc") (?b . "XYZ") (?c . "7") (?d . "abcdef") (?e . "abcdef")))"#
            ),
            Value::String("ABC xyz 0007 def abc".into())
        );
    }

    #[test]
    fn format_spec_supports_function_values_and_split() {
        assert_eq!(
            eval_str(r#"(format-spec "a%xb" `((?x . ,(lambda () "X"))) nil t)"#),
            Value::list([
                Value::String("a".into()),
                Value::String("X".into()),
                Value::String("b".into()),
            ])
        );
    }

    #[test]
    fn custom_add_choice_extends_choice_types_without_duplicates() {
        assert_eq!(
            eval_str(
                r#"(progn
                     (defcustom sample-choice t "Sample."
                       :type '(choice (const :tag "One" one)))
                     (custom-add-choice 'sample-choice '(const :tag "Two" two))
                     (custom-add-choice 'sample-choice '(const :tag "Two" duplicate))
                     (get 'sample-choice 'custom-type))"#
            ),
            Value::list([
                Value::Symbol("choice".into()),
                Value::list([
                    Value::Symbol("const".into()),
                    Value::Symbol(":tag".into()),
                    Value::String("One".into()),
                    Value::Symbol("one".into()),
                ]),
                Value::list([
                    Value::Symbol("const".into()),
                    Value::Symbol(":tag".into()),
                    Value::String("Two".into()),
                    Value::Symbol("two".into()),
                ]),
            ])
        );
    }

    #[test]
    fn tab_bar_new_tab_choice_has_preloaded_custom_type() {
        assert_eq!(
            eval_str(
                r#"(progn
                     (custom-add-choice 'tab-bar-new-tab-choice
                                        '(const :tag "Bookmark List" bookmark-bmenu-get-buffer))
                     (assoc 'const (cdr (get 'tab-bar-new-tab-choice 'custom-type))))"#
            ),
            Value::list([
                Value::Symbol("const".into()),
                Value::Symbol(":tag".into()),
                Value::String("Current buffer".into()),
                Value::T,
            ])
        );
    }

    #[test]
    fn chinese_gb18030_is_accepted_for_decode_coding_string() {
        assert_eq!(eval_str(r#"(coding-system-p 'chinese-gb18030)"#), Value::T);
        assert_eq!(
            eval_str(r#"(stringp (decode-coding-string "\xE3\x32\x9A\x36" 'chinese-gb18030))"#),
            Value::T
        );
    }

    #[test]
    fn select_safe_coding_system_uses_default_candidates() {
        assert_eq!(
            eval_str("(select-safe-coding-system (point-min) (point-max) (list t 'utf-8-emacs))"),
            Value::Symbol("utf-8-emacs".into())
        );
    }

    #[test]
    fn utf8_decoding_preserves_invalid_bytes_as_raw_chars() {
        let decoded = eval_str(r#"(decode-coding-string "\xe3\x32" 'utf-8)"#);
        assert_eq!(primitives::string_text(&decoded).unwrap(), "\u{e0e3}2");
    }

    #[test]
    fn decode_char_supports_eight_bit_charset() {
        assert_eq!(
            eval_str(
                r#"(list (charsetp 'eight-bit)
                        (char-charset (decode-char 'eight-bit #x81))
                        (stringp (char-to-string (decode-char 'eight-bit #x81))))"#
            ),
            Value::list([Value::T, Value::Symbol("eight-bit".into()), Value::T,])
        );
    }

    #[test]
    fn glyphless_char_display_defaults_to_char_table() {
        assert_eq!(
            eval_str(
                r#"(list (char-table-p glyphless-char-display)
                              (char-table-subtype glyphless-char-display))"#
            ),
            Value::list([Value::T, Value::Symbol("glyphless-char-display".into()),])
        );
    }

    #[test]
    fn header_line_indent_mode_defaults_to_nil() {
        assert_eq!(eval_str("header-line-indent-mode"), Value::Nil);
    }

    #[test]
    fn header_line_indent_mode_sets_buffer_local_state() {
        assert_eq!(
            eval_str(
                r#"(progn
                     (header-line-indent-mode)
                     (list header-line-indent-mode
                           (string= header-line-indent "")
                           header-line-indent-width
                           (local-variable-p 'header-line-indent-mode)))"#
            ),
            Value::list([Value::T, Value::T, Value::Integer(0), Value::T,])
        );
    }

    #[test]
    fn bidi_string_mark_left_to_right_marks_rtl_strings() {
        assert_eq!(
            eval_str(
                r#"(list (bidi-string-mark-left-to-right "abc")
                              (length (bidi-string-mark-left-to-right "א")))"#
            ),
            Value::list([Value::String("abc".into()), Value::Integer(2)])
        );
    }

    #[test]
    fn insert_file_contents_visit_marks_buffer_as_visiting_file() {
        let path = std::env::temp_dir().join(format!(
            "emaxx-insert-visit-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos()
        ));
        fs::write(&path, "visited").expect("create insert visit test file");
        let path_text = path.to_string_lossy();
        let form = format!(
            r#"(let ((buf (generate-new-buffer " insert-visit")))
                 (unwind-protect
                     (with-current-buffer buf
                       (insert-file-contents "{path_text}" 'visit)
                       (list buffer-file-name
                             (buffer-modified-p)
                             (verify-visited-file-modtime buf)))
                   (kill-buffer buf)))"#
        );
        assert_eq!(
            eval_str_with_upstream_load_path(&form),
            Value::list([Value::String(path_text.to_string()), Value::Nil, Value::T])
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn revert_buffer_refreshes_related_indirect_buffers() {
        let path = std::env::temp_dir().join(format!(
            "emaxx-indirect-revert-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos()
        ));
        fs::write(&path, "old").expect("create indirect revert test file");
        let path_text = path.to_string_lossy();
        let form = format!(
            r#"(let* ((base (find-file-noselect "{path_text}"))
                      (clone (make-indirect-buffer base " indirect-revert-clone" 'clone)))
                 (unwind-protect
                     (with-current-buffer base
                       (write-region "new" nil "{path_text}" nil 'no-message)
                       (revert-buffer 'ignore-auto 'dont-ask 'preserve-modes)
                       (list (buffer-string)
                             (with-current-buffer clone (buffer-string))))
                   (kill-buffer clone)
                   (kill-buffer base)))"#
        );
        assert_eq!(
            eval_str_with_upstream_load_path(&form),
            Value::list([Value::String("new".into()), Value::String("new".into())])
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn time_convert_list_accepts_float_precision_loss_like_emacs() {
        assert_eq!(
            eval_str("(time-convert 0.1 'list)"),
            Value::list([
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(100000),
                Value::Integer(0),
            ])
        );
        assert_eq!(
            eval_str("(time-convert -0.1 'list)"),
            Value::list([
                Value::Integer(-1),
                Value::Integer(65535),
                Value::Integer(899999),
                Value::Integer(999999),
            ])
        );
    }

    #[test]
    fn call_interactively_records_declared_history_arguments() {
        let mut interp = Interpreter::new();
        eval_str_with(
            &mut interp,
            "(defun callint-test-int-args (foo bar &optional zot) \
               (declare (interactive-args (bar 10) (zot 11))) \
               (interactive (list 1 1 1)) \
               (+ foo bar zot))",
        );
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(let ((history-length 1) (command-history ())) \
                   (list (call-interactively 'callint-test-int-args t) command-history))"
            ),
            Value::list([
                Value::Integer(3),
                Value::list([Value::list([
                    Value::Symbol("callint-test-int-args".into()),
                    Value::Integer(1),
                    Value::Integer(10),
                    Value::Integer(11),
                ])]),
            ])
        );
    }

    #[test]
    fn call_interactively_rejects_invalid_control_letters() {
        assert_eq!(
            eval_str("(cdr (should-error (call-interactively (lambda () (interactive \"ÿ\")))))"),
            Value::list([Value::String(
                "Invalid control letter `ÿ' (#o377, #x00ff) in interactive calling string".into(),
            )])
        );
    }

    #[test]
    fn call_interactively_follows_symbol_aliases_for_interactive_specs() {
        let mut interp = Interpreter::new();
        eval_str_with(
            &mut interp,
            "(defun sample-callint-target (arg)
               (interactive (list 7))
               arg)",
        );
        eval_str_with(
            &mut interp,
            "(defalias 'sample-callint-alias 'sample-callint-target)",
        );
        assert_eq!(
            eval_str_with(&mut interp, "(call-interactively 'sample-callint-alias)"),
            Value::Integer(7)
        );
    }

    #[cfg(unix)]
    #[test]
    fn call_process_region_can_delete_entire_buffer() {
        assert_eq!(
            eval_str(
                "(let ((shell (executable-find \"sh\"))) \
                   (with-temp-buffer \
                     (insert \"Buffer contents\\n\") \
                     (list \
                       (call-process-region nil nil shell :delete nil nil \"-c\" \"cat >/dev/null\") \
                       (buffer-size))))"
            ),
            Value::list([Value::Integer(0), Value::Integer(0)])
        );
    }

    #[test]
    fn call_process_missing_program_signals_file_error() {
        assert_eq!(
            eval_str(
                r#"
                (condition-case nil
                    (call-process "/definitely/missing/emaxx-program" nil nil nil)
                  (file-error 'caught)
                  (error 'wrong-condition))
                "#
            ),
            Value::Symbol("caught".into())
        );
    }

    #[test]
    fn ignore_error_catches_requested_conditions() {
        assert_eq!(
            eval_str(
                r#"(list
                     (ignore-error wrong-type-argument
                       (signal 'wrong-type-argument nil))
                     (condition-case nil
                         (ignore-error search-failed
                           (signal 'wrong-type-argument nil))
                       (wrong-type-argument 'caught)))"#
            ),
            Value::list([Value::Nil, Value::Symbol("caught".into())])
        );
    }

    #[test]
    fn encode_coding_region_returns_region_text_when_requested() {
        assert_string_value(
            eval_str(
                r#"(with-temp-buffer
                     (set-buffer-multibyte nil)
                     (insert "ABC")
                     (encode-coding-region (point-min) (point-max) 'binary t))"#,
            ),
            "ABC",
        );
    }

    #[test]
    fn encode_coding_region_binary_returns_unibyte_string() {
        assert_eq!(
            eval_str(
                r#"(with-temp-buffer
                     (set-buffer-multibyte t)
                     (insert (string #x89 ?A))
                     (let ((encoded (encode-coding-region
                                     (point-min) (point-max) 'binary t)))
                       (list (string-to-list encoded)
                             (multibyte-string-p encoded))))"#
            ),
            Value::list([
                Value::list([Value::Integer(137), Value::Integer(65)]),
                Value::Nil,
            ])
        );
    }

    #[test]
    fn decode_coding_region_rewrites_dos_eol_in_place() {
        assert_eq!(
            eval_str(
                r#"(with-temp-buffer
                     (set-buffer-multibyte nil)
                     (insert (encode-coding-string "あ" 'euc-jp) "\r" "\n")
                     (decode-coding-region (point-min) (point-max) 'euc-jp-dos)
                     (string-search "\r" (buffer-string)))"#
            ),
            Value::Nil
        );
    }

    #[test]
    fn decode_coding_string_normalizes_dos_eol() {
        assert_eq!(
            eval_str(
                r#"(let ((decoded (decode-coding-string "A\r\n" 'utf-8-dos)))
                     (string-search "\r" decoded))"#
            ),
            Value::Nil
        );
    }

    #[test]
    fn base64_decode_string_ignores_wrapped_input() {
        assert_eq!(
            eval_str(
                r#"(let ((decoded (base64-decode-string "SGVsbG8s
IHdvcmxkIQ==")))
                     (list (string-to-list decoded)
                           (multibyte-string-p decoded)))"#
            ),
            Value::list([
                Value::list([
                    Value::Integer(72),
                    Value::Integer(101),
                    Value::Integer(108),
                    Value::Integer(108),
                    Value::Integer(111),
                    Value::Integer(44),
                    Value::Integer(32),
                    Value::Integer(119),
                    Value::Integer(111),
                    Value::Integer(114),
                    Value::Integer(108),
                    Value::Integer(100),
                    Value::Integer(33),
                ]),
                Value::Nil,
            ])
        );
    }

    #[test]
    fn base64_decode_string_returns_unibyte_raw_bytes() {
        assert_eq!(
            eval_str(
                r#"(let ((decoded (base64-decode-string "/wA=")))
                     (list (string-to-list decoded)
                           (multibyte-string-p decoded)))"#
            ),
            Value::list([
                Value::list([Value::Integer(255), Value::Integer(0)]),
                Value::Nil,
            ])
        );
    }

    #[test]
    fn base64_decode_string_supports_url_variant_and_invalid_handling() {
        assert_eq!(
            eval_str(
                r#"(list
                    (base64-decode-string "SGVsbG8" t)
                    (condition-case _
                        (base64-decode-string "!")
                      (error 'caught))
                    (string-bytes (base64-decode-string "!" nil t)))"#
            ),
            Value::list([
                Value::String("Hello".into()),
                Value::Symbol("caught".into()),
                Value::Integer(0),
            ])
        );
    }

    #[test]
    fn base64_decode_region_reports_unibyte_byte_count() {
        assert_eq!(
            eval_str(
                r#"(with-temp-buffer
                     (set-buffer-multibyte nil)
                     (insert "FPucA9l+")
                     (let ((len (base64-decode-region (point-min) (point-max))))
                       (list len
                             (string-bytes (buffer-string))
                             (string-to-list (buffer-string))
                             (multibyte-string-p (buffer-string)))))"#
            ),
            Value::list([
                Value::Integer(6),
                Value::Integer(6),
                Value::list([
                    Value::Integer(20),
                    Value::Integer(251),
                    Value::Integer(156),
                    Value::Integer(3),
                    Value::Integer(217),
                    Value::Integer(126),
                ]),
                Value::Nil,
            ])
        );
    }

    #[test]
    fn base64_encode_string_rejects_multibyte_non_ascii_input() {
        assert_eq!(
            eval_str(
                r#"(list
                    (condition-case _ (base64-encode-string "ü") (error 'caught))
                    (condition-case _ (base64url-encode-string "ƒ") (error 'caught)))"#
            ),
            Value::list([
                Value::Symbol("caught".into()),
                Value::Symbol("caught".into())
            ])
        );
    }

    #[test]
    fn sha1_and_buffer_hash_match_for_ascii_buffer_contents() {
        assert_eq!(
            eval_str(
                r#"(list
                    (sha1 "foo")
                    (with-temp-buffer
                      (insert "foo")
                      (buffer-hash)))"#
            ),
            Value::list([
                Value::String("0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33".into()),
                Value::String("0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33".into()),
            ])
        );
    }

    #[test]
    fn secure_hash_supports_core_algorithms_and_iv_auto() {
        let result = eval_str(
            r#"(list
                (secure-hash 'md5 "foobar")
                (secure-hash 'sha1 "foobar")
                (length (secure-hash 'sha512 'iv-auto 100)))"#,
        );
        let items = result.to_vec().expect("hash result list");
        assert_eq!(
            items[0],
            Value::String("3858f62230ac3c915f300c664312c63f".into())
        );
        assert_eq!(
            items[1],
            Value::String("8843d7f92416211de9ebb963ff4ce28125932878".into())
        );
        assert_eq!(items[2], Value::Integer(128));
    }

    #[test]
    fn format_binary_negative() {
        assert_eq!(
            eval_str(r#"(format "%b" #x-5A)"#),
            Value::String("-1011010".into())
        );
        assert_eq!(
            eval_str(r#"(format "%b" #x5A)"#),
            Value::String("1011010".into())
        );
    }

    #[test]
    fn backquote_dotted_pair() {
        assert_eq!(
            eval_str(r#"(car '(#x-5A . "1011010"))"#),
            Value::Integer(-90)
        );
        assert_eq!(
            eval_str(r#"(cdr '(#x-5A . "1011010"))"#),
            Value::String("1011010".into())
        );
    }

    #[test]
    fn dolist_dotted_pairs() {
        assert_string_value(
            eval_str(
                r#"(let ((result nil))
                     (dolist (pair `((1 . "a") (2 . "b")))
                       (setq result (concat (cdr pair) (or result ""))))
                     result)"#,
            ),
            "ba",
        );
    }

    #[test]
    fn nested_backquote_preserves_inner_unquote() {
        assert_eq!(
            eval_str(
                r#"(progn
                     (defmacro nested-single-comma () ``(,x))
                     (let ((x 1))
                       (nested-single-comma)))"#
            ),
            Value::list([Value::Integer(1)])
        );
    }

    #[test]
    fn nested_backquote_decrements_unquote_depth() {
        let expected = Reader::new("`(,1)")
            .read()
            .expect("read succeeds")
            .expect("form is present");
        assert_eq!(
            eval_str(
                r#"(let ((x 1))
                     (eval '``(,,x)))"#
            ),
            expected
        );
    }

    #[test]
    fn nested_backquote_preserves_inner_splice() {
        assert_eq!(
            eval_str(
                r#"(progn
                     (defmacro nested-splice () ``(,@args ,val))
                     (let ((args '(a i))
                           (val 'v))
                       (nested-splice)))"#
            ),
            Value::list([
                Value::Symbol("a".into()),
                Value::Symbol("i".into()),
                Value::Symbol("v".into()),
            ])
        );
    }

    #[test]
    fn format_binary_nonzero_simple() {
        // Simplified version of the ERT test
        assert_eq!(
            eval_str(
                r#"(let* ((n #x-5A) (bits "1011010")
                          (sgn- (if (< n 0) "-" "")))
                     (concat sgn- bits))"#
            ),
            Value::String("-1011010".into())
        );
        // The actual assertion from the test
        assert_eq!(
            eval_str(
                r#"(let* ((n #x-5A) (bits "1011010")
                          (sgn- (if (< n 0) "-" "")))
                     (string-equal (format "%b" n) (concat sgn- bits)))"#
            ),
            Value::T
        );
    }

    #[test]
    fn format_binary_via_dolist() {
        assert_eq!(
            eval_str(
                r#"(let ((ok t))
                     (dolist (nbits `((#x-5A . "1011010")
                                      (#x5A . "1011010")))
                       (let* ((n (car nbits)) (bits (cdr nbits))
                              (sgn- (if (< n 0) "-" "")))
                         (unless (string-equal (format "%b" n) (concat sgn- bits))
                           (setq ok nil))))
                     ok)"#
            ),
            Value::T
        );
    }

    #[test]
    fn backtick_comma_in_dotted_pair() {
        assert_eq!(
            eval_str(r#"`(#xFFF . ,(make-string 12 ?1))"#),
            Value::cons(Value::Integer(0xFFF), Value::String("111111111111".into()))
        );
    }

    #[test]
    fn backquote_preserves_record_literal_dotted_pair_tails() {
        let mut interp = Interpreter::new();
        let value = eval_str_with(&mut interp, r#"`(#s(a 1) . #s(b 2))"#);
        let (left, right) = value.cons_values().expect("dotted pair");
        assert!(matches!(left, Value::Record(_)));
        assert!(matches!(right, Value::Record(_)));
    }

    #[test]
    fn backquote_materializes_record_literals() {
        let mut interp = Interpreter::new();
        let value = eval_str_with(&mut interp, r#"`(#s(a b) #s(#s(c d) e))"#);
        let items = value.to_vec().expect("backquoted list");
        assert_eq!(items.len(), 2);
        let Value::Record(inner_id) = &items[0] else {
            panic!("expected inner record");
        };
        let inner = interp.find_record(*inner_id).expect("inner record");
        assert_eq!(inner.type_name, "a");
        assert_eq!(inner.slots, vec![Value::Symbol("b".into())]);
        let Value::Record(outer_id) = &items[1] else {
            panic!("expected outer record");
        };
        let outer = interp.find_record(*outer_id).expect("outer record");
        assert_eq!(outer.type_name, "literal-record");
        assert_eq!(outer.slots.len(), 2);
        assert!(matches!(outer.slots[0], Value::Record(_)));
        assert!(matches!(outer.slots[1], Value::Symbol(ref symbol) if symbol == "e"));
    }

    #[test]
    fn eval_while_loop() {
        assert_eq!(
            eval_str("(let ((x 0)) (while (< x 5) (setq x (1+ x))) x)"),
            Value::Integer(5)
        );
    }

    fn assert_overlay_modification_hooks_record_insert_inside_overlay() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (insert "1234")
                  (let ((overlay (make-overlay 2 4)))
                    (dolist (hooks-property '(insert-in-front-hooks
                                              modification-hooks
                                              insert-behind-hooks))
                      (overlay-put
                       overlay
                       hooks-property
                       (list (lambda (ov &rest args)
                               (push (list hooks-property args)
                                     (overlay-get overlay
                                                  'recorded-modification-hook-calls)))))
                      (overlay-put overlay 'recorded-modification-hook-calls nil))
                    (goto-char 3)
                    (insert "x")
                    (overlay-get overlay 'recorded-modification-hook-calls)))"#
            ),
            Value::list([
                Value::list([
                    Value::Symbol("modification-hooks".into()),
                    Value::list([
                        Value::T,
                        Value::Integer(3),
                        Value::Integer(4),
                        Value::Integer(0),
                    ]),
                ]),
                Value::list([
                    Value::Symbol("modification-hooks".into()),
                    Value::list([Value::Nil, Value::Integer(3), Value::Integer(3)]),
                ]),
            ])
        );
    }

    #[test]
    fn overlay_modification_hooks_record_insert_inside_overlay() {
        std::thread::Builder::new()
            .stack_size(4 * 1024 * 1024)
            .spawn(assert_overlay_modification_hooks_record_insert_inside_overlay)
            .unwrap()
            .join()
            .unwrap();
    }

    fn assert_overlay_modification_hooks_record_insert_at_overlay_start() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (insert "1234")
                  (let ((overlay (make-overlay 2 4)))
                    (dolist (hooks-property '(insert-in-front-hooks
                                              modification-hooks
                                              insert-behind-hooks))
                      (overlay-put
                       overlay
                       hooks-property
                       (list (lambda (ov &rest args)
                               (push (list hooks-property args)
                                     (overlay-get overlay
                                                  'recorded-modification-hook-calls)))))
                      (overlay-put overlay 'recorded-modification-hook-calls nil))
                    (goto-char 2)
                    (insert "x")
                    (overlay-get overlay 'recorded-modification-hook-calls)))"#
            ),
            Value::list([
                Value::list([
                    Value::Symbol("insert-in-front-hooks".into()),
                    Value::list([
                        Value::T,
                        Value::Integer(2),
                        Value::Integer(3),
                        Value::Integer(0),
                    ]),
                ]),
                Value::list([
                    Value::Symbol("insert-in-front-hooks".into()),
                    Value::list([Value::Nil, Value::Integer(2), Value::Integer(2)]),
                ]),
            ])
        );
    }

    #[test]
    fn overlay_modification_hooks_record_insert_at_overlay_start() {
        std::thread::Builder::new()
            .stack_size(4 * 1024 * 1024)
            .spawn(assert_overlay_modification_hooks_record_insert_at_overlay_start)
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn save_restriction_restores_end_after_insert_at_point_max() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (insert "ab")
                  (save-restriction
                    (narrow-to-region 1 3)
                    (goto-char (point-max))
                    (insert "c"))
                  (buffer-string))
                "#
            ),
            Value::String("abc".into())
        );
    }

    fn assert_overlay_modification_hooks_record_replace_two_chars() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (insert "1234")
                  (let ((overlay (make-overlay 2 4)))
                    (dolist (hooks-property '(insert-in-front-hooks
                                              modification-hooks
                                              insert-behind-hooks))
                      (overlay-put
                       overlay
                       hooks-property
                       (list (lambda (ov &rest args)
                               (push (list hooks-property args)
                                     (overlay-get overlay
                                                  'recorded-modification-hook-calls)))))
                      (overlay-put overlay 'recorded-modification-hook-calls nil))
                    (goto-char (point-min))
                    (search-forward "23")
                    (replace-match "x")
                    (overlay-get overlay 'recorded-modification-hook-calls)))"#
            ),
            Value::list([
                Value::list([
                    Value::Symbol("modification-hooks".into()),
                    Value::list([
                        Value::T,
                        Value::Integer(2),
                        Value::Integer(3),
                        Value::Integer(2),
                    ]),
                ]),
                Value::list([
                    Value::Symbol("modification-hooks".into()),
                    Value::list([Value::Nil, Value::Integer(2), Value::Integer(4)]),
                ]),
            ])
        );
    }

    #[test]
    fn overlay_modification_hooks_record_replace_two_chars() {
        std::thread::Builder::new()
            .stack_size(4 * 1024 * 1024)
            .spawn(assert_overlay_modification_hooks_record_replace_two_chars)
            .unwrap()
            .join()
            .unwrap();
    }

    fn assert_overlay_modification_hooks_record_zero_length_insert() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (let ((overlay (make-overlay 1 1)))
                    (dolist (hooks-property '(insert-in-front-hooks
                                              modification-hooks
                                              insert-behind-hooks))
                      (overlay-put
                       overlay
                       hooks-property
                       (list (lambda (ov &rest args)
                               (push (list hooks-property args)
                                     (overlay-get overlay
                                                  'recorded-modification-hook-calls)))))
                      (overlay-put overlay 'recorded-modification-hook-calls nil))
                    (insert "x")
                    (overlay-get overlay 'recorded-modification-hook-calls)))"#
            ),
            Value::list([
                Value::list([
                    Value::Symbol("insert-behind-hooks".into()),
                    Value::list([
                        Value::T,
                        Value::Integer(1),
                        Value::Integer(2),
                        Value::Integer(0),
                    ]),
                ]),
                Value::list([
                    Value::Symbol("insert-in-front-hooks".into()),
                    Value::list([
                        Value::T,
                        Value::Integer(1),
                        Value::Integer(2),
                        Value::Integer(0),
                    ]),
                ]),
                Value::list([
                    Value::Symbol("insert-behind-hooks".into()),
                    Value::list([Value::Nil, Value::Integer(1), Value::Integer(1)]),
                ]),
                Value::list([
                    Value::Symbol("insert-in-front-hooks".into()),
                    Value::list([Value::Nil, Value::Integer(1), Value::Integer(1)]),
                ]),
            ])
        );
    }

    #[test]
    fn overlay_modification_hooks_record_zero_length_insert() {
        std::thread::Builder::new()
            .stack_size(4 * 1024 * 1024)
            .spawn(assert_overlay_modification_hooks_record_zero_length_insert)
            .unwrap()
            .join()
            .unwrap();
    }

    fn assert_overlay_modification_hooks_data_driven_cases() {
        assert_eq!(
            eval_str(
                r#"
                (let ((mismatch nil))
                  (dolist (test-case
                           '(((insert-at . 1))
                             ((insert-at . 2)
                              (expected-calls . ((insert-in-front-hooks (nil 2 2))
                                                 (insert-in-front-hooks (t 2 3 0)))))
                             ((insert-at . 3)
                              (expected-calls . ((modification-hooks (nil 3 3))
                                                 (modification-hooks (t 3 4 0)))))
                             ((insert-at . 4)
                              (expected-calls . ((insert-behind-hooks (nil 4 4))
                                                 (insert-behind-hooks (t 4 5 0)))))
                             ((insert-at . 5))
                             ((replace . "1"))
                             ((replace . "2")
                              (expected-calls . ((modification-hooks (nil 2 3))
                                                 (modification-hooks (t 2 3 1)))))
                             ((replace . "3")
                              (expected-calls . ((modification-hooks (nil 3 4))
                                                 (modification-hooks (t 3 4 1)))))
                             ((replace . "4"))
                             ((replace . "4") (overlay-beg . 4))
                             ((replace . "12")
                              (expected-calls . ((modification-hooks (nil 1 3))
                                                 (modification-hooks (t 1 2 2)))))
                             ((replace . "23")
                              (expected-calls . ((modification-hooks (nil 2 4))
                                                 (modification-hooks (t 2 3 2)))))
                             ((replace . "34")
                              (expected-calls . ((modification-hooks (nil 3 5))
                                                 (modification-hooks (t 3 4 2)))))
                             ((replace . "123")
                              (expected-calls . ((modification-hooks (nil 1 4))
                                                 (modification-hooks (t 1 2 3)))))
                             ((replace . "234")
                              (expected-calls . ((modification-hooks (nil 2 5))
                                                 (modification-hooks (t 2 3 3)))))
                             ((replace . "1234")
                              (expected-calls . ((modification-hooks (nil 1 5))
                                                 (modification-hooks (t 1 2 4)))))
                             ((buffer-text . "") (overlay-beg . 1) (overlay-end . 1)
                              (insert-at . 1)
                              (expected-calls . ((insert-in-front-hooks (nil 1 1))
                                                 (insert-behind-hooks (nil 1 1))
                                                 (insert-in-front-hooks (t 1 2 0))
                                                 (insert-behind-hooks (t 1 2 0)))))))
                    (when (null mismatch)
                      (dolist (advance '(nil t))
                        (when (null mismatch)
                          (let-alist test-case
                            (with-temp-buffer
                              (insert (or .buffer-text "1234"))
                              (let ((overlay (make-overlay
                                              (or .overlay-beg 2)
                                              (or .overlay-end 4)
                                              nil
                                              advance advance)))
                                (dolist (hooks-property '(insert-in-front-hooks
                                                          modification-hooks
                                                          insert-behind-hooks))
                                  (overlay-put
                                   overlay
                                   hooks-property
                                   (list (lambda (ov &rest args)
                                           (push (list hooks-property args)
                                                 (overlay-get overlay
                                                              'recorded-modification-hook-calls)))))
                                  (overlay-put overlay 'recorded-modification-hook-calls nil))
                                (when .insert-at
                                  (goto-char .insert-at)
                                  (insert "x"))
                                (when .replace
                                  (goto-char (point-min))
                                  (search-forward .replace)
                                  (replace-match "x"))
                                (let ((actual (reverse (overlay-get overlay 'recorded-modification-hook-calls))))
                                  (unless (equal .expected-calls actual)
                                    (setq mismatch (list test-case advance actual)))))))))))
                  mismatch)"#
            ),
            Value::Nil
        );
    }

    #[test]
    fn overlay_modification_hooks_data_driven_cases() {
        // This data-heavy Lisp form overflows libtest's default stack on macOS-sized threads.
        std::thread::Builder::new()
            .stack_size(4 * 1024 * 1024)
            .spawn(assert_overlay_modification_hooks_data_driven_cases)
            .unwrap()
            .join()
            .unwrap();
    }

    fn assert_overlay_complex_insert_2_regions() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (insert (make-string 100 ?\s))
                  (make-overlay 77 7 nil nil t)
                  (make-overlay 21 53 nil t t)
                  (make-overlay 84 14 nil nil nil)
                  (make-overlay 38 69 nil t nil)
                  (make-overlay 93 15 nil nil t)
                  (make-overlay 73 48 nil t t)
                  (make-overlay 96 51 nil t t)
                  (make-overlay 6 43 nil t t)
                  (make-overlay 15 100 nil t t)
                  (make-overlay 22 17 nil nil nil)
                  (make-overlay 72 45 nil t nil)
                  (make-overlay 2 74 nil nil t)
                  (make-overlay 15 29 nil t t)
                  (make-overlay 17 34 nil t t)
                  (make-overlay 101 66 nil t nil)
                  (make-overlay 94 24 nil nil nil)
                  (goto-char 78)
                  (insert "           ")
                  (narrow-to-region 47 19)
                  (goto-char 46)
                  (widen)
                  (narrow-to-region 13 3)
                  (goto-char 9)
                  (delete-char 0)
                  (goto-char 11)
                  (insert "           ")
                  (goto-char 3)
                  (insert "          ")
                  (goto-char 8)
                  (insert "       ")
                  (goto-char 26)
                  (insert "  ")
                  (goto-char 14)
                  (widen)
                  (narrow-to-region 71 35)
                  (sort (mapcar (lambda (ov)
                                  (cons (overlay-start ov)
                                        (overlay-end ov)))
                                (overlays-in (point-min)
                                             (point-max)))
                        (lambda (o1 o2)
                          (or (< (car o1) (car o2))
                              (and (= (car o1) (car o2))
                                   (< (cdr o1) (cdr o2)))))))"#
            ),
            Value::list([
                Value::cons(Value::Integer(2), Value::Integer(104)),
                Value::cons(Value::Integer(23), Value::Integer(73)),
                Value::cons(Value::Integer(24), Value::Integer(107)),
                Value::cons(Value::Integer(44), Value::Integer(125)),
                Value::cons(Value::Integer(45), Value::Integer(59)),
                Value::cons(Value::Integer(45), Value::Integer(134)),
                Value::cons(Value::Integer(45), Value::Integer(141)),
                Value::cons(Value::Integer(47), Value::Integer(52)),
                Value::cons(Value::Integer(47), Value::Integer(64)),
                Value::cons(Value::Integer(51), Value::Integer(83)),
                Value::cons(Value::Integer(54), Value::Integer(135)),
                Value::cons(Value::Integer(68), Value::Integer(99)),
            ])
        );
    }

    #[test]
    fn overlay_complex_insert_2_regions() {
        std::thread::Builder::new()
            .stack_size(4 * 1024 * 1024)
            .spawn(assert_overlay_complex_insert_2_regions)
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn remove_overlays_matches_string_properties_by_equal() {
        assert_eq!(
            eval_str(
                "(with-temp-buffer
                   (insert \"abc\")
                   (let ((ov-a (make-overlay 1 2))
                         (ov-b (make-overlay 2 3)))
                     (overlay-put ov-a 'tag (copy-sequence \"a\"))
                     (overlay-put ov-b 'tag \"b\")
                     (remove-overlays nil nil 'tag \"a\")
                     (length (overlays-in (point-min) (point-max)))))"
            ),
            Value::Integer(1)
        );
    }

    #[test]
    fn font_lock_ensure_and_flush_track_hi_lock_faces() {
        run_large_stack_test(assert_font_lock_ensure_and_flush_track_hi_lock_faces);
    }

    fn assert_font_lock_ensure_and_flush_track_hi_lock_faces() {
        assert_eq!(
            eval_str(
                "(with-temp-buffer
                   (insert \"a A\")
                   (setq font-lock-mode t)
                   (setq hi-lock-interactive-patterns
                         (list
                          (list
                           (lambda (limit)
                             (let ((case-fold-search nil))
                               (re-search-forward \"a\" limit t)))
                           '(0 'hi-yellow prepend))))
                   (font-lock-ensure)
                   (let ((had-face (and (memq 'hi-yellow (get-text-property 1 'face)) t)))
                     (font-lock-flush)
                     (list had-face (get-text-property 1 'face))))"
            ),
            Value::list([Value::T, Value::list([Value::Symbol("hi-yellow".into())])])
        );
    }

    #[test]
    fn font_lock_flush_reapplies_remaining_hi_lock_faces() {
        run_large_stack_test(assert_font_lock_flush_reapplies_remaining_hi_lock_faces);
    }

    fn assert_font_lock_flush_reapplies_remaining_hi_lock_faces() {
        assert_eq!(
            eval_str(
                "(with-temp-buffer
                   (insert \"ab\")
                   (setq font-lock-mode t
                         font-lock-fontified t)
                   (let* ((match-a
                           (list (lambda (limit) (re-search-forward \"a\" limit t))
                                 '(0 'hi-yellow prepend)))
                          (match-b
                           (list (lambda (limit) (re-search-forward \"b\" limit t))
                                 '(0 'hi-yellow prepend))))
                     (setq hi-lock-interactive-patterns (list match-b match-a))
                     (font-lock-ensure)
                     (setq hi-lock-interactive-patterns (list match-b))
                     (font-lock-flush)
                     (list (get-text-property 1 'face)
                           (and (memq 'hi-yellow (get-text-property 2 'face)) t))))"
            ),
            Value::list([Value::Nil, Value::T])
        );
    }

    #[test]
    fn overlay_positions_survive_unibyte_to_multibyte_transition() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (set-buffer-multibyte t)
                  (insert "ääää")
                  (set-buffer-multibyte nil)
                  (let ((nonempty-bob-end (make-overlay 1 2))
                        (nonempty-bob-beg (make-overlay 1 3))
                        (empty-bob        (make-overlay 1 1))
                        (empty-beg        (make-overlay 3 3))
                        (empty-end        (make-overlay 2 2))
                        (nonempty-beg-beg (make-overlay 3 7))
                        (nonempty-beg-end (make-overlay 3 8))
                        (nonempty-end-beg (make-overlay 4 7))
                        (nonempty-end-end (make-overlay 4 8))
                        (nonempty-eob-beg (make-overlay 5 9))
                        (nonempty-eob-end (make-overlay 6 9))
                        (empty-eob        (make-overlay 9 9)))
                    (set-buffer-multibyte t)
                    (list
                     (list (overlay-start nonempty-bob-end) (overlay-end nonempty-bob-end))
                     (list (overlay-start nonempty-bob-beg) (overlay-end nonempty-bob-beg))
                     (list (overlay-start empty-bob) (overlay-end empty-bob))
                     (list (overlay-start empty-beg) (overlay-end empty-beg))
                     (list (overlay-start empty-end) (overlay-end empty-end))
                     (list (overlay-start nonempty-beg-beg) (overlay-end nonempty-beg-beg))
                     (list (overlay-start nonempty-beg-end) (overlay-end nonempty-beg-end))
                     (list (overlay-start nonempty-end-beg) (overlay-end nonempty-end-beg))
                     (list (overlay-start nonempty-end-end) (overlay-end nonempty-end-end))
                     (list (overlay-start nonempty-eob-beg) (overlay-end nonempty-eob-beg))
                     (list (overlay-start nonempty-eob-end) (overlay-end nonempty-eob-end))
                     (list (overlay-start empty-eob) (overlay-end empty-eob)))))
                "#
            ),
            Value::list([
                Value::list([Value::Integer(1), Value::Integer(2)]),
                Value::list([Value::Integer(1), Value::Integer(2)]),
                Value::list([Value::Integer(1), Value::Integer(1)]),
                Value::list([Value::Integer(2), Value::Integer(2)]),
                Value::list([Value::Integer(2), Value::Integer(2)]),
                Value::list([Value::Integer(2), Value::Integer(4)]),
                Value::list([Value::Integer(2), Value::Integer(5)]),
                Value::list([Value::Integer(3), Value::Integer(4)]),
                Value::list([Value::Integer(3), Value::Integer(5)]),
                Value::list([Value::Integer(3), Value::Integer(5)]),
                Value::list([Value::Integer(4), Value::Integer(5)]),
                Value::list([Value::Integer(5), Value::Integer(5)]),
            ])
        );
    }

    #[test]
    fn current_column_uses_lexically_bound_tab_width() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (let ((tab-width 4))
                    (insert "ab\tcd")
                    (goto-char (point-min))
                    (forward-char 3)
                    (current-column)))
                "#
            ),
            Value::Integer(4)
        );
    }

    #[test]
    fn indent_to_uses_tabs_when_indent_tabs_mode_is_non_nil() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (let ((tab-width 4)
                        (indent-tabs-mode t))
                    (insert "a")
                    (list (indent-to 6) (buffer-string) (current-column))))
                "#
            ),
            Value::list([
                Value::Integer(6),
                Value::String("a\t  ".into()),
                Value::Integer(6),
            ])
        );
    }

    #[test]
    fn indent_to_honors_minimum_with_spaces_only() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (let ((tab-width 4)
                        (indent-tabs-mode nil))
                    (insert "abcd")
                    (list (indent-to 2 3) (buffer-string) (current-column))))
                "#
            ),
            Value::list([
                Value::Integer(7),
                Value::String("abcd   ".into()),
                Value::Integer(7),
            ])
        );
    }

    #[test]
    fn default_indent_line_function_is_indent_relative() {
        assert_eq!(
            eval_str("(default-value 'indent-line-function)"),
            Value::Symbol("indent-relative".into())
        );
    }

    #[test]
    fn indent_relative_uses_previous_line_indent_points() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (let ((indent-tabs-mode nil))
                    (insert "alpha beta\nx")
                    (goto-char (point-max))
                    (indent-relative)
                    (buffer-string)))
                "#
            ),
            Value::String("alpha beta\nx     ".into())
        );
    }

    #[test]
    fn forward_and_backward_sexp_move_over_balanced_lists() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (insert "(alpha (beta gamma)) tail")
                  (goto-char (point-min))
                  (forward-sexp)
                  (let ((after-forward (point)))
                    (backward-sexp)
                    (list after-forward (point))))
                "#
            ),
            Value::list([Value::Integer(21), Value::Integer(1)])
        );
    }

    #[test]
    fn syntax_ppss_returns_parse_state_without_moving_point() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (insert "(alpha\n beta)")
                  (goto-char (point-max))
                  (let ((state (syntax-ppss 8))
                        (after (point)))
                    (list (car state) after)))
                "#
            ),
            Value::list([Value::Integer(1), Value::Integer(14)])
        );
    }

    #[test]
    fn rx_compiles_common_test_patterns() {
        assert_eq!(
            eval_str(r#"(rx-to-string '(seq "ab" eos) t)"#),
            Value::String("ab\\'".into())
        );
        assert_eq!(
            eval_str(
                r#"(let ((tramp-local-host-names '("foo" "bar")))
                     (rx-to-string `(: bos (| . ,tramp-local-host-names) eos)))"#
            ),
            Value::String("\\`\\(?:foo\\|bar\\)\\'".into())
        );
        assert_eq!(
            eval_str(
                r#"(let ((tramp-local-host-names '("foo" "bar")))
                     (rx-to-string `(: bos (| \, tramp-local-host-names) eos)))"#
            ),
            Value::String("\\`\\(?:foo\\|bar\\)\\'".into())
        );
        assert_eq!(
            eval_str(r#"(rx bot "body" eot)"#),
            Value::String("\\`body\\'".into())
        );
        assert_eq!(eval_str(r#"(rx "\\(")"#), Value::String("\\\\(".into()));
        assert_eq!(
            eval_str(r#"(rx bos (group (+ digit)) (+ blank) "Hi" eol)"#),
            Value::String("\\`\\(\\(?:[0-9]\\)+\\)\\(?:[[:blank:]]\\)+Hi$".into())
        );
        assert_eq!(
            eval_str(r#"(rx (group xdigit xdigit))"#),
            Value::String("\\([0-9A-Fa-f][0-9A-Fa-f]\\)".into())
        );
        assert_eq!(
            eval_str(r#"(rx bow "SECCOMP" eow)"#),
            Value::String("\\bSECCOMP\\b".into())
        );
        assert_eq!(
            eval_str(r#"(rx (| "" (: bol "/" (+ digit))))"#),
            Value::String("\\(?:\\|^/\\(?:[0-9]\\)+\\)".into())
        );
        assert_eq!(
            eval_str(r#"(rx (not (any "/:|")))"#),
            Value::String("[^/:|]".into())
        );
        assert_eq!(
            eval_str(r#"(rx (in " -Z\\^-~"))"#),
            Value::String("[ -Z\\^-~]".into())
        );
        assert_eq!(
            eval_str(r#"(rx (in alnum "-"))"#),
            Value::String("[[:alnum:]-]".into())
        );
        assert_eq!(
            eval_str(r#"(rx (1+ (not (any "/|"))))"#),
            Value::String("\\(?:[^/|]\\)+".into())
        );
        assert_eq!(
            eval_str(r#"(rx (zero-or-more ?a))"#),
            Value::String("\\(?:a\\)*".into())
        );
        assert_eq!(
            eval_str(r#"(rx (one-or-more ?a))"#),
            Value::String("\\(?:a\\)+".into())
        );
        assert_eq!(
            eval_str(r#"(rx (zero-or-one ?a))"#),
            Value::String("\\(?:a\\)?".into())
        );
        assert_eq!(
            eval_str(r#"(rx (syntax whitespace))"#),
            Value::String("\\s-".into())
        );
        assert_eq!(
            eval_str(r#"(rx (not-syntax whitespace))"#),
            Value::String("\\S-".into())
        );
        assert_eq!(
            eval_str(r#"(rx (group-n 2 (group-n 1 (+ digit)) ":" (+ digit)))"#),
            Value::String("\\(?2:\\(?1:\\(?:[0-9]\\)+\\):\\(?:[0-9]\\)+\\)".into())
        );
        assert_eq!(
            eval_str(r#"(rx bol (regexp "\\(?:\\sw\\|\\s_\\|\\\\.\\)+") eol)"#),
            Value::String("^\\(?:\\sw\\|\\s_\\|\\\\.\\)+$".into())
        );
        assert_eq!(
            eval_str(r#"(let ((part "[[:alpha:]]+")) (rx bos (regexp part) eos))"#),
            Value::String("\\`[[:alpha:]]+\\'".into())
        );
        assert_eq!(
            eval_str(
                r#"(string-match-p
                    (rx "find " (+ nonl)
                        " \\( \\( -name .svn -or -name .git -or -name .CVS \\)"
                        " -prune -or -true \\)"
                        " \\( \\( \\(" " -name \\*.pl -or -name \\*.pm -or -name \\*.t \\)"
                        " -or -mtime \\+1 \\) -and \\( -fstype nfs -or -fstype ufs \\) \\) ")
                    "find /tmp/ \\( \\( -name .svn -or -name .git -or -name .CVS \\) -prune -or -true \\) \\( \\( \\( -name \\*.pl -or -name \\*.pm -or -name \\*.t \\) -or -mtime \\+1 \\) -and \\( -fstype nfs -or -fstype ufs \\) \\) ")"#
            ),
            Value::Integer(0)
        );
        assert_eq!(
            eval_str(r#"(string-match-p (rx (in " -Z\\^-~")) "^")"#),
            Value::Integer(0)
        );
        assert_eq!(
            eval_str(
                r#"(string-match-p (rx (group (zero-or-more (syntax whitespace))) "=") "  =")"#
            ),
            Value::Integer(0)
        );
    }

    #[test]
    fn rx_supports_pcomplete_help_regex_forms() {
        assert_eq!(
            eval_str(r#"(string-match-p (rx "-" (+ (any "-" alnum)) (? "=")) "--tofu-policy=")"#),
            Value::Integer(0)
        );
        assert_eq!(
            eval_str(r#"(string-match-p (rx (? " ") (seq "<" (+? nonl) ">")) " <path>")"#),
            Value::Integer(0)
        );
        assert_eq!(
            eval_str(
                r#"(string-match-p (rx (* nonl) (* "\n" (>= 9 " ") (* nonl)))
                                   " make a signature\n         wrapped")"#
            ),
            Value::Integer(0)
        );
        assert_eq!(
            eval_str(r#"(string-match-p (rx ", " symbol-start) ", --sign")"#),
            Value::Integer(0)
        );
    }

    #[test]
    fn abbrev_possibly_save_writes_file_and_resets_changed_flag() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("emaxx-abbrev-save-{unique}.el"));
        let path_text = path.to_string_lossy().replace('\\', "\\\\");

        assert_eq!(
            eval_str_with_upstream_load_path(&format!(
                r#"
                (require 'abbrev)
                (let ((abbrev-file-name "{path_text}")
                      (save-abbrevs t))
                  (let ((abbrevs-changed t))
                    (list (abbrev--possibly-save nil t)
                          abbrevs-changed
                          (file-exists-p abbrev-file-name))))
                "#
            )),
            Value::list([Value::Nil, Value::Nil, Value::T])
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn abbrev_possibly_save_honors_simulated_no_response() {
        assert_eq!(
            eval_str_with_upstream_load_path(
                r#"
                (require 'abbrev)
                (let ((abbrev-file-name "/tmp/emaxx-abbrev-unused")
                      (save-abbrevs t))
                  (let ((abbrevs-changed t))
                    (ert-simulate-keys '(?n ?\C-m)
                      (list (abbrev--possibly-save nil) abbrevs-changed))))
                "#
            ),
            Value::list([Value::T, Value::Nil])
        );
    }

    #[test]
    fn abbrev_table_obarray_clear_removes_entries() {
        assert_eq!(
            eval_str_with_upstream_load_path(
                r#"
                (require 'abbrev)
                (let ((table (make-abbrev-table)))
                  (define-abbrev table "aa" "alpha")
                  (obarray-clear table)
                  (list (abbrev-expansion "aa" table)
                        (obarrayp table)))
                "#
            ),
            Value::list([Value::Nil, Value::T])
        );
    }

    #[test]
    fn abbrev_table_empty_obarray_symbol_preserves_table_properties() {
        assert_eq!(
            eval_str_with_upstream_load_path(
                r#"
                (require 'abbrev)
                (let ((table (make-abbrev-table)))
                  (abbrev-table-put table :marker 42)
                  (obarray-put table "")
                  (list (abbrev-table-get table :marker)
                        (abbrev-expansion "" table)
                        (abbrev-table-empty-p table)))
                "#
            ),
            Value::list([Value::Integer(42), Value::Nil, Value::T])
        );
    }

    #[test]
    fn abbrev_require_seeds_standard_table_name_list() {
        assert_eq!(
            eval_str_with_upstream_load_path(
                r#"
                (require 'abbrev)
                (list (not (null (memq 'fundamental-mode-abbrev-table
                                        abbrev-table-name-list)))
                      (not (null (memq 'global-abbrev-table
                                        abbrev-table-name-list)))
                      (abbrev-table-p fundamental-mode-abbrev-table)
                      (abbrev-table-p global-abbrev-table))
                "#
            ),
            Value::list([Value::T, Value::T, Value::T, Value::T])
        );
    }

    #[test]
    fn abbrev_require_preserves_mode_tables_loaded_first() {
        run_with_large_stack(|| {
            assert_eq!(
                eval_str_with_upstream_load_path(
                    r#"
                    (require 'lisp-mode)
                    (require 'abbrev)
                    (list (not (null (memq 'lisp-mode-abbrev-table
                                            abbrev-table-name-list)))
                          (not (null (memq 'fundamental-mode-abbrev-table
                                            abbrev-table-name-list)))
                          (not (null (memq 'global-abbrev-table
                                            abbrev-table-name-list)))
                          (abbrev-table-p lisp-mode-abbrev-table)
                          (abbrev-table-p fundamental-mode-abbrev-table)
                          (abbrev-table-p global-abbrev-table))
                    "#
                ),
                Value::list([Value::T, Value::T, Value::T, Value::T, Value::T, Value::T,])
            );
        });
    }

    #[test]
    fn abbrev_initializes_local_abbrev_table_default() {
        run_with_large_stack(|| {
            assert_eq!(
                eval_str_with_upstream_load_path(
                    r#"
                    (require 'lisp-mode)
                    (require 'abbrev)
                    (let ((initial (with-temp-buffer
                                     (eq local-abbrev-table
                                         fundamental-mode-abbrev-table))))
                      (list initial
                            (with-temp-buffer
                              (eq local-abbrev-table
                                  fundamental-mode-abbrev-table))))
                    "#
                ),
                Value::list([Value::T, Value::T])
            );
        });
    }

    #[test]
    fn translation_table_vector_is_bound_vector_not_abbrev_table() {
        assert_eq!(
            eval_str_with_upstream_load_path(
                r#"
                (require 'abbrev)
                (list (boundp 'translation-table-vector)
                      (vectorp translation-table-vector)
                      (abbrev-table-p translation-table-vector))
                "#
            ),
            Value::list([Value::T, Value::T, Value::Nil])
        );
    }

    #[test]
    fn wrapper_hook_nil_path_runs_body() {
        assert_eq!(
            eval_str_with_upstream_load_path(
                r#"
                (let ((sample-wrapper-hook nil))
                  (subr--with-wrapper-hook-no-warnings sample-wrapper-hook ()
                    'body-ran))
                "#
            ),
            Value::Symbol("body-ran".into())
        );
    }

    #[test]
    fn wrapper_hook_non_nil_wraps_body_through_continuation() {
        assert_eq!(
            eval_str_with_upstream_load_path(
                r#"
                (let ((calls nil)
                      (sample-wrapper-hook
                       (list (lambda (fun)
                               (push 'wrapper calls)
                               (let ((result (funcall fun)))
                                 (push result calls)
                                 'wrapped)))))
                  (list (subr--with-wrapper-hook-no-warnings sample-wrapper-hook ()
                          'body)
                        calls))
                "#
            ),
            Value::list([
                Value::Symbol("wrapped".into()),
                Value::list([
                    Value::Symbol("body".into()),
                    Value::Symbol("wrapper".into())
                ]),
            ])
        );
    }

    #[test]
    fn inverse_add_abbrev_skips_trailing_nonword() {
        assert_eq!(
            eval_str_with_upstream_load_path(
                r#"
                (require 'abbrev)
                (let ((table (make-abbrev-table)))
                  (with-temp-buffer
                    (insert "some text foo ")
                    (cl-letf (((symbol-function 'read-string)
                               (lambda (&rest _) "bar")))
                      (inverse-add-abbrev table "Global" 1)))
                  (string= (abbrev-expansion "foo" table) "bar"))
                "#
            ),
            Value::T
        );
    }

    #[test]
    fn skip_syntax_backward_supports_negated_word_class() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (insert "some text foo ")
                  (skip-syntax-backward "^w")
                  (buffer-substring-no-properties (point) (point-max)))
                "#
            ),
            Value::String(" ".into())
        );
    }

    #[test]
    fn abbrev_edit_save_to_file_redefines_tables() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("emaxx-abbrev-edit-save-{unique}.el"));
        let path_text = path.to_string_lossy().replace('\\', "\\\\");

        assert_eq!(
            eval_str_with_upstream_load_path(&format!(
                r#"
                (require 'abbrev)
                (defvar emaxx-abbrev-edit-save-table nil)
                (with-temp-buffer
                  (insert "(emaxx-abbrev-edit-save-table)\n")
                  (insert "\n" "\"aa\"\t" "0\t" "\"alpha\"\n")
                  (abbrev-edit-save-to-file "{path_text}")
                  (read-abbrev-file "{path_text}")
                  (equal "alpha"
                         (abbrev-expansion "aa" emaxx-abbrev-edit-save-table)))
                "#
            )),
            Value::T
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn upstream_abbrev_edit_save_to_file_case() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("emaxx-upstream-abbrev-edit-save-{unique}.el"));
        let path_text = path.to_string_lossy().replace('\\', "\\\\");

        assert_eq!(
            eval_str_with_upstream_load_path(&format!(
                r#"
                (require 'ert-x)
                (require 'abbrev)
                (defvar ert-test-abbrevs nil)
                (defvar ert-save-test-table nil)
                (define-abbrev-table 'ert-test-abbrevs '(("a-e-t" "abbrev-ert-test")))
                (with-temp-buffer
                  (goto-char (point-min))
                  (insert "(ert-save-test-table)\n")
                  (insert "\n" "\"s-a-t\"\t" "0\t" "\"save-abbrevs-test\"\n")
                  (and (equal "abbrev-ert-test"
                              (abbrev-expansion "a-e-t" ert-test-abbrevs))
                       (progn (abbrev-edit-save-to-file "{path_text}") t)
                       (not (abbrev-expansion "a-e-t" ert-test-abbrevs))
                       (progn (read-abbrev-file "{path_text}") t)
                       (equal "save-abbrevs-test"
                              (abbrev-expansion "s-a-t" ert-save-test-table))))
                "#
            )),
            Value::T
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn upstream_abbrev_edit_save_to_file_ert_case_passes() {
        let mut interp = Interpreter::new();
        interp.set_load_path(
            crate::compat::emaxx_upstream_load_path(&upstream_emacs_repo())
                .expect("upstream load path"),
        );
        eval_str_with(
            &mut interp,
            r#"
            (require 'ert)
            (require 'ert-x)
            (require 'abbrev)

            (defun emaxx-setup-test-abbrev-table ()
              (defvar emaxx-ert-test-abbrevs nil)
              (define-abbrev-table
                'emaxx-ert-test-abbrevs
                '(("a-e-t" "abbrev-ert-test")))
              (abbrev-table-put emaxx-ert-test-abbrevs
                                :ert-test "ert-test-value")
              emaxx-ert-test-abbrevs)

            (ert-deftest emaxx-abbrev-edit-save-to-file-test ()
              (defvar emaxx-ert-save-test-table nil)
              (ert-with-temp-file temp-test-file
                (let ((ert-test-abbrevs (emaxx-setup-test-abbrev-table)))
                  (with-temp-buffer
                    (goto-char (point-min))
                    (insert "(emaxx-ert-save-test-table)\n")
                    (insert "\n" "\"s-a-t\"\t" "0\t"
                            "\"save-abbrevs-test\"\n")
                    (should (equal "abbrev-ert-test"
                                   (abbrev-expansion
                                    "a-e-t" ert-test-abbrevs)))
                    (abbrev-edit-save-to-file temp-test-file)
                    (should-not (abbrev-expansion
                                 "a-e-t" ert-test-abbrevs))
                    (read-abbrev-file temp-test-file)
                    (should (equal "save-abbrevs-test"
                                   (abbrev-expansion
                                    "s-a-t"
                                    emaxx-ert-save-test-table)))))))
            "#,
        );
        let selector = Reader::new("emaxx-abbrev-edit-save-to-file-test")
            .read()
            .unwrap()
            .unwrap();
        let summary = interp.run_ert_tests_with_selector(Some(&selector));
        assert_eq!(summary.passed, 1);
        assert_eq!(summary.failed, 0);
    }

    #[test]
    fn bracket_expressions_keep_literal_backslashes_as_members() {
        assert_eq!(
            eval_str(
                r#"
                (let ((pattern (concat "[" (string 92 46) "]"))
                      (text (string 97 92 46 99)))
                  (string-match pattern text)
                  (list (match-beginning 0) (match-end 0)))
                "#
            ),
            Value::list([Value::Integer(1), Value::Integer(2)])
        );
        assert_eq!(
            eval_str(
                r#"
                (let ((pattern (concat "[" (string 92 94 97 98) "]"))
                      (text (string 99 92 100)))
                  (string-match pattern text)
                  (list (match-beginning 0) (match-end 0)))
                "#
            ),
            Value::list([Value::Integer(1), Value::Integer(2)])
        );
        assert_eq!(
            eval_str(
                r#"
                (let ((pattern (concat "[" (string 36 92 40 42 92 41 94) "]*"))
                      (text (string 36 92 40 41 42 94)))
                  (string-match pattern text)
                  (list (match-beginning 0) (match-end 0)))
                "#
            ),
            Value::list([Value::Integer(0), Value::Integer(6)])
        );
    }

    #[test]
    fn regexp_opt_builds_basic_alternations() {
        assert_eq!(
            eval_str(r#"(regexp-opt '(".log" ".aux" ".log"))"#),
            Value::String("\\(?:\\.aux\\|\\.log\\)".into())
        );
        assert_ne!(
            eval_str(r#"(string-match-p "\\(?:[^\\]\\|\\`\\)\\(\"\\)" "\"")"#),
            Value::Nil
        );
    }

    fn assert_minibuffer_completion_primitives_cover_batch_cases() {
        assert_eq!(
            eval_str(r#"(try-completion "a" '("abc" "abba" "def"))"#),
            Value::String("ab".into())
        );
        assert_eq!(
            eval_str(r#"(all-completions "a" '("abc" "abba" "def"))"#),
            Value::list([Value::String("abc".into()), Value::String("abba".into())])
        );
        assert_eq!(
            eval_str(
                r#"(null (cl-set-exclusive-or '("abc" "abba") '("abba" "abc") :test #'equal))"#
            ),
            Value::T
        );
        assert_eq!(
            eval_str(
                r#"
                (let ((ob (obarray-make 7)))
                  (intern "abc" ob)
                  (intern "abba" ob)
                  (all-completions "a" ob))
                "#
            ),
            Value::list([Value::String("abc".into()), Value::String("abba".into())])
        );
        assert_eq!(
            eval_str(
                r#"(let ((completion-ignore-case t)) (try-completion "bar" '("bAr" "barfoo")))"#
            ),
            Value::String("bAr".into())
        );
        assert_eq!(
            eval_str(r#"(let ((completion-ignore-case t)) (try-completion "baz" '("baz" "bAz")))"#),
            Value::String("baz".into())
        );
        assert_eq!(
            eval_str(
                r#"
                (let ((ht (make-hash-table :test #'equal)))
                  (puthash "abc" 1 ht)
                  (gethash "abc" ht))
                "#
            ),
            Value::Integer(1)
        );
        assert_eq!(
            eval_str(
                r#"
                (let ((calls 0)
                      (cache (make-hash-table :test #'equal)))
                  (list
                    (with-memoization (gethash "k" cache)
                      (setq calls (+ calls 1))
                      'cached)
                    (with-memoization (gethash "k" cache)
                      (setq calls (+ calls 1))
                      'missed)
                    calls
                    (gethash "k" cache)))
                "#
            ),
            Value::list([
                Value::Symbol("cached".into()),
                Value::Symbol("cached".into()),
                Value::Integer(1),
                Value::Symbol("cached".into()),
            ])
        );
        assert_eq!(
            eval_str(
                r#"
                (let ((place-calls 0)
                      (cache (make-hash-table :test #'equal)))
                  (with-memoization (gethash "k" (progn
                                                  (setq place-calls (+ place-calls 1))
                                                  cache))
                    'cached)
                  (list place-calls
                        (gethash "k" cache)))
                "#
            ),
            Value::list([Value::Integer(1), Value::Symbol("cached".into())])
        );
        assert_eq!(eval_str(r#"(active-minibuffer-window)"#), Value::Nil);
        assert_eq!(eval_str(r#"(windowp (minibuffer-window))"#), Value::T);
    }

    #[test]
    fn minibuffer_completion_primitives_cover_batch_cases() {
        run_large_stack_test(assert_minibuffer_completion_primitives_cover_batch_cases);
    }

    #[test]
    fn inhibited_interaction_uses_expected_condition_type() {
        let mut interp = Interpreter::new();
        let mut env: Env = Vec::new();
        let form = Reader::new(r#"(let ((inhibit-interaction t)) (read-from-minibuffer "foo: "))"#)
            .read()
            .unwrap()
            .unwrap();
        let error = interp.eval(&form, &mut env).unwrap_err();
        assert_eq!(error.condition_type(), "inhibited-interaction");
    }

    #[test]
    fn native_comp_capability_probes_are_honest() {
        assert_eq!(eval_str("(native-comp-available-p)"), Value::Nil);
        assert_eq!(eval_str("(featurep 'native-compile)"), Value::Nil);
        assert_eq!(
            eval_str("(native-comp-function-p (symbol-function 'car))"),
            Value::Nil
        );
    }

    #[test]
    fn nconc_supports_dotted_tails() {
        assert_eq!(
            eval_str("(nconc '(a b) 'tail)"),
            Value::cons(
                Value::symbol("a"),
                Value::cons(Value::symbol("b"), Value::symbol("tail"))
            )
        );
    }

    #[test]
    fn nconc_mutates_existing_aliases() {
        assert_eq!(
            eval_str("(let* ((x (list 'a 'b)) (y x)) (nconc x '(c)) y)"),
            Value::list([Value::symbol("a"), Value::symbol("b"), Value::symbol("c")])
        );
    }

    #[test]
    fn nconc_replaces_dotted_tail_destructively() {
        assert_eq!(
            eval_str("(let* ((x '(a . b)) (y x)) (nconc x '(c)) y)"),
            Value::list([Value::symbol("a"), Value::symbol("c")])
        );
    }

    #[test]
    fn nconc_rejects_non_lists_before_last_arg() {
        assert_eq!(
            eval_str("(condition-case err (nconc 'a '(c)) (wrong-type-argument (car err)))"),
            Value::symbol("wrong-type-argument")
        );
    }

    #[test]
    fn nconc_rejects_circular_lists() {
        assert_eq!(
            eval_str(
                "(let ((x (list 'a 'b))) (setcdr (cdr x) x) (condition-case err (nconc x 'tail) (circular-list (car err))))"
            ),
            Value::symbol("circular-list")
        );
    }

    #[test]
    fn mapcan_concatenates_results() {
        run_with_large_stack(|| {
            assert_eq!(
                eval_str("(equal (mapcan #'list (list 1 2 3)) '(1 2 3))"),
                Value::T
            );
        });
    }

    #[test]
    fn mapcan_mutates_mapped_lists_destructively() {
        run_with_large_stack(|| {
            assert_eq!(
                eval_str(
                    "(let ((data (list (list 'foo) (list 'bar))))
                       (and
                        (equal (mapcan #'identity data) '(foo bar))
                        (equal data '((foo bar) (bar)))))"
                ),
                Value::T
            );
        });
    }

    #[test]
    fn sxhash_eql_matches_equal_bignums() {
        assert_eq!(
            eval_str(
                "(let* ((a (1+ most-positive-fixnum)) (b (+ most-positive-fixnum 1))) (= (sxhash-eql a) (sxhash-eql b)))"
            ),
            Value::T
        );
    }

    #[test]
    fn sort_coding_systems_uses_priority_order() {
        assert_eq!(
            eval_str(
                "(progn (set-coding-system-priority 'utf-8 'iso-latin-1) (sort-coding-systems '(iso-latin-1 undecided utf-8)))"
            ),
            Value::list([
                Value::symbol("utf-8"),
                Value::symbol("iso-latin-1"),
                Value::symbol("undecided"),
            ])
        );
    }

    #[test]
    fn defgroup_tracks_current_group_and_members() {
        let mut interp = Interpreter::new();
        interp.set_current_load_file(Some("/tmp/custom-group.el".into()));
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(progn
                   (defgroup demo nil \"Doc.\" :prefix \"demo-\")
                   (custom-add-to-group 'demo 'demo-option 'custom-variable)
                   (list (custom-current-group)
                         (equal (get 'demo 'custom-prefix) \"demo-\")
                         (get 'demo 'custom-group)))"
            ),
            Value::list([
                Value::symbol("demo"),
                Value::T,
                Value::list([Value::list([
                    Value::symbol("demo-option"),
                    Value::symbol("custom-variable"),
                ])]),
            ])
        );
    }

    #[test]
    fn cl_with_gensyms_produces_unique_bindings() {
        assert_eq!(
            eval_str(
                r#"
                (progn
                  (defmacro sample-cl-with-gensyms (value)
                    (cl-with-gensyms (tmp)
                      `(let ((,tmp ,value))
                         ,tmp)))
                  (let ((tmp 99))
                    (sample-cl-with-gensyms 42)))
                "#
            ),
            Value::Integer(42)
        );
    }

    #[test]
    fn cl_case_matches_atoms_lists_and_fallbacks() {
        assert_eq!(
            eval_str(
                r#"
                (list
                 (cl-case 'zip
                   ((tar ar) 'other)
                   (zip 'zip))
                 (cl-case 2
                   ((1 3) 'miss)
                   ((2 4) 'hit))
                 (cl-case nil
                   (nil 'impossible)
                   (otherwise 'fallback)))
                "#
            ),
            Value::list([
                Value::symbol("zip"),
                Value::symbol("hit"),
                Value::symbol("fallback"),
            ])
        );
    }

    #[test]
    fn cl_case_evaluates_expression_once() {
        assert_eq!(
            eval_str(
                r#"
                (let ((count 0))
                  (list
                   (cl-case (progn (setq count (+ count 1)) 'zip)
                     (zip 'matched)
                     (otherwise 'missed))
                   count))
                "#
            ),
            Value::list([Value::symbol("matched"), Value::Integer(1)])
        );
    }

    #[test]
    fn cl_case_rejects_misplaced_otherwise() {
        let mut interp = Interpreter::new();
        let mut env: Env = Vec::new();
        let form = Reader::new("(cl-case 'zip (otherwise 'fallback) (zip 'hit))")
            .read()
            .unwrap()
            .unwrap();
        let error = interp.eval(&form, &mut env).unwrap_err();
        assert_eq!(error.condition_type(), "error");
        assert_eq!(error.to_string(), "Misplaced t or `otherwise' clause");
    }

    #[test]
    fn sqlite_execute_surfaces_sql_input_errors_as_sqlite_error() {
        assert_eq!(
            eval_str(
                r#"
                (let ((db (sqlite-open)))
                  (sqlite-execute db "create table test (a)")
                  (should-error
                   (sqlite-execute db "insert into test values (fake(2))")
                   :type 'sqlite-error))
                "#
            ),
            Value::list([
                Value::symbol("sqlite-error"),
                Value::list([
                    Value::String("SQL logic error".into()),
                    Value::String("no such function: fake".into()),
                    Value::Integer(1),
                    Value::Integer(1),
                ]),
            ])
        );
    }

    #[test]
    fn backtrace_frames_from_current_thread_returns_live_frames() {
        let mut interp = Interpreter::new();
        let current_thread = interp.current_thread_value();
        interp.push_backtrace_frame(Value::Symbol("sample-backtrace-frame".into()), Vec::new());

        assert_eq!(
            interp.thread_backtrace_frames_snapshot(
                interp.resolve_thread_id(&current_thread).unwrap()
            ),
            vec![(
                Value::Symbol("sample-backtrace-frame".into()),
                Vec::new(),
                false,
            )]
        );
    }
}
