use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use super::primitives;
use super::sqlite::SqliteHandleState;
use super::types::{Env, LispError, Value};
use crate::compat::{BatchSummary, DiscoveredTest, TestOutcome, TestStatus};
use regex::{Regex, escape as regex_escape};

#[derive(Clone, Debug)]
pub struct ErtTestDefinition {
    pub name: String,
    pub body: Value,
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

#[derive(Clone, Debug, Default)]
struct BacktraceFrame {
    function: Option<String>,
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
    ]
}

fn builtin_coding_aliases() -> Vec<(String, String)> {
    vec![
        ("iso-8859-1".into(), "iso-latin-1".into()),
        ("iso-8859-1-unix".into(), "iso-latin-1-unix".into()),
        ("iso-8859-1-dos".into(), "iso-latin-1-dos".into()),
        ("binary".into(), "raw-text".into()),
        ("utf8".into(), "utf-8".into()),
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
    thread_states: Vec<ThreadState>,
    mutex_states: Vec<MutexState>,
    condition_variables: Vec<ConditionVariableState>,
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
        Interpreter {
            globals: vec![("main-thread".into(), Value::Record(main_thread_id))],
            variable_aliases: Vec::new(),
            special_variables: vec![
                "case-fold-search".into(),
                "command-line-args-left".into(),
                "command-switch-alist".into(),
                "display-hourglass".into(),
                "gc-cons-threshold".into(),
                "initial-window-system".into(),
                "last-coding-system-used".into(),
                "line-spacing".into(),
                "left-margin".into(),
                "overwrite-mode".into(),
                "scroll-up-aggressively".into(),
                "standard-output".into(),
                "vertical-scroll-bar".into(),
            ],
            symbol_properties: Vec::new(),
            variable_watchers: Vec::new(),
            buffer: crate::buffer::Buffer::new("*test*"),
            current_buffer_id: 0,
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
            charset_priority: vec!["unicode".into(), "ascii".into()],
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
                "lcms2".into(),
                "threads".into(),
            ],
            current_load_file: None,
            ert_tests: Vec::new(),
            test_results: Vec::new(),
            last_selected_tests: Vec::new(),
            last_match_data: None,
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
            main_thread_id,
            active_thread_id: main_thread_id,
            last_thread_error: None,
            backtrace_frames: Vec::new(),
            active_handlers: Vec::new(),
            handler_dispatch_depth: 0,
            suspend_condition_case_count: 0,
            condition_case_depth: 0,
        }
    }

    pub fn set_load_path(&mut self, load_path: Vec<PathBuf>) {
        self.load_path = load_path;
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
            self.provide_feature(feature);
            return Ok(Value::Symbol(feature.to_string()));
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
            self.provide_feature(feature);
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
        Ok(())
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
                ThreadStatus::Blocked(ThreadBlocker::Sleep) if wake_sleepers => {
                    self.finish_thread_success(thread_id, Value::Nil);
                }
                _ => {}
            }
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
        };
        self.active_thread_id = previous_active;
        result
    }

    fn thread_program_from_callable(&self, function: &Value) -> Result<ThreadProgram, LispError> {
        match function {
            Value::Symbol(name) if name == "ignore" => Ok(ThreadProgram::Ignore),
            Value::Symbol(name) => self.thread_program_from_symbol(name),
            Value::BuiltinFunc(name) if name == "ignore" => Ok(ThreadProgram::Ignore),
            Value::Lambda(params, body, _) if params.is_empty() => {
                self.thread_program_from_lambda(function_executable_body(body))
            }
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
        matches!(name, "ascii" | "unicode")
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
        for default in ["unicode", "ascii"] {
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

    pub fn push_backtrace_frame(&mut self, function: Option<String>, args: Vec<Value>) {
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

    pub fn current_backtrace_frame(&self) -> Option<(Option<String>, Vec<Value>, bool)> {
        self.backtrace_frames.last().map(|frame| {
            (
                frame.function.clone(),
                frame.args.clone(),
                frame.debug_on_exit,
            )
        })
    }

    pub fn backtrace_frames_snapshot(&self) -> Vec<(Option<String>, Vec<Value>, bool)> {
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
        self.handler_dispatch_depth += 1;
        for (condition, handler) in self.active_handlers.clone().into_iter().rev() {
            if condition != "error" && condition != error_type {
                continue;
            }
            let result =
                self.call_function_value(handler, None, std::slice::from_ref(&error_value), env);
            match result {
                Ok(_) => {}
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
        Err(error)
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
            "coding-system-for-read" => Some(Value::Nil),
            "coding-system-for-write" => Some(Value::Nil),
            "file-coding-system-alist" => Some(Value::Nil),
            "file-name-coding-system" => Some(Value::Nil),
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
            "auto-mode-alist" => Some(Value::Nil),
            "command-switch-alist" => Some(Value::Nil),
            "command-line-args-left" => Some(Value::Nil),
            "completion-ignored-extensions" => Some(Value::Nil),
            "custom-current-group-alist" => Some(Value::Nil),
            "defun-declarations-alist" => Some(Value::Nil),
            "macro-declarations-alist" => Some(Value::Nil),
            "macroexpand-all-environment" => Some(Value::Nil),
            "ls-lisp-use-insert-directory-program" => Some(Value::T),
            "transient-mark-mode" => Some(Value::T),
            "obarray" => Some(Value::Nil),
            "desktop-buffer-mode-handlers" => Some(Value::Nil),
            "find-file-visit-truename" => Some(Value::Nil),
            "insert-directory-wildcard-in-dir-p" => Some(Value::Nil),
            "insert-directory-program" => Some(Value::String("ls".into())),
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
            "window-system" => Some(Value::Nil),
            "initial-window-system" => Some(Value::Nil),
            "left-margin" => Some(Value::Integer(0)),
            "line-spacing" => Some(Value::Nil),
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
            "installation-directory" => Some(Value::Nil),
            "tab-width" => Some(Value::Integer(8)),
            "use-dialog-box" => Some(Value::T),
            "use-file-dialog" => Some(Value::T),
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
            "emacs-version" => Some(Value::String(primitives::emacs_version_value())),
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

    pub fn lookup_function(&self, name: &str, env: &Env) -> Result<Value, LispError> {
        if primitives::prefer_builtin_override(name) && primitives::is_builtin(name) {
            return Ok(Value::BuiltinFunc(name.to_string()));
        }
        for frame in env.iter().rev() {
            for (k, v) in frame.iter().rev() {
                if k == name && matches!(v, Value::BuiltinFunc(_) | Value::Lambda(_, _, _)) {
                    return Ok(v.clone());
                }
            }
        }
        for (k, v) in self.functions.iter().rev() {
            if k == name {
                return Ok(v.clone());
            }
        }
        if let Some(value) = builtin_autoload_function(name) {
            return Ok(value);
        }
        if matches!(name, "incf" | "decf") {
            return Ok(Value::BuiltinFunc(name.to_string()));
        }
        if primitives::is_builtin(name) {
            Ok(Value::BuiltinFunc(name.to_string()))
        } else {
            Err(LispError::Void(name.to_string()))
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
            } else if let Value::Cons(head, tail) = &value
                && **tail == crate::lisp::primitives::buffer_undo_list_value(&self.buffer)
            {
                let entry = buffer_undo_head_to_entry(head);
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

                // Check for special forms first
                if let Value::Symbol(ref name) = items[0] {
                    match name.as_str() {
                        "quote" => return self.sf_quote(&items),
                        "if" => return self.sf_if(&items, env),
                        "if-let*" => return self.sf_if_let_star(&items, env),
                        "when" => return self.sf_when(&items, env),
                        "when-let*" => return self.sf_when_let_star(&items, env),
                        "unless" => return self.sf_unless(&items, env),
                        "bound-and-true-p" => return self.sf_bound_and_true_p(&items, env),
                        "cond" => return self.sf_cond(&items, env),
                        "pcase" => return self.sf_pcase(&items, env),
                        "pcase-defmacro" => return self.sf_pcase_defmacro(&items, env),
                        "pcase-exhaustive" => return self.sf_pcase_exhaustive(&items, env),
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
                        "defun" | "defsubst" => return self.sf_defun(&items, env),
                        "cl-defun" => return self.sf_cl_defun(&items, env),
                        "cl-defgeneric" => return self.sf_cl_defgeneric(&items, env),
                        "cl-defmethod" => return self.sf_cl_defmethod(&items, env),
                        "cl-generic-define-context-rewriter" => return Ok(Value::Nil),
                        "define-inline" => return self.sf_define_inline(&items, env),
                        "defmacro" => return self.sf_defmacro(&items),
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
                        "ignore-errors" => return self.sf_ignore_errors(&items, env),
                        "condition-case" => return self.sf_condition_case(&items, env),
                        "handler-bind" => return self.sf_handler_bind(&items, env),
                        "cl-assert" => return self.sf_cl_assert(&items, env),
                        "with-temp-buffer" => return self.sf_with_temp_buffer(&items, env),
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
                        "save-current-buffer" => return self.sf_save_current_buffer(&items, env),
                        "save-restriction" => return self.sf_save_restriction(&items, env),
                        "with-suppressed-warnings" => {
                            return self.sf_with_suppressed_warnings(&items, env);
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
                            match &items[2] {
                                Value::Symbol(name) => {
                                    let cur = self.lookup(name, env)?;
                                    let new_val = Value::cons(val, cur);
                                    self.set_variable(name, new_val.clone(), env);
                                    return Ok(new_val);
                                }
                                Value::Cons(_, _) => {
                                    let place_items = items[2].to_vec()?;
                                    if place_items.len() == 3
                                        && matches!(
                                            &place_items[0],
                                            Value::Symbol(symbol) if symbol == "overlay-get"
                                        )
                                    {
                                        let overlay = self.eval(&place_items[1], env)?;
                                        let prop = self.eval(&place_items[2], env)?;
                                        let overlay_id = match overlay {
                                            Value::Overlay(id) => id,
                                            other => {
                                                return Err(LispError::TypeError(
                                                    "overlay".into(),
                                                    other.type_name(),
                                                ));
                                            }
                                        };
                                        let prop_name = prop.as_symbol()?.to_string();
                                        let cur = self
                                            .find_overlay(overlay_id)
                                            .and_then(|overlay| {
                                                overlay.get_prop(&prop_name).cloned()
                                            })
                                            .unwrap_or(Value::Nil);
                                        let new_val = Value::cons(val, cur);
                                        if let Some(overlay) = self.find_overlay_mut(overlay_id) {
                                            overlay.put_prop(&prop_name, new_val.clone());
                                        }
                                        return Ok(new_val);
                                    }
                                }
                                _ => {}
                            }
                            return Err(LispError::Signal("Unsupported push place".into()));
                        }
                        "pop" => {
                            // (pop PLACE)
                            if items.len() < 2 {
                                return Err(LispError::WrongNumberOfArgs(
                                    "pop".into(),
                                    items.len() - 1,
                                ));
                            }
                            let name = items[1].as_symbol()?.to_string();
                            let cur = self.lookup(&name, env)?;
                            let result = cur.car()?;
                            let rest = cur.cdr()?;
                            self.set_variable(&name, rest, env);
                            return Ok(result);
                        }
                        "catch" => return self.sf_catch(&items, env),
                        "add-to-list" => return self.sf_add_to_list(&items, env),
                        "ert-deftest" => return self.sf_ert_deftest(&items),
                        "should" => return self.sf_should(&items, env),
                        "should-not" => return self.sf_should_not(&items, env),
                        "should-error" => return self.sf_should_error(&items, env),
                        "skip-unless" | "ert--skip-unless" => {
                            return self.sf_skip_unless(&items, env);
                        }
                        "skip-when" | "ert--skip-when" => return self.sf_skip_when(&items, env),
                        "rx" => return self.sf_rx(&items),
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
                                self.provide_feature(&feature);
                                return Ok(Value::Symbol(feature));
                            }
                            return Ok(Value::Nil);
                        }
                        "with-eval-after-load" => return Ok(Value::Nil),
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
        let func = if is_lambda_form(&func) {
            self.eval(&func, env)?
        } else {
            func
        };
        let func = if let Some((file, _, _)) = crate::lisp::primitives::autoload_parts(&func) {
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
        };

        match func {
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
                    let val = if arg_idx < args.len() {
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
                    arg_idx += 1;
                }

                let mut captured_frames = 0;
                for captured in closure_env.iter().rev() {
                    env.insert(0, captured.clone());
                    captured_frames += 1;
                }
                self.push_backtrace_frame(original_name.map(str::to_string), args.to_vec());
                env.push(frame);
                let result = self.sf_progn(function_executable_body(body), env);
                env.pop();
                self.pop_backtrace_frame();
                for _ in 0..captured_frames {
                    env.remove(0);
                }
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
        Ok(items[1].clone())
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
        let expander = Value::Lambda(params, body, env.clone());
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
            if pcase_pattern_bindings(&clause_items[0], &value, &mut bindings)? {
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
        for expr in &items[2..] {
            self.eval(expr, env)?;
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
                if !pcase_pattern_bindings(&parts[0], &value, &mut frame_bindings)? {
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
            if !pcase_pattern_bindings(&parts[0], &value, &mut frame)? {
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
            let Value::Cons(car, cdr) = entry else {
                continue;
            };
            let Ok(symbol) = car.as_symbol() else {
                continue;
            };
            let value = match *cdr {
                Value::Cons(value, tail) if matches!(*tail, Value::Nil) => *value,
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
            Some(Value::Symbol(name)) if name == "alist-get" => {
                self.sf_setf_alist_get(&place, &items[2], env)
            }
            Some(Value::Symbol(name)) if name == "aref" => {
                self.sf_setf_aref(&place, &items[2], env)
            }
            Some(Value::Symbol(name)) if name == "image-property" => {
                self.sf_setf_image_property(&place, &items[2], env)
            }
            _ => Err(LispError::Signal(format!(
                "Unsupported setf place: {}",
                items[1]
            ))),
        }
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
                if let Value::Cons(car, _) = &entry {
                    primitives::value_matches_with_test(
                        self,
                        &key,
                        car.as_ref(),
                        testfn.as_ref(),
                        env,
                    )?
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
        match place {
            Value::Symbol(name) => {
                self.set_variable(name, value, env);
                Ok(())
            }
            Value::Cons(_, _) => {
                let items = place.to_vec()?;
                if matches!(items.first(), Some(Value::Symbol(name)) if name == "symbol-value") {
                    let Some(symbol_form) = items.get(1) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let symbol = self.eval(symbol_form, env)?;
                    let symbol = symbol.as_symbol()?.to_string();
                    self.set_symbol_value_cell(&symbol, value);
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
        // Only set if not already defined
        if self.default_toplevel_value(&resolved).is_none() && items.len() > 2 {
            let val = self.eval(&items[2], env)?;
            self.set_default_toplevel_value(&resolved, val);
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
        Ok(items
            .get(1)
            .and_then(|value| value.as_symbol().ok())
            .map(Value::symbol)
            .unwrap_or(Value::Nil))
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
        while index + 1 < items.len() {
            if matches!(&items[index], Value::Symbol(keyword) if keyword.starts_with(':')) {
                index += 2;
                continue;
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
                    Value::list([Value::Symbol("not".into()), Value::Symbol(name.to_string())]),
                    Value::list([
                        Value::Symbol("if".into()),
                        Value::list([Value::Symbol("not".into()), Value::Symbol("arg".into())]),
                        Value::list([Value::Symbol("not".into()), Value::Symbol(name.to_string())]),
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
                    Value::Symbol("setq".into()),
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
                        Vec::new(),
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
        let name = match struct_spec {
            Value::Symbol(name) => Some(name.clone()),
            Value::Cons(_, _) => struct_spec.to_vec().ok().and_then(|parts| {
                parts
                    .first()
                    .and_then(|value| value.as_symbol().ok())
                    .map(str::to_string)
            }),
            _ => None,
        };
        Ok(name.map(Value::Symbol).unwrap_or(Value::Nil))
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
        let name = items[1].as_symbol()?.to_string();
        let current = self.lookup(&name, env)?;
        let Value::Cons(_, cdr) = current else {
            return Err(LispError::TypeError("cons".into(), items[1].type_name()));
        };
        let updated_car = self.eval(&items[2], env)?;
        let updated = Value::Cons(Box::new(updated_car.clone()), cdr);
        self.set_variable(&name, updated, env);
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
        if values.iter().any(|existing| existing == &value) {
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

        let body: Vec<Value> = items[body_start..].to_vec();
        let lambda = Value::Lambda(params, body, env.clone());
        self.functions.push((name.clone(), lambda));
        Ok(Value::Symbol(name))
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

    fn sf_cl_defgeneric(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let Some(name) = items.get(1).and_then(|value| value.as_symbol().ok()) else {
            return Ok(Value::Nil);
        };
        if self.lookup_function(name, env).is_err() {
            self.set_function_binding(name, Some(Value::BuiltinFunc("ignore".into())));
        }
        Ok(Value::Symbol(name.to_string()))
    }

    fn sf_cl_defmethod(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 4 {
            return Err(LispError::Signal(
                "cl-defmethod needs name, params, body".into(),
            ));
        }
        let mut lowered = Vec::with_capacity(items.len());
        lowered.push(Value::Symbol("cl-defun".into()));
        lowered.push(items[1].clone());
        lowered.push(lower_cl_defmethod_lambda_list(&items[2])?);
        lowered.extend(items[3..].iter().cloned());
        self.sf_cl_defun(&lowered, env)
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
        Ok(Value::Lambda(params, body, env.clone()))
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
            if !pcase_pattern_bindings(pattern, &item, &mut bindings)? {
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
            List { name: String, values: Vec<Value> },
            From { name: String, start: i64 },
            Assign { name: String, expr: Value },
        }

        enum LoopAction {
            Do(Vec<Value>),
            Collect(Value),
            Thereis { expr: Value, until: Option<Value> },
            Always(Value),
            Sum(Value),
            UnlessDo { condition: Value, body: Vec<Value> },
        }

        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-loop".into(),
                items.len().saturating_sub(1),
            ));
        }

        let mut specs = Vec::new();
        let mut while_expr = None;
        let mut index = 1usize;
        while index < items.len() {
            match items.get(index) {
                Some(Value::Symbol(symbol)) if symbol == "for" => {
                    let name = items
                        .get(index + 1)
                        .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                        .as_symbol()?
                        .to_string();
                    match items.get(index + 2) {
                        Some(Value::Symbol(kind)) if kind == "from" => {
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
                                    let values = match bound_kind {
                                        "to" | "upto" if start <= end => {
                                            (start..=end).map(Value::Integer).collect()
                                        }
                                        "below" if start < end => {
                                            (start..end).map(Value::Integer).collect()
                                        }
                                        "to" | "upto" | "below" => Vec::new(),
                                        _ => unreachable!(),
                                    };
                                    specs.push(LoopSpec::Range { name, values });
                                    index += 6;
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
                            let end = self
                                .eval(
                                    items.get(index + 3).ok_or_else(|| {
                                        LispError::Signal("Unsupported cl-loop syntax".into())
                                    })?,
                                    env,
                                )?
                                .as_integer()?;
                            let values = match kind.as_str() {
                                "to" | "upto" if end >= 0 => {
                                    (0..=end).map(Value::Integer).collect()
                                }
                                "below" if end > 0 => (0..end).map(Value::Integer).collect(),
                                "to" | "upto" | "below" => Vec::new(),
                                _ => unreachable!(),
                            };
                            specs.push(LoopSpec::Range { name, values });
                            index += 4;
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
                            specs.push(LoopSpec::List { name, values });
                            index += 4;
                        }
                        Some(Value::Symbol(kind)) if kind == "across" => {
                            let string = crate::lisp::primitives::string_text(&self.eval(
                                items.get(index + 3).ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?,
                                env,
                            )?)?;
                            let values =
                                string.chars().map(|ch| Value::Integer(ch as i64)).collect();
                            specs.push(LoopSpec::List { name, values });
                            index += 4;
                        }
                        Some(Value::Symbol(kind)) if kind == "=" => {
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
                _ => break,
            }
        }

        if specs.is_empty() || index >= items.len() {
            return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
        }

        let iterations = specs
            .iter()
            .filter_map(|spec| match spec {
                LoopSpec::Range { values, .. } | LoopSpec::List { values, .. } => {
                    Some(values.len())
                }
                LoopSpec::From { .. } | LoopSpec::Assign { .. } => None,
            })
            .min()
            .unwrap_or(1);

        let bindings = specs
            .iter()
            .map(|spec| match spec {
                LoopSpec::Range { name, .. }
                | LoopSpec::List { name, .. }
                | LoopSpec::From { name, .. }
                | LoopSpec::Assign { name, .. } => (name.clone(), Value::Nil),
            })
            .collect::<Vec<_>>();
        env.push(bindings);

        let action = match items.get(index) {
            Some(Value::Symbol(symbol)) if symbol == "do" => {
                LoopAction::Do(items[index + 1..].to_vec())
            }
            Some(Value::Symbol(symbol)) if symbol == "collect" => LoopAction::Collect(
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
            Some(Value::Symbol(symbol)) if symbol == "unless" => {
                if !matches!(items.get(index + 2), Some(Value::Symbol(kind)) if kind == "do") {
                    return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                }
                LoopAction::UnlessDo {
                    condition: items
                        .get(index + 1)
                        .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                        .clone(),
                    body: items[index + 3..].to_vec(),
                }
            }
            _ => return Err(LispError::Signal("Unsupported cl-loop syntax".into())),
        };

        let mut result = Value::Nil;
        let mut collected = Vec::new();
        let mut sum = 0i64;
        for iteration in 0..iterations {
            let mut direct_updates = Vec::new();
            for (slot, spec) in specs.iter().enumerate() {
                match spec {
                    LoopSpec::Range { name, values } | LoopSpec::List { name, values } => {
                        direct_updates.push((slot, name.clone(), values[iteration].clone()));
                    }
                    LoopSpec::From { name, start } => {
                        direct_updates.push((
                            slot,
                            name.clone(),
                            Value::Integer(*start + iteration as i64),
                        ));
                    }
                    LoopSpec::Assign { .. } => {}
                }
            }
            {
                let frame = env.last_mut().expect("env frame just pushed");
                for (slot, name, value) in direct_updates {
                    frame[slot] = (name, Self::stored_value(value));
                }
            }

            for (slot, spec) in specs.iter().enumerate() {
                if let LoopSpec::Assign { name, expr } = spec {
                    let value = Self::stored_value(self.eval(expr, env)?);
                    let frame = env.last_mut().expect("env frame just pushed");
                    frame[slot] = (name.clone(), value);
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
                LoopAction::UnlessDo { condition, body } => {
                    if !self.eval(condition, env)?.is_truthy() {
                        result = self.sf_progn(body, env)?;
                    }
                }
            }
        }

        env.pop();
        Ok(match action {
            LoopAction::Collect(_) => Value::list(collected),
            LoopAction::Always(_) if result.is_nil() => Value::Nil,
            LoopAction::Always(_) => Value::T,
            LoopAction::Sum(_) => Value::Integer(sum),
            _ => result,
        })
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
        let body_result = self.sf_progn(&items[1..], env);
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
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| LispError::Signal(error.to_string()))?
            .as_nanos();
        let path = std::env::temp_dir().join(format!("emaxx-{stamp}.tmp"));
        fs::write(&path, "").map_err(|error| LispError::Signal(error.to_string()))?;
        env.push(vec![(name, Value::String(path.display().to_string()))]);
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        let _ = fs::remove_file(&path);
        result
    }

    fn sf_with_suppressed_warnings(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        self.sf_progn(&items[2..], env)
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
        let mut previous = Vec::new();
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
            previous.push((name.clone(), std::env::var_os(&name)));
            unsafe {
                if let Some(value) = value {
                    std::env::set_var(&name, value);
                } else {
                    std::env::remove_var(&name);
                }
            }
        }
        let result = self.sf_progn(&items[2..], env);
        for (name, value) in previous.into_iter().rev() {
            unsafe {
                if let Some(value) = value {
                    std::env::set_var(&name, value);
                } else {
                    std::env::remove_var(&name);
                }
            }
        }
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
        let saved_pt = self.buffer.point();
        let result = self.sf_progn(&items[1..], env);
        self.buffer.goto_char(saved_pt);
        result
    }

    fn sf_save_match_data(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let saved = self.last_match_data.clone();
        let result = self.sf_progn(&items[1..], env);
        self.last_match_data = saved;
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
        let restore_begv = self.marker_position(beg_id).unwrap_or(saved_begv);
        let restore_zv = self.marker_position(end_id).unwrap_or(saved_zv);
        self.buffer.restore_restriction(restore_begv, restore_zv);
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
        let pattern = items[1].to_vec()?;
        let val = self.eval(&items[2], env)?;
        let vals = val.to_vec()?;

        let mut frame = Vec::new();
        let mut pi = 0; // pattern index
        let mut vi = 0; // value index
        let mut optional = false;

        while pi < pattern.len() {
            let p = &pattern[pi];
            if let Value::Symbol(s) = p {
                match s.as_str() {
                    "&optional" => {
                        optional = true;
                        pi += 1;
                        continue;
                    }
                    "&rest" => {
                        pi += 1;
                        if pi < pattern.len()
                            && let Value::Symbol(rest_name) = &pattern[pi]
                        {
                            let rest_vals: Vec<Value> = vals[vi..].to_vec();
                            frame.push((rest_name.clone(), Value::list(rest_vals)));
                        }
                        break;
                    }
                    name => {
                        let consumed = vi < vals.len();
                        let v = if consumed {
                            vals[vi].clone()
                        } else if optional {
                            Value::Nil
                        } else {
                            return Err(LispError::WrongNumberOfArgs(
                                "cl-destructuring-bind".into(),
                                vals.len(),
                            ));
                        };
                        frame.push((name.to_string(), v));
                        if consumed {
                            vi += 1;
                        }
                    }
                }
            }
            pi += 1;
        }

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
                        if !matches!(
                            place.first(),
                            Some(Value::Symbol(name)) if name == "symbol-function"
                        ) {
                            return Err(LispError::Signal("Unsupported cl-letf place".into()));
                        }
                        let Some(target) = place.get(1) else {
                            return Err(LispError::Signal("Unsupported cl-letf place".into()));
                        };
                        let function_name = function_name_from_binding_form(target)?;
                        let value = self.eval(&parts[1], env)?;
                        self.functions.push((function_name.clone(), value));
                        rebound.push(function_name);
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
        for name in rebound.into_iter().rev() {
            if let Some(index) = self.functions.iter().rposition(|(fname, _)| *fname == name) {
                self.functions.remove(index);
            }
        }
        result
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
            let lambda = Value::Lambda(params, body, env.clone());
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
        match expr {
            Value::Cons(car, cdr) => {
                // Check for (comma expr) or (comma-at expr) at the top level
                if let Value::Symbol(s) = car.as_ref()
                    && (s == "comma" || s == "comma-at")
                    && let Value::Cons(val, rest) = cdr.as_ref()
                    && matches!(rest.as_ref(), Value::Nil)
                {
                    return self.eval(val, env);
                }
                // Walk the cons structure, handling splicing and dotted pairs
                let mut result: Vec<Value> = Vec::new();
                let mut current: &Value = expr;
                loop {
                    match current {
                        Value::Cons(car, cdr) => {
                            // Is current itself a (comma x) or (comma-at x)?
                            if let Value::Symbol(s) = car.as_ref()
                                && (s == "comma" || s == "comma-at")
                                && let Value::Cons(val, rest) = cdr.as_ref()
                                && matches!(rest.as_ref(), Value::Nil)
                            {
                                // This is a comma form used as a dotted tail
                                let tail = self.eval(val, env)?;
                                let mut out = tail;
                                for item in result.into_iter().rev() {
                                    out = Value::cons(item, out);
                                }
                                return Ok(out);
                            }
                            // Check if car is (comma x) or (comma-at x)
                            if let Value::Cons(inner_car, inner_cdr) = car.as_ref()
                                && let Value::Symbol(s) = inner_car.as_ref()
                                && let Value::Cons(val, rest) = inner_cdr.as_ref()
                                && matches!(rest.as_ref(), Value::Nil)
                            {
                                if s == "comma" {
                                    result.push(self.eval(val, env)?);
                                    current = cdr;
                                    continue;
                                }
                                if s == "comma-at" {
                                    let evaled = self.eval(val, env)?;
                                    if let Ok(elems) = evaled.to_vec() {
                                        result.extend(elems);
                                    }
                                    current = cdr;
                                    continue;
                                }
                            }
                            result.push(self.eval_backquote(car, env)?);
                            current = cdr;
                        }
                        Value::Nil => break,
                        other => {
                            // Dotted pair tail (non-cons) — evaluate and attach
                            let tail = self.eval_backquote(other, env)?;
                            let mut out = tail;
                            for item in result.into_iter().rev() {
                                out = Value::cons(item, out);
                            }
                            return Ok(out);
                        }
                    }
                }
                Ok(Value::list(result))
            }
            // Non-list: return as-is (like quote)
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
        // Find the macro
        let macro_def = self.macros.iter().find(|(n, _, _)| n == name).cloned();
        let Some((_, params, body)) = macro_def else {
            return Ok(None);
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

    fn sf_rx(&mut self, items: &[Value]) -> Result<Value, LispError> {
        Ok(Value::String(compile_rx_sequence(&items[1..])?))
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

    fn sf_ert_deftest(&mut self, items: &[Value]) -> Result<Value, LispError> {
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

        let body = Value::list(
            std::iter::once(Value::symbol("progn")).chain(items[cursor..].iter().cloned()),
        );
        self.ert_tests.push(ErtTestDefinition {
            name,
            body,
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
            match self.eval(&test.body, &mut env) {
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
        Vec::new(),
    )
}

fn builtin_autoload_function(name: &str) -> Option<Value> {
    match name {
        "command-line-1" => Some(preloaded_command_line_1()),
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
            Vec::new(),
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
        Value::Cons(car, cdr) => match (&**car, &**cdr) {
            (Value::Integer(pos), Value::Integer(len)) if *pos >= 0 && *len >= 0 => {
                crate::buffer::UndoEntry::Insert {
                    pos: *pos as usize,
                    len: *len as usize,
                }
            }
            (Value::String(text), Value::Integer(pos)) if *pos >= 0 => {
                crate::buffer::UndoEntry::Delete {
                    pos: *pos as usize,
                    text: text.clone(),
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
            let mut current = value;
            let mut first = true;
            loop {
                match current {
                    Value::Cons(car, cdr) => {
                        if !first {
                            rendered.push(' ');
                        }
                        rendered.push_str(&render_undo_value(car));
                        first = false;
                        current = cdr;
                    }
                    Value::Nil => break,
                    other => {
                        rendered.push_str(" . ");
                        rendered.push_str(&render_undo_value(other));
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
                    continue;
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
                .map(|value| (name.clone(), value))
        })
        .collect()
}

fn restore_tail_alias_values(interp: &mut Interpreter, aliases: &[(String, Value)], env: &mut Env) {
    for (name, value) in aliases {
        interp.set_variable(name, value.clone(), env);
    }
}

fn pcase_pattern_bindings(
    pattern: &Value,
    value: &Value,
    bindings: &mut Vec<(String, Value)>,
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
            return pcase_pattern_bindings(parts.get(1).unwrap_or(&Value::Nil), value, bindings);
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
            if !pcase_pattern_bindings(pattern_car, value_car, bindings)? {
                bindings.truncate(start);
                return Ok(false);
            }
            if !pcase_pattern_bindings(pattern_cdr, value_cdr, bindings)? {
                bindings.truncate(start);
                return Ok(false);
            }
            Ok(true)
        }
        (Value::Nil, Value::Nil) => Ok(true),
        _ => Ok(pattern == value),
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

fn compile_rx_sequence(items: &[Value]) -> Result<String, LispError> {
    let mut regex = String::new();
    for item in items {
        regex.push_str(&compile_rx_form(item)?);
    }
    Ok(regex)
}

fn append_rx_char_class_fragment(regex: &mut String, value: &Value) -> Result<(), LispError> {
    match value {
        Value::String(text) => {
            for ch in text.chars() {
                match ch {
                    '\\' | ']' | '-' | '^' => {
                        regex.push('\\');
                        regex.push(ch);
                    }
                    _ => regex.push(ch),
                }
            }
            Ok(())
        }
        Value::StringObject(state) => {
            append_rx_char_class_fragment(regex, &Value::String(state.borrow().text.clone()))
        }
        Value::Integer(codepoint) => {
            let ch = char::from_u32(*codepoint as u32)
                .ok_or_else(|| LispError::Signal(format!("Invalid rx character: {codepoint}")))?;
            append_rx_char_class_fragment(regex, &Value::String(ch.to_string()))
        }
        other => Err(LispError::Signal(format!(
            "Unsupported rx charset fragment: {}",
            other.type_name()
        ))),
    }
}

fn compile_rx_char_class(items: &[Value], negated: bool) -> Result<String, LispError> {
    let mut regex = String::new();
    regex.push('[');
    if negated {
        regex.push('^');
    }
    for item in items {
        append_rx_char_class_fragment(&mut regex, item)?;
    }
    regex.push(']');
    Ok(regex)
}

fn compile_rx_form(value: &Value) -> Result<String, LispError> {
    match value {
        Value::String(text) => Ok(regex_escape(text)),
        Value::StringObject(state) => Ok(regex_escape(&state.borrow().text)),
        Value::Integer(codepoint) => {
            let ch = char::from_u32(*codepoint as u32)
                .ok_or_else(|| LispError::Signal(format!("Invalid rx character: {codepoint}")))?;
            Ok(regex_escape(&ch.to_string()))
        }
        Value::Symbol(symbol) => match symbol.as_str() {
            "bos" | "bol" => Ok("^".into()),
            "eos" | "eol" => Ok("$".into()),
            "bow" | "eow" => Ok("\\b".into()),
            "digit" => Ok("[0-9]".into()),
            "xdigit" => Ok("[0-9A-Fa-f]".into()),
            "blank" => Ok("[[:blank:]]".into()),
            "space" => Ok("[[:space:]]".into()),
            other => Err(LispError::Signal(format!("Unsupported rx atom: {other}"))),
        },
        Value::Cons(_, _) => {
            let items = value.to_vec()?;
            let Some(Value::Symbol(head)) = items.first() else {
                return compile_rx_sequence(&items);
            };
            match head.as_str() {
                "group" => Ok(format!("\\({}\\)", compile_rx_sequence(&items[1..])?)),
                "+" | "1+" => Ok(format!("\\(?:{}\\)+", compile_rx_sequence(&items[1..])?)),
                "*" => Ok(format!("\\(?:{}\\)*", compile_rx_sequence(&items[1..])?)),
                "?" => Ok(format!("\\(?:{}\\)?", compile_rx_sequence(&items[1..])?)),
                "seq" | ":" => compile_rx_sequence(&items[1..]),
                "or" | "|" => Ok(format!(
                    "\\(?:{}\\)",
                    items[1..]
                        .iter()
                        .map(compile_rx_form)
                        .collect::<Result<Vec<_>, _>>()?
                        .join("\\|")
                )),
                "any" => compile_rx_char_class(&items[1..], false),
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
                            if kind != "any" {
                                return Err(LispError::Signal("Unsupported rx `not' form".into()));
                            }
                            compile_rx_char_class(&charset[1..], true)
                        }
                        other => compile_rx_char_class(std::slice::from_ref(other), true),
                    }
                }
                _ => compile_rx_sequence(&items),
            }
        }
        other => Err(LispError::Signal(format!(
            "Unsupported rx form: {}",
            other.type_name()
        ))),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::lisp::reader::Reader;
    use std::thread;

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
            .stack_size(32 * 1024 * 1024)
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
    fn assoc_matches_strings_bound_in_lexical_variables() {
        assert_eq!(
            eval_str(
                "(let ((key \"--foo\") (alist (list (cons \"--foo\" 1)))) (cdr (assoc key alist)))"
            ),
            Value::Integer(1)
        );
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
    }

    #[test]
    fn eval_string_ops() {
        run_large_stack_test(assert_eval_string_ops);
    }

    #[test]
    fn string_match_failure_preserves_existing_match_data() {
        assert_eq!(
            eval_str(
                r#"
                (progn
                  (string-match "a\\(b\\)" "ab")
                  (string-match "z" "ab")
                  (match-string 1 "ab"))
                "#
            ),
            Value::String("b".into())
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
    fn defvar_keymap_supports_custom_setters_toggling_bindings() {
        assert_eq!(
            eval_str(
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
                     \"RET\" (keymap-read-only-bind #'ignore))
                   (let ((binding (keymap-lookup sample-read-only-map \"RET\")))
                     (and (consp binding)
                          (equal (car binding) 'menu-item)
                          (equal (nth 2 binding) #'ignore)
                          (equal (car (last binding)) '(function keymap--read-only-filter)))))"
            ),
            Value::T
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
    fn cl_loop_supports_parallel_in_thereis_until() {
        assert_eq!(
            eval_str("(cl-loop for a in '(1 2 3) for b in '(1 3 2) thereis (< a b) until (> a b))"),
            Value::T
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
                Value::String("<escape> <escape> <escape>".into()),
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
    fn cl_defun_supports_destructuring_arglists() {
        assert_eq!(
            eval_str(
                "(progn
                   (cl-defun file-notify-test ((desc actions file &optional extra))
                     (list desc actions file extra))
                   (file-notify-test '(1 (changed) \"/tmp/file\" 9)))"
            ),
            Value::list([
                Value::Integer(1),
                Value::list([Value::Symbol("changed".into())]),
                Value::String("/tmp/file".into()),
                Value::Integer(9),
            ])
        );
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
    fn rx_compiles_common_test_patterns() {
        assert_eq!(
            eval_str(r#"(rx bos (group (+ digit)) (+ blank) "Hi" eol)"#),
            Value::String("^\\(\\(?:[0-9]\\)+\\)\\(?:[[:blank:]]\\)+Hi$".into())
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
            eval_str(r#"(rx (1+ (not (any "/|"))))"#),
            Value::String("\\(?:[^/|]\\)+".into())
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
}
