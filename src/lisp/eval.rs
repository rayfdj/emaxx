use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::path::PathBuf;
use std::process::Child;
use std::rc::{Rc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::primitives;
use super::reader::{CHAR_TABLE_LITERAL_SYMBOL, RECORD_LITERAL_SYMBOL};
use super::sqlite::SqliteHandleState;
use super::types::{
    EmacsTermination, Env, LispError, SharedEnv, Value, interned_symbol_value, shared_env,
};
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
mod faces;
mod generated_autoloads;
mod loops;
mod macros;
mod preload;
mod resource_forms;
pub(crate) mod runtime;
mod rx;
mod threads;
mod treesit;
mod variables;
use bootstrap::*;
pub(crate) use preload::*;
mod ert;

pub(crate) use rx::compile_rx_to_string;

// lread.c's complete GNU 30.2 DEFVAR_LISP/DEFVAR_BOOL contract.  C-defined
// Lisp variables are special: a `let' binding in a lexical caller must remain
// visible to separately defined functions.  Package upgrades depend on this
// for `load-path', and readers/loaders use the same contract for the rest of
// this group, so keep the source manifest together instead of adding isolated
// compatibility exceptions as tests happen to exercise them.
const GNU_LREAD_SPECIAL_VARIABLES: &[&str] = &[
    "obarray",
    "values",
    "standard-input",
    "read-circle",
    "load-path",
    "load-suffixes",
    "module-file-suffix",
    "dynamic-library-suffixes",
    "load-file-rep-suffixes",
    "load-in-progress",
    "after-load-alist",
    "load-history",
    "load-file-name",
    "load-true-file-name",
    "user-init-file",
    "current-load-list",
    "load-read-function",
    "load-source-file-function",
    "load-force-doc-strings",
    "load-convert-to-unibyte",
    "source-directory",
    "preloaded-file-list",
    "byte-boolean-vars",
    "load-dangerous-libraries",
    "force-load-messages",
    "bytecomp-version-regexp",
    "lexical-binding",
    "eval-buffer-list",
    "lread--unescaped-character-literals",
    "load-prefer-newer",
    "load-no-native",
    "read-symbol-shorthands",
    "macroexp--dynvars",
];

// emacs.c's locale controls are native DEFVAR_LISP cells.  Startup's real
// `set-locale-environment' policy reads and dynamically binds them before
// most dumped libraries run, so they must exist independently of whichever
// Lisp package first happens to exercise locale setup.
const GNU_EMACS_LOCALE_SPECIAL_VARIABLES: &[&str] =
    &["system-messages-locale", "system-time-locale"];

const GNU_TREESIT_SPECIAL_VARIABLES: &[&str] =
    &["treesit-extra-load-path", "treesit-load-name-override-list"];

// keyboard.c owns both Lisp timer queues.  Like every native DEFVAR_LISP,
// they are special: test harnesses deliberately bind private queue copies,
// and timer.el must mutate and the event loop must drain that same dynamic
// binding even though its helper functions were defined elsewhere.
const GNU_KEYBOARD_TIMER_SPECIAL_VARIABLES: &[&str] = &["timer-list", "timer-idle-list"];

// image.c's native image variables exist before image.el or shr.el load and
// retain dynamic binding semantics under lexical binding.  Keep the complete
// feature-independent group together; `imagemagick-render-type' is omitted
// because GNU only defines it when built with ImageMagick support.
const GNU_IMAGE_SPECIAL_VARIABLES: &[&str] = &[
    "image-types",
    "max-image-size",
    "cross-disabled-images",
    "x-bitmap-file-path",
    "image-cache-eviction-delay",
    "image-scaling-factor",
];

// buffer.c's complete GNU 30.2 DEFVAR_PER_BUFFER contract.  These variables
// are both special under lexical binding and automatically local to the
// current buffer when assigned.  Keeping the manifest together prevents a
// newly exercised preloaded variable from accidentally behaving like an
// ordinary Emaxx global (the fill-column/string-fill regression did exactly
// that after lexical closure boundaries became correct).
const GNU_NATIVE_PER_BUFFER_VARIABLES: &[&str] = &[
    "abbrev-mode",
    "auto-fill-function",
    "bidi-display-reordering",
    "bidi-paragraph-direction",
    "bidi-paragraph-separate-re",
    "bidi-paragraph-start-re",
    "buffer-auto-save-file-format",
    "buffer-auto-save-file-name",
    "buffer-backed-up",
    "buffer-display-count",
    "buffer-display-table",
    "buffer-display-time",
    "buffer-file-coding-system",
    "buffer-file-format",
    "buffer-file-name",
    "buffer-file-truename",
    "buffer-invisibility-spec",
    "buffer-read-only",
    "buffer-saved-size",
    "buffer-undo-list",
    "cache-long-scans",
    "ctl-arrow",
    "cursor-in-non-selected-windows",
    "cursor-type",
    "default-directory",
    "enable-multibyte-characters",
    "fill-column",
    "fringe-cursor-alist",
    "fringe-indicator-alist",
    "fringes-outside-margins",
    "header-line-format",
    "horizontal-scroll-bar",
    "indicate-buffer-boundaries",
    "indicate-empty-lines",
    "left-fringe-width",
    "left-margin",
    "left-margin-width",
    "line-spacing",
    "local-abbrev-table",
    "local-minor-modes",
    "major-mode",
    "mark-active",
    "mode-line-format",
    "mode-name",
    "overwrite-mode",
    "point-before-scroll",
    "right-fringe-width",
    "right-margin-width",
    "scroll-bar-height",
    "scroll-bar-width",
    "scroll-down-aggressively",
    "scroll-up-aggressively",
    "selective-display",
    "selective-display-ellipses",
    "tab-line-format",
    "tab-width",
    "text-conversion-style",
    "truncate-lines",
    "vertical-scroll-bar",
    "word-wrap",
];

// buffer.c's buffer_permanent_local_flags table.  Unlike Lisp variables
// carrying only a `permanent-local' property, these slots are made permanent
// by the host and must retain that metadata before dumped Lisp is loaded.
const GNU_NATIVE_PERMANENT_LOCAL_VARIABLES: &[&str] =
    &["truncate-lines", "buffer-file-coding-system"];

// The DEFVAR_PER_BUFFER slots absent from buffer.c's buffer_local_flags table
// have index -1: unlike the default-inheriting entries above, every buffer
// owns their value unconditionally.
const GNU_ALWAYS_LOCAL_PER_BUFFER_VARIABLES: &[&str] = &[
    "buffer-auto-save-file-format",
    "buffer-auto-save-file-name",
    "buffer-backed-up",
    "buffer-display-count",
    "buffer-display-time",
    "buffer-file-format",
    "buffer-file-name",
    "buffer-file-truename",
    "buffer-invisibility-spec",
    "buffer-read-only",
    "buffer-saved-size",
    "buffer-undo-list",
    "default-directory",
    "enable-multibyte-characters",
    "local-minor-modes",
    "major-mode",
    "mark-active",
    "mode-name",
    "point-before-scroll",
];

// Edebug specs that GNU Emacs registers through `declare (debug ...)` forms
// in preloaded Lisp files for macros emaxx implements natively.
fn builtin_edebug_form_specs() -> Vec<(String, Vec<(String, Value)>)> {
    [
        (
            "lambda",
            "(&define lambda-list lambda-doc [&optional (\"interactive\" interactive)] def-body)",
        ),
        ("when", "t"),
        ("unless", "t"),
        ("dolist", "((symbolp form &optional form) body)"),
        ("dotimes", "((symbolp form &optional form) body)"),
        ("push", "(form gv-place)"),
        ("pop", "(gv-place)"),
        ("setq-default", "setq"),
        ("setq-local", "setq"),
        ("buffer-local-set-state", "setq"),
        ("defvar-local", "defvar"),
        ("ignore-errors", "t"),
        ("ignore-error", "t"),
        ("letrec", "let"),
        ("dlet", "let"),
        (
            "if-let*",
            "((&rest [&or symbolp (symbolp form) (form)]) body)",
        ),
        ("when-let*", "if-let*"),
        ("and-let*", "if-let*"),
        (
            "if-let",
            "([&or (symbolp form) (&rest [&or symbolp (symbolp form) (form)])] body)",
        ),
        ("when-let", "if-let"),
        ("while-let", "if-let"),
        ("with-current-buffer", "t"),
        ("with-temp-buffer", "t"),
        ("with-temp-message", "t"),
        ("with-local-quit", "t"),
        ("while-no-input", "t"),
        ("save-window-excursion", "t"),
        ("with-selected-window", "t"),
        ("with-selected-frame", "t"),
        ("atomic-change-group", "t"),
        ("with-syntax-table", "t"),
        ("with-demoted-errors", "t"),
        ("condition-case-unless-debug", "condition-case"),
        ("delay-mode-hooks", "t"),
        ("track-mouse", "(def-body)"),
        ("noreturn", "t"),
        (
            "cl-defmethod",
            "(&define [&name [sexp [&rest cl-generic--method-qualifier-p] listp] cl--generic-edebug-make-name nil] lambda-doc def-body)",
        ),
        (
            "cl-defgeneric",
            "(&define &interpose [&name sexp] cl--generic-edebug-remember-name listp lambda-doc [&rest [&or (\"declare\" &rest sexp) (\":argument-precedence-order\" &rest sexp) (&define \":method\" [&name [[&rest cl-generic--method-qualifier-p] listp] cl--generic-edebug-make-name in:method] lambda-doc def-body)]] def-body)",
        ),
        (
            "cl-macrolet",
            "(&interpose (&rest (&define [&name symbolp \"@cl-macrolet@\"] [&name [] gensym] cl-macro-list cl-declarations-or-string def-body)) cl--edebug-macrolet-interposer cl-declarations body)",
        ),
        ("1value", "t"),
    ]
    .into_iter()
    .filter_map(|(symbol, spec_text)| {
        let spec = crate::lisp::reader::Reader::new(spec_text)
            .read_all()
            .ok()?
            .into_iter()
            .next()?;
        Some((
            symbol.to_string(),
            vec![("edebug-form-spec".to_string(), spec)],
        ))
    })
    .collect()
}

// Declaration specs GNU registers in byte-run.el and gv.el so Edebug can
// instrument handler lambdas inside `(declare ...)' forms.
fn builtin_edebug_declaration_specs() -> Vec<(String, Vec<(String, Value)>)> {
    let spec_text = "(&or symbolp (\"lambda\" &define lambda-list lambda-doc def-body))";
    ["compiler-macro", "gv-expander", "gv-setter"]
        .into_iter()
        .filter_map(|symbol| {
            let spec = crate::lisp::reader::Reader::new(spec_text)
                .read_all()
                .ok()?
                .into_iter()
                .next()?;
            Some((
                symbol.to_string(),
                vec![("edebug-declaration-spec".to_string(), spec)],
            ))
        })
        .collect()
}

// Element specs GNU registers with `def-edebug-elem-spec' in cl-macs.el
// (and gv.el's `gv-place').  cl-lib is native in emaxx so cl-macs.el never
// loads; without these, instrumenting any spec that references
// `cl-lambda-list' (cl-defun, cl-defmethod, nadvice's macros...) fails with
// "cl-lambda-list is not a form-spec or function".
fn builtin_edebug_elem_specs() -> Vec<(String, Vec<(String, Value)>)> {
    [
        ("cl-declarations", "(&rest (\"cl-declare\" &rest sexp))"),
        (
            "cl-declarations-or-string",
            "(lambda-doc &or (\"declare\" def-declarations) cl-declarations)",
        ),
        (
            "cl-lambda-list",
            "(([&rest cl-lambda-arg]
               [&optional [\"&optional\" cl-&optional-arg &rest cl-&optional-arg]]
               [&optional [\"&rest\" cl-lambda-arg]]
               [&optional [\"&key\" [cl-&key-arg &rest cl-&key-arg]
                           &optional \"&allow-other-keys\"]]
               [&optional [\"&aux\" &rest
                           &or (cl-lambda-arg &optional def-form) arg]]
               . [&or arg nil]))",
        ),
        (
            "cl-&optional-arg",
            "(&or (cl-lambda-arg &optional def-form arg) arg)",
        ),
        (
            "cl-&key-arg",
            "(&or ([&or (symbolp cl-lambda-arg) arg] &optional def-form arg) arg)",
        ),
        ("cl-lambda-arg", "(&or arg cl-lambda-list1)"),
        (
            "cl-lambda-list1",
            "(([&optional [\"&whole\" arg]]
               [&rest cl-lambda-arg]
               [&optional [\"&optional\" cl-&optional-arg &rest cl-&optional-arg]]
               [&optional [\"&rest\" cl-lambda-arg]]
               [&optional [\"&key\" cl-&key-arg &rest cl-&key-arg
                           &optional \"&allow-other-keys\"]]
               [&optional [\"&aux\" &rest
                           &or (cl-lambda-arg &optional def-form) arg]]
               . [&or arg nil]))",
        ),
        ("cl-type-spec", "(sexp)"),
        (
            "cl-macro-list",
            "(([&optional \"&whole\" arg]
               [&optional \"&environment\" arg]
               [&rest cl-macro-arg]
               [&optional [\"&optional\" &rest
                           &or (cl-macro-arg &optional def-form cl-macro-arg) arg]]
               [&optional [[&or \"&rest\" \"&body\"] cl-macro-arg]]
               [&optional [\"&key\" [&rest
                                     [&or ([&or (symbolp cl-macro-arg) arg]
                                           &optional def-form cl-macro-arg)
                                          arg]]
                           &optional \"&allow-other-keys\"]]
               [&optional [\"&aux\" &rest
                           &or (cl-macro-arg &optional def-form) arg]]
               [&optional \"&environment\" arg]
               . [&or arg nil]))",
        ),
        ("cl-macro-arg", "(&or arg cl-macro-list)"),
        ("gv-place", "(form)"),
    ]
    .into_iter()
    .filter_map(|(symbol, spec_text)| {
        let spec = crate::lisp::reader::Reader::new(spec_text)
            .read_all()
            .ok()?
            .into_iter()
            .next()?;
        Some((
            symbol.to_string(),
            vec![("edebug-elem-spec".to_string(), spec)],
        ))
    })
    .collect()
}

fn builtin_error_symbol_properties() -> Vec<(String, Vec<(String, Value)>)> {
    let definitions: &[(&str, &[&str], &str)] = &[
        ("error", &["error"], "error"),
        ("quit", &["quit"], "Quit"),
        ("minibuffer-quit", &["minibuffer-quit", "quit"], "Quit"),
        ("user-error", &["user-error", "error"], ""),
        (
            "wrong-length-argument",
            &["wrong-length-argument", "error"],
            "Wrong length argument",
        ),
        (
            "wrong-type-argument",
            &["wrong-type-argument", "error"],
            "Wrong type argument",
        ),
        (
            "type-mismatch",
            &["type-mismatch", "error"],
            "Types do not match",
        ),
        (
            "args-out-of-range",
            &["args-out-of-range", "error"],
            "Args out of range",
        ),
        (
            "void-function",
            &["void-function", "error"],
            "Symbol's function definition is void",
        ),
        (
            "cyclic-function-indirection",
            &["cyclic-function-indirection", "error"],
            "Symbol's chain of function indirections contains a loop",
        ),
        (
            "cyclic-variable-indirection",
            &["cyclic-variable-indirection", "error"],
            "Symbol's chain of variable indirections contains a loop",
        ),
        (
            "circular-list",
            &["circular-list", "error"],
            "List contains a loop",
        ),
        (
            "void-variable",
            &["void-variable", "error"],
            "Symbol's value as variable is void",
        ),
        (
            "setting-constant",
            &["setting-constant", "error"],
            "Attempt to set a constant symbol",
        ),
        (
            "trapping-constant",
            &["trapping-constant", "error"],
            "Attempt to trap writes to a constant symbol",
        ),
        (
            "invalid-read-syntax",
            &["invalid-read-syntax", "error"],
            "Invalid read syntax",
        ),
        (
            "invalid-function",
            &["invalid-function", "error"],
            "Invalid function",
        ),
        (
            "wrong-number-of-arguments",
            &["wrong-number-of-arguments", "error"],
            "Wrong number of arguments",
        ),
        ("no-catch", &["no-catch", "error"], "No catch for tag"),
        (
            "end-of-file",
            &["end-of-file", "error"],
            "End of file during parsing",
        ),
        ("arith-error", &["arith-error", "error"], "Arithmetic error"),
        (
            "beginning-of-buffer",
            &["beginning-of-buffer", "error"],
            "Beginning of buffer",
        ),
        (
            "end-of-buffer",
            &["end-of-buffer", "error"],
            "End of buffer",
        ),
        (
            "buffer-read-only",
            &["buffer-read-only", "error"],
            "Buffer is read-only",
        ),
        (
            "text-read-only",
            &["text-read-only", "buffer-read-only", "error"],
            "Text is read-only",
        ),
        (
            "inhibited-interaction",
            &["inhibited-interaction", "error"],
            "User interaction while inhibited",
        ),
        (
            "domain-error",
            &["domain-error", "arith-error", "error"],
            "Arithmetic domain error",
        ),
        (
            "range-error",
            &["range-error", "arith-error", "error"],
            "Arithmetic range error",
        ),
        (
            "singularity-error",
            &["singularity-error", "domain-error", "arith-error", "error"],
            "Arithmetic singularity error",
        ),
        (
            "overflow-error",
            &["overflow-error", "range-error", "arith-error", "error"],
            "Arithmetic overflow error",
        ),
        (
            "underflow-error",
            &["underflow-error", "range-error", "arith-error", "error"],
            "Arithmetic underflow error",
        ),
        (
            "recursion-error",
            &["recursion-error", "error"],
            "Excessive recursive calling error",
        ),
        (
            "excessive-lisp-nesting",
            &["excessive-lisp-nesting", "recursion-error", "error"],
            "Lisp nesting exceeds `max-lisp-eval-depth'",
        ),
        (
            "excessive-variable-binding",
            &["excessive-variable-binding", "recursion-error", "error"],
            "Variable binding depth exceeds max-specpdl-size",
        ),
        ("file-error", &["file-error", "error"], "File error"),
        (
            "file-already-exists",
            &["file-already-exists", "file-error", "error"],
            "File already exists",
        ),
        (
            "file-date-error",
            &["file-date-error", "file-error", "error"],
            "Cannot set file date",
        ),
        (
            "file-missing",
            &["file-missing", "file-error", "error"],
            "File is missing",
        ),
        (
            "permission-denied",
            &["permission-denied", "file-error", "error"],
            "Cannot access file or directory",
        ),
        (
            "file-notify-error",
            &["file-notify-error", "file-error", "error"],
            "File notification error",
        ),
        (
            "remote-file-error",
            &["remote-file-error", "file-error", "error"],
            "Remote file error",
        ),
        (
            "search-failed",
            &["search-failed", "error"],
            "Search failed",
        ),
        (
            "user-search-failed",
            &["user-search-failed", "user-error", "search-failed", "error"],
            "Search failed",
        ),
        (
            "invalid-regexp",
            &["invalid-regexp", "error"],
            "Invalid regexp",
        ),
        ("scan-error", &["scan-error", "error"], "Scan error"),
        (
            "module-load-failed",
            &["module-load-failed", "error"],
            "Module load failed",
        ),
        (
            "module-open-failed",
            &["module-open-failed", "module-load-failed", "error"],
            "Module could not be opened",
        ),
        (
            "module-not-gpl-compatible",
            &["module-not-gpl-compatible", "module-load-failed", "error"],
            "Module is not GPL compatible",
        ),
        (
            "missing-module-init-function",
            &[
                "missing-module-init-function",
                "module-load-failed",
                "error",
            ],
            "Module does not export an initialization function",
        ),
        (
            "module-init-failed",
            &["module-init-failed", "module-load-failed", "error"],
            "Module initialization failed",
        ),
        // cl-generic is dumped in GNU and native/preprovided in Emaxx.
        ("cl-no-method", &["cl-no-method", "error"], "No method"),
        (
            "cl-no-next-method",
            &["cl-no-next-method", "cl-no-method", "error"],
            "No next method",
        ),
        (
            "cl-no-primary-method",
            &["cl-no-primary-method", "cl-no-method", "error"],
            "No primary method",
        ),
        (
            "cl-no-applicable-method",
            &["cl-no-applicable-method", "cl-no-method", "error"],
            "No applicable method",
        ),
    ];

    definitions
        .iter()
        .map(|(name, conditions, message)| {
            (
                (*name).to_string(),
                vec![
                    (
                        "error-conditions".to_string(),
                        Value::list(
                            conditions
                                .iter()
                                .map(|condition| Value::Symbol((*condition).to_string())),
                        ),
                    ),
                    (
                        "error-message".to_string(),
                        Value::String((*message).to_string()),
                    ),
                ],
            )
        })
        .collect()
}

fn builtin_symbol_properties() -> Vec<(String, Value)> {
    let mut properties: Vec<(String, Vec<(String, Value)>)> = [
        ("autoload", 3),
        ("defadvice", 3),
        ("defalias", 3),
        ("defconst", 3),
        ("defconstant", 3),
        ("defgeneric", 3),
        ("defmacro", 3),
        ("defmethod", 3),
        ("defparameter", 3),
        ("defsubst", 3),
        ("defun", 3),
        ("defvar", 3),
        ("defvaralias", 3),
        ("define-category", 2),
        ("define-compiler-macro", 3),
        ("define-setf-expander", 3),
    ]
    .into_iter()
    .map(|(symbol, doc_index)| {
        (
            symbol.to_string(),
            vec![("doc-string-elt".to_string(), Value::Integer(doc_index))],
        )
    })
    .collect();
    properties.extend(builtin_edebug_form_specs());
    properties.extend(builtin_edebug_declaration_specs());
    properties.extend(builtin_edebug_elem_specs());
    properties.extend(builtin_error_symbol_properties());
    properties.extend(
        [
            ("gnutls-e-interrupted", -52),
            ("gnutls-e-again", -28),
            ("gnutls-e-invalid-session", -10),
            ("gnutls-e-not-ready-for-handshake", -65_500),
        ]
        .into_iter()
        .map(|(symbol, code)| {
            (
                symbol.to_string(),
                vec![("gnutls-code".to_string(), Value::Integer(code))],
            )
        }),
    );
    // GNU: (function-put 'lambda 'doc-string-elt 2); pp's code formatter
    // keeps only pre-docstring elements on the first line.  Merged into
    // lambda's existing entry (per-symbol entries replace wholesale).
    if let Some(entry) = properties
        .iter_mut()
        .rev()
        .find(|(name, _)| name == "lambda")
    {
        entry
            .1
            .push(("doc-string-elt".to_string(), Value::Integer(2)));
    } else {
        properties.push((
            "lambda".to_string(),
            vec![("doc-string-elt".to_string(), Value::Integer(2))],
        ));
    }
    properties
        .into_iter()
        .map(|(symbol, properties)| {
            let plist = properties
                .into_iter()
                .flat_map(|(property, value)| [Value::Symbol(property), value]);
            (symbol, Value::list(plist))
        })
        .collect()
}

// One live keyboard-macro execution: recursive edits started while the macro
// runs continue consuming events from the same shared cursor.
#[derive(Clone, Debug)]
pub(crate) struct KbdMacroExecutionState {
    pub(crate) events: Vec<Value>,
    pub(crate) index: usize,
}

// keyboard.c keeps these as one kboard-owned input state.  Keeping the same
// ownership boundary here prevents command-key, raw-key, lossage, focus, and
// dribble primitives from drifting into unrelated Lisp-variable shims.
#[derive(Clone, Debug, Default)]
pub(crate) struct KeyboardInputState {
    pub(crate) command_keys: Vec<Value>,
    pub(crate) single_command_start: usize,
    pub(crate) raw_keys: Vec<Value>,
    pub(crate) recent_keys: Vec<Value>,
    pub(crate) dribble_file: Option<PathBuf>,
    pub(crate) internal_last_event_frame: Option<Value>,
}

// One active `ert-with-message-capture` scope.  When the capture variable is
// special it stays dynamically bound for the body, so helpers called from the
// body observe every message as soon as it is issued, like the upstream
// `message' advice does.
#[derive(Clone, Debug)]
pub struct MessageCapture {
    pub text: String,
    pub live_var: Option<String>,
}

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

pub(crate) struct TreeSitterQueryState {
    pub(crate) record_id: u64,
    pub(crate) language: Value,
    pub(crate) source: Value,
    pub(crate) query: Option<std::rc::Rc<tree_sitter::Query>>,
}

pub(crate) struct TreeSitterLanguageState {
    pub(crate) symbol: String,
    pub(crate) language: tree_sitter::Language,
    // Drop the Language before unloading the module which owns its static data.
    pub(crate) _library: Option<libloading::Library>,
}

pub(crate) struct TreeSitterParserState {
    pub(crate) record_id: u64,
    pub(crate) parser: tree_sitter::Parser,
    pub(crate) tree: Option<tree_sitter::Tree>,
    pub(crate) language: Value,
    pub(crate) buffer_id: u64,
    pub(crate) buffer: Value,
    pub(crate) list_buffer_id: u64,
    pub(crate) tag: Value,
    pub(crate) deleted: bool,
    pub(crate) included_ranges: Value,
    pub(crate) notifiers: Vec<Value>,
    pub(crate) parsed_tick: Option<crate::buffer::ModCount>,
    pub(crate) visible_region: Option<(usize, usize)>,
    pub(crate) generation: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct TreeSitterNodeState {
    pub(crate) record_id: u64,
    pub(crate) parser_id: u64,
    pub(crate) node_id: usize,
    pub(crate) generation: u64,
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

/// An in-flight native url-retrieve: a worker thread fetches the raw
/// HTTP response; accept-process-output style waits deliver it to the
/// response buffer and run the callback.
pub(crate) struct PendingUrlRetrieval {
    pub(crate) buffer_id: u64,
    pub(crate) url: String,
    pub(crate) callback: Value,
    pub(crate) cbargs: Vec<Value>,
    pub(crate) receiver: std::sync::mpsc::Receiver<Result<Vec<u8>, String>>,
}

#[derive(Clone, Debug)]
pub(crate) struct SpecialBindingRestore {
    binding_id: u64,
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
    /// Snapshot of the evaluator environment at this activation while a
    /// debugger is active.  Frames retain their identity stamps so
    /// `backtrace-eval' assignments can update the suspended lexical cells.
    lexical_context: Option<Env>,
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
    /// Whether this thread's event wait is currently servicing process
    /// callbacks on behalf of user input.
    waiting_for_user_input: bool,
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

#[derive(Clone, Debug)]
pub(crate) struct CombinedAfterChangeState {
    pub(crate) buffer_id: u64,
    /// (unchanged chars before, unchanged chars after, inserted - deleted)
    pub(crate) changes: Vec<(i64, i64, i64)>,
}

/// Which standard hash-table test a runtime-accelerated table uses; custom
/// user tests stay on the entry-list slow path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) enum RuntimeHashTest {
    Eq,
    /// GNU's default `make-hash-table' test.
    #[default]
    Eql,
    Equal,
}

#[derive(Clone, Debug, Default)]
struct EqualHashTableState {
    test: RuntimeHashTest,
    entries: Vec<(Value, Value)>,
    key_index: HashMap<Option<i64>, Vec<usize>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ProcessStatus {
    Run,
    Stop,
    Exit,
    /// Subprocess terminated by an operating-system signal.
    Signal,
    /// Nonblocking network connection not yet reported as established.
    Connect,
    /// Network connection established (client or accepted server child).
    Open,
    /// Network connection closed.
    Closed,
    /// Network server accepting connections.
    Listen,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProcessKind {
    Real,
    Pipe,
    Network,
    Serial,
}

impl ProcessStatus {
    fn symbol(&self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Stop => "stop",
            Self::Exit => "exit",
            Self::Signal => "signal",
            Self::Connect => "connect",
            Self::Open => "open",
            Self::Closed => "closed",
            Self::Listen => "listen",
        }
    }

    fn is_live(&self) -> bool {
        matches!(
            self,
            Self::Run | Self::Stop | Self::Connect | Self::Open | Self::Listen
        )
    }
}

/// A network process's OS-level object (GNU process.c network processes).
pub(crate) enum NetworkRuntime {
    Listener(std::net::TcpListener),
    Stream(std::net::TcpStream),
    Datagram {
        socket: std::net::UdpSocket,
        remote: Option<std::net::SocketAddr>,
    },
    /// `:family local' — unix domain sockets (erc-d's direct tests).
    UnixListener(std::os::unix::net::UnixListener),
    UnixStream(std::os::unix::net::UnixStream),
}

impl std::fmt::Debug for NetworkRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkRuntime").finish_non_exhaustive()
    }
}

/// A serial descriptor is a process connection, but neither a subprocess nor
/// a network socket.  Keep it separate so type checks cannot accidentally
/// grant network-only operations to serial processes.
pub(crate) struct SerialRuntime {
    pub(crate) port: serialport::TTYPort,
}

impl std::fmt::Debug for SerialRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SerialRuntime").finish_non_exhaustive()
    }
}

pub(crate) struct RunningProcess {
    pub(crate) child: Child,
    /// Writable master for a pseudo-terminal connected to child stdin.
    pub(crate) pty_input: Option<fs::File>,
    /// Readable master for a pseudo-terminal connected to child output.
    pub(crate) pty_output: Option<fs::File>,
    /// Parent-held slave descriptor.  Darwin can discard unread master-side
    /// bytes when the final slave closes, so keep one slave alive until the
    /// child has exited and its output has been drained.
    pub(crate) pty_slave_guard: Option<fs::File>,
}

impl std::fmt::Debug for RunningProcess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunningProcess").finish_non_exhaustive()
    }
}

const GNUTLS_STAGE_INIT: i64 = 4;

pub(crate) type GnuTlsDeinit = unsafe extern "C" fn(*mut std::ffi::c_void);
pub(crate) type GnuTlsRecordRecv =
    unsafe extern "C" fn(*mut std::ffi::c_void, *mut std::ffi::c_void, usize) -> isize;
pub(crate) type GnuTlsRecordSend =
    unsafe extern "C" fn(*mut std::ffi::c_void, *const std::ffi::c_void, usize) -> isize;
pub(crate) type GnuTlsHandshake = unsafe extern "C" fn(*mut std::ffi::c_void) -> std::ffi::c_int;
pub(crate) type GnuTlsBye =
    unsafe extern "C" fn(*mut std::ffi::c_void, std::ffi::c_int) -> std::ffi::c_int;
pub(crate) type GnuTlsErrorString =
    unsafe extern "C" fn(std::ffi::c_int) -> *const std::ffi::c_char;
pub(crate) type GnuTlsErrorIsFatal = unsafe extern "C" fn(std::ffi::c_int) -> std::ffi::c_int;

pub(crate) struct GnuTlsSessionApi {
    pub(crate) session_deinit: GnuTlsDeinit,
    pub(crate) credential_deinit: GnuTlsDeinit,
    pub(crate) record_recv: GnuTlsRecordRecv,
    pub(crate) record_send: GnuTlsRecordSend,
    pub(crate) handshake: GnuTlsHandshake,
    pub(crate) bye: GnuTlsBye,
    pub(crate) error_string: GnuTlsErrorString,
    pub(crate) error_is_fatal: GnuTlsErrorIsFatal,
}

pub(crate) struct ProcessGnuTlsSession {
    // Keep the library alive until every session and credential destructor has
    // run; all stored function pointers are owned by this library.
    _library: libloading::Library,
    state: *mut std::ffi::c_void,
    credential: *mut std::ffi::c_void,
    api: GnuTlsSessionApi,
    ready: bool,
}

impl ProcessGnuTlsSession {
    pub(crate) fn new(
        library: libloading::Library,
        state: *mut std::ffi::c_void,
        credential: *mut std::ffi::c_void,
        api: GnuTlsSessionApi,
        ready: bool,
    ) -> Self {
        Self {
            _library: library,
            state,
            credential,
            api,
            ready,
        }
    }

    fn error(&self, operation: &str, code: std::ffi::c_int) -> LispError {
        // SAFETY: GnuTLS accepts every error code and returns a static C string.
        let description = unsafe {
            let pointer = (self.api.error_string)(code);
            (!pointer.is_null()).then(|| {
                std::ffi::CStr::from_ptr(pointer)
                    .to_string_lossy()
                    .into_owned()
            })
        }
        .unwrap_or_else(|| code.to_string());
        LispError::Signal(format!("GnuTLS {operation} failed: {description}"))
    }

    pub(crate) fn handshake(&mut self, complete: bool) -> Result<std::ffi::c_int, LispError> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            // SAFETY: STATE is live until this owner is dropped.
            let result = unsafe { (self.api.handshake)(self.state) };
            match result {
                0 => {
                    self.ready = true;
                    return Ok(0);
                }
                -28 if !complete => return Ok(result),
                -28 | -52 if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                -28 | -52 => return Err(LispError::Signal("GnuTLS handshake timed out".into())),
                error => return Ok(error),
            }
        }
    }

    pub(crate) fn receive(&mut self) -> Result<(Vec<u8>, bool), LispError> {
        if !self.ready {
            let result = self.handshake(false)?;
            if matches!(result, -28 | -52) {
                return Ok((Vec::new(), false));
            }
            if result < 0 {
                return Err(self.error("handshake", result));
            }
        }
        let mut output = Vec::new();
        let mut chunk = [0_u8; 16_384];
        loop {
            // SAFETY: STATE is live and CHUNK is writable for its full length.
            let result = unsafe {
                (self.api.record_recv)(self.state, chunk.as_mut_ptr().cast(), chunk.len())
            };
            match result {
                0 => return Ok((output, true)),
                length if length > 0 => output.extend_from_slice(&chunk[..length as usize]),
                -52 => continue,
                -28 => return Ok((output, false)),
                -9 => return Ok((output, true)),
                error => {
                    // GNU turns fatal record-layer failures into EOF and
                    // leaves other repairable conditions for the next poll.
                    // SAFETY: Every C integer is accepted by this classifier.
                    let closed =
                        unsafe { (self.api.error_is_fatal)(error as std::ffi::c_int) } != 0;
                    return Ok((output, closed));
                }
            }
        }
    }

    pub(crate) fn send_all(&mut self, input: &[u8]) -> Result<(), LispError> {
        if !self.ready {
            let result = self.handshake(true)?;
            if result < 0 {
                return Err(self.error("handshake", result));
            }
        }
        let mut written = 0;
        while written < input.len() {
            // SAFETY: STATE is live and the remaining input is borrowed for
            // this call only.
            let result = unsafe {
                (self.api.record_send)(
                    self.state,
                    input[written..].as_ptr().cast(),
                    input.len() - written,
                )
            };
            match result {
                length if length > 0 => written += length as usize,
                -28 | -52 => std::thread::sleep(std::time::Duration::from_millis(1)),
                error => return Err(self.error("send", error as std::ffi::c_int)),
            }
        }
        Ok(())
    }

    pub(crate) fn bye(&mut self, continue_transport: bool) -> std::ffi::c_int {
        // SAFETY: STATE is live.  GnuTLS defines 0 as SHUT_RDWR and 1 as
        // SHUT_WR, matching GNU's CONT mapping.
        unsafe { (self.api.bye)(self.state, if continue_transport { 1 } else { 0 }) }
    }

    pub(crate) fn raw_state(&self) -> *mut std::ffi::c_void {
        self.state
    }
}

impl Drop for ProcessGnuTlsSession {
    fn drop(&mut self) {
        // SAFETY: The constructor receives one live session and one live
        // credential, each uniquely owned and released exactly once here.
        unsafe {
            (self.api.session_deinit)(self.state);
            (self.api.credential_deinit)(self.credential);
        }
    }
}

struct ProcessGnuTlsState {
    boot_parameters: Value,
    initstage: i64,
    active: bool,
    session: Option<ProcessGnuTlsSession>,
    peer_status: Value,
}

impl Default for ProcessGnuTlsState {
    fn default() -> Self {
        Self {
            boot_parameters: Value::Nil,
            initstage: 0,
            active: false,
            session: None,
            peer_status: Value::Nil,
        }
    }
}

struct ProcessState {
    record_id: u64,
    kind: ProcessKind,
    buffer_id: Option<u64>,
    mark_marker_id: u64,
    status: ProcessStatus,
    filter: Option<Value>,
    sentinel: Option<Value>,
    /// Whether the terminal subprocess status has already invoked SENTINEL.
    sentinel_notified: bool,
    /// Network process :log function (server connection events).
    log: Option<Value>,
    /// The process name (process-name / get-process).
    name: String,
    /// Thread whose event loop owns this process's descriptors.  None means
    /// any thread may service it.
    thread_id: Option<u64>,
    query_on_exit_flag: bool,
    /// GNU reuses p->command = t to stop traffic on connection records.
    /// Keep that flow-control bit separate from the real subprocess argv.
    traffic_stopped: bool,
    /// GNU p->inherit_coding_system_flag.  This is independent from the
    /// process coding pair and can be toggled after process creation.
    inherit_coding_system_flag: bool,
    decoding: Value,
    encoding: Value,
    program: Option<String>,
    argv: Vec<String>,
    /// Optional pipe process receiving this child's standard error.
    stderr_process_id: Option<u64>,
    /// Child exit code, or fatal signal number for signaled termination.
    exit_code: Option<i32>,
    /// Fatal signal number when the OS reports signaled termination.
    exit_signal: Option<i32>,
    /// OS pid retained after the Child handle is reaped; nil for pipe and
    /// network process records.
    os_pid: Option<u32>,
    runtime: Option<RunningProcess>,
    network: Option<NetworkRuntime>,
    serial: Option<SerialRuntime>,
    /// Network :host/:service as given at creation (process-contact).
    contact_host: Option<String>,
    contact_service: Option<i64>,
    /// A server child's peer address (process-contact :remote-ish info
    /// and sentinel event strings).
    remote: Option<String>,
    /// The server process a child connection came from.
    parent_server_id: Option<u64>,
    /// Output drained from the pipes when the child's exit was noticed
    /// before the pump ran; delivered by the next poll so no tail output
    /// is lost (epg reads gpg's final status lines this way).
    pending_stdout: Vec<u8>,
    pending_stderr: Vec<u8>,
    /// Monotonic count of nonempty output deliveries.  A targeted
    /// `accept-process-output' may service other processes while it waits,
    /// but only a change to the requested process satisfies that wait.
    output_delivery_count: u64,
    /// The process property list (process-put/process-get).
    plist: Value,
    /// Private GnuTLS setup/session state, separate from the public process
    /// property list just as it is in GNU's `Lisp_Process`.
    gnutls: ProcessGnuTlsState,
    /// GNU p->childp: t for a real child process; for a network process
    /// the full keyword contact plist as make-network-process received
    /// it, with :service resolved and :local/:remote address vectors
    /// appended (process-contact with KEY t).
    contact: Value,
}

#[derive(Clone, Debug)]
pub(crate) struct WindowConfigurationSnapshot {
    current_buffer_id: u64,
    selected_window_id: u64,
    selected_window_slots: Vec<Value>,
    window_records: Vec<(u64, Vec<Value>)>,
    root_window: Value,
    frame_width: i64,
    frame_height: i64,
}

#[derive(Clone)]
struct FileNotifyWatch {
    path: Option<String>,
    callback: Value,
    active: bool,
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
    /// Earliest wall-clock instant the timer may fire; GNU never runs a
    /// timer before its scheduled time.  `None` means due immediately.
    due: Option<std::time::Instant>,
    /// Reschedule interval in seconds for repeating timers.
    repeat: Option<f64>,
}

#[derive(Clone)]
pub(crate) struct MacroBinding {
    pub(crate) name: String,
    pub(crate) expander: Value,
}

#[derive(Clone, Debug)]
pub(crate) struct FringeBitmapState {
    pub(crate) name: String,
    pub(crate) id: i64,
    pub(crate) standard: bool,
    pub(crate) definition: Option<Value>,
    pub(crate) face: Value,
}

#[derive(Clone, Debug)]
pub(crate) struct CompositionState {
    pub(crate) components: Value,
    pub(crate) relative: bool,
    pub(crate) width: i64,
}

pub(crate) const LFACE_VECTOR_SIZE: usize = 20;
pub(crate) const LFACE_INHERIT_INDEX: usize = 16;

#[derive(Clone, Debug)]
pub(crate) struct LispFaceState {
    pub(crate) name: String,
    pub(crate) id: Option<i64>,
    pub(crate) global: Option<Value>,
    pub(crate) selected_frame: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FontPatternState {
    pub(crate) family: Option<String>,
    pub(crate) registry: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FontsetTargetState {
    Character(i64),
    Range(i64, i64),
    Script(String),
    Fallback,
}

#[derive(Clone, Debug)]
pub(crate) struct FontsetMappingState {
    pub(crate) target: FontsetTargetState,
    pub(crate) patterns: Vec<Option<FontPatternState>>,
}

#[derive(Clone, Debug)]
pub(crate) struct FontsetState {
    pub(crate) name: String,
    pub(crate) mappings: Vec<FontsetMappingState>,
}

#[derive(Clone, Debug)]
pub(crate) struct FrameState {
    pub(crate) id: u64,
    pub(crate) name: Value,
    pub(crate) live: bool,
    pub(crate) width: i64,
    pub(crate) height: i64,
    pub(crate) text_height: i64,
    pub(crate) parameter_width: i64,
    pub(crate) parameter_height: i64,
    pub(crate) parameter_overrides: Vec<(String, Value)>,
    pub(crate) focus_frame_id: Option<u64>,
    pub(crate) left: i64,
    pub(crate) top: i64,
    pub(crate) window_state_change: bool,
    pub(crate) after_make_frame: bool,
    pub(crate) pointer_invisible: bool,
    pub(crate) was_invisible: bool,
}

fn empty_lisp_face_vector() -> Value {
    Value::list(
        std::iter::once(Value::symbol("vector-literal"))
            .chain(std::iter::once(Value::symbol("face")))
            .chain(std::iter::repeat_n(
                Value::symbol("unspecified"),
                LFACE_VECTOR_SIZE - 1,
            )),
    )
}

/// The interpreter state: holds the global environment, the current buffer,
/// and ERT test results.
pub struct Interpreter {
    /// Global variable bindings (defvar, setq at top level).
    globals: Vec<(String, Value)>,
    /// Last-wins index over `globals` so global variable reads are O(1);
    /// every mutation of `globals` keeps this in sync.
    globals_index: HashMap<String, Value>,
    /// Variable aliases keyed by alias name.
    variable_aliases: Vec<(String, String)>,
    /// Alias → target index mirroring `variable_aliases` (at most one entry
    /// per alias) so name resolution on the hot lookup path is O(1).
    variable_aliases_index: HashMap<String, String>,
    /// Variables with dynamic binding semantics.
    special_variables: Vec<String>,
    /// Membership index over `special_variables` so hot binding paths can
    /// test specialness in O(1).
    special_variables_index: HashSet<String>,
    /// Names ever declared locally special via a non-top-level one-arg
    /// `defvar`; lets of other names skip the env marker scan entirely.
    local_special_names: HashSet<String>,
    /// Names declared by a TOP-LEVEL one-arg `defvar` in a lexical-binding
    /// file: GNU scopes these to the file (dynamic `let's, dynamic
    /// references) while `special-variable-p' stays nil.  emaxx applies
    /// the dynamic-binding treatment session-wide but keeps them out of
    /// the official special set.
    soft_special_names: HashSet<String>,
    /// Names currently bound by an active `dlet': treated as dynamic for
    /// binding and reference resolution while the dlet body runs (counted,
    /// since dlets nest).
    dlet_active_names: HashMap<String, u32>,
    /// Env index below which SPECIAL variable references must not resolve
    /// through lexical frames: set to the caller boundary when a function
    /// body runs on the caller's env chain, so a callee's reference to a
    /// special name reads the dynamic value like GNU instead of a caller's
    /// same-named lexical argument (bug#47552 semantics).
    pub(crate) special_scan_floor: usize,
    pub(crate) lisp_eval_depth: usize,
    pub(crate) kbd_macro_executions: Vec<KbdMacroExecutionState>,
    pub(crate) kbd_macro_definition: Vec<Value>,
    pub(crate) kbd_macro_committed_len: usize,
    pub(crate) keyboard_input: KeyboardInputState,
    pub(crate) command_loop_recursion_depth: usize,
    /// GNU's process-local time-zone rule.  Keep it interpreter-local here:
    /// Rust tests run interpreters concurrently in one host process, so
    /// mutating the host `TZ' would leak state between otherwise isolated
    /// Emacs instances.
    pub(crate) local_time_zone_rule: Value,
    /// Symbol properties keyed by symbol name.  Each value is the actual live
    /// Lisp plist, matching GNU symbols' plist cell rather than a Rust-side
    /// projection that loses `setcar'/`setcdr' mutations.
    symbol_properties: Vec<(String, Value)>,
    /// Symbols explicitly interned into the standard obarray.
    interned_symbols: Vec<String>,
    /// Membership index for `interned_symbols'.  Keeping insertion order in
    /// the vector makes completion deterministic, while this set prevents
    /// source loading from turning symbol interning into a quadratic scan.
    interned_symbol_names: HashSet<String>,
    /// Runtime record representing GNU's preloaded standard `obarray'.  Its
    /// symbol view is synthesized from the interpreter's canonical namespace
    /// indexes rather than duplicated in the record's storage slot.
    standard_obarray_id: u64,
    /// Variable watchers keyed by canonical variable name.
    variable_watchers: Vec<(String, Vec<Value>)>,
    /// The current buffer being operated on.
    pub buffer: crate::buffer::Buffer,
    /// Keymap selected by `use-global-map'.  GNU keeps this independently
    /// from the Lisp variable `global-map'.
    current_global_map: Option<Value>,
    /// The ID of the current buffer.
    current_buffer_id: u64,
    /// The currently selected window record.
    selected_window_id: u64,
    /// Whether each window should display its cursor on the next redisplay.
    /// Missing entries retain GNU's visible-by-default state.
    window_cursor_visibility: HashMap<u64, bool>,
    /// Window selected when the last window-change cycle completed.
    old_selected_window_id: u64,
    /// Per-frame old selected window.  GNU's initial batch frame leaves this
    /// unset until the first completed window-change cycle.
    frame_old_selected_window_id: Option<u64>,
    /// Monotonic selection stamp used by `window-use-time'.
    window_select_count: i64,
    /// Opaque frame identities and their frame-local state.  The headless
    /// runtime begins with one TTY frame; keeping its state keyed by identity
    /// prevents frame objects from collapsing into an ordinary Lisp symbol.
    pub(crate) frame_states: Vec<FrameState>,
    pub(crate) selected_frame_id: u64,
    pub(crate) old_selected_frame_id: u64,
    /// dispnew.c's internal frame/buffer menu state vector.
    frame_and_buffer_state: Value,
    /// Terminal-local parameters for the single runtime terminal.
    ///
    /// GNU stores this as an alist and `set-terminal-parameter' accepts any
    /// Lisp object as a key, even though the getter's public contract requires
    /// a symbol.  Keep the native representation equally general.
    terminal_parameters: Vec<(Value, Value)>,
    /// Whether the single headless/bootstrap terminal has not been deleted.
    /// GNU keeps deleted terminal objects as non-live Lisp identities while
    /// removing them from `terminal-list`.
    terminal_live: bool,
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
    /// Stable GNU `mark-marker' identities, one for each live buffer.
    buffer_mark_marker_ids: HashMap<u64, u64>,
    /// Char tables allocated by the interpreter.
    char_tables: Vec<CharTableState>,
    /// Stable lazy tables returned by `unicode-property-table-internal'.
    /// GNU caches these in `char-code-property-alist'; recreating a table on
    /// every character lookup makes Unicode-wide scans catastrophically
    /// quadratic in allocation and also loses `put-char-code-property' data.
    unicode_property_table_ids: HashMap<String, u64>,
    /// Indexed storage for GNU `equal' hash tables.  Record slots retain
    /// metadata compatibility, while this sidecar gives structured Lisp keys
    /// the same hashed lookup shape as Emacs's native implementation.
    equal_hash_tables: HashMap<u64, EqualHashTableState>,
    /// Charset aliases defined at runtime.
    charset_aliases: Vec<(String, String)>,
    /// Registered charsets and their stable GNU-compatible numeric IDs.
    charset_ids: Vec<(String, i64)>,
    /// Charset plist overrides keyed by canonical charset name.
    charset_plists: Vec<(String, Value)>,
    /// Current charset priority order.
    charset_priority: Vec<String>,
    /// ISO charset associations keyed by (dimension, chars, final).
    iso_charsets: Vec<(i64, i64, u32, String)>,
    /// Coding systems keyed by canonical name.
    coding_systems: Vec<CodingSystemState>,
    /// GNU ccl.c's private registration table.  Lisp symbols refer to these
    /// entries through their `ccl-program-idx' property.
    pub(crate) ccl_programs: Vec<Option<(String, Value)>>,
    /// Coding-system aliases keyed by alias name.
    coding_aliases: Vec<(String, String)>,
    /// Current coding-system priority order.
    coding_priority: Vec<String>,
    /// Current terminal coding system.
    terminal_coding: Option<String>,
    /// Current keyboard coding system.
    keyboard_coding: Option<String>,
    input_interrupt_mode: bool,
    /// Shared standard category table.
    standard_category_table_id: Option<u64>,
    /// Shared standard case table.
    standard_case_table_id: Option<u64>,
    /// Case tables derived from GNU's ASCII-only case table.
    ascii_case_table_ids: Vec<u64>,
    /// Buffer-local case tables keyed by buffer id.
    buffer_case_tables: Vec<(u64, u64)>,
    /// Next char-table ID for identity tracking.
    next_char_table_id: u64,
    /// Allocated record objects.
    records: Vec<RecordState>,
    /// Decoded byte-code programs indexed by record ID minus one — ids are
    /// dense and never freed, so the slot vector doubles as the cache map
    /// (see bytecode::vm).
    pub(crate) bytecode_program_cache:
        Vec<Option<std::rc::Rc<crate::lisp::bytecode::vm::CachedProgram>>>,
    /// SQLite objects keyed by record ID.
    sqlite_handles: Vec<(u64, SqliteHandleState)>,
    /// Lazily compiled Tree-sitter queries keyed by opaque record identity.
    treesit_queries: Vec<TreeSitterQueryState>,
    /// Official Tree-sitter parsers keyed by opaque Lisp record identity.
    treesit_parsers: Vec<TreeSitterParserState>,
    /// Stable node identities resolved against their parser's current tree.
    treesit_nodes: Vec<TreeSitterNodeState>,
    /// Loaded grammar modules, deliberately dropped after parsers and trees.
    treesit_languages: Vec<TreeSitterLanguageState>,
    // nadvice state: per-symbol advice entries (newest first = outermost)
    // plus the unadvised base definition; entries added before the symbol
    // is defined stay pending until a defun/defalias installs a base.
    pub(crate) advice_registry: std::collections::HashMap<String, AdviceState>,
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
    next_special_binding_id: u64,
    /// Active labeled restrictions keyed by (buffer id, label, start, end).
    labeled_restrictions: Vec<(u64, String, usize, usize)>,
    /// Indirect buffer mapping: (buffer id, base buffer id).
    indirect_buffers: Vec<(u64, u64)>,
    /// Prevent recursive before/after-change hook re-entry.
    change_hooks_running: usize,
    /// User-defined macros: name → (params, body).
    macros: Vec<MacroBinding>,
    /// Occurrence counts per macro name so the hot macroexpansion path can
    /// reject non-macro heads without scanning the positional table.
    macros_name_counts: HashMap<String, u32, crate::lisp::primitives::FnvBuildHasher>,
    /// User-defined functions in the function namespace.
    functions: Vec<(String, Value)>,
    /// Last-wins index over `functions` so the hot function-lookup path is
    /// O(1); every mutation of `functions` keeps this in sync.
    functions_index: HashMap<String, Value, crate::lisp::primitives::FnvBuildHasher>,
    /// GNU connect_counter: numbers accepted server-child connections
    /// (unix children are named "NAME <N>" from it).
    pub(crate) network_connect_counter: u64,
    /// Bumped on every function/macro (re)definition; validates the
    /// `not_macro_names` verdicts below.
    definition_generation: u64,
    /// Names the macroexpansion probe determined are NOT macros, from
    /// GLOBAL state only (no cl-flet frame involved), stamped with the
    /// generation that verdict was computed at.  Skips the whole probe on
    /// the hot per-form path while any definition change invalidates all
    /// verdicts at once.
    not_macro_names: HashMap<String, u64>,
    /// Per-callsite macro expansions, keyed by the form's car cell address
    /// and effective lexical mode, and validated against `definition_generation` (compiled
    /// GNU code expands each macro call once; interpreted emaxx would
    /// otherwise re-expand per evaluation — the dominant cost under
    /// erc's message load).  The cached entry holds the ORIGINAL form
    /// too, so its cons stays alive and the address is never reused.
    macro_expansion_cache: HashMap<(usize, bool), (u64, Value, Value)>,
    /// Immutable lambda code keyed by the source form's car-cell identity.
    /// The weak source witness prevents a recycled allocator address from
    /// aliasing an unrelated form whose older closure is still alive.
    lambda_source_bodies: HashMap<usize, LambdaSourceBodyCacheEntry>,
    /// Features currently available in this interpreter.
    provided_features: Vec<String>,
    /// Forms waiting for a feature to be provided.
    after_load_forms: Vec<(String, Vec<Value>, Env)>,
    /// File currently being loaded, if any.
    current_load_file: Option<String>,
    /// Source files whose compile-time-only forms must not acquire runtime
    /// load-history entries while Emaxx interprets their source fallback.
    load_history_suppressed_files: Vec<String>,
    // File the currently-running ERT test was defined in; used by
    // `ert-resource-directory' without making `load-file-name' non-nil
    // during test bodies (it is nil there in GNU).
    pub(crate) ert_test_source_file: Option<String>,
    pub(crate) current_ert_test_name: Option<String>,
    /// Collected ERT test definitions.
    pub ert_tests: Vec<ErtTestDefinition>,
    /// Results from the most recent ERT run.
    pub test_results: Vec<TestOutcome>,
    /// Selected test names from the most recent ERT run.
    pub last_selected_tests: Vec<String>,
    /// A `kill-emacs` request waiting for the process-owning batch boundary.
    /// Keeping it explicit prevents an internal error-demotion path from
    /// accidentally turning the native noreturn primitive into a catchable
    /// Lisp condition.
    pending_termination: Option<EmacsTermination>,
    /// The latest regexp match data in buffer coordinates.
    pub last_match_data: Option<Vec<Option<(usize, usize)>>>,
    /// Source buffer for buffer-origin match data; string searches leave this unset.
    pub last_match_data_buffer_id: Option<u64>,
    pub profiler_memory_running: bool,
    pub profiler_memory_log_pending: bool,
    pub profiler_cpu_running: bool,
    pub profiler_cpu_log_pending: bool,
    pub message_capture_stack: Vec<MessageCapture>,
    /// Identity of the function activation currently being evaluated, plus
    /// recently captured closure environments keyed by activation.  Sibling
    /// lambdas captured in one activation with an unchanged lexical
    /// environment share one environment cell, so a `setq' through one
    /// closure is visible to the others like upstream lexical binding.
    current_activation_id: u64,
    next_activation_id: u64,
    closure_capture_cache: Vec<(u64, std::rc::Weak<std::cell::RefCell<Env>>)>,
    /// Canonical values for mutated captured lexical cells, keyed first by
    /// the exact identity stamp of their frame and then by binding name.
    /// Environments are still represented as cheap snapshots; this overlay
    /// gives those snapshots GNU's shared-cell mutation semantics without
    /// ever aliasing unrelated frames that merely have the same shape.
    lexical_cell_updates: HashMap<i64, HashMap<String, Value>>,
    /// Weak owners of captured frames.  This lets assignments distinguish a
    /// genuinely captured lexical cell from an ordinary marked local without
    /// retaining every closure ever created.
    captured_lexical_frames: HashMap<i64, Vec<std::rc::Weak<std::cell::RefCell<Env>>>>,
    /// Evaluation context belongs to the closure object, not to its captured
    /// variable frames.  Keep weak identities here so metadata can never
    /// affect environment lookup, emptiness, or frame merging.  Absence is
    /// meaningful: Rust-generated dispatch lambdas inherit their caller,
    /// while Lisp lambdas explicitly record lexical or dynamic evaluation.
    closure_eval_contexts: HashMap<usize, (std::rc::Weak<std::cell::RefCell<Env>>, bool)>,
    closure_eval_context_registrations: usize,
    pub lossage_size: i64,
    interactive_call_depth: usize,
    pub(crate) lisp_face_states: Vec<LispFaceState>,
    selected_frame_face_hash_table: Option<Value>,
    pub(crate) next_lisp_face_id: i64,
    pub(crate) font_selection_order: [String; 4],
    pub(crate) alternative_font_family_alist: Value,
    pub(crate) alternative_font_registry_alist: Value,
    pub(crate) tty_suppress_bold_inverse_default_colors: bool,
    pub(crate) fontset_states: Vec<FontsetState>,
    pub(crate) fringe_bitmap_states: Vec<FringeBitmapState>,
    pub(crate) composition_states: Vec<CompositionState>,
    /// Raw etc/DOC byte offsets installed in built-in subroutine objects by
    /// `Snarf-documentation`, keyed by the canonical native function name.
    pub(crate) builtin_doc_offsets: HashMap<String, i64>,
    syntax_word_chars: Vec<u32>,
    standard_syntax_table_id: u64,
    undo_sequence: Option<UndoSequenceState>,
    load_path: Vec<PathBuf>,
    loading_features: Vec<String>,
    lambda_capture_overrides: Vec<bool>,
    lambda_trim_overrides: Vec<bool>,
    thread_states: Vec<ThreadState>,
    mutex_states: Vec<MutexState>,
    condition_variables: Vec<ConditionVariableState>,
    combined_after_change: Option<CombinedAfterChangeState>,
    process_states: Vec<ProcessState>,
    class_states: Vec<ClassState>,
    class_parent_overrides: Vec<(u64, Vec<String>)>,
    // Object records whose GNU type tag is the class object rather than the
    // class symbol (objects created with `eieio-backward-compatibility' nil
    // and every class's default-object cache); they print with the class
    // object expanded in place of the type symbol.
    class_object_tagged_records: HashSet<u64>,
    generalizer_states: Vec<GenericGeneralizerState>,
    pending_timers: Vec<ScheduledTimer>,
    /// Source-loaded callbacks stand in for GNU's byte-compiled Lisp.  Keep
    /// defsubst definitions removed by loadhist alive until the active timer
    /// returns, matching calls that GNU compiled inline into the callback.
    timer_callback_depth: usize,
    deferred_defsubst_unbindings: Vec<(String, Value)>,
    /// Quoted templates already scanned and found free of reader marker
    /// forms; `quote' returns them as-is (keyed by car-cell address, the
    /// stored Value keeps the template alive so keys stay unique).
    pub(crate) plain_quote_templates: HashMap<usize, Value>,
    pending_file_notifications: Vec<(String, String)>,
    file_notify_watches: HashMap<i64, FileNotifyWatch>,
    pub(crate) gnu_pcase_load_attempted: bool,
    pub(crate) gnu_rx_load_attempted: bool,
    pub(crate) pending_url_retrievals: Vec<PendingUrlRetrieval>,
    main_thread_id: u64,
    active_thread_id: u64,
    last_thread_error: Option<Value>,
    backtrace_frames: Vec<BacktraceFrame>,
    active_handlers: Vec<ActiveHandler>,
    /// Dynamically active `catch' tags.  GNU's `throw' signals `no-catch'
    /// immediately when no `eq' tag is live, allowing condition handlers to
    /// observe the error without intercepting throws bound for an outer catch.
    active_catch_tags: Vec<Value>,
    handler_dispatch_depth: usize,
    suspend_condition_case_count: usize,
    window_margins: Vec<(u64, Option<i64>, Option<i64>)>,
}

/// One entry in the dynamic handler stack, mirroring GNU's handlerlist.
/// `signal' walks this innermost-first: a matching `condition-case' clause
/// stops the search before any outer `handler-bind' functions run, while
/// matching `handler-bind' functions run at the signal point (pre-unwind).
#[derive(Clone)]
pub(crate) enum ActiveHandler {
    /// A single CONDITION/HANDLER pair from `handler-bind'.
    Bind(String, Value),
    /// The clause heads of an active `condition-case' (minus :success).
    Case(Vec<Value>),
}

type LambdaSourceBodyCacheEntry = (Weak<std::cell::RefCell<Value>>, Weak<Vec<Value>>);

fn make_query_replace_map(interp: &mut Interpreter) -> Value {
    let map = primitives::make_runtime_keymap(interp, Some("query-replace-map"));
    // replace.el is part of GNU's dumped image.  map-y-or-n-p and other
    // preloaded prompt helpers therefore see this complete response map
    // before `replace' is explicitly loaded.
    for (key, answer) in [
        ("SPC", "act"),
        ("DEL", "skip"),
        ("delete", "skip"),
        ("backspace", "skip"),
        ("y", "act"),
        ("n", "skip"),
        ("Y", "act"),
        ("N", "skip"),
        ("e", "edit-replacement"),
        ("E", "edit-replacement-exact-case"),
        (",", "act-and-show"),
        ("q", "exit"),
        ("RET", "exit"),
        ("return", "exit"),
        (".", "act-and-exit"),
        ("C-r", "edit"),
        ("C-w", "delete-and-edit"),
        ("C-l", "recenter"),
        ("!", "automatic"),
        ("^", "backup"),
        ("u", "undo"),
        ("U", "undo-all"),
        ("C-h", "help"),
        ("f1", "help"),
        ("help", "help"),
        ("?", "help"),
        ("C-g", "quit"),
        ("C-]", "quit"),
        ("C-v", "scroll-up"),
        ("M-v", "scroll-down"),
        ("next", "scroll-up"),
        ("prior", "scroll-down"),
        ("C-M-v", "scroll-other-window"),
        ("M-next", "scroll-other-window"),
        ("C-M-S-v", "scroll-other-window-down"),
        ("M-prior", "scroll-other-window-down"),
        ("escape", "exit-prefix"),
    ] {
        primitives::keymap_define_binding_with_placement(
            interp,
            &map,
            key,
            Some(vec![key.into()]),
            Value::Symbol(answer.into()),
            true,
        )
        .expect("static query-replace-map binding");
    }
    map
}

fn make_visual_line_mode_map(interp: &mut Interpreter) -> Value {
    let map = primitives::make_runtime_keymap(interp, Some("visual-line-mode-map"));
    for (command, replacement) in [
        ("kill-line", "kill-visual-line"),
        ("move-beginning-of-line", "beginning-of-visual-line"),
        ("move-end-of-line", "end-of-visual-line"),
    ] {
        let parts = vec!["<remap>".into(), format!("<{command}>")];
        let _ = primitives::keymap_define_binding_with_placement(
            interp,
            &map,
            &parts.join(" "),
            Some(parts),
            Value::Symbol(replacement.into()),
            true,
        );
    }
    map
}

impl Default for Interpreter {
    fn default() -> Self {
        Self::new()
    }
}

impl Interpreter {
    pub(crate) fn request_termination(&mut self, termination: EmacsTermination) {
        self.pending_termination = Some(termination);
    }

    pub(crate) fn pending_termination(&self) -> Option<&EmacsTermination> {
        self.pending_termination.as_ref()
    }

    pub(crate) fn take_pending_termination(&mut self) -> Option<EmacsTermination> {
        self.pending_termination.take()
    }

    pub fn new() -> Self {
        let main_thread_id = 1u64;
        let standard_obarray_id = 2u64;
        let standard_syntax_table_id = 1u64;
        let local_time_zone_rule = std::env::var("TZ")
            .map(Value::String)
            .unwrap_or_else(|_| Value::Symbol("wall".into()));
        let frame_name =
            primitives::make_shared_string_value_with_multibyte("F1".into(), Vec::new(), true);
        let fringe_bitmaps = Value::list(
            primitives::STANDARD_FRINGE_BITMAPS
                .iter()
                .rev()
                .map(|name| Value::symbol(name)),
        );
        let fringe_bitmap_states = primitives::STANDARD_FRINGE_BITMAPS
            .iter()
            .enumerate()
            .map(|(index, name)| FringeBitmapState {
                name: (*name).into(),
                id: (index + 1) as i64,
                standard: true,
                definition: None,
                face: Value::Nil,
            })
            .collect();
        let mut interp = Interpreter {
            globals: vec![
                ("main-thread".into(), Value::Record(main_thread_id)),
                ("obarray".into(), Value::Record(standard_obarray_id)),
                ("cl--proclaims-deferred".into(), Value::Nil),
                ("cl-old-struct-compat-mode".into(), Value::Nil),
                // GNU frame.c defines this native variable before frame.el
                // and any dumped/preloaded Lisp run.
                ("default-frame-alist".into(), Value::Nil),
                ("fringe-bitmaps".into(), fringe_bitmaps),
                (
                    "image-types".into(),
                    Value::list([
                        Value::symbol("pbm"),
                        Value::symbol("png"),
                        Value::symbol("jpeg"),
                        Value::symbol("gif"),
                        Value::symbol("svg"),
                        Value::symbol("xbm"),
                        Value::symbol("xpm"),
                        Value::symbol("webp"),
                        Value::symbol("tiff"),
                    ]),
                ),
                ("max-image-size".into(), Value::Float(10.0)),
                ("cross-disabled-images".into(), Value::Nil),
                (
                    "x-bitmap-file-path".into(),
                    Value::list([Value::String(".".into())]),
                ),
                ("image-cache-eviction-delay".into(), Value::Integer(300)),
                ("image-scaling-factor".into(), Value::symbol("auto")),
                (
                    "command-line-args".into(),
                    primitives::command_line_args_value(),
                ),
                ("cpp-font-lock-keywords".into(), Value::Nil),
                ("current-load-list".into(), Value::Nil),
                ("load-history".into(), Value::Nil),
                ("case-replace".into(), Value::T),
                ("byte-compile-log-buffer".into(), Value::Nil),
                ("defining-kbd-macro".into(), Value::Nil),
                ("delay-mode-hooks".into(), Value::Nil),
                ("delayed-after-hook-functions".into(), Value::Nil),
                ("delayed-mode-hooks".into(), Value::Nil),
                ("executing-kbd-macro".into(), Value::Nil),
                ("executing-kbd-macro-index".into(), Value::Integer(0)),
                ("exec-path".into(), current_exec_path()),
                ("kbd-macro-termination-hook".into(), Value::Nil),
                ("last-kbd-macro".into(), Value::Nil),
                ("file-name-handler-alist".into(), Value::Nil),
                ("inhibit-file-name-handlers".into(), Value::Nil),
                ("inhibit-file-name-operation".into(), Value::Nil),
                ("inhibit-read-only".into(), Value::Nil),
                ("kill-emacs-hook".into(), Value::Nil),
                ("delete-terminal-functions".into(), Value::Nil),
                ("null-device".into(), Value::String("/dev/null".into())),
                (
                    "default-process-coding-system".into(),
                    Value::cons(Value::symbol("utf-8-unix"), Value::symbol("utf-8-unix")),
                ),
                ("process-coding-system-alist".into(), Value::Nil),
                ("network-coding-system-alist".into(), Value::Nil),
                ("delete-exited-processes".into(), Value::T),
                ("fast-read-process-output".into(), Value::T),
                ("internal--daemon-sockname".into(), Value::Nil),
                (
                    "interrupt-process-functions".into(),
                    Value::list([Value::symbol("internal-default-interrupt-process")]),
                ),
                ("process-adaptive-read-buffering".into(), Value::T),
                ("process-connection-type".into(), Value::T),
                ("process-error-pause-time".into(), Value::Integer(1)),
                ("inherit-process-coding-system".into(), Value::Nil),
                ("process-prioritize-lower-fds".into(), Value::Nil),
                ("read-process-output-max".into(), Value::Integer(65_536)),
                ("selection-converter-alist".into(), Value::Nil),
                (
                    "signal-process-functions".into(),
                    Value::list([Value::symbol("internal-default-signal-process")]),
                ),
                ("system-uses-terminfo".into(), Value::T),
                ("warning-fill-column".into(), Value::Integer(78)),
                ("warning-fill-prefix".into(), Value::Nil),
                (
                    "warning-levels".into(),
                    Value::list([
                        Value::list([
                            Value::symbol(":emergency"),
                            Value::String("Emergency%s: ".into()),
                            Value::symbol("ding"),
                        ]),
                        Value::list([Value::symbol(":error"), Value::String("Error%s: ".into())]),
                        Value::list([
                            Value::symbol(":warning"),
                            Value::String("Warning%s: ".into()),
                        ]),
                        Value::list([Value::symbol(":debug"), Value::String("Debug%s: ".into())]),
                    ]),
                ),
                ("warning-minimum-level".into(), Value::symbol(":warning")),
                (
                    "warning-minimum-log-level".into(),
                    Value::symbol(":warning"),
                ),
                ("warning-prefix-function".into(), Value::Nil),
                ("warning-series".into(), Value::Nil),
                ("warning-suppress-log-types".into(), Value::Nil),
                ("warning-suppress-types".into(), Value::Nil),
                ("warning-type-format".into(), Value::String(" (%s)".into())),
                (
                    "vc-directory-exclusion-list".into(),
                    preloaded_vc_directory_exclusion_list(),
                ),
                ("lread--unescaped-character-literals".into(), Value::Nil),
                (
                    "standard-output".into(),
                    Value::Symbol("external-debugging-output".into()),
                ),
                ("emaxx-external-debugging-output-target".into(), Value::Nil),
                (
                    "code-conversion-map-vector".into(),
                    Value::list(
                        std::iter::once(Value::Symbol("vector-literal".into()))
                            .chain(std::iter::repeat_n(Value::Nil, 16)),
                    ),
                ),
                ("translation-hash-table-vector".into(), Value::Nil),
                ("font-ccl-encoder-alist".into(), Value::Nil),
                ("charset-revision-table".into(), Value::Nil),
                ("enable-character-translation".into(), Value::T),
                ("last-code-conversion-error".into(), Value::Nil),
                (
                    "latin-extra-code-table".into(),
                    Value::list(
                        std::iter::once(Value::Symbol("vector-literal".into()))
                            .chain(std::iter::repeat_n(Value::Nil, 256)),
                    ),
                ),
                ("select-safe-coding-system-function".into(), Value::Nil),
                ("standard-translation-table-for-decode".into(), Value::Nil),
                ("standard-translation-table-for-encode".into(), Value::Nil),
                ("translation-table-for-input".into(), Value::Nil),
            ],
            variable_aliases: Vec::new(),
            variable_aliases_index: HashMap::new(),
            special_variables_index: HashSet::new(),
            local_special_names: HashSet::new(),
            soft_special_names: HashSet::new(),
            dlet_active_names: HashMap::new(),
            special_scan_floor: 0,
            lisp_eval_depth: 0,
            kbd_macro_executions: Vec::new(),
            kbd_macro_definition: Vec::new(),
            kbd_macro_committed_len: 0,
            keyboard_input: KeyboardInputState::default(),
            command_loop_recursion_depth: 0,
            local_time_zone_rule,
            special_variables: vec![
                "case-fold-search".into(),
                "executing-kbd-macro".into(),
                "executing-kbd-macro-index".into(),
                "defining-kbd-macro".into(),
                "kbd-macro-termination-hook".into(),
                "last-kbd-macro".into(),
                "track-mouse".into(),
                "last-input-event".into(),
                "last-command-event".into(),
                "last-event-frame".into(),
                "last-nonmenu-event".into(),
                "prefix-arg".into(),
                "last-prefix-arg".into(),
                "current-prefix-arg".into(),
                "signal-hook-function".into(),
                "command-error-function".into(),
                "gensym-counter".into(),
                "minor-mode-overriding-map-alist".into(),
                "overriding-terminal-local-map".into(),
                "overriding-local-map".into(),
                "standard-input".into(),
                "debug-on-error".into(),
                "debug-on-quit".into(),
                "inhibit-redisplay".into(),
                "inhibit-quit".into(),
                "quit-flag".into(),
                "unread-command-events".into(),
                "coding-system-alist".into(),
                "char-code-property-alist".into(),
                "overlay-arrow-position".into(),
                "overlay-arrow-string".into(),
                "load-read-function".into(),
                "byte-compile-log-buffer".into(),
                "command-line-args".into(),
                "command-line-args-left".into(),
                "command-switch-alist".into(),
                "cl--proclaims-deferred".into(),
                "current-load-list".into(),
                "load-history".into(),
                "default-frame-alist".into(),
                "delay-mode-hooks".into(),
                "delayed-after-hook-functions".into(),
                "delayed-mode-hooks".into(),
                "display-hourglass".into(),
                "exec-path".into(),
                "file-name-handler-alist".into(),
                "gc-cons-threshold".into(),
                "inhibit-read-only".into(),
                "inhibit-file-name-handlers".into(),
                "inhibit-file-name-operation".into(),
                "indent-tabs-mode".into(),
                "initial-window-system".into(),
                "last-coding-system-used".into(),
                "coding-system-for-read".into(),
                "coding-system-for-write".into(),
                "file-coding-system-alist".into(),
                "process-coding-system-alist".into(),
                "network-coding-system-alist".into(),
                "line-spacing".into(),
                "left-margin".into(),
                "last-command".into(),
                "load-force-doc-strings".into(),
                "load-read-function".into(),
                "null-device".into(),
                "overwrite-mode".into(),
                "default-process-coding-system".into(),
                "delete-exited-processes".into(),
                "fast-read-process-output".into(),
                "internal--daemon-sockname".into(),
                "interrupt-process-functions".into(),
                "process-adaptive-read-buffering".into(),
                "process-connection-type".into(),
                "process-error-pause-time".into(),
                "inherit-process-coding-system".into(),
                "process-environment".into(),
                "process-prioritize-lower-fds".into(),
                "read-process-output-max".into(),
                "selection-converter-alist".into(),
                "signal-process-functions".into(),
                "warning-fill-prefix".into(),
                "warning-prefix-function".into(),
                "warning-series".into(),
                "warning-suppress-log-types".into(),
                "warning-suppress-types".into(),
                "warning-type-format".into(),
                "window-system".into(),
                "scroll-preserve-screen-position".into(),
                "scroll-up-aggressively".into(),
                "standard-output".into(),
                "vertical-scroll-bar".into(),
                "vc-directory-exclusion-list".into(),
            ],
            symbol_properties: builtin_symbol_properties(),
            interned_symbols: Vec::new(),
            interned_symbol_names: HashSet::new(),
            standard_obarray_id,
            variable_watchers: Vec::new(),
            buffer: crate::buffer::Buffer::new("*test*"),
            current_global_map: None,
            current_buffer_id: 0,
            selected_window_id: 0,
            window_cursor_visibility: HashMap::new(),
            old_selected_window_id: 0,
            frame_old_selected_window_id: None,
            window_select_count: 1,
            frame_states: vec![FrameState {
                id: 1,
                name: frame_name,
                live: true,
                width: 80,
                height: 25,
                text_height: 25,
                parameter_width: 80,
                parameter_height: 25,
                parameter_overrides: Vec::new(),
                focus_frame_id: None,
                left: 0,
                top: 0,
                window_state_change: false,
                after_make_frame: true,
                pointer_invisible: false,
                was_invisible: false,
            }],
            selected_frame_id: 1,
            old_selected_frame_id: 1,
            frame_and_buffer_state: Value::Nil,
            terminal_parameters: Vec::new(),
            terminal_live: true,
            inactive_buffers: vec![(1, crate::buffer::Buffer::new("*Messages*"))],
            buffer_list: vec![(0, "*test*".to_string()), (1, "*Messages*".to_string())],
            next_buffer_id: 2,
            next_overlay_id: 1,
            next_marker_id: 1,
            markers: Vec::new(),
            buffer_mark_marker_ids: HashMap::new(),
            char_tables: vec![
                CharTableState {
                    id: standard_syntax_table_id,
                    subtype: Some("syntax-table".into()),
                    default: Value::Nil,
                    parent: None,
                    extra_slots: Vec::new(),
                    entries: standard_syntax_table_entries(),
                    category_docs: Vec::new(),
                },
                // GNU text-mode-syntax-table: `"' and `\' are
                // punctuation, `'' is a word constituent with the prefix
                // flag (Bug#15014 hinges on `"' NOT being a string quote).
                CharTableState {
                    id: 2,
                    subtype: Some("syntax-table".into()),
                    default: Value::Nil,
                    parent: Some(standard_syntax_table_id),
                    extra_slots: Vec::new(),
                    entries: vec![
                        CharTableEntry {
                            start: '"' as u32,
                            end: '"' as u32,
                            value: Value::String(".".into()),
                        },
                        CharTableEntry {
                            start: '\\' as u32,
                            end: '\\' as u32,
                            value: Value::String(".".into()),
                        },
                        CharTableEntry {
                            start: '\'' as u32,
                            end: '\'' as u32,
                            value: Value::String("w p".into()),
                        },
                    ],
                    category_docs: Vec::new(),
                },
                // GNU lisp-data-mode-syntax-table, exposed through the
                // `emacs-lisp-mode-syntax-table' variable so that
                // copy-syntax-table callers (ietf-drums.el) inherit the
                // symbol-constituent punctuation entries.
                CharTableState {
                    id: 3,
                    subtype: Some("syntax-table".into()),
                    default: Value::Nil,
                    parent: Some(standard_syntax_table_id),
                    extra_slots: Vec::new(),
                    entries: lisp_data_syntax_table_entries(),
                    category_docs: Vec::new(),
                },
            ],
            unicode_property_table_ids: HashMap::new(),
            equal_hash_tables: HashMap::new(),
            charset_aliases: Vec::new(),
            charset_ids: vec![
                ("ascii".into(), 0),
                ("iso-8859-1".into(), 1),
                ("unicode".into(), 2),
                ("emacs".into(), 3),
                ("eight-bit".into(), 4),
            ],
            // charset.c creates these five records before dumped Lisp runs.
            // Their direct mappings are part of the preload contract: merely
            // reserving the names makes charset codings appear valid while
            // rejecting every non-ASCII character (notably Latin-1).
            charset_plists: vec![
                (
                    "ascii".into(),
                    Value::list([
                        Value::symbol(":ascii-compatible-p"),
                        Value::T,
                        Value::symbol(":code-offset"),
                        Value::Integer(0),
                    ]),
                ),
                (
                    "iso-8859-1".into(),
                    Value::list([
                        Value::symbol(":ascii-compatible-p"),
                        Value::T,
                        Value::symbol(":code-offset"),
                        Value::Integer(0),
                    ]),
                ),
                (
                    "unicode".into(),
                    Value::list([
                        Value::symbol(":ascii-compatible-p"),
                        Value::T,
                        Value::symbol(":code-offset"),
                        Value::Integer(0),
                    ]),
                ),
                (
                    "emacs".into(),
                    Value::list([
                        Value::symbol(":ascii-compatible-p"),
                        Value::T,
                        Value::symbol(":code-offset"),
                        Value::Integer(0),
                    ]),
                ),
                (
                    "eight-bit".into(),
                    Value::list([
                        Value::symbol(":ascii-compatible-p"),
                        Value::Nil,
                        Value::symbol(":code-offset"),
                        Value::Integer(0x3f_ff80),
                    ]),
                ),
            ],
            charset_priority: vec!["unicode".into(), "ascii".into(), "eight-bit".into()],
            iso_charsets: vec![(1, 94, 'B' as u32, "ascii".into())],
            coding_systems: builtin_coding_systems(),
            ccl_programs: vec![None; 32],
            coding_aliases: builtin_coding_aliases(),
            coding_priority: builtin_coding_priority(),
            terminal_coding: None,
            keyboard_coding: None,
            input_interrupt_mode: true,
            standard_category_table_id: None,
            standard_case_table_id: None,
            ascii_case_table_ids: Vec::new(),
            buffer_case_tables: Vec::new(),
            next_char_table_id: 4,
            records: vec![
                RecordState {
                    id: main_thread_id,
                    type_name: "thread".into(),
                    slots: Vec::new(),
                },
                RecordState {
                    id: standard_obarray_id,
                    type_name: "obarray".into(),
                    slots: vec![Value::Nil],
                },
            ],
            sqlite_handles: Vec::new(),
            bytecode_program_cache: Vec::new(),
            treesit_queries: Vec::new(),
            treesit_languages: Vec::new(),
            treesit_parsers: Vec::new(),
            treesit_nodes: Vec::new(),
            advice_registry: std::collections::HashMap::new(),
            next_record_id: 3,
            next_finalizer_id: 1,
            next_generated_symbol_id: 1,
            buffer_local_hooks: Vec::new(),
            buffer_locals: Vec::new(),
            buffer_syntax_tables: Vec::new(),
            auto_buffer_locals: vec![
                "case-fold-search".into(),
                // font-core.el: (defvar-local font-lock-defaults nil)
                "font-lock-defaults".into(),
                // syntax.el: (defvar-local syntax-propertize--done -1)
                "syntax-propertize--done".into(),
                "cursor-in-non-selected-windows".into(),
                "left-margin".into(),
                "line-spacing".into(),
                "overwrite-mode".into(),
                "scroll-up-aggressively".into(),
                "vertical-scroll-bar".into(),
            ],
            active_special_restores: Vec::new(),
            next_special_binding_id: 1,
            labeled_restrictions: Vec::new(),
            indirect_buffers: Vec::new(),
            change_hooks_running: 0,
            macros: Vec::new(),
            macros_name_counts: HashMap::default(),
            functions: Vec::new(),
            functions_index: HashMap::default(),
            network_connect_counter: 0,
            definition_generation: 0,
            not_macro_names: HashMap::new(),
            macro_expansion_cache: HashMap::new(),
            lambda_source_bodies: HashMap::new(),
            provided_features: vec![
                // GNU bindings.el advertises these host-backed primitive
                // families in the dumped image.
                "base64".into(),
                // GNU dumps cl-preloaded.el's circular CL class/structure
                // foundation.  Emaxx's equivalent host metadata is
                // installed below before any included struct is defined.
                "cl-preloaded".into(),
                "emacs".into(),
                "emaxx".into(),
                "ert".into(),
                "kqueue".into(),
                "lcms2".into(),
                // GNU process.c provides make-network-process with its
                // capability subfeatures; the property is set alongside in
                // Interpreter::new (featurep consults it).
                "make-network-process".into(),
                "md5".into(),
                "native-compile".into(),
                // GNU preloads oclosure.el (loadup.el); emaxx implements
                // oclosures natively, so (require 'oclosure) must not load
                // the GNU file over them.
                "oclosure".into(),
                "overlay".into(),
                "sha1".into(),
                "text-properties".into(),
                // `url' itself is a Lisp package: claiming its feature here
                // would make `(require 'url)' skip url.el's parser, record,
                // cookie, and method setup.  Only the HTTP transport entry
                // points are pinned to Rust (see `prefer_builtin_override').
                "url-http".into(),
                "threads".into(),
            ],
            after_load_forms: Vec::new(),
            current_load_file: None,
            load_history_suppressed_files: Vec::new(),
            ert_test_source_file: None,
            current_ert_test_name: None,
            ert_tests: Vec::new(),
            test_results: Vec::new(),
            last_selected_tests: Vec::new(),
            pending_termination: None,
            last_match_data: None,
            last_match_data_buffer_id: None,
            profiler_memory_running: false,
            profiler_memory_log_pending: false,
            profiler_cpu_running: false,
            profiler_cpu_log_pending: false,
            message_capture_stack: Vec::new(),
            current_activation_id: 0,
            next_activation_id: 0,
            closure_capture_cache: Vec::new(),
            lexical_cell_updates: HashMap::new(),
            captured_lexical_frames: HashMap::new(),
            closure_eval_contexts: HashMap::new(),
            closure_eval_context_registrations: 0,
            lossage_size: 300,
            interactive_call_depth: 0,
            lisp_face_states: vec![LispFaceState {
                name: "default".into(),
                id: Some(0),
                global: Some(empty_lisp_face_vector()),
                selected_frame: Some(empty_lisp_face_vector()),
            }],
            selected_frame_face_hash_table: None,
            next_lisp_face_id: 1,
            font_selection_order: [
                ":width".into(),
                ":height".into(),
                ":weight".into(),
                ":slant".into(),
            ],
            alternative_font_family_alist: Value::Nil,
            alternative_font_registry_alist: Value::Nil,
            tty_suppress_bold_inverse_default_colors: false,
            fontset_states: vec![FontsetState {
                name: "-*-*-*-*-*-*-*-*-*-*-*-*-fontset-default".into(),
                mappings: Vec::new(),
            }],
            fringe_bitmap_states,
            composition_states: Vec::new(),
            builtin_doc_offsets: HashMap::new(),
            syntax_word_chars: Vec::new(),
            standard_syntax_table_id,
            undo_sequence: None,
            load_path: Vec::new(),
            loading_features: Vec::new(),
            lambda_capture_overrides: Vec::new(),
            lambda_trim_overrides: Vec::new(),
            thread_states: vec![ThreadState {
                record_id: main_thread_id,
                name: None,
                buffer_id: 0,
                buffer_disposition: BufferDisposition::Default,
                buffer_killed: false,
                status: ThreadStatus::Runnable,
                program: ThreadProgram::Main,
                outcome: None,
                waiting_for_user_input: false,
            }],
            mutex_states: Vec::new(),
            condition_variables: Vec::new(),
            combined_after_change: None,
            process_states: Vec::new(),
            class_states: Vec::new(),
            class_parent_overrides: Vec::new(),
            class_object_tagged_records: HashSet::new(),
            generalizer_states: Vec::new(),
            pending_timers: Vec::new(),
            timer_callback_depth: 0,
            deferred_defsubst_unbindings: Vec::new(),
            plain_quote_templates: HashMap::new(),
            pending_file_notifications: Vec::new(),
            file_notify_watches: HashMap::new(),
            gnu_pcase_load_attempted: false,
            gnu_rx_load_attempted: false,
            pending_url_retrievals: Vec::new(),
            main_thread_id,
            active_thread_id: main_thread_id,
            last_thread_error: None,
            backtrace_frames: Vec::new(),
            active_handlers: Vec::new(),
            active_catch_tags: Vec::new(),
            handler_dispatch_depth: 0,
            suspend_condition_case_count: 0,
            window_margins: Vec::new(),
            globals_index: HashMap::new(),
        };
        interp.globals_index = interp.globals.iter().cloned().collect();
        interp.special_variables_index = interp.special_variables.iter().cloned().collect();
        // GNU's dumped autoload variables originate in `defvar' / `defcustom'
        // forms: keep their defaults lazy, but install the special declaration.
        for name in generated_autoloads::generated_dumped_variable_names() {
            interp.mark_special_variable(name);
        }
        interp.mark_special_variable("fringe-bitmaps");
        for (index, name) in primitives::STANDARD_FRINGE_BITMAPS.iter().enumerate() {
            interp.put_symbol_property(name, "fringe", Value::Integer((index + 1) as i64));
        }
        for name in GNU_LREAD_SPECIAL_VARIABLES {
            interp.mark_special_variable(name);
        }
        for name in GNU_EMACS_LOCALE_SPECIAL_VARIABLES {
            interp.set_global_binding(name, Value::Nil);
            interp.mark_special_variable(name);
        }
        for name in GNU_TREESIT_SPECIAL_VARIABLES {
            interp.set_global_binding(name, Value::Nil);
            interp.mark_special_variable(name);
        }
        for name in GNU_KEYBOARD_TIMER_SPECIAL_VARIABLES {
            interp.mark_special_variable(name);
        }
        for name in GNU_IMAGE_SPECIAL_VARIABLES {
            interp.mark_special_variable(name);
        }
        for name in GNU_NATIVE_PER_BUFFER_VARIABLES {
            interp.mark_per_buffer_special(name);
        }
        for name in GNU_NATIVE_PERMANENT_LOCAL_VARIABLES {
            interp.put_symbol_property(name, "permanent-local", Value::T);
        }
        for name in GNU_ALWAYS_LOCAL_PER_BUFFER_VARIABLES {
            interp.mark_always_buffer_local_special(name);
        }
        // search.c uses its own Lisp variable rather than a Buffer field, but
        // GNU gives it the same always-buffer-local behavior.
        interp.mark_per_buffer_special("case-fold-search");
        // GNU process.c: (provide 'make-network-process '(...subfeatures)),
        // consulted by two-argument `featurep' (erc-d's unix-socket test
        // checks (featurep 'make-network-process '(:family local))).
        {
            let pair = |key: &str, value: Value| Value::list([Value::Symbol(key.into()), value]);
            let subfeatures = Value::list([
                Value::Symbol(":reuseaddr".into()),
                Value::Symbol(":priority".into()),
                Value::Symbol(":oobinline".into()),
                Value::Symbol(":linger".into()),
                Value::Symbol(":keepalive".into()),
                Value::Symbol(":dontroute".into()),
                Value::Symbol(":broadcast".into()),
                Value::Symbol(":bindtodevice".into()),
                pair(":server", Value::T),
                pair(":service", Value::T),
                pair(":family", Value::Symbol("ipv6".into())),
                pair(":family", Value::Symbol("ipv4".into())),
                pair(":family", Value::Symbol("local".into())),
                pair(":type", Value::Symbol("seqpacket".into())),
                pair(":type", Value::Symbol("datagram".into())),
                pair(":nowait", Value::T),
            ]);
            interp.put_symbol_property("make-network-process", "subfeatures", subfeatures);
        }
        interp.put_symbol_property(
            "overlay",
            "subfeatures",
            Value::list([
                Value::Symbol("display".into()),
                Value::Symbol("syntax-table".into()),
                Value::Symbol("field".into()),
            ]),
        );
        interp.put_symbol_property(
            "text-properties",
            "subfeatures",
            Value::list([
                Value::Symbol("display".into()),
                Value::Symbol("syntax-table".into()),
                Value::Symbol("field".into()),
                Value::Symbol("point-entered".into()),
            ]),
        );
        for class_name in primitives::builtin_class_names() {
            interp.put_symbol_property(
                class_name,
                "cl--class",
                interned_symbol_value((*class_name).into()),
            );
            if let Some(predicate) = primitives::builtin_class_predicate(class_name) {
                interp.put_symbol_property(
                    class_name,
                    "cl-deftype-satisfies",
                    Value::Symbol(predicate.into()),
                );
            }
        }
        for (type_name, predicate) in primitives::builtin_cl_satisfies_types() {
            interp.put_symbol_property(
                type_name,
                "cl-deftype-satisfies",
                Value::Symbol((*predicate).into()),
            );
        }
        // simple.el dumps decoded-time's `:type list' cl-defstruct.  Emaxx
        // keeps its accessors host-backed, but must publish the same struct
        // and compiler-macro metadata so GNU gv.el can lower their setf
        // places to list cells.
        let decoded_time_slots = [
            "second", "minute", "hour", "day", "month", "year", "weekday", "dst", "zone",
        ];
        interp.put_symbol_property(
            "decoded-time",
            "emaxx-struct-slots",
            Value::list(decoded_time_slots.into_iter().map(Value::symbol)),
        );
        interp.put_symbol_property(
            "decoded-time",
            "emaxx-struct-defaults",
            Value::list(
                decoded_time_slots
                    .into_iter()
                    .enumerate()
                    .map(|(index, _)| {
                        if index == 7 {
                            Value::Integer(-1)
                        } else {
                            Value::Nil
                        }
                    }),
            ),
        );
        interp.put_symbol_property(
            "decoded-time",
            "emaxx-struct-slot-descs",
            Value::list(
                std::iter::once(Value::list([Value::symbol("cl-tag-slot")])).chain(
                    decoded_time_slots
                        .into_iter()
                        .map(|slot| Value::list([Value::symbol(slot)])),
                ),
            ),
        );
        interp.put_symbol_property(
            "decoded-time",
            "emaxx-struct-sequence-type",
            Value::symbol("list"),
        );
        for (index, slot) in decoded_time_slots.into_iter().enumerate() {
            let accessor = format!("decoded-time-{slot}");
            interp.put_symbol_property(
                &accessor,
                "emaxx-struct-type",
                Value::symbol("decoded-time"),
            );
            interp.put_symbol_property(
                &accessor,
                "emaxx-struct-slot",
                Value::Integer(index as i64),
            );
            interp.install_struct_accessor_compiler_macro(&accessor, "nth", index);
        }

        // cl-preloaded.el also dumps cl-slot-descriptor before ordinary
        // libraries run.  Its accessors stay host-backed in Emaxx, while the
        // structure and compiler-macro metadata remain observable Lisp
        // contract (notably to gv.el and CEDET).
        let cl_slot_descriptor_slots = ["name", "initform", "type", "props"];
        interp.put_symbol_property(
            "cl-slot-descriptor",
            "emaxx-struct-slots",
            Value::list(cl_slot_descriptor_slots.into_iter().map(Value::symbol)),
        );
        interp.put_symbol_property(
            "cl-slot-descriptor",
            "emaxx-struct-defaults",
            Value::list(std::iter::repeat_n(
                Value::Nil,
                cl_slot_descriptor_slots.len(),
            )),
        );
        interp.put_symbol_property(
            "cl-slot-descriptor",
            "emaxx-struct-slot-descs",
            Value::list(
                std::iter::once(Value::list([Value::symbol("cl-tag-slot")])).chain(
                    cl_slot_descriptor_slots
                        .into_iter()
                        .map(|slot| Value::list([Value::symbol(slot)])),
                ),
            ),
        );
        interp.put_symbol_property(
            "cl-slot-descriptor",
            "emaxx-struct-sequence-type",
            Value::Nil,
        );
        for (index, slot) in cl_slot_descriptor_slots.into_iter().enumerate() {
            let accessor = format!("cl--slot-descriptor-{slot}");
            interp.put_symbol_property(
                &accessor,
                "emaxx-struct-type",
                Value::symbol("cl-slot-descriptor"),
            );
            interp.put_symbol_property(
                &accessor,
                "emaxx-struct-slot",
                Value::Integer(index as i64),
            );
            interp.install_struct_accessor_compiler_macro(&accessor, "aref", index + 1);
        }

        // cl-preloaded.el creates this parent before eieio-core defines
        // `eieio--class' with (:include cl--class).  The source bootstrap is
        // intentionally circular and builds GNU's record/class object
        // representation; Emaxx owns that low-level representation in Rust,
        // so install the dumped parent metadata at the same boundary.  The
        // ordinary `cl-defstruct' producer then derives every inherited
        // EIEIO accessor and `(setf ACCESSOR)' function from this one table.
        let cl_class_slots = ["name", "docstring", "parents", "slots", "index-table"];
        interp.put_symbol_property(
            "cl--class",
            "emaxx-struct-slots",
            Value::list(cl_class_slots.into_iter().map(Value::symbol)),
        );
        interp.put_symbol_property(
            "cl--class",
            "emaxx-struct-defaults",
            Value::list(std::iter::repeat_n(Value::Nil, cl_class_slots.len())),
        );
        interp.put_symbol_property(
            "cl--class",
            "emaxx-struct-slot-descs",
            Value::list(
                std::iter::once(Value::list([Value::symbol("cl-tag-slot")])).chain(
                    cl_class_slots
                        .into_iter()
                        .map(|slot| Value::list([Value::symbol(slot)])),
                ),
            ),
        );
        interp.put_symbol_property("cl--class", "emaxx-struct-sequence-type", Value::Nil);
        for (slot, index) in [("name", 0usize), ("parents", 2), ("index-table", 4)] {
            let accessor = format!("cl--class-{slot}");
            interp.put_symbol_property(&accessor, "emaxx-struct-type", Value::symbol("cl--class"));
            interp.put_symbol_property(
                &accessor,
                "emaxx-struct-slot",
                Value::Integer(index as i64),
            );
            interp.install_struct_accessor_compiler_macro(&accessor, "aref", index + 1);
        }
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
        // C-x is bound through the prefix command symbol in GNU; keep the
        // symbol's definition around so `where-is-internal' can find it.
        interp.push_function_binding("Control-X-prefix", ctl_x_map.clone());
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
        interp.current_global_map = Some(global_map.clone());
        interp.set_global_binding("global-map", global_map);
        // Dumped mode maps are identity-bearing Lisp objects.  A computed
        // fallback would allocate a fresh `(keymap ...)' facade on every
        // variable read, breaking `eq', mapatoms, mutation, and aliases.
        for name in [
            "text-mode-map",
            "lisp-mode-shared-map",
            "lisp-mode-map",
            "emacs-lisp-mode-map",
            "special-mode-map",
        ] {
            let keymap = primitives::make_runtime_keymap(&mut interp, Some(name));
            interp.set_global_binding(name, keymap);
        }
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
        // subr.el constructs the initial global map before bindings.el is
        // dumped.  C-] is intentionally defined there (and not repeated by
        // bindings.el), so the native bootstrap side of that existing
        // boundary must carry it too.
        let _ = primitives::keymap_define_binding(
            &mut interp,
            &global_map,
            "C-]",
            Value::Symbol("abort-recursive-edit".into()),
        );
        // bindings.el's dumped global map supplies this canonical motion
        // binding.  Help's command-key substitution reads the live map, so
        // omitting it changes every `\[forward-char]' doc reference into the
        // unbound-command fallback instead of GNU's `C-f'.
        let _ = primitives::keymap_define_binding(
            &mut interp,
            &global_map,
            "C-f",
            Value::Symbol("forward-char".into()),
        );
        let menu_bar_edit_menu = primitives::make_runtime_keymap(&mut interp, Some("Edit"));
        interp.set_global_binding("menu-bar-edit-menu", menu_bar_edit_menu);
        let input_decode_map =
            primitives::make_runtime_keymap(&mut interp, Some("input-decode-map"));
        interp.set_global_binding("input-decode-map", input_decode_map);
        // keyboard.c creates these identity-bearing translation/event maps
        // before bindings.el is dumped.  Keep the native map family together
        // so loading the owning Lisp bindings never depends on ad-hoc nil
        // placeholders.
        for name in [
            "special-event-map",
            "function-key-map",
            "key-translation-map",
        ] {
            let keymap = primitives::make_runtime_keymap(&mut interp, Some(name));
            interp.set_global_binding(name, keymap);
            interp.mark_special_variable(name);
        }
        let minibuffer_local_map =
            primitives::make_runtime_keymap(&mut interp, Some("minibuffer-local-map"));
        interp.set_global_binding("minibuffer-local-map", minibuffer_local_map);
        let minibuffer_local_completion_map =
            primitives::make_runtime_keymap(&mut interp, Some("minibuffer-local-completion-map"));
        interp.set_global_binding(
            "minibuffer-local-completion-map",
            minibuffer_local_completion_map,
        );
        let query_replace_map = make_query_replace_map(&mut interp);
        interp.set_global_binding("query-replace-map", query_replace_map);
        // `visual-line-mode' deliberately stays native in Emaxx, so its
        // native bootstrap owns the same complete mode contract that GNU's
        // dumped simple.el creates: a stable map, mode variable, hook family,
        // and minor-mode registry entry.
        let visual_line_mode_map = make_visual_line_mode_map(&mut interp);
        interp.set_global_binding("visual-line-mode-map", visual_line_mode_map.clone());
        interp.mark_special_variable("visual-line-mode-map");
        interp.set_global_binding("mouse-wheel-buttons", Value::Nil);
        interp.set_global_binding(
            "minor-mode-map-alist",
            Value::list([Value::cons(
                Value::Symbol("visual-line-mode".into()),
                visual_line_mode_map,
            )]),
        );
        interp.set_global_binding(
            "minor-mode-list",
            Value::list([Value::Symbol("visual-line-mode".into())]),
        );
        primitives::ensure_standard_abbrev_tables(&mut interp);
        interp.set_global_binding("visual-line-mode", Value::Nil);
        interp.mark_special_variable("visual-line-mode");
        interp.mark_auto_buffer_local("visual-line-mode");
        for hook in [
            "visual-line-mode-hook",
            "visual-line-mode-on-hook",
            "visual-line-mode-off-hook",
        ] {
            interp.set_global_binding(hook, Value::Nil);
            interp.mark_special_variable(hook);
        }
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
        let mode_line_format = default_mode_line_format();
        interp.set_global_binding("mode-line-format", mode_line_format.clone());
        interp.mark_per_buffer_special("mode-line-format");
        interp.put_symbol_property(
            "mode-line-format",
            "standard-value",
            Value::list([quoted_literal(&mode_line_format)]),
        );
        for name in ["header-line-format", "tab-line-format"] {
            interp.set_global_binding(name, Value::Nil);
            interp.mark_per_buffer_special(name);
        }
        interp.set_global_binding(
            "mode-line-buffer-identification",
            Value::list([Value::String("%12b".into())]),
        );
        let glyphless_char_display =
            interp.make_char_table(Some("glyphless-char-display".into()), Value::Nil);
        interp.set_global_binding("glyphless-char-display", glyphless_char_display);
        let char_script_table =
            interp.make_char_table(Some("char-script-table".into()), Value::Nil);
        interp.set_global_binding("char-script-table", char_script_table);
        let auto_fill_chars = interp.make_char_table(Some("auto-fill-chars".into()), Value::Nil);
        if let Value::CharTable(table_id) = auto_fill_chars {
            interp
                .char_table_set(table_id, ' ' as u32, Value::T)
                .expect("initialize auto-fill-chars space entry");
            interp
                .char_table_set(table_id, '\n' as u32, Value::T)
                .expect("initialize auto-fill-chars newline entry");
            interp.set_global_binding("auto-fill-chars", Value::CharTable(table_id));
        }
        let char_width_table = interp.make_char_table(None, Value::Integer(1));
        if let Value::CharTable(table_id) = char_width_table {
            interp
                .char_table_set_range(table_id, 0x80, 0x9f, Value::Integer(4))
                .expect("initialize C1 character widths");
            interp.set_global_binding("char-width-table", Value::CharTable(table_id));
        }
        let ambiguous_width_chars = interp.make_char_table(None, Value::Nil);
        interp.set_global_binding("ambiguous-width-chars", ambiguous_width_chars);
        let printable_chars = interp.make_char_table(None, Value::Nil);
        if let Value::CharTable(table_id) = printable_chars {
            interp
                .char_table_set_range(table_id, 32, 126, Value::T)
                .expect("initialize ASCII printable characters");
            interp
                .char_table_set_range(table_id, 160, 0x3f_ffff, Value::T)
                .expect("initialize multibyte printable characters");
            interp.set_global_binding("printable-chars", Value::CharTable(table_id));
        }
        interp.set_global_binding("script-representative-chars", Value::Nil);
        interp.set_global_binding("unicode-category-table", Value::Nil);
        interp.set_global_binding("auto-composition-function", Value::Nil);
        let composition_function_table = interp.make_char_table(None, Value::Nil);
        interp.set_global_binding("composition-function-table", composition_function_table);
        interp.set_global_binding("auto-composition-emoji-eligible-codepoints", Value::Nil);
        for name in [
            "auto-fill-chars",
            "char-width-table",
            "ambiguous-width-chars",
            "printable-chars",
            "char-script-table",
            "script-representative-chars",
            "unicode-category-table",
            "auto-composition-function",
            "composition-function-table",
            "auto-composition-emoji-eligible-codepoints",
        ] {
            interp.mark_special_variable(name);
        }
        interp.set_global_binding("buffer-read-only", Value::Nil);
        interp.set_global_binding("dump-mode", Value::Nil);
        interp.mark_special_variable("dump-mode");
        interp.set_global_binding("charset-map-path", Value::Nil);
        interp.set_global_binding("inhibit-load-charset-map", Value::Nil);
        interp.set_global_binding("current-iso639-language", Value::Nil);
        for name in [
            "charset-map-path",
            "inhibit-load-charset-map",
            "charset-list",
            "current-iso639-language",
        ] {
            interp.mark_special_variable(name);
        }
        // GNU defines this C variable as both special and automatically
        // buffer-local.  A dynamic binding therefore belongs to the buffer
        // where it was made and must not make a newly selected buffer read-only.
        interp.mark_always_buffer_local_special("buffer-read-only");
        for (name, value) in [
            ("delete-auto-save-files", Value::T),
            ("kill-buffer-delete-auto-save-files", Value::Nil),
        ] {
            interp.set_global_binding(name, value);
            interp.mark_special_variable(name);
        }
        interp.set_global_binding("read-only-mode", Value::Nil);
        interp.mark_auto_buffer_local("read-only-mode");
        // GNU's preloaded `(declare (indent N))' effects: every symbol
        // carrying a `lisp-indent-function' property at oracle startup
        // (None encodes the symbol `defun').  calculate-lisp-indent and
        // lisp-indent-function consult these for special-form indentation.
        const PRELOADED_LISP_INDENT: &[(&str, Option<i64>)] = &[
            ("and-let*", Some(1)),
            ("atomic-change-group", Some(0)),
            ("autoload", None),
            ("benchmark-progn", Some(0)),
            ("benchmark-run", Some(1)),
            ("benchmark-run-compiled", Some(1)),
            ("bindings--define-key", Some(2)),
            ("catch", Some(1)),
            ("cl--define-built-in-type", Some(2)),
            ("cl-defgeneric", Some(2)),
            ("cl-defmethod", None),
            ("cl-generic-define-context-rewriter", None),
            ("cl-generic-define-generalizer", Some(1)),
            ("combine-after-change-calls", Some(0)),
            ("combine-change-calls", Some(2)),
            ("comment-with-narrowing", Some(2)),
            ("condition-case", Some(2)),
            ("condition-case-unless-debug", Some(2)),
            ("def-edebug-elem-spec", Some(1)),
            ("def-edebug-spec", Some(1)),
            ("defadvice", Some(2)),
            ("defalias", None),
            ("defconst", None),
            ("defcustom", None),
            ("defface", None),
            ("defgroup", None),
            ("defimage", None),
            ("define-abbrev", None),
            ("define-abbrev-table", None),
            ("define-advice", Some(2)),
            ("define-alternatives", None),
            ("define-auto-insert", None),
            ("define-button-type", None),
            ("define-category", None),
            ("define-ccl-program", None),
            ("define-char-code-property", None),
            ("define-charset", None),
            ("define-charset-internal", None),
            ("define-coding-system", None),
            ("define-derived-mode", None),
            ("define-fringe-bitmap", None),
            ("define-generic-mode", Some(1)),
            ("define-globalized-minor-mode", None),
            ("define-ibuffer-column", None),
            ("define-ibuffer-filter", Some(2)),
            ("define-ibuffer-op", Some(2)),
            ("define-ibuffer-sorter", Some(1)),
            ("define-inline", None),
            ("define-iso-single-byte-charset", None),
            ("define-key-after", None),
            ("define-keymap", None),
            ("define-mail-user-agent", None),
            ("define-minor-mode", None),
            ("define-multisession-variable", None),
            ("define-obsolete-function-alias", None),
            ("define-obsolete-variable-alias", None),
            ("define-short-documentation-group", None),
            ("define-skeleton", None),
            ("define-translation-hash-table", None),
            ("define-translation-table", None),
            ("define-widget", None),
            ("define-widget-keywords", None),
            ("defmacro", Some(2)),
            ("defmath", None),
            ("defsubst", Some(2)),
            ("deftheme", Some(1)),
            ("defun", Some(2)),
            ("defvar", None),
            ("defvar-keymap", Some(1)),
            ("defvar-local", Some(2)),
            ("defvaralias", None),
            ("delay-mode-hooks", Some(0)),
            ("dlet", Some(1)),
            ("dolist", Some(1)),
            ("dolist-with-progress-reporter", Some(2)),
            ("dont-compile", Some(0)),
            ("dotimes", Some(1)),
            ("dotimes-with-progress-reporter", Some(2)),
            ("easy-menu-define", None),
            ("easy-mmode-defmap", Some(1)),
            ("easy-mmode-defsyntax", Some(1)),
            ("eldoc--documentation-strategy-defcustom", Some(2)),
            ("ert-deftest", Some(2)),
            ("ert-font-lock-deftest", Some(1)),
            ("ert-font-lock-deftest-file", Some(1)),
            ("eval-after-load", Some(1)),
            ("eval-and-compile", Some(0)),
            ("eval-when-compile", Some(0)),
            ("gv-define-expander", Some(1)),
            ("gv-define-setter", Some(2)),
            ("gv-letplace", Some(2)),
            ("handler-bind", Some(1)),
            ("handler-case", Some(1)),
            ("if", Some(2)),
            ("if-let", Some(2)),
            ("if-let*", Some(2)),
            ("ignore-error", Some(1)),
            ("ignore-errors", Some(0)),
            ("isearch-define-mode-toggle", None),
            ("keymap-set-after", None),
            ("lambda", None),
            ("let", Some(1)),
            ("let*", Some(1)),
            ("let-alist", Some(1)),
            ("let-when-compile", Some(1)),
            ("letrec", Some(1)),
            ("macroexp--accumulate", Some(1)),
            ("macroexp--with-extended-form-stack", Some(1)),
            ("macroexp-let2", Some(3)),
            ("macroexp-let2*", Some(2)),
            ("minibuffer-with-setup-hook", Some(1)),
            ("named-let", Some(2)),
            ("oclosure--lambda", Some(3)),
            ("oclosure-define", Some(1)),
            ("oclosure-lambda", Some(2)),
            ("pcase", Some(1)),
            ("pcase-defmacro", Some(2)),
            ("pcase-dolist", Some(1)),
            ("pcase-exhaustive", Some(1)),
            ("pcase-lambda", None),
            ("pcase-let", Some(1)),
            ("pcase-let*", Some(1)),
            ("prog1", Some(1)),
            ("prog2", Some(2)),
            ("progn", Some(0)),
            ("replace--push-stack", Some(0)),
            ("rx-define", None),
            ("rx-let", Some(1)),
            ("rx-let-eval", Some(1)),
            ("save-current-buffer", Some(0)),
            ("save-excursion", Some(0)),
            ("save-mark-and-excursion", Some(0)),
            ("save-match-data", Some(0)),
            ("save-restriction", Some(0)),
            ("save-selected-window", Some(0)),
            ("save-window-excursion", Some(0)),
            ("seq-doseq", Some(1)),
            ("seq-let", Some(2)),
            ("static-if", Some(2)),
            ("track-mouse", Some(0)),
            ("transient-append-suffix", None),
            ("transient-insert-suffix", None),
            ("transient-remove-suffix", None),
            ("transient-replace-suffix", None),
            ("unless", Some(1)),
            ("unwind-protect", Some(1)),
            ("use-package", None),
            ("wallpaper-setter-create", Some(1)),
            ("when", Some(1)),
            ("when-let", Some(1)),
            ("when-let*", Some(1)),
            ("which-key-add-keymap-based-replacements", None),
            ("which-key-add-major-mode-key-based-replacements", None),
            ("while", Some(1)),
            ("while-let", Some(1)),
            ("while-no-input", Some(0)),
            ("with-auto-compression-mode", Some(0)),
            ("with-case-table", Some(1)),
            ("with-category-table", Some(1)),
            ("with-coding-priority", Some(1)),
            ("with-connection-local-application-variables", Some(1)),
            ("with-current-buffer", Some(1)),
            ("with-current-buffer-window", Some(3)),
            ("with-delayed-message", Some(1)),
            ("with-demoted-errors", Some(1)),
            ("with-displayed-buffer-window", Some(3)),
            ("with-environment-variables", Some(1)),
            ("with-eval-after-load", Some(1)),
            ("with-existing-directory", Some(0)),
            ("with-file-modes", Some(1)),
            ("with-help-window", Some(1)),
            ("with-local-quit", Some(0)),
            ("with-locale-environment", Some(1)),
            ("with-memoization", Some(1)),
            ("with-minibuffer-completions-window", Some(0)),
            ("with-minibuffer-selected-window", Some(0)),
            ("with-mutex", Some(1)),
            ("with-no-warnings", Some(0)),
            ("with-output-to-string", Some(0)),
            ("with-output-to-temp-buffer", Some(1)),
            ("with-restriction", Some(2)),
            ("with-selected-frame", Some(1)),
            ("with-selected-window", Some(1)),
            ("with-silent-modifications", Some(0)),
            ("with-suppressed-warnings", Some(1)),
            ("with-syntax-table", Some(1)),
            ("with-temp-buffer", Some(0)),
            ("with-temp-buffer-window", Some(3)),
            ("with-temp-file", Some(1)),
            ("with-temp-message", Some(1)),
            ("with-timeout", Some(1)),
            ("with-undo-amalgamate", Some(0)),
            ("with-window-non-dedicated", Some(1)),
            ("with-wrapper-hook", Some(2)),
            ("without-remote-files", Some(0)),
            ("without-restriction", Some(0)),
        ];
        for (name, method) in PRELOADED_LISP_INDENT {
            let value = match method {
                Some(count) => Value::Integer(*count),
                None => Value::Symbol("defun".into()),
            };
            interp.put_symbol_property(name, "lisp-indent-function", value);
        }
        interp.put_symbol_property("default-directory", "permanent-local", Value::T);
        // GNU declares default-directory with DEFVAR_PER_BUFFER: setting it
        // only ever affects the current buffer, so a dired/find-file in one
        // buffer must not redirect unrelated buffers (and later `ls' spawns)
        // into a directory that gets deleted by test cleanup.
        // Special (dynamically scoped) like every DEFVAR_PER_BUFFER
        // variable: `let' must go through the special-binding machinery,
        // which records the binding buffer, so a setq from another buffer
        // creates that buffer's own local instead of mutating the binding.
        interp.mark_always_buffer_local_special("default-directory");
        // buffer.c exposes these native Buffer fields through
        // DEFVAR_PER_BUFFER.  They are therefore dynamic specials as well as
        // automatically buffer-local: a lexical caller may bind a file name
        // for separately defined code (bookmark.el does), but that binding
        // must not become the file name of a newly selected buffer.
        for name in ["buffer-file-name", "buffer-file-truename"] {
            interp.mark_always_buffer_local_special(name);
        }
        for name in ["buffer-auto-save-file-name", "selective-display"] {
            interp.set_global_binding(name, Value::Nil);
        }
        // emacs.c defines this host flag with DEFVAR_BOOL.  Batch tests may
        // dynamically bind it around separately defined interactive code
        // (Viper does); lexical isolation must not hide that binding.
        interp.mark_special_variable("noninteractive");
        interp.mark_special_variable("delete-terminal-functions");
        // dispnew.c publishes this DEFVAR_INT before isearch.el is dumped.
        // Batch terminals have no output baud rate, represented by zero.
        interp.set_global_binding("baud-rate", Value::Integer(0));
        interp.mark_special_variable("baud-rate");
        // keyboard.c installs `list' as the pass-through input method before
        // dumped Lisp loads.  Isearch saves and buffer-locally suppresses it.
        interp.set_global_binding("input-method-function", Value::Symbol("list".into()));
        interp.mark_special_variable("input-method-function");
        // GNU keeps this dynamically scoped variable globally bound to nil;
        // loading a lexical file binds it to t only for that load.
        interp.set_global_binding("lexical-binding", Value::Nil);
        interp.mark_always_buffer_local_special("lexical-binding");
        // GNU preloads files.el, where this defcustom is globally bound.
        // abbrev.el consumes it without requiring files.el itself.
        interp.set_global_binding("save-abbrevs", Value::T);
        interp.mark_special_variable("save-abbrevs");
        // callproc.c defines both as DEFVAR_LISP variables before dumped
        // Lisp is loaded.  Keep their host-computed defaults in bindings.rs,
        // but record the special declaration here so lexical callers can
        // dynamically override the shell used by separately defined code.
        interp.mark_special_variable("shell-file-name");
        interp.mark_special_variable("shell-command-switch");
        interp.mark_special_variable("exec-path");
        // doc.c defines this as a native Lisp variable.  Help's quoting
        // policy calls the C accessor from separately defined Lisp, so a
        // lexical caller's `let' must establish a dynamic binding.
        interp.mark_special_variable("text-quoting-style");
        // simple.el is dumped by GNU.  Gnus, Ibuffer, Dired, and Tramp read
        // this shell-command state without requiring simple.el, so keep the
        // adjacent public defaults together instead of discovering them one
        // void variable at a time.
        for (name, value) in [
            (
                "shell-command-buffer-name",
                Value::String("*Shell Command Output*".into()),
            ),
            (
                "shell-command-buffer-name-async",
                Value::String("*Async Shell Command*".into()),
            ),
            ("shell-command-history", Value::Nil),
            ("shell-command-default-error-buffer", Value::Nil),
            (
                "async-shell-command-buffer",
                Value::Symbol("confirm-new-buffer".into()),
            ),
            ("async-shell-command-display-buffer", Value::T),
            ("async-shell-command-width", Value::Nil),
            ("shell-command-prompt-show-cwd", Value::Nil),
            ("shell-command-dont-erase-buffer", Value::Nil),
            ("shell-command-saved-pos", Value::Nil),
        ] {
            interp.set_global_binding(name, value);
            interp.mark_special_variable(name);
        }
        // subr.el's prompt policy is let-bound by callers and consumed by
        // separately defined save commands.
        interp.set_global_binding("use-dialog-box", Value::Nil);
        interp.mark_special_variable("use-dialog-box");
        // fileio.c exposes this as a dynamically scoped DEFVAR_LISP.  Temp
        // helpers are defined separately and must observe callers' let-bindings.
        interp.mark_special_variable("temporary-file-directory");
        // editfns.c defines this before paragraphs.el is dumped.  Paragraph
        // and line motion bind it around calls into separately defined
        // functions, so a lexical binding here would silently leave field
        // constraints enabled (most visibly at non-sticky shell prompts).
        interp.set_global_binding("inhibit-field-text-motion", Value::Nil);
        interp.mark_special_variable("inhibit-field-text-motion");
        // GNU print.c installs these primitive DEFVARs before Lisp startup.
        // Keep the declaration and default together: printer helpers such as
        // cl-print dynamically bind the limits in one function and expect the
        // primitive printer called through another function to observe them.
        for (name, value) in [
            ("standard-output", Value::T),
            ("print-circle", Value::Nil),
            ("print-continuous-numbering", Value::Nil),
            ("print-gensym", Value::Nil),
            ("print-integers-as-characters", Value::Nil),
            ("print-length", Value::Nil),
            ("print-level", Value::Nil),
            ("print-quoted", Value::T),
        ] {
            interp.set_global_binding(name, value);
            interp.mark_special_variable(name);
        }
        interp.set_global_binding("char-property-alias-alist", Value::Nil);
        interp.mark_special_variable("char-property-alias-alist");
        // syntax.c exposes both scanner switches as primitive DEFVAR_BOOLs.
        // They must be special so a caller's lexical `let' remains visible
        // through separately defined Lisp helpers such as `syntax-after'.
        for name in ["parse-sexp-ignore-comments", "parse-sexp-lookup-properties"] {
            interp.set_global_binding(name, Value::Nil);
            interp.mark_special_variable(name);
        }
        // GNU textprop.c supplies syntax-table/display, and the dumped Lisp
        // image adds composition/fill-space.  `insert-and-inherit' consults
        // this process-wide special when deciding which adjacent properties
        // may propagate onto newly inserted text.
        interp.set_global_binding(
            "text-property-default-nonsticky",
            Value::list([
                Value::cons(Value::Symbol("fill-space".into()), Value::T),
                Value::cons(Value::Symbol("composition".into()), Value::T),
                Value::cons(Value::Symbol("syntax-table".into()), Value::T),
                Value::cons(Value::Symbol("display".into()), Value::T),
            ]),
        );
        interp.mark_special_variable("text-property-default-nonsticky");
        // files.el is dumped in GNU, so these `defvar-local' contracts must
        // exist before user/test code can set them and thereby trigger the
        // lazy files.el load.  Otherwise the first assignment leaks globally
        // and changes the save policy of every later buffer.
        for name in [
            "write-file-functions",
            "local-write-file-hooks",
            "write-contents-functions",
        ] {
            interp.set_global_binding(name, Value::Nil);
            interp.mark_per_buffer_special(name);
            interp.put_symbol_property(name, "permanent-local", Value::T);
        }
        interp.set_global_binding("buffer-save-without-query", Value::Nil);
        interp.mark_per_buffer_special("buffer-save-without-query");
        for (name, value) in [
            ("save-some-buffers-default-predicate", Value::Nil),
            ("save-some-buffers-functions", Value::Nil),
            ("kill-emacs-query-functions", Value::Nil),
            ("confirm-kill-emacs", Value::Nil),
            ("confirm-kill-processes", Value::T),
        ] {
            interp.set_global_binding(name, value);
            interp.mark_special_variable(name);
        }
        interp.set_global_binding("require-final-newline", Value::Nil);
        interp.mark_special_variable("require-final-newline");
        // files.el defines this as nil and then calls
        // `make-variable-buffer-local'.  Merely carrying the property is not
        // enough: otherwise setting it in one buffer changes every buffer's
        // save policy.
        interp.set_global_binding("buffer-offer-save", Value::Nil);
        interp.mark_per_buffer_special("buffer-offer-save");
        interp.put_symbol_property("buffer-offer-save", "permanent-local", Value::T);
        interp.put_symbol_property("backup-inhibited", "permanent-local", Value::T);
        // mule.el is dumped before files.el.  Save/revert code reads this
        // automatically buffer-local coding choice directly.
        interp.set_global_binding("buffer-file-coding-system-explicit", Value::Nil);
        interp.mark_per_buffer_special("buffer-file-coding-system-explicit");
        interp.put_symbol_property(
            "buffer-file-coding-system-explicit",
            "permanent-local",
            Value::T,
        );
        // GNU loadup preloads vc-hooks.el and uniquify.el before files.el.
        // files.el reads these bindings directly, without boundp guards.
        interp.set_global_binding("vc-mode", Value::Nil);
        interp.mark_per_buffer_special("vc-mode");
        interp.put_symbol_property("vc-mode", "permanent-local", Value::T);
        for (name, value) in [
            (
                "uniquify-buffer-name-style",
                Value::Symbol("post-forward-angle-brackets".into()),
            ),
            ("uniquify-separator", Value::Nil),
            ("uniquify-trailing-separator-p", Value::Nil),
        ] {
            interp.set_global_binding(name, value);
            interp.mark_special_variable(name);
        }
        // callproc.c publishes this complete host-program manifest before
        // any Lisp is loaded.  Gnus and the preloaded tag/VC libraries read
        // different members directly, and DEFVAR_LISP makes every member
        // dynamically scoped.  Their values live in `default_var_value'
        // (emacsclient has a compatibility-tree-aware default there).
        for name in [
            "ctags-program-name",
            "etags-program-name",
            "hexl-program-name",
            "emacsclient-program-name",
            "movemail-program-name",
            "ebrowse-program-name",
            "rcs2log-program-name",
        ] {
            interp.mark_special_variable(name);
        }
        // Native minibuffer variables likewise exist before Lisp is loaded
        // and are consumed by preloaded prompt helpers.
        interp.set_global_binding(
            "minibuffer-prompt-properties",
            Value::list([Value::Symbol("read-only".into()), Value::T]),
        );
        interp.mark_special_variable("minibuffer-prompt-properties");
        interp.set_global_binding("minibuffer-auto-raise", Value::Nil);
        interp.mark_special_variable("minibuffer-auto-raise");
        // keyboard.c defines this before minibuffer.el.  Completion callers
        // dynamically shorten it, so both the native default and special
        // binding contract must exist before their lexical code is loaded.
        interp.set_global_binding("minibuffer-message-timeout", Value::Integer(2));
        interp.mark_special_variable("minibuffer-message-timeout");
        // minibuffer.el preloads these dispatch hooks.  Callers dynamically
        // override them around a separately defined reader (ERT does this to
        // make prompts deterministic), so lexical fallback bindings are not
        // sufficient.
        for name in ["read-buffer-function", "read-file-name-function"] {
            interp.set_global_binding(name, Value::Nil);
            interp.mark_special_variable(name);
        }
        interp.set_global_binding(
            "exec-directory",
            Value::String(
                primitives::current_invocation_directory()
                    .unwrap_or_else(primitives::default_directory),
            ),
        );
        interp.mark_special_variable("exec-directory");
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
        // callint.c DEFVAR_KBOARD/DEFVAR_LISP variables.  Command helpers
        // dynamically bind these around calls into separately defined
        // functions, so lexical code must still observe the active prefix.
        interp.set_global_binding("prefix-arg", Value::Nil);
        interp.set_global_binding("last-prefix-arg", Value::Nil);
        interp.set_global_binding("current-prefix-arg", Value::Nil);
        // keyboard.c exposes command-loop state through DEFVAR_LISP and
        // DEFVAR_KBOARD.  Those C definitions are special declarations just
        // like Lisp `defvar': a lexical `let' around a call must be visible
        // inside the separately defined callee.  Keep this as one coherent
        // group so new command clients do not each need a compatibility shim.
        for name in [
            "last-command",
            "real-last-command",
            "last-repeatable-command",
            "this-command",
            "real-this-command",
            "current-minibuffer-command",
            "this-command-keys-shift-translated",
            "this-original-command",
        ] {
            interp.set_global_binding(name, Value::Nil);
            interp.mark_special_variable(name);
        }
        // keyboard.c's integer command-loop counters are ordinary special
        // variables at the Lisp boundary.  Keyboard-macro playback updates
        // the active dynamic binding once for every complete key sequence.
        for name in ["num-input-keys", "num-nonmacro-input-events"] {
            interp.set_global_binding(name, Value::Integer(0));
            interp.mark_special_variable(name);
        }
        // eval.c defines the debugger controls before loading dumped Lisp.
        // Their special declarations are part of the evaluator boundary:
        // ERT, Edebug, and command-loop code let-bind `debugger' or its
        // policy in one lexical function and expect separately defined error
        // handlers to observe the active binding.  These are the dumped
        // batch defaults, after debug.el has replaced `debug-early'.
        for (name, value) in [
            ("debugger", Value::Symbol("debug".into())),
            ("debug-on-error", Value::Nil),
            ("debug-on-quit", Value::Nil),
            ("debug-on-signal", Value::Nil),
            ("debugger-may-continue", Value::T),
            ("debug-on-next-call", Value::Nil),
            ("backtrace-on-error-noninteractive", Value::T),
        ] {
            interp.set_global_binding(name, value);
            interp.mark_special_variable(name);
        }
        // minibuf.c plus the dumped minibuffer.el provide the completion
        // variables consumed by the native completion engine.  They are not
        // mere fallback constants: each DEFVAR/defcustom also declares the
        // name special, so callers can let-bind policy around a completion
        // function defined elsewhere (Completion Preview does exactly this).
        interp.set_global_binding("completion-ignore-case", Value::Nil);
        interp.set_global_binding("completion-regexp-list", Value::Nil);
        interp.set_global_binding("completion-auto-help", Value::T);
        interp.set_global_binding("completion-extra-properties", Value::Nil);
        interp.set_global_binding("enable-recursive-minibuffers", Value::Nil);
        // minibuf.c also publishes the per-read completion session before
        // minibuffer.el is dumped.  Completion-in-region users (including
        // Eshell) legitimately call the dumped helpers outside an active
        // minibuffer, where these variables remain bound to nil.
        for name in [
            "minibuffer-completion-table",
            "minibuffer-completion-predicate",
            "minibuffer-completion-confirm",
            "minibuffer-help-form",
            "minibuffer-history-position",
            "minibuffer-allow-text-properties",
        ] {
            interp.set_global_binding(name, Value::Nil);
        }
        interp.set_global_binding("minibuffer-history-variable", Value::Integer(0));
        for name in [
            "completion-ignore-case",
            "completion-regexp-list",
            "completion-auto-help",
            "completion-extra-properties",
            "completion-styles",
            "completion-styles-alist",
            "enable-recursive-minibuffers",
            "minibuffer-completion-table",
            "minibuffer-completion-predicate",
            "minibuffer-completion-confirm",
            "minibuffer-help-form",
            "minibuffer-history-variable",
            "minibuffer-history-position",
            "minibuffer-allow-text-properties",
        ] {
            interp.mark_special_variable(name);
        }
        // callproc.c/lread.c publish the installation-directory values as C
        // DEFVARs.  Tests and startup helpers deliberately let-bind these
        // around calls into preloaded functions (for example, a `t' entry in
        // `custom-theme-load-path' expands relative to `data-directory').
        for name in [
            "source-directory",
            "data-directory",
            "doc-directory",
            "internal-doc-file-name",
            "configure-info-directory",
        ] {
            interp.mark_special_variable(name);
        }
        // GNU loadup.el preloads eldoc.el before the dumped image is used.
        // descr-text.el intentionally consumes this option without requiring
        // ElDoc itself, so preserve both the dumped default and defcustom's
        // special declaration at the runtime boundary.
        interp.set_global_binding(
            "eldoc-echo-area-use-multiline-p",
            Value::Symbol("truncate-sym-name-if-fit".into()),
        );
        interp.mark_special_variable("eldoc-echo-area-use-multiline-p");
        interp.set_global_binding("tab-bar-new-tab-choice", Value::T);
        interp.set_global_binding("max-lisp-eval-depth", Value::Integer(1600));
        interp.put_symbol_property(
            "tab-bar-new-tab-choice",
            "custom-type",
            tab_bar_new_tab_choice_custom_type(),
        );
        interp.set_global_binding("search-upper-case", Value::Symbol("not-yanks".into()));
        // search.c primitive DEFVARs.  Search helpers bind these around
        // calls into separately defined code, so both are dynamic specials.
        interp.set_global_binding("search-spaces-regexp", Value::Nil);
        interp.mark_special_variable("search-spaces-regexp");
        interp.set_global_binding("inhibit-changing-match-data", Value::Nil);
        interp.mark_special_variable("inhibit-changing-match-data");
        interp.set_global_binding("search-whitespace-regexp", Value::String("[ \t]+".into()));
        // GNU preloads window.el, whose `defcustom' both initializes this
        // user action table and declares it special.  Buffer-display policy
        // is commonly let-bound in a lexical caller and consumed by a
        // separately defined display function (ERC does exactly this), so a
        // merely lexical Emaxx binding silently loses the user action.
        interp.set_global_binding("display-buffer-alist", Value::Nil);
        interp.mark_special_variable("display-buffer-alist");
        // window.c establishes this complete variable family before dumped
        // window.el.  These are native dynamic variables, not optional Lisp
        // defaults: separately defined display and scrolling functions
        // routinely let-bind them across function boundaries.
        for name in [
            "temp-buffer-show-function",
            "minibuffer-completing-file-name",
            "minibuffer-scroll-window",
            "other-window-scroll-buffer",
            "other-window-scroll-default",
            "scroll-preserve-screen-position",
            "window-point-insertion-type",
            "window-buffer-change-functions",
            "window-size-change-functions",
            "window-selection-change-functions",
            "window-state-change-functions",
            "window-state-change-hook",
            "window-configuration-change-hook",
            "window-restore-killed-buffer-windows",
            "window-scroll-functions",
            "window-combination-resize",
            "window-resize-pixelwise",
            "fast-but-imprecise-scrolling",
        ] {
            interp.set_global_binding(name, Value::Nil);
            interp.mark_special_variable(name);
        }
        for name in ["mode-line-in-non-selected-windows", "auto-window-vscroll"] {
            interp.set_global_binding(name, Value::T);
            interp.mark_special_variable(name);
        }
        for (name, value) in [
            ("next-screen-context-lines", Value::Integer(2)),
            ("recenter-redisplay", Value::Symbol("tty".into())),
            (
                "window-combination-limit",
                Value::Symbol("window-size".into()),
            ),
            (
                "window-persistent-parameters",
                Value::list([Value::cons(Value::Symbol("clone-of".into()), Value::T)]),
            ),
        ] {
            interp.set_global_binding(name, value);
            interp.mark_special_variable(name);
        }
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
            primitives::window_record_slots(
                Some(interp.current_buffer_id),
                interp.buffer.point_min(),
                Value::Nil,
                (
                    interp.frame_width(),
                    interp.frame_height().saturating_sub(1).max(1),
                    0,
                    0,
                ),
            ),
        );
        interp.set_global_binding("emaxx-root-window", selected_window.clone());
        let Value::Record(selected_window_id) = selected_window else {
            unreachable!("window records use Value::Record");
        };
        interp.selected_window_id = selected_window_id;
        interp.old_selected_window_id = selected_window_id;
        if let Some(window) = interp.find_record_mut(selected_window_id) {
            window.slots[primitives::WINDOW_USE_TIME_SLOT] = Value::Integer(1);
        }
        let (minibuffer_buffer_id, _) = interp.create_buffer(" *Minibuf-0*");
        let minibuffer_window = interp.create_record(
            "window",
            primitives::window_record_slots(
                Some(minibuffer_buffer_id),
                1,
                Value::Symbol(primitives::MINIBUFFER_WINDOW_KIND.into()),
                (
                    interp.frame_width(),
                    1,
                    0,
                    interp.frame_height().saturating_sub(1).max(1),
                ),
            ),
        );
        interp.set_global_binding("emaxx-minibuffer-window", minibuffer_window);
        interp.set_global_binding("emaxx-minibuffer-selected-window", Value::Nil);
        interp
    }

    pub fn current_global_map_value(&self) -> Value {
        self.current_global_map
            .clone()
            .or_else(|| self.lookup_var("global-map", &Vec::new()))
            .unwrap_or(Value::Nil)
    }

    pub fn set_current_global_map_value(&mut self, keymap: Value) {
        self.current_global_map = Some(keymap);
    }

    pub fn set_load_path(&mut self, load_path: Vec<PathBuf>) {
        self.load_path = load_path;
    }

    pub(crate) fn configured_load_path(&self) -> &[PathBuf] {
        &self.load_path
    }

    pub(crate) fn push_lambda_capture_override(&mut self, capture: bool) {
        self.lambda_capture_overrides.push(capture);
        self.lambda_trim_overrides.push(false);
    }

    pub(crate) fn push_lambda_eval_context(&mut self, capture: bool, trim_context: bool) {
        self.lambda_capture_overrides.push(capture);
        self.lambda_trim_overrides.push(trim_context);
    }

    pub(crate) fn pop_lambda_capture_override(&mut self) {
        self.lambda_capture_overrides.pop();
        self.lambda_trim_overrides.pop();
    }

    pub(crate) fn lambda_capture_override(&self) -> Option<bool> {
        self.lambda_capture_overrides.last().copied()
    }

    pub(crate) fn lambda_trim_override(&self) -> bool {
        self.lambda_trim_overrides.last().copied().unwrap_or(false)
    }

    pub(crate) fn mark_closure_eval_context(&mut self, env: &SharedEnv, lexical: bool) {
        self.closure_eval_context_registrations =
            self.closure_eval_context_registrations.wrapping_add(1);
        if self.closure_eval_context_registrations.is_multiple_of(4096) {
            self.closure_eval_contexts
                .retain(|_, (owner, _)| owner.strong_count() > 0);
        }
        let identity = Rc::as_ptr(env) as usize;
        self.closure_eval_contexts
            .insert(identity, (Rc::downgrade(env), lexical));
    }

    pub(crate) fn mark_lexical_closure_env(&mut self, env: &SharedEnv) {
        self.mark_closure_eval_context(env, true);
    }

    pub(crate) fn closure_eval_context(&self, env: &SharedEnv) -> Option<bool> {
        let identity = Rc::as_ptr(env) as usize;
        let (owner, lexical) = self.closure_eval_contexts.get(&identity)?;
        owner
            .upgrade()
            .is_some_and(|owner| Rc::ptr_eq(&owner, env))
            .then_some(*lexical)
    }

    pub(crate) fn closure_env_is_lexical(&self, env: &SharedEnv) -> bool {
        self.closure_eval_context(env) == Some(true)
    }

    pub(crate) fn register_captured_lexical_frames(&mut self, closure_env: &SharedEnv) {
        let frame_ids = closure_env
            .borrow()
            .iter()
            .filter_map(|frame| Self::frame_identity(frame))
            .collect::<Vec<_>>();
        let owner = Rc::downgrade(closure_env);
        for frame_id in frame_ids {
            let owners = self.captured_lexical_frames.entry(frame_id).or_default();
            owners.retain(|weak| weak.strong_count() > 0);
            if !owners
                .iter()
                .any(|weak| weak.as_ptr() == Rc::as_ptr(closure_env))
            {
                owners.push(owner.clone());
            }
        }
    }

    pub(crate) fn record_lexical_cell_update_if_captured(
        &mut self,
        frame_id: i64,
        name: &str,
        value: &Value,
    ) {
        let already_shared = self.lexical_cell_updates.contains_key(&frame_id);
        let has_live_owner =
            self.captured_lexical_frames
                .get_mut(&frame_id)
                .is_some_and(|owners| {
                    owners.retain(|weak| weak.strong_count() > 0);
                    !owners.is_empty()
                });
        if !has_live_owner {
            self.captured_lexical_frames.remove(&frame_id);
        }
        if already_shared || has_live_owner {
            self.lexical_cell_updates
                .entry(frame_id)
                .or_default()
                .insert(name.to_string(), value.clone());
        }
    }

    fn refresh_captured_lexical_cells(&self, env: &mut Env) {
        for frame in env {
            let Some(updates) = Self::frame_identity(frame)
                .and_then(|frame_id| self.lexical_cell_updates.get(&frame_id))
            else {
                continue;
            };
            for (name, value) in frame {
                if let Some(updated) = updates.get(name) {
                    *value = updated.clone();
                }
            }
        }
    }

    /// Read a closure binding through the same shared-cell overlay used when
    /// the closure is invoked.  Captured environments are snapshots, while
    /// assignments made after capture live in `lexical_cell_updates' until
    /// the next invocation refreshes that snapshot.  Observers such as
    /// `equal' must not see a stale pre-assignment value in the meantime.
    pub(crate) fn effective_captured_binding(
        &self,
        closure_env: &SharedEnv,
        name: &str,
    ) -> Option<Value> {
        for frame in closure_env.borrow().iter().rev() {
            let shared_update = Self::frame_identity(frame)
                .and_then(|frame_id| self.lexical_cell_updates.get(&frame_id))
                .and_then(|updates| updates.get(name));
            if let Some((_, value)) = frame.iter().rev().find(|(bound, _)| bound == name) {
                return Some(shared_update.cloned().unwrap_or_else(|| value.clone()));
            }
        }
        None
    }

    pub(crate) fn eval_with_closure_env<F>(
        &mut self,
        closure_env: &SharedEnv,
        env: &mut Env,
        evaluate: F,
    ) -> Result<Value, LispError>
    where
        F: FnOnce(&mut Self, &mut Env) -> Result<Value, LispError>,
    {
        self.register_captured_lexical_frames(closure_env);
        let mut captured_snapshot = closure_env.borrow().clone();
        self.refresh_captured_lexical_cells(&mut captured_snapshot);
        if captured_snapshot.is_empty() {
            // Only lexical closures reach this path with an empty capture;
            // dynamic closures are handled directly on the caller chain.
            // Preserve the lexical scope boundary without manufacturing a
            // fake binding that leaks into instrumentation/capture analysis.
            return evaluate(self, &mut Vec::new());
        }

        if env_has_truthy_binding(env, "__closure-isolated-current-env") {
            let mut call_env = captured_snapshot.clone();
            let result = evaluate(self, &mut call_env);
            self.refresh_captured_lexical_cells(&mut call_env);
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
                }
            }
            return result;
        }

        self.refresh_captured_lexical_cells(env);
        let frame_mapping = Self::align_captured_frames(&captured_snapshot, env);
        let mut call_env = Self::merge_lexical_lambda_env(env, &captured_snapshot, &frame_mapping);
        let result = evaluate(self, &mut call_env);
        self.refresh_captured_lexical_cells(&mut call_env);
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
        self.refresh_captured_lexical_cells(env);
        // Lexical bindings are shared cells in GNU Emacs.  Two sibling
        // closures can capture different snapshots of the surrounding
        // environment (for example, consecutive `let*' initializers) while
        // still sharing the frames that already existed.  Propagate updates
        // by the frames' stable identity stamps so a mutation through one
        // closure is immediately visible through the other.
        let shared_frame_updates = call_env
            .iter()
            .take(captured_snapshot.len())
            .filter(|frame| Self::frame_identity(frame).is_some())
            .cloned()
            .collect::<Vec<_>>();
        self.sync_cached_closure_frames(&shared_frame_updates);
        result
    }

    fn sync_cached_closure_frames(&mut self, updates: &[Vec<(String, Value)>]) {
        if updates.is_empty() {
            return;
        }
        self.closure_capture_cache
            .retain(|(_, weak)| weak.strong_count() > 0);
        for (_, weak) in &self.closure_capture_cache {
            let Some(shared) = weak.upgrade() else {
                continue;
            };
            let mut captured = shared.borrow_mut();
            for frame in captured.iter_mut() {
                let Some(identity) = Self::frame_identity(frame) else {
                    continue;
                };
                if let Some(update) = updates
                    .iter()
                    .find(|candidate| Self::frame_identity(candidate) == Some(identity))
                {
                    *frame = update.clone();
                }
            }
        }
    }

    // Append echo-area output to the active `ert-with-message-capture'
    // scope, keeping a dynamically bound capture variable current.
    pub fn append_message_capture(&mut self, text: &str, newline: bool, env: &mut Env) {
        let live_update = if let Some(capture) = self.message_capture_stack.last_mut() {
            capture.text.push_str(text);
            if newline {
                capture.text.push('\n');
            }
            capture
                .live_var
                .clone()
                .map(|var| (var, capture.text.clone()))
        } else {
            None
        };
        if let Some((var, captured)) = live_update {
            self.set_variable(&var, Value::String(captured), env);
        }
    }

    // Capture a lambda's lexical environment, sharing the environment cell
    // with sibling closures from the same activation whose captured content
    // is identical.
    pub(crate) fn capture_closure_env(&mut self, mut captured: Env) -> SharedEnv {
        self.refresh_captured_lexical_cells(&mut captured);
        let activation = self.current_activation_id;
        self.closure_capture_cache
            .retain(|(_, weak)| weak.strong_count() > 0);
        let mut matching = None;
        for (id, weak) in self.closure_capture_cache.iter().rev() {
            if *id == activation
                && let Some(existing) = weak.upgrade()
                && bounded_env_eq(&existing.borrow(), &captured, &mut 4096)
            {
                matching = Some(existing);
                break;
            }
        }
        if let Some(existing) = matching {
            self.register_captured_lexical_frames(&existing);
            return existing;
        }
        let shared = shared_env(captured);
        self.closure_capture_cache
            .push((activation, Rc::downgrade(&shared)));
        if self.closure_capture_cache.len() > 128 {
            self.closure_capture_cache.remove(0);
        }
        self.register_captured_lexical_frames(&shared);
        shared
    }

    pub(crate) fn enter_activation(&mut self) -> u64 {
        let previous = self.current_activation_id;
        self.next_activation_id += 1;
        self.current_activation_id = self.next_activation_id;
        previous
    }

    pub(crate) fn leave_activation(&mut self, previous: u64) {
        self.current_activation_id = previous;
    }

    pub(crate) fn push_interactive_call(&mut self) {
        self.interactive_call_depth += 1;
    }

    pub(crate) fn pop_interactive_call(&mut self) {
        self.interactive_call_depth = self.interactive_call_depth.saturating_sub(1);
    }

    pub(crate) fn in_interactive_call(&self) -> bool {
        self.interactive_call_depth > 0
    }
}

// Environment comparison for closure-cell sharing.  Cyclic or very deep
// values must not recurse without bound; when the budget runs out the
// environments simply count as different and each closure keeps its own
// cell.
fn bounded_env_eq(left: &Env, right: &Env, budget: &mut usize) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter().zip(right.iter()).all(|(a, b)| {
        a.len() == b.len()
            && a.iter()
                .zip(b.iter())
                .all(|((an, av), (bn, bv))| an == bn && bounded_value_eq(av, bv, budget))
    })
}

fn bounded_value_eq(left: &Value, right: &Value, budget: &mut usize) -> bool {
    if *budget == 0 {
        return false;
    }
    *budget -= 1;
    match (left, right) {
        (Value::Cons(a1, a2), Value::Cons(b1, b2)) => {
            bounded_value_eq(&a1.borrow(), &b1.borrow(), budget)
                && bounded_value_eq(&a2.borrow(), &b2.borrow(), budget)
        }
        (Value::Lambda(ap, ab, ae), Value::Lambda(bp, bb, be)) => {
            ap == bp
                && Rc::ptr_eq(ae, be)
                && ab.len() == bb.len()
                && ab
                    .iter()
                    .zip(bb.iter())
                    .all(|(a, b)| bounded_value_eq(a, b, budget))
        }
        _ => left == right,
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

pub(crate) const OCLOSURE_TYPE_MARKER: &str = "--emaxx-oclosure-type";

#[derive(Clone, Debug, Default)]
pub(crate) struct AdviceState {
    pub(crate) entries: Vec<AdviceEntry>,
    pub(crate) base: Option<Value>,
}

#[derive(Clone, Debug)]
pub(crate) struct AdviceEntry {
    pub(crate) where_kind: String,
    pub(crate) function: Value,
    pub(crate) name: Option<Value>,
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

fn normalize_if_let_spec(spec: &Value) -> Result<Vec<Value>, LispError> {
    let items = spec.to_vec()?;
    let old_single_binding_syntax = !items.is_empty()
        && items.len() <= 2
        && !matches!(items[0], Value::Nil | Value::Cons(_, _));
    Ok(if old_single_binding_syntax {
        vec![spec.clone()]
    } else {
        items
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
        LispError::VoidFunction(symbol) => Value::list([
            Value::Symbol("void-function".into()),
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
        LispError::Terminate(_) => {
            Value::list([Value::Symbol("emaxx--process-termination".into())])
        }
        LispError::SignalValue(value) => value.clone(),
    }
}

fn buffer_undo_head_to_entry(value: &Value) -> crate::buffer::UndoEntry {
    match value {
        Value::Nil => crate::buffer::UndoEntry::Boundary,
        Value::Cons(_, _) => match value.cons_values() {
            // GNU records an insertion as (BEG . END).
            Some((Value::Integer(beg), Value::Integer(end))) if beg >= 0 && end >= beg => {
                crate::buffer::UndoEntry::Insert {
                    pos: beg as usize,
                    len: (end - beg) as usize,
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
        // GNU records an insertion as (BEG . END).
        crate::buffer::UndoEntry::Insert { pos, len } => Value::cons(
            Value::Integer(*pos as i64),
            Value::Integer((*pos + *len) as i64),
        ),
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
        Value::Frame(id) => format!("#<frame id:{id}>"),
        Value::Terminal(id) => format!("#<terminal id:{id}>"),
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
    if body.len().saturating_sub(start) > 1
        && matches!(
            body.get(start),
            Some(Value::Symbol(marker)) if marker == ":closure-dont-trim-context"
                || marker == ":closure-isolated-current-env"
                || marker == ":closure-transparent-env"
                || marker == ":closure-oclosure"
        )
    {
        start += 1;
    }
    &body[start..]
}

fn body_has_marker(body: &[Value], marker_name: &str) -> bool {
    let mut start = 0usize;
    if body.len() > 1
        && matches!(
            body.first(),
            Some(Value::String(_) | Value::StringObject(_))
        )
    {
        start = 1;
    }
    matches!(
        body.get(start),
        Some(Value::Symbol(marker)) if marker == marker_name
    ) && body.len().saturating_sub(start) > 1
}

fn env_has_truthy_binding(env: &Env, name: &str) -> bool {
    env.iter()
        .rev()
        .flat_map(|frame| frame.iter().rev())
        .find(|(binding_name, _)| binding_name == name)
        .is_some_and(|(_, value)| value.is_truthy())
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

fn function_declare_gv_setter_handler(form: &Value) -> Option<Value> {
    let items = form.to_vec().ok()?;
    if !matches!(items.first(), Some(Value::Symbol(name)) if name == "declare") {
        return None;
    }
    items[1..].iter().find_map(|declaration| {
        let declaration_items = declaration.to_vec().ok()?;
        match declaration_items.as_slice() {
            [Value::Symbol(kind), handler @ Value::Cons(_, _)] if kind == "gv-setter" => {
                Some(handler.clone())
            }
            _ => None,
        }
    })
}

pub(crate) fn function_declare_gv_expander(form: &Value) -> Option<Value> {
    let items = form.to_vec().ok()?;
    if !matches!(items.first(), Some(Value::Symbol(name)) if name == "declare") {
        return None;
    }
    items[1..].iter().find_map(|declaration| {
        let declaration_items = declaration.to_vec().ok()?;
        match declaration_items.as_slice() {
            [Value::Symbol(kind), handler] if kind == "gv-expander" => Some(handler.clone()),
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
        | Value::Frame(_)
        | Value::Terminal(_)
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
                || is_char_table_literal_reader_form(value)
                || is_record_literal_reader_form(value)
        }
        Value::Symbol(_) => false,
    }
}

fn is_record_literal_reader_form(value: &Value) -> bool {
    // Cheap car probe first: every list evaluation passes through here,
    // and to_vec would allocate for each one.
    let Value::Cons(car, _) = value else {
        return false;
    };
    if !matches!(&*car.borrow(), Value::Symbol(name) if name == RECORD_LITERAL_SYMBOL) {
        return false;
    }
    let Ok(items) = value.to_vec() else {
        return false;
    };
    items[1..].iter().all(is_record_literal_slot_form)
}

fn is_char_table_literal_reader_form(value: &Value) -> bool {
    let Value::Cons(car, _) = value else {
        return false;
    };
    matches!(&*car.borrow(), Value::Symbol(name) if name == CHAR_TABLE_LITERAL_SYMBOL)
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
        || is_char_table_literal_reader_form(value)
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

fn is_backquote_head(symbol: &str) -> bool {
    symbol == "backquote" || symbol == "`"
}

fn comma_head_kind(symbol: &str) -> Option<&'static str> {
    match symbol {
        "comma" | "," => Some("comma"),
        "comma-at" | ",@" => Some("comma-at"),
        _ => None,
    }
}

fn backquote_unquote_form(value: &Value) -> Option<(&'static str, Value)> {
    let items = value.to_vec().ok()?;
    match items.as_slice() {
        [Value::Symbol(symbol), value] => comma_head_kind(symbol).map(|kind| (kind, value.clone())),
        _ => None,
    }
}

fn nested_backquote_body(value: &Value) -> Option<Value> {
    let items = value.to_vec().ok()?;
    match items.as_slice() {
        [Value::Symbol(symbol), body] if is_backquote_head(symbol) => Some(body.clone()),
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
    raw_rest_param: Option<String>,
    remaining_args_name: Option<String>,
    optional_bindings: Vec<ClOptionalBinding>,
    rest_binding: Option<Value>,
    reject_remaining_args: bool,
    required_count: usize,
    keyword_bindings: Vec<ClKeyBinding>,
    // (PATTERN INIT) pairs from `&aux', bound sequentially after the
    // arguments; PATTERN may destructure.
    aux_bindings: Vec<(Value, Value)>,
}

struct ClOptionalBinding {
    pattern: Value,
    default_value: Value,
    supplied_name: Option<String>,
}

struct ClKeyBinding {
    variable_name: String,
    keyword_name: String,
    default_value: Value,
    supplied_name: Option<String>,
}

fn cl_keyword_name_for_variable(variable_name: &str) -> String {
    format!(":{}", variable_name.trim_start_matches('_'))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ClDefunSection {
    Required,
    Optional,
    RestName,
    AfterRest,
    Key,
    Aux,
}

fn lower_cl_defun_lambda_list(name: &str, spec: &Value) -> Result<LoweredClDefun, LispError> {
    let items = match spec {
        Value::Nil => Vec::new(),
        Value::Cons(_, _) => spec.to_vec()?,
        _ => return Err(invalid_function(spec.clone())),
    };

    let mut lowered = Vec::with_capacity(items.len());
    let mut destructuring_bindings = Vec::new();
    let mut optional_bindings = Vec::new();
    let mut rest_binding = None;
    let mut keyword_bindings = Vec::new();
    let mut aux_bindings = Vec::new();
    let mut section = ClDefunSection::Required;
    let mut saw_key = false;
    let mut required_count = 0;

    for (index, item) in items.into_iter().enumerate() {
        if let Value::Symbol(symbol) = &item {
            match symbol.as_str() {
                "&optional" => {
                    if section != ClDefunSection::Required {
                        return Err(invalid_function(spec.clone()));
                    }
                    section = ClDefunSection::Optional;
                    continue;
                }
                "&rest" | "&body" => {
                    if !matches!(section, ClDefunSection::Required | ClDefunSection::Optional) {
                        return Err(invalid_function(spec.clone()));
                    }
                    section = ClDefunSection::RestName;
                    continue;
                }
                "&key" => {
                    if !matches!(
                        section,
                        ClDefunSection::Required
                            | ClDefunSection::Optional
                            | ClDefunSection::AfterRest
                    ) {
                        return Err(invalid_function(spec.clone()));
                    }
                    section = ClDefunSection::Key;
                    saw_key = true;
                    continue;
                }
                "&allow-other-keys" if section == ClDefunSection::Key => continue,
                "&aux" => {
                    if section == ClDefunSection::RestName {
                        return Err(invalid_function(spec.clone()));
                    }
                    section = ClDefunSection::Aux;
                    continue;
                }
                "&whole" | "&environment" => {
                    return Err(LispError::Signal(format!(
                        "Unsupported cl-defun lambda list keyword: {symbol}"
                    )));
                }
                _ => {}
            }
        }

        match section {
            ClDefunSection::Required => {
                required_count += 1;
                if let Value::Symbol(symbol) = item {
                    lowered.push(Value::Symbol(symbol));
                } else if matches!(item, Value::Cons(_, _)) {
                    let temp_name = format!("emaxx--cl-defun-{name}-arg-{index}");
                    lowered.push(Value::Symbol(temp_name.clone()));
                    destructuring_bindings.push((item, temp_name));
                } else {
                    return Err(invalid_function(spec.clone()));
                }
            }
            ClDefunSection::Optional => {
                optional_bindings.push(parse_cl_defun_optional_binding(item)?);
            }
            ClDefunSection::RestName => {
                if !matches!(item, Value::Symbol(_) | Value::Cons(_, _)) {
                    return Err(invalid_function(spec.clone()));
                }
                rest_binding = Some(item);
                section = ClDefunSection::AfterRest;
            }
            ClDefunSection::AfterRest => return Err(invalid_function(spec.clone())),
            ClDefunSection::Key => match item {
                Value::Symbol(symbol) => keyword_bindings.push(ClKeyBinding {
                    variable_name: symbol.clone(),
                    keyword_name: cl_keyword_name_for_variable(&symbol),
                    default_value: Value::Nil,
                    supplied_name: None,
                }),
                Value::Cons(_, _) => {
                    keyword_bindings.push(parse_cl_defun_key_binding(item)?);
                }
                _ => return Err(invalid_function(spec.clone())),
            },
            ClDefunSection::Aux => {
                if let Value::Symbol(symbol) = item {
                    aux_bindings.push((Value::Symbol(symbol), Value::Nil));
                    continue;
                }
                let parts = item.to_vec()?;
                let pattern = parts
                    .first()
                    .cloned()
                    .ok_or_else(|| invalid_function(spec.clone()))?;
                let init = parts.get(1).cloned().unwrap_or(Value::Nil);
                aux_bindings.push((pattern, init));
            }
        }
    }

    if section == ClDefunSection::RestName {
        return Err(invalid_function(spec.clone()));
    }

    let uses_rest_args = !optional_bindings.is_empty() || rest_binding.is_some() || saw_key;
    let reject_remaining_args = !optional_bindings.is_empty() && rest_binding.is_none() && !saw_key;
    let (raw_rest_param, remaining_args_name) = if uses_rest_args {
        let raw_name = format!("emaxx--cl-defun-{name}-raw-rest");
        let remaining_name = format!("emaxx--cl-defun-{name}-remaining");
        lowered.push(Value::Symbol("&rest".into()));
        lowered.push(Value::Symbol(raw_name.clone()));
        (Some(raw_name), Some(remaining_name))
    } else {
        (None, None)
    };

    Ok(LoweredClDefun {
        params: lowered,
        destructuring_bindings,
        raw_rest_param,
        remaining_args_name,
        optional_bindings,
        rest_binding,
        reject_remaining_args,
        required_count,
        keyword_bindings,
        aux_bindings,
    })
}

fn parse_cl_defun_optional_binding(spec: Value) -> Result<ClOptionalBinding, LispError> {
    let (pattern, default_value, supplied_name) = match spec {
        Value::Symbol(_) => (spec, Value::Nil, None),
        Value::Cons(_, _) => {
            let items = spec.to_vec()?;
            if items.is_empty() || items.len() > 3 {
                return Err(LispError::Signal(
                    "Unsupported cl-defun &optional binding".into(),
                ));
            }
            let pattern = items[0].clone();
            if !matches!(pattern, Value::Symbol(_) | Value::Cons(_, _)) {
                return Err(LispError::Signal(
                    "Unsupported cl-defun &optional binding".into(),
                ));
            }
            let default_value = items.get(1).cloned().unwrap_or(Value::Nil);
            let supplied_name = match items.get(2) {
                Some(Value::Symbol(name)) => Some(name.clone()),
                Some(_) => {
                    return Err(LispError::Signal(
                        "Unsupported cl-defun &optional binding".into(),
                    ));
                }
                None => None,
            };
            (pattern, default_value, supplied_name)
        }
        _ => {
            return Err(LispError::Signal(
                "Unsupported cl-defun &optional binding".into(),
            ));
        }
    };

    Ok(ClOptionalBinding {
        pattern,
        default_value,
        supplied_name,
    })
}

fn parse_cl_defun_key_binding(spec: Value) -> Result<ClKeyBinding, LispError> {
    if let Value::Symbol(variable_name) = spec {
        return Ok(ClKeyBinding {
            keyword_name: cl_keyword_name_for_variable(&variable_name),
            variable_name,
            default_value: Value::Nil,
            supplied_name: None,
        });
    }
    let items = spec.to_vec()?;
    if items.is_empty() {
        return Err(LispError::Signal(
            "Unsupported cl-defun &key binding".into(),
        ));
    }

    let (keyword_name, variable_name, default_value, supplied_name) = match items.as_slice() {
        [Value::Symbol(variable_name)] => (
            cl_keyword_name_for_variable(variable_name),
            variable_name.clone(),
            Value::Nil,
            None,
        ),
        [Value::Symbol(variable_name), default_value] => (
            cl_keyword_name_for_variable(variable_name),
            variable_name.clone(),
            default_value.clone(),
            None,
        ),
        [
            Value::Symbol(variable_name),
            default_value,
            Value::Symbol(supplied_name),
        ] => (
            cl_keyword_name_for_variable(variable_name),
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
                keyword_name.clone(),
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
                keyword_name.clone(),
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
                keyword_name.clone(),
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

fn is_lambda_list_keyword(symbol: &str) -> bool {
    matches!(
        symbol,
        "&optional" | "&rest" | "&body" | "&key" | "&allow-other-keys" | "&aux"
    )
}

fn cl_defmethod_destructuring_parameter_name(index: usize) -> String {
    format!("emaxx--cl-defmethod-arg-{index}")
}

struct LoweredClDefmethodLambdaList {
    value: Value,
    destructuring_bindings: Vec<(Value, String)>,
}

fn lower_cl_defmethod_lambda_list(spec: &Value) -> Result<LoweredClDefmethodLambdaList, LispError> {
    let items = spec.to_vec()?;
    let mut lowered = Vec::with_capacity(items.len());
    let mut destructuring_bindings = Vec::new();
    let mut skipping_context = false;
    let mut required = true;
    let mut expecting_rest_parameter = false;
    let mut rest_parameter = None;
    let mut key_pattern: Option<Vec<Value>> = None;

    for (index, item) in items.into_iter().enumerate() {
        if let Some(pattern) = &mut key_pattern {
            pattern.push(item);
            continue;
        }
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
                if symbol == "&key" {
                    required = false;
                    key_pattern = Some(vec![Value::Symbol(symbol)]);
                    continue;
                }
                if expecting_rest_parameter {
                    rest_parameter = Some(symbol.clone());
                    expecting_rest_parameter = false;
                } else if matches!(symbol.as_str(), "&rest" | "&body") {
                    expecting_rest_parameter = true;
                }
                if is_lambda_list_keyword(&symbol) {
                    required = false;
                }
                lowered.push(Value::Symbol(symbol));
            }
            Value::Cons(_, _) => {
                if skipping_context {
                    continue;
                }
                let parts = item.to_vec()?;
                if required
                    && let Some(pattern @ Value::Cons(_, _)) = parts.first()
                    && cl_defmethod_specializer_kind(parts.get(1)).is_some()
                {
                    let parameter = cl_defmethod_destructuring_parameter_name(index);
                    lowered.push(Value::Symbol(parameter.clone()));
                    destructuring_bindings.push((pattern.clone(), parameter));
                } else if let Some(Value::Symbol(variable_name)) = parts.first() {
                    lowered.push(Value::Symbol(variable_name.clone()));
                } else if required {
                    let parameter = cl_defmethod_destructuring_parameter_name(index);
                    lowered.push(Value::Symbol(parameter.clone()));
                    destructuring_bindings.push((item, parameter));
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

    if let Some(pattern) = key_pattern {
        let parameter = if let Some(parameter) = rest_parameter {
            parameter
        } else {
            let parameter = cl_defmethod_destructuring_parameter_name(lowered.len());
            lowered.push(Value::Symbol("&rest".into()));
            lowered.push(Value::Symbol(parameter.clone()));
            parameter
        };
        destructuring_bindings.push((Value::list(pattern), parameter));
    }

    Ok(LoweredClDefmethodLambdaList {
        value: Value::list(lowered),
        destructuring_bindings,
    })
}

fn lambda_list_fixed_params(params: &[String]) -> Vec<String> {
    let mut fixed = Vec::new();
    for param in params {
        if param == "&rest" || param == "&body" {
            break;
        }
        if !is_lambda_list_keyword(param) {
            fixed.push(param.clone());
        }
    }
    fixed
}

fn lambda_list_rest_param_from_params(params: &[String]) -> Option<String> {
    params.windows(2).find_map(|pair| match pair {
        [keyword, name] if keyword == "&rest" || keyword == "&body" => Some(name.clone()),
        _ => None,
    })
}

fn lambda_list_arity_range(params: &[String]) -> (usize, Option<usize>) {
    let mut required = 0;
    let mut maximum = 0;
    let mut optional = false;
    for param in params {
        match param.as_str() {
            "&optional" => optional = true,
            "&rest" | "&body" => return (required, None),
            keyword if is_lambda_list_keyword(keyword) => {}
            _ => {
                maximum += 1;
                if !optional {
                    required += 1;
                }
            }
        }
    }
    (required, Some(maximum))
}

fn cl_defmethod_dispatch_wrapper_params(
    spec: &Value,
    specializer_variable: &str,
    rest_param: &str,
) -> Result<Vec<String>, LispError> {
    let mut params = Vec::new();
    let mut previous_was_rest_keyword = false;
    let items = spec.to_vec()?;
    if items.is_empty() {
        return Ok(params);
    }
    for item in items {
        let name = item.as_symbol()?.to_string();
        params.push(name.clone());
        if name == specializer_variable {
            if previous_was_rest_keyword {
                return Ok(params);
            }
            params.push("&rest".into());
            params.push(rest_param.into());
            return Ok(params);
        }
        previous_was_rest_keyword = name == "&rest" || name == "&body";
    }
    Err(LispError::Signal(
        "cl-defmethod dispatch lost specializer variable".into(),
    ))
}

fn cl_defmethod_dispatch_stop_variable(
    dispatch_spec: &Value,
    specialized_variable_names: &[String],
    default_variable: &str,
) -> Result<String, LispError> {
    let specialized_variables = specialized_variable_names
        .iter()
        .map(|variable| variable.trim_start_matches('_').to_string())
        .collect::<HashSet<_>>();
    let mut stop_variable = default_variable.to_string();
    for item in dispatch_spec.to_vec()? {
        if let Ok(name) = item.as_symbol() {
            let argument_key = name.trim_start_matches('_');
            if specialized_variables.contains(argument_key) {
                stop_variable = name.to_string();
            }
        }
    }
    Ok(stop_variable)
}

#[derive(Clone)]
enum ClDefmethodSpecializerKind {
    Class(String),
    Subclass(String),
    Eql(Value),
    Head(Value),
}

#[derive(Clone)]
struct ClDefmethodSpecializer {
    variable: String,
    kind: ClDefmethodSpecializerKind,
    is_context: bool,
    /// For `&context (EXPR SPEC)` entries whose first element is an
    /// expression (context rewriters expand to these): the form the
    /// dispatch evaluates instead of reading `variable`.
    context_expr: Option<Value>,
}

impl ClDefmethodSpecializer {
    fn class_name(&self) -> Option<&str> {
        match &self.kind {
            ClDefmethodSpecializerKind::Class(class_name) => Some(class_name),
            ClDefmethodSpecializerKind::Subclass(_)
            | ClDefmethodSpecializerKind::Eql(_)
            | ClDefmethodSpecializerKind::Head(_) => None,
        }
    }

    fn metadata_value(&self) -> Value {
        match &self.kind {
            ClDefmethodSpecializerKind::Class(class_name) => Value::list([
                Value::Symbol("class".into()),
                Value::Symbol(class_name.clone()),
            ]),
            ClDefmethodSpecializerKind::Subclass(class_name) => Value::list([
                Value::Symbol("subclass".into()),
                Value::Symbol(class_name.clone()),
            ]),
            ClDefmethodSpecializerKind::Eql(value) => {
                Value::list([Value::Symbol("eql".into()), value.clone()])
            }
            ClDefmethodSpecializerKind::Head(value) => {
                Value::list([Value::Symbol("head".into()), value.clone()])
            }
        }
    }
}

/// Parse the SPEC half of a specializer entry ((ARG SPEC)) into a kind.
fn cl_defmethod_specializer_kind(spec: Option<&Value>) -> Option<ClDefmethodSpecializerKind> {
    match spec {
        Some(Value::Symbol(class_name)) => {
            Some(ClDefmethodSpecializerKind::Class(class_name.clone()))
        }
        Some(Value::T) => Some(ClDefmethodSpecializerKind::Class("t".into())),
        Some(compound @ Value::Cons(_, _)) => {
            let specializer = compound.to_vec().ok()?;
            match specializer.first() {
                Some(Value::Symbol(name)) if name == "eql" => {
                    Some(ClDefmethodSpecializerKind::Eql(specializer.get(1)?.clone()))
                }
                Some(Value::Symbol(name)) if name == "subclass" => match specializer.get(1) {
                    Some(Value::Symbol(class_name)) => {
                        Some(ClDefmethodSpecializerKind::Subclass(class_name.clone()))
                    }
                    _ => None,
                },
                Some(Value::Symbol(name)) if name == "head" => Some(
                    ClDefmethodSpecializerKind::Head(specializer.get(1)?.clone()),
                ),
                _ => None,
            }
        }
        _ => None,
    }
}

fn cl_defmethod_specializers(spec: &Value) -> Result<Vec<ClDefmethodSpecializer>, LispError> {
    let mut next_is_context = false;
    let mut specializers = Vec::new();
    for (index, item) in spec.to_vec()?.into_iter().enumerate() {
        if matches!(&item, Value::Symbol(symbol) if symbol == "&context") {
            next_is_context = true;
            continue;
        }
        let Value::Cons(_, _) = item else {
            continue;
        };
        let parts = item.to_vec()?;
        // &context (EXPR SPEC) where EXPR is an expression, not a variable
        // (context rewriters expand to this shape): dispatch evaluates EXPR.
        if next_is_context && let Some(expr @ Value::Cons(_, _)) = parts.first() {
            if let Some(kind) = cl_defmethod_specializer_kind(parts.get(1)) {
                // Two methods that differ only in their context EXPR are
                // distinct methods, so the variable naming (which feeds the
                // method identity key) must incorporate the expression.
                let fingerprint = expr
                    .to_string()
                    .chars()
                    .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                    .collect::<String>();
                specializers.push(ClDefmethodSpecializer {
                    variable: format!("--cl-context-{}-{fingerprint}", specializers.len()),
                    kind,
                    is_context: true,
                    context_expr: Some(expr.clone()),
                });
            }
            next_is_context = false;
            continue;
        }
        if let Some(Value::Cons(_, _)) = parts.first()
            && let Some(kind) = cl_defmethod_specializer_kind(parts.get(1))
        {
            specializers.push(ClDefmethodSpecializer {
                variable: cl_defmethod_destructuring_parameter_name(index),
                kind,
                is_context: false,
                context_expr: None,
            });
            next_is_context = false;
            continue;
        }
        let Some(Value::Symbol(variable)) = parts.first() else {
            continue;
        };
        if next_is_context && let Some(Value::Cons(_, _)) = parts.get(1) {
            let specializer = parts[1].to_vec()?;
            if matches!(specializer.first(), Some(Value::Symbol(name)) if name == "eql")
                && let Some(value) = specializer.get(1)
            {
                specializers.push(ClDefmethodSpecializer {
                    variable: variable.clone(),
                    kind: ClDefmethodSpecializerKind::Eql(value.clone()),
                    is_context: true,
                    context_expr: None,
                });
            }
            next_is_context = false;
            continue;
        }
        if let Some(kind) = cl_defmethod_specializer_kind(parts.get(1)) {
            specializers.push(ClDefmethodSpecializer {
                variable: variable.clone(),
                kind,
                is_context: next_is_context,
                context_expr: None,
            });
        }
        next_is_context = false;
    }
    Ok(specializers)
}

fn cl_defmethod_qualifier_key(qualifiers: &[Value]) -> String {
    let parts = qualifiers
        .iter()
        .filter_map(|value| value.as_symbol().ok())
        .map(|name| name.trim_start_matches(':'))
        .map(|name| name.replace([':', '\'', ' ', '(', ')', '-'], "_"))
        .collect::<Vec<_>>();
    if parts.is_empty() {
        String::new()
    } else {
        format!("{}_", parts.join("_"))
    }
}

fn cl_defmethod_load_history_specializers(spec: &Value) -> Vec<Value> {
    let Ok(items) = spec.to_vec() else {
        return Vec::new();
    };
    let mut specializers = Vec::new();
    let mut is_context = false;
    for item in items {
        if matches!(&item, Value::Symbol(symbol) if symbol == "&context") {
            is_context = true;
            continue;
        }
        let Ok(parts) = item.to_vec() else {
            continue;
        };
        if is_context {
            specializers.push(item);
            is_context = false;
        } else if let Some(specializer) = parts.get(1) {
            specializers.push(specializer.clone());
        }
    }
    specializers
}

fn cl_generic_no_applicable_function(method_name: &str, params: &[String]) -> Value {
    let fixed_params = lambda_list_fixed_params(params);
    let rest_param = lambda_list_rest_param_from_params(params);
    let mut args = vec![Value::Symbol("list".into())];
    args.extend(fixed_params.iter().cloned().map(Value::Symbol));
    let args = if let Some(rest_param) = rest_param {
        Value::list([
            Value::Symbol("append".into()),
            Value::list(args),
            Value::Symbol(rest_param),
        ])
    } else {
        Value::list(args)
    };
    Value::Lambda(
        params.to_vec(),
        vec![Value::list([
            Value::Symbol("emaxx--cl-generic-apply-next".into()),
            Value::Nil,
            Value::list([
                Value::Symbol("quote".into()),
                Value::Symbol(method_name.to_string()),
            ]),
            Value::list([
                Value::Symbol("quote".into()),
                Value::Symbol("no-applicable".into()),
            ]),
            args,
        ])]
        .into(),
        shared_env(Vec::new()),
    )
}

fn cl_defmethod_around_previous_binding(
    function: &Value,
    method_name: &str,
    is_applicable: &mut impl FnMut(&str) -> bool,
) -> Option<(SharedEnv, String, Value)> {
    let prefix = format!(
        "__emaxx_previous_method_{}_around_",
        method_name.replace('-', "_")
    );
    let Value::Lambda(_, _, closure_env) = function else {
        return None;
    };
    for frame in closure_env.borrow().iter() {
        for (name, value) in frame {
            if name.starts_with(&prefix) {
                let around_class_key = &name[prefix.len()..];
                let around_class = around_class_key
                    .split_once("class_")
                    .map(|(_, class_name)| class_name)
                    .unwrap_or(around_class_key);
                // The specializer-less :around wrapper binds
                // `..._around_class_t_method' (note the suffix).
                let around_class = around_class.strip_suffix("_method").unwrap_or(around_class);
                if is_applicable(around_class) {
                    let method_previous_name = format!("{name}_method");
                    let current_method_name = format!("{method_previous_name}_current");
                    for frame in closure_env.borrow().iter() {
                        for (candidate_name, candidate_value) in frame {
                            if candidate_name != &current_method_name {
                                continue;
                            }
                            let Value::Lambda(_, _, current_method_env) = candidate_value else {
                                continue;
                            };
                            for current_method_frame in current_method_env.borrow().iter() {
                                for (current_method_name, current_method_value) in
                                    current_method_frame
                                {
                                    if current_method_name == &method_previous_name {
                                        return Some((
                                            current_method_env.clone(),
                                            current_method_name.clone(),
                                            current_method_value.clone(),
                                        ));
                                    }
                                }
                            }
                        }
                    }
                    return Some((closure_env.clone(), name.clone(), value.clone()));
                }
                if let Some(nested) =
                    cl_defmethod_around_previous_binding(value, method_name, is_applicable)
                {
                    return Some(nested);
                }
            }
        }
    }
    None
}

fn cl_defmethod_advice_original_binding(function: &Value) -> Option<(SharedEnv, String, Value)> {
    // GNU nadvice advice OBJECTS (oclosures): the dispatch root is the
    // chain's innermost `cdr' slot; method registration grafts the rebuilt
    // dispatch back by mutating that slot in place (advice--subst-main).
    if oclosure_lambda_type(function).is_some_and(|type_name| type_name == "advice") {
        let mut current = function.clone();
        loop {
            let Value::Lambda(_, _, closure_env) = &current else {
                return None;
            };
            let closure_env = closure_env.clone();
            let cdr = {
                let contents = closure_env.borrow();
                contents.iter().rev().find_map(|frame| {
                    frame
                        .iter()
                        .any(|(key, _)| key == OCLOSURE_TYPE_MARKER)
                        .then(|| {
                            frame
                                .iter()
                                .find(|(key, _)| key == "cdr")
                                .map(|(_, value)| value.clone())
                                .unwrap_or(Value::Nil)
                        })
                })?
            };
            if oclosure_lambda_type(&cdr).is_some_and(|type_name| type_name == "advice") {
                current = cdr;
                continue;
            }
            return Some((closure_env, "cdr".to_string(), cdr));
        }
    }
    let Value::Lambda(params, _, closure_env) = function else {
        return None;
    };
    // Only a function that IS an advice wrapper counts: its rest parameter
    // carries the wrapper's unique suffix, which names the captured original.
    // Dispatch wrappers can capture unrelated advice activations in their
    // closure env, so scanning every frame for any original binding would
    // misroute method registration through the advice-splice path.
    let rest_param = params.last()?;
    let original_name = rest_param
        .strip_prefix("__emaxx-advice-around-args-")
        .map(|unique| format!("__emaxx-advice-around-original-{unique}"))
        .or_else(|| {
            rest_param
                .strip_prefix("__emaxx-advice-after-args-")
                .map(|unique| format!("__emaxx-advice-after-original-{unique}"))
        })?;
    for frame in closure_env.borrow().iter() {
        for (name, value) in frame {
            if name == &original_name {
                return Some((closure_env.clone(), name.clone(), value.clone()));
            }
        }
    }
    None
}

fn oclosure_lambda_type(value: &Value) -> Option<String> {
    let Value::Lambda(_, body, closure_env) = value else {
        return None;
    };
    // Real oclosures carry the oclosure marker as their first executable
    // body form; a dispatch wrapper that merely CAPTURED an oclosure's
    // frames must not be mistaken for one.
    let first = body
        .iter()
        .find(|form| !matches!(form, Value::String(_) | Value::StringObject(_)))?;
    if !matches!(first, Value::Symbol(marker) if marker == ":closure-oclosure") {
        return None;
    }
    let contents = closure_env.borrow();
    contents.iter().rev().find_map(|frame| {
        frame
            .iter()
            .find(|(key, _)| key == OCLOSURE_TYPE_MARKER)
            .and_then(|(_, value)| value.as_symbol().ok().map(String::from))
    })
}

fn cl_defmethod_previous_binding(
    function: &Value,
    previous_method_symbol: &str,
) -> Option<(SharedEnv, String, Value)> {
    let mut seen = HashSet::new();
    let mut seen_cons = HashSet::new();
    cl_defmethod_previous_binding_inner(function, previous_method_symbol, &mut seen, &mut seen_cons)
}

// Scan a generic function's closure graph for a binding with the given
// name, returning its value (used to find a re-registered method's stored
// body so it can be replaced in place like GNU).
fn cl_defmethod_find_named_binding(function: &Value, name: &str) -> Option<Value> {
    let mut seen_envs = HashSet::new();
    cl_defmethod_named_binding_inner(function, name, None, &mut seen_envs)
}

fn cl_defmethod_set_named_binding(function: &Value, name: &str, replacement: &Value) -> bool {
    let mut seen_envs = HashSet::new();
    cl_defmethod_named_binding_inner(function, name, Some(replacement), &mut seen_envs).is_some()
}

fn cl_defmethod_replace_child_environment(
    function: &Value,
    target_env_id: usize,
    replacement: &Value,
) -> bool {
    fn replace(
        function: &Value,
        target_env_id: usize,
        replacement: &Value,
        seen_envs: &mut HashSet<usize>,
    ) -> bool {
        let Value::Lambda(_, _, closure_env) = function else {
            return false;
        };
        let env_id = closure_env.as_ptr() as usize;
        if !seen_envs.insert(env_id) {
            return false;
        }
        let mut changed = false;
        let mut nested = Vec::new();
        {
            let mut closure_env = closure_env.borrow_mut();
            for frame in closure_env.iter_mut() {
                for (name, value) in frame.iter_mut() {
                    if !name.starts_with("__emaxx_") {
                        continue;
                    }
                    if matches!(value, Value::Lambda(_, _, child) if child.as_ptr() as usize == target_env_id)
                    {
                        *value = replacement.clone();
                        changed = true;
                    } else {
                        nested.push(value.clone());
                    }
                }
            }
        }
        for value in nested {
            changed |= replace(&value, target_env_id, replacement, seen_envs);
        }
        changed
    }

    replace(function, target_env_id, replacement, &mut HashSet::new())
}

fn cl_defmethod_contains_binding_fragment(function: &Value, fragment: &str) -> bool {
    fn contains(function: &Value, fragment: &str, seen_envs: &mut HashSet<usize>) -> bool {
        let Value::Lambda(_, _, closure_env) = function else {
            return false;
        };
        let env_id = closure_env.as_ptr() as usize;
        if !seen_envs.insert(env_id) {
            return false;
        }
        let nested = closure_env
            .borrow()
            .iter()
            .flat_map(|frame| frame.iter())
            .filter(|(name, _)| name.starts_with("__emaxx_"))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<Vec<_>>();
        nested.iter().any(|(name, _)| name.contains(fragment))
            || nested
                .into_iter()
                .any(|(_, value)| contains(&value, fragment, seen_envs))
    }

    contains(function, fragment, &mut HashSet::new())
}

fn cl_defmethod_named_binding_inner(
    function: &Value,
    name: &str,
    replacement: Option<&Value>,
    seen_envs: &mut HashSet<usize>,
) -> Option<Value> {
    let Value::Lambda(_, _, closure_env) = function else {
        return None;
    };
    let env_id = closure_env.as_ptr() as usize;
    if !seen_envs.insert(env_id) {
        return None;
    }
    let mut nested = Vec::new();
    {
        let mut closure_env = closure_env.borrow_mut();
        for frame in closure_env.iter_mut() {
            for (binding_name, value) in frame.iter_mut() {
                if binding_name == name {
                    let found = value.clone();
                    if let Some(replacement) = replacement {
                        *value = replacement.clone();
                    }
                    return Some(found);
                }
                if binding_name.starts_with("__emaxx_") {
                    nested.push(value.clone());
                }
            }
        }
    }
    nested
        .into_iter()
        .find_map(|value| cl_defmethod_named_binding_inner(&value, name, replacement, seen_envs))
}

fn cl_defmethod_replace_ignore_previous_bindings(
    interp: &Interpreter,
    function: &Value,
    replacement: &Value,
) -> bool {
    let mut seen_envs = HashSet::new();
    let mut seen_cons = HashSet::new();
    cl_defmethod_replace_ignore_previous_bindings_inner(
        interp,
        function,
        replacement,
        &mut seen_envs,
        &mut seen_cons,
    )
}

fn cl_defmethod_replace_ignore_previous_bindings_inner(
    interp: &Interpreter,
    function: &Value,
    replacement: &Value,
    seen_envs: &mut HashSet<usize>,
    seen_cons: &mut HashSet<usize>,
) -> bool {
    match function {
        Value::Lambda(_, _, closure_env) => {
            let env_id = closure_env.as_ptr() as usize;
            if !seen_envs.insert(env_id) {
                return false;
            }
            let mut replaced = false;
            let mut nested = Vec::new();
            {
                let mut closure_env = closure_env.borrow_mut();
                for frame in closure_env.iter_mut() {
                    for (name, value) in frame.iter_mut() {
                        if (name.starts_with("__emaxx_previous_method_")
                            || name.starts_with("__emaxx_"))
                            && interp.callable_is_ignore(value)
                        {
                            *value = replacement.clone();
                            replaced = true;
                        } else {
                            nested.push(value.clone());
                        }
                    }
                }
            }
            nested.into_iter().fold(replaced, |replaced, value| {
                cl_defmethod_replace_ignore_previous_bindings_inner(
                    interp,
                    &value,
                    replacement,
                    seen_envs,
                    seen_cons,
                ) || replaced
            })
        }
        Value::Cons(car, cdr) => {
            let cons_id = car.as_ptr() as usize;
            if !seen_cons.insert(cons_id) {
                return false;
            }
            cl_defmethod_replace_ignore_previous_bindings_inner(
                interp,
                &car.borrow(),
                replacement,
                seen_envs,
                seen_cons,
            ) || cl_defmethod_replace_ignore_previous_bindings_inner(
                interp,
                &cdr.borrow(),
                replacement,
                seen_envs,
                seen_cons,
            )
        }
        _ => false,
    }
}

// Replace the terminal/default callable captured by an existing generic
// dispatch graph.  Re-evaluating `cl-defgeneric' changes that default method
// but preserves methods which other libraries have already registered.
fn cl_defmethod_replace_terminal_previous_bindings(function: &Value, replacement: &Value) -> bool {
    fn is_dispatch_chain(function: &Value) -> bool {
        ["previous_method_", "before_method_", "after_method_"]
            .iter()
            .any(|fragment| cl_defmethod_contains_binding_fragment(function, fragment))
    }

    fn replace(
        function: &Value,
        replacement: &Value,
        seen_envs: &mut HashSet<usize>,
        seen_cons: &mut HashSet<usize>,
    ) -> bool {
        match function {
            Value::Lambda(_, _, closure_env) => {
                let env_id = closure_env.as_ptr() as usize;
                if !seen_envs.insert(env_id) {
                    return false;
                }
                let mut replaced = false;
                let mut nested = Vec::new();
                {
                    let mut closure_env = closure_env.borrow_mut();
                    for frame in closure_env.iter_mut() {
                        for (name, value) in frame.iter_mut() {
                            if (name.starts_with("__emaxx_previous_method_")
                                || name.starts_with("__emaxx_before_method_")
                                || name.starts_with("__emaxx_after_method_"))
                                && !is_dispatch_chain(value)
                            {
                                *value = replacement.clone();
                                replaced = true;
                            } else if name.starts_with("__emaxx_") {
                                nested.push(value.clone());
                            }
                        }
                    }
                }
                nested.into_iter().fold(replaced, |replaced, value| {
                    replace(&value, replacement, seen_envs, seen_cons) || replaced
                })
            }
            Value::Cons(car, cdr) => {
                let cons_id = car.as_ptr() as usize;
                if !seen_cons.insert(cons_id) {
                    return false;
                }
                replace(&car.borrow(), replacement, seen_envs, seen_cons)
                    || replace(&cdr.borrow(), replacement, seen_envs, seen_cons)
            }
            _ => false,
        }
    }

    replace(
        function,
        replacement,
        &mut HashSet::new(),
        &mut HashSet::new(),
    )
}

// A :before/:after wrapper's first closure frame binds the previous chain
// under a `__emaxx_{before,after}_method_...' name plus its specializer
// metadata, so registration can walk the qualifier stack and keep it
// ordered most-specific-outermost with primaries below the whole stack.
fn cl_defmethod_qualifier_wrapper_parts(
    function: &Value,
) -> Option<(SharedEnv, String, Value, Option<Value>)> {
    let Value::Lambda(_, _, closure_env) = function else {
        return None;
    };
    let borrowed = closure_env.borrow();
    let frame = borrowed.first()?;
    let mut previous = None;
    let mut specializer = None;
    for (name, value) in frame {
        if name.starts_with("__emaxx_before_method_") || name.starts_with("__emaxx_after_method_") {
            previous = Some((name.clone(), value.clone()));
        } else if name == "__emaxx-qualifier-specializer" {
            specializer = Some(value.clone());
        }
    }
    let (name, value) = previous?;
    drop(borrowed);
    Some((closure_env.clone(), name, value, specializer))
}

fn cl_defmethod_previous_binding_inner(
    function: &Value,
    previous_method_symbol: &str,
    seen_envs: &mut HashSet<usize>,
    seen_cons: &mut HashSet<usize>,
) -> Option<(SharedEnv, String, Value)> {
    match function {
        Value::Lambda(_, _, closure_env) => {
            let env_id = closure_env.as_ptr() as usize;
            if !seen_envs.insert(env_id) {
                return None;
            }
            for frame in closure_env.borrow().iter() {
                for (name, value) in frame {
                    if name == previous_method_symbol {
                        return Some((closure_env.clone(), name.clone(), value.clone()));
                    }
                }
            }
            for frame in closure_env.borrow().iter() {
                for (_, value) in frame {
                    if let Some(found) = cl_defmethod_previous_binding_inner(
                        value,
                        previous_method_symbol,
                        seen_envs,
                        seen_cons,
                    ) {
                        return Some(found);
                    }
                }
            }
            None
        }
        Value::Cons(car, cdr) => {
            let cons_id = car.as_ptr() as usize;
            if !seen_cons.insert(cons_id) {
                return None;
            }
            cl_defmethod_previous_binding_inner(
                &car.borrow(),
                previous_method_symbol,
                seen_envs,
                seen_cons,
            )
            .or_else(|| {
                cl_defmethod_previous_binding_inner(
                    &cdr.borrow(),
                    previous_method_symbol,
                    seen_envs,
                    seen_cons,
                )
            })
        }
        _ => None,
    }
}

fn rewrite_cl_call_next_method_forms(
    forms: &[Value],
    generic_name: &str,
    previous_method_symbol: &str,
    default_args: &Value,
    next_method_p: Value,
) -> Result<Vec<Value>, LispError> {
    forms
        .iter()
        .map(|form| {
            rewrite_cl_call_next_method_form(
                form,
                generic_name,
                previous_method_symbol,
                default_args,
                &next_method_p,
            )
        })
        .collect()
}

fn rewrite_cl_next_method_p_forms(
    forms: &[Value],
    generic_name: &str,
    default_args: &Value,
    next_method_p: Value,
) -> Result<Vec<Value>, LispError> {
    forms
        .iter()
        .map(|form| {
            rewrite_cl_call_next_method_form(
                form,
                generic_name,
                "ignore",
                default_args,
                &next_method_p,
            )
        })
        .collect()
}

fn rewrite_cl_call_next_method_form(
    form: &Value,
    generic_name: &str,
    previous_method_symbol: &str,
    default_args: &Value,
    next_method_p: &Value,
) -> Result<Value, LispError> {
    let Ok(items) = form.to_vec() else {
        return Ok(form.clone());
    };
    let Some(Value::Symbol(head)) = items.first() else {
        return items
            .iter()
            .map(|item| {
                rewrite_cl_call_next_method_form(
                    item,
                    generic_name,
                    previous_method_symbol,
                    default_args,
                    next_method_p,
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Value::list);
    };
    match head.as_str() {
        // `(apply #'cl-call-next-method ...)' captures the next method as a
        // function value (eieio-base.el's `eieio-named' constructor).  The
        // previous-method symbol is a variable holding that function, so the
        // sharp-quote rewrites to a plain variable reference.
        "function"
            if items.len() == 2
                && matches!(&items[1], Value::Symbol(symbol) if symbol == "cl-call-next-method") =>
        {
            Ok(if previous_method_symbol == "ignore" {
                Value::list([
                    Value::Symbol("function".into()),
                    Value::Symbol("ignore".into()),
                ])
            } else {
                Value::Symbol(previous_method_symbol.to_string())
            })
        }
        "quote" | "function" => Ok(form.clone()),
        "cl-next-method-p" if items.len() == 1 => Ok(next_method_p.clone()),
        "cl-call-next-method" => {
            let args = if items.len() == 1 {
                default_args.clone()
            } else {
                let mut list_form = Vec::with_capacity(items.len());
                list_form.push(Value::Symbol("list".into()));
                list_form.extend(items[1..].iter().cloned());
                Value::list(list_form)
            };
            // The previous-method variable keeps the `ignore' sentinel until
            // a later registration splices a method below this one; the
            // runtime helper applies a real next method and routes the
            // sentinel to `cl-no-next-method' like GNU.  A method installed
            // directly as the generic function has no previous variable at
            // all — pass nil so the helper always dispatches the hook.
            let previous_reference = if previous_method_symbol == "ignore" {
                Value::Nil
            } else {
                Value::Symbol(previous_method_symbol.to_string())
            };
            Ok(Value::list([
                Value::Symbol("emaxx--cl-generic-apply-next".into()),
                previous_reference,
                Value::list([
                    Value::Symbol("quote".into()),
                    Value::Symbol(generic_name.to_string()),
                ]),
                Value::list([
                    Value::Symbol("quote".into()),
                    Value::Symbol("no-next".into()),
                ]),
                args,
            ]))
        }
        _ => items
            .iter()
            .map(|item| {
                rewrite_cl_call_next_method_form(
                    item,
                    generic_name,
                    previous_method_symbol,
                    default_args,
                    next_method_p,
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Value::list),
    }
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
        "cl-letf" => substitute_symbol_macros_in_cl_letf(&items, expansions),
        "setq" => substitute_symbol_macros_in_setq(&items, expansions),
        _ => {
            let mut rewritten = Vec::with_capacity(items.len());
            rewritten.push(items[0].clone());
            for item in &items[1..] {
                rewritten.push(substitute_symbol_macros(item, expansions)?);
            }
            Ok(Value::list(rewritten))
        }
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

fn substitute_symbol_macros_in_cl_letf(
    items: &[Value],
    expansions: &HashMap<String, Value>,
) -> Result<Value, LispError> {
    let Some(bindings_value) = items.get(1) else {
        return Ok(Value::list(items.iter().cloned()));
    };
    let mut rewritten_bindings = Vec::new();
    for binding in bindings_value.to_vec()? {
        let parts = binding.to_vec()?;
        if parts.is_empty() {
            rewritten_bindings.push(Value::list(parts));
            continue;
        }
        let mut rewritten = Vec::with_capacity(parts.len());
        rewritten.push(substitute_symbol_macros(&parts[0], expansions)?);
        for form in &parts[1..] {
            rewritten.push(substitute_symbol_macros(form, expansions)?);
        }
        rewritten_bindings.push(Value::list(rewritten));
    }

    let mut rewritten = Vec::with_capacity(items.len());
    rewritten.push(items[0].clone());
    rewritten.push(Value::list(rewritten_bindings));
    for form in &items[2..] {
        rewritten.push(substitute_symbol_macros(form, expansions)?);
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
        "comma" | "," => items
            .get(1)
            .map(lower_define_inline_form)
            .unwrap_or(Value::Nil),
        "quote" if items.len() == 2 => match items[1].to_vec() {
            Ok(quoted) if matches!(quoted.first(), Some(Value::Symbol(name)) if comma_head_kind(name) == Some("comma")) => {
                quoted
                    .get(1)
                    .map(lower_define_inline_form)
                    .unwrap_or(Value::Nil)
            }
            _ => Value::list([Value::Symbol("quote".into()), items[1].clone()]),
        },
        "function" | "function-quote" if items.len() == 2 => match items[1].to_vec() {
            Ok(quoted) if matches!(quoted.first(), Some(Value::Symbol(name)) if comma_head_kind(name) == Some("comma")) => {
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

/// Allocation-free pre-scan for `setcdr' anywhere in FORM, so the hot
/// `if' path skips the tail-alias machinery entirely.  BUDGET caps the
/// walk (reader forms can be circular); an exhausted budget reports
/// true, deferring to the cycle-safe slow path.
pub(crate) fn form_mentions_setcdr(value: &Value, budget: &mut u32) -> bool {
    if *budget == 0 {
        return true;
    }
    *budget -= 1;
    match value {
        Value::Symbol(name) => name == "setcdr",
        Value::Cons(car, cdr) => {
            form_mentions_setcdr(&car.borrow(), budget)
                || form_mentions_setcdr(&cdr.borrow(), budget)
        }
        _ => false,
    }
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

fn parse_cl_defstruct_constructor_params(
    items: Vec<Value>,
) -> (Vec<String>, Vec<(String, Value)>, bool) {
    let mut params = Vec::new();
    let mut aux_bindings = Vec::new();
    let mut in_aux = false;
    let mut direct_lambda = true;
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
            if name.starts_with('&') && name != "&optional" {
                direct_lambda = false;
            }
            params.push(name.to_string());
        } else {
            // Destructuring and default-bearing CL parameters require the
            // general &rest parser below; ordinary interpreted lambdas
            // cannot represent them directly.
            direct_lambda = false;
        }
    }
    (params, aux_bindings, direct_lambda)
}

fn cl_defstruct_constructor_aux_let_bindings(
    params: &[String],
    aux_bindings: Vec<(String, Value)>,
) -> Vec<Value> {
    let mut bindings = Vec::new();
    let mut positional_index = 0usize;
    let mut mode = "required";
    for param in params {
        match param.as_str() {
            "&optional" => {
                mode = "optional";
                continue;
            }
            "&key" => {
                mode = "key";
                continue;
            }
            "&rest" | "&body" => {
                mode = "rest";
                continue;
            }
            "&allow-other-keys" => continue,
            marker if marker.starts_with('&') => continue,
            _ => {}
        }

        let value_form = match mode {
            "key" => Value::list([
                Value::Symbol("plist-get".into()),
                Value::Symbol("args".into()),
                Value::Symbol(format!(":{param}")),
            ]),
            "rest" => {
                mode = "after-rest";
                Value::list([
                    Value::Symbol("nthcdr".into()),
                    Value::Integer(positional_index as i64),
                    Value::Symbol("args".into()),
                ])
            }
            "after-rest" => continue,
            _ => {
                let form = Value::list([
                    Value::Symbol("nth".into()),
                    Value::Integer(positional_index as i64),
                    Value::Symbol("args".into()),
                ]);
                positional_index += 1;
                form
            }
        };
        bindings.push(Value::list([Value::Symbol(param.clone()), value_form]));
    }
    bindings.extend(
        aux_bindings
            .into_iter()
            .map(|(name, form)| Value::list([Value::Symbol(name), form])),
    );
    bindings
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

// GNU pcase--funcall for `app' patterns: a call form may name the object
// with `_'; without a placeholder the object becomes the last argument.
fn pcase_apply_app_function(
    interp: &mut Interpreter,
    env: &mut Env,
    function: &Value,
    value: &Value,
) -> Result<Value, LispError> {
    if let Ok(items) = function.to_vec()
        && !items.is_empty()
        && !matches!(items.first(), Some(Value::Symbol(head)) if head == "lambda" || head == "closure")
    {
        let mut call_form = Vec::with_capacity(items.len() + 1);
        let mut saw_placeholder = false;
        for (index, item) in items.iter().enumerate() {
            if index > 0 && matches!(item, Value::Symbol(name) if name == "_") {
                saw_placeholder = true;
                call_form.push(quoted_literal(value));
            } else {
                call_form.push(item.clone());
            }
        }
        if !saw_placeholder {
            call_form.push(quoted_literal(value));
        }
        return interp.eval(&Value::list(call_form), env);
    }
    interp.call_function_value(function.clone(), None, std::slice::from_ref(value), env)
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
        // GNU pcase-let only destructures: membership tests on literal
        // symbols inside a backquote (like the `_ _' in icons.el's
        // `(,parent ,spec _ _)) are dropped, not checked.
        if name.starts_with(':') {
            return Ok((lenient_list_match && backquoted) || pattern == value);
        }
        if backquoted {
            return Ok(lenient_list_match || pattern == value);
        }
        bindings.push((name.clone(), value.clone()));
        return Ok(true);
    }
    if let Ok(parts) = pattern.to_vec() {
        if matches!(parts.first(), Some(Value::Symbol(name)) if is_backquote_head(name)) {
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
            if matches!(parts.first(), Some(Value::Symbol(name)) if comma_head_kind(name).is_some())
            {
                let Some(pattern) = parts.get(1) else {
                    return Ok(false);
                };
                if let Value::Symbol(name) = pattern {
                    bindings.push((name.clone(), value.clone()));
                    return Ok(true);
                }
                return pcase_pattern_bindings_inner(
                    interp,
                    env,
                    pattern,
                    value,
                    bindings,
                    lenient_list_match,
                    false,
                );
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
                let matches = pcase_predicate_matches(interp, env, &predicate_form, value)?;
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
            if matches!(parts.first(), Some(Value::Symbol(name)) if comma_head_kind(name).is_some())
                && let Some(Value::Symbol(name)) = parts.get(1)
            {
                bindings.push((name.clone(), value.clone()));
                return Ok(true);
            }
            // GNU (app FUN PAT): apply FUN to the object (`_' in a call
            // form stands for the object; otherwise it is appended as the
            // last argument) and match PAT against the result.
            if !backquoted
                && matches!(parts.first(), Some(Value::Symbol(name)) if name == "app")
                && parts.len() >= 3
            {
                let result = pcase_apply_app_function(interp, env, &parts[1], value)?;
                return pcase_pattern_bindings_inner(
                    interp,
                    env,
                    &parts[2],
                    &result,
                    bindings,
                    lenient_list_match,
                    backquoted,
                );
            }
            // pcase-defmacro extensions (map.el's `(map ...)', ...):
            // expand through the head symbol's `pcase-macroexpander' and
            // match the expansion.
            if !backquoted
                && let Some(Value::Symbol(head)) = parts.first()
                && let Some(expander) = interp.get_symbol_property(head, "pcase-macroexpander")
                && expander.is_truthy()
            {
                let expanded = interp.call_function_value(expander, None, &parts[1..], env)?;
                return pcase_pattern_bindings_inner(
                    interp,
                    env,
                    &expanded,
                    value,
                    bindings,
                    lenient_list_match,
                    backquoted,
                );
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

fn pcase_predicate_matches(
    interp: &mut Interpreter,
    env: &mut Env,
    predicate_form: &Value,
    value: &Value,
) -> Result<bool, LispError> {
    if let Ok(mut items) = predicate_form.to_vec()
        && !items.is_empty()
    {
        // A (lambda ...) pred is a function to call on VALUE; any other list
        // form is a partial application that VALUE gets appended to, e.g.
        // `(pred (> 5))' matches when `(> 5 VALUE)' is non-nil.
        if matches!(items.first(), Some(Value::Symbol(head)) if head == "lambda" || head == "function" || head == "closure")
        {
            let function = interp.eval(predicate_form, env)?;
            return Ok(crate::lisp::primitives::call_function_value(
                interp,
                &function,
                std::slice::from_ref(value),
                env,
            )?
            .is_truthy());
        }
        items.push(quoted_literal(value));
        return Ok(interp.eval(&Value::list(items), env)?.is_truthy());
    }
    let predicate = match interp.eval(predicate_form, env) {
        Ok(predicate) => predicate,
        Err(LispError::Void(_)) if matches!(predicate_form, Value::Symbol(_)) => {
            predicate_form.clone()
        }
        Err(error) => return Err(error),
    };
    Ok(crate::lisp::primitives::call_function_value(
        interp,
        &predicate,
        std::slice::from_ref(value),
        env,
    )?
    .is_truthy())
}

fn is_compat_preloaded_feature(feature: &str) -> bool {
    matches!(
        feature,
        "cl-extra"
            | "cl-generic"
            | "cl-lib"
            | "cus-load"
            | "edmacro"
            | "hex-util"
            | "map"
            | "rfc2104"
            | "seq"
            | "thread"
    )
}

fn build_signal_value(condition: Value, data: Value) -> Value {
    if let Ok(items) = data.to_vec() {
        Value::cons(condition, Value::list(items))
    } else {
        // GNU keeps non-list DATA as the cdr: (signal 'foo 4) is caught
        // as the dotted pair (foo . 4).
        Value::cons(condition, data)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
