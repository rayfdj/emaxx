use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::path::PathBuf;
use std::process::Child;
use std::rc::{Rc, Weak};
use std::time::{Duration, SystemTime};

use super::primitives;
use super::sqlite::SqliteHandleState;
use super::types::{
    ConsCell, EmacsTermination, Env, EnvFrame, LambdaValue, LispError, ReaderClosureKind,
    ReaderForm, SharedEnv, Value, WeakConsSlot, shared_env,
};
use crate::compat::{BatchSummary, DiscoveredTest, TestOutcome, TestStatus};
use hashlink::LinkedHashMap;
use regex::Regex;

mod bindings;
mod bootstrap;
mod buffers;
mod coding;
mod control_forms;
mod core;
mod definitions;
mod faces;
mod loops;
mod macros;
mod resource_forms;
pub(crate) mod runtime;
mod threads;
mod treesit;
mod variables;
use bootstrap::*;
pub(crate) use core::is_special_form_name;
mod ert;

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

// image.c's native image variables exist before image.el or shr.el load and
// retain dynamic binding semantics under lexical binding.  Keep the complete
// feature-independent group together; `imagemagick-render-type' is omitted
// because GNU only defines it when built with ImageMagick support.
fn gnu_image_special_variables() -> [(&'static str, Value); 6] {
    [
        (
            "image-types",
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
        ("max-image-size", Value::float(10.0)),
        ("cross-disabled-images", Value::Nil),
        (
            "x-bitmap-file-path",
            Value::list([Value::String(".".into())]),
        ),
        ("image-cache-eviction-delay", Value::Integer(300)),
        ("image-scaling-factor", Value::symbol("auto")),
    ]
}

// buffer.c and insdel.c publish the change-hook controls as native DEFVARs.
// They are process-wide value cells (modes may make the hook variables local),
// and lexical `let' must still bind them dynamically across helper calls.
const GNU_CHANGE_HOOK_SPECIAL_VARIABLES: &[&str] = &[
    "before-change-functions",
    "after-change-functions",
    "first-change-hook",
    "combine-after-change-calls",
    "inhibit-modification-hooks",
];

#[derive(Clone, Copy)]
struct StartupFeature {
    name: &'static str,
    subfeatures: Option<fn() -> Value>,
}

impl StartupFeature {
    const fn new(name: &'static str) -> Self {
        Self {
            name,
            subfeatures: None,
        }
    }

    const fn with_subfeatures(mut self, subfeatures: fn() -> Value) -> Self {
        self.subfeatures = Some(subfeatures);
        self
    }
}

fn make_network_process_subfeatures() -> Value {
    let pair = |key: &str, value: Value| Value::list([Value::Symbol(key.into()), value]);
    Value::list([
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
    ])
}

fn overlay_subfeatures() -> Value {
    Value::list([
        Value::Symbol("display".into()),
        Value::Symbol("syntax-table".into()),
        Value::Symbol("field".into()),
    ])
}

fn text_properties_subfeatures() -> Value {
    Value::list([
        Value::Symbol("display".into()),
        Value::Symbol("syntax-table".into()),
        Value::Symbol("field".into()),
        Value::Symbol("point-entered".into()),
    ])
}

// One manifest owns startup feature membership and capability metadata.
// A feature with subfeatures must not require its name to be repeated in a
// separate property-registration block.
const STARTUP_FEATURES: &[StartupFeature] = &[
    // GNU bindings.el advertises this host-backed primitive family.
    StartupFeature::new("base64"),
    StartupFeature::new("emacs"),
    StartupFeature::new("kqueue"),
    StartupFeature::new("lcms2"),
    StartupFeature::new("make-network-process").with_subfeatures(make_network_process_subfeatures),
    StartupFeature::new("md5"),
    // GNU provides this when its comp.c backend is built, independently of
    // whether libgccjit can be opened at runtime.  This build contains the
    // corresponding Rust backend; `native-comp-available-p' performs the
    // separate dynamic-library availability check.
    StartupFeature::new("native-compile"),
    StartupFeature::new("overlay").with_subfeatures(overlay_subfeatures),
    StartupFeature::new("sha1"),
    StartupFeature::new("text-properties").with_subfeatures(text_properties_subfeatures),
    StartupFeature::new("threads"),
];

#[derive(Clone, Copy)]
enum StaticStartupValue {
    T,
    Integer(i64),
}

impl StaticStartupValue {
    fn value(self) -> Value {
        match self {
            Self::T => Value::T,
            Self::Integer(value) => Value::Integer(value),
        }
    }
}

#[derive(Clone, Copy)]
struct NativeGlobalVariable {
    name: &'static str,
    default: StaticStartupValue,
}

// xdisp.c owns these value cells and their startup defaults.  GNU Elisp
// consumers such as simple.el and ert.el use them, but do not own or recreate
// them.  Keep each native name, value, and special declaration in one entry.
const GNU_XDISP_GLOBAL_VARIABLES: &[NativeGlobalVariable] = &[
    NativeGlobalVariable {
        name: "auto-hscroll-mode",
        default: StaticStartupValue::T,
    },
    NativeGlobalVariable {
        name: "hscroll-margin",
        default: StaticStartupValue::Integer(5),
    },
    NativeGlobalVariable {
        name: "hscroll-step",
        default: StaticStartupValue::Integer(0),
    },
    NativeGlobalVariable {
        name: "scroll-minibuffer-conservatively",
        default: StaticStartupValue::T,
    },
    NativeGlobalVariable {
        name: "truncate-partial-width-windows",
        default: StaticStartupValue::Integer(50),
    },
    NativeGlobalVariable {
        name: "message-log-max",
        default: StaticStartupValue::Integer(1000),
    },
];

// buffer.c's complete GNU 30.2 DEFVAR_PER_BUFFER contract.  These variables
// are both special under lexical binding and automatically local to the
// current buffer when assigned.  Keeping the manifest together prevents a
// newly exercised preloaded variable from accidentally behaving like an
// ordinary Emaxx global (the fill-column/string-fill regression did exactly
// that after lexical closure boundaries became correct).
#[derive(Clone, Copy)]
struct NativePerBufferVariable {
    name: &'static str,
    always_local: bool,
    permanent: bool,
}

impl NativePerBufferVariable {
    const fn new(name: &'static str) -> Self {
        Self {
            name,
            always_local: false,
            permanent: false,
        }
    }

    const fn always_local(mut self) -> Self {
        self.always_local = true;
        self
    }

    const fn permanent(mut self) -> Self {
        self.permanent = true;
        self
    }
}

// This is also the single source for buffer.c's always-local slots (those
// absent from buffer_local_flags), buffer_permanent_local_flags, and the
// permanent-local properties that dumped bindings.el adds to native slots.
// Encoding those properties on the owning entry makes it impossible to add a
// name to one registry while forgetting its matching entry in another.
const GNU_NATIVE_PER_BUFFER_VARIABLES: &[NativePerBufferVariable] = &[
    NativePerBufferVariable::new("abbrev-mode"),
    NativePerBufferVariable::new("auto-fill-function"),
    NativePerBufferVariable::new("bidi-display-reordering"),
    NativePerBufferVariable::new("bidi-paragraph-direction"),
    NativePerBufferVariable::new("bidi-paragraph-separate-re"),
    NativePerBufferVariable::new("bidi-paragraph-start-re"),
    NativePerBufferVariable::new("buffer-auto-save-file-format")
        .always_local()
        .permanent(),
    NativePerBufferVariable::new("buffer-auto-save-file-name")
        .always_local()
        .permanent(),
    NativePerBufferVariable::new("buffer-backed-up")
        .always_local()
        .permanent(),
    NativePerBufferVariable::new("buffer-display-count")
        .always_local()
        .permanent(),
    NativePerBufferVariable::new("buffer-display-table"),
    NativePerBufferVariable::new("buffer-display-time")
        .always_local()
        .permanent(),
    NativePerBufferVariable::new("buffer-file-coding-system").permanent(),
    NativePerBufferVariable::new("buffer-file-format")
        .always_local()
        .permanent(),
    NativePerBufferVariable::new("buffer-file-name")
        .always_local()
        .permanent(),
    NativePerBufferVariable::new("buffer-file-truename")
        .always_local()
        .permanent(),
    NativePerBufferVariable::new("buffer-invisibility-spec").always_local(),
    NativePerBufferVariable::new("buffer-read-only")
        .always_local()
        .permanent(),
    NativePerBufferVariable::new("buffer-saved-size")
        .always_local()
        .permanent(),
    NativePerBufferVariable::new("buffer-undo-list")
        .always_local()
        .permanent(),
    NativePerBufferVariable::new("cache-long-scans"),
    NativePerBufferVariable::new("ctl-arrow"),
    NativePerBufferVariable::new("cursor-in-non-selected-windows"),
    NativePerBufferVariable::new("cursor-type"),
    NativePerBufferVariable::new("default-directory")
        .always_local()
        .permanent(),
    NativePerBufferVariable::new("enable-multibyte-characters")
        .always_local()
        .permanent(),
    NativePerBufferVariable::new("fill-column"),
    NativePerBufferVariable::new("fringe-cursor-alist"),
    NativePerBufferVariable::new("fringe-indicator-alist"),
    NativePerBufferVariable::new("fringes-outside-margins"),
    NativePerBufferVariable::new("header-line-format"),
    NativePerBufferVariable::new("horizontal-scroll-bar"),
    NativePerBufferVariable::new("indicate-buffer-boundaries"),
    NativePerBufferVariable::new("indicate-empty-lines"),
    NativePerBufferVariable::new("left-fringe-width"),
    NativePerBufferVariable::new("left-margin"),
    NativePerBufferVariable::new("left-margin-width"),
    NativePerBufferVariable::new("line-spacing"),
    NativePerBufferVariable::new("local-abbrev-table"),
    NativePerBufferVariable::new("local-minor-modes").always_local(),
    NativePerBufferVariable::new("major-mode").always_local(),
    NativePerBufferVariable::new("mark-active")
        .always_local()
        .permanent(),
    NativePerBufferVariable::new("mode-line-format"),
    NativePerBufferVariable::new("mode-name").always_local(),
    NativePerBufferVariable::new("overwrite-mode"),
    NativePerBufferVariable::new("point-before-scroll")
        .always_local()
        .permanent(),
    NativePerBufferVariable::new("right-fringe-width"),
    NativePerBufferVariable::new("right-margin-width"),
    NativePerBufferVariable::new("scroll-bar-height"),
    NativePerBufferVariable::new("scroll-bar-width"),
    NativePerBufferVariable::new("scroll-down-aggressively"),
    NativePerBufferVariable::new("scroll-up-aggressively"),
    NativePerBufferVariable::new("selective-display"),
    NativePerBufferVariable::new("selective-display-ellipses"),
    // syntax.c defines this native value cell and then makes it buffer-local.
    NativePerBufferVariable::new("syntax-propertize--done"),
    NativePerBufferVariable::new("tab-line-format"),
    NativePerBufferVariable::new("tab-width"),
    NativePerBufferVariable::new("text-conversion-style"),
    NativePerBufferVariable::new("truncate-lines").permanent(),
    NativePerBufferVariable::new("vertical-scroll-bar"),
    NativePerBufferVariable::new("word-wrap"),
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
        (
            "coding-system-error",
            &["coding-system-error", "error"],
            "Invalid coding system",
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
        // sqlite.c defines these condition hierarchies even when a caller
        // never loads sqlite.el.  Generic condition consumers such as ERT's
        // `should-error' inspect the symbol properties after the native
        // primitive signals.
        ("sqlite-error", &["sqlite-error", "error"], "Database error"),
        (
            "sqlite-locked-error",
            &["sqlite-locked-error", "sqlite-error", "error"],
            "Database locked",
        ),
        // json.c installs these native condition symbols independently of
        // json.el.  Parsers signal the leaf condition and generic consumers
        // such as `condition-case' and ERT match through this hierarchy.
        ("json-error", &["json-error", "error"], "generic JSON error"),
        (
            "json-parse-error",
            &["json-parse-error", "json-error", "error"],
            "could not parse JSON stream",
        ),
        (
            "json-end-of-file",
            &[
                "json-end-of-file",
                "json-parse-error",
                "json-error",
                "error",
            ],
            "end of JSON stream",
        ),
        (
            "json-trailing-content",
            &[
                "json-trailing-content",
                "json-parse-error",
                "json-error",
                "error",
            ],
            "trailing content after JSON stream",
        ),
        (
            "json-utf8-decode-error",
            &["json-utf8-decode-error", "json-error", "error"],
            "invalid utf-8 encoding",
        ),
        // comp.c registers the native compiler and loader condition tree in
        // C, independently of comp.el.  Keep that ownership here: these are
        // the exact `error-conditions' and `error-message' values installed
        // by syms_of_comp.
        (
            "native-compiler-error",
            &["native-compiler-error", "error"],
            "Native compiler error",
        ),
        (
            "native-ice",
            &["native-ice", "native-compiler-error", "error"],
            "Internal native compiler error",
        ),
        (
            "native-lisp-load-failed",
            &["native-lisp-load-failed", "error"],
            "Native elisp load failed",
        ),
        (
            "native-lisp-wrong-reloc",
            &[
                "native-lisp-wrong-reloc",
                "native-lisp-load-failed",
                "error",
            ],
            "Primitive redefined or wrong relocation",
        ),
        (
            "wrong-register-subr-call",
            &[
                "wrong-register-subr-call",
                "native-lisp-load-failed",
                "error",
            ],
            "comp--register-subr can only be called during native lisp load phase.",
        ),
        (
            "native-lisp-file-inconsistent",
            &[
                "native-lisp-file-inconsistent",
                "native-lisp-load-failed",
                "error",
            ],
            "eln file inconsistent with current runtime configuration, please recompile",
        ),
        (
            "comp-sanitizer-error",
            &["comp-sanitizer-error", "error"],
            "Native code sanitizer runtime error",
        ),
        // treesit.c likewise owns these condition registrations.
        (
            "treesit-buffer-too-large",
            &["treesit-buffer-too-large", "treesit-error", "error"],
            "Buffer too large (> 4GiB)",
        ),
        (
            "treesit-error",
            &["treesit-error", "error"],
            "Generic tree-sitter error",
        ),
        (
            "treesit-invalid-predicate",
            &["treesit-invalid-predicate", "treesit-error", "error"],
            "Invalid predicate, see `treesit-thing-settings' for valid forms for a predicate",
        ),
        (
            "treesit-load-language-error",
            &["treesit-load-language-error", "treesit-error", "error"],
            "Cannot load language definition",
        ),
        (
            "treesit-node-buffer-killed",
            &["treesit-node-buffer-killed", "treesit-error", "error"],
            "The buffer associated with this node is killed",
        ),
        (
            "treesit-node-outdated",
            &["treesit-node-outdated", "treesit-error", "error"],
            "This node is outdated, please retrieve a new one",
        ),
        (
            "treesit-parse-error",
            &["treesit-parse-error", "treesit-error", "error"],
            "Parse failed",
        ),
        (
            "treesit-parser-deleted",
            &["treesit-parser-deleted", "treesit-error", "error"],
            "This parser is deleted and cannot be used",
        ),
        (
            "treesit-query-error",
            &["treesit-query-error", "treesit-error", "error"],
            "Query pattern is malformed",
        ),
        (
            "treesit-range-invalid",
            &["treesit-range-invalid", "treesit-error", "error"],
            "RANGES are invalid: they have to be ordered and should not overlap",
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
                                .map(|condition| Value::Symbol((*condition).to_string().into())),
                        ),
                    ),
                    (
                        "error-message".to_string(),
                        Value::String((*message).to_string().into()),
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
                .flat_map(|(property, value)| [Value::Symbol(property.into()), value]);
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
    /// Buffer whose persistent `mark-marker' identity this marker represents.
    ///
    /// This is independent of `buffer_id': clearing a buffer's mark detaches
    /// the marker without changing its identity.  Keeping the relationship on
    /// the marker also makes `set-marker' constant-time instead of reverse
    /// scanning every live buffer-mark entry.
    pub mark_buffer_id: Option<u64>,
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
    ascii_entry_indices: Option<Box<[usize; 128]>>,
    /// Lazily-built non-overlapping view of `entries': each map key is a
    /// range start, the payload its inclusive end plus the index of the
    /// newest log entry covering it.  The log itself must stay append-only
    /// (printing and `equal' compare it verbatim), so this is an index over
    /// it, kept incrementally current by `push_entry' and dropped whenever
    /// the log is replaced wholesale.
    resolved_ranges: std::cell::RefCell<Option<ResolvedCharRanges>>,
}

/// Range start -> (inclusive range end, newest covering entry index).
type ResolvedCharRanges = std::collections::BTreeMap<u32, (u32, usize)>;

#[derive(Clone, Debug)]
pub struct CharTableEntry {
    pub start: u32,
    pub end: u32,
    pub value: Value,
}

#[derive(Clone, Debug)]
struct RegexpSyntaxClassCache {
    table_id: u64,
    char_table_generation: u64,
    rendered: [String; 16],
}

/// Range segments of the syntax table, resolved once for the scanners that
/// cannot hold an interpreter borrow (`skip-chars-forward' and friends).
/// Keyed like the rendered-class cache so a table mutation invalidates it.
#[derive(Clone)]
pub(crate) struct SyntaxSegmentCache {
    table_id: u64,
    char_table_generation: u64,
    pub(crate) segments: std::rc::Rc<Vec<(u32, u32, crate::lisp::primitives::syntax::SyntaxClass)>>,
}

impl CharTableState {
    pub(crate) fn new(id: u64, subtype: Option<String>, default: Value) -> Self {
        Self::with_entries(id, subtype, default, None, Vec::new())
    }

    pub(crate) fn with_entries(
        id: u64,
        subtype: Option<String>,
        default: Value,
        parent: Option<u64>,
        entries: Vec<CharTableEntry>,
    ) -> Self {
        let ascii_entry_indices = Self::build_ascii_entry_indices(&entries);
        Self {
            id,
            subtype,
            default,
            parent,
            extra_slots: Vec::new(),
            entries,
            category_docs: Vec::new(),
            ascii_entry_indices,
            resolved_ranges: std::cell::RefCell::new(None),
        }
    }

    fn build_ascii_entry_indices(entries: &[CharTableEntry]) -> Option<Box<[usize; 128]>> {
        let mut indices = None;
        for (index, entry) in entries.iter().enumerate() {
            if entry.start >= 128 {
                continue;
            }
            let indices = indices.get_or_insert_with(|| Box::new([usize::MAX; 128]));
            for slot in entry.start as usize..=entry.end.min(127) as usize {
                indices[slot] = index;
            }
        }
        indices
    }

    pub(crate) fn push_entry(&mut self, entry: CharTableEntry) {
        let index = self.entries.len();
        let start = entry.start;
        let end = entry.end;
        self.entries.push(entry);
        if let Some(map) = self.resolved_ranges.get_mut().as_mut() {
            Self::overlay_resolved_range(map, start, end, index);
        }
        if start >= 128 {
            return;
        }
        let indices = self
            .ascii_entry_indices
            .get_or_insert_with(|| Box::new([usize::MAX; 128]));
        for slot in start as usize..=end.min(127) as usize {
            indices[slot] = index;
        }
    }

    pub(crate) fn replace_entries(&mut self, entries: Vec<CharTableEntry>) {
        self.ascii_entry_indices = Self::build_ascii_entry_indices(&entries);
        self.entries = entries;
        *self.resolved_ranges.get_mut() = None;
    }

    pub(crate) fn clear_entries(&mut self) {
        self.entries.clear();
        self.ascii_entry_indices = None;
        *self.resolved_ranges.get_mut() = None;
    }

    /// Overlay `[start, end] -> index' onto a non-overlapping range map,
    /// trimming or splitting whatever older ranges it eclipses.
    fn overlay_resolved_range(map: &mut ResolvedCharRanges, start: u32, end: u32, index: usize) {
        if let Some((&prev_start, &(prev_end, prev_index))) = map.range(..start).next_back()
            && prev_end >= start
        {
            map.insert(prev_start, (start - 1, prev_index));
            if prev_end > end {
                map.insert(end + 1, (prev_end, prev_index));
            }
        }
        let eclipsed: Vec<u32> = map.range(start..=end).map(|(&s, _)| s).collect();
        for eclipsed_start in eclipsed {
            let (eclipsed_end, eclipsed_index) = map
                .remove(&eclipsed_start)
                .expect("resolved range vanished mid-overlay");
            if eclipsed_end > end {
                map.insert(end + 1, (eclipsed_end, eclipsed_index));
            }
        }
        map.insert(start, (end, index));
    }

    fn with_resolved_ranges<R>(&self, read: impl FnOnce(&ResolvedCharRanges) -> R) -> R {
        let mut borrow = self.resolved_ranges.borrow_mut();
        let map = borrow.get_or_insert_with(|| {
            let mut map = ResolvedCharRanges::new();
            for (index, entry) in self.entries.iter().enumerate() {
                Self::overlay_resolved_range(&mut map, entry.start, entry.end, index);
            }
            map
        });
        read(map)
    }

    pub(crate) fn explicit_entry(&self, key: u32) -> Option<&CharTableEntry> {
        if key < 128 {
            let index = *self.ascii_entry_indices.as_ref()?.get(key as usize)?;
            return (index != usize::MAX)
                .then_some(index)
                .and_then(|index| self.entries.get(index));
        }
        self.with_resolved_ranges(|map| {
            let (_, &(end, index)) = map.range(..=key).next_back()?;
            (end >= key).then_some(index)
        })
        .and_then(|index| self.entries.get(index))
    }

    /// The effective explicit ranges in ascending character order: newer log
    /// entries mask older ones, and nil writes mask without being reported
    /// as values.
    pub(crate) fn effective_ranges(&self) -> Vec<CharTableEntry> {
        self.with_resolved_ranges(|map| {
            map.iter()
                .filter_map(|(&start, &(end, index))| {
                    let value = &self.entries[index].value;
                    (!value.is_nil()).then(|| CharTableEntry {
                        start,
                        end,
                        value: value.clone(),
                    })
                })
                .collect()
        })
    }

    /// Append points in `[start, end]` at which this table's effective
    /// explicit range can change.  `resolved_ranges` already folds the
    /// append-only write log into the current, non-overlapping view, so
    /// callers do not need to rescan every historical write.
    pub(crate) fn append_change_boundaries(&self, start: u32, end: u32, boundaries: &mut Vec<u32>) {
        self.with_resolved_ranges(|map| {
            if let Some((_, &(range_end, _))) = map.range(..start).next_back()
                && range_end >= start
                && range_end < end
            {
                boundaries.push(range_end + 1);
            }

            for (&range_start, &(range_end, _)) in map.range(start..=end) {
                boundaries.push(range_start);
                if range_end < end {
                    boundaries.push(range_end + 1);
                }
            }
        });
    }
}

/// GNU vectorlike representation carried by Emaxx's shared record arena.
///
/// `Value::Record` is an internal storage choice, not an Elisp type.  Keep
/// the public pseudovector kind explicit so a real `(record 'thread ...)' is
/// never confused with a native thread merely because their printed type
/// names happen to match.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RecordKind {
    Record,
    BoolVector,
    Closure,
    Font,
    SymbolWithPos,
    Process,
    HashTable,
    Obarray,
    Window,
    WindowConfiguration,
    Thread,
    Mutex,
    ConditionVariable,
    NativeCompUnit,
    NativeCompiledFunction,
    TreeSitterParser,
    TreeSitterNode,
    TreeSitterCompiledQuery,
    Sqlite,
    /// Identity-bearing host facade whose public Elisp representation is a
    /// list.  GNU itself therefore never exposes this as a pseudovector.
    Keymap,
}

impl RecordKind {
    fn gnu_vector_slots(self, logical_slots: usize) -> usize {
        match self {
            // alloc.c:allocate_record stores the type as the first payload
            // word; vector accounting also includes the one-word header.
            Self::Record => logical_slots.saturating_add(2),
            // Both interpreted and byte-code closures are ordinary vectors
            // retagged PVEC_CLOSURE.
            Self::Closure => logical_slots.saturating_add(1),
            // lisp.h:Lisp_Bool_Vector is header + bit count + packed words.
            Self::BoolVector => 2_usize.saturating_add(logical_slots.div_ceil(64)),
            Self::SymbolWithPos => 3,
            // Verified from the configured GNU headers: 72 and 24 bytes.
            Self::HashTable => 9,
            Self::Obarray => 3,
            // Both configured structs are 88 bytes on the supported GNU
            // 64-bit ABI (comp.h and lisp.h:Lisp_Subr).
            Self::NativeCompUnit | Self::NativeCompiledFunction => 11,
            // Keymaps are Lisp cons structures in GNU, not pseudovectors.
            Self::Keymap => 0,
            // These fixed-layout host objects are added as their C allocation
            // sites are mapped; never substitute the Rust struct size.
            Self::Font
            | Self::Process
            | Self::Window
            | Self::WindowConfiguration
            | Self::Thread
            | Self::Mutex
            | Self::ConditionVariable
            | Self::TreeSitterParser
            | Self::TreeSitterNode
            | Self::TreeSitterCompiledQuery
            | Self::Sqlite => 0,
        }
    }
}

pub(crate) fn gnu_hash_grown_capacity(mut capacity: usize, high_water: usize) -> usize {
    while capacity < high_water {
        if capacity == 0 {
            capacity = 6;
            continue;
        }
        let base = capacity.max(6);
        capacity = if base <= 64 {
            base.saturating_mul(4)
        } else {
            base.saturating_mul(2)
        };
        if capacity == usize::MAX {
            break;
        }
    }
    capacity
}

pub(crate) fn gnu_hash_table_storage_bytes(capacity: usize) -> usize {
    if capacity == 0 {
        return 0;
    }
    let rounded = capacity.checked_next_power_of_two().unwrap_or(usize::MAX);
    let index_slots = if rounded == capacity {
        rounded.saturating_mul(2)
    } else {
        rounded
    };
    // fns.c: two Lisp_Object array words per entry, one u32 hash, one i32
    // next link, and a power-of-two i32 bucket index.
    capacity
        .saturating_mul(24)
        .saturating_add(index_slots.saturating_mul(4))
}

pub(crate) fn gnu_hash_table_index_slots(capacity: usize) -> usize {
    if capacity == 0 {
        return 1;
    }
    let rounded = capacity.checked_next_power_of_two().unwrap_or(usize::MAX);
    if rounded == capacity {
        rounded.saturating_mul(2)
    } else {
        rounded
    }
}

#[derive(Clone, Debug)]
pub struct RecordState {
    pub id: u64,
    /// GNU stores the record type in slot zero and permits either a symbol or
    /// an arbitrary type descriptor there.  Keep the Lisp object itself as
    /// the single source of truth; host pseudovectors use symbol tags.
    pub type_tag: Value,
    pub slots: Vec<Value>,
    pub(crate) kind: RecordKind,
}

impl RecordState {
    pub(crate) fn symbol_type_name(&self) -> Option<&str> {
        self.type_tag.as_symbol().ok()
    }

    pub(crate) fn has_symbol_type(&self, name: &str) -> bool {
        self.symbol_type_name() == Some(name)
    }
}

/// Image-template clone semantics (see ProcessState::clone): live
/// tree-sitter handles are not template state; a pristine template holds
/// none, so cloning one is a caller bug.
impl Clone for TreeSitterQueryState {
    fn clone(&self) -> Self {
        panic!("image-template clone with a live tree-sitter query");
    }
}
impl Clone for TreeSitterLanguageState {
    fn clone(&self) -> Self {
        panic!("image-template clone with a loaded tree-sitter language");
    }
}
impl Clone for TreeSitterParserState {
    fn clone(&self) -> Self {
        panic!("image-template clone with a live tree-sitter parser");
    }
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum SpecialBindingScope {
    Global,
    BufferLocal(u64),
}

#[derive(Clone, Debug)]
pub(crate) struct SpecialBindingRestore {
    binding_id: u64,
    name: String,
    scope: SpecialBindingScope,
    binding_buffer_id: Option<u64>,
    previous: Option<Value>,
    previous_undo_state: Option<crate::buffer::UndoState>,
    // `kill-all-local-variables' can remove a buffer-local value while a
    // dynamic binding of that value is active.  The binding then stops
    // participating in lookup and assignment.  On unwind GNU restores the
    // old local value only if code created a new local cell in the meantime.
    local_binding_killed: bool,
}

#[derive(Clone, Debug)]
struct BacktraceFrame {
    function: Value,
    args: Vec<Value>,
    /// eval.c:record_in_backtrace retains the caller's Lisp_Object argument
    /// vector.  Native Ffuncall already has that exact word vector, so keep
    /// it lazy and materialize Values only if Lisp inspects the frame.
    native_args: Option<smallvec::SmallVec<[usize; 8]>>,
    /// Original list form for an unevaluated frame.  GNU backtraces retain
    /// the live Lisp form; keeping it here avoids cloning its function symbol
    /// and every argument on each interpreted call.  Debugger-facing APIs
    /// materialize the two projections only when somebody inspects a frame.
    source_form: Option<Value>,
    locals: Vec<(String, Value)>,
    /// Snapshot of the evaluator environment at this activation while a
    /// debugger is active.  Frames retain their identity stamps so
    /// `backtrace-eval' assignments can update the suspended lexical cells.
    lexical_context: Option<Env>,
    evald: bool,
    debug_on_exit: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct BatchErrorBacktrace {
    pub(crate) enabled: bool,
    pub(crate) frames: Vec<(bool, Value, Vec<Value>, bool)>,
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
    /// Run a real callable to completion in one scheduler step.  Emaxx has no
    /// preemptive threads, so such a body cannot interleave with the main
    /// thread; that is a tracked architectural gap, not something to simulate.
    Call(Value),
    Noop,
    /// Body shapes the cooperative scheduler can actually step.  These are
    /// recognised by shape only — never by function or variable name.
    Sleep {
        blocked: bool,
    },
    InfiniteYield,
    SignalMainThread,
}

#[derive(Clone, Debug)]
enum ThreadOutcome {
    /// `delivered' distinguishes a signal INJECTED with `thread-signal' from
    /// an error the body raised itself.  GNU keeps them in different places:
    /// a body error is caught by thread.c:815's internal_condition_case,
    /// recorded for `thread-last-error', and the thread finishes with a nil
    /// result -- `thread-join' returns nil.  `thread-signal' sets the
    /// target's `error_symbol', and `Fthread_join' SNAPSHOTS that field on
    /// entry (thread.c:1081) and re-raises it after the target dies
    /// (thread.c:1088) -- which is what threads-mutex-signal requires: the
    /// injected `quit' comes out of the JOIN.
    Returned(Value),
    Signaled {
        value: Value,
        delivered: bool,
    },
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

/// One marker-tracked entry in editfns.c's labeled restriction stack.
///
/// The sentinel is represented structurally because GNU uses an uninterned
/// symbol for it; ordinary labels retain their Lisp identity and are compared
/// with `eq', not by their printed names.
#[derive(Clone)]
pub(crate) struct LabeledRestriction {
    buffer_id: u64,
    label: Option<Value>,
    beg_marker_id: u64,
    end_marker_id: u64,
}

/// Rust representation of the state saved by GNU C's
/// `record_unwind_protect_excursion`/`save_excursion_save` pair.
pub(crate) struct SavedExcursion {
    buffer_id: u64,
    point: usize,
    marker_id: u64,
}

/// Rust representation of the state saved by GNU C's
/// `save_restriction_save`.  Marker-backed bounds retain their position
/// across edits exactly like the interpreter's `save-restriction` path.
pub(crate) struct SavedRestriction {
    buffer_id: u64,
    bounds: SavedRestrictionBounds,
    labeled: Vec<LabeledRestriction>,
}

enum SavedRestrictionBounds {
    Wide,
    Narrow {
        beginning: usize,
        end: usize,
        beginning_marker_id: u64,
        end_marker_id: u64,
    },
}

/// A memoized funcall resolution for a symbol callee (see
/// `function_resolution_cache`).
#[derive(Clone)]
pub(crate) enum FunctionResolution {
    /// Direct native dispatch by name is valid; carries the name facts so
    /// the hit path performs no facts probe at all.
    DirectBuiltin(crate::lisp::primitives::NameFacts),
    /// The name resolved to this exact function value.
    Resolved(Value),
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
    /// fns.c:Lisp_Hash_Table.table_size.  This is allocated capacity, not
    /// entry count; removals and `clrhash' deliberately retain it.
    capacity: usize,
    entries: Vec<(Value, Value)>,
    // fns.c stores entries in stable key/value slots.  Removing an entry
    // links its slot onto a LIFO free list; reinsertion reuses that slot,
    // and DOHASH/maphash walk slots in numeric order.  Keep the compact Rust
    // entry vector sorted by these slot numbers so public iteration has the
    // same order without giving up contiguous storage.
    slot_indices: Vec<usize>,
    free_slots: Vec<usize>,
    next_slot: usize,
    key_index: HashMap<Option<i64>, Vec<usize>, crate::lisp::primitives::FnvBuildHasher>,
}

/// Indexed storage for hash tables with an Elisp-defined test.  GNU fns.c
/// calls the Elisp hash function once for a probe, follows only that bucket,
/// and calls the Elisp comparator for collisions.  Keep the returned hashes
/// beside the entries so existing keys are not re-hashed on every lookup.
#[derive(Clone)]
struct CustomHashTableState {
    /// fns.c:Lisp_Hash_Table.table_size.
    capacity: usize,
    entries: Vec<(Value, Value)>,
    hashes: Vec<i64>,
    slot_indices: Vec<usize>,
    free_slots: Vec<usize>,
    next_slot: usize,
    key_index: HashMap<i64, Vec<usize>, crate::lisp::primitives::FnvBuildHasher>,
}

impl CustomHashTableState {
    fn empty(capacity: usize) -> Self {
        Self {
            capacity,
            entries: Vec::new(),
            hashes: Vec::new(),
            slot_indices: Vec::new(),
            free_slots: Vec::new(),
            next_slot: 0,
            key_index: HashMap::default(),
        }
    }

    fn rebuild_index(&mut self) {
        self.key_index.clear();
        for (index, hash) in self.hashes.iter().copied().enumerate() {
            self.key_index.entry(hash).or_default().push(index);
        }
    }
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
    /// Network connection or TLS negotiation failed.
    Failed,
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
            Self::Failed => "failed",
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
    /// The pty slave's device path ("/dev/ttysNNN"), when this process runs
    /// on a pseudo-terminal.  `process-tty-name' reports it; python.el's
    /// send path branches on it to route long lines through a temp file
    /// instead of overflowing the canonical 1024-byte line buffer.
    pub(crate) pty_slave_name: Option<String>,
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

/// Clone panics on a live TLS session (image-template semantics).
impl Clone for ProcessGnuTlsState {
    fn clone(&self) -> Self {
        if self.session.is_some() {
            panic!("image-template clone with a live gnutls session");
        }
        Self {
            boot_parameters: self.boot_parameters.clone(),
            initstage: self.initstage,
            active: self.active,
            session: None,
            peer_status: self.peer_status.clone(),
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

/// Image-template clone semantics: an interpreter holding live external
/// resources (child processes, sqlite handles, tree-sitter parsers) is not
/// a valid template; cloning one is a caller bug and panics rather than
/// silently sharing or dropping a live handle.
impl Clone for ProcessState {
    fn clone(&self) -> Self {
        if self.runtime.is_some() || self.network.is_some() || self.serial.is_some() {
            panic!("image-template clone with a live process runtime");
        }
        Self {
            record_id: self.record_id,
            kind: self.kind,
            buffer_id: self.buffer_id,
            mark_marker_id: self.mark_marker_id,
            status: self.status.clone(),
            filter: self.filter.clone(),
            sentinel: self.sentinel.clone(),
            sentinel_notified: self.sentinel_notified,
            log: self.log.clone(),
            name: self.name.clone(),
            thread_id: self.thread_id,
            query_on_exit_flag: self.query_on_exit_flag,
            traffic_stopped: self.traffic_stopped,
            inherit_coding_system_flag: self.inherit_coding_system_flag,
            decoding: self.decoding.clone(),
            encoding: self.encoding.clone(),
            program: self.program.clone(),
            argv: self.argv.clone(),
            stderr_process_id: self.stderr_process_id,
            exit_code: self.exit_code,
            exit_signal: self.exit_signal,
            os_pid: self.os_pid,
            runtime: None,
            network: None,
            serial: None,
            contact_host: self.contact_host.clone(),
            contact_service: self.contact_service,
            remote: self.remote.clone(),
            parent_server_id: self.parent_server_id,
            pending_stdout: self.pending_stdout.clone(),
            pending_stderr: self.pending_stderr.clone(),
            output_delivery_count: self.output_delivery_count,
            plist: self.plist.clone(),
            gnutls: self.gnutls.clone(),
            contact: self.contact.clone(),
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
    root_window_id: u64,
    frame_width: i64,
    frame_height: i64,
}

#[derive(Clone)]
struct FileNotifyWatch {
    path: Option<String>,
    callback: Value,
    active: bool,
    fingerprint: Option<FileNotifyFingerprint>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum FileNotifyFingerprint {
    Missing,
    Present {
        modified: Option<SystemTime>,
        len: u64,
        is_directory: bool,
    },
}

#[derive(Clone)]
struct PendingFileNotification {
    path: String,
    action: String,
    callbacks: Vec<(i64, Value)>,
}

#[derive(Clone)]
pub(crate) struct FileNameHandlerMatchCacheEntry {
    pub(crate) handler_alist: Value,
    pub(crate) cons_epoch: crate::lisp::types::ConsMutationEpoch,
    pub(crate) definition_generation: u64,
    pub(crate) pattern_snapshots: Vec<(Value, String)>,
    pub(crate) matches: Vec<(usize, Value)>,
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

fn tty_default_lisp_face_vector() -> Value {
    let mut attributes =
        std::iter::repeat_n(Value::symbol("unspecified"), LFACE_VECTOR_SIZE).collect::<Vec<_>>();
    attributes[0] = Value::symbol("face");
    attributes[9] = Value::String("unspecified-fg".into());
    attributes[10] = Value::String("unspecified-bg".into());
    Value::list(std::iter::once(Value::symbol("vector-literal")).chain(attributes))
}

type OrderedBindings = LinkedHashMap<String, Value, crate::lisp::primitives::FnvBuildHasher>;

fn ordered_bindings(entries: impl IntoIterator<Item = (String, Value)>) -> OrderedBindings {
    let mut bindings = OrderedBindings::with_hasher(Default::default());
    bindings.extend(entries);
    bindings
}

type OrderedHooks = LinkedHashMap<String, Vec<Value>, crate::lisp::primitives::FnvBuildHasher>;

fn ordered_hooks(entries: impl IntoIterator<Item = (String, Vec<Value>)>) -> OrderedHooks {
    let mut hooks = OrderedHooks::with_hasher(Default::default());
    hooks.extend(entries);
    hooks
}

type BufferLocalBindings = HashMap<u64, OrderedBindings, crate::lisp::primitives::FnvBuildHasher>;
type BufferLocalHooks = HashMap<u64, OrderedHooks, crate::lisp::primitives::FnvBuildHasher>;

type OrderedNameIndex = HashMap<String, usize, crate::lisp::primitives::FnvBuildHasher>;

/// Build a last-wins index over an ordered symbol/value registry.
///
/// The vector remains the canonical, deterministic representation used for
/// enumeration.  Mutations use this index instead of duplicating each live
/// Lisp value in a second container.
fn ordered_name_index(entries: &[(String, Value)]) -> OrderedNameIndex {
    entries
        .iter()
        .enumerate()
        .map(|(index, (name, _))| (name.clone(), index))
        .collect()
}

#[derive(Clone, Debug, Default)]
pub(crate) struct MinibufferRuntimeState {
    active_buffer_id: Option<u64>,
    active_window_id: Option<u64>,
    depth: usize,
    prompt: Option<String>,
}

/// The interpreter state: holds the global environment, the current buffer,
/// and ERT test results.
/// Issue #11: a graph-preserving deep copy of every Lisp value reachable
/// from an image-template interpreter.  `Interpreter::clone' copies the
/// host-side tables but SHARES the Rc value graphs, so interior mutation
/// (setcar on a preloaded list, put-text-property on a preloaded string, a
/// closure environment assignment) would bleed between the template and
/// its clones -- the eval_02 validation run caught exactly that.  The
/// copier rebuilds each mutable node once (memo per Rc identity, so
/// sharing *inside* the clone is preserved) and leaves immutable
/// representations (interned strings, big integers, symbols) shared.
struct ImageGraphCopier {
    cons: std::collections::HashMap<usize, Value>,
    strings: std::collections::HashMap<usize, Value>,
    lambdas: std::collections::HashMap<usize, Value>,
    reader_forms: std::collections::HashMap<usize, Value>,
    envs: std::collections::HashMap<usize, crate::lisp::types::SharedEnv>,
    /// Old-env pointers whose shell has been (or is being) filled.  Marked
    /// before the fill so a recursive closure (lambda -> captured env ->
    /// same lambda) terminates: the inner visit sees the shell and stops.
    envs_filled: std::collections::HashSet<usize>,
    frames: std::collections::HashMap<usize, crate::lisp::types::EnvFrame>,
}

impl ImageGraphCopier {
    fn new() -> Self {
        Self {
            cons: Default::default(),
            strings: Default::default(),
            lambdas: Default::default(),
            reader_forms: Default::default(),
            envs: Default::default(),
            envs_filled: Default::default(),
            frames: Default::default(),
        }
    }

    fn copy(&mut self, value: &Value) -> Value {
        match value {
            Value::Cons(_) => self.copy_cons_chain(value),
            Value::StringObject(state) => {
                let key = std::rc::Rc::as_ptr(state) as usize;
                if let Some(copied) = self.strings.get(&key) {
                    return copied.clone();
                }
                let mut inner = state.borrow().clone();
                let copied_state = std::rc::Rc::new(std::cell::RefCell::new(
                    crate::lisp::types::SharedStringState {
                        text: std::mem::take(&mut inner.text),
                        props: Vec::new(),
                        multibyte: inner.multibyte,
                        extended_chars: std::mem::take(&mut inner.extended_chars),
                    },
                ));
                crate::lisp::types::register_string_object(&copied_state);
                let copied = Value::StringObject(copied_state);
                self.strings.insert(key, copied.clone());
                let props = state.borrow().props.clone();
                let copied_props = props
                    .iter()
                    .map(|span| crate::lisp::types::StringPropertySpan {
                        start: span.start,
                        end: span.end,
                        props: span
                            .props
                            .iter()
                            .map(|(name, prop)| (name.clone(), self.copy(prop)))
                            .collect(),
                    })
                    .collect();
                if let Value::StringObject(new_state) = &copied {
                    new_state.borrow_mut().props = copied_props;
                }
                copied
            }
            Value::Lambda(lambda) => {
                let key = std::rc::Rc::as_ptr(lambda) as usize;
                if let Some(copied) = self.lambdas.get(&key) {
                    return copied.clone();
                }
                let copied = Value::allocated_lambda(crate::lisp::types::LambdaValue {
                    params: lambda.params.clone(),
                    public_parameters: lambda
                        .public_parameters
                        .as_ref()
                        .map(|parameters| self.copy(parameters)),
                    body: std::rc::Rc::new(
                        lambda.body.iter().map(|form| self.copy(form)).collect(),
                    ),
                    env: self.env_shell(&lambda.env),
                    documentation: lambda
                        .documentation
                        .as_ref()
                        .map(|documentation| self.copy(documentation)),
                    interactive: lambda
                        .interactive
                        .as_ref()
                        .map(|interactive| self.copy(interactive)),
                    public_environment: lambda
                        .public_environment
                        .as_ref()
                        .map(|environment| self.copy(environment)),
                });
                self.lambdas.insert(key, copied.clone());
                self.fill_env(&lambda.env);
                copied
            }
            Value::ReaderForm(form) => {
                let key = std::rc::Rc::as_ptr(form) as usize;
                if let Some(copied) = self.reader_forms.get(&key) {
                    return copied.clone();
                }
                let copied = Value::ReaderForm(std::rc::Rc::new(match form.as_ref() {
                    crate::lisp::types::ReaderForm::CircularLabel { id, payload } => {
                        crate::lisp::types::ReaderForm::CircularLabel {
                            id: *id,
                            payload: self.copy(payload),
                        }
                    }
                    crate::lisp::types::ReaderForm::CircularReference(id) => {
                        crate::lisp::types::ReaderForm::CircularReference(*id)
                    }
                    crate::lisp::types::ReaderForm::HashTable { fields } => {
                        crate::lisp::types::ReaderForm::HashTable {
                            fields: fields.iter().map(|field| self.copy(field)).collect(),
                        }
                    }
                    crate::lisp::types::ReaderForm::CharTable { fields } => {
                        crate::lisp::types::ReaderForm::CharTable {
                            fields: fields.iter().map(|field| self.copy(field)).collect(),
                        }
                    }
                    crate::lisp::types::ReaderForm::SubCharTable { fields } => {
                        crate::lisp::types::ReaderForm::SubCharTable {
                            fields: fields.iter().map(|field| self.copy(field)).collect(),
                        }
                    }
                    crate::lisp::types::ReaderForm::Record { slots } => {
                        crate::lisp::types::ReaderForm::Record {
                            slots: slots.iter().map(|slot| self.copy(slot)).collect(),
                        }
                    }
                    crate::lisp::types::ReaderForm::Closure { kind, slots } => {
                        crate::lisp::types::ReaderForm::Closure {
                            kind: *kind,
                            slots: slots.iter().map(|slot| self.copy(slot)).collect(),
                        }
                    }
                    crate::lisp::types::ReaderForm::BoolVector { bits } => {
                        crate::lisp::types::ReaderForm::BoolVector { bits: bits.clone() }
                    }
                    crate::lisp::types::ReaderForm::PositionedSymbol { name, pos } => {
                        crate::lisp::types::ReaderForm::PositionedSymbol {
                            name: name.clone(),
                            pos: *pos,
                        }
                    }
                }));
                self.reader_forms.insert(key, copied.clone());
                copied
            }
            other => other.clone(),
        }
    }

    /// Copy a cons chain iteratively along the cdr spine: preloaded lists
    /// run to tens of thousands of elements, far past the stack budget a
    /// naive recursive copy would need.
    fn copy_cons_chain(&mut self, head: &Value) -> Value {
        let mut spine = Vec::new();
        let mut cursor = head.clone();
        while let Value::Cons(cell) = &cursor {
            let key = crate::lisp::types::ConsCell::identity(cell);
            if self.cons.contains_key(&key) {
                break;
            }
            let placeholder = Value::cons(Value::Nil, Value::Nil);
            self.cons.insert(key, placeholder.clone());
            spine.push((cell.clone(), placeholder.clone()));
            let next = cell.cdr.borrow().clone();
            cursor = next;
        }
        for (source, copied) in spine.into_iter().rev() {
            let car = source.car.borrow().clone();
            let cdr = source.cdr.borrow().clone();
            let copied_car = self.copy(&car);
            let copied_cdr = self.copy(&cdr);
            let Value::Cons(cell) = &copied else {
                unreachable!("cons placeholder is a cons")
            };
            *cell.car.borrow_mut() = copied_car;
            *cell.cdr.borrow_mut() = copied_cdr;
        }
        self.copy_known(head)
    }

    fn copy_known(&mut self, value: &Value) -> Value {
        if let Value::Cons(cell) = value {
            let key = crate::lisp::types::ConsCell::identity(cell);
            if let Some(copied) = self.cons.get(&key) {
                return copied.clone();
            }
        }
        self.copy(value)
    }

    /// Get or create the copy of ENV without filling its frames yet.  A
    /// lambda memoizes itself between taking the shell and filling it, so
    /// a recursive closure (lambda -> captured env -> same lambda) hits
    /// the lambda memo instead of copying itself twice.
    fn env_shell(&mut self, env: &crate::lisp::types::SharedEnv) -> crate::lisp::types::SharedEnv {
        let key = std::rc::Rc::as_ptr(env) as usize;
        if let Some(copied) = self.envs.get(&key) {
            return copied.clone();
        }
        let shell: crate::lisp::types::SharedEnv =
            std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        self.envs.insert(key, shell.clone());
        shell
    }

    fn fill_env(&mut self, env: &crate::lisp::types::SharedEnv) {
        let key = std::rc::Rc::as_ptr(env) as usize;
        if !self.envs_filled.insert(key) {
            return;
        }
        let copied = self.env_shell(env);
        let frames = env.borrow().clone();
        let copied_frames = frames
            .iter()
            .map(|frame| self.copy_frame(frame))
            .collect::<Vec<_>>();
        *copied.borrow_mut() = copied_frames;
    }

    fn copy_frame(&mut self, frame: &crate::lisp::types::EnvFrame) -> crate::lisp::types::EnvFrame {
        let key = frame.identity_ptr();
        if let Some(copied) = self.frames.get(&key) {
            return copied.clone();
        }
        let copied = frame.deep_copy_with(&mut |value| self.copy(value));
        self.frames.insert(key, copied.clone());
        copied
    }
}

/// Finding 110: the live-object counts behind `garbage-collect'.
///
/// GNU's numbers come from allocator bookkeeping (gcstat), not from a
/// heap walk; emaxx keeps the equivalent books in types.rs -- a live
/// cons-cell counter maintained at construction and Drop, and Weak
/// registries of string allocations swept lazily.  What each field means
/// here, where the object models differ:
///
/// - `conses' counts every live cons cell, vector-literal spines
///   included: vectors ride on conses internally, so cons cells are where
///   their storage truthfully is.  `vectors'/`vector_slots' are 0 -- no
///   vector heap objects exist in this implementation.
/// - `floats' is 0: emaxx floats are immediate f64s, not heap cells.
/// - `intervals' counts text-property spans (buffer spans plus string
///   spans), the closest live analogue of GNU's interval tree nodes.
/// - Markers, overlays, char-tables, frames and records are id-indexed
///   host state rather than Lisp heap objects and have no row of their
///   own.  Records are never reclaimed, so values they reference remain
///   live allocations and stay counted.
#[derive(Default)]
pub(crate) struct LiveObjectCensus {
    pub(crate) conses: usize,
    pub(crate) symbols: usize,
    pub(crate) strings: usize,
    pub(crate) string_bytes: usize,
    pub(crate) vectors: usize,
    pub(crate) vector_slots: usize,
    pub(crate) floats: usize,
    pub(crate) intervals: usize,
    pub(crate) buffers: usize,
    pub(crate) hash_table_bytes: usize,
}

#[derive(Default)]
struct LispReachability {
    big_integers: HashSet<usize>,
    floats: HashSet<usize>,
    strings: HashSet<usize>,
    string_objects: HashSet<usize>,
    symbols: HashSet<String>,
    conses: HashSet<usize>,
    lambdas: HashSet<usize>,
    buffers: HashSet<usize>,
    markers: HashSet<u64>,
    overlays: HashSet<u64>,
    char_tables: HashSet<u64>,
    frames: HashSet<u64>,
    terminals: HashSet<u64>,
    records: HashSet<u64>,
    finalizers: HashSet<u64>,
    reader_forms: HashSet<usize>,
}

pub(crate) struct WeakHashReachability {
    pub(crate) tables: Vec<WeakHashTableReachability>,
    pub(crate) live_records: HashSet<u64>,
}

pub(crate) type WeakHashTableReachability = (u64, Vec<(Value, Value)>, Vec<bool>);

impl LispReachability {
    fn contains(&self, value: &Value) -> bool {
        match value {
            Value::Nil | Value::T | Value::Integer(_) | Value::BuiltinFunc(_) | Value::Unbound => {
                true
            }
            Value::BigInteger(value) => self.big_integers.contains(&value.identity_ptr()),
            Value::Float(value) => self.floats.contains(&value.identity_ptr()),
            Value::String(value) => self.strings.contains(&value.identity_ptr()),
            Value::StringObject(value) => {
                self.string_objects.contains(&(Rc::as_ptr(value) as usize))
            }
            Value::Symbol(symbol) => {
                crate::lisp::types::visible_symbol_name(symbol) == symbol.as_str()
                    || self.symbols.contains(symbol.as_str())
            }
            Value::Cons(value) => self.conses.contains(&ConsCell::identity(value)),
            Value::Lambda(value) => self.lambdas.contains(&(Rc::as_ptr(value) as usize)),
            Value::Buffer(value) => self.buffers.contains(&(Rc::as_ptr(value) as usize)),
            Value::Marker(id) => self.markers.contains(id),
            Value::Overlay(id) => self.overlays.contains(id),
            Value::CharTable(id) => self.char_tables.contains(id),
            Value::Frame(id) => self.frames.contains(id),
            Value::Terminal(id) => self.terminals.contains(id),
            Value::Record(id) => self.records.contains(id),
            Value::Finalizer(id) => self.finalizers.contains(id),
            Value::ReaderForm(value) => self.reader_forms.contains(&(Rc::as_ptr(value) as usize)),
        }
    }

    fn mark_env(&mut self, interp: &Interpreter, env: &Env) -> bool {
        let mut changed = false;
        for frame in env {
            for (_, value) in frame {
                changed |= self.mark(interp, value);
            }
            if let Some(environment) = frame.lisp_environment() {
                changed |= self.mark(interp, environment);
            }
        }
        changed
    }

    fn mark(&mut self, interp: &Interpreter, value: &Value) -> bool {
        let newly_marked = match value {
            Value::Nil | Value::T | Value::Integer(_) | Value::BuiltinFunc(_) | Value::Unbound => {
                false
            }
            Value::BigInteger(value) => self.big_integers.insert(value.identity_ptr()),
            Value::Float(value) => self.floats.insert(value.identity_ptr()),
            Value::String(value) => self.strings.insert(value.identity_ptr()),
            Value::StringObject(value) => self.string_objects.insert(Rc::as_ptr(value) as usize),
            Value::Symbol(symbol) => self.symbols.insert(symbol.as_str().to_owned()),
            Value::Cons(value) => self.conses.insert(ConsCell::identity(value)),
            Value::Lambda(value) => self.lambdas.insert(Rc::as_ptr(value) as usize),
            Value::Buffer(value) => self.buffers.insert(Rc::as_ptr(value) as usize),
            Value::Marker(id) => self.markers.insert(*id),
            Value::Overlay(id) => self.overlays.insert(*id),
            Value::CharTable(id) => self.char_tables.insert(*id),
            Value::Frame(id) => self.frames.insert(*id),
            Value::Terminal(id) => self.terminals.insert(*id),
            Value::Record(id) => self.records.insert(*id),
            Value::Finalizer(id) => self.finalizers.insert(*id),
            Value::ReaderForm(value) => self.reader_forms.insert(Rc::as_ptr(value) as usize),
        };
        if !newly_marked {
            return false;
        }

        match value {
            Value::StringObject(value) => {
                let children = value
                    .borrow()
                    .props
                    .iter()
                    .flat_map(|span| span.props.iter().map(|(_, value)| value.clone()))
                    .collect::<Vec<_>>();
                for child in &children {
                    self.mark(interp, child);
                }
            }
            Value::Cons(cell) => {
                let children = [
                    Value::Cons(cell.clone()).car(),
                    Value::Cons(cell.clone()).cdr(),
                ];
                for child in children.into_iter().flatten() {
                    self.mark(interp, &child);
                }
            }
            Value::Lambda(lambda) => {
                if let Some(value) = &lambda.public_parameters {
                    self.mark(interp, value);
                }
                for value in lambda.body.iter() {
                    self.mark(interp, value);
                }
                self.mark_env(interp, &lambda.env.borrow());
                for value in [
                    lambda.documentation.as_ref(),
                    lambda.interactive.as_ref(),
                    lambda.public_environment.as_ref(),
                ]
                .into_iter()
                .flatten()
                {
                    self.mark(interp, value);
                }
            }
            Value::Buffer(buffer) => {
                self.strings.insert(buffer.name.identity_ptr());
            }
            Value::CharTable(id) => {
                if let Some(table) = interp.find_char_table(*id) {
                    let children = std::iter::once(table.default.clone())
                        .chain(table.extra_slots.iter().cloned())
                        .chain(table.entries.iter().map(|entry| entry.value.clone()))
                        .collect::<Vec<_>>();
                    for child in &children {
                        self.mark(interp, child);
                    }
                }
            }
            Value::Frame(id) => {
                if let Some(frame) = interp.frame_states.iter().find(|frame| frame.id == *id) {
                    let children = std::iter::once(frame.name.clone())
                        .chain(
                            frame
                                .parameter_overrides
                                .iter()
                                .map(|(_, value)| value.clone()),
                        )
                        .collect::<Vec<_>>();
                    for child in &children {
                        self.mark(interp, child);
                    }
                }
            }
            Value::Record(id) => {
                let Some(record) = interp.find_record(*id) else {
                    return true;
                };
                let weak_hash = record.kind == RecordKind::HashTable
                    && record.slots.get(5).is_some_and(Value::is_truthy);
                let mut children = Vec::with_capacity(record.slots.len() + 1);
                children.push(record.type_tag.clone());
                children.extend(
                    record
                        .slots
                        .iter()
                        .enumerate()
                        .filter(|(index, _)| record.kind != RecordKind::HashTable || *index != 1)
                        .map(|(_, value)| value.clone()),
                );
                if record.kind == RecordKind::HashTable
                    && !weak_hash
                    && let Some((_, entries)) = crate::lisp::json::hash_table_entries(interp, value)
                {
                    children.extend(entries.into_iter().flat_map(|(key, value)| [key, value]));
                }
                for child in &children {
                    self.mark(interp, child);
                }
            }
            Value::ReaderForm(form) => {
                let children: &[Value] = match form.as_ref() {
                    ReaderForm::CircularLabel { payload, .. } => std::slice::from_ref(payload),
                    ReaderForm::HashTable { fields }
                    | ReaderForm::CharTable { fields }
                    | ReaderForm::SubCharTable { fields }
                    | ReaderForm::Record { slots: fields }
                    | ReaderForm::Closure { slots: fields, .. } => fields,
                    ReaderForm::CircularReference(_)
                    | ReaderForm::BoolVector { .. }
                    | ReaderForm::PositionedSymbol { .. } => &[],
                };
                for child in children {
                    self.mark(interp, child);
                }
            }
            Value::Nil
            | Value::T
            | Value::Integer(_)
            | Value::BigInteger(_)
            | Value::Float(_)
            | Value::String(_)
            | Value::Symbol(_)
            | Value::BuiltinFunc(_)
            | Value::Marker(_)
            | Value::Overlay(_)
            | Value::Terminal(_)
            | Value::Finalizer(_)
            | Value::Unbound => {}
        }
        true
    }
}

impl LiveObjectCensus {
    /// alloc.c:total_bytes_of_live_objects.  These are the GNU C layouts for
    /// the supported 64-bit ABI: Lisp_Cons=16, Lisp_Symbol=48,
    /// Lisp_String=32, Lisp_Float=8, interval=56, and one vector word=8.
    pub(crate) fn total_bytes_of_live_objects(&self) -> usize {
        self.conses
            .saturating_mul(16)
            .saturating_add(self.symbols.saturating_mul(48))
            .saturating_add(self.string_bytes)
            .saturating_add(self.vector_slots.saturating_mul(8))
            .saturating_add(self.floats.saturating_mul(8))
            .saturating_add(self.intervals.saturating_mul(56))
            .saturating_add(self.strings.saturating_mul(32))
            .saturating_add(self.hash_table_bytes)
    }
}

impl Interpreter {
    /// Mark Lisp objects from the interpreter's actual roots, then apply
    /// GNU's iterative weak-hash rule.  Hash entries are deliberately not
    /// roots of a weak table; an entry that survives one table can mark an
    /// object which in turn makes an entry in another table survive, so the
    /// pass repeats to a fixed point exactly like alloc.c.
    pub(crate) fn weak_hash_reachability(
        &self,
        env: &Env,
        native_roots: &[Value],
    ) -> WeakHashReachability {
        let mut marked = LispReachability::default();
        marked.mark_env(self, env);
        for value in native_roots {
            marked.mark(self, value);
        }

        let mut mark = |value: &Value| {
            marked.mark(self, value);
        };
        for (_, value) in self.globals.iter() {
            mark(value);
        }
        for bindings in self.buffer_locals.values() {
            for (_, value) in bindings {
                mark(value);
            }
        }
        for hooks in self.buffer_local_hooks.values() {
            for (_, functions) in hooks {
                for function in functions {
                    mark(function);
                }
            }
        }
        for value in &self.kbd_macro_definition {
            mark(value);
        }
        mark(&self.local_time_zone_rule);
        for (_, value) in &self.symbol_properties {
            mark(value);
        }
        for (_, watchers) in &self.variable_watchers {
            for watcher in watchers {
                mark(watcher);
            }
        }
        if let Some(value) = &self.current_global_map {
            mark(value);
        }
        mark(&self.frame_and_buffer_state);
        for (key, value) in &self.terminal_parameters {
            mark(key);
            mark(value);
        }
        for table in &self.char_tables {
            mark(&Value::CharTable(table.id));
        }
        for (_, value) in &self.charset_plists {
            mark(value);
        }
        for (_, value) in self.ccl_programs.iter().flatten() {
            mark(value);
        }
        for execution in &self.kbd_macro_executions {
            for event in &execution.events {
                mark(event);
            }
        }
        for event in self
            .keyboard_input
            .command_keys
            .iter()
            .chain(&self.keyboard_input.raw_keys)
            .chain(&self.keyboard_input.recent_keys)
        {
            mark(event);
        }
        if let Some(value) = &self.keyboard_input.internal_last_event_frame {
            mark(value);
        }
        for frame in &self.frame_states {
            if frame.live {
                mark(&Value::Frame(frame.id));
            }
        }
        for coding in &self.coding_systems {
            mark(&coding.plist);
        }
        for (_, function) in &self.functions {
            mark(function);
        }
        for updates in self.lexical_cell_updates.values() {
            for value in updates.values() {
                mark(value);
            }
        }
        if let Some(value) = &self.selected_frame_face_hash_table {
            mark(value);
        }
        mark(&self.alternative_font_family_alist);
        mark(&self.alternative_font_registry_alist);
        for (_, value) in &self.deferred_defsubst_unbindings {
            mark(value);
        }
        if let Some(value) = &self.last_thread_error {
            mark(value);
        }
        for value in &self.active_catch_tags {
            mark(value);
        }
        for timer in &self.pending_timers {
            mark(&timer.function);
            for argument in &timer.args {
                mark(argument);
            }
        }
        for notification in &self.pending_file_notifications {
            for (_, callback) in &notification.callbacks {
                mark(callback);
            }
        }
        for watch in self.file_notify_watches.values() {
            mark(&watch.callback);
        }
        for frame in &self.backtrace_frames {
            mark(&frame.function);
            for argument in &frame.args {
                mark(argument);
            }
            if let Some(form) = &frame.source_form {
                mark(form);
            }
            for (_, value) in &frame.locals {
                mark(value);
            }
            if let Some(context) = &frame.lexical_context {
                for lexical_frame in context {
                    for (_, value) in lexical_frame {
                        mark(value);
                    }
                    if let Some(environment) = lexical_frame.lisp_environment() {
                        mark(environment);
                    }
                }
            }
        }
        if let Some(backtrace) = &self.batch_error_backtrace {
            for (_, function, arguments, _) in &backtrace.frames {
                mark(function);
                for argument in arguments {
                    mark(argument);
                }
            }
        }
        for handler in &self.active_handlers {
            match handler {
                ActiveHandler::Bind(_, function) => mark(function),
                ActiveHandler::Case(heads) => {
                    for head in heads {
                        mark(head);
                    }
                }
            }
        }
        for face in &self.lisp_face_states {
            for value in [face.global.as_ref(), face.selected_frame.as_ref()]
                .into_iter()
                .flatten()
            {
                mark(value);
            }
        }
        for bitmap in &self.fringe_bitmap_states {
            if let Some(value) = &bitmap.definition {
                mark(value);
            }
            mark(&bitmap.face);
        }
        for composition in &self.composition_states {
            mark(&composition.components);
        }
        for test in &self.ert_tests {
            mark(&test.body);
        }
        for restore in &self.active_special_restores {
            if let Some(value) = &restore.previous {
                mark(value);
            }
        }
        for restriction in &self.labeled_restrictions {
            if let Some(value) = &restriction.label {
                mark(value);
            }
        }
        for process in &self.process_states {
            mark(&Value::Record(process.record_id));
            for value in [
                process.filter.as_ref(),
                process.sentinel.as_ref(),
                process.log.as_ref(),
            ]
            .into_iter()
            .flatten()
            {
                mark(value);
            }
            for value in [
                &process.decoding,
                &process.encoding,
                &process.plist,
                &process.contact,
            ] {
                mark(value);
            }
        }
        for thread in &self.thread_states {
            mark(&Value::Record(thread.record_id));
            if let ThreadProgram::Call(function) = &thread.program {
                mark(function);
            }
            match &thread.outcome {
                Some(ThreadOutcome::Returned(value))
                | Some(ThreadOutcome::Signaled { value, .. }) => mark(value),
                None => {}
            }
        }
        for value in self.plain_quote_templates.values() {
            mark(&value.value);
        }
        let mut visit_buffer = |value: &Value| {
            mark(value);
        };
        self.buffer.visit_lisp_values(&mut visit_buffer);
        for (_, buffer) in &self.inactive_buffers {
            buffer.visit_lisp_values(&mut visit_buffer);
        }
        for id in [
            self.standard_obarray_id,
            self.selected_window_id,
            self.root_window_id,
            self.minibuffer_window_id,
        ] {
            mark(&Value::Record(id));
        }

        let weak_tables = self
            .records
            .iter()
            .filter(|record| record.kind == RecordKind::HashTable)
            .filter_map(|record| {
                let weakness = record.slots.get(5)?.as_symbol().ok()?.to_owned();
                let entries =
                    crate::lisp::json::hash_table_entries(self, &Value::Record(record.id))?.1;
                Some((record.id, weakness, entries))
            })
            .collect::<Vec<_>>();

        loop {
            let mut changed = false;
            for (id, weakness, entries) in &weak_tables {
                if !marked.records.contains(id) {
                    continue;
                }
                for (key, value) in entries {
                    let strong_key = marked.contains(key);
                    let strong_value = marked.contains(value);
                    let keep = match weakness.as_str() {
                        "key" => strong_key,
                        "value" => strong_value,
                        "key-and-value" => strong_key && strong_value,
                        "key-or-value" => strong_key || strong_value,
                        _ => true,
                    };
                    if keep {
                        changed |= marked.mark(self, key);
                        changed |= marked.mark(self, value);
                    }
                }
            }
            if !changed {
                break;
            }
        }

        let tables = weak_tables
            .into_iter()
            .map(|(id, _, entries)| {
                let keep = if marked.records.contains(&id) {
                    entries
                        .iter()
                        .map(|(key, value)| marked.contains(key) && marked.contains(value))
                        .collect()
                } else {
                    vec![false; entries.len()]
                };
                (id, entries, keep)
            })
            .collect();
        WeakHashReachability {
            tables,
            live_records: marked.records,
        }
    }

    pub(crate) fn install_gc_record_census(&mut self, live_records: HashSet<u64>) {
        self.gc_live_record_ids = live_records;
        self.gc_record_high_water = self.next_record_id;
        self.gc_has_record_census = true;
    }

    /// Assemble the live-object census from the allocation books (see
    /// `LiveObjectCensus' and types.rs's live-object accounting).  This is
    /// O(live strings), never a graph walk: loadup.el runs
    /// `garbage-collect' after every file it loads, so the census is on
    /// the boot path.
    pub(crate) fn live_object_census(&self) -> LiveObjectCensus {
        let strings = crate::lisp::types::census_live_strings();
        let vectors = crate::lisp::types::census_live_vectors();
        let symbols = self
            .known_symbol_count()
            .saturating_add(crate::lisp::types::census_live_uninterned_symbols());
        let mut vector_count = vectors.count;
        let mut vector_slots = vectors.slots;
        for record in self.records.iter().filter(|record| {
            !self.gc_has_record_census
                || record.id >= self.gc_record_high_water
                || self.gc_live_record_ids.contains(&record.id)
        }) {
            let slots = record.kind.gnu_vector_slots(record.slots.len());
            if slots != 0 {
                vector_count += 1;
                vector_slots = vector_slots.saturating_add(slots);
            }
        }
        let mut census = LiveObjectCensus {
            conses: crate::lisp::types::census_live_conses()
                .saturating_sub(vectors.representation_conses),
            symbols,
            strings: strings.count,
            string_bytes: strings.bytes,
            vectors: vector_count,
            vector_slots,
            floats: crate::lisp::types::census_live_floats(),
            intervals: strings.property_spans,
            buffers: 1 + self.inactive_buffers.len(),
            hash_table_bytes: self.gnu_hash_storage_bytes(symbols),
        };
        census.intervals += self.buffer.text_property_span_count();
        for (_, buffer) in &self.inactive_buffers {
            census.intervals += buffer.text_property_span_count();
        }
        census
    }

    fn gnu_hash_storage_bytes(&self, symbol_count: usize) -> usize {
        let mut bytes = 0_usize;
        for record in self.records.iter().filter(|record| {
            !self.gc_has_record_census
                || record.id >= self.gc_record_high_water
                || self.gc_live_record_ids.contains(&record.id)
        }) {
            match record.kind {
                RecordKind::HashTable => {
                    let capacity = self.gnu_hash_table_capacity(record.id).unwrap_or(0);
                    bytes = bytes.saturating_add(gnu_hash_table_storage_bytes(capacity));
                }
                RecordKind::Obarray => {
                    let capacity = if record.id == self.standard_obarray_id {
                        let mut capacity = 1_usize << 15;
                        while symbol_count > capacity {
                            capacity = capacity.saturating_mul(2);
                        }
                        capacity
                    } else {
                        // lread.c:obarray_default_bits is 3.  Nonstandard
                        // obarray growth is accounted when its symbol arena is
                        // moved out of the encoded namespace representation.
                        1_usize << 3
                    };
                    bytes = bytes.saturating_add(capacity.saturating_mul(8));
                }
                _ => {}
            }
        }
        bytes
    }

    /// Issue #11: clone this interpreter for use as an independent image.
    /// `Interpreter::clone' shares every Rc-backed Lisp value with the
    /// original; this method then rewrites all Lisp values reachable from
    /// interpreter state through one memoized graph copy, so mutating a
    /// preloaded object in the clone (setcar, put-text-property, closure
    /// variable assignment, puthash) can never leak back into the
    /// template.  Identity-keyed caches are dropped because the copied
    /// cells have new identities; they repopulate on use.
    pub fn deep_clone_image(&self) -> Interpreter {
        let mut clone = self.clone();
        let mut copier = ImageGraphCopier::new();
        {
            let c = &mut copier;
            for (_, value) in clone.globals.iter_mut() {
                *value = c.copy(value);
            }
            for bindings in clone.buffer_locals.values_mut() {
                for (_, value) in bindings.iter_mut() {
                    *value = c.copy(value);
                }
            }
            for hooks in clone.buffer_local_hooks.values_mut() {
                for (_, functions) in hooks.iter_mut() {
                    for function in functions {
                        *function = c.copy(function);
                    }
                }
            }
            for value in &mut clone.kbd_macro_definition {
                *value = c.copy(value);
            }
            clone.local_time_zone_rule = c.copy(&clone.local_time_zone_rule.clone());
            for (_, value) in &mut clone.symbol_properties {
                *value = c.copy(value);
            }
            for (_, watchers) in &mut clone.variable_watchers {
                for watcher in watchers {
                    *watcher = c.copy(watcher);
                }
            }
            if let Some(map) = &clone.current_global_map {
                clone.current_global_map = Some(c.copy(map));
            }
            clone.frame_and_buffer_state = c.copy(&clone.frame_and_buffer_state.clone());
            for (key, value) in &mut clone.terminal_parameters {
                *key = c.copy(key);
                *value = c.copy(value);
            }
            for table in &mut clone.char_tables {
                table.default = c.copy(&table.default.clone());
                for slot in &mut table.extra_slots {
                    *slot = c.copy(slot);
                }
                for entry in &mut table.entries {
                    entry.value = c.copy(&entry.value.clone());
                }
            }
            for (_, plist) in &mut clone.charset_plists {
                *plist = c.copy(plist);
            }
            for program in clone.ccl_programs.iter_mut().flatten() {
                program.1 = c.copy(&program.1.clone());
            }
            for record in &mut clone.records {
                record.type_tag = c.copy(&record.type_tag.clone());
                for slot in &mut record.slots {
                    *slot = c.copy(slot);
                }
            }
            for execution in &mut clone.kbd_macro_executions {
                for event in &mut execution.events {
                    *event = c.copy(event);
                }
            }
            for event in &mut clone.keyboard_input.command_keys {
                *event = c.copy(event);
            }
            for event in &mut clone.keyboard_input.raw_keys {
                *event = c.copy(event);
            }
            for event in &mut clone.keyboard_input.recent_keys {
                *event = c.copy(event);
            }
            if let Some(frame) = &clone.keyboard_input.internal_last_event_frame {
                clone.keyboard_input.internal_last_event_frame = Some(c.copy(frame));
            }
            for frame in &mut clone.frame_states {
                frame.name = c.copy(&frame.name.clone());
                for (_, value) in &mut frame.parameter_overrides {
                    *value = c.copy(value);
                }
            }
            for table in clone.equal_hash_tables.values_mut() {
                for (key, value) in &mut table.entries {
                    *key = c.copy(key);
                    *value = c.copy(value);
                }
            }
            for coding in &mut clone.coding_systems {
                coding.plist = c.copy(&coding.plist.clone());
            }
            for (_, function) in &mut clone.functions {
                *function = c.copy(function);
            }
            for function in clone.functions_index.values_mut() {
                *function = c.copy(function);
            }
            for updates in clone.lexical_cell_updates.values_mut() {
                for value in updates.values_mut() {
                    *value = c.copy(value);
                }
            }
            if let Some(table) = &clone.selected_frame_face_hash_table {
                clone.selected_frame_face_hash_table = Some(c.copy(table));
            }
            clone.alternative_font_family_alist =
                c.copy(&clone.alternative_font_family_alist.clone());
            clone.alternative_font_registry_alist =
                c.copy(&clone.alternative_font_registry_alist.clone());
            for (_, value) in &mut clone.deferred_defsubst_unbindings {
                *value = c.copy(value);
            }
            if let Some(error) = &clone.last_thread_error {
                clone.last_thread_error = Some(c.copy(error));
            }
            for tag in &mut clone.active_catch_tags {
                *tag = c.copy(tag);
            }
            for timer in &mut clone.pending_timers {
                timer.function = c.copy(&timer.function.clone());
                for arg in &mut timer.args {
                    *arg = c.copy(arg);
                }
            }
            for notification in &mut clone.pending_file_notifications {
                for (_, callback) in &mut notification.callbacks {
                    *callback = c.copy(callback);
                }
            }
            for watch in clone.file_notify_watches.values_mut() {
                watch.callback = c.copy(&watch.callback.clone());
            }
            for frame in &mut clone.backtrace_frames {
                frame.function = c.copy(&frame.function.clone());
                for arg in &mut frame.args {
                    *arg = c.copy(arg);
                }
                if let Some(form) = &frame.source_form {
                    frame.source_form = Some(c.copy(form));
                }
                for (_, value) in &mut frame.locals {
                    *value = c.copy(value);
                }
            }
            if let Some(backtrace) = &mut clone.batch_error_backtrace {
                for (_, function, args, _) in &mut backtrace.frames {
                    *function = c.copy(&function.clone());
                    for arg in args {
                        *arg = c.copy(arg);
                    }
                }
            }
            for handler in &mut clone.active_handlers {
                match handler {
                    ActiveHandler::Bind(_, function) => *function = c.copy(&function.clone()),
                    ActiveHandler::Case(heads) => {
                        for head in heads {
                            *head = c.copy(head);
                        }
                    }
                }
            }
            for face in &mut clone.lisp_face_states {
                if let Some(global) = &face.global {
                    face.global = Some(c.copy(global));
                }
                if let Some(selected) = &face.selected_frame {
                    face.selected_frame = Some(c.copy(selected));
                }
            }
            for bitmap in &mut clone.fringe_bitmap_states {
                if let Some(definition) = &bitmap.definition {
                    bitmap.definition = Some(c.copy(definition));
                }
                bitmap.face = c.copy(&bitmap.face.clone());
            }
            for composition in &mut clone.composition_states {
                composition.components = c.copy(&composition.components.clone());
            }
            for test in &mut clone.ert_tests {
                test.body = c.copy(&test.body.clone());
            }
            for restore in &mut clone.active_special_restores {
                if let Some(previous) = &restore.previous {
                    restore.previous = Some(c.copy(previous));
                }
            }
            for restriction in &mut clone.labeled_restrictions {
                if let Some(label) = &restriction.label {
                    restriction.label = Some(c.copy(label));
                }
            }
            for process in &mut clone.process_states {
                if let Some(filter) = &process.filter {
                    process.filter = Some(c.copy(filter));
                }
                if let Some(sentinel) = &process.sentinel {
                    process.sentinel = Some(c.copy(sentinel));
                }
                if let Some(log) = &process.log {
                    process.log = Some(c.copy(log));
                }
                process.decoding = c.copy(&process.decoding.clone());
                process.encoding = c.copy(&process.encoding.clone());
                process.plist = c.copy(&process.plist.clone());
                process.contact = c.copy(&process.contact.clone());
            }
            let mut copy = |value: &Value| c.copy(value);
            clone.buffer.rewrite_lisp_values(&mut copy);
            for (_, buffer) in &mut clone.inactive_buffers {
                buffer.rewrite_lisp_values(&mut copy);
            }
        }

        // Identity-keyed caches: the copied cells have fresh identities, so
        // every cached verdict keyed by (or holding) template cells is
        // stale.  All of these repopulate lazily.
        clone.plain_quote_templates.clear();
        clone.source_form_items_cache.clear();
        clone.lambda_source_bodies.clear();
        clone.function_resolution_cache.clear();
        clone.file_name_handler_match_cache.clear();
        clone.bytecode_program_cache.clear();
        clone.keymap_bindings_cache.get_mut().clear();
        *clone.regexp_syntax_class_cache.get_mut() = None;
        *clone.syntax_segment_cache.get_mut() = None;
        clone.vm_stack_pool.clear();
        clone.backtrace_args_pool.clear();

        // The public-cons registry for keymap records is keyed by cons cell
        // identity; remap each identity to its copy.  A registered cons the
        // copy never reached is unreachable from the clone -- drop it.
        let remap_cons_identity = |copier: &ImageGraphCopier, identity: usize| -> Option<usize> {
            match copier.cons.get(&identity) {
                Some(Value::Cons(cell)) => Some(crate::lisp::types::ConsCell::identity(cell)),
                _ => None,
            }
        };
        clone.keymap_public_cons_owners = clone
            .keymap_public_cons_owners
            .drain()
            .filter_map(|(identity, owners)| {
                remap_cons_identity(&copier, identity).map(|identity| (identity, owners))
            })
            .collect();
        for identities in clone.keymap_public_cons_ids.values_mut() {
            *identities = identities
                .drain(..)
                .filter_map(|identity| remap_cons_identity(&copier, identity))
                .collect();
        }

        // Weak closure-environment registries point at template envs (the
        // template stays alive, so the weaks stay upgradable -- exactly the
        // channel the deep copy exists to sever).  Remap each entry to the
        // copied env; drop entries whose env the copy never reached.
        let remap_weak = |copier: &ImageGraphCopier,
                          weak: &std::rc::Weak<std::cell::RefCell<Env>>|
         -> Option<std::rc::Weak<std::cell::RefCell<Env>>> {
            copier
                .envs
                .get(&(weak.as_ptr() as usize))
                .map(std::rc::Rc::downgrade)
        };
        clone.closure_capture_cache = clone
            .closure_capture_cache
            .drain(..)
            .filter_map(|(id, weak)| remap_weak(&copier, &weak).map(|weak| (id, weak)))
            .collect();
        clone.closure_eval_contexts = clone
            .closure_eval_contexts
            .drain()
            .filter_map(|(_, (weak, lexical))| {
                remap_weak(&copier, &weak).map(|weak| (weak.as_ptr() as usize, (weak, lexical)))
            })
            .collect();

        // `eq'/`eql' hash tables bucket conses and lambdas by cell
        // identity; the copies have new identities, so rebuild every
        // bucket index from the rewritten entries.
        let mut tables = std::mem::take(&mut clone.equal_hash_tables);
        for table in tables.values_mut() {
            let mut key_index: HashMap<
                Option<i64>,
                Vec<usize>,
                crate::lisp::primitives::FnvBuildHasher,
            > = HashMap::default();
            for (index, (key, _)) in table.entries.iter().enumerate() {
                let bucket =
                    crate::lisp::primitives::runtime_hash_bucket_key(&clone, table.test, key);
                key_index.entry(bucket).or_default().push(index);
            }
            table.key_index = key_index;
        }
        clone.equal_hash_tables = tables;

        clone
    }
}

/// Counts live template-derived interpreters (issue #11).  The image
/// template shares Rc graphs with its clones, which is sound only while
/// uses never overlap across threads; the token's Drop keeps this count
/// honest so the template path can refuse concurrent use loudly instead
/// of racing.
pub(crate) static IMAGE_TEMPLATE_ACTIVE: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

pub(crate) struct ImageTemplateToken;

impl Drop for ImageTemplateToken {
    fn drop(&mut self) {
        IMAGE_TEMPLATE_ACTIVE.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// The interpreter state: holds the global environment, the current buffer,
/// and ERT test results.
#[derive(Clone)]
pub struct Interpreter {
    /// Present only on interpreters cloned from the test image template.
    /// Never read: it exists for its Drop, which decrements the live-clone
    /// counter that keeps template use single-threaded.
    #[allow(dead_code)]
    pub(crate) image_template_token: Option<std::sync::Arc<ImageTemplateToken>>,
    /// Global variable bindings (defvar, setq at top level).  GNU exposes
    /// deterministic symbol enumeration while value-cell access and removal
    /// are hash operations, so keep both properties in one canonical store.
    globals: OrderedBindings,
    /// Variable aliases keyed by alias name.
    variable_aliases: Vec<(String, String)>,
    /// Alias → target index mirroring `variable_aliases` (at most one entry
    /// per alias) so name resolution on the hot lookup path is O(1).
    variable_aliases_index: HashMap<String, String>,
    /// Variables with dynamic binding semantics.
    special_variables: Vec<String>,
    /// Membership index over `special_variables` so hot binding paths can
    /// test specialness in O(1).
    special_variables_index: HashSet<String, crate::lisp::primitives::FnvBuildHasher>,
    /// Names ever declared locally special via a non-top-level one-arg
    /// `defvar`; lets of other names skip the env marker scan entirely.
    local_special_names: HashSet<String>,
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
    /// alloc.c's nesting counter.  Hash-table user tests enter this section
    /// so arbitrary callback Lisp cannot collect the table being probed.
    garbage_collection_inhibited: usize,
    /// Consecutive `thread-yield's from a stepped (non-main) thread during
    /// which drive_threads ran nothing else.  The parent that could change
    /// this thread's loop condition is suspended up-stack until the step
    /// returns, so past a threshold the yield loop can never progress and
    /// signals the cooperative-model deadlock (finding 84's class).
    pub(crate) fruitless_stepped_yields: u32,
    pub(crate) kbd_macro_executions: Vec<KbdMacroExecutionState>,
    pub(crate) kbd_macro_definition: Vec<Value>,
    pub(crate) kbd_macro_committed_len: usize,
    pub(crate) keyboard_input: KeyboardInputState,
    pub(crate) command_loop_recursion_depth: usize,
    minibuffer_runtime: MinibufferRuntimeState,
    /// Destination installed by the native `redirect-debugging-output'
    /// primitive.  GNU keeps this in C state; it must not leak through a
    /// project-private Lisp variable.
    pub(crate) external_debugging_output_target: Option<String>,
    /// The live libgccjit arena and runtime ABI state owned by GNU's `comp.c`
    /// in the reference implementation.  All compiler policy and LIMPLE
    /// remain in the unchanged `comp.el` frontend.
    pub(crate) native_compiler: crate::lisp::native_comp::NativeCompilerState,
    /// Process-local creation permissions owned by GNU's C runtime.  Keep
    /// this as typed interpreter state so isolated Rust interpreters neither
    /// communicate through a private Lisp variable nor mutate the host
    /// process umask behind concurrently running tests.
    pub(crate) default_file_modes: i64,
    /// GNU's process-local time-zone rule.  Keep it interpreter-local here:
    /// Rust tests run interpreters concurrently in one host process, so
    /// mutating the host `TZ' would leak state between otherwise isolated
    /// Emacs instances.
    pub(crate) local_time_zone_rule: Value,
    /// Symbol properties keyed by symbol name.  Each value is the actual live
    /// Lisp plist, matching GNU symbols' plist cell rather than a Rust-side
    /// projection that loses `setcar'/`setcdr' mutations.
    symbol_properties: Vec<(String, Value)>,
    /// Last-wins position index over `symbol_properties`.  The ordered vector
    /// remains canonical for deterministic symbol enumeration.
    symbol_properties_index: OrderedNameIndex,
    /// Symbols explicitly interned into the standard obarray.
    interned_symbols: Vec<String>,
    /// Membership index for `interned_symbols'.  Keeping insertion order in
    /// the vector makes completion deterministic, while this set prevents
    /// source loading from turning symbol interning into a quadratic scan.
    interned_symbol_names: HashSet<String>,
    /// Names removed from the standard obarray while their old symbol cells
    /// remain live.  GNU keeps those cells on the detached symbol object;
    /// this tombstone prevents function/value indexes from accidentally
    /// making the name look interned again until a reader or `intern' creates
    /// the new canonical name.
    uninterned_standard_symbol_names: HashSet<String>,
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
    /// Runtime keymaps keep stable record identity internally while exposing
    /// GNU's mutable cons-list surface to Lisp.  This reverse index makes a
    /// nested `setcar'/`setcdr' on that surface update its owning record at
    /// the mutation door instead of requiring read-side rescans.
    keymap_public_cons_owners: HashMap<usize, Vec<u64>>,
    /// Forward half of `keymap_public_cons_owners', used to unregister one
    /// refreshed keymap without scanning every live public cons view.
    keymap_public_cons_ids: HashMap<u64, Vec<usize>>,
    /// The ID of the current buffer.
    current_buffer_id: u64,
    /// The currently selected window record.
    selected_window_id: u64,
    /// Root and minibuffer window identities are interpreter/frame state,
    /// never Lisp variables.
    root_window_id: u64,
    minibuffer_window_id: u64,
    minibuffer_selected_window_id: Option<u64>,
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
    /// File names retained by dead buffer objects.  GNU kills the buffer's
    /// text but keeps these buffer slots readable through the Lisp object;
    /// Eglot relies on `(buffer-file-name BUFFER)' after killing BUFFER in
    /// order to revisit the same file.
    killed_buffer_file_names: HashMap<u64, Option<String>>,
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
    /// Live marker IDs by buffer.  Marker objects remain allocated after they
    /// detach, but edits and buffer teardown must touch only the small live
    /// set belonging to that buffer.  The ordered set preserves marker-ID
    /// iteration order for undo and match-data restoration.
    markers_by_buffer: HashMap<u64, BTreeSet<u64>>,
    /// Stable GNU `mark-marker' identities, one for each live buffer.
    buffer_mark_marker_ids: HashMap<u64, u64>,
    /// Char tables allocated by the interpreter.
    char_tables: Vec<CharTableState>,
    /// Monotonic stamp for every mutable character-table access.  Regexp
    /// syntax-class rendering is derived from a whole parent chain, so a
    /// single generation owned by the character-table mutation door avoids
    /// duplicating descendant invalidation logic at every Lisp operation.
    char_table_mutation_generation: u64,
    /// The rendered current-table syntax classes are expensive to derive and
    /// are reused by many different compiled patterns.  This one-entry cache
    /// is stamped with both table identity and the mutation generation;
    /// regexp code declines to populate it for tables containing mutable
    /// Lisp entry objects whose in-place changes bypass the table mutation
    /// door.
    regexp_syntax_class_cache: RefCell<Option<RegexpSyntaxClassCache>>,
    syntax_segment_cache: RefCell<Option<SyntaxSegmentCache>>,
    /// Indexed storage for GNU `equal' hash tables.  Record slots retain
    /// metadata compatibility, while this sidecar gives structured Lisp keys
    /// the same hashed lookup shape as Emacs's native implementation.
    /// Keyed by dense record id with the shared identity hasher: SipHash
    /// showed up at 6% of a `puthash'/`gethash' kernel just locating the
    /// table per operation.
    equal_hash_tables: HashMap<u64, EqualHashTableState, crate::lisp::types::IdentityBuildHasher>,
    custom_hash_tables: HashMap<u64, CustomHashTableState, crate::lisp::types::IdentityBuildHasher>,
    /// Hash tables whose user-defined hash or comparison function is on the
    /// stack.  GNU fns.c flips the table's `mutable` bit for exactly this
    /// critical section; mutation primitives reject the table instead of
    /// copying and comparing all of its entries around every callback.
    hash_tables_under_test: HashSet<u64, crate::lisp::types::IdentityBuildHasher>,
    immutable_hash_tables: HashSet<u64, crate::lisp::types::IdentityBuildHasher>,
    /// Charset aliases defined at runtime.
    charset_aliases: Vec<(String, String)>,
    /// Registered charsets and their stable GNU-compatible numeric IDs.
    charset_ids: Vec<(String, i64)>,
    /// Charset plist overrides keyed by canonical charset name.
    charset_plists: Vec<(String, Value)>,
    /// Current charset priority order.
    charset_priority: Vec<String>,
    /// `charset-list' order: every charset and alias name, newest first
    /// (charset.c prepends on each new definition and each alias).
    charset_names: Vec<String>,
    /// Charsets defined with :supplementary-p; they sort after every
    /// non-supplementary charset in the ordered (priority) list.
    charset_supplementary: HashSet<String>,
    /// coding.c's Vsjis_coding_system: the most recently defined
    /// shift-jis-type coding system.  decode-sjis-char/encode-sjis-char
    /// convert through ITS charset list -- japanese.el defines
    /// japanese-shift-jis-2004 after japanese-shift-jis, so the
    /// primitives answer through the JIS X 0213 charsets while the
    /// `sjis' string codec stays on JIS X 0208.
    pub(crate) sjis_coding_system: String,
    /// coding.c's Vbig5_coding_system, same contract as sjis above.
    pub(crate) big5_coding_system: String,
    /// Charsets currently unified with Unicode (charset.c's UNIFIED_P
    /// flag, set by `unify-charset').  mule-conf.el unifies the CJK
    /// offset-method charsets at load; while unified, code<->character
    /// conversion goes through the charset's `:unify-map' file.
    charset_unified: HashSet<String>,
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
    /// Live record IDs grouped by their current type tag.  Records remain in
    /// dense ID order for identity lookup; this derived index avoids scanning
    /// every byte-code function, hash table, and EIEIO object when a caller
    /// needs one runtime class (notably windows during buffer teardown).
    /// `create_record` and `retag_record` are the only mutation points.
    record_ids_by_type_index: HashMap<String, BTreeSet<u64>>,
    /// Record mark bits from the most recent real reachability pass.  Dense
    /// host storage keeps IDs stable, but dead records must not contribute to
    /// GNU's post-sweep live-byte census.  IDs at or above the high-water mark
    /// were allocated after that collection and remain live until the next.
    gc_live_record_ids: HashSet<u64>,
    gc_record_high_water: u64,
    gc_has_record_census: bool,
    /// Decoded byte-code programs indexed by record ID minus one — ids are
    /// dense and never freed, so the slot vector doubles as the cache map
    /// (see bytecode::vm).
    pub(crate) bytecode_program_cache:
        Vec<Option<std::rc::Rc<crate::lisp::bytecode::vm::CachedProgram>>>,
    /// Materialized, ordered keymap bindings per keymap record, invalidated
    /// exactly like the byte-code cache: `find_record_mut' is the only
    /// mutation door for records, so `define-key' drops the slot.  Key
    /// lookup walks every active map per keystroke; re-parsing each map's
    /// string entries per lookup made `key-binding' cost milliseconds.
    pub(crate) keymap_bindings_cache:
        std::cell::RefCell<Vec<Option<crate::lisp::primitives::CachedKeymapIndex>>>,
    /// Recycled operand stacks for the byte-code VM: one Vec per active
    /// nesting level, reused across calls to avoid per-call allocation.
    pub(crate) vm_stack_pool: Vec<Vec<Value>>,
    /// Recycled argument buffers for backtrace frames, same idea.
    backtrace_args_pool: Vec<Vec<Value>>,
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
    /// Next record ID for identity tracking.
    next_record_id: u64,
    /// Next finalizer ID for identity tracking.
    next_finalizer_id: u64,
    /// Next generated symbol ID used by built-in macro expansion helpers.
    /// Buffer-local hook lists grouped by buffer, in per-buffer insertion
    /// order.  This is the sole backing store for local hook metadata.
    buffer_local_hooks: BufferLocalHooks,
    /// Buffer-local variable cells grouped by buffer, in per-buffer insertion
    /// order.  GNU's buffer slot lookup is constant-time; mode-heavy code must
    /// not scan every other live buffer's locals on each variable read.
    buffer_locals: BufferLocalBindings,
    /// Buffer-local syntax tables keyed by buffer id.
    buffer_syntax_tables: Vec<(u64, u64)>,
    /// Variables that automatically become buffer-local when set.
    auto_buffer_locals: HashSet<String, crate::lisp::primitives::FnvBuildHasher>,
    /// Native DEFVAR_PER_BUFFER variables, kept as host metadata rather than
    /// exposed through private Lisp symbol properties.
    per_buffer_specials: HashSet<String>,
    /// The DEFVAR_PER_BUFFER subset whose GNU slot index is -1 and therefore
    /// remains local in every buffer.
    always_buffer_local_specials: HashSet<String>,
    /// Active dynamic special bindings in stack order.
    active_special_restores: Vec<SpecialBindingRestore>,
    next_special_binding_id: u64,
    /// Indices into `active_special_restores' marking where suspended
    /// ancestor threads' records end.  GNU's unbind_for_thread_switch walks
    /// only the OUTGOING thread's own specpdl; swapping the whole stack
    /// re-exposed a grandparent's let values to a grandchild (audit finding
    /// on the first version of the thread-switch swap).
    thread_swap_boundaries: Vec<usize>,
    /// Marker-tracked labeled restrictions, with the innermost entry last.
    labeled_restrictions: Vec<LabeledRestriction>,
    /// Indirect buffer mapping: (buffer id, base buffer id).
    indirect_buffers: Vec<(u64, u64)>,
    /// Prevent recursive before/after-change hook re-entry.
    change_hooks_running: usize,
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
    /// Per-name funcall resolutions stamped with the generation they were
    /// computed at; consulted only when the env carries no
    /// cl-flet/cl-labels frames, so repeat calls skip name-facts probes
    /// and function-cell lookup entirely (see call_function_value_inner).
    pub(crate) function_resolution_cache:
        HashMap<String, (u64, FunctionResolution), crate::lisp::primitives::FnvBuildHasher>,
    /// Names the macroexpansion probe determined are NOT macros, from
    /// GLOBAL state only (no cl-flet frame involved), stamped with the
    /// generation that verdict was computed at.  Skips the whole probe on
    /// the hot per-form path while any definition change invalidates all
    /// verdicts at once.
    not_macro_names: HashMap<String, u64>,
    /// Flattened source forms keyed by their car-cell identity.  Entries are
    /// derived snapshots stamped with the global cons-mutation epoch and a
    /// weak source witness, never a second syntax authority.
    source_form_items_cache: HashMap<
        usize,
        ConsMutationStamped<SourceFormCacheEntry>,
        crate::lisp::primitives::FnvBuildHasher,
    >,
    /// Immutable lambda code keyed by the source form's car-cell identity.
    /// The weak source witness prevents a recycled allocator address from
    /// aliasing an unrelated form whose older closure is still alive.
    lambda_source_bodies: HashMap<usize, ConsMutationStamped<LambdaSourceBodyCacheEntry>>,
    /// Features currently available in this interpreter.
    provided_features: Vec<String>,
    /// Forms waiting for a feature to be provided.
    /// File currently being loaded, if any.
    current_load_file: Option<String>,
    /// Physical standard-Lisp source prefix and the build-tree prefix GNU's
    /// dumped load path exposes for that source.  File reads stay isolated;
    /// observable load provenance follows the image/runtime contract.
    load_source_provenance_remap: Option<(PathBuf, PathBuf)>,
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
    /// Last character written to the batch `standard-output' stream.
    ///
    /// GNU's `terpri' keeps this process-local printer state so ENSURE can
    /// decide whether stdout is already at the beginning of a line.  Keep it
    /// interpreter-local because Rust tests run independent interpreters in
    /// parallel inside one host process.
    pub(crate) batch_standard_output_last_char: Option<char>,
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
    load_path: Vec<PathBuf>,
    /// Prefer GNU bytecode artifacts after the source-based bootstrap has
    /// established the dumped Lisp runtime expected by compiled libraries.
    prefer_compiled_loads: bool,
    /// Features whose `require' loads are active, innermost last.  This is
    /// GNU's `require_nesting_list', not an alternate source of provided
    /// features: bounded recursive requires are part of the loader contract.
    require_nesting: Vec<String>,
    lambda_capture_overrides: Vec<bool>,
    thread_states: Vec<ThreadState>,
    mutex_states: Vec<MutexState>,
    condition_variables: Vec<ConditionVariableState>,
    combined_after_change: Option<CombinedAfterChangeState>,
    process_states: Vec<ProcessState>,
    pending_timers: Vec<ScheduledTimer>,
    /// Source-loaded callbacks stand in for GNU's byte-compiled Lisp.  Keep
    /// defsubst definitions removed by loadhist alive until the active timer
    /// returns, matching calls that GNU compiled inline into the callback.
    timer_callback_depth: usize,
    deferred_defsubst_unbindings: Vec<(String, Value)>,
    /// Quoted templates already scanned and found free of reader marker
    /// forms; `quote' returns them as-is (keyed by car-cell address, the
    /// stored Value keeps the template alive so keys stay unique).
    plain_quote_templates: HashMap<usize, ConsMutationStamped<Value>>,
    pending_file_notifications: Vec<PendingFileNotification>,
    file_notify_watches: HashMap<i64, FileNotifyWatch>,
    pub(crate) file_name_handler_match_cache: HashMap<
        (String, String),
        FileNameHandlerMatchCacheEntry,
        crate::lisp::primitives::FnvBuildHasher,
    >,
    main_thread_id: u64,
    active_thread_id: u64,
    last_thread_error: Option<Value>,
    backtrace_frames: Vec<BacktraceFrame>,
    batch_error_backtrace: Option<BatchErrorBacktrace>,
    active_handlers: Vec<ActiveHandler>,
    /// Dynamically active `catch' tags.  GNU's `throw' signals `no-catch'
    /// immediately when no `eq' tag is live, allowing condition handlers to
    /// observe the error without intercepting throws bound for an outer catch.
    active_catch_tags: Vec<Value>,
    handler_dispatch_depth: usize,
    suspend_condition_case_count: usize,
    window_margins: Vec<(u64, Option<i64>, Option<i64>)>,
    /// Live terminal color count published by the tty frontend; batch
    /// sessions keep GNU's dumb-terminal zero.
    pub(crate) tty_display_color_cells: i64,
    pub(crate) tty_terminal_type: Option<String>,
    /// True once a live tty published its frame size; the layout then
    /// tracks `menu-bar-lines' changes like GNU's adjust_frame_size.
    pub(crate) tty_frame_sized: bool,
    /// Bumped whenever a face definition changes, GNU's face_change
    /// flag: the frontend invalidates its resolved-attribute cache on a
    /// new value instead of re-resolving faces every redisplay.
    pub(crate) face_change_count: u64,
}

/// One entry in the dynamic handler stack, mirroring GNU's handlerlist.
/// `signal' walks this innermost-first: a matching `condition-case' clause
/// stops the search before any outer `handler-bind' functions run, while
/// matching `handler-bind' functions run at the signal point (pre-unwind).
#[derive(Clone)]
pub(crate) enum ActiveHandler {
    /// One CONDITIONS/HANDLER pair from `handler-bind'.  Keep the condition
    /// list grouped so a handler whose list contains both a child condition
    /// and one of its parents still runs exactly once for a signal.
    Bind(Vec<String>, Value),
    /// The clause heads of an active `condition-case' (minus :success).
    Case(Vec<Value>),
}

impl<T: Clone> Clone for ConsMutationStamped<T> {
    fn clone(&self) -> Self {
        Self {
            mutations: self.mutations.clone(),
            value: self.value.clone(),
        }
    }
}

struct ConsMutationStamped<T> {
    mutations: crate::lisp::types::ConsMutationSnapshot,
    value: T,
}

impl<T> ConsMutationStamped<T> {
    fn new(mutations: crate::lisp::types::ConsMutationSnapshot, value: T) -> Self {
        Self { mutations, value }
    }

    fn current(&self) -> Option<&T> {
        self.mutations.is_current().then_some(&self.value)
    }
}

struct SourceFunctionCallCacheEntry {
    definition_generation: u64,
    resolution: FunctionResolution,
}

#[derive(Default)]
struct SourceMacroCallCache {
    not_macro_generation: Option<u64>,
}

#[derive(Clone)]
struct SourceFormCacheEntry {
    source: WeakConsSlot,
    analysis: SourceFormAnalysis,
}

/// Immutable decisions derived from one source cons tree.
///
/// The enclosing `ConsMutationStamped` entry is the only validity authority:
/// mutation of a cons field used by this source form invalidates the
/// flattened items and every classification below together.
#[derive(Clone)]
struct SourceFormAnalysis {
    items: Rc<Vec<Value>>,
    native_form: Option<core::NativeForm>,
    literal_kind: core::SourceLiteralKind,
    /// The generation-stamped non-macro verdict shares the source-analysis
    /// lifetime.  Actual macro expansions are deliberately never cached:
    /// GNU's interpreted evaluator invokes the macro expander on every
    /// evaluation, and expanders may depend on state or create fresh objects.
    macro_calls: Rc<RefCell<SourceMacroCallCache>>,
    /// Generation-stamped function-cell resolution for this exact callsite.
    /// Local cl-flet/cl-labels frames bypass it before lookup.
    function_call: Rc<RefCell<Option<SourceFunctionCallCacheEntry>>>,
}

#[derive(Clone)]
struct LambdaSourceBodyCacheEntry {
    source: WeakConsSlot,
    body: Weak<Vec<Value>>,
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
            .map(|value| Value::String(value.into()))
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
            image_template_token: None,
            globals: ordered_bindings(vec![
                ("main-thread".into(), Value::Record(main_thread_id)),
                ("obarray".into(), Value::Record(standard_obarray_id)),
                ("cl--proclaims-deferred".into(), Value::Nil),
                ("cl-old-struct-compat-mode".into(), Value::Nil),
                // GNU frame.c defines this native variable before frame.el
                // and any dumped/preloaded Lisp run.
                ("default-frame-alist".into(), Value::Nil),
                (
                    "command-line-args".into(),
                    primitives::command_line_args_value(),
                ),
                // emacs.c set_initial_environment builds BOTH lists once
                // from environ at startup; process-environment is an
                // ordinary Lisp list afterwards, so a let-binding plus
                // setenv-internal's delq splice one shared cons chain
                // exactly as GNU's do.  (A fresh list synthesized on every
                // lookup broke that sharing: python-tests' unset-inside-let
                // reverted on unwind where GNU's stays spliced out.)
                (
                    "initial-environment".into(),
                    Value::list(
                        std::env::vars()
                            .map(|(name, value)| Value::String(format!("{name}={value}").into()))
                            .collect::<Vec<_>>(),
                    ),
                ),
                (
                    "process-environment".into(),
                    Value::list(
                        std::env::vars()
                            .map(|(name, value)| Value::String(format!("{name}={value}").into()))
                            .collect::<Vec<_>>(),
                    ),
                ),
                ("cpp-font-lock-keywords".into(), Value::Nil),
                ("current-load-list".into(), Value::Nil),
                ("load-history".into(), Value::Nil),
                // doc.c publishes the object-file inventory used to map
                // native definitions back to their C sources.  The concrete
                // value is populated from the standard DOC file during the
                // reconstructed dump/startup phase.
                ("build-files".into(), Value::Nil),
                ("case-replace".into(), Value::T),
                // xdisp.c publishes the complete overlay-arrow value-cell
                // cluster before loadup.el begins.  simple.el extends the
                // variable list while it is preloaded, so the list and its
                // member variables must share this native startup owner.
                ("overlay-arrow-position".into(), Value::Nil),
                ("overlay-arrow-string".into(), Value::String("=>".into())),
                (
                    "overlay-arrow-variable-list".into(),
                    Value::list([Value::symbol("overlay-arrow-position")]),
                ),
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
                // fileio.c owns this DEFVAR_LISP cell.  The native
                // `insert-file-contents' tail reads it after either the host
                // reader or a file-name handler returns, so a lexical
                // caller's `let' must remain dynamically visible there.
                ("after-insert-file-functions".into(), Value::Nil),
                // File-less embeddings need a compact mode-selection
                // fallback.  Keep it as removable provisional state so GNU
                // files.el can replace it atomically during batch startup.
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
                ("lread--unescaped-character-literals".into(), Value::Nil),
                (
                    "standard-output".into(),
                    Value::Symbol("external-debugging-output".into()),
                ),
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
            ]),
            variable_aliases: Vec::new(),
            variable_aliases_index: HashMap::new(),
            special_variables_index: HashSet::default(),
            local_special_names: HashSet::new(),
            dlet_active_names: HashMap::new(),
            special_scan_floor: 0,
            lisp_eval_depth: 0,
            garbage_collection_inhibited: 0,
            fruitless_stepped_yields: 0,
            kbd_macro_executions: Vec::new(),
            kbd_macro_definition: Vec::new(),
            kbd_macro_committed_len: 0,
            keyboard_input: KeyboardInputState::default(),
            command_loop_recursion_depth: 0,
            minibuffer_runtime: MinibufferRuntimeState::default(),
            external_debugging_output_target: None,
            native_compiler: crate::lisp::native_comp::NativeCompilerState::default(),
            default_file_modes: 0o755,
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
                "load-read-function".into(),
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
            symbol_properties_index: HashMap::default(),
            interned_symbols: Vec::new(),
            interned_symbol_names: HashSet::new(),
            uninterned_standard_symbol_names: HashSet::new(),
            standard_obarray_id,
            variable_watchers: Vec::new(),
            buffer: crate::buffer::Buffer::new("*scratch*"),
            current_global_map: None,
            keymap_public_cons_owners: HashMap::new(),
            keymap_public_cons_ids: HashMap::new(),
            current_buffer_id: 0,
            selected_window_id: 0,
            root_window_id: 0,
            minibuffer_window_id: 0,
            minibuffer_selected_window_id: None,
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
            killed_buffer_file_names: HashMap::new(),
            // GNU's batch `buffer-list' is (*scratch* " *Minibuf-0*"
            // *Messages*); *Messages* joins the list once the minibuffer
            // buffer exists, below.
            buffer_list: vec![(0, "*scratch*".to_string())],
            next_buffer_id: 2,
            next_overlay_id: 1,
            next_marker_id: 1,
            markers: Vec::new(),
            markers_by_buffer: HashMap::new(),
            buffer_mark_marker_ids: HashMap::new(),
            char_tables: vec![
                CharTableState::with_entries(
                    standard_syntax_table_id,
                    Some("syntax-table".into()),
                    Value::Nil,
                    None,
                    standard_syntax_table_entries(),
                ),
                // GNU text-mode-syntax-table: `"' and `\' are
                // punctuation, `'' is a word constituent with the prefix
                // flag (Bug#15014 hinges on `"' NOT being a string quote).
                CharTableState::with_entries(
                    2,
                    Some("syntax-table".into()),
                    Value::Nil,
                    Some(standard_syntax_table_id),
                    vec![
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
                ),
                // GNU lisp-data-mode-syntax-table.  Lisp symbols inherit its
                // punctuation entries, including the generic `@' prefix.
                CharTableState::with_entries(
                    3,
                    Some("syntax-table".into()),
                    Value::Nil,
                    Some(standard_syntax_table_id),
                    lisp_data_syntax_table_entries(),
                ),
                // GNU emacs-lisp-mode-syntax-table is a child of the data
                // table, but deliberately removes `@''s generic prefix flag:
                // syntax-propertize adds it back only for the `,@' reader
                // token (bug#24542).
                CharTableState::with_entries(
                    4,
                    Some("syntax-table".into()),
                    Value::Nil,
                    Some(3),
                    vec![CharTableEntry {
                        start: '@' as u32,
                        end: '@' as u32,
                        value: syntax_spec_value("_"),
                    }],
                ),
            ],
            char_table_mutation_generation: 0,
            regexp_syntax_class_cache: RefCell::new(None),
            syntax_segment_cache: RefCell::new(None),
            equal_hash_tables: HashMap::default(),
            custom_hash_tables: HashMap::default(),
            hash_tables_under_test: HashSet::default(),
            immutable_hash_tables: HashSet::default(),
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
            // charset.c's C-level definitions, in definition order: the
            // ordered list keeps non-supplementary charsets first (`emacs'
            // and `eight-bit' are supplementary), and `charset-list' holds
            // newest-first.
            charset_priority: vec![
                "ascii".into(),
                "iso-8859-1".into(),
                "unicode".into(),
                "emacs".into(),
                "eight-bit".into(),
            ],
            charset_names: vec![
                "eight-bit".into(),
                "emacs".into(),
                "unicode".into(),
                "iso-8859-1".into(),
                "ascii".into(),
            ],
            charset_supplementary: ["emacs".to_string(), "eight-bit".to_string()]
                .into_iter()
                .collect(),
            charset_unified: HashSet::new(),
            sjis_coding_system: "sjis".into(),
            big5_coding_system: "big5".into(),
            iso_charsets: vec![(1, 94, 'B' as u32, "ascii".into())],
            coding_systems: builtin_coding_systems(),
            ccl_programs: vec![None; 32],
            coding_aliases: builtin_coding_aliases(),
            coding_priority: builtin_coding_priority(),
            terminal_coding: None,
            // keyboard.c initializes keyboard decoding to no-conversion; a
            // batch GNU answers `no-conversion' for (keyboard-coding-system)
            // before any Lisp touches it (oracle-pinned under LANG=C).
            keyboard_coding: Some("no-conversion".into()),
            input_interrupt_mode: true,
            standard_category_table_id: None,
            standard_case_table_id: None,
            ascii_case_table_ids: Vec::new(),
            buffer_case_tables: Vec::new(),
            next_char_table_id: 5,
            records: vec![
                RecordState {
                    id: main_thread_id,
                    type_tag: Value::symbol("thread"),
                    slots: Vec::new(),
                    kind: RecordKind::Thread,
                },
                RecordState {
                    id: standard_obarray_id,
                    type_tag: Value::symbol("obarray"),
                    slots: vec![Value::Nil],
                    kind: RecordKind::Obarray,
                },
            ],
            record_ids_by_type_index: HashMap::from([
                ("thread".into(), BTreeSet::from([main_thread_id])),
                ("obarray".into(), BTreeSet::from([standard_obarray_id])),
            ]),
            gc_live_record_ids: HashSet::new(),
            gc_record_high_water: 0,
            gc_has_record_census: false,
            sqlite_handles: Vec::new(),
            bytecode_program_cache: Vec::new(),
            keymap_bindings_cache: std::cell::RefCell::new(Vec::new()),
            vm_stack_pool: Vec::new(),
            backtrace_args_pool: Vec::new(),
            treesit_queries: Vec::new(),
            treesit_languages: Vec::new(),
            treesit_parsers: Vec::new(),
            treesit_nodes: Vec::new(),
            next_record_id: 3,
            next_finalizer_id: 1,
            buffer_local_hooks: HashMap::default(),
            buffer_locals: HashMap::default(),
            buffer_syntax_tables: Vec::new(),
            auto_buffer_locals: HashSet::default(),
            per_buffer_specials: HashSet::new(),
            always_buffer_local_specials: HashSet::new(),
            active_special_restores: Vec::new(),
            next_special_binding_id: 1,
            thread_swap_boundaries: Vec::new(),
            labeled_restrictions: Vec::new(),
            indirect_buffers: Vec::new(),
            change_hooks_running: 0,
            functions: Vec::new(),
            functions_index: HashMap::default(),
            network_connect_counter: 0,
            definition_generation: 0,
            function_resolution_cache: HashMap::default(),
            not_macro_names: HashMap::new(),
            source_form_items_cache: HashMap::default(),
            lambda_source_bodies: HashMap::new(),
            provided_features: STARTUP_FEATURES
                .iter()
                .map(|feature| feature.name.to_string())
                .collect(),
            current_load_file: None,
            load_source_provenance_remap: None,
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
            batch_standard_output_last_char: None,
            current_activation_id: 0,
            next_activation_id: 0,
            closure_capture_cache: Vec::new(),
            lexical_cell_updates: HashMap::new(),
            closure_eval_contexts: HashMap::new(),
            closure_eval_context_registrations: 0,
            lossage_size: 300,
            interactive_call_depth: 0,
            lisp_face_states: vec![LispFaceState {
                name: "default".into(),
                id: Some(0),
                global: Some(empty_lisp_face_vector()),
                selected_frame: Some(tty_default_lisp_face_vector()),
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
            load_path: Vec::new(),
            prefer_compiled_loads: false,
            require_nesting: Vec::new(),
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
                waiting_for_user_input: false,
            }],
            mutex_states: Vec::new(),
            condition_variables: Vec::new(),
            combined_after_change: None,
            process_states: Vec::new(),
            pending_timers: Vec::new(),
            timer_callback_depth: 0,
            deferred_defsubst_unbindings: Vec::new(),
            plain_quote_templates: HashMap::new(),
            pending_file_notifications: Vec::new(),
            file_notify_watches: HashMap::new(),
            file_name_handler_match_cache: HashMap::default(),
            main_thread_id,
            active_thread_id: main_thread_id,
            last_thread_error: None,
            backtrace_frames: Vec::new(),
            batch_error_backtrace: None,
            active_handlers: Vec::new(),
            active_catch_tags: Vec::new(),
            handler_dispatch_depth: 0,
            suspend_condition_case_count: 0,
            window_margins: Vec::new(),
            tty_display_color_cells: 0,
            tty_terminal_type: None,
            tty_frame_sized: false,
            face_change_count: 0,
        };
        interp.symbol_properties_index = ordered_name_index(&interp.symbol_properties);
        // Startup globals are dumped `defvar'/DEFVAR value cells, hence
        // intrinsically special.  Fold declarations and values through one
        // registration path so a new startup global cannot require a shadow
        // entry in `special_variables'; this also removes duplicate names.
        let declared_specials = std::mem::take(&mut interp.special_variables);
        let startup_globals = interp
            .globals
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        for name in declared_specials.into_iter().chain(startup_globals) {
            interp.mark_special_variable(&name);
        }
        interp.initialize_native_face_variables();
        // gnutls.c publishes both variables before preloading gnutls.el.
        // The shared library is resolved dynamically here, so availability
        // refreshes the version value while preserving GNU's unavailable
        // sentinel and the native logging default from startup onward.
        interp.define_special_variable("libgnutls-version", Value::Integer(-1));
        interp.define_special_variable("gnutls-log-level", Value::Integer(0));
        // alloc.c exposes the allocator's emergency state before jit-lock.el
        // and every fontification client.  JIT lock only reads this native
        // cell; its policy remains in the upstream Lisp owner.
        interp.define_special_variable("memory-full", Value::Nil);
        interp.define_special_variable(
            "memory-signal-data",
            Value::list([
                Value::symbol("error"),
                Value::String(
                    "Memory exhausted--use M-x save-some-buffers then exit and restart Emacs"
                        .into(),
                ),
            ]),
        );
        // data.c exposes the symbol-position comparison switch as a native
        // DEFVAR_BOOL.  It is globally bound to nil and intrinsically
        // special before bytecomp.el loads; the compiler dynamically binds
        // it around `eq', whose C contract then treats positioned symbols as
        // their bare symbols.
        interp.define_special_variable("symbols-with-pos-enabled", Value::Nil);
        // fns.c's compile-time plist shadow: bytecomp's top-level
        // `function-put'/`define-symbol-prop' handler pushes entries here and
        // `get' consults it before the symbol's own plist.
        interp.define_special_variable("overriding-plist-environment", Value::Nil);
        // The remaining minibuf.c DEFVARs.  `minibuffer-setup-hook' and
        // `minibuffer-exit-hook' matter beyond their values: without the C
        // declaration a `let' on them under lexical binding is lexical, so
        // minibuffer.el's own `minibuffer-with-setup-hook' machinery (which
        // installs the completion session with `setq-local') silently never
        // runs.
        interp.define_special_variable("minibuffer-setup-hook", Value::Nil);
        interp.define_special_variable("minibuffer-exit-hook", Value::Nil);
        interp.define_special_variable("minibuffer-follows-selected-frame", Value::T);
        interp.define_special_variable("read-buffer-completion-ignore-case", Value::Nil);
        interp.define_special_variable("read-hide-char", Value::Nil);
        // emacs.c's session-mode flag: nil in the dumped default; batch
        // startup flips it to t.
        interp.define_special_variable("noninteractive", Value::Nil);
        // xdisp.c's trailing-whitespace highlight switch, read by dumped
        // simple.el commands like `kill-line'.
        interp.define_special_variable("show-trailing-whitespace", Value::Nil);
        // indent.c:2486 DEFVAR_BOOL, initialised to 1.  This must be a real
        // binding, not a `builtin_var_value' fallback: simple.el's
        // `define-minor-mode indent-tabs-mode' supplies no :init-value, so its
        // defcustom keeps whatever the variable already holds — and a fallback
        // that `defvar' cannot see is indistinguishable from unbound, which
        // let preloaded Lisp overwrite GNU's C default with nil.
        interp.define_special_variable("indent-tabs-mode", Value::T);

        // Install every C-owned DEFVAR as a real binding (finding 37 made
        // general; the list is verified against the pinned checkout's
        // DEFVAR_* declarations).  `defvar'/`defcustom' in preloaded Lisp
        // then see these bound, exactly as they see GNU's C state.

        // Second DEFVAR completeness tranche (finding 69): the remaining
        // oracle-bound C DEFVARs.  These construction values belong to the
        // corresponding syms_of_* initializers.  Later C init_* phases and
        // unchanged GNU Lisp loadup own their subsequent mutations; folding
        // an observed dumped value into this layer skips behavior.
        let frame_title_format = Value::list([
            Value::symbol("multiple-frames"),
            Value::string("%b"),
            Value::list([
                Value::string(""),
                Value::string("%b - GNU Emacs at "),
                Value::symbol("system-name"),
            ]),
        ]);
        for (name, value) in [
            ("alternate-fontname-alist", Value::Nil),
            ("attempt-orderly-shutdown-on-fatal-signal", Value::T),
            ("attempt-stack-overflow-recovery", Value::T),
            ("auto-composition-mode", Value::T),
            ("auto-save-include-big-deletions", Value::Nil),
            ("auto-save-list-file-name", Value::Nil),
            ("backtrace-on-redisplay-error", Value::Nil),
            ("bidi-inhibit-bpa", Value::Nil),
            ("binary-as-unsigned", Value::Nil),
            ("buffer-access-fontified-property", Value::Nil),
            ("buffer-access-fontify-functions", Value::Nil),
            ("buffer-list-update-hook", Value::Nil),
            ("cannot-suspend", Value::Nil),
            ("change-major-mode-hook", Value::Nil),
            ("clone-indirect-buffer-hook", Value::Nil),
            ("coding-system-require-warning", Value::Nil),
            ("command-debug-status", Value::Nil),
            ("command-history", Value::Nil),
            ("comment-end-can-be-escaped", Value::Nil),
            ("comment-use-syntax-ppss", Value::T),
            // The live Rust comp.c replacement initializes its ABI hash,
            // native version directory, and runtime tables after the base
            // startup values below.  Never seed another executable's own
            // build identity here (2026-08-23 audit finding 77).
            ("comp-ctxt", Value::Nil),
            ("comp-file-preloaded-p", Value::Nil),
            ("comp-sanitizer-active", Value::Nil),
            (
                "compose-chars-after-function",
                Value::symbol("compose-chars-after"),
            ),
            // The *-consed counters are live allocation telemetry; Emaxx does
            // not count allocations, so they start at zero rather than at a
            // frozen snapshot of the oracle's own counters (which the first
            // draft of this tranche fabricated).
            ("cons-cells-consed", Value::Integer(0)),
            ("current-key-remap-sequence", Value::Nil),
            ("current-time-list", Value::T),
            ("cursor-in-echo-area", Value::Nil),
            ("debug-on-message", Value::Nil),
            ("default-frame-scroll-bars", Value::symbol("right")),
            ("default-minibuffer-frame", Value::Nil),
            ("default-text-properties", Value::Nil),
            ("delayed-warnings-list", Value::Nil),
            ("delete-frame-functions", Value::Nil),
            ("describe-bindings-check-shadowing-in-ranges", Value::Nil),
            ("disable-ascii-optimization", Value::Nil),
            ("disable-inhibit-text-conversion", Value::Nil),
            ("disable-point-adjustment", Value::Nil),
            ("display-line-numbers-offset", Value::Integer(0)),
            ("display-monitors-changed-functions", Value::Nil),
            ("display-pixels-per-inch", Value::float(72.0)),
            ("dynamic-library-alist", Value::Nil),
            ("echo-area-clear-hook", Value::Nil),
            (
                "emacs-copyright",
                Value::String("Copyright (C) 2025 Free Software Foundation, Inc.".into()),
            ),
            ("emulation-mode-map-alists", Value::Nil),
            ("enable-disabled-menus-and-buttons", Value::Nil),
            ("extra-keyboard-modifiers", Value::Integer(0)),
            ("float-output-format", Value::Nil),
            // Zeroed like its *-consed siblings: live allocation telemetry
            // (the frozen oracle snapshot here survived the first sweep).
            ("floats-consed", Value::Integer(0)),
            // font.c:5965; init_font later applies EMACS_FONT_LOG policy.
            ("font-log", Value::Nil),
            ("fontification-functions", Value::Nil),
            ("frame-alpha-lower-limit", Value::Integer(20)),
            ("frame-size-history", Value::Nil),
            ("frame-title-format", frame_title_format.clone()),
            ("global-disable-point-adjustment", Value::Nil),
            ("global-mode-string", Value::Nil),
            ("glyph-table", Value::Nil),
            // xdisp.c assigns the very same Lisp object to both variables.
            ("icon-title-format", frame_title_format),
            ("iconify-child-frame", Value::symbol("iconify-top-level")),
            ("inhibit--record-char", Value::Nil),
            ("inhibit-bidi-mirroring", Value::Nil),
            ("inhibit-compacting-font-caches", Value::Nil),
            ("inhibit-debugger", Value::Nil),
            ("inhibit-eval-during-redisplay", Value::Nil),
            ("inhibit-free-realized-faces", Value::Nil),
            ("inhibit-menubar-update", Value::Nil),
            ("inhibit-mouse-event-check", Value::Nil),
            ("input-method-previous-message", Value::Nil),
            ("input-pending-p-filter-events", Value::T),
            ("integer-width", Value::Integer(65536)),
            (
                "internal--top-level-message",
                Value::String("Back to top level".into()),
            ),
            // eval.c initializes this to nil.  Unchanged GNU loadup.el sets
            // it to `cconv-make-interpreted-closure' only after loading the
            // compiled cconv and macroexp libraries.
            ("internal-make-interpreted-closure-function", Value::Nil),
            ("internal-when-entered-debugger", Value::Integer(-1)),
            ("intervals-consed", Value::Integer(0)),
            ("large-hscroll-threshold", Value::Integer(10000)),
            ("last-command-event", Value::Nil),
            ("line-prefix", Value::Nil),
            ("lisp-eval-depth-reserve", Value::Integer(200)),
            (
                "long-line-optimizations-bol-search-limit",
                Value::Integer(128),
            ),
            (
                "long-line-optimizations-region-size",
                Value::Integer(500000),
            ),
            ("long-line-threshold", Value::Integer(50000)),
            ("make-window-start-visible", Value::Nil),
            ("max-redisplay-ticks", Value::Integer(0)),
            ("menu-prompt-more-char", Value::Integer(32)),
            ("menu-updating-frame", Value::Nil),
            ("message-truncate-lines", Value::Nil),
            ("messages-buffer-name", Value::String("*Messages*".into())),
            ("mouse-fine-grained-tracking", Value::Nil),
            ("mouse-leave-buffer-hook", Value::Nil),
            ("mouse-position-function", Value::Nil),
            ("move-frame-functions", Value::Nil),
            ("multiple-frames", Value::Nil),
            ("mwheel-coalesce-scroll-events", Value::T),
            // comp.c leaves the zero-initialized Lisp_Object nil.  GNU's
            // unchanged loadup.el enables this immediately before pdump.
            ("native-comp-enable-subr-trampolines", Value::Nil),
            ("native-comp-jit-compilation", Value::T),
            ("nobreak-char-ascii-display", Value::Nil),
            ("nobreak-char-display", Value::T),
            // editfns.c:140 fills this from `uname'.  It was transcribed as
            // this host's own `uname -r' output (audit finding 101), which is
            // copied build identity rather than a computed value.
            (
                "operating-system-release",
                crate::lisp::primitives::uname_field(crate::lisp::primitives::UnameField::Release)
                    .map(|release| Value::String(release.into()))
                    .unwrap_or(Value::Nil),
            ),
            ("overriding-local-map-menu-flag", Value::Nil),
            // pdumper.c documents `pdumper-fingerprint' as "unique to each
            // build of Emacs"; the value is computed lazily from THIS
            // executable in builtin_var_value, never copied from the
            // oracle's binary (2026-08-23 audit finding 77).
            ("post-gc-hook", Value::Nil),
            ("post-select-region-hook", Value::Nil),
            ("print-escape-control-characters", Value::Nil),
            ("print-escape-multibyte", Value::Nil),
            ("print-escape-newlines", Value::Nil),
            ("print-escape-nonascii", Value::Nil),
            ("print-number-table", Value::Nil),
            ("print-unreadable-function", Value::Nil),
            ("profiler-log-size", Value::Integer(10000)),
            ("profiler-max-stack-depth", Value::Integer(16)),
            ("pure-bytes-used", Value::Integer(32)),
            ("query-all-font-backends", Value::Nil),
            ("redisplay-adhoc-scroll-in-resize-mini-windows", Value::T),
            ("redisplay-dont-pause", Value::T),
            ("redisplay-skip-fontification-on-input", Value::Nil),
            ("redisplay-skip-initial-frame", Value::T),
            (
                "report-emacs-bug-address",
                Value::String("bug-gnu-emacs@gnu.org".into()),
            ),
            ("resume-tty-functions", Value::Nil),
            ("scroll-bar-adjust-thumb-portion", Value::T),
            ("shared-game-score-directory", Value::Nil),
            ("string-chars-consed", Value::Integer(0)),
            ("strings-consed", Value::Integer(0)),
            ("suspend-tty-functions", Value::Nil),
            ("symbols-consed", Value::Integer(0)),
            ("system-key-alist", Value::Nil),
            ("tab-bar--dragging-in-progress", Value::Nil),
            ("tab-bar-separator-image-expression", Value::Nil),
            ("throw-on-input", Value::Nil),
            ("tool-bar-separator-image-expression", Value::Nil),
            ("treesit-thing-settings", Value::Nil),
            ("tty-erase-char", Value::Nil),
            ("tty-menu-calls-mouse-position-function", Value::Nil),
            ("undo-inhibit-record-point", Value::Nil),
            ("unread-input-method-events", Value::Nil),
            ("unread-post-input-method-events", Value::Nil),
            ("use-default-font-for-symbols", Value::T),
            ("vector-cells-consed", Value::Integer(0)),
            ("where-is-preferred-modifier", Value::Nil),
            ("wrap-prefix", Value::Nil),
            ("write-region-annotate-functions", Value::Nil),
            ("write-region-annotations-so-far", Value::Nil),
            ("write-region-inhibit-fsync", Value::T),
            ("write-region-post-annotation-function", Value::Nil),
            ("x-max-tooltip-size", Value::Nil),
            ("x-resource-class", Value::String("Emacs".into())),
            ("x-resource-name", Value::Nil),
            ("x-show-tooltip-timeout", Value::Integer(5)),
            ("x-stretch-cursor", Value::Nil),
            ("x-toolkit-scroll-bars", Value::T),
            ("xft-ignore-color-fonts", Value::T),
        ] {
            interp.define_special_variable(name, value);
        }

        // Portable list-valued DEFVARs from the same completeness sweep.
        // The live native-comp backend initializes its comp-*-h tables and
        // comp-subr-list separately.
        for (name, value) in [
            (
                "coding-category-list",
                Value::list([
                    Value::symbol("coding-category-utf-8"),
                    Value::symbol("coding-category-iso-7"),
                    Value::symbol("coding-category-charset"),
                    Value::symbol("coding-category-iso-7-else"),
                    Value::symbol("coding-category-iso-8-else"),
                    Value::symbol("coding-category-emacs-mule"),
                    Value::symbol("coding-category-raw-text"),
                    Value::symbol("coding-category-iso-7-tight"),
                    Value::symbol("coding-category-iso-8-1"),
                    Value::symbol("coding-category-iso-8-2"),
                    Value::symbol("coding-category-utf-8-auto"),
                    Value::symbol("coding-category-utf-8-sig"),
                    Value::symbol("coding-category-utf-16-auto"),
                    Value::symbol("coding-category-utf-16-be"),
                    Value::symbol("coding-category-utf-16-le"),
                    Value::symbol("coding-category-utf-16-be-nosig"),
                    Value::symbol("coding-category-utf-16-le-nosig"),
                    Value::symbol("coding-category-sjis"),
                    Value::symbol("coding-category-big5"),
                    Value::symbol("coding-category-ccl"),
                    Value::symbol("coding-category-undecided"),
                ]),
            ),
            (
                "frame-inhibit-implied-resize",
                Value::list([Value::symbol("tab-bar-lines")]),
            ),
            (
                "selection-inhibit-update-commands",
                Value::list([
                    Value::symbol("handle-switch-frame"),
                    Value::symbol("handle-select-window"),
                ]),
            ),
            (
                "while-no-input-ignore-events",
                Value::list([
                    Value::symbol("thread-event"),
                    Value::symbol("file-notify"),
                    Value::symbol("select-window"),
                    Value::symbol("help-echo"),
                    Value::symbol("move-frame"),
                    Value::symbol("iconify-frame"),
                    Value::symbol("make-frame-visible"),
                    Value::symbol("focus-in"),
                    Value::symbol("focus-out"),
                    Value::symbol("config-changed-event"),
                    Value::symbol("selection-request"),
                ]),
            ),
            (
                "fontset-alias-alist",
                Value::list([Value::cons(
                    Value::String("-*-*-*-*-*-*-*-*-*-*-*-*-fontset-default".into()),
                    Value::String("fontset-default".into()),
                )]),
            ),
        ] {
            interp.define_special_variable(name, value);
        }
        // frame.c exposes the initial stdout frame itself, not a copied
        // description or a nil placeholder.
        interp.define_special_variable("terminal-frame", Value::Frame(interp.selected_frame_id));
        // xdisp.c creates both diagnostic counters as ordinary default-test
        // hash tables.  They are live Lisp objects and therefore participate
        // in the same GC graph as every other C-owned table.
        for name in [
            "redisplay--all-windows-cause",
            "redisplay--mode-lines-cause",
        ] {
            let table = crate::lisp::json::make_hash_table(&mut interp, "eql", Vec::new());
            interp.define_special_variable(name, table);
        }

        for name in crate::lisp::eval::bindings::C_OWNED_DEFVAR_NAMES {
            if let Some(value) = interp.builtin_var_value(name) {
                interp.define_special_variable(name, value);
            }
        }
        // minibuf.c's history controls, read by subr.el's `add-to-history'.
        interp.define_special_variable("history-length", Value::Integer(100));
        interp.define_special_variable("history-delete-duplicates", Value::Nil);
        interp.define_special_variable("history-add-new-input", Value::T);
        for variable in GNU_XDISP_GLOBAL_VARIABLES {
            interp.define_special_variable(variable.name, variable.default.value());
        }
        // coding.c's end-of-line mnemonics.  startup.el later replaces the
        // non-native platform labels; keep that Lisp-owned transition out of
        // the C construction layer.
        interp.define_special_variable("eol-mnemonic-unix", Value::String(":".into()));
        interp.define_special_variable("eol-mnemonic-dos", Value::String("\\".into()));
        interp.define_special_variable("eol-mnemonic-mac", Value::String("/".into()));
        interp.define_special_variable("eol-mnemonic-undecided", Value::String(":".into()));
        interp.define_special_variable("fringe-bitmaps", fringe_bitmaps);
        for (index, name) in primitives::STANDARD_FRINGE_BITMAPS.iter().enumerate() {
            interp.put_symbol_property(name, "fringe", Value::Integer((index + 1) as i64));
        }
        for name in GNU_LREAD_SPECIAL_VARIABLES {
            interp.mark_special_variable(name);
        }
        for name in GNU_EMACS_LOCALE_SPECIAL_VARIABLES {
            interp.define_special_variable(name, Value::Nil);
        }
        for name in GNU_TREESIT_SPECIAL_VARIABLES {
            interp.define_special_variable(name, Value::Nil);
        }
        for (name, value) in gnu_image_special_variables() {
            interp.define_special_variable(name, value);
        }
        for name in GNU_CHANGE_HOOK_SPECIAL_VARIABLES {
            interp.define_special_variable(name, Value::Nil);
        }
        for variable in GNU_NATIVE_PER_BUFFER_VARIABLES {
            if variable.always_local {
                interp.mark_always_buffer_local_special(variable.name);
            } else {
                interp.mark_per_buffer_special(variable.name);
            }
            if variable.permanent {
                interp.put_symbol_property(variable.name, "permanent-local", Value::T);
            }
        }
        // search.c uses its own Lisp variable rather than a Buffer field, but
        // GNU gives it the same always-buffer-local behavior.
        interp.mark_per_buffer_special("case-fold-search");
        for feature in STARTUP_FEATURES {
            if let Some(subfeatures) = feature.subfeatures {
                interp.put_symbol_property(feature.name, "subfeatures", subfeatures());
            }
        }
        // GNU's C creates no global-map/esc-map/ctl-x-map/menu variables:
        // subr.el and its preloaded successors own every one of them
        // (keymap.c staticpros only `current_global_map').  Pre-seeding
        // native maps here made subr.el's `defvar' keep the incomplete
        // native objects, silently dropping dumped bindings like
        // `C-x b' -> `switch-to-buffer' from the reconstructed image.
        // keymap.c's own DEFVAR_LISP map is the one exception.
        let minibuffer_local_map =
            primitives::make_runtime_keymap(&mut interp, Some("minibuffer-local-map"));
        interp.define_special_variable("minibuffer-local-map", minibuffer_local_map);
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
            interp.define_special_variable(name, keymap);
        }
        // `visual-line-mode' deliberately stays native in Emaxx, so its
        // native bootstrap owns the same complete mode contract that GNU's
        // dumped simple.el creates: a stable map, mode variable, hook family,
        // and minor-mode registry entry.
        let visual_line_mode_map = make_visual_line_mode_map(&mut interp);
        interp.define_special_variable("visual-line-mode-map", visual_line_mode_map.clone());
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
        interp.define_per_buffer_special("visual-line-mode", Value::Nil);
        for hook in [
            "visual-line-mode-hook",
            "visual-line-mode-on-hook",
            "visual-line-mode-off-hook",
        ] {
            interp.define_special_variable(hook, Value::Nil);
        }
        interp.define_per_buffer_special("font-lock-mode", Value::Nil);
        interp.define_per_buffer_special("font-lock-fontified", Value::Nil);
        interp.define_per_buffer_special("header-line-indent-mode", Value::Nil);
        interp.set_global_binding("major-mode", Value::Symbol("fundamental-mode".into()));
        interp.set_global_binding("mode-name", Value::String("Fundamental".into()));
        // buffer.c:4794 seeds the C default as the string "%-", with the
        // comment "real setup is done in bindings.el"; that preloaded file
        // installs the real spec and its `standard-value'.  Transcribing
        // bindings.el here made the bare host claim a dumped image it does
        // not have.
        interp.define_per_buffer_special("mode-line-format", Value::String("%-".into()));
        for name in ["header-line-format", "tab-line-format"] {
            interp.define_per_buffer_special(name, Value::Nil);
        }
        // bindings.el owns `mode-line-buffer-identification' via defvar-local,
        // and its value carries text properties from
        // `propertized-buffer-identification'.  Pre-seeding a plain ("%12b")
        // here made that defvar keep the native value, exactly as the
        // pre-seeded keymaps did.
        let glyphless_char_display =
            interp.make_char_table(Some("glyphless-char-display".into()), Value::Nil);
        interp.set_global_binding("glyphless-char-display", glyphless_char_display);
        let char_script_table =
            interp.make_char_table(Some("char-script-table".into()), Value::Nil);
        interp.define_special_variable("char-script-table", char_script_table);
        let auto_fill_chars = interp.make_char_table(Some("auto-fill-chars".into()), Value::Nil);
        if let Value::CharTable(table_id) = auto_fill_chars {
            interp
                .char_table_set(table_id, ' ' as u32, Value::T)
                .expect("initialize auto-fill-chars space entry");
            interp
                .char_table_set(table_id, '\n' as u32, Value::T)
                .expect("initialize auto-fill-chars newline entry");
            interp.define_special_variable("auto-fill-chars", Value::CharTable(table_id));
        }
        let char_width_table = interp.make_char_table(None, Value::Integer(1));
        if let Value::CharTable(table_id) = char_width_table {
            interp
                .char_table_set_range(table_id, 0x80, 0x9f, Value::Integer(4))
                .expect("initialize C1 character widths");
            interp.define_special_variable("char-width-table", Value::CharTable(table_id));
        }
        let ambiguous_width_chars = interp.make_char_table(None, Value::Nil);
        interp.define_special_variable("ambiguous-width-chars", ambiguous_width_chars);
        let printable_chars = interp.make_char_table(None, Value::Nil);
        if let Value::CharTable(table_id) = printable_chars {
            interp
                .char_table_set_range(table_id, 32, 126, Value::T)
                .expect("initialize ASCII printable characters");
            interp
                .char_table_set_range(table_id, 160, 0x3f_ffff, Value::T)
                .expect("initialize multibyte printable characters");
            interp.define_special_variable("printable-chars", Value::CharTable(table_id));
        }
        interp.define_special_variable("script-representative-chars", Value::Nil);
        interp.define_special_variable("unicode-category-table", Value::Nil);
        interp.define_special_variable("auto-composition-function", Value::Nil);
        let composition_function_table = interp.make_char_table(None, Value::Nil);
        interp.define_special_variable("composition-function-table", composition_function_table);
        interp.define_special_variable("auto-composition-emoji-eligible-codepoints", Value::Nil);
        // GNU defines this C variable as both special and automatically
        // buffer-local.  A dynamic binding therefore belongs to the buffer
        // where it was made and must not make a newly selected buffer read-only.
        interp.define_always_buffer_local_special("buffer-read-only", Value::Nil);
        interp.define_special_variable("dump-mode", Value::Nil);
        interp.define_special_variable("charset-map-path", Value::Nil);
        interp.define_special_variable("inhibit-load-charset-map", Value::Nil);
        interp.define_special_variable("current-iso639-language", Value::Nil);
        interp.mark_special_variable("charset-list");
        for (name, value) in [
            ("delete-auto-save-files", Value::T),
            ("kill-buffer-delete-auto-save-files", Value::Nil),
        ] {
            interp.define_special_variable(name, value);
        }
        interp.define_per_buffer_special("read-only-mode", Value::Nil);
        // No `lisp-indent-function' properties are seeded here.  GNU's C never
        // sets that property (zero occurrences in src/*.c); every one comes from
        // `(declare (indent N))' processed by byte-run.el, `lisp-mode.el''s
        // `put' forms, or loaddefs.el's `function-put' forms as those files
        // load.  Seeding them made the bare host claim a dumped image it does
        // not have.
        // Defaults not synthesized by `builtin_var_value'; locality and
        // permanence come exclusively from GNU_NATIVE_PER_BUFFER_VARIABLES.
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
        interp.define_special_variable("baud-rate", Value::Integer(0));
        // keyboard.c installs `list' as the pass-through input method before
        // dumped Lisp loads.  Isearch saves and buffer-locally suppresses it.
        interp.define_special_variable("input-method-function", Value::Symbol("list".into()));
        // GNU 30.2 lread.c defines the global cell and then calls
        // `make-variable-buffer-local': fresh buffers inherit nil and only
        // acquire a local binding when a file cookie or explicit assignment
        // sets one.  Treating it as an always-local buffer slot makes
        // `local-variable-p' lie and suppresses bytecomp's missing-cookie
        // warning.
        interp.define_per_buffer_special("lexical-binding", Value::Nil);
        // GNU preloads files.el, where this defcustom is globally bound.
        // abbrev.el consumes it without requiring files.el itself.
        interp.define_special_variable("save-abbrevs", Value::T);
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
        // doc.c:733 defines this alongside it as a DEFVAR_BOOL, so a `let'
        // on it must establish a dynamic binding too.  GNU's own Lisp only
        // ever `setq's it (startup.el:1466 forces it to t in a non-batch
        // session, where the grave fallback then comes from the
        // standard-display-table branch instead).
        //
        // This must be a REAL binding rather than a `builtin_var_value'
        // fallback: `default-boundp' answers from the globals map, and GNU
        // reports t for it.  The VALUE is still computed, never asserted --
        // emacs.c:1665 sets it from `using_utf8 ()' at startup, and the
        // OnceLock behind `locale_uses_utf8' gives the same once-per-process
        // semantics.
        interp.define_special_variable(
            "internal--text-quoting-flag",
            if crate::lisp::primitives::values::locale_uses_utf8() {
                Value::T
            } else {
                Value::Nil
            },
        );
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
            interp.define_special_variable(name, value);
        }
        // fns.c initializes this true.  subr.el consumes the C-owned policy
        // but does not replace its value during loadup.
        interp.define_special_variable("use-dialog-box", Value::T);
        interp.define_special_variable("use-short-answers", Value::Nil);
        // fileio.c exposes this as a dynamically scoped DEFVAR_LISP.  Temp
        // helpers are defined separately and must observe callers' let-bindings.
        interp.mark_special_variable("temporary-file-directory");
        // editfns.c defines this before paragraphs.el is dumped.  Paragraph
        // and line motion bind it around calls into separately defined
        // functions, so a lexical binding here would silently leave field
        // constraints enabled (most visibly at non-sticky shell prompts).
        interp.define_special_variable("inhibit-field-text-motion", Value::Nil);
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
            // GNU print.c owns this DEFVAR_BOOL.  The byte compiler binds it
            // while source-position symbols are enabled so diagnostics and
            // generated output contain ordinary symbol names.
            ("print-symbols-bare", Value::Nil),
            (
                "print-charset-text-property",
                Value::Symbol("default".into()),
            ),
            ("print-length", Value::Nil),
            ("print-level", Value::Nil),
            ("print-quoted", Value::T),
        ] {
            interp.define_special_variable(name, value);
        }
        interp.define_special_variable("char-property-alias-alist", Value::Nil);
        // syntax.c exposes both scanner switches as primitive DEFVAR_BOOLs.
        // They must be special so a caller's lexical `let' remains visible
        // through separately defined Lisp helpers such as `syntax-after'.
        for name in ["parse-sexp-ignore-comments", "parse-sexp-lookup-properties"] {
            interp.define_special_variable(name, Value::Nil);
        }
        // syntax.c owns this function table as a native DEFVAR_LISP.  Word
        // modes install buffer-local tables, then dynamically bind an empty
        // table while their boundary callback calls ordinary word motion to
        // avoid reentrancy.  Keep the value and special declaration atomic.
        let word_boundary_table = interp.make_char_table(None, Value::Nil);
        interp.define_special_variable("find-word-boundary-function-table", word_boundary_table);
        // syntax.c also owns this scanner policy switch.  Its default is t,
        // and Lisp navigation/indentation code dynamically binds it while
        // calling separately defined helpers.
        interp.define_special_variable("open-paren-in-column-0-is-defun-start", Value::T);
        // GNU textprop.c supplies syntax-table/display, and the dumped Lisp
        // image adds composition/fill-space.  `insert-and-inherit' consults
        // this process-wide special when deciding which adjacent properties
        // may propagate onto newly inserted text.
        interp.define_special_variable(
            "text-property-default-nonsticky",
            Value::list([
                Value::cons(Value::Symbol("fill-space".into()), Value::T),
                Value::cons(Value::Symbol("composition".into()), Value::T),
                Value::cons(Value::Symbol("syntax-table".into()), Value::T),
                Value::cons(Value::Symbol("display".into()), Value::T),
            ]),
        );
        // buffer.c syms_of_buffer: `Fput (Qkill_buffer_hook,
        // Qpermanent_local, Qt)' -- a buffer-local kill hook survives major
        // mode changes (erc-d's canned-dialog buffers register their
        // cleanup before switching to lisp-data-mode).
        interp.put_symbol_property("kill-buffer-hook", "permanent-local", Value::T);
        // files.el keeps `write-file-functions' global by default: a local
        // binding is allowed and survives a mode change, but ordinary global
        // additions must remain visible in every buffer.  The two legacy and
        // contents hooks are genuinely `defvar-local'.
        interp.define_special_variable("write-file-functions", Value::Nil);
        interp.put_symbol_property("write-file-functions", "permanent-local", Value::T);
        interp.define_per_buffer_special("local-write-file-hooks", Value::Nil);
        interp.put_symbol_property("local-write-file-hooks", "permanent-local", Value::T);
        interp.define_per_buffer_special("write-contents-functions", Value::Nil);
        interp.define_per_buffer_special("buffer-save-without-query", Value::Nil);
        for (name, value) in [
            ("save-some-buffers-default-predicate", Value::Nil),
            ("save-some-buffers-functions", Value::Nil),
            ("kill-emacs-query-functions", Value::Nil),
            ("confirm-kill-emacs", Value::Nil),
            ("confirm-kill-processes", Value::T),
        ] {
            interp.define_special_variable(name, value);
        }
        interp.define_special_variable("require-final-newline", Value::Nil);
        // files.el defines this as nil and then calls
        // `make-variable-buffer-local'.  Merely carrying the property is not
        // enough: otherwise setting it in one buffer changes every buffer's
        // save policy.
        interp.define_per_buffer_special("buffer-offer-save", Value::Nil);
        interp.put_symbol_property("buffer-offer-save", "permanent-local", Value::T);
        interp.put_symbol_property("backup-inhibited", "permanent-local", Value::T);
        // mule.el is dumped before files.el.  Save/revert code reads this
        // automatically buffer-local coding choice directly.
        interp.define_per_buffer_special("buffer-file-coding-system-explicit", Value::Nil);
        interp.put_symbol_property(
            "buffer-file-coding-system-explicit",
            "permanent-local",
            Value::T,
        );
        // GNU loadup preloads vc-hooks.el and uniquify.el before files.el.
        // files.el reads these bindings directly, without boundp guards.
        interp.define_per_buffer_special("vc-mode", Value::Nil);
        interp.put_symbol_property("vc-mode", "permanent-local", Value::T);
        for (name, value) in [
            (
                "uniquify-buffer-name-style",
                Value::Symbol("post-forward-angle-brackets".into()),
            ),
            ("uniquify-separator", Value::Nil),
            ("uniquify-trailing-separator-p", Value::Nil),
        ] {
            interp.define_special_variable(name, value);
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
        interp.define_special_variable(
            "minibuffer-prompt-properties",
            Value::list([Value::Symbol("read-only".into()), Value::T]),
        );
        interp.define_special_variable("minibuffer-auto-raise", Value::Nil);
        // minibuf.c defines this host boolean beside the native minibuffer
        // state.  Its dynamic scope is observable through dumped Elisp such
        // as subr.el's `y-or-n-p', which calls the native reader from a
        // separately defined function.
        interp.define_special_variable("inhibit-interaction", Value::Nil);
        // keyboard.c defines this before minibuffer.el.  Completion callers
        // dynamically shorten it, so both the native default and special
        // binding contract must exist before their lexical code is loaded.
        interp.define_special_variable("minibuffer-message-timeout", Value::Integer(2));
        // minibuf.c owns `read-expression-history' and
        // `read-buffer-function'; fileio.c owns `read-file-name-function'.
        // Each DEFVAR_LISP exists before dumped Lisp runs and declares the
        // value cell special, so lexical callers can dynamically bind it
        // around separately defined prompt code.
        for name in [
            "read-expression-history",
            "read-buffer-function",
            "read-file-name-function",
        ] {
            interp.define_special_variable(name, Value::Nil);
        }
        interp.define_special_variable(
            "exec-directory",
            Value::String(
                primitives::current_invocation_directory()
                    .unwrap_or_else(primitives::default_directory)
                    .into(),
            ),
        );
        interp.define_per_buffer_special("mark-ring", Value::Nil);
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
        for (name, value) in [
            ("last-command", Value::Nil),
            ("real-last-command", Value::Nil),
            ("last-repeatable-command", Value::Nil),
            ("this-command", Value::Nil),
            ("real-this-command", Value::Nil),
            ("current-minibuffer-command", Value::Nil),
            ("this-command-keys-shift-translated", Value::Nil),
            ("this-original-command", Value::Nil),
            ("auto-save-interval", Value::Integer(300)),
            ("auto-save-no-message", Value::Nil),
            ("auto-save-timeout", Value::Integer(30)),
            ("echo-keystrokes", Value::Integer(1)),
            ("echo-keystrokes-help", Value::T),
            ("polling-period", Value::float(2.0)),
            ("double-click-time", Value::Integer(500)),
            ("double-click-fuzz", Value::Integer(3)),
            ("num-input-keys", Value::Integer(0)),
            ("num-nonmacro-input-events", Value::Integer(0)),
            ("last-event-frame", Value::Nil),
            ("last-event-device", Value::Nil),
            ("help-char", Value::Integer(8)),
            ("help-event-list", Value::Nil),
            ("help-form", Value::Nil),
            ("prefix-help-command", Value::Nil),
        ] {
            interp.define_special_variable(name, value);
        }
        // eval.c defines the debugger controls before loading dumped Lisp.
        // Their special declarations are part of the evaluator boundary:
        // ERT, Edebug, and command-loop code let-bind `debugger' or its
        // policy in one lexical function and expect separately defined error
        // handlers to observe the active binding.  Keep eval.c's initial
        // `debug-early' value here; unchanged loaddefs.el replaces it with
        // `debug' while building the Lisp image.
        for (name, value) in [
            ("debugger", Value::Symbol("debug-early".into())),
            ("debug-on-error", Value::Nil),
            ("debug-on-quit", Value::Nil),
            ("debug-on-signal", Value::Nil),
            ("debugger-may-continue", Value::T),
            ("debug-on-next-call", Value::Nil),
            ("backtrace-on-error-noninteractive", Value::T),
        ] {
            interp.define_special_variable(name, value);
        }
        // minibuf.c plus the dumped minibuffer.el provide the completion
        // variables consumed by the native completion engine.  They are not
        // mere fallback constants: each DEFVAR/defcustom also declares the
        // name special, so callers can let-bind policy around a completion
        // function defined elsewhere (Completion Preview does exactly this).
        interp.define_special_variable("completion-ignore-case", Value::Nil);
        interp.define_special_variable("completion-regexp-list", Value::Nil);
        interp.define_special_variable("completion-auto-help", Value::T);
        interp.define_special_variable("completion-extra-properties", Value::Nil);
        interp.define_special_variable("enable-recursive-minibuffers", Value::Nil);
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
            interp.define_special_variable(name, Value::Nil);
        }
        interp.define_special_variable("minibuffer-history-variable", Value::Integer(0));
        for name in ["completion-styles", "completion-styles-alist"] {
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
        interp.define_special_variable(
            "eldoc-echo-area-use-multiline-p",
            Value::Symbol("truncate-sym-name-if-fit".into()),
        );
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
        interp.define_special_variable("search-spaces-regexp", Value::Nil);
        interp.define_special_variable("inhibit-changing-match-data", Value::Nil);
        interp.set_global_binding("search-whitespace-regexp", Value::String("[ \t]+".into()));
        // GNU preloads window.el, whose `defcustom' both initializes this
        // user action table and declares it special.  Buffer-display policy
        // is commonly let-bound in a lexical caller and consumed by a
        // separately defined display function (ERC does exactly this), so a
        // merely lexical Emaxx binding silently loses the user action.
        interp.define_special_variable("display-buffer-alist", Value::Nil);
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
            interp.define_special_variable(name, Value::Nil);
        }
        for name in ["mode-line-in-non-selected-windows", "auto-window-vscroll"] {
            interp.define_special_variable(name, Value::T);
        }
        // keyboard.c's dumped translation table default; simple.el reads it
        // during interactive input handling.
        interp.define_special_variable("keyboard-translate-table", Value::Nil);
        // C-owned DEFVARs (xdisp.c, frame.c, dispnew.c, keyboard.c, undo.c,
        // callint.c, minibuf.c, alloc.c, emacs.c, terminal.c, term.c,
        // fringe.c, syntax.c, fns.c) restored as real construction bindings
        // with their C initialization values: the step-5 reclassification
        // dropped them as if Lisp-owned, but GNU's C defines every one
        // before loadup, and preloaded Lisp reads them unconditionally
        // (simple.el's line-move reads `scroll-conservatively' on every
        // interactive C-p, which is how the tty battery caught the gap).
        for (name, value) in [
            ("blink-cursor-alist", Value::Nil),
            ("composition-break-at-point", Value::Nil),
            ("debug-on-event", Value::Symbol("sigusr2".into())),
            ("display-fill-column-indicator", Value::Nil),
            ("display-fill-column-indicator-character", Value::Nil),
            ("display-fill-column-indicator-column", Value::T),
            ("display-line-numbers", Value::Nil),
            ("display-line-numbers-current-absolute", Value::T),
            ("display-line-numbers-major-tick", Value::Integer(0)),
            ("display-line-numbers-minor-tick", Value::Integer(0)),
            ("display-line-numbers-widen", Value::Nil),
            ("display-line-numbers-width", Value::Nil),
            ("display-raw-bytes-as-hex", Value::Nil),
            ("focus-follows-mouse", Value::Nil),
            (
                "frame-inhibit-implied-resize",
                Value::list([Value::Symbol("tab-bar-lines".into())]),
            ),
            ("frame-resize-pixelwise", Value::Nil),
            ("garbage-collection-messages", Value::Nil),
            // alloc.c:8191.  emacs.c changes this later at executable startup:
            // an initialized batch process gets 1.0, while temacs/loadup and
            // interactive processes keep this C default.
            ("gc-cons-percentage", Value::float(0.1)),
            ("highlight-nonselected-windows", Value::Nil),
            ("hourglass-delay", Value::Integer(1)),
            (
                "iconify-child-frame",
                Value::Symbol("iconify-top-level".into()),
            ),
            ("inverse-video", Value::Nil),
            ("line-number-display-limit", Value::Nil),
            ("line-number-display-limit-width", Value::Integer(200)),
            ("make-cursor-line-fully-visible", Value::T),
            ("make-pointer-invisible", Value::T),
            ("mark-even-if-inactive", Value::T),
            ("maximum-scroll-margin", Value::float(0.25)),
            ("menu-bar-mode", Value::T),
            ("menu-prompting", Value::T),
            ("minibuffer-follows-selected-frame", Value::T),
            ("mode-line-compact", Value::Nil),
            ("mouse-autoselect-window", Value::Nil),
            ("mouse-highlight", Value::T),
            ("mouse-prefer-closest-glyph", Value::Nil),
            ("no-redraw-on-reenter", Value::Nil),
            ("overflow-newline-into-fringe", Value::T),
            ("overline-margin", Value::Integer(2)),
            ("read-buffer-completion-ignore-case", Value::Nil),
            ("record-all-keys", Value::Nil),
            (
                "report-emacs-bug-address",
                Value::String("bug-gnu-emacs@gnu.org".into()),
            ),
            ("resize-mini-frames", Value::Nil),
            ("ring-bell-function", Value::Nil),
            ("scroll-conservatively", Value::Integer(0)),
            ("scroll-step", Value::Integer(0)),
            ("show-trailing-whitespace", Value::Nil),
            ("tab-bar-position", Value::Nil),
            ("tool-bar-max-label-size", Value::Integer(14)),
            ("tool-bar-mode", Value::T),
            ("tool-bar-style", Value::Nil),
            ("tooltip-reuse-hidden-frame", Value::Nil),
            ("translate-upper-case-key-bindings", Value::T),
            ("underline-minimum-offset", Value::Integer(1)),
            ("undo-limit", Value::Integer(160000)),
            // undo.c:474.  emacs.c changes this to nil when it processes the
            // batch switch; keep the C initializer at the construction layer.
            ("undo-outer-limit", Value::Integer(24_000_000)),
            ("undo-strong-limit", Value::Integer(240000)),
            ("unibyte-display-via-language-environment", Value::Nil),
            ("use-system-tooltips", Value::T),
            ("visible-bell", Value::Nil),
            ("visible-cursor", Value::T),
            ("void-text-area-pointer", Value::Symbol("arrow".into())),
            ("word-wrap-by-category", Value::Nil),
            ("words-include-escapes", Value::Nil),
            ("x-underline-at-descent-line", Value::Nil),
            // DEFVAR_BOOL with no explicit assignment: C-false, and the oracle
            // agrees (nil); the tty branch's t was a transcription slip.
            ("x-use-underline-position-properties", Value::Nil),
            ("yes-or-no-prompt", Value::String("(yes or no) ".into())),
            // syntax.c: forward-comment's backward scan anchors its
            // recovery parse through `syntax-ppss' by default, and the
            // escaped-ender rule is a bound (buffer-local-capable) nil,
            // not an absent variable.
            ("comment-use-syntax-ppss", Value::T),
            ("comment-end-can-be-escaped", Value::Nil),
        ] {
            interp.define_special_variable(name, value);
        }
        // syntax.c:3797 Fmake_variable_buffer_local: a setq must localize,
        // never leak into the global default.
        interp.mark_auto_buffer_local("comment-end-can-be-escaped");
        for (name, value) in [
            ("next-screen-context-lines", Value::Integer(2)),
            ("eol-mnemonic-unix", Value::String(":".into())),
            ("eol-mnemonic-dos", Value::String("\\".into())),
            ("eol-mnemonic-mac", Value::String("/".into())),
            ("eol-mnemonic-undecided", Value::String(":".into())),
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
            interp.define_special_variable(name, value);
        }
        let selected_window = interp.create_pseudovector(
            RecordKind::Window,
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
        let Value::Record(selected_window_id) = selected_window else {
            unreachable!("window records use Value::Record");
        };
        interp.selected_window_id = selected_window_id;
        interp.root_window_id = selected_window_id;
        interp.old_selected_window_id = selected_window_id;
        if let Some(window) = interp.find_record_mut(selected_window_id) {
            window.slots[primitives::WINDOW_USE_TIME_SLOT] = Value::Integer(1);
        }
        let (minibuffer_buffer_id, _) = interp.create_buffer(" *Minibuf-0*");
        interp.buffer_list.push((1, "*Messages*".to_string()));
        let minibuffer_window = interp.create_pseudovector(
            RecordKind::Window,
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
        let Value::Record(minibuffer_window_id) = minibuffer_window else {
            unreachable!("window records use Value::Record");
        };
        interp.minibuffer_window_id = minibuffer_window_id;
        // Interpreter::new also constructs several dumped values after the
        // base struct exists (keymaps, tables, and window objects).  Reconcile
        // the completed image through the same registry before exposing it;
        // later user `setq' calls are intentionally outside this boundary.
        let completed_startup_globals = interp
            .globals
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        for name in completed_startup_globals {
            interp.mark_special_variable(&name);
        }
        // GNU's defsubr interns every C primitive's name in the standard
        // obarray as the image is built (lread.c:defsubr -> intern_c_string),
        // so a bare `emacs -Q -batch' answers `intern-soft' for names like
        // `1-' or `decode-sjis-char' before any Lisp mentions them.  Emaxx
        // dispatches those primitives by name without a per-name Lisp
        // binding, so register the same committed DEFUN contract surface the
        // dispatch layer is built and gated against (finding 112/#27).  An
        // `arity: None' contract names a DEFUN outside the oracle build
        // (another platform's *.c); the oracle never defsubrs those, so
        // neither does this registration.
        for contract in primitives::GNU_C_PRIMITIVES {
            if contract.arity.is_some() {
                interp.intern_symbol_name(contract.name);
            }
        }
        // GNU's syms_of_* initializers likewise intern every DEFSYM
        // (lisp.h:DEFSYM -> staticpro'd intern_c_string), covering names
        // with neither a function nor a variable cell: process-attribute
        // keys (`comm', `ctime'), GnuTLS constants, font keywords.  The
        // DEFSYM manifest is a source contract over all of src/*.c; which
        // files this image "compiles" comes from the DEFUN manifest's
        // availability facts: a file registers its DEFSYMs when at least
        // one of its DEFUNs is available in the oracle build.  Sources of
        // other window systems (android*/w32*/haiku*/pgtk*, xfns.c,
        // xmenu.c) reach that set only through DEFUN names shared across
        // platforms, so they are excluded by name — the same platform
        // taxonomy as docs/oracle-build-contract.md.
        {
            let compiled_files: std::collections::HashSet<&'static str> =
                primitives::GNU_C_PRIMITIVES
                    .iter()
                    .filter(|contract| contract.arity.is_some())
                    .flat_map(|contract| contract.origins.split(", "))
                    .filter(|file| {
                        !(file.starts_with("android")
                            || file.starts_with("w32")
                            || file.starts_with("haiku")
                            || file.starts_with("pgtk")
                            || *file == "xfns.c"
                            || *file == "xmenu.c")
                    })
                    .collect();
            for defsym in primitives::GNU_C_DEFSYMS {
                if defsym
                    .origins
                    .split(", ")
                    .any(|file| compiled_files.contains(file))
                {
                    interp.intern_symbol_name(defsym.name);
                }
            }
        }
        crate::lisp::native_comp::initialize_runtime(&mut interp);
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

    pub(crate) fn set_prefer_compiled_loads(&mut self, prefer: bool) {
        self.prefer_compiled_loads = prefer;
    }

    pub(crate) fn prefers_compiled_loads(&self) -> bool {
        self.prefer_compiled_loads
    }

    pub(crate) fn configured_load_path(&self) -> &[PathBuf] {
        &self.load_path
    }

    pub(crate) fn push_lambda_capture_override(&mut self, capture: bool) {
        self.lambda_capture_overrides.push(capture);
    }

    pub(crate) fn push_lambda_eval_context(&mut self, capture: bool) {
        self.lambda_capture_overrides.push(capture);
    }

    pub(crate) fn with_lambda_eval_context<T>(
        &mut self,
        capture: bool,
        operation: impl FnOnce(&mut Self) -> T,
    ) -> T {
        self.push_lambda_eval_context(capture);
        let result = operation(self);
        self.pop_lambda_capture_override();
        result
    }

    pub(crate) fn pop_lambda_capture_override(&mut self) {
        self.lambda_capture_overrides.pop();
    }

    pub(crate) fn lambda_capture_override(&self) -> Option<bool> {
        self.lambda_capture_overrides.last().copied()
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

    /// Materialize the six GNU-visible interpreted-closure slots from the
    /// typed runtime representation.  Every Lisp-facing consumer (`aref',
    /// equality, hashing, documentation, and interactive metadata) must use
    /// this owner rather than reconstructing a partial slot layout.
    pub(crate) fn interpreted_closure_slots(&self, lambda: &LambdaValue) -> Vec<Value> {
        let environment = if let Some(environment) = &lambda.public_environment {
            // A captured variable mutated after a merge-path call can live
            // only in `lexical_cell_updates' (the write-back replaced this
            // closure's frames, detaching them from the alist).  Fold those
            // updates into the cached alist's own conses, so `aref' and
            // `byte-compile' see current values through GNU's cons
            // identities (the alist conses ARE the storage in GNU).
            for frame in lambda.env.borrow().iter() {
                if let Some(updates) = Self::frame_identity(frame)
                    .and_then(|frame_id| self.lexical_cell_updates.get(&frame_id))
                {
                    for (name, value) in updates {
                        bindings::set_lisp_environment_binding(environment, name, value.clone());
                    }
                }
            }
            environment.clone()
        } else {
            let mut environment = Vec::new();
            for frame in lambda.env.borrow().iter().rev() {
                let shared_updates = Self::frame_identity(frame)
                    .and_then(|frame_id| self.lexical_cell_updates.get(&frame_id));
                for position in (0..=frame.len()).rev() {
                    for (_, name) in frame
                        .local_special_declarations()
                        .iter()
                        .rev()
                        .filter(|(declared_at, _)| *declared_at == position)
                    {
                        environment.push(Value::Symbol(name.clone().into()));
                    }
                    if position > 0 {
                        let (name, value) = &frame[position - 1];
                        environment.push(
                            frame.canonical_lisp_binding(
                                position - 1,
                                name,
                                shared_updates
                                    .and_then(|updates| updates.get(name))
                                    .cloned()
                                    .unwrap_or_else(|| value.clone()),
                            ),
                        );
                    }
                }
            }
            // GNU represents an EMPTY lexical environment as `(t)'.  A
            // nonempty lexical environment is just its bindings; appending
            // `t' there changes the public ENV supplied to
            // `make-interpreted-closure'.
            if environment.is_empty() && self.closure_env_is_lexical(&lambda.env) {
                environment.push(Value::T);
            }
            Value::list(environment)
        };

        let mut slots = vec![
            lambda.public_parameters.clone().unwrap_or_else(|| {
                Value::list(
                    lambda
                        .params
                        .iter()
                        .map(|param| Value::Symbol(param.clone().into())),
                )
            }),
            Value::list(lambda.body.as_ref().clone()),
            environment,
        ];
        if lambda.interactive.is_some() || lambda.documentation.is_some() {
            slots.push(Value::Nil);
            slots.push(lambda.documentation.clone().unwrap_or(Value::Nil));
        }
        if let Some(interactive) = &lambda.interactive {
            slots.push(interactive.clone());
        }
        slots
    }

    /// Build the GNU-visible lexical-environment alist for a source lambda
    /// and attach its binding conses to the typed captured frames.  Each
    /// closure gets its own alist spine, while sibling closures reuse the
    /// same binding conses, matching GNU's observable `(nil t t ...)'
    /// identity pattern for their two slot-two objects and first entries.
    pub(crate) fn materialize_public_interpreted_environment(
        &self,
        closure_env: &SharedEnv,
    ) -> Value {
        let lexical = self.closure_env_is_lexical(closure_env);
        let mut all_entries = Vec::new();
        let mut frames = closure_env.borrow_mut();
        for index in (0..frames.len()).rev() {
            let bindings = frames[index].iter().cloned().collect::<Vec<_>>();
            let entries = if let Some(environment) = frames[index].lisp_environment() {
                lisp_environment_entries(environment)
            } else {
                let shared_updates = Self::frame_identity(&frames[index])
                    .and_then(|frame_id| self.lexical_cell_updates.get(&frame_id));
                let mut entries = Vec::new();
                for position in (0..=bindings.len()).rev() {
                    for (_, name) in frames[index]
                        .local_special_declarations()
                        .iter()
                        .rev()
                        .filter(|(declared_at, _)| *declared_at == position)
                    {
                        entries.push(Value::Symbol(name.clone().into()));
                    }
                    if position > 0 {
                        let (name, value) = &bindings[position - 1];
                        entries.push(
                            frames[index].canonical_lisp_binding(
                                position - 1,
                                name,
                                shared_updates
                                    .and_then(|updates| updates.get(name))
                                    .cloned()
                                    .unwrap_or_else(|| value.clone()),
                            ),
                        );
                    }
                }
                entries
            };
            if frames[index].lisp_environment().is_none() {
                frames[index].set_lisp_environment(Value::list(entries.iter().cloned()));
            }
            all_entries.extend(entries);
        }
        if all_entries.is_empty() && lexical {
            Value::list([Value::T])
        } else {
            Value::list(all_entries)
        }
    }

    /// Register TAG as an active `catch' target for the extent of a native
    /// command-loop boundary (see GNU read_minibuf's `catch \='exit`).
    pub(crate) fn push_catch_tag(&mut self, tag: Value) {
        self.active_catch_tags.push(tag);
    }

    pub(crate) fn pop_catch_tag(&mut self) {
        self.active_catch_tags.pop();
    }

    pub(crate) fn record_lexical_cell_update_if_captured(
        &mut self,
        frame_id: i64,
        name: &str,
        value: &Value,
        captured: bool,
    ) {
        let already_shared = self.lexical_cell_updates.contains_key(&frame_id);
        if already_shared || captured {
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

    pub(crate) fn eval_with_closure_env<F>(
        &mut self,
        closure_env: &SharedEnv,
        env: &mut Env,
        evaluate: F,
    ) -> Result<Value, LispError>
    where
        F: FnOnce(&mut Self, &mut Env) -> Result<Value, LispError>,
    {
        self.refresh_captured_lexical_cells(env);

        // The common immediately-invoked-closure case still has every
        // captured lexical frame live in the caller.  Stable frame identities
        // prove these are the same GNU lexical cells, so evaluating directly
        // on the live environment avoids cloning and later republishing the
        // entire capture.  Require an exact frame-for-frame match: trimmed,
        // escaped, synthetic, and merely same-shaped environments retain the
        // general isolated merge path below.
        let captured_is_current = {
            let captured = closure_env.borrow();
            !captured.is_empty()
                && captured.len() == env.len()
                && captured.iter().zip(env.iter()).all(|(captured, current)| {
                    let identity = Self::frame_identity(captured);
                    identity.is_some() && identity == Self::frame_identity(current)
                })
        };
        if captured_is_current {
            return evaluate(self, env);
        }

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
        result
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
            self.set_variable(&var, Value::String(captured.into()), env);
        }
    }

    // Capture a lambda's lexical environment, sharing the environment cell
    // with sibling closures from the same activation whose captured content
    // is identical.
    pub(crate) fn capture_closure_env(&mut self, mut captured: Env) -> SharedEnv {
        self.refresh_captured_lexical_cells(&mut captured);
        for frame in &captured {
            frame.mark_captured();
        }
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
            return existing;
        }
        let shared = shared_env(captured);
        self.closure_capture_cache
            .push((activation, Rc::downgrade(&shared)));
        if self.closure_capture_cache.len() > 128 {
            self.closure_capture_cache.remove(0);
        }
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
        a.identity() == b.identity()
            && a.has_function_bindings() == b.has_function_bindings()
            && a.local_special_declarations() == b.local_special_declarations()
            && a.len() == b.len()
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
        (Value::Cons(a), Value::Cons(b)) => {
            bounded_value_eq(&a.car.borrow(), &b.car.borrow(), budget)
                && bounded_value_eq(&a.cdr.borrow(), &b.cdr.borrow(), budget)
        }
        (Value::Lambda(a), Value::Lambda(b)) => {
            a.params == b.params
                && Rc::ptr_eq(&a.env, &b.env)
                && a.body.len() == b.body.len()
                && a.body
                    .iter()
                    .zip(b.body.iter())
                    .all(|(a, b)| bounded_value_eq(a, b, budget))
        }
        _ => left == right,
    }
}

fn symbol_name(value: &Value) -> Option<String> {
    match value {
        Value::Symbol(name) => Some(name.to_string()),
        _ => None,
    }
}

fn function_name_from_binding_form(value: &Value) -> Result<String, LispError> {
    match value {
        Value::Cons(_) => {
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
            Err(LispError::WrongTypeArgument(
                "symbolp".into(),
                other.clone(),
            ))
        }
        _ => match unquote(value) {
            Value::Symbol(name) => Ok(name.to_string()),
            other => Err(LispError::WrongTypeArgument(
                "symbolp".into(),
                other.clone(),
            )),
        },
    }
}

fn assignment_target_name(value: &Value) -> Result<String, LispError> {
    match value {
        Value::Symbol(name) => Ok(name.to_string()),
        Value::Nil => Ok("nil".into()),
        Value::T => Ok("t".into()),
        other => Err(LispError::WrongTypeArgument(
            "symbolp".into(),
            other.clone(),
        )),
    }
}

fn unquote(value: &Value) -> Value {
    match value {
        Value::Cons(_) => {
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

pub(crate) fn error_condition_value(error: &LispError) -> Value {
    match error {
        LispError::TypeError(expected, got) => Value::list([
            Value::Symbol("wrong-type-argument".into()),
            Value::Symbol(expected.clone().into()),
            match got.as_str() {
                "nil" => Value::Nil,
                _ => Value::String(got.clone().into()),
            },
        ]),
        // GNU's datum: the predicate symbol and the offending value itself.
        LispError::WrongTypeArgument(predicate, value) => Value::list([
            Value::Symbol("wrong-type-argument".into()),
            Value::Symbol(predicate.clone().into()),
            value.clone(),
        ]),
        LispError::Void(symbol) => Value::list([
            Value::Symbol("void-variable".into()),
            Value::Symbol(symbol.clone().into()),
        ]),
        LispError::VoidFunction(symbol) => Value::list([
            Value::Symbol("void-function".into()),
            Value::Symbol(symbol.clone().into()),
        ]),
        LispError::WrongNumberOfArgs(name, count) => Value::list([
            Value::Symbol("wrong-number-of-arguments".into()),
            Value::Symbol(name.clone().into()),
            Value::Integer(*count as i64),
        ]),
        LispError::EndOfInput => Value::list([Value::Symbol("end-of-file".into()), Value::Nil]),
        LispError::TestSkipped(message) => Value::list([
            Value::Symbol("ert-test-skipped".into()),
            Value::String(message.clone().into()),
        ]),
        LispError::ErtTestFailed(message) => Value::list([
            Value::Symbol("ert-test-failed".into()),
            Value::String(message.clone().into()),
        ]),
        LispError::ReadError(message) => Value::list([
            Value::Symbol("invalid-read-syntax".into()),
            Value::String(message.clone().into()),
        ]),
        LispError::Signal(message) => Value::list([
            Value::Symbol("error".into()),
            Value::String(message.clone().into()),
        ]),
        LispError::Throw(tag, value) => {
            Value::list([Value::Symbol("no-catch".into()), tag.clone(), value.clone()])
        }
        LispError::Terminate(_) => {
            unreachable!("process termination cannot be converted to a Lisp condition")
        }
        LispError::VmReturn(_) => unreachable!("bytecode return escaped the VM"),
        LispError::SignalValue(value) => value.clone(),
    }
}

fn buffer_undo_head_to_entry(value: &Value) -> crate::buffer::UndoEntry {
    match value {
        Value::Nil => crate::buffer::UndoEntry::Boundary,
        Value::Cons(_) => match value.cons_values() {
            // GNU records an insertion as (BEG . END).
            Some((Value::Integer(beg), Value::Integer(end))) if beg >= 0 && end >= beg => {
                crate::buffer::UndoEntry::Insert {
                    pos: beg as usize,
                    len: (end - beg) as usize,
                }
            }
            Some((Value::String(text), Value::Integer(pos))) => crate::buffer::UndoEntry::Delete {
                pos: pos.unsigned_abs() as usize,
                point_after: pos < 0,
                text: text.to_string(),
                props: Vec::new(),
                extended_chars: Vec::new(),
                markers: Vec::new(),
            },
            _ => crate::buffer::UndoEntry::Opaque(value.clone()),
        },
        _ => crate::buffer::UndoEntry::Opaque(value.clone()),
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
        )
    {
        start += 1;
    }
    &body[start..]
}

fn lisp_environment_entries(environment: &Value) -> Vec<Value> {
    let mut entries = Vec::new();
    let mut cursor = environment.clone();
    for _ in 0..65_536 {
        let Value::Cons(list_cell) = cursor else {
            break;
        };
        entries.push(list_cell.car.borrow().clone());
        cursor = list_cell.cdr.borrow().clone();
    }
    entries
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

fn is_lambda_form(interp: &Interpreter, value: &Value, env: &Env) -> bool {
    value.to_vec().ok().is_some_and(|items| {
        items.first().is_some_and(|head| {
            matches!(head, Value::Symbol(name) if name == "lambda")
                || (crate::lisp::primitives::symbols_with_pos_enabled(interp, env)
                    && matches!(
                        crate::lisp::primitives::symbol_with_pos_parts(interp, head),
                        Some((Value::Symbol(name), _)) if name == "lambda"
                    ))
        })
    })
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

// GNU pcase--funcall for `app' patterns: a call form may name the object
// with `_'; without a placeholder the object becomes the last argument.

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
