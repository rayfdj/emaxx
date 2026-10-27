//! Owned host entry points for the process-wide Lisp runtime.
//!
//! GC objects cannot escape an editor call or be retained while another OS
//! thread collects the heap. Raw interpreter and value APIs are internal:
//!
//! ```compile_fail
//! use emaxx::lisp::{eval::Interpreter, types::Value};
//! let mut interpreter = Interpreter::new();
//! let retained = Value::cons(Value::Nil, Value::Nil);
//! ```

/// Define a primitive dispatcher and derive its name probe from the same arms.
///
/// Dispatch routing and implementation must share one inventory: otherwise a
/// newly implemented primitive can remain unreachable merely because a second
/// `handles` list was not updated.
/// A fixed Qsymbol-style reference owned by the current editor entry.
macro_rules! cached_symbol {
    ($name:literal) => {{
        const SYMBOL: $crate::lisp::types::symbols::SymbolKey =
            $crate::lisp::types::symbols::SymbolKey::new($name);
        SYMBOL
    }};
}

macro_rules! dispatch_handles {
    ($name:ident;) => {
        false
    };
    ($name:ident; , $($rest:tt)*) => {
        dispatch_handles!($name; $($rest)*)
    };
    ($name:ident; #[dispatch($($property:ident),+)] $($rest:tt)*) => {
        dispatch_handles!($name; $($rest)*)
    };
    (
        $name:ident;
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:block
        $($rest:tt)*
    ) => {
        match $name {
            $(#[$attribute])*
            $pattern => true,
            _ => dispatch_handles!($name; $($rest)*),
        }
    };
    (
        $name:ident;
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:expr,
        $($rest:tt)*
    ) => {
        match $name {
            $(#[$attribute])*
            $pattern => true,
            _ => dispatch_handles!($name; $($rest)*),
        }
    };
}

#[cfg(test)]
macro_rules! dispatch_visit_patterns {
    ($visitor:ident;) => {
        ()
    };
    ($visitor:ident; , $($rest:tt)*) => {
        dispatch_visit_patterns!($visitor; $($rest)*)
    };
    ($visitor:ident; #[dispatch($($property:ident),+)] $($rest:tt)*) => {
        dispatch_visit_patterns!($visitor; $($rest)*)
    };
    (
        $visitor:ident;
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:block
        $($rest:tt)*
    ) => {{
        $(#[$attribute])*
        $visitor(stringify!($pattern));
        dispatch_visit_patterns!($visitor; $($rest)*)
    }};
    (
        $visitor:ident;
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:expr,
        $($rest:tt)*
    ) => {{
        $(#[$attribute])*
        $visitor(stringify!($pattern));
        dispatch_visit_patterns!($visitor; $($rest)*)
    }};
}

// The synthetic dispatch regression below still exercises attribute gating.
// No production subr may bypass its current Lisp function cell.
#[cfg(test)]
macro_rules! dispatch_select_builtin_override {
    ($name:ident, $pattern:pat =>) => {
        false
    };
    ($name:ident, $pattern:pat => builtin_override $(, $rest:ident)*) => {
        matches!($name, $pattern)
    };
    ($name:ident, $pattern:pat => $other:ident $(, $rest:ident)*) => {
        dispatch_select_builtin_override!($name, $pattern => $($rest),*)
    };
}

#[cfg(test)]
macro_rules! dispatch_property {
    ($selector:ident, $name:ident;) => {
        false
    };
    ($selector:ident, $name:ident; , $($rest:tt)*) => {
        dispatch_property!($selector, $name; $($rest)*)
    };
    (
        $selector:ident, $name:ident;
        #[dispatch($($property:ident),+)]
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:block
        $($rest:tt)*
    ) => {
        (match $name {
            $(#[$attribute])*
            $pattern => $selector!($name, $pattern => $($property),+),
            _ => false,
        }) || dispatch_property!($selector, $name; $($rest)*)
    };
    (
        $selector:ident, $name:ident;
        #[dispatch($($property:ident),+)]
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:expr,
        $($rest:tt)*
    ) => {
        (match $name {
            $(#[$attribute])*
            $pattern => $selector!($name, $pattern => $($property),+),
            _ => false,
        }) || dispatch_property!($selector, $name; $($rest)*)
    };
    (
        $selector:ident, $name:ident;
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:block
        $($rest:tt)*
    ) => {
        dispatch_property!($selector, $name; $($rest)*)
    };
    (
        $selector:ident, $name:ident;
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:expr,
        $($rest:tt)*
    ) => {
        dispatch_property!($selector, $name; $($rest)*)
    };
}

/// The lifted-primitive table of a `define_dispatch!' body: one function
/// per literal arm (an alternation gives one per name), each with the
/// arm's body and the name bound as the dispatcher's `name'.  Only the
/// argument shapes `(interp, name, args, env)', `(interp, name, args)'
/// and `(name, args)' are lifted; a guarded or non-literal arm and any
/// other shape stays in the match.
macro_rules! dispatch_table {
    (($($argument:ident),*) [$($entries:tt)*]) => {
        &[$($entries)*]
    };
    (($($argument:ident),*) [$($entries:tt)*] , $($rest:tt)*) => {
        dispatch_table!(($($argument),*) [$($entries)*] $($rest)*)
    };
    (
        ($($argument:ident),*) [$($entries:tt)*]
        #[dispatch($($property:ident),+)] $($rest:tt)*
    ) => {
        dispatch_table!(($($argument),*) [$($entries)*] $($rest)*)
    };
    // A guarded literal arm is not lifted.
    (
        ($($argument:ident),*) [$($entries:tt)*]
        $(#[$attribute:meta])*
        $($lit:literal)|+ if $guard:expr => $body:block
        $($rest:tt)*
    ) => {
        dispatch_table!(($($argument),*) [$($entries)*] $($rest)*)
    };
    (
        ($($argument:ident),*) [$($entries:tt)*]
        $(#[$attribute:meta])*
        $($lit:literal)|+ if $guard:expr => $body:expr,
        $($rest:tt)*
    ) => {
        dispatch_table!(($($argument),*) [$($entries)*] $($rest)*)
    };
    // Split alternations before lifting so each element retains the arm's
    // attributes, including cfg/cfg_attr.  Applying them only to the residual
    // match would still compile unavailable platform bodies into this table.
    (
        ($($argument:ident),*) [$($entries:tt)*]
        $(#[$attribute:meta])*
        $first:literal | $($other:literal)|+ => $body:block
        $($rest:tt)*
    ) => {
        dispatch_table!(($($argument),*) [$($entries)*]
            $(#[$attribute])* $first => $body
            $(#[$attribute])* $($other)|+ => $body
            $($rest)*)
    };
    (
        ($($argument:ident),*) [$($entries:tt)*]
        $(#[$attribute:meta])*
        $first:literal | $($other:literal)|+ => $body:expr,
        $($rest:tt)*
    ) => {
        dispatch_table!(($($argument),*) [$($entries)*]
            $(#[$attribute])* $first => { $body }
            $(#[$attribute])* $($other)|+ => { $body }
            $($rest)*)
    };
    // (interp, name, args, env)
    (
        ($interp:ident, $name:ident, $args:ident, $env:ident) [$($entries:tt)*]
        $(#[$attribute:meta])*
        $lit:literal => $body:block
        $($rest:tt)*
    ) => {
        dispatch_table!(($interp, $name, $args, $env) [$($entries)* $(#[$attribute])* ($lit, {
            #[allow(unused_variables, unused_mut, clippy::needless_return, clippy::needless_question_mark)]
            fn lifted(
                $interp: &mut crate::lisp::eval::Interpreter,
                $args: &[crate::lisp::types::Value],
                $env: &mut crate::lisp::types::Env,
            ) -> Result<crate::lisp::types::Value, crate::lisp::types::LispError> {
                let $name: &str = $lit;
                $body
            }
            lifted as crate::lisp::primitives::DirectPrimitive
        }),] $($rest)*)
    };
    (
        ($interp:ident, $name:ident, $args:ident, $env:ident) [$($entries:tt)*]
        $(#[$attribute:meta])*
        $lit:literal => $body:expr,
        $($rest:tt)*
    ) => {
        dispatch_table!(($interp, $name, $args, $env) [$($entries)* $(#[$attribute])* ($lit, {
            #[allow(unused_variables, unused_mut, clippy::needless_return, clippy::needless_question_mark)]
            fn lifted(
                $interp: &mut crate::lisp::eval::Interpreter,
                $args: &[crate::lisp::types::Value],
                $env: &mut crate::lisp::types::Env,
            ) -> Result<crate::lisp::types::Value, crate::lisp::types::LispError> {
                let $name: &str = $lit;
                $body
            }
            lifted as crate::lisp::primitives::DirectPrimitive
        }),] $($rest)*)
    };
    // (interp, name, args)
    (
        ($interp:ident, $name:ident, $args:ident) [$($entries:tt)*]
        $(#[$attribute:meta])*
        $lit:literal => $body:block
        $($rest:tt)*
    ) => {
        dispatch_table!(($interp, $name, $args) [$($entries)* $(#[$attribute])* ($lit, {
            #[allow(unused_variables, unused_mut, clippy::needless_return, clippy::needless_question_mark)]
            fn lifted(
                $interp: &mut crate::lisp::eval::Interpreter,
                $args: &[crate::lisp::types::Value],
                _env: &mut crate::lisp::types::Env,
            ) -> Result<crate::lisp::types::Value, crate::lisp::types::LispError> {
                let $name: &str = $lit;
                $body
            }
            lifted as crate::lisp::primitives::DirectPrimitive
        }),] $($rest)*)
    };
    (
        ($interp:ident, $name:ident, $args:ident) [$($entries:tt)*]
        $(#[$attribute:meta])*
        $lit:literal => $body:expr,
        $($rest:tt)*
    ) => {
        dispatch_table!(($interp, $name, $args) [$($entries)* $(#[$attribute])* ($lit, {
            #[allow(unused_variables, unused_mut, clippy::needless_return, clippy::needless_question_mark)]
            fn lifted(
                $interp: &mut crate::lisp::eval::Interpreter,
                $args: &[crate::lisp::types::Value],
                _env: &mut crate::lisp::types::Env,
            ) -> Result<crate::lisp::types::Value, crate::lisp::types::LispError> {
                let $name: &str = $lit;
                $body
            }
            lifted as crate::lisp::primitives::DirectPrimitive
        }),] $($rest)*)
    };
    // (name, args)
    (
        ($name:ident, $args:ident) [$($entries:tt)*]
        $(#[$attribute:meta])*
        $lit:literal => $body:block
        $($rest:tt)*
    ) => {
        dispatch_table!(($name, $args) [$($entries)* $(#[$attribute])* ($lit, {
            #[allow(unused_variables, unused_mut, clippy::needless_return, clippy::needless_question_mark)]
            fn lifted(
                _interp: &mut crate::lisp::eval::Interpreter,
                $args: &[crate::lisp::types::Value],
                _env: &mut crate::lisp::types::Env,
            ) -> Result<crate::lisp::types::Value, crate::lisp::types::LispError> {
                let $name: &str = $lit;
                $body
            }
            lifted as crate::lisp::primitives::DirectPrimitive
        }),] $($rest)*)
    };
    (
        ($name:ident, $args:ident) [$($entries:tt)*]
        $(#[$attribute:meta])*
        $lit:literal => $body:expr,
        $($rest:tt)*
    ) => {
        dispatch_table!(($name, $args) [$($entries)* $(#[$attribute])* ($lit, {
            #[allow(unused_variables, unused_mut, clippy::needless_return, clippy::needless_question_mark)]
            fn lifted(
                _interp: &mut crate::lisp::eval::Interpreter,
                $args: &[crate::lisp::types::Value],
                _env: &mut crate::lisp::types::Env,
            ) -> Result<crate::lisp::types::Value, crate::lisp::types::LispError> {
                let $name: &str = $lit;
                $body
            }
            lifted as crate::lisp::primitives::DirectPrimitive
        }),] $($rest)*)
    };
    // Any other arm or shape: not lifted.
    (
        ($($argument:ident),*) [$($entries:tt)*]
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:block
        $($rest:tt)*
    ) => {
        dispatch_table!(($($argument),*) [$($entries)*] $($rest)*)
    };
    (
        ($($argument:ident),*) [$($entries:tt)*]
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:expr,
        $($rest:tt)*
    ) => {
        dispatch_table!(($($argument),*) [$($entries)*] $($rest)*)
    };
}

macro_rules! dispatch_call {
    (($($argument:ident),*) $name:ident; $($arms:tt)*) => {
        dispatch_call!(@collect ($($argument),*) $name [] $($arms)*)
    };
    // The residual match of a lifted shape: the arms a pointer cannot
    // carry, then the table for a name outside the manifest (a manifest
    // name never enters: its facts hold the pointer).
    (@collect ($interp:ident, $name_:ident, $args:ident, $env:ident) $name:ident [$($collected:tt)*]) => {
        match $name {
            $($collected)*
            _ => match LIFTED_PRIMITIVES.iter().find(|(lifted, _)| *lifted == $name) {
                Some((_, body)) => body($interp, $args, $env),
                None => unreachable!("primitive dispatcher called for unsupported name: {}", $name),
            },
        }
    };
    (@collect ($interp:ident, $name_:ident, $args:ident) $name:ident [$($collected:tt)*]) => {
        match $name {
            $($collected)*
            _ => match LIFTED_PRIMITIVES.iter().find(|(lifted, _)| *lifted == $name) {
                Some((_, body)) => body($interp, $args, &mut crate::lisp::types::Env::new()),
                None => unreachable!("primitive dispatcher called for unsupported name: {}", $name),
            },
        }
    };
    (@collect ($($argument:ident),*) $name:ident [$($collected:tt)*]) => {
        match $name {
            $($collected)*
            _ => unreachable!("primitive dispatcher called for unsupported name: {}", $name),
        }
    };
    (@collect ($($argument:ident),*) $name:ident [$($collected:tt)*] , $($rest:tt)*) => {
        dispatch_call!(@collect ($($argument),*) $name [$($collected)*] $($rest)*)
    };
    (
        @collect ($($argument:ident),*) $name:ident [$($collected:tt)*]
        #[dispatch($($property:ident),+)]
        $($rest:tt)*
    ) => {
        dispatch_call!(@collect ($($argument),*) $name [$($collected)*] $($rest)*)
    };
    // A literal arm of a lifted shape is in the table.
    (
        @collect ($interp:ident, $name_:ident, $args:ident, $env:ident) $name:ident [$($collected:tt)*]
        $(#[$attribute:meta])*
        $($lit:literal)|+ => $body:block
        $($rest:tt)*
    ) => {
        dispatch_call!(@collect ($interp, $name_, $args, $env) $name [$($collected)*] $($rest)*)
    };
    (
        @collect ($interp:ident, $name_:ident, $args:ident, $env:ident) $name:ident [$($collected:tt)*]
        $(#[$attribute:meta])*
        $($lit:literal)|+ => $body:expr,
        $($rest:tt)*
    ) => {
        dispatch_call!(@collect ($interp, $name_, $args, $env) $name [$($collected)*] $($rest)*)
    };
    (
        @collect ($interp:ident, $name_:ident, $args:ident) $name:ident [$($collected:tt)*]
        $(#[$attribute:meta])*
        $($lit:literal)|+ => $body:block
        $($rest:tt)*
    ) => {
        dispatch_call!(@collect ($interp, $name_, $args) $name [$($collected)*] $($rest)*)
    };
    (
        @collect ($interp:ident, $name_:ident, $args:ident) $name:ident [$($collected:tt)*]
        $(#[$attribute:meta])*
        $($lit:literal)|+ => $body:expr,
        $($rest:tt)*
    ) => {
        dispatch_call!(@collect ($interp, $name_, $args) $name [$($collected)*] $($rest)*)
    };
    (
        @collect ($($argument:ident),*) $name:ident [$($collected:tt)*]
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:block
        $($rest:tt)*
    ) => {
        dispatch_call!(@collect ($($argument),*) $name [
            $($collected)*
            $(#[$attribute])*
            $pattern $(if $guard)? => $body,
        ] $($rest)*)
    };
    (
        @collect ($($argument:ident),*) $name:ident [$($collected:tt)*]
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:expr,
        $($rest:tt)*
    ) => {
        dispatch_call!(@collect ($($argument),*) $name [
            $($collected)*
            $(#[$attribute])*
            $pattern $(if $guard)? => $body,
        ] $($rest)*)
    };
}

macro_rules! define_dispatch {
    (
        $(#[$attribute:meta])*
        $visibility:vis fn $call:ident(
            $($argument:ident: $argument_type:ty),* $(,)?
        ) -> $return_type:ty {
            match $name:ident {
                $($arms:tt)*
            }
        }
    ) => {
        $visibility fn handles(name: &str) -> bool {
            dispatch_handles!(name; $($arms)*)
        }

        /// lisp.h's subr per primitive: the body of each literal arm as a
        /// function, by name.  A call resolves the name to its pointer once
        /// (the facts kept per symbol) and calls through it; the match
        /// below keeps only what a pointer cannot carry.
        $visibility const LIFTED_PRIMITIVES: &[(&str, crate::lisp::primitives::DirectPrimitive)] =
            dispatch_table!(($($argument),*) [] $($arms)*);

        #[cfg(test)]
        $visibility fn prefer_builtin(name: &str) -> bool {
            let _ = name;
            dispatch_property!(dispatch_select_builtin_override, name; $($arms)*)
        }

        #[cfg(test)]
        $visibility fn visit_handled_patterns(visitor: &mut impl FnMut(&'static str)) {
            dispatch_visit_patterns!(visitor; $($arms)*);
        }

        $(#[$attribute])*
        $visibility fn $call(
            $($argument: $argument_type),*
        ) -> $return_type {
            dispatch_call!(($($argument),*) $name; $($arms)*)
        }
    };
}

pub(crate) mod alloc;
pub(crate) mod bytecode;
pub(crate) mod eval;
pub(crate) mod json;
pub(crate) mod md5;
pub(crate) mod modules;
pub(crate) mod native_comp;
pub(crate) mod primitives;
pub(crate) mod reader;
pub(crate) mod regex_emacs;
pub(crate) mod runtime;
pub(crate) mod sqlite;
pub(crate) mod types;

pub use primitives::{DaemonState, set_build_details, set_daemon_state};

use std::path::Path;

use crate::lisp::types::LispError;

/// EMAXX_TRACE_LOAD_ERRORS, latched once.  main() latches it at startup
/// and then SCRUBS the variable from the environment: the compat harness
/// sets the knob only on the measured emaxx runner (never on the GNU
/// oracle), so Lisp `getenv' and any child emacs a test spawns must see
/// the same clean environment on both runners, while this process keeps
/// its own diagnostics.
static TRACE_LOAD_ERRORS: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

pub fn latch_trace_load_errors() {
    let _ = TRACE_LOAD_ERRORS.set(std::env::var_os("EMAXX_TRACE_LOAD_ERRORS").is_some());
}

/// `fflush (stdout)' for the batch process: the stdio-style buffer behind
/// `standard-output' (primitives::batch_stdout) reaches the descriptor.
pub fn flush_batch_stdout() {
    let _ = primitives::batch_stdout::flush();
}

pub(crate) fn trace_load_errors_enabled() -> bool {
    *TRACE_LOAD_ERRORS.get_or_init(|| std::env::var_os("EMAXX_TRACE_LOAD_ERRORS").is_some())
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct SourceFileSettings {
    lexical_binding: bool,
    read_symbol_shorthands: Vec<(String, String)>,
}

fn append_message(interp: &mut eval::Interpreter, text: &str) {
    let buffer_id = interp
        .find_buffer("*Messages*")
        .map(|(id, _)| id)
        .unwrap_or_else(|| interp.create_buffer("*Messages*").0);
    if let Some(mut buffer) = interp.get_buffer_by_id_mut(buffer_id) {
        let end = buffer.point_max();
        buffer.goto_char(end);
        buffer.insert(&(text.to_string() + "\n"));
    }
}

pub(crate) fn unescaped_character_literal_warning(
    interp: &mut eval::Interpreter,
    env: &mut types::Env,
) -> Result<Option<String>, types::LispError> {
    let Ok(function) =
        interp.lookup_function("byte-run--unescaped-character-literals-warning", env)
    else {
        return Ok(None);
    };
    let warning = interp.call_function_value(
        function,
        Some("byte-run--unescaped-character-literals-warning"),
        &[],
        env,
    )?;
    if warning.is_nil() {
        Ok(None)
    } else {
        primitives::string_text(&warning).map(Some)
    }
}

fn format_loading_warning(
    interp: &mut eval::Interpreter,
    env: &mut types::Env,
    path: &Path,
    warning: String,
) -> Result<String, types::LispError> {
    let function = interp.lookup_function("format-message", env)?;
    let message = interp.call_function_value(
        function,
        Some("format-message"),
        &[
            types::Value::String("Loading `%s': %s".into()),
            types::Value::String(path.display().to_string().into()),
            types::Value::String(warning.into()),
        ],
        env,
    )?;
    primitives::string_text(&message)
}

/// What `load' reads a file's bytes as.
enum LoadText {
    /// A compiled file, read as Fload reads one directly (readevalloop on
    /// Qget_file_char): the bytes themselves.
    File(Vec<u8>),
    /// Source text, decoded as load-with-code-conversion decodes it.
    Source(String),
    /// `utf-8-emacs' source that is not UTF-8: its bytes, read as the
    /// buffer decoding them holds them.
    Internal(Vec<u8>),
}

/// Fload's choice: a file named `.elc' or starting with the `;ELC' header
/// is compiled and read directly, its bytes those of Emacs's internal
/// representation (`byte-write-target-file' writes it with
/// `no-conversion'); any other is source to decode first.
fn load_text(path: &Path, bytes: Vec<u8>) -> Result<LoadText, types::LispError> {
    if bytes.starts_with(b";ELC") || path.extension().is_some_and(|extension| extension == "elc") {
        return Ok(LoadText::File(bytes));
    }
    match String::from_utf8(bytes) {
        Ok(source) => Ok(LoadText::Source(source)),
        Err(error) => {
            let bytes = error.into_bytes();
            match primitives::coding_tag_from_bytes(&bytes).as_deref() {
                Some("utf-8-emacs") => Ok(LoadText::Internal(bytes)),
                coding => Err(types::LispError::Signal(format!(
                    "Cannot read {} as {} source",
                    path.display(),
                    coding.unwrap_or("UTF-8")
                ))),
            }
        }
    }
}

/// The input of one readevalloop read.
#[derive(Clone, Copy)]
pub(crate) enum LoadInput<'a> {
    Text(&'a str),
    File(&'a [u8]),
    Internal(&'a [u8]),
}

fn read_source_bytes(path: &Path) -> Result<Vec<u8>, types::LispError> {
    crate::file_system::read(path).map_err(|error| {
        types::LispError::Signal(format!("Cannot read {}: {}", path.display(), error))
    })
}

fn source_settings(source: &str) -> Result<SourceFileSettings, types::LispError> {
    let lexical_binding =
        extract_mode_line_variable(source, "lexical-binding").as_deref() == Some("t");
    let read_symbol_shorthands = match extract_file_local_variable(source, "read-symbol-shorthands")
    {
        Some(raw_value) => parse_symbol_shorthands(&raw_value)?,
        None => Vec::new(),
    };
    Ok(SourceFileSettings {
        lexical_binding,
        read_symbol_shorthands,
    })
}

/// The first two lines of a compiled file, where its mode line may be.
fn file_head(bytes: &[u8]) -> std::borrow::Cow<'_, str> {
    let mut lines = bytes.split_inclusive(|byte| *byte == b'\n');
    let length = lines.by_ref().take(2).map(<[u8]>::len).sum();
    String::from_utf8_lossy(&bytes[..length])
}

pub(crate) fn extract_mode_line_variable(source: &str, variable: &str) -> Option<String> {
    for line in source.lines().take(2) {
        let Some(start) = line.find("-*-") else {
            continue;
        };
        let contents = &line[start + 3..];
        let Some(end) = contents.find("-*-") else {
            continue;
        };
        for field in contents[..end].split(';') {
            let Some((name, value)) = field.split_once(':') else {
                continue;
            };
            if name.trim() == variable {
                return Some(value.trim().to_string());
            }
        }
    }
    None
}

fn extract_file_local_variable(source: &str, variable: &str) -> Option<String> {
    let mut current_block = None;
    let mut last_block = None;
    for line in source.lines() {
        let trimmed = line.trim_start();
        let comment_text = trimmed.trim_start_matches(';');
        let comment_text = comment_text.strip_prefix(' ').unwrap_or(comment_text);
        if comment_text.trim() == "Local Variables:" {
            current_block = Some(Vec::new());
            continue;
        }
        if comment_text.trim() == "End:" {
            if let Some(block) = current_block.take() {
                last_block = Some(block);
            }
            continue;
        }
        if let Some(block) = current_block.as_mut() {
            block.push(comment_text.to_string());
        }
    }

    let declaration = format!("{variable}:");
    let mut value = None;
    for line in last_block? {
        if value.is_none() {
            let Some(first_line) = line.strip_prefix(&declaration) else {
                continue;
            };
            value = Some(first_line.trim().to_string());
        } else if let Some(value) = value.as_mut() {
            value.push('\n');
            value.push_str(&line);
        }

        let candidate = value.as_ref().expect("file-local value was initialized");
        let read = reader::Reader::new(candidate)
            .read()
            .map_err(LispError::into_kind);
        match read {
            Ok(Some(_)) => return value,
            Err(types::LispErrorKind::EndOfInput) | Ok(None) => {}
            Err(_) => return value,
        }
    }
    value
}

fn parse_shorthand_string(value: &types::Value) -> Result<String, types::LispError> {
    match value.kind() {
        types::Kind::StringObject(state) => Ok(state.borrow().text()),
        other => Err(types::LispError::WrongTypeArgument(
            "stringp".into(),
            other.value(),
        )),
    }
}

fn parse_symbol_shorthands(raw_value: &str) -> Result<Vec<(String, String)>, types::LispError> {
    let mut reader = reader::Reader::new(raw_value);
    let Some(value) = reader.read()? else {
        return Ok(Vec::new());
    };
    let entries = value.to_vec()?;
    let mut shorthands = Vec::with_capacity(entries.len());
    for entry in entries {
        let Some((from, to)) = entry.cons_values() else {
            return Err(types::LispError::WrongTypeArgument("consp".into(), entry));
        };
        shorthands.push((parse_shorthand_string(&from)?, parse_shorthand_string(&to)?));
    }
    Ok(shorthands)
}

fn read_symbol_shorthands_value(shorthands: &[(String, String)]) -> types::Value {
    types::Value::list(shorthands.iter().map(|(from, to)| {
        types::Value::cons(
            types::Value::String(from.clone().into()),
            types::Value::String(to.clone().into()),
        )
    }))
}

#[cfg(test)]
pub(crate) fn read_source_forms(
    source: &str,
) -> Result<alloc::RootedVec<types::Value>, types::LispError> {
    read_source_forms_with_unescaped_literals(source).map(|(forms, _)| forms)
}

#[cfg(test)]
pub(crate) fn read_source_forms_with_unescaped_literals(
    source: &str,
) -> Result<(alloc::RootedVec<types::Value>, types::Value), types::LispError> {
    let settings = source_settings(source)?;
    let mut reader =
        reader::Reader::with_symbol_shorthands(source, settings.read_symbol_shorthands);
    let forms = reader.read_all()?;
    let literals = types::Value::list(
        reader
            .unescaped_character_literals()
            .map(types::Value::Integer),
    );
    Ok((forms, literals))
}

pub(crate) fn load_file_strict(
    interp: &mut eval::Interpreter,
    path: &Path,
) -> Result<(), types::LispError> {
    load_file_strict_until(interp, path, |_| false).map(|_| ())
}

/// Load PATH through the ordinary GNU `readevalloop' boundary, but stop
/// before the first top-level form accepted by STOP_BEFORE.  The image
/// builder uses this for loadup.el: GNU's dumped state includes loadup's
/// initialization forms but not its final transfer into `top-level'.
pub(crate) fn load_file_strict_until(
    interp: &mut eval::Interpreter,
    path: &Path,
    stop_before: impl FnMut(&types::Value) -> bool,
) -> Result<bool, types::LispError> {
    load_file_strict_until_or_error(interp, path, stop_before, |_| false)
}

/// The loadup image builder needs to stop at GNU's C portable-dumper call,
/// after the surrounding unchanged Lisp has performed its pre-dump state
/// transitions.  STOP_AFTER_ERROR recognizes that unavailable C boundary;
/// all ordinary loads pass a predicate that is always false.
pub(crate) fn load_file_strict_until_or_error(
    interp: &mut eval::Interpreter,
    path: &Path,
    stop_before: impl FnMut(&types::Value) -> bool,
    stop_after_error: impl FnMut(&types::LispError) -> bool,
) -> Result<bool, types::LispError> {
    let requested_source = read_source_bytes(path)?;
    read_lisp_file_bytes(
        interp,
        path,
        requested_source,
        None,
        stop_before,
        stop_after_error,
    )
}

/// Fload's direct-reader branch consumes the descriptor openp selected. It
/// remains alive until reading/evaluation and dynamic unwinding are complete.
pub(crate) fn load_file_strict_opened(
    interp: &mut eval::Interpreter,
    path: &Path,
    mut file: crate::file_system::File,
    history: &str,
) -> Result<(), types::LispError> {
    let bytes = crate::file_system::read_open_file(&mut file).map_err(|error| {
        primitives::file_operation_error("Reading file", &error, &path.display().to_string())
    })?;
    let result = read_lisp_file_bytes(interp, path, bytes, Some(history), |_| false, |_| false);
    drop(file);
    result.map(|_| ())
}

/// One readevalloop read: the form of SOURCE at byte OFFSET, read in the
/// current obarray with the current `read-symbol-shorthands', its
/// identity-bearing literals built, OFFSET moved past it.  None at the end.
#[cfg(test)]
pub(crate) fn read_next_load_form(
    interp: &mut eval::Interpreter,
    source: &str,
    offset: &mut usize,
    env: &mut types::Env,
) -> Result<Option<types::Value>, types::LispError> {
    read_next_load_input_form(interp, LoadInput::Text(source), offset, env)
}

/// `read_next_load_form' over any of `load''s inputs.
pub(crate) fn read_next_load_input_form(
    interp: &mut eval::Interpreter,
    input: LoadInput<'_>,
    offset: &mut usize,
    env: &mut types::Env,
) -> Result<Option<types::Value>, types::LispError> {
    let shorthands = primitives::read_symbol_shorthands_in_env(interp, env)?;
    let (value, needs_materialization, unescaped) = {
        let mut context = eval::LispReadContext::current(interp, env);
        let (reader, base) = match input {
            LoadInput::Text(source) => (
                reader::Reader::with_symbol_shorthands(&source[*offset..], shorthands),
                *offset,
            ),
            LoadInput::File(bytes) => (reader::Reader::from_file(bytes, *offset, shorthands), 0),
            LoadInput::Internal(bytes) => (
                reader::Reader::from_text_bytes(&bytes[*offset..], shorthands),
                *offset,
            ),
        };
        let mut reader = reader.with_read_context(&mut context);
        let value = reader.read();
        *offset = base + reader.position();
        let value = value?;
        (
            value,
            reader.emitted_reader_forms(),
            reader.unescaped_character_literals().collect::<Vec<_>>(),
        )
    };
    if !unescaped.is_empty() {
        primitives::record_unescaped_character_literals(interp, unescaped, env);
    }
    match value {
        Some(value) if needs_materialization => interp
            .materialize_read_object_literals(value, env)
            .map(Some),
        value => Ok(value),
    }
}

fn read_lisp_file_bytes(
    interp: &mut eval::Interpreter,
    path: &Path,
    requested_source: Vec<u8>,
    outer_load_history: Option<&str>,
    mut stop_before: impl FnMut(&types::Value) -> bool,
    mut stop_after_error: impl FnMut(&types::LispError) -> bool,
) -> Result<bool, types::LispError> {
    // Resolution has already selected the file.  GNU executes that exact
    // path: an explicit or resolver-selected `.elc' must never silently run a
    // sibling `.el'.  Versioned `.elc' files can contain ordinary readable
    // Lisp as well as `#[...]' bytecode objects; the same reader handles both.
    let text = load_text(path, requested_source)?;
    let compiled = matches!(text, LoadText::File(_));
    // GNU readevalloop decides this once, before reading the first form: it
    // eagerly expands source only when macroexp.el's owner function is
    // already installed, and never for byte-compiled input.  A nested
    // `require' that defines the function must not retroactively change the
    // policy for the outer file.
    let macroexpander_ready = interp
        .lookup_function("internal-macroexpand-for-load", &types::Env::new())
        .is_ok();
    let eager_macroexpand = macroexpander_ready && !compiled;
    // Fload sets `lexical-binding' from a compiled file's first line
    // (lisp_file_lexical_cookie) and leaves `read-symbol-shorthands' alone;
    // source gets both from its text (load-with-code-conversion).
    let settings = match &text {
        LoadText::File(bytes) => SourceFileSettings {
            lexical_binding: extract_mode_line_variable(&file_head(bytes), "lexical-binding")
                .as_deref()
                == Some("t"),
            read_symbol_shorthands: Vec::new(),
        },
        LoadText::Source(source) => source_settings(source)?,
        LoadText::Internal(bytes) => source_settings(&String::from_utf8_lossy(bytes))?,
    };
    let input = match &text {
        LoadText::File(bytes) => LoadInput::File(bytes),
        LoadText::Source(source) => LoadInput::Text(source),
        LoadText::Internal(bytes) => LoadInput::Internal(bytes),
    };
    let load_file = outer_load_history.map(str::to_owned).unwrap_or_else(|| {
        interp
            .load_source_provenance_path(path)
            .display()
            .to_string()
    });
    interp.with_lambda_eval_context(settings.lexical_binding, |interp| {
        let previous = interp.set_current_load_file(Some(load_file.clone()));
        let mut env = if settings.lexical_binding {
            crate::lisp::types::Env::from_vec(vec![types::EnvFrame::lexical()])
        } else {
            types::Env::new()
        };
        // GNU `load' establishes these as real specbind layers.  A Rust-only
        // current-file side channel is insufficient: an outer Lisp binding such
        // as `(let ((load-file-name nil)) (load ...))' must be shadowed by the
        // file being loaded, then restored on every exit path.
        let macroexp_dynvars = interp
            .lookup_var("macroexp--dynvars", &env)
            .unwrap_or(types::Value::Nil);
        let mut dynamic_restores = Vec::with_capacity(9);
        for (name, value) in [
            (
                "load-file-name",
                types::Value::String(load_file.clone().into()),
            ),
            (
                "load-true-file-name",
                types::Value::string(&path.display().to_string()),
            ),
            ("inhibit-file-name-operation", types::Value::Nil),
            ("load-in-progress", types::Value::T),
            (
                "lexical-binding",
                if settings.lexical_binding {
                    types::Value::T
                } else {
                    types::Value::Nil
                },
            ),
            ("macroexp--dynvars", macroexp_dynvars),
            (
                "read-symbol-shorthands",
                read_symbol_shorthands_value(&settings.read_symbol_shorthands),
            ),
            (
                "current-load-list",
                types::Value::list([types::Value::String(load_file.clone().into())]),
            ),
            ("lread--unescaped-character-literals", types::Value::Nil),
        ] {
            // Fload of a compiled file binds no `read-symbol-shorthands'.
            if compiled && name == "read-symbol-shorthands" {
                continue;
            }
            if outer_load_history.is_some()
                && matches!(
                    name,
                    "lexical-binding" | "lread--unescaped-character-literals"
                )
            {
                if name == "lexical-binding" && settings.lexical_binding {
                    interp.set_variable(name, types::Value::T, &mut env);
                }
                continue;
            }
            match interp.bind_special_dynamic(name, value, &mut env) {
                Ok(restore) => dynamic_restores.push(restore),
                Err(error) => {
                    let _ =
                        restore_special_dynamic_bindings(interp, &mut dynamic_restores, &mut env);
                    interp.set_current_load_file(previous);
                    return Err(error);
                }
            }
        }
        // lread.c:readevalloop reads one form, interning each symbol in the
        // obarray current then and building its objects, and evaluates it
        // before reading the next: an earlier form can change the obarray,
        // `read-symbol-shorthands' or the definitions a later literal needs,
        // and a read error comes only after the forms before it ran.
        let mut stopped = false;
        let mut offset = 0;
        let mut form_index = 0;
        loop {
            let form = match read_next_load_input_form(interp, input, &mut offset, &mut env) {
                Ok(Some(form)) => form,
                Ok(None) => break,
                Err(error) => {
                    if trace_load_errors_enabled() {
                        eprintln!(
                            "read error in {} at byte offset {offset} (form {}): {error:?}",
                            path.display(),
                            form_index + 1
                        );
                    }
                    let _ =
                        restore_special_dynamic_bindings(interp, &mut dynamic_restores, &mut env);
                    interp.set_current_load_file(previous);
                    return Err(error);
                }
            };
            form_index += 1;
            if stop_before(&form) {
                stopped = true;
                break;
            }
            let result = if eager_macroexpand {
                primitives::eager_expand_eval(interp, &form, &mut env)
            } else {
                interp.eval(&form, &mut env)
            };
            if let Err(error) = result {
                if stop_after_error(&error) {
                    // The recognized C handoff is successful control flow
                    // for image reconstruction, not a batch error retained
                    // in the restored process.
                    interp.clear_batch_error_backtrace();
                    stopped = true;
                    break;
                }
                if trace_load_errors_enabled() {
                    let head = form
                        .car()
                        .ok()
                        .and_then(|value| value.symbol_str().map(str::to_owned))
                        .unwrap_or_else(|| form.type_name());
                    eprintln!(
                        "load error in {} at form {} ({head}): {error:?}",
                        path.display(),
                        form_index + 1
                    );
                    // Peek, never take: the toplevel batch reporter prints
                    // the GNU-format backtrace from this same snapshot.
                    if let Some(snapshot) = interp.peek_batch_error_backtrace() {
                        for (_, function, _, _) in snapshot.frames.iter().take(20) {
                            eprintln!("  load frame: {function}");
                        }
                    }
                }
                let _ = restore_special_dynamic_bindings(interp, &mut dynamic_restores, &mut env);
                interp.set_current_load_file(previous);
                return Err(error);
            }
        }
        if !stopped {
            // lread.c:readevalloop commits history only after reaching EOF.
            // The image handoff stops inside loadup, before its final form
            // restores the filename in current-load-list. That unfinished
            // load must not be published as a completed history entry.
            let current_load_list = interp
                .lookup_var("current-load-list", &types::Env::new())
                .unwrap_or_else(|| {
                    types::Value::list([types::Value::String(load_file.clone().into())])
                });
            interp.commit_entire_load_history(&types::Value::string(&load_file), current_load_list);
        }
        // Fload's load_warn_unescaped_character_literals, once the forms
        // have run and before the load's bindings unwind.
        let warning_message = match (if outer_load_history.is_some() {
            Ok(None)
        } else {
            unescaped_character_literal_warning(interp, &mut env)
        })
        .and_then(|warning| {
            warning
                .map(|warning| format_loading_warning(interp, &mut env, path, warning))
                .transpose()
        }) {
            Ok(message) => message,
            Err(error) => {
                let _ = restore_special_dynamic_bindings(interp, &mut dynamic_restores, &mut env);
                interp.set_current_load_file(previous);
                return Err(error);
            }
        };
        if let Some(message) = warning_message {
            append_message(interp, &message);
        }
        restore_special_dynamic_bindings(interp, &mut dynamic_restores, &mut env)?;
        interp.set_current_load_file(previous);

        // GNU 30.2 lread.c:Fload calls the Lisp-owned
        // `do-after-load-evaluation' only after all load bindings unwind.
        // Invoke that actual GNU Elisp owner when it is present; early
        // bootstrap loads deliberately have no such function yet.
        if !stopped
            && outer_load_history.is_none()
            && let Ok(after_load) = interp.lookup_function("do-after-load-evaluation", &env)
        {
            interp.call_function_value(
                after_load,
                Some("do-after-load-evaluation"),
                &[types::Value::String(load_file.into())],
                &mut env,
            )?;
        }
        reader::forget_saved_lazy_strings();
        Ok(stopped)
    })
}

fn restore_special_dynamic_bindings(
    interp: &mut eval::Interpreter,
    restores: &mut Vec<eval::SpecialBindingRestore>,
    env: &mut types::Env,
) -> Result<(), types::LispError> {
    while let Some(restore) = restores.pop() {
        interp.restore_special_dynamic(restore, env)?;
    }
    Ok(())
}

/// Load and run a complete ERT file through initialized GNU Lisp and the
/// same reporter used by the compatibility CLI. Only owned host data leaves
/// the runtime; load failures are errors, and individual outcomes remain in
/// the returned report (including skips and unexpected failures).
/// `executable` is the editor binary GNU comp.el can launch for compiler
/// children. An embedding application's executable need not implement the
/// editor CLI, so its path must be supplied explicitly.
pub fn run_ert_file(path: &Path, executable: &Path) -> Result<crate::compat::BatchReport, String> {
    runtime::with_runtime(|| {
        crate::batch::with_batch_stack(|| run_ert_file_owned(path, executable))
    })?
}

fn run_ert_file_owned(
    path: &Path,
    executable: &Path,
) -> Result<crate::compat::BatchReport, String> {
    let _symbols = types::symbols::EditorSymbolScope::enter();
    let executable = executable
        .canonicalize()
        .map_err(|error| format!("resolve editor executable: {error}"))?;
    let directory = executable
        .parent()
        .ok_or_else(|| "editor executable has no parent directory".to_string())?;
    let name = executable
        .file_name()
        .ok_or_else(|| "editor executable has no file name".to_string())?;
    let options = crate::batch::BatchRunOptions {
        no_site_lisp: true,
        ..Default::default()
    };
    let mut interp = crate::batch::initialize_batch_interpreter(&options)?;
    // comp.el's comp--final uses these ordinary GNU variables. Keep the
    // compiler enabled and let its normal child process report any failure.
    interp.set_global_binding(
        "invocation-name",
        types::Value::string(&name.to_string_lossy()),
    );
    interp.set_global_binding(
        "invocation-directory",
        types::Value::string(&format!("{}/", directory.display())),
    );
    let helper = crate::compat::compat_path("compat/emacs_compat_runner.el");
    load_file_strict(&mut interp, &helper).map_err(|error| error.to_string())?;
    load_file_strict(&mut interp, path).map_err(|error| error.to_string())?;
    let forms = reader::Reader::new(
        "(json-encode
           (emaxx-compat--call-with-batch-environment
            (lambda () (emaxx-compat--report t))))",
    )
    .read_all()
    .map_err(|error| error.to_string())?;
    let mut env = types::Env::new();
    let value = interp
        .eval(&forms[0], &mut env)
        .map_err(|error| error.to_string())?;
    let json = primitives::string_like(&value)
        .ok_or_else(|| "ERT reporter did not return JSON text".to_string())?;
    let mut report: crate::compat::BatchReport =
        serde_json::from_str(&json.text).map_err(|error| format!("decode ERT report: {error}"))?;
    // Host provenance for this library call, which has no harness-supplied
    // runner/file environment variables. Preserve every Lisp outcome.
    report.runner = "emaxx".into();
    report.file = path.display().to_string();
    let issues = crate::compat::report_integrity_issues(&report);
    if !issues.is_empty() {
        return Err(format!("inconsistent ERT report: {issues:?}"));
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::{
        LoadText, extract_file_local_variable, load_text, parse_symbol_shorthands,
        read_source_forms, source_settings,
    };
    use std::path::Path;

    /// The forms `load' reads from BYTES, a file named PATH, read without
    /// an interpreter (so `#$' reads as nil).
    fn read_load_text_forms(
        path: &Path,
        bytes: Vec<u8>,
    ) -> super::alloc::RootedVec<super::types::Value> {
        match load_text(path, bytes).expect("load text") {
            LoadText::File(bytes) => super::reader::Reader::from_file(&bytes, 0, Vec::new())
                .read_all()
                .expect("read compiled forms"),
            LoadText::Source(source) => read_source_forms(&source).expect("read source forms"),
            LoadText::Internal(bytes) => super::reader::Reader::from_text_bytes(&bytes, Vec::new())
                .read_all()
                .expect("read utf-8-emacs forms"),
        }
    }

    #[test]
    fn primitive_dispatch_preserves_platform_cfg() {
        use crate::lisp::{
            eval::Interpreter,
            types::{LispError, Value},
        };

        define_dispatch! {
            fn call(interp: &mut Interpreter, name: &str, args: &[Value]) -> Result<Value, LispError> {
                match name {
                    #[cfg(any())]
                    "unavailable-a" | "unavailable-b" => {
                        unavailable_platform_function(interp, args)
                    }
                    #[cfg_attr(all(), cfg(any()))]
                    "unavailable-expression" => unavailable_platform_function(interp, args),
                    #[dispatch(builtin_override)]
                    #[cfg(any())]
                    "unavailable-override" => {
                        unavailable_platform_function(interp, args)
                    }
                    #[cfg(all())]
                    "first" | "second" => {
                        Ok(Value::Integer(if name == "first" { 1 } else { 2 }))
                    }
                    #[dispatch(builtin_override)]
                    #[cfg(all())]
                    "third" => Ok(Value::Integer(3)),
                }
            }
        }

        assert_eq!(
            LIFTED_PRIMITIVES
                .iter()
                .map(|(name, _)| *name)
                .collect::<Vec<_>>(),
            ["first", "second", "third"]
        );
        for name in [
            "unavailable-a",
            "unavailable-b",
            "unavailable-expression",
            "unavailable-override",
        ] {
            assert!(!handles(name));
            assert!(!prefer_builtin(name));
        }
        assert!(prefer_builtin("third"));
        let mut patterns = Vec::new();
        visit_handled_patterns(&mut |pattern| patterns.push(pattern));
        assert_eq!(patterns, ["\"first\" | \"second\"", "\"third\""]);

        let mut interp = Interpreter::new();
        for (name, expected) in [("first", 1), ("second", 2), ("third", 3)] {
            assert!(handles(name));
            assert_eq!(
                call(&mut interp, name, &[]).expect("enabled primitive dispatches"),
                Value::Integer(expected)
            );
        }
    }

    #[test]
    fn parses_compact_lexical_binding_modelines() {
        for source in [
            ";;; -*- lexical-binding:t -*-\n",
            "#!/bin/sh\n;;; -*- mode: emacs-lisp; lexical-binding: t; -*-\n",
        ] {
            assert!(
                source_settings(source)
                    .expect("source settings should parse a valid modeline")
                    .lexical_binding
            );
        }

        for source in [
            ";;; -*- lexical-binding:nil -*-\n",
            ";;; -*- not-lexical-binding: t -*-\n",
        ] {
            assert!(
                !source_settings(source)
                    .expect("source settings should reject unrelated or nil fields")
                    .lexical_binding
            );
        }
    }

    #[test]
    fn parses_read_symbol_shorthands_from_local_variables_block() {
        let source = r#"
(ert-deftest ft-sample ())

;; Local Variables:
;; read-symbol-shorthands: (("ft-" . "fns-tests-"))
;; This comment is not a variable assignment.
;; End:
"#;

        assert_eq!(
            extract_file_local_variable(source, "read-symbol-shorthands"),
            Some(r#"(("ft-" . "fns-tests-"))"#.into())
        );
        assert_eq!(
            parse_symbol_shorthands(r#"(("ft-" . "fns-tests-"))"#)
                .expect("symbol shorthand alist should parse"),
            vec![("ft-".into(), "fns-tests-".into())]
        );
        assert_eq!(
            source_settings(source)
                .expect("source settings should parse file-local symbol shorthands")
                .read_symbol_shorthands,
            vec![("ft-".into(), "fns-tests-".into())]
        );
    }

    #[test]
    fn parses_multiline_read_symbol_shorthands_from_magit_style_blocks() {
        let source = r#"
(provide 'sample)

;; Local Variables:
;; indent-tabs-mode: nil
;; read-symbol-shorthands: (
;;   ("and$"     . "cond-let--and$")
;;   ("when-let" . "cond-let--when-let"))
;; End:
"#;

        assert_eq!(
            source_settings(source)
                .expect("multiline file-local symbol shorthands should parse")
                .read_symbol_shorthands,
            vec![
                ("and$".into(), "cond-let--and$".into()),
                ("when-let".into(), "cond-let--when-let".into()),
            ]
        );
    }

    #[test]
    fn compiled_file_markers_inside_strings_comments_and_symbols_are_text() {
        let source = ";ELC\x1e\n(#[0 \"raw #@12 bytes #$ (#$ . 9) \\\"tail\" [nil] 1])\n\
                      ; #@7 and #$ are comment text\n\
                      (list byte-compile--\\#$ byte-compile--\\#@7)\n";
        let forms = read_load_text_forms(Path::new("/tmp/sample.elc"), source.into());
        assert_eq!(forms.len(), 2);
        let symbols = forms[1].to_vec().expect("list form");
        assert_eq!(
            symbols
                .iter()
                .map(|symbol| symbol.symbol_str().map(str::to_owned))
                .collect::<Vec<_>>(),
            [
                Some("list"),
                Some("byte-compile--#$"),
                Some("byte-compile--#@7")
            ]
            .map(|name| name.map(str::to_owned))
        );
    }

    #[test]
    fn compiled_file_skips_lazy_strings_by_byte_count() {
        // skip_lazy_string from a file counts the bytes after the digits,
        // a multibyte character and an embedded ^_ included; #@00 ends
        // the input.
        let source = ";ELC\x1e\n#@8 h\u{e9}\x1f\x1fx\x1f(a)#@00 (b";
        let forms = read_load_text_forms(Path::new("/tmp/sample.elc"), source.into());
        assert_eq!(forms.len(), 2);
        assert_eq!(format!("{}", forms[0]), "(a)");
        assert!(forms[1].is_nil());
    }

    #[test]
    fn utf8_emacs_source_preserves_extended_character_literals_as_integers() {
        let mut bytes = b";;; -*- coding: utf-8-emacs; -*-\n(setq sample ?".to_vec();
        bytes.extend([0xF6, 0xA0, 0x87, 0x8A]);
        bytes.extend(b")\n");
        assert_eq!(
            read_load_text_forms(Path::new("ethiopic-sample.el"), bytes)[0],
            super::types::Value::list([
                super::types::Value::symbol("setq"),
                super::types::Value::symbol("sample"),
                super::types::Value::Integer(0x1A_01CA),
            ])
        );
    }

    #[test]
    fn utf8_emacs_source_preserves_extended_string_characters() {
        let mut bytes = b";;; -*- coding: utf-8-emacs; -*-\n(setq sample \"".to_vec();
        bytes.extend([0xF6, 0xA0, 0x87, 0x8A]);
        bytes.extend(b"\")\n");
        let form = read_load_text_forms(Path::new("extended-string.el"), bytes)
            .remove(0)
            .to_vec()
            .expect("setq form");
        let string = super::primitives::string_like(&form[2]).expect("string value");
        assert_eq!(string.character_codes(), vec![0x1A_01CA]);
        assert_eq!(string.extended_chars, vec![(0, 0x1A_01CA)]);
        assert_eq!(string.byte_len().expect("utf-8-emacs byte length"), 4);
        assert_eq!(
            super::primitives::internal_string_bytes(&string)
                .expect("encode utf-8-emacs string bytes"),
            [0xF6, 0xA0, 0x87, 0x8A]
        );
    }

    #[test]
    fn utf8_emacs_source_decodes_five_byte_and_raw_byte8_characters() {
        let mut bytes = b";;; -*- coding: utf-8-emacs; -*-\n(setq sample \"".to_vec();
        bytes.extend([0xF8, 0x88, 0x80, 0x80, 0x80]);
        bytes.extend([0xC0, 0x80]);
        bytes.extend(b"\")\n");
        let form = read_load_text_forms(Path::new("extended-ranges.el"), bytes)
            .remove(0)
            .to_vec()
            .expect("setq form");
        let string = super::primitives::string_like(&form[2]).expect("string value");
        assert_eq!(string.character_codes(), vec![0x20_0000, 0x3F_FF80]);
        assert_eq!(string.byte_len().expect("utf-8-emacs byte length"), 7);
        assert_eq!(
            super::primitives::internal_string_bytes(&string)
                .expect("encode complete utf-8-emacs range"),
            [0xF8, 0x88, 0x80, 0x80, 0x80, 0xC0, 0x80]
        );
    }

    #[test]
    fn compiled_lisp_decodes_internal_utf8_emacs_without_a_cookie() {
        let mut bytes = b";ELC\x1e\0\0\0\n(setq sample \"".to_vec();
        bytes.extend([0xF6, 0xA0, 0x87, 0x8A]);
        bytes.extend(b"\")\n");
        let form = read_load_text_forms(Path::new("extended-string.elc"), bytes)
            .remove(0)
            .to_vec()
            .expect("setq form");
        let string = super::primitives::string_like(&form[2]).expect("string value");
        assert_eq!(string.character_codes(), vec![0x1A_01CA]);
        assert_eq!(
            super::primitives::internal_string_bytes(&string)
                .expect("encode compiled utf-8-emacs string bytes"),
            [0xF6, 0xA0, 0x87, 0x8A]
        );
    }

    #[test]
    fn compiled_lisp_preserves_raw_bytecode_octets_inside_strings() {
        let mut bytes = b";ELC\x1e\0\0\0\n(setq sample \"".to_vec();
        bytes.extend([0xC0, 0x01, 0xC2, 0xFF]);
        bytes.extend(b"\")\n");
        let form = read_load_text_forms(Path::new("raw-bytecode.elc"), bytes)
            .remove(0)
            .to_vec()
            .expect("setq form");
        let string = super::primitives::string_like(&form[2]).expect("string value");
        assert!(!string.multibyte);
        assert_eq!(
            super::primitives::internal_string_bytes(&string)
                .expect("recover compiled raw byte string"),
            [0xC0, 0x01, 0xC2, 0xFF]
        );
    }

    #[test]
    fn file_loader_materializes_reader_objects_before_macroexpansion_and_eval() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "emaxx-reader-boundary-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).expect("create reader-boundary fixture directory");

        let roots = std::iter::repeat_n("nil", 64).collect::<Vec<_>>().join(" ");
        let source_path = directory.join("reader-object.el");
        std::fs::write(
            &source_path,
            format!("(setq loaded-source-table #^[nil nil test nil {roots}])\n"),
        )
        .expect("write source reader-object fixture");
        let compiled_path = directory.join("reader-object.elc");
        std::fs::write(
            &compiled_path,
            format!(";ELC\x1e\n(setq loaded-compiled-table #^[nil nil test nil {roots}])\n"),
        )
        .expect("write compiled reader-object fixture");

        let mut interp = super::eval::Interpreter::new();
        let mut env = super::types::Env::new();
        for form in super::reader::Reader::new(
            "(setq eager-owner-saw-char-table nil)\n\
             (defalias 'internal-macroexpand-for-load\n\
               (function\n\
                 (lambda (form full)\n\
                   (if (and full\n\
                            (eq (car-safe form) 'setq)\n\
                            (char-table-p\n\
                              (car-safe (cdr-safe (cdr-safe form)))))\n\
                       (setq eager-owner-saw-char-table t))\n\
                   form)))",
        )
        .read_all()
        .expect("read eager-owner fixture")
        {
            interp
                .eval(&form, &mut env)
                .expect("install eager-owner fixture");
        }

        super::load_file_strict(&mut interp, &source_path)
            .expect("load source reader-object fixture");
        assert_eq!(
            interp.lookup_var("eager-owner-saw-char-table", &env),
            Some(super::types::Value::T),
            "GNU's Lisp macroexpander must receive the C-reader object, not a private placeholder"
        );
        assert!(matches!(
            interp
                .lookup_var("loaded-source-table", &env)
                .map(|v| v.kind()),
            Some(super::types::Kind::CharTable(_))
        ));

        super::load_file_strict(&mut interp, &compiled_path)
            .expect("load compiled reader-object fixture");
        assert!(matches!(
            interp
                .lookup_var("loaded-compiled-table", &env)
                .map(|v| v.kind()),
            Some(super::types::Kind::CharTable(_))
        ));

        std::fs::remove_dir_all(directory).expect("remove reader-boundary fixture directory");
    }

    #[test]
    fn closure_reader_materialization_never_evaluates_slot_data() {
        let callable_looking_data = super::types::Value::list([
            super::types::Value::symbol("reader-data-must-not-be-called"),
            super::types::Value::symbol("payload"),
        ]);
        let literal = super::types::Value::ReaderForm(crate::lisp::alloc::VectorlikeRef::allocate(
            super::types::ReaderForm::Closure {
                kind: super::types::ReaderClosureKind::ByteCode,
                slots: vec![
                    super::types::Value::Integer(0),
                    super::types::Value::String(String::new().into()),
                    super::types::Value::vector([]),
                    super::types::Value::Integer(0),
                    callable_looking_data,
                ],
            },
        ));
        let mut interp = super::eval::Interpreter::new();
        let mut env = super::types::Env::new();
        let materialized = interp
            .materialize_read_object_literals(literal, &mut env)
            .expect("reader construction must treat every closure slot as data");
        let super::types::Kind::Closure(closure) = materialized.kind() else {
            panic!("byte-code reader form must become a closure pseudovector");
        };
        // lread.c:bytecode_from_rev_list constructs PVEC_CLOSURE and keeps
        // this slot as data. Ordinary GNU confirms the five-slot object
        // without invoking the callable-looking documentation form.
        assert_eq!(closure.public_len(), 5);
        assert_eq!(closure.get(4), Some(callable_looking_data));
    }
}
