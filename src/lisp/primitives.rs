use super::eval::{Interpreter, RunningProcess, SpawnedChild};
use super::json::{self, JsonParseOptions};
use super::sqlite;
use super::types::{EmacsTermination, Env, LispError, StringObjectRef, Value};
use crate::file_system as fs;
use flate2::read::GzDecoder;
use lcms2::{CIECAM02, CIELab, CIELabExt, CIEXYZ, JCh, ViewingConditions};
use lcms2_sys::Surround;
use libxml::tree::{Node as LibxmlNode, NodeType as LibxmlNodeType};
use num_bigint::{BigInt, Sign};
use num_traits::{Signed, ToPrimitive, Zero};
use regex::Regex;
use sha1::{Digest, Sha1};
use sha2::{Sha224, Sha256, Sha384, Sha512};
use sha3::{Sha3_224, Sha3_256, Sha3_384, Sha3_512};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
#[cfg(unix)]
use std::ffi::CString;
use std::io::ErrorKind;
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

pub(crate) const PORTABLE_DUMPER_UNAVAILABLE: &str = "Portable dumper backend is unavailable";
#[cfg(unix)]
use std::os::unix::fs::symlink;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use unicode_general_category::get_general_category;
use unicode_width::UnicodeWidthChar;

mod accessors_random;
pub(crate) mod batch_stdout;
mod buffers;
mod case;
pub(crate) mod casetab;
mod ccl;
pub(crate) mod coding;
mod color_lcms;
mod completion;
mod dispatch;
pub(crate) use dispatch::restore_unicode_menu_case_table;
mod file_io;
mod filelock;
mod generated_builtin_arities;
pub(crate) mod generated_gnu_c_bool_variables;
mod generated_gnu_c_defsyms;
pub(crate) mod generated_gnu_c_int_variables;
pub(crate) use generated_gnu_c_defsyms::GNU_C_DEFSYMS;
#[allow(dead_code)]
mod generated_gnu_c_primitives;
#[cfg(not(target_os = "linux"))]
pub(crate) use generated_gnu_c_primitives::{
    GNU_C_PRIMITIVES, generated_gnu_c_primitive_available, generated_gnu_c_primitive_special_form,
};
// The generated inventories come from contracted oracle builds.  Runtime
// ownership must select the host inventory: advertising the Linux `inotify'
// feature while retaining Darwin's kqueue-owned function cells makes the
// actual backend unreachable through ordinary Lisp.
#[cfg(target_os = "linux")]
pub(crate) mod generated_gnu_c_forwarded_variables_linux;
#[cfg(target_os = "macos")]
pub(crate) mod generated_gnu_c_forwarded_variables_macos;
#[allow(dead_code)]
mod generated_gnu_c_primitives_linux;
/// The names the contracted oracle build forwards to a C slot
/// (`SYMBOL_FORWARDED'), sorted, for the contracted Linux and macOS builds.
/// Unprobed platforms retain an empty manifest.
pub(crate) fn gnu_c_forwarded_variables() -> &'static [&'static str] {
    #[cfg(target_os = "linux")]
    {
        generated_gnu_c_forwarded_variables_linux::GNU_C_FORWARDED_VARIABLES
    }
    #[cfg(target_os = "macos")]
    {
        generated_gnu_c_forwarded_variables_macos::GNU_C_FORWARDED_VARIABLES
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        &[]
    }
}

#[cfg(target_os = "linux")]
pub(crate) mod generated_gnu_c_forwarded_defaults_linux;
/// The `-Q --batch' values the contracted oracle gives the C-forwarded
/// variables Emaxx has no C owner for, as `(name, printed value)'; empty
/// off Linux (disclosed in the ledger).
pub(crate) fn gnu_c_forwarded_defaults() -> &'static [(&'static str, &'static str)] {
    #[cfg(target_os = "linux")]
    {
        generated_gnu_c_forwarded_defaults_linux::GNU_C_FORWARDED_DEFAULTS
    }
    #[cfg(not(target_os = "linux"))]
    {
        &[]
    }
}

/// The forwarded names the contracted oracle already reports as
/// `SYMBOL_LOCALIZED' at `-Q --batch' (buffer.c/keyboard.c make them
/// buffer-local during initialization), sorted.
pub(crate) fn gnu_c_localized_forwarded_variables() -> &'static [&'static str] {
    #[cfg(target_os = "linux")]
    {
        generated_gnu_c_forwarded_variables_linux::GNU_C_LOCALIZED_FORWARDED_VARIABLES
    }
    #[cfg(target_os = "macos")]
    {
        generated_gnu_c_forwarded_variables_macos::GNU_C_LOCALIZED_FORWARDED_VARIABLES
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        &[]
    }
}
#[cfg(target_os = "linux")]
pub(crate) use generated_gnu_c_primitives_linux::{
    GNU_C_PRIMITIVES, generated_gnu_c_primitive_available, generated_gnu_c_primitive_special_form,
};
mod fields;
mod hash_insert;
mod hooks_overlays;
mod insdel;
mod interactive;
mod invisibility;
mod keymap;
mod keys;
mod loading;
pub(crate) mod move_it;
mod numeric_time;
pub(crate) mod pdumper;
pub(crate) mod print;
mod processes;
mod purecopy;
mod regexp;
mod search;
mod sequences;
mod strftime;
mod strings;
mod styled_format;
mod sxhash;
pub(crate) mod syntax;
mod syntax_scan;
mod system;
mod text;
pub(crate) mod textprop;
mod tz;
pub(crate) mod values;
mod window;

pub(crate) use super::eval::is_special_form_name;
pub(crate) use accessors_random::*;
pub(crate) use buffers::*;
pub(crate) use case::*;
pub(crate) use coding::*;
pub(crate) use color_lcms::*;
pub(crate) use completion::*;
pub(crate) use dispatch::StringCompositions;
pub(crate) use dispatch::display::log_message_text;
pub(crate) use dispatch::display::prin1_to_string;
pub(crate) use dispatch::gnutls::{AsyncGnuTlsProgress, progress_async_gnutls};
pub(crate) use fields::*;
pub(crate) use file_io::*;
pub(crate) use filelock::*;
pub(crate) use hash_insert::*;
pub(crate) use hooks_overlays::*;
pub(crate) use insdel::*;
pub(crate) use interactive::*;
pub(crate) use invisibility::*;
pub(crate) use keymap::*;
pub(crate) use keys::*;
pub(crate) use loading::*;
pub(crate) use numeric_time::*;
pub(crate) use print::*;
pub(crate) use processes::*;
pub(crate) use purecopy::*;
pub(crate) use sequences::*;
pub(crate) use strings::*;
pub(crate) use styled_format::*;
pub(crate) use sxhash::*;
pub(crate) use system::*;
pub use system::{DaemonState, set_build_details, set_daemon_state};
pub(crate) use text::*;
pub(crate) use textprop::*;
pub(crate) use values::*;
pub(crate) use window::*;

pub(crate) const STANDARD_FRINGE_BITMAPS: &[&str] = &[
    "question-mark",
    "exclamation-mark",
    "left-arrow",
    "right-arrow",
    "up-arrow",
    "down-arrow",
    "left-curly-arrow",
    "right-curly-arrow",
    "large-circle",
    "left-triangle",
    "right-triangle",
    "top-left-angle",
    "top-right-angle",
    "bottom-left-angle",
    "bottom-right-angle",
    "left-bracket",
    "right-bracket",
    "filled-rectangle",
    "hollow-rectangle",
    "filled-square",
    "hollow-square",
    "vertical-bar",
    "horizontal-bar",
    "empty-line",
];

const RAW_BYTE_REGEX_BASE: u32 = 0xE000;
static TEMP_NAME_COUNTER: AtomicU64 = AtomicU64::new(0);
static MAKE_SYMBOL_COUNTER: AtomicU64 = AtomicU64::new(0);

/// The next id `make-symbol' would give an uninterned symbol.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn next_make_symbol_id() -> u64 {
    MAKE_SYMBOL_COUNTER.fetch_add(1, AtomicOrdering::Relaxed)
}
static FILE_NOTIFY_DESCRIPTOR_COUNTER: AtomicU64 = AtomicU64::new(1);

fn signal_condition(condition: &str) -> LispError {
    // (signal CONDITION nil): the error object is (CONDITION), not
    // (CONDITION nil) — a nil DATA must not surface as a ": nil" tail
    // in the echoed error message ("End of buffer", not
    // "End of buffer: nil").
    LispError::SignalValue(Value::list([Value::Symbol(condition.into())]))
}

pub(crate) fn wrong_type_argument(predicate: &str, value: Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("wrong-type-argument".into()),
        Value::Symbol(predicate.into()),
        value,
    ]))
}

pub(crate) fn arith_error() -> LispError {
    LispError::SignalValue(Value::list([Value::Symbol("arith-error".into())]))
}

pub(crate) use dispatch::DirectPrimitive;
pub(crate) use dispatch::FnvBuildHasher;
pub(crate) use dispatch::NameFacts;
pub(crate) use dispatch::compute_name_facts;
pub(crate) use dispatch::echo_area_message;
#[cfg(test)]
pub(crate) use dispatch::echo_area_message_with_spans;
#[cfg(test)]
pub(crate) use dispatch::has_dispatch_handler;
pub(crate) use dispatch::live_ordinary_window_ids;
pub(crate) use dispatch::mark_thread_local_roots;
pub(crate) use dispatch::name_facts;
pub(crate) use dispatch::native_elisp_load;
#[cfg(test)]
pub(crate) use dispatch::render_mode_line_glass;
pub(crate) use dispatch::set_internal_symbol;
pub(crate) use dispatch::unibyte_doc_string;
pub(crate) use dispatch::{
    EchoSpans, echo_area_message_tick, echo_area_print_logged, echo_display_message,
    expire_echo_area_message, set_echo_area_message, set_echo_area_message_with_spans,
    string_face_spans,
};
pub(crate) use dispatch::{
    LineNumberLayout, LineNumberMode, ModeLineCells, TtyFaceAttrs, WindowRenderInfo,
    adjust_frame_size, apply_staged_root_window_height, change_frame_size,
    do_pending_window_change, frame_top_margin, render_window_header_line, render_window_mode_line,
    render_window_tab_line, resolve_tty_face_attrs, run_window_change_functions,
    store_window_hscroll_state, window_face_spans, window_hscroll_state, window_line_number_layout,
    window_render_layout, window_text_area_columns, window_total_columns,
};
pub use dispatch::{call, is_builtin};
pub(crate) use dispatch::{call_with_facts, call_with_facts_arity_checked};
pub(crate) use search::category_member;
pub(crate) use search::fast_string_match;
#[cfg(test)]
pub(crate) use search::{elisp_regex_compile_count, reset_elisp_regex_compile_count};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod compat_runtime_tests;
