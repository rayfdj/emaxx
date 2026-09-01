use super::eval::{BufferDisposition, Interpreter, RunningProcess};
use super::json::{self, JsonArrayType, JsonObjectType, JsonParseOptions};
use super::sqlite;
use super::types::{
    ConsSlot, EmacsTermination, Env, LispError, SharedStringState, StringPropertySpan, Value,
    WeakConsSlot, shared_env,
};
use crate::buffer::TextPropertySpan;
use chrono::{Datelike, FixedOffset, Local, TimeZone, Timelike, Utc};
use fancy_regex::Regex as FancyRegex;
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
use std::collections::{HashMap, HashSet, VecDeque};
#[cfg(unix)]
use std::ffi::CString;
use std::fs;
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
#[cfg(unix)]
use std::os::unix::fs::symlink;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{cell::RefCell, rc::Rc};
use unicode_general_category::get_general_category;
use unicode_width::UnicodeWidthChar;

mod accessors_random;
mod buffers;
mod case;
mod ccl;
pub(crate) mod coding;
mod color_lcms;
mod completion;
mod dispatch;
mod file_io;
mod generated_builtin_arities;
mod generated_gnu_c_defsyms;
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
#[allow(dead_code)]
mod generated_gnu_c_primitives_linux;
#[cfg(target_os = "linux")]
pub(crate) use generated_gnu_c_primitives_linux::{
    GNU_C_PRIMITIVES, generated_gnu_c_primitive_available, generated_gnu_c_primitive_special_form,
};
mod hash_insert;
mod hooks_overlays;
mod interactive;
mod invisibility;
mod keys;
mod loading;
mod numeric_time;
pub(crate) mod print;
mod processes;
mod regexp;
mod sequences;
mod strings;
pub(crate) mod syntax;
mod system;
mod text;
pub(crate) mod values;
mod window;

pub(crate) use super::eval::is_special_form_name;
pub(crate) use accessors_random::*;
pub(crate) use buffers::*;
pub(crate) use case::case_table_default_value;
pub(crate) use case::*;
pub(crate) use coding::*;
pub(crate) use color_lcms::*;
pub(crate) use completion::*;
pub(crate) use dispatch::gnutls::{AsyncGnuTlsProgress, progress_async_gnutls};
pub(crate) use file_io::*;
pub(crate) use hash_insert::*;
pub(crate) use hooks_overlays::*;
pub(crate) use interactive::*;
pub(crate) use invisibility::*;
pub(crate) use keys::*;
pub(crate) use loading::*;
pub(crate) use numeric_time::*;
pub(crate) use print::*;
pub(crate) use processes::*;
pub(crate) use sequences::*;
pub(crate) use strings::*;
pub(crate) use syntax::standard_syntax_table_default_value;
pub(crate) use system::*;
pub(crate) use text::*;
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

const RAW_CHAR_SENTINEL: char = '\u{F8FF}';
const RAW_BYTE_REGEX_BASE: u32 = 0xE000;
type VectorSlotCache = HashMap<usize, (WeakConsSlot, Rc<Vec<ConsSlot>>), dispatch::FnvBuildHasher>;
static SYSTEM_CONFIGURATION: OnceLock<String> = OnceLock::new();
static TEMP_NAME_COUNTER: AtomicU64 = AtomicU64::new(0);
static MAKE_SYMBOL_COUNTER: AtomicU64 = AtomicU64::new(0);
static FILE_NOTIFY_DESCRIPTOR_COUNTER: AtomicU64 = AtomicU64::new(1);
static RANDOM_STATE: AtomicU64 = AtomicU64::new(0x1234_5678_9abc_def0);
static RANDOM_SEED_COUNTER: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static VECTOR_SLOT_CACHE: RefCell<VectorSlotCache> = RefCell::new(HashMap::default());
}

fn signal_condition(condition: &str) -> LispError {
    // (signal CONDITION nil): the error object is (CONDITION), not
    // (CONDITION nil) — a nil DATA must not surface as a ": nil" tail
    // in the echoed error message ("End of buffer", not
    // "End of buffer: nil").
    LispError::SignalValue(Value::list([Value::Symbol(condition.into())]))
}

fn beginning_of_line_at(interp: &mut Interpreter, pos: usize) -> usize {
    let saved = interp.buffer.point();
    interp.buffer.goto_char(pos);
    let start = interp.buffer.beginning_of_line();
    interp.buffer.goto_char(saved);
    start
}

fn move_lines_from(interp: &mut Interpreter, start: usize, count: isize) -> (usize, isize) {
    let saved = interp.buffer.point();
    interp.buffer.goto_char(start);
    interp.buffer.beginning_of_line();
    let shortage = interp.buffer.forward_line(count);
    let target = interp.buffer.point();
    interp.buffer.goto_char(saved);
    (target, shortage)
}

fn line_distance(interp: &Interpreter, start: usize, target: usize) -> usize {
    if target <= start {
        return 0;
    }
    interp
        .buffer
        .buffer_substring(start, target)
        .unwrap_or_default()
        .chars()
        .filter(|ch| *ch == '\n')
        .count()
}

pub(crate) fn prefer_builtin_override(name: &str) -> bool {
    dispatch::prefer_builtin_override(name)
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

pub(crate) use dispatch::FnvBuildHasher;
pub(crate) use dispatch::NameFacts;
pub(crate) use dispatch::call_with_facts;
pub(crate) use dispatch::echo_area_message;
#[cfg(test)]
pub(crate) use dispatch::echo_area_message_with_spans;
#[cfg(test)]
pub(crate) use dispatch::has_dispatch_handler;
pub(crate) use dispatch::name_facts;
#[cfg(test)]
pub(crate) use dispatch::render_mode_line_glass;
pub(crate) use dispatch::{
    EchoSpans, echo_area_message_tick, echo_area_print, echo_display_message,
    expire_echo_area_message, set_echo_area_message, set_echo_area_message_with_spans,
    string_face_spans,
};
pub(crate) use dispatch::{
    LineNumberLayout, LineNumberMode, TtyFaceAttrs, WindowRenderInfo, render_window_header_line,
    render_window_mode_line, render_window_tab_line, resolve_tty_face_attrs,
    store_window_hscroll_state, window_face_spans, window_hscroll_state, window_line_number_layout,
    window_render_layout,
};
pub use dispatch::{call, is_builtin};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod compat_runtime_tests;
