use super::eval::{BufferDisposition, Interpreter, error_condition_value};
use super::json::{self, JsonArrayType, JsonObjectType, JsonParseOptions};
use super::sqlite;
use super::types::{
    ConsSlot, Env, LispError, SharedStringState, StringPropertySpan, Value, shared_env,
};
use crate::buffer::TextPropertySpan;
use chrono::{Datelike, FixedOffset, Local, TimeZone, Timelike, Utc};
use fancy_regex::Regex as FancyRegex;
use flate2::read::GzDecoder;
use lcms2::{CIECAM02, CIELab, CIELabExt, CIEXYZ, JCh, ViewingConditions};
use lcms2_sys::Surround;
use num_bigint::{BigInt, Sign};
use num_traits::{Signed, ToPrimitive, Zero};
use regex::Regex;
use roxmltree::{Document, Node, NodeType};
use sha1::{Digest, Sha1};
use sha2::{Sha224, Sha256, Sha384, Sha512};
use sha3::{Sha3_224, Sha3_256, Sha3_384, Sha3_512};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
#[cfg(unix)]
use std::ffi::CString;
use std::fs;
use std::io::ErrorKind;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::os::unix::fs::symlink;
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{
    cell::RefCell,
    rc::{Rc, Weak},
};
use unicode_general_category::get_general_category;
use unicode_names2::name as unicode_name;
use unicode_width::UnicodeWidthChar;

mod accessors_random;
mod buffers;
mod case;
mod coding;
mod color_lcms;
mod completion;
mod dispatch;
mod file_io;
mod hash_insert;
mod hooks_overlays;
mod interactive;
mod keys;
mod loading;
mod modes;
mod numeric_time;
mod objects;
mod print;
mod processes;
mod regexp;
mod sequences;
mod strings;
mod syntax;
mod system;
mod text;
mod values;

pub(crate) use accessors_random::*;
pub(crate) use buffers::*;
pub(crate) use case::case_table_default_value;
pub(crate) use case::*;
pub(crate) use coding::*;
pub(crate) use color_lcms::*;
pub(crate) use completion::*;
pub(crate) use file_io::*;
pub(crate) use hash_insert::*;
pub(crate) use hooks_overlays::*;
pub(crate) use interactive::*;
pub(crate) use keys::*;
pub(crate) use loading::*;
pub(crate) use numeric_time::*;
pub(crate) use objects::*;
pub(crate) use print::*;
pub(crate) use processes::*;
pub(crate) use sequences::*;
pub(crate) use strings::*;
pub(crate) use system::*;
pub(crate) use text::*;
pub(crate) use values::*;

const RAW_CHAR_SENTINEL: char = '\u{F8FF}';
const RAW_BYTE_REGEX_BASE: u32 = 0xE000;
type FileChangeFingerprint = Option<(u64, u128)>;
type FileChangeCache = HashMap<String, FileChangeFingerprint>;
type VectorSlotCache = HashMap<usize, (Weak<RefCell<Value>>, Rc<Vec<ConsSlot>>)>;
static SYSTEM_CONFIGURATION: OnceLock<String> = OnceLock::new();
static TEMP_NAME_COUNTER: AtomicU64 = AtomicU64::new(0);
static GENSYM_COUNTER: AtomicU64 = AtomicU64::new(0);
static MAKE_SYMBOL_COUNTER: AtomicU64 = AtomicU64::new(0);
static FILE_NOTIFY_DESCRIPTOR_COUNTER: AtomicU64 = AtomicU64::new(1);
static RANDOM_STATE: AtomicU64 = AtomicU64::new(0x1234_5678_9abc_def0);
static RANDOM_SEED_COUNTER: AtomicU64 = AtomicU64::new(0);
static FILE_CHANGE_CACHE: OnceLock<Mutex<FileChangeCache>> = OnceLock::new();
static ACTIVE_FILE_NOTIFY_DESCRIPTORS: OnceLock<Mutex<HashSet<i64>>> = OnceLock::new();
const TREESIT_LINECOL_CACHE_VAR: &str = "emaxx--treesit-linecol-cache";
const BUFFER_MENU_BUFFER_NAME: &str = "*Buffer List*";
const BUFFER_MENU_ENTRIES_VAR: &str = "emaxx--buffer-menu-entries";
const DEFAULT_SELECTED_WINDOW_HEIGHT: usize = 24;
const WINDOW_BUFFER_SLOT: usize = 0;
const WINDOW_START_SLOT: usize = 1;

thread_local! {
    static VECTOR_SLOT_CACHE: RefCell<VectorSlotCache> = RefCell::new(HashMap::new());
}

fn is_sqlite_builtin(name: &str) -> bool {
    matches!(
        name,
        "sqlite-open"
            | "sqlite-close"
            | "sqlite-execute"
            | "sqlite-select"
            | "sqlite-execute-batch"
            | "sqlite-transaction"
            | "sqlite-commit"
            | "sqlite-rollback"
            | "sqlite-load-extension"
            | "sqlite-next"
            | "sqlite-more-p"
            | "sqlite-finalize"
            | "sqlite-version"
            | "sqlitep"
            | "sqlite-available-p"
    )
}

fn is_time_builtin(name: &str) -> bool {
    matches!(
        name,
        "current-time-zone"
            | "current-time"
            | "current-time-string"
            | "decode-time"
            | "decoded-time-day"
            | "decoded-time-dst"
            | "decoded-time-hour"
            | "decoded-time-minute"
            | "decoded-time-month"
            | "decoded-time-second"
            | "decoded-time-weekday"
            | "decoded-time-year"
            | "decoded-time-zone"
            | "encode-time"
            | "float-time"
            | "format-time-string"
            | "time-add"
            | "time-convert"
            | "time-equal-p"
            | "time-less-p"
            | "time-since"
            | "time-subtract"
    )
}

fn is_lcms_builtin(name: &str) -> bool {
    matches!(
        name,
        "lcms-cie-de2000"
            | "lcms-xyz->jch"
            | "lcms-jch->xyz"
            | "lcms-jch->jab"
            | "lcms-jab->jch"
            | "lcms-cam02-ucs"
            | "lcms2-available-p"
            | "lcms-temp->white-point"
    )
}

fn treesit_linecol_cache_value(line: i64, col: i64, bytepos: i64) -> Value {
    Value::list([
        Value::Symbol(":line".into()),
        Value::Integer(line),
        Value::Symbol(":col".into()),
        Value::Integer(col),
        Value::Symbol(":bytepos".into()),
        Value::Integer(bytepos),
    ])
}

fn treesit_default_linecol_cache() -> Value {
    treesit_linecol_cache_value(0, 0, 0)
}

fn treesit_linecol_at(interp: &Interpreter, pos: usize) -> Result<Value, LispError> {
    let buffer = interp.current_buffer();
    if pos < buffer.point_min() || pos > buffer.point_max() {
        return Err(LispError::Signal("args-out-of-range".into()));
    }
    let mut line = 1i64;
    let mut col = 0i64;
    for current in buffer.point_min()..pos {
        match buffer.char_at(current) {
            Some('\n') => {
                line += 1;
                col = 0;
            }
            Some(_) => col += 1,
            None => {}
        }
    }
    Ok(Value::cons(Value::Integer(line), Value::Integer(col)))
}

fn signal_condition(condition: &str) -> LispError {
    LispError::SignalValue(Value::list([Value::Symbol(condition.into()), Value::Nil]))
}

fn scan_error() -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("scan-error".into()),
        Value::Nil,
    ]))
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

fn line_distance_in_buffer(
    interp: &Interpreter,
    buffer_id: u64,
    start: usize,
    target: usize,
) -> usize {
    if target <= start {
        return 0;
    }
    let text = if buffer_id == interp.current_buffer_id() {
        interp.buffer.buffer_substring(start, target)
    } else {
        interp
            .get_buffer_by_id(buffer_id)
            .map(|buffer| buffer.buffer_substring(start, target))
            .unwrap_or_else(|| interp.buffer.buffer_substring(start, target))
    }
    .unwrap_or_default();
    text.chars().filter(|ch| *ch == '\n').count()
}

fn replace_buffer_contents(
    interp: &mut Interpreter,
    buffer_id: u64,
    text: &str,
) -> Result<(), LispError> {
    let buffer = interp
        .get_buffer_by_id_mut(buffer_id)
        .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
    let end = buffer.point_max();
    let _ = buffer.delete_region(1, end);
    buffer.goto_char(1);
    buffer.insert(text);
    buffer.goto_char(1);
    Ok(())
}

fn current_line_text(interp: &mut Interpreter) -> Result<String, LispError> {
    let saved = interp.buffer.point();
    let start = interp.buffer.beginning_of_line();
    interp.buffer.goto_char(saved);
    let end = interp.buffer.end_of_line();
    interp.buffer.goto_char(saved);
    interp
        .buffer
        .buffer_substring(start, end)
        .map_err(|error| LispError::Signal(error.to_string()))
}

fn advertised_function_name(interp: &Interpreter, value: &Value) -> Result<String, LispError> {
    interp.function_binding_name(value).ok_or_else(|| {
        LispError::SignalValue(Value::list([
            Value::Symbol("invalid-function".into()),
            value.clone(),
        ]))
    })
}

fn thread_list_thread_at_point(interp: &mut Interpreter) -> Result<u64, LispError> {
    let line = current_line_text(interp)?;
    let Some(name) = line.split_whitespace().next() else {
        return Err(LispError::Signal("No thread at point".into()));
    };
    for thread in interp.live_threads() {
        let thread_id = interp.resolve_thread_id(&thread)?;
        if interp.thread_name(thread_id).as_deref() == Some(name) {
            return Ok(thread_id);
        }
    }
    Err(LispError::Signal("No thread at point".into()))
}

fn thread_list_row(
    interp: &mut Interpreter,
    thread_id: u64,
    env: &Env,
) -> Result<String, LispError> {
    let thread_value = Value::Record(thread_id);
    let thread_name = interp.thread_name(thread_id).unwrap_or_else(|| {
        if thread_value == interp.current_thread_value() {
            "Main".into()
        } else {
            format!("#<thread id:{thread_id}>")
        }
    });
    let (status, blocker) = if !interp.thread_live(thread_id) {
        (String::from("Finished"), String::new())
    } else if thread_value == interp.current_thread_value() {
        (String::from("Running"), String::new())
    } else {
        let blocker = interp.thread_blocker_value(thread_id);
        if blocker.is_truthy() {
            (
                String::from("Blocked"),
                render_prin1_ephemeral(interp, &blocker, env)?,
            )
        } else {
            (String::from("Yielded"), String::new())
        }
    };
    Ok(format!("{thread_name}\t{status}\t{blocker}\n"))
}

fn current_window_start(interp: &Interpreter) -> usize {
    interp.selected_window_start()
}

fn buffer_point_bounds(interp: &Interpreter, buffer_id: u64) -> (usize, usize) {
    interp
        .buffer_bounds_by_id(buffer_id)
        .unwrap_or((interp.buffer.point_min(), interp.buffer.point_max()))
}

fn buffer_line_start_at(interp: &Interpreter, buffer_id: u64, pos: usize) -> usize {
    if buffer_id == interp.current_buffer_id() {
        interp.buffer.line_start_at(pos)
    } else {
        interp
            .get_buffer_by_id(buffer_id)
            .map(|buffer| buffer.line_start_at(pos))
            .unwrap_or_else(|| interp.buffer.line_start_at(pos))
    }
}

fn set_current_window_start(interp: &mut Interpreter, start: usize) {
    let buffer_id = interp.selected_window_buffer_id();
    let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
    let start = start.clamp(point_min, point_max);
    let start = buffer_line_start_at(interp, buffer_id, start);
    interp.set_selected_window_start(start);
}

fn window_record_id_from_value(interp: &Interpreter, value: &Value) -> Option<u64> {
    match value {
        Value::Record(id)
            if interp
                .find_record(*id)
                .is_some_and(|record| record.type_name == "window") =>
        {
            Some(*id)
        }
        Value::Symbol(symbol) if symbol == "window" => Some(interp.selected_window_id()),
        _ => None,
    }
}

fn window_buffer_id(interp: &Interpreter, value: &Value) -> Option<u64> {
    match window_record_id_from_value(interp, value) {
        Some(id) if id == interp.selected_window_id() => Some(interp.selected_window_buffer_id()),
        Some(id) => interp
            .find_record(id)
            .and_then(|record| record.slots.get(WINDOW_BUFFER_SLOT))
            .and_then(|slot| slot.as_integer().ok())
            .map(|buffer_id| buffer_id.max(0) as u64),
        None => None,
    }
}

fn window_start(interp: &Interpreter, value: Option<&Value>) -> Result<usize, LispError> {
    match value {
        None => Ok(current_window_start(interp)),
        Some(value) => {
            let Some(id) = window_record_id_from_value(interp, value) else {
                return Err(LispError::TypeError("window".into(), value.type_name()));
            };
            if id == interp.selected_window_id() {
                return Ok(current_window_start(interp));
            }
            let buffer_id = window_buffer_id(interp, value).unwrap_or(interp.current_buffer_id());
            let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
            Ok(interp
                .find_record(id)
                .and_then(|record| record.slots.get(WINDOW_START_SLOT))
                .and_then(|slot| slot.as_integer().ok())
                .map(|start| start.clamp(point_min as i64, point_max as i64) as usize)
                .unwrap_or(point_min))
        }
    }
}

fn set_window_start_value(
    interp: &mut Interpreter,
    window: &Value,
    start: usize,
) -> Result<(), LispError> {
    let Some(id) = window_record_id_from_value(interp, window) else {
        return Err(LispError::TypeError("window".into(), window.type_name()));
    };
    let buffer_id = window_buffer_id(interp, window).unwrap_or(interp.current_buffer_id());
    let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
    let start = start.clamp(point_min, point_max);
    let start = buffer_line_start_at(interp, buffer_id, start);
    if id == interp.selected_window_id() {
        interp.set_selected_window_start(start);
        return Ok(());
    }
    let Some(record) = interp.find_record_mut(id) else {
        return Err(LispError::TypeError("window".into(), window.type_name()));
    };
    if record.slots.len() <= WINDOW_START_SLOT {
        record.slots.resize(WINDOW_START_SLOT + 1, Value::Nil);
    }
    record.slots[WINDOW_START_SLOT] = Value::Integer(start as i64);
    Ok(())
}

fn scroll_preserve_screen_position(interp: &Interpreter, env: &Env) -> bool {
    interp
        .lookup_var("scroll-preserve-screen-position", env)
        .is_some_and(|value| !value.is_nil())
}

fn resolve_window_line(value: Option<&Value>, default_line: usize) -> Result<isize, LispError> {
    let line = match value {
        None | Some(Value::Nil) => default_line as i64,
        Some(value) => prefix_numeric_value(value)?.as_integer()?,
    };
    Ok(if line >= 0 {
        line as isize
    } else {
        (DEFAULT_SELECTED_WINDOW_HEIGHT as isize + line as isize).max(0)
    })
}

fn scroll_selected_window(
    interp: &mut Interpreter,
    count: isize,
    env: &Env,
) -> Result<(), LispError> {
    let window_start = current_window_start(interp);
    let point_line = beginning_of_line_at(interp, interp.buffer.point());
    let point_offset = line_distance(interp, window_start, point_line);
    let (new_start, shortage) = move_lines_from(interp, window_start, count);
    if shortage != 0 {
        return Err(if count >= 0 {
            signal_condition("end-of-buffer")
        } else {
            signal_condition("beginning-of-buffer")
        });
    }

    set_current_window_start(interp, new_start);

    if scroll_preserve_screen_position(interp, env) || point_line < new_start {
        let (target, target_shortage) = move_lines_from(interp, new_start, point_offset as isize);
        if target_shortage > 0 {
            interp.buffer.goto_char(interp.buffer.point_max());
        } else {
            interp.buffer.goto_char(target);
        }
    }

    Ok(())
}

pub(crate) fn prefer_builtin_override(name: &str) -> bool {
    matches!(
        name,
        "user-error"
            | "byte-compile"
            | "byte-compile-check-lambda-list"
            | "cl-type-of"
            | "cl-find-class"
            | "cl--class-parents"
            | "cl--class-allparents"
            | "cl--class-children"
            | "eieio--object-class"
            | "eieio--class-name"
            | "eieio-object-p"
            | "slot-boundp"
            | "make-instance"
            | "clone"
            | "semanticdb-find-tags-by-class"
            | "semanticdb-find-tags-by-name"
            | "semanticdb-find-tags-for-completion"
            | "semantic-ctxt-current-symbol"
            | "semantic-ctxt-current-symbol-and-bounds"
            | "semantic-analyze-possible-completions"
            | "semantic-analyze-tag-references"
            | "semantic-analyze-refs-impl"
            | "semantic-analyze-refs-proto"
            | "semantic-equivalent-tag-p"
            | "semantic-go-to-tag"
            | "semantic-clear-toplevel-cache"
            | "semanticdb-typecache-find"
            | "semanticdb-typecache-add-dependant"
            | "eieio-oref"
            | "eieio-oset"
            | "slot-value"
            | "cl-typep"
            | "built-in-class-p"
            | "cl-functionp"
            | "url-scheme-get-property"
            | "macroexpand-1"
            | "macroexpand-all"
            | "cl-parse-integer"
            | "read-key"
            | "regexp-opt"
            | "rx-to-string"
            | "timerp"
            | "header-line-indent-mode"
    )
}

pub use dispatch::{call, is_builtin};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod lcms_response_tests;

#[cfg(test)]
mod compat_runtime_tests;
